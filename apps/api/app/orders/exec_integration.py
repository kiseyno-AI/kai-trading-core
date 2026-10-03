from __future__ import annotations

import asyncio
from collections import Counter, defaultdict
import copy
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import re
from typing import Any, Callable, Iterable, Sequence
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import select
from sqlalchemy.orm import defer

from app.agent_context.decision_input_quality import build_decision_input_quality_envelope
from app.agent_context.roles import SHORT_TERM_DECIDER_AGENT_NAME, SIGNAL_DECIDER_AGENT_NAME
from app.core.audit_logging import request_with_audit
from app.core.settings import settings
from app.db import session_scope
from app.db_models import SignalDeciderDecisionSnapshot, SignalDeciderExecutionPlanItem
from app.market.clock import async_get_market_state, get_market_state
from app.market.session import (
    CALENDAR_SOURCE_EXCHANGE_CALENDARS,
    CALENDAR_SOURCE_STATUS_FALLBACK,
    CALENDAR_SOURCE_STATUS_MISSING_DATE,
    CALENDAR_SOURCE_STATUS_UNAVAILABLE,
    async_get_krx_calendar_day,
)
from app.orders.execution_handoff_contract import (
    EXEC_AGENT_DECIDER_HANDOFF_SCHEMA_VERSION,
    EXECUTION_HANDOFF_ACK_SCHEMA_VERSION,
    EXECUTION_HANDOFF_CAPABILITY,
    EXECUTION_HANDOFF_CONTRACT_VERSION,
    EXECUTION_HANDOFF_SOURCE,
    EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION,
    EXECUTION_REVOCATION_CAPABILITY,
    EXECUTION_REVOCATION_SCHEMA_VERSION,
    EXECUTION_RUNTIME_DASHBOARD_READ_MODEL_SCHEMA_VERSION,
    EXECUTION_RUNTIME_TODAY_ACTIVITY_SCHEMA_VERSION,
    PORTFOLIO_SUMMARY_READ_MODEL_SOURCE,
    serialize_exec_agent_decider_handoff_payload,
    serialize_execution_daily_board_payload,
    serialize_execution_dashboard_freshness_payload,
    serialize_execution_monitored_symbols_payload,
    serialize_execution_plan_dispatch_payload,
    serialize_execution_revocation_payload,
    serialize_execution_runtime_exec_card_payload,
    serialize_execution_runtime_status_payload,
    serialize_execution_symbol_detail_payload,
    serialize_execution_today_activity_payload,
    validate_execution_plan_dispatch_ack,
    validate_execution_revocation_ack,
)
from app.orders.buy_authority_bridge import (
    build_core_buy_authority_observation,
    core_exec_buy_authority_bridge,
)
from app.orders.exec_payload_slimming import (
    compact_exec_item_link,
    compact_exec_readback,
    compact_exec_request_summary,
    compact_exec_response_item,
    compact_exec_response_summary,
    slim_signal_decider_exec_submission_payload,
    summarize_error,
)
from app.orders.next_open_execution import (
    ELIGIBILITY_BLOCKED,
    ELIGIBILITY_ELIGIBLE,
    ELIGIBILITY_MONITOR_CANDIDATE,
    EXECUTION_STATUS_AWAITING_TRIGGER,
    EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING,
    EXECUTION_STATUS_DISPATCH_PENDING_REMOTE_ACK,
    EXECUTION_STATUS_PLANNED,
    EXECUTION_STATUS_SUBMIT_FAILED,
    EXECUTION_STATUS_SUBMITTED,
    EXECUTION_WINDOW_NEXT_OPEN,
    NEXT_OPEN_COST_EVIDENCE_POLICY_VERSION,
    SHORT_TERM_MIN_RAW_TRADING_VALUE_KRW,
    SHORT_TERM_MIN_REWARD_RISK_RATIO,
    UNSPECIFIED_ROUTE_ACCOUNT_REFERENCE,
    _PlanRefreshRevisionConflict,
    _execution_plan_row_authority_fingerprint,
    _current_route_live_market_buy_authority,
    _is_canonical_closed_market_cost_absence,
    _load_live_market_snapshot_context,
    _async_previous_trading_day,
    async_normalize_next_open_target_trade_date,
    signal_decider_next_open_execution_service,
)
from app.orders.portfolio_summary_service import portfolio_summary_service
from app.orders.signal_decider_execution_outcomes import signal_decider_execution_outcome_service
from app.quotes.display_prices import (
    DISPLAY_FALLBACK_SOURCES,
    build_display_price_fields,
)
from app.quotes.kis_realtime_prices import (
    KISRealtimeMonitoredSymbolSource,
    kis_realtime_price_service,
)
from app.repositories.symbol_repository import load_symbols

INTEGRATION_MODE_DISABLED = 'disabled'
INTEGRATION_MODE_OBSERVE = 'observe'
INTEGRATION_MODE_DISPATCH = 'dispatch'
VALID_INTEGRATION_MODES = {
    INTEGRATION_MODE_DISABLED,
    INTEGRATION_MODE_OBSERVE,
    INTEGRATION_MODE_DISPATCH,
}

EXEC_SERVICE_NAME = 'kai-trading-exec'
EXEC_INTENT_NEXT_OPEN_BUY = 'next_open_buy'
EXEC_INTENT_INTRADAY_WATCH_BUY = 'intraday_watch_buy'
EXEC_INTENT_TARGET_SELL = 'target_sell'
EXEC_INTENT_STOP_SELL = 'stop_sell'
EXEC_ENTRY_BUY_INTENTS = {EXEC_INTENT_NEXT_OPEN_BUY, EXEC_INTENT_INTRADAY_WATCH_BUY}
EXEC_SOURCE_SCHEMA_VERSION = EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION

EXEC_DISPATCH_STATUS_PREVIEW = 'preview'
EXEC_DISPATCH_STATUS_PENDING_REMOTE_ACK = 'pending_remote_ack'
EXEC_DISPATCH_STATUS_ACCEPTED = 'accepted'
EXEC_DISPATCH_STATUS_PARTIAL = 'partial'
EXEC_DISPATCH_STATUS_FAILED = 'failed'
EXEC_DISPATCH_STATUS_SKIPPED = 'skipped'
EXEC_DISPATCH_STATUS_ALREADY_DISPATCHED = 'already_dispatched'
EXEC_DISPATCH_STATUS_NOT_CONFIGURED = 'not_configured'
EXEC_DISPATCH_STATUS_CONTRACT_ERROR = 'contract_error'
EXEC_DISPATCH_STATUS_UNAVAILABLE = 'unavailable'
MULTI_ACCOUNT_EXEC_ENDPOINT_UNSUPPORTED = 'multi_account_exec_endpoint_unsupported'
LIVE_EXEC_ACCOUNT_TARGET_REQUIRED = 'live_exec_account_target_required'
EXEC_DESTINATION_BINDING_SCHEMA_VERSION = 'core_exec_destination_binding_v1'
EXEC_DISPATCH_PROOF_SCHEMA_VERSION = 'core_exec_dispatch_proof_v2'
EXEC_POSITION_SCOPE_PROOF_SCHEMA_VERSION = 'execution_position_plan_scope_v1'
EXEC_RUNTIME_MONITORED_SYMBOL_MEMBERSHIP_SCHEMA_VERSION = (
    'runtime_monitored_symbol_membership_v1'
)
EXEC_RUNTIME_KRX_MEMBERSHIP_EXCHANGES = (
    'KOSDAQ',
    'KOSPI',
    'KRX',
    'NXT',
    'SOR',
)
EXEC_RUNTIME_KIS_EXECUTION_SCOPE_EXCHANGES = (
    'AMEX',
    'KOSDAQ',
    'KOSPI',
    'KRX',
    'NASD',
    'NXT',
    'NYSE',
    'SOR',
)
CORE_EXEC_ACTIVATION_BINDING_SCHEMA_VERSION = (
    'core_execution_activation_binding_v1'
)
EXECUTION_ACTIVATION_SCHEMA_VERSION = 'execution_handoff_activation_v1'
EXECUTION_ACTIVATION_CAPABILITY = 'core_confirmed_activation_v1'
CORE_ACTIVATION_FENCE_RETRY_ATTEMPTS = 6
CORE_ACTIVATION_FENCE_RETRY_DELAY_SECONDS = 0.025
EXEC_DECIDER_HANDOFF_MAX_BYTES = 12 * 1024
EXEC_ENTRY_RISK_CONTEXT_MAX_BYTES = 32 * 1024
EXEC_EXIT_RISK_CONTEXT_MAX_BYTES = 8 * 1024
EXEC_PROTECTIVE_EXIT_RISK_CONTEXT_ESSENTIAL_KEYS = (
    '_core_activation',
    'trigger_rule',
    'schema_version',
    'agent_name',
    'decision_agent_name',
    'core_plan_item_id',
    'account_reference',
    'target_trade_date',
    'source_market_date',
    'decision_snapshot_id',
    'decision_run_id',
    'target_price',
    'stop_loss_price',
    'confidence_score_10',
    'conviction_score_10',
    'market_regime_context',
    'decider_handoff_context_digest',
)

REMOTE_FETCH_STATUS_OK = 'ok'
REMOTE_FETCH_STATUS_DISABLED = 'disabled'
REMOTE_FETCH_STATUS_NOT_CONFIGURED = 'not_configured'
REMOTE_FETCH_STATUS_UNAVAILABLE = 'unavailable'
REMOTE_FETCH_STATUS_CONTRACT_ERROR = 'contract_error'

REMOTE_ACTIVE_STATUSES = {
    'accepted',
    'created',
    'monitoring',
    'open',
    'partial_fill',
    'partially_filled',
    'pending',
    'queued',
    'running',
    'submitted',
    'working',
}
REMOTE_FAILED_STATUSES = {'canceled', 'cancelled', 'failed', 'rejected'}
REMOTE_SCHEDULED_STATUSES = {'accepted', 'created', 'pending', 'queued'}
REMOTE_SUBMITTED_STATUSES = {'open', 'running', 'submitted', 'working'}
REMOTE_PARTIAL_STATUSES = {'partial', 'partial_fill', 'partially_filled'}
REMOTE_FILLED_STATUSES = {'complete', 'completed', 'done', 'filled'}
REMOTE_CANCELLED_STATUSES = {'canceled', 'cancelled'}
REMOTE_TERMINAL_FAILURE_STATUSES = REMOTE_FAILED_STATUSES - REMOTE_CANCELLED_STATUSES

UI_RUNTIME_STATUS_READY = 'ready'
UI_RUNTIME_STATUS_DEGRADED = 'degraded'
UI_RUNTIME_STATUS_OFFLINE = 'offline'
UI_RUNTIME_STATUS_PAUSED = 'paused'
UI_RUNTIME_STATUS_KILL_SWITCHED = 'kill_switched'

UI_BOARD_STATUS_PLANNED = 'planned'
UI_BOARD_STATUS_ACCEPTED = 'accepted'
UI_BOARD_STATUS_SCHEDULED = 'scheduled'
UI_BOARD_STATUS_SUBMITTED = 'submitted'
UI_BOARD_STATUS_PARTIAL = 'partial'
UI_BOARD_STATUS_FILLED = 'filled'
UI_BOARD_STATUS_CANCELLED = 'cancelled'
UI_BOARD_STATUS_FAILED = 'failed'

UI_MONITORING_STATE_WATCHING_TARGET_STOP = 'watching_target_stop'
UI_MONITORING_STATE_WATCHING_FILL = 'watching_fill'
UI_MONITORING_STATE_RECONCILING = 'reconciling'
UI_MONITORING_STATE_IDLE = 'idle'

EXEC_AGENT_APPROVE_ACTIONS = {'approve', 'approved'}
EXEC_AGENT_REJECT_ACTIONS = {
    'block',
    'blocked',
    'cancel',
    'cancelled',
    'decline',
    'declined',
    'denied',
    'deny',
    'expired',
    'failed',
    'not_approved',
    'reject',
    'rejected',
}
EXEC_MONITORING_ACTIVE_STATUSES = {
    'accepted',
    'active',
    'created',
    'monitoring',
    'open',
    'partial',
    'partial_fill',
    'partially_filled',
    'pending',
    'queued',
    'running',
    'scheduled',
    'submitted',
    'target_sell_monitoring',
    'watch_monitoring_active',
    'watch_monitoring_pending',
    'working',
}
EXEC_MONITORING_TERMINAL_STATUSES = {
    'canceled',
    'cancelled',
    'closed',
    'declined',
    'denied',
    'done',
    'expired',
    'failed',
    'not_approved',
    'reject',
    'rejected',
    'stop_sell_cancelled',
    'target_sell_cancelled',
}
EXEC_AGENT_ACCEPTED_ENTRY_STATUSES = {
    'accepted',
    'active',
    'created',
    'monitoring',
    'open',
    'partial',
    'partial_fill',
    'partially_filled',
    'pending',
    'queued',
    'scheduled',
    'submitted',
    'watch_monitoring_active',
    'working',
}
EXEC_WATCH_STATE_SYNC_SCHEMA_VERSION = 'exec_watch_state_sync_v1'
EXEC_WATCH_STATE_SYNC_WARNING_CODE = 'exec_watch_state_divergence'
EXEC_WATCH_READBACK_ACTIVE_STATUSES = EXEC_MONITORING_ACTIVE_STATUSES | EXEC_AGENT_ACCEPTED_ENTRY_STATUSES | {
    'watch_buy_submitted',
    'watch_monitoring_pending',
}
MARKET_REGIME_AGGRESSIVE_OPEN_MULTIPLIER_CAP = Decimal('1.0')
MARKET_REGIME_TREND_DAY_MULTIPLIER_CAP = Decimal('1.0')
MARKET_REGIME_DEFAULT_MULTIPLIER_CAP = Decimal('1.0')
MARKET_REGIME_REQUIRED_EXEC_GATES = [
    'kill_switch',
    'account_drawdown_gate',
    'symbol_caps',
    'max_notional_caps',
    'stop_loss_required',
]

DASHBOARD_ENDPOINT_DAILY_BOARD = 'daily_board'
DASHBOARD_ENDPOINT_MONITORED_SYMBOLS = 'monitored_symbols'
DASHBOARD_ENDPOINT_SYMBOL_DETAIL = 'symbol_detail'
DASHBOARD_READ_MODEL_SCHEMA_VERSION = EXECUTION_RUNTIME_DASHBOARD_READ_MODEL_SCHEMA_VERSION
DASHBOARD_REFRESH_STATUS_NOT_REQUESTED = 'not_requested'
DASHBOARD_REFRESH_STATUS_SCHEDULED = 'scheduled'
DASHBOARD_REFRESH_STATUS_RUNNING = 'running'
DASHBOARD_REFRESH_STATUS_DEBOUNCED = 'debounced'
DASHBOARD_REFRESH_STATUS_SUCCEEDED = 'succeeded'
DASHBOARD_REFRESH_STATUS_TIMEOUT = 'timeout'
DASHBOARD_REFRESH_STATUS_FAILED = 'failed'
DASHBOARD_FRESHNESS_STATUS_FRESH = 'fresh'
DASHBOARD_FRESHNESS_STATUS_STALE = 'stale'
DASHBOARD_FRESHNESS_STATUS_PARTIAL = 'partial'
DASHBOARD_FRESHNESS_STATUS_REFRESHING = 'refreshing'
DASHBOARD_FRESHNESS_STATUS_UNAVAILABLE = 'unavailable'
DASHBOARD_TODAY_ACTIVITY_SCHEMA_VERSION = EXECUTION_RUNTIME_TODAY_ACTIVITY_SCHEMA_VERSION
DASHBOARD_TODAY_ACTIVITY_BROKER_ORDER_ID_LIMIT = 5
EXEC_RUNTIME_MONITORED_MEMBERSHIP_MAX_AGE = timedelta(seconds=60)
EXEC_RUNTIME_MONITORED_MEMBERSHIP_MAX_FUTURE_SKEW = timedelta(seconds=30)
REALTIME_MONITORED_PORTFOLIO_WAIT_SECONDS = 5.0

MARKET_TIME_ZONE = ZoneInfo('Asia/Seoul')
DELEGATED_GATE_POLICY_VERSION = 'core_delegated_gate_policy_v1'
DELEGATED_GATE_LIQUIDITY_CODE = 'short_term_liquidity_floor_block_delegated_to_exec_watch'
DELEGATED_GATE_REWARD_RISK_CODE = 'short_term_reward_risk_ratio_block_delegated_to_exec_watch'
DELEGATED_GATE_LIVE_COST_CODE = 'transaction_cost_evidence_pending_live_delegated_to_exec_watch'
DELEGATED_GATE_REQUIRED_FIELDS = frozenset(
    {
        'code',
        'observed_value',
        'field_path',
        'comparator',
        'threshold',
        'policy_version',
        'observed_at',
        'recheck_source',
        'expires_at',
    }
)
CALENDAR_FALLBACK_STATUSES = {
    CALENDAR_SOURCE_STATUS_FALLBACK,
    CALENDAR_SOURCE_STATUS_MISSING_DATE,
    CALENDAR_SOURCE_STATUS_UNAVAILABLE,
}


@dataclass
class DashboardReadModelCacheRecord:
    payload: dict[str, Any]
    cached_at: datetime


@dataclass(frozen=True)
class ExecRuntimeMonitoredSymbolMembership:
    symbols: frozenset[str]
    membership_complete: bool
    source_version: datetime | None = None


@dataclass(frozen=True)
class PortfolioHoldingSymbolMembership:
    symbols: frozenset[str]
    membership_complete: bool
    source_version: datetime | None = None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def _retry_plan_refresh_revision_conflict(
    operation: Callable[[], Any],
) -> Any:
    # Keep the remote-activation safety fence fail-closed, while allowing the
    # peer transaction that accepted the same plan to release its date lock.
    for attempt in range(CORE_ACTIVATION_FENCE_RETRY_ATTEMPTS):
        try:
            return await asyncio.to_thread(operation)
        except _PlanRefreshRevisionConflict:
            if attempt + 1 >= CORE_ACTIVATION_FENCE_RETRY_ATTEMPTS:
                raise
            await asyncio.sleep(
                CORE_ACTIVATION_FENCE_RETRY_DELAY_SECONDS * (2**attempt)
            )
    raise AssertionError('unreachable plan refresh conflict retry state')


def _normalize_optional_text(value: Any, *, lowercase: bool = False) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    return normalized.lower() if lowercase else normalized


def _unsupported_exec_sizing_route_reason(
    traceability: object,
) -> str | None:
    payload = traceability if isinstance(traceability, dict) else {}
    market = str(payload.get('market') or '').strip().upper()
    exchange = str(payload.get('exchange') or '').strip().upper()
    currency = str(payload.get('currency') or '').strip().upper()
    asset_class = str(payload.get('asset_class') or '').strip().lower()
    broker = str(payload.get('broker') or '').strip().lower()
    provider = str(payload.get('provider') or '').strip().lower()
    venue = str(payload.get('venue') or '').strip().lower()
    session_policy = (
        payload.get('session_policy')
        if isinstance(payload.get('session_policy'), dict)
        else {}
    )
    timezone_name = str(session_policy.get('timezone') or '').strip().lower()
    us_route_markers = {'US', 'NASDAQ', 'NASD', 'NYSE', 'NYSE_AMERICAN', 'AMEX'}
    if (
        market in us_route_markers
        or exchange in us_route_markers
        or currency == 'USD'
        or asset_class == 'us_equity'
        or timezone_name == 'america/new_york'
    ):
        return 'unsupported_us_equity_exec_agent_sizing'
    if (
        (market and market not in {'KRX', 'KOSPI', 'KOSDAQ', 'KONEX'})
        or (exchange and exchange not in {'KRX', 'KOSPI', 'KOSDAQ', 'KONEX'})
        or (currency and currency != 'KRW')
        or (asset_class and asset_class not in {'stock', 'kr_equity'})
        or (broker and broker != 'kis')
        or (provider and provider != 'kis')
        or (venue and venue != 'kis')
        or (timezone_name and timezone_name != 'asia/seoul')
        or session_policy.get('allow_extended_hours') is True
    ):
        return 'unsupported_execution_route_for_v4'
    return None


def _normalize_date(value: str | date | None) -> date:
    if isinstance(value, date):
        return value
    if value is None:
        state = get_market_state()
        return date.fromisoformat(state.server_time[:10])
    return date.fromisoformat(str(value))


def _calendar_day_requires_operator_override(calendar_day: Any) -> bool:
    source = _normalize_optional_text(getattr(calendar_day, 'calendar_source', None), lowercase=True)
    status = _normalize_optional_text(getattr(calendar_day, 'calendar_source_status', None), lowercase=True)
    if source == CALENDAR_SOURCE_EXCHANGE_CALENDARS:
        return True
    return bool(status and status in CALENDAR_FALLBACK_STATUSES)


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None or value == '':
        return None
    if isinstance(value, Decimal):
        return value if value.is_finite() else None
    try:
        parsed = Decimal(str(value))
        return parsed if parsed.is_finite() else None
    except (InvalidOperation, ValueError):
        return None


def _positive_decimal(value: Any) -> Decimal | None:
    parsed = _safe_decimal(value)
    if parsed is not None and parsed > Decimal('0'):
        return parsed
    return None


def _decimal_to_text(value: Decimal | None) -> str | None:
    if value is None or not value.is_finite():
        return None
    return format(value, 'f')


def _to_json_number(value: Decimal | None) -> int | float | None:
    if value is None or not value.is_finite():
        return None
    if value == value.to_integral():
        return int(value)
    rendered = float(value)
    return rendered if math.isfinite(rendered) else None


def _market_regime_multiplier_cap(market_regime: str | None) -> Decimal:
    if market_regime == 'aggressive_open':
        return MARKET_REGIME_AGGRESSIVE_OPEN_MULTIPLIER_CAP
    if market_regime == 'trend_day':
        return MARKET_REGIME_TREND_DAY_MULTIPLIER_CAP
    return MARKET_REGIME_DEFAULT_MULTIPLIER_CAP


def _build_market_regime_sizing_policy(market_regime_context: dict[str, Any]) -> dict[str, Any]:
    market_regime = _normalize_optional_text(market_regime_context.get('market_regime'), lowercase=True)
    if market_regime is None:
        market_regime = _normalize_optional_text(market_regime_context.get('risk_mode'), lowercase=True) or 'normal'
    quantity_multiplier = _safe_decimal(market_regime_context.get('quantity_multiplier'))
    recommended_sizing_multiplier = _safe_decimal(
        market_regime_context.get('recommended_sizing_multiplier')
    )
    if quantity_multiplier is not None:
        raw_multiplier = quantity_multiplier
    elif recommended_sizing_multiplier is not None:
        raw_multiplier = recommended_sizing_multiplier
    else:
        raw_multiplier = Decimal('1')
    sizing_window = (
        copy.deepcopy(market_regime_context.get('sizing_window'))
        if isinstance(market_regime_context.get('sizing_window'), dict)
        else None
    )
    cap = _market_regime_multiplier_cap(market_regime)
    sizing_window_cap = _safe_decimal((sizing_window or {}).get('quantity_multiplier_cap'))
    if sizing_window_cap is not None:
        cap = min(cap, max(sizing_window_cap, Decimal('0')))
    multiplier = min(max(raw_multiplier, Decimal('0')), cap)
    live_upsize_status = _normalize_optional_text(
        market_regime_context.get('live_upsize_status'),
        lowercase=True,
    )
    if live_upsize_status is None:
        live_upsize_status = (
            'shadow_only_uncalibrated'
            if market_regime in {'aggressive_open', 'trend_day'}
            else 'not_applicable'
        )
    if sizing_window is not None:
        sizing_window['quantity_multiplier_cap'] = _to_json_number(cap)
        sizing_window['entry_priority_boost'] = False
        sizing_window['live_upsize_status'] = live_upsize_status
    valid_until = (
        _normalize_optional_text(market_regime_context.get('valid_until'))
        or _normalize_optional_text((sizing_window or {}).get('valid_until'))
    )
    return {
        'schema_version': 'market_regime_sizing_v1',
        'market_regime': market_regime,
        'regime_confidence': market_regime_context.get('regime_confidence'),
        'regime_reasons': list(market_regime_context.get('regime_reasons') or []),
        'quantity_multiplier': _to_json_number(multiplier),
        'raw_quantity_multiplier': _to_json_number(raw_multiplier),
        'quantity_multiplier_cap': _to_json_number(cap),
        'entry_priority_boost': False,
        'live_upsize_status': live_upsize_status,
        'valid_until': valid_until,
        'sizing_window': sizing_window,
        'exec_must_enforce_window': market_regime in {'aggressive_open', 'trend_day'},
        'required_exec_gates': list(MARKET_REGIME_REQUIRED_EXEC_GATES),
        'core_bypasses_safety_gates': False,
        'stop_loss_required': True,
    }


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return _decimal_to_text(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    return value


def _dispatch_json_size(value: Any) -> int:
    return len(
        json.dumps(
            _json_safe(value),
            ensure_ascii=False,
            sort_keys=True,
            separators=(',', ':'),
        ).encode('utf-8')
    )


def _dispatch_payload_digest(value: Any) -> str:
    encoded = json.dumps(
        _json_safe(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(',', ':'),
    ).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def _compact_dispatch_value(value: Any, *, depth: int = 0) -> Any:
    safe = _json_safe(value)
    if isinstance(safe, str):
        if len(safe) <= 512:
            return safe
        return {
            'preview': safe[:512],
            'truncated': True,
            'original_length': len(safe),
        }
    if isinstance(safe, list):
        if depth >= 3:
            return {
                'projection_digest': _dispatch_payload_digest(safe),
                'omitted_item_count': len(safe),
            }
        compacted = [
            _compact_dispatch_value(item, depth=depth + 1)
            for item in safe[:8]
        ]
        if len(safe) > 8:
            compacted.append({'omitted_item_count': len(safe) - 8})
        return compacted
    if isinstance(safe, dict):
        if depth >= 3:
            return {
                'projection_digest': _dispatch_payload_digest(safe),
                'omitted_key_count': len(safe),
            }
        compacted: dict[str, Any] = {}
        for index, (key, item) in enumerate(safe.items()):
            if index >= 24:
                compacted['omitted_key_count'] = len(safe) - 24
                break
            compacted[str(key)] = _compact_dispatch_value(
                item,
                depth=depth + 1,
            )
        return compacted
    return safe


def _bounded_dispatch_object(
    payload: dict[str, Any],
    *,
    max_bytes: int,
    label: str,
    essential_keys: Sequence[str],
) -> dict[str, Any]:
    safe_payload = _json_safe(payload)
    if not isinstance(safe_payload, dict):
        raise SignalDeciderExecPayloadContractError(
            f'{label} must be a JSON object'
        )
    original_size = _dispatch_json_size(safe_payload)
    if original_size <= max_bytes:
        return safe_payload

    compacted = _compact_dispatch_value(safe_payload)
    if not isinstance(compacted, dict):
        compacted = {}
    compacted['dispatch_projection'] = {
        'schema_version': 'exec_dispatch_projection_v1',
        'source_digest': _dispatch_payload_digest(safe_payload),
        'source_size_bytes': original_size,
    }
    required_projection_keys = {
        key for key in essential_keys if key in safe_payload
    }
    if (
        required_projection_keys.issubset(compacted)
        and _dispatch_json_size(compacted) <= max_bytes
    ):
        return compacted

    reduced = {
        key: _compact_dispatch_value(safe_payload[key], depth=1)
        for key in essential_keys
        if key in safe_payload
    }
    reduced['dispatch_projection'] = compacted['dispatch_projection']
    final_size = _dispatch_json_size(reduced)
    if final_size > max_bytes:
        raise SignalDeciderExecPayloadContractError(
            f'{label} exceeds bounded projection budget: '
            f'{final_size}>{max_bytes}'
        )
    return reduced


def _serialize_error(*, code: str, message: str, retryable: bool = False, status_code: int | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        'code': code,
        'message': message,
        'retryable': retryable,
    }
    if status_code is not None:
        payload['status_code'] = status_code
    return payload


def _sanitize_key(value: str) -> str:
    return re.sub(r'[^A-Za-z0-9]+', '_', value).strip('_') or 'unknown'


def _normalize_decision_agent_name(value: Any) -> str:
    normalized = _normalize_optional_text(value, lowercase=True)
    return normalized or SIGNAL_DECIDER_AGENT_NAME


def _normalize_short_term_generic_action(*, raw_action: Any, generic_action: Any) -> Any:
    tactical_action = _normalize_optional_text(raw_action, lowercase=True)
    if tactical_action == 'watch_for_trigger':
        return 'monitor_candidate'
    if tactical_action == 'scalp_buy_candidate':
        return 'buy'
    return generic_action


def _collect_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ('items', 'plans', 'orders', 'executions', 'positions', 'data', 'results'):
        candidate = payload.get(key)
        if isinstance(candidate, list):
            return [item for item in candidate if isinstance(item, dict)]
    if isinstance(payload.get('plan'), dict):
        return [payload['plan']]
    return []


def _delegated_gate_codes(
    validation: dict[str, Any],
    *,
    decision_agent_name: str,
) -> list[str]:
    warnings = validation.get('warnings') if isinstance(validation.get('warnings'), list) else []
    normalized_agent_name = _normalize_optional_text(decision_agent_name, lowercase=True)
    allowed_codes = (
        {DELEGATED_GATE_LIQUIDITY_CODE, DELEGATED_GATE_REWARD_RISK_CODE}
        if normalized_agent_name == SHORT_TERM_DECIDER_AGENT_NAME
        else {DELEGATED_GATE_LIVE_COST_CODE}
        if normalized_agent_name == SIGNAL_DECIDER_AGENT_NAME
        else set()
    )
    codes: list[str] = []
    for warning in warnings:
        if not isinstance(warning, dict):
            continue
        code = _normalize_optional_text(warning.get('code'), lowercase=True)
        if code not in allowed_codes or code in codes:
            continue
        codes.append(code)
    return codes


def _delegated_gate_observed_at(snapshot: SignalDeciderDecisionSnapshot | None) -> datetime | None:
    observed_at = getattr(snapshot, 'feedback_received_at', None)
    if not isinstance(observed_at, datetime):
        return None
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        return None
    return observed_at.astimezone(timezone.utc)


def _delegated_gates(
    *,
    row: SignalDeciderExecutionPlanItem,
    snapshot: SignalDeciderDecisionSnapshot | None,
    validation: dict[str, Any],
    codes: Sequence[str],
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    observed_at = _delegated_gate_observed_at(snapshot)
    evaluated_at = now or _utcnow()
    if evaluated_at.tzinfo is None or evaluated_at.utcoffset() is None:
        return []
    evaluated_at = evaluated_at.astimezone(timezone.utc)
    expires_at = datetime.combine(
        row.target_trade_date,
        time(hour=15, minute=30),
        tzinfo=MARKET_TIME_ZONE,
    )
    if (
        observed_at is None
        or observed_at > evaluated_at
        or observed_at > expires_at
        or evaluated_at >= expires_at
    ):
        return []
    observed_at_text = observed_at.isoformat()
    expires_at_text = expires_at.isoformat()
    short_term_quality = (
        validation.get('short_term_entry_quality')
        if isinstance(validation.get('short_term_entry_quality'), dict)
        else {}
    )
    performance_gate = (
        validation.get('performance_gate')
        if isinstance(validation.get('performance_gate'), dict)
        else {}
    )
    economics = (
        performance_gate.get('expected_return_economics')
        if isinstance(performance_gate.get('expected_return_economics'), dict)
        else {}
    )
    gate_specs: dict[str, dict[str, Any]] = {
        DELEGATED_GATE_LIQUIDITY_CODE: {
            'observed_value': _decimal_to_text(
                _safe_decimal(short_term_quality.get('raw_trading_value_krw'))
            ),
            'field_path': 'risk_context.validation.short_term_entry_quality.raw_trading_value_krw',
            'comparator': 'gte',
            'threshold': _decimal_to_text(SHORT_TERM_MIN_RAW_TRADING_VALUE_KRW),
            'recheck_source': 'exec_trigger_time_market_liquidity',
        },
        DELEGATED_GATE_REWARD_RISK_CODE: {
            'observed_value': _decimal_to_text(
                _safe_decimal(short_term_quality.get('reward_risk_ratio'))
            ),
            'field_path': 'risk_context.validation.short_term_entry_quality.reward_risk_ratio',
            'comparator': 'gte',
            'threshold': _decimal_to_text(SHORT_TERM_MIN_REWARD_RISK_RATIO),
            'recheck_source': 'exec_trigger_time_price_geometry_and_cost_model',
        },
    }
    if (
        economics.get('status') == 'pending_live_cost_confirmation'
        and economics.get('expected_slippage_bps') is None
        and economics.get('expected_slippage_basis') == 'unavailable'
    ):
        gate_specs[DELEGATED_GATE_LIVE_COST_CODE] = {
            'observed_value': None,
            'field_path': 'risk_context.execution_feasibility_context.expected_slippage_bps',
            'comparator': 'finite_non_negative_with_basis',
            'threshold': '0',
            'recheck_source': 'exec_trigger_time_quote_and_cost_model',
        }
    return [
        {
            'code': code,
            **gate_specs[code],
            'policy_version': DELEGATED_GATE_POLICY_VERSION,
            'observed_at': observed_at_text,
            'expires_at': expires_at_text,
        }
        for code in codes
        if code in gate_specs
    ]


def _delegated_gate_contract_error(
    *,
    decision_agent_name: str,
    codes: Any,
    confirmation_required: Any,
    gates: Any,
    now: datetime | None = None,
) -> str | None:
    if not isinstance(codes, list) or not isinstance(gates, list):
        return 'delegated_gate_collections_invalid'
    if confirmation_required is not bool(codes):
        return 'delegated_gate_confirmation_mismatch'
    normalized_agent_name = _normalize_optional_text(decision_agent_name, lowercase=True)
    allowed_codes = (
        {DELEGATED_GATE_LIQUIDITY_CODE, DELEGATED_GATE_REWARD_RISK_CODE}
        if normalized_agent_name == SHORT_TERM_DECIDER_AGENT_NAME
        else {DELEGATED_GATE_LIVE_COST_CODE}
        if normalized_agent_name == SIGNAL_DECIDER_AGENT_NAME
        else set()
    )
    if (
        any(not isinstance(code, str) or code not in allowed_codes for code in codes)
        or len(codes) != len(set(codes))
    ):
        return 'delegated_gate_code_not_allowlisted'
    if len(gates) != len(codes):
        return 'delegated_gate_count_mismatch'
    if any(not isinstance(gate, dict) for gate in gates):
        return 'delegated_gate_shape_invalid'
    if [gate.get('code') for gate in gates] != codes:
        return 'delegated_gate_code_binding_mismatch'

    evaluated_at = now or _utcnow()
    if evaluated_at.tzinfo is None or evaluated_at.utcoffset() is None:
        return 'delegated_gate_evaluation_time_invalid'
    evaluated_at = evaluated_at.astimezone(timezone.utc)
    field_contracts = {
        DELEGATED_GATE_LIQUIDITY_CODE: {
            'field_path': 'risk_context.validation.short_term_entry_quality.raw_trading_value_krw',
            'comparator': 'gte',
            'threshold': _decimal_to_text(SHORT_TERM_MIN_RAW_TRADING_VALUE_KRW),
            'recheck_source': 'exec_trigger_time_market_liquidity',
        },
        DELEGATED_GATE_REWARD_RISK_CODE: {
            'field_path': 'risk_context.validation.short_term_entry_quality.reward_risk_ratio',
            'comparator': 'gte',
            'threshold': _decimal_to_text(SHORT_TERM_MIN_REWARD_RISK_RATIO),
            'recheck_source': 'exec_trigger_time_price_geometry_and_cost_model',
        },
        DELEGATED_GATE_LIVE_COST_CODE: {
            'field_path': 'risk_context.execution_feasibility_context.expected_slippage_bps',
            'comparator': 'finite_non_negative_with_basis',
            'threshold': '0',
            'recheck_source': 'exec_trigger_time_quote_and_cost_model',
        },
    }
    for gate in gates:
        if set(gate) != DELEGATED_GATE_REQUIRED_FIELDS:
            return 'delegated_gate_fields_invalid'
        code = gate['code']
        expected_fields = field_contracts[code]
        if gate.get('policy_version') != DELEGATED_GATE_POLICY_VERSION or any(
            gate.get(key) != value for key, value in expected_fields.items()
        ):
            return 'delegated_gate_policy_binding_invalid'
        try:
            observed_at = datetime.fromisoformat(str(gate.get('observed_at') or ''))
            expires_at = datetime.fromisoformat(str(gate.get('expires_at') or ''))
        except ValueError:
            return 'delegated_gate_timestamp_invalid'
        if (
            observed_at.tzinfo is None
            or observed_at.utcoffset() is None
            or expires_at.tzinfo is None
            or expires_at.utcoffset() is None
            or observed_at.astimezone(timezone.utc) > evaluated_at
            or evaluated_at >= expires_at.astimezone(timezone.utc)
        ):
            return 'delegated_gate_timestamp_invalid'
        if code == DELEGATED_GATE_LIVE_COST_CODE:
            if gate.get('observed_value') is not None:
                return 'delegated_gate_observed_value_invalid'
        else:
            observed_value = _safe_decimal(gate.get('observed_value'))
            threshold = _safe_decimal(gate.get('threshold'))
            if observed_value is None or threshold is None or observed_value >= threshold:
                return 'delegated_gate_observed_value_invalid'
    return None


def _extract_field(payload: dict[str, Any], *names: str) -> Any:
    current: Any = payload
    for name in names:
        if not isinstance(current, dict):
            return None
        current = current.get(name)
    return current


def _strip_signal_decider_only_labels(payload: dict[str, Any], *, decision_agent_name: str) -> dict[str, Any]:
    if decision_agent_name != SIGNAL_DECIDER_AGENT_NAME:
        return payload
    return {
        key: value
        for key, value in payload.items()
        if key not in {'role', 'agent_name'}
    }


def _decision_input_quality_from_payloads(
    *,
    symbol: str,
    source_market_date: date,
    request_payload: dict[str, Any],
    validation: dict[str, Any],
    traceability: dict[str, Any],
    allocation_budget_krw: Any = None,
    estimated_reference_price: Any = None,
    planned_notional_krw: Any = None,
) -> dict[str, Any]:
    for payload in (validation, traceability, request_payload):
        context = payload.get('decision_input_quality_context') if isinstance(payload, dict) else None
        if isinstance(context, dict) and context:
            return context
    symbol_day_context = request_payload.get('symbol_day_context')
    if isinstance(symbol_day_context, dict) and isinstance(symbol_day_context.get('decision_input_quality_context'), dict):
        return symbol_day_context['decision_input_quality_context']

    decider_quality = request_payload.get('decider_data_quality_context')
    pre_decider = request_payload.get('pre_decider_enrichment_context')
    if isinstance(symbol_day_context, dict):
        if not isinstance(decider_quality, dict):
            decider_quality = symbol_day_context.get('decider_data_quality_context')
        if not isinstance(pre_decider, dict):
            pre_decider = symbol_day_context.get('pre_decider_enrichment_context')
    return build_decision_input_quality_envelope(
        symbol=symbol,
        market_date=source_market_date.isoformat(),
        decider_data_quality_context=decider_quality if isinstance(decider_quality, dict) else {},
        pre_decider_enrichment_context=pre_decider if isinstance(pre_decider, dict) else {},
        allocation_budget_krw=allocation_budget_krw,
        estimated_reference_price=estimated_reference_price,
        planned_notional_krw=planned_notional_krw,
    )


def _market_regime_context_from_payloads(
    *,
    request_payload: dict[str, Any],
    validation: dict[str, Any],
    traceability: dict[str, Any],
) -> dict[str, Any]:
    for payload in (validation, traceability, request_payload):
        context = payload.get('market_regime_context') if isinstance(payload, dict) else None
        if isinstance(context, dict) and context:
            return context
    symbol_day_context = request_payload.get('symbol_day_context')
    if isinstance(symbol_day_context, dict) and isinstance(symbol_day_context.get('market_regime_context'), dict):
        return symbol_day_context['market_regime_context']
    return {}


def _strip_signal_decider_fixed_sizing_preview(payload: dict[str, Any], *, decision_agent_name: str) -> dict[str, Any]:
    if decision_agent_name != SIGNAL_DECIDER_AGENT_NAME:
        return payload
    sanitized = dict(payload)
    pricing = sanitized.get('pricing')
    if isinstance(pricing, dict):
        sanitized['pricing'] = {
            **pricing,
            'planned_quantity': None,
            'estimated_notional_krw': None,
        }
    sanitized['pricing_note'] = 'Sizing for signal-decider dispatch is delegated to exec-agent.'
    return sanitized


def _extract_agent_approved_quantity(payload: dict[str, Any]) -> Decimal | None:
    approval_value = payload.get('approved')
    if approval_value is False:
        return None
    if isinstance(approval_value, str) and approval_value.strip().lower() in {'false', 'no', 'rejected', 'declined'}:
        return None

    for key in ('approval_status', 'decision_status', 'status'):
        status_value = payload.get(key)
        if isinstance(status_value, str) and status_value.strip().lower() in {
            'rejected',
            'declined',
            'denied',
            'not_approved',
            'blocked',
        }:
            return None

    for key in (
        'agent_approved_quantity',
        'approved_quantity',
        'sized_quantity',
        'target_quantity',
        'order_quantity',
        'requested_quantity',
        'quantity',
    ):
        parsed = _positive_decimal(payload.get(key))
        if parsed is not None:
            return parsed
    for key in (
        'agent_sizing',
        'sizing',
        'sizing_decision',
        'quantity_decision',
        'approval',
        'decision',
        'order',
        'materialized_order',
    ):
        nested = payload.get(key)
        if isinstance(nested, dict):
            parsed = _extract_agent_approved_quantity(nested)
            if parsed is not None:
                return parsed
    return None


def _append_limited(items: Sequence[dict[str, Any]], item: dict[str, Any], *, limit: int = 10) -> list[dict[str, Any]]:
    values = list(items)
    values.append(item)
    return values[-limit:]


def _fingerprint_exec_plan(payload: dict[str, Any]) -> str:
    normalized = _json_safe(payload)
    if isinstance(normalized, dict):
        normalized = dict(normalized)
        normalized.pop('requested_at', None)
        metadata = normalized.get('metadata')
        if isinstance(metadata, dict):
            metadata = dict(metadata)
            metadata.pop('trigger', None)
            metadata.pop('requested_at', None)
            normalized['metadata'] = metadata
    encoded = json.dumps(normalized, sort_keys=True, separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def _validated_sha256_hex(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if (
        normalized != value
        or normalized.lower() != normalized
        or len(normalized) != 64
        or any(character not in '0123456789abcdef' for character in normalized)
    ):
        return None
    return normalized


def _bind_core_confirmed_activation(
    payload: dict[str, Any],
    *,
    authority_generation: dict[str, Any],
) -> dict[str, Any]:
    authority_generation_fingerprint = _validated_sha256_hex(
        authority_generation.get('generation_fingerprint')
    )
    if (
        authority_generation.get('status') != 'valid'
        or authority_generation_fingerprint is None
    ):
        raise SignalDeciderExecPayloadContractError(
            'valid dispatch authority generation is required before Exec activation binding'
        )
    candidate = copy.deepcopy(payload)
    items = candidate.get('items')
    if not isinstance(items, list) or not items:
        raise SignalDeciderExecPayloadContractError(
            'core-confirmed activation requires non-empty execution plan items'
        )
    binding = {
        'schema_version': CORE_EXEC_ACTIVATION_BINDING_SCHEMA_VERSION,
        'mode': 'core_confirmed',
        'authority_generation_fingerprint': authority_generation_fingerprint,
    }
    for item in items:
        if not isinstance(item, dict):
            raise SignalDeciderExecPayloadContractError(
                'core-confirmed activation requires object execution plan items'
            )
        risk_context = item.get('risk_context')
        risk_context = risk_context if isinstance(risk_context, dict) else {}
        item_intent = _normalize_optional_text(item.get('intent'), lowercase=True)
        if item_intent in {EXEC_INTENT_TARGET_SELL, EXEC_INTENT_STOP_SELL}:
            trigger_rule = _normalize_optional_text(
                risk_context.get('trigger_rule'),
                lowercase=True,
            )
            if trigger_rule != item_intent:
                raise SignalDeciderExecPayloadContractError(
                    'protective exit risk context must bind its trigger rule'
                )
            item['risk_context'] = _bounded_dispatch_object(
                {
                    '_core_activation': copy.deepcopy(binding),
                    'trigger_rule': trigger_rule,
                    **{
                        key: copy.deepcopy(value)
                        for key, value in risk_context.items()
                        if key not in {'_core_activation', 'trigger_rule'}
                    },
                },
                max_bytes=EXEC_EXIT_RISK_CONTEXT_MAX_BYTES,
                label='exec_protective_exit_risk_context',
                essential_keys=(
                    EXEC_PROTECTIVE_EXIT_RISK_CONTEXT_ESSENTIAL_KEYS
                ),
            )
        else:
            item['risk_context'] = {
                **risk_context,
                '_core_activation': copy.deepcopy(binding),
            }
    try:
        return serialize_execution_plan_dispatch_payload(candidate)
    except ValueError as exc:
        raise SignalDeciderExecPayloadContractError(str(exc)) from exc


def _core_activation_binding_from_plan(
    payload: dict[str, Any],
) -> dict[str, str] | None:
    items = payload.get('items')
    if not isinstance(items, list) or not items:
        return None
    bindings: list[dict[str, Any]] = []
    for item in items:
        risk_context = (
            item.get('risk_context') if isinstance(item, dict) else None
        )
        binding = (
            risk_context.get('_core_activation')
            if isinstance(risk_context, dict)
            else None
        )
        if not isinstance(binding, dict):
            return None
        bindings.append(binding)
    first = bindings[0]
    authority_generation_fingerprint = _validated_sha256_hex(
        first.get('authority_generation_fingerprint')
    )
    expected = {
        'schema_version': CORE_EXEC_ACTIVATION_BINDING_SCHEMA_VERSION,
        'mode': 'core_confirmed',
        'authority_generation_fingerprint': authority_generation_fingerprint,
    }
    if authority_generation_fingerprint is None or any(
        binding != expected for binding in bindings
    ):
        return None
    return expected


def _build_execution_activation_payload(
    *,
    plan_payload: dict[str, Any],
    response_payload: dict[str, Any],
) -> dict[str, Any]:
    plan_id = _normalize_optional_text(plan_payload.get('plan_id'))
    metadata = (
        plan_payload.get('metadata')
        if isinstance(plan_payload.get('metadata'), dict)
        else {}
    )
    dispatch_material_fingerprint = _validated_sha256_hex(
        metadata.get('dispatch_material_fingerprint')
    )
    binding = _core_activation_binding_from_plan(plan_payload)
    plan_items = (
        plan_payload.get('items')
        if isinstance(plan_payload.get('items'), list)
        else []
    )
    if not plan_id or dispatch_material_fingerprint is None or binding is None:
        raise ValueError('execution plan is missing an exact Core activation binding')
    submitted_core_item_keys = [
        str(item.get('core_item_key') or '')
        for item in plan_items
        if isinstance(item, dict)
    ]
    if (
        len(submitted_core_item_keys) != len(plan_items)
        or not submitted_core_item_keys
        or any(not key for key in submitted_core_item_keys)
    ):
        raise ValueError('execution activation items require exact Core item keys')
    response_by_key = validate_execution_plan_dispatch_ack(
        response_payload,
        plan_id=plan_id,
        submitted_core_item_keys=submitted_core_item_keys,
        expected_dispatch_material_fingerprint=dispatch_material_fingerprint,
    )
    execution_plan_id = _normalize_optional_text(
        response_payload.get('execution_plan_id')
    )
    if not execution_plan_id:
        raise ValueError('execution activation requires an accepted Exec plan id')
    return {
        'schema_version': EXECUTION_ACTIVATION_SCHEMA_VERSION,
        'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
        'required_capability': EXECUTION_ACTIVATION_CAPABILITY,
        'plan_id': plan_id,
        'execution_plan_id': execution_plan_id,
        'dispatch_material_fingerprint': dispatch_material_fingerprint,
        'authority_generation_fingerprint': binding[
            'authority_generation_fingerprint'
        ],
        'requested_at': str(plan_payload['requested_at']),
        'items': [
            {
                'core_item_key': str(item['core_item_key']),
                'execution_plan_item_id': str(
                    response_by_key[str(item['core_item_key'])][
                        'execution_plan_item_id'
                    ]
                ),
                'decision_snapshot_id': str(item['decision_snapshot_id']),
            }
            for item in plan_items
        ],
    }


def _validate_execution_activation_ack(
    payload: dict[str, Any],
    *,
    request_payload: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    if not isinstance(payload, dict):
        raise ValueError('execution activation ACK must be an object')
    plan_id = str(request_payload['plan_id'])
    execution_plan_id = str(request_payload['execution_plan_id'])
    contract_ack = (
        payload.get('contract_ack')
        if isinstance(payload.get('contract_ack'), dict)
        else {}
    )
    expected_contract_ack = {
        'schema_version': EXECUTION_ACTIVATION_SCHEMA_VERSION,
        'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
        'capability': EXECUTION_ACTIVATION_CAPABILITY,
        'plan_id': plan_id,
        'status': 'activated',
        'dispatch_material_fingerprint': request_payload[
            'dispatch_material_fingerprint'
        ],
        'authority_generation_fingerprint': request_payload[
            'authority_generation_fingerprint'
        ],
    }
    if (
        payload.get('plan_id') != plan_id
        or payload.get('execution_plan_id') != execution_plan_id
        or contract_ack != expected_contract_ack
    ):
        raise ValueError('execution activation ACK header does not match the request')
    expected_by_key = {
        str(item['core_item_key']): {
            'core_item_key': str(item['core_item_key']),
            'execution_plan_item_id': str(item['execution_plan_item_id']),
            'decision_snapshot_id': str(item['decision_snapshot_id']),
            'execution_plan_id': execution_plan_id,
            'status': 'activated',
        }
        for item in request_payload['items']
    }
    raw_item_acks = payload.get('item_acks')
    if not isinstance(raw_item_acks, list):
        raise ValueError('execution activation ACK items are missing')
    actual_by_key = {
        str(item.get('core_item_key') or ''): item
        for item in raw_item_acks
        if isinstance(item, dict)
    }
    if (
        len(actual_by_key) != len(raw_item_acks)
        or actual_by_key != expected_by_key
        or payload.get('activated_item_count') != len(expected_by_key)
    ):
        raise ValueError('execution activation ACK item identities do not match')
    return actual_by_key


def _build_pending_core_activation_request(
    rows: Sequence[SignalDeciderExecutionPlanItem],
) -> tuple[dict[str, Any], dict[int, str], dict[str, Any]]:
    """Rebuild only the exact activation contract durably attested by Core."""
    ordered_rows = sorted(rows, key=lambda row: int(row.id))
    if not ordered_rows or len({int(row.id) for row in ordered_rows}) != len(
        ordered_rows
    ):
        raise ValueError('pending Core activation rows are missing or duplicated')

    exec_states: list[dict[str, Any]] = []
    contract_proofs: list[dict[str, Any]] = []
    request_summaries: list[dict[str, Any]] = []
    response_summaries: list[dict[str, Any]] = []
    destinations: list[dict[str, Any]] = []
    revision_fingerprints: dict[int, str] = {}
    for row in ordered_rows:
        if (
            str(row.planned_side or '').strip().lower() != 'buy'
            or row.execution_status
            not in {
                EXECUTION_STATUS_SUBMITTED,
                EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING,
            }
            or not _row_has_accepted_exec_dispatch(row)
        ):
            raise ValueError(
                'pending Core activation no longer has exact accepted BUY authority'
            )
        submission = (
            row.submission_payload
            if isinstance(row.submission_payload, dict)
            else {}
        )
        exec_dispatch = (
            submission.get('exec_dispatch')
            if isinstance(submission.get('exec_dispatch'), dict)
            else {}
        )
        proof = (
            exec_dispatch.get('contract_proof')
            if isinstance(exec_dispatch.get('contract_proof'), dict)
            else {}
        )
        request_summary = (
            exec_dispatch.get('last_request_summary')
            if isinstance(exec_dispatch.get('last_request_summary'), dict)
            else {}
        )
        response_summary = (
            exec_dispatch.get('last_response_summary')
            if isinstance(exec_dispatch.get('last_response_summary'), dict)
            else {}
        )
        destination = _exec_dispatch_destination_binding(exec_dispatch)
        revision_fingerprint = _validated_sha256_hex(
            exec_dispatch.get('dispatch_revision_fingerprint')
        )
        if destination is None or revision_fingerprint is None:
            raise ValueError(
                'pending Core activation lost its destination or revision binding'
            )
        exec_states.append(exec_dispatch)
        contract_proofs.append(proof)
        request_summaries.append(request_summary)
        response_summaries.append(response_summary)
        destinations.append(destination)
        revision_fingerprints[int(row.id)] = revision_fingerprint

    account_references = {
        _normalize_optional_text(row.account_reference) for row in ordered_rows
    }
    target_trade_dates = {row.target_trade_date for row in ordered_rows}
    plan_ids = {
        _normalize_optional_text(state.get('plan_id')) for state in exec_states
    }
    execution_plan_ids = {
        _normalize_optional_text(proof.get('execution_plan_id'))
        for proof in contract_proofs
    }
    material_fingerprints = {
        _validated_sha256_hex(proof.get('dispatch_material_fingerprint'))
        for proof in contract_proofs
    }
    plan_fingerprints = {
        _validated_sha256_hex(state.get('plan_fingerprint'))
        for state in exec_states
    }
    activation_proofs = [
        proof.get('core_activation')
        if isinstance(proof.get('core_activation'), dict)
        else {}
        for proof in contract_proofs
    ]
    authority_generation_fingerprints = {
        _validated_sha256_hex(
            activation.get('authority_generation_fingerprint')
        )
        for activation in activation_proofs
    }
    if (
        None in account_references
        or len(account_references) != 1
        or len(target_trade_dates) != 1
        or None in plan_ids
        or len(plan_ids) != 1
        or None in execution_plan_ids
        or len(execution_plan_ids) != 1
        or None in material_fingerprints
        or len(material_fingerprints) != 1
        or None in plan_fingerprints
        or len(plan_fingerprints) != 1
        or None in authority_generation_fingerprints
        or len(authority_generation_fingerprints) != 1
        or len(
            {
                json.dumps(destination, sort_keys=True, separators=(',', ':'))
                for destination in destinations
            }
        )
        != 1
    ):
        raise ValueError('pending Core activation group authority is inconsistent')

    plan_id = str(next(iter(plan_ids)))
    execution_plan_id = str(next(iter(execution_plan_ids)))
    dispatch_material_fingerprint = str(next(iter(material_fingerprints)))
    authority_generation_fingerprint = str(
        next(iter(authority_generation_fingerprints))
    )
    expected_activation_proof = {
        'schema_version': CORE_EXEC_ACTIVATION_BINDING_SCHEMA_VERSION,
        'mode': 'core_confirmed',
        'authority_generation_fingerprint': authority_generation_fingerprint,
        'status': 'pending',
        'dispatch_material_fingerprint': dispatch_material_fingerprint,
    }
    if any(
        activation != expected_activation_proof
        for activation in activation_proofs
    ):
        raise ValueError('pending Core activation proof is not exact')

    request_total_counts = {
        summary.get('total_item_count') for summary in request_summaries
    }
    activation_requested_ats = {
        _normalize_optional_text(summary.get('requested_at'))
        for summary in request_summaries
    }
    response_total_counts = {
        summary.get('total_item_count') for summary in response_summaries
    }
    accepted_item_counts = {
        summary.get('accepted_item_count') for summary in response_summaries
    }
    rejected_item_counts = {
        summary.get('rejected_item_count') for summary in response_summaries
    }
    if (
        len(request_total_counts) != 1
        or None in activation_requested_ats
        or len(activation_requested_ats) != 1
        or len(response_total_counts) != 1
        or len(accepted_item_counts) != 1
        or rejected_item_counts != {0}
    ):
        raise ValueError('pending Core activation item totals are inconsistent')
    total_item_count = next(iter(request_total_counts))
    if (
        not isinstance(total_item_count, int)
        or isinstance(total_item_count, bool)
        or total_item_count <= 0
        or response_total_counts != {total_item_count}
        or accepted_item_counts != {total_item_count}
    ):
        raise ValueError('pending Core activation item totals are invalid')

    activation_items: list[dict[str, str]] = []
    for row, state, request_summary, response_summary in zip(
        ordered_rows,
        exec_states,
        request_summaries,
        response_summaries,
        strict=True,
    ):
        if (
            request_summary.get('plan_id') != plan_id
            or request_summary.get('row_item_count')
            != len(state.get('item_links') or [])
            or response_summary.get('execution_plan_id') != execution_plan_id
            or response_summary.get('row_item_count')
            != len(state.get('item_links') or [])
        ):
            raise ValueError(
                'pending Core activation row summaries are incomplete'
            )
        request_items = request_summary.get('row_items')
        if not isinstance(request_items, list):
            raise ValueError('pending Core activation request items are missing')
        request_by_key = {
            str(item.get('core_item_key') or ''): item
            for item in request_items
            if isinstance(item, dict)
        }
        links = state.get('item_links')
        if not isinstance(links, list) or not links:
            raise ValueError('pending Core activation item links are missing')
        link_keys = {
            str(link.get('core_item_key') or '')
            for link in links
            if isinstance(link, dict)
        }
        if (
            len(request_by_key) != len(request_items)
            or set(request_by_key) != link_keys
            or len(link_keys) != len(links)
            or '' in link_keys
            or any(
                str(request_by_key[key].get('decision_snapshot_id') or '')
                != str(row.decision_snapshot_id)
                for key in link_keys
            )
        ):
            raise ValueError(
                'pending Core activation item-to-snapshot binding changed'
            )
        for link in links:
            core_item_key = str(link['core_item_key'])
            linked_execution_plan_id = _normalize_optional_text(
                link.get('exec_plan_id')
            )
            execution_plan_item_id = _normalize_optional_text(
                link.get('exec_plan_item_id')
            )
            if (
                linked_execution_plan_id != execution_plan_id
                or execution_plan_item_id is None
            ):
                raise ValueError(
                    'pending Core activation remote item binding changed'
                )
            activation_items.append(
                {
                    'core_item_key': core_item_key,
                    'execution_plan_item_id': execution_plan_item_id,
                    'decision_snapshot_id': str(row.decision_snapshot_id),
                }
            )

    if (
        len(activation_items) != total_item_count
        or len({item['core_item_key'] for item in activation_items})
        != total_item_count
        or len(
            {item['execution_plan_item_id'] for item in activation_items}
        )
        != total_item_count
    ):
        raise ValueError('pending Core activation does not cover the whole plan')

    return (
        {
            'schema_version': EXECUTION_ACTIVATION_SCHEMA_VERSION,
            'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
            'required_capability': EXECUTION_ACTIVATION_CAPABILITY,
            'plan_id': plan_id,
            'execution_plan_id': execution_plan_id,
            'dispatch_material_fingerprint': dispatch_material_fingerprint,
            'authority_generation_fingerprint': (
                authority_generation_fingerprint
            ),
            'requested_at': str(next(iter(activation_requested_ats))),
            'items': activation_items,
        },
        revision_fingerprints,
        destinations[0],
    )


def _row_dispatch_revision_fingerprint(
    row: SignalDeciderExecutionPlanItem,
) -> str:
    return _execution_plan_row_authority_fingerprint(row)


def _dispatch_authority_row_bindings(
    rows: Sequence[SignalDeciderExecutionPlanItem],
    *,
    revision_fingerprints: dict[int, str],
) -> dict[int, dict[str, Any]]:
    return {
        int(row.id): {
            'decision_snapshot_id': int(row.decision_snapshot_id),
            'row_revision_fingerprint': revision_fingerprints[int(row.id)],
        }
        for row in rows
    }


def _dispatch_authority_generation_conflict(
    *,
    before: dict[str, Any],
    after: dict[str, Any],
) -> dict[str, Any] | None:
    before_fingerprint = _normalize_optional_text(
        before.get('generation_fingerprint')
    )
    after_fingerprint = _normalize_optional_text(
        after.get('generation_fingerprint')
    )
    if (
        before.get('status') == 'valid'
        and after.get('status') == 'valid'
        and before_fingerprint
        and before_fingerprint == after_fingerprint
    ):
        return None
    reason_code = _normalize_optional_text(after.get('reason_code'))
    if not reason_code:
        reason_code = (
            'current_live_market_evidence_unavailable'
            if after.get('status') != 'valid'
            else 'latest_snapshot_replaced'
        )
    return {
        'reason_code': reason_code,
        'replacement_action': (
            _normalize_optional_text(after.get('replacement_action')) or 'wait'
        ),
        'replacement_actionable': bool(
            after.get('replacement_actionable')
        ),
        'before_generation_fingerprint': before_fingerprint,
        'after_generation_fingerprint': after_fingerprint,
    }


def _is_actionable_generation_replacement(
    conflict: dict[str, Any] | None,
) -> bool:
    return bool(
        conflict is not None
        and conflict.get('reason_code') == 'latest_snapshot_replaced'
        and conflict.get('replacement_actionable') is True
    )


def _is_superseded_activation_error(exc: Exception) -> bool:
    """Return whether activation failed because a newer Core generation won.

    These failures are convergence signals, not evidence that live market data
    is unavailable.  Mapping them to the market-evidence revocation path would
    poison the replacement generation and can permanently remove an otherwise
    valid watch candidate from Exec monitoring.
    """
    if isinstance(exc, _PlanRefreshRevisionConflict):
        return True
    return False


def _row_readback_revision_binding(
    row: SignalDeciderExecutionPlanItem,
) -> dict[str, Any]:
    submission = row.submission_payload if isinstance(row.submission_payload, dict) else {}
    exec_dispatch = (
        submission.get('exec_dispatch')
        if isinstance(submission.get('exec_dispatch'), dict)
        else {}
    )
    contract_proof = (
        exec_dispatch.get('contract_proof')
        if isinstance(exec_dispatch.get('contract_proof'), dict)
        else None
    )
    normalized_proof = _json_safe(contract_proof)
    proof_fingerprint = None
    if normalized_proof is not None:
        encoded_proof = json.dumps(
            normalized_proof,
            ensure_ascii=False,
            sort_keys=True,
            separators=(',', ':'),
        ).encode('utf-8')
        proof_fingerprint = hashlib.sha256(encoded_proof).hexdigest()
    return {
        'plan_id': _normalize_optional_text(exec_dispatch.get('plan_id')),
        'dispatch_revision_fingerprint': _normalize_optional_text(
            exec_dispatch.get('dispatch_revision_fingerprint')
        ),
        'current_row_revision_fingerprint': _row_dispatch_revision_fingerprint(row),
        'accepted_contract_proof': _row_has_accepted_exec_dispatch(row),
        'contract_proof_fingerprint': proof_fingerprint,
    }


def _recovered_dispatch_revision_fingerprints(
    rows: Sequence[SignalDeciderExecutionPlanItem],
    *,
    plan_id: str,
    plan_fingerprint: str,
    current_revision_fingerprints: dict[int, str],
) -> dict[int, str]:
    recovered_revision_fingerprints = dict(current_revision_fingerprints)
    for row in rows:
        row_id = int(row.id)
        submission = row.submission_payload if isinstance(row.submission_payload, dict) else {}
        exec_dispatch = (
            submission.get('exec_dispatch')
            if isinstance(submission.get('exec_dispatch'), dict)
            else {}
        )
        if _normalize_optional_text(exec_dispatch.get('plan_id')) != plan_id:
            continue
        prior_plan_fingerprint = _normalize_optional_text(
            exec_dispatch.get('plan_fingerprint')
        )
        if not prior_plan_fingerprint or prior_plan_fingerprint == plan_fingerprint:
            continue
        prior_revision_fingerprint = _normalize_optional_text(
            exec_dispatch.get('dispatch_revision_fingerprint')
        )
        if prior_revision_fingerprint:
            recovered_revision_fingerprints[row_id] = prior_revision_fingerprint
            continue
        legacy_binding = '|'.join((plan_id, str(row_id), prior_plan_fingerprint))
        recovered_revision_fingerprints[row_id] = (
            'unbound-legacy-dispatch:'
            f'{hashlib.sha256(legacy_binding.encode("utf-8")).hexdigest()}'
        )
    return recovered_revision_fingerprints


def _accepted_item_bindings_from_links(
    item_links: Any,
) -> list[dict[str, str]] | None:
    if not isinstance(item_links, list) or not item_links:
        return None
    bindings: list[dict[str, str]] = []
    for link in item_links:
        if not isinstance(link, dict):
            return None
        core_item_key = _normalize_optional_text(link.get('core_item_key'))
        execution_plan_id = _normalize_optional_text(link.get('exec_plan_id'))
        execution_plan_item_id = _normalize_optional_text(
            link.get('exec_plan_item_id')
        )
        if not core_item_key or not execution_plan_id or not execution_plan_item_id:
            return None
        bindings.append(
            {
                'core_item_key': core_item_key,
                'execution_plan_id': execution_plan_id,
                'execution_plan_item_id': execution_plan_item_id,
            }
        )
    return bindings


def _accepted_item_bindings_from_response_summary(
    response_summary: Any,
) -> list[dict[str, str]] | None:
    if not isinstance(response_summary, dict):
        return None
    row_items = response_summary.get('row_items')
    if not isinstance(row_items, list) or not row_items:
        return None
    bindings: list[dict[str, str]] = []
    for item in row_items:
        if not isinstance(item, dict):
            return None
        core_item_key = _normalize_optional_text(item.get('core_item_key'))
        execution_plan_id = _normalize_optional_text(item.get('execution_plan_id'))
        execution_plan_item_id = _normalize_optional_text(
            item.get('execution_plan_item_id')
        )
        if not core_item_key or not execution_plan_id or not execution_plan_item_id:
            return None
        bindings.append(
            {
                'core_item_key': core_item_key,
                'execution_plan_id': execution_plan_id,
                'execution_plan_item_id': execution_plan_item_id,
            }
        )
    return bindings


def _row_has_accepted_exec_dispatch(row: SignalDeciderExecutionPlanItem) -> bool:
    submission = row.submission_payload if isinstance(row.submission_payload, dict) else {}
    exec_dispatch = submission.get('exec_dispatch') if isinstance(submission.get('exec_dispatch'), dict) else {}
    if _normalize_status(exec_dispatch.get('dispatch_status')) != EXEC_DISPATCH_STATUS_ACCEPTED:
        return False
    plan_id = _normalize_optional_text(exec_dispatch.get('plan_id'))
    proof = exec_dispatch.get('contract_proof') if isinstance(exec_dispatch.get('contract_proof'), dict) else {}
    contract_ack = proof.get('contract_ack') if isinstance(proof.get('contract_ack'), dict) else {}
    submitted_keys = proof.get('submitted_core_item_keys')
    dispatch_material_fingerprint = _normalize_optional_text(
        proof.get('dispatch_material_fingerprint')
    )
    request_summary = (
        exec_dispatch.get('last_request_summary')
        if isinstance(exec_dispatch.get('last_request_summary'), dict)
        else {}
    )
    request_metadata = (
        request_summary.get('metadata')
        if isinstance(request_summary.get('metadata'), dict)
        else {}
    )
    response_summary = (
        exec_dispatch.get('last_response_summary')
        if isinstance(exec_dispatch.get('last_response_summary'), dict)
        else {}
    )
    response_contract_ack = (
        response_summary.get('contract_ack')
        if isinstance(response_summary.get('contract_ack'), dict)
        else {}
    )
    proof_execution_plan_id = _normalize_optional_text(
        proof.get('execution_plan_id')
    )
    response_execution_plan_id = _normalize_optional_text(
        response_summary.get('execution_plan_id')
    )
    destination_binding = _exec_dispatch_destination_binding(exec_dispatch)
    if (
        not plan_id
        or destination_binding is None
        or destination_binding.get('account_reference')
        != _normalize_optional_text(getattr(row, 'account_reference', None))
        or not dispatch_material_fingerprint
        or len(dispatch_material_fingerprint) != 64
        or any(
            character not in '0123456789abcdef'
            for character in dispatch_material_fingerprint
        )
        or proof.get('schema_version') != EXEC_DISPATCH_PROOF_SCHEMA_VERSION
        or not proof_execution_plan_id
        or not response_execution_plan_id
        or proof_execution_plan_id != response_execution_plan_id
        or contract_ack != {
            'schema_version': EXECUTION_HANDOFF_ACK_SCHEMA_VERSION,
            'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
            'capability': EXECUTION_HANDOFF_CAPABILITY,
            'plan_id': plan_id,
            'status': EXEC_DISPATCH_STATUS_ACCEPTED,
            'dispatch_material_fingerprint': dispatch_material_fingerprint,
        }
        or request_metadata.get('dispatch_material_fingerprint')
        != dispatch_material_fingerprint
        or response_contract_ack != contract_ack
        or not isinstance(submitted_keys, list)
        or not submitted_keys
        or len(submitted_keys) != len(set(submitted_keys))
        or proof.get('accepted_item_count') != len(submitted_keys)
        or proof.get('rejected_item_count') != 0
    ):
        return False
    item_links = exec_dispatch.get('item_links') if isinstance(exec_dispatch.get('item_links'), list) else []
    accepted_item_bindings = _accepted_item_bindings_from_links(item_links)
    proof_item_bindings = proof.get('accepted_item_bindings')
    response_item_bindings = _accepted_item_bindings_from_response_summary(
        response_summary
    )
    link_keys = [
        _normalize_optional_text(link.get('core_item_key'))
        for link in item_links
        if isinstance(link, dict)
    ]
    exec_plan_ids = [
        _normalize_optional_text(link.get('exec_plan_id'))
        for link in item_links
        if isinstance(link, dict)
    ]
    exec_plan_item_ids = [
        _normalize_optional_text(link.get('exec_plan_item_id'))
        for link in item_links
        if isinstance(link, dict)
    ]
    links_by_key = {
        _normalize_optional_text(link.get('core_item_key')): link
        for link in item_links
        if isinstance(link, dict) and _normalize_optional_text(link.get('core_item_key'))
    }
    return bool(
        len(item_links) == len(submitted_keys)
        and len(link_keys) == len(submitted_keys)
        and len(set(link_keys)) == len(submitted_keys)
        and len(exec_plan_ids) == len(submitted_keys)
        and None not in exec_plan_ids
        and len(set(exec_plan_ids)) == 1
        and exec_plan_ids[0] == proof_execution_plan_id
        and len(exec_plan_item_ids) == len(submitted_keys)
        and None not in exec_plan_item_ids
        and len(set(exec_plan_item_ids)) == len(submitted_keys)
        and set(links_by_key) == set(submitted_keys)
        and accepted_item_bindings is not None
        and proof_item_bindings == accepted_item_bindings
        and response_item_bindings == accepted_item_bindings
        and [binding['core_item_key'] for binding in accepted_item_bindings]
        == submitted_keys
        and all(
            _normalize_status(link.get('remote_status')) == EXEC_DISPATCH_STATUS_ACCEPTED
            and _normalize_optional_text(link.get('exec_plan_id'))
            and _normalize_optional_text(link.get('exec_plan_item_id'))
            for link in links_by_key.values()
        )
    )


def _delta_dispatch_batch_key(rows: Sequence[SignalDeciderExecutionPlanItem]) -> str:
    identities: list[str] = []
    for row in rows:
        validation = row.__dict__.get('validation_payload')
        validation = validation if isinstance(validation, dict) else {}
        submission = row.submission_payload if isinstance(row.submission_payload, dict) else {}
        revocation = submission.get('revocation') if isinstance(submission.get('revocation'), dict) else {}
        identities.append(
            ':'.join(
                (
                    str(row.id),
                    str(row.decision_snapshot_id),
                    str(row.symbol),
                    str(validation.get('preopen_policy_fingerprint') or ''),
                    str(validation.get('dispatch_generation_nonce') or ''),
                    str(revocation.get('replacement_decision_snapshot_id') or ''),
                )
            )
        )
    identities.sort()
    return hashlib.sha256('|'.join(identities).encode('utf-8')).hexdigest()[:12]


def _delta_dispatch_plan_id(base_plan_id: str, batch_key: str) -> str:
    suffix = f'-delta-{batch_key}'
    return f'{base_plan_id[:128 - len(suffix)]}{suffix}'


def _partition_exec_dispatch_rows(
    rows: Sequence[SignalDeciderExecutionPlanItem],
    *,
    force: bool,
) -> tuple[list[SignalDeciderExecutionPlanItem], list[SignalDeciderExecutionPlanItem]]:
    del force  # Explicit retries never override a remote authority claim.

    def has_remote_authority_claim(row: SignalDeciderExecutionPlanItem) -> bool:
        if _row_has_accepted_exec_dispatch(row):
            return True
        submission = (
            row.submission_payload
            if isinstance(row.submission_payload, dict)
            else {}
        )
        exec_dispatch = (
            submission.get('exec_dispatch')
            if isinstance(submission.get('exec_dispatch'), dict)
            else {}
        )
        return _normalize_status(exec_dispatch.get('dispatch_status')) in {
            EXEC_DISPATCH_STATUS_ACCEPTED,
            'pending_remote_ack',
        }

    guarded = [row for row in rows if has_remote_authority_claim(row)]
    pending = [row for row in rows if not has_remote_authority_claim(row)]
    return guarded, pending


def _normalize_status(value: Any) -> str | None:
    normalized = _normalize_optional_text(value, lowercase=True)
    return normalized.replace(' ', '_') if normalized else None


def _coalesce_text(*values: Any, lowercase: bool = False) -> str | None:
    for value in values:
        normalized = _normalize_optional_text(value, lowercase=lowercase)
        if normalized:
            return normalized
    return None


def _normalize_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {'1', 'true', 'yes', 'y', 'on'}:
        return True
    if normalized in {'0', 'false', 'no', 'n', 'off'}:
        return False
    return None


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = _normalize_optional_text(value)
    if not text:
        return None
    normalized = text[:-1] + '+00:00' if text.endswith('Z') else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _latest_iso_timestamp(values: Iterable[Any]) -> str | None:
    latest: datetime | None = None
    for value in values:
        parsed = _parse_iso_datetime(value)
        if parsed is None:
            continue
        if latest is None or parsed > latest:
            latest = parsed
    return latest.isoformat() if latest is not None else None


def _serialize_issue(code: str, message: str, *, retryable: bool = False, status_code: int | None = None) -> dict[str, Any]:
    return _serialize_error(code=code, message=message, retryable=retryable, status_code=status_code)


def _normalize_exec_destination_base_url(value: Any) -> str | None:
    raw = _normalize_optional_text(value)
    if raw is None:
        return None
    try:
        parsed = urlsplit(raw)
        port = parsed.port
    except ValueError:
        return None
    scheme = parsed.scheme.lower()
    hostname = parsed.hostname.lower() if parsed.hostname else None
    if (
        scheme not in {'http', 'https'}
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        return None
    effective_port = port or (443 if scheme == 'https' else 80)
    rendered_host = f'[{hostname}]' if ':' in hostname else hostname
    path = parsed.path.rstrip('/')
    return f'{scheme}://{rendered_host}:{effective_port}{path}'


def _exec_destination_binding_fingerprint(material: dict[str, Any]) -> str:
    encoded = json.dumps(
        material,
        ensure_ascii=True,
        sort_keys=True,
        separators=(',', ':'),
    ).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def _build_exec_destination_binding(
    *,
    base_url: Any,
    runtime_mode: Any,
    dispatch_source: Any,
    account_reference: Any,
) -> dict[str, Any]:
    normalized_base_url = _normalize_exec_destination_base_url(base_url)
    normalized_runtime_mode = _normalize_optional_text(runtime_mode, lowercase=True)
    normalized_dispatch_source = _normalize_optional_text(dispatch_source)
    normalized_account_reference = _normalize_optional_text(account_reference)
    if normalized_base_url is None:
        raise ValueError('Exec destination base URL is missing or unsafe')
    if normalized_runtime_mode not in {'mock', 'paper', 'live'}:
        raise ValueError('Exec destination runtime mode is invalid')
    if normalized_dispatch_source is None:
        raise ValueError('Exec destination dispatch source is missing')
    if normalized_account_reference is None:
        raise ValueError('Exec destination account reference is missing')
    material = {
        'schema_version': EXEC_DESTINATION_BINDING_SCHEMA_VERSION,
        'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
        'base_url': normalized_base_url,
        'runtime_mode': normalized_runtime_mode,
        'dispatch_source': normalized_dispatch_source,
        'account_reference': normalized_account_reference,
    }
    return {
        **material,
        'destination_fingerprint': _exec_destination_binding_fingerprint(material),
    }


def _validated_exec_destination_binding(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    try:
        canonical = _build_exec_destination_binding(
            base_url=value.get('base_url'),
            runtime_mode=value.get('runtime_mode'),
            dispatch_source=value.get('dispatch_source'),
            account_reference=value.get('account_reference'),
        )
    except ValueError:
        return None
    return canonical if value == canonical else None


def _exec_dispatch_destination_binding(
    exec_dispatch: dict[str, Any],
) -> dict[str, Any] | None:
    direct = _validated_exec_destination_binding(
        exec_dispatch.get('destination_binding')
    )
    proof = (
        exec_dispatch.get('contract_proof')
        if isinstance(exec_dispatch.get('contract_proof'), dict)
        else {}
    )
    proof_binding = _validated_exec_destination_binding(
        proof.get('destination_binding')
    )
    if direct is None or proof_binding is None or direct != proof_binding:
        return None
    return direct


class SignalDeciderExecClientError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        code: str,
        retryable: bool = False,
        status_code: int | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.status_code = status_code
        self.payload = payload or {}


class SignalDeciderExecPayloadContractError(RuntimeError):
    """Raised before dispatch when a Core-owned Exec payload cannot be proven safe."""


def _dispatch_group_requires_core_krx_buy_authority(
    *,
    route_family: str,
    rows: Sequence[SignalDeciderExecutionPlanItem],
) -> bool:
    return bool(
        route_family == 'kis_krx_krw'
        and any(
            str(row.planned_side or '').strip().lower() == 'buy'
            for row in rows
        )
    )


class SignalDeciderExecClient:
    def __init__(
        self,
        *,
        base_url: str | None = None,
        bearer_token: str | None = None,
        timeout_seconds: float | None = None,
    ) -> None:
        self.base_url = _normalize_optional_text(base_url if base_url is not None else settings.signal_decider_exec_base_url)
        self.bearer_token = _normalize_optional_text(
            bearer_token if bearer_token is not None else settings.signal_decider_exec_bearer_token
        )
        self.timeout_seconds = float(
            timeout_seconds if timeout_seconds is not None else settings.signal_decider_exec_request_timeout_seconds
        )

    async def get_runtime_status(self) -> dict[str, Any]:
        return await self._request_json('GET', '/runtime/status')

    async def get_runtime_monitored_symbol_membership(self) -> dict[str, Any]:
        return await self._request_json(
            'GET',
            '/runtime/monitored-symbol-membership',
        )

    async def post_execution_plan(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._request_json('POST', '/v4/execution-plans', json_body=payload)

    async def post_execution_plan_activation(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return await self._request_json(
            'POST',
            '/v4/execution-plans/activations',
            json_body=payload,
        )

    async def post_buy_authority_observation(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return await self._request_json(
            'POST',
            '/runtime/core-buy-authority/observations',
            json_body=payload,
        )

    async def list_execution_plans(self, *, external_plan_id: str | None = None) -> dict[str, Any]:
        params = {'external_plan_id': external_plan_id} if external_plan_id else None
        return await self._request_json('GET', '/v4/execution-plans', params=params)

    async def post_execution_plan_revocation(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._request_json(
            'POST',
            '/v4/execution-plans/revocations',
            json_body=payload,
        )

    async def list_orders(self, *, external_plan_id: str | None = None, symbols: Sequence[str] | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if external_plan_id:
            params['external_plan_id'] = external_plan_id
        if symbols:
            params['symbols'] = ','.join(sorted({_normalize_optional_text(item) for item in symbols if item}))
        return await self._request_json('GET', '/orders', params=params or None)

    async def list_executions(
        self,
        *,
        external_plan_id: str | None = None,
        symbols: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if external_plan_id:
            params['external_plan_id'] = external_plan_id
        if symbols:
            params['symbols'] = ','.join(sorted({_normalize_optional_text(item) for item in symbols if item}))
        return await self._request_json('GET', '/executions', params=params or None)

    async def list_positions(
        self,
        *,
        external_plan_id: str,
        execution_plan_id: str,
        symbols: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        params['external_plan_id'] = external_plan_id
        params['execution_plan_id'] = execution_plan_id
        if symbols:
            params['symbols'] = ','.join(sorted({_normalize_optional_text(item) for item in symbols if item}))
        return await self._request_json('GET', '/positions', params=params)

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: Any = None,
    ) -> dict[str, Any]:
        if not self.base_url:
            raise SignalDeciderExecClientError(
                'signal_decider_exec_base_url is not configured',
                code=EXEC_DISPATCH_STATUS_NOT_CONFIGURED,
            )
        headers = {'Accept': 'application/json'}
        if self.bearer_token:
            headers['Authorization'] = f'Bearer {self.bearer_token}'
        async with httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout_seconds) as client:
            try:
                response = await request_with_audit(
                    client,
                    EXEC_SERVICE_NAME,
                    method,
                    path,
                    headers=headers,
                    params=params,
                    json_body=json_body,
                    secret_values=(self.bearer_token or '',),
                )
            except httpx.TimeoutException as exc:
                raise SignalDeciderExecClientError(
                    'exec request timed out',
                    code=EXEC_DISPATCH_STATUS_UNAVAILABLE,
                    retryable=True,
                ) from exc
            except httpx.HTTPError as exc:
                raise SignalDeciderExecClientError(
                    'exec transport failure',
                    code=EXEC_DISPATCH_STATUS_UNAVAILABLE,
                    retryable=True,
                ) from exc

        try:
            payload = response.json() if response.content else {}
        except ValueError:
            payload = {'text': response.text[:500]}

        if response.status_code >= 400:
            code = EXEC_DISPATCH_STATUS_FAILED
            if response.status_code in {401, 403}:
                code = EXEC_DISPATCH_STATUS_NOT_CONFIGURED
            elif response.status_code in {404, 405, 422}:
                code = EXEC_DISPATCH_STATUS_CONTRACT_ERROR
            elif response.status_code >= 500:
                code = EXEC_DISPATCH_STATUS_UNAVAILABLE
            raise SignalDeciderExecClientError(
                f'exec request failed with status {response.status_code}',
                code=code,
                retryable=response.status_code >= 500,
                status_code=response.status_code,
                payload=payload if isinstance(payload, dict) else {'payload': payload},
            )
        if isinstance(payload, dict):
            return payload
        return {'items': payload} if isinstance(payload, list) else {'payload': payload}


class SignalDeciderExecIntegrationService:
    def __init__(
        self,
        *,
        client: SignalDeciderExecClient | None = None,
    ) -> None:
        self.client = client or SignalDeciderExecClient()
        self.dashboard_cache_ttl_seconds = max(int(settings.signal_decider_exec_dashboard_cache_ttl_seconds), 0)
        self.dashboard_refresh_debounce_seconds = max(
            float(settings.signal_decider_exec_dashboard_refresh_debounce_seconds),
            0.0,
        )
        self.dashboard_refresh_timeout_seconds = max(
            float(settings.signal_decider_exec_dashboard_refresh_timeout_seconds),
            0.0,
        )
        self._dashboard_cache: dict[tuple[str, str, str | None], DashboardReadModelCacheRecord] = {}
        self._dashboard_build_locks: dict[tuple[str, str, str | None], asyncio.Lock] = {}
        self._dashboard_refresh_tasks: dict[tuple[str, str | None, str | None], asyncio.Task] = {}
        self._dashboard_refresh_state: dict[tuple[str, str | None, str | None], dict[str, Any]] = {}
        self._latest_exec_monitoring_membership: tuple[
            date,
            ExecRuntimeMonitoredSymbolMembership,
        ] | None = None
        self._realtime_exec_membership_tasks: dict[
            date,
            asyncio.Task[ExecRuntimeMonitoredSymbolMembership | None],
        ] = {}
        self._latest_portfolio_holding_membership: tuple[
            date,
            PortfolioHoldingSymbolMembership,
        ] | None = None
        self._realtime_portfolio_membership_tasks: dict[
            date,
            asyncio.Task[PortfolioHoldingSymbolMembership | None],
        ] = {}

    async def _synchronize_open_buy_authority(
        self,
        *,
        target_trade_date: date,
        account_reference: str | None,
        integration: dict[str, Any],
    ) -> dict[str, Any]:
        """Deliver deterministic KRX authority before any OPEN buy dispatch."""

        market_state = await async_get_market_state()
        snapshot_context, snapshot_load_error = await asyncio.to_thread(
            _load_live_market_snapshot_context
        )
        route_authority = _current_route_live_market_buy_authority(
            target_trade_date=target_trade_date,
            market_state=market_state,
            snapshot_context=snapshot_context,
            snapshot_load_error=snapshot_load_error,
        )
        sync: dict[str, Any] = {
            'applies': route_authority.get('applies') is True,
            'route_action': route_authority.get('action'),
            'route_status': route_authority.get('status'),
            'source_observed_at': route_authority.get('observed_at'),
            'accounts': {},
            'errors': [],
        }
        if not sync['applies']:
            return sync

        configured_targets, target_errors = (
            signal_decider_next_open_execution_service._parse_account_targets()
        )
        if target_errors:
            sync['errors'].extend(target_errors)
        targets = [
            target
            for target in configured_targets
            if account_reference is None
            or target.account_reference == account_reference
        ]
        if not targets:
            sync['errors'].append(
                'No explicit configured KIS account target matched the OPEN buy-authority scope.'
            )
            return sync

        runtime_mode = str(integration.get('runtime_mode') or '').strip().lower()
        for target in targets:
            try:
                observation = build_core_buy_authority_observation(
                    route_authority=route_authority,
                    runtime_mode=runtime_mode,
                    account_reference=target.account_reference,
                )
                staged = await asyncio.to_thread(
                    core_exec_buy_authority_bridge.stage,
                    observation,
                )
                flushed = await core_exec_buy_authority_bridge.flush_scope(
                    scope_key=staged.scope_key,
                    client=self.client,
                )
                sync['accounts'][target.account_reference] = {
                    'scope_key': staged.scope_key,
                    'observation_id': staged.observation_id,
                    'dispatch_allowed': flushed.dispatch_allowed,
                    'errors': list(flushed.errors),
                    'ack': (
                        flushed.ack.model_dump(mode='json')
                        if flushed.ack is not None
                        else None
                    ),
                }
            except Exception as exc:  # noqa: BLE001 - uncertainty is a buy fence
                sync['accounts'][target.account_reference] = {
                    'scope_key': None,
                    'observation_id': None,
                    'dispatch_allowed': False,
                    'errors': [f'{type(exc).__name__}: {exc}'],
                    'ack': None,
                }
        return sync

    def _destination_binding_for_payload(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        metadata = payload.get('metadata') if isinstance(payload.get('metadata'), dict) else {}
        raw_client_base_url = getattr(self.client, 'base_url', None)
        client_base_url = (
            raw_client_base_url
            if isinstance(raw_client_base_url, str)
            and raw_client_base_url.strip()
            else settings.signal_decider_exec_base_url
        )
        return _build_exec_destination_binding(
            base_url=client_base_url,
            runtime_mode=payload.get('mode'),
            dispatch_source=payload.get('source'),
            account_reference=metadata.get('account_reference'),
        )

    def _active_destination_binding(
        self,
        *,
        account_reference: str,
        integration: dict[str, Any],
    ) -> dict[str, Any]:
        settings_base_url = _normalize_exec_destination_base_url(
            integration.get('base_url')
        )
        raw_client_base_url = getattr(self.client, 'base_url', None)
        client_base_url = (
            _normalize_exec_destination_base_url(raw_client_base_url)
            if isinstance(raw_client_base_url, str)
            and raw_client_base_url.strip()
            else settings_base_url
        )
        if settings_base_url is None or client_base_url is None:
            raise ValueError('Exec destination is not configured safely')
        if settings_base_url != client_base_url:
            raise ValueError('Exec client destination does not match current settings')
        return _build_exec_destination_binding(
            base_url=client_base_url,
            runtime_mode=integration.get('runtime_mode'),
            dispatch_source=integration.get('dispatch_source'),
            account_reference=account_reference,
        )

    @staticmethod
    def _mark_exec_destination_manual_review(
        *,
        row_id: int,
        trigger: str,
        reason_code: str,
        expected_destination: dict[str, Any] | None,
        active_destination: dict[str, Any] | None,
    ) -> None:
        now = _utcnow()
        with session_scope() as session:
            row = session.scalar(
                select(SignalDeciderExecutionPlanItem)
                .where(SignalDeciderExecutionPlanItem.id == row_id)
                .with_for_update()
            )
            if row is None or row.execution_status not in {
                'revocation_pending',
                'revocation_manual_review',
                EXECUTION_STATUS_SUBMITTED,
                EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING,
                EXECUTION_STATUS_DISPATCH_PENDING_REMOTE_ACK,
            }:
                return
            submission = (
                copy.deepcopy(row.submission_payload)
                if isinstance(row.submission_payload, dict)
                else {}
            )
            revocation = (
                copy.deepcopy(submission.get('revocation'))
                if isinstance(submission.get('revocation'), dict)
                else {}
            )
            exec_dispatch = (
                submission.get('exec_dispatch')
                if isinstance(submission.get('exec_dispatch'), dict)
                else {}
            )
            reconciliation = {
                'status': 'manual_review_required',
                'reason_code': reason_code,
                'observed_at': now.isoformat(),
                'trigger': trigger,
                'plan_id': revocation.get('plan_id')
                or exec_dispatch.get('plan_id'),
                'expected_destination': copy.deepcopy(expected_destination),
                'active_destination': copy.deepcopy(active_destination),
            }
            submission['dispatch_reconciliation'] = reconciliation
            if row.execution_status == 'revocation_pending':
                submission['revocation'] = {
                    **revocation,
                    'status': 'manual_review_required',
                    'manual_review_reason_code': reason_code,
                    'manual_review_observed_at': now.isoformat(),
                }
            row.eligibility_status = 'blocked'
            row.execution_status = 'revocation_manual_review'
            row.submission_payload = slim_signal_decider_exec_submission_payload(
                submission
            ).payload
            session.flush()

    def integration_status(self) -> dict[str, Any]:
        mode = _normalize_optional_text(settings.signal_decider_exec_integration_mode, lowercase=True) or INTEGRATION_MODE_DISABLED
        issues: list[dict[str, Any]] = []
        if mode not in VALID_INTEGRATION_MODES:
            issues.append(_serialize_error(code='invalid_mode', message=f'Unsupported integration mode "{mode}".'))
        enabled = mode in {INTEGRATION_MODE_OBSERVE, INTEGRATION_MODE_DISPATCH}
        base_url = _normalize_optional_text(settings.signal_decider_exec_base_url)
        if enabled and not base_url:
            issues.append(
                _serialize_error(
                    code='missing_base_url',
                    message='SIGNAL_DECIDER_EXEC_BASE_URL must be configured when exec integration is enabled.',
                )
            )
        normalized_base_url = _normalize_exec_destination_base_url(base_url)
        if enabled and base_url and normalized_base_url is None:
            issues.append(
                _serialize_error(
                    code='unsafe_base_url',
                    message=(
                        'SIGNAL_DECIDER_EXEC_BASE_URL must be an HTTP(S) origin/path '
                        'without credentials, query parameters, or fragments.'
                    ),
                )
            )
        raw_client_base_url = getattr(self.client, 'base_url', None)
        if (
            enabled
            and isinstance(raw_client_base_url, str)
            and raw_client_base_url.strip()
        ):
            client_base_url = _normalize_exec_destination_base_url(
                raw_client_base_url
            )
            if normalized_base_url != client_base_url:
                issues.append(
                    _serialize_error(
                        code='exec_client_destination_mismatch',
                        message=(
                            'The active Exec client destination does not match current '
                            'settings; restart or rebuild the client before dispatch.'
                        ),
                    )
                )
        runtime_mode = _normalize_optional_text(
            settings.signal_decider_exec_runtime_mode,
            lowercase=True,
        ) or 'mock'
        configured_dispatch_source = _normalize_optional_text(
            settings.signal_decider_exec_dispatch_source
        )
        if configured_dispatch_source != EXECUTION_HANDOFF_SOURCE:
            issues.append(
                _serialize_error(
                    code='invalid_dispatch_source',
                    message=(
                        'SIGNAL_DECIDER_EXEC_DISPATCH_SOURCE must be the canonical '
                        f'v4 source "{EXECUTION_HANDOFF_SOURCE}".'
                    ),
                )
            )
        (
            configured_account_targets,
            account_target_errors,
        ) = signal_decider_next_open_execution_service._parse_account_targets()
        if (
            mode == INTEGRATION_MODE_DISPATCH
            and runtime_mode == 'live'
            and (
                account_target_errors
                or len(configured_account_targets) != 1
                or configured_account_targets[0].account_reference
                == UNSPECIFIED_ROUTE_ACCOUNT_REFERENCE
            )
        ):
            issues.append(
                {
                    **_serialize_error(
                        code=LIVE_EXEC_ACCOUNT_TARGET_REQUIRED,
                        message=(
                            'Live Exec dispatch requires exactly one explicit valid KIS '
                            'account target; the synthetic unspecified route is monitor-only.'
                        ),
                    ),
                    'configured_account_target_count': len(
                        configured_account_targets
                    ),
                    'account_target_error_count': len(account_target_errors),
                    'required_contract': (
                        'one_explicit_valid_kis_account_target_for_live_exec'
                    ),
                }
            )
        if mode == INTEGRATION_MODE_DISPATCH and len(configured_account_targets) > 1:
            issues.append(
                {
                    **_serialize_error(
                        code=MULTI_ACCOUNT_EXEC_ENDPOINT_UNSUPPORTED,
                        message=(
                            'One Exec base URL cannot safely prove routing for multiple explicit account targets; '
                            'configure one account target per Exec endpoint before dispatch.'
                        ),
                    ),
                    'configured_account_target_count': len(configured_account_targets),
                    'required_contract': 'one_explicit_account_target_per_exec_endpoint',
                }
            )
        config_status = 'ready'
        if not enabled:
            config_status = REMOTE_FETCH_STATUS_DISABLED
        elif issues:
            config_status = REMOTE_FETCH_STATUS_NOT_CONFIGURED
        return {
            'mode': mode,
            'enabled': enabled,
            'delegate_execution': mode == INTEGRATION_MODE_DISPATCH,
            'config_status': config_status,
            'base_url': base_url if normalized_base_url is not None else None,
            'bearer_token_configured': bool(_normalize_optional_text(settings.signal_decider_exec_bearer_token)),
            'dispatch_source': EXECUTION_HANDOFF_SOURCE,
            'runtime_mode': runtime_mode,
            'request_timeout_seconds': float(settings.signal_decider_exec_request_timeout_seconds),
            'issues': issues,
        }

    def should_delegate_execution(self) -> bool:
        return bool(
            self.integration_status()['delegate_execution']
            and settings.signal_decider_next_open_enabled
        )

    @staticmethod
    def _row_requires_ambiguous_acceptance_reconciliation(
        row: SignalDeciderExecutionPlanItem,
    ) -> bool:
        submission = row.submission_payload if isinstance(row.submission_payload, dict) else {}
        reconciliation = (
            submission.get('dispatch_reconciliation')
            if isinstance(submission.get('dispatch_reconciliation'), dict)
            else {}
        )
        return bool(
            row.execution_status == 'revocation_manual_review'
            and _normalize_optional_text(
                reconciliation.get('reason_code'),
                lowercase=True,
            )
            in {
                'ambiguous_remote_acceptance',
                'ambiguous_remote_acceptance_after_core_revision',
            }
        )

    @staticmethod
    def _legacy_retryable_ambiguous_write_ahead_state(
        row: SignalDeciderExecutionPlanItem,
    ) -> dict[str, Any] | None:
        submission = row.submission_payload if isinstance(row.submission_payload, dict) else {}
        reconciliation = (
            submission.get('dispatch_reconciliation')
            if isinstance(submission.get('dispatch_reconciliation'), dict)
            else {}
        )
        exec_dispatch = (
            submission.get('exec_dispatch')
            if isinstance(submission.get('exec_dispatch'), dict)
            else {}
        )
        reconciliation_error = (
            reconciliation.get('error')
            if isinstance(reconciliation.get('error'), dict)
            else {}
        )
        dispatch_error = (
            exec_dispatch.get('last_error')
            if isinstance(exec_dispatch.get('last_error'), dict)
            else {}
        )
        current_revision_fingerprint = _row_dispatch_revision_fingerprint(row)
        revision_fingerprints = {
            _validated_sha256_hex(value)
            for value in (
                reconciliation.get('submitted_dispatch_revision_fingerprint'),
                reconciliation.get('current_dispatch_revision_fingerprint'),
                exec_dispatch.get('dispatch_revision_fingerprint'),
                current_revision_fingerprint,
            )
        }
        plan_id = _normalize_optional_text(exec_dispatch.get('plan_id'))
        prior_eligibility_status = _normalize_optional_text(
            exec_dispatch.get('prior_eligibility_status'),
            lowercase=True,
        )
        if not (
            row.execution_status == 'revocation_manual_review'
            and row.eligibility_status == 'blocked'
            and _normalize_optional_text(
                reconciliation.get('reason_code'),
                lowercase=True,
            )
            == 'ambiguous_remote_acceptance'
            and reconciliation_error.get('retryable') is True
            and dispatch_error.get('retryable') is True
            and plan_id
            and reconciliation.get('plan_id') == plan_id
            and _validated_sha256_hex(exec_dispatch.get('plan_fingerprint'))
            and reconciliation.get('plan_fingerprint')
            == exec_dispatch.get('plan_fingerprint')
            and _validated_sha256_hex(
                exec_dispatch.get('dispatch_material_fingerprint')
            )
            and _validated_exec_destination_binding(
                exec_dispatch.get('destination_binding')
            )
            is not None
            and exec_dispatch.get('item_links')
            and isinstance(exec_dispatch.get('authority_generation_before'), dict)
            and exec_dispatch['authority_generation_before'].get('status') == 'valid'
            and _normalize_optional_text(
                exec_dispatch.get('prior_execution_status'),
                lowercase=True,
            )
            and prior_eligibility_status
            and revision_fingerprints == {current_revision_fingerprint}
        ):
            return None
        return {
            'plan_id': plan_id,
            'prior_eligibility_status': prior_eligibility_status,
        }

    def _requeue_legacy_retryable_ambiguous_write_aheads(
        self,
        *,
        rows: Sequence[SignalDeciderExecutionPlanItem],
        trigger: str,
    ) -> None:
        grouped_rows = self._group_rows_by_plan_id(rows)
        for plan_id, group_rows in grouped_rows.items():
            if not plan_id or not group_rows or any(
                self._legacy_retryable_ambiguous_write_ahead_state(row) is None
                for row in group_rows
            ):
                continue
            row_ids = [int(row.id) for row in group_rows]
            now = _utcnow()
            with session_scope(
                statement_timeout_ms=(
                    settings.signal_decider_db_statement_timeout_ms
                ),
                lock_timeout_ms=settings.signal_decider_db_lock_timeout_ms,
            ) as session:
                persisted_rows = session.execute(
                    select(SignalDeciderExecutionPlanItem)
                    .where(SignalDeciderExecutionPlanItem.id.in_(row_ids))
                    .order_by(SignalDeciderExecutionPlanItem.id.asc())
                    .with_for_update()
                ).scalars().all()
                persisted_states = [
                    self._legacy_retryable_ambiguous_write_ahead_state(row)
                    for row in persisted_rows
                ]
                if (
                    len(persisted_rows) != len(row_ids)
                    or any(state is None for state in persisted_states)
                ):
                    continue
                for row, state in zip(persisted_rows, persisted_states):
                    assert state is not None
                    submission = copy.deepcopy(row.submission_payload)
                    exec_dispatch = copy.deepcopy(submission['exec_dispatch'])
                    exec_dispatch['dispatch_status'] = (
                        EXEC_DISPATCH_STATUS_PENDING_REMOTE_ACK
                    )
                    submission['exec_dispatch'] = exec_dispatch
                    submission['dispatch_reconciliation'] = {
                        **copy.deepcopy(submission['dispatch_reconciliation']),
                        'status': EXEC_DISPATCH_STATUS_PENDING_REMOTE_ACK,
                        'requeued_at': now.isoformat(),
                        'requeue_trigger': trigger,
                    }
                    row.execution_status = (
                        EXECUTION_STATUS_DISPATCH_PENDING_REMOTE_ACK
                    )
                    row.eligibility_status = state[
                        'prior_eligibility_status'
                    ]
                    row.submission_payload = (
                        slim_signal_decider_exec_submission_payload(
                            submission
                        ).payload
                    )
                session.flush()

    @staticmethod
    def _row_requires_destination_manual_review(
        row: SignalDeciderExecutionPlanItem,
    ) -> bool:
        submission = row.submission_payload if isinstance(row.submission_payload, dict) else {}
        reconciliation = (
            submission.get('dispatch_reconciliation')
            if isinstance(submission.get('dispatch_reconciliation'), dict)
            else {}
        )
        return bool(
            row.execution_status == 'revocation_manual_review'
            and _normalize_optional_text(
                reconciliation.get('reason_code'),
                lowercase=True,
            )
            in {
                'exec_destination_binding_missing',
                'exec_destination_binding_mismatch',
                'exec_destination_mismatch',
                'unproven_remote_acceptance',
            }
        )

    def _has_ambiguous_remote_acceptance(
        self,
        *,
        target_trade_date: date,
        account_reference: str | None,
    ) -> bool:
        rows = self._load_next_open_rows(
            target_trade_date=target_trade_date,
            account_reference=account_reference,
            include_payloads=True,
        )
        return any(
            self._row_requires_ambiguous_acceptance_reconciliation(row)
            for row in rows
        )

    def should_manage_delegated_authority(
        self,
        *,
        target_trade_date: str | date | None = None,
    ) -> bool:
        integration = self.integration_status()
        if (
            integration['delegate_execution']
            and settings.signal_decider_next_open_enabled
        ):
            return True
        with session_scope() as session:
            stmt = select(SignalDeciderExecutionPlanItem).where(
                SignalDeciderExecutionPlanItem.execution_window == EXECUTION_WINDOW_NEXT_OPEN,
            )
            if target_trade_date is not None:
                stmt = stmt.where(
                    SignalDeciderExecutionPlanItem.target_trade_date
                    == _normalize_date(target_trade_date)
                )
            rows = session.execute(stmt).scalars().all()
            for row in rows:
                session.expunge(row)
        for row in rows:
            if row.execution_status in {
                'revocation_pending',
                EXECUTION_STATUS_DISPATCH_PENDING_REMOTE_ACK,
            }:
                return True
            submission = row.submission_payload if isinstance(row.submission_payload, dict) else {}
            dispatch = (
                submission.get('exec_dispatch')
                if isinstance(submission.get('exec_dispatch'), dict)
                else {}
            )
            if (
                _normalize_status(dispatch.get('dispatch_status'))
                == EXEC_DISPATCH_STATUS_PENDING_REMOTE_ACK
            ):
                return True
            if self._row_requires_ambiguous_acceptance_reconciliation(row):
                return True
            if self._row_requires_destination_manual_review(row):
                return True
            if row.execution_status not in {
                EXECUTION_STATUS_SUBMITTED,
                EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING,
            }:
                continue
            if (
                _normalize_status(dispatch.get('dispatch_status')) == EXEC_DISPATCH_STATUS_ACCEPTED
                and isinstance(dispatch.get('item_links'), list)
                and bool(dispatch['item_links'])
            ):
                return True
        return False

    async def revoke_delegated_buys_for_unavailable_market_evidence(
        self,
        *,
        target_trade_date: str | date,
        trigger: str,
        account_reference: str | None = None,
    ) -> dict[str, Any]:
        """Durably stage and synchronously flush the critical-chain BUY safety fence."""
        resolved_target_date = _normalize_date(target_trade_date)
        resolved_account_reference = _normalize_optional_text(account_reference)
        staged = await asyncio.to_thread(
            signal_decider_next_open_execution_service.stage_market_evidence_unavailable_revocations,
            target_trade_date=resolved_target_date,
            account_reference=resolved_account_reference,
            failure_source=trigger,
        )
        revocations = await self._flush_pending_exec_revocations(
            target_trade_date=resolved_target_date,
            account_reference=resolved_account_reference,
            integration=self.integration_status(),
            trigger=trigger,
        )
        return {
            **staged,
            'revocations': revocations,
        }

    async def runtime_status(self, *, refresh: bool = True) -> dict[str, Any]:
        integration = self.integration_status()
        runtime = {
            'status': integration['config_status'],
            'payload': None,
            'error': integration['issues'][0] if integration['issues'] else None,
        }
        if not refresh or integration['config_status'] in {REMOTE_FETCH_STATUS_DISABLED, REMOTE_FETCH_STATUS_NOT_CONFIGURED}:
            return {
                'integration': integration,
                'runtime': runtime,
            }
        try:
            payload = await self.client.get_runtime_status()
        except SignalDeciderExecClientError as exc:
            runtime = {
                'status': self._client_error_status(exc),
                'payload': None,
                'error': _serialize_error(
                    code=exc.code,
                    message=str(exc),
                    retryable=exc.retryable,
                    status_code=exc.status_code,
                ),
            }
        else:
            runtime = {
                'status': REMOTE_FETCH_STATUS_OK,
                'payload': payload,
                'error': None,
            }
        return {
            'integration': integration,
            'runtime': runtime,
        }

    @staticmethod
    async def _inspect_dispatch_authority_generation(
        *,
        target_trade_date: date,
        rows: Sequence[SignalDeciderExecutionPlanItem],
        revision_fingerprints: dict[int, str],
        retry_plan_refresh_conflict: bool = False,
    ) -> dict[str, Any]:
        def inspect() -> dict[str, Any]:
            return (
                signal_decider_next_open_execution_service
                .inspect_dispatch_authority_generation(
                    target_trade_date=target_trade_date,
                    expected_row_bindings=_dispatch_authority_row_bindings(
                        rows,
                        revision_fingerprints=revision_fingerprints,
                    ),
                )
            )

        try:
            if retry_plan_refresh_conflict:
                return await _retry_plan_refresh_revision_conflict(inspect)
            return await asyncio.to_thread(inspect)
        except Exception as exc:  # noqa: BLE001 - dispatch must fail closed
            return {
                'status': 'invalid',
                'generation_fingerprint': None,
                'reason_code': 'current_live_market_evidence_unavailable',
                'replacement_action': 'reject',
                'replacement_actionable': False,
                'error_type': type(exc).__name__,
            }

    @staticmethod
    def _persist_core_activation_ack(
        *,
        rows: Sequence[SignalDeciderExecutionPlanItem],
        revision_fingerprints: dict[int, str],
        request_payload: dict[str, Any],
        response_payload: dict[str, Any],
    ) -> None:
        row_ids = [int(row.id) for row in rows]
        if not row_ids or set(revision_fingerprints) != set(row_ids):
            raise ValueError(
                'Core activation revision fingerprints must bind every row'
            )
        request_by_key = {
            str(item['core_item_key']): item
            for item in request_payload['items']
        }
        with session_scope() as session:
            durable_generation = (
                signal_decider_next_open_execution_service
                .inspect_dispatch_authority_generation(
                    target_trade_date=rows[0].target_trade_date,
                    expected_row_bindings=_dispatch_authority_row_bindings(
                        rows,
                        revision_fingerprints=revision_fingerprints,
                    ),
                    _session=session,
                )
            )
            if (
                _dispatch_authority_generation_conflict(
                    before={
                        'status': 'valid',
                        'generation_fingerprint': request_payload[
                            'authority_generation_fingerprint'
                        ],
                    },
                    after=durable_generation,
                )
                is not None
            ):
                raise ValueError(
                    'Core activation authority changed before durable ACK'
                )
            persisted_rows = session.execute(
                select(SignalDeciderExecutionPlanItem)
                .where(SignalDeciderExecutionPlanItem.id.in_(row_ids))
                .order_by(SignalDeciderExecutionPlanItem.id.asc())
                .with_for_update()
            ).scalars().all()
            if len(persisted_rows) != len(row_ids):
                raise ValueError('Core activation rows changed before ACK persistence')

            prepared: list[
                tuple[
                    SignalDeciderExecutionPlanItem,
                    dict[str, Any],
                    dict[str, Any],
                ]
            ] = []
            for row in persisted_rows:
                if row.execution_status in {
                    'revocation_pending',
                    'revocation_manual_review',
                    'revoked',
                }:
                    raise ValueError('Core activation row was revoked before ACK persistence')
                submission = (
                    copy.deepcopy(row.submission_payload)
                    if isinstance(row.submission_payload, dict)
                    else {}
                )
                exec_dispatch = (
                    copy.deepcopy(submission.get('exec_dispatch'))
                    if isinstance(submission.get('exec_dispatch'), dict)
                    else {}
                )
                proof = (
                    copy.deepcopy(exec_dispatch.get('contract_proof'))
                    if isinstance(exec_dispatch.get('contract_proof'), dict)
                    else {}
                )
                links = [
                    link
                    for link in (exec_dispatch.get('item_links') or [])
                    if isinstance(link, dict)
                ]
                linked_by_key = {
                    str(link.get('core_item_key') or ''): link
                    for link in links
                }
                submitted_keys = proof.get('submitted_core_item_keys')
                expected_keys = (
                    {str(key) for key in submitted_keys}
                    if isinstance(submitted_keys, list)
                    else set()
                )
                core_activation = (
                    proof.get('core_activation')
                    if isinstance(proof.get('core_activation'), dict)
                    else {}
                )
                activation_status = _normalize_status(
                    core_activation.get('status')
                )
                pending_activation = {
                    'schema_version': (
                        CORE_EXEC_ACTIVATION_BINDING_SCHEMA_VERSION
                    ),
                    'mode': 'core_confirmed',
                    'authority_generation_fingerprint': request_payload[
                        'authority_generation_fingerprint'
                    ],
                    'status': 'pending',
                    'dispatch_material_fingerprint': request_payload[
                        'dispatch_material_fingerprint'
                    ],
                }
                if (
                    exec_dispatch.get('plan_id') != request_payload['plan_id']
                    or proof.get('execution_plan_id')
                    != request_payload['execution_plan_id']
                    or proof.get('dispatch_material_fingerprint')
                    != request_payload['dispatch_material_fingerprint']
                    or _validated_sha256_hex(
                        exec_dispatch.get('dispatch_revision_fingerprint')
                    )
                    != _row_dispatch_revision_fingerprint(row)
                    or (
                        core_activation != pending_activation
                        and not (
                            activation_status == 'activated'
                            and core_activation.get('schema_version')
                            == EXECUTION_ACTIVATION_SCHEMA_VERSION
                            and core_activation.get(
                                'dispatch_material_fingerprint'
                            )
                            == request_payload[
                                'dispatch_material_fingerprint'
                            ]
                            and core_activation.get(
                                'authority_generation_fingerprint'
                            )
                            == request_payload[
                                'authority_generation_fingerprint'
                            ]
                        )
                    )
                    or not expected_keys
                    or not expected_keys.issubset(request_by_key)
                    or set(linked_by_key) != expected_keys
                    or any(
                        str(request_by_key[key]['decision_snapshot_id'])
                        != str(row.decision_snapshot_id)
                        for key in expected_keys
                    )
                    or any(
                        str(linked_by_key[key].get('exec_plan_item_id') or '')
                        != str(request_by_key[key]['execution_plan_item_id'])
                        for key in expected_keys
                    )
                ):
                    raise ValueError(
                        'Core activation ACK no longer matches persisted dispatch authority'
                    )
                prepared.append((row, submission, exec_dispatch))

            activated_at = _utcnow().isoformat()
            for row, submission, exec_dispatch in prepared:
                proof = copy.deepcopy(exec_dispatch['contract_proof'])
                proof['core_activation'] = {
                    'schema_version': EXECUTION_ACTIVATION_SCHEMA_VERSION,
                    'status': 'activated',
                    'activated_at': activated_at,
                    'dispatch_material_fingerprint': request_payload[
                        'dispatch_material_fingerprint'
                    ],
                    'authority_generation_fingerprint': request_payload[
                        'authority_generation_fingerprint'
                    ],
                    'contract_ack': copy.deepcopy(
                        response_payload.get('contract_ack')
                    ),
                }
                exec_dispatch['contract_proof'] = proof
                submission['exec_dispatch'] = exec_dispatch
                row.submission_payload = slim_signal_decider_exec_submission_payload(
                    submission
                ).payload
            session.flush()

    async def _activate_core_confirmed_dispatch(
        self,
        *,
        rows: Sequence[SignalDeciderExecutionPlanItem],
        plan_payload: dict[str, Any],
        response_payload: dict[str, Any],
        persisted_group_result: dict[str, Any],
        target_trade_date: date,
        account_reference: str,
        trigger: str,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        if (
            persisted_group_result.get('dispatch_status')
            != EXEC_DISPATCH_STATUS_ACCEPTED
            or _core_activation_binding_from_plan(plan_payload) is None
        ):
            return persisted_group_result, []

        activation_request: dict[str, Any] | None = None
        try:
            binding = _core_activation_binding_from_plan(plan_payload)
            if binding is None:
                raise ValueError(
                    'execution plan is missing a Core activation binding'
                )
            revision_fingerprints = {
                int(row.id): _row_dispatch_revision_fingerprint(row)
                for row in rows
            }
            expected_generation = {
                'status': 'valid',
                'generation_fingerprint': binding[
                    'authority_generation_fingerprint'
                ],
            }
            generation_before = (
                await self._inspect_dispatch_authority_generation(
                    target_trade_date=target_trade_date,
                    rows=rows,
                    revision_fingerprints=revision_fingerprints,
                )
            )
            generation_conflict = _dispatch_authority_generation_conflict(
                before=expected_generation,
                after=generation_before,
            )
            if generation_conflict is not None:
                if (
                    generation_before.get('error_type')
                    == '_PlanRefreshRevisionConflict'
                ):
                    return (
                        {
                            **persisted_group_result,
                            'activation_status': 'pending',
                            'activation_error': _serialize_error(
                                code='execution_activation_fence_busy',
                                message=(
                                    'Core activation fence is busy; the Exec '
                                    'plan remains quarantined for retry.'
                                ),
                                retryable=True,
                            ),
                        },
                        [],
                    )
                if _is_actionable_generation_replacement(generation_conflict):
                    return (
                        {
                            **persisted_group_result,
                            'activation_status': 'superseded_generation',
                            'activation_error': _serialize_error(
                                code='execution_activation_generation_superseded',
                                message=(
                                    'A newer actionable Core generation replaced '
                                    'the accepted Exec plan before activation; the '
                                    'replacement remains eligible for redispatch.'
                                ),
                                retryable=True,
                            ),
                        },
                        [],
                    )
                raise ValueError(
                    'Core activation authority changed before activation POST'
                )
            activation_request = _build_execution_activation_payload(
                plan_payload=plan_payload,
                response_payload=response_payload,
            )
            activation_response = (
                await self.client.post_execution_plan_activation(
                    activation_request
                )
            )
            _validate_execution_activation_ack(
                activation_response,
                request_payload=activation_request,
            )
            generation_after = (
                await self._inspect_dispatch_authority_generation(
                    target_trade_date=target_trade_date,
                    rows=rows,
                    revision_fingerprints=revision_fingerprints,
                    retry_plan_refresh_conflict=True,
                )
            )
            generation_conflict = _dispatch_authority_generation_conflict(
                before=expected_generation,
                after=generation_after,
            )
            if generation_conflict is not None:
                if not _is_actionable_generation_replacement(
                    generation_conflict
                ):
                    raise ValueError(
                        'Core activation authority changed during activation POST'
                    )
                return (
                    {
                        **persisted_group_result,
                        'activation_status': 'superseded_generation',
                        'activation_error': _serialize_error(
                            code='execution_activation_generation_superseded',
                            message=(
                                'A newer Core authority generation replaced the '
                                'plan during activation; do not poison the newer '
                                'generation with a market-evidence revocation.'
                            ),
                            retryable=True,
                        ),
                    },
                    [],
                )
            await _retry_plan_refresh_revision_conflict(
                lambda: self._persist_core_activation_ack(
                    rows=rows,
                    revision_fingerprints=revision_fingerprints,
                    request_payload=activation_request,
                    response_payload=activation_response,
                )
            )
        except Exception as exc:  # noqa: BLE001 - classify stale generation separately
            if _is_superseded_activation_error(exc):
                return (
                    {
                        **persisted_group_result,
                        'activation_status': 'superseded_generation',
                        'activation_error': _serialize_error(
                            code='execution_activation_generation_superseded',
                            message=str(exc),
                            retryable=True,
                            status_code=(
                                exc.status_code
                                if isinstance(exc, SignalDeciderExecClientError)
                                else None
                            ),
                        ),
                    },
                    [],
                )
            if isinstance(exc, SignalDeciderExecClientError) and exc.retryable:
                return (
                    {
                        **persisted_group_result,
                        'activation_status': 'pending',
                        'activation_error': _serialize_error(
                            code=exc.code,
                            message=str(exc),
                            retryable=True,
                            status_code=exc.status_code,
                        ),
                        'activation_request_created': (
                            activation_request is not None
                        ),
                    },
                    [],
                )
            error = (
                _serialize_error(
                    code=exc.code,
                    message=str(exc),
                    retryable=exc.retryable,
                    status_code=exc.status_code,
                )
                if isinstance(exc, SignalDeciderExecClientError)
                else _serialize_error(
                    code='execution_activation_ack_invalid',
                    message=str(exc),
                    retryable=isinstance(exc, AttributeError),
                )
            )
            await asyncio.to_thread(
                signal_decider_next_open_execution_service
                .stage_market_evidence_unavailable_revocations,
                target_trade_date=target_trade_date,
                account_reference=account_reference,
                row_ids=[int(row.id) for row in rows],
                failure_source=f'{trigger}_core_activation_failed',
            )
            return (
                {
                    **persisted_group_result,
                    'dispatch_status': 'revocation_pending',
                    'activation_status': 'failed_closed',
                    'activation_error': error,
                    'activation_request_created': activation_request is not None,
                },
                [],
            )

        return (
            {
                **persisted_group_result,
                'activation_status': 'activated',
                'activation': {
                    'schema_version': EXECUTION_ACTIVATION_SCHEMA_VERSION,
                    'authority_generation_fingerprint': activation_request[
                        'authority_generation_fingerprint'
                    ],
                    'dispatch_material_fingerprint': activation_request[
                        'dispatch_material_fingerprint'
                    ],
                },
            },
            [],
        )

    async def _reconcile_pending_core_activations(
        self,
        *,
        target_trade_date: date,
        account_reference: str | None,
        integration: dict[str, Any],
        buy_authority_sync: dict[str, Any],
        trigger: str,
    ) -> list[dict[str, Any]]:
        """Retry exact quarantined plans after Core lost the activation window."""
        rows = self._load_next_open_rows(
            target_trade_date=target_trade_date,
            account_reference=account_reference,
            include_payloads=True,
        )
        pending_rows: list[SignalDeciderExecutionPlanItem] = []
        for row in rows:
            submission = (
                row.submission_payload
                if isinstance(row.submission_payload, dict)
                else {}
            )
            exec_dispatch = (
                submission.get('exec_dispatch')
                if isinstance(submission.get('exec_dispatch'), dict)
                else {}
            )
            proof = (
                exec_dispatch.get('contract_proof')
                if isinstance(exec_dispatch.get('contract_proof'), dict)
                else {}
            )
            activation = (
                proof.get('core_activation')
                if isinstance(proof.get('core_activation'), dict)
                else None
            )
            if (
                _normalize_status(exec_dispatch.get('dispatch_status'))
                == EXEC_DISPATCH_STATUS_ACCEPTED
                and isinstance(activation, dict)
                and _normalize_status(activation.get('status')) == 'pending'
                and row.execution_status
                not in {'revocation_pending', 'revocation_manual_review', 'revoked'}
            ):
                pending_rows.append(row)

        results: list[dict[str, Any]] = []
        for plan_id, group_rows in self._group_rows_by_plan_id(
            pending_rows
        ).items():
            group_accounts = sorted(
                {
                    str(row.account_reference)
                    for row in group_rows
                    if _normalize_optional_text(row.account_reference)
                }
            )

            async def fail_closed(
                *,
                reason_code: str,
                error: dict[str, Any] | None = None,
            ) -> list[dict[str, Any]]:
                revocations: list[dict[str, Any]] = []
                for group_account in group_accounts:
                    await asyncio.to_thread(
                        signal_decider_next_open_execution_service
                        .stage_market_evidence_unavailable_revocations,
                        target_trade_date=target_trade_date,
                        account_reference=group_account,
                        row_ids=[int(row.id) for row in group_rows],
                        failure_source=(
                            f'{trigger}_pending_core_activation_{reason_code}'
                        ),
                    )
                    revocations.extend(
                        await self._flush_pending_exec_revocations(
                            target_trade_date=target_trade_date,
                            account_reference=group_account,
                            integration=integration,
                            trigger=(
                                f'{trigger}_pending_core_activation_'
                                f'{reason_code}'
                            ),
                        )
                    )
                results.append(
                    {
                        'plan_id': plan_id,
                        'status': 'revocation_pending',
                        'reason_code': reason_code,
                        'error': error,
                        'revocations': revocations,
                    }
                )
                return revocations

            try:
                (
                    activation_request,
                    revision_fingerprints,
                    expected_destination,
                ) = _build_pending_core_activation_request(group_rows)
            except ValueError as exc:
                await fail_closed(
                    reason_code='persisted_contract_invalid',
                    error=_serialize_error(
                        code='execution_activation_persisted_contract_invalid',
                        message=str(exc),
                        retryable=False,
                    ),
                )
                continue

            group_account = group_accounts[0]
            try:
                active_destination = self._active_destination_binding(
                    account_reference=group_account,
                    integration=integration,
                )
            except ValueError:
                active_destination = None
            if active_destination != expected_destination:
                reason_code = (
                    'exec_destination_binding_missing'
                    if active_destination is None
                    else 'exec_destination_mismatch'
                )
                for row in group_rows:
                    self._mark_exec_destination_manual_review(
                        row_id=int(row.id),
                        trigger=trigger,
                        reason_code=reason_code,
                        expected_destination=expected_destination,
                        active_destination=active_destination,
                    )
                results.append(
                    {
                        'plan_id': plan_id,
                        'status': 'manual_review_required',
                        'reason_code': reason_code,
                    }
                )
                continue

            requires_buy_authority = any(
                _dispatch_group_requires_core_krx_buy_authority(
                    route_family=route_family,
                    rows=scoped_rows,
                )
                for (
                    _scoped_account,
                    _decision_agent_name,
                    route_family,
                ), scoped_rows in self._group_rows_by_dispatch_scope(
                    group_rows
                ).items()
            )
            if requires_buy_authority and buy_authority_sync.get('applies') is True:
                synchronized_accounts = buy_authority_sync.get('accounts')
                account_authority = (
                    synchronized_accounts.get(group_account)
                    if isinstance(synchronized_accounts, dict)
                    else None
                )
                if not isinstance(account_authority, dict) or not account_authority.get(
                    'dispatch_allowed'
                ):
                    results.append(
                        {
                            'plan_id': plan_id,
                            'status': 'activation_pending',
                            'reason_code': (
                                'core_exec_buy_authority_not_acknowledged'
                            ),
                        }
                    )
                    continue

            expected_generation = {
                'status': 'valid',
                'generation_fingerprint': activation_request[
                    'authority_generation_fingerprint'
                ],
            }
            generation_before = (
                await self._inspect_dispatch_authority_generation(
                    target_trade_date=target_trade_date,
                    rows=group_rows,
                    revision_fingerprints=revision_fingerprints,
                )
            )
            generation_conflict = _dispatch_authority_generation_conflict(
                before=expected_generation,
                after=generation_before,
            )
            if generation_conflict is not None:
                if (
                    generation_before.get('error_type')
                    == '_PlanRefreshRevisionConflict'
                ):
                    results.append(
                        {
                            'plan_id': plan_id,
                            'status': 'activation_pending',
                            'reason_code': 'activation_fence_busy',
                        }
                    )
                    continue
                if _is_actionable_generation_replacement(generation_conflict):
                    results.append(
                        {
                            'plan_id': plan_id,
                            'status': 'superseded_generation',
                            'reason_code': 'authority_generation_changed',
                        }
                    )
                else:
                    await fail_closed(reason_code='authority_generation_changed')
                continue

            try:
                activation_response = (
                    await self.client.post_execution_plan_activation(
                        activation_request
                    )
                )
                _validate_execution_activation_ack(
                    activation_response,
                    request_payload=activation_request,
                )
                generation_after = (
                    await self._inspect_dispatch_authority_generation(
                        target_trade_date=target_trade_date,
                        rows=group_rows,
                        revision_fingerprints=revision_fingerprints,
                        retry_plan_refresh_conflict=True,
                    )
                )
                generation_conflict = _dispatch_authority_generation_conflict(
                    before=expected_generation,
                    after=generation_after,
                )
                if generation_conflict is not None:
                    if _is_actionable_generation_replacement(
                        generation_conflict
                    ):
                        results.append(
                            {
                                'plan_id': plan_id,
                                'status': 'superseded_generation',
                                'reason_code': (
                                    'post_activation_authority_changed'
                                ),
                            }
                        )
                    else:
                        await fail_closed(
                            reason_code='post_activation_authority_changed'
                        )
                    continue
                await _retry_plan_refresh_revision_conflict(
                    lambda: self._persist_core_activation_ack(
                        rows=group_rows,
                        revision_fingerprints=revision_fingerprints,
                        request_payload=activation_request,
                        response_payload=activation_response,
                    )
                )
            except Exception as exc:  # noqa: BLE001 - stale generations converge forward
                if _is_superseded_activation_error(exc):
                    results.append(
                        {
                            'plan_id': plan_id,
                            'status': 'superseded_generation',
                            'reason_code': 'activation_generation_superseded',
                        }
                    )
                    continue
                if isinstance(exc, SignalDeciderExecClientError) and exc.retryable:
                    results.append(
                        {
                            'plan_id': plan_id,
                            'status': 'activation_pending',
                            'reason_code': 'activation_transport_retryable',
                            'error': _serialize_error(
                                code=exc.code,
                                message=str(exc),
                                retryable=True,
                                status_code=exc.status_code,
                            ),
                        }
                    )
                    continue
                error = (
                    _serialize_error(
                        code=exc.code,
                        message=str(exc),
                        retryable=exc.retryable,
                        status_code=exc.status_code,
                    )
                    if isinstance(exc, SignalDeciderExecClientError)
                    else _serialize_error(
                        code='execution_activation_recovery_failed',
                        message=str(exc),
                        retryable=isinstance(exc, AttributeError),
                    )
                )
                await fail_closed(
                    reason_code='activation_recovery_failed',
                    error=error,
                )
                continue

            results.append(
                {
                    'plan_id': plan_id,
                    'status': 'activated',
                    'activated_item_count': len(activation_request['items']),
                }
            )
        return results

    async def _reconcile_pending_dispatch_write_ahead(
        self,
        *,
        target_trade_date: date,
        account_reference: str | None,
        integration: dict[str, Any],
        buy_authority_sync: dict[str, Any],
        trigger: str,
    ) -> list[dict[str, Any]]:
        rows = self._load_next_open_rows(
            target_trade_date=target_trade_date,
            account_reference=account_reference,
            include_payloads=True,
        )
        self._requeue_legacy_retryable_ambiguous_write_aheads(
            rows=rows,
            trigger=trigger,
        )
        rows = self._load_next_open_rows(
            target_trade_date=target_trade_date,
            account_reference=account_reference,
            include_payloads=True,
        )
        pending_rows = [
            row
            for row in rows
            if row.execution_status
            == EXECUTION_STATUS_DISPATCH_PENDING_REMOTE_ACK
            or _normalize_status(
                (
                    row.submission_payload.get('exec_dispatch')
                    if isinstance(row.submission_payload, dict)
                    and isinstance(row.submission_payload.get('exec_dispatch'), dict)
                    else {}
                ).get('dispatch_status')
            )
            == 'pending_remote_ack'
        ]
        results: list[dict[str, Any]] = []
        for plan_id, group_rows in self._group_rows_by_plan_id(pending_rows).items():
            if not plan_id or not group_rows:
                continue
            exec_states = [
                row.submission_payload['exec_dispatch']
                for row in group_rows
                if isinstance(row.submission_payload, dict)
                and isinstance(row.submission_payload.get('exec_dispatch'), dict)
            ]
            expected_destination = (
                _validated_exec_destination_binding(
                    exec_states[0].get('destination_binding')
                )
                if len(exec_states) == len(group_rows)
                else None
            )
            try:
                active_destination = self._active_destination_binding(
                    account_reference=str(group_rows[0].account_reference),
                    integration=integration,
                )
            except ValueError:
                active_destination = None
            destination_matches = bool(
                expected_destination is not None
                and active_destination == expected_destination
                and all(
                    _validated_exec_destination_binding(
                        state.get('destination_binding')
                    )
                    == expected_destination
                    for state in exec_states
                )
            )
            if not destination_matches:
                reason_code = (
                    'exec_destination_binding_missing'
                    if expected_destination is None
                    else 'exec_destination_mismatch'
                )
                for row in group_rows:
                    self._mark_exec_destination_manual_review(
                        row_id=int(row.id),
                        trigger=trigger,
                        reason_code=reason_code,
                        expected_destination=expected_destination,
                        active_destination=active_destination,
                    )
                results.append(
                    {
                        'plan_id': plan_id,
                        'status': 'manual_review_required',
                        'reason_code': reason_code,
                    }
                )
                continue

            if any(
                str(row.planned_side or '').strip().lower() != 'buy'
                for row in group_rows
            ):
                reason_code = (
                    'dispatch_write_ahead_non_buy_invariant_violation'
                )
                for row in group_rows:
                    self._mark_exec_destination_manual_review(
                        row_id=int(row.id),
                        trigger=trigger,
                        reason_code=reason_code,
                        expected_destination=expected_destination,
                        active_destination=active_destination,
                    )
                results.append(
                    {
                        'plan_id': plan_id,
                        'status': 'manual_review_required',
                        'reason_code': reason_code,
                    }
                )
                continue

            dispatch_material_fingerprints = {
                _normalize_optional_text(
                    state.get('dispatch_material_fingerprint')
                )
                for state in exec_states
            }
            plan_fingerprints = {
                _normalize_optional_text(state.get('plan_fingerprint'))
                for state in exec_states
            }
            revision_fingerprints = {
                int(row.id): _normalize_optional_text(
                    row.submission_payload['exec_dispatch'].get(
                        'dispatch_revision_fingerprint'
                    )
                )
                for row in group_rows
            }
            if (
                None in dispatch_material_fingerprints
                or len(dispatch_material_fingerprints) != 1
                or None in plan_fingerprints
                or len(plan_fingerprints) != 1
                or any(value is None for value in revision_fingerprints.values())
            ):
                for row in group_rows:
                    self._mark_exec_destination_manual_review(
                        row_id=int(row.id),
                        trigger=trigger,
                        reason_code='dispatch_write_ahead_invalid',
                        expected_destination=expected_destination,
                        active_destination=active_destination,
                    )
                results.append(
                    {
                        'plan_id': plan_id,
                        'status': 'manual_review_required',
                        'reason_code': 'dispatch_write_ahead_invalid',
                    }
                )
                continue

            batch_keys = {
                _normalize_optional_text(state.get('dispatch_batch_key'))
                for state in exec_states
            }
            delta_batch_key = next(iter(batch_keys)) if len(batch_keys) == 1 else None
            try:
                payload, row_links = self._build_exec_plan_payload(
                    rows=group_rows,
                    snapshots=self._load_snapshots(group_rows),
                    trigger=f'{trigger}_write_ahead_recovery',
                    delta_batch_key=delta_batch_key,
                )
            except SignalDeciderExecPayloadContractError:
                payload = {}
                row_links = {}
            stored_authority_generations = [
                state.get('authority_generation_before')
                for state in exec_states
                if isinstance(state.get('authority_generation_before'), dict)
            ]
            stored_activation_bindings = [
                generation.get('core_activation')
                for generation in stored_authority_generations
                if isinstance(generation.get('core_activation'), dict)
            ]
            if payload and stored_activation_bindings:
                generation_fingerprints = {
                    _validated_sha256_hex(
                        generation.get('generation_fingerprint')
                    )
                    for generation in stored_authority_generations
                    if generation.get('status') == 'valid'
                }
                if (
                    len(stored_authority_generations) != len(exec_states)
                    or len(stored_activation_bindings) != len(exec_states)
                    or len(
                        {
                            json.dumps(
                                binding,
                                sort_keys=True,
                                separators=(',', ':'),
                            )
                            for binding in stored_activation_bindings
                        }
                    )
                    != 1
                    or None in generation_fingerprints
                    or len(generation_fingerprints) != 1
                ):
                    payload = {}
                    row_links = {}
                else:
                    try:
                        payload = _bind_core_confirmed_activation(
                            payload,
                            authority_generation={
                                'status': 'valid',
                                'generation_fingerprint': next(
                                    iter(generation_fingerprints)
                                ),
                            },
                        )
                    except SignalDeciderExecPayloadContractError:
                        payload = {}
                        row_links = {}
            expected_material_fingerprint = next(
                iter(dispatch_material_fingerprints)
            )
            expected_plan_fingerprint = next(iter(plan_fingerprints))
            reconstructed_core_keys = {
                str(link.get('core_item_key'))
                for links in row_links.values()
                for link in links
                if isinstance(link, dict) and link.get('core_item_key')
            }
            stored_core_keys = {
                str(link.get('core_item_key'))
                for state in exec_states
                for link in (state.get('item_links') or [])
                if isinstance(link, dict) and link.get('core_item_key')
            }
            row_core_key_bindings_match = all(
                {
                    str(link.get('core_item_key'))
                    for link in (
                        row.submission_payload['exec_dispatch'].get(
                            'item_links'
                        )
                        or []
                    )
                    if isinstance(link, dict) and link.get('core_item_key')
                }
                == {
                    str(link.get('core_item_key'))
                    for link in row_links.get(int(row.id), [])
                    if isinstance(link, dict) and link.get('core_item_key')
                }
                for row in group_rows
            )
            local_material_matches = bool(
                payload
                and payload.get('plan_id') == plan_id
                and payload.get('metadata', {}).get(
                    'dispatch_material_fingerprint'
                )
                == expected_material_fingerprint
                and _fingerprint_exec_plan(payload) == expected_plan_fingerprint
                and reconstructed_core_keys == stored_core_keys
                and reconstructed_core_keys
                and row_core_key_bindings_match
            )
            if not local_material_matches:
                revision_changed = any(
                    _row_dispatch_revision_fingerprint(row)
                    != revision_fingerprints.get(int(row.id))
                    for row in group_rows
                )
                reason_code = (
                    'ambiguous_remote_acceptance_after_core_revision'
                    if revision_changed
                    else 'dispatch_write_ahead_material_mismatch'
                )
                for row in group_rows:
                    self._mark_exec_destination_manual_review(
                        row_id=int(row.id),
                        trigger=trigger,
                        reason_code=reason_code,
                        expected_destination=expected_destination,
                        active_destination=active_destination,
                    )
                results.append(
                    {
                        'plan_id': plan_id,
                        'status': 'manual_review_required',
                        'reason_code': reason_code,
                    }
                )
                continue

            try:
                readback = await self.client.list_execution_plans(
                    external_plan_id=plan_id
                )
            except SignalDeciderExecClientError as exc:
                results.append(
                    {
                        'plan_id': plan_id,
                        'status': 'reconciliation_unavailable',
                        'error': _serialize_error(
                            code=exc.code,
                            message=str(exc),
                            retryable=exc.retryable,
                            status_code=exc.status_code,
                        ),
                    }
                )
                continue
            remote_plans = _collect_items(readback)
            remote_plan = next(
                (
                    item
                    for item in remote_plans
                    if _normalize_optional_text(
                        item.get('plan_id') or item.get('external_plan_id')
                    )
                    == plan_id
                ),
                None,
            )
            if remote_plan is None and remote_plans:
                reason_code = 'dispatch_write_ahead_readback_conflict'
                for row in group_rows:
                    self._mark_exec_destination_manual_review(
                        row_id=int(row.id),
                        trigger=trigger,
                        reason_code=reason_code,
                        expected_destination=expected_destination,
                        active_destination=active_destination,
                    )
                results.append(
                    {
                        'plan_id': plan_id,
                        'status': 'manual_review_required',
                        'reason_code': reason_code,
                    }
                )
                continue
            if remote_plan is None:
                can_retry = bool(
                    integration.get('delegate_execution')
                    and integration.get('config_status') == 'ready'
                    and settings.signal_decider_next_open_enabled
                )
                authority_generation_before = (
                    exec_states[0].get('authority_generation_before')
                    if isinstance(
                        exec_states[0].get('authority_generation_before'),
                        dict,
                    )
                    else None
                )
                authority_generation_after = (
                    await self._inspect_dispatch_authority_generation(
                        target_trade_date=target_trade_date,
                        rows=group_rows,
                        revision_fingerprints={
                            row_id: str(value)
                            for row_id, value in revision_fingerprints.items()
                        },
                    )
                )
                generation_unchanged = bool(
                    authority_generation_before is not None
                    and _dispatch_authority_generation_conflict(
                        before=authority_generation_before,
                        after=authority_generation_after,
                    )
                    is None
                )
                if not generation_unchanged:
                    self._release_not_found_dispatch_write_ahead(
                        rows=group_rows,
                        submitted_revision_fingerprints={
                            row_id: str(value)
                            for row_id, value in revision_fingerprints.items()
                        },
                        plan_id=plan_id,
                        plan_fingerprint=str(expected_plan_fingerprint),
                        dispatch_material_fingerprint=str(
                            expected_material_fingerprint
                        ),
                        destination_binding=expected_destination,
                        trigger=trigger,
                        reason_code='dispatch_authority_generation_changed',
                    )
                    results.append(
                        {
                            'plan_id': plan_id,
                            'status': 'not_found_write_ahead_released',
                            'reason_code': 'dispatch_authority_generation_changed',
                        }
                    )
                    continue
                if not can_retry:
                    results.append(
                        {
                            'plan_id': plan_id,
                            'status': 'not_found_pending_retry',
                            'reason_code': 'execution_control_disabled',
                        }
                    )
                    continue
                required_authority_accounts = {
                    scoped_account
                    for (
                        scoped_account,
                        _decision_agent_name,
                        route_family,
                    ), scoped_rows in self._group_rows_by_dispatch_scope(
                        group_rows
                    ).items()
                    if _dispatch_group_requires_core_krx_buy_authority(
                        route_family=route_family,
                        rows=scoped_rows,
                    )
                }
                if (
                    required_authority_accounts
                    and buy_authority_sync.get('applies') is True
                ):
                    synchronized_accounts = buy_authority_sync.get('accounts')
                    account_results = (
                        synchronized_accounts
                        if isinstance(synchronized_accounts, dict)
                        else {}
                    )
                    blocked_accounts = sorted(
                        scoped_account
                        for scoped_account in required_authority_accounts
                        if not isinstance(
                            account_results.get(scoped_account),
                            dict,
                        )
                        or not account_results[scoped_account].get(
                            'dispatch_allowed'
                        )
                    )
                    if blocked_accounts:
                        results.append(
                            {
                                'plan_id': plan_id,
                                'status': 'not_found_pending_retry',
                                'reason_code': (
                                    'core_exec_buy_authority_not_acknowledged'
                                ),
                                'blocked_account_references': blocked_accounts,
                            }
                        )
                        continue
                try:
                    candidate_response = await self.client.post_execution_plan(
                        payload
                    )
                except SignalDeciderExecClientError as exc:
                    recovery_status = 'not_attempted'
                    if (
                        exc.status_code == 409
                        or exc.retryable
                        or exc.code == EXEC_DISPATCH_STATUS_UNAVAILABLE
                    ):
                        (
                            candidate_response,
                            recovery_status,
                        ) = await self._recover_duplicate_dispatch_response(
                            plan_id=plan_id,
                            row_links=row_links,
                            expected_dispatch_material_fingerprint=str(
                                expected_material_fingerprint
                            ),
                        )
                    else:
                        candidate_response = None
                    if candidate_response is None:
                        preserve_write_ahead = bool(
                            exc.retryable
                            and recovery_status
                            in {'not_found', 'unavailable'}
                        )
                        preserved_write_ahead = self._persist_dispatch_failure(
                            rows=group_rows,
                            submitted_revision_fingerprints={
                                row_id: str(value)
                                for row_id, value in revision_fingerprints.items()
                            },
                            row_links=row_links,
                            plan_id=plan_id,
                            fingerprint=str(expected_plan_fingerprint),
                            payload=payload,
                            trigger=f'{trigger}_write_ahead_retry',
                            status=self._client_error_status(exc),
                            error=_serialize_error(
                                code=exc.code,
                                message=str(exc),
                                retryable=exc.retryable,
                                status_code=exc.status_code,
                            ),
                            remote_acceptance_ambiguous=True,
                            preserve_write_ahead=preserve_write_ahead,
                        )
                        results.append(
                            {
                                'plan_id': plan_id,
                                'status': (
                                    'not_found_pending_retry'
                                    if preserved_write_ahead
                                    else 'manual_review_required'
                                ),
                                'reason_code': (
                                    'retryable_remote_acceptance_ambiguous'
                                    if preserved_write_ahead
                                    else 'ambiguous_remote_acceptance'
                                ),
                            }
                        )
                        continue
                persisted = await self._persist_dispatch_success_with_revision_retry(
                    rows=group_rows,
                    submitted_revision_fingerprints={
                        row_id: str(value)
                        for row_id, value in revision_fingerprints.items()
                    },
                    row_links=row_links,
                    plan_id=plan_id,
                    fingerprint=str(expected_plan_fingerprint),
                    payload=payload,
                    response_payload=candidate_response,
                    trigger=f'{trigger}_write_ahead_retry',
                    authority_generation_before=authority_generation_before,
                )
                persisted, _ = await self._activate_core_confirmed_dispatch(
                    rows=group_rows,
                    plan_payload=payload,
                    response_payload=candidate_response,
                    persisted_group_result=persisted,
                    target_trade_date=target_trade_date,
                    account_reference=str(group_rows[0].account_reference),
                    trigger=f'{trigger}_write_ahead_retry',
                )
                results.append({'plan_id': plan_id, **persisted})
                if persisted.get('dispatch_status') == 'revocation_pending':
                    await self._flush_pending_exec_revocations(
                        target_trade_date=target_trade_date,
                        account_reference=str(
                            group_rows[0].account_reference
                        ),
                        integration=integration,
                        trigger=(
                            f'{trigger}_write_ahead_retry_revision_fence'
                        ),
                    )
                continue
            candidate_response = {
                'plan_id': remote_plan.get('plan_id') or plan_id,
                'execution_plan_id': remote_plan.get('execution_plan_id'),
                'contract_ack': remote_plan.get('contract_ack'),
                'accepted_item_count': remote_plan.get('accepted_item_count'),
                'rejected_item_count': remote_plan.get('rejected_item_count'),
                'item_acks': remote_plan.get('item_acks'),
            }
            authority_generation_before = (
                exec_states[0].get('authority_generation_before')
                if isinstance(exec_states[0].get('authority_generation_before'), dict)
                else None
            )
            activation_allowed = bool(
                integration.get('delegate_execution')
                and integration.get('config_status') == 'ready'
                and settings.signal_decider_next_open_enabled
            )
            persisted = await self._persist_dispatch_success_with_revision_retry(
                rows=group_rows,
                submitted_revision_fingerprints={
                    row_id: str(value)
                    for row_id, value in revision_fingerprints.items()
                },
                row_links=row_links,
                plan_id=plan_id,
                fingerprint=str(expected_plan_fingerprint),
                payload=payload,
                response_payload=candidate_response,
                trigger=f'{trigger}_write_ahead_recovery',
                authority_generation_before=(
                    authority_generation_before
                    if activation_allowed
                    else None
                ),
            )
            if activation_allowed:
                persisted, _ = await self._activate_core_confirmed_dispatch(
                    rows=group_rows,
                    plan_payload=payload,
                    response_payload=candidate_response,
                    persisted_group_result=persisted,
                    target_trade_date=target_trade_date,
                    account_reference=str(group_rows[0].account_reference),
                    trigger=f'{trigger}_write_ahead_recovery',
                )
            results.append({'plan_id': plan_id, **persisted})
            if (
                persisted.get('dispatch_status') == 'revocation_pending'
                and activation_allowed
            ):
                await self._flush_pending_exec_revocations(
                    target_trade_date=target_trade_date,
                    account_reference=str(group_rows[0].account_reference),
                    integration=integration,
                    trigger=f'{trigger}_write_ahead_recovery_revision_fence',
                )
        return results

    async def dispatch_next_open_plan(
        self,
        *,
        target_trade_date: str | date | None = None,
        account_reference: str | None = None,
        refresh_plan: bool = True,
        dry_run: bool | None = None,
        force: bool = False,
        trigger: str = 'manual',
    ) -> dict[str, Any]:
        integration = self.integration_status()
        resolved_account_reference = _normalize_optional_text(account_reference)
        resolved_dry_run = settings.signal_decider_next_open_dry_run if dry_run is None else dry_run
        if target_trade_date is None:
            default_target_date = date.fromisoformat((await async_get_market_state()).server_time[:10])
        else:
            default_target_date = _normalize_date(target_trade_date)
        resolved_target_date = await async_normalize_next_open_target_trade_date(
            target_trade_date,
            default=default_target_date,
            allow_fallback=True,
        )
        calendar_day = await async_get_krx_calendar_day(resolved_target_date, allow_fallback=True)
        source_market_date = await _async_previous_trading_day(resolved_target_date)

        execution_control_disabled = bool(
            integration['mode'] != INTEGRATION_MODE_DISPATCH
            or not settings.signal_decider_next_open_enabled
        )
        buy_authority_sync: dict[str, Any] = {
            'applies': False,
            'accounts': {},
            'errors': [],
        }
        buy_authority_sync_deferred = bool(
            not execution_control_disabled and refresh_plan
        )
        buy_authority_sync_completed = False

        async def synchronize_buy_authority_if_needed() -> None:
            nonlocal buy_authority_sync, buy_authority_sync_completed
            if execution_control_disabled or buy_authority_sync_completed:
                return
            buy_authority_sync = await self._synchronize_open_buy_authority(
                target_trade_date=resolved_target_date,
                account_reference=resolved_account_reference,
                integration=integration,
            )
            buy_authority_sync_completed = True

        reconciliation_buy_authority_sync = buy_authority_sync
        if not execution_control_disabled:
            if buy_authority_sync_deferred:
                # Preserve accepted-remote write-ahead recovery before the
                # planner can revise local rows, but never retry a missing
                # remote BUY against authority sampled before that refresh.
                reconciliation_buy_authority_sync = {
                    'applies': True,
                    'route_action': 'block_new_buy',
                    'route_status': 'plan_refresh_pending',
                    'source_observed_at': None,
                    'accounts': {},
                    'errors': [
                        'OPEN buy authority is deferred until the refreshed '
                        'plan has sampled current live market evidence.'
                    ],
                }
            else:
                await synchronize_buy_authority_if_needed()
                reconciliation_buy_authority_sync = buy_authority_sync

        try:
            await self._reconcile_pending_dispatch_write_ahead(
                target_trade_date=resolved_target_date,
                account_reference=resolved_account_reference,
                integration=integration,
                buy_authority_sync=reconciliation_buy_authority_sync,
                trigger=trigger,
            )
        except Exception:
            # Before this ordering fix, authority had already been synchronized
            # here. Preserve that fail-closed operational guarantee on an
            # unexpected pre-refresh reconciliation failure.
            await synchronize_buy_authority_if_needed()
            raise
        managed_delegated_authority = bool(
            execution_control_disabled
            and self.should_manage_delegated_authority(
                target_trade_date=resolved_target_date,
            )
        )
        if execution_control_disabled and (
            not resolved_dry_run
            or managed_delegated_authority
        ):
            if (
                managed_delegated_authority
                and _normalize_optional_text(integration.get('base_url'))
                and self._has_ambiguous_remote_acceptance(
                    target_trade_date=resolved_target_date,
                    account_reference=resolved_account_reference,
                )
            ):
                await self._refresh_exec_readback(
                    target_trade_date=resolved_target_date,
                    account_reference=resolved_account_reference,
                    symbol=None,
                    trigger=f'{trigger}_ambiguous_acceptance_reconciliation',
                    runtime_payload={
                        'status': 'reconciliation',
                        'payload': None,
                        'error': None,
                    },
                )
            await asyncio.to_thread(
                signal_decider_next_open_execution_service.stage_execution_control_revocations,
                target_trade_date=resolved_target_date,
                account_reference=resolved_account_reference,
            )
            revocation_results = await self._flush_pending_exec_revocations(
                target_trade_date=resolved_target_date,
                account_reference=resolved_account_reference,
                integration=integration,
                trigger=trigger,
            )
            plan_view = signal_decider_next_open_execution_service.list_plan_items(
                target_trade_date=resolved_target_date,
                account_reference=resolved_account_reference,
                refresh=False,
            )
            return {
                'request': {
                    'target_trade_date': resolved_target_date.isoformat(),
                    'account_reference': resolved_account_reference,
                    'refresh_plan': refresh_plan,
                    'dry_run': resolved_dry_run,
                    'force': force,
                    'trigger': trigger,
                },
                'integration': integration,
                'dispatch': {'groups': [], 'preview_plans': []},
                'summary': {
                    **(plan_view.get('summary') or {}),
                    'delegated_to_exec': True,
                    'triggered': False,
                    'reason': (
                        'execution_disabled'
                        if not settings.signal_decider_next_open_enabled
                        else 'execution_control_disabled'
                    ),
                    'dispatch_mode': integration['mode'],
                    'translated_item_count': 0,
                    'dispatched_group_count': 0,
                    'failed_group_count': sum(
                        result.get('status') not in {'revoked', 'manual_review_required'}
                        for result in revocation_results
                    ),
                    'skipped_group_count': 0,
                    'preview_group_count': 0,
                    'revoked_group_count': sum(
                        result.get('status') == 'revoked'
                        for result in revocation_results
                    ),
                    'revocation_manual_review_count': sum(
                        result.get('status') == 'manual_review_required'
                        for result in revocation_results
                    ),
                },
                'items': plan_view.get('items') or [],
            }

        if not force and not resolved_dry_run and _calendar_day_requires_operator_override(calendar_day):
            await synchronize_buy_authority_if_needed()
            plan_view = signal_decider_next_open_execution_service.list_plan_items(
                target_trade_date=resolved_target_date,
                account_reference=resolved_account_reference,
                refresh=False,
            )
            return {
                'request': {
                    'target_trade_date': resolved_target_date.isoformat(),
                    'account_reference': resolved_account_reference,
                    'refresh_plan': refresh_plan,
                    'dry_run': resolved_dry_run,
                    'force': force,
                    'trigger': trigger,
                },
                'integration': integration,
                'dispatch': {
                    'groups': [],
                    'preview_plans': [],
                },
                'summary': {
                    **(plan_view.get('summary') or {}),
                    'delegated_to_exec': True,
                    'triggered': False,
                    'reason': 'calendar_unavailable',
                    'dispatch_mode': integration['mode'],
                    'translated_item_count': 0,
                    'dispatched_group_count': 0,
                    'failed_group_count': 0,
                    'skipped_group_count': 0,
                    'preview_group_count': 0,
                    'calendar_source': calendar_day.calendar_source,
                    'calendar_source_status': calendar_day.calendar_source_status,
                    'calendar_source_error': calendar_day.calendar_source_error,
                },
                'items': plan_view.get('items') or [],
            }

        if not force and not settings.signal_decider_next_open_enabled and not resolved_dry_run:
            plan_view = signal_decider_next_open_execution_service.list_plan_items(
                target_trade_date=resolved_target_date,
                account_reference=resolved_account_reference,
                refresh=False,
            )
            return {
                'request': {
                    'target_trade_date': resolved_target_date.isoformat(),
                    'account_reference': resolved_account_reference,
                    'refresh_plan': refresh_plan,
                    'dry_run': resolved_dry_run,
                    'force': force,
                    'trigger': trigger,
                },
                'integration': integration,
                'dispatch': {
                    'groups': [],
                    'preview_plans': [],
                },
                'summary': {
                    **(plan_view.get('summary') or {}),
                    'delegated_to_exec': True,
                    'triggered': False,
                    'reason': 'execution_disabled',
                    'dispatch_mode': integration['mode'],
                    'translated_item_count': 0,
                    'dispatched_group_count': 0,
                    'failed_group_count': 0,
                    'skipped_group_count': 0,
                    'preview_group_count': 0,
                },
                'items': plan_view.get('items') or [],
            }

        enrichment_preflight: dict[str, Any] | None = None
        plan_refreshed_since_buy_authority_sync = False
        if refresh_plan:
            try:
                refresh_result = await asyncio.to_thread(
                    signal_decider_next_open_execution_service.refresh_plan_items,
                    target_trade_date=resolved_target_date,
                    trigger=f'{trigger}_plan_refresh',
                    force=force,
                    _live_market_snapshot_loop=asyncio.get_running_loop(),
                )
            except Exception:
                await synchronize_buy_authority_if_needed()
                raise
            plan_refreshed_since_buy_authority_sync = True
            enrichment_preflight = (
                refresh_result.get('enrichment_preflight')
                if isinstance(refresh_result, dict)
                and isinstance(refresh_result.get('enrichment_preflight'), dict)
                else None
            )
        elif force:
            enrichment_preflight = {
                'market_date': source_market_date.isoformat(),
                'status': 'force_bypassed',
                'allowed': True,
                'failed_categories': [],
                'category_row_counts': {},
            }
        else:
            enrichment_preflight = await asyncio.to_thread(
                signal_decider_next_open_execution_service.inspect_source_market_enrichment_preflight,
                source_market_date=source_market_date,
                trigger=f'{trigger}_dispatch',
            )

        try:
            revocation_results = await self._flush_pending_exec_revocations(
                target_trade_date=resolved_target_date,
                account_reference=resolved_account_reference,
                integration=integration,
                trigger=trigger,
            )
            if any(
                result.get('status') == 'revoked'
                and result.get('replacement_actionable')
                for result in revocation_results
            ):
                await asyncio.to_thread(
                    signal_decider_next_open_execution_service.refresh_plan_items,
                    target_trade_date=resolved_target_date,
                    trigger=f'{trigger}_revocation_ack_refresh',
                    force=force,
                    _live_market_snapshot_loop=asyncio.get_running_loop(),
                )
                plan_refreshed_since_buy_authority_sync = True
        except Exception:
            await synchronize_buy_authority_if_needed()
            raise

        if (
            not execution_control_disabled
            and plan_refreshed_since_buy_authority_sync
        ):
            # The planner may refresh the persisted live snapshot after several
            # minutes of source validation. Bind Exec to that final evidence,
            # not to the cache generation that existed before planning began.
            await synchronize_buy_authority_if_needed()
            await self._reconcile_pending_dispatch_write_ahead(
                target_trade_date=resolved_target_date,
                account_reference=resolved_account_reference,
                integration=integration,
                buy_authority_sync=buy_authority_sync,
                trigger=f'{trigger}_post_refresh_authority',
            )

        if enrichment_preflight is not None and not force and not enrichment_preflight.get('allowed'):
            existing_rows = self._load_next_open_rows(
                target_trade_date=resolved_target_date,
                account_reference=resolved_account_reference,
                include_payloads=True,
            )
            has_eligible_short_term_watch = any(
                _normalize_decision_agent_name(row.decision_agent_name) == SHORT_TERM_DECIDER_AGENT_NAME
                and self._row_is_dispatchable(row)
                for row in existing_rows
            )
            signal_decider_next_open_execution_service.block_existing_plan_rows_for_enrichment_preflight(
                target_trade_date=resolved_target_date,
                source_market_date=source_market_date,
                preflight=enrichment_preflight,
                decision_agent_names=(
                    [SIGNAL_DECIDER_AGENT_NAME]
                    if has_eligible_short_term_watch
                    else None
                ),
            )
            plan_view = signal_decider_next_open_execution_service.list_plan_items(
                target_trade_date=resolved_target_date,
                account_reference=resolved_account_reference,
                refresh=False,
            )
            if has_eligible_short_term_watch:
                enrichment_preflight = {
                    **enrichment_preflight,
                    'dispatch_note': (
                        'Global PyKRX enrichment coverage is partial; eligible short-term candidates passed '
                        'candidate-level enrichment checks and remain dispatchable for guarded monitoring.'
                    ),
                }
            else:
                return {
                    'request': {
                        'target_trade_date': resolved_target_date.isoformat(),
                        'account_reference': resolved_account_reference,
                        'refresh_plan': refresh_plan,
                        'dry_run': resolved_dry_run,
                        'force': force,
                        'trigger': trigger,
                    },
                    'integration': integration,
                    'buy_authority_sync': buy_authority_sync,
                    'dispatch': {
                        'groups': [],
                        'preview_plans': [],
                    },
                    'summary': {
                        **(plan_view.get('summary') or {}),
                        'delegated_to_exec': True,
                        'triggered': False,
                        'reason': 'pykrx_enrichment_preflight_blocked',
                        'dispatch_mode': integration['mode'],
                        'translated_item_count': 0,
                        'dispatched_group_count': 0,
                        'failed_group_count': 0,
                        'skipped_group_count': 0,
                        'preview_group_count': 0,
                        'enrichment_preflight_allowed': False,
                        'enrichment_preflight_status': enrichment_preflight.get('status'),
                        'enrichment_failed_categories': enrichment_preflight.get('failed_categories') or [],
                    },
                    'enrichment_preflight': enrichment_preflight,
                    'items': plan_view.get('items') or [],
                }

        if not resolved_dry_run:
            await self._reconcile_pending_core_activations(
                target_trade_date=resolved_target_date,
                account_reference=resolved_account_reference,
                integration=integration,
                buy_authority_sync=buy_authority_sync,
                trigger=trigger,
            )

        rows = self._load_next_open_rows(
            target_trade_date=resolved_target_date,
            account_reference=resolved_account_reference,
        )
        if self._fence_unproven_remote_acceptance(
            rows=rows,
            trigger=trigger,
        ):
            rows = self._load_next_open_rows(
                target_trade_date=resolved_target_date,
                account_reference=resolved_account_reference,
            )
        snapshots = self._load_snapshots(rows)
        grouped = self._group_rows_by_dispatch_scope(rows)

        group_results: list[dict[str, Any]] = []
        translated_item_count = 0
        dispatched_group_count = 0
        failed_group_count = 0
        skipped_group_count = 0
        preview_plans: list[dict[str, Any]] = []

        for (account_key, decision_agent_name, route_family), account_rows in grouped.items():
            eligible_rows = [row for row in account_rows if self._row_is_dispatchable(row)]
            if not eligible_rows:
                skipped_group_count += 1
                group_results.append(
                    {
                        'account_reference': account_key,
                        'decision_agent_name': decision_agent_name,
                        'dispatch_status': EXEC_DISPATCH_STATUS_SKIPPED,
                        'message': 'No eligible core plan rows were available for dispatch.',
                        'eligible_count': 0,
                    }
                )
                continue

            if (
                _dispatch_group_requires_core_krx_buy_authority(
                    route_family=route_family,
                    rows=eligible_rows,
                )
                and buy_authority_sync.get('applies') is True
            ):
                account_authority = (
                    buy_authority_sync.get('accounts', {}).get(account_key)
                    if isinstance(buy_authority_sync.get('accounts'), dict)
                    else None
                )
                if not isinstance(account_authority, dict) or not account_authority.get(
                    'dispatch_allowed'
                ):
                    skipped_group_count += 1
                    group_results.append(
                        {
                            'account_reference': account_key,
                            'decision_agent_name': decision_agent_name,
                            'dispatch_status': EXEC_DISPATCH_STATUS_SKIPPED,
                            'message': (
                                'Core OPEN buy authority was not acknowledged as '
                                'allowing this exact Exec account scope.'
                            ),
                            'reason': 'core_exec_buy_authority_not_acknowledged',
                            'eligible_count': len(eligible_rows),
                            'buy_authority': account_authority,
                            'buy_authority_errors': (
                                buy_authority_sync.get('errors') or []
                            ),
                        }
                    )
                    continue

            already_dispatched_rows, dispatch_rows = _partition_exec_dispatch_rows(
                eligible_rows,
                force=force,
            )
            if not dispatch_rows:
                plan_payload, _ = self._build_exec_plan_payload(
                    rows=eligible_rows,
                    snapshots=snapshots,
                    trigger=trigger,
                )
                translated_item_count += len(plan_payload['items'])
                skipped_group_count += 1
                group_results.append(
                    {
                        'account_reference': account_key,
                        'decision_agent_name': decision_agent_name,
                        'plan_id': _normalize_optional_text(plan_payload.get('plan_id')) or 'unknown-plan',
                        'dispatch_status': EXEC_DISPATCH_STATUS_ALREADY_DISPATCHED,
                        'eligible_count': len(eligible_rows),
                        'already_dispatched_count': len(already_dispatched_rows),
                        'dispatch_candidate_count': 0,
                        'translated_item_count': len(plan_payload['items']),
                    }
                )
                continue

            delta_batch_key = (
                _delta_dispatch_batch_key(dispatch_rows)
                if already_dispatched_rows
                else None
            )
            try:
                plan_payload, row_links = self._build_exec_plan_payload(
                    rows=dispatch_rows,
                    snapshots=snapshots,
                    trigger=trigger,
                    delta_batch_key=delta_batch_key,
                )
            except SignalDeciderExecPayloadContractError as exc:
                contract_message = str(exc)
                contract_reason_code = next(
                    (
                        reason_code
                        for reason_code in (
                            'unsupported_us_equity_exec_agent_sizing',
                            'unsupported_execution_route_for_v4',
                            LIVE_EXEC_ACCOUNT_TARGET_REQUIRED,
                        )
                        if reason_code in contract_message
                    ),
                    None,
                )
                unsupported_route_reason = (
                    contract_reason_code
                    if contract_reason_code
                    in {
                        'unsupported_us_equity_exec_agent_sizing',
                        'unsupported_execution_route_for_v4',
                    }
                    else None
                )
                if unsupported_route_reason is not None:
                    self._persist_unsupported_execution_route(
                        rows=dispatch_rows,
                        trigger=trigger,
                        reason_code=unsupported_route_reason,
                    )
                failed_group_count += 1
                group_results.append(
                    {
                        'account_reference': account_key,
                        'decision_agent_name': decision_agent_name,
                        'dispatch_status': EXEC_DISPATCH_STATUS_CONTRACT_ERROR,
                        'eligible_count': len(eligible_rows),
                        'already_dispatched_count': len(already_dispatched_rows),
                        'dispatch_candidate_count': len(dispatch_rows),
                        'translated_item_count': 0,
                        'error': _serialize_error(
                            code=(
                                'execution_handoff_route_unsupported'
                                if unsupported_route_reason is not None
                                else contract_reason_code
                                or 'delegated_gate_contract_invalid'
                            ),
                            message=contract_message,
                        ),
                    }
                )
                continue
            translated_item_count += len(plan_payload['items'])
            plan_id = _normalize_optional_text(plan_payload.get('plan_id')) or 'unknown-plan'
            fingerprint = _fingerprint_exec_plan(plan_payload)
            dispatch_revision_fingerprints = {
                int(row.id): _row_dispatch_revision_fingerprint(row)
                for row in dispatch_rows
            }
            authority_generation_before: dict[str, Any] | None = None
            if not resolved_dry_run:
                authority_generation_before = (
                    await self._inspect_dispatch_authority_generation(
                        target_trade_date=resolved_target_date,
                        rows=dispatch_rows,
                        revision_fingerprints=dispatch_revision_fingerprints,
                        retry_plan_refresh_conflict=True,
                    )
                )
                if authority_generation_before.get('status') != 'valid':
                    reason_code = _normalize_optional_text(
                        authority_generation_before.get('reason_code')
                    ) or 'current_live_market_evidence_unavailable'
                    fence_error: dict[str, Any] | None = None
                    generation_fence: dict[str, Any] | None = None
                    try:
                        generation_fence = await asyncio.to_thread(
                            signal_decider_next_open_execution_service.apply_dispatch_authority_generation_fence,
                            target_trade_date=resolved_target_date,
                            expected_row_bindings=_dispatch_authority_row_bindings(
                                dispatch_rows,
                                revision_fingerprints=dispatch_revision_fingerprints,
                            ),
                        )
                    except Exception as exc:  # noqa: BLE001 - no POST on uncertain authority
                        fence_error = _serialize_error(
                            code='dispatch_authority_fence_failed',
                            message=str(exc),
                            retryable=True,
                        )
                        if reason_code == 'current_live_market_evidence_unavailable':
                            await asyncio.to_thread(
                                signal_decider_next_open_execution_service.stage_market_evidence_unavailable_revocations,
                                target_trade_date=resolved_target_date,
                                account_reference=account_key,
                                failure_source=f'{trigger}_predispatch_generation_fence',
                            )
                    generation_revocations = await self._flush_pending_exec_revocations(
                        target_trade_date=resolved_target_date,
                        account_reference=account_key,
                        integration=integration,
                        trigger=f'{trigger}_predispatch_generation_fence',
                    )
                    revocation_results.extend(generation_revocations)
                    skipped_group_count += 1
                    group_results.append(
                        {
                            'account_reference': account_key,
                            'decision_agent_name': decision_agent_name,
                            'plan_id': plan_id,
                            'dispatch_status': EXEC_DISPATCH_STATUS_SKIPPED,
                            'eligible_count': len(eligible_rows),
                            'already_dispatched_count': len(already_dispatched_rows),
                            'dispatch_candidate_count': len(dispatch_rows),
                            'translated_item_count': len(plan_payload['items']),
                            'reason': reason_code,
                            'authority_generation': authority_generation_before,
                            'generation_fence': generation_fence,
                            'error': fence_error,
                        }
                    )
                    continue

                try:
                    plan_payload = _bind_core_confirmed_activation(
                        plan_payload,
                        authority_generation=authority_generation_before,
                    )
                except SignalDeciderExecPayloadContractError as exc:
                    failed_group_count += 1
                    group_results.append(
                        {
                            'account_reference': account_key,
                            'decision_agent_name': decision_agent_name,
                            'plan_id': plan_id,
                            'dispatch_status': EXEC_DISPATCH_STATUS_CONTRACT_ERROR,
                            'eligible_count': len(eligible_rows),
                            'already_dispatched_count': len(
                                already_dispatched_rows
                            ),
                            'dispatch_candidate_count': len(dispatch_rows),
                            'translated_item_count': len(plan_payload['items']),
                            'error': _serialize_error(
                                code='execution_activation_binding_invalid',
                                message=str(exc),
                            ),
                        }
                    )
                    continue
                fingerprint = _fingerprint_exec_plan(plan_payload)

            if not force and self._rows_match_existing_dispatch(dispatch_rows, plan_id=plan_id, fingerprint=fingerprint):
                skipped_group_count += 1
                group_results.append(
                    {
                        'account_reference': account_key,
                        'decision_agent_name': decision_agent_name,
                        'plan_id': plan_id,
                        'dispatch_status': EXEC_DISPATCH_STATUS_ALREADY_DISPATCHED,
                        'eligible_count': len(eligible_rows),
                        'already_dispatched_count': len(already_dispatched_rows),
                        'dispatch_candidate_count': len(dispatch_rows),
                        'translated_item_count': len(plan_payload['items']),
                    }
                )
                continue

            if resolved_dry_run:
                preview_plans.append(
                    {
                        'account_reference': account_key,
                        'decision_agent_name': decision_agent_name,
                        'plan_id': plan_id,
                        'translated_item_count': len(plan_payload['items']),
                        'payload': plan_payload,
                    }
                )
                group_results.append(
                    {
                        'account_reference': account_key,
                        'decision_agent_name': decision_agent_name,
                        'plan_id': plan_id,
                        'dispatch_status': EXEC_DISPATCH_STATUS_PREVIEW,
                        'eligible_count': len(eligible_rows),
                        'already_dispatched_count': len(already_dispatched_rows),
                        'dispatch_candidate_count': len(dispatch_rows),
                        'translated_item_count': len(plan_payload['items']),
                    }
                )
                continue

            if integration['config_status'] != 'ready':
                failed_group_count += 1
                self._persist_dispatch_failure(
                    rows=dispatch_rows,
                    submitted_revision_fingerprints=dispatch_revision_fingerprints,
                    row_links=row_links,
                    plan_id=plan_id,
                    fingerprint=fingerprint,
                    payload=plan_payload,
                    trigger=trigger,
                    status=EXEC_DISPATCH_STATUS_NOT_CONFIGURED,
                    error=integration['issues'][0] if integration['issues'] else _serialize_error(
                        code='not_configured',
                        message='Exec integration is not configured.',
                    ),
                )
                group_results.append(
                    {
                        'account_reference': account_key,
                        'decision_agent_name': decision_agent_name,
                        'plan_id': plan_id,
                        'dispatch_status': EXEC_DISPATCH_STATUS_NOT_CONFIGURED,
                        'eligible_count': len(eligible_rows),
                        'already_dispatched_count': len(already_dispatched_rows),
                        'dispatch_candidate_count': len(dispatch_rows),
                        'translated_item_count': len(plan_payload['items']),
                        'error': integration['issues'][0] if integration['issues'] else _serialize_error(
                            code='not_configured',
                            message='Exec integration is not configured.',
                        ),
                    }
                )
                continue

            try:
                destination_binding = self._destination_binding_for_payload(
                    plan_payload
                )
                self._persist_dispatch_write_ahead(
                    rows=dispatch_rows,
                    submitted_revision_fingerprints=dispatch_revision_fingerprints,
                    row_links=row_links,
                    plan_id=plan_id,
                    fingerprint=fingerprint,
                    payload=plan_payload,
                    trigger=trigger,
                    destination_binding=destination_binding,
                    authority_generation_before=authority_generation_before,
                )
            except ValueError as exc:
                failed_group_count += 1
                group_results.append(
                    {
                        'account_reference': account_key,
                        'decision_agent_name': decision_agent_name,
                        'plan_id': plan_id,
                        'dispatch_status': EXEC_DISPATCH_STATUS_CONTRACT_ERROR,
                        'eligible_count': len(eligible_rows),
                        'already_dispatched_count': len(already_dispatched_rows),
                        'dispatch_candidate_count': len(dispatch_rows),
                        'translated_item_count': len(plan_payload['items']),
                        'error': _serialize_error(
                            code='dispatch_write_ahead_failed',
                            message=str(exc),
                            retryable=True,
                        ),
                    }
                )
                continue

            try:
                response_payload = await self.client.post_execution_plan(plan_payload)
            except SignalDeciderExecClientError as exc:
                recoverable_dispatch_error = bool(
                    exc.status_code == 409
                    or exc.retryable
                    or exc.code == EXEC_DISPATCH_STATUS_UNAVAILABLE
                )
                recovery_status = 'not_attempted'
                if recoverable_dispatch_error:
                    (
                        response_payload,
                        recovery_status,
                    ) = await self._recover_duplicate_dispatch_response(
                        plan_id=plan_id,
                        row_links=row_links,
                        expected_dispatch_material_fingerprint=(
                            plan_payload['metadata'][
                                'dispatch_material_fingerprint'
                            ]
                        ),
                    )
                    if response_payload is not None:
                        recovered_revision_fingerprints = (
                            _recovered_dispatch_revision_fingerprints(
                                dispatch_rows,
                                plan_id=plan_id,
                                plan_fingerprint=fingerprint,
                                current_revision_fingerprints=(
                                    dispatch_revision_fingerprints
                                ),
                            )
                        )
                        persisted_group_result = (
                            await self._persist_dispatch_success_with_revision_retry(
                                rows=dispatch_rows,
                                submitted_revision_fingerprints=(
                                    recovered_revision_fingerprints
                                ),
                                row_links=row_links,
                                plan_id=plan_id,
                                fingerprint=fingerprint,
                                payload=plan_payload,
                                response_payload=response_payload,
                                trigger=trigger,
                                authority_generation_before=(
                                    authority_generation_before
                                ),
                            )
                        )
                        (
                            persisted_group_result,
                            activation_revocations,
                        ) = await self._activate_core_confirmed_dispatch(
                            rows=dispatch_rows,
                            plan_payload=plan_payload,
                            response_payload=response_payload,
                            persisted_group_result=persisted_group_result,
                            target_trade_date=resolved_target_date,
                            account_reference=account_key,
                            trigger=trigger,
                        )
                        revocation_results.extend(activation_revocations)
                        if persisted_group_result['dispatch_status'] == 'revocation_pending':
                            post_ack_revocations = await self._flush_pending_exec_revocations(
                                target_trade_date=resolved_target_date,
                                account_reference=account_key,
                                integration=integration,
                                trigger=f'{trigger}_revision_fence',
                            )
                            revocation_results.extend(post_ack_revocations)
                        if persisted_group_result['dispatch_status'] == EXEC_DISPATCH_STATUS_CONTRACT_ERROR:
                            failed_group_count += 1
                        elif persisted_group_result['dispatch_status'] in {
                            'revocation_pending',
                            EXEC_DISPATCH_STATUS_PENDING_REMOTE_ACK,
                        }:
                            skipped_group_count += 1
                        else:
                            dispatched_group_count += 1
                        group_results.append(
                            {
                                'account_reference': account_key,
                                'decision_agent_name': decision_agent_name,
                                'plan_id': plan_id,
                                'already_dispatched_count': len(already_dispatched_rows),
                                'dispatch_candidate_count': len(dispatch_rows),
                                'recovered_from_duplicate': exc.status_code == 409,
                                'recovered_from_retryable_error': exc.status_code != 409,
                                'recovery_error': _serialize_error(
                                    code=exc.code,
                                    message=str(exc),
                                    retryable=exc.retryable,
                                    status_code=exc.status_code,
                                ),
                                **persisted_group_result,
                            }
                        )
                        continue
                if (
                    exc.retryable
                    and recovery_status in {'not_found', 'unavailable'}
                ):
                    pending_error = _serialize_error(
                        code=exc.code,
                        message=str(exc),
                        retryable=exc.retryable,
                        status_code=exc.status_code,
                    )
                    preserved_write_ahead = self._persist_dispatch_failure(
                        rows=dispatch_rows,
                        submitted_revision_fingerprints=(
                            dispatch_revision_fingerprints
                        ),
                        row_links=row_links,
                        plan_id=plan_id,
                        fingerprint=fingerprint,
                        payload=plan_payload,
                        trigger=trigger,
                        status=self._client_error_status(exc),
                        error=pending_error,
                        remote_acceptance_ambiguous=True,
                        preserve_write_ahead=True,
                    )
                    if preserved_write_ahead:
                        skipped_group_count += 1
                    else:
                        failed_group_count += 1
                    group_results.append(
                        {
                            'account_reference': account_key,
                            'decision_agent_name': decision_agent_name,
                            'plan_id': plan_id,
                            'dispatch_status': (
                                EXEC_DISPATCH_STATUS_PENDING_REMOTE_ACK
                                if preserved_write_ahead
                                else self._client_error_status(exc)
                            ),
                            'eligible_count': len(eligible_rows),
                            'already_dispatched_count': len(
                                already_dispatched_rows
                            ),
                            'dispatch_candidate_count': len(dispatch_rows),
                            'translated_item_count': len(
                                plan_payload['items']
                            ),
                            'reconciliation_status': recovery_status,
                            'error': pending_error,
                        }
                    )
                    continue
                status = self._client_error_status(exc)
                failed_group_count += 1
                self._persist_dispatch_failure(
                    rows=dispatch_rows,
                    submitted_revision_fingerprints=dispatch_revision_fingerprints,
                    row_links=row_links,
                    plan_id=plan_id,
                    fingerprint=fingerprint,
                    payload=plan_payload,
                    trigger=trigger,
                    status=status,
                    error=_serialize_error(
                        code=exc.code,
                        message=str(exc),
                        retryable=exc.retryable,
                        status_code=exc.status_code,
                    ),
                    remote_acceptance_ambiguous=True,
                )
                group_results.append(
                    {
                        'account_reference': account_key,
                        'decision_agent_name': decision_agent_name,
                        'plan_id': plan_id,
                        'dispatch_status': status,
                        'eligible_count': len(eligible_rows),
                        'already_dispatched_count': len(already_dispatched_rows),
                        'dispatch_candidate_count': len(dispatch_rows),
                        'translated_item_count': len(plan_payload['items']),
                        'error': _serialize_error(
                            code=exc.code,
                            message=str(exc),
                            retryable=exc.retryable,
                            status_code=exc.status_code,
                        ),
                    }
                )
                continue

            persisted_group_result = (
                await self._persist_dispatch_success_with_revision_retry(
                    rows=dispatch_rows,
                    submitted_revision_fingerprints=dispatch_revision_fingerprints,
                    row_links=row_links,
                    plan_id=plan_id,
                    fingerprint=fingerprint,
                    payload=plan_payload,
                    response_payload=response_payload,
                    trigger=trigger,
                    authority_generation_before=authority_generation_before,
                )
            )
            (
                persisted_group_result,
                activation_revocations,
            ) = await self._activate_core_confirmed_dispatch(
                rows=dispatch_rows,
                plan_payload=plan_payload,
                response_payload=response_payload,
                persisted_group_result=persisted_group_result,
                target_trade_date=resolved_target_date,
                account_reference=account_key,
                trigger=trigger,
            )
            revocation_results.extend(activation_revocations)
            if persisted_group_result['dispatch_status'] == 'revocation_pending':
                post_ack_revocations = await self._flush_pending_exec_revocations(
                    target_trade_date=resolved_target_date,
                    account_reference=account_key,
                    integration=integration,
                    trigger=f'{trigger}_revision_fence',
                )
                revocation_results.extend(post_ack_revocations)
            if persisted_group_result['dispatch_status'] == EXEC_DISPATCH_STATUS_CONTRACT_ERROR:
                failed_group_count += 1
            elif persisted_group_result['dispatch_status'] in {
                'revocation_pending',
                EXEC_DISPATCH_STATUS_PENDING_REMOTE_ACK,
            }:
                skipped_group_count += 1
            else:
                dispatched_group_count += 1
            group_results.append(
                {
                    'account_reference': account_key,
                    'decision_agent_name': decision_agent_name,
                    'plan_id': plan_id,
                    'already_dispatched_count': len(already_dispatched_rows),
                    'dispatch_candidate_count': len(dispatch_rows),
                    **persisted_group_result,
                }
            )

        plan_view = await asyncio.to_thread(
            signal_decider_next_open_execution_service.list_plan_items,
            target_trade_date=resolved_target_date,
            account_reference=resolved_account_reference,
            refresh=False,
        )

        summary = {
            **(plan_view.get('summary') or {}),
            'delegated_to_exec': True,
            'triggered': bool(dispatched_group_count),
            'dispatch_mode': integration['mode'],
            'translated_item_count': translated_item_count,
            'dispatched_group_count': dispatched_group_count,
            'failed_group_count': failed_group_count,
            'skipped_group_count': skipped_group_count,
            'preview_group_count': len(preview_plans),
            'revoked_group_count': sum(
                result.get('status') == 'revoked' for result in revocation_results
            ),
            'revocation_manual_review_count': sum(
                result.get('status') == 'manual_review_required'
                for result in revocation_results
            ),
            'enrichment_preflight_allowed': (
                bool(enrichment_preflight.get('allowed')) if enrichment_preflight else None
            ),
            'enrichment_preflight_status': (
                enrichment_preflight.get('status') if enrichment_preflight else None
            ),
            'enrichment_failed_categories': (
                enrichment_preflight.get('failed_categories') if enrichment_preflight else []
            ),
        }
        return {
            'request': {
                'target_trade_date': resolved_target_date.isoformat(),
                'account_reference': resolved_account_reference,
                'refresh_plan': refresh_plan,
                'dry_run': resolved_dry_run,
                'force': force,
                'trigger': trigger,
            },
            'integration': integration,
            'buy_authority_sync': buy_authority_sync,
            'dispatch': {
                'groups': group_results,
                'preview_plans': preview_plans,
            },
            'summary': summary,
            'enrichment_preflight': enrichment_preflight,
            'items': plan_view.get('items') or [],
        }

    async def _flush_pending_exec_revocations(
        self,
        *,
        target_trade_date: date,
        account_reference: str | None,
        integration: dict[str, Any],
        trigger: str,
    ) -> list[dict[str, Any]]:
        rows = self._load_next_open_rows(
            target_trade_date=target_trade_date,
            account_reference=account_reference,
            include_payloads=True,
        )
        results: list[dict[str, Any]] = []
        for row in rows:
            if row.execution_status != 'revocation_pending':
                continue
            submission = row.submission_payload if isinstance(row.submission_payload, dict) else {}
            revocation = submission.get('revocation') if isinstance(submission.get('revocation'), dict) else {}
            item_links = revocation.get('item_links') if isinstance(revocation.get('item_links'), list) else []
            expected_destination = _validated_exec_destination_binding(
                revocation.get('destination_binding')
            )
            exec_dispatch = (
                submission.get('exec_dispatch')
                if isinstance(submission.get('exec_dispatch'), dict)
                else {}
            )
            accepted_destination = (
                _exec_dispatch_destination_binding(exec_dispatch)
                if exec_dispatch
                else None
            )
            try:
                active_destination = self._active_destination_binding(
                    account_reference=str(row.account_reference),
                    integration=integration,
                )
            except ValueError:
                active_destination = None
            destination_reason_code: str | None = None
            if expected_destination is None:
                destination_reason_code = 'exec_destination_binding_missing'
            elif accepted_destination is not None and (
                accepted_destination != expected_destination
            ):
                destination_reason_code = 'exec_destination_binding_mismatch'
            elif active_destination is not None and (
                active_destination != expected_destination
            ):
                destination_reason_code = 'exec_destination_mismatch'
            if destination_reason_code is not None:
                self._mark_exec_destination_manual_review(
                    row_id=int(row.id),
                    trigger=trigger,
                    reason_code=destination_reason_code,
                    expected_destination=expected_destination,
                    active_destination=active_destination,
                )
                results.append(
                    {
                        'row_id': row.id,
                        'status': 'manual_review_required',
                        'reason_code': destination_reason_code,
                        'trigger': trigger,
                    }
                )
                continue
            if active_destination is None:
                results.append({'row_id': row.id, 'status': 'not_configured'})
                continue
            try:
                replacement_action = revocation.get('replacement_action')
                if (
                    revocation.get('reason_code')
                    == 'current_live_market_evidence_unavailable'
                    and str(
                        revocation.get('replacement_observation_id') or ''
                    ).startswith(
                        'observation:current_live_market_evidence_unavailable:'
                    )
                ):
                    # Repair revocations created before pending contracts were
                    # made immutable. Market-evidence loss always revokes to
                    # reject; a later actionable planner generation may have
                    # overwritten this field while the ACK was outstanding.
                    replacement_action = 'reject'
                request_payload = serialize_execution_revocation_payload(
                    {
                        'schema_version': EXECUTION_REVOCATION_SCHEMA_VERSION,
                        'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
                        'required_capability': EXECUTION_REVOCATION_CAPABILITY,
                        'plan_id': revocation.get('plan_id'),
                        'requested_at': revocation.get('requested_at') or _utcnow().isoformat(),
                        'reason_code': revocation.get('reason_code'),
                        'replacement_decision_snapshot_id': revocation.get(
                            'replacement_decision_snapshot_id'
                        ),
                        'replacement_action': replacement_action,
                        'items': [
                            {
                                'core_item_key': link.get('core_item_key'),
                                'execution_plan_item_id': link.get('exec_plan_item_id'),
                                'decision_snapshot_id': revocation.get('prior_decision_snapshot_id'),
                            }
                            for link in item_links
                        ],
                    }
                )
            except ValueError as exc:
                results.append(
                    {
                        'row_id': row.id,
                        'status': 'contract_error',
                        'error': _serialize_error(
                            code='execution_revocation_request_invalid',
                            message=str(exc),
                        ),
                    }
                )
                continue
            if not _normalize_optional_text(integration.get('base_url')):
                results.append({'row_id': row.id, 'status': 'not_configured'})
                continue
            try:
                response_payload = await self.client.post_execution_plan_revocation(request_payload)
            except SignalDeciderExecClientError as exc:
                if exc.status_code == 409 and isinstance(exc.payload, dict):
                    response_payload = exc.payload
                else:
                    results.append(
                        {
                            'row_id': row.id,
                            'status': exc.code,
                            'error': _serialize_error(
                                code=exc.code,
                                message=str(exc),
                                retryable=exc.retryable,
                                status_code=exc.status_code,
                            ),
                        }
                    )
                    continue
            try:
                ack_status, _ = validate_execution_revocation_ack(
                    response_payload,
                    plan_id=request_payload['plan_id'],
                    submitted_core_item_keys=[
                        item['core_item_key'] for item in request_payload['items']
                    ],
                    expected_reason_code=request_payload['reason_code'],
                    submitted_items=item_links,
                )
                if ack_status == 'not_found':
                    ack_status = 'manual_review_required'
                signal_decider_next_open_execution_service.apply_exec_revocation_ack(
                    row_id=int(row.id),
                    request_payload=request_payload,
                    response_payload=response_payload,
                    ack_status=ack_status,
                )
            except ValueError as exc:
                results.append(
                    {
                        'row_id': row.id,
                        'status': 'contract_error',
                        'error': _serialize_error(
                            code='execution_revocation_ack_invalid',
                            message=str(exc),
                        ),
                    }
                )
                continue
            results.append(
                {
                    'row_id': row.id,
                    'status': ack_status,
                    'replacement_actionable': bool(
                        revocation.get('replacement_actionable')
                    ),
                    'trigger': trigger,
                }
            )
        return results

    async def _recover_duplicate_dispatch_response(
        self,
        *,
        plan_id: str,
        row_links: dict[int, list[dict[str, Any]]],
        expected_dispatch_material_fingerprint: str,
    ) -> tuple[dict[str, Any] | None, str]:
        try:
            readback = await self.client.list_execution_plans(external_plan_id=plan_id)
        except SignalDeciderExecClientError:
            return None, 'unavailable'
        plans = _collect_items(readback)
        plan = next(
            (
                item
                for item in plans
                if _normalize_optional_text(item.get('plan_id') or item.get('external_plan_id')) == plan_id
            ),
            None,
        )
        if not isinstance(plan, dict):
            return (None, 'not_found') if not plans else (None, 'conflict')
        if _normalize_status(plan.get('status')) != EXEC_DISPATCH_STATUS_ACCEPTED:
            return None, 'conflict'
        submitted_core_keys: list[str] = []
        for links in row_links.values():
            for link in links:
                core_key = _normalize_optional_text(link.get('core_item_key'))
                if core_key:
                    submitted_core_keys.append(core_key)
        candidate = {
            'plan_id': plan.get('plan_id'),
            'execution_plan_id': plan.get('execution_plan_id'),
            'accepted_item_count': plan.get('accepted_item_count'),
            'rejected_item_count': plan.get('rejected_item_count'),
            'contract_ack': plan.get('contract_ack'),
            'item_acks': plan.get('item_acks'),
        }
        try:
            validate_execution_plan_dispatch_ack(
                candidate,
                plan_id=plan_id,
                submitted_core_item_keys=submitted_core_keys,
                expected_dispatch_material_fingerprint=(
                    expected_dispatch_material_fingerprint
                ),
            )
        except ValueError:
            return None, 'conflict'
        return candidate, 'accepted'

    async def list_signal_decider_exec_overview(
        self,
        *,
        target_trade_date: str | date | None = None,
        account_reference: str | None = None,
        symbol: str | None = None,
        refresh: bool = False,
        trigger: str = 'manual',
    ) -> dict[str, Any]:
        integration = self.integration_status()
        resolved_target_date = _normalize_date(target_trade_date)
        resolved_account_reference = _normalize_optional_text(account_reference)
        resolved_symbol = _normalize_optional_text(symbol, lowercase=False)

        runtime_result = await self.runtime_status(refresh=refresh)
        if refresh and integration['enabled'] and integration['config_status'] == 'ready':
            await self._refresh_exec_readback(
                target_trade_date=resolved_target_date,
                account_reference=resolved_account_reference,
                symbol=resolved_symbol,
                trigger=trigger,
                runtime_payload=runtime_result['runtime'],
            )

        plan_view = await asyncio.to_thread(
            signal_decider_next_open_execution_service.list_plan_items,
            target_trade_date=resolved_target_date,
            account_reference=resolved_account_reference,
            refresh=False,
        )
        overview_rows = await asyncio.to_thread(
            self._load_next_open_rows,
            target_trade_date=resolved_target_date,
            account_reference=resolved_account_reference,
            symbol=resolved_symbol,
            include_payloads=True,
        )
        base_items = [self._serialize_execution_plan_row_for_runtime(row) for row in overview_rows]
        if resolved_symbol:
            base_items = [item for item in base_items if _normalize_optional_text(item.get('symbol')) == resolved_symbol]
        items = [self._augment_exec_overview_item(item) for item in base_items]
        summary = self._build_exec_overview_summary(items=items, plan_summary=plan_view.get('summary') or {})
        return {
            'request': {
                'target_trade_date': resolved_target_date.isoformat(),
                'account_reference': resolved_account_reference,
                'symbol': resolved_symbol,
                'refresh': refresh,
            },
            'integration': runtime_result['integration'],
            'runtime': runtime_result['runtime'],
            'summary': summary,
            'items': items,
        }

    async def list_intraday_exec_watch(
        self,
        *,
        account_reference: str | None = None,
        symbol: str | None = None,
        refresh: bool = False,
        trigger: str = 'manual',
    ) -> dict[str, Any]:
        overview = await self.list_signal_decider_exec_overview(
            account_reference=account_reference,
            symbol=symbol,
            refresh=refresh,
            trigger=trigger,
        )
        items = [
            self._build_intraday_watch_item(item)
            for item in overview['items']
            if _normalize_decision_agent_name(item.get('decision_agent_name'))
            in {SIGNAL_DECIDER_AGENT_NAME, SHORT_TERM_DECIDER_AGENT_NAME}
        ]
        summary = {
            'delegated_to_exec': True,
            'items_total': len(items),
            'ready_count': sum(1 for item in items if item.get('remote_state', {}).get('derived_status') == 'open'),
            'blocked_count': sum(1 for item in items if item.get('eligibility_status') == ELIGIBILITY_BLOCKED),
            'submitted_count': sum(
                1
                for item in items
                if _normalize_status((item.get('dispatch') or {}).get('status'))
                in {EXEC_DISPATCH_STATUS_ACCEPTED, EXEC_DISPATCH_STATUS_PARTIAL}
            ),
            'closed_count': sum(
                1 for item in items if item.get('remote_state', {}).get('derived_status') == 'closed'
            ),
        }
        return {
            'request': {
                'account_reference': _normalize_optional_text(account_reference),
                'symbol': _normalize_optional_text(symbol),
                'refresh': refresh,
            },
            'integration': overview['integration'],
            'runtime': overview['runtime'],
            'summary': summary,
            'items': items,
        }

    def schedule_signal_decider_exec_readback_refresh(
        self,
        *,
        target_trade_date: str | date | None = None,
        account_reference: str | None = None,
        symbol: str | None = None,
        trigger: str = 'manual',
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        resolved_target_date = _normalize_date(target_trade_date)
        resolved_account_reference = _normalize_optional_text(account_reference)
        resolved_symbol = _normalize_optional_text(symbol, lowercase=False)
        refresh_state = self._ensure_dashboard_background_refresh(
            target_trade_date=resolved_target_date,
            account_reference=resolved_account_reference,
            symbol=resolved_symbol,
            refresh=True,
            trigger=trigger,
            timeout_seconds=timeout_seconds,
        )
        return {
            'request': {
                'target_trade_date': resolved_target_date.isoformat(),
                'account_reference': resolved_account_reference,
                'symbol': resolved_symbol,
                'refresh': True,
                'trigger': trigger,
                'timeout_seconds': refresh_state.get('timeout_seconds'),
            },
            'integration': self.integration_status(),
            'refresh': refresh_state,
        }

    async def dashboard_runtime_status(self, *, refresh: bool = True) -> dict[str, Any]:
        runtime_result = await self.runtime_status(refresh=refresh)
        return self._build_dashboard_runtime_status(
            integration=runtime_result['integration'],
            runtime=runtime_result['runtime'],
        )

    async def get_runtime_daily_board(
        self,
        *,
        market_date: str | date | None = None,
        refresh: bool = True,
        trigger: str = 'manual',
    ) -> dict[str, Any]:
        resolved_market_date = _normalize_date(market_date)
        refresh_state = self._ensure_dashboard_background_refresh(
            target_trade_date=resolved_market_date,
            account_reference=None,
            symbol=None,
            refresh=refresh,
            trigger=trigger,
        )
        cached = self._load_dashboard_cached_payload(
            endpoint=DASHBOARD_ENDPOINT_DAILY_BOARD,
            market_date=resolved_market_date,
            symbol=None,
            refresh_state=refresh_state,
            allow_stale=True,
        )
        if cached is not None:
            return cached

        return await self._build_dashboard_payload_singleflight(
            endpoint=DASHBOARD_ENDPOINT_DAILY_BOARD,
            market_date=resolved_market_date,
            symbol=None,
            refresh_state=refresh_state,
            build=lambda: self._build_and_cache_runtime_daily_board(
                market_date=resolved_market_date,
                refresh=refresh,
                refresh_state=refresh_state,
                trigger=trigger,
            ),
        )

    async def list_runtime_monitored_symbols(
        self,
        *,
        market_date: str | date | None = None,
        refresh: bool = True,
        trigger: str = 'manual',
        publish_realtime_observation: bool = True,
    ) -> dict[str, Any]:
        resolved_market_date = _normalize_date(market_date)
        should_publish_realtime_observation = bool(
            publish_realtime_observation
            and resolved_market_date
            == _utcnow().astimezone(MARKET_TIME_ZONE).date()
        )
        observation_revision = (
            kis_realtime_price_service.observation_revision()
            if should_publish_realtime_observation
            else None
        )
        refresh_state = self._ensure_dashboard_background_refresh(
            target_trade_date=resolved_market_date,
            account_reference=None,
            symbol=None,
            refresh=refresh,
            trigger=trigger,
        )
        cached = self._load_dashboard_cached_payload(
            endpoint=DASHBOARD_ENDPOINT_MONITORED_SYMBOLS,
            market_date=resolved_market_date,
            symbol=None,
            refresh_state=refresh_state,
            allow_stale=True,
        )
        if cached is not None:
            payload = cached
        else:
            payload = await self._build_dashboard_payload_singleflight(
                endpoint=DASHBOARD_ENDPOINT_MONITORED_SYMBOLS,
                market_date=resolved_market_date,
                symbol=None,
                refresh_state=refresh_state,
                build=lambda: self._build_and_cache_runtime_monitored_symbols(
                    market_date=resolved_market_date,
                    refresh=refresh,
                    refresh_state=refresh_state,
                    trigger=trigger,
                ),
            )
        if should_publish_realtime_observation:
            try:
                kis_realtime_price_service.observe_runtime_monitored_payload(
                    payload,
                    trigger=trigger,
                    expected_observation_revision=observation_revision,
                )
            except Exception:
                pass
        return payload

    async def load_realtime_monitored_symbol_source(
        self,
        *,
        market_date: str | date | None = None,
        trigger: str,
    ) -> KISRealtimeMonitoredSymbolSource:
        """Seed realtime subscriptions without waiting for a dashboard build."""
        resolved_market_date = _normalize_date(market_date)
        refresh_key = self._dashboard_refresh_key(
            target_trade_date=resolved_market_date,
            account_reference=None,
            symbol=None,
        )
        cache_refresh_state = self._dashboard_refresh_payload(
            refresh_key,
            requested=False,
        )
        cached = self._load_dashboard_cached_payload(
            endpoint=DASHBOARD_ENDPOINT_MONITORED_SYMBOLS,
            market_date=resolved_market_date,
            symbol=None,
            refresh_state=cache_refresh_state,
            allow_stale=True,
        )
        cached_source: KISRealtimeMonitoredSymbolSource | None = None
        if cached is not None:
            cached_source = (
                kis_realtime_price_service._runtime_monitored_source_from_payload(
                    cached
                )
            )
        now = _utcnow().astimezone(timezone.utc)
        if cached_source is not None and (
            cached_source.market_date != resolved_market_date.isoformat()
            or cached_source.source_version is None
            or cached_source.source_version
            > now + EXEC_RUNTIME_MONITORED_MEMBERSHIP_MAX_FUTURE_SKEW
        ):
            cached_source = None
        portfolio_membership_task = (
            self._start_realtime_portfolio_membership_refresh(
                market_date=resolved_market_date,
                trigger=trigger,
            )
        )
        exec_membership_task = self._start_realtime_exec_membership_refresh(
            market_date=resolved_market_date,
            trigger=trigger,
        )
        # Give cache-backed membership reads one event-loop turn to complete,
        # without putting a usable exact dashboard snapshot behind live I/O.
        await asyncio.sleep(0)
        if cached_source is not None and cached_source.membership_complete:
            holding_membership = (
                self._take_completed_realtime_portfolio_membership(
                    market_date=resolved_market_date,
                    task=portfolio_membership_task,
                )
            )
        else:
            holding_membership = (
                await self._wait_for_realtime_portfolio_membership(
                    market_date=resolved_market_date,
                    task=portfolio_membership_task,
                )
            )
        holding_symbols = sorted(
            holding_membership.symbols
            if holding_membership is not None
            else ()
        )
        exec_membership = self._take_completed_realtime_exec_membership(
            market_date=resolved_market_date,
            task=exec_membership_task,
        )
        exec_symbols = sorted(
            exec_membership.symbols
            if exec_membership is not None
            else ()
        )
        cached_symbols = cached_source.symbols if cached_source else ()
        cached_critical = set(
            cached_source.critical_protected_symbols
            if cached_source
            else ()
        )
        cached_protected = set(
            cached_source.protected_symbols if cached_source else ()
        )
        critical_symbols = set(holding_symbols) | cached_critical
        protected_symbols = (
            critical_symbols | set(exec_symbols) | cached_protected
        )
        cached_authority_is_current = bool(
            cached_source is not None
            and cached_source.membership_complete
            and holding_membership is not None
            and holding_membership.membership_complete
            and holding_membership.source_version is not None
            and exec_membership is not None
            and exec_membership.membership_complete
            and exec_membership.source_version is not None
            and set(holding_symbols).issubset(cached_critical)
            and set(exec_symbols).issubset(set(cached_symbols))
            and holding_membership.source_version
            <= cached_source.source_version
            and exec_membership.source_version
            <= cached_source.source_version
        )
        ordered_symbols = list(
            dict.fromkeys(
                [
                    *holding_symbols,
                    *(
                        symbol
                        for symbol in cached_symbols
                        if symbol in cached_critical
                    ),
                    *exec_symbols,
                    *(
                        symbol
                        for symbol in cached_symbols
                        if symbol in cached_protected
                        and symbol not in critical_symbols
                    ),
                    *cached_symbols,
                ]
            )
        )
        source_versions = [
            value
            for value in (
                cached_source.source_version if cached_source else None,
                holding_membership.source_version
                if holding_membership is not None
                else None,
                exec_membership.source_version
                if exec_membership is not None
                else None,
            )
            if value is not None
        ]
        self._ensure_dashboard_background_refresh(
            target_trade_date=resolved_market_date,
            account_reference=None,
            symbol=None,
            refresh=True,
            trigger=trigger,
        )
        return KISRealtimeMonitoredSymbolSource(
            symbols=tuple(ordered_symbols),
            market_date=resolved_market_date.isoformat(),
            source_version=max(source_versions) if source_versions else None,
            # New broker facts are merge-only until a complete dashboard cache
            # includes them. An unchanged complete cache may retain its exact
            # removal authority.
            membership_complete=cached_authority_is_current,
            protected_symbols=tuple(
                symbol
                for symbol in ordered_symbols
                if symbol in protected_symbols
            ),
            critical_protected_symbols=tuple(
                symbol
                for symbol in ordered_symbols
                if symbol in critical_symbols
            ),
        )

    def _start_realtime_portfolio_membership_refresh(
        self,
        *,
        market_date: date,
        trigger: str,
    ) -> asyncio.Task[PortfolioHoldingSymbolMembership | None]:
        task = self._realtime_portfolio_membership_tasks.get(market_date)
        if task is not None:
            return task
        task = asyncio.create_task(
            self._load_live_portfolio_holding_membership(
                market_date=market_date,
                trigger=trigger,
            ),
            name=(
                'portfolio-realtime-membership-refresh:'
                f'{market_date.isoformat()}'
            ),
        )
        self._realtime_portfolio_membership_tasks[market_date] = task
        return task

    def _take_completed_realtime_portfolio_membership(
        self,
        *,
        market_date: date,
        task: asyncio.Task[PortfolioHoldingSymbolMembership | None],
    ) -> PortfolioHoldingSymbolMembership | None:
        if not task.done():
            return self._degraded_cached_portfolio_holding_membership(
                market_date
            )
        if self._realtime_portfolio_membership_tasks.get(market_date) is task:
            self._realtime_portfolio_membership_tasks.pop(
                market_date,
                None,
            )
        try:
            return task.result()
        except (asyncio.CancelledError, Exception):
            return self._degraded_cached_portfolio_holding_membership(
                market_date
            )

    async def _wait_for_realtime_portfolio_membership(
        self,
        *,
        market_date: date,
        task: asyncio.Task[PortfolioHoldingSymbolMembership | None],
    ) -> PortfolioHoldingSymbolMembership | None:
        try:
            await asyncio.wait_for(
                asyncio.shield(task),
                timeout=REALTIME_MONITORED_PORTFOLIO_WAIT_SECONDS,
            )
        except TimeoutError:
            return self._degraded_cached_portfolio_holding_membership(
                market_date
            )
        except Exception:
            pass
        return self._take_completed_realtime_portfolio_membership(
            market_date=market_date,
            task=task,
        )

    def _start_realtime_exec_membership_refresh(
        self,
        *,
        market_date: date,
        trigger: str,
    ) -> asyncio.Task[ExecRuntimeMonitoredSymbolMembership | None]:
        task = self._realtime_exec_membership_tasks.get(market_date)
        if task is not None:
            return task
        task = asyncio.create_task(
            self._load_live_exec_monitoring_symbols(
                market_date=market_date,
                trigger=trigger,
            ),
            name=(
                'exec-realtime-membership-refresh:'
                f'{market_date.isoformat()}'
            ),
        )
        self._realtime_exec_membership_tasks[market_date] = task
        return task

    def _take_completed_realtime_exec_membership(
        self,
        *,
        market_date: date,
        task: asyncio.Task[ExecRuntimeMonitoredSymbolMembership | None],
    ) -> ExecRuntimeMonitoredSymbolMembership | None:
        if not task.done():
            return self._degraded_cached_exec_monitoring_membership(
                market_date
            )
        if self._realtime_exec_membership_tasks.get(market_date) is task:
            self._realtime_exec_membership_tasks.pop(market_date, None)
        try:
            return task.result()
        except (asyncio.CancelledError, Exception):
            return self._degraded_cached_exec_monitoring_membership(
                market_date
            )

    async def _load_live_exec_monitoring_symbols(
        self,
        *,
        market_date: date,
        trigger: str,
    ) -> ExecRuntimeMonitoredSymbolMembership | None:
        now = _utcnow().astimezone(timezone.utc)
        integration = self.integration_status()
        if integration['config_status'] in {REMOTE_FETCH_STATUS_DISABLED, REMOTE_FETCH_STATUS_NOT_CONFIGURED}:
            return None
        if market_date != now.astimezone(MARKET_TIME_ZONE).date():
            return None
        try:
            payload = await self.client.get_runtime_monitored_symbol_membership()
        except SignalDeciderExecClientError:
            return self._degraded_cached_exec_monitoring_membership(market_date)

        if (
            payload.get('schema_version')
            != EXEC_RUNTIME_MONITORED_SYMBOL_MEMBERSHIP_SCHEMA_VERSION
            or payload.get('market_date') != market_date.isoformat()
            or not isinstance(payload.get('membership_complete'), bool)
        ):
            return self._degraded_cached_exec_monitoring_membership(market_date)

        generated_at_text = payload.get('generated_at')
        if not isinstance(generated_at_text, str):
            return self._degraded_cached_exec_monitoring_membership(market_date)
        try:
            generated_at = datetime.fromisoformat(
                generated_at_text.strip().replace('Z', '+00:00')
            )
        except ValueError:
            return self._degraded_cached_exec_monitoring_membership(market_date)
        if (
            generated_at.tzinfo is None
            or generated_at.utcoffset() != timedelta(0)
            or generated_at.astimezone(MARKET_TIME_ZONE).date() != market_date
            or generated_at > (
                now + EXEC_RUNTIME_MONITORED_MEMBERSHIP_MAX_FUTURE_SKEW
            )
            or generated_at < (
                now - EXEC_RUNTIME_MONITORED_MEMBERSHIP_MAX_AGE
            )
        ):
            return self._degraded_cached_exec_monitoring_membership(market_date)

        membership_complete = payload['membership_complete']
        membership_scope = payload.get('membership_scope')
        membership_exchanges = (
            membership_scope.get('exchanges')
            if isinstance(membership_scope, dict)
            else None
        )
        coverage = payload.get('coverage')
        execution_scope = (
            coverage.get('execution_scope')
            if isinstance(coverage, dict)
            else None
        )
        exchanges = (
            execution_scope.get('exchanges')
            if isinstance(execution_scope, dict)
            else None
        )
        valid_execution_scope_sources = {
            'configured_broker_account',
            'configured_account_scope',
        }
        if integration['runtime_mode'] != 'live':
            valid_execution_scope_sources.add('simulated_provider_route')
        if (
            not isinstance(coverage, dict)
            or not isinstance(membership_scope, dict)
            or membership_scope.get('market') != 'KRX'
            or membership_exchanges
            != list(EXEC_RUNTIME_KRX_MEMBERSHIP_EXCHANGES)
            or coverage.get('status')
            != ('complete' if membership_complete else 'unavailable')
            or coverage.get('provider') != 'kis'
            or coverage.get('runtime_mode') != integration['runtime_mode']
            or not isinstance(execution_scope, dict)
            or execution_scope.get('provider') != 'kis'
            or not isinstance(exchanges, list)
            or any(not isinstance(exchange, str) for exchange in exchanges)
            or 'KRX' not in exchanges
            or (
                membership_complete
                and (
                    coverage.get('reason') is not None
                    or execution_scope.get('status') != 'available'
                    or execution_scope.get('source')
                    not in valid_execution_scope_sources
                    or execution_scope.get('reason') is not None
                    or exchanges
                    != list(EXEC_RUNTIME_KIS_EXECUTION_SCOPE_EXCHANGES)
                )
            )
            or (
                not membership_complete
                and not _normalize_optional_text(coverage.get('reason'))
            )
        ):
            return self._degraded_cached_exec_monitoring_membership(market_date)

        raw_symbols = payload.get('symbols')
        if not isinstance(raw_symbols, list):
            return self._degraded_cached_exec_monitoring_membership(market_date)
        normalized_symbols: list[str] = []
        for item in raw_symbols:
            if not isinstance(item, str):
                return self._degraded_cached_exec_monitoring_membership(
                    market_date
                )
            symbol = item.strip().upper()
            if (
                not symbol
                or symbol != item
                or re.fullmatch(r'[0-9]{6}', symbol) is None
            ):
                return self._degraded_cached_exec_monitoring_membership(
                    market_date
                )
            normalized_symbols.append(symbol)
        if normalized_symbols != sorted(set(normalized_symbols)):
            return self._degraded_cached_exec_monitoring_membership(market_date)

        membership = ExecRuntimeMonitoredSymbolMembership(
            symbols=frozenset(normalized_symbols),
            membership_complete=membership_complete,
            source_version=generated_at.astimezone(timezone.utc),
        )
        latest = self._latest_exec_monitoring_membership
        if (
            latest is not None
            and latest[0] == market_date
            and latest[1].source_version is not None
            and membership.source_version is not None
        ):
            if membership.source_version < latest[1].source_version:
                return latest[1]
            if (
                membership.source_version == latest[1].source_version
                and (
                    membership.symbols != latest[1].symbols
                    or membership.membership_complete
                    != latest[1].membership_complete
                )
            ):
                membership = ExecRuntimeMonitoredSymbolMembership(
                    symbols=frozenset(
                        membership.symbols | latest[1].symbols
                    ),
                    membership_complete=False,
                    source_version=membership.source_version,
                )
            elif not membership.membership_complete:
                membership = ExecRuntimeMonitoredSymbolMembership(
                    symbols=frozenset(
                        membership.symbols | latest[1].symbols
                    ),
                    membership_complete=False,
                    source_version=membership.source_version,
                )
        self._latest_exec_monitoring_membership = (market_date, membership)
        return membership

    def _degraded_cached_exec_monitoring_membership(
        self,
        market_date: date,
    ) -> ExecRuntimeMonitoredSymbolMembership | None:
        latest = self._latest_exec_monitoring_membership
        if latest is None or latest[0] != market_date:
            return None
        return ExecRuntimeMonitoredSymbolMembership(
            symbols=latest[1].symbols,
            membership_complete=False,
            source_version=latest[1].source_version,
        )

    async def _load_live_portfolio_holding_membership(
        self,
        *,
        market_date: date,
        trigger: str,
    ) -> PortfolioHoldingSymbolMembership | None:
        now = _utcnow().astimezone(timezone.utc)
        if market_date != now.astimezone(MARKET_TIME_ZONE).date():
            return None
        try:
            payload = await portfolio_summary_service.get_portfolio_summary(
                refresh=False,
                trigger=f'{trigger}:runtime_monitored_holdings',
            )
        except Exception:
            return self._degraded_cached_portfolio_holding_membership(
                market_date
            )

        freshness = (
            payload.get('freshness')
            if isinstance(payload.get('freshness'), dict)
            else {}
        )
        accounts = payload.get('accounts')
        account_coverage = payload.get('accountCoverage')
        source_version_text = payload.get('asOf') or freshness.get(
            'lastRefreshSucceededAt'
        )
        source_version: datetime | None = None
        if isinstance(source_version_text, str):
            try:
                parsed_source_version = datetime.fromisoformat(
                    source_version_text.strip().replace('Z', '+00:00')
                )
            except ValueError:
                parsed_source_version = None
            if (
                parsed_source_version is not None
                and parsed_source_version.tzinfo is not None
                and parsed_source_version.utcoffset() == timedelta(0)
            ):
                source_version = parsed_source_version.astimezone(timezone.utc)

        if (
            source_version is not None
            and source_version
            > now + EXEC_RUNTIME_MONITORED_MEMBERSHIP_MAX_FUTURE_SKEW
        ):
            return self._degraded_cached_portfolio_holding_membership(
                market_date
            )

        symbols: set[str] = set()
        source_is_structurally_valid = bool(
            payload.get('source') == PORTFOLIO_SUMMARY_READ_MODEL_SOURCE
            and isinstance(accounts, list)
            and accounts
            and isinstance(account_coverage, dict)
            and account_coverage.get('status') == 'complete'
            and account_coverage.get('configuredCount') == len(accounts)
            and account_coverage.get('returnedCount') == len(accounts)
        )
        account_memberships_complete = source_is_structurally_valid
        account_ids: set[str] = set()
        for account in accounts if isinstance(accounts, list) else []:
            if not isinstance(account, dict):
                source_is_structurally_valid = False
                account_memberships_complete = False
                continue
            account_id = _normalize_optional_text(account.get('id'))
            if not account_id or account_id in account_ids:
                source_is_structurally_valid = False
            elif account_id:
                account_ids.add(account_id)
            if account.get('broker') != 'kis':
                source_is_structurally_valid = False
            completeness = account.get('holdingsCompleteness')
            holdings_complete = bool(
                isinstance(completeness, dict)
                and completeness.get('status') == 'complete'
            )
            account_memberships_complete = bool(
                account_memberships_complete and holdings_complete
            )
            holdings = account.get('holdings')
            observed_holdings = account.get('observedHoldings')
            if not isinstance(holdings, list):
                holdings = []
                if holdings_complete:
                    source_is_structurally_valid = False
            if not isinstance(observed_holdings, list):
                observed_holdings = []
            if not holdings and not observed_holdings and not holdings_complete:
                source_is_structurally_valid = False
            for holding in [*holdings, *observed_holdings]:
                if not isinstance(holding, dict):
                    source_is_structurally_valid = False
                    continue
                raw_symbol = holding.get('symbol')
                symbol = (
                    raw_symbol.strip()
                    if isinstance(raw_symbol, str)
                    else None
                )
                quantity = _safe_decimal(holding.get('quantity'))
                if (
                    symbol is None
                    or re.fullmatch(r'[0-9]{6}', symbol) is None
                    or quantity is None
                    or quantity < Decimal('0')
                ):
                    source_is_structurally_valid = False
                    continue
                if quantity > Decimal('0'):
                    symbols.add(symbol)

        source_is_current = bool(
            source_version is not None
            and source_version.astimezone(MARKET_TIME_ZONE).date()
            == market_date
            and source_version
            >= now - EXEC_RUNTIME_MONITORED_MEMBERSHIP_MAX_AGE
        )
        membership = PortfolioHoldingSymbolMembership(
            symbols=frozenset(symbols),
            membership_complete=bool(
                source_is_structurally_valid
                and account_memberships_complete
                and source_is_current
                and freshness.get('isStale') is False
                and freshness.get('refreshInFlight') is False
            ),
            source_version=source_version,
        )
        latest = self._latest_portfolio_holding_membership
        if latest is not None and latest[0] == market_date:
            latest_membership = latest[1]
            if (
                membership.source_version is not None
                and latest_membership.source_version is not None
                and membership.source_version
                < latest_membership.source_version
            ):
                return self._degraded_cached_portfolio_holding_membership(
                    market_date
                )
            if (
                not membership.membership_complete
                or (
                    membership.source_version is not None
                    and membership.source_version
                    == latest_membership.source_version
                    and (
                        membership.symbols != latest_membership.symbols
                        or membership.membership_complete
                        != latest_membership.membership_complete
                    )
                )
            ):
                membership = PortfolioHoldingSymbolMembership(
                    symbols=frozenset(
                        membership.symbols | latest_membership.symbols
                    ),
                    membership_complete=False,
                    source_version=(
                        membership.source_version
                        or latest_membership.source_version
                    ),
                )
        self._latest_portfolio_holding_membership = (
            market_date,
            membership,
        )
        return membership

    def _degraded_cached_portfolio_holding_membership(
        self,
        market_date: date,
    ) -> PortfolioHoldingSymbolMembership | None:
        latest = self._latest_portfolio_holding_membership
        if latest is None or latest[0] != market_date:
            return None
        return PortfolioHoldingSymbolMembership(
            symbols=latest[1].symbols,
            membership_complete=False,
            source_version=latest[1].source_version,
        )

    async def _load_live_portfolio_holding_symbols(
        self,
        *,
        trigger: str,
    ) -> set[str] | None:
        membership = await self._load_live_portfolio_holding_membership(
            market_date=_utcnow().astimezone(MARKET_TIME_ZONE).date(),
            trigger=trigger,
        )
        if membership is None or not membership.membership_complete:
            return None
        return set(membership.symbols)

    @staticmethod
    def _collect_runtime_plan_item_ids(items: Sequence[dict[str, Any]]) -> set[int]:
        ids: set[int] = set()
        for item in items:
            parsed = _safe_decimal(item.get('plan_item_id'))
            if parsed is not None:
                ids.add(int(parsed))
        return ids

    @staticmethod
    def _collect_runtime_held_scopes(items: Sequence[dict[str, Any]]) -> set[tuple[str, str]]:
        scopes: set[tuple[str, str]] = set()
        for item in items:
            if not item.get('held_position') and (_safe_decimal(item.get('position_quantity')) or Decimal('0')) <= Decimal('0'):
                continue
            account_reference = _normalize_optional_text(item.get('account_reference'))
            symbol = _normalize_optional_text(item.get('symbol'))
            if account_reference and symbol:
                scopes.add((account_reference, symbol))
        return scopes

    def _load_historical_held_runtime_board_items(
        self,
        *,
        market_date: date,
        runtime_status: str,
        existing_plan_item_ids: set[int],
        existing_held_scopes: set[tuple[str, str]],
        live_holding_symbols: set[str] | None,
        live_holding_membership_complete: bool = False,
    ) -> list[dict[str, Any]]:
        lookback_start = market_date - timedelta(days=365)
        with session_scope() as session:
            rows = session.execute(
                select(SignalDeciderExecutionPlanItem)
                .where(
                    SignalDeciderExecutionPlanItem.execution_window == EXECUTION_WINDOW_NEXT_OPEN,
                    SignalDeciderExecutionPlanItem.target_trade_date < market_date,
                    SignalDeciderExecutionPlanItem.target_trade_date >= lookback_start,
                    SignalDeciderExecutionPlanItem.submission_payload.is_not(None),
                )
                .order_by(
                    SignalDeciderExecutionPlanItem.target_trade_date.desc(),
                    SignalDeciderExecutionPlanItem.id.desc(),
                )
                .limit(500)
            ).scalars().all()
            serialized_rows = [self._serialize_execution_plan_row_for_runtime(row) for row in rows]

        candidate_items = [self._augment_exec_overview_item(item) for item in serialized_rows]
        held_symbols = [
            item.get('symbol')
            for item in candidate_items
            if self._overview_item_has_held_position(item)
            and self._overview_item_has_recent_readback(item, market_date=market_date)
            and self._historical_held_symbol_is_live(
                item,
                live_holding_symbols=live_holding_symbols,
                live_holding_membership_complete=(
                    live_holding_membership_complete
                ),
            )
            and _safe_decimal(item.get('plan_item_id')) is not None
            and int(_safe_decimal(item.get('plan_item_id'))) not in existing_plan_item_ids
        ]
        symbol_metadata_map = self._load_symbol_metadata_map(held_symbols)

        held_items: list[dict[str, Any]] = []
        seen_scopes = set(existing_held_scopes)
        for item in candidate_items:
            parsed_plan_item_id = _safe_decimal(item.get('plan_item_id'))
            if parsed_plan_item_id is None or int(parsed_plan_item_id) in existing_plan_item_ids:
                continue
            if not self._overview_item_has_held_position(item):
                continue
            if not self._overview_item_has_recent_readback(item, market_date=market_date):
                continue
            if not self._historical_held_symbol_is_live(
                item,
                live_holding_symbols=live_holding_symbols,
                live_holding_membership_complete=(
                    live_holding_membership_complete
                ),
            ):
                continue
            account_reference = _normalize_optional_text(item.get('account_reference'))
            symbol = _normalize_optional_text(item.get('symbol'))
            if not account_reference or not symbol:
                continue
            scope = (account_reference, symbol)
            if scope in seen_scopes:
                continue
            seen_scopes.add(scope)
            held_items.append(
                self._build_runtime_board_item(
                    item=item,
                    market_date=market_date,
                    symbol_metadata_map=symbol_metadata_map,
                    runtime_status=runtime_status,
                )
            )
        return held_items

    @staticmethod
    def _historical_held_symbol_is_live(
        item: dict[str, Any],
        *,
        live_holding_symbols: set[str] | None,
        live_holding_membership_complete: bool = False,
    ) -> bool:
        if (
            live_holding_symbols is None
            or not live_holding_membership_complete
        ):
            return True
        symbol = _normalize_optional_text(item.get('symbol'))
        return bool(symbol and symbol in live_holding_symbols)

    @staticmethod
    def _serialize_execution_plan_row_for_runtime(row: SignalDeciderExecutionPlanItem) -> dict[str, Any]:
        return {
            'plan_item_id': row.id,
            'execution_window': row.execution_window,
            'agent_name': row.decision_agent_name,
            'decision_agent_name': row.decision_agent_name,
            'target_trade_date': row.target_trade_date.isoformat(),
            'source_market_date': row.source_market_date.isoformat(),
            'account_reference': row.account_reference,
            'account_label': row.account_label,
            'symbol': row.symbol,
            'policy_version': row.policy_version,
            'decision_snapshot_id': row.decision_snapshot_id,
            'planned_side': row.planned_side,
            'planned_order_type': row.planned_order_type,
            'planned_time_in_force': row.planned_time_in_force,
            'allocation_budget_krw': _decimal_to_text(_safe_decimal(row.allocation_budget_krw)),
            'estimated_reference_price': row.estimated_reference_price,
            'planned_quantity': _decimal_to_text(_safe_decimal(row.planned_quantity)),
            'estimated_notional_krw': _decimal_to_text(_safe_decimal(row.estimated_notional_krw)),
            'eligibility_status': row.eligibility_status,
            'execution_status': row.execution_status,
            'client_order_id': row.client_order_id,
            'order_request_id': row.order_request_id,
            'broker_order_row_id': row.broker_order_row_id,
            'validation': row.validation_payload,
            'traceability': row.traceability_payload,
            'submission': row.submission_payload,
            'first_ready_at': row.first_ready_at.isoformat() if row.first_ready_at else None,
            'last_evaluated_at': row.last_evaluated_at.isoformat() if row.last_evaluated_at else None,
            'submitted_at': row.submitted_at.isoformat() if row.submitted_at else None,
        }

    @staticmethod
    def _overview_item_has_held_position(item: dict[str, Any]) -> bool:
        remote_state = item.get('remote_state') if isinstance(item.get('remote_state'), dict) else {}
        positions = remote_state.get('positions') if isinstance(remote_state.get('positions'), list) else []
        if any((_safe_decimal(position.get('quantity') or position.get('available_quantity')) or Decimal('0')) > Decimal('0') for position in positions):
            return True
        return (_safe_decimal(remote_state.get('open_quantity')) or Decimal('0')) > Decimal('0')

    @staticmethod
    def _overview_item_has_recent_readback(
        item: dict[str, Any],
        *,
        market_date: date,
        max_age_days: int = 30,
    ) -> bool:
        remote_state = item.get('remote_state') if isinstance(item.get('remote_state'), dict) else {}
        last_synced_at = _parse_iso_datetime(remote_state.get('last_synced_at'))
        if last_synced_at is None:
            return False
        latest_allowed_date = market_date - timedelta(days=max_age_days)
        return latest_allowed_date <= last_synced_at.date() <= market_date

    async def get_runtime_symbol_detail(
        self,
        *,
        market_date: str | date | None = None,
        symbol: str,
        refresh: bool = True,
        trigger: str = 'manual',
    ) -> dict[str, Any]:
        resolved_market_date = _normalize_date(market_date)
        resolved_symbol = _normalize_optional_text(symbol)
        if not resolved_symbol:
            raise ValueError('symbol is required')
        refresh_state = self._ensure_dashboard_background_refresh(
            target_trade_date=resolved_market_date,
            account_reference=None,
            symbol=resolved_symbol,
            trigger=trigger,
            refresh=refresh,
        )
        cached = self._load_dashboard_cached_payload(
            endpoint=DASHBOARD_ENDPOINT_SYMBOL_DETAIL,
            market_date=resolved_market_date,
            symbol=resolved_symbol,
            refresh_state=refresh_state,
            allow_stale=True,
        )
        if cached is not None:
            return cached

        return await self._build_dashboard_payload_singleflight(
            endpoint=DASHBOARD_ENDPOINT_SYMBOL_DETAIL,
            market_date=resolved_market_date,
            symbol=resolved_symbol,
            refresh_state=refresh_state,
            build=lambda: self._build_and_cache_runtime_symbol_detail(
                market_date=resolved_market_date,
                symbol=resolved_symbol,
                refresh=refresh,
                refresh_state=refresh_state,
                trigger=trigger,
            ),
        )

    async def _build_and_cache_runtime_daily_board(
        self,
        *,
        market_date: date,
        refresh: bool,
        refresh_state: dict[str, Any],
        trigger: str,
    ) -> dict[str, Any]:
        overview = await self._load_dashboard_read_model_overview(
            target_trade_date=market_date,
            account_reference=None,
            symbol=None,
            trigger=trigger,
        )
        payload = await asyncio.to_thread(
            self._build_runtime_daily_board_payload,
            market_date=market_date,
            refresh=refresh,
            overview=overview,
            refresh_state=refresh_state,
            served_from_cache=False,
        )
        self._store_dashboard_cached_payload(
            endpoint=DASHBOARD_ENDPOINT_DAILY_BOARD,
            market_date=market_date,
            symbol=None,
            payload=payload,
        )
        return payload

    async def _build_and_cache_runtime_monitored_symbols(
        self,
        *,
        market_date: date,
        refresh: bool,
        refresh_state: dict[str, Any],
        trigger: str,
    ) -> dict[str, Any]:
        board = await self.get_runtime_daily_board(
            market_date=market_date,
            refresh=False,
            trigger=trigger,
        )
        live_portfolio_holding_membership = (
            await self._load_live_portfolio_holding_membership(
                market_date=market_date,
                trigger=trigger,
            )
        )
        live_holding_symbols = (
            set(live_portfolio_holding_membership.symbols)
            if live_portfolio_holding_membership is not None
            else None
        )
        live_exec_monitoring_membership = await self._load_live_exec_monitoring_symbols(
            market_date=market_date,
            trigger=trigger,
        )
        payload = await asyncio.to_thread(
            self._build_runtime_monitored_symbols_payload,
            board=board,
            refresh=refresh,
            refresh_state=refresh_state,
            served_from_cache=False,
            live_holding_symbols=live_holding_symbols,
            live_portfolio_holding_membership=(
                live_portfolio_holding_membership
            ),
            live_exec_monitoring_membership=live_exec_monitoring_membership,
        )
        self._store_dashboard_cached_payload(
            endpoint=DASHBOARD_ENDPOINT_MONITORED_SYMBOLS,
            market_date=market_date,
            symbol=None,
            payload=payload,
        )
        return payload

    async def _build_and_cache_runtime_symbol_detail(
        self,
        *,
        market_date: date,
        symbol: str,
        refresh: bool,
        refresh_state: dict[str, Any],
        trigger: str,
    ) -> dict[str, Any]:
        overview = await self._load_dashboard_read_model_overview(
            target_trade_date=market_date,
            account_reference=None,
            symbol=symbol,
            trigger=trigger,
        )
        payload = await asyncio.to_thread(
            self._build_runtime_symbol_detail_payload,
            market_date=market_date,
            symbol=symbol,
            refresh=refresh,
            overview=overview,
            refresh_state=refresh_state,
            served_from_cache=False,
        )
        self._store_dashboard_cached_payload(
            endpoint=DASHBOARD_ENDPOINT_SYMBOL_DETAIL,
            market_date=market_date,
            symbol=symbol,
            payload=payload,
        )
        return payload

    def _build_runtime_daily_board_payload(
        self,
        *,
        market_date: date,
        refresh: bool,
        overview: dict[str, Any],
        refresh_state: dict[str, Any],
        served_from_cache: bool,
    ) -> dict[str, Any]:
        generated_at = _utcnow()
        overview_items = overview.get('items') or []
        symbol_metadata_map = self._load_symbol_metadata_map(item.get('symbol') for item in overview_items)
        runtime_read_model = self._resolve_dashboard_runtime_read_model(
            overview=overview,
            overview_items=overview_items,
        )
        runtime = self._build_dashboard_runtime_status(
            integration=overview['integration'],
            runtime=runtime_read_model,
        )
        items = [
            self._build_runtime_board_item(
                item=item,
                market_date=market_date,
                symbol_metadata_map=symbol_metadata_map,
                runtime_status=runtime['status'],
            )
            for item in overview_items
        ]
        activity_coverage = self._build_runtime_daily_board_activity_coverage(
            items=items,
            runtime=runtime_read_model,
            runtime_activity_applicable=(
                market_date == generated_at.astimezone(MARKET_TIME_ZONE).date()
            ),
        )
        payload = {
            'request': {
                'market_date': market_date.isoformat(),
                'refresh': refresh,
            },
            'runtime': runtime,
            'market_date': market_date.isoformat(),
            'summary': self._build_runtime_daily_board_summary(items=items),
            'activity_coverage': activity_coverage,
            'freshness': self._build_dashboard_freshness(
                endpoint=DASHBOARD_ENDPOINT_DAILY_BOARD,
                market_date=market_date,
                generated_at=generated_at,
                served_from_cache=served_from_cache,
                runtime_status=runtime['status'],
                overview_items=overview_items,
                output_item_count=len(items),
                refresh_state=refresh_state,
                read_model_availability_status=activity_coverage['status'],
            ),
            'refresh': refresh_state,
            'items': items,
        }
        return serialize_execution_daily_board_payload(payload)

    def _build_runtime_monitored_symbols_payload(
        self,
        *,
        board: dict[str, Any],
        refresh: bool,
        refresh_state: dict[str, Any],
        served_from_cache: bool,
        live_holding_symbols: set[str] | None = None,
        live_portfolio_holding_membership: (
            PortfolioHoldingSymbolMembership | None
        ) = None,
        live_exec_monitoring_membership: ExecRuntimeMonitoredSymbolMembership | None = None,
    ) -> dict[str, Any]:
        resolved_market_date = date.fromisoformat(str(board['market_date']))
        if live_portfolio_holding_membership is not None:
            live_holding_symbols = set(
                live_portfolio_holding_membership.symbols
            )
            holdings_membership_complete = (
                live_portfolio_holding_membership.membership_complete
            )
            holdings_membership_status = (
                'complete' if holdings_membership_complete else 'partial'
            )
        else:
            holdings_membership_complete = live_holding_symbols is not None
            holdings_membership_status = (
                'complete'
                if holdings_membership_complete
                else 'unknown'
            )
        board_items = list(board.get('items') or [])
        if (
            board_items
            or not holdings_membership_complete
            or bool(live_holding_symbols)
        ):
            board_items.extend(
                self._load_historical_held_runtime_board_items(
                    market_date=resolved_market_date,
                    runtime_status=board['runtime']['status'],
                    existing_plan_item_ids=self._collect_runtime_plan_item_ids(board_items),
                    existing_held_scopes=self._collect_runtime_held_scopes(board_items),
                    live_holding_symbols=live_holding_symbols,
                    live_holding_membership_complete=(
                        holdings_membership_complete
                    ),
                )
            )
        live_exec_monitoring_symbols = (
            set(live_exec_monitoring_membership.symbols)
            if live_exec_monitoring_membership is not None
            else set()
        )
        live_exec_membership_complete = bool(
            live_exec_monitoring_membership is not None
            and live_exec_monitoring_membership.membership_complete
        )
        board_items = [
            self._apply_direct_runtime_membership_truth(
                item=item,
                holdings_membership_complete=holdings_membership_complete,
                held_position=bool(
                    live_holding_symbols is not None
                    and _normalize_optional_text(item.get('symbol'))
                    in live_holding_symbols
                ),
                exec_membership_complete=live_exec_membership_complete,
                exec_monitoring_active=(
                    _normalize_optional_text(item.get('symbol'))
                    in live_exec_monitoring_symbols
                ),
            )
            for item in board_items
        ]
        direct_membership_symbols = {
            *(live_holding_symbols or set()),
            *live_exec_monitoring_symbols,
        }
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in board_items:
            symbol = _normalize_optional_text(item.get('symbol'))
            if symbol:
                grouped[symbol].append(item)

        included_groups = {
            symbol: grouped_items
            for symbol, grouped_items in grouped.items()
            if self._should_include_monitored_symbol_group(grouped_items)
        }
        realtime_symbols = self._prioritized_runtime_symbols_for_realtime(included_groups)
        items = [
            self._build_runtime_monitored_symbol_item(
                symbol=symbol,
                items=included_groups[symbol],
                runtime_status=board['runtime']['status'],
                market_date=resolved_market_date,
            )
            for symbol in realtime_symbols
        ]
        included_symbols = {item['symbol'] for item in items}
        missing_symbols = sorted(direct_membership_symbols - included_symbols)
        if missing_symbols:
            symbol_metadata_map = self._load_symbol_metadata_map(missing_symbols)
            items.extend(
                self._build_direct_runtime_membership_item(
                    symbol=symbol,
                    market_date=resolved_market_date,
                    metadata=symbol_metadata_map.get(symbol),
                    held_position=bool(
                        live_holding_symbols is not None
                        and symbol in live_holding_symbols
                    ),
                    exec_monitoring_active=(
                        symbol in live_exec_monitoring_symbols
                    ),
                )
                for symbol in missing_symbols
            )
        item_by_symbol = {item['symbol']: item for item in items}
        ranking_groups = dict(included_groups)
        ranking_groups.update(
            {
                symbol: [item_by_symbol[symbol]]
                for symbol in missing_symbols
            }
        )
        realtime_symbols = self._prioritized_runtime_symbols_for_realtime(
            ranking_groups
        )
        items = [item_by_symbol[symbol] for symbol in realtime_symbols]
        monitoring_facts_by_symbol: dict[
            str,
            list[dict[str, Any]],
        ] = defaultdict(list)
        for fact in board['runtime'].get('active_monitoring_items') or []:
            if not isinstance(fact, dict):
                continue
            symbol = _normalize_optional_text(fact.get('symbol'))
            if symbol:
                monitoring_facts_by_symbol[symbol].append(fact)
        monitoring_facts_complete = bool(
            board['runtime'].get('active_monitoring_facts_complete')
        )
        items = [
            self._apply_live_exec_monitoring_truth(
                item=item,
                facts=monitoring_facts_by_symbol.get(item['symbol'], []),
                facts_complete=monitoring_facts_complete,
            )
            for item in items
        ]
        generated_at = _utcnow()
        freshness = self._build_runtime_monitored_freshness(
            source_freshness=board.get('freshness'),
            endpoint=DASHBOARD_ENDPOINT_MONITORED_SYMBOLS,
            market_date=resolved_market_date,
            generated_at=generated_at,
            served_from_cache=served_from_cache,
            runtime_status=board['runtime']['status'],
            overview_items=board_items,
            output_item_count=len(items),
            refresh_state=refresh_state,
        )
        payload = {
            'request': {
                'market_date': board['market_date'],
                'refresh': refresh,
            },
            'runtime': board['runtime'],
            'market_date': board['market_date'],
            'summary': self._build_runtime_monitored_summary(items=items),
            'realtime_subscription': {
                'desired_symbols': realtime_symbols,
                'desired_symbol_count': len(realtime_symbols),
                'max_subscriptions': settings.kis_realtime_price_max_subscriptions,
            },
            'source_coverage': {
                'membership_complete': bool(
                    holdings_membership_complete
                    and live_exec_membership_complete
                ),
                'live_holdings': (
                    holdings_membership_status
                ),
                'live_exec_monitoring': (
                    'complete'
                    if live_exec_membership_complete
                    else 'unknown'
                ),
            },
            'freshness': freshness,
            'refresh': refresh_state,
            'items': items,
        }
        return serialize_execution_monitored_symbols_payload(payload)

    @staticmethod
    def _apply_live_exec_monitoring_truth(
        *,
        item: dict[str, Any],
        facts: Sequence[dict[str, Any]],
        facts_complete: bool,
    ) -> dict[str, Any]:
        result = dict(item)
        if not facts_complete or not bool(item.get('exec_monitoring_active')):
            return result

        monitoring_statuses = {
            status
            for status in (item.get('monitoring_statuses') or [])
            if _normalize_optional_text(status)
        }
        monitoring_statuses.discard('runtime_monitoring_facts_unavailable')
        if not facts:
            result.update(
                {
                    'target_price': None,
                    'stop_loss_price': None,
                    'trigger_price': None,
                    'monitoring_state': UI_MONITORING_STATE_RECONCILING,
                    'last_event_at': None,
                }
            )
            monitoring_statuses.discard('target_sell_monitoring')
            monitoring_statuses.discard('stop_sell_monitoring')
            monitoring_statuses.add('runtime_monitoring_facts_unavailable')
            result['monitoring_statuses'] = sorted(monitoring_statuses)
            return result

        facts_by_intent = {
            intent: fact
            for fact in facts
            if isinstance(fact, dict)
            and (
                intent := _normalize_optional_text(
                    fact.get('intent'),
                    lowercase=True,
                )
            )
        }
        target_fact = facts_by_intent.get('target_sell')
        stop_fact = facts_by_intent.get('stop_sell')
        result['target_price'] = (
            target_fact.get('trigger_price')
            if target_fact is not None
            else None
        )
        result['stop_loss_price'] = (
            stop_fact.get('trigger_price')
            if stop_fact is not None
            else None
        )
        result['trigger_price'] = None

        for fact in facts:
            status = _normalize_optional_text(fact.get('status'), lowercase=True)
            if status:
                monitoring_statuses.add(status)
        result['monitoring_statuses'] = sorted(monitoring_statuses)

        if target_fact is not None or stop_fact is not None:
            result['monitoring_state'] = UI_MONITORING_STATE_WATCHING_TARGET_STOP

        observed_at = _latest_iso_timestamp(
            fact.get('observed_at')
            for fact in facts
        )
        if observed_at is not None:
            result['last_event_at'] = observed_at

        observed_price = None
        for fact in reversed(facts):
            observed_price = _safe_decimal(fact.get('observed_price'))
            if observed_price is not None:
                break
        if observed_price is not None:
            result.update(
                {
                    'current_price': _to_json_number(observed_price),
                    'price_as_of': observed_at,
                    'price_received_at': observed_at,
                    'price_source': 'exec_runtime_monitoring',
                    'price_status': 'available',
                    'display_price_available': True,
                }
            )
        return result

    @staticmethod
    def _apply_direct_runtime_membership_truth(
        *,
        item: dict[str, Any],
        holdings_membership_complete: bool,
        held_position: bool,
        exec_membership_complete: bool,
        exec_monitoring_active: bool,
    ) -> dict[str, Any]:
        result = dict(item)
        original_exec_showable = bool(item.get('exec_runtime_showable'))
        monitoring_state = _normalize_optional_text(
            item.get('monitoring_state'),
            lowercase=True,
        )

        if holdings_membership_complete:
            result['held_position'] = held_position
            if held_position:
                if (
                    _safe_decimal(item.get('position_quantity'))
                    or Decimal('0')
                ) <= Decimal('0'):
                    result['position_quantity'] = None
                result['position_side'] = 'long'
            else:
                result['position_quantity'] = 0
                result['position_average_price'] = None
                result['position_side'] = None
                result['open_quantity'] = 0
        elif held_position:
            result['held_position'] = True
            result['position_side'] = 'long'

        if exec_monitoring_active:
            result['exec_monitoring_active'] = True
            result['exec_runtime_showable'] = True
            result['exec_runtime_state'] = (
                item.get('exec_runtime_state')
                if original_exec_showable
                or item.get('exec_agent_approved') is True
                else 'runtime_membership_confirmed'
            )
            result['exec_runtime_exclusion_reason'] = None
            result['exec_runtime_exclusion_reasons'] = []
        elif exec_membership_complete:
            result['exec_monitoring_active'] = False
            result['exec_runtime_showable'] = False
            result['exec_runtime_state'] = 'no_runtime_membership'
            result['exec_runtime_exclusion_reason'] = (
                'no_exec_runtime_membership'
            )
            result['exec_runtime_exclusion_reasons'] = [
                'no_exec_runtime_membership'
            ]

        if held_position or exec_monitoring_active:
            if monitoring_state in {None, UI_MONITORING_STATE_IDLE}:
                result['monitoring_state'] = UI_MONITORING_STATE_RECONCILING
        elif (
            holdings_membership_complete
            and exec_membership_complete
            and monitoring_state
            in {
                UI_MONITORING_STATE_WATCHING_TARGET_STOP,
                UI_MONITORING_STATE_RECONCILING,
            }
        ):
            result['monitoring_state'] = UI_MONITORING_STATE_IDLE

        monitoring_statuses = {
            status
            for status in (item.get('monitoring_statuses') or [])
            if _normalize_optional_text(status)
        }
        if holdings_membership_complete:
            monitoring_statuses.discard('portfolio_holding_active')
            if held_position:
                monitoring_statuses.add('portfolio_holding_active')
        if exec_membership_complete or exec_monitoring_active:
            monitoring_statuses.discard('runtime_membership_active')
            if exec_monitoring_active:
                monitoring_statuses.add('runtime_membership_active')
        result['monitoring_statuses'] = sorted(monitoring_statuses)

        if exec_monitoring_active and not result.get('exec_agent_approved'):
            result.update(
                {
                    'exec_agent_action': None,
                    'exec_agent_reason_code': None,
                    'exec_agent_target_profile': None,
                    'exec_agent_target_tag': None,
                    'exec_agent_special_tactical_scalp': None,
                }
            )
        return result

    def _build_direct_runtime_membership_item(
        self,
        *,
        symbol: str,
        market_date: date,
        metadata: dict[str, Any] | None,
        held_position: bool,
        exec_monitoring_active: bool,
    ) -> dict[str, Any]:
        resolved_metadata = metadata or {'name': symbol, 'market': None}
        monitoring_statuses = []
        if held_position:
            monitoring_statuses.append('portfolio_holding_active')
        if exec_monitoring_active:
            monitoring_statuses.append('runtime_membership_active')
        exclusion_reason = (
            None
            if exec_monitoring_active
            else 'no_exec_runtime_membership'
        )
        return {
            'symbol': symbol,
            'name': resolved_metadata['name'],
            'market': resolved_metadata['market'],
            **self._price_fields_for_symbol(symbol, market_date=market_date),
            'monitoring_state': UI_MONITORING_STATE_RECONCILING,
            'exec_monitoring_active': exec_monitoring_active,
            'exec_runtime_showable': exec_monitoring_active,
            'exec_runtime_state': (
                'runtime_membership_only'
                if exec_monitoring_active
                else 'portfolio_membership_only'
            ),
            'exec_runtime_exclusion_reason': exclusion_reason,
            'exec_runtime_exclusion_reasons': (
                [] if exclusion_reason is None else [exclusion_reason]
            ),
            'core_state_divergence_count': 0,
            'core_state_divergence_warnings': [],
            'exec_agent_approved': False,
            'exec_agent_action': None,
            'exec_agent_reason_code': None,
            'exec_agent_target_profile': None,
            'exec_agent_target_tag': None,
            'exec_agent_special_tactical_scalp': None,
            'held_position': held_position,
            'position_quantity': None,
            'position_average_price': None,
            'approved_quantity': None,
            'approved_notional': None,
            'monitoring_statuses': monitoring_statuses,
            'position_side': 'long' if held_position else None,
            'planned_intents': [],
            'tactical_actions': [],
            'handoff_intents': [],
            'target_price': None,
            'stop_loss_price': None,
            'trigger_price': None,
            'chase_limit_price': None,
            'expected_holding_period': None,
            'decision_agent_names': [],
            'last_event_at': None,
            'account_references': [],
            'status_counts': {},
        }

    def _build_runtime_symbol_detail_payload(
        self,
        *,
        market_date: date,
        symbol: str,
        refresh: bool,
        overview: dict[str, Any],
        refresh_state: dict[str, Any],
        served_from_cache: bool,
    ) -> dict[str, Any]:
        generated_at = _utcnow()
        overview_items = overview.get('items') or []
        symbol_metadata_map = self._load_symbol_metadata_map([symbol])
        runtime = self._build_dashboard_runtime_status(
            integration=overview['integration'],
            runtime=self._resolve_dashboard_runtime_read_model(overview=overview, overview_items=overview_items),
        )
        items = [
            self._build_runtime_symbol_detail_item(
                item=item,
                market_date=market_date,
                symbol_metadata_map=symbol_metadata_map,
                runtime_status=runtime['status'],
            )
            for item in overview_items
        ]
        metadata = symbol_metadata_map.get(
            symbol,
            {
                'name': symbol,
                'market': None,
            },
        )
        payload = {
            'request': {
                'market_date': market_date.isoformat(),
                'symbol': symbol,
                'refresh': refresh,
            },
            'runtime': runtime,
            'market_date': market_date.isoformat(),
            'symbol': symbol,
            'name': metadata['name'],
            'market': metadata['market'],
            **self._price_fields_for_symbol(symbol, market_date=market_date),
            'planning': {
                'decision_source': (
                    next(iter(agent_names))
                    if len(
                        agent_names := {
                            _normalize_decision_agent_name(item.get('decision_agent_name'))
                            for item in items
                            if _normalize_optional_text(item.get('decision_agent_name'))
                        }
                    )
                    == 1
                    else 'multiple'
                ),
                'decision_agent_names': sorted(agent_names),
                'decision_snapshot_ids': sorted(
                    {
                        int(item.get('decision_snapshot_id'))
                        for item in items
                        if item.get('decision_snapshot_id') is not None
                    }
                ),
                'decision_run_ids': sorted(
                    {
                        str((item.get('traceability') or {}).get('decision_run_id'))
                        for item in items
                        if _normalize_optional_text((item.get('traceability') or {}).get('decision_run_id'))
                    }
                ),
                'source_feedback_ids': sorted(
                    {
                        int((item.get('traceability') or {}).get('source_feedback_id') or (item.get('traceability') or {}).get('feedback_id'))
                        for item in items
                        if (item.get('traceability') or {}).get('source_feedback_id') is not None
                        or (item.get('traceability') or {}).get('feedback_id') is not None
                    }
                ),
                'policy_versions': sorted(
                    {
                        str(item.get('policy_version'))
                        for item in items
                        if _normalize_optional_text(item.get('policy_version'))
                    }
                ),
                'account_references': sorted(
                    {
                        str(item.get('account_reference'))
                        for item in items
                        if _normalize_optional_text(item.get('account_reference'))
                    }
                ),
            },
            'monitoring': self._build_runtime_symbol_monitoring(items=items, runtime_status=runtime['status']),
            'summary': self._build_runtime_symbol_detail_summary(items=items),
            'freshness': self._build_dashboard_freshness(
                endpoint=DASHBOARD_ENDPOINT_SYMBOL_DETAIL,
                market_date=market_date,
                generated_at=generated_at,
                served_from_cache=served_from_cache,
                runtime_status=runtime['status'],
                overview_items=overview_items,
                output_item_count=len(items),
                refresh_state=refresh_state,
            ),
            'refresh': refresh_state,
            'items': items,
        }
        return serialize_execution_symbol_detail_payload(payload)

    @staticmethod
    def _resolve_dashboard_runtime_read_model(
        *,
        overview: dict[str, Any],
        overview_items: Sequence[dict[str, Any]],
    ) -> dict[str, Any]:
        fallback = overview.get('runtime') if isinstance(overview.get('runtime'), dict) else {}
        latest_runtime: dict[str, Any] | None = None
        latest_synced_at: datetime | None = None
        for item in overview_items:
            remote_state = item.get('remote_state') if isinstance(item.get('remote_state'), dict) else {}
            runtime = remote_state.get('runtime') if isinstance(remote_state.get('runtime'), dict) else None
            if runtime is None:
                continue
            synced_at = _parse_iso_datetime(remote_state.get('last_synced_at')) or datetime.min.replace(tzinfo=timezone.utc)
            if latest_runtime is None or synced_at > (latest_synced_at or datetime.min.replace(tzinfo=timezone.utc)):
                latest_runtime = runtime
                latest_synced_at = synced_at
        return latest_runtime or fallback

    async def _load_dashboard_read_model_overview(
        self,
        *,
        target_trade_date: date,
        account_reference: str | None,
        symbol: str | None,
        trigger: str,
    ) -> dict[str, Any]:
        return await self.list_signal_decider_exec_overview(
            target_trade_date=target_trade_date,
            account_reference=account_reference,
            symbol=symbol,
            refresh=False,
            trigger=trigger,
        )

    def _ensure_dashboard_background_refresh(
        self,
        *,
        target_trade_date: date,
        account_reference: str | None,
        symbol: str | None,
        refresh: bool,
        trigger: str,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        key = self._dashboard_refresh_key(
            target_trade_date=target_trade_date,
            account_reference=account_reference,
            symbol=symbol,
        )
        resolved_timeout_seconds = self._resolve_dashboard_refresh_timeout(timeout_seconds)
        if not refresh:
            return self._dashboard_refresh_payload(
                key,
                requested=False,
                status=DASHBOARD_REFRESH_STATUS_NOT_REQUESTED,
            )

        integration = self.integration_status()
        if not integration['enabled'] or integration['config_status'] != 'ready':
            status = integration['config_status']
            self._dashboard_refresh_state[key] = {
                **self._dashboard_refresh_state.get(key, {}),
                'status': status,
                'timeout_seconds': resolved_timeout_seconds,
                'last_error': integration['issues'][0] if integration['issues'] else None,
            }
            return self._dashboard_refresh_payload(key, requested=True, status=status)

        task = self._dashboard_refresh_tasks.get(key)
        if task is not None and not task.done():
            return self._dashboard_refresh_payload(
                key,
                requested=True,
                status=DASHBOARD_REFRESH_STATUS_RUNNING,
            )

        now = _utcnow()
        state = self._dashboard_refresh_state.get(key) or {}
        last_attempted_at = _parse_iso_datetime(state.get('last_attempted_at'))
        if last_attempted_at is not None:
            elapsed_seconds = (now - last_attempted_at).total_seconds()
            if elapsed_seconds < self.dashboard_refresh_debounce_seconds:
                debounced_status = (
                    state.get('status')
                    if state.get('status') in {
                        DASHBOARD_REFRESH_STATUS_FAILED,
                        DASHBOARD_REFRESH_STATUS_SUCCEEDED,
                        DASHBOARD_REFRESH_STATUS_TIMEOUT,
                    }
                    else DASHBOARD_REFRESH_STATUS_DEBOUNCED
                )
                payload = self._dashboard_refresh_payload(
                    key,
                    requested=True,
                    status=debounced_status,
                )
                payload['debounced'] = True
                return payload

        self._dashboard_refresh_state[key] = {
            **state,
            'status': DASHBOARD_REFRESH_STATUS_SCHEDULED,
            'timeout_seconds': resolved_timeout_seconds,
            'last_attempted_at': now.isoformat(),
            'last_trigger': trigger,
            'last_error': None,
        }
        task = asyncio.create_task(
            self._refresh_dashboard_read_model(
                key=key,
                target_trade_date=target_trade_date,
                account_reference=account_reference,
                symbol=symbol,
                trigger=trigger,
                timeout_seconds=resolved_timeout_seconds,
            ),
            name=f'exec-runtime-dashboard-refresh:{target_trade_date.isoformat()}:{symbol or "all"}',
        )
        self._dashboard_refresh_tasks[key] = task
        task.add_done_callback(lambda completed: self._finalize_dashboard_refresh_task(key, completed))
        return self._dashboard_refresh_payload(
            key,
            requested=True,
            status=DASHBOARD_REFRESH_STATUS_SCHEDULED,
        )

    async def _refresh_dashboard_read_model(
        self,
        *,
        key: tuple[str, str | None, str | None],
        target_trade_date: date,
        account_reference: str | None,
        symbol: str | None,
        trigger: str,
        timeout_seconds: float,
    ) -> None:
        state = self._dashboard_refresh_state.get(key) or {}
        started_at = _utcnow()
        self._dashboard_refresh_state[key] = {
            **state,
            'status': DASHBOARD_REFRESH_STATUS_RUNNING,
            'started_at': started_at.isoformat(),
            'last_error': None,
        }
        try:
            refresh_coro = self.list_signal_decider_exec_overview(
                target_trade_date=target_trade_date,
                account_reference=account_reference,
                symbol=symbol,
                refresh=True,
                trigger=f'{trigger}:dashboard_background_refresh',
            )
            if timeout_seconds > 0:
                overview = await asyncio.wait_for(
                    refresh_coro,
                    timeout=timeout_seconds,
                )
            else:
                overview = await refresh_coro
        except asyncio.TimeoutError:
            finished_at = _utcnow()
            self._dashboard_refresh_state[key] = {
                **self._dashboard_refresh_state.get(key, {}),
                'status': DASHBOARD_REFRESH_STATUS_TIMEOUT,
                'finished_at': finished_at.isoformat(),
                'last_error': _serialize_error(
                    code='dashboard_refresh_timeout',
                    message=(
                        'Exec dashboard refresh exceeded '
                        f'{timeout_seconds:g}s and was left on the cached read model.'
                    ),
                    retryable=True,
                ),
            }
            return
        except Exception as exc:
            finished_at = _utcnow()
            self._dashboard_refresh_state[key] = {
                **self._dashboard_refresh_state.get(key, {}),
                'status': DASHBOARD_REFRESH_STATUS_FAILED,
                'finished_at': finished_at.isoformat(),
                'last_error': _serialize_error(
                    code='dashboard_refresh_failed',
                    message=str(exc),
                    retryable=True,
                ),
            }
            return

        finished_at = _utcnow()
        self._dashboard_refresh_state[key] = {
            **self._dashboard_refresh_state.get(key, {}),
            'status': DASHBOARD_REFRESH_STATUS_SUCCEEDED,
            'finished_at': finished_at.isoformat(),
            'last_succeeded_at': finished_at.isoformat(),
            'last_error': None,
        }
        refresh_state = self._dashboard_refresh_payload(
            key,
            requested=True,
            status=DASHBOARD_REFRESH_STATUS_SUCCEEDED,
        )
        live_holding_symbols = None
        live_portfolio_holding_membership = None
        live_exec_monitoring_membership = None
        if symbol is None:
            background_trigger = f'{trigger}:dashboard_background_refresh'
            live_portfolio_holding_membership = (
                await self._load_live_portfolio_holding_membership(
                    market_date=target_trade_date,
                    trigger=background_trigger,
                )
            )
            live_holding_symbols = (
                set(live_portfolio_holding_membership.symbols)
                if live_portfolio_holding_membership is not None
                else None
            )
            live_exec_monitoring_membership = await self._load_live_exec_monitoring_symbols(
                market_date=target_trade_date,
                trigger=background_trigger,
            )
        await self._store_dashboard_caches_from_overview(
            market_date=target_trade_date,
            symbol=symbol,
            overview=overview,
            refresh_state=refresh_state,
            live_holding_symbols=live_holding_symbols,
            live_portfolio_holding_membership=(
                live_portfolio_holding_membership
            ),
            live_exec_monitoring_membership=live_exec_monitoring_membership,
        )

    def _finalize_dashboard_refresh_task(
        self,
        key: tuple[str, str | None, str | None],
        task: asyncio.Task,
    ) -> None:
        if self._dashboard_refresh_tasks.get(key) is not task:
            if not task.cancelled():
                task.exception()
            return
        self._dashboard_refresh_tasks.pop(key, None)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self._dashboard_refresh_state[key] = {
                **self._dashboard_refresh_state.get(key, {}),
                'status': DASHBOARD_REFRESH_STATUS_FAILED,
                'finished_at': _utcnow().isoformat(),
                'last_error': _serialize_error(
                    code='dashboard_refresh_task_failed',
                    message=str(exc),
                    retryable=True,
                ),
            }

    async def _store_dashboard_caches_from_overview(
        self,
        *,
        market_date: date,
        symbol: str | None,
        overview: dict[str, Any],
        refresh_state: dict[str, Any],
        live_holding_symbols: set[str] | None = None,
        live_portfolio_holding_membership: (
            PortfolioHoldingSymbolMembership | None
        ) = None,
        live_exec_monitoring_membership: ExecRuntimeMonitoredSymbolMembership | None = None,
    ) -> None:
        if symbol:
            detail_cache_key = self._dashboard_cache_key(
                DASHBOARD_ENDPOINT_SYMBOL_DETAIL,
                market_date,
                symbol,
            )
            detail_lock = self._dashboard_build_locks.get(detail_cache_key)
            if detail_lock is None:
                detail_lock = asyncio.Lock()
                self._dashboard_build_locks[detail_cache_key] = detail_lock
            async with detail_lock:
                detail = self._build_runtime_symbol_detail_payload(
                    market_date=market_date,
                    symbol=symbol,
                    refresh=True,
                    overview=overview,
                    refresh_state=refresh_state,
                    served_from_cache=False,
                )
                self._store_dashboard_cached_payload(
                    endpoint=DASHBOARD_ENDPOINT_SYMBOL_DETAIL,
                    market_date=market_date,
                    symbol=symbol,
                    payload=detail,
                )
            return

        board_cache_key = self._dashboard_cache_key(
            DASHBOARD_ENDPOINT_DAILY_BOARD,
            market_date,
            None,
        )
        board_lock = self._dashboard_build_locks.get(board_cache_key)
        if board_lock is None:
            board_lock = asyncio.Lock()
            self._dashboard_build_locks[board_cache_key] = board_lock
        async with board_lock:
            board = self._build_runtime_daily_board_payload(
                market_date=market_date,
                refresh=True,
                overview=overview,
                refresh_state=refresh_state,
                served_from_cache=False,
            )
            self._store_dashboard_cached_payload(
                endpoint=DASHBOARD_ENDPOINT_DAILY_BOARD,
                market_date=market_date,
                symbol=None,
                payload=board,
            )

        monitored_cache_key = self._dashboard_cache_key(
            DASHBOARD_ENDPOINT_MONITORED_SYMBOLS,
            market_date,
            None,
        )
        monitored_lock = self._dashboard_build_locks.get(
            monitored_cache_key
        )
        if monitored_lock is None:
            monitored_lock = asyncio.Lock()
            self._dashboard_build_locks[monitored_cache_key] = (
                monitored_lock
            )
        async with monitored_lock:
            monitored = self._build_runtime_monitored_symbols_payload(
                board=board,
                refresh=True,
                refresh_state=refresh_state,
                served_from_cache=False,
                live_holding_symbols=live_holding_symbols,
                live_portfolio_holding_membership=(
                    live_portfolio_holding_membership
                ),
                live_exec_monitoring_membership=(
                    live_exec_monitoring_membership
                ),
            )
            self._store_dashboard_cached_payload(
                endpoint=DASHBOARD_ENDPOINT_MONITORED_SYMBOLS,
                market_date=market_date,
                symbol=None,
                payload=monitored,
            )

    def _load_dashboard_cached_payload(
        self,
        *,
        endpoint: str,
        market_date: date,
        symbol: str | None,
        refresh_state: dict[str, Any],
        allow_stale: bool,
    ) -> dict[str, Any] | None:
        record = self._dashboard_cache.get(self._dashboard_cache_key(endpoint, market_date, symbol))
        if record is None:
            return None
        if not allow_stale and self._dashboard_cache_is_due(record):
            return None
        return self._decorate_dashboard_cached_payload(record, refresh_state=refresh_state)

    async def _build_dashboard_payload_singleflight(
        self,
        *,
        endpoint: str,
        market_date: date,
        symbol: str | None,
        refresh_state: dict[str, Any],
        build,
    ) -> dict[str, Any]:
        cache_key = self._dashboard_cache_key(endpoint, market_date, symbol)
        lock = self._dashboard_build_locks.get(cache_key)
        if lock is None:
            lock = asyncio.Lock()
            self._dashboard_build_locks[cache_key] = lock
        async with lock:
            cached = self._load_dashboard_cached_payload(
                endpoint=endpoint,
                market_date=market_date,
                symbol=symbol,
                refresh_state=refresh_state,
                allow_stale=False,
            )
            if cached is not None:
                return cached
            return await build()

    def _store_dashboard_cached_payload(
        self,
        *,
        endpoint: str,
        market_date: date,
        symbol: str | None,
        payload: dict[str, Any],
    ) -> None:
        cache_key = self._dashboard_cache_key(endpoint, market_date, symbol)
        self._dashboard_cache[cache_key] = DashboardReadModelCacheRecord(
            payload=copy.deepcopy(payload),
            cached_at=_utcnow(),
        )
        self._prune_dashboard_cache()

    def _decorate_dashboard_cached_payload(
        self,
        record: DashboardReadModelCacheRecord,
        *,
        refresh_state: dict[str, Any],
    ) -> dict[str, Any]:
        payload = copy.deepcopy(record.payload)
        request_payload = payload.get('request') if isinstance(payload.get('request'), dict) else {}
        request_payload['refresh'] = bool(refresh_state.get('requested'))
        payload['request'] = request_payload
        freshness = payload.get('freshness') if isinstance(payload.get('freshness'), dict) else {}
        stale_by_cache = self._dashboard_cache_is_due(record)
        refreshing = self._dashboard_refresh_in_flight(refresh_state)
        refresh_failed = refresh_state.get('status') in {
            DASHBOARD_REFRESH_STATUS_FAILED,
            DASHBOARD_REFRESH_STATUS_TIMEOUT,
        }
        if refreshing:
            status = DASHBOARD_FRESHNESS_STATUS_REFRESHING
        elif freshness.get('status') == DASHBOARD_FRESHNESS_STATUS_UNAVAILABLE:
            status = DASHBOARD_FRESHNESS_STATUS_UNAVAILABLE
        elif stale_by_cache or freshness.get('is_stale'):
            status = DASHBOARD_FRESHNESS_STATUS_STALE
        elif refresh_failed or freshness.get('is_partial'):
            status = DASHBOARD_FRESHNESS_STATUS_PARTIAL
        else:
            status = DASHBOARD_FRESHNESS_STATUS_FRESH
        freshness = {
            **freshness,
            'status': status,
            'served_from_cache': True,
            'read_model_source': 'in_process_cache',
            'cached_at': record.cached_at.isoformat(),
            'cache_age_seconds': self._dashboard_cache_age_seconds(record),
            'cache_ttl_seconds': self.dashboard_cache_ttl_seconds,
            'is_stale': bool(stale_by_cache or freshness.get('is_stale')),
            'is_partial': bool(
                refresh_failed or freshness.get('is_partial')
            ),
            'refreshing': refreshing,
        }
        payload['freshness'] = freshness
        payload['refresh'] = refresh_state
        return payload

    def _build_dashboard_freshness(
        self,
        *,
        endpoint: str,
        market_date: date,
        generated_at: datetime,
        served_from_cache: bool,
        runtime_status: str,
        overview_items: Sequence[dict[str, Any]],
        output_item_count: int,
        refresh_state: dict[str, Any],
        read_model_availability_status: str | None = None,
    ) -> dict[str, Any]:
        last_readback_synced_at = _latest_iso_timestamp(
            (item.get('remote_state') or {}).get('last_synced_at')
            for item in overview_items
        )
        readback_synced_count = sum(
            1
            for item in overview_items
            if _normalize_optional_text((item.get('remote_state') or {}).get('last_synced_at'))
        )
        readback_missing_count = max(len(overview_items) - readback_synced_count, 0)
        readback_age_seconds = self._seconds_since(last_readback_synced_at)
        stale_by_readback = (
            readback_age_seconds is not None
            and self.dashboard_cache_ttl_seconds > 0
            and readback_age_seconds > self.dashboard_cache_ttl_seconds
        )
        status = self._resolve_dashboard_freshness_status(
            output_item_count=output_item_count,
            overview_item_count=len(overview_items),
            readback_synced_count=readback_synced_count,
            readback_missing_count=readback_missing_count,
            stale_by_readback=stale_by_readback,
            runtime_status=runtime_status,
            refresh_state=refresh_state,
            read_model_availability_status=read_model_availability_status,
        )
        return serialize_execution_dashboard_freshness_payload({
            'schema_version': DASHBOARD_READ_MODEL_SCHEMA_VERSION,
            'endpoint': endpoint,
            'market_date': market_date.isoformat(),
            'status': status,
            'served_from_cache': served_from_cache,
            'read_model_source': 'in_process_cache' if served_from_cache else 'persisted_exec_read_model',
            'generated_at': generated_at.isoformat(),
            'cached_at': generated_at.isoformat(),
            'cache_age_seconds': 0,
            'cache_ttl_seconds': self.dashboard_cache_ttl_seconds,
            'is_stale': bool(
                stale_by_readback
                or status in {DASHBOARD_FRESHNESS_STATUS_STALE, DASHBOARD_FRESHNESS_STATUS_UNAVAILABLE}
            ),
            'is_partial': bool(readback_missing_count > 0 or status == DASHBOARD_FRESHNESS_STATUS_PARTIAL),
            'refreshing': self._dashboard_refresh_in_flight(refresh_state),
            'last_readback_synced_at': last_readback_synced_at,
            'readback_age_seconds': readback_age_seconds,
            'readback_synced_count': readback_synced_count,
            'readback_missing_count': readback_missing_count,
            'overview_item_count': len(overview_items),
            'output_item_count': output_item_count,
        })

    def _build_runtime_monitored_freshness(
        self,
        *,
        source_freshness: object,
        endpoint: str,
        market_date: date,
        generated_at: datetime,
        served_from_cache: bool,
        runtime_status: str,
        overview_items: Sequence[dict[str, Any]],
        output_item_count: int,
        refresh_state: dict[str, Any],
    ) -> dict[str, Any]:
        if (
            not isinstance(source_freshness, dict)
            or source_freshness.get('schema_version')
            != DASHBOARD_READ_MODEL_SCHEMA_VERSION
            or source_freshness.get('endpoint')
            != DASHBOARD_ENDPOINT_DAILY_BOARD
            or source_freshness.get('market_date') != market_date.isoformat()
            or source_freshness.get('status')
            not in {
                DASHBOARD_FRESHNESS_STATUS_FRESH,
                DASHBOARD_FRESHNESS_STATUS_STALE,
                DASHBOARD_FRESHNESS_STATUS_PARTIAL,
                DASHBOARD_FRESHNESS_STATUS_REFRESHING,
                DASHBOARD_FRESHNESS_STATUS_UNAVAILABLE,
            }
            or _parse_iso_datetime(source_freshness.get('generated_at')) is None
        ):
            fallback = self._build_dashboard_freshness(
                endpoint=endpoint,
                market_date=market_date,
                generated_at=generated_at,
                served_from_cache=served_from_cache,
                runtime_status=runtime_status,
                overview_items=overview_items,
                output_item_count=output_item_count,
                refresh_state=refresh_state,
            )
            return serialize_execution_dashboard_freshness_payload({
                **fallback,
                'status': DASHBOARD_FRESHNESS_STATUS_PARTIAL,
                'is_partial': True,
            })

        source_status = source_freshness['status']
        source_refreshing = bool(
            source_status == DASHBOARD_FRESHNESS_STATUS_REFRESHING
            or source_freshness.get('refreshing')
        )
        source_unavailable = source_status == DASHBOARD_FRESHNESS_STATUS_UNAVAILABLE
        source_stale = bool(
            source_status == DASHBOARD_FRESHNESS_STATUS_STALE
            or source_freshness.get('is_stale')
        )
        source_partial = bool(
            source_status == DASHBOARD_FRESHNESS_STATUS_PARTIAL
            or source_freshness.get('is_partial')
            or refresh_state.get('status')
            in {
                DASHBOARD_REFRESH_STATUS_FAILED,
                DASHBOARD_REFRESH_STATUS_TIMEOUT,
            }
        )
        refreshing = bool(
            self._dashboard_refresh_in_flight(refresh_state)
            or source_refreshing
        )
        if refreshing:
            status = DASHBOARD_FRESHNESS_STATUS_REFRESHING
        elif source_unavailable:
            status = DASHBOARD_FRESHNESS_STATUS_UNAVAILABLE
        elif source_stale:
            status = DASHBOARD_FRESHNESS_STATUS_STALE
        elif source_partial:
            status = DASHBOARD_FRESHNESS_STATUS_PARTIAL
        else:
            status = DASHBOARD_FRESHNESS_STATUS_FRESH

        effective_served_from_cache = bool(
            served_from_cache or source_freshness.get('served_from_cache')
        )

        def nonnegative_int(field: str, default: int = 0) -> int:
            value = source_freshness.get(field)
            return value if isinstance(value, int) and value >= 0 else default

        readback_age_seconds = source_freshness.get('readback_age_seconds')
        if not isinstance(readback_age_seconds, int) or readback_age_seconds < 0:
            readback_age_seconds = None
        last_readback_synced_at = source_freshness.get(
            'last_readback_synced_at'
        )
        if not isinstance(last_readback_synced_at, str):
            last_readback_synced_at = None

        return serialize_execution_dashboard_freshness_payload({
            'schema_version': DASHBOARD_READ_MODEL_SCHEMA_VERSION,
            'endpoint': endpoint,
            'market_date': market_date.isoformat(),
            'status': status,
            'served_from_cache': effective_served_from_cache,
            'read_model_source': (
                'in_process_cache'
                if effective_served_from_cache
                else source_freshness.get('read_model_source')
                or 'persisted_exec_read_model'
            ),
            'generated_at': generated_at.isoformat(),
            'cached_at': generated_at.isoformat(),
            'cache_age_seconds': nonnegative_int('cache_age_seconds'),
            'cache_ttl_seconds': self.dashboard_cache_ttl_seconds,
            'is_stale': bool(
                source_stale
                or status
                in {
                    DASHBOARD_FRESHNESS_STATUS_STALE,
                    DASHBOARD_FRESHNESS_STATUS_UNAVAILABLE,
                }
            ),
            'is_partial': bool(
                source_partial
                or status == DASHBOARD_FRESHNESS_STATUS_PARTIAL
            ),
            'refreshing': refreshing,
            'last_readback_synced_at': last_readback_synced_at,
            'readback_age_seconds': readback_age_seconds,
            'readback_synced_count': nonnegative_int('readback_synced_count'),
            'readback_missing_count': nonnegative_int('readback_missing_count'),
            'overview_item_count': nonnegative_int(
                'overview_item_count',
                len(overview_items),
            ),
            'output_item_count': output_item_count,
        })

    def _resolve_dashboard_freshness_status(
        self,
        *,
        output_item_count: int,
        overview_item_count: int,
        readback_synced_count: int,
        readback_missing_count: int,
        stale_by_readback: bool,
        runtime_status: str,
        refresh_state: dict[str, Any],
        read_model_availability_status: str | None = None,
    ) -> str:
        if self._dashboard_refresh_in_flight(refresh_state):
            return DASHBOARD_FRESHNESS_STATUS_REFRESHING
        if read_model_availability_status == 'unavailable':
            return DASHBOARD_FRESHNESS_STATUS_UNAVAILABLE
        if output_item_count == 0 and overview_item_count == 0:
            return DASHBOARD_FRESHNESS_STATUS_FRESH
        if runtime_status in {UI_RUNTIME_STATUS_OFFLINE, UI_RUNTIME_STATUS_DEGRADED} and readback_synced_count == 0:
            return DASHBOARD_FRESHNESS_STATUS_PARTIAL
        if readback_missing_count > 0:
            return DASHBOARD_FRESHNESS_STATUS_PARTIAL
        if stale_by_readback:
            return DASHBOARD_FRESHNESS_STATUS_STALE
        return DASHBOARD_FRESHNESS_STATUS_FRESH

    def _dashboard_refresh_payload(
        self,
        key: tuple[str, str | None, str | None],
        *,
        requested: bool,
        status: str | None = None,
    ) -> dict[str, Any]:
        state = self._dashboard_refresh_state.get(key) or {}
        task = self._dashboard_refresh_tasks.get(key)
        in_flight = task is not None and not task.done()
        resolved_status = status or state.get('status') or DASHBOARD_REFRESH_STATUS_NOT_REQUESTED
        return {
            'requested': requested,
            'status': resolved_status,
            'background': requested and resolved_status != DASHBOARD_REFRESH_STATUS_NOT_REQUESTED,
            'in_flight': in_flight,
            'debounced': resolved_status == DASHBOARD_REFRESH_STATUS_DEBOUNCED,
            'timeout_seconds': state.get('timeout_seconds', self.dashboard_refresh_timeout_seconds),
            'debounce_seconds': self.dashboard_refresh_debounce_seconds,
            'last_trigger': state.get('last_trigger'),
            'last_attempted_at': state.get('last_attempted_at'),
            'started_at': state.get('started_at'),
            'finished_at': state.get('finished_at'),
            'last_succeeded_at': state.get('last_succeeded_at'),
            'last_error': state.get('last_error'),
        }

    @staticmethod
    def _dashboard_refresh_key(
        *,
        target_trade_date: date,
        account_reference: str | None,
        symbol: str | None,
    ) -> tuple[str, str | None, str | None]:
        return (
            target_trade_date.isoformat(),
            _normalize_optional_text(account_reference),
            _normalize_optional_text(symbol),
        )

    def _resolve_dashboard_refresh_timeout(self, timeout_seconds: float | None) -> float:
        if timeout_seconds is None:
            return self.dashboard_refresh_timeout_seconds
        return max(float(timeout_seconds), 0.0)

    @staticmethod
    def _dashboard_cache_key(
        endpoint: str,
        market_date: date,
        symbol: str | None,
    ) -> tuple[str, str, str | None]:
        return (
            endpoint,
            market_date.isoformat(),
            _normalize_optional_text(symbol),
        )

    @staticmethod
    def _dashboard_refresh_in_flight(refresh_state: dict[str, Any]) -> bool:
        return bool(refresh_state.get('in_flight')) or refresh_state.get('status') in {
            DASHBOARD_REFRESH_STATUS_SCHEDULED,
            DASHBOARD_REFRESH_STATUS_RUNNING,
        }

    def _dashboard_cache_is_due(self, record: DashboardReadModelCacheRecord) -> bool:
        if self.dashboard_cache_ttl_seconds <= 0:
            return True
        return self._dashboard_cache_age_seconds(record) > self.dashboard_cache_ttl_seconds

    @staticmethod
    def _dashboard_cache_age_seconds(record: DashboardReadModelCacheRecord) -> int:
        own_cache_age_seconds = max(
            int((_utcnow() - record.cached_at).total_seconds()),
            0,
        )
        freshness = (
            record.payload.get('freshness')
            if isinstance(record.payload.get('freshness'), dict)
            else {}
        )
        inherited_cache_age_seconds = freshness.get('cache_age_seconds')
        if (
            not isinstance(inherited_cache_age_seconds, int)
            or isinstance(inherited_cache_age_seconds, bool)
            or inherited_cache_age_seconds < 0
        ):
            inherited_cache_age_seconds = 0
        return inherited_cache_age_seconds + own_cache_age_seconds

    @staticmethod
    def _seconds_since(value: Any) -> int | None:
        parsed = _parse_iso_datetime(value)
        if parsed is None:
            return None
        return max(int((_utcnow() - parsed).total_seconds()), 0)

    def _prune_dashboard_cache(self) -> None:
        max_entries = 96
        if len(self._dashboard_cache) <= max_entries:
            return
        sorted_items = sorted(self._dashboard_cache.items(), key=lambda item: item[1].cached_at)
        for cache_key, _ in sorted_items[: len(self._dashboard_cache) - max_entries]:
            self._dashboard_cache.pop(cache_key, None)

    def _load_next_open_rows(
        self,
        *,
        target_trade_date: date,
        account_reference: str | None = None,
        symbol: str | None = None,
        include_payloads: bool = True,
    ) -> list[SignalDeciderExecutionPlanItem]:
        with session_scope() as session:
            query = (
                select(SignalDeciderExecutionPlanItem)
                .where(
                    SignalDeciderExecutionPlanItem.execution_window == EXECUTION_WINDOW_NEXT_OPEN,
                    SignalDeciderExecutionPlanItem.target_trade_date == target_trade_date,
                )
                .order_by(
                    SignalDeciderExecutionPlanItem.account_reference.asc(),
                    SignalDeciderExecutionPlanItem.decision_agent_name.asc(),
                    SignalDeciderExecutionPlanItem.symbol.asc(),
                    SignalDeciderExecutionPlanItem.id.asc(),
                )
            )
            if not include_payloads:
                query = query.options(
                    defer(SignalDeciderExecutionPlanItem.validation_payload),
                    defer(SignalDeciderExecutionPlanItem.traceability_payload),
                    defer(SignalDeciderExecutionPlanItem.submission_payload),
                )
            if account_reference:
                query = query.where(SignalDeciderExecutionPlanItem.account_reference == account_reference)
            if symbol:
                query = query.where(SignalDeciderExecutionPlanItem.symbol == symbol)
            rows = session.execute(query).scalars().all()
            for row in rows:
                session.expunge(row)
            return rows

    @staticmethod
    def _fence_unproven_remote_acceptance(
        *,
        rows: Sequence[SignalDeciderExecutionPlanItem],
        trigger: str,
    ) -> int:
        candidate_ids: list[int] = []
        for row in rows:
            submission = (
                row.submission_payload
                if isinstance(row.submission_payload, dict)
                else {}
            )
            exec_dispatch = (
                submission.get('exec_dispatch')
                if isinstance(submission.get('exec_dispatch'), dict)
                else {}
            )
            if (
                _normalize_status(exec_dispatch.get('dispatch_status'))
                == EXEC_DISPATCH_STATUS_ACCEPTED
                and not _row_has_accepted_exec_dispatch(row)
            ):
                candidate_ids.append(int(row.id))
        if not candidate_ids:
            return 0

        now = _utcnow()
        fenced_count = 0
        with session_scope() as session:
            persisted_rows = session.execute(
                select(SignalDeciderExecutionPlanItem)
                .where(SignalDeciderExecutionPlanItem.id.in_(candidate_ids))
                .order_by(SignalDeciderExecutionPlanItem.id.asc())
                .with_for_update()
            ).scalars().all()
            for row in persisted_rows:
                submission = (
                    copy.deepcopy(row.submission_payload)
                    if isinstance(row.submission_payload, dict)
                    else {}
                )
                exec_dispatch = (
                    submission.get('exec_dispatch')
                    if isinstance(submission.get('exec_dispatch'), dict)
                    else {}
                )
                if (
                    _normalize_status(exec_dispatch.get('dispatch_status'))
                    != EXEC_DISPATCH_STATUS_ACCEPTED
                    or _row_has_accepted_exec_dispatch(row)
                ):
                    continue
                submission['dispatch_reconciliation'] = {
                    'status': 'manual_review_required',
                    'reason_code': 'unproven_remote_acceptance',
                    'observed_at': now.isoformat(),
                    'trigger': trigger,
                    'plan_id': exec_dispatch.get('plan_id'),
                    'raw_dispatch_status': exec_dispatch.get('dispatch_status'),
                }
                row.eligibility_status = 'blocked'
                row.execution_status = 'revocation_manual_review'
                row.submission_payload = slim_signal_decider_exec_submission_payload(
                    submission
                ).payload
                fenced_count += 1
            session.flush()
        return fenced_count

    def _load_snapshots(
        self,
        rows: Sequence[SignalDeciderExecutionPlanItem],
    ) -> dict[int, SignalDeciderDecisionSnapshot]:
        snapshot_ids = sorted({int(row.decision_snapshot_id) for row in rows if row.decision_snapshot_id is not None})
        if not snapshot_ids:
            return {}
        with session_scope() as session:
            snapshot_rows = session.execute(
                select(SignalDeciderDecisionSnapshot).where(SignalDeciderDecisionSnapshot.id.in_(snapshot_ids))
            ).scalars().all()
            snapshots: dict[int, SignalDeciderDecisionSnapshot] = {}
            for row in snapshot_rows:
                session.expunge(row)
                snapshots[row.id] = row
            return snapshots

    @staticmethod
    def _group_rows_by_dispatch_scope(
        rows: Sequence[SignalDeciderExecutionPlanItem],
    ) -> dict[tuple[str, str, str], list[SignalDeciderExecutionPlanItem]]:
        grouped: dict[
            tuple[str, str, str],
            list[SignalDeciderExecutionPlanItem],
        ] = defaultdict(list)
        for row in rows:
            route_family = _unsupported_exec_sizing_route_reason(
                row.traceability_payload
            ) or 'kis_krx_krw'
            grouped[
                (
                    row.account_reference,
                    _normalize_decision_agent_name(row.decision_agent_name),
                    route_family,
                )
            ].append(row)
        return grouped

    @staticmethod
    def _row_is_dispatchable(row: SignalDeciderExecutionPlanItem) -> bool:
        if str(row.planned_side or '').strip().lower() != 'buy':
            return False
        decision_agent_name = _normalize_decision_agent_name(row.decision_agent_name)
        if row.eligibility_status == ELIGIBILITY_MONITOR_CANDIDATE:
            if decision_agent_name != SHORT_TERM_DECIDER_AGENT_NAME:
                return False
        elif row.eligibility_status != ELIGIBILITY_ELIGIBLE:
            return False
        validation = row.validation_payload if isinstance(row.validation_payload, dict) else {}
        if decision_agent_name == SIGNAL_DECIDER_AGENT_NAME:
            if (
                validation.get('execution_cost_evidence_policy_version')
                != NEXT_OPEN_COST_EVIDENCE_POLICY_VERSION
                or validation.get('blocking_reasons')
            ):
                return False
            pre_decider_context = validation.get('pre_decider_enrichment_context')
            if (
                not isinstance(pre_decider_context, dict)
                or pre_decider_context.get('schema_version') != 'pre_decider_enrichment_v1'
            ):
                return False
            rebuilt_quality = build_decision_input_quality_envelope(
                market_date=row.source_market_date.isoformat(),
                pre_decider_enrichment_context=pre_decider_context,
            )
            execution_feasibility = rebuilt_quality['execution_feasibility']
            performance_gate = validation.get('performance_gate')
            economics = (
                performance_gate.get('expected_return_economics')
                if isinstance(performance_gate, dict)
                and isinstance(performance_gate.get('expected_return_economics'), dict)
                else {}
            )
            if economics.get('status') == 'pending_live_cost_confirmation':
                warning_codes = {
                    item.get('code')
                    for item in validation.get('warnings') or []
                    if isinstance(item, dict)
                }
                return bool(
                    DELEGATED_GATE_LIVE_COST_CODE in warning_codes
                    and 'expected_slippage_bps' in economics
                    and economics.get('expected_slippage_bps') is None
                    and economics.get('expected_slippage_basis') == 'unavailable'
                    and economics.get('server_non_spread_round_trip_cost_basis')
                    == 'deferred_to_exec_final_full_cost_check'
                    and 'estimated_total_transaction_cost_bps' in economics
                    and economics.get('estimated_total_transaction_cost_bps') is None
                    and _is_canonical_closed_market_cost_absence(
                        pre_decider_enrichment_context=pre_decider_context,
                        execution_feasibility_context=execution_feasibility,
                    )
                )
            rebuilt_slippage = _safe_decimal(execution_feasibility.get('expected_slippage_bps'))
            admitted_slippage = _safe_decimal(economics.get('expected_slippage_bps'))
            return bool(
                economics.get('status') == 'ready'
                and rebuilt_slippage is not None
                and rebuilt_slippage >= 0
                and rebuilt_slippage == admitted_slippage
                and execution_feasibility.get('expected_slippage_basis') == 'quote_spread_bps'
                and economics.get('expected_slippage_basis') == 'quote_spread_bps'
                and economics.get('server_non_spread_round_trip_cost_basis')
                == 'deferred_to_exec_final_full_cost_check'
                and economics.get('estimated_total_transaction_cost_bps') is None
            )
        if decision_agent_name != SHORT_TERM_DECIDER_AGENT_NAME:
            return False
        if validation.get('blocking_reasons'):
            return False
        traceability = row.traceability_payload if isinstance(row.traceability_payload, dict) else {}
        short_term = validation.get('short_term') if isinstance(validation.get('short_term'), dict) else {}
        handoff = validation.get('handoff') if isinstance(validation.get('handoff'), dict) else {}
        tactical_action = _normalize_optional_text(
            traceability.get('tactical_action') or short_term.get('tactical_action'),
            lowercase=True,
        )
        handoff_intent = _normalize_optional_text(
            traceability.get('handoff_intent') or handoff.get('handoff_intent') or short_term.get('handoff_intent'),
            lowercase=True,
        )
        if handoff_intent is None and tactical_action in {'scalp_buy_candidate', 'watch_for_trigger'}:
            handoff_intent = 'tactical_buy' if tactical_action == 'scalp_buy_candidate' else EXEC_INTENT_INTRADAY_WATCH_BUY
        return (
            tactical_action in {'scalp_buy_candidate', 'watch_for_trigger'}
            and handoff_intent in {'tactical_buy', EXEC_INTENT_INTRADAY_WATCH_BUY}
            and _positive_decimal(traceability.get('trigger_price') or short_term.get('trigger_price')) is not None
            and _positive_decimal(traceability.get('chase_limit_price') or short_term.get('chase_limit_price')) is not None
            and _positive_decimal(traceability.get('target_price') or short_term.get('target_price')) is not None
            and _positive_decimal(traceability.get('stop_loss_price') or short_term.get('stop_loss_price')) is not None
        )

    def _build_decider_handoff_context(
        self,
        *,
        row: SignalDeciderExecutionPlanItem,
        snapshot: SignalDeciderDecisionSnapshot | None,
        traceability: dict[str, Any],
        validation: dict[str, Any],
        decision_traceability: dict[str, Any],
    ) -> dict[str, Any]:
        row_decision_agent_name = _normalize_decision_agent_name(row.decision_agent_name)
        pre_decider_enrichment_context = (
            traceability.get('pre_decider_enrichment_context')
            if isinstance(traceability.get('pre_decider_enrichment_context'), dict)
            else validation.get('pre_decider_enrichment_context')
            if isinstance(validation.get('pre_decider_enrichment_context'), dict)
            else {}
        )
        request_payload = (
            snapshot.request_payload
            if snapshot is not None and isinstance(snapshot.request_payload, dict)
            else {}
        )
        if not pre_decider_enrichment_context:
            request_pre_decider = request_payload.get('pre_decider_enrichment_context')
            symbol_day_context = request_payload.get('symbol_day_context')
            if isinstance(request_pre_decider, dict):
                pre_decider_enrichment_context = request_pre_decider
            elif isinstance(symbol_day_context, dict) and isinstance(
                symbol_day_context.get('pre_decider_enrichment_context'),
                dict,
            ):
                pre_decider_enrichment_context = symbol_day_context['pre_decider_enrichment_context']
        input_feature_snapshot_payload = (
            snapshot.input_feature_snapshot_payload
            if snapshot is not None and isinstance(snapshot.input_feature_snapshot_payload, dict)
            else {}
        )
        response_payload = (
            snapshot.response_payload
            if snapshot is not None and isinstance(snapshot.response_payload, dict)
            else {}
        )
        response_summary_payload = (
            snapshot.response_summary_payload
            if snapshot is not None and isinstance(snapshot.response_summary_payload, dict)
            else {}
        )
        decision_input_quality_context = _decision_input_quality_from_payloads(
            symbol=row.symbol,
            source_market_date=row.source_market_date,
            request_payload=request_payload,
            validation=validation,
            traceability=traceability,
            allocation_budget_krw=row.allocation_budget_krw,
            estimated_reference_price=row.estimated_reference_price,
            planned_notional_krw=row.estimated_notional_krw,
        )
        market_regime_context = _market_regime_context_from_payloads(
            request_payload=request_payload,
            validation=validation,
            traceability=traceability,
        )
        request_payload = _strip_signal_decider_only_labels(
            request_payload,
            decision_agent_name=row_decision_agent_name,
        )
        response_payload = _strip_signal_decider_only_labels(
            response_payload,
            decision_agent_name=row_decision_agent_name,
        )
        raw_action = traceability.get('raw_action') or traceability.get('tactical_action')
        generic_action = _normalize_short_term_generic_action(
            raw_action=raw_action,
            generic_action=traceability.get('generic_action'),
        )
        handoff_intent = traceability.get('handoff_intent')
        if handoff_intent is None and row_decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME:
            if raw_action == 'scalp_buy_candidate':
                handoff_intent = 'tactical_buy'
            elif raw_action == 'watch_for_trigger':
                handoff_intent = EXEC_INTENT_INTRADAY_WATCH_BUY
        alignment_gate = (
            traceability.get('alignment_gate')
            if isinstance(traceability.get('alignment_gate'), dict)
            else validation.get('alignment_gate')
            if isinstance(validation.get('alignment_gate'), dict)
            else None
        )
        final_enforcement_decision = (
            alignment_gate.get('enforcement_decision')
            if isinstance(alignment_gate, dict)
            else traceability.get('final_enforcement_decision')
            or validation.get('final_enforcement_decision')
        )
        requested_quantity = (
            alignment_gate.get('requested_quantity')
            if isinstance(alignment_gate, dict)
            else traceability.get('requested_quantity')
            if traceability.get('requested_quantity') is not None
            else validation.get('requested_quantity')
        )
        final_quantity = (
            alignment_gate.get('final_quantity')
            if isinstance(alignment_gate, dict)
            else traceability.get('final_quantity')
            if traceability.get('final_quantity') is not None
            else validation.get('final_quantity')
        )
        return serialize_exec_agent_decider_handoff_payload(
            _json_safe(
                {
                    'schema_version': EXEC_AGENT_DECIDER_HANDOFF_SCHEMA_VERSION,
                    'classification': EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING,
                    'symbol': row.symbol,
                    'target_trade_date': row.target_trade_date.isoformat(),
                    'source_market_date': row.source_market_date.isoformat(),
                    'traceability': {
                        'core_plan_item_id': row.id,
                        'core_client_order_id': row.client_order_id,
                        'decision_snapshot_id': row.decision_snapshot_id,
                        'feedback_id': traceability.get('source_feedback_id') or traceability.get('feedback_id'),
                        'source_feedback_id': traceability.get('source_feedback_id') or traceability.get('feedback_id'),
                        'decision_run_id': traceability.get('decision_run_id'),
                        'policy_version': row.policy_version,
                        'decision_agent_name': row_decision_agent_name,
                        'raw_action': raw_action,
                        'tactical_action': traceability.get('tactical_action'),
                        'generic_action': generic_action,
                        'handoff_intent': handoff_intent,
                        'signal_decider_alignment': traceability.get('signal_decider_alignment')
                        or validation.get('signal_decider_alignment'),
                        'alignment_gate_action': traceability.get('alignment_gate_action')
                        or validation.get('alignment_gate_action'),
                        'alignment_gate_reason': traceability.get('alignment_gate_reason')
                        or validation.get('alignment_gate_reason'),
                        'alignment_signal_decider_decision_snapshot_id': traceability.get(
                            'alignment_signal_decider_decision_snapshot_id'
                        )
                        or validation.get('alignment_signal_decider_decision_snapshot_id'),
                        'alignment_gate': alignment_gate,
                        'final_enforcement_decision': final_enforcement_decision,
                        'requested_quantity': requested_quantity,
                        'final_quantity': final_quantity,
                        'decision_traceability': decision_traceability,
                    },
                    'decider_input_context': {
                        'request_payload': request_payload,
                        'input_feature_snapshot_payload': input_feature_snapshot_payload,
                        'pre_decider_enrichment_context': pre_decider_enrichment_context,
                        'market_regime_context': market_regime_context,
                        'decision_input_quality_context': decision_input_quality_context,
                    },
                    'market_regime_context': market_regime_context,
                    'decision_input_quality_context': decision_input_quality_context,
                    'decider_output': {
                        'decision_action': snapshot.decision_action if snapshot is not None else None,
                        'preferred_action': snapshot.preferred_action if snapshot is not None else None,
                        'next_day_open_action': snapshot.next_day_open_action if snapshot is not None else None,
                        'raw_action': raw_action,
                        'tactical_action': traceability.get('tactical_action'),
                        'generic_action': generic_action,
                        'handoff_intent': handoff_intent,
                        'signal_decider_alignment': traceability.get('signal_decider_alignment')
                        or validation.get('signal_decider_alignment'),
                        'alignment_gate_action': traceability.get('alignment_gate_action')
                        or validation.get('alignment_gate_action'),
                        'alignment_gate_reason': traceability.get('alignment_gate_reason')
                        or validation.get('alignment_gate_reason'),
                        'alignment_gate': alignment_gate,
                        'final_enforcement_decision': final_enforcement_decision,
                        'requested_quantity': requested_quantity,
                        'final_quantity': final_quantity,
                        'entry_price_basis': snapshot.entry_price_basis
                        if snapshot is not None
                        else traceability.get('entry_price_basis'),
                        'trigger_price': traceability.get('trigger_price'),
                        'chase_limit_price': traceability.get('chase_limit_price'),
                        'target_price': traceability.get('target_price'),
                        'stop_loss_price': traceability.get('stop_loss_price'),
                        'expected_return_pct': snapshot.expected_return_pct if snapshot is not None else None,
                        'confidence_score_10': snapshot.confidence_score_10
                        if snapshot is not None
                        else traceability.get('confidence_score_10'),
                        'conviction_score_10': snapshot.conviction_score_10
                        if snapshot is not None
                        else traceability.get('conviction_score_10'),
                        'thesis': snapshot.thesis_summary if snapshot is not None else traceability.get('thesis_summary'),
                        'thesis_summary': snapshot.thesis_summary
                        if snapshot is not None
                        else traceability.get('thesis_summary'),
                        'supporting_evidence': snapshot.supporting_evidence_payload
                        if snapshot is not None
                        else traceability.get('supporting_evidence') or [],
                        'trigger_conditions': response_payload.get('trigger_conditions')
                        or response_payload.get('trigger_conditions_payload')
                        or traceability.get('trigger_conditions')
                        or traceability.get('invalidation_conditions')
                        or (snapshot.invalidation_conditions_payload if snapshot is not None else []),
                        'risk_flags': response_payload.get('risk_flags')
                        or response_payload.get('tactical_risks')
                        or traceability.get('risk_flags')
                        or traceability.get('decision_risks')
                        or (snapshot.decision_risks_payload if snapshot is not None else []),
                        'data_quality_warnings': snapshot.data_quality_warnings_payload
                        if snapshot is not None
                        else traceability.get('data_quality_warnings') or [],
                        'response_payload': response_payload,
                        'response_summary_payload': response_summary_payload,
                    },
                    'eod_pre_decider_enrichment_metadata': pre_decider_enrichment_context,
                }
            )
        )

    def _build_exec_plan_payload(
        self,
        *,
        rows: Sequence[SignalDeciderExecutionPlanItem],
        snapshots: dict[int, SignalDeciderDecisionSnapshot],
        trigger: str,
        delta_batch_key: str | None = None,
    ) -> tuple[dict[str, Any], dict[int, list[dict[str, Any]]]]:
        first_row = rows[0]
        decision_agent_name = _normalize_decision_agent_name(first_row.decision_agent_name)
        source_market_date = first_row.source_market_date.isoformat()
        plan_id = (
            f'core-{_sanitize_key(decision_agent_name)}-{first_row.target_trade_date.strftime("%Y%m%d")}-'
            f'{_sanitize_key(first_row.account_reference)}'
        )
        # Exec treats plan_id as an idempotency key.  The candidate set can
        # legitimately change during the same trading day (for example after
        # fresh market evidence arrives), so the account/day base id alone is
        # not a stable identity for the submitted material.  Always bind the
        # id to the exact row generation.  Replays of the same generation keep
        # the same id, while a changed candidate set cannot collide with an
        # earlier rejected or quarantined Exec plan.
        effective_batch_key = delta_batch_key or _delta_dispatch_batch_key(rows)
        plan_id = _delta_dispatch_plan_id(plan_id, effective_batch_key)
        runtime_mode = _normalize_optional_text(settings.signal_decider_exec_runtime_mode, lowercase=True) or 'mock'
        dispatch_source = EXECUTION_HANDOFF_SOURCE
        if runtime_mode == 'live':
            (
                configured_account_targets,
                account_target_errors,
            ) = signal_decider_next_open_execution_service._parse_account_targets()
            expected_account_reference = (
                configured_account_targets[0].account_reference
                if not account_target_errors
                and len(configured_account_targets) == 1
                and configured_account_targets[0].account_reference
                != UNSPECIFIED_ROUTE_ACCOUNT_REFERENCE
                else None
            )
            if expected_account_reference is None or any(
                row.account_reference != expected_account_reference for row in rows
            ):
                raise SignalDeciderExecPayloadContractError(
                    f'{LIVE_EXEC_ACCOUNT_TARGET_REQUIRED}:'
                    'live handoff rows must exactly match the one explicit KIS account target'
                )
        items: list[dict[str, Any]] = []
        row_links: dict[int, list[dict[str, Any]]] = defaultdict(list)
        requested_at = _utcnow().isoformat()

        for row in rows:
            snapshot = snapshots.get(int(row.decision_snapshot_id))
            row_decision_agent_name = _normalize_decision_agent_name(row.decision_agent_name)
            traceability = row.traceability_payload if isinstance(row.traceability_payload, dict) else {}
            unsupported_route_reason = _unsupported_exec_sizing_route_reason(
                traceability
            )
            if unsupported_route_reason is not None:
                raise SignalDeciderExecPayloadContractError(
                    f'{row_decision_agent_name}:{row.id}:'
                    f'{unsupported_route_reason}'
                )
            execution_scope = {
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
            }
            validation = row.validation_payload if isinstance(row.validation_payload, dict) else {}
            validation = _strip_signal_decider_fixed_sizing_preview(
                validation,
                decision_agent_name=row_decision_agent_name,
            )
            is_short_term = row_decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME
            short_term_context = (
                validation.get('short_term')
                if isinstance(validation.get('short_term'), dict)
                else {}
            )
            decision_traceability = traceability.get('decision_traceability') or traceability.get('signal_traceability') or {}
            if not isinstance(decision_traceability, dict):
                decision_traceability = {}
            response_summary = (
                short_term_context.get('response_summary')
                if short_term_context.get('response_summary') is not None
                else traceability.get('response_summary')
            )
            response_payload_summary = (
                short_term_context.get('response_payload_summary')
                if short_term_context.get('response_payload_summary') is not None
                else traceability.get('response_payload_summary')
            )
            if isinstance(response_payload_summary, dict):
                response_payload_summary = _strip_signal_decider_only_labels(
                    response_payload_summary,
                    decision_agent_name=row_decision_agent_name,
                )
            quantity = _safe_decimal(row.planned_quantity)
            uses_exec_agent_sizing = row_decision_agent_name in {
                SIGNAL_DECIDER_AGENT_NAME,
                SHORT_TERM_DECIDER_AGENT_NAME,
            }
            target_price = snapshot.target_price if snapshot is not None else traceability.get('target_price')
            stop_loss_price = snapshot.stop_loss_price if snapshot is not None else traceability.get('stop_loss_price')
            trigger_price = traceability.get('trigger_price')
            chase_limit_price = traceability.get('chase_limit_price')
            raw_action = traceability.get('raw_action') or traceability.get('tactical_action')
            generic_action = _normalize_short_term_generic_action(
                raw_action=raw_action,
                generic_action=traceability.get('generic_action'),
            )
            handoff_intent = (
                _normalize_optional_text(traceability.get('handoff_intent'), lowercase=True)
                or (
                    'tactical_buy'
                    if is_short_term and raw_action == 'scalp_buy_candidate'
                    else EXEC_INTENT_INTRADAY_WATCH_BUY
                    if is_short_term
                    else EXEC_INTENT_NEXT_OPEN_BUY
                )
            )
            entry_intent = EXEC_INTENT_INTRADAY_WATCH_BUY if is_short_term else EXEC_INTENT_NEXT_OPEN_BUY
            entry_execution_mode = (
                _normalize_optional_text(traceability.get('entry_execution_mode'))
                or ('pending_trigger_limit' if is_short_term else 'next_open_market')
            )
            core_entry_key = f'core-plan-{row.id}:{entry_intent}'
            full_decider_handoff_context = self._build_decider_handoff_context(
                row=row,
                snapshot=snapshot,
                traceability=traceability,
                validation=validation,
                decision_traceability=decision_traceability,
            )
            decision_input_quality_context = (
                full_decider_handoff_context.get('decision_input_quality_context')
                if isinstance(full_decider_handoff_context.get('decision_input_quality_context'), dict)
                else {}
            )
            market_regime_context = (
                full_decider_handoff_context.get('market_regime_context')
                if isinstance(full_decider_handoff_context.get('market_regime_context'), dict)
                else {}
            )
            market_regime_sizing_policy = _build_market_regime_sizing_policy(market_regime_context)
            execution_feasibility_context = (
                decision_input_quality_context.get('execution_feasibility')
                if isinstance(decision_input_quality_context.get('execution_feasibility'), dict)
                else {}
            )
            requested_delegated_gate_codes = _delegated_gate_codes(
                validation,
                decision_agent_name=row_decision_agent_name,
            )
            delegated_gate_evaluated_at = _utcnow()
            delegated_gates = _delegated_gates(
                row=row,
                snapshot=snapshot,
                validation=validation,
                codes=requested_delegated_gate_codes,
                now=delegated_gate_evaluated_at,
            )
            delegated_gate_error = _delegated_gate_contract_error(
                decision_agent_name=row_decision_agent_name,
                codes=requested_delegated_gate_codes,
                confirmation_required=bool(requested_delegated_gate_codes),
                gates=delegated_gates,
                now=delegated_gate_evaluated_at,
            )
            if delegated_gate_error is not None:
                raise SignalDeciderExecPayloadContractError(
                    f'{row_decision_agent_name}:{row.id}:{delegated_gate_error}'
                )
            delegated_gate_codes = [gate['code'] for gate in delegated_gates]
            delegated_gate_confirmation_required = bool(delegated_gates)
            alignment_gate = (
                traceability.get('alignment_gate')
                if isinstance(traceability.get('alignment_gate'), dict)
                else validation.get('alignment_gate')
                if isinstance(validation.get('alignment_gate'), dict)
                else None
            )
            final_enforcement_decision = (
                alignment_gate.get('enforcement_decision')
                if isinstance(alignment_gate, dict)
                else traceability.get('final_enforcement_decision')
                or validation.get('final_enforcement_decision')
            )
            requested_enforcement_quantity = (
                _safe_decimal(alignment_gate.get('requested_quantity'))
                if isinstance(alignment_gate, dict)
                else _safe_decimal(traceability.get('requested_quantity'))
                or _safe_decimal(validation.get('requested_quantity'))
            )
            final_enforcement_quantity = (
                _safe_decimal(alignment_gate.get('final_quantity'))
                if isinstance(alignment_gate, dict)
                else _safe_decimal(traceability.get('final_quantity'))
                or _safe_decimal(validation.get('final_quantity'))
            )
            max_holding_close_policy = (
                {
                    'enabled': True,
                    'type': 'exec_agent_default',
                    'intent': 'max_holding_close_sell',
                    'max_holding_trading_sessions': 3,
                    'reason': 'short_term_exec_agent_sizing_default',
                }
                if row_decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME
                else {
                    'enabled': False,
                    'type': 'none',
                    'intent': None,
                    'max_holding_trading_sessions': None,
                    'reason': 'general_signal_decider_uses_decider_holding_context_no_short_term_forced_close',
                }
            )
            decider_handoff_context_digest = _dispatch_payload_digest(
                full_decider_handoff_context
            )
            decider_handoff_context = _bounded_dispatch_object(
                full_decider_handoff_context,
                max_bytes=EXEC_DECIDER_HANDOFF_MAX_BYTES,
                label='exec_decider_handoff_context',
                essential_keys=(
                    'schema_version',
                    'classification',
                    'symbol',
                    'target_trade_date',
                    'source_market_date',
                    'traceability',
                    'decider_input_context',
                    'market_regime_context',
                    'decision_input_quality_context',
                    'decider_output',
                    'eod_pre_decider_enrichment_metadata',
                ),
            )
            validation_projection = _bounded_dispatch_object(
                validation,
                max_bytes=6 * 1024,
                label='exec_validation_context',
                essential_keys=(
                    'blocking_reasons',
                    'warnings',
                    'short_term_entry_quality',
                    'performance_gate',
                    'alignment_gate',
                    'signal_decider_alignment',
                    'alignment_gate_action',
                    'alignment_gate_reason',
                    'alignment_signal_decider_decision_snapshot_id',
                    'pricing_note',
                ),
            )
            traceability_projection = _bounded_dispatch_object(
                decision_traceability,
                max_bytes=4 * 1024,
                label='exec_decision_traceability',
                essential_keys=(
                    'schema_version',
                    'gate_checks',
                    'decision_provenance',
                    'model_identity',
                ),
            )
            decision_input_quality_projection = _bounded_dispatch_object(
                decision_input_quality_context,
                max_bytes=6 * 1024,
                label='exec_decision_input_quality_context',
                essential_keys=(
                    'schema_version',
                    'overall_status',
                    'execution_feasibility',
                    'final_judgment',
                    'confidence_adjustment',
                ),
            )
            market_regime_projection = _bounded_dispatch_object(
                market_regime_context,
                max_bytes=4 * 1024,
                label='exec_market_regime_context',
                essential_keys=(
                    'schema_version',
                    'market_regime',
                    'posture',
                    'reason_code',
                    'recommended_exec_action',
                    'recommended_sizing_multiplier',
                    'market_breadth',
                    'sector_context',
                ),
            )
            execution_feasibility_projection = _bounded_dispatch_object(
                execution_feasibility_context,
                max_bytes=3 * 1024,
                label='exec_execution_feasibility_context',
                essential_keys=(
                    'status',
                    'score',
                    'expected_slippage_bps',
                    'expected_slippage_basis',
                    'expected_slippage_status',
                    'expected_slippage_reason',
                    'liquidity_tier',
                    'max_safe_notional_krw',
                    'sizing_hints',
                ),
            )
            common_risk_context = {
                'schema_version': EXEC_SOURCE_SCHEMA_VERSION,
                'classification': full_decider_handoff_context['classification'],
                'agent_name': row_decision_agent_name,
                'decision_agent_name': row_decision_agent_name,
                'core_plan_item_id': row.id,
                'core_client_order_id': row.client_order_id,
                'core_execution_window': row.execution_window,
                'account_reference': row.account_reference,
                'account_label': row.account_label,
                'target_trade_date': row.target_trade_date.isoformat(),
                'source_market_date': row.source_market_date.isoformat(),
                'policy_version': row.policy_version,
                'decision_snapshot_id': row.decision_snapshot_id,
                'source_feedback_id': traceability.get('source_feedback_id') or traceability.get('feedback_id'),
                'decision_run_id': traceability.get('decision_run_id'),
                'target_price': target_price,
                'stop_loss_price': stop_loss_price,
                'allocation_budget_krw': (
                    _decimal_to_text(_safe_decimal(traceability.get('allocation_budget_krw')))
                    or _decimal_to_text(_safe_decimal(row.allocation_budget_krw))
                ),
                'suggested_holding_period': traceability.get('suggested_holding_period'),
                'expected_holding_period': traceability.get('expected_holding_period'),
                'max_holding_close_policy': max_holding_close_policy,
                'raw_action': raw_action,
                'tactical_action': traceability.get('tactical_action'),
                'generic_action': generic_action,
                'handoff_intent': handoff_intent,
                'signal_decider_alignment': traceability.get('signal_decider_alignment')
                or validation.get('signal_decider_alignment'),
                'alignment_gate_action': traceability.get('alignment_gate_action')
                or validation.get('alignment_gate_action'),
                'alignment_gate_reason': traceability.get('alignment_gate_reason')
                or validation.get('alignment_gate_reason'),
                'alignment_signal_decider_decision_snapshot_id': traceability.get(
                    'alignment_signal_decider_decision_snapshot_id'
                )
                or validation.get('alignment_signal_decider_decision_snapshot_id'),
                'alignment_gate': alignment_gate,
                'final_enforcement_decision': final_enforcement_decision,
                'requested_quantity': _decimal_to_text(requested_enforcement_quantity),
                'final_quantity': _decimal_to_text(final_enforcement_quantity),
                'handoff_active': bool(traceability.get('handoff_active')) if is_short_term else True,
                'original_watch_trigger_price': trigger_price,
                'original_watch_chase_limit_price': chase_limit_price,
                'allocation_mode': (
                    _normalize_optional_text(traceability.get('allocation_mode'))
                    or (
                        'unspecified_route'
                        if _normalize_optional_text(traceability.get('account_reference')) == 'KIS:00000000-00'
                        else 'specified'
                    )
                ),
                'entry_execution_mode': entry_execution_mode,
                'monitoring_candidate': bool(is_short_term),
                'monitoring_intent': entry_intent if is_short_term else None,
                'decider_handoff_context_digest': decider_handoff_context_digest,
            }
            entry_common_risk_context = {
                **common_risk_context,
                'validation': validation_projection,
                'traceability': traceability_projection,
                'decider_handoff_context': decider_handoff_context,
                'market_regime_context': market_regime_projection,
                'market_regime_sizing_policy': market_regime_sizing_policy,
                'decision_input_quality_context': decision_input_quality_projection,
                'execution_feasibility_context': execution_feasibility_projection,
            }
            enforced_probe = bool(
                is_short_term
                and final_enforcement_decision == 'downsized'
                and final_enforcement_quantity is not None
                and final_enforcement_quantity > Decimal('0')
            )
            if enforced_probe:
                quantity_policy = {
                    'type': 'exec_agent_sized',
                    # The v4 boundary delegates the final order quantity to
                    # Exec. Core's WAIT-conflict decision is a hard ceiling,
                    # not executable fixed sizing.
                    'max_quantity': _decimal_to_text(final_enforcement_quantity),
                    'budget_krw': _decimal_to_text(_safe_decimal(row.allocation_budget_krw)),
                    'allocation_mode': traceability.get('allocation_mode') or 'specified',
                    'account_reference': row.account_reference,
                    'account_label': row.account_label,
                    'estimated_reference_price': row.estimated_reference_price,
                    'estimated_notional_krw': _decimal_to_text(_safe_decimal(row.estimated_notional_krw)),
                    'pricing_note': 'Core-enforced deterministic WAIT-conflict probe quantity.',
                    'enforcement_source': 'core_wait_conflict_alignment_gate',
                    'market_regime_sizing': market_regime_sizing_policy,
                }
            elif uses_exec_agent_sizing:
                if row_decision_agent_name == SIGNAL_DECIDER_AGENT_NAME:
                    estimated_notional_krw = None
                    pricing_note = 'Sizing for signal-decider dispatch is delegated to exec-agent.'
                else:
                    estimated_notional_krw = _decimal_to_text(_safe_decimal(row.estimated_notional_krw))
                    pricing_note = (
                        validation.get('pricing_note')
                        if isinstance(validation.get('pricing_note'), str)
                        else None
                    )
                quantity_policy = {
                    'type': 'exec_agent_sized',
                    'budget_krw': _decimal_to_text(_safe_decimal(row.allocation_budget_krw)),
                    'allocation_mode': traceability.get('allocation_mode') or 'specified',
                    'account_reference': row.account_reference,
                    'account_label': row.account_label,
                    'estimated_reference_price': row.estimated_reference_price,
                    'estimated_notional_krw': estimated_notional_krw,
                    'pricing_note': pricing_note,
                    'sizing_hints': list(execution_feasibility_context.get('sizing_hints') or []),
                    'max_safe_notional_krw': execution_feasibility_context.get('max_safe_notional_krw'),
                    'liquidity_tier': execution_feasibility_context.get('liquidity_tier'),
                    'market_regime_sizing': market_regime_sizing_policy,
                }
            else:
                quantity_policy = {
                    'type': 'fixed_quantity',
                    'quantity': _decimal_to_text(quantity),
                    'budget_krw': _decimal_to_text(_safe_decimal(row.allocation_budget_krw)),
                    'estimated_reference_price': row.estimated_reference_price,
                    'estimated_notional_krw': _decimal_to_text(_safe_decimal(row.estimated_notional_krw)),
                    'market_regime_sizing': market_regime_sizing_policy,
                }
            quantity_policy.update(
                {
                    'final_enforcement_decision': final_enforcement_decision,
                    'requested_quantity': _decimal_to_text(requested_enforcement_quantity),
                    'final_quantity': _decimal_to_text(final_enforcement_quantity),
                    'market_regime': market_regime_sizing_policy['market_regime'],
                    'quantity_multiplier': market_regime_sizing_policy['quantity_multiplier'],
                    'quantity_multiplier_cap': market_regime_sizing_policy['quantity_multiplier_cap'],
                    'entry_priority_boost': market_regime_sizing_policy['entry_priority_boost'],
                    'quantity_multiplier_valid_until': market_regime_sizing_policy['valid_until'],
                }
            )
            entry_risk_context = _bounded_dispatch_object(
                {
                    **entry_common_risk_context,
                    'delegated_gate_codes': delegated_gate_codes,
                    'delegated_gate_confirmation_required': delegated_gate_confirmation_required,
                    'delegated_gates': delegated_gates,
                    'entry_price_basis': snapshot.entry_price_basis if snapshot is not None else 'next_open',
                    'expected_return_pct': _to_json_number(
                        _safe_decimal(snapshot.expected_return_pct if snapshot else None)
                    ),
                    'confidence_score_10': _to_json_number(
                        _safe_decimal(snapshot.confidence_score_10 if snapshot is not None else None)
                    ),
                    'conviction_score_10': _to_json_number(
                        _safe_decimal(snapshot.conviction_score_10 if snapshot is not None else None)
                    ),
                    'thesis_summary': snapshot.thesis_summary if snapshot is not None else None,
                    'supporting_evidence': snapshot.supporting_evidence_payload if snapshot is not None else [],
                    'decision_risks': snapshot.decision_risks_payload if snapshot is not None else [],
                    'invalidation_conditions': snapshot.invalidation_conditions_payload if snapshot is not None else [],
                    'data_quality_warnings': snapshot.data_quality_warnings_payload if snapshot is not None else [],
                    'response_summary': response_summary if isinstance(response_summary, dict) else None,
                    'response_payload_summary': (
                        response_payload_summary
                        if isinstance(response_payload_summary, dict)
                        else None
                    ),
                },
                max_bytes=EXEC_ENTRY_RISK_CONTEXT_MAX_BYTES,
                label='exec_entry_risk_context',
                essential_keys=(
                    'schema_version',
                    'classification',
                    'agent_name',
                    'decision_agent_name',
                    'core_plan_item_id',
                    'core_client_order_id',
                    'account_reference',
                    'account_label',
                    'target_trade_date',
                    'source_market_date',
                    'policy_version',
                    'decision_snapshot_id',
                    'decision_run_id',
                    'target_price',
                    'stop_loss_price',
                    'allocation_budget_krw',
                    'entry_execution_mode',
                    'delegated_gate_codes',
                    'delegated_gate_confirmation_required',
                    'delegated_gates',
                    'validation',
                    'decider_handoff_context',
                    'decider_handoff_context_digest',
                    'market_regime_context',
                    'market_regime_sizing_policy',
                    'decision_input_quality_context',
                    'execution_feasibility_context',
                    'expected_return_pct',
                    'confidence_score_10',
                    'conviction_score_10',
                    'thesis_summary',
                    'supporting_evidence',
                    'decision_risks',
                ),
            )
            full_protective_exit_risk_context = {
                'schema_version': EXEC_SOURCE_SCHEMA_VERSION,
                'agent_name': row_decision_agent_name,
                'decision_agent_name': row_decision_agent_name,
                'core_plan_item_id': row.id,
                'account_reference': row.account_reference,
                'account_label': row.account_label,
                'target_trade_date': row.target_trade_date.isoformat(),
                'source_market_date': row.source_market_date.isoformat(),
                'policy_version': row.policy_version,
                'decision_snapshot_id': row.decision_snapshot_id,
                'decision_run_id': traceability.get('decision_run_id'),
                'target_price': target_price,
                'stop_loss_price': stop_loss_price,
                'expected_return_pct': _to_json_number(
                    _safe_decimal(snapshot.expected_return_pct if snapshot else None)
                ),
                'confidence_score_10': _to_json_number(
                    _safe_decimal(snapshot.confidence_score_10 if snapshot is not None else None)
                ),
                'conviction_score_10': _to_json_number(
                    _safe_decimal(snapshot.conviction_score_10 if snapshot is not None else None)
                ),
                'thesis_summary': snapshot.thesis_summary if snapshot is not None else None,
                'supporting_evidence': snapshot.supporting_evidence_payload if snapshot is not None else [],
                'decision_risks': snapshot.decision_risks_payload if snapshot is not None else [],
                'market_regime_context': market_regime_projection,
                'decider_handoff_context_digest': decider_handoff_context_digest,
            }
            entry_item = {
                'core_item_key': core_entry_key,
                'source_plan_item_id': core_entry_key,
                'agent_name': row_decision_agent_name,
                'decision_agent_name': row_decision_agent_name,
                'symbol': row.symbol,
                **execution_scope,
                'side': 'buy',
                'intent': entry_intent,
                'handoff_intent': handoff_intent,
                'raw_action': raw_action,
                'tactical_action': traceability.get('tactical_action'),
                'generic_action': generic_action,
                'quantity_policy': quantity_policy,
                'limit_price': _to_json_number(_safe_decimal(chase_limit_price)) if is_short_term else None,
                'trigger_price': _to_json_number(_safe_decimal(trigger_price)) if is_short_term else None,
                'time_in_force': row.planned_time_in_force,
                'risk_context': entry_risk_context,
                'decision_snapshot_id': str(row.decision_snapshot_id),
            }
            items.append(entry_item)
            row_links[row.id].append(
                {
                    'core_item_key': core_entry_key,
                    'intent': entry_intent,
                    'handoff_intent': handoff_intent,
                    'side': 'buy',
                    'symbol': row.symbol,
                    'trigger_price': (
                        _to_json_number(_safe_decimal(trigger_price))
                        if row_decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME
                        else None
                    ),
                    'limit_price': (
                        _to_json_number(_safe_decimal(chase_limit_price))
                        if row_decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME
                        else None
                    ),
                }
            )

            if target_price is not None:
                target_key = f'core-plan-{row.id}:{EXEC_INTENT_TARGET_SELL}'
                target_quantity_policy = {
                    'type': 'position_exit_from_entry',
                    'linked_entry_item_key': core_entry_key,
                }
                if not uses_exec_agent_sizing:
                    target_quantity_policy['quantity'] = _decimal_to_text(quantity)
                items.append(
                    {
                        'core_item_key': target_key,
                        'source_plan_item_id': target_key,
                        'agent_name': row_decision_agent_name,
                        'decision_agent_name': row_decision_agent_name,
                        'symbol': row.symbol,
                        **execution_scope,
                        'side': 'sell',
                        'intent': EXEC_INTENT_TARGET_SELL,
                        'quantity_policy': target_quantity_policy,
                        'limit_price': None,
                        'trigger_price': target_price,
                        'time_in_force': row.planned_time_in_force,
                        'risk_context': _bounded_dispatch_object(
                            {
                                'trigger_rule': EXEC_INTENT_TARGET_SELL,
                                **full_protective_exit_risk_context,
                            },
                            max_bytes=EXEC_EXIT_RISK_CONTEXT_MAX_BYTES,
                            label='exec_target_exit_risk_context',
                            essential_keys=(
                                EXEC_PROTECTIVE_EXIT_RISK_CONTEXT_ESSENTIAL_KEYS
                            ),
                        ),
                        'decision_snapshot_id': str(row.decision_snapshot_id),
                    }
                )
                row_links[row.id].append(
                    {
                        'core_item_key': target_key,
                        'intent': EXEC_INTENT_TARGET_SELL,
                        'side': 'sell',
                        'symbol': row.symbol,
                        **execution_scope,
                        'trigger_price': target_price,
                    }
                )

            if stop_loss_price is not None:
                stop_key = f'core-plan-{row.id}:{EXEC_INTENT_STOP_SELL}'
                stop_quantity_policy = {
                    'type': 'position_exit_from_entry',
                    'linked_entry_item_key': core_entry_key,
                }
                if not uses_exec_agent_sizing:
                    stop_quantity_policy['quantity'] = _decimal_to_text(quantity)
                items.append(
                    {
                        'core_item_key': stop_key,
                        'source_plan_item_id': stop_key,
                        'agent_name': row_decision_agent_name,
                        'decision_agent_name': row_decision_agent_name,
                        'symbol': row.symbol,
                        **execution_scope,
                        'side': 'sell',
                        'intent': EXEC_INTENT_STOP_SELL,
                        'quantity_policy': stop_quantity_policy,
                        'limit_price': None,
                        'trigger_price': stop_loss_price,
                        'time_in_force': row.planned_time_in_force,
                        'risk_context': _bounded_dispatch_object(
                            {
                                'trigger_rule': EXEC_INTENT_STOP_SELL,
                                **full_protective_exit_risk_context,
                            },
                            max_bytes=EXEC_EXIT_RISK_CONTEXT_MAX_BYTES,
                            label='exec_stop_exit_risk_context',
                            essential_keys=(
                                EXEC_PROTECTIVE_EXIT_RISK_CONTEXT_ESSENTIAL_KEYS
                            ),
                        ),
                        'decision_snapshot_id': str(row.decision_snapshot_id),
                    }
                )
                row_links[row.id].append(
                    {
                        'core_item_key': stop_key,
                        'intent': EXEC_INTENT_STOP_SELL,
                        'side': 'sell',
                        'symbol': row.symbol,
                        'trigger_price': stop_loss_price,
                    }
                )

        payload = {
            'plan_id': plan_id,
            'source': dispatch_source,
            'mode': runtime_mode,
            'market_date': first_row.target_trade_date.isoformat(),
            'strategy_run_id': (
                f'{decision_agent_name}:{first_row.target_trade_date.isoformat()}:{_sanitize_key(first_row.account_reference)}'
            ),
            'requested_at': requested_at,
            'items': items,
            'metadata': {
                'schema_version': EXEC_SOURCE_SCHEMA_VERSION,
                'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
                'required_capability': EXECUTION_HANDOFF_CAPABILITY,
                'agent_name': decision_agent_name,
                'decision_agent_name': decision_agent_name,
                'trigger': trigger,
                'requested_at': requested_at,
                'account_reference': first_row.account_reference,
                'account_label': first_row.account_label,
                'source_market_date': source_market_date,
                'target_trade_date': first_row.target_trade_date.isoformat(),
                'dispatch_batch_type': 'delta' if effective_batch_key else 'initial',
                'dispatch_batch_key': effective_batch_key,
            },
        }
        try:
            serialized_payload = serialize_execution_plan_dispatch_payload(payload)
        except ValueError as exc:
            raise SignalDeciderExecPayloadContractError(str(exc)) from exc
        return serialized_payload, row_links

    def _rows_match_existing_dispatch(
        self,
        rows: Sequence[SignalDeciderExecutionPlanItem],
        *,
        plan_id: str,
        fingerprint: str,
    ) -> bool:
        matching = True
        for row in rows:
            submission = row.submission_payload if isinstance(row.submission_payload, dict) else {}
            exec_dispatch = submission.get('exec_dispatch') if isinstance(submission.get('exec_dispatch'), dict) else {}
            if exec_dispatch.get('plan_id') != plan_id:
                matching = False
                break
            if exec_dispatch.get('plan_fingerprint') != fingerprint:
                matching = False
                break
            if not _row_has_accepted_exec_dispatch(row):
                matching = False
                break
        return matching

    def _persist_dispatch_failure(
        self,
        *,
        rows: Sequence[SignalDeciderExecutionPlanItem],
        submitted_revision_fingerprints: dict[int, str],
        row_links: dict[int, list[dict[str, Any]]],
        plan_id: str,
        fingerprint: str,
        payload: dict[str, Any],
        trigger: str,
        status: str,
        error: dict[str, Any],
        remote_acceptance_ambiguous: bool = False,
        preserve_write_ahead: bool = False,
    ) -> bool:
        now = _utcnow()
        row_ids = [int(row.id) for row in rows]
        submitted_by_id = {int(row.id): row for row in rows}
        preserved_row_count = 0
        if set(submitted_revision_fingerprints) != set(row_ids):
            raise ValueError('dispatch revision fingerprints must bind every submitted row')
        with session_scope() as session:
            persisted_rows = session.execute(
                select(SignalDeciderExecutionPlanItem)
                .where(SignalDeciderExecutionPlanItem.id.in_(row_ids))
                .with_for_update()
            ).scalars().all()
            for row in persisted_rows:
                submitted_row = submitted_by_id[int(row.id)]
                submitted_revision_fingerprint = submitted_revision_fingerprints[
                    int(row.id)
                ]
                current_revision_fingerprint = _row_dispatch_revision_fingerprint(row)
                if row.execution_status in {
                    'revocation_pending',
                    'revocation_manual_review',
                    'revoked',
                }:
                    continue
                existing_submission = (
                    copy.deepcopy(row.submission_payload)
                    if isinstance(row.submission_payload, dict)
                    else {}
                )
                existing_exec = (
                    existing_submission.get('exec_dispatch')
                    if isinstance(existing_submission.get('exec_dispatch'), dict)
                    else {}
                )
                expected_core_keys = {
                    str(link['core_item_key'])
                    for link in row_links.get(int(row.id), [])
                }
                write_ahead_core_keys = {
                    str(link.get('core_item_key'))
                    for link in (existing_exec.get('item_links') or [])
                    if isinstance(link, dict) and link.get('core_item_key')
                }
                write_ahead_matches = bool(
                    row.execution_status
                    == EXECUTION_STATUS_DISPATCH_PENDING_REMOTE_ACK
                    and _normalize_status(existing_exec.get('dispatch_status'))
                    == 'pending_remote_ack'
                    and existing_exec.get('plan_id') == plan_id
                    and existing_exec.get('plan_fingerprint') == fingerprint
                    and existing_exec.get('dispatch_revision_fingerprint')
                    == submitted_revision_fingerprint
                    and existing_exec.get('dispatch_material_fingerprint')
                    == payload['metadata']['dispatch_material_fingerprint']
                    and existing_exec.get('prior_execution_status')
                    == submitted_row.execution_status
                    and existing_exec.get('prior_eligibility_status')
                    == submitted_row.eligibility_status
                    and write_ahead_core_keys == expected_core_keys
                )
                row_revision_changed = bool(
                    int(row.decision_snapshot_id)
                    != int(submitted_row.decision_snapshot_id)
                    or current_revision_fingerprint
                    != submitted_revision_fingerprint
                    or (
                        not write_ahead_matches
                        and (
                            row.execution_status != submitted_row.execution_status
                            or row.eligibility_status
                            != submitted_row.eligibility_status
                        )
                    )
                    or (
                        write_ahead_matches
                        and row.eligibility_status
                        != existing_exec.get('prior_eligibility_status')
                    )
                )
                authority_fingerprint_changed = bool(
                    current_revision_fingerprint
                    != submitted_revision_fingerprint
                )
                attempt = {
                    'attempted_at': now.isoformat(),
                    'trigger': trigger,
                    'dispatch_status': status,
                    'error': summarize_error(error),
                }
                compact_existing_links = [
                    compact
                    for link in (existing_exec.get('item_links') or row_links.get(row.id, []))
                    if (compact := compact_exec_item_link(link))
                ]
                failed_exec_dispatch = {
                    'plan_id': plan_id,
                    'plan_fingerprint': fingerprint,
                    'dispatch_revision_fingerprint': submitted_revision_fingerprint,
                    'dispatch_material_fingerprint': existing_exec.get(
                        'dispatch_material_fingerprint'
                    )
                    or payload['metadata'].get('dispatch_material_fingerprint'),
                    'dispatch_batch_key': existing_exec.get('dispatch_batch_key'),
                    'dispatch_status': status,
                    'prior_execution_status': existing_exec.get(
                        'prior_execution_status'
                    ),
                    'prior_eligibility_status': existing_exec.get(
                        'prior_eligibility_status'
                    ),
                    'last_attempt_at': now.isoformat(),
                    'last_success_at': existing_exec.get('last_success_at'),
                    'last_error': summarize_error(error),
                    'item_links': compact_existing_links,
                    'destination_binding': copy.deepcopy(
                        existing_exec.get('destination_binding')
                    ),
                    'authority_generation_before': copy.deepcopy(
                        existing_exec.get('authority_generation_before')
                    ),
                    'attempts': _append_limited(existing_exec.get('attempts') or [], attempt),
                    'last_request_summary': compact_exec_request_summary(
                        payload,
                        item_links=row_links.get(row.id, []),
                    ),
                }
                if remote_acceptance_ambiguous:
                    if (
                        preserve_write_ahead
                        and error.get('retryable') is True
                        and not row_revision_changed
                        and write_ahead_matches
                    ):
                        failed_exec_dispatch['dispatch_status'] = (
                            EXEC_DISPATCH_STATUS_PENDING_REMOTE_ACK
                        )
                        existing_submission.update(
                            {
                                'mode': 'exec_dispatch',
                                'latest': {
                                    **attempt,
                                    'dispatch_status': (
                                        EXEC_DISPATCH_STATUS_PENDING_REMOTE_ACK
                                    ),
                                },
                                'exec_dispatch': failed_exec_dispatch,
                                'dispatch_reconciliation': {
                                    'status': (
                                        EXEC_DISPATCH_STATUS_PENDING_REMOTE_ACK
                                    ),
                                    'plan_id': plan_id,
                                    'plan_fingerprint': fingerprint,
                                    'observed_at': now.isoformat(),
                                    'reason_code': (
                                        'retryable_remote_acceptance_ambiguous'
                                    ),
                                    'submitted_dispatch_revision_fingerprint': (
                                        submitted_revision_fingerprint
                                    ),
                                    'current_dispatch_revision_fingerprint': (
                                        current_revision_fingerprint
                                    ),
                                    'error': summarize_error(error),
                                },
                            }
                        )
                        row.eligibility_status = str(
                            existing_exec.get('prior_eligibility_status')
                            or submitted_row.eligibility_status
                        )
                        row.execution_status = (
                            EXECUTION_STATUS_DISPATCH_PENDING_REMOTE_ACK
                        )
                        row.submission_payload = (
                            slim_signal_decider_exec_submission_payload(
                                existing_submission
                            ).payload
                        )
                        preserved_row_count += 1
                        continue
                    existing_submission['dispatch_reconciliation'] = {
                        'status': 'manual_review_required',
                        'plan_id': plan_id,
                        'plan_fingerprint': fingerprint,
                        'observed_at': now.isoformat(),
                        'reason_code': (
                            'ambiguous_remote_acceptance_after_core_revision'
                            if authority_fingerprint_changed
                            else 'ambiguous_remote_acceptance'
                        ),
                        'submitted_dispatch_revision_fingerprint': (
                            submitted_revision_fingerprint
                        ),
                        'current_dispatch_revision_fingerprint': (
                            current_revision_fingerprint
                        ),
                        'error': summarize_error(error),
                    }
                    if not _row_has_accepted_exec_dispatch(row):
                        existing_submission.update(
                            {
                                'mode': 'exec_dispatch',
                                'latest': attempt,
                                'exec_dispatch': failed_exec_dispatch,
                            }
                        )
                    row.eligibility_status = 'blocked'
                    row.execution_status = 'revocation_manual_review'
                    row.submission_payload = slim_signal_decider_exec_submission_payload(
                        existing_submission
                    ).payload
                    continue
                if row_revision_changed:
                    continue
                row.execution_status = EXECUTION_STATUS_SUBMIT_FAILED
                submission = {
                    **existing_submission,
                    'mode': 'exec_dispatch',
                    'latest': attempt,
                    'exec_dispatch': failed_exec_dispatch,
                }
                row.submission_payload = slim_signal_decider_exec_submission_payload(submission).payload
            session.flush()
        return bool(
            preserve_write_ahead
            and preserved_row_count == len(row_ids)
        )

    @staticmethod
    def _release_not_found_dispatch_write_ahead(
        *,
        rows: Sequence[SignalDeciderExecutionPlanItem],
        submitted_revision_fingerprints: dict[int, str],
        plan_id: str,
        plan_fingerprint: str,
        dispatch_material_fingerprint: str,
        destination_binding: dict[str, Any],
        trigger: str,
        reason_code: str,
    ) -> None:
        now = _utcnow()
        row_ids = [int(row.id) for row in rows]
        with session_scope() as session:
            persisted_rows = session.execute(
                select(SignalDeciderExecutionPlanItem)
                .where(SignalDeciderExecutionPlanItem.id.in_(row_ids))
                .order_by(SignalDeciderExecutionPlanItem.id.asc())
                .with_for_update()
            ).scalars().all()
            if len(persisted_rows) != len(row_ids):
                return
            for row in persisted_rows:
                submission = (
                    row.submission_payload
                    if isinstance(row.submission_payload, dict)
                    else {}
                )
                exec_dispatch = (
                    submission.get('exec_dispatch')
                    if isinstance(submission.get('exec_dispatch'), dict)
                    else {}
                )
                if not (
                    row.execution_status
                    == EXECUTION_STATUS_DISPATCH_PENDING_REMOTE_ACK
                    and _normalize_status(exec_dispatch.get('dispatch_status'))
                    == 'pending_remote_ack'
                    and exec_dispatch.get('plan_id') == plan_id
                    and exec_dispatch.get('plan_fingerprint')
                    == plan_fingerprint
                    and exec_dispatch.get('dispatch_revision_fingerprint')
                    == submitted_revision_fingerprints.get(int(row.id))
                    and exec_dispatch.get('dispatch_material_fingerprint')
                    == dispatch_material_fingerprint
                    and _validated_exec_destination_binding(
                        exec_dispatch.get('destination_binding')
                    )
                    == destination_binding
                ):
                    return

            for row in persisted_rows:
                submission = copy.deepcopy(row.submission_payload)
                exec_dispatch = submission.pop('exec_dispatch')
                submission['dispatch_reconciliation'] = {
                    'status': 'confirmed_not_found',
                    'reason_code': reason_code,
                    'observed_at': now.isoformat(),
                    'trigger': trigger,
                    'plan_id': plan_id,
                    'plan_fingerprint': plan_fingerprint,
                    'dispatch_material_fingerprint': (
                        dispatch_material_fingerprint
                    ),
                    'destination_binding': copy.deepcopy(
                        destination_binding
                    ),
                    'remote_authority_created': False,
                }
                row.execution_status = (
                    _normalize_optional_text(
                        exec_dispatch.get('prior_execution_status'),
                        lowercase=True,
                    )
                    or EXECUTION_STATUS_PLANNED
                )
                row.eligibility_status = (
                    _normalize_optional_text(
                        exec_dispatch.get('prior_eligibility_status'),
                        lowercase=True,
                    )
                    or row.eligibility_status
                )
                row.submission_payload = submission
            session.flush()

    @staticmethod
    def _persist_dispatch_write_ahead(
        *,
        rows: Sequence[SignalDeciderExecutionPlanItem],
        submitted_revision_fingerprints: dict[int, str],
        row_links: dict[int, list[dict[str, Any]]],
        plan_id: str,
        fingerprint: str,
        payload: dict[str, Any],
        trigger: str,
        destination_binding: dict[str, Any],
        authority_generation_before: dict[str, Any] | None,
    ) -> None:
        now = _utcnow()
        core_activation_binding = _core_activation_binding_from_plan(payload)
        row_ids = [int(row.id) for row in rows]
        submitted_by_id = {int(row.id): row for row in rows}
        if set(submitted_revision_fingerprints) != set(row_ids):
            raise ValueError('dispatch revision fingerprints must bind every submitted row')
        with session_scope() as session:
            persisted_rows = session.execute(
                select(SignalDeciderExecutionPlanItem)
                .where(SignalDeciderExecutionPlanItem.id.in_(row_ids))
                .order_by(SignalDeciderExecutionPlanItem.id.asc())
                .with_for_update()
            ).scalars().all()
            if len(persisted_rows) != len(row_ids):
                raise ValueError('dispatch write-ahead rows changed before POST')
            for row in persisted_rows:
                submitted = submitted_by_id[int(row.id)]
                if (
                    int(row.decision_snapshot_id)
                    != int(submitted.decision_snapshot_id)
                    or row.execution_status != submitted.execution_status
                    or row.eligibility_status != submitted.eligibility_status
                    or _row_dispatch_revision_fingerprint(row)
                    != submitted_revision_fingerprints[int(row.id)]
                ):
                    raise ValueError('dispatch write-ahead revision changed before POST')
                submission = (
                    row.submission_payload
                    if isinstance(row.submission_payload, dict)
                    else {}
                )
                existing_exec = (
                    submission.get('exec_dispatch')
                    if isinstance(submission.get('exec_dispatch'), dict)
                    else {}
                )
                if _normalize_status(existing_exec.get('dispatch_status')) in {
                    EXEC_DISPATCH_STATUS_ACCEPTED,
                    'pending_remote_ack',
                }:
                    raise ValueError('dispatch write-ahead found existing remote authority')

            for row in persisted_rows:
                existing_submission = (
                    copy.deepcopy(row.submission_payload)
                    if isinstance(row.submission_payload, dict)
                    else {}
                )
                existing_exec = (
                    existing_submission.get('exec_dispatch')
                    if isinstance(existing_submission.get('exec_dispatch'), dict)
                    else {}
                )
                attempt = {
                    'attempted_at': now.isoformat(),
                    'trigger': trigger,
                    'dispatch_status': 'pending_remote_ack',
                }
                exec_dispatch = {
                    'plan_id': plan_id,
                    'plan_fingerprint': fingerprint,
                    'dispatch_revision_fingerprint': (
                        submitted_revision_fingerprints[int(row.id)]
                    ),
                    'dispatch_material_fingerprint': payload['metadata'][
                        'dispatch_material_fingerprint'
                    ],
                    'dispatch_batch_key': payload['metadata'].get(
                        'dispatch_batch_key'
                    ),
                    'dispatch_status': 'pending_remote_ack',
                    'prior_execution_status': row.execution_status,
                    'prior_eligibility_status': row.eligibility_status,
                    'last_attempt_at': now.isoformat(),
                    'last_success_at': existing_exec.get('last_success_at'),
                    'last_error': None,
                    'item_links': [
                        compact
                        for link in row_links.get(int(row.id), [])
                        if (compact := compact_exec_item_link(link))
                    ],
                    'destination_binding': copy.deepcopy(destination_binding),
                    'authority_generation_before': (
                        {
                            'status': authority_generation_before.get('status'),
                            'generation_fingerprint': authority_generation_before.get(
                                'generation_fingerprint'
                            ),
                            **(
                                {
                                    'core_activation': copy.deepcopy(
                                        core_activation_binding
                                    )
                                }
                                if core_activation_binding is not None
                                else {}
                            ),
                        }
                        if isinstance(authority_generation_before, dict)
                        else None
                    ),
                    'attempts': _append_limited(
                        existing_exec.get('attempts') or [],
                        attempt,
                    ),
                    'last_request_summary': compact_exec_request_summary(
                        payload,
                        item_links=row_links.get(int(row.id), []),
                    ),
                }
                row.execution_status = EXECUTION_STATUS_DISPATCH_PENDING_REMOTE_ACK
                row.submission_payload = slim_signal_decider_exec_submission_payload(
                    {
                        **existing_submission,
                        'mode': 'exec_dispatch',
                        'latest': attempt,
                        'exec_dispatch': exec_dispatch,
                    }
                ).payload
            session.flush()

    @staticmethod
    def _persist_unsupported_execution_route(
        *,
        rows: Sequence[SignalDeciderExecutionPlanItem],
        trigger: str,
        reason_code: str,
    ) -> None:
        now = _utcnow()
        submitted_by_id = {int(row.id): row for row in rows}
        with session_scope() as session:
            persisted_rows = session.execute(
                select(SignalDeciderExecutionPlanItem)
                .where(SignalDeciderExecutionPlanItem.id.in_(submitted_by_id))
                .with_for_update()
            ).scalars().all()
            for row in persisted_rows:
                submitted = submitted_by_id[int(row.id)]
                if row.execution_status in {
                    'revocation_pending',
                    'revocation_manual_review',
                    'revoked',
                }:
                    continue
                if int(row.decision_snapshot_id) != int(submitted.decision_snapshot_id):
                    continue
                existing_submission = (
                    copy.deepcopy(row.submission_payload)
                    if isinstance(row.submission_payload, dict)
                    else {}
                )
                existing_submission['contract_error'] = {
                    'reason_code': reason_code,
                    'trigger': trigger,
                    'observed_at': now.isoformat(),
                    'retryable': False,
                }
                row.eligibility_status = 'blocked'
                row.execution_status = EXEC_DISPATCH_STATUS_CONTRACT_ERROR
                row.submission_payload = slim_signal_decider_exec_submission_payload(
                    existing_submission
                ).payload
            session.flush()

    async def _persist_dispatch_success_with_revision_retry(
        self,
        **kwargs: Any,
    ) -> dict[str, Any]:
        last_pending_result: dict[str, Any] | None = None

        def persist() -> dict[str, Any]:
            nonlocal last_pending_result
            result = self._persist_dispatch_success(**kwargs)
            error = result.get('error')
            if (
                result.get('dispatch_status')
                == EXEC_DISPATCH_STATUS_PENDING_REMOTE_ACK
                and isinstance(error, dict)
                and error.get('code') == 'dispatch_ack_authority_fence_busy'
            ):
                last_pending_result = result
                raise _PlanRefreshRevisionConflict(
                    'post-ACK dispatch authority persistence is busy'
                )
            return result

        try:
            return await _retry_plan_refresh_revision_conflict(persist)
        except _PlanRefreshRevisionConflict:
            if last_pending_result is None:
                raise
            return last_pending_result

    def _persist_dispatch_success(
        self,
        *,
        rows: Sequence[SignalDeciderExecutionPlanItem],
        submitted_revision_fingerprints: dict[int, str],
        row_links: dict[int, list[dict[str, Any]]],
        plan_id: str,
        fingerprint: str,
        payload: dict[str, Any],
        response_payload: dict[str, Any],
        trigger: str,
        authority_generation_before: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        destination_binding = self._destination_binding_for_payload(payload)
        core_activation_binding = _core_activation_binding_from_plan(payload)
        submitted_core_item_keys = [
            str(link['core_item_key'])
            for row in rows
            for link in row_links.get(row.id, [])
        ]
        try:
            response_by_key = validate_execution_plan_dispatch_ack(
                response_payload,
                plan_id=plan_id,
                submitted_core_item_keys=submitted_core_item_keys,
                expected_dispatch_material_fingerprint=payload['metadata'][
                    'dispatch_material_fingerprint'
                ],
            )
        except ValueError as exc:
            error = _serialize_error(
                code='execution_handoff_ack_invalid',
                message=str(exc),
                retryable=False,
            )
            self._persist_dispatch_failure(
                rows=rows,
                submitted_revision_fingerprints=submitted_revision_fingerprints,
                row_links=row_links,
                plan_id=plan_id,
                fingerprint=fingerprint,
                payload=payload,
                trigger=trigger,
                status=EXEC_DISPATCH_STATUS_CONTRACT_ERROR,
                error=error,
                remote_acceptance_ambiguous=True,
            )
            return {
                'dispatch_status': EXEC_DISPATCH_STATUS_CONTRACT_ERROR,
                'eligible_count': len(rows),
                'translated_item_count': len(submitted_core_item_keys),
                'accepted_item_count': 0,
                'rejected_item_count': len(submitted_core_item_keys),
                'error': error,
            }

        now = _utcnow()
        row_ids = [int(row.id) for row in rows]
        submitted_by_id = {int(row.id): row for row in rows}
        if set(submitted_revision_fingerprints) != set(row_ids):
            raise ValueError('dispatch revision fingerprints must bind every submitted row')
        accepted_item_count = 0
        revision_conflict_count = 0

        with session_scope() as session:
            authority_generation_conflict: dict[str, Any] | None = None
            if authority_generation_before is not None and rows:
                try:
                    authority_generation_after = (
                        signal_decider_next_open_execution_service
                        .inspect_dispatch_authority_generation(
                            target_trade_date=rows[0].target_trade_date,
                            expected_row_bindings=_dispatch_authority_row_bindings(
                                rows,
                                revision_fingerprints=(
                                    submitted_revision_fingerprints
                                ),
                            ),
                            _session=session,
                        )
                    )
                except _PlanRefreshRevisionConflict:
                    # The remote ACK is durable, but another worker currently
                    # owns the authority fence. Preserve the write-ahead state
                    # so exact readback reconciliation can finish later; never
                    # let contention escape while a peer transaction may still
                    # be committing the same ACK.
                    return {
                        'dispatch_status': (
                            EXEC_DISPATCH_STATUS_PENDING_REMOTE_ACK
                        ),
                        'eligible_count': len(rows),
                        'translated_item_count': len(
                            submitted_core_item_keys
                        ),
                        'accepted_item_count': 0,
                        'rejected_item_count': 0,
                        'pending_item_count': len(
                            submitted_core_item_keys
                        ),
                        'error': _serialize_error(
                            code='dispatch_ack_authority_fence_busy',
                            message=(
                                'Core authority validation is busy; the '
                                'remote ACK remains quarantined for exact '
                                'readback reconciliation.'
                            ),
                            retryable=True,
                        ),
                    }
                authority_generation_conflict = (
                    _dispatch_authority_generation_conflict(
                        before=authority_generation_before,
                        after=authority_generation_after,
                    )
                )
            persisted_rows = session.execute(
                select(SignalDeciderExecutionPlanItem)
                .where(SignalDeciderExecutionPlanItem.id.in_(row_ids))
                .with_for_update()
            ).scalars().all()
            for row in persisted_rows:
                submitted_row = submitted_by_id[int(row.id)]
                submitted_revision_fingerprint = submitted_revision_fingerprints[
                    int(row.id)
                ]
                current_revision_fingerprint = _row_dispatch_revision_fingerprint(row)
                if row.execution_status in {
                    'revocation_pending',
                    'revocation_manual_review',
                    'revoked',
                }:
                    revision_conflict_count += 1
                    continue
                snapshot_changed = int(row.decision_snapshot_id) != int(
                    submitted_row.decision_snapshot_id
                )
                dispatch_material_changed = (
                    current_revision_fingerprint
                    != submitted_revision_fingerprint
                )
                current_submission = (
                    row.submission_payload
                    if isinstance(row.submission_payload, dict)
                    else {}
                )
                current_exec = (
                    current_submission.get('exec_dispatch')
                    if isinstance(current_submission.get('exec_dispatch'), dict)
                    else {}
                )
                existing_links_by_key = {
                    _normalize_optional_text(link.get('core_item_key')): link
                    for link in (current_exec.get('item_links') or [])
                    if isinstance(link, dict)
                    and _normalize_optional_text(link.get('core_item_key'))
                }
                exact_accepted_replay = bool(
                    _row_has_accepted_exec_dispatch(row)
                    and current_exec.get('plan_id') == plan_id
                    and current_exec.get('plan_fingerprint') == fingerprint
                    and _exec_dispatch_destination_binding(current_exec)
                    == destination_binding
                    and set(existing_links_by_key)
                    == {
                        str(link['core_item_key'])
                        for link in row_links.get(int(row.id), [])
                    }
                    and all(
                        _normalize_optional_text(
                            existing_links_by_key[core_key].get(
                                'exec_plan_id'
                            )
                        )
                        == _normalize_optional_text(
                            response_by_key[core_key].get(
                                'execution_plan_id'
                            )
                        )
                        and _normalize_optional_text(
                            existing_links_by_key[core_key].get(
                                'exec_plan_item_id'
                            )
                        )
                        == _normalize_optional_text(
                            response_by_key[core_key].get(
                                'execution_plan_item_id'
                            )
                        )
                        for core_key in existing_links_by_key
                    )
                )
                if exact_accepted_replay:
                    accepted_item_count += len(
                        row_links.get(int(row.id), [])
                    )
                    continue
                expected_core_keys = {
                    str(link['core_item_key'])
                    for link in row_links.get(int(row.id), [])
                }
                write_ahead_core_keys = {
                    str(link.get('core_item_key'))
                    for link in (current_exec.get('item_links') or [])
                    if isinstance(link, dict) and link.get('core_item_key')
                }
                write_ahead_matches = bool(
                    row.execution_status
                    == EXECUTION_STATUS_DISPATCH_PENDING_REMOTE_ACK
                    and current_exec.get('dispatch_status') == 'pending_remote_ack'
                    and current_exec.get('plan_id') == plan_id
                    and current_exec.get('plan_fingerprint') == fingerprint
                    and current_exec.get('dispatch_revision_fingerprint')
                    == submitted_revision_fingerprint
                    and current_exec.get('dispatch_material_fingerprint')
                    == payload['metadata']['dispatch_material_fingerprint']
                    and _validated_exec_destination_binding(
                        current_exec.get('destination_binding')
                    )
                    == destination_binding
                    and current_exec.get('prior_execution_status')
                    == submitted_row.execution_status
                    and current_exec.get('prior_eligibility_status')
                    == submitted_row.eligibility_status
                    and write_ahead_core_keys == expected_core_keys
                )
                row_state_changed = bool(
                    not write_ahead_matches
                    and (
                        row.execution_status != submitted_row.execution_status
                        or row.eligibility_status != submitted_row.eligibility_status
                    )
                )
                if (
                    snapshot_changed
                    or dispatch_material_changed
                    or row_state_changed
                    or authority_generation_conflict is not None
                ):
                    response_links = []
                    for link in row_links.get(row.id, []):
                        response_item = response_by_key[link['core_item_key']]
                        response_links.append(
                            {
                                **link,
                                'exec_plan_id': response_item['execution_plan_id'],
                                'exec_plan_item_id': response_item['execution_plan_item_id'],
                                'remote_status': 'accepted',
                            }
                        )
                    current_submission = (
                        copy.deepcopy(row.submission_payload)
                        if isinstance(row.submission_payload, dict)
                        else {}
                    )
                    current_history = [
                        copy.deepcopy(item)
                        for item in (current_submission.get('revision_history') or [])
                        if isinstance(item, dict)
                    ]
                    current_history.append(
                        {
                            'decision_snapshot_id': str(submitted_row.decision_snapshot_id),
                            'decision_agent_name': submitted_row.decision_agent_name,
                            'symbol': submitted_row.symbol,
                            'dispatch_evidence': {
                                'plan_id': plan_id,
                                'plan_fingerprint': fingerprint,
                                'dispatch_revision_fingerprint': (
                                    submitted_revision_fingerprint
                                ),
                                'dispatch_status': 'accepted',
                                'destination_binding': copy.deepcopy(
                                    destination_binding
                                ),
                                'item_links': copy.deepcopy(response_links),
                            },
                            'archived_at': now.isoformat(),
                        }
                    )
                    current_traceability = (
                        row.traceability_payload
                        if isinstance(row.traceability_payload, dict)
                        else {}
                    )
                    current_validation = (
                        row.validation_payload
                        if isinstance(row.validation_payload, dict)
                        else {}
                    )
                    current_blocking_reasons = [
                        item
                        for item in (current_validation.get('blocking_reasons') or [])
                        if isinstance(item, dict)
                        and _normalize_optional_text(item.get('code'))
                    ]
                    if (
                        row_state_changed
                        or dispatch_material_changed
                        or snapshot_changed
                    ) and current_blocking_reasons:
                        reason_code = str(current_blocking_reasons[0]['code'])
                        replacement_action = (
                            'reject'
                            if reason_code
                            in {
                                'current_live_market_risk_off',
                                'current_live_market_evidence_unavailable',
                            }
                            else 'wait'
                        )
                        replacement_actionable = False
                    elif authority_generation_conflict is not None:
                        replacement_action = (
                            _normalize_optional_text(
                                authority_generation_conflict.get(
                                    'replacement_action'
                                )
                            )
                            or 'wait'
                        )
                        replacement_actionable = bool(
                            authority_generation_conflict.get(
                                'replacement_actionable'
                            )
                        )
                        reason_code = str(
                            authority_generation_conflict['reason_code']
                        )
                    else:
                        replacement_action = (
                            current_traceability.get('raw_action')
                            or current_traceability.get('generic_action')
                            or 'buy'
                            if row.eligibility_status in {
                                'eligible',
                                'monitor_candidate',
                            }
                            else 'wait'
                        )
                        replacement_actionable = row.eligibility_status in {
                            'eligible',
                            'monitor_candidate',
                        }
                        reason_code = (
                            'latest_snapshot_replaced'
                            if snapshot_changed
                            else 'policy_scope_changed'
                        )
                    replacement_observation_id = None
                    replacement_revision_id = str(row.decision_snapshot_id)
                    if authority_generation_conflict is not None:
                        revision_material = '|'.join(
                            (
                                reason_code,
                                str(
                                    authority_generation_conflict.get(
                                        'before_generation_fingerprint'
                                    )
                                    or ''
                                ),
                                str(
                                    authority_generation_conflict.get(
                                        'after_generation_fingerprint'
                                    )
                                    or ''
                                ),
                            )
                        )
                        replacement_observation_id = (
                            'observation:dispatch_authority_generation:'
                            f'{hashlib.sha256(revision_material.encode("utf-8")).hexdigest()[:20]}'
                        )
                        replacement_revision_id = replacement_observation_id
                    elif not snapshot_changed:
                        revision_material = '|'.join(
                            (
                                current_revision_fingerprint,
                                str(row.execution_status),
                                str(row.eligibility_status),
                            )
                        )
                        replacement_observation_id = (
                            'observation:dispatch_material_changed:'
                            f'{hashlib.sha256(revision_material.encode("utf-8")).hexdigest()[:20]}'
                        )
                        replacement_revision_id = replacement_observation_id
                    current_submission.update(
                        {
                            'revision_history': current_history[-5:],
                            'revocation': {
                                'schema_version': 'core_execution_revocation_state_v1',
                                'status': 'pending',
                                'reason_code': reason_code,
                                'requested_at': now.isoformat(),
                                'prior_decision_snapshot_id': str(submitted_row.decision_snapshot_id),
                                'replacement_decision_snapshot_id': replacement_revision_id,
                                'replacement_observation_id': replacement_observation_id,
                                'prior_dispatch_revision_fingerprint': (
                                    submitted_revision_fingerprint
                                ),
                                'replacement_dispatch_revision_fingerprint': (
                                    current_revision_fingerprint
                                ),
                                'prior_authority_generation_fingerprint': (
                                    authority_generation_conflict.get(
                                        'before_generation_fingerprint'
                                    )
                                    if authority_generation_conflict is not None
                                    else None
                                ),
                                'replacement_authority_generation_fingerprint': (
                                    authority_generation_conflict.get(
                                        'after_generation_fingerprint'
                                    )
                                    if authority_generation_conflict is not None
                                    else None
                                ),
                                'replacement_action': replacement_action,
                                'replacement_actionable': replacement_actionable,
                                'plan_id': plan_id,
                                'destination_binding': copy.deepcopy(
                                    destination_binding
                                ),
                                'item_links': response_links,
                            },
                        }
                    )
                    row.eligibility_status = 'blocked'
                    row.execution_status = 'revocation_pending'
                    updated_validation = (
                        copy.deepcopy(row.validation_payload)
                        if isinstance(row.validation_payload, dict)
                        else {}
                    )
                    updated_validation['blocking_reasons'] = [
                        {
                            'code': reason_code,
                            'message': (
                                'Dispatch authority changed while Exec was accepting the plan; '
                                'the newly accepted BUY authority must be revoked.'
                            ),
                        }
                    ]
                    row.validation_payload = updated_validation
                    row.submission_payload = current_submission
                    revision_conflict_count += 1
                    continue
                links = []
                entry_response_item: dict[str, Any] = {}
                for link in row_links.get(row.id, []):
                    response_item = response_by_key[link['core_item_key']]
                    item_status = str(response_item['status'])
                    accepted_item_count += 1
                    if link.get('intent') in EXEC_ENTRY_BUY_INTENTS:
                        entry_response_item = response_item
                    links.append(
                        {
                            **link,
                            'exec_plan_id': _normalize_optional_text(response_item.get('execution_plan_id')),
                            'exec_plan_item_id': _normalize_optional_text(response_item.get('execution_plan_item_id')),
                            'remote_status': item_status,
                            'response_item': compact_exec_response_item(response_item) if response_item else None,
                        }
                    )

                existing_submission = row.submission_payload if isinstance(row.submission_payload, dict) else {}
                existing_exec = (
                    existing_submission.get('exec_dispatch')
                    if isinstance(existing_submission.get('exec_dispatch'), dict)
                    else {}
                )
                response_summary = {
                    'accepted_item_count': response_payload.get('accepted_item_count'),
                    'rejected_item_count': response_payload.get('rejected_item_count'),
                    'validation_failures': response_payload.get('validation_failures') or [],
                }
                attempt = {
                    'attempted_at': now.isoformat(),
                    'trigger': trigger,
                    'dispatch_status': EXEC_DISPATCH_STATUS_ACCEPTED,
                    'response_summary': response_summary,
                }
                exec_dispatch = {
                    'plan_id': plan_id,
                    'plan_fingerprint': fingerprint,
                    'dispatch_revision_fingerprint': (
                        submitted_revision_fingerprint
                    ),
                    'dispatch_status': EXEC_DISPATCH_STATUS_ACCEPTED,
                    'last_attempt_at': now.isoformat(),
                    'last_success_at': now.isoformat(),
                    'last_error': None,
                    'item_links': [
                        compact
                        for item in links
                        if (compact := compact_exec_item_link(item))
                    ],
                    'destination_binding': copy.deepcopy(destination_binding),
                    'contract_proof': {
                        'schema_version': EXEC_DISPATCH_PROOF_SCHEMA_VERSION,
                        'execution_plan_id': str(
                            response_payload['execution_plan_id']
                        ),
                        'dispatch_material_fingerprint': payload['metadata'][
                            'dispatch_material_fingerprint'
                        ],
                        'destination_binding': copy.deepcopy(
                            destination_binding
                        ),
                        'contract_ack': copy.deepcopy(response_payload.get('contract_ack')),
                        'submitted_core_item_keys': [
                            str(link['core_item_key'])
                            for link in row_links.get(row.id, [])
                        ],
                        'accepted_item_bindings': [
                            {
                                'core_item_key': str(link['core_item_key']),
                                'execution_plan_id': str(
                                    response_by_key[link['core_item_key']][
                                        'execution_plan_id'
                                    ]
                                ),
                                'execution_plan_item_id': str(
                                    response_by_key[link['core_item_key']][
                                        'execution_plan_item_id'
                                    ]
                                ),
                            }
                            for link in row_links.get(row.id, [])
                        ],
                        'accepted_item_count': len(row_links.get(row.id, [])),
                        'rejected_item_count': 0,
                        **(
                            {
                                'core_activation': {
                                    **core_activation_binding,
                                    'status': 'pending',
                                    'dispatch_material_fingerprint': payload[
                                        'metadata'
                                    ]['dispatch_material_fingerprint'],
                                }
                            }
                            if core_activation_binding is not None
                            else {}
                        ),
                    },
                    'attempts': _append_limited(existing_exec.get('attempts') or [], attempt),
                    'last_request_summary': compact_exec_request_summary(
                        payload,
                        item_links=row_links.get(row.id, []),
                    ),
                    'last_response_summary': compact_exec_response_summary(
                        response_payload,
                        item_links=row_links.get(row.id, []),
                    ),
                }
                row_decision_agent_name = _normalize_decision_agent_name(row.decision_agent_name)
                if row_decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME:
                    row.execution_status = EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING
                elif row_decision_agent_name == SIGNAL_DECIDER_AGENT_NAME:
                    row.execution_status = (
                        EXECUTION_STATUS_SUBMITTED
                        if _extract_agent_approved_quantity(entry_response_item) is not None
                        else EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING
                    )
                else:
                    row.execution_status = EXECUTION_STATUS_SUBMITTED
                row.submitted_at = row.submitted_at or now
                existing_submission.pop('dispatch_reconciliation', None)
                submission = {
                    **existing_submission,
                    'mode': 'exec_dispatch',
                    'latest': attempt,
                    'exec_dispatch': exec_dispatch,
                }
                row.submission_payload = slim_signal_decider_exec_submission_payload(submission).payload
            session.flush()

        return {
            'dispatch_status': (
                'revocation_pending'
                if revision_conflict_count
                else EXEC_DISPATCH_STATUS_ACCEPTED
            ),
            'eligible_count': len(rows),
            'translated_item_count': sum(len(row_links.get(row.id, [])) for row in rows),
            'accepted_item_count': accepted_item_count,
            'rejected_item_count': 0,
            'revision_conflict_count': revision_conflict_count,
        }

    async def _refresh_exec_readback(
        self,
        *,
        target_trade_date: date,
        account_reference: str | None,
        symbol: str | None,
        trigger: str,
        runtime_payload: dict[str, Any],
    ) -> None:
        rows = self._load_next_open_rows(
            target_trade_date=target_trade_date,
            account_reference=account_reference,
            symbol=symbol,
            include_payloads=True,
        )
        grouped = self._group_rows_by_plan_id(rows)
        integration = self.integration_status()
        for plan_id, group_rows in grouped.items():
            if not plan_id:
                continue
            group_exec_dispatches = [
                row.submission_payload['exec_dispatch']
                for row in group_rows
                if isinstance(row.submission_payload, dict)
                and isinstance(row.submission_payload.get('exec_dispatch'), dict)
            ]
            expected_destination = (
                _validated_exec_destination_binding(
                    group_exec_dispatches[0].get('destination_binding')
                )
                if len(group_exec_dispatches) == len(group_rows)
                else None
            )
            try:
                active_destination = self._active_destination_binding(
                    account_reference=str(group_rows[0].account_reference),
                    integration=integration,
                )
            except ValueError:
                active_destination = None
            destination_matches = bool(
                expected_destination is not None
                and active_destination == expected_destination
                and all(
                    _validated_exec_destination_binding(
                        exec_dispatch.get('destination_binding')
                    )
                    == expected_destination
                    for exec_dispatch in group_exec_dispatches
                )
            )
            if not destination_matches:
                reason_code = (
                    'exec_destination_binding_missing'
                    if expected_destination is None
                    else 'exec_destination_mismatch'
                )
                for row in group_rows:
                    self._mark_exec_destination_manual_review(
                        row_id=int(row.id),
                        trigger=trigger,
                        reason_code=reason_code,
                        expected_destination=expected_destination,
                        active_destination=active_destination,
                    )
                continue
            expected_row_bindings = {
                int(row.id): _row_readback_revision_binding(row)
                for row in group_rows
            }
            symbols = sorted({row.symbol for row in group_rows if row.symbol})
            plans_result = await self._fetch_remote_endpoint(
                lambda: self.client.list_execution_plans(external_plan_id=plan_id)
            )
            orders_result = await self._fetch_remote_endpoint(
                lambda: self.client.list_orders(external_plan_id=plan_id, symbols=symbols)
            )
            executions_result = await self._fetch_remote_endpoint(
                lambda: self.client.list_executions(external_plan_id=plan_id, symbols=symbols)
            )
            execution_plan_id = self._accepted_execution_plan_id_for_rows(
                group_rows
            )
            if execution_plan_id is None:
                positions_result = self._position_scope_contract_error(
                    code='position_plan_scope_unavailable',
                    message=(
                        'Accepted Exec plan identity is unavailable for scoped '
                        'position readback.'
                    ),
                )
            else:
                positions_result = await self._fetch_remote_endpoint(
                    lambda: self.client.list_positions(
                        external_plan_id=plan_id,
                        execution_plan_id=execution_plan_id,
                        symbols=symbols,
                    )
                )
                positions_result = self._validate_remote_position_scope_proofs(
                    result=positions_result,
                    external_plan_id=plan_id,
                    execution_plan_id=execution_plan_id,
                )
            self._persist_group_readback(
                rows=group_rows,
                expected_row_bindings=expected_row_bindings,
                trigger=trigger,
                runtime_payload=runtime_payload,
                plans_result=plans_result,
                orders_result=orders_result,
                executions_result=executions_result,
                positions_result=positions_result,
            )

    @staticmethod
    def _group_rows_by_plan_id(
        rows: Sequence[SignalDeciderExecutionPlanItem],
    ) -> dict[str, list[SignalDeciderExecutionPlanItem]]:
        grouped: dict[str, list[SignalDeciderExecutionPlanItem]] = defaultdict(list)
        for row in rows:
            submission = row.submission_payload if isinstance(row.submission_payload, dict) else {}
            exec_dispatch = submission.get('exec_dispatch') if isinstance(submission.get('exec_dispatch'), dict) else {}
            plan_id = _normalize_optional_text(exec_dispatch.get('plan_id'))
            if plan_id:
                grouped[plan_id].append(row)
        return grouped

    async def _fetch_remote_endpoint(self, factory: Any) -> dict[str, Any]:
        try:
            payload = await factory()
        except SignalDeciderExecClientError as exc:
            return {
                'status': self._client_error_status(exc),
                'payload': None,
                'items': [],
                'error': _serialize_error(
                    code=exc.code,
                    message=str(exc),
                    retryable=exc.retryable,
                    status_code=exc.status_code,
                ),
            }
        return {
            'status': REMOTE_FETCH_STATUS_OK,
            'payload': payload,
            'items': _collect_items(payload),
            'error': None,
        }

    @staticmethod
    def _looks_like_exec_plan_item(payload: dict[str, Any]) -> bool:
        if any(
            _normalize_optional_text(payload.get(key))
            for key in (
                'core_item_key',
                'client_key',
                'external_item_key',
                'execution_plan_item_id',
                'plan_item_id',
                'id',
            )
        ):
            return True
        return bool(
            _normalize_optional_text(payload.get('symbol'))
            and _normalize_optional_text(payload.get('intent') or payload.get('handoff_intent'))
        )

    @classmethod
    def _collect_exec_plan_readback_items(cls, *, plans_result: dict[str, Any]) -> list[dict[str, Any]]:
        payload = plans_result.get('payload') if isinstance(plans_result.get('payload'), dict) else {}
        plan_items: list[dict[str, Any]] = []

        def collect_from(candidate: Any) -> None:
            if not isinstance(candidate, dict):
                return
            nested_items: list[dict[str, Any]] = []
            for key in (
                'items',
                'item_acks',
                'plan_items',
                'execution_plan_items',
                'execution_items',
                'orders',
            ):
                value = candidate.get(key)
                if isinstance(value, list):
                    nested_items.extend(item for item in value if isinstance(item, dict))
            nested_plan = candidate.get('plan') if isinstance(candidate.get('plan'), dict) else None
            if nested_plan is not None:
                collect_from(nested_plan)
            if nested_items:
                for nested_item in nested_items:
                    collect_from(nested_item)
                return
            if cls._looks_like_exec_plan_item(candidate):
                plan_items.append(candidate)

        for item in _collect_items(payload):
            collect_from(item)
        collect_from(payload.get('plan') if isinstance(payload, dict) else None)
        return plan_items

    @staticmethod
    def _exec_plan_item_status(payload: dict[str, Any]) -> str | None:
        for key in (
            'status',
            'state',
            'plan_item_status',
            'decision_status',
            'monitoring_status',
            'monitoring_state',
            'watch_status',
            'lifecycle_status',
        ):
            status = _normalize_status(payload.get(key))
            if status:
                return status
        monitoring = payload.get('monitoring') if isinstance(payload.get('monitoring'), dict) else {}
        for key in ('status', 'state', 'monitoring_status', 'monitoring_state', 'watch_status'):
            status = _normalize_status(monitoring.get(key))
            if status:
                return status
        return None

    @staticmethod
    def _exec_plan_item_ids(payload: dict[str, Any]) -> set[str]:
        values = {
            _normalize_optional_text(payload.get(key))
            for key in (
                'execution_plan_item_id',
                'plan_item_id',
                'id',
                'exec_plan_item_id',
            )
        }
        return {value for value in values if value}

    @staticmethod
    def _exec_plan_ids(payload: dict[str, Any]) -> set[str]:
        values = {
            _normalize_optional_text(payload.get(key))
            for key in (
                'execution_plan_id',
                'exec_plan_id',
                'external_plan_id',
                'plan_id',
            )
        }
        return {value for value in values if value}

    @staticmethod
    def _exec_plan_item_core_keys(payload: dict[str, Any]) -> set[str]:
        values = {
            _normalize_optional_text(payload.get(key))
            for key in (
                'core_item_key',
                'client_key',
                'external_item_key',
            )
        }
        return {value for value in values if value}

    @classmethod
    def _bind_ambiguous_dispatch_from_strict_readback_ack(
        cls,
        *,
        exec_dispatch: dict[str, Any],
        plans_result: dict[str, Any],
    ) -> dict[str, Any] | None:
        plan_id = _normalize_optional_text(exec_dispatch.get('plan_id'))
        dispatch_material_fingerprint = _normalize_optional_text(
            exec_dispatch.get('dispatch_material_fingerprint')
        )
        item_links = [
            dict(link)
            for link in (exec_dispatch.get('item_links') or [])
            if isinstance(link, dict)
        ]
        submitted_core_item_keys = [
            str(link.get('core_item_key'))
            for link in item_links
            if _normalize_optional_text(link.get('core_item_key'))
        ]
        destination_binding = _validated_exec_destination_binding(
            exec_dispatch.get('destination_binding')
        )
        request_summary = (
            exec_dispatch.get('last_request_summary')
            if isinstance(exec_dispatch.get('last_request_summary'), dict)
            else {}
        )
        request_metadata = (
            request_summary.get('metadata')
            if isinstance(request_summary.get('metadata'), dict)
            else {}
        )
        if (
            not plan_id
            or not dispatch_material_fingerprint
            or not item_links
            or len(submitted_core_item_keys) != len(item_links)
            or destination_binding is None
            or request_metadata.get('dispatch_material_fingerprint')
            != dispatch_material_fingerprint
        ):
            return None
        payload = (
            plans_result.get('payload')
            if isinstance(plans_result.get('payload'), dict)
            else {}
        )
        remote_plan = next(
            (
                item
                for item in _collect_items(payload)
                if _normalize_optional_text(
                    item.get('plan_id') or item.get('external_plan_id')
                )
                == plan_id
            ),
            None,
        )
        if not isinstance(remote_plan, dict):
            return None
        raw_item_acks = remote_plan.get('item_acks')
        if not isinstance(raw_item_acks, list):
            raw_item_acks = remote_plan.get('items')
        candidate_response = {
            'plan_id': remote_plan.get('plan_id') or plan_id,
            'execution_plan_id': remote_plan.get('execution_plan_id'),
            'contract_ack': remote_plan.get('contract_ack'),
            'accepted_item_count': remote_plan.get('accepted_item_count'),
            'rejected_item_count': remote_plan.get('rejected_item_count'),
            'item_acks': raw_item_acks,
        }
        try:
            response_by_key = validate_execution_plan_dispatch_ack(
                candidate_response,
                plan_id=plan_id,
                submitted_core_item_keys=submitted_core_item_keys,
                expected_dispatch_material_fingerprint=(
                    dispatch_material_fingerprint
                ),
            )
        except ValueError:
            return None
        bound_links = []
        for link in item_links:
            core_item_key = str(link['core_item_key'])
            response_item = response_by_key[core_item_key]
            bound_links.append(
                {
                    **link,
                    'exec_plan_id': str(response_item['execution_plan_id']),
                    'exec_plan_item_id': str(
                        response_item['execution_plan_item_id']
                    ),
                    'remote_status': str(response_item['status']),
                    'response_item': compact_exec_response_item(response_item),
                }
            )
        accepted_item_bindings = _accepted_item_bindings_from_links(bound_links)
        if accepted_item_bindings is None:
            return None
        return {
            'item_links': bound_links,
            'contract_proof': {
                'schema_version': EXEC_DISPATCH_PROOF_SCHEMA_VERSION,
                'execution_plan_id': str(
                    candidate_response['execution_plan_id']
                ),
                'dispatch_material_fingerprint': dispatch_material_fingerprint,
                'destination_binding': copy.deepcopy(destination_binding),
                'contract_ack': copy.deepcopy(candidate_response['contract_ack']),
                'submitted_core_item_keys': submitted_core_item_keys,
                'accepted_item_bindings': accepted_item_bindings,
                'accepted_item_count': len(submitted_core_item_keys),
                'rejected_item_count': 0,
            },
            'last_response_summary': compact_exec_response_summary(
                candidate_response,
                item_links=item_links,
            ),
        }

    @classmethod
    def _match_exec_plan_readback_item(
        cls,
        *,
        link: dict[str, Any],
        plan_items: Sequence[dict[str, Any]],
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        link_core_key = _normalize_optional_text(link.get('core_item_key'))
        link_item_id = _normalize_optional_text(link.get('exec_plan_item_id'))
        link_plan_id = _normalize_optional_text(link.get('exec_plan_id'))
        if not link_core_key or not link_item_id or not link_plan_id:
            return None, {
                'core_item_key': link_core_key,
                'expected_execution_plan_id': link_plan_id,
                'expected_execution_plan_item_id': link_item_id,
                'reason_code': 'accepted_item_binding_missing',
            }

        related_items = [
            item
            for item in plan_items
            if link_core_key in cls._exec_plan_item_core_keys(item)
            or link_item_id in cls._exec_plan_item_ids(item)
        ]
        for item in related_items:
            if (
                link_core_key in cls._exec_plan_item_core_keys(item)
                and link_item_id in cls._exec_plan_item_ids(item)
                and link_plan_id in cls._exec_plan_ids(item)
            ):
                return item, None
        if not related_items:
            return None, None
        return None, {
            'core_item_key': link_core_key,
            'expected_execution_plan_id': link_plan_id,
            'expected_execution_plan_item_id': link_item_id,
            'observed_candidates': [
                {
                    'core_item_keys': sorted(cls._exec_plan_item_core_keys(item)),
                    'execution_plan_ids': sorted(cls._exec_plan_ids(item)),
                    'execution_plan_item_ids': sorted(
                        cls._exec_plan_item_ids(item)
                    ),
                }
                for item in related_items[:5]
            ],
            'reason_code': 'exec_readback_identity_mismatch',
        }

    @classmethod
    def _merge_exec_plan_readback_links(
        cls,
        *,
        item_links: Sequence[dict[str, Any]],
        plans_result: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        plan_items = cls._collect_exec_plan_readback_items(plans_result=plans_result)
        if not plan_items:
            return (
                [dict(link) for link in item_links if isinstance(link, dict)],
                [],
            )

        merged_links: list[dict[str, Any]] = []
        identity_mismatches: list[dict[str, Any]] = []
        for raw_link in item_links:
            if not isinstance(raw_link, dict):
                continue
            link = dict(raw_link)
            matched_item, identity_mismatch = cls._match_exec_plan_readback_item(
                link=link,
                plan_items=plan_items,
            )
            if identity_mismatch is not None:
                identity_mismatches.append(identity_mismatch)
            if matched_item is None:
                merged_links.append(link)
                continue

            status = cls._exec_plan_item_status(matched_item)
            if status:
                link['remote_status'] = status
            response_item = dict(link.get('response_item') or {})
            compact_item = compact_exec_response_item(matched_item)
            if compact_item:
                response_item.update(compact_item)
            if response_item:
                link['response_item'] = response_item
            merged_links.append(link)
        return merged_links, identity_mismatches

    @classmethod
    def _entry_link_active_statuses(cls, *, item_links: Sequence[dict[str, Any]]) -> list[str]:
        statuses: set[str] = set()
        for link in item_links:
            if not isinstance(link, dict):
                continue
            if _normalize_optional_text(link.get('intent'), lowercase=True) not in EXEC_ENTRY_BUY_INTENTS:
                continue
            statuses.update(
                status
                for status in cls._entry_link_statuses(link)
                if status in EXEC_WATCH_READBACK_ACTIVE_STATUSES
            )
        return sorted(statuses)

    @staticmethod
    def _append_warning_once(warnings: Sequence[Any], warning: dict[str, Any]) -> list[Any]:
        values = list(warnings)
        warning_code = _normalize_optional_text(warning.get('code'))
        if warning_code and any(
            isinstance(item, dict) and _normalize_optional_text(item.get('code')) == warning_code
            for item in values
        ):
            return values
        values.append(warning)
        return values

    @classmethod
    def _is_short_term_watch_buy_row(cls, row: SignalDeciderExecutionPlanItem) -> bool:
        if _normalize_decision_agent_name(row.decision_agent_name) != SHORT_TERM_DECIDER_AGENT_NAME:
            return False
        if _normalize_optional_text(row.planned_side, lowercase=True) != 'buy':
            return False
        traceability = row.traceability_payload if isinstance(row.traceability_payload, dict) else {}
        validation = row.validation_payload if isinstance(row.validation_payload, dict) else {}
        short_term = validation.get('short_term') if isinstance(validation.get('short_term'), dict) else {}
        raw_action = _normalize_optional_text(
            traceability.get('raw_action')
            or traceability.get('tactical_action')
            or short_term.get('raw_action')
            or short_term.get('tactical_action'),
            lowercase=True,
        )
        handoff_intent = _normalize_optional_text(
            traceability.get('handoff_intent') or short_term.get('handoff_intent'),
            lowercase=True,
        )
        return bool(
            raw_action in {'scalp_buy_candidate', 'watch_for_trigger'}
            or handoff_intent in {'tactical_buy', EXEC_INTENT_INTRADAY_WATCH_BUY, EXEC_INTENT_NEXT_OPEN_BUY}
        )

    def _sync_short_term_exec_watch_state(
        self,
        *,
        row: SignalDeciderExecutionPlanItem,
        item_links: Sequence[dict[str, Any]],
        now: datetime,
        trigger: str,
    ) -> dict[str, Any] | None:
        if not self._is_short_term_watch_buy_row(row):
            return None
        if _normalize_optional_text(row.execution_status, lowercase=True) in {
            'revocation_pending',
            'revocation_manual_review',
            'revoked',
        }:
            return None
        active_statuses = self._entry_link_active_statuses(item_links=item_links)
        if not active_statuses:
            return None

        original_eligibility_status = _normalize_optional_text(row.eligibility_status, lowercase=True)
        original_execution_status = _normalize_optional_text(row.execution_status, lowercase=True)
        core_needs_exec_watch_sync = bool(
            original_eligibility_status == ELIGIBILITY_BLOCKED
            or original_execution_status in {EXECUTION_STATUS_PLANNED, EXECUTION_STATUS_AWAITING_TRIGGER}
            or original_execution_status == EXECUTION_STATUS_SUBMIT_FAILED
        )
        if not core_needs_exec_watch_sync:
            return None
        validation = row.validation_payload if isinstance(row.validation_payload, dict) else {}
        traceability = row.traceability_payload if isinstance(row.traceability_payload, dict) else {}
        blocking_reasons = validation.get('blocking_reasons') if isinstance(validation.get('blocking_reasons'), list) else []
        warnings = validation.get('warnings') if isinstance(validation.get('warnings'), list) else []
        exec_watch_status = (
            'exec_watch_monitoring_active'
            if any(status in {'watch_monitoring_active', 'active', 'monitoring', 'open', 'working'} for status in active_statuses)
            else 'exec_watch_monitoring_pending'
        )
        audit = {
            'schema_version': EXEC_WATCH_STATE_SYNC_SCHEMA_VERSION,
            'status': exec_watch_status,
            'synced_at': now.isoformat(),
            'trigger': trigger,
            'source': 'exec_readback',
            'active_exec_statuses': active_statuses,
            'core_state_before': {
                'eligibility_status': original_eligibility_status,
                'execution_status': original_execution_status,
            },
            'core_state_after': {
                'eligibility_status': original_eligibility_status,
                'execution_status': EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING,
            },
            'divergence': core_needs_exec_watch_sync,
            'preserved_blocking_reason_count': len(blocking_reasons),
            'preserved_warning_count': len(warnings),
        }

        row.execution_status = EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING
        row.traceability_payload = {
            **traceability,
            'handoff_active': True,
            'exec_monitoring_active': True,
            'exec_agent_approved': True,
            'approval_state': 'approved',
            'exec_watch_state_sync': audit,
        }
        warning = {
            'code': EXEC_WATCH_STATE_SYNC_WARNING_CODE,
            'message': (
                'Exec readback shows active or pending watch monitoring while core still had blocked/planned '
                'or failed state; preserving original gate evidence and surfacing Exec watch state for monitoring.'
            ),
            'severity': 'warning',
            'exec_watch_status': exec_watch_status,
            'active_exec_statuses': active_statuses,
        }
        row.validation_payload = {
            **validation,
            'blocking_reasons': blocking_reasons,
            'warnings': self._append_warning_once(warnings, warning),
            'exec_watch_state_sync': audit,
        }
        return audit

    def _persist_group_readback(
        self,
        *,
        rows: Sequence[SignalDeciderExecutionPlanItem],
        expected_row_bindings: dict[int, dict[str, Any]],
        trigger: str,
        runtime_payload: dict[str, Any],
        plans_result: dict[str, Any],
        orders_result: dict[str, Any],
        executions_result: dict[str, Any],
        positions_result: dict[str, Any],
    ) -> None:
        now = _utcnow()
        row_ids = [int(row.id) for row in rows]
        applied_row_ids: list[int] = []
        with session_scope() as session:
            persisted_rows = session.execute(
                select(SignalDeciderExecutionPlanItem)
                .where(SignalDeciderExecutionPlanItem.id.in_(row_ids))
                .with_for_update()
            ).scalars().all()
            for row in persisted_rows:
                existing_submission = row.submission_payload if isinstance(row.submission_payload, dict) else {}
                exec_dispatch = (
                    existing_submission.get('exec_dispatch')
                    if isinstance(existing_submission.get('exec_dispatch'), dict)
                    else {}
                )
                expected_binding = expected_row_bindings.get(int(row.id))
                current_binding = _row_readback_revision_binding(row)
                if expected_binding is None or current_binding != expected_binding:
                    discard_attempt = {
                        'attempted_at': now.isoformat(),
                        'trigger': trigger,
                        'status': 'readback_discarded_revision_mismatch',
                        'expected_plan_id': (
                            expected_binding.get('plan_id')
                            if isinstance(expected_binding, dict)
                            else None
                        ),
                        'current_plan_id': current_binding.get('plan_id'),
                        'expected_dispatch_revision_fingerprint': (
                            expected_binding.get('dispatch_revision_fingerprint')
                            if isinstance(expected_binding, dict)
                            else None
                        ),
                        'current_dispatch_revision_fingerprint': current_binding.get(
                            'dispatch_revision_fingerprint'
                        ),
                        'expected_row_revision_fingerprint': (
                            expected_binding.get('current_row_revision_fingerprint')
                            if isinstance(expected_binding, dict)
                            else None
                        ),
                        'current_row_revision_fingerprint': current_binding.get(
                            'current_row_revision_fingerprint'
                        ),
                        'expected_contract_proof_fingerprint': (
                            expected_binding.get('contract_proof_fingerprint')
                            if isinstance(expected_binding, dict)
                            else None
                        ),
                        'current_contract_proof_fingerprint': current_binding.get(
                            'contract_proof_fingerprint'
                        ),
                    }
                    preserved_exec_dispatch = {
                        **copy.deepcopy(exec_dispatch),
                        'attempts': _append_limited(
                            exec_dispatch.get('attempts') or [],
                            discard_attempt,
                        ),
                    }
                    row.submission_payload = slim_signal_decider_exec_submission_payload(
                        {
                            **existing_submission,
                            'exec_dispatch': preserved_exec_dispatch,
                        }
                    ).payload
                    continue
                contract_proof = (
                    copy.deepcopy(exec_dispatch.get('contract_proof'))
                    if _row_has_accepted_exec_dispatch(row)
                    and isinstance(exec_dispatch.get('contract_proof'), dict)
                    else None
                )
                if (
                    contract_proof is None
                    and self._row_requires_ambiguous_acceptance_reconciliation(
                        row
                    )
                ):
                    recovered_binding = (
                        self._bind_ambiguous_dispatch_from_strict_readback_ack(
                            exec_dispatch=exec_dispatch,
                            plans_result=plans_result,
                        )
                    )
                    if recovered_binding is not None:
                        exec_dispatch = {
                            **exec_dispatch,
                            'dispatch_status': EXEC_DISPATCH_STATUS_ACCEPTED,
                            'last_success_at': now.isoformat(),
                            'last_error': None,
                            'item_links': recovered_binding['item_links'],
                            'contract_proof': recovered_binding[
                                'contract_proof'
                            ],
                            'last_response_summary': recovered_binding[
                                'last_response_summary'
                            ],
                        }
                        contract_proof = copy.deepcopy(
                            recovered_binding['contract_proof']
                        )
                item_links, identity_mismatches = self._merge_exec_plan_readback_links(
                    item_links=exec_dispatch.get('item_links') or [],
                    plans_result=plans_result,
                )
                if identity_mismatches:
                    identity_error = _serialize_error(
                        code='exec_readback_identity_mismatch',
                        message=(
                            'Exec plan readback identities conflict with the accepted '
                            'Core dispatch binding.'
                        ),
                    )
                    discard_attempt = {
                        'attempted_at': now.isoformat(),
                        'trigger': trigger,
                        'status': 'readback_discarded_identity_mismatch',
                        'reason_code': 'exec_readback_identity_mismatch',
                        'mismatch_count': len(identity_mismatches),
                    }
                    preserved_exec_dispatch = {
                        **copy.deepcopy(exec_dispatch),
                        'attempts': _append_limited(
                            exec_dispatch.get('attempts') or [],
                            discard_attempt,
                        ),
                        'readback': compact_exec_readback(
                            {
                                'last_synced_at': now.isoformat(),
                                'trigger': trigger,
                                'runtime': runtime_payload,
                                'execution_plans': {
                                    'status': REMOTE_FETCH_STATUS_CONTRACT_ERROR,
                                    'error': identity_error,
                                },
                                'orders': {
                                    'status': REMOTE_FETCH_STATUS_CONTRACT_ERROR,
                                    'count': 0,
                                    'items': [],
                                    'error': identity_error,
                                },
                                'executions': {
                                    'status': REMOTE_FETCH_STATUS_CONTRACT_ERROR,
                                    'count': 0,
                                    'items': [],
                                    'error': identity_error,
                                },
                                'positions': {
                                    'status': REMOTE_FETCH_STATUS_CONTRACT_ERROR,
                                    'count': 0,
                                    'items': [],
                                    'error': identity_error,
                                },
                                'derived_state': {
                                    'derived_status': REMOTE_FETCH_STATUS_UNAVAILABLE,
                                    'entry_filled_quantity': None,
                                    'exit_filled_quantity': None,
                                    'open_quantity': None,
                                    'closed_quantity': None,
                                    'plan_item_status_counts': {},
                                    'order_status_counts': {},
                                    'endpoint_statuses': {
                                        key: REMOTE_FETCH_STATUS_CONTRACT_ERROR
                                        for key in (
                                            'plans',
                                            'orders',
                                            'executions',
                                            'positions',
                                        )
                                    },
                                },
                            }
                        ),
                    }
                    row.submission_payload = slim_signal_decider_exec_submission_payload(
                        {
                            **existing_submission,
                            'dispatch_reconciliation': {
                                'status': 'manual_review_required',
                                'reason_code': 'exec_readback_identity_mismatch',
                                'observed_at': now.isoformat(),
                                'trigger': trigger,
                                'plan_id': exec_dispatch.get('plan_id'),
                                'identity_mismatches': identity_mismatches,
                            },
                            'exec_dispatch': preserved_exec_dispatch,
                        }
                    ).payload
                    continue
                matched_orders, order_identity_mismatches = self._match_remote_orders(
                    row=row,
                    item_links=item_links,
                    orders=orders_result['items'],
                )
                row_orders_result = orders_result
                if order_identity_mismatches:
                    row_orders_result = self._position_scope_contract_error(
                        code='order_readback_identity_mismatch',
                        message=(
                            'Exec order readback returned an explicit plan-item identity '
                            'that does not match the accepted Core item binding.'
                        ),
                    )
                matched_executions, execution_identity_mismatches = self._match_remote_executions(
                    row=row,
                    item_links=item_links,
                    executions=executions_result['items'],
                    orders=matched_orders,
                )
                row_executions_result = executions_result
                if execution_identity_mismatches:
                    row_executions_result = self._position_scope_contract_error(
                        code='execution_readback_identity_mismatch',
                        message=(
                            'Exec execution readback returned an explicit order or '
                            'plan-item identity that does not match the accepted Core item binding.'
                        ),
                    )
                matched_positions = self._match_remote_positions(row=row, positions=positions_result['items'])
                derived_state = self._derive_remote_state(
                    row=row,
                    item_links=item_links,
                    orders=matched_orders,
                    executions=matched_executions,
                    positions=matched_positions,
                    endpoint_statuses={
                        'plans': plans_result['status'],
                        'orders': row_orders_result['status'],
                        'executions': row_executions_result['status'],
                        'positions': positions_result['status'],
                    },
                )
                readback = {
                    'last_synced_at': now.isoformat(),
                    'trigger': trigger,
                    'runtime': runtime_payload,
                    'execution_plans': {
                        'status': plans_result['status'],
                        'error': plans_result['error'],
                    },
                    'orders': {
                        'status': row_orders_result['status'],
                        'count': 0 if order_identity_mismatches else len(matched_orders),
                        'items': [] if order_identity_mismatches else matched_orders,
                        'error': row_orders_result['error'],
                    },
                    'executions': {
                        'status': row_executions_result['status'],
                        'count': 0 if execution_identity_mismatches else len(matched_executions),
                        'items': [] if execution_identity_mismatches else matched_executions,
                        'error': row_executions_result['error'],
                    },
                    'positions': {
                        'status': positions_result['status'],
                        'count': len(matched_positions),
                        'items': matched_positions,
                        'error': positions_result['error'],
                    },
                    'derived_state': derived_state,
                }
                compact_readback = compact_exec_readback(readback)
                core_state_sync = self._sync_short_term_exec_watch_state(
                    row=row,
                    item_links=item_links,
                    now=now,
                    trigger=trigger,
                ) or exec_dispatch.get('core_state_sync')
                exec_dispatch = {
                    'plan_id': exec_dispatch.get('plan_id'),
                    'plan_fingerprint': exec_dispatch.get('plan_fingerprint'),
                    'dispatch_revision_fingerprint': exec_dispatch.get(
                        'dispatch_revision_fingerprint'
                    ),
                    'dispatch_material_fingerprint': exec_dispatch.get(
                        'dispatch_material_fingerprint'
                    ),
                    'dispatch_batch_key': exec_dispatch.get(
                        'dispatch_batch_key'
                    ),
                    'dispatch_status': exec_dispatch.get('dispatch_status'),
                    'prior_execution_status': exec_dispatch.get(
                        'prior_execution_status'
                    ),
                    'prior_eligibility_status': exec_dispatch.get(
                        'prior_eligibility_status'
                    ),
                    'last_attempt_at': exec_dispatch.get('last_attempt_at'),
                    'last_success_at': exec_dispatch.get('last_success_at'),
                    'last_error': summarize_error(exec_dispatch.get('last_error')),
                    'item_links': [
                        compact
                        for item in item_links
                        if (compact := compact_exec_item_link(item))
                    ],
                    'destination_binding': copy.deepcopy(
                        exec_dispatch.get('destination_binding')
                    ),
                    'authority_generation_before': copy.deepcopy(
                        exec_dispatch.get('authority_generation_before')
                    ),
                    'attempts': exec_dispatch.get('attempts') or [],
                    'last_request_summary': exec_dispatch.get('last_request_summary'),
                    'last_response_summary': exec_dispatch.get('last_response_summary'),
                    'readback': compact_readback,
                }
                if isinstance(core_state_sync, dict):
                    exec_dispatch['core_state_sync'] = core_state_sync
                if contract_proof is not None:
                    exec_dispatch['contract_proof'] = contract_proof
                submission = {
                    **existing_submission,
                    'exec_dispatch': exec_dispatch,
                }
                row.submission_payload = slim_signal_decider_exec_submission_payload(submission).payload
                applied_row_ids.append(int(row.id))
            session.flush()
        if applied_row_ids:
            signal_decider_execution_outcome_service.sync_entry_plan_items(
                entry_plan_item_ids=applied_row_ids,
            )

    @staticmethod
    def _match_remote_orders(
        *,
        row: SignalDeciderExecutionPlanItem,
        item_links: Sequence[dict[str, Any]],
        orders: Sequence[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        links_by_exec_item_id = {
            exec_item_id: link
            for link in item_links
            if (exec_item_id := _normalize_optional_text(link.get('exec_plan_item_id')))
        }
        exec_item_ids = set(links_by_exec_item_id)
        matched: list[dict[str, Any]] = []
        identity_mismatches: list[dict[str, Any]] = []
        for order in orders:
            order_item_ids = {
                normalized_item_id
                for raw_item_id in (
                    order.get('execution_plan_item_id'),
                    order.get('plan_item_id'),
                    _extract_field(order, 'execution_plan_item', 'id'),
                )
                if (normalized_item_id := _normalize_optional_text(raw_item_id))
            }
            order_item_id = next(iter(order_item_ids)) if len(order_item_ids) == 1 else None
            symbol = _normalize_optional_text(order.get('symbol'))
            if order_item_ids:
                expected_link = links_by_exec_item_id.get(order_item_id)
                expected_symbol = (
                    _normalize_optional_text(expected_link.get('symbol'))
                    if expected_link is not None
                    else None
                ) or row.symbol
                expected_side = (
                    _normalize_optional_text(
                        expected_link.get('side'),
                        lowercase=True,
                    )
                    if expected_link is not None
                    else None
                )
                observed_side = _normalize_optional_text(
                    order.get('side'),
                    lowercase=True,
                )
                content_matches = bool(
                    (symbol is None or symbol == expected_symbol)
                    and (
                        observed_side is None
                        or expected_side is None
                        or observed_side == expected_side
                    )
                )
                if (
                    len(order_item_ids) == 1
                    and expected_link is not None
                    and content_matches
                ):
                    matched.append(order)
                elif (
                    any(item_id in exec_item_ids for item_id in order_item_ids)
                    or (symbol and symbol == row.symbol)
                ):
                    identity_mismatches.append(
                        {
                            'observed_execution_plan_item_id': order_item_id,
                            'observed_execution_plan_item_ids': sorted(order_item_ids),
                            'expected_execution_plan_item_ids': sorted(exec_item_ids),
                            'observed_symbol': symbol,
                            'expected_symbol': expected_symbol,
                            'observed_side': observed_side,
                            'expected_side': expected_side,
                        }
                    )
                continue
            if symbol and symbol == row.symbol:
                matched.append(order)
        return matched, identity_mismatches

    @staticmethod
    def _match_remote_executions(
        *,
        row: SignalDeciderExecutionPlanItem,
        item_links: Sequence[dict[str, Any]],
        executions: Sequence[dict[str, Any]],
        orders: Sequence[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        links_by_exec_item_id = {
            exec_item_id: link
            for link in item_links
            if (exec_item_id := _normalize_optional_text(link.get('exec_plan_item_id')))
        }
        exec_item_ids = set(links_by_exec_item_id)
        internal_order_indexes: dict[str, set[int]] = defaultdict(set)
        external_order_indexes: dict[str, set[int]] = defaultdict(set)
        any_order_indexes: dict[str, set[int]] = defaultdict(set)
        for order_index, order in enumerate(orders):
            for raw_order_id in (order.get('id'), order.get('broker_order_pk')):
                if normalized_order_id := _normalize_optional_text(raw_order_id):
                    internal_order_indexes[normalized_order_id].add(order_index)
                    any_order_indexes[normalized_order_id].add(order_index)
            for raw_order_id in (order.get('broker_order_id'), order.get('order_id')):
                if normalized_order_id := _normalize_optional_text(raw_order_id):
                    external_order_indexes[normalized_order_id].add(order_index)
                    any_order_indexes[normalized_order_id].add(order_index)
        order_ids = set(any_order_indexes)
        matched: list[dict[str, Any]] = []
        identity_mismatches: list[dict[str, Any]] = []
        for execution in executions:
            symbol = _normalize_optional_text(execution.get('symbol'))
            execution_item_ids = {
                normalized_item_id
                for raw_item_id in (
                    execution.get('execution_plan_item_id'),
                    execution.get('plan_item_id'),
                    execution.get('exec_plan_item_id'),
                )
                if (normalized_item_id := _normalize_optional_text(raw_item_id))
            }
            execution_item_id = (
                next(iter(execution_item_ids))
                if len(execution_item_ids) == 1
                else None
            )
            observed_order_identities = [
                (field_name, normalized_order_id, index)
                for field_name, raw_order_id, index in (
                    (
                        'broker_order_pk',
                        execution.get('broker_order_pk'),
                        internal_order_indexes,
                    ),
                    (
                        'broker_order_id',
                        execution.get('broker_order_id'),
                        external_order_indexes,
                    ),
                    ('order_id', execution.get('order_id'), any_order_indexes),
                )
                if (normalized_order_id := _normalize_optional_text(raw_order_id))
            ]
            linked_order_indexes: set[int] | None = None
            for _, normalized_order_id, index in observed_order_identities:
                candidates = set(index.get(normalized_order_id, set()))
                linked_order_indexes = (
                    candidates
                    if linked_order_indexes is None
                    else linked_order_indexes & candidates
                )
            linked_order_index = (
                next(iter(linked_order_indexes))
                if linked_order_indexes is not None and len(linked_order_indexes) == 1
                else None
            )
            linked_order = (
                orders[linked_order_index]
                if linked_order_index is not None
                else None
            )
            linked_order_id = (
                observed_order_identities[0][1]
                if observed_order_identities
                else None
            )
            explicit_identity_present = bool(
                execution_item_ids or observed_order_identities
            )
            item_identity_matches = bool(
                len(execution_item_ids) == 1
                and execution_item_id in exec_item_ids
            )
            order_identity_matches = bool(
                observed_order_identities and linked_order is not None
            )
            if explicit_identity_present:
                expected_link = links_by_exec_item_id.get(execution_item_id)
                linked_order_item_ids = (
                    {
                        normalized_item_id
                        for raw_item_id in (
                            linked_order.get('execution_plan_item_id'),
                            linked_order.get('plan_item_id'),
                            _extract_field(
                                linked_order,
                                'execution_plan_item',
                                'id',
                            ),
                        )
                        if (
                            normalized_item_id := _normalize_optional_text(
                                raw_item_id
                            )
                        )
                    }
                    if linked_order is not None
                    else set()
                )
                linked_order_item_id = (
                    next(iter(linked_order_item_ids))
                    if len(linked_order_item_ids) == 1
                    else None
                )
                cross_binding_matches = bool(
                    not (execution_item_ids and observed_order_identities)
                    or (
                        linked_order_item_id is not None
                        and linked_order_item_id == execution_item_id
                    )
                )
                expected_symbol = (
                    _normalize_optional_text(expected_link.get('symbol'))
                    if expected_link is not None
                    else _normalize_optional_text(linked_order.get('symbol'))
                    if linked_order is not None
                    else None
                ) or row.symbol
                expected_side = (
                    _normalize_optional_text(
                        expected_link.get('side'),
                        lowercase=True,
                    )
                    if expected_link is not None
                    else _normalize_optional_text(
                        linked_order.get('side'),
                        lowercase=True,
                    )
                    if linked_order is not None
                    else None
                )
                observed_side = _normalize_optional_text(
                    execution.get('side'),
                    lowercase=True,
                )
                content_matches = bool(
                    (symbol is None or symbol == expected_symbol)
                    and (
                        observed_side is None
                        or expected_side is None
                        or observed_side == expected_side
                    )
                )
                all_explicit_identities_match = bool(
                    (not execution_item_ids or item_identity_matches)
                    and (not observed_order_identities or order_identity_matches)
                    and cross_binding_matches
                    and content_matches
                )
                if all_explicit_identities_match:
                    matched.append(execution)
                elif (
                    expected_link is not None
                    or linked_order is not None
                    or (symbol and symbol == row.symbol)
                ):
                    identity_mismatches.append(
                        {
                            'observed_execution_plan_item_id': execution_item_id,
                            'observed_execution_plan_item_ids': sorted(
                                execution_item_ids
                            ),
                            'observed_broker_order_id': linked_order_id,
                            'observed_order_identities': {
                                field_name: value
                                for field_name, value, _ in observed_order_identities
                            },
                            'expected_execution_plan_item_ids': sorted(exec_item_ids),
                            'expected_broker_order_ids': sorted(order_ids),
                            'linked_order_execution_plan_item_id': linked_order_item_id,
                            'observed_symbol': symbol,
                            'expected_symbol': expected_symbol,
                            'observed_side': observed_side,
                            'expected_side': expected_side,
                        }
                    )
                continue
            if symbol and symbol == row.symbol:
                matched.append(execution)
        return matched, identity_mismatches

    @staticmethod
    def _accepted_execution_plan_id_for_rows(
        rows: Sequence[SignalDeciderExecutionPlanItem],
    ) -> str | None:
        execution_plan_ids: set[str] = set()
        for row in rows:
            if not _row_has_accepted_exec_dispatch(row):
                return None
            submission = (
                row.submission_payload
                if isinstance(row.submission_payload, dict)
                else {}
            )
            exec_dispatch = (
                submission.get('exec_dispatch')
                if isinstance(submission.get('exec_dispatch'), dict)
                else {}
            )
            proof = (
                exec_dispatch.get('contract_proof')
                if isinstance(exec_dispatch.get('contract_proof'), dict)
                else {}
            )
            execution_plan_id = _normalize_optional_text(
                proof.get('execution_plan_id')
            )
            if execution_plan_id is None:
                return None
            execution_plan_ids.add(execution_plan_id)
        return (
            next(iter(execution_plan_ids))
            if len(execution_plan_ids) == 1
            else None
        )

    @staticmethod
    def _position_scope_contract_error(
        *,
        code: str,
        message: str,
    ) -> dict[str, Any]:
        return {
            'status': REMOTE_FETCH_STATUS_CONTRACT_ERROR,
            'payload': None,
            'items': [],
            'error': _serialize_error(code=code, message=message),
        }

    @classmethod
    def _validate_remote_position_scope_proofs(
        cls,
        *,
        result: dict[str, Any],
        external_plan_id: str,
        execution_plan_id: str,
    ) -> dict[str, Any]:
        if result.get('status') != REMOTE_FETCH_STATUS_OK:
            return result
        required_proof_fields = {
            'schema_version',
            'plan_id',
            'execution_plan_id',
            'provider',
            'account_scope',
            'exchange',
        }
        account_scopes: set[str] = set()
        for position in result.get('items') or []:
            proof = (
                position.get('position_scope_proof')
                if isinstance(position, dict)
                and isinstance(position.get('position_scope_proof'), dict)
                else None
            )
            proof_provider = _normalize_optional_text(
                proof.get('provider') if proof else None,
                lowercase=True,
            )
            position_provider = _normalize_optional_text(
                position.get('provider') if isinstance(position, dict) else None,
                lowercase=True,
            )
            proof_account_scope = _normalize_optional_text(
                proof.get('account_scope') if proof else None
            )
            position_account_scope = _normalize_optional_text(
                position.get('account_scope') if isinstance(position, dict) else None
            )
            proof_exchange = _normalize_optional_text(
                proof.get('exchange') if proof else None
            )
            position_exchange = _normalize_optional_text(
                position.get('exchange') if isinstance(position, dict) else None
            )
            if (
                proof is None
                or set(proof) != required_proof_fields
                or proof.get('schema_version')
                != EXEC_POSITION_SCOPE_PROOF_SCHEMA_VERSION
                or _normalize_optional_text(proof.get('plan_id'))
                != external_plan_id
                or _normalize_optional_text(proof.get('execution_plan_id'))
                != execution_plan_id
                or proof_provider != position_provider
                or proof_provider != 'kis'
                or proof_account_scope != position_account_scope
                or proof_account_scope is None
                or proof_exchange != position_exchange
                or str(proof_exchange or '').upper() != 'KRX'
            ):
                return cls._position_scope_contract_error(
                    code='position_scope_proof_invalid',
                    message=(
                        'Exec position readback was not bound to the requested '
                        'plan and opaque account route.'
                    ),
                )
            account_scopes.add(proof_account_scope)
        if len(account_scopes) > 1:
            return cls._position_scope_contract_error(
                code='position_scope_proof_invalid',
                message=(
                    'Exec position readback mixed multiple opaque account routes '
                    'for one accepted execution plan.'
                ),
            )
        return result

    @staticmethod
    def _match_remote_positions(
        *,
        row: SignalDeciderExecutionPlanItem,
        positions: Sequence[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        return [
            item
            for item in positions
            if _normalize_optional_text(item.get('symbol')) == row.symbol
        ]

    def _derive_remote_state(
        self,
        *,
        row: SignalDeciderExecutionPlanItem,
        item_links: Sequence[dict[str, Any]],
        orders: Sequence[dict[str, Any]],
        executions: Sequence[dict[str, Any]],
        positions: Sequence[dict[str, Any]],
        endpoint_statuses: dict[str, str],
    ) -> dict[str, Any]:
        if any(status != REMOTE_FETCH_STATUS_OK for status in endpoint_statuses.values()):
            return {
                'derived_status': REMOTE_FETCH_STATUS_UNAVAILABLE,
                'entry_filled_quantity': None,
                'exit_filled_quantity': None,
                'open_quantity': None,
                'closed_quantity': None,
                'plan_item_status_counts': {},
                'order_status_counts': {},
                'endpoint_statuses': endpoint_statuses,
            }

        buy_quantity = Decimal('0')
        sell_quantity = Decimal('0')
        order_status_counts: Counter[str] = Counter()
        for order in orders:
            status = _normalize_status(order.get('status') or order.get('current_status') or order.get('broker_status'))
            if status:
                order_status_counts[status] += 1
        for execution in executions:
            quantity = _safe_decimal(execution.get('quantity') or execution.get('filled_quantity')) or Decimal('0')
            side = _normalize_status(execution.get('side'))
            if side == 'sell':
                sell_quantity += quantity
            else:
                buy_quantity += quantity
        if buy_quantity <= Decimal('0'):
            buy_quantity = self._sum_order_filled_quantity(orders=orders, side='buy')
        if sell_quantity <= Decimal('0'):
            sell_quantity = self._sum_order_filled_quantity(orders=orders, side='sell')

        position_quantity = Decimal('0')
        if positions:
            first_position = positions[0]
            position_quantity = _safe_decimal(first_position.get('quantity')) or Decimal('0')

        item_statuses = {
            _normalize_status(link.get('remote_status'))
            for link in item_links
            if _normalize_status(link.get('remote_status'))
        }
        plan_item_status_counts = Counter(status for status in item_statuses if status)
        status_set = item_statuses.union(set(order_status_counts.keys()))
        has_entry_fill_evidence = (
            buy_quantity > Decimal('0')
            or self._has_order_fill_evidence(orders=orders, side='buy')
            or bool(order_status_counts.keys() & REMOTE_PARTIAL_STATUSES)
        )
        has_exit_fill_evidence = sell_quantity > Decimal('0') or self._has_order_fill_evidence(orders=orders, side='sell')
        has_plan_fill_evidence = has_entry_fill_evidence or has_exit_fill_evidence
        attributed_position_quantity = position_quantity if has_plan_fill_evidence else Decimal('0')
        open_quantity = max(attributed_position_quantity, buy_quantity - sell_quantity)

        derived_status = EXEC_DISPATCH_STATUS_ACCEPTED
        if sell_quantity > 0 and (open_quantity <= 0 or (buy_quantity > 0 and sell_quantity >= buy_quantity)):
            derived_status = 'closed'
        elif open_quantity > 0 or has_entry_fill_evidence:
            derived_status = 'open'
        elif any(status in REMOTE_SUBMITTED_STATUSES for status in item_statuses):
            derived_status = 'working'
        elif any(status in REMOTE_ACTIVE_STATUSES for status in order_status_counts):
            derived_status = 'working'
        elif any(status in REMOTE_CANCELLED_STATUSES for status in status_set):
            derived_status = UI_BOARD_STATUS_CANCELLED
        elif any(status in REMOTE_TERMINAL_FAILURE_STATUSES for status in status_set):
            derived_status = 'failed'
        elif any(status in EXEC_WATCH_READBACK_ACTIVE_STATUSES for status in item_statuses):
            derived_status = EXEC_DISPATCH_STATUS_ACCEPTED

        return {
            'derived_status': derived_status,
            'entry_filled_quantity': _decimal_to_text(buy_quantity if buy_quantity > 0 else None),
            'exit_filled_quantity': _decimal_to_text(sell_quantity if sell_quantity > 0 else None),
            'open_quantity': _decimal_to_text(open_quantity if open_quantity > 0 else Decimal('0')),
            'closed_quantity': _decimal_to_text(sell_quantity if sell_quantity > 0 else Decimal('0')),
            'has_plan_attributed_fill_evidence': has_plan_fill_evidence,
            'plan_item_status_counts': dict(sorted(plan_item_status_counts.items())),
            'order_status_counts': dict(sorted(order_status_counts.items())),
            'endpoint_statuses': endpoint_statuses,
        }

    @staticmethod
    def _build_core_state_divergence_warning(
        *,
        sync_audit: dict[str, Any] | None,
        item: dict[str, Any],
        active_statuses: Sequence[str] | None = None,
    ) -> dict[str, Any] | None:
        if isinstance(sync_audit, dict) and not bool(sync_audit.get('divergence')):
            return None
        statuses = list(active_statuses or [])
        if not statuses and isinstance(sync_audit, dict):
            statuses = [
                str(status)
                for status in (sync_audit.get('active_exec_statuses') or [])
                if _normalize_optional_text(status)
            ]
        if not statuses:
            return None
        core_state_before = sync_audit.get('core_state_before') if isinstance(sync_audit, dict) else None
        if not isinstance(core_state_before, dict):
            core_state_before = {
                'eligibility_status': item.get('eligibility_status'),
                'execution_status': item.get('execution_status'),
            }
        return {
            'code': EXEC_WATCH_STATE_SYNC_WARNING_CODE,
            'message': (
                'Exec has active or pending watch monitoring while core still had blocked/planned state; '
                'runtime views are using Exec watch truth and preserving original core gate evidence.'
            ),
            'core_state_before': core_state_before,
            'exec_statuses': statuses,
        }

    def _augment_exec_overview_item(self, item: dict[str, Any]) -> dict[str, Any]:
        submission = item.get('submission') if isinstance(item.get('submission'), dict) else {}
        exec_dispatch = submission.get('exec_dispatch') if isinstance(submission.get('exec_dispatch'), dict) else {}
        readback = exec_dispatch.get('readback') if isinstance(exec_dispatch.get('readback'), dict) else {}
        traceability = item.get('traceability') if isinstance(item.get('traceability'), dict) else {}
        validation = item.get('validation') if isinstance(item.get('validation'), dict) else {}
        decision_agent_name = _normalize_decision_agent_name(
            item.get('decision_agent_name') or traceability.get('decision_agent_name') or traceability.get('agent_name')
        )
        dispatch_status = _normalize_status(exec_dispatch.get('dispatch_status'))
        derived_state = readback.get('derived_state') if isinstance(readback.get('derived_state'), dict) else {}
        raw_action = traceability.get('raw_action') or traceability.get('tactical_action')
        generic_action = _normalize_short_term_generic_action(
            raw_action=raw_action,
            generic_action=traceability.get('generic_action'),
        )
        handoff_intent = traceability.get('handoff_intent')
        if handoff_intent is None and decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME:
            if raw_action == 'scalp_buy_candidate':
                handoff_intent = 'tactical_buy'
            elif raw_action == 'watch_for_trigger':
                handoff_intent = EXEC_INTENT_INTRADAY_WATCH_BUY
        handoff_active = bool(traceability.get('handoff_active')) or bool(
            decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME
            and item.get('eligibility_status') in {ELIGIBILITY_ELIGIBLE, ELIGIBILITY_MONITOR_CANDIDATE}
            and handoff_intent in {'tactical_buy', EXEC_INTENT_INTRADAY_WATCH_BUY}
        )
        dispatch_payload = {
            'status': dispatch_status
            or (
                EXEC_DISPATCH_STATUS_SKIPPED
                if item.get('eligibility_status') == ELIGIBILITY_BLOCKED
                else EXECUTION_STATUS_PLANNED
            ),
            'plan_id': exec_dispatch.get('plan_id'),
            'plan_fingerprint': exec_dispatch.get('plan_fingerprint'),
            'last_attempt_at': exec_dispatch.get('last_attempt_at'),
            'last_success_at': exec_dispatch.get('last_success_at'),
            'last_error': exec_dispatch.get('last_error'),
            'item_links': exec_dispatch.get('item_links') or [],
        }
        agent_decision = self._extract_exec_agent_decision({**item, 'dispatch': dispatch_payload})
        exec_agent_approved = bool(agent_decision['approved'])
        exec_agent_rejected = bool(agent_decision['rejected'])
        remote_status = _normalize_status(derived_state.get('derived_status'))
        terminal = remote_status in EXEC_MONITORING_TERMINAL_STATUSES or dispatch_status in {
            EXEC_DISPATCH_STATUS_FAILED,
            EXEC_DISPATCH_STATUS_CONTRACT_ERROR,
            EXEC_DISPATCH_STATUS_UNAVAILABLE,
        }
        held_position = (_positive_decimal(derived_state.get('open_quantity')) or Decimal('0')) > Decimal('0')
        active_remote = (
            dispatch_status in {EXEC_DISPATCH_STATUS_ACCEPTED, EXEC_DISPATCH_STATUS_PARTIAL}
            or remote_status in EXEC_MONITORING_ACTIVE_STATUSES
        )
        exec_monitoring_active = bool(held_position or (exec_agent_approved and active_remote and not terminal))
        active_exec_statuses = self._entry_link_active_statuses(item_links=dispatch_payload.get('item_links') or [])
        core_was_blocked_or_planned = bool(
            item.get('eligibility_status') == ELIGIBILITY_BLOCKED
            or _normalize_status(item.get('execution_status'))
            in {EXECUTION_STATUS_PLANNED, EXECUTION_STATUS_AWAITING_TRIGGER}
        )
        sync_audit = (
            exec_dispatch.get('core_state_sync')
            if isinstance(exec_dispatch.get('core_state_sync'), dict)
            else validation.get('exec_watch_state_sync')
            if isinstance(validation.get('exec_watch_state_sync'), dict)
            else None
        )
        divergence_warning = self._build_core_state_divergence_warning(
            sync_audit=sync_audit
            if isinstance(sync_audit, dict)
            else {'divergence': core_was_blocked_or_planned},
            item=item,
            active_statuses=active_exec_statuses
            if decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME and core_was_blocked_or_planned and exec_monitoring_active
            else [],
        )
        if exec_agent_approved:
            approval_state = 'approved'
        elif exec_agent_rejected:
            approval_state = 'rejected'
        elif item.get('eligibility_status') == ELIGIBILITY_BLOCKED:
            approval_state = 'blocked'
        elif dispatch_status in {EXEC_DISPATCH_STATUS_ACCEPTED, EXEC_DISPATCH_STATUS_PARTIAL}:
            approval_state = 'pending_exec_agent'
        elif dispatch_status:
            approval_state = dispatch_status
        else:
            approval_state = 'not_dispatched'
        return {
            **item,
            'agent_name': decision_agent_name,
            'decision_agent_name': decision_agent_name,
            'raw_action': raw_action,
            'generic_action': generic_action,
            'handoff_intent': handoff_intent,
            'handoff_active': handoff_active,
            'exec_monitoring_active': exec_monitoring_active,
            'exec_agent_approved': exec_agent_approved,
            'approval_state': approval_state,
            'exec_agent_action': agent_decision['action'],
            'exec_agent_reason_code': agent_decision['reason_code'],
            'exec_agent_target_profile': agent_decision['target_profile'],
            'exec_agent_target_tag': agent_decision['target_tag'],
            'exec_watch_state_sync': sync_audit,
            'core_state_divergence_warning': divergence_warning,
            'target_price': traceability.get('target_price'),
            'stop_loss_price': traceability.get('stop_loss_price'),
            'trigger_price': traceability.get('trigger_price'),
            'chase_limit_price': traceability.get('chase_limit_price'),
            'decision_run_id': traceability.get('decision_run_id'),
            'source_feedback_id': traceability.get('source_feedback_id') or traceability.get('feedback_id'),
            'tactical_action': traceability.get('tactical_action'),
            'expected_holding_period': traceability.get('expected_holding_period')
            or traceability.get('suggested_holding_period'),
            'entry_execution_mode': traceability.get('entry_execution_mode'),
            'dispatch': dispatch_payload,
            'remote_state': {
                'last_synced_at': readback.get('last_synced_at'),
                'runtime': readback.get('runtime') if isinstance(readback.get('runtime'), dict) else None,
                'derived_status': derived_state.get('derived_status'),
                'entry_filled_quantity': derived_state.get('entry_filled_quantity'),
                'exit_filled_quantity': derived_state.get('exit_filled_quantity'),
                'open_quantity': derived_state.get('open_quantity'),
                'closed_quantity': derived_state.get('closed_quantity'),
                'plan_item_status_counts': derived_state.get('plan_item_status_counts') or {},
                'orders': _extract_field(readback, 'orders', 'items') or [],
                'executions': _extract_field(readback, 'executions', 'items') or [],
                'positions': _extract_field(readback, 'positions', 'items') or [],
                'endpoint_statuses': derived_state.get('endpoint_statuses') or {},
            },
        }

    def _build_exec_overview_summary(self, *, items: Sequence[dict[str, Any]], plan_summary: dict[str, Any]) -> dict[str, Any]:
        dispatch_status_counts = Counter(
            _normalize_status((item.get('dispatch') or {}).get('status')) or 'unknown'
            for item in items
        )
        remote_status_counts = Counter(
            _normalize_status((item.get('remote_state') or {}).get('derived_status')) or 'unknown'
            for item in items
        )
        divergence_items = [
            item
            for item in items
            if isinstance(item.get('core_state_divergence_warning'), dict)
        ]
        warnings = []
        if divergence_items:
            warnings.append(
                {
                    'code': EXEC_WATCH_STATE_SYNC_WARNING_CODE,
                    'message': (
                        'Exec has active or pending watch monitoring for rows that core still had as blocked/planned.'
                    ),
                    'count': len(divergence_items),
                    'symbols': sorted(
                        {
                            str(item.get('symbol'))
                            for item in divergence_items
                            if _normalize_optional_text(item.get('symbol'))
                        }
                    ),
                }
            )
        return {
            **plan_summary,
            'decision_agent_counts': dict(
                sorted(
                    Counter(
                        _normalize_decision_agent_name(item.get('decision_agent_name'))
                        for item in items
                    ).items()
                )
            ),
            'dispatch_status_counts': dict(sorted(dispatch_status_counts.items())),
            'remote_status_counts': dict(sorted(remote_status_counts.items())),
            'remote_open_count': sum(
                1 for item in items if (item.get('remote_state') or {}).get('derived_status') == 'open'
            ),
            'remote_closed_count': sum(
                1 for item in items if (item.get('remote_state') or {}).get('derived_status') == 'closed'
            ),
            'core_exec_state_divergence_count': len(divergence_items),
            'warnings': warnings,
        }

    def _build_intraday_watch_item(self, item: dict[str, Any]) -> dict[str, Any]:
        return {
            'plan_item_id': item.get('plan_item_id'),
            'execution_window': item.get('execution_window'),
            'agent_name': item.get('decision_agent_name'),
            'decision_agent_name': item.get('decision_agent_name'),
            'target_trade_date': item.get('target_trade_date'),
            'source_market_date': item.get('source_market_date'),
            'account_reference': item.get('account_reference'),
            'account_label': item.get('account_label'),
            'symbol': item.get('symbol'),
            'policy_version': item.get('policy_version'),
            'decision_snapshot_id': item.get('decision_snapshot_id'),
            'decision_run_id': item.get('decision_run_id'),
            'source_feedback_id': item.get('source_feedback_id'),
            'raw_action': item.get('raw_action'),
            'tactical_action': item.get('tactical_action'),
            'generic_action': item.get('generic_action'),
            'handoff_intent': item.get('handoff_intent'),
            'handoff_active': item.get('handoff_active'),
            'exec_monitoring_active': item.get('exec_monitoring_active'),
            'exec_agent_approved': item.get('exec_agent_approved'),
            'approval_state': item.get('approval_state'),
            'exec_watch_state_sync': item.get('exec_watch_state_sync'),
            'core_state_divergence_warning': item.get('core_state_divergence_warning'),
            'eligibility_status': item.get('eligibility_status'),
            'execution_status': item.get('execution_status'),
            'target_price': item.get('target_price'),
            'stop_loss_price': item.get('stop_loss_price'),
            'trigger_price': item.get('trigger_price'),
            'chase_limit_price': item.get('chase_limit_price'),
            'expected_holding_period': item.get('expected_holding_period'),
            'entry_execution_mode': item.get('entry_execution_mode'),
            'blocker_reason': item.get('blocker_reason'),
            'data_quality_warnings': item.get('data_quality_warnings') or [],
            'client_order_id': item.get('client_order_id'),
            'dispatch': item.get('dispatch'),
            'remote_state': item.get('remote_state'),
            'traceability': item.get('traceability'),
            'validation': item.get('validation'),
        }

    def _build_dashboard_runtime_status(
        self,
        *,
        integration: dict[str, Any],
        runtime: dict[str, Any],
    ) -> dict[str, Any]:
        payload = runtime.get('payload') if isinstance(runtime.get('payload'), dict) else {}
        safety = payload.get('safety') if isinstance(payload.get('safety'), dict) else {}
        counts = payload.get('counts') if isinstance(payload.get('counts'), dict) else {}
        readiness = payload.get('readiness') if isinstance(payload.get('readiness'), dict) else {}
        monitoring = payload.get('monitoring') if isinstance(payload.get('monitoring'), dict) else {}
        liveness = payload.get('liveness') if isinstance(payload.get('liveness'), dict) else {}
        liveness_monitoring = (
            liveness.get('monitoring') if isinstance(liveness.get('monitoring'), dict) else {}
        )
        autonomous = liveness.get('autonomous') if isinstance(liveness.get('autonomous'), dict) else {}
        autonomous_monitoring = (
            autonomous.get('monitoring') if isinstance(autonomous.get('monitoring'), dict) else {}
        )
        (
            active_monitoring_items,
            active_monitoring_facts_complete,
        ) = self._extract_active_exec_monitoring_facts(autonomous_monitoring)
        symbols = payload.get('symbols') if isinstance(payload.get('symbols'), dict) else None
        daily_pnl = payload.get('daily_pnl') if isinstance(payload.get('daily_pnl'), dict) else None
        weekly_pnl = payload.get('weekly_pnl') if isinstance(payload.get('weekly_pnl'), dict) else None
        monthly_pnl = payload.get('monthly_pnl') if isinstance(payload.get('monthly_pnl'), dict) else None
        actionable_alerts = payload.get('actionable_alerts') if isinstance(payload.get('actionable_alerts'), dict) else None
        pnl_coverage = self._build_runtime_pnl_coverage(runtime=runtime, payload=payload)

        paused = _normalize_bool(
            _coalesce_text(
                safety.get('paused'),
                payload.get('paused'),
                _extract_field(payload, 'runtime', 'paused'),
            )
        )
        paused = bool(paused) if paused is not None else False

        kill_switch_enabled = _normalize_bool(
            _coalesce_text(
                safety.get('kill_switch_enabled'),
                payload.get('kill_switch_enabled'),
                payload.get('kill_switched'),
                _extract_field(payload, 'runtime', 'kill_switch_enabled'),
            )
        )
        kill_switch_enabled = bool(kill_switch_enabled) if kill_switch_enabled is not None else False

        automation_enabled = _normalize_bool(
            _coalesce_text(
                safety.get('automation_enabled'),
                payload.get('automation_enabled'),
                _extract_field(payload, 'runtime', 'automation_enabled'),
            )
        )
        if automation_enabled is None:
            automation_enabled = not paused and not kill_switch_enabled and bool(integration.get('enabled'))

        ready = _normalize_bool(payload.get('ready'))
        if ready is None:
            ready = runtime.get('status') == REMOTE_FETCH_STATUS_OK and not kill_switch_enabled and not paused

        monitoring_alive = _normalize_bool(
            _coalesce_text(
                monitoring.get('alive'),
                monitoring.get('running'),
                monitoring.get('ready'),
                liveness_monitoring.get('alive'),
                liveness_monitoring.get('running'),
                autonomous_monitoring.get('alive'),
                autonomous_monitoring.get('running'),
                payload.get('monitoring_alive'),
                _extract_field(payload, 'health', 'monitoring_alive'),
            )
        )
        monitoring_status = _coalesce_text(
            autonomous_monitoring.get('status'),
            monitoring.get('status'),
            liveness_monitoring.get('status'),
            lowercase=True,
        )
        last_heartbeat_at = _coalesce_text(
            monitoring.get('last_heartbeat_at'),
            monitoring.get('last_seen_at'),
            autonomous_monitoring.get('last_heartbeat_at'),
            autonomous_monitoring.get('last_seen_at'),
            liveness_monitoring.get('last_heartbeat_at'),
            liveness_monitoring.get('last_seen_at'),
            payload.get('last_heartbeat_at'),
            payload.get('monitoring_last_heartbeat_at'),
            _extract_field(payload, 'health', 'last_heartbeat_at'),
        )
        if monitoring_alive is None:
            if monitoring_status == 'running':
                monitoring_alive = _parse_iso_datetime(last_heartbeat_at) is not None
            elif monitoring_status in {'disabled', 'idle', 'not_started', 'error', 'degraded'}:
                monitoring_alive = False
            else:
                monitoring_alive = bool(ready and runtime.get('status') == REMOTE_FETCH_STATUS_OK)

        open_order_count = self._coerce_integer(
            counts.get('open_orders'),
            payload.get('open_order_count'),
            payload.get('open_orders'),
        )
        active_execution_job_count = self._coerce_integer(
            counts.get('active_execution_jobs'),
            payload.get('active_execution_job_count'),
            payload.get('active_jobs'),
        )

        issues = list(integration.get('issues') or [])
        runtime_error = runtime.get('error')
        if isinstance(runtime_error, dict):
            issues.append(runtime_error)
        for reason in readiness.get('reasons') or []:
            if isinstance(reason, dict):
                issues.append(
                    _serialize_issue(
                        _normalize_optional_text(reason.get('code'), lowercase=True) or 'readiness_reason',
                        _normalize_optional_text(reason.get('message')) or json.dumps(reason, sort_keys=True),
                    )
                )
                continue
            reason_text = _normalize_optional_text(reason)
            if reason_text:
                issues.append(_serialize_issue('readiness_reason', reason_text))

        runtime_status = _normalize_optional_text(runtime.get('status'), lowercase=True)
        exec_connected = runtime_status in {REMOTE_FETCH_STATUS_OK, UI_RUNTIME_STATUS_READY}

        if kill_switch_enabled:
            status = UI_RUNTIME_STATUS_KILL_SWITCHED
        elif paused:
            status = UI_RUNTIME_STATUS_PAUSED
        elif runtime_status in {REMOTE_FETCH_STATUS_DISABLED, REMOTE_FETCH_STATUS_NOT_CONFIGURED, REMOTE_FETCH_STATUS_UNAVAILABLE}:
            status = UI_RUNTIME_STATUS_OFFLINE
        elif runtime_status == REMOTE_FETCH_STATUS_CONTRACT_ERROR:
            status = UI_RUNTIME_STATUS_DEGRADED
        elif exec_connected and ready and monitoring_alive and not issues:
            status = UI_RUNTIME_STATUS_READY
        elif exec_connected:
            status = UI_RUNTIME_STATUS_DEGRADED
        else:
            status = UI_RUNTIME_STATUS_OFFLINE

        return serialize_execution_runtime_status_payload({
            'status': status,
            'mode': _coalesce_text(payload.get('mode'), integration.get('runtime_mode')) or 'mock',
            'exec_connected': exec_connected,
            'automation_enabled': bool(automation_enabled),
            'paused': paused,
            'kill_switch_enabled': kill_switch_enabled,
            'monitoring_alive': bool(monitoring_alive),
            'last_heartbeat_at': last_heartbeat_at,
            'open_order_count': open_order_count,
            'active_execution_job_count': active_execution_job_count,
            'symbols': symbols,
            'daily_pnl': daily_pnl,
            'weekly_pnl': weekly_pnl,
            'monthly_pnl': monthly_pnl,
            'pnl_coverage': pnl_coverage,
            'actionable_alerts': actionable_alerts,
            'active_monitoring_items': active_monitoring_items,
            'active_monitoring_facts_complete': (
                active_monitoring_facts_complete
            ),
            'issues': issues,
            'core_integration_mode': integration.get('mode'),
            'delegate_execution': bool(integration.get('delegate_execution')),
        })

    @staticmethod
    def _extract_active_exec_monitoring_facts(
        autonomous_monitoring: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], bool]:
        last_cycle = autonomous_monitoring.get('last_cycle')
        if not isinstance(last_cycle, dict):
            return [], False
        items = last_cycle.get('items')
        if not isinstance(items, list):
            return [], False

        observed_at = _parse_iso_datetime(last_cycle.get('triggered_at'))
        if observed_at is None:
            observed_at = _parse_iso_datetime(
                autonomous_monitoring.get('last_success_at')
            )
        observed_at_text = observed_at.isoformat() if observed_at else None
        facts: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            symbol = _normalize_optional_text(item.get('symbol'))
            intent = _normalize_optional_text(item.get('intent'), lowercase=True)
            status = _normalize_optional_text(item.get('status'), lowercase=True)
            outcome = _normalize_optional_text(item.get('outcome'), lowercase=True)
            is_active = bool(status and 'monitoring' in status) or outcome in {
                'active',
                'deferred',
                'monitoring',
                'pending',
                'waiting',
            }
            if not symbol or not intent or not is_active:
                continue
            facts.append(
                {
                    'symbol': symbol,
                    'intent': intent,
                    'status': status,
                    'trigger_price': _decimal_to_text(
                        _safe_decimal(item.get('trigger_price'))
                    ),
                    'observed_price': _decimal_to_text(
                        _safe_decimal(item.get('observed_price'))
                    ),
                    'outcome': outcome,
                    'observed_at': observed_at_text,
                }
            )
        facts_complete = bool(
            observed_at is not None
            and last_cycle.get('items_truncated') is not True
        )
        return facts, facts_complete

    @staticmethod
    def _build_runtime_pnl_coverage(
        *,
        runtime: dict[str, Any],
        payload: dict[str, Any],
    ) -> dict[str, dict[str, str]]:
        runtime_status = _normalize_optional_text(runtime.get('status'), lowercase=True)
        if not payload:
            if runtime_status in {REMOTE_FETCH_STATUS_DISABLED, REMOTE_FETCH_STATUS_NOT_CONFIGURED}:
                status_reason = 'runtime_not_configured'
            elif runtime_status == REMOTE_FETCH_STATUS_OK:
                status_reason = 'runtime_payload_not_loaded'
            else:
                status_reason = 'runtime_unavailable'
            return {
                period: {
                    'status': 'unavailable',
                    'status_reason': status_reason,
                    'source': 'exec_runtime_status',
                }
                for period in ('daily', 'weekly', 'monthly')
            }

        coverage: dict[str, dict[str, str]] = {}
        for period, field_name, value_field in (
            ('daily', 'daily_pnl', 'daily_realized_pnl'),
            ('weekly', 'weekly_pnl', 'weekly_realized_pnl'),
            ('monthly', 'monthly_pnl', 'monthly_realized_pnl'),
        ):
            value = payload.get(field_name)
            if field_name not in payload:
                status = 'unavailable'
                status_reason = 'not_provided_by_exec'
            elif not isinstance(value, dict):
                status = 'unavailable'
                status_reason = 'invalid_exec_payload'
            elif value.get(value_field) is None:
                status = 'unavailable'
                status_reason = (
                    _normalize_optional_text(value.get('status_reason'), lowercase=True)
                    or 'not_computable_by_exec'
                )
            else:
                status = 'available'
                status_reason = 'provided_by_exec'
            coverage[period] = {
                'status': status,
                'status_reason': status_reason,
                'source': 'exec_runtime_status',
            }
        return coverage

    @classmethod
    def _build_runtime_daily_board_activity_coverage(
        cls,
        *,
        items: Sequence[dict[str, Any]],
        runtime: dict[str, Any],
        runtime_activity_applicable: bool = True,
    ) -> dict[str, Any]:
        exec_activity_detected = (
            cls._exec_runtime_activity_detected(runtime=runtime)
            if runtime_activity_applicable
            else None
        )
        if items:
            status = 'available'
            status_reason = 'core_read_model_items_available'
        elif exec_activity_detected is True:
            status = 'unavailable'
            status_reason = 'exec_activity_not_materialized'
        elif exec_activity_detected is False:
            status = 'no_activity'
            status_reason = 'no_exec_activity'
        else:
            status = 'unavailable'
            status_reason = 'runtime_read_model_unavailable'
        return {
            'status': status,
            'status_reason': status_reason,
            'exec_activity_detected': exec_activity_detected,
            'source': 'exec_runtime_status',
        }

    @staticmethod
    def _exec_runtime_activity_detected(*, runtime: dict[str, Any]) -> bool | None:
        payload = runtime.get('payload') if isinstance(runtime.get('payload'), dict) else None
        if payload is None:
            return None

        explicit_inactivity_detected = False
        invalid_activity_summary_detected = False

        counts_value = payload.get('counts')
        counts = counts_value if isinstance(counts_value, dict) else {}
        if 'counts' in payload and not isinstance(counts_value, dict):
            invalid_activity_summary_detected = True
        for field_name in ('open_orders', 'active_execution_jobs'):
            if field_name not in counts:
                continue
            try:
                count = int(counts[field_name])
                if count > 0:
                    return True
            except (TypeError, ValueError):
                invalid_activity_summary_detected = True
                continue
            explicit_inactivity_detected = True

        symbols_value = payload.get('symbols')
        symbols = symbols_value if isinstance(symbols_value, dict) else {}
        if 'symbols' in payload and not isinstance(symbols_value, dict):
            invalid_activity_summary_detected = True
        for field_name in ('monitored', 'scheduled_today', 'filled_today', 'failed_today', 'positions'):
            if field_name not in symbols:
                continue
            value = symbols.get(field_name)
            if not isinstance(value, (list, tuple, set)):
                invalid_activity_summary_detected = True
                continue
            if value:
                return True
            explicit_inactivity_detected = True

        filled_counts_value = symbols.get('filled_today_counts')
        filled_counts = filled_counts_value if isinstance(filled_counts_value, dict) else {}
        if 'filled_today_counts' in symbols and not isinstance(filled_counts_value, dict):
            invalid_activity_summary_detected = True
        for value in filled_counts.values():
            if not isinstance(value, dict):
                invalid_activity_summary_detected = True
                continue
            for field_name in ('symbols', 'fills', 'quantity'):
                if field_name not in value:
                    continue
                try:
                    count = float(value[field_name])
                    if count > 0:
                        return True
                except (TypeError, ValueError):
                    invalid_activity_summary_detected = True
                    continue
                explicit_inactivity_detected = True

        autonomous_monitoring = _extract_field(payload, 'liveness', 'autonomous', 'monitoring')
        if isinstance(autonomous_monitoring, dict):
            last_cycle_value = autonomous_monitoring.get('last_cycle')
            last_cycle = last_cycle_value if isinstance(last_cycle_value, dict) else {}
            if 'last_cycle' in autonomous_monitoring and not isinstance(last_cycle_value, dict):
                invalid_activity_summary_detected = True
            for field_name in ('monitored_symbols', 'items'):
                if field_name not in last_cycle:
                    continue
                value = last_cycle[field_name]
                if not isinstance(value, (list, tuple, set)):
                    invalid_activity_summary_detected = True
                    continue
                if value:
                    return True
                explicit_inactivity_detected = True

        if invalid_activity_summary_detected:
            return None
        if explicit_inactivity_detected:
            return False
        return None

    @classmethod
    def _iter_exec_agent_payloads(cls, item: dict[str, Any]) -> Iterable[dict[str, Any]]:
        dispatch = item.get('dispatch') if isinstance(item.get('dispatch'), dict) else {}
        links = [link for link in dispatch.get('item_links') or [] if isinstance(link, dict)]
        entry_links = [
            link
            for link in links
            if _normalize_optional_text(link.get('intent'), lowercase=True) in EXEC_ENTRY_BUY_INTENTS
        ]
        for link in entry_links or links:
            response_item = link.get('response_item') if isinstance(link.get('response_item'), dict) else {}
            if response_item:
                yield response_item
                yield from cls._iter_nested_exec_agent_payloads(response_item)

    @classmethod
    def _iter_nested_exec_agent_payloads(cls, payload: dict[str, Any], *, depth: int = 0) -> Iterable[dict[str, Any]]:
        if depth >= 3:
            return
        for key in (
            'agent_sizing',
            'sizing',
            'sizing_decision',
            'quantity_decision',
            'approval',
            'decision',
            'audit',
            'exec_agent_audit',
            'order',
            'materialized_order',
        ):
            nested = payload.get(key)
            if isinstance(nested, dict):
                yield nested
                yield from cls._iter_nested_exec_agent_payloads(nested, depth=depth + 1)

    @staticmethod
    def _extract_agent_approval_status(payload: dict[str, Any]) -> bool | None:
        for key in ('approved', 'is_approved', 'exec_agent_approved'):
            parsed = _normalize_bool(payload.get(key))
            if parsed is not None:
                return parsed
        for key in (
            'exec_agent_action',
            'agent_action',
            'approval_action',
            'decision_action',
            'risk_decision',
            'approval_status',
            'decision_status',
            'status',
            'action',
        ):
            normalized = _normalize_status(payload.get(key))
            if normalized in EXEC_AGENT_APPROVE_ACTIONS:
                return True
            if normalized in EXEC_AGENT_REJECT_ACTIONS:
                return False
        return None

    @staticmethod
    def _first_agent_text(payloads: Sequence[dict[str, Any]], keys: Sequence[str]) -> str | None:
        for payload in payloads:
            for key in keys:
                value = _normalize_optional_text(payload.get(key))
                if value:
                    return value
        return None

    @staticmethod
    def _first_agent_bool(payloads: Sequence[dict[str, Any]], keys: Sequence[str]) -> bool | None:
        for payload in payloads:
            for key in keys:
                parsed = _normalize_bool(payload.get(key))
                if parsed is not None:
                    return parsed
        return None

    @staticmethod
    def _entry_link_statuses(link: dict[str, Any]) -> set[str]:
        statuses: set[str] = set()

        def add(value: Any) -> None:
            normalized = _normalize_status(value)
            if normalized:
                statuses.add(normalized)

        for key in ('remote_status', 'status', 'plan_item_status', 'decision_status', 'monitoring_status'):
            add(link.get(key))
        response_item = link.get('response_item') if isinstance(link.get('response_item'), dict) else {}
        for key in (
            'status',
            'plan_item_status',
            'decision_status',
            'monitoring_status',
            'monitoring_state',
            'watch_status',
        ):
            add(response_item.get(key))
        for key in ('monitoring_statuses', 'monitoringStatuses', 'statuses'):
            values = response_item.get(key)
            if isinstance(values, list):
                for value in values:
                    add(value)
        monitoring = response_item.get('monitoring')
        if isinstance(monitoring, dict):
            for key in ('status', 'state', 'monitoring_status', 'monitoring_state', 'watch_status'):
                add(monitoring.get(key))
        return statuses

    @classmethod
    def _has_exec_agent_accepted_entry(cls, item: dict[str, Any]) -> bool:
        dispatch = item.get('dispatch') if isinstance(item.get('dispatch'), dict) else {}
        for link in dispatch.get('item_links') or []:
            if not isinstance(link, dict):
                continue
            if _normalize_optional_text(link.get('intent'), lowercase=True) not in EXEC_ENTRY_BUY_INTENTS:
                continue
            statuses = cls._entry_link_statuses(link)
            if statuses & EXEC_MONITORING_TERMINAL_STATUSES:
                continue
            if statuses & EXEC_WATCH_READBACK_ACTIVE_STATUSES:
                return True
        return False

    @classmethod
    def _extract_exec_agent_decision(cls, item: dict[str, Any]) -> dict[str, Any]:
        payloads = list(cls._iter_exec_agent_payloads(item))
        approval_values = [
            parsed
            for payload in payloads
            if (parsed := cls._extract_agent_approval_status(payload)) is not None
        ]
        approved_quantity: Decimal | None = None
        approved_notional: Decimal | None = None
        for payload in payloads:
            if approved_quantity is None:
                approved_quantity = _extract_agent_approved_quantity(payload)
            if approved_notional is None:
                for key in (
                    'approved_notional',
                    'approved_notional_krw',
                    'notional',
                    'notional_krw',
                    'order_notional',
                    'order_notional_krw',
                ):
                    approved_notional = _positive_decimal(payload.get(key))
                    if approved_notional is not None:
                        break

        approved = any(value is True for value in approval_values)
        rejected = any(value is False for value in approval_values)
        if not approved and approved_quantity is not None and not rejected:
            approved = True
        if not approved and not rejected and cls._has_exec_agent_accepted_entry(item):
            approved = True

        action = cls._first_agent_text(
            payloads,
            (
                'exec_agent_action',
                'agent_action',
                'approval_action',
                'decision_action',
                'risk_decision',
                'action',
            ),
        )
        normalized_action = _normalize_status(action)
        if not action:
            if approved:
                action = 'approve'
            elif rejected:
                action = 'reject'
        elif normalized_action in EXEC_AGENT_APPROVE_ACTIONS:
            action = 'approve'
        elif normalized_action in EXEC_AGENT_REJECT_ACTIONS:
            action = 'reject'

        return {
            'approved': approved,
            'rejected': rejected and not approved,
            'action': action,
            'reason_code': cls._first_agent_text(
                payloads,
                (
                    'reason_code',
                    'reasonCode',
                    'approval_reason_code',
                    'rejection_reason_code',
                    'reason',
                    'rejection_reason',
                    'message',
                ),
            ),
            'target_profile': cls._first_agent_text(payloads, ('target_profile', 'targetProfile')),
            'target_tag': cls._first_agent_text(payloads, ('target_tag', 'targetTag')),
            'special_tactical_scalp': cls._first_agent_bool(
                payloads,
                ('special_tactical_scalp', 'specialTacticalScalp'),
            ),
            'approved_quantity': approved_quantity,
            'approved_notional': approved_notional,
        }

    @classmethod
    def _collect_runtime_monitoring_statuses(
        cls,
        *,
        item: dict[str, Any],
        status: str,
        monitoring_state: str,
        intent: str,
        planned_intents: Sequence[str],
        held_position: bool,
        exec_agent_approved: bool,
    ) -> list[str]:
        statuses: set[str] = set()

        def add_status(value: Any) -> None:
            normalized = _normalize_status(value)
            if normalized:
                statuses.add(normalized)

        def collect_from_payload(payload: dict[str, Any]) -> None:
            for key in (
                'monitoring_status',
                'monitoring_state',
                'watch_status',
                'plan_item_status',
                'decision_status',
                'status',
                'remote_status',
            ):
                add_status(payload.get(key))
            for key in ('monitoring_statuses', 'monitoringStatuses', 'statuses'):
                values = payload.get(key)
                if isinstance(values, list):
                    for value in values:
                        add_status(value)
            monitoring = payload.get('monitoring')
            if isinstance(monitoring, dict):
                collect_from_payload(monitoring)

        dispatch = item.get('dispatch') if isinstance(item.get('dispatch'), dict) else {}
        for link in dispatch.get('item_links') or []:
            if not isinstance(link, dict):
                continue
            collect_from_payload(link)
            response_item = link.get('response_item')
            if isinstance(response_item, dict):
                collect_from_payload(response_item)
                for nested in cls._iter_nested_exec_agent_payloads(response_item):
                    collect_from_payload(nested)

        if held_position:
            if EXEC_INTENT_TARGET_SELL in planned_intents or _positive_decimal(item.get('target_price')) is not None:
                statuses.add('target_sell_monitoring')
            if EXEC_INTENT_STOP_SELL in planned_intents or _positive_decimal(item.get('stop_loss_price')) is not None:
                statuses.add('stop_sell_monitoring')

        active_entry_status = status in {
            UI_BOARD_STATUS_ACCEPTED,
            UI_BOARD_STATUS_SCHEDULED,
            UI_BOARD_STATUS_SUBMITTED,
            UI_BOARD_STATUS_PARTIAL,
        }
        if exec_agent_approved and intent in EXEC_ENTRY_BUY_INTENTS:
            if monitoring_state == UI_MONITORING_STATE_WATCHING_FILL or active_entry_status:
                statuses.add('watch_monitoring_active')
            elif status == UI_BOARD_STATUS_PLANNED:
                statuses.add('watch_monitoring_pending')

        return sorted(statuses)

    @staticmethod
    def _has_active_exec_monitoring_status(statuses: Sequence[str]) -> bool:
        for status in statuses:
            normalized = _normalize_status(status)
            if not normalized or normalized in EXEC_MONITORING_TERMINAL_STATUSES:
                continue
            if normalized in EXEC_MONITORING_ACTIVE_STATUSES:
                return True
            if normalized.endswith('_monitoring') or normalized.startswith('watch_monitoring_'):
                return True
        return False

    @staticmethod
    def _has_terminal_exec_monitoring_status(*, status: str, statuses: Sequence[str]) -> bool:
        if _normalize_status(status) in {UI_BOARD_STATUS_CANCELLED, UI_BOARD_STATUS_FAILED}:
            return True
        return any(_normalize_status(value) in EXEC_MONITORING_TERMINAL_STATUSES for value in statuses)

    @classmethod
    def _build_runtime_exec_contract(
        cls,
        *,
        item: dict[str, Any],
        status: str,
        monitoring_state: str,
        intent: str,
        planned_intents: Sequence[str],
        position_quantity: Decimal,
        position_average_price: Decimal | None,
    ) -> dict[str, Any]:
        held_position = position_quantity > Decimal('0')
        agent_decision = cls._extract_exec_agent_decision(item)
        exec_agent_approved = bool(agent_decision['approved'])
        monitoring_statuses = cls._collect_runtime_monitoring_statuses(
            item=item,
            status=status,
            monitoring_state=monitoring_state,
            intent=intent,
            planned_intents=planned_intents,
            held_position=held_position,
            exec_agent_approved=exec_agent_approved,
        )
        terminal = cls._has_terminal_exec_monitoring_status(status=status, statuses=monitoring_statuses)
        active_monitoring = cls._has_active_exec_monitoring_status(monitoring_statuses)
        exec_monitoring_active = held_position or (exec_agent_approved and active_monitoring and not terminal)
        has_execution_plan = bool(_normalize_optional_text((item.get('dispatch') or {}).get('plan_id')))

        if held_position:
            runtime_state = 'held_position'
            exclusion_reason = None
        elif exec_agent_approved and active_monitoring and not terminal:
            runtime_state = 'exec_approved_watch'
            exclusion_reason = None
        elif agent_decision['rejected']:
            runtime_state = 'exec_agent_rejected'
            exclusion_reason = 'exec_agent_rejected'
        elif terminal:
            runtime_state = 'terminal_no_position'
            exclusion_reason = 'terminal_no_position'
        elif exec_agent_approved:
            runtime_state = 'exec_approved_inactive'
            exclusion_reason = 'exec_approved_without_active_monitoring'
        elif has_execution_plan or planned_intents or status == UI_BOARD_STATUS_PLANNED:
            runtime_state = 'core_planned_only'
            exclusion_reason = 'core_planned_not_exec_approved'
        else:
            runtime_state = 'stale_no_active_monitoring'
            exclusion_reason = 'stale_no_active_monitoring'

        return serialize_execution_runtime_exec_card_payload({
            'exec_monitoring_active': exec_monitoring_active,
            'exec_runtime_showable': exec_monitoring_active,
            'exec_runtime_state': runtime_state,
            'exec_runtime_exclusion_reason': exclusion_reason,
            'exec_agent_approved': exec_agent_approved,
            'exec_agent_action': agent_decision['action'],
            'exec_agent_reason_code': agent_decision['reason_code'],
            'exec_agent_target_profile': agent_decision['target_profile'],
            'exec_agent_target_tag': agent_decision['target_tag'],
            'exec_agent_special_tactical_scalp': agent_decision['special_tactical_scalp'],
            'held_position': held_position,
            'position_quantity': _to_json_number(position_quantity),
            'position_average_price': _to_json_number(position_average_price),
            'approved_quantity': _to_json_number(agent_decision['approved_quantity']),
            'approved_notional': _decimal_to_text(agent_decision['approved_notional']),
            'monitoring_statuses': monitoring_statuses,
        })

    def _build_runtime_board_item(
        self,
        *,
        item: dict[str, Any],
        market_date: date,
        symbol_metadata_map: dict[str, dict[str, Any]],
        runtime_status: str,
    ) -> dict[str, Any]:
        symbol = _normalize_optional_text(item.get('symbol')) or 'UNKNOWN'
        decision_agent_name = _normalize_decision_agent_name(item.get('decision_agent_name'))
        metadata = symbol_metadata_map.get(symbol, {'name': symbol, 'market': None})
        remote_state = item.get('remote_state') if isinstance(item.get('remote_state'), dict) else {}
        orders = remote_state.get('orders') if isinstance(remote_state.get('orders'), list) else []
        executions = remote_state.get('executions') if isinstance(remote_state.get('executions'), list) else []
        positions = remote_state.get('positions') if isinstance(remote_state.get('positions'), list) else []

        planned_quantity = _safe_decimal(item.get('planned_quantity'))
        entry_filled_quantity = _safe_decimal(remote_state.get('entry_filled_quantity'))
        entry_execution_quantity = self._sum_execution_quantity(executions=executions, side='buy')
        entry_order_quantity = self._sum_order_filled_quantity(orders=orders, side='buy')
        if entry_filled_quantity is None:
            entry_filled_quantity = entry_execution_quantity
        if entry_filled_quantity <= Decimal('0'):
            entry_filled_quantity = max(entry_filled_quantity, entry_execution_quantity, entry_order_quantity)
        exit_filled_quantity = _safe_decimal(remote_state.get('exit_filled_quantity'))
        exit_execution_quantity = self._sum_execution_quantity(executions=executions, side='sell')
        exit_order_quantity = self._sum_order_filled_quantity(orders=orders, side='sell')
        if exit_filled_quantity is None:
            exit_filled_quantity = exit_execution_quantity
        if exit_filled_quantity <= Decimal('0'):
            exit_filled_quantity = max(exit_filled_quantity, exit_execution_quantity, exit_order_quantity)

        order_statuses = self._collect_order_statuses(orders=orders)
        has_entry_fill_evidence = (
            entry_filled_quantity > Decimal('0')
            or self._has_order_fill_evidence(orders=orders, side='buy')
            or bool(order_statuses & REMOTE_PARTIAL_STATUSES)
        )
        has_exit_fill_evidence = (
            exit_filled_quantity > Decimal('0') or self._has_order_fill_evidence(orders=orders, side='sell')
        )
        readback_fill_evidence = _normalize_bool(remote_state.get('has_plan_attributed_fill_evidence'))
        has_plan_attributed_fill_evidence = (
            has_entry_fill_evidence or has_exit_fill_evidence or bool(readback_fill_evidence)
        )
        remote_open_quantity = _safe_decimal(remote_state.get('open_quantity'))
        position_quantity = self._sum_position_quantity(positions=positions)
        position_average_price = self._weighted_position_average_price(positions=positions)
        attributed_position_quantity = (
            max(remote_open_quantity or Decimal('0'), position_quantity)
            if has_plan_attributed_fill_evidence
            else Decimal('0')
        )
        open_quantity = max(
            attributed_position_quantity,
            entry_filled_quantity - exit_filled_quantity,
            Decimal('0'),
        )
        today_activity = self._build_today_activity(
            market_date=market_date,
            orders=orders,
            executions=executions,
            net_open_quantity=open_quantity,
        )
        status = self._normalize_board_item_status(
            item=item,
            order_statuses=order_statuses,
            planned_quantity=planned_quantity,
            entry_filled_quantity=entry_filled_quantity,
            exit_filled_quantity=exit_filled_quantity,
            open_quantity=open_quantity,
            has_plan_attributed_fill_evidence=has_plan_attributed_fill_evidence,
        )
        planned_intents = self._collect_planned_intents(item=item)
        monitoring_state = self._resolve_monitoring_state(
            status=status,
            open_quantity=open_quantity,
            planned_intents=planned_intents,
            runtime_status=runtime_status,
            has_execution_plan=bool(_normalize_optional_text((item.get('dispatch') or {}).get('plan_id'))),
            has_target_stop_watch=self._has_target_stop_watch_facts(item),
        )
        intent = self._resolve_entry_intent(item=item)
        contract_position_quantity = max(position_quantity, open_quantity)
        exec_contract = self._build_runtime_exec_contract(
            item=item,
            status=status,
            monitoring_state=monitoring_state,
            intent=intent,
            planned_intents=planned_intents,
            position_quantity=contract_position_quantity,
            position_average_price=position_average_price,
        )
        return {
            'plan_item_id': item.get('plan_item_id'),
            'account_reference': item.get('account_reference'),
            'account_label': item.get('account_label'),
            'symbol': symbol,
            'name': metadata['name'],
            'market': metadata['market'],
            'agent_name': decision_agent_name,
            'decision_agent_name': decision_agent_name,
            'intent': intent,
            'raw_action': item.get('raw_action'),
            'tactical_action': item.get('tactical_action'),
            'generic_action': item.get('generic_action'),
            'handoff_intent': item.get('handoff_intent'),
            'handoff_active': item.get('handoff_active'),
            'approval_state': item.get('approval_state'),
            'exec_watch_state_sync': item.get('exec_watch_state_sync'),
            'core_state_divergence_warning': item.get('core_state_divergence_warning'),
            'status': status,
            'monitoring_state': monitoring_state,
            **exec_contract,
            'position_side': 'long'
            if open_quantity > Decimal('0') or position_quantity > Decimal('0') or planned_quantity is not None
            else None,
            'planned_intents': planned_intents,
            'planned_quantity': _to_json_number(planned_quantity),
            'filled_quantity': _to_json_number(entry_filled_quantity),
            'exit_filled_quantity': _to_json_number(exit_filled_quantity),
            'open_quantity': _to_json_number(open_quantity),
            'planned_price': item.get('estimated_reference_price'),
            'avg_fill_price': self._resolve_average_fill_price(executions=executions, positions=positions),
            'today_activity': today_activity,
            'decision_source': decision_agent_name,
            'decision_snapshot_id': item.get('decision_snapshot_id'),
            'source_feedback_id': item.get('source_feedback_id'),
            'decision_run_id': item.get('decision_run_id'),
            'policy_version': item.get('policy_version'),
            'execution_plan_id': _normalize_optional_text((item.get('dispatch') or {}).get('plan_id')),
            'broker_order_id': self._resolve_broker_order_id(orders=orders, executions=executions),
            'last_updated_at': self._resolve_last_updated_at(
                item=item,
                orders=orders,
                executions=executions,
                positions=positions,
            ),
            'failure_reason': self._resolve_failure_reason(item=item, order_statuses=order_statuses, status=status),
            'target_price': item.get('target_price'),
            'stop_loss_price': item.get('stop_loss_price'),
            'trigger_price': item.get('trigger_price'),
            'chase_limit_price': item.get('chase_limit_price'),
            'expected_holding_period': item.get('expected_holding_period'),
            'entry_execution_mode': item.get('entry_execution_mode'),
            'target_trade_date': item.get('target_trade_date'),
            'source_market_date': item.get('source_market_date'),
            **self._price_fields_for_symbol(symbol, market_date=market_date),
        }

    def _build_runtime_daily_board_summary(self, *, items: Sequence[dict[str, Any]]) -> dict[str, Any]:
        counts = {
            UI_BOARD_STATUS_PLANNED: 0,
            UI_BOARD_STATUS_ACCEPTED: 0,
            UI_BOARD_STATUS_SCHEDULED: 0,
            UI_BOARD_STATUS_SUBMITTED: 0,
            UI_BOARD_STATUS_PARTIAL: 0,
            UI_BOARD_STATUS_FILLED: 0,
            UI_BOARD_STATUS_CANCELLED: 0,
            UI_BOARD_STATUS_FAILED: 0,
        }
        for item in items:
            status = _normalize_optional_text(item.get('status'), lowercase=True)
            if status in counts:
                counts[status] += 1
        return {
            'items_total': len(items),
            **counts,
        }

    def _build_runtime_monitored_symbol_item(
        self,
        *,
        symbol: str,
        items: Sequence[dict[str, Any]],
        runtime_status: str,
        market_date: date,
    ) -> dict[str, Any]:
        first = items[0]
        monitoring = self._build_runtime_symbol_monitoring(items=items, runtime_status=runtime_status)
        preferred_items = sorted(
            items,
            key=lambda item: (
                bool(item.get('held_position')),
                bool(item.get('exec_agent_approved')),
                bool(item.get('exec_runtime_showable')),
                _normalize_optional_text(item.get('last_updated_at')) or '',
            ),
            reverse=True,
        )

        def first_value(field_name: str) -> Any:
            for candidate in preferred_items:
                value = candidate.get(field_name)
                if value is not None and value != '':
                    return value
            return None

        monitoring_statuses = sorted(
            {
                status
                for item in items
                for status in (item.get('monitoring_statuses') or [])
                if _normalize_optional_text(status)
            }
        )
        position_by_account: dict[str, tuple[Decimal, Decimal | None]] = {}
        for index, item in enumerate(items):
            quantity = _safe_decimal(item.get('position_quantity')) or Decimal('0')
            if quantity <= Decimal('0'):
                continue
            account_key = (
                _normalize_optional_text(item.get('account_reference'))
                or _normalize_optional_text(item.get('plan_item_id'))
                or str(index)
            )
            existing_quantity = position_by_account.get(account_key, (Decimal('0'), None))[0]
            if quantity >= existing_quantity:
                position_by_account[account_key] = (quantity, _safe_decimal(item.get('position_average_price')))
        position_quantity = sum((quantity for quantity, _ in position_by_account.values()), start=Decimal('0'))
        approved_quantity = sum(
            ((_safe_decimal(item.get('approved_quantity')) or Decimal('0')) for item in items),
            start=Decimal('0'),
        )
        approved_notional = sum(
            ((_safe_decimal(item.get('approved_notional')) or Decimal('0')) for item in items),
            start=Decimal('0'),
        )
        position_average_price = self._weighted_average_from_quantity_price_pairs(position_by_account.values())
        exclusion_reasons = sorted(
            {
                reason
                for item in items
                if (reason := _normalize_optional_text(item.get('exec_runtime_exclusion_reason'), lowercase=True))
            }
        )
        divergence_warnings = [
            warning
            for item in items
            if isinstance((warning := item.get('core_state_divergence_warning')), dict)
        ]
        exec_runtime_showable = any(bool(item.get('exec_runtime_showable')) for item in items)
        exec_monitoring_active = any(bool(item.get('exec_monitoring_active')) for item in items)
        held_position = bool(
            position_quantity > Decimal('0')
            or any(bool(item.get('held_position')) for item in items)
        )
        return {
            'symbol': symbol,
            'name': first.get('name') or symbol,
            'market': first.get('market'),
            **self._price_fields_for_symbol(symbol, market_date=market_date),
            'monitoring_state': monitoring['state'],
            'exec_monitoring_active': exec_monitoring_active,
            'exec_runtime_showable': exec_runtime_showable,
            'exec_runtime_state': first_value('exec_runtime_state') or 'stale_no_active_monitoring',
            'exec_runtime_exclusion_reason': None
            if exec_runtime_showable
            else (exclusion_reasons[0] if exclusion_reasons else 'stale_no_active_monitoring'),
            'exec_runtime_exclusion_reasons': [] if exec_runtime_showable else exclusion_reasons,
            'core_state_divergence_count': len(divergence_warnings),
            'core_state_divergence_warnings': divergence_warnings,
            'exec_agent_approved': any(bool(item.get('exec_agent_approved')) for item in items),
            'exec_agent_action': first_value('exec_agent_action'),
            'exec_agent_reason_code': first_value('exec_agent_reason_code'),
            'exec_agent_target_profile': first_value('exec_agent_target_profile'),
            'exec_agent_target_tag': first_value('exec_agent_target_tag'),
            'exec_agent_special_tactical_scalp': first_value('exec_agent_special_tactical_scalp'),
            'held_position': held_position,
            'position_quantity': (
                _to_json_number(position_quantity)
                if position_quantity > Decimal('0')
                else None
                if held_position
                else 0
            ),
            'position_average_price': _to_json_number(position_average_price),
            'approved_quantity': _to_json_number(approved_quantity if approved_quantity > Decimal('0') else None),
            'approved_notional': _decimal_to_text(approved_notional if approved_notional > Decimal('0') else None),
            'monitoring_statuses': monitoring_statuses,
            'position_side': monitoring['position_side'],
            'planned_intents': monitoring['planned_intents'],
            'tactical_actions': sorted(
                {
                    str(item.get('tactical_action'))
                    for item in items
                    if _normalize_optional_text(item.get('tactical_action'))
                }
            ),
            'handoff_intents': sorted(
                {
                    str(item.get('handoff_intent'))
                    for item in items
                    if _normalize_optional_text(item.get('handoff_intent'))
                }
            ),
            'target_price': first_value('target_price'),
            'stop_loss_price': first_value('stop_loss_price'),
            'trigger_price': first_value('trigger_price'),
            'chase_limit_price': first_value('chase_limit_price'),
            'expected_holding_period': first_value('expected_holding_period'),
            'decision_agent_names': sorted(
                {
                    _normalize_decision_agent_name(item.get('decision_agent_name'))
                    for item in items
                    if _normalize_optional_text(item.get('decision_agent_name'))
                }
            ),
            'last_event_at': monitoring['last_event_at'],
            'account_references': sorted(
                {
                    str(item.get('account_reference'))
                    for item in items
                    if _normalize_optional_text(item.get('account_reference'))
                }
            ),
            'status_counts': dict(
                sorted(
                    Counter(
                        _normalize_optional_text(item.get('status'), lowercase=True) or 'unknown'
                        for item in items
                    ).items()
                )
            ),
        }

    def _should_include_monitored_symbol_group(self, items: Sequence[dict[str, Any]]) -> bool:
        for item in items:
            if item.get('exec_runtime_showable') or item.get('held_position'):
                return True
            if _normalize_optional_text(item.get('monitoring_state'), lowercase=True) != UI_MONITORING_STATE_IDLE:
                return True
            if _normalize_optional_text(item.get('status'), lowercase=True) in {
                UI_BOARD_STATUS_ACCEPTED,
                UI_BOARD_STATUS_SCHEDULED,
                UI_BOARD_STATUS_SUBMITTED,
                UI_BOARD_STATUS_PARTIAL,
                UI_BOARD_STATUS_FILLED,
            }:
                return True
            if (_safe_decimal(item.get('filled_quantity')) or Decimal('0')) > Decimal('0'):
                return True
            if (_safe_decimal(item.get('exit_filled_quantity')) or Decimal('0')) > Decimal('0'):
                return True
            if (_safe_decimal(item.get('open_quantity')) or Decimal('0')) > Decimal('0'):
                return True
            if _normalize_optional_text(item.get('broker_order_id')):
                return True
            if item.get('planned_intents'):
                return True
            if self._has_target_stop_watch_facts(item):
                return True
        return False

    @classmethod
    def _prioritized_runtime_symbols_for_realtime(
        cls,
        grouped: dict[str, Sequence[dict[str, Any]]],
    ) -> list[str]:
        ranked = [
            (
                -cls._runtime_symbol_realtime_priority(items),
                symbol,
            )
            for symbol, items in grouped.items()
        ]
        return [
            symbol
            for _, symbol in sorted(ranked)
        ]

    @classmethod
    def _runtime_symbol_realtime_priority(cls, items: Sequence[dict[str, Any]]) -> int:
        priority = 0
        for item in items:
            status = _normalize_optional_text(item.get('status'), lowercase=True)
            monitoring_state = _normalize_optional_text(item.get('monitoring_state'), lowercase=True)
            intent = _normalize_optional_text(item.get('intent'), lowercase=True)
            open_quantity = _safe_decimal(item.get('open_quantity')) or Decimal('0')
            if item.get('held_position'):
                priority = max(priority, 500)
                continue
            if monitoring_state == UI_MONITORING_STATE_RECONCILING:
                priority = max(priority, 400)
                continue
            has_target_stop = bool(
                set(item.get('planned_intents') or []) & {EXEC_INTENT_TARGET_SELL, EXEC_INTENT_STOP_SELL}
            ) or any(
                _positive_decimal(item.get(field_name)) is not None
                for field_name in ('target_price', 'stop_loss_price')
            )
            if (
                monitoring_state == UI_MONITORING_STATE_WATCHING_TARGET_STOP
                and open_quantity > Decimal('0')
                and has_target_stop
            ):
                priority = max(priority, 400)
                continue
            if (
                intent == EXEC_INTENT_INTRADAY_WATCH_BUY
                and status in {
                    UI_BOARD_STATUS_PLANNED,
                    UI_BOARD_STATUS_ACCEPTED,
                    UI_BOARD_STATUS_SCHEDULED,
                    UI_BOARD_STATUS_SUBMITTED,
                    UI_BOARD_STATUS_PARTIAL,
                }
                and any(
                    _positive_decimal(item.get(field_name)) is not None
                    for field_name in ('trigger_price', 'chase_limit_price')
                )
            ):
                priority = max(priority, 300)
                continue
            if status in {
                UI_BOARD_STATUS_ACCEPTED,
                UI_BOARD_STATUS_SCHEDULED,
                UI_BOARD_STATUS_SUBMITTED,
                UI_BOARD_STATUS_PARTIAL,
            } or _normalize_optional_text(item.get('broker_order_id')):
                priority = max(priority, 200)
                continue
            if item.get('planned_intents') or cls._has_target_stop_watch_facts(item):
                priority = max(priority, 100)
        return priority

    @staticmethod
    def _price_fields_for_symbol(symbol: Any, *, market_date: date) -> dict[str, Any]:
        try:
            realtime_fields = kis_realtime_price_service.price_fields_for_symbol(symbol)
            return build_display_price_fields(
                symbol,
                market_date=market_date,
                realtime_fields=realtime_fields,
            )
        except Exception:
            return {
                'current_price': None,
                'price_as_of': None,
                'price_received_at': None,
                'price_source': None,
                'price_status': 'unavailable',
                'display_price_available': False,
                'realtime_subscribed': False,
                'realtime_subscription_reason': 'unavailable',
            }

    def _build_runtime_monitored_summary(self, *, items: Sequence[dict[str, Any]]) -> dict[str, Any]:
        state_counts = Counter(
            _normalize_optional_text(item.get('monitoring_state'), lowercase=True) or 'unknown'
            for item in items
        )
        price_status_counts = Counter(
            _normalize_optional_text(item.get('price_status'), lowercase=True) or 'unknown'
            for item in items
        )
        price_source_counts = Counter(
            _normalize_optional_text(item.get('price_source'), lowercase=True) or 'none'
            for item in items
        )
        fallback_covered_count = sum(
            1
            for item in items
            if item.get('display_price_available')
            and _normalize_optional_text(item.get('price_source'), lowercase=True) in DISPLAY_FALLBACK_SOURCES
        )
        return {
            'items_total': len(items),
            'watching_target_stop': state_counts.get(UI_MONITORING_STATE_WATCHING_TARGET_STOP, 0),
            'watching_fill': state_counts.get(UI_MONITORING_STATE_WATCHING_FILL, 0),
            'reconciling': state_counts.get(UI_MONITORING_STATE_RECONCILING, 0),
            'idle': state_counts.get(UI_MONITORING_STATE_IDLE, 0),
            'monitoring_state_counts': dict(sorted(state_counts.items())),
            'desired_symbol_count': len(items),
            'realtime_subscribed_count': sum(1 for item in items if item.get('realtime_subscribed')),
            'capacity_limited_count': sum(
                1
                for item in items
                if _normalize_optional_text(item.get('realtime_subscription_reason'), lowercase=True) == 'capacity_limited'
            ),
            'cached_fallback_covered_count': fallback_covered_count,
            'truly_unavailable_count': sum(1 for item in items if not item.get('display_price_available')),
            'display_price_available_count': sum(1 for item in items if item.get('display_price_available')),
            'price_status_counts': dict(sorted(price_status_counts.items())),
            'price_source_counts': dict(sorted(price_source_counts.items())),
            'core_state_divergence_count': sum(
                int(_safe_decimal(item.get('core_state_divergence_count')) or Decimal('0'))
                for item in items
            ),
        }

    def _build_runtime_symbol_detail_item(
        self,
        *,
        item: dict[str, Any],
        market_date: date,
        symbol_metadata_map: dict[str, dict[str, Any]],
        runtime_status: str,
    ) -> dict[str, Any]:
        board_item = self._build_runtime_board_item(
            item=item,
            market_date=market_date,
            symbol_metadata_map=symbol_metadata_map,
            runtime_status=runtime_status,
        )
        remote_state = item.get('remote_state') if isinstance(item.get('remote_state'), dict) else {}
        return {
            **board_item,
            'dispatch': item.get('dispatch'),
            'remote_state': {
                'derived_status': remote_state.get('derived_status'),
                'last_synced_at': remote_state.get('last_synced_at'),
                'endpoint_statuses': remote_state.get('endpoint_statuses') or {},
            },
            'orders': remote_state.get('orders') or [],
            'executions': remote_state.get('executions') or [],
            'positions': remote_state.get('positions') or [],
            'validation': item.get('validation'),
            'traceability': item.get('traceability'),
        }

    def _build_runtime_symbol_detail_summary(self, *, items: Sequence[dict[str, Any]]) -> dict[str, Any]:
        status_counts = Counter(
            _normalize_optional_text(item.get('status'), lowercase=True) or 'unknown'
            for item in items
        )
        planned_quantity_total = sum(
            ((_safe_decimal(item.get('planned_quantity')) or Decimal('0')) for item in items),
            start=Decimal('0'),
        )
        filled_quantity_total = sum(
            ((_safe_decimal(item.get('filled_quantity')) or Decimal('0')) for item in items),
            start=Decimal('0'),
        )
        open_quantity_total = sum(
            ((_safe_decimal(item.get('open_quantity')) or Decimal('0')) for item in items),
            start=Decimal('0'),
        )
        return {
            'items_total': len(items),
            'account_count': len(
                {
                    str(item.get('account_reference'))
                    for item in items
                    if _normalize_optional_text(item.get('account_reference'))
                }
            ),
            'decision_agent_counts': dict(
                sorted(
                    Counter(
                        _normalize_decision_agent_name(item.get('decision_agent_name'))
                        for item in items
                    ).items()
                )
            ),
            'status_counts': dict(sorted(status_counts.items())),
            'planned_quantity_total': _to_json_number(planned_quantity_total),
            'filled_quantity_total': _to_json_number(filled_quantity_total),
            'open_quantity_total': _to_json_number(open_quantity_total),
            'last_updated_at': _latest_iso_timestamp(item.get('last_updated_at') for item in items),
        }

    def _build_runtime_symbol_monitoring(self, *, items: Sequence[dict[str, Any]], runtime_status: str) -> dict[str, Any]:
        if not items:
            return {
                'state': UI_MONITORING_STATE_IDLE,
                'position_side': None,
                'planned_intents': [],
                'last_event_at': None,
            }
        states = [
            _normalize_optional_text(item.get('monitoring_state'), lowercase=True) or UI_MONITORING_STATE_IDLE
            for item in items
        ]
        priority = {
            UI_MONITORING_STATE_RECONCILING: 4,
            UI_MONITORING_STATE_WATCHING_TARGET_STOP: 3,
            UI_MONITORING_STATE_WATCHING_FILL: 2,
            UI_MONITORING_STATE_IDLE: 1,
        }
        state = max(states, key=lambda value: priority.get(value, 0))
        return {
            'state': state,
            'position_side': 'long'
            if any((_safe_decimal(item.get('open_quantity')) or Decimal('0')) > Decimal('0') for item in items)
            or any((_safe_decimal(item.get('position_quantity')) or Decimal('0')) > Decimal('0') for item in items)
            or any(bool(item.get('held_position')) for item in items)
            else None,
            'planned_intents': sorted(
                {
                    str(intent)
                    for item in items
                    for intent in (item.get('planned_intents') or [])
                    if _normalize_optional_text(intent)
                }
            ),
            'last_event_at': _latest_iso_timestamp(item.get('last_updated_at') for item in items),
            'runtime_status': runtime_status,
        }

    @staticmethod
    def _load_symbol_metadata_map(symbols: Iterable[Any]) -> dict[str, dict[str, Any]]:
        normalized_symbols = sorted(
            {
                normalized
                for value in symbols
                if (normalized := _normalize_optional_text(value))
            }
        )
        if not normalized_symbols:
            return {}
        rows = load_symbols(symbols=normalized_symbols, limit=max(len(normalized_symbols), 1))
        return {
            row['symbol']: {
                'name': row.get('name_ko') or row['symbol'],
                'market': row.get('market'),
            }
            for row in rows
        }

    @staticmethod
    def _coerce_integer(*values: Any) -> int:
        for value in values:
            decimal_value = _safe_decimal(value)
            if decimal_value is None:
                continue
            return int(decimal_value)
        return 0

    @staticmethod
    def _sum_execution_quantity(*, executions: Sequence[dict[str, Any]], side: str) -> Decimal:
        total = Decimal('0')
        for execution in executions:
            execution_side = _normalize_status(execution.get('side'))
            if execution_side != side:
                continue
            total += _safe_decimal(execution.get('quantity') or execution.get('filled_quantity')) or Decimal('0')
        return total

    @staticmethod
    def _sum_order_filled_quantity(*, orders: Sequence[dict[str, Any]], side: str) -> Decimal:
        total = Decimal('0')
        for order in orders:
            order_side = _normalize_status(order.get('side'))
            if side == 'sell':
                if order_side != 'sell':
                    continue
            elif order_side == 'sell':
                continue

            filled_quantity = _safe_decimal(
                order.get('filled_quantity')
                or order.get('executed_quantity')
                or order.get('cumulative_filled_quantity')
            )
            if filled_quantity is None and _normalize_status(
                order.get('status') or order.get('current_status') or order.get('broker_status')
            ) in REMOTE_FILLED_STATUSES:
                filled_quantity = _safe_decimal(order.get('quantity'))
            total += filled_quantity or Decimal('0')
        return total

    @staticmethod
    def _has_order_fill_evidence(*, orders: Sequence[dict[str, Any]], side: str) -> bool:
        for order in orders:
            order_side = _normalize_status(order.get('side'))
            if side == 'sell':
                if order_side != 'sell':
                    continue
            elif order_side == 'sell':
                continue

            if (
                _safe_decimal(
                    order.get('filled_quantity')
                    or order.get('executed_quantity')
                    or order.get('cumulative_filled_quantity')
                )
                or Decimal('0')
            ) > Decimal('0'):
                return True
            status = _normalize_status(order.get('status') or order.get('current_status') or order.get('broker_status'))
            if status in REMOTE_PARTIAL_STATUSES or status in REMOTE_FILLED_STATUSES:
                return True
        return False

    @staticmethod
    def _sum_position_quantity(*, positions: Sequence[dict[str, Any]]) -> Decimal:
        total = Decimal('0')
        for position in positions:
            total += _safe_decimal(position.get('quantity') or position.get('available_quantity')) or Decimal('0')
        return total

    @staticmethod
    def _weighted_position_average_price(*, positions: Sequence[dict[str, Any]]) -> Decimal | None:
        total_quantity = Decimal('0')
        total_notional = Decimal('0')
        fallback_price: Decimal | None = None
        for position in positions:
            quantity = _safe_decimal(position.get('quantity') or position.get('available_quantity')) or Decimal('0')
            price = _safe_decimal(position.get('average_price') or position.get('avg_price'))
            if price is not None and fallback_price is None:
                fallback_price = price
            if quantity <= Decimal('0') or price is None:
                continue
            total_quantity += quantity
            total_notional += quantity * price
        if total_quantity > Decimal('0'):
            return total_notional / total_quantity
        return fallback_price

    @staticmethod
    def _weighted_average_from_quantity_price_pairs(
        pairs: Iterable[tuple[Decimal, Decimal | None]],
    ) -> Decimal | None:
        total_quantity = Decimal('0')
        total_notional = Decimal('0')
        fallback_price: Decimal | None = None
        for quantity, price in pairs:
            if price is not None and fallback_price is None:
                fallback_price = price
            if quantity <= Decimal('0') or price is None:
                continue
            total_quantity += quantity
            total_notional += quantity * price
        if total_quantity > Decimal('0'):
            return total_notional / total_quantity
        return fallback_price

    @staticmethod
    def _collect_order_statuses(*, orders: Sequence[dict[str, Any]]) -> set[str]:
        statuses: set[str] = set()
        for order in orders:
            status = _normalize_status(order.get('status') or order.get('current_status') or order.get('broker_status'))
            if status:
                statuses.add(status)
        return statuses

    @classmethod
    def _build_today_activity(
        cls,
        *,
        market_date: date,
        orders: Sequence[dict[str, Any]],
        executions: Sequence[dict[str, Any]],
        net_open_quantity: Decimal,
    ) -> dict[str, Any]:
        buy = cls._build_today_activity_side(
            side='buy',
            market_date=market_date,
            orders=orders,
            executions=executions,
        )
        sell = cls._build_today_activity_side(
            side='sell',
            market_date=market_date,
            orders=orders,
            executions=executions,
        )
        return serialize_execution_today_activity_payload({
            'schema_version': DASHBOARD_TODAY_ACTIVITY_SCHEMA_VERSION,
            'market_date': market_date.isoformat(),
            'buy_activity_status': buy['status'],
            'buy_activity_source': buy['source'],
            'buy_filled_quantity_today': _to_json_number(buy['filled_quantity_today']),
            'buy_avg_fill_price_today': _to_json_number(buy['avg_fill_price_today']),
            'buy_last_filled_at': buy['last_filled_at'],
            'buy_broker_order_ids': buy['broker_order_ids'],
            'buy_broker_order_ids_omitted_count': buy['broker_order_ids_omitted_count'],
            'sell_activity_status': sell['status'],
            'sell_activity_source': sell['source'],
            'sell_filled_quantity_today': _to_json_number(sell['filled_quantity_today']),
            'sell_avg_fill_price_today': _to_json_number(sell['avg_fill_price_today']),
            'sell_last_filled_at': sell['last_filled_at'],
            'sell_broker_order_ids': sell['broker_order_ids'],
            'sell_broker_order_ids_omitted_count': sell['broker_order_ids_omitted_count'],
            'net_open_quantity': _to_json_number(net_open_quantity),
        })

    @classmethod
    def _build_today_activity_side(
        cls,
        *,
        side: str,
        market_date: date,
        orders: Sequence[dict[str, Any]],
        executions: Sequence[dict[str, Any]],
    ) -> dict[str, Any]:
        execution_facts, execution_has_untimestamped = cls._collect_today_execution_facts(
            executions=executions,
            side=side,
            market_date=market_date,
        )
        if execution_facts:
            return cls._summarize_today_activity_facts(
                facts=execution_facts,
                source='executions',
                has_untimestamped_fills=execution_has_untimestamped,
            )

        order_facts, order_has_untimestamped = cls._collect_today_order_facts(
            orders=orders,
            side=side,
            market_date=market_date,
        )
        if order_facts:
            return cls._summarize_today_activity_facts(
                facts=order_facts,
                source='orders',
                has_untimestamped_fills=execution_has_untimestamped or order_has_untimestamped,
            )

        status = 'unavailable' if execution_has_untimestamped or order_has_untimestamped else 'none'
        return {
            'status': status,
            'source': None,
            'filled_quantity_today': None if status == 'unavailable' else Decimal('0'),
            'avg_fill_price_today': None,
            'last_filled_at': None,
            'broker_order_ids': [],
            'broker_order_ids_omitted_count': 0,
        }

    @classmethod
    def _collect_today_execution_facts(
        cls,
        *,
        executions: Sequence[dict[str, Any]],
        side: str,
        market_date: date,
    ) -> tuple[list[dict[str, Any]], bool]:
        facts: list[dict[str, Any]] = []
        has_untimestamped_fills = False
        for execution in executions:
            if cls._normalize_activity_side(execution.get('side')) != side:
                continue
            quantity = _safe_decimal(execution.get('quantity') or execution.get('filled_quantity'))
            if quantity is None or quantity <= Decimal('0'):
                continue
            filled_at = cls._activity_timestamp(
                execution,
                fields=('executed_at', 'filled_at', 'last_filled_at', 'created_at', 'updated_at'),
            )
            if filled_at is None:
                has_untimestamped_fills = True
                continue
            if filled_at.astimezone(MARKET_TIME_ZONE).date() != market_date:
                continue
            facts.append(
                {
                    'quantity': quantity,
                    'price': cls._activity_price(execution),
                    'filled_at': filled_at.isoformat(),
                    'broker_order_id': cls._activity_broker_order_id(execution),
                }
            )
        return facts, has_untimestamped_fills

    @classmethod
    def _collect_today_order_facts(
        cls,
        *,
        orders: Sequence[dict[str, Any]],
        side: str,
        market_date: date,
    ) -> tuple[list[dict[str, Any]], bool]:
        facts: list[dict[str, Any]] = []
        has_untimestamped_fills = False
        for order in orders:
            if cls._normalize_activity_side(order.get('side')) != side:
                continue
            quantity = _safe_decimal(
                order.get('filled_quantity')
                or order.get('executed_quantity')
                or order.get('cumulative_filled_quantity')
            )
            if quantity is None and _normalize_status(
                order.get('status') or order.get('current_status') or order.get('broker_status')
            ) in REMOTE_FILLED_STATUSES:
                quantity = _safe_decimal(order.get('quantity'))
            if quantity is None or quantity <= Decimal('0'):
                continue
            filled_at = cls._activity_timestamp(
                order,
                fields=('filled_at', 'last_filled_at', 'updated_at', 'created_at', 'submitted_at'),
            )
            if filled_at is None:
                has_untimestamped_fills = True
                continue
            if filled_at.astimezone(MARKET_TIME_ZONE).date() != market_date:
                continue
            facts.append(
                {
                    'quantity': quantity,
                    'price': cls._activity_price(order),
                    'filled_at': filled_at.isoformat(),
                    'broker_order_id': cls._activity_broker_order_id(order),
                }
            )
        return facts, has_untimestamped_fills

    @classmethod
    def _summarize_today_activity_facts(
        cls,
        *,
        facts: Sequence[dict[str, Any]],
        source: str,
        has_untimestamped_fills: bool,
    ) -> dict[str, Any]:
        total_quantity = sum(
            ((_safe_decimal(fact.get('quantity')) or Decimal('0')) for fact in facts),
            start=Decimal('0'),
        )
        priced_quantity = Decimal('0')
        priced_notional = Decimal('0')
        for fact in facts:
            quantity = _safe_decimal(fact.get('quantity')) or Decimal('0')
            price = _safe_decimal(fact.get('price'))
            if quantity <= Decimal('0') or price is None or price <= Decimal('0'):
                continue
            priced_quantity += quantity
            priced_notional += quantity * price
        broker_order_ids, omitted_count = cls._limited_activity_broker_order_ids(
            fact.get('broker_order_id') for fact in facts
        )
        return {
            'status': 'partial' if has_untimestamped_fills else 'available',
            'source': source,
            'filled_quantity_today': total_quantity,
            'avg_fill_price_today': (priced_notional / priced_quantity) if priced_quantity > Decimal('0') else None,
            'last_filled_at': _latest_iso_timestamp(fact.get('filled_at') for fact in facts),
            'broker_order_ids': broker_order_ids,
            'broker_order_ids_omitted_count': omitted_count,
        }

    @staticmethod
    def _normalize_activity_side(value: Any) -> str | None:
        normalized = _normalize_optional_text(value, lowercase=True)
        if normalized in {'buy', 'b', 'bid'}:
            return 'buy'
        if normalized in {'sell', 's', 'ask'}:
            return 'sell'
        return None

    @staticmethod
    def _activity_timestamp(payload: dict[str, Any], *, fields: Sequence[str]) -> datetime | None:
        for field in fields:
            parsed = _parse_iso_datetime(payload.get(field))
            if parsed is not None:
                return parsed
        return None

    @staticmethod
    def _activity_price(payload: dict[str, Any]) -> Decimal | None:
        return _safe_decimal(
            payload.get('average_fill_price')
            or payload.get('avg_fill_price')
            or payload.get('fill_price')
            or payload.get('price')
            or payload.get('avg_price')
        )

    @staticmethod
    def _activity_broker_order_id(payload: dict[str, Any]) -> str | None:
        return _coalesce_text(
            payload.get('broker_order_id'),
            payload.get('order_id'),
            payload.get('broker_order_pk'),
            payload.get('id'),
        )

    @staticmethod
    def _limited_activity_broker_order_ids(values: Iterable[Any]) -> tuple[list[str], int]:
        ids: list[str] = []
        seen: set[str] = set()
        omitted_count = 0
        for value in values:
            normalized = _normalize_optional_text(value)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            if len(ids) < DASHBOARD_TODAY_ACTIVITY_BROKER_ORDER_ID_LIMIT:
                ids.append(normalized)
            else:
                omitted_count += 1
        return ids, omitted_count

    @classmethod
    def _exec_watch_board_status_from_links(cls, *, item: dict[str, Any]) -> str | None:
        if _normalize_decision_agent_name(item.get('decision_agent_name')) != SHORT_TERM_DECIDER_AGENT_NAME:
            return None
        dispatch = item.get('dispatch') if isinstance(item.get('dispatch'), dict) else {}
        statuses = set(cls._entry_link_active_statuses(item_links=dispatch.get('item_links') or []))
        if not statuses:
            return None
        if statuses & REMOTE_PARTIAL_STATUSES:
            return UI_BOARD_STATUS_PARTIAL
        if statuses & ({'watch_buy_submitted'} | REMOTE_SUBMITTED_STATUSES):
            return UI_BOARD_STATUS_SUBMITTED
        if statuses & {'watch_monitoring_active', 'active', 'monitoring'}:
            return UI_BOARD_STATUS_ACCEPTED
        if statuses & ({'watch_monitoring_pending'} | REMOTE_SCHEDULED_STATUSES):
            return UI_BOARD_STATUS_SCHEDULED
        if statuses & EXEC_WATCH_READBACK_ACTIVE_STATUSES:
            return UI_BOARD_STATUS_ACCEPTED
        return None

    def _normalize_board_item_status(
        self,
        *,
        item: dict[str, Any],
        order_statuses: set[str],
        planned_quantity: Decimal | None,
        entry_filled_quantity: Decimal,
        exit_filled_quantity: Decimal,
        open_quantity: Decimal,
        has_plan_attributed_fill_evidence: bool,
    ) -> str:
        eligibility_status = _normalize_optional_text(item.get('eligibility_status'), lowercase=True)
        execution_status = _normalize_optional_text(item.get('execution_status'), lowercase=True)
        dispatch_status = _normalize_optional_text((item.get('dispatch') or {}).get('status'), lowercase=True)
        remote_state = item.get('remote_state') if isinstance(item.get('remote_state'), dict) else {}
        derived_status = _normalize_optional_text(remote_state.get('derived_status'), lowercase=True)
        failed_order_statuses = order_statuses & REMOTE_TERMINAL_FAILURE_STATUSES

        if order_statuses & REMOTE_PARTIAL_STATUSES:
            return UI_BOARD_STATUS_PARTIAL
        if planned_quantity is not None and entry_filled_quantity > Decimal('0') and entry_filled_quantity < planned_quantity:
            return UI_BOARD_STATUS_PARTIAL
        if has_plan_attributed_fill_evidence and exit_filled_quantity > Decimal('0') and open_quantity > Decimal('0'):
            return UI_BOARD_STATUS_PARTIAL
        if has_plan_attributed_fill_evidence:
            return UI_BOARD_STATUS_FILLED
        if order_statuses & REMOTE_SUBMITTED_STATUSES:
            return UI_BOARD_STATUS_SUBMITTED
        if order_statuses & REMOTE_SCHEDULED_STATUSES:
            return UI_BOARD_STATUS_SCHEDULED
        if (order_statuses & REMOTE_CANCELLED_STATUSES or derived_status == UI_BOARD_STATUS_CANCELLED) and (
            entry_filled_quantity <= Decimal('0')
        ):
            return UI_BOARD_STATUS_CANCELLED
        if failed_order_statuses or derived_status == 'failed':
            return UI_BOARD_STATUS_FAILED
        if execution_status == EXECUTION_STATUS_SUBMIT_FAILED or dispatch_status in {
            EXEC_DISPATCH_STATUS_FAILED,
            EXEC_DISPATCH_STATUS_NOT_CONFIGURED,
            EXEC_DISPATCH_STATUS_CONTRACT_ERROR,
            EXEC_DISPATCH_STATUS_UNAVAILABLE,
        }:
            return UI_BOARD_STATUS_FAILED
        exec_watch_board_status = self._exec_watch_board_status_from_links(item=item)
        if exec_watch_board_status is not None:
            return exec_watch_board_status
        if eligibility_status == ELIGIBILITY_BLOCKED:
            return UI_BOARD_STATUS_FAILED
        if dispatch_status in {EXEC_DISPATCH_STATUS_ACCEPTED, EXEC_DISPATCH_STATUS_PARTIAL}:
            return UI_BOARD_STATUS_ACCEPTED
        return UI_BOARD_STATUS_PLANNED

    def _resolve_monitoring_state(
        self,
        *,
        status: str,
        open_quantity: Decimal,
        planned_intents: Sequence[str],
        runtime_status: str,
        has_execution_plan: bool,
        has_target_stop_watch: bool,
    ) -> str:
        if runtime_status in {UI_RUNTIME_STATUS_DEGRADED, UI_RUNTIME_STATUS_OFFLINE} and has_execution_plan:
            return UI_MONITORING_STATE_RECONCILING
        if open_quantity > Decimal('0') and (planned_intents or has_target_stop_watch):
            return UI_MONITORING_STATE_WATCHING_TARGET_STOP
        if status in {
            UI_BOARD_STATUS_ACCEPTED,
            UI_BOARD_STATUS_SCHEDULED,
            UI_BOARD_STATUS_SUBMITTED,
            UI_BOARD_STATUS_PARTIAL,
        }:
            return UI_MONITORING_STATE_WATCHING_FILL
        if has_target_stop_watch:
            return UI_MONITORING_STATE_WATCHING_FILL
        return UI_MONITORING_STATE_IDLE

    @staticmethod
    def _collect_planned_intents(*, item: dict[str, Any]) -> list[str]:
        dispatch = item.get('dispatch') if isinstance(item.get('dispatch'), dict) else {}
        intents = []
        for link in dispatch.get('item_links') or []:
            intent = _normalize_optional_text(link.get('intent'), lowercase=True)
            if intent and intent not in EXEC_ENTRY_BUY_INTENTS:
                intents.append(intent)
        if _positive_decimal(item.get('target_price')) is not None:
            intents.append(EXEC_INTENT_TARGET_SELL)
        if _positive_decimal(item.get('stop_loss_price')) is not None:
            intents.append(EXEC_INTENT_STOP_SELL)
        return sorted(dict.fromkeys(intents))

    @staticmethod
    def _has_target_stop_watch_facts(item: dict[str, Any]) -> bool:
        return any(
            _positive_decimal(item.get(field_name)) is not None
            for field_name in ('target_price', 'stop_loss_price', 'trigger_price', 'chase_limit_price')
        )

    @staticmethod
    def _resolve_entry_intent(*, item: dict[str, Any]) -> str:
        dispatch = item.get('dispatch') if isinstance(item.get('dispatch'), dict) else {}
        for link in dispatch.get('item_links') or []:
            intent = _normalize_optional_text(link.get('intent'), lowercase=True)
            if intent in EXEC_ENTRY_BUY_INTENTS:
                return intent
        return EXEC_INTENT_NEXT_OPEN_BUY

    @staticmethod
    def _resolve_average_fill_price(*, executions: Sequence[dict[str, Any]], positions: Sequence[dict[str, Any]]) -> int | float | None:
        total_quantity = Decimal('0')
        total_notional = Decimal('0')
        for execution in executions:
            if _normalize_status(execution.get('side')) == 'sell':
                continue
            quantity = _safe_decimal(execution.get('quantity') or execution.get('filled_quantity'))
            price = _safe_decimal(
                execution.get('average_fill_price')
                or execution.get('fill_price')
                or execution.get('price')
            )
            if quantity is None or quantity <= Decimal('0') or price is None or price <= Decimal('0'):
                continue
            total_quantity += quantity
            total_notional += quantity * price
        if total_quantity > Decimal('0'):
            return _to_json_number(total_notional / total_quantity)
        if positions:
            return _to_json_number(_safe_decimal(positions[0].get('average_price')))
        return None

    @staticmethod
    def _resolve_broker_order_id(*, orders: Sequence[dict[str, Any]], executions: Sequence[dict[str, Any]]) -> str | None:
        for order in orders:
            resolved = _coalesce_text(order.get('broker_order_id'), order.get('id'), order.get('order_id'))
            if resolved:
                return resolved
        for execution in executions:
            resolved = _coalesce_text(execution.get('broker_order_id'), execution.get('order_id'))
            if resolved:
                return resolved
        return None

    def _resolve_last_updated_at(
        self,
        *,
        item: dict[str, Any],
        orders: Sequence[dict[str, Any]],
        executions: Sequence[dict[str, Any]],
        positions: Sequence[dict[str, Any]],
    ) -> str | None:
        values: list[Any] = [
            item.get('submitted_at'),
            item.get('last_evaluated_at'),
            item.get('first_ready_at'),
            (item.get('dispatch') or {}).get('last_attempt_at'),
            (item.get('dispatch') or {}).get('last_success_at'),
            (item.get('remote_state') or {}).get('last_synced_at'),
        ]
        for payload in orders:
            values.extend(
                [
                    payload.get('updated_at'),
                    payload.get('last_event_at'),
                    payload.get('submitted_at'),
                    payload.get('created_at'),
                ]
            )
        for payload in executions:
            values.extend(
                [
                    payload.get('executed_at'),
                    payload.get('filled_at'),
                    payload.get('created_at'),
                    payload.get('updated_at'),
                ]
            )
        for payload in positions:
            values.extend(
                [
                    payload.get('updated_at'),
                    payload.get('as_of'),
                    payload.get('captured_at'),
                ]
            )
        return _latest_iso_timestamp(values)

    def _resolve_failure_reason(self, *, item: dict[str, Any], order_statuses: set[str], status: str) -> str | None:
        validation = item.get('validation') if isinstance(item.get('validation'), dict) else {}
        blocking_reasons = validation.get('blocking_reasons') if isinstance(validation.get('blocking_reasons'), list) else []
        if status == UI_BOARD_STATUS_FAILED and blocking_reasons:
            first_reason = blocking_reasons[0]
            if isinstance(first_reason, dict):
                return _normalize_optional_text(first_reason.get('message'))
            return _normalize_optional_text(first_reason)

        dispatch = item.get('dispatch') if isinstance(item.get('dispatch'), dict) else {}
        last_error = dispatch.get('last_error') if isinstance(dispatch.get('last_error'), dict) else {}
        if status == UI_BOARD_STATUS_FAILED and last_error:
            return _normalize_optional_text(last_error.get('message')) or _normalize_optional_text(last_error.get('code'))

        if status == UI_BOARD_STATUS_CANCELLED:
            return 'Execution was cancelled before entry fill completed.'
        if status == UI_BOARD_STATUS_FAILED and order_statuses:
            if 'rejected' in order_statuses:
                return 'Exec rejected the order.'
            if 'failed' in order_statuses:
                return 'Exec reported a failed order lifecycle.'
        return None

    @staticmethod
    def _client_error_status(exc: SignalDeciderExecClientError) -> str:
        if exc.code in {EXEC_DISPATCH_STATUS_NOT_CONFIGURED, EXEC_DISPATCH_STATUS_CONTRACT_ERROR}:
            return exc.code
        return REMOTE_FETCH_STATUS_UNAVAILABLE


signal_decider_exec_integration_service = SignalDeciderExecIntegrationService()
