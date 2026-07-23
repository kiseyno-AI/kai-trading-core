from __future__ import annotations

import asyncio
from collections import Counter, defaultdict
import copy
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
from typing import Any, Iterable, Sequence
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import select

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
    EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION,
    EXECUTION_RUNTIME_DASHBOARD_READ_MODEL_SCHEMA_VERSION,
    EXECUTION_RUNTIME_TODAY_ACTIVITY_SCHEMA_VERSION,
    serialize_exec_agent_decider_handoff_payload,
    serialize_execution_daily_board_payload,
    serialize_execution_dashboard_freshness_payload,
    serialize_execution_monitored_symbols_payload,
    serialize_execution_plan_dispatch_payload,
    serialize_execution_runtime_exec_card_payload,
    serialize_execution_runtime_status_payload,
    serialize_execution_symbol_detail_payload,
    serialize_execution_today_activity_payload,
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
    EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING,
    EXECUTION_STATUS_PLANNED,
    EXECUTION_STATUS_SUBMIT_FAILED,
    EXECUTION_STATUS_SUBMITTED,
    EXECUTION_WINDOW_NEXT_OPEN,
    _async_previous_trading_day,
    async_normalize_next_open_target_trade_date,
    signal_decider_next_open_execution_service,
)
from app.orders.portfolio_summary_service import portfolio_summary_service
from app.quotes.display_prices import (
    DISPLAY_FALLBACK_SOURCES,
    build_display_price_fields,
)
from app.quotes.kis_realtime_prices import kis_realtime_price_service
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
EXEC_DISPATCH_STATUS_ACCEPTED = 'accepted'
EXEC_DISPATCH_STATUS_PARTIAL = 'partial'
EXEC_DISPATCH_STATUS_FAILED = 'failed'
EXEC_DISPATCH_STATUS_SKIPPED = 'skipped'
EXEC_DISPATCH_STATUS_ALREADY_DISPATCHED = 'already_dispatched'
EXEC_DISPATCH_STATUS_NOT_CONFIGURED = 'not_configured'
EXEC_DISPATCH_STATUS_CONTRACT_ERROR = 'contract_error'
EXEC_DISPATCH_STATUS_UNAVAILABLE = 'unavailable'

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
MARKET_REGIME_AGGRESSIVE_OPEN_MULTIPLIER_CAP = Decimal('2.0')
MARKET_REGIME_TREND_DAY_MULTIPLIER_CAP = Decimal('1.25')
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

MARKET_TIME_ZONE = ZoneInfo('Asia/Seoul')
CALENDAR_FALLBACK_STATUSES = {
    CALENDAR_SOURCE_STATUS_FALLBACK,
    CALENDAR_SOURCE_STATUS_MISSING_DATE,
    CALENDAR_SOURCE_STATUS_UNAVAILABLE,
}


@dataclass
class DashboardReadModelCacheRecord:
    payload: dict[str, Any]
    cached_at: datetime


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_optional_text(value: Any, *, lowercase: bool = False) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    return normalized.lower() if lowercase else normalized


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
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _positive_decimal(value: Any) -> Decimal | None:
    parsed = _safe_decimal(value)
    if parsed is not None and parsed > Decimal('0'):
        return parsed
    return None


def _decimal_to_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, 'f')


def _to_json_number(value: Decimal | None) -> int | float | None:
    if value is None:
        return None
    if value == value.to_integral():
        return int(value)
    return float(value)


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
    raw_multiplier = (
        _safe_decimal(market_regime_context.get('quantity_multiplier'))
        or _safe_decimal(market_regime_context.get('recommended_sizing_multiplier'))
        or Decimal('1')
    )
    cap = _market_regime_multiplier_cap(market_regime)
    multiplier = min(max(raw_multiplier, Decimal('0')), cap)
    sizing_window = (
        copy.deepcopy(market_regime_context.get('sizing_window'))
        if isinstance(market_regime_context.get('sizing_window'), dict)
        else None
    )
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
        'entry_priority_boost': bool(market_regime_context.get('entry_priority_boost')),
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

    async def post_execution_plan(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._request_json('POST', '/execution-plans', json_body=payload)

    async def list_execution_plans(self, *, external_plan_id: str | None = None) -> dict[str, Any]:
        params = {'external_plan_id': external_plan_id} if external_plan_id else None
        return await self._request_json('GET', '/execution-plans', params=params)

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

    async def list_positions(self, *, symbols: Sequence[str] | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if symbols:
            params['symbols'] = ','.join(sorted({_normalize_optional_text(item) for item in symbols if item}))
        return await self._request_json('GET', '/positions', params=params or None)

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
        self._dashboard_refresh_tasks: dict[tuple[str, str | None, str | None], asyncio.Task] = {}
        self._dashboard_refresh_state: dict[tuple[str, str | None, str | None], dict[str, Any]] = {}

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
        runtime_mode = _normalize_optional_text(settings.signal_decider_exec_runtime_mode, lowercase=True) or 'mock'
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
            'base_url': base_url,
            'bearer_token_configured': bool(_normalize_optional_text(settings.signal_decider_exec_bearer_token)),
            'dispatch_source': _normalize_optional_text(settings.signal_decider_exec_dispatch_source) or 'kai-trading-core',
            'runtime_mode': runtime_mode,
            'request_timeout_seconds': float(settings.signal_decider_exec_request_timeout_seconds),
            'issues': issues,
        }

    def should_delegate_execution(self) -> bool:
        return self.integration_status()['delegate_execution']

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

        if not force and not resolved_dry_run and _calendar_day_requires_operator_override(calendar_day):
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
        if refresh_plan:
            refresh_result = signal_decider_next_open_execution_service.refresh_plan_items(
                target_trade_date=resolved_target_date,
                trigger=f'{trigger}_plan_refresh',
                force=force,
            )
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
            enrichment_preflight = (
                signal_decider_next_open_execution_service.inspect_source_market_enrichment_preflight(
                    source_market_date=source_market_date,
                    trigger=f'{trigger}_dispatch',
                )
            )

        if enrichment_preflight is not None and not force and not enrichment_preflight.get('allowed'):
            existing_rows = self._load_next_open_rows(
                target_trade_date=resolved_target_date,
                account_reference=resolved_account_reference,
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

        for (account_key, decision_agent_name), account_rows in grouped.items():
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
            plan_payload, row_links = self._build_exec_plan_payload(
                rows=eligible_rows,
                snapshots=snapshots,
                trigger=trigger,
            )
            translated_item_count += len(plan_payload['items'])
            plan_id = _normalize_optional_text(plan_payload.get('plan_id')) or 'unknown-plan'
            fingerprint = _fingerprint_exec_plan(plan_payload)

            if not force and self._rows_match_existing_dispatch(eligible_rows, plan_id=plan_id, fingerprint=fingerprint):
                skipped_group_count += 1
                group_results.append(
                    {
                        'account_reference': account_key,
                        'decision_agent_name': decision_agent_name,
                        'plan_id': plan_id,
                        'dispatch_status': EXEC_DISPATCH_STATUS_ALREADY_DISPATCHED,
                        'eligible_count': len(eligible_rows),
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
                        'translated_item_count': len(plan_payload['items']),
                    }
                )
                continue

            if integration['config_status'] != 'ready':
                failed_group_count += 1
                self._persist_dispatch_failure(
                    rows=eligible_rows,
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
                        'translated_item_count': len(plan_payload['items']),
                    }
                )
                continue

            try:
                response_payload = await self.client.post_execution_plan(plan_payload)
            except SignalDeciderExecClientError as exc:
                if exc.status_code == 409:
                    response_payload = await self._recover_duplicate_dispatch_response(
                        plan_id=plan_id,
                        row_links=row_links,
                    )
                    if response_payload is not None:
                        persisted_group_result = self._persist_dispatch_success(
                            rows=eligible_rows,
                            row_links=row_links,
                            plan_id=plan_id,
                            fingerprint=fingerprint,
                            payload=plan_payload,
                            response_payload=response_payload,
                            trigger=trigger,
                        )
                        dispatched_group_count += 1
                        group_results.append(
                            {
                                'account_reference': account_key,
                                'decision_agent_name': decision_agent_name,
                                'plan_id': plan_id,
                                'recovered_from_duplicate': True,
                                **persisted_group_result,
                            }
                        )
                        continue
                status = self._client_error_status(exc)
                failed_group_count += 1
                self._persist_dispatch_failure(
                    rows=eligible_rows,
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
                )
                group_results.append(
                    {
                        'account_reference': account_key,
                        'decision_agent_name': decision_agent_name,
                        'plan_id': plan_id,
                        'dispatch_status': status,
                        'eligible_count': len(eligible_rows),
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

            persisted_group_result = self._persist_dispatch_success(
                rows=eligible_rows,
                row_links=row_links,
                plan_id=plan_id,
                fingerprint=fingerprint,
                payload=plan_payload,
                response_payload=response_payload,
                trigger=trigger,
            )
            dispatched_group_count += 1
            group_results.append(
                {
                    'account_reference': account_key,
                    'decision_agent_name': decision_agent_name,
                    'plan_id': plan_id,
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
            'dispatch': {
                'groups': group_results,
                'preview_plans': preview_plans,
            },
            'summary': summary,
            'enrichment_preflight': enrichment_preflight,
            'items': plan_view.get('items') or [],
        }

    async def _recover_duplicate_dispatch_response(
        self,
        *,
        plan_id: str,
        row_links: dict[int, list[dict[str, Any]]],
    ) -> dict[str, Any] | None:
        try:
            readback = await self.client.list_execution_plans(external_plan_id=plan_id)
        except SignalDeciderExecClientError:
            return None
        plans = _collect_items(readback)
        plan = next(
            (
                item
                for item in plans
                if _normalize_optional_text(item.get('plan_id') or item.get('external_plan_id')) == plan_id
            ),
            plans[0] if len(plans) == 1 else None,
        )
        if not isinstance(plan, dict):
            return None
        status = _normalize_status(plan.get('status'))
        if status not in {EXEC_DISPATCH_STATUS_ACCEPTED, EXEC_DISPATCH_STATUS_PARTIAL, 'accepted'}:
            return None
        exec_items = _collect_items(plan)
        by_symbol_intent: dict[tuple[str | None, str | None], dict[str, Any]] = {}
        for item in exec_items:
            by_symbol_intent[
                (
                    _normalize_optional_text(item.get('symbol')),
                    _normalize_optional_text(item.get('intent')),
                )
            ] = item
        recovered_items: list[dict[str, Any]] = []
        for links in row_links.values():
            for link in links:
                item = by_symbol_intent.get(
                    (
                        _normalize_optional_text(link.get('symbol')),
                        _normalize_optional_text(link.get('intent')),
                    )
                )
                if item is None:
                    continue
                recovered_items.append(
                    {
                        **item,
                        'core_item_key': link.get('core_item_key'),
                        'status': item.get('status') or EXEC_DISPATCH_STATUS_ACCEPTED,
                    }
                )
        if not recovered_items:
            return None
        return {
            'plan_id': plan_id,
            'execution_plan_id': plan.get('execution_plan_id') or plan.get('id'),
            'status': EXEC_DISPATCH_STATUS_ACCEPTED,
            'accepted_item_count': len(recovered_items),
            'rejected_item_count': 0,
            'items': recovered_items,
            'duplicate_recovered': True,
        }

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
        base_items = plan_view.get('items') or []
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
            allow_stale=False,
        )
        if cached is not None:
            return cached

        overview = await self._load_dashboard_read_model_overview(
            target_trade_date=resolved_market_date,
            account_reference=None,
            symbol=None,
            trigger=trigger,
        )
        payload = await asyncio.to_thread(
            self._build_runtime_daily_board_payload,
            market_date=resolved_market_date,
            refresh=refresh,
            overview=overview,
            refresh_state=refresh_state,
            served_from_cache=False,
        )
        self._store_dashboard_cached_payload(
            endpoint=DASHBOARD_ENDPOINT_DAILY_BOARD,
            market_date=resolved_market_date,
            symbol=None,
            payload=payload,
        )
        return payload

    async def list_runtime_monitored_symbols(
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
            endpoint=DASHBOARD_ENDPOINT_MONITORED_SYMBOLS,
            market_date=resolved_market_date,
            symbol=None,
            refresh_state=refresh_state,
            allow_stale=False,
        )
        if cached is not None:
            self._observe_runtime_symbols_for_realtime_prices(
                (item.get('symbol') for item in cached.get('items') or []),
                trigger=trigger,
            )
            return cached

        board = await self.get_runtime_daily_board(
            market_date=resolved_market_date,
            refresh=False,
            trigger=trigger,
        )
        live_holding_symbols = await self._load_live_portfolio_holding_symbols(trigger=trigger)
        live_exec_monitoring_symbols = await self._load_live_exec_monitoring_symbols(
            market_date=resolved_market_date,
            trigger=trigger,
        )
        payload = await asyncio.to_thread(
            self._build_runtime_monitored_symbols_payload,
            board=board,
            refresh=refresh,
            refresh_state=refresh_state,
            served_from_cache=False,
            live_holding_symbols=live_holding_symbols,
            live_exec_monitoring_symbols=live_exec_monitoring_symbols,
        )
        self._observe_runtime_symbols_for_realtime_prices(
            (item.get('symbol') for item in payload['items']),
            trigger=trigger,
        )
        self._store_dashboard_cached_payload(
            endpoint=DASHBOARD_ENDPOINT_MONITORED_SYMBOLS,
            market_date=resolved_market_date,
            symbol=None,
            payload=payload,
        )
        return payload

    async def _load_live_exec_monitoring_symbols(self, *, market_date: date, trigger: str) -> set[str] | None:
        integration = self.integration_status()
        if integration['config_status'] in {REMOTE_FETCH_STATUS_DISABLED, REMOTE_FETCH_STATUS_NOT_CONFIGURED}:
            return None
        try:
            payload = await self.client.get_runtime_status()
        except SignalDeciderExecClientError:
            return None
        return self._extract_live_exec_monitoring_symbols(payload=payload, market_date=market_date)

    @classmethod
    def _extract_live_exec_monitoring_symbols(cls, *, payload: dict[str, Any], market_date: date) -> set[str]:
        monitoring = _extract_field(payload, 'liveness', 'autonomous', 'monitoring')
        if not isinstance(monitoring, dict):
            return set()
        last_cycle = monitoring.get('last_cycle') if isinstance(monitoring.get('last_cycle'), dict) else {}
        items = last_cycle.get('items') if isinstance(last_cycle.get('items'), list) else []
        symbols: set[str] = set()
        market_date_text = market_date.isoformat()
        compact_market_date = market_date.strftime('%Y%m%d')
        for item in items:
            if not isinstance(item, dict):
                continue
            if _normalize_optional_text(item.get('intent'), lowercase=True) not in EXEC_ENTRY_BUY_INTENTS:
                continue
            status = _normalize_status(item.get('status'))
            if status != 'watch_monitoring_active':
                continue
            source_identity = item.get('source_identity') if isinstance(item.get('source_identity'), dict) else {}
            source_plan_id = _normalize_optional_text(source_identity.get('source_plan_id')) or ''
            core_plan_id = _normalize_optional_text(source_identity.get('core_plan_id')) or ''
            strategy_run_id = _normalize_optional_text(source_identity.get('strategy_run_id')) or ''
            if (
                compact_market_date not in source_plan_id
                and compact_market_date not in core_plan_id
                and f':{market_date_text}:' not in strategy_run_id
            ):
                continue
            symbol = _normalize_optional_text(item.get('symbol'))
            if symbol:
                symbols.add(symbol)
        return symbols

    async def _load_live_portfolio_holding_symbols(self, *, trigger: str) -> set[str] | None:
        try:
            payload = await portfolio_summary_service.get_portfolio_summary(
                refresh=False,
                trigger=f'{trigger}:runtime_monitored_holdings',
            )
        except Exception:
            return None

        symbols: set[str] = set()
        for account in payload.get('accounts') or []:
            if not isinstance(account, dict):
                continue
            for holding in account.get('holdings') or []:
                if not isinstance(holding, dict):
                    continue
                symbol = _normalize_optional_text(holding.get('symbol'))
                quantity = _safe_decimal(holding.get('quantity')) or Decimal('0')
                if symbol and quantity > Decimal('0'):
                    symbols.add(symbol)
        return symbols

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
            and self._historical_held_symbol_is_live(item, live_holding_symbols=live_holding_symbols)
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
            if not self._historical_held_symbol_is_live(item, live_holding_symbols=live_holding_symbols):
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
    ) -> bool:
        if live_holding_symbols is None:
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
            allow_stale=False,
        )
        if cached is not None:
            return cached

        overview = await self._load_dashboard_read_model_overview(
            target_trade_date=resolved_market_date,
            account_reference=None,
            symbol=resolved_symbol,
            trigger=trigger,
        )
        payload = await asyncio.to_thread(
            self._build_runtime_symbol_detail_payload,
            market_date=resolved_market_date,
            symbol=resolved_symbol,
            refresh=refresh,
            overview=overview,
            refresh_state=refresh_state,
            served_from_cache=False,
        )
        self._store_dashboard_cached_payload(
            endpoint=DASHBOARD_ENDPOINT_SYMBOL_DETAIL,
            market_date=resolved_market_date,
            symbol=resolved_symbol,
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
        runtime = self._build_dashboard_runtime_status(
            integration=overview['integration'],
            runtime=self._resolve_dashboard_runtime_read_model(overview=overview, overview_items=overview_items),
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
        payload = {
            'request': {
                'market_date': market_date.isoformat(),
                'refresh': refresh,
            },
            'runtime': runtime,
            'market_date': market_date.isoformat(),
            'summary': self._build_runtime_daily_board_summary(items=items),
            'freshness': self._build_dashboard_freshness(
                endpoint=DASHBOARD_ENDPOINT_DAILY_BOARD,
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
        return serialize_execution_daily_board_payload(payload)

    def _build_runtime_monitored_symbols_payload(
        self,
        *,
        board: dict[str, Any],
        refresh: bool,
        refresh_state: dict[str, Any],
        served_from_cache: bool,
        live_holding_symbols: set[str] | None = None,
        live_exec_monitoring_symbols: set[str] | None = None,
    ) -> dict[str, Any]:
        resolved_market_date = date.fromisoformat(str(board['market_date']))
        board_items = list(board.get('items') or [])
        if board_items or live_holding_symbols is None or live_holding_symbols:
            board_items.extend(
                self._load_historical_held_runtime_board_items(
                    market_date=resolved_market_date,
                    runtime_status=board['runtime']['status'],
                    existing_plan_item_ids=self._collect_runtime_plan_item_ids(board_items),
                    existing_held_scopes=self._collect_runtime_held_scopes(board_items),
                    live_holding_symbols=live_holding_symbols,
                )
            )
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        live_exec_monitoring_symbols = live_exec_monitoring_symbols or set()
        for item in board_items:
            symbol = _normalize_optional_text(item.get('symbol'))
            if symbol:
                live_exec_watch = symbol in live_exec_monitoring_symbols and _normalize_optional_text(
                    item.get('intent'), lowercase=True
                ) in EXEC_ENTRY_BUY_INTENTS
                if live_exec_watch:
                    item = {
                        **item,
                        'approval_state': 'approved',
                        'exec_agent_approved': True,
                        'exec_agent_action': item.get('exec_agent_action') or 'approve',
                        'exec_monitoring_active': True,
                        'exec_runtime_showable': True,
                        'exec_runtime_state': 'exec_approved_watch',
                        'exec_runtime_exclusion_reason': None,
                        'monitoring_statuses': sorted(
                            {
                                *(item.get('monitoring_statuses') or []),
                                'watch_monitoring_active',
                            }
                        ),
                    }
                grouped[symbol].append(item)

        included_groups = {
            symbol: grouped_items
            for symbol, grouped_items in grouped.items()
            if self._should_include_monitored_symbol_group(grouped_items)
        }
        realtime_symbols = self._prioritized_runtime_symbols_for_realtime(included_groups)
        self._observe_runtime_symbols_for_realtime_prices(
            realtime_symbols,
            trigger='dashboard_read_model',
        )
        items = [
            self._build_runtime_monitored_symbol_item(
                symbol=symbol,
                items=included_groups[symbol],
                runtime_status=board['runtime']['status'],
                market_date=resolved_market_date,
            )
            for symbol in realtime_symbols
        ]
        generated_at = _utcnow()
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
            'freshness': self._build_dashboard_freshness(
                endpoint=DASHBOARD_ENDPOINT_MONITORED_SYMBOLS,
                market_date=resolved_market_date,
                generated_at=generated_at,
                served_from_cache=served_from_cache,
                runtime_status=board['runtime']['status'],
                overview_items=board_items,
                output_item_count=len(items),
                refresh_state=refresh_state,
            ),
            'refresh': refresh_state,
            'items': items,
        }
        return serialize_execution_monitored_symbols_payload(payload)

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
        live_exec_monitoring_symbols = None
        if symbol is None:
            background_trigger = f'{trigger}:dashboard_background_refresh'
            live_holding_symbols = await self._load_live_portfolio_holding_symbols(
                trigger=background_trigger,
            )
            live_exec_monitoring_symbols = await self._load_live_exec_monitoring_symbols(
                market_date=target_trade_date,
                trigger=background_trigger,
            )
        self._store_dashboard_caches_from_overview(
            market_date=target_trade_date,
            symbol=symbol,
            overview=overview,
            refresh_state=refresh_state,
            live_holding_symbols=live_holding_symbols,
            live_exec_monitoring_symbols=live_exec_monitoring_symbols,
        )

    def _finalize_dashboard_refresh_task(
        self,
        key: tuple[str, str | None, str | None],
        task: asyncio.Task,
    ) -> None:
        if self._dashboard_refresh_tasks.get(key) is task:
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

    def _store_dashboard_caches_from_overview(
        self,
        *,
        market_date: date,
        symbol: str | None,
        overview: dict[str, Any],
        refresh_state: dict[str, Any],
        live_holding_symbols: set[str] | None = None,
        live_exec_monitoring_symbols: set[str] | None = None,
    ) -> None:
        if symbol:
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
        monitored = self._build_runtime_monitored_symbols_payload(
            board=board,
            refresh=True,
            refresh_state=refresh_state,
            served_from_cache=False,
            live_holding_symbols=live_holding_symbols,
            live_exec_monitoring_symbols=live_exec_monitoring_symbols,
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
        if refreshing:
            status = DASHBOARD_FRESHNESS_STATUS_REFRESHING
        elif stale_by_cache or freshness.get('is_stale'):
            status = DASHBOARD_FRESHNESS_STATUS_STALE
        elif freshness.get('is_partial'):
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
            'is_stale': bool(stale_by_readback or status == DASHBOARD_FRESHNESS_STATUS_STALE),
            'is_partial': bool(readback_missing_count > 0 or status == DASHBOARD_FRESHNESS_STATUS_PARTIAL),
            'refreshing': self._dashboard_refresh_in_flight(refresh_state),
            'last_readback_synced_at': last_readback_synced_at,
            'readback_age_seconds': readback_age_seconds,
            'readback_synced_count': readback_synced_count,
            'readback_missing_count': readback_missing_count,
            'overview_item_count': len(overview_items),
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
    ) -> str:
        if self._dashboard_refresh_in_flight(refresh_state):
            return DASHBOARD_FRESHNESS_STATUS_REFRESHING
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
        return max(int((_utcnow() - record.cached_at).total_seconds()), 0)

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
            if account_reference:
                query = query.where(SignalDeciderExecutionPlanItem.account_reference == account_reference)
            if symbol:
                query = query.where(SignalDeciderExecutionPlanItem.symbol == symbol)
            rows = session.execute(query).scalars().all()
            for row in rows:
                session.expunge(row)
            return rows

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
    ) -> dict[tuple[str, str], list[SignalDeciderExecutionPlanItem]]:
        grouped: dict[tuple[str, str], list[SignalDeciderExecutionPlanItem]] = defaultdict(list)
        for row in rows:
            grouped[(row.account_reference, _normalize_decision_agent_name(row.decision_agent_name))].append(row)
        return grouped

    @staticmethod
    def _row_is_dispatchable(row: SignalDeciderExecutionPlanItem) -> bool:
        if row.eligibility_status != ELIGIBILITY_ELIGIBLE:
            return False
        decision_agent_name = _normalize_decision_agent_name(row.decision_agent_name)
        if decision_agent_name != SHORT_TERM_DECIDER_AGENT_NAME:
            return True
        validation = row.validation_payload if isinstance(row.validation_payload, dict) else {}
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
        generic_action = traceability.get('generic_action')
        if generic_action is None and raw_action in {'scalp_buy_candidate', 'watch_for_trigger'}:
            generic_action = 'buy'
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
    ) -> tuple[dict[str, Any], dict[int, list[dict[str, Any]]]]:
        first_row = rows[0]
        decision_agent_name = _normalize_decision_agent_name(first_row.decision_agent_name)
        source_market_date = first_row.source_market_date.isoformat()
        plan_id = (
            f'core-{_sanitize_key(decision_agent_name)}-{first_row.target_trade_date.strftime("%Y%m%d")}-'
            f'{_sanitize_key(first_row.account_reference)}'
        )
        runtime_mode = _normalize_optional_text(settings.signal_decider_exec_runtime_mode, lowercase=True) or 'mock'
        dispatch_source = _normalize_optional_text(settings.signal_decider_exec_dispatch_source) or 'kai-trading-core'
        items: list[dict[str, Any]] = []
        row_links: dict[int, list[dict[str, Any]]] = defaultdict(list)
        requested_at = _utcnow().isoformat()

        for row in rows:
            snapshot = snapshots.get(int(row.decision_snapshot_id))
            row_decision_agent_name = _normalize_decision_agent_name(row.decision_agent_name)
            traceability = row.traceability_payload if isinstance(row.traceability_payload, dict) else {}
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
            generic_action = traceability.get('generic_action')
            if generic_action is None and raw_action in {'scalp_buy_candidate', 'watch_for_trigger'}:
                generic_action = 'buy'
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
            decider_handoff_context = self._build_decider_handoff_context(
                row=row,
                snapshot=snapshot,
                traceability=traceability,
                validation=validation,
                decision_traceability=decision_traceability,
            )
            decision_input_quality_context = (
                decider_handoff_context.get('decision_input_quality_context')
                if isinstance(decider_handoff_context.get('decision_input_quality_context'), dict)
                else {}
            )
            market_regime_context = (
                decider_handoff_context.get('market_regime_context')
                if isinstance(decider_handoff_context.get('market_regime_context'), dict)
                else {}
            )
            market_regime_sizing_policy = _build_market_regime_sizing_policy(market_regime_context)
            execution_feasibility_context = (
                decision_input_quality_context.get('execution_feasibility')
                if isinstance(decision_input_quality_context.get('execution_feasibility'), dict)
                else {}
            )
            alignment_gate = (
                traceability.get('alignment_gate')
                if isinstance(traceability.get('alignment_gate'), dict)
                else validation.get('alignment_gate')
                if isinstance(validation.get('alignment_gate'), dict)
                else None
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
            common_risk_context = {
                'schema_version': EXEC_SOURCE_SCHEMA_VERSION,
                'classification': decider_handoff_context['classification'],
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
                'validation': validation,
                'traceability': decision_traceability,
                'decider_handoff_context': decider_handoff_context,
                'market_regime_context': market_regime_context,
                'market_regime_sizing_policy': market_regime_sizing_policy,
                'decision_input_quality_context': decision_input_quality_context,
                'execution_feasibility_context': execution_feasibility_context,
            }
            if uses_exec_agent_sizing:
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
                    'market_regime': market_regime_sizing_policy['market_regime'],
                    'quantity_multiplier': market_regime_sizing_policy['quantity_multiplier'],
                    'quantity_multiplier_cap': market_regime_sizing_policy['quantity_multiplier_cap'],
                    'entry_priority_boost': market_regime_sizing_policy['entry_priority_boost'],
                    'quantity_multiplier_valid_until': market_regime_sizing_policy['valid_until'],
                }
            )
            entry_item = {
                'core_item_key': core_entry_key,
                'agent_name': row_decision_agent_name,
                'decision_agent_name': row_decision_agent_name,
                'symbol': row.symbol,
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
                'risk_context': {
                    **common_risk_context,
                    'entry_price_basis': snapshot.entry_price_basis if snapshot is not None else 'next_open',
                    'expected_return_pct': _to_json_number(_safe_decimal(snapshot.expected_return_pct if snapshot else None)),
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
                    'response_payload_summary': response_payload_summary if isinstance(response_payload_summary, dict) else None,
                },
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
                        'agent_name': row_decision_agent_name,
                        'decision_agent_name': row_decision_agent_name,
                        'symbol': row.symbol,
                        'side': 'sell',
                        'intent': EXEC_INTENT_TARGET_SELL,
                        'quantity_policy': target_quantity_policy,
                        'limit_price': None,
                        'trigger_price': target_price,
                        'time_in_force': row.planned_time_in_force,
                        'risk_context': {
                            **common_risk_context,
                            'trigger_rule': EXEC_INTENT_TARGET_SELL,
                        },
                        'decision_snapshot_id': str(row.decision_snapshot_id),
                    }
                )
                row_links[row.id].append(
                    {
                        'core_item_key': target_key,
                        'intent': EXEC_INTENT_TARGET_SELL,
                        'side': 'sell',
                        'symbol': row.symbol,
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
                        'agent_name': row_decision_agent_name,
                        'decision_agent_name': row_decision_agent_name,
                        'symbol': row.symbol,
                        'side': 'sell',
                        'intent': EXEC_INTENT_STOP_SELL,
                        'quantity_policy': stop_quantity_policy,
                        'limit_price': None,
                        'trigger_price': stop_loss_price,
                        'time_in_force': row.planned_time_in_force,
                        'risk_context': {
                            **common_risk_context,
                            'trigger_rule': EXEC_INTENT_STOP_SELL,
                        },
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
                'agent_name': decision_agent_name,
                'decision_agent_name': decision_agent_name,
                'trigger': trigger,
                'requested_at': requested_at,
                'account_reference': first_row.account_reference,
                'account_label': first_row.account_label,
                'source_market_date': source_market_date,
                'target_trade_date': first_row.target_trade_date.isoformat(),
            },
        }
        return serialize_execution_plan_dispatch_payload(payload), row_links

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
            if _normalize_status(exec_dispatch.get('dispatch_status')) not in {
                EXEC_DISPATCH_STATUS_ACCEPTED,
                EXEC_DISPATCH_STATUS_PARTIAL,
            }:
                matching = False
                break
        return matching

    def _persist_dispatch_failure(
        self,
        *,
        rows: Sequence[SignalDeciderExecutionPlanItem],
        row_links: dict[int, list[dict[str, Any]]],
        plan_id: str,
        fingerprint: str,
        payload: dict[str, Any],
        trigger: str,
        status: str,
        error: dict[str, Any],
    ) -> None:
        now = _utcnow()
        row_ids = [int(row.id) for row in rows]
        with session_scope() as session:
            persisted_rows = session.execute(
                select(SignalDeciderExecutionPlanItem).where(SignalDeciderExecutionPlanItem.id.in_(row_ids))
            ).scalars().all()
            for row in persisted_rows:
                row.execution_status = EXECUTION_STATUS_SUBMIT_FAILED
                existing_submission = row.submission_payload if isinstance(row.submission_payload, dict) else {}
                existing_exec = (
                    existing_submission.get('exec_dispatch')
                    if isinstance(existing_submission.get('exec_dispatch'), dict)
                    else {}
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
                exec_dispatch = {
                    'plan_id': plan_id,
                    'plan_fingerprint': fingerprint,
                    'dispatch_status': status,
                    'last_attempt_at': now.isoformat(),
                    'last_success_at': existing_exec.get('last_success_at'),
                    'last_error': summarize_error(error),
                    'item_links': compact_existing_links,
                    'attempts': _append_limited(existing_exec.get('attempts') or [], attempt),
                    'last_request_summary': compact_exec_request_summary(
                        payload,
                        item_links=row_links.get(row.id, []),
                    ),
                }
                submission = {
                    **existing_submission,
                    'mode': 'exec_dispatch',
                    'latest': attempt,
                    'exec_dispatch': exec_dispatch,
                }
                row.submission_payload = slim_signal_decider_exec_submission_payload(submission).payload
            session.flush()

    def _persist_dispatch_success(
        self,
        *,
        rows: Sequence[SignalDeciderExecutionPlanItem],
        row_links: dict[int, list[dict[str, Any]]],
        plan_id: str,
        fingerprint: str,
        payload: dict[str, Any],
        response_payload: dict[str, Any],
        trigger: str,
    ) -> dict[str, Any]:
        response_items = _collect_items(response_payload)
        response_by_key = {}
        for item in response_items:
            key = (
                _normalize_optional_text(item.get('core_item_key'))
                or _normalize_optional_text(item.get('client_key'))
                or _normalize_optional_text(item.get('external_item_key'))
            )
            if key:
                response_by_key[key] = item

        now = _utcnow()
        row_ids = [int(row.id) for row in rows]
        row_statuses: Counter[str] = Counter()
        accepted_item_count = 0
        rejected_item_count = 0

        with session_scope() as session:
            persisted_rows = session.execute(
                select(SignalDeciderExecutionPlanItem).where(SignalDeciderExecutionPlanItem.id.in_(row_ids))
            ).scalars().all()
            for row in persisted_rows:
                links = []
                row_dispatch_status = EXEC_DISPATCH_STATUS_ACCEPTED
                entry_response_item: dict[str, Any] = {}
                for link in row_links.get(row.id, []):
                    response_item = response_by_key.get(link['core_item_key']) or {}
                    item_status = _normalize_status(
                        response_item.get('status')
                        or response_item.get('plan_item_status')
                        or response_item.get('decision_status')
                    ) or EXEC_DISPATCH_STATUS_ACCEPTED
                    if item_status in REMOTE_FAILED_STATUSES:
                        row_dispatch_status = EXEC_DISPATCH_STATUS_PARTIAL
                        rejected_item_count += 1
                    else:
                        accepted_item_count += 1
                    if link.get('intent') in EXEC_ENTRY_BUY_INTENTS:
                        entry_response_item = response_item
                    links.append(
                        {
                            **link,
                            'exec_plan_id': _normalize_optional_text(response_item.get('execution_plan_id')),
                            'exec_plan_item_id': _normalize_optional_text(
                                response_item.get('execution_plan_item_id')
                                or response_item.get('plan_item_id')
                                or response_item.get('id')
                            ),
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
                    'dispatch_status': row_dispatch_status,
                    'response_summary': response_summary,
                }
                exec_dispatch = {
                    'plan_id': plan_id,
                    'plan_fingerprint': fingerprint,
                    'dispatch_status': row_dispatch_status,
                    'last_attempt_at': now.isoformat(),
                    'last_success_at': now.isoformat(),
                    'last_error': None,
                    'item_links': [
                        compact
                        for item in links
                        if (compact := compact_exec_item_link(item))
                    ],
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
                if row_dispatch_status in {EXEC_DISPATCH_STATUS_ACCEPTED, EXEC_DISPATCH_STATUS_PARTIAL}:
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
                else:
                    row.execution_status = EXECUTION_STATUS_SUBMIT_FAILED
                row.submitted_at = row.submitted_at or now
                submission = {
                    **existing_submission,
                    'mode': 'exec_dispatch',
                    'latest': attempt,
                    'exec_dispatch': exec_dispatch,
                }
                row.submission_payload = slim_signal_decider_exec_submission_payload(submission).payload
                row_statuses[row_dispatch_status] += 1
            session.flush()

        overall_status = (
            EXEC_DISPATCH_STATUS_PARTIAL
            if row_statuses.get(EXEC_DISPATCH_STATUS_PARTIAL)
            else EXEC_DISPATCH_STATUS_ACCEPTED
        )
        return {
            'dispatch_status': overall_status,
            'eligible_count': len(rows),
            'translated_item_count': sum(len(row_links.get(row.id, [])) for row in rows),
            'accepted_item_count': accepted_item_count,
            'rejected_item_count': rejected_item_count,
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
        )
        grouped = self._group_rows_by_plan_id(rows)
        for plan_id, group_rows in grouped.items():
            if not plan_id:
                continue
            symbols = sorted({row.symbol for row in group_rows if row.symbol})
            group_account_reference = _normalize_optional_text(group_rows[0].account_reference) if group_rows else None
            plans_result = await self._fetch_remote_endpoint(
                lambda: self.client.list_execution_plans(external_plan_id=plan_id)
            )
            orders_result = await self._fetch_remote_endpoint(
                lambda: self.client.list_orders(external_plan_id=plan_id, symbols=symbols)
            )
            executions_result = await self._fetch_remote_endpoint(
                lambda: self.client.list_executions(external_plan_id=plan_id, symbols=symbols)
            )
            positions_result = await self._fetch_remote_endpoint(
                lambda: self.client.list_positions(symbols=symbols)
            )
            positions_result = {
                **positions_result,
                'items': self._filter_remote_positions_for_account(
                    positions=positions_result['items'],
                    account_reference=group_account_reference,
                ),
            }
            self._persist_group_readback(
                rows=group_rows,
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

    def _persist_group_readback(
        self,
        *,
        rows: Sequence[SignalDeciderExecutionPlanItem],
        trigger: str,
        runtime_payload: dict[str, Any],
        plans_result: dict[str, Any],
        orders_result: dict[str, Any],
        executions_result: dict[str, Any],
        positions_result: dict[str, Any],
    ) -> None:
        now = _utcnow()
        row_ids = [int(row.id) for row in rows]
        with session_scope() as session:
            persisted_rows = session.execute(
                select(SignalDeciderExecutionPlanItem).where(SignalDeciderExecutionPlanItem.id.in_(row_ids))
            ).scalars().all()
            for row in persisted_rows:
                existing_submission = row.submission_payload if isinstance(row.submission_payload, dict) else {}
                exec_dispatch = (
                    existing_submission.get('exec_dispatch')
                    if isinstance(existing_submission.get('exec_dispatch'), dict)
                    else {}
                )
                item_links = exec_dispatch.get('item_links') or []
                matched_orders = self._match_remote_orders(row=row, item_links=item_links, orders=orders_result['items'])
                matched_executions = self._match_remote_executions(
                    row=row,
                    item_links=item_links,
                    executions=executions_result['items'],
                    orders=matched_orders,
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
                        'orders': orders_result['status'],
                        'executions': executions_result['status'],
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
                        'status': orders_result['status'],
                        'count': len(matched_orders),
                        'items': matched_orders,
                        'error': orders_result['error'],
                    },
                    'executions': {
                        'status': executions_result['status'],
                        'count': len(matched_executions),
                        'items': matched_executions,
                        'error': executions_result['error'],
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
                exec_dispatch = {
                    'plan_id': exec_dispatch.get('plan_id'),
                    'plan_fingerprint': exec_dispatch.get('plan_fingerprint'),
                    'dispatch_status': exec_dispatch.get('dispatch_status'),
                    'last_attempt_at': exec_dispatch.get('last_attempt_at'),
                    'last_success_at': exec_dispatch.get('last_success_at'),
                    'last_error': summarize_error(exec_dispatch.get('last_error')),
                    'item_links': [
                        compact
                        for item in item_links
                        if (compact := compact_exec_item_link(item))
                    ],
                    'attempts': exec_dispatch.get('attempts') or [],
                    'last_request_summary': exec_dispatch.get('last_request_summary'),
                    'last_response_summary': exec_dispatch.get('last_response_summary'),
                    'readback': compact_readback,
                }
                submission = {
                    **existing_submission,
                    'exec_dispatch': exec_dispatch,
                }
                row.submission_payload = slim_signal_decider_exec_submission_payload(submission).payload
            session.flush()

    @staticmethod
    def _match_remote_orders(
        *,
        row: SignalDeciderExecutionPlanItem,
        item_links: Sequence[dict[str, Any]],
        orders: Sequence[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        exec_item_ids = {
            _normalize_optional_text(link.get('exec_plan_item_id'))
            for link in item_links
            if _normalize_optional_text(link.get('exec_plan_item_id'))
        }
        matched: list[dict[str, Any]] = []
        for order in orders:
            order_item_id = (
                _normalize_optional_text(order.get('execution_plan_item_id'))
                or _normalize_optional_text(order.get('plan_item_id'))
                or _normalize_optional_text(_extract_field(order, 'execution_plan_item', 'id'))
            )
            symbol = _normalize_optional_text(order.get('symbol'))
            if exec_item_ids and order_item_id and order_item_id in exec_item_ids:
                matched.append(order)
                continue
            if symbol and symbol == row.symbol:
                matched.append(order)
        return matched

    @staticmethod
    def _match_remote_executions(
        *,
        row: SignalDeciderExecutionPlanItem,
        item_links: Sequence[dict[str, Any]],
        executions: Sequence[dict[str, Any]],
        orders: Sequence[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        order_ids = {
            _normalize_optional_text(order.get('id'))
            or _normalize_optional_text(order.get('broker_order_id'))
            for order in orders
            if _normalize_optional_text(order.get('id')) or _normalize_optional_text(order.get('broker_order_id'))
        }
        matched: list[dict[str, Any]] = []
        for execution in executions:
            symbol = _normalize_optional_text(execution.get('symbol'))
            linked_order_id = (
                _normalize_optional_text(execution.get('broker_order_id'))
                or _normalize_optional_text(execution.get('order_id'))
                or _normalize_optional_text(execution.get('broker_order_pk'))
            )
            if linked_order_id and linked_order_id in order_ids:
                matched.append(execution)
                continue
            if symbol and symbol == row.symbol:
                matched.append(execution)
        return matched

    @staticmethod
    def _extract_remote_account_reference(payload: dict[str, Any]) -> str | None:
        for candidate in (
            payload.get('account_reference'),
            payload.get('accountReference'),
            _extract_field(payload, 'account', 'account_reference'),
            _extract_field(payload, 'account', 'accountReference'),
            _extract_field(payload, 'account', 'reference'),
            _extract_field(payload, 'metadata', 'account_reference'),
            _extract_field(payload, 'metadata', 'accountReference'),
            _extract_field(payload, 'risk_context', 'account_reference'),
            _extract_field(payload, 'riskContext', 'accountReference'),
        ):
            normalized = _normalize_optional_text(candidate)
            if normalized:
                return normalized
        return None

    @classmethod
    def _filter_remote_positions_for_account(
        cls,
        *,
        positions: Sequence[dict[str, Any]],
        account_reference: str | None,
    ) -> list[dict[str, Any]]:
        resolved_account_reference = _normalize_optional_text(account_reference)
        normalized_positions = list(positions)
        if not resolved_account_reference:
            return normalized_positions
        scoped_accounts = [cls._extract_remote_account_reference(item) for item in normalized_positions]
        if not any(account is not None for account in scoped_accounts):
            return normalized_positions
        return [
            item
            for item, position_account_reference in zip(normalized_positions, scoped_accounts)
            if position_account_reference == resolved_account_reference
        ]

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
        elif any(status in REMOTE_ACTIVE_STATUSES for status in order_status_counts):
            derived_status = 'working'
        elif any(status in REMOTE_CANCELLED_STATUSES for status in status_set):
            derived_status = UI_BOARD_STATUS_CANCELLED
        elif any(status in REMOTE_TERMINAL_FAILURE_STATUSES for status in status_set):
            derived_status = 'failed'

        return {
            'derived_status': derived_status,
            'entry_filled_quantity': _decimal_to_text(buy_quantity if buy_quantity > 0 else None),
            'exit_filled_quantity': _decimal_to_text(sell_quantity if sell_quantity > 0 else None),
            'open_quantity': _decimal_to_text(open_quantity if open_quantity > 0 else Decimal('0')),
            'closed_quantity': _decimal_to_text(sell_quantity if sell_quantity > 0 else Decimal('0')),
            'has_plan_attributed_fill_evidence': has_plan_fill_evidence,
            'order_status_counts': dict(sorted(order_status_counts.items())),
            'endpoint_statuses': endpoint_statuses,
        }

    def _augment_exec_overview_item(self, item: dict[str, Any]) -> dict[str, Any]:
        submission = item.get('submission') if isinstance(item.get('submission'), dict) else {}
        exec_dispatch = submission.get('exec_dispatch') if isinstance(submission.get('exec_dispatch'), dict) else {}
        readback = exec_dispatch.get('readback') if isinstance(exec_dispatch.get('readback'), dict) else {}
        traceability = item.get('traceability') if isinstance(item.get('traceability'), dict) else {}
        decision_agent_name = _normalize_decision_agent_name(
            item.get('decision_agent_name') or traceability.get('decision_agent_name') or traceability.get('agent_name')
        )
        dispatch_status = _normalize_status(exec_dispatch.get('dispatch_status'))
        derived_state = readback.get('derived_state') if isinstance(readback.get('derived_state'), dict) else {}
        raw_action = traceability.get('raw_action') or traceability.get('tactical_action')
        generic_action = traceability.get('generic_action')
        if generic_action is None and raw_action in {'scalp_buy_candidate', 'watch_for_trigger'}:
            generic_action = 'buy'
        handoff_intent = traceability.get('handoff_intent')
        if handoff_intent is None and decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME:
            if raw_action == 'scalp_buy_candidate':
                handoff_intent = 'tactical_buy'
            elif raw_action == 'watch_for_trigger':
                handoff_intent = EXEC_INTENT_INTRADAY_WATCH_BUY
        handoff_active = bool(traceability.get('handoff_active')) or bool(
            decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME
            and item.get('eligibility_status') == ELIGIBILITY_ELIGIBLE
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
                payload.get('monitoring_alive'),
                _extract_field(payload, 'health', 'monitoring_alive'),
            )
        )
        if monitoring_alive is None:
            monitoring_alive = bool(ready and runtime.get('status') == REMOTE_FETCH_STATUS_OK)

        last_heartbeat_at = _coalesce_text(
            monitoring.get('last_heartbeat_at'),
            monitoring.get('last_seen_at'),
            payload.get('last_heartbeat_at'),
            payload.get('monitoring_last_heartbeat_at'),
            _extract_field(payload, 'health', 'last_heartbeat_at'),
        )

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
            'issues': issues,
            'core_integration_mode': integration.get('mode'),
            'delegate_execution': bool(integration.get('delegate_execution')),
        })

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
            if statuses & EXEC_AGENT_ACCEPTED_ENTRY_STATUSES:
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
        exec_runtime_showable = any(bool(item.get('exec_runtime_showable')) for item in items)
        exec_monitoring_active = any(bool(item.get('exec_monitoring_active')) for item in items)
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
            'exec_agent_approved': any(bool(item.get('exec_agent_approved')) for item in items),
            'exec_agent_action': first_value('exec_agent_action'),
            'exec_agent_reason_code': first_value('exec_agent_reason_code'),
            'exec_agent_target_profile': first_value('exec_agent_target_profile'),
            'exec_agent_target_tag': first_value('exec_agent_target_tag'),
            'exec_agent_special_tactical_scalp': first_value('exec_agent_special_tactical_scalp'),
            'held_position': position_quantity > Decimal('0') or any(bool(item.get('held_position')) for item in items),
            'position_quantity': _to_json_number(position_quantity),
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
    def _observe_runtime_symbols_for_realtime_prices(symbols: Iterable[Any], *, trigger: str) -> None:
        try:
            kis_realtime_price_service.observe_monitored_symbols(
                symbols,
                trigger=trigger,
                preserve_order=True,
            )
        except Exception:
            pass

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
