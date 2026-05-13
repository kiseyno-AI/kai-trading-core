from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
from typing import Any, Iterable, Sequence

import httpx
from sqlalchemy import select

from app.agent_context.roles import SHORT_TERM_DECIDER_AGENT_NAME, SIGNAL_DECIDER_AGENT_NAME
from app.core.audit_logging import request_with_audit
from app.core.settings import settings
from app.db import session_scope
from app.db_models import SignalDeciderDecisionSnapshot, SignalDeciderExecutionPlanItem
from app.market.clock import get_market_state
from app.orders.next_open_execution import (
    ELIGIBILITY_BLOCKED,
    ELIGIBILITY_ELIGIBLE,
    EXECUTION_STATUS_PLANNED,
    EXECUTION_STATUS_SUBMIT_FAILED,
    EXECUTION_STATUS_SUBMITTED,
    EXECUTION_WINDOW_NEXT_OPEN,
    signal_decider_next_open_execution_service,
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
EXEC_INTENT_TARGET_SELL = 'target_sell'
EXEC_INTENT_STOP_SELL = 'stop_sell'
EXEC_SOURCE_SCHEMA_VERSION = 'signal_decider_exec_plan_v2'

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


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None or value == '':
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
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
        resolved_target_date = _normalize_date(target_trade_date)
        resolved_account_reference = _normalize_optional_text(account_reference)
        resolved_dry_run = settings.signal_decider_next_open_dry_run if dry_run is None else dry_run

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

        if refresh_plan:
            signal_decider_next_open_execution_service.refresh_plan_items(
                target_trade_date=resolved_target_date,
                trigger=f'{trigger}_plan_refresh',
            )

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
            eligible_rows = [row for row in account_rows if row.eligibility_status == ELIGIBILITY_ELIGIBLE]
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
                status = self._client_error_status(exc)
                failed_group_count += 1
                self._persist_dispatch_failure(
                    rows=eligible_rows,
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

        plan_view = signal_decider_next_open_execution_service.list_plan_items(
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
            'items': plan_view.get('items') or [],
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

        plan_view = signal_decider_next_open_execution_service.list_plan_items(
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
            if _normalize_decision_agent_name(item.get('decision_agent_name')) == SIGNAL_DECIDER_AGENT_NAME
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
        overview = await self._load_dashboard_overview(
            target_trade_date=resolved_market_date,
            account_reference=None,
            symbol=None,
            refresh=refresh,
            trigger=trigger,
        )
        symbol_metadata_map = self._load_symbol_metadata_map(item.get('symbol') for item in overview['items'])
        runtime = self._build_dashboard_runtime_status(
            integration=overview['integration'],
            runtime=overview['runtime'],
        )
        items = [
            self._build_runtime_board_item(
                item=item,
                symbol_metadata_map=symbol_metadata_map,
                runtime_status=runtime['status'],
            )
            for item in overview['items']
        ]
        return {
            'request': {
                'market_date': resolved_market_date.isoformat(),
                'refresh': refresh,
            },
            'runtime': runtime,
            'market_date': resolved_market_date.isoformat(),
            'summary': self._build_runtime_daily_board_summary(items=items),
            'items': items,
        }

    async def list_runtime_monitored_symbols(
        self,
        *,
        market_date: str | date | None = None,
        refresh: bool = True,
        trigger: str = 'manual',
    ) -> dict[str, Any]:
        board = await self.get_runtime_daily_board(
            market_date=market_date,
            refresh=refresh,
            trigger=trigger,
        )
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in board['items']:
            symbol = _normalize_optional_text(item.get('symbol'))
            if symbol:
                grouped[symbol].append(item)

        items = [
            self._build_runtime_monitored_symbol_item(
                symbol=symbol,
                items=grouped_items,
                runtime_status=board['runtime']['status'],
            )
            for symbol, grouped_items in sorted(grouped.items())
            if self._should_include_monitored_symbol_group(grouped_items)
        ]
        return {
            'request': {
                'market_date': board['market_date'],
                'refresh': refresh,
            },
            'runtime': board['runtime'],
            'market_date': board['market_date'],
            'summary': self._build_runtime_monitored_summary(items=items),
            'items': items,
        }

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
        overview = await self._load_dashboard_overview(
            target_trade_date=resolved_market_date,
            account_reference=None,
            symbol=resolved_symbol,
            refresh=refresh,
            trigger=trigger,
        )
        symbol_metadata_map = self._load_symbol_metadata_map([resolved_symbol])
        runtime = self._build_dashboard_runtime_status(
            integration=overview['integration'],
            runtime=overview['runtime'],
        )
        items = [
            self._build_runtime_symbol_detail_item(
                item=item,
                symbol_metadata_map=symbol_metadata_map,
                runtime_status=runtime['status'],
            )
            for item in overview['items']
        ]
        metadata = symbol_metadata_map.get(
            resolved_symbol,
            {
                'name': resolved_symbol,
                'market': None,
            },
        )
        return {
            'request': {
                'market_date': resolved_market_date.isoformat(),
                'symbol': resolved_symbol,
                'refresh': refresh,
            },
            'runtime': runtime,
            'market_date': resolved_market_date.isoformat(),
            'symbol': resolved_symbol,
            'name': metadata['name'],
            'market': metadata['market'],
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
            'items': items,
        }

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
            quantity = _safe_decimal(row.planned_quantity)
            target_price = snapshot.target_price if snapshot is not None else traceability.get('target_price')
            stop_loss_price = snapshot.stop_loss_price if snapshot is not None else traceability.get('stop_loss_price')
            trigger_price = traceability.get('trigger_price')
            chase_limit_price = traceability.get('chase_limit_price')
            entry_execution_mode = (
                'pending_trigger_limit'
                if row_decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME
                else 'next_open_market'
            )
            core_entry_key = f'core-plan-{row.id}:{EXEC_INTENT_NEXT_OPEN_BUY}'
            common_risk_context = {
                'schema_version': EXEC_SOURCE_SCHEMA_VERSION,
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
                'tactical_action': traceability.get('tactical_action'),
                'trigger_price': trigger_price,
                'chase_limit_price': chase_limit_price,
                'allocation_mode': (
                    _normalize_optional_text(traceability.get('allocation_mode'))
                    or (
                        'unspecified_route'
                        if _normalize_optional_text(traceability.get('account_reference')) == 'KIS:00000000-00'
                        else 'specified'
                    )
                ),
                'entry_execution_mode': entry_execution_mode,
                'validation': validation,
                'traceability': decision_traceability,
            }
            if not is_short_term:
                quantity_policy = {
                    'type': 'fixed_quantity',
                    'quantity': _decimal_to_text(quantity),
                    'budget_krw': _decimal_to_text(_safe_decimal(row.allocation_budget_krw)),
                    'estimated_reference_price': row.estimated_reference_price,
                    'estimated_notional_krw': _decimal_to_text(_safe_decimal(row.estimated_notional_krw)),
                }
            else:
                quantity_policy = {
                    'type': 'exec_agent_sized',
                    'budget_krw': _decimal_to_text(_safe_decimal(row.allocation_budget_krw)),
                    'allocation_mode': traceability.get('allocation_mode') or 'specified',
                    'account_reference': row.account_reference,
                    'account_label': row.account_label,
                    'estimated_reference_price': row.estimated_reference_price,
                    'estimated_notional_krw': _decimal_to_text(_safe_decimal(row.estimated_notional_krw)),
                    'pricing_note': (validation.get('pricing_note') if isinstance(validation.get('pricing_note'), str) else None),
                }
            entry_item = {
                'core_item_key': core_entry_key,
                'agent_name': row_decision_agent_name,
                'decision_agent_name': row_decision_agent_name,
                'symbol': row.symbol,
                'side': 'buy',
                'intent': EXEC_INTENT_NEXT_OPEN_BUY,
                'quantity_policy': quantity_policy,
                'limit_price': (
                    _to_json_number(_safe_decimal(chase_limit_price))
                    if row_decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME
                    else None
                ),
                'trigger_price': (
                    _to_json_number(_safe_decimal(trigger_price))
                    if row_decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME
                    else None
                ),
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
                    'intent': EXEC_INTENT_NEXT_OPEN_BUY,
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
                items.append(
                    {
                        'core_item_key': target_key,
                        'agent_name': row_decision_agent_name,
                        'decision_agent_name': row_decision_agent_name,
                        'symbol': row.symbol,
                        'side': 'sell',
                        'intent': EXEC_INTENT_TARGET_SELL,
                        'quantity_policy': {
                            'type': 'position_exit_from_entry',
                            'linked_entry_item_key': core_entry_key,
                            'quantity': _decimal_to_text(quantity),
                        },
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
                items.append(
                    {
                        'core_item_key': stop_key,
                        'agent_name': row_decision_agent_name,
                        'decision_agent_name': row_decision_agent_name,
                        'symbol': row.symbol,
                        'side': 'sell',
                        'intent': EXEC_INTENT_STOP_SELL,
                        'quantity_policy': {
                            'type': 'position_exit_from_entry',
                            'linked_entry_item_key': core_entry_key,
                            'quantity': _decimal_to_text(quantity),
                        },
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
        return payload, row_links

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
                    'error': error,
                }
                exec_dispatch = {
                    **existing_exec,
                    'plan_id': plan_id,
                    'plan_fingerprint': fingerprint,
                    'dispatch_status': status,
                    'last_attempt_at': now.isoformat(),
                    'last_error': error,
                    'attempts': _append_limited(existing_exec.get('attempts') or [], attempt),
                    'last_request_payload': _json_safe(payload),
                }
                row.submission_payload = {
                    **existing_submission,
                    'mode': 'exec_dispatch',
                    'latest': attempt,
                    'exec_dispatch': exec_dispatch,
                }
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
                            'response_item': response_item if response_item else None,
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
                    **existing_exec,
                    'plan_id': plan_id,
                    'plan_fingerprint': fingerprint,
                    'dispatch_status': row_dispatch_status,
                    'last_attempt_at': now.isoformat(),
                    'last_success_at': now.isoformat(),
                    'last_error': None,
                    'item_links': links,
                    'attempts': _append_limited(existing_exec.get('attempts') or [], attempt),
                    'last_request_payload': _json_safe(payload),
                    'last_response_payload': _json_safe(response_payload),
                }
                row.execution_status = (
                    EXECUTION_STATUS_SUBMITTED
                    if row_dispatch_status in {EXEC_DISPATCH_STATUS_ACCEPTED, EXEC_DISPATCH_STATUS_PARTIAL}
                    else EXECUTION_STATUS_SUBMIT_FAILED
                )
                row.submitted_at = row.submitted_at or now
                row.submission_payload = {
                    **existing_submission,
                    'mode': 'exec_dispatch',
                    'latest': attempt,
                    'exec_dispatch': exec_dispatch,
                }
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
                exec_dispatch = {
                    **exec_dispatch,
                    'readback': _json_safe(readback),
                }
                row.submission_payload = {
                    **existing_submission,
                    'exec_dispatch': exec_dispatch,
                }
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
        derived_status = EXEC_DISPATCH_STATUS_ACCEPTED
        if sell_quantity > 0 and (position_quantity <= 0 or (buy_quantity > 0 and sell_quantity >= buy_quantity)):
            derived_status = 'closed'
        elif position_quantity > 0 or buy_quantity > 0:
            derived_status = 'open'
        elif any(status in REMOTE_ACTIVE_STATUSES for status in order_status_counts):
            derived_status = 'working'
        elif any(status in REMOTE_CANCELLED_STATUSES for status in status_set):
            derived_status = UI_BOARD_STATUS_CANCELLED
        elif any(status in REMOTE_TERMINAL_FAILURE_STATUSES for status in status_set):
            derived_status = 'failed'

        open_quantity = max(position_quantity, buy_quantity - sell_quantity)
        return {
            'derived_status': derived_status,
            'entry_filled_quantity': _decimal_to_text(buy_quantity if buy_quantity > 0 else None),
            'exit_filled_quantity': _decimal_to_text(sell_quantity if sell_quantity > 0 else None),
            'open_quantity': _decimal_to_text(open_quantity if open_quantity > 0 else Decimal('0')),
            'closed_quantity': _decimal_to_text(sell_quantity if sell_quantity > 0 else Decimal('0')),
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
        return {
            **item,
            'agent_name': decision_agent_name,
            'decision_agent_name': decision_agent_name,
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
            'dispatch': {
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
            },
            'remote_state': {
                'last_synced_at': readback.get('last_synced_at'),
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
            'eligibility_status': item.get('eligibility_status'),
            'execution_status': item.get('execution_status'),
            'target_price': item.get('target_price'),
            'stop_loss_price': item.get('stop_loss_price'),
            'client_order_id': item.get('client_order_id'),
            'dispatch': item.get('dispatch'),
            'remote_state': item.get('remote_state'),
            'traceability': item.get('traceability'),
            'validation': item.get('validation'),
        }

    async def _load_dashboard_overview(
        self,
        *,
        target_trade_date: date,
        account_reference: str | None,
        symbol: str | None,
        refresh: bool,
        trigger: str,
    ) -> dict[str, Any]:
        overview = await self.list_signal_decider_exec_overview(
            target_trade_date=target_trade_date,
            account_reference=account_reference,
            symbol=symbol,
            refresh=refresh,
            trigger=trigger,
        )
        if refresh:
            return overview
        live_runtime = await self.runtime_status(refresh=True)
        return {
            **overview,
            'integration': live_runtime['integration'],
            'runtime': live_runtime['runtime'],
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

        return {
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
        }

    def _build_runtime_board_item(
        self,
        *,
        item: dict[str, Any],
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
        if entry_filled_quantity is None:
            entry_filled_quantity = self._sum_execution_quantity(executions=executions, side='buy')
        exit_filled_quantity = _safe_decimal(remote_state.get('exit_filled_quantity'))
        if exit_filled_quantity is None:
            exit_filled_quantity = self._sum_execution_quantity(executions=executions, side='sell')
        open_quantity = _safe_decimal(remote_state.get('open_quantity'))
        if open_quantity is None:
            open_quantity = max(Decimal('0'), entry_filled_quantity - exit_filled_quantity)

        order_statuses = self._collect_order_statuses(orders=orders)
        status = self._normalize_board_item_status(
            item=item,
            order_statuses=order_statuses,
            planned_quantity=planned_quantity,
            entry_filled_quantity=entry_filled_quantity,
            exit_filled_quantity=exit_filled_quantity,
            open_quantity=open_quantity,
        )
        planned_intents = self._collect_planned_intents(item=item)
        monitoring_state = self._resolve_monitoring_state(
            status=status,
            open_quantity=open_quantity,
            planned_intents=planned_intents,
            runtime_status=runtime_status,
            has_execution_plan=bool(_normalize_optional_text((item.get('dispatch') or {}).get('plan_id'))),
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
            'intent': EXEC_INTENT_NEXT_OPEN_BUY,
            'status': status,
            'monitoring_state': monitoring_state,
            'position_side': 'long' if open_quantity > Decimal('0') or planned_quantity is not None else None,
            'planned_intents': planned_intents,
            'planned_quantity': _to_json_number(planned_quantity),
            'filled_quantity': _to_json_number(entry_filled_quantity),
            'open_quantity': _to_json_number(open_quantity),
            'planned_price': item.get('estimated_reference_price'),
            'avg_fill_price': self._resolve_average_fill_price(executions=executions, positions=positions),
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
    ) -> dict[str, Any]:
        first = items[0]
        monitoring = self._build_runtime_symbol_monitoring(items=items, runtime_status=runtime_status)
        return {
            'symbol': symbol,
            'name': first.get('name') or symbol,
            'market': first.get('market'),
            'monitoring_state': monitoring['state'],
            'position_side': monitoring['position_side'],
            'planned_intents': monitoring['planned_intents'],
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
        return False

    def _build_runtime_monitored_summary(self, *, items: Sequence[dict[str, Any]]) -> dict[str, Any]:
        state_counts = Counter(
            _normalize_optional_text(item.get('monitoring_state'), lowercase=True) or 'unknown'
            for item in items
        )
        return {
            'items_total': len(items),
            'watching_target_stop': state_counts.get(UI_MONITORING_STATE_WATCHING_TARGET_STOP, 0),
            'watching_fill': state_counts.get(UI_MONITORING_STATE_WATCHING_FILL, 0),
            'reconciling': state_counts.get(UI_MONITORING_STATE_RECONCILING, 0),
            'idle': state_counts.get(UI_MONITORING_STATE_IDLE, 0),
            'monitoring_state_counts': dict(sorted(state_counts.items())),
        }

    def _build_runtime_symbol_detail_item(
        self,
        *,
        item: dict[str, Any],
        symbol_metadata_map: dict[str, dict[str, Any]],
        runtime_status: str,
    ) -> dict[str, Any]:
        board_item = self._build_runtime_board_item(
            item=item,
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
    def _collect_order_statuses(*, orders: Sequence[dict[str, Any]]) -> set[str]:
        statuses: set[str] = set()
        for order in orders:
            status = _normalize_status(order.get('status') or order.get('current_status') or order.get('broker_status'))
            if status:
                statuses.add(status)
        return statuses

    def _normalize_board_item_status(
        self,
        *,
        item: dict[str, Any],
        order_statuses: set[str],
        planned_quantity: Decimal | None,
        entry_filled_quantity: Decimal,
        exit_filled_quantity: Decimal,
        open_quantity: Decimal,
    ) -> str:
        eligibility_status = _normalize_optional_text(item.get('eligibility_status'), lowercase=True)
        execution_status = _normalize_optional_text(item.get('execution_status'), lowercase=True)
        dispatch_status = _normalize_optional_text((item.get('dispatch') or {}).get('status'), lowercase=True)
        remote_state = item.get('remote_state') if isinstance(item.get('remote_state'), dict) else {}
        derived_status = _normalize_optional_text(remote_state.get('derived_status'), lowercase=True)
        failed_order_statuses = order_statuses & REMOTE_TERMINAL_FAILURE_STATUSES

        if eligibility_status == ELIGIBILITY_BLOCKED:
            return UI_BOARD_STATUS_FAILED
        if execution_status == EXECUTION_STATUS_SUBMIT_FAILED or dispatch_status in {
            EXEC_DISPATCH_STATUS_FAILED,
            EXEC_DISPATCH_STATUS_NOT_CONFIGURED,
            EXEC_DISPATCH_STATUS_CONTRACT_ERROR,
            EXEC_DISPATCH_STATUS_UNAVAILABLE,
        }:
            return UI_BOARD_STATUS_FAILED
        if (order_statuses & REMOTE_CANCELLED_STATUSES or derived_status == UI_BOARD_STATUS_CANCELLED) and (
            entry_filled_quantity <= Decimal('0')
        ):
            return UI_BOARD_STATUS_CANCELLED
        if failed_order_statuses or derived_status == 'failed':
            return UI_BOARD_STATUS_FAILED
        if order_statuses & REMOTE_PARTIAL_STATUSES:
            return UI_BOARD_STATUS_PARTIAL
        if planned_quantity is not None and entry_filled_quantity > Decimal('0') and entry_filled_quantity < planned_quantity:
            return UI_BOARD_STATUS_PARTIAL
        if exit_filled_quantity > Decimal('0') and open_quantity > Decimal('0'):
            return UI_BOARD_STATUS_PARTIAL
        if entry_filled_quantity > Decimal('0'):
            return UI_BOARD_STATUS_FILLED
        if order_statuses & REMOTE_SUBMITTED_STATUSES:
            return UI_BOARD_STATUS_SUBMITTED
        if order_statuses & REMOTE_SCHEDULED_STATUSES:
            return UI_BOARD_STATUS_SCHEDULED
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
    ) -> str:
        if runtime_status in {UI_RUNTIME_STATUS_DEGRADED, UI_RUNTIME_STATUS_OFFLINE} and has_execution_plan:
            return UI_MONITORING_STATE_RECONCILING
        if open_quantity > Decimal('0') and planned_intents:
            return UI_MONITORING_STATE_WATCHING_TARGET_STOP
        if status in {
            UI_BOARD_STATUS_ACCEPTED,
            UI_BOARD_STATUS_SCHEDULED,
            UI_BOARD_STATUS_SUBMITTED,
            UI_BOARD_STATUS_PARTIAL,
        }:
            return UI_MONITORING_STATE_WATCHING_FILL
        return UI_MONITORING_STATE_IDLE

    @staticmethod
    def _collect_planned_intents(*, item: dict[str, Any]) -> list[str]:
        dispatch = item.get('dispatch') if isinstance(item.get('dispatch'), dict) else {}
        intents = []
        for link in dispatch.get('item_links') or []:
            intent = _normalize_optional_text(link.get('intent'), lowercase=True)
            if intent and intent != EXEC_INTENT_NEXT_OPEN_BUY:
                intents.append(intent)
        return sorted(dict.fromkeys(intents))

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
