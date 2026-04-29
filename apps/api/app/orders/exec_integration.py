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
EXEC_SOURCE_SCHEMA_VERSION = 'signal_decider_exec_plan_v1'

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
        grouped = self._group_rows_by_account(rows)

        group_results: list[dict[str, Any]] = []
        translated_item_count = 0
        dispatched_group_count = 0
        failed_group_count = 0
        skipped_group_count = 0
        preview_plans: list[dict[str, Any]] = []

        for account_key, account_rows in grouped.items():
            eligible_rows = [row for row in account_rows if row.eligibility_status == ELIGIBILITY_ELIGIBLE]
            if not eligible_rows:
                skipped_group_count += 1
                group_results.append(
                    {
                        'account_reference': account_key,
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
                        'plan_id': plan_id,
                        'translated_item_count': len(plan_payload['items']),
                        'payload': plan_payload,
                    }
                )
                group_results.append(
                    {
                        'account_reference': account_key,
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
        items = [self._build_intraday_watch_item(item) for item in overview['items']]
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
    def _group_rows_by_account(
        rows: Sequence[SignalDeciderExecutionPlanItem],
    ) -> dict[str, list[SignalDeciderExecutionPlanItem]]:
        grouped: dict[str, list[SignalDeciderExecutionPlanItem]] = defaultdict(list)
        for row in rows:
            grouped[row.account_reference].append(row)
        return grouped

    def _build_exec_plan_payload(
        self,
        *,
        rows: Sequence[SignalDeciderExecutionPlanItem],
        snapshots: dict[int, SignalDeciderDecisionSnapshot],
        trigger: str,
    ) -> tuple[dict[str, Any], dict[int, list[dict[str, Any]]]]:
        first_row = rows[0]
        source_market_date = first_row.source_market_date.isoformat()
        plan_id = (
            f'core-signal-decider-{first_row.target_trade_date.strftime("%Y%m%d")}-'
            f'{_sanitize_key(first_row.account_reference)}'
        )
        runtime_mode = _normalize_optional_text(settings.signal_decider_exec_runtime_mode, lowercase=True) or 'mock'
        dispatch_source = _normalize_optional_text(settings.signal_decider_exec_dispatch_source) or 'kai-trading-core'
        items: list[dict[str, Any]] = []
        row_links: dict[int, list[dict[str, Any]]] = defaultdict(list)
        requested_at = _utcnow().isoformat()

        for row in rows:
            snapshot = snapshots.get(int(row.decision_snapshot_id))
            traceability = row.traceability_payload if isinstance(row.traceability_payload, dict) else {}
            validation = row.validation_payload if isinstance(row.validation_payload, dict) else {}
            quantity = _safe_decimal(row.planned_quantity)
            target_price = snapshot.target_price if snapshot is not None else traceability.get('target_price')
            stop_loss_price = snapshot.stop_loss_price if snapshot is not None else traceability.get('stop_loss_price')
            core_entry_key = f'core-plan-{row.id}:{EXEC_INTENT_NEXT_OPEN_BUY}'
            common_risk_context = {
                'schema_version': EXEC_SOURCE_SCHEMA_VERSION,
                'core_plan_item_id': row.id,
                'core_client_order_id': row.client_order_id,
                'core_execution_window': row.execution_window,
                'account_reference': row.account_reference,
                'account_label': row.account_label,
                'target_trade_date': row.target_trade_date.isoformat(),
                'source_market_date': source_market_date,
                'policy_version': row.policy_version,
                'decision_snapshot_id': row.decision_snapshot_id,
                'decision_run_id': traceability.get('decision_run_id'),
                'target_price': target_price,
                'stop_loss_price': stop_loss_price,
                'suggested_holding_period': traceability.get('suggested_holding_period'),
                'validation': validation,
                'traceability': traceability.get('signal_traceability') or {},
            }
            entry_item = {
                'core_item_key': core_entry_key,
                'symbol': row.symbol,
                'side': 'buy',
                'intent': EXEC_INTENT_NEXT_OPEN_BUY,
                'quantity_policy': {
                    'type': 'fixed_quantity',
                    'quantity': _decimal_to_text(quantity),
                    'budget_krw': _decimal_to_text(_safe_decimal(row.allocation_budget_krw)),
                    'estimated_reference_price': row.estimated_reference_price,
                    'estimated_notional_krw': _decimal_to_text(_safe_decimal(row.estimated_notional_krw)),
                },
                'limit_price': None,
                'trigger_price': None,
                'time_in_force': row.planned_time_in_force,
                'risk_context': {
                    **common_risk_context,
                    'entry_price_basis': snapshot.entry_price_basis if snapshot is not None else 'next_open',
                    'expected_return_pct': _to_json_number(_safe_decimal(snapshot.expected_return_pct if snapshot else None)),
                    'confidence_score_10': _to_json_number(_safe_decimal(snapshot.confidence_score_10 if snapshot else None)),
                    'conviction_score_10': _to_json_number(_safe_decimal(snapshot.conviction_score_10 if snapshot else None)),
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
                    'trigger_price': None,
                }
            )

            if target_price is not None:
                target_key = f'core-plan-{row.id}:{EXEC_INTENT_TARGET_SELL}'
                items.append(
                    {
                        'core_item_key': target_key,
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
            'strategy_run_id': f'signal-decider:{first_row.target_trade_date.isoformat()}:{_sanitize_key(first_row.account_reference)}',
            'requested_at': requested_at,
            'items': items,
            'metadata': {
                'schema_version': EXEC_SOURCE_SCHEMA_VERSION,
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
        derived_status = EXEC_DISPATCH_STATUS_ACCEPTED
        if sell_quantity > 0 and (position_quantity <= 0 or (buy_quantity > 0 and sell_quantity >= buy_quantity)):
            derived_status = 'closed'
        elif position_quantity > 0 or buy_quantity > 0:
            derived_status = 'open'
        elif any(status in REMOTE_ACTIVE_STATUSES for status in order_status_counts):
            derived_status = 'working'
        elif any(status in REMOTE_FAILED_STATUSES for status in item_statuses.union(set(order_status_counts.keys()))):
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
        dispatch_status = _normalize_status(exec_dispatch.get('dispatch_status'))
        derived_state = readback.get('derived_state') if isinstance(readback.get('derived_state'), dict) else {}
        return {
            **item,
            'target_price': traceability.get('target_price'),
            'stop_loss_price': traceability.get('stop_loss_price'),
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

    @staticmethod
    def _client_error_status(exc: SignalDeciderExecClientError) -> str:
        if exc.code in {EXEC_DISPATCH_STATUS_NOT_CONFIGURED, EXEC_DISPATCH_STATUS_CONTRACT_ERROR}:
            return exc.code
        return REMOTE_FETCH_STATUS_UNAVAILABLE


signal_decider_exec_integration_service = SignalDeciderExecIntegrationService()
