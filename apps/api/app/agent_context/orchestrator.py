from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Sequence

import httpx

from app.agent_context.payload_builder import agent_payload_builder
from app.agent_context.read_service import agent_context_read_service
from app.agent_context.response_contract import build_contract_prompt_suffix, normalize_agent_response
from app.core.settings import settings
from app.db import session_scope
from app.db_models import AgentContextFeedback, EODCollectionRun

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AgentDispatchSpec:
    role: str
    agent_name: str
    instruction: str


DEFAULT_AGENT_ROLES: tuple[AgentDispatchSpec, ...] = (
    AgentDispatchSpec(
        role='eod-summarizer',
        agent_name='eod-summarizer',
        instruction='Summarize the EOD market session, breadth, leaders, laggards, and notable anomalies conservatively.',
    ),
    AgentDispatchSpec(
        role='technical-summarizer',
        agent_name='technical-summarizer',
        instruction='Review the EOD technical state, regime, breakouts, reversals, and technical risk flags conservatively.',
    ),
    AgentDispatchSpec(
        role='fundamental-summarizer',
        agent_name='fundamental-summarizer',
        instruction='Review DART and fundamentals context for the session, focusing on material disclosures, freshness, and caveats.',
    ),
)


class AgentOrchestratorError(RuntimeError):
    pass


class AgentDispatchConfigError(AgentOrchestratorError):
    pass


class AgentDispatchRetryableError(AgentOrchestratorError):
    pass


class AgentDispatchTerminalError(AgentOrchestratorError):
    pass


class OpenClawAgentClient:
    def __init__(
        self,
        *,
        base_url: str,
        gateway_token: str,
        timeout_seconds: float,
        max_retries: int,
        retry_backoff_seconds: float,
    ) -> None:
        self.base_url = base_url.rstrip('/')
        self.gateway_token = gateway_token
        self.timeout_seconds = timeout_seconds
        self.max_retries = max(0, max_retries)
        self.retry_backoff_seconds = max(0.0, retry_backoff_seconds)

    def _headers(self) -> dict[str, str]:
        return {
            'Authorization': f'Bearer {self.gateway_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }

    def _build_message(self, *, spec: AgentDispatchSpec, payload: dict[str, Any]) -> str:
        return (
            f"Role: {spec.role}\n"
            f"Instruction: {spec.instruction}\n"
            f"{build_contract_prompt_suffix()}\n"
            "If data quality is degraded, say so explicitly in risk_flags/details.\n\n"
            f"Context:\n{payload}"
        )

    def dispatch(self, *, spec: AgentDispatchSpec, payload: dict[str, Any]) -> dict[str, Any]:
        url = f'{self.base_url}/v1/responses'
        body = {
            'model': f'openclaw/{spec.agent_name}',
            'input': self._build_message(spec=spec, payload=payload),
        }
        attempt = 0
        last_error: Exception | None = None
        while attempt <= self.max_retries:
            attempt += 1
            started_at = datetime.now(timezone.utc)
            try:
                with httpx.Client(timeout=self.timeout_seconds, headers=self._headers()) as client:
                    response = client.post(url, json=body)
                if response.status_code >= 500:
                    raise AgentDispatchRetryableError(f'OpenClaw server error {response.status_code}: {response.text[:300]}')
                if response.status_code in (408, 409, 429):
                    raise AgentDispatchRetryableError(f'OpenClaw retryable error {response.status_code}: {response.text[:300]}')
                if response.status_code >= 400:
                    raise AgentDispatchTerminalError(f'OpenClaw dispatch failed {response.status_code}: {response.text[:300]}')
                parsed = response.json()
                output_text = None
                for item in parsed.get('output', []):
                    if item.get('type') != 'message':
                        continue
                    for content in item.get('content', []):
                        if content.get('type') == 'output_text':
                            output_text = content.get('text')
                            break
                    if output_text:
                        break
                return {
                    'ok': True,
                    'attempt': attempt,
                    'started_at': started_at.isoformat(),
                    'completed_at': datetime.now(timezone.utc).isoformat(),
                    'response': parsed,
                    'output_text': output_text,
                    'normalized_feedback': normalize_agent_response(
                        role=spec.role,
                        output_text=output_text,
                        raw_response=parsed,
                    ),
                }
            except AgentDispatchTerminalError:
                raise
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt > self.max_retries:
                    break
                if self.retry_backoff_seconds > 0:
                    time.sleep(self.retry_backoff_seconds * attempt)
        raise AgentDispatchRetryableError(str(last_error) if last_error else 'unknown dispatch failure')


class AgentFeedbackOrchestrator:
    def __init__(self) -> None:
        self.client: OpenClawAgentClient | None = None

    def _validate_config(self) -> None:
        if not settings.openclaw_base_url:
            raise AgentDispatchConfigError('OPENCLAW_BASE_URL is required')
        if not settings.openclaw_gateway_token:
            raise AgentDispatchConfigError('OPENCLAW_GATEWAY_TOKEN is required')

    def _get_client(self) -> OpenClawAgentClient:
        self._validate_config()
        if self.client is None:
            self.client = OpenClawAgentClient(
                base_url=str(settings.openclaw_base_url),
                gateway_token=str(settings.openclaw_gateway_token),
                timeout_seconds=settings.openclaw_agent_timeout_seconds,
                max_retries=settings.openclaw_agent_max_retries,
                retry_backoff_seconds=settings.openclaw_agent_retry_backoff_seconds,
            )
        return self.client

    def _get_run(self, *, eod_run_id: int) -> EODCollectionRun:
        with session_scope() as session:
            run = session.get(EODCollectionRun, eod_run_id)
            if run is None:
                raise ValueError(f'eod_run_id {eod_run_id} not found')
            session.expunge(run)
            return run

    def _build_payloads(self, *, run: EODCollectionRun) -> dict[str, dict[str, Any]]:
        payloads = agent_payload_builder.build_payloads(run=run)
        request_id = str(uuid.uuid4())
        return {role: payload | {'request_id': request_id} for role, payload in payloads.items()}

    def _store_feedback(
        self,
        *,
        market_date: date,
        agent_name: str,
        run_id: str,
        payload: dict[str, Any],
        response: dict[str, Any],
        context: dict[str, Any],
    ) -> dict[str, Any]:
        with session_scope() as session:
            record = AgentContextFeedback(
                market_date=market_date,
                symbol='__MARKET__',
                agent_name=agent_name,
                agent_version=None,
                run_id=run_id,
                summary_payload={
                    'role': payload.get('role'),
                    'dispatch': {
                        'attempt': response.get('attempt'),
                        'started_at': response.get('started_at'),
                        'completed_at': response.get('completed_at'),
                    },
                    'normalized_feedback': response.get('normalized_feedback'),
                },
                feedback_payload=response.get('normalized_feedback') or response,
                context_payload=context,
                received_at=datetime.now(timezone.utc),
            )
            session.add(record)
            session.flush()
            return {
                'id': record.id,
                'agent_name': record.agent_name,
                'run_id': record.run_id,
            }

    def _select_specs(self, *, roles: Sequence[str] | None, available_roles: Sequence[str] | None = None) -> list[AgentDispatchSpec]:
        requested_roles = {item.strip().lower() for item in roles or [] if item and item.strip()}
        specs = [
            spec
            for spec in DEFAULT_AGENT_ROLES
            if (available_roles is None or spec.role in available_roles)
            and (not requested_roles or spec.role in requested_roles or spec.agent_name in requested_roles)
        ]
        if not specs:
            raise ValueError('No agent roles selected')
        return specs

    def _dispatch_and_store(
        self,
        *,
        market_date: date,
        request_run_id: str,
        specs: Sequence[AgentDispatchSpec],
        payloads: dict[str, dict[str, Any]],
        symbol: str = '__MARKET__',
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        items: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        for spec in specs:
            try:
                role_payload = payloads[spec.role]
                response = self._get_client().dispatch(spec=spec, payload=role_payload)
                stored = self._store_feedback(
                    market_date=market_date,
                    agent_name=spec.agent_name,
                    run_id=request_run_id,
                    payload=role_payload,
                    response=response,
                    context=role_payload,
                )
                if symbol != '__MARKET__':
                    with session_scope() as session:
                        record = session.get(AgentContextFeedback, stored['id'])
                        if record is not None:
                            record.symbol = symbol
                            session.flush()
                items.append(
                    {
                        'role': spec.role,
                        'agent_name': spec.agent_name,
                        'symbol': symbol,
                        'status': 'completed',
                        'storage': stored,
                        'response': response,
                    }
                )
            except AgentDispatchTerminalError as exc:
                failures.append({'role': spec.role, 'agent_name': spec.agent_name, 'symbol': symbol, 'status': 'failed', 'retryable': False, 'error': str(exc)})
            except Exception as exc:  # noqa: BLE001
                failures.append({'role': spec.role, 'agent_name': spec.agent_name, 'symbol': symbol, 'status': 'failed', 'retryable': True, 'error': str(exc)})
        return items, failures

    def dispatch_for_eod_run(self, *, eod_run_id: int, roles: Sequence[str] | None = None) -> dict[str, Any]:
        run = self._get_run(eod_run_id=eod_run_id)
        payloads = self._build_payloads(run=run)
        specs = self._select_specs(roles=roles, available_roles=payloads.keys())
        request_run_id = f'eod-run-{run.id}-{uuid.uuid4().hex[:10]}'
        items, failures = self._dispatch_and_store(
            market_date=run.trade_date,
            request_run_id=request_run_id,
            specs=specs,
            payloads=payloads,
            symbol='__MARKET__',
        )

        status = 'completed'
        if failures and items:
            status = 'partial'
        elif failures and not items:
            status = 'failed'
        return {
            'request': {
                'eod_run_id': run.id,
                'trade_date': run.trade_date.isoformat(),
                'request_run_id': request_run_id,
                'roles': [spec.role for spec in specs],
            },
            'summary': {
                'status': status,
                'completed': len(items),
                'failed': len(failures),
            },
            'items': items,
            'failures': failures,
        }

    def dispatch_for_symbols(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str],
        roles: Sequence[str] | None = None,
        fs_div: str | None = None,
        disclosures_limit: int = 5,
    ) -> dict[str, Any]:
        normalized_market_date = market_date if isinstance(market_date, date) else date.fromisoformat(market_date)
        symbol_context = agent_context_read_service.get_symbol_day_context(
            market_date=normalized_market_date,
            symbols=symbols,
            fs_div=fs_div,
            disclosures_limit=disclosures_limit,
        )
        specs = self._select_specs(roles=roles)
        request_run_id = f'symbol-run-{normalized_market_date.isoformat()}-{uuid.uuid4().hex[:10]}'
        items: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []

        for item in symbol_context['items']:
            symbol = item['symbol']
            payloads = {
                spec.role: {
                    'role': spec.role,
                    'market_date': normalized_market_date.isoformat(),
                    'symbol': symbol,
                    'company': item.get('company'),
                    'availability': item.get('availability'),
                    'technical_snapshot': item.get('technical_snapshot'),
                    'action_summary': item.get('action_summary'),
                    'signal_features': item.get('signal_features'),
                    'fundamentals': item.get('fundamentals'),
                    'recent_disclosures': item.get('recent_disclosures'),
                }
                for spec in specs
            }
            symbol_items, symbol_failures = self._dispatch_and_store(
                market_date=normalized_market_date,
                request_run_id=request_run_id,
                specs=specs,
                payloads=payloads,
                symbol=symbol,
            )
            items.extend(symbol_items)
            failures.extend(symbol_failures)

        status = 'completed'
        if failures and items:
            status = 'partial'
        elif failures and not items:
            status = 'failed'
        return {
            'request': {
                'market_date': normalized_market_date.isoformat(),
                'symbols': list(symbols),
                'request_run_id': request_run_id,
                'roles': [spec.role for spec in specs],
                'fs_div': fs_div,
                'disclosures_limit': disclosures_limit,
            },
            'summary': {
                'status': status,
                'completed': len(items),
                'failed': len(failures),
            },
            'items': items,
            'failures': failures,
        }


agent_feedback_orchestrator = AgentFeedbackOrchestrator()
