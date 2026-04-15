from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Sequence

import httpx
from sqlalchemy import select

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
            "Return strict JSON with keys role, summary, confidence, risk_flags, and details.\n"
            "If data quality is degraded, say so explicitly in risk_flags/details.\n\n"
            f"Context:\n{payload}"
        )

    def dispatch(self, *, spec: AgentDispatchSpec, payload: dict[str, Any]) -> dict[str, Any]:
        url = f'{self.base_url}/sessions/send'
        body = {
            'label': spec.agent_name,
            'message': self._build_message(spec=spec, payload=payload),
            'timeoutSeconds': self.timeout_seconds,
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
                return {
                    'ok': True,
                    'attempt': attempt,
                    'started_at': started_at.isoformat(),
                    'completed_at': datetime.now(timezone.utc).isoformat(),
                    'response': parsed,
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
        from app.core.settings import settings

        if not settings.openclaw_base_url:
            raise AgentDispatchConfigError('OPENCLAW_BASE_URL is required')
        if not settings.openclaw_gateway_token:
            raise AgentDispatchConfigError('OPENCLAW_GATEWAY_TOKEN is required')

    def _get_client(self) -> OpenClawAgentClient:
        from app.core.settings import settings

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
            if run.status not in {'completed', 'completed_with_errors'}:
                raise ValueError(f'eod_run_id {eod_run_id} is not finalized (status={run.status})')
            session.expunge(run)
            return run

    def _build_payload(self, *, run: EODCollectionRun) -> dict[str, Any]:
        metadata = run.run_metadata or {}
        return {
            'request_id': str(uuid.uuid4()),
            'trade_date': run.trade_date.isoformat(),
            'market': run.market,
            'eod_run': {
                'id': run.id,
                'status': run.status,
                'trigger': run.trigger,
                'source': run.source,
                'symbols_total': run.symbols_total,
                'symbols_succeeded': run.symbols_succeeded,
                'symbols_failed': run.symbols_failed,
                'error_message': run.error_message,
                'started_at': run.started_at.isoformat() if run.started_at else None,
                'completed_at': run.completed_at.isoformat() if run.completed_at else None,
            },
            'data_quality': {
                'has_errors': run.status == 'completed_with_errors' or bool(run.error_message),
                'error_message': run.error_message,
            },
            'run_metadata': metadata,
        }

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
                },
                feedback_payload=response,
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

    def dispatch_for_eod_run(self, *, eod_run_id: int, roles: Sequence[str] | None = None) -> dict[str, Any]:
        run = self._get_run(eod_run_id=eod_run_id)
        payload = self._build_payload(run=run)
        requested_roles = {item.strip().lower() for item in roles or [] if item and item.strip()}
        specs = [spec for spec in DEFAULT_AGENT_ROLES if not requested_roles or spec.role in requested_roles or spec.agent_name in requested_roles]
        if not specs:
            raise ValueError('No agent roles selected')

        request_run_id = f'eod-run-{run.id}-{uuid.uuid4().hex[:10]}'
        items: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        for spec in specs:
            try:
                response = self._get_client().dispatch(spec=spec, payload=payload | {'role': spec.role})
                stored = self._store_feedback(
                    market_date=run.trade_date,
                    agent_name=spec.agent_name,
                    run_id=request_run_id,
                    payload={'role': spec.role},
                    response=response,
                    context=payload,
                )
                items.append(
                    {
                        'role': spec.role,
                        'agent_name': spec.agent_name,
                        'status': 'completed',
                        'storage': stored,
                        'response': response,
                    }
                )
            except AgentDispatchTerminalError as exc:
                failures.append({'role': spec.role, 'agent_name': spec.agent_name, 'status': 'failed', 'retryable': False, 'error': str(exc)})
            except Exception as exc:  # noqa: BLE001
                failures.append({'role': spec.role, 'agent_name': spec.agent_name, 'status': 'failed', 'retryable': True, 'error': str(exc)})

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


from app.core.settings import settings

agent_feedback_orchestrator = AgentFeedbackOrchestrator()
