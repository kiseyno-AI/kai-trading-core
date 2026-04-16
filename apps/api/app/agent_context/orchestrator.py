from __future__ import annotations

import concurrent.futures
import hashlib
import json
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Sequence
from urllib.parse import quote

import httpx

from app.agent_context.payload_builder import agent_payload_builder
from app.agent_context.read_service import agent_context_read_service
from app.agent_context.reuse_service import agent_context_reuse_service
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


def _stable_slot_for_value(*, value: str, pool_size: int) -> int:
    normalized = value.strip() or 'default'
    digest = hashlib.sha256(normalized.encode('utf-8')).hexdigest()
    return int(digest[:8], 16) % max(1, pool_size)


def _feedback_session_key(*, agent_name: str, slot: int) -> str:
    return f"feedback/{agent_name}/slot-{slot:02d}"


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

    def _extract_session_ref(self, parsed: dict[str, Any], *, explicit_session_key: str | None = None) -> dict[str, Any] | None:
        if explicit_session_key:
            return {
                'session_key': explicit_session_key,
                'response_id': parsed.get('id'),
            }
        session_key = parsed.get('session_key') or parsed.get('sessionKey')
        if isinstance(session_key, str) and session_key.strip():
            return {
                'session_key': session_key.strip(),
                'response_id': parsed.get('id'),
            }
        return None

    def cleanup_session(self, session_ref: dict[str, Any] | None) -> dict[str, Any]:
        if not session_ref or not session_ref.get('session_key'):
            return {'status': 'skipped', 'reason': 'missing_session_key'}
        session_key = str(session_ref['session_key']).strip()
        url = f"{self.base_url}/sessions/{quote(session_key, safe='')}/kill"
        try:
            with httpx.Client(timeout=max(10.0, min(self.timeout_seconds, 30.0)), headers=self._headers()) as client:
                response = client.post(url)
            if response.status_code == 404:
                return {
                    'status': 'not_found',
                    'status_code': response.status_code,
                    'session_key': session_key,
                }
            if response.status_code >= 400:
                return {
                    'status': 'failed',
                    'status_code': response.status_code,
                    'session_key': session_key,
                    'error': response.text[:300],
                }
            parsed = response.json()
            return {
                'status': 'completed' if parsed.get('killed') else 'not_killed',
                'status_code': response.status_code,
                'session_key': session_key,
                'killed': bool(parsed.get('killed')),
            }
        except Exception as exc:  # noqa: BLE001
            return {
                'status': 'failed',
                'session_key': session_key,
                'error': str(exc),
            }

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

    def _build_batch_message(
        self,
        *,
        spec: AgentDispatchSpec,
        market_date: date,
        batch_items: Sequence[dict[str, Any]],
    ) -> str:
        example_feedback = {
            'role': spec.role,
            'summary': 'Conservative one-paragraph summary for the symbols in this batch',
            'confidence': 0.5,
            'risk_flags': ['example_risk_flag'],
            'details': {'note': 'brief structured evidence'},
            'improvement_notes': ['brief note about missing or helpful extra data'],
        }
        example = {
            'items': [
                {
                    'symbol': batch_items[0].get('symbol', '000000') if batch_items else '000000',
                    'feedback': example_feedback,
                }
            ]
        }
        return (
            f"Role: {spec.role}\n"
            f"Instruction: {spec.instruction}\n"
            "Analyze every symbol in this batch directly.\n"
            "Return JSON only with top-level shape {\"items\": [{\"symbol\": \"...\", \"feedback\": {...}}]}.\n"
            "Do not omit symbols. If one symbol is weak or blocked, still return conservative failure-oriented feedback for that symbol.\n"
            "No markdown fences. No prose before or after JSON.\n"
            f"{build_contract_prompt_suffix()}\n"
            f"JSON example:\n{json.dumps(example, ensure_ascii=False)}\n\n"
            f"Market date: {market_date.isoformat()}\n"
            f"Batch payload:\n{json.dumps(list(batch_items), ensure_ascii=False)}"
        )

    def dispatch(self, *, spec: AgentDispatchSpec, payload: dict[str, Any]) -> dict[str, Any]:
        url = f'{self.base_url}/v1/responses'
        request_session_key = str(payload.get('session_key') or f"feedback/{spec.agent_name}/{payload.get('request_id') or uuid.uuid4().hex}")
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
                headers = self._headers() | {'x-openclaw-session-key': request_session_key}
                with httpx.Client(timeout=self.timeout_seconds, headers=headers) as client:
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
                    'session_ref': self._extract_session_ref(parsed, explicit_session_key=request_session_key),
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

    def dispatch_batch(
        self,
        *,
        spec: AgentDispatchSpec,
        market_date: date,
        batch_items: Sequence[dict[str, Any]],
    ) -> dict[str, Any]:
        url = f'{self.base_url}/v1/responses'
        request_session_key = None
        if batch_items:
            request_session_key = batch_items[0].get('session_key')
        request_session_key = str(request_session_key or f"feedback/{spec.agent_name}/{market_date.isoformat()}/{uuid.uuid4().hex}")
        body = {
            'model': f'openclaw/{spec.agent_name}',
            'input': self._build_batch_message(spec=spec, market_date=market_date, batch_items=batch_items),
        }
        started_at = datetime.now(timezone.utc)
        headers = self._headers() | {'x-openclaw-session-key': request_session_key}
        with httpx.Client(timeout=max(self.timeout_seconds, settings.openclaw_agent_subtask_timeout_seconds), headers=headers) as client:
            response = client.post(url, json=body)
        if response.status_code >= 400:
            raise AgentDispatchTerminalError(f'OpenClaw batch failed {response.status_code}: {response.text[:300]}')
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
        if not output_text:
            raise AgentDispatchTerminalError('OpenClaw batch returned no output_text')
        normalized_text = output_text.strip()
        if normalized_text.startswith('```'):
            parts = normalized_text.split('```')
            normalized_text = ''
            for part in parts:
                part = part.strip()
                if not part:
                    continue
                if part.startswith('json'):
                    part = part[4:].strip()
                if part.startswith('{') and part.endswith('}'):
                    normalized_text = part
                    break
        if not normalized_text.startswith('{'):
            start = normalized_text.find('{')
            end = normalized_text.rfind('}')
            if start != -1 and end != -1 and end > start:
                normalized_text = normalized_text[start:end + 1]
        try:
            decoded = json.loads(normalized_text)
        except json.JSONDecodeError:
            repaired_items: list[dict[str, Any]] = []
            for batch_item in batch_items:
                symbol = batch_item.get('symbol')
                if not symbol:
                    continue
                repaired_items.append(
                    {
                        'symbol': symbol,
                        'feedback': normalize_agent_response(
                            role=spec.role,
                            output_text=output_text,
                            raw_response=parsed,
                        ),
                    }
                )
            if repaired_items:
                decoded = {'items': repaired_items}
            else:
                raise AgentDispatchTerminalError('OpenClaw batch returned invalid JSON and no repairable symbols were present')
        return {
            'ok': True,
            'started_at': started_at.isoformat(),
            'completed_at': datetime.now(timezone.utc).isoformat(),
            'response': parsed,
            'output_text': output_text,
            'session_ref': self._extract_session_ref(parsed, explicit_session_key=request_session_key),
            'decoded': decoded,
        }


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
            if run.status not in {'completed', 'completed_with_errors'}:
                raise ValueError(f'eod_run_id {eod_run_id} is not finalized (status={run.status})')
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
            response: dict[str, Any] | None = None
            role_payload = payloads[spec.role]
            try:
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
                logger.warning('Agent feedback dispatch failed', extra={'agent_name': spec.agent_name, 'symbol': symbol, 'session_ref': response.get('session_ref') if response else None, 'error': str(exc)})
                failures.append({'role': spec.role, 'agent_name': spec.agent_name, 'symbol': symbol, 'status': 'failed', 'retryable': False, 'error': str(exc)})
            except Exception as exc:  # noqa: BLE001
                logger.warning('Agent feedback dispatch raised', extra={'agent_name': spec.agent_name, 'symbol': symbol, 'session_ref': response.get('session_ref') if response else None, 'error': str(exc)})
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
        reuse_existing: bool = True,
    ) -> dict[str, Any]:
        normalized_market_date = market_date if isinstance(market_date, date) else date.fromisoformat(market_date)
        symbol_context = agent_context_read_service.get_symbol_day_context(
            market_date=normalized_market_date,
            symbols=symbols,
            fs_div=fs_div,
            disclosures_limit=disclosures_limit,
        )
        specs = self._select_specs(roles=roles)
        requested_roles = [spec.role for spec in specs]
        request_run_id = f'symbol-run-{normalized_market_date.isoformat()}-{uuid.uuid4().hex[:10]}'
        items: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        batch_size = max(1, settings.openclaw_agent_symbol_chunk_size)
        max_workers = max(1, settings.openclaw_agent_parallel_subtasks)

        reuse_decisions = agent_context_reuse_service.build_latest_decisions(
            market_date=normalized_market_date,
            symbols=symbols,
            roles=requested_roles,
        ) if reuse_existing else {}

        role_worker_count = max_workers
        role_batch_jobs: dict[str, list[tuple[AgentDispatchSpec, list[dict[str, Any]]]]] = {}
        for spec in specs:
            dispatch_items = []
            for item in symbol_context['items']:
                decision = reuse_decisions.get(item['symbol'], {}).get(spec.role) if reuse_existing else None
                if decision and not decision.should_dispatch and decision.latest_feedback is not None:
                    items.append(
                        {
                            'role': spec.role,
                            'agent_name': spec.agent_name,
                            'symbol': item['symbol'],
                            'status': 'reused',
                            'reuse_reason': decision.reason,
                            'storage': {'id': decision.latest_feedback_id, 'agent_name': spec.agent_name, 'run_id': decision.latest_feedback.get('run_id')},
                            'response': {'normalized_feedback': decision.latest_feedback.get('feedback_payload')},
                        }
                    )
                    continue
                slot = _stable_slot_for_value(value=item['symbol'], pool_size=role_worker_count) + 1
                dispatch_items.append(
                    {
                        **item,
                        'session_key': _feedback_session_key(agent_name=spec.agent_name, slot=slot),
                    }
                )
            role_batches = [dispatch_items[i:i + batch_size] for i in range(0, len(dispatch_items), batch_size)]
            role_batch_jobs[spec.role] = [(spec, batch) for batch in role_batches]

        total_max_workers = max(1, role_worker_count * max(1, len(specs)))
        with concurrent.futures.ThreadPoolExecutor(max_workers=total_max_workers) as executor:
            future_map = {
                executor.submit(
                    self._get_client().dispatch_batch,
                    spec=spec,
                    market_date=normalized_market_date,
                    batch_items=batch,
                ): (spec, batch)
                for role_jobs in role_batch_jobs.values()
                for spec, batch in role_jobs
            }
            for future in concurrent.futures.as_completed(future_map):
                spec, batch = future_map[future]
                batch_result: dict[str, Any] | None = None
                try:
                    batch_result = future.result()
                    decoded_items = batch_result.get('decoded', {}).get('items', [])
                    by_symbol = {
                        item['symbol']: item
                        for item in decoded_items
                        if isinstance(item, dict) and item.get('symbol')
                    }
                    for source_item in batch:
                        symbol = source_item['symbol']
                        normalized_feedback = by_symbol.get(symbol, {}).get('feedback')
                        if not normalized_feedback:
                            normalized_feedback = {
                                'role': spec.role,
                                'summary': f'{spec.role} batch did not return feedback for {symbol}',
                                'confidence': 0.0,
                                'risk_flags': ['missing_batch_feedback'],
                                'details': {
                                    'symbol': symbol,
                                    'market_date': normalized_market_date.isoformat(),
                                },
                                'improvement_notes': ['Ensure every batch response returns one normalized feedback object per assigned symbol'],
                            }
                        stored = self._store_feedback(
                            market_date=normalized_market_date,
                            agent_name=spec.agent_name,
                            run_id=request_run_id,
                            payload={'role': spec.role, 'symbol': symbol},
                            response={
                                'attempt': 1,
                                'started_at': batch_result.get('started_at'),
                                'completed_at': batch_result.get('completed_at'),
                                'normalized_feedback': normalized_feedback,
                            },
                            context=source_item,
                        )
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
                                'response': {'normalized_feedback': normalized_feedback},
                            }
                        )
                except Exception as exc:  # noqa: BLE001
                    logger.warning('Agent feedback batch failed', extra={'agent_name': spec.agent_name, 'session_ref': batch_result.get('session_ref') if batch_result else None, 'error': str(exc)})
                    for source_item in batch:
                        failures.append(
                            {
                                'role': spec.role,
                                'agent_name': spec.agent_name,
                                'symbol': source_item['symbol'],
                                'status': 'failed',
                                'retryable': True,
                                'error': str(exc),
                            }
                        )

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
                'symbol_chunk_size': batch_size,
                'parallel_subtasks': role_worker_count,
                'role_parallel_subtasks': role_worker_count,
                'total_parallel_subtasks': total_max_workers,
                'reuse_existing': reuse_existing,
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
