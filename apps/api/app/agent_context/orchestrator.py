from __future__ import annotations

import concurrent.futures
import json
import logging
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Sequence

import httpx
from sqlalchemy import select

from app.agent_context.feedback_service import agent_context_feedback_service
from app.agent_context.payload_builder import agent_payload_builder
from app.agent_context.read_service import agent_context_read_service
from app.agent_context.response_contract import build_contract_prompt_suffix, normalize_agent_response
from app.agent_context.reuse_service import agent_context_reuse_service
from app.core.settings import settings
from app.db import session_scope
from app.db_models import AgentContextFeedback, EODCollectionRun

logger = logging.getLogger(__name__)


class AgentDispatchError(RuntimeError):
    pass


class AgentDispatchRetryableError(AgentDispatchError):
    pass


class AgentDispatchTerminalError(AgentDispatchError):
    pass


@dataclass(frozen=True)
class AgentDispatchSpec:
    role: str
    agent_name: str
    instruction: str


DISPATCH_SPECS: tuple[AgentDispatchSpec, ...] = (
    AgentDispatchSpec(
        role='eod-summarizer',
        agent_name='eod-summarizer',
        instruction='Summarize the latest end-of-day trading session conservatively, emphasizing breadth, leaders, laggards, anomalies, and data gaps.',
    ),
    AgentDispatchSpec(
        role='technical-summarizer',
        agent_name='technical-summarizer',
        instruction='Summarize the technical setup conservatively using only the supplied inputs, including trend, momentum, support/resistance, and invalidation risks.',
    ),
    AgentDispatchSpec(
        role='fundamental-summarizer',
        agent_name='fundamental-summarizer',
        instruction='Summarize the latest fundamentals conservatively, highlighting business quality, balance-sheet risks, cash-flow traits, and missing context.',
    ),
)


def _feedback_session_key(*, agent_name: str, slot: int) -> str:
    return f"feedback/{agent_name}/slot-{slot:02d}"


class OpenClawAgentClient:
    def __init__(self) -> None:
        self.base_url = settings.openclaw_base_url.rstrip('/')
        self.gateway_token = settings.openclaw_gateway_token
        self.timeout_seconds = max(1, settings.openclaw_agent_timeout_seconds)
        self.max_retries = max(0, settings.openclaw_agent_max_retries)
        self.retry_backoff_seconds = max(0, settings.openclaw_agent_retry_backoff_seconds)

    def _headers(self) -> dict[str, str]:
        headers = {'Content-Type': 'application/json'}
        if self.gateway_token:
            headers['Authorization'] = f'Bearer {self.gateway_token}'
        return headers

    @staticmethod
    def _extract_session_ref(parsed: dict[str, Any], *, explicit_session_key: str | None = None) -> dict[str, Any] | None:
        session_key = explicit_session_key
        session_id = parsed.get('id')
        if not session_key and isinstance(parsed.get('session'), dict):
            session_key = parsed['session'].get('key')
        if not session_key and isinstance(parsed.get('metadata'), dict):
            session_key = parsed['metadata'].get('session_key')
        if session_key or session_id:
            return {
                'session_key': session_key,
                'session_id': session_id,
            }
        return None

    def _build_message(self, *, spec: AgentDispatchSpec, payload: dict[str, Any]) -> str:
        return (
            f"Role: {spec.role}\n"
            f"Instruction: {spec.instruction}\n"
            f"{build_contract_prompt_suffix()}\n\n"
            f"Payload:\n{json.dumps(payload, ensure_ascii=False)}"
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
            'summary': 'brief conservative summary',
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
        session_keys = {
            str(item.get('session_key'))
            for item in batch_items
            if item.get('session_key')
        }
        if len(session_keys) > 1:
            raise ValueError(f'Batch items for {spec.agent_name} mixed session slots: {sorted(session_keys)}')
        request_session_key = next(iter(session_keys), None)
        request_session_key = str(request_session_key or f"feedback/{spec.agent_name}/{market_date.isoformat()}/{uuid.uuid4().hex}")
        body = {
            'model': f'openclaw/{spec.agent_name}',
            'input': self._build_batch_message(spec=spec, market_date=market_date, batch_items=batch_items),
        }
        attempt = 0
        last_error: Exception | None = None
        while attempt <= self.max_retries:
            attempt += 1
            started_at = datetime.now(timezone.utc)
            response: httpx.Response | None = None
            try:
                headers = self._headers() | {'x-openclaw-session-key': request_session_key}
                with httpx.Client(timeout=max(self.timeout_seconds, settings.openclaw_agent_subtask_timeout_seconds), headers=headers) as client:
                    response = client.post(url, json=body)
                if response.status_code >= 500:
                    raise AgentDispatchRetryableError(f'OpenClaw batch server error {response.status_code}: {response.text[:300]}')
                if response.status_code in (408, 409, 429):
                    raise AgentDispatchRetryableError(f'OpenClaw batch retryable error {response.status_code}: {response.text[:300]}')
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
                if parsed.get('status') in {'failed', 'aborted', 'cancelled', 'canceled'}:
                    raise AgentDispatchRetryableError(
                        f"OpenClaw batch ended with status={parsed.get('status')}"
                    )
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
                    'attempt': attempt,
                    'started_at': started_at.isoformat(),
                    'completed_at': datetime.now(timezone.utc).isoformat(),
                    'response': parsed,
                    'output_text': output_text,
                    'session_ref': self._extract_session_ref(parsed, explicit_session_key=request_session_key),
                    'decoded': decoded,
                }
            except AgentDispatchTerminalError:
                raise
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                logger.warning(
                    'OpenClaw batch dispatch attempt failed',
                    extra={
                        'agent_name': spec.agent_name,
                        'role': spec.role,
                        'attempt': attempt,
                        'max_retries': self.max_retries,
                        'batch_size': len(batch_items),
                        'session_key': request_session_key,
                        'error_type': type(exc).__name__,
                        'error': str(exc),
                        'response_status_code': response.status_code if response is not None else None,
                    },
                    exc_info=True,
                )
                if attempt > self.max_retries:
                    break
                if self.retry_backoff_seconds > 0:
                    time.sleep(self.retry_backoff_seconds * attempt)
        raise AgentDispatchRetryableError(str(last_error) if last_error else 'unknown batch dispatch failure')


class AgentFeedbackOrchestrator:
    def __init__(self) -> None:
        self.client: OpenClawAgentClient | None = None

    def _get_client(self) -> OpenClawAgentClient:
        if self.client is None:
            self.client = OpenClawAgentClient()
        return self.client

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
        result = agent_context_feedback_service.submit_feedback(
            market_date=market_date,
            symbol=str(payload.get('symbol') or ''),
            agent_name=agent_name,
            run_id=run_id,
            summary=response,
            feedback=response.get('normalized_feedback') if isinstance(response, dict) else None,
            context=context,
        )
        return result['item']

    def _dispatch_and_store(
        self,
        *,
        market_date: date,
        request_run_id: str,
        specs: Sequence[AgentDispatchSpec],
        payloads: dict[str, dict[str, Any]],
        symbol: str,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        items: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        for spec in specs:
            payload = payloads[spec.role]
            response: dict[str, Any] | None = None
            try:
                response = self._get_client().dispatch(spec=spec, payload=payload)
                stored = self._store_feedback(
                    market_date=market_date,
                    agent_name=spec.agent_name,
                    run_id=request_run_id,
                    payload={'role': spec.role, 'symbol': symbol},
                    response=response,
                    context=payload,
                )
                items.append(
                    {
                        'role': spec.role,
                        'agent_name': spec.agent_name,
                        'symbol': symbol,
                        'status': 'completed',
                        'storage': stored,
                        'response': {'normalized_feedback': response.get('normalized_feedback')},
                    }
                )
            except AgentDispatchTerminalError as exc:
                logger.warning('Agent feedback dispatch failed', extra={'agent_name': spec.agent_name, 'symbol': symbol, 'session_ref': response.get('session_ref') if response else None, 'error': str(exc)})
                failures.append({'role': spec.role, 'agent_name': spec.agent_name, 'symbol': symbol, 'status': 'failed', 'retryable': False, 'error': str(exc)})
            except Exception as exc:  # noqa: BLE001
                logger.warning('Agent feedback dispatch raised', extra={'agent_name': spec.agent_name, 'symbol': symbol, 'session_ref': response.get('session_ref') if response else None, 'error': str(exc)})
                failures.append({'role': spec.role, 'agent_name': spec.agent_name, 'symbol': symbol, 'status': 'failed', 'retryable': True, 'error': str(exc)})
        return items, failures

    def _select_specs(self, *, roles: Sequence[str] | None, available_roles: Sequence[str] | None = None) -> list[AgentDispatchSpec]:
        requested = {role.strip() for role in roles} if roles else None
        available = set(available_roles) if available_roles else None
        specs = [
            spec
            for spec in DISPATCH_SPECS
            if (requested is None or spec.role in requested) and (available is None or spec.role in available)
        ]
        if not specs:
            raise ValueError('No matching agent feedback roles requested')
        return specs

    def dispatch_for_eod_run(
        self,
        *,
        eod_run_id: int,
        roles: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        with session_scope() as session:
            run = session.get(EODCollectionRun, eod_run_id)
            if run is None:
                raise ValueError(f'EOD run not found: {eod_run_id}')
            payloads = agent_payload_builder.build_payloads(run=run)
            trade_date = run.trade_date
            resolved_run_id = run.id
        specs = self._select_specs(roles=roles, available_roles=payloads.keys())
        request_run_id = f'eod-run-{resolved_run_id}-{uuid.uuid4().hex[:10]}'
        items, failures = self._dispatch_and_store(
            market_date=trade_date,
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
                'eod_run_id': resolved_run_id,
                'trade_date': trade_date.isoformat(),
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
        role_batch_jobs: dict[str, dict[int, list[tuple[AgentDispatchSpec, list[dict[str, Any]]]]]] = {}
        logger.info(
            'Agent feedback symbol run started',
            extra={
                'request_run_id': request_run_id,
                'market_date': normalized_market_date.isoformat(),
                'symbol_count': len(symbols),
                'roles': requested_roles,
                'batch_size': batch_size,
                'role_parallel_subtasks': role_worker_count,
                'reuse_existing': reuse_existing,
            },
        )
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
                dispatch_items.append({**item})

            role_batches: list[tuple[int, list[dict[str, Any]]]] = []
            role_slot_jobs: dict[int, list[tuple[AgentDispatchSpec, list[dict[str, Any]]]]] = {}
            if dispatch_items:
                slot_count = max(1, role_worker_count)
                indexed_batches = [
                    dispatch_items[i:i + batch_size]
                    for i in range(0, len(dispatch_items), batch_size)
                ]
                for batch_index, batch in enumerate(indexed_batches):
                    slot = (batch_index % slot_count) + 1
                    batch_with_session = [
                        {
                            **batch_item,
                            'session_key': _feedback_session_key(agent_name=spec.agent_name, slot=slot),
                        }
                        for batch_item in batch
                    ]
                    role_batches.append((slot, batch_with_session))
                    role_slot_jobs.setdefault(slot, []).append((spec, batch_with_session))
            role_batch_jobs[spec.role] = role_slot_jobs
            logger.info(
                'Agent feedback role batches prepared',
                extra={
                    'request_run_id': request_run_id,
                    'role': spec.role,
                    'agent_name': spec.agent_name,
                    'dispatch_symbol_count': len(dispatch_items),
                    'batch_count': len(role_batches),
                    'session_slot_count': len(role_slot_jobs),
                },
            )

        total_max_workers = max(1, role_worker_count * max(1, len(specs)))
        role_execution_limit = max(1, role_worker_count)
        dispatch_future_timeout = max(
            settings.openclaw_agent_subtask_timeout_seconds,
            settings.openclaw_agent_timeout_seconds,
        ) + max(0, settings.openclaw_agent_retry_backoff_seconds * max(1, settings.openclaw_agent_max_retries)) + 30
        slot_cooldown_seconds = max(0.0, settings.openclaw_agent_slot_cooldown_seconds)
        with concurrent.futures.ThreadPoolExecutor(max_workers=total_max_workers) as executor:
            future_map: dict[concurrent.futures.Future[dict[str, Any]], tuple[AgentDispatchSpec, int, list[dict[str, Any]], float]] = {}
            pending_role_jobs: dict[str, dict[int, list[tuple[AgentDispatchSpec, list[dict[str, Any]]]]]] = {
                role: {
                    slot: list(jobs)
                    for slot, jobs in slot_jobs.items()
                }
                for role, slot_jobs in role_batch_jobs.items()
            }
            slot_cooldowns_until: dict[tuple[str, int], float] = {}
            future_lock = threading.Lock()

            def submit_next(role: str, slot: int) -> None:
                cooldown_key = (role, slot)
                cooldown_until = slot_cooldowns_until.get(cooldown_key)
                if cooldown_until is not None and cooldown_until > time.monotonic():
                    return
                role_jobs = pending_role_jobs.get(role) or {}
                jobs = role_jobs.get(slot) or []
                if not jobs:
                    return
                spec, batch = jobs.pop(0)
                future = executor.submit(
                    self._get_client().dispatch_batch,
                    spec=spec,
                    market_date=normalized_market_date,
                    batch_items=batch,
                )
                with future_lock:
                    future_map[future] = (spec, slot, batch, time.monotonic())
                logger.info(
                    'Agent feedback batch submitted',
                    extra={
                        'request_run_id': request_run_id,
                        'role': spec.role,
                        'agent_name': spec.agent_name,
                        'session_slot': slot,
                        'batch_size': len(batch),
                        'symbols': [item.get('symbol') for item in batch],
                    },
                )

            def set_slot_cooldown(role: str, slot: int, reason: str, batch: Sequence[dict[str, Any]]) -> None:
                if slot_cooldown_seconds <= 0:
                    submit_next(role, slot)
                    return
                slot_cooldowns_until[(role, slot)] = time.monotonic() + slot_cooldown_seconds
                logger.warning(
                    'Agent feedback slot cooldown started',
                    extra={
                        'request_run_id': request_run_id,
                        'role': role,
                        'session_slot': slot,
                        'cooldown_seconds': slot_cooldown_seconds,
                        'reason': reason,
                        'symbols': [item.get('symbol') for item in batch],
                    },
                )

            def release_cooled_slots() -> None:
                if slot_cooldown_seconds <= 0:
                    return
                now = time.monotonic()
                for cooldown_key, cooldown_until in list(slot_cooldowns_until.items()):
                    if cooldown_until > now:
                        continue
                    del slot_cooldowns_until[cooldown_key]
                    role, slot = cooldown_key
                    logger.info(
                        'Agent feedback slot cooldown finished',
                        extra={
                            'request_run_id': request_run_id,
                            'role': role,
                            'session_slot': slot,
                        },
                    )
                    submit_next(role, slot)

            for role, slot_jobs in pending_role_jobs.items():
                for slot in sorted(slot_jobs.keys()):
                    submit_next(role, slot)
            logger.info(
                'Agent feedback batch dispatch submitted',
                extra={
                    'request_run_id': request_run_id,
                    'future_count': len(future_map),
                    'total_parallel_subtasks': total_max_workers,
                    'role_execution_limit': role_execution_limit,
                    'dispatch_future_timeout_seconds': dispatch_future_timeout,
                    'slot_cooldown_seconds': slot_cooldown_seconds,
                },
            )
            completed_batches = 0
            failed_batches = 0
            stored_feedback_count = 0
            timed_out_futures: set[concurrent.futures.Future[dict[str, Any]]] = set()
            while True:
                release_cooled_slots()
                with future_lock:
                    active_futures = list(future_map.keys())
                remaining_jobs = any(
                    jobs
                    for slot_jobs in pending_role_jobs.values()
                    for jobs in slot_jobs.values()
                )
                if not active_futures and not remaining_jobs:
                    break
                if not active_futures and remaining_jobs:
                    time.sleep(1)
                    continue
                done, _ = concurrent.futures.wait(
                    active_futures,
                    timeout=1,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                timed_out_entries: list[tuple[concurrent.futures.Future[dict[str, Any]], AgentDispatchSpec, int, list[dict[str, Any]]]] = []
                if not done:
                    now = time.monotonic()
                    with future_lock:
                        for future, (spec, slot, batch, started_monotonic) in list(future_map.items()):
                            if future in timed_out_futures:
                                continue
                            if now - started_monotonic < dispatch_future_timeout:
                                continue
                            timed_out_entries.append((future, spec, slot, batch))
                    for future, spec, slot, batch in timed_out_entries:
                        if not future.cancel():
                            timed_out_futures.add(future)
                            logger.warning(
                                'Agent feedback batch timed out but remains in flight',
                                extra={
                                    'request_run_id': request_run_id,
                                    'role': spec.role,
                                    'agent_name': spec.agent_name,
                                    'session_slot': slot,
                                    'batch_size': len(batch),
                                    'failed_batches': failed_batches,
                                    'completed_batches': completed_batches,
                                    'timeout_seconds': dispatch_future_timeout,
                                    'cancellation_succeeded': False,
                                },
                            )
                            continue
                        with future_lock:
                            entry = future_map.pop(future, None)
                        if entry is None:
                            continue
                        spec, slot, batch, _started_monotonic = entry
                        failed_batches += 1
                        logger.warning(
                            'Agent feedback batch timed out',
                            extra={
                                'request_run_id': request_run_id,
                                'role': spec.role,
                                'agent_name': spec.agent_name,
                                'session_slot': slot,
                                'batch_size': len(batch),
                                'failed_batches': failed_batches,
                                'completed_batches': completed_batches,
                                'timeout_seconds': dispatch_future_timeout,
                                'cancellation_succeeded': True,
                                'symbols': [item.get('symbol') for item in batch],
                            },
                        )
                        for source_item in batch:
                            failures.append(
                                {
                                    'role': spec.role,
                                    'agent_name': spec.agent_name,
                                    'symbol': source_item['symbol'],
                                    'status': 'failed',
                                    'retryable': True,
                                    'error': f'batch_timeout_after_{int(dispatch_future_timeout)}s',
                                    'retry_stage': 'slot_timeout',
                                    'session_slot': slot,
                                }
                            )
                        set_slot_cooldown(spec.role, slot, f'batch_timeout_after_{int(dispatch_future_timeout)}s', batch)
                    continue
                for future in done:
                    timed_out_futures.discard(future)
                    with future_lock:
                        entry = future_map.pop(future, None)
                    if entry is None:
                        continue
                    spec, slot, batch, _started_monotonic = entry
                    batch_result: dict[str, Any] | None = None
                    batch_failed = False
                    try:
                        batch_result = future.result()
                        decoded_items = batch_result.get('decoded', {}).get('items', [])
                        by_symbol = {
                            item['symbol']: item
                            for item in decoded_items
                            if isinstance(item, dict) and item.get('symbol')
                        }
                        completed_batches += 1
                        logger.info(
                            'Agent feedback batch completed',
                            extra={
                                'request_run_id': request_run_id,
                                'role': spec.role,
                                'agent_name': spec.agent_name,
                                'session_slot': slot,
                                'batch_size': len(batch),
                                'session_ref': batch_result.get('session_ref'),
                                'completed_batches': completed_batches,
                                'failed_batches': failed_batches,
                                'symbols': [item.get('symbol') for item in batch],
                            },
                        )
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
                                    'attempt': batch_result.get('attempt', 1),
                                    'started_at': batch_result.get('started_at'),
                                    'completed_at': batch_result.get('completed_at'),
                                    'normalized_feedback': normalized_feedback,
                                },
                                context=source_item,
                            )
                            stored_feedback_count += 1
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
                        batch_failed = True
                        failed_batches += 1
                        abortlike = _is_abortlike_error(exc)
                        logger.warning(
                            'Agent feedback batch failed',
                            extra={
                                'request_run_id': request_run_id,
                                'role': spec.role,
                                'agent_name': spec.agent_name,
                                'session_slot': slot,
                                'batch_size': len(batch),
                                'session_ref': batch_result.get('session_ref') if batch_result else None,
                                'failed_batches': failed_batches,
                                'completed_batches': completed_batches,
                                'error_type': type(exc).__name__,
                                'error': str(exc),
                                'abortlike': abortlike,
                                'symbols': [item.get('symbol') for item in batch],
                            },
                            exc_info=True,
                        )
                        for source_item in batch:
                            failures.append(
                                {
                                    'role': spec.role,
                                    'agent_name': spec.agent_name,
                                    'symbol': source_item['symbol'],
                                    'status': 'failed',
                                    'retryable': True,
                                    'error': f'{type(exc).__name__}: {exc}',
                                    'retry_stage': 'batch_exception',
                                    'session_slot': slot,
                                }
                            )
                        if abortlike:
                            set_slot_cooldown(spec.role, slot, str(exc), batch)
                        else:
                            submit_next(spec.role, slot)
                    finally:
                        if not batch_failed:
                            submit_next(spec.role, slot)

        status = 'completed'
        if failures and items:
            status = 'partial'
        elif failures and not items:
            status = 'failed'

        logger.info(
            'Agent feedback symbol run finished',
            extra={
                'request_run_id': request_run_id,
                'market_date': normalized_market_date.isoformat(),
                'status': status,
                'completed_items': len(items),
                'failed_items': len(failures),
                'stored_feedback_count': stored_feedback_count,
                'completed_batches': completed_batches,
                'failed_batches': failed_batches,
            },
        )

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
