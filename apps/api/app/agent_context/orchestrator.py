from __future__ import annotations

import concurrent.futures
import json
import logging
import re
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Sequence
from urllib.parse import quote, urlparse

import httpx
from fastapi.encoders import jsonable_encoder
from sqlalchemy import select

ABORTLIKE_ERROR_MARKERS = (
    'this operation was aborted',
    'aborted',
    'abortedlastrun',
    'killed',
    'failed',
)


def _is_abortlike_error(error: Any) -> bool:
    text = str(error or '').lower()
    return any(marker in text for marker in ABORTLIKE_ERROR_MARKERS)


from app.agent_context.feedback_service import agent_context_feedback_service
from app.agent_context.runtime_state import RuntimeFeedbackRunState, runtime_feedback_state_store
from app.agent_context.payload_builder import agent_payload_builder
from app.agent_context.read_service import agent_context_read_service
from app.agent_context.response_contract import build_contract_prompt_suffix, normalize_agent_response
from app.agent_context.reuse_service import agent_context_reuse_service
from app.agent_context.session_cleanup_service import SessionCleanupTarget, agent_feedback_session_cleanup_service
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


def _fresh_feedback_session_key(*, agent_name: str, request_run_id: str, slot: int) -> str:
    return f"feedback/{agent_name}/{request_run_id}/slot-{slot:02d}/{uuid.uuid4().hex[:10]}"


def _subagent_test_session_key(*, agent_name: str, request_run_id: str, symbol: str) -> str:
    safe_symbol = ''.join(ch for ch in symbol if ch.isalnum())[:24] or 'symbol'
    return f"subagent-test/{agent_name}/{request_run_id}/{safe_symbol}-{uuid.uuid4().hex[:8]}"


def _collector_session_key(*, agent_name: str) -> str:
    return _feedback_session_key(agent_name=agent_name, slot=1)


def _trim_batch_item_for_prompt(item: dict[str, Any]) -> dict[str, Any]:
    trimmed = json.loads(json.dumps(item, ensure_ascii=False))
    technical_snapshot = trimmed.get('technical_snapshot')
    if isinstance(technical_snapshot, dict):
        technical_snapshot.pop('signal_payload', None)
    action_summary = trimmed.get('action_summary')
    if isinstance(action_summary, dict):
        action_summary.pop('summary_payload', None)
    fundamentals = trimmed.get('fundamentals')
    if isinstance(fundamentals, dict):
        latest_snapshot = fundamentals.get('latest_snapshot')
        if isinstance(latest_snapshot, dict):
            latest_snapshot.pop('raw_rows', None)
            statement = latest_snapshot.get('statement')
            if isinstance(statement, dict):
                keep_statement_keys = {'fs_div', 'fs_label', 'statement_type', 'currency', 'source_status', 'source_message', 'row_count'}
                latest_snapshot['statement'] = {k: v for k, v in statement.items() if k in keep_statement_keys}
        history_summary = fundamentals.get('history_summary')
        if isinstance(history_summary, dict):
            recent_periods = history_summary.get('recent_periods')
            if isinstance(recent_periods, list):
                history_summary['recent_periods'] = recent_periods[:3]
    recent_disclosures = trimmed.get('recent_disclosures')
    if isinstance(recent_disclosures, dict):
        items = recent_disclosures.get('items')
        if isinstance(items, list):
            recent_disclosures['items'] = items[:3]
            recent_disclosures['count'] = min(recent_disclosures.get('count', len(items)), len(recent_disclosures['items']))
    return trimmed


def _extract_json_object(text: str | None) -> str | None:
    if not text:
        return None
    normalized = text.strip()
    if not normalized:
        return None
    if normalized.startswith('```'):
        parts = normalized.split('```')
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if part.startswith('json'):
                part = part[4:].strip()
            candidate = _extract_json_object(part)
            if candidate:
                return candidate
    start = normalized.find('{')
    if start == -1:
        return None
    depth = 0
    in_string = False
    escaped = False
    for idx in range(start, len(normalized)):
        char = normalized[idx]
        if in_string:
            if escaped:
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
            continue
        if char == '{':
            depth += 1
        elif char == '}':
            depth -= 1
            if depth == 0:
                return normalized[start:idx + 1]
    return None


class OpenClawAgentClient:
    def __init__(self) -> None:
        self.base_url = settings.openclaw_base_url.rstrip('/')
        self.gateway_token = settings.openclaw_gateway_token
        self.timeout_seconds = max(1, settings.openclaw_agent_timeout_seconds)
        self.max_retries = max(0, settings.openclaw_agent_max_retries)
        self.retry_backoff_seconds = max(0, settings.openclaw_agent_retry_backoff_seconds)

    def _tool_invoke_url(self) -> str:
        parsed = urlparse(self.base_url)
        if not parsed.scheme or not parsed.netloc:
            raise AgentDispatchTerminalError(f'Invalid OPENCLAW_BASE_URL: {self.base_url}')
        return f'{parsed.scheme}://{parsed.netloc}/tools/invoke'

    def _sessions_history_url(self) -> str:
        parsed = urlparse(self.base_url)
        if not parsed.scheme or not parsed.netloc:
            raise AgentDispatchTerminalError(f'Invalid OPENCLAW_BASE_URL: {self.base_url}')
        return f'{parsed.scheme}://{parsed.netloc}/tools/invoke'

    def fetch_session_history(self, *, session_key: str, limit: int = 40, include_tools: bool = True) -> list[dict[str, Any]]:
        url = self._sessions_history_url()
        body = {
            'tool': 'sessions_history',
            'args': {
                'sessionKey': session_key,
                'limit': limit,
                'includeTools': include_tools,
            },
        }
        with httpx.Client(timeout=max(self.timeout_seconds, settings.openclaw_agent_subtask_timeout_seconds), headers=self._headers()) as client:
            response = client.post(url, json=body)
        if response.status_code >= 400:
            raise AgentDispatchTerminalError(f'OpenClaw sessions_history failed {response.status_code}: {response.text[:300]}')
        parsed = response.json()
        result = parsed.get('result') if isinstance(parsed.get('result'), dict) else {}
        details = result.get('details') if isinstance(result.get('details'), dict) else {}
        messages = details.get('messages')
        if isinstance(messages, list):
            return messages
        return []

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
        result = parsed.get('result') if isinstance(parsed.get('result'), dict) else None
        details = result.get('details') if isinstance(result, dict) and isinstance(result.get('details'), dict) else None
        if not session_key and isinstance(result, dict):
            session_key = result.get('childSessionKey') or result.get('sessionKey')
        if not session_key and details:
            session_key = details.get('childSessionKey') or details.get('sessionKey')
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
        request_marker: str,
    ) -> str:
        example_feedback = {
            'role': spec.role,
            'summary': 'brief conservative summary',
            'confidence': 0.5,
            'risk_flags': ['example_risk_flag'],
            'details': {'note': 'brief structured evidence'},
            'improvement_notes': ['brief note about missing or helpful extra data'],
        }
        prepared_items = [_trim_batch_item_for_prompt(item) for item in batch_items]
        example = {
            'items': [
                {
                    'symbol': prepared_items[0].get('symbol', '000000') if prepared_items else '000000',
                    'feedback': example_feedback,
                }
            ]
        }
        base_message = (
            f"Request marker: {request_marker}\n"
            f"Role: {spec.role}\n"
            f"Instruction: {spec.instruction}\n"
            "Analyze each assigned symbol directly using only the supplied payload.\n"
            "Return one aggregated JSON object for the whole batch.\n"
            "Return JSON only with top-level shape {\"items\": [{\"symbol\": \"...\", \"feedback\": {...}}]}.\n"
            "Each symbol must appear exactly once. Do not omit symbols.\n"
            "For each symbol, feedback must use top-level keys role, summary, confidence, risk_flags, details, improvement_notes.\n"
            "If some inputs are weak or missing, still return conservative structured feedback for that symbol instead of prose or placeholders.\n"
            "Do not mention subagents, children, spawning, waiting, or internal orchestration.\n"
            "No markdown fences. No prose before or after JSON.\n"
            f"{build_contract_prompt_suffix()}\n"
            f"JSON example:\n{json.dumps(example, ensure_ascii=False)}\n\n"
            f"Market date: {market_date.isoformat()}\n"
            f"Assigned batch payload:\n{json.dumps(list(prepared_items), ensure_ascii=False)}"
        )
        if len(base_message) <= settings.openclaw_agent_max_prompt_chars:
            return base_message
        while len(prepared_items) > 1:
            prepared_items = prepared_items[:-1]
            candidate = (
                f"Request marker: {request_marker}\n"
                f"Role: {spec.role}\n"
                f"Instruction: {spec.instruction}\n"
                "Analyze each assigned symbol directly and return aggregated JSON only.\n"
                "Each symbol must appear exactly once in {\"items\": [...]} with structured feedback.\n"
                f"{build_contract_prompt_suffix()}\n"
                f"Market date: {market_date.isoformat()}\n"
                f"Assigned batch payload:\n{json.dumps(list(prepared_items), ensure_ascii=False)}"
            )
            if len(candidate) <= settings.openclaw_agent_max_prompt_chars:
                return candidate
        return (
            f"Role: {spec.role}\n"
            f"Instruction: {spec.instruction}\n"
            "Analyze the assigned symbol directly and return aggregated JSON only.\n"
            "Return {\"items\": [{\"symbol\": \"...\", \"feedback\": {...}}]} with exactly one item.\n"
            f"{build_contract_prompt_suffix()}\n"
            f"Market date: {market_date.isoformat()}\n"
            f"Assigned batch payload:\n{json.dumps([_trim_batch_item_for_prompt(batch_items[0])], ensure_ascii=False)}"
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

    def spawn_symbol_subagent(
        self,
        *,
        spec: AgentDispatchSpec,
        market_date: date,
        symbol_item: dict[str, Any],
        request_run_id: str,
    ) -> dict[str, Any]:
        url = self._tool_invoke_url()
        symbol = str(symbol_item.get('symbol') or '').strip()
        if not symbol:
            raise AgentDispatchTerminalError('symbol_item.symbol is required for subagent spawn')
        child_label = f"{spec.agent_name}-{symbol}"
        child_session_key = _subagent_test_session_key(agent_name=spec.agent_name, request_run_id=request_run_id, symbol=symbol)
        task_payload = _trim_batch_item_for_prompt(symbol_item)
        task = (
            f"Read and follow /Users/shawn/.openclaw/agents/{spec.agent_name}/agent/SUBAGENT_WORKER.md exactly.\n"
            f"Role: {spec.role}\n"
            f"Instruction: {spec.instruction}\n"
            "You are a leaf worker, not a conversational assistant.\n"
            "Analyze exactly one assigned symbol. Do not spawn children.\n"
            "Return JSON only in the worker output shape: {\"items\":[{\"symbol\":\"...\",\"feedback\":{...}}]}.\n"
            "Do not return free-form Korean prose, explanations, or partial notes.\n"
            "Do not say things like '정리됐습니다', '추가 정리본입니다', or '요약 나왔습니다'.\n"
            "Each assigned symbol must appear exactly once in items.\n"
            "Inside feedback, use top-level keys role, summary, confidence, risk_flags, details, improvement_notes.\n"
            "No markdown fences. No prose before or after JSON.\n"
            f"{build_contract_prompt_suffix()}\n\n"
            f"Market date: {market_date.isoformat()}\n"
            f"Symbol payload:\n{json.dumps(task_payload, ensure_ascii=False)}"
        )
        body = {
            'tool': 'sessions_spawn',
            'args': {
                'runtime': 'subagent',
                'agentId': spec.agent_name,
                'mode': 'run',
                'cleanup': 'delete',
                'task': task,
                'label': child_label,
                'timeoutSeconds': max(self.timeout_seconds, settings.openclaw_agent_subtask_timeout_seconds),
                'runTimeoutSeconds': max(self.timeout_seconds, settings.openclaw_agent_subtask_timeout_seconds),
                'lightContext': True,
            },
            'sessionKey': child_session_key,
        }
        headers = self._headers()
        attempt = 0
        last_error: Exception | None = None
        while attempt <= self.max_retries:
            attempt += 1
            started_at = datetime.now(timezone.utc)
            try:
                with httpx.Client(timeout=max(self.timeout_seconds, settings.openclaw_agent_subtask_timeout_seconds) + 30, headers=headers) as client:
                    response = client.post(url, json=body)
                if response.status_code >= 500:
                    raise AgentDispatchRetryableError(f'sessions_spawn server error {response.status_code}: {response.text[:300]}')
                if response.status_code in (408, 409, 429):
                    raise AgentDispatchRetryableError(f'sessions_spawn retryable error {response.status_code}: {response.text[:300]}')
                if response.status_code == 404:
                    raise AgentDispatchTerminalError('sessions_spawn is not available over /tools/invoke; gateway HTTP deny likely still active')
                if response.status_code >= 400:
                    raise AgentDispatchTerminalError(f'sessions_spawn failed {response.status_code}: {response.text[:500]}')
                parsed = response.json()
                result = parsed.get('result') if isinstance(parsed, dict) else None
                if not isinstance(result, dict):
                    raise AgentDispatchTerminalError(f'sessions_spawn returned unexpected result shape: {parsed}')
                details = result.get('details') if isinstance(result.get('details'), dict) else None
                result_status = details.get('status') if details else result.get('status')
                if result_status not in (None, 'accepted'):
                    raise AgentDispatchTerminalError(str(result))
                session_ref = self._extract_session_ref(parsed)
                child_session_ref = session_ref.get('session_key') if session_ref else None
                if not child_session_ref:
                    raise AgentDispatchTerminalError(f'sessions_spawn did not return childSessionKey: {result}')
                return {
                    'ok': True,
                    'attempt': attempt,
                    'started_at': started_at.isoformat(),
                    'completed_at': datetime.now(timezone.utc).isoformat(),
                    'response': parsed,
                    'output_text': None,
                    'session_ref': {
                        'session_key': child_session_ref,
                        'session_id': None,
                    },
                    'normalized_feedback': None,
                    'spawn_result': result,
                }
            except AgentDispatchTerminalError:
                raise
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt > self.max_retries:
                    break
                if self.retry_backoff_seconds > 0:
                    time.sleep(self.retry_backoff_seconds * attempt)
        raise AgentDispatchRetryableError(str(last_error) if last_error else 'unknown sessions_spawn failure')

    def fetch_subagent_response(
        self,
        *,
        child_session_key: str,
        spec: AgentDispatchSpec,
    ) -> dict[str, Any]:
        url = self._sessions_history_url()
        body = {
            'tool': 'sessions_history',
            'args': {
                'sessionKey': child_session_key,
                'limit': 12,
                'includeTools': True,
            },
        }
        headers = self._headers()
        try:
            with httpx.Client(timeout=self.timeout_seconds, headers=headers) as client:
                response = client.post(url, json=body)
            if response.status_code >= 500:
                raise AgentDispatchRetryableError(f'subagent history fetch server error {response.status_code}: {response.text[:300]}')
            if response.status_code in (408, 409, 429):
                raise AgentDispatchRetryableError(f'subagent history fetch retryable error {response.status_code}: {response.text[:300]}')
            if response.status_code == 404:
                raise AgentDispatchTerminalError('sessions_history is not available over /tools/invoke; gateway HTTP allowlist likely still missing it')
            if response.status_code >= 400:
                raise AgentDispatchTerminalError(f'subagent history fetch failed {response.status_code}: {response.text[:300]}')
            parsed = response.json()
            result = parsed.get('result') if isinstance(parsed, dict) else None
            if not isinstance(result, dict):
                raise AgentDispatchTerminalError(f'sessions_history returned unexpected result shape: {parsed}')
            details = result.get('details') if isinstance(result.get('details'), dict) else result
            messages = details.get('messages') if isinstance(details.get('messages'), list) else []
            output_text = None
            assistant_message = None
            done = False
            for message in reversed(messages):
                if not isinstance(message, dict):
                    continue
                if str(message.get('role') or '') != 'assistant':
                    continue
                candidate = message.get('content')
                if isinstance(candidate, str) and candidate.strip():
                    output_text = candidate.strip()
                    assistant_message = message
                    done = True
                    break
            normalized_feedback = None
            if output_text and output_text.strip() not in {'NO_REPLY', 'no_reply'}:
                normalized_feedback = normalize_agent_response(
                    role=spec.role,
                    output_text=output_text,
                    raw_response=assistant_message or parsed,
                )
            return {
                'done': done,
                'response': parsed,
                'output_text': output_text,
                'normalized_feedback': normalized_feedback,
                'session_ref': {'session_key': child_session_key, 'session_id': None},
                'history_result': details,
            }
        except AgentDispatchError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise AgentDispatchRetryableError(str(exc)) from exc

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
        fixed_session_key = request_session_key
        request_marker = f"batch-request::{spec.agent_name}::{market_date.isoformat()}::{uuid.uuid4().hex[:10]}"
        body = {
            'model': f'openclaw/{spec.agent_name}',
            'input': self._build_batch_message(
                spec=spec,
                market_date=market_date,
                batch_items=batch_items,
                request_marker=request_marker,
            ),
        }
        attempt = 0
        last_error: Exception | None = None
        while attempt <= self.max_retries:
            attempt += 1
            started_at = datetime.now(timezone.utc)
            response: httpx.Response | None = None
            try:
                request_session_key = fixed_session_key
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
                if parsed.get('status') in {'failed', 'aborted', 'cancelled', 'canceled'}:
                    raise AgentDispatchRetryableError(
                        f"OpenClaw batch ended with status={parsed.get('status')}"
                    )
                normalized_text = _extract_json_object(output_text)
                decoded = None
                if normalized_text:
                    try:
                        decoded = json.loads(normalized_text)
                    except json.JSONDecodeError:
                        decoded = None
                session_ref = self._extract_session_ref(parsed, explicit_session_key=request_session_key)
                session_history = None
                if decoded is None and request_session_key:
                    session_history = self.fetch_session_history(session_key=request_session_key, limit=80, include_tools=True)
                    marker_index = None
                    for idx, message in enumerate(session_history):
                        if message.get('role') != 'user':
                            continue
                        text_chunks: list[str] = []
                        for content in message.get('content', []):
                            if content.get('type') in {'text', 'input_text', 'output_text'} and isinstance(content.get('text'), str):
                                text_chunks.append(content['text'])
                        candidate_text = '\n'.join(text_chunks)
                        if request_marker in candidate_text:
                            marker_index = idx
                    if marker_index is not None:
                        for message in session_history[marker_index + 1:]:
                            if message.get('role') != 'assistant':
                                continue
                            text_chunks = []
                            for content in message.get('content', []):
                                if content.get('type') in {'text', 'output_text'} and isinstance(content.get('text'), str):
                                    text_chunks.append(content['text'])
                            candidate_text = '\n'.join(text_chunks).strip()
                            candidate_json = _extract_json_object(candidate_text)
                            if not candidate_json:
                                continue
                            try:
                                candidate_decoded = json.loads(candidate_json)
                            except json.JSONDecodeError:
                                continue
                            if isinstance(candidate_decoded, dict) and isinstance(candidate_decoded.get('items'), list):
                                decoded = candidate_decoded
                                output_text = candidate_text
                                break
                if decoded is None:
                    raise AgentDispatchRetryableError('OpenClaw batch returned invalid JSON')
                return {
                    'ok': True,
                    'attempt': attempt,
                    'started_at': started_at.isoformat(),
                    'completed_at': datetime.now(timezone.utc).isoformat(),
                    'response': parsed,
                    'output_text': output_text,
                    'session_ref': session_ref,
                    'decoded': decoded,
                    'history_checked': bool(session_history) if session_history is not None else False,
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

    def _wait_for_subagent_feedback(
        self,
        *,
        child_session_key: str,
        spec: AgentDispatchSpec,
        timeout_seconds: int,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        last_response: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            response = self._get_client().fetch_subagent_response(
                child_session_key=child_session_key,
                spec=spec,
            )
            if response.get('done'):
                normalized_feedback = response.get('normalized_feedback') or {}
                if normalized_feedback:
                    return response
                last_response = response
            time.sleep(1)
        if last_response is not None:
            return last_response
        raise AgentDispatchRetryableError(f'subagent response not observed within {timeout_seconds}s for {spec.agent_name}:{child_session_key}')

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
        serializable_response = jsonable_encoder(response)
        serializable_context = jsonable_encoder(context)
        result = agent_context_feedback_service.submit_feedback(
            market_date=market_date,
            symbol=str(payload.get('symbol') or ''),
            agent_name=agent_name,
            run_id=run_id,
            summary=serializable_response,
            feedback=serializable_response.get('normalized_feedback') if isinstance(serializable_response, dict) else None,
            context=serializable_context,
        )
        session_ref = response.get('session_ref') if isinstance(response, dict) else None
        if isinstance(session_ref, dict):
            session_key = session_ref.get('session_key')
            if isinstance(session_key, str) and session_key.strip():
                cleanup_result = agent_feedback_session_cleanup_service.delete_now(
                    target=SessionCleanupTarget(
                        market_date=market_date,
                        symbol=str(payload.get('symbol') or ''),
                        agent_name=agent_name,
                        run_id=run_id,
                        session_key=session_key.strip(),
                    )
                )
                result['item']['session_cleanup'] = cleanup_result
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
        request_run_id = f'symbol-run-{normalized_market_date.isoformat()}-{uuid.uuid4().hex[:10]}'
        requested_symbols = [str(item['symbol']) for item in symbol_context['items']]
        batch_size = max(1, settings.openclaw_agent_symbol_chunk_size)
        role_parallelism = 10
        state = RuntimeFeedbackRunState(
            run_id=request_run_id,
            market_date=normalized_market_date,
            requested_symbols=requested_symbols,
            requested_roles=[spec.role for spec in specs],
            role_sequence=[spec.role for spec in specs],
            parallel_per_role=role_parallelism,
            batch_size=batch_size,
            started_at=datetime.now(timezone.utc),
            current_role=specs[0].role if specs else None,
        )
        runtime_feedback_state_store.start_run(state)

        def mark_activity(role: str, slot: int, batch_symbols: list[str] | None = None, done: bool = False) -> None:
            def _update(current: RuntimeFeedbackRunState) -> None:
                current.current_role = role
                current.last_activity_at = datetime.now(timezone.utc)
                role_slots = current.in_flight_slots_by_role.setdefault(role, {})
                if done:
                    role_slots.pop(slot, None)
                else:
                    role_slots[slot] = {
                        'symbols': batch_symbols or [],
                        'updated_at': current.last_activity_at.isoformat(),
                    }
            runtime_feedback_state_store.update_run(normalized_market_date, _update)

        def buffer_success(role: str, symbol: str, context: dict[str, Any], response: dict[str, Any]) -> None:
            normalized_feedback = response.get('normalized_feedback')
            def _update(current: RuntimeFeedbackRunState) -> None:
                current.last_activity_at = datetime.now(timezone.utc)
                current.buffered_feedback_rows.append(
                    {
                        'market_date': normalized_market_date,
                        'symbol': symbol,
                        'agent_name': role,
                        'run_id': request_run_id,
                        'summary': jsonable_encoder(response),
                        'feedback': normalized_feedback if isinstance(normalized_feedback, dict) else {},
                        'context': jsonable_encoder(context),
                    }
                )
                current.completed_symbols_by_role.setdefault(role, set()).add(symbol)
            runtime_feedback_state_store.update_run(normalized_market_date, _update)

        def buffer_failure(role: str, symbol: str, error: str, retryable: bool) -> None:
            def _update(current: RuntimeFeedbackRunState) -> None:
                current.last_activity_at = datetime.now(timezone.utc)
                current.buffered_failures.append(
                    {
                        'role': role,
                        'agent_name': role,
                        'symbol': symbol,
                        'status': 'failed',
                        'retryable': retryable,
                        'error': error,
                    }
                )
                current.failed_symbols_by_role.setdefault(role, set()).add(symbol)
            runtime_feedback_state_store.update_run(normalized_market_date, _update)

        all_items: list[dict[str, Any]] = []
        all_failures: list[dict[str, Any]] = []
        for spec in specs:
            role = spec.role
            state.current_role = role
            dispatch_items = [{**item} for item in symbol_context['items']]
            indexed_batches = [dispatch_items[i:i + batch_size] for i in range(0, len(dispatch_items), batch_size)]
            queue: list[tuple[int, list[dict[str, Any]]]] = []
            for batch_index, batch in enumerate(indexed_batches):
                slot = (batch_index % role_parallelism) + 1
                fresh_session_key = _fresh_feedback_session_key(
                    agent_name=spec.agent_name,
                    request_run_id=request_run_id,
                    slot=slot,
                )
                batch_with_session = [
                    {
                        **batch_item,
                        'session_slot': slot,
                        'session_key': fresh_session_key,
                    }
                    for batch_item in batch
                ]
                queue.append((slot, batch_with_session))

            with concurrent.futures.ThreadPoolExecutor(max_workers=role_parallelism) as executor:
                future_map: dict[concurrent.futures.Future[dict[str, Any]], tuple[int, list[dict[str, Any]]]] = {}

                def submit_next() -> None:
                    if not queue:
                        return
                    slot, batch = queue.pop(0)
                    mark_activity(role, slot, [item['symbol'] for item in batch], done=False)
                    future = executor.submit(
                        self._get_client().dispatch_batch,
                        spec=spec,
                        market_date=normalized_market_date,
                        batch_items=batch,
                    )
                    future_map[future] = (slot, batch)

                for _ in range(min(role_parallelism, len(queue))):
                    submit_next()

                while future_map:
                    done, _ = concurrent.futures.wait(list(future_map.keys()), return_when=concurrent.futures.FIRST_COMPLETED)
                    for future in done:
                        slot, batch = future_map.pop(future)
                        try:
                            batch_result = future.result()
                            decoded_items = batch_result.get('decoded', {}).get('items', [])
                            by_symbol = {
                                item['symbol']: item for item in decoded_items if isinstance(item, dict) and item.get('symbol')
                            }
                            for source_item in batch:
                                symbol = source_item['symbol']
                                normalized_feedback = by_symbol.get(symbol, {}).get('feedback')
                                if not normalized_feedback:
                                    failure = {
                                        'role': role,
                                        'agent_name': spec.agent_name,
                                        'symbol': symbol,
                                        'status': 'failed',
                                        'retryable': True,
                                        'error': 'missing_batch_feedback',
                                    }
                                    all_failures.append(failure)
                                    buffer_failure(role, symbol, 'missing_batch_feedback', True)
                                    continue
                                response_payload = {
                                    'attempt': batch_result.get('attempt', 1),
                                    'started_at': batch_result.get('started_at'),
                                    'completed_at': batch_result.get('completed_at'),
                                    'normalized_feedback': normalized_feedback,
                                    'session_ref': batch_result.get('session_ref'),
                                }
                                buffer_success(spec.agent_name, symbol, source_item, response_payload)
                                all_items.append(
                                    {
                                        'role': role,
                                        'agent_name': spec.agent_name,
                                        'symbol': symbol,
                                        'status': 'completed',
                                        'response': {'normalized_feedback': normalized_feedback},
                                    }
                                )
                        except Exception as exc:
                            for source_item in batch:
                                failure = {
                                    'role': role,
                                    'agent_name': spec.agent_name,
                                    'symbol': source_item['symbol'],
                                    'status': 'failed',
                                    'retryable': True,
                                    'error': f'{type(exc).__name__}: {exc}',
                                }
                                all_failures.append(failure)
                                buffer_failure(role, source_item['symbol'], f'{type(exc).__name__}: {exc}', True)
                        finally:
                            mark_activity(role, slot, done=True)
                            submit_next()

        final_state = runtime_feedback_state_store.get_run(normalized_market_date) or state
        buffered_rows = list(final_state.buffered_feedback_rows)
        flush_payload = [
            {
                'market_date': row['market_date'],
                'symbol': row['symbol'],
                'agent_name': row['agent_name'],
                'run_id': row['run_id'],
                'summary': row['summary'],
                'feedback': row['feedback'],
                'context': row['context'],
            }
            for row in buffered_rows
        ]
        flush_result = agent_context_feedback_service.bulk_submit_feedback(items=flush_payload)
        completed_count = len(buffered_rows)
        failed_count = len(final_state.buffered_failures)
        final_status = 'completed' if failed_count == 0 else ('partial' if completed_count > 0 else 'failed')

        def _finish(current: RuntimeFeedbackRunState) -> None:
            current.status = final_status
            current.completed_at = datetime.now(timezone.utc)
            current.last_activity_at = current.completed_at
            for role_slots in current.in_flight_slots_by_role.values():
                role_slots.clear()
        runtime_feedback_state_store.update_run(normalized_market_date, _finish)

        return {
            'request': {
                'market_date': normalized_market_date.isoformat(),
                'symbols': list(symbols),
                'request_run_id': request_run_id,
                'roles': [spec.role for spec in specs],
                'fs_div': fs_div,
                'disclosures_limit': disclosures_limit,
                'symbol_chunk_size': batch_size,
                'parallel_subtasks': role_parallelism,
                'role_parallel_subtasks': role_parallelism,
                'total_parallel_subtasks': role_parallelism,
                'reuse_existing': reuse_existing,
                'mode': 'role_sequential_runtime_buffered',
            },
            'summary': {
                'status': final_status,
                'completed': completed_count,
                'failed': failed_count,
                'flushed_to_db': flush_result.get('count', 0),
            },
            'items': all_items,
            'failures': all_failures,
        }

    def dispatch_for_symbols_via_subagents(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str],
        roles: Sequence[str] | None = None,
        fs_div: str | None = None,
        disclosures_limit: int = 5,
        per_role_parallelism: int = 5,
    ) -> dict[str, Any]:
        normalized_market_date = market_date if isinstance(market_date, date) else date.fromisoformat(market_date)
        symbol_context = agent_context_read_service.get_symbol_day_context(
            market_date=normalized_market_date,
            symbols=symbols,
            fs_div=fs_div,
            disclosures_limit=disclosures_limit,
        )
        specs = self._select_specs(roles=roles)
        request_run_id = f'subagent-symbol-run-{normalized_market_date.isoformat()}-{uuid.uuid4().hex[:10]}'
        items: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        batch_size = 2
        role_worker_count = 1
        total_max_workers = max(1, len(specs))

        role_batches: list[tuple[AgentDispatchSpec, list[dict[str, Any]]]] = []
        for spec in specs:
            dispatch_items = [{**item} for item in symbol_context['items']]
            for start in range(0, len(dispatch_items), batch_size):
                batch = dispatch_items[start:start + batch_size]
                batch_with_session = [
                    {
                        **batch_item,
                        'session_slot': 1,
                        'session_key': _collector_session_key(agent_name=spec.agent_name),
                    }
                    for batch_item in batch
                ]
                role_batches.append((spec, batch_with_session))

        with concurrent.futures.ThreadPoolExecutor(max_workers=total_max_workers) as executor:
            future_map = {
                executor.submit(
                    self._get_client().dispatch_batch,
                    spec=spec,
                    market_date=normalized_market_date,
                    batch_items=batch,
                ): (spec, batch)
                for spec, batch in role_batches
            }
            for future in concurrent.futures.as_completed(future_map):
                spec, batch = future_map[future]
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
                            failures.append(
                                {
                                    'role': spec.role,
                                    'agent_name': spec.agent_name,
                                    'symbol': symbol,
                                    'status': 'failed',
                                    'retryable': True,
                                    'error': 'parent_slot_missing_symbol_feedback',
                                }
                            )
                            continue
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
                                'session_ref': batch_result.get('session_ref'),
                                'collector_mode': 'slot_parent_subagent_fanout',
                            },
                            context=source_item,
                        )
                        items.append(
                            {
                                'role': spec.role,
                                'agent_name': spec.agent_name,
                                'symbol': symbol,
                                'status': 'completed',
                                'storage': stored,
                                'response': {'normalized_feedback': normalized_feedback},
                                'session_ref': batch_result.get('session_ref'),
                            }
                        )
                except Exception as exc:  # noqa: BLE001
                    for source_item in batch:
                        failures.append(
                            {
                                'role': spec.role,
                                'agent_name': spec.agent_name,
                                'symbol': source_item['symbol'],
                                'status': 'failed',
                                'retryable': True,
                                'error': f'{type(exc).__name__}: {exc}',
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
                'per_role_parallelism': role_worker_count,
                'batch_size': batch_size,
                'total_parallel_subtasks': total_max_workers,
                'mode': 'slot_parent_subagent_fanout',
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
