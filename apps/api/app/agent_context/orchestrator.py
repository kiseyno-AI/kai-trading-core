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
from app.agent_context.feedback_failure_service import agent_context_feedback_failure_service
from app.agent_context.runtime_state import RuntimeFeedbackRunState, runtime_feedback_state_store
from app.agent_context.payload_builder import agent_payload_builder
from app.agent_context.read_service import agent_context_read_service
from app.agent_context.roles import SIGNAL_DECIDER_AGENT_NAME, SUMMARIZER_AGENT_NAMES
from app.agent_context.run_state_service import agent_context_feedback_run_state_service
from app.agent_context.response_contract import build_contract_prompt_suffix, normalize_agent_response
from app.agent_context.reuse_service import agent_context_reuse_service
from app.agent_context.session_cleanup_service import SessionCleanupTarget, agent_feedback_session_cleanup_service
from app.core.settings import settings
from app.db import session_scope
from app.db_models import AgentContextFeedback, EODCollectionRun
from app.market_data_store import load_daily_piotroski_filter_results

logger = logging.getLogger(__name__)


def _feedback_runtime_log(stage: str, **payload: Any) -> None:
    if 'headers' in payload and isinstance(payload['headers'], dict):
        payload['headers_snapshot'] = dict(payload['headers'])
    line = {
        'scope': 'agent_feedback_runtime',
        'stage': stage,
        **payload,
    }
    debug_path = __import__('pathlib').Path(__file__).resolve().parents[2] / 'var' / 'log' / 'agent-feedback-runtime.log'
    try:
        debug_path.parent.mkdir(parents=True, exist_ok=True)
        with debug_path.open('a', encoding='utf-8') as fh:
            fh.write(json.dumps(line, ensure_ascii=False, default=str) + '\n')
    except OSError:
        pass


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


UPSTREAM_SUMMARY_ROLES: tuple[str, ...] = SUMMARIZER_AGENT_NAMES


DISPATCH_SPECS: tuple[AgentDispatchSpec, ...] = (
    AgentDispatchSpec(
        role='eod-summarizer',
        agent_name='eod-summarizer',
        instruction='Summarize the latest end-of-day trading session conservatively, emphasizing breadth, leaders, laggards, anomalies, and data gaps.',
    ),
    AgentDispatchSpec(
        role='technical-summarizer',
        agent_name='technical-summarizer',
        instruction='Summarize the latest technical setup conservatively using only the supplied inputs, emphasizing trend, momentum, support/resistance, invalidation risks, anomalies, and data gaps.',
    ),
    AgentDispatchSpec(
        role='fundamental-summarizer',
        agent_name='fundamental-summarizer',
        instruction='Summarize the latest fundamentals conservatively, emphasizing business quality, balance-sheet risks, cash-flow traits, anomalies, and data gaps.',
    ),
    AgentDispatchSpec(
        role=SIGNAL_DECIDER_AGENT_NAME,
        agent_name=SIGNAL_DECIDER_AGENT_NAME,
        instruction=(
            'Synthesize the three specialist summaries into conservative next-trading-day decision support only, '
            'choosing buy, wait, or reject while surfacing evidence, disagreement, invalidation, data-quality warnings, '
            'and realistic target or stop levels without execution logic, leverage, sizing, or fake precision.'
        ),
    ),
)


def _feedback_session_key(*, agent_name: str, slot: int) -> str:
    return f"feedback/{agent_name}/slot-{slot:02d}"


def _fresh_feedback_session_key(*, agent_name: str, request_run_id: str, slot: int) -> str:
    return f"feedback/{agent_name}/{request_run_id}/slot-{slot:02d}/{uuid.uuid4().hex[:10]}"


def _subagent_test_session_key(*, agent_name: str, request_run_id: str, symbol: str) -> str:
    safe_symbol = ''.join(ch for ch in symbol if ch.isalnum())[:24] or 'symbol'
    return f"subagent-test/{agent_name}/{request_run_id}/{safe_symbol}-{uuid.uuid4().hex[:8]}"


def _normalize_requested_symbols(symbols: Sequence[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        for piece in (raw_symbol or '').split(','):
            symbol = piece.strip().upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            normalized.append(symbol)
    if not normalized:
        raise ValueError('At least one symbol is required')
    return normalized


def _split_requested_symbols_by_piotroski_passed_filter(
    *,
    market_date: date,
    requested_symbols: Sequence[str],
) -> tuple[list[str], list[str]]:
    passed_rows = load_daily_piotroski_filter_results(
        market_date=market_date,
        symbols=list(requested_symbols),
        passed_filter=True,
        limit=None,
    )
    passed_symbol_set = {str(item['symbol']) for item in passed_rows}
    eligible_symbols = [symbol for symbol in requested_symbols if symbol in passed_symbol_set]
    skipped_symbols = [symbol for symbol in requested_symbols if symbol not in passed_symbol_set]
    return eligible_symbols, skipped_symbols


def _strip_runtime_session_fields(item: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in item.items()
        if key not in {'session_slot', 'session_key'}
    }


def _feedback_row_lookup(rows: Sequence[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        symbol = str(row.get('symbol') or '').strip()
        agent_name = str(row.get('agent_name') or '').strip()
        if not symbol or not agent_name:
            continue
        lookup[(symbol, agent_name)] = row
    return lookup


def _build_response_storage_payload(
    *,
    spec: AgentDispatchSpec,
    batch_result: dict[str, Any],
    normalized_feedback: dict[str, Any],
    collector_mode: str,
) -> dict[str, Any]:
    raw_response = batch_result.get('response') if isinstance(batch_result.get('response'), dict) else None
    model_name = None
    if raw_response is not None:
        model_name = raw_response.get('model') or raw_response.get('model_name')
    return {
        'attempt': batch_result.get('attempt', 1),
        'started_at': batch_result.get('started_at'),
        'completed_at': batch_result.get('completed_at'),
        'model': model_name or f'openclaw/{spec.agent_name}',
        'output_text': batch_result.get('output_text'),
        'raw_response': raw_response,
        'normalized_feedback': normalized_feedback,
        'session_ref': batch_result.get('session_ref'),
        'collector_mode': collector_mode,
    }


def _trim_batch_item_for_prompt(item: dict[str, Any]) -> dict[str, Any]:
    trimmed = json.loads(json.dumps(item, ensure_ascii=False))
    symbol_day_context = trimmed.get('symbol_day_context')
    if isinstance(symbol_day_context, dict):
        trimmed['symbol_day_context'] = _trim_batch_item_for_prompt(symbol_day_context)
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
    if normalized.startswith('[intent:'):
        newline_index = normalized.find("\n")
        if newline_index != -1:
            candidate = _extract_json_object(normalized[newline_index + 1:])
            if candidate:
                return candidate
    if '[tool_code]' in normalized and '[/tool_code]' in normalized:
        start_marker = normalized.find('[tool_code]') + len('[tool_code]')
        end_marker = normalized.find('[/tool_code]', start_marker)
        if end_marker != -1:
            tool_block = normalized[start_marker:end_marker].strip()
            json_call_match = re.search(r'json\.dumps\((\{.*\})\)\s*$', tool_block, re.DOTALL)
            if json_call_match:
                candidate = _extract_json_object(json_call_match.group(1))
                if candidate:
                    return candidate
            candidate = _extract_json_object(tool_block)
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
                candidate = normalized[start:idx + 1]
                try:
                    json.loads(candidate)
                    return candidate
                except json.JSONDecodeError:
                    continue
    return None


class OpenClawAgentClient:
    def __init__(self) -> None:
        self.base_url = settings.openclaw_base_url.rstrip('/')
        self.gateway_token = settings.openclaw_gateway_token
        self.timeout_seconds = settings.openclaw_agent_timeout_seconds if settings.openclaw_agent_timeout_seconds > 0 else None
        self.http_timeout_seconds = self.timeout_seconds
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
        with httpx.Client(timeout=self.http_timeout_seconds, headers=self._headers()) as client:
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
            f"{build_contract_prompt_suffix(role=spec.role)}\n\n"
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
        example_symbol = str(batch_items[0].get('symbol') or '000000') if batch_items else '000000'
        if spec.role == SIGNAL_DECIDER_AGENT_NAME:
            example_feedback = {
                'role': spec.role,
                'agent_name': spec.agent_name,
                'symbol': example_symbol,
                'market_date': market_date.isoformat(),
                'preferred_action': 'wait',
                'next_day_open_action': 'wait',
                'entry_price_basis': 'next_open',
                'target_price': None,
                'expected_return_pct': None,
                'stop_loss_price': None,
                'suggested_holding_period': '3-10 trading days',
                'confidence_score_10': 6,
                'conviction_score_10': 5,
                'thesis_summary': 'brief conservative decision-support thesis',
                'supporting_evidence': ['example evidence'],
                'decision_risks': ['example decision risk'],
                'invalidation_conditions': ['example invalidation'],
                'data_quality_warnings': ['example warning'],
                'specialist_disagreement': ['example disagreement'],
                'traceability': {
                    'agent_name': spec.agent_name,
                    'symbol': example_symbol,
                    'market_date': market_date.isoformat(),
                },
                'summary': 'brief conservative decision-support thesis',
                'confidence': 0.6,
                'risk_flags': ['example_warning'],
                'details': {'decision_support': {'decision_support_only': True}},
                'improvement_notes': ['example note about missing or helpful extra data'],
            }
        else:
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
            "For each symbol, feedback must include role, summary, confidence, risk_flags, details, improvement_notes, and every additional role-specific field required by the contract.\n"
            "Inside details, do not repeat raw payload facts or echo numeric/source fields already present in the request.\n"
            "Use details only for compact interpretation, relationships, anomalies, conservative takeaways, and missing-context notes that add new value beyond the payload.\n"
            "Prefer derived judgments over restating inputs; if a field is already in the payload, reference its implication instead of copying it.\n"
            "If some inputs are weak or missing, still return conservative structured feedback for that symbol instead of prose or placeholders.\n"
            "Do not mention subagents, children, spawning, waiting, or internal orchestration.\n"
            "Do not output tool_code, python, print(...), json.dumps(...), markdown fences, code blocks, intent tags, analysis, reasoning, or explanatory prose.\n"
            "Do not prepend labels like [intent:...], do not wrap JSON in tags, and do not add any text before or after the JSON.\n"
            "Do not restate the request, do not describe what you will do, and do not explain the schema. Output the final JSON only.\n"
            "Your final answer must be a raw JSON object only, beginning with { and ending with }.\n"
            f"{build_contract_prompt_suffix(role=spec.role)}\n"
            f"JSON example:\n{json.dumps(example, ensure_ascii=False)}\n\n"
            f"Market date: {market_date.isoformat()}\n"
            f"Assigned batch payload:\n{json.dumps(list(prepared_items), ensure_ascii=False)}"
        )
        max_chars = int(settings.openclaw_agent_max_prompt_chars or 0)
        if max_chars <= 0 or len(base_message) <= max_chars:
            return base_message
        while len(prepared_items) > 1:
            prepared_items = prepared_items[:-1]
            candidate = (
                f"Request marker: {request_marker}\n"
                f"Role: {spec.role}\n"
                f"Instruction: {spec.instruction}\n"
                "Analyze each assigned symbol directly and return aggregated JSON only.\n"
                "Each symbol must appear exactly once in {\"items\": [...]} with structured feedback.\n"
                "No tool_code, no print(json.dumps(...)), no markdown fences, no intent tags, no prose, no request restatement, raw JSON object only.\n"
                f"{build_contract_prompt_suffix(role=spec.role)}\n"
                f"Market date: {market_date.isoformat()}\n"
                f"Assigned batch payload:\n{json.dumps(list(prepared_items), ensure_ascii=False)}"
            )
            if max_chars <= 0 or len(candidate) <= max_chars:
                return candidate
        return (
            f"Role: {spec.role}\n"
            f"Instruction: {spec.instruction}\n"
            "Analyze the assigned symbol directly and return aggregated JSON only.\n"
            "Return {\"items\": [{\"symbol\": \"...\", \"feedback\": {...}}]} with exactly one item.\n"
            "No tool_code, no print(json.dumps(...)), no markdown fences, no intent tags, no prose, no request restatement, raw JSON object only.\n"
            f"{build_contract_prompt_suffix(role=spec.role)}\n"
            f"Market date: {market_date.isoformat()}\n"
            f"Assigned batch payload:\n{json.dumps([_trim_batch_item_for_prompt(batch_items[0])], ensure_ascii=False)}"
        )

    def dispatch(self, *, spec: AgentDispatchSpec, payload: dict[str, Any]) -> dict[str, Any]:
        url = f'{self.base_url}/v1/responses'
        request_session_key = str(payload.get('session_key') or f"agent:{spec.agent_name}:feedback/{spec.agent_name}/{payload.get('request_id') or uuid.uuid4().hex}")
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
                headers = self._headers() | {
                    'x-openclaw-session-key': request_session_key,
                    'x-openclaw-agent-id': spec.agent_name,
                }
                _feedback_runtime_log(
                    'dispatch_single_http_target',
                    agent_name=spec.agent_name,
                    role=spec.role,
                    url=url,
                    headers=headers,
                    session_key=request_session_key,
                    request_body=body,
                )
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
                        market_date=payload.get('market_date') or payload.get('trade_date'),
                        symbol=str(payload.get('symbol') or '') or None,
                        agent_name=spec.agent_name,
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
            f"{build_contract_prompt_suffix(role=spec.role)}\n\n"
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
                'timeoutSeconds': self.http_timeout_seconds,
                'runTimeoutSeconds': self.http_timeout_seconds,
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
                with httpx.Client(timeout=self.http_timeout_seconds + 30, headers=headers) as client:
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
                    market_date=parsed.get('market_date'),
                    symbol=None,
                    agent_name=spec.agent_name,
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
                headers = self._headers() | {
                    'x-openclaw-session-key': request_session_key,
                    'x-openclaw-agent-id': spec.agent_name,
                }
                _feedback_runtime_log(
                    'dispatch_batch_request',
                    agent_name=spec.agent_name,
                    role=spec.role,
                    market_date=market_date.isoformat(),
                    session_key=request_session_key,
                    headers=headers,
                    batch_symbols=[str(item.get('symbol')) for item in batch_items],
                    request_body=body,
                )
                _feedback_runtime_log(
                    'dispatch_batch_http_target',
                    agent_name=spec.agent_name,
                    role=spec.role,
                    market_date=market_date.isoformat(),
                    url=url,
                    headers=headers,
                    session_key=request_session_key,
                    request_body=body,
                )
                with httpx.Client(timeout=self.http_timeout_seconds, headers=headers) as client:
                    response = client.post(url, json=body)
                if response.status_code >= 500:
                    raise AgentDispatchRetryableError(f'OpenClaw batch server error {response.status_code}: {response.text[:300]}')
                if response.status_code in (408, 409, 429):
                    raise AgentDispatchRetryableError(f'OpenClaw batch retryable error {response.status_code}: {response.text[:300]}')
                if response.status_code >= 400:
                    raise AgentDispatchTerminalError(f'OpenClaw batch failed {response.status_code}: {response.text[:300]}')
                parsed = response.json()
                _feedback_runtime_log(
                    'dispatch_batch_response',
                    agent_name=spec.agent_name,
                    role=spec.role,
                    market_date=market_date.isoformat(),
                    session_key=request_session_key,
                    status_code=response.status_code,
                    response_body=parsed,
                )
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
                if isinstance(decoded, dict) and not isinstance(decoded.get('items'), list):
                    symbol_items = []
                    for batch_item in batch_items:
                        symbol = str(batch_item.get('symbol') or '').strip()
                        if not symbol:
                            continue
                        normalized_feedback = normalize_agent_response(
                            role=spec.role,
                            output_text=output_text or json.dumps(decoded, ensure_ascii=False),
                            raw_response=decoded,
                            market_date=batch_item.get('market_date') or market_date.isoformat(),
                            symbol=symbol,
                            agent_name=spec.agent_name,
                        )
                        symbol_items.append(
                            {
                                'symbol': symbol,
                                'feedback': normalized_feedback,
                            }
                        )
                    decoded = {
                        'items': symbol_items,
                    }
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

    @staticmethod
    def _build_upstream_feedback_bundle(
        *,
        symbol: str,
        market_date: date,
        buffered_lookup: dict[tuple[str, str], dict[str, Any]],
        upstream_reuse_decisions: dict[str, dict[str, Any]],
    ) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], list[str]]:
        upstream_summaries: dict[str, dict[str, Any]] = {}
        upstream_traceability: dict[str, dict[str, Any]] = {}
        missing_roles: list[str] = []
        symbol_decisions = upstream_reuse_decisions.get(symbol, {})

        for role in UPSTREAM_SUMMARY_ROLES:
            buffered_row = buffered_lookup.get((symbol, role))
            if buffered_row is not None:
                feedback_payload = buffered_row.get('feedback') if isinstance(buffered_row.get('feedback'), dict) else None
                if feedback_payload:
                    upstream_summaries[role] = feedback_payload
                    upstream_traceability[role] = {
                        'agent_name': role,
                        'run_id': buffered_row.get('run_id'),
                        'market_date': market_date.isoformat(),
                        'source': 'current_run_buffer',
                    }
                    continue

            decision = symbol_decisions.get(role)
            if decision is not None and bool(decision.should_dispatch):
                missing_roles.append(role)
                continue
            latest_feedback = decision.latest_feedback if decision is not None else None
            feedback_payload = latest_feedback.get('feedback_payload') if isinstance(latest_feedback, dict) else None
            feedback_market_date = latest_feedback.get('market_date') if isinstance(latest_feedback, dict) else None
            if isinstance(feedback_payload, dict) and feedback_market_date == market_date.isoformat():
                upstream_summaries[role] = feedback_payload
                upstream_traceability[role] = {
                    'agent_name': role,
                    'run_id': latest_feedback.get('run_id'),
                    'market_date': feedback_market_date,
                    'received_at': latest_feedback.get('received_at'),
                    'feedback_id': latest_feedback.get('id'),
                    'source': 'persisted_feedback',
                }
                continue

            missing_roles.append(role)

        return upstream_summaries, upstream_traceability, missing_roles

    @staticmethod
    def _build_signal_decider_payload(
        *,
        item: dict[str, Any],
        market_date: date,
        request_run_id: str,
        upstream_summaries: dict[str, dict[str, Any]],
        upstream_traceability: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        symbol_day_context = jsonable_encoder(_strip_runtime_session_fields(item))
        requested_at = datetime.now(timezone.utc).isoformat()
        return {
            'role': SIGNAL_DECIDER_AGENT_NAME,
            'agent_name': SIGNAL_DECIDER_AGENT_NAME,
            'symbol': str(item.get('symbol') or ''),
            'market_date': market_date.isoformat(),
            'symbol_day_context': symbol_day_context,
            'upstream_summaries': jsonable_encoder(upstream_summaries),
            'upstream_traceability': jsonable_encoder(upstream_traceability),
            'decision_support_constraints': {
                'decision_support_only': True,
                'allow_execution_logic': False,
                'allow_position_sizing': False,
                'allow_leverage_guidance': False,
                'avoid_fake_precision': True,
            },
            'orchestration': {
                'request_run_id': request_run_id,
                'requested_at': requested_at,
                'request_model': f'openclaw/{SIGNAL_DECIDER_AGENT_NAME}',
                'source': 'agent_feedback_orchestrator.dispatch_for_symbols',
                'mode': 'role_sequential_runtime_buffered',
                'required_upstream_roles': list(UPSTREAM_SUMMARY_ROLES),
            },
        }

    def _prepare_role_dispatch_items(
        self,
        *,
        role: str,
        market_date: date,
        request_run_id: str,
        symbol_context_items: Sequence[dict[str, Any]],
        reuse_decisions: dict[str, dict[str, Any]],
        upstream_reuse_decisions: dict[str, dict[str, Any]],
        all_failures: list[dict[str, Any]],
        buffer_failure,
    ) -> list[dict[str, Any]]:
        buffered_lookup = {}
        if role == SIGNAL_DECIDER_AGENT_NAME:
            snapshot = runtime_feedback_state_store.get_run(market_date, run_id=request_run_id) or RuntimeFeedbackRunState(
                run_id=request_run_id,
                market_date=market_date,
                requested_symbols=[],
                requested_roles=[],
                role_sequence=[],
                parallel_per_role=0,
                batch_size=1,
                started_at=datetime.now(timezone.utc),
            )
            buffered_lookup = _feedback_row_lookup(snapshot.buffered_feedback_rows)

        dispatch_items: list[dict[str, Any]] = []
        for item in symbol_context_items:
            symbol = str(item.get('symbol') or '')
            decision = reuse_decisions.get(symbol, {}).get(role)
            if decision is not None and not decision.should_dispatch:
                continue
            if role != SIGNAL_DECIDER_AGENT_NAME:
                dispatch_items.append({**item})
                continue

            upstream_summaries, upstream_traceability, missing_roles = self._build_upstream_feedback_bundle(
                symbol=symbol,
                market_date=market_date,
                buffered_lookup=buffered_lookup,
                upstream_reuse_decisions=upstream_reuse_decisions,
            )
            if missing_roles:
                error = f"missing_decider_upstream_summaries:{','.join(missing_roles)}"
                failure = {
                    'role': role,
                    'agent_name': role,
                    'symbol': symbol,
                    'status': 'failed',
                    'retryable': True,
                    'error': error,
                }
                all_failures.append(failure)
                buffer_failure(role, symbol, error, True)
                _feedback_runtime_log(
                    'decider_prerequisite_missing',
                    market_date=market_date.isoformat(),
                    request_run_id=request_run_id,
                    role=role,
                    symbol=symbol,
                    missing_roles=missing_roles,
                )
                continue
            dispatch_items.append(
                self._build_signal_decider_payload(
                    item=item,
                    market_date=market_date,
                    request_run_id=request_run_id,
                    upstream_summaries=upstream_summaries,
                    upstream_traceability=upstream_traceability,
                )
            )
        return dispatch_items

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

    @staticmethod
    def _session_key_from_response(response: dict[str, Any] | None) -> str | None:
        session_ref = response.get('session_ref') if isinstance(response, dict) else None
        if not isinstance(session_ref, dict):
            return None
        session_key = session_ref.get('session_key')
        if not isinstance(session_key, str) or not session_key.strip():
            return None
        return session_key.strip()

    def _cleanup_feedback_session(
        self,
        *,
        market_date: date,
        symbol: str,
        agent_name: str,
        run_id: str | None,
        response: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        session_key = self._session_key_from_response(response)
        if session_key is None:
            return None
        return agent_feedback_session_cleanup_service.delete_now(
            target=SessionCleanupTarget(
                market_date=market_date,
                symbol=symbol,
                agent_name=agent_name,
                run_id=run_id,
                session_key=session_key,
            )
        )

    def _cleanup_buffered_feedback_sessions(self, *, rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
        cleanup_results: list[dict[str, Any]] = []
        seen_session_keys: set[str] = set()
        for row in rows:
            response = row.get('summary') if isinstance(row.get('summary'), dict) else None
            session_key = self._session_key_from_response(response)
            if session_key is None or session_key in seen_session_keys:
                continue
            cleanup_result = self._cleanup_feedback_session(
                market_date=row['market_date'],
                symbol=str(row.get('symbol') or ''),
                agent_name=str(row.get('agent_name') or ''),
                run_id=row.get('run_id'),
                response=response,
            )
            if cleanup_result is not None:
                cleanup_results.append(cleanup_result)
            seen_session_keys.add(session_key)
        return cleanup_results

    def _cleanup_persisted_run_sessions(
        self,
        *,
        market_date: date,
        request_run_id: str,
        role: str | None = None,
    ) -> list[dict[str, Any]]:
        state = runtime_feedback_state_store.get_run(market_date, run_id=request_run_id)
        if state is None:
            return []

        rows_to_cleanup: list[dict[str, Any]] = []
        cleaned_session_keys = set(state.cleaned_session_keys)
        for row in state.buffered_feedback_rows:
            agent_name = str(row.get('agent_name') or '')
            if role is not None and agent_name != role:
                continue
            response = row.get('summary') if isinstance(row.get('summary'), dict) else None
            session_key = self._session_key_from_response(response)
            if session_key is None or session_key in cleaned_session_keys:
                continue
            rows_to_cleanup.append(row)
            cleaned_session_keys.add(session_key)

        if not rows_to_cleanup:
            return []

        cleanup_results = self._cleanup_buffered_feedback_sessions(rows=rows_to_cleanup)
        session_keys = {
            session_key
            for session_key in (
                self._session_key_from_response(row.get('summary') if isinstance(row.get('summary'), dict) else None)
                for row in rows_to_cleanup
            )
            if session_key is not None
        }

        def _mark_cleaned(current: RuntimeFeedbackRunState) -> None:
            current.cleaned_session_keys.update(session_keys)

        runtime_feedback_state_store.update_run(market_date, _mark_cleaned, run_id=request_run_id)
        return cleanup_results

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
        cleanup_result = self._cleanup_feedback_session(
            market_date=market_date,
            symbol=str(payload.get('symbol') or ''),
            agent_name=agent_name,
            run_id=run_id,
            response=response,
        )
        if cleanup_result is not None:
            result['item']['session_cleanup'] = cleanup_result
        return result['item']

    def _build_run_summary_payload(
        self,
        *,
        market_date: date,
        request_payload: dict[str, Any],
        state: RuntimeFeedbackRunState,
        lifecycle_stage: str,
        current_role: str | None = None,
        flush_result: dict[str, Any] | None = None,
        fatal_error: str | None = None,
    ) -> dict[str, Any]:
        progress_summary = state.to_progress_summary()
        role_progress = progress_summary.get('role_progress') if isinstance(progress_summary.get('role_progress'), dict) else {}
        return {
            'market_date': market_date.isoformat(),
            'request': request_payload,
            'lifecycle_stage': lifecycle_stage,
            'current_role': current_role or state.current_role,
            'role_sequence': list(state.role_sequence),
            'role_statuses': {
                role: role_payload.get('status')
                for role, role_payload in role_progress.items()
                if isinstance(role_payload, dict)
            },
            'progress_summary': progress_summary,
            'buffered_feedback_count': len(state.buffered_feedback_rows),
            'buffered_failure_count': len(state.buffered_failures),
            'pending_feedback_count': len(state.pending_feedback_rows),
            'pending_failure_count': len(state.pending_failures),
            'flushed_feedback_count': state.flushed_feedback_count,
            'flushed_failure_count': state.flushed_failure_count,
            'reused_symbols_by_role': {
                role: sorted(symbols)
                for role, symbols in state.reused_symbols_by_role.items()
            },
            'flush_result': flush_result,
            'fatal_error': fatal_error,
            'last_activity_at': state.last_activity_at.isoformat() if state.last_activity_at else None,
        }

    def _checkpoint_run_state(
        self,
        *,
        market_date: date,
        request_run_id: str,
        request_payload: dict[str, Any],
        lifecycle_stage: str,
        current_role: str | None = None,
        status: str = 'running',
        flush_result: dict[str, Any] | None = None,
        fatal_error: str | None = None,
    ) -> dict[str, Any]:
        state = runtime_feedback_state_store.get_run(market_date, run_id=request_run_id)
        if state is None:
            raise RuntimeError(f'feedback runtime state missing for {request_run_id}')
        summary_payload = self._build_run_summary_payload(
            market_date=market_date,
            request_payload=request_payload,
            state=state,
            lifecycle_stage=lifecycle_stage,
            current_role=current_role,
            flush_result=flush_result,
            fatal_error=fatal_error,
        )
        completed_count = int(summary_payload['progress_summary'].get('completed_feedback_count') or 0)
        failed_count = int(summary_payload['progress_summary'].get('failed_items') or 0)
        return agent_context_feedback_run_state_service.checkpoint_run(
            run_id=request_run_id,
            status=status,
            completed_count=completed_count,
            failed_count=failed_count,
            summary_payload=summary_payload,
        )

    def _flush_and_checkpoint_pending_run_buffers(
        self,
        *,
        market_date: date,
        request_run_id: str,
        request_payload: dict[str, Any],
        reason: str,
        lifecycle_stage: str,
        current_role: str | None,
        status: str = 'running',
    ) -> dict[str, Any]:
        flush_result = self._flush_pending_run_buffers(
            market_date=market_date,
            request_run_id=request_run_id,
            reason=reason,
        )
        if flush_result['feedback_rows_seen'] or flush_result['failure_rows_seen']:
            self._checkpoint_run_state(
                market_date=market_date,
                request_run_id=request_run_id,
                request_payload=request_payload,
                lifecycle_stage=lifecycle_stage,
                current_role=current_role,
                status=status,
                flush_result=flush_result,
            )
        return flush_result

    def _flush_pending_run_buffers(
        self,
        *,
        market_date: date,
        request_run_id: str,
        reason: str,
    ) -> dict[str, Any]:
        state = runtime_feedback_state_store.get_run(market_date, run_id=request_run_id)
        if state is None:
            raise RuntimeError(f'feedback runtime state missing for {request_run_id}')

        pending_feedback_rows = list(state.pending_feedback_rows)
        pending_failures = list(state.pending_failures)
        flush_result: dict[str, Any] = {
            'reason': reason,
            'feedback_rows_seen': len(pending_feedback_rows),
            'failure_rows_seen': len(pending_failures),
            'feedback_rows_persisted': 0,
            'failure_rows_persisted': 0,
            'feedback_error': None,
            'failure_error': None,
            'status': 'noop',
        }
        if not pending_feedback_rows and not pending_failures:
            return flush_result

        if pending_feedback_rows:
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
                for row in pending_feedback_rows
            ]
            feedback_result = agent_context_feedback_service.bulk_submit_feedback(items=flush_payload)

            def _clear_feedback(current: RuntimeFeedbackRunState) -> None:
                current.pending_feedback_rows = current.pending_feedback_rows[len(pending_feedback_rows):]
                current.buffered_feedback_rows.extend(pending_feedback_rows)
                for row in pending_feedback_rows:
                    symbol = str(row.get('symbol') or '')
                    agent_name = str(row.get('agent_name') or '')
                    if symbol and agent_name:
                        current.completed_symbols_by_role.setdefault(agent_name, set()).add(symbol)
                current.flushed_feedback_count += len(pending_feedback_rows)
                current.last_activity_at = datetime.now(timezone.utc)

            runtime_feedback_state_store.update_run(market_date, _clear_feedback, run_id=request_run_id)
            flush_result['feedback_rows_persisted'] = int(feedback_result.get('count', 0))
            flush_result['feedback_run_ids'] = [
                item.get('agent', {}).get('run_id')
                for item in feedback_result.get('items', [])
                if isinstance(item, dict)
            ]
            flush_result['status'] = 'feedback_flushed'

        if pending_failures:
            persisted_failures = []
            try:
                for failure in pending_failures:
                    persisted_failures.append(
                        agent_context_feedback_failure_service.record_failure(
                            market_date=market_date,
                            symbol=str(failure.get('symbol') or ''),
                            agent_name=str(failure.get('agent_name') or failure.get('role') or ''),
                            run_id=request_run_id,
                            failure_kind=str(failure.get('failure_kind') or failure.get('error') or 'dispatch_failed'),
                            retryable=bool(failure.get('retryable', True)),
                            summary={
                                'role': failure.get('role'),
                                'status': failure.get('status'),
                                'error': failure.get('error'),
                                'retryable': failure.get('retryable', True),
                                'failure_kind': failure.get('failure_kind') or failure.get('error') or 'dispatch_failed',
                            },
                            context=failure.get('context') if isinstance(failure.get('context'), dict) else None,
                        )
                    )
            except Exception as exc:  # noqa: BLE001
                flush_result['failure_error'] = f'{type(exc).__name__}: {exc}'
                flush_result['status'] = 'partial_flush_failed' if flush_result['feedback_rows_persisted'] else 'failed'
                raise
            else:
                def _clear_failures(current: RuntimeFeedbackRunState) -> None:
                    current.pending_failures = current.pending_failures[len(pending_failures):]
                    current.buffered_failures.extend(pending_failures)
                    for failure in pending_failures:
                        role = str(failure.get('role') or failure.get('agent_name') or '')
                        symbol = str(failure.get('symbol') or '')
                        if role and symbol:
                            current.failed_symbols_by_role.setdefault(role, set()).add(symbol)
                    current.flushed_failure_count += len(pending_failures)
                    current.last_activity_at = datetime.now(timezone.utc)

                runtime_feedback_state_store.update_run(market_date, _clear_failures, run_id=request_run_id)
                flush_result['failure_rows_persisted'] = len(persisted_failures)
                flush_result['failure_ids'] = [item.get('id') for item in persisted_failures if isinstance(item, dict)]
                flush_result['status'] = 'flushed'

        return flush_result

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
        specs = self._select_specs(roles=roles)
        requested_symbols = _normalize_requested_symbols(symbols)
        dispatch_symbols, skipped_symbols = _split_requested_symbols_by_piotroski_passed_filter(
            market_date=normalized_market_date,
            requested_symbols=requested_symbols,
        )
        request_run_id = f'symbol-run-{normalized_market_date.isoformat()}-{uuid.uuid4().hex[:10]}'
        batch_size = max(1, settings.openclaw_agent_symbol_chunk_size)
        role_parallelism = max(1, settings.openclaw_agent_parallel_subtasks)
        request_payload = {
            'market_date': normalized_market_date.isoformat(),
            'symbols': list(dispatch_symbols),
            'requested_symbols': requested_symbols,
            'request_run_id': request_run_id,
            'roles': [spec.role for spec in specs],
            'fs_div': fs_div,
            'disclosures_limit': disclosures_limit,
            'symbol_chunk_size': batch_size,
            'parallel_subtasks': role_parallelism,
            'role_parallel_subtasks': role_parallelism,
            'total_parallel_subtasks': role_parallelism,
            'reuse_existing': reuse_existing,
            'scope': 'piotroski_passed_symbols_only',
            'skipped_symbols': skipped_symbols,
            'mode': 'role_sequential_runtime_buffered',
        }
        state = RuntimeFeedbackRunState(
            run_id=request_run_id,
            market_date=normalized_market_date,
            requested_symbols=dispatch_symbols,
            requested_roles=[spec.role for spec in specs],
            role_sequence=[spec.role for spec in specs],
            parallel_per_role=role_parallelism,
            batch_size=batch_size,
            started_at=datetime.now(timezone.utc),
            current_role=specs[0].role if specs else None,
        )
        runtime_feedback_state_store.start_run(state)
        agent_context_feedback_run_state_service.start_run(
            run_id=request_run_id,
            market_date=normalized_market_date,
            symbols_total=len(dispatch_symbols),
            request_payload=request_payload,
            summary_payload=self._build_run_summary_payload(
                market_date=normalized_market_date,
                request_payload=request_payload,
                state=state,
                lifecycle_stage='run_started',
                current_role=state.current_role,
            ),
        )
        _feedback_runtime_log(
            'run_started',
            market_date=normalized_market_date.isoformat(),
            request_run_id=request_run_id,
            requested_symbols=dispatch_symbols,
            skipped_symbols=skipped_symbols,
            requested_roles=[spec.role for spec in specs],
            batch_size=batch_size,
            parallel_subtasks=role_parallelism,
        )

        def mark_activity(role: str, slot: int, batch_symbols: list[str] | None = None, done: bool = False) -> None:
            def _update(current: RuntimeFeedbackRunState) -> None:
                current.current_role = role
                current.status = 'running'
                current.last_activity_at = datetime.now(timezone.utc)
                role_slots = current.in_flight_slots_by_role.setdefault(role, {})
                if done:
                    role_slots.pop(slot, None)
                else:
                    role_slots[slot] = {
                        'symbols': batch_symbols or [],
                        'updated_at': current.last_activity_at.isoformat(),
                    }
            runtime_feedback_state_store.update_run(normalized_market_date, _update, run_id=request_run_id)
            _feedback_runtime_log(
                'slot_activity',
                market_date=normalized_market_date.isoformat(),
                request_run_id=request_run_id,
                role=role,
                slot=slot,
                batch_symbols=batch_symbols or [],
                done=done,
            )

        def mark_reused(role: str, symbols_to_reuse: Sequence[str]) -> None:
            reused_symbols = list(symbols_to_reuse)
            if not reused_symbols:
                return

            def _update(current: RuntimeFeedbackRunState) -> None:
                current.current_role = role
                current.status = 'running'
                current.last_activity_at = datetime.now(timezone.utc)
                current.completed_symbols_by_role.setdefault(role, set()).update(reused_symbols)
                current.reused_symbols_by_role.setdefault(role, set()).update(reused_symbols)

            runtime_feedback_state_store.update_run(normalized_market_date, _update, run_id=request_run_id)
            _feedback_runtime_log(
                'reuse_applied',
                market_date=normalized_market_date.isoformat(),
                request_run_id=request_run_id,
                role=role,
                reused_symbols=reused_symbols,
            )

        def buffer_success(role: str, symbol: str, context: dict[str, Any], response: dict[str, Any]) -> None:
            normalized_feedback = response.get('normalized_feedback')

            def _update(current: RuntimeFeedbackRunState) -> None:
                current.last_activity_at = datetime.now(timezone.utc)
                stored_row = {
                    'market_date': normalized_market_date,
                    'symbol': symbol,
                    'agent_name': role,
                    'run_id': request_run_id,
                    'summary': jsonable_encoder(response),
                    'feedback': normalized_feedback if isinstance(normalized_feedback, dict) else {},
                    'context': jsonable_encoder(context),
                }
                current.pending_feedback_rows.append(stored_row)
            runtime_feedback_state_store.update_run(normalized_market_date, _update, run_id=request_run_id)
            snapshot = runtime_feedback_state_store.get_run(normalized_market_date, run_id=request_run_id)
            _feedback_runtime_log(
                'buffer_state_after_success',
                market_date=normalized_market_date.isoformat(),
                request_run_id=request_run_id,
                role=role,
                symbol=symbol,
                buffered_feedback_count=len(snapshot.buffered_feedback_rows) if snapshot else None,
                buffered_failure_count=len(snapshot.buffered_failures) if snapshot else None,
                pending_feedback_count=len(snapshot.pending_feedback_rows) if snapshot else None,
                pending_failure_count=len(snapshot.pending_failures) if snapshot else None,
                completed_symbols_by_role={k: len(v) for k, v in snapshot.completed_symbols_by_role.items()} if snapshot else None,
            )

        def buffer_failure(
            role: str,
            symbol: str,
            error: str,
            retryable: bool,
            *,
            context: dict[str, Any] | None = None,
            agent_name: str | None = None,
        ) -> None:
            def _update(current: RuntimeFeedbackRunState) -> None:
                current.last_activity_at = datetime.now(timezone.utc)
                failure_row = {
                    'role': role,
                    'agent_name': agent_name or role,
                    'symbol': symbol,
                    'status': 'failed',
                    'retryable': retryable,
                    'error': error,
                    'failure_kind': error,
                    'context': jsonable_encoder(context) if isinstance(context, dict) else None,
                }
                current.pending_failures.append(failure_row)

            runtime_feedback_state_store.update_run(normalized_market_date, _update, run_id=request_run_id)
            snapshot = runtime_feedback_state_store.get_run(normalized_market_date, run_id=request_run_id)
            _feedback_runtime_log(
                'buffer_state_after_failure',
                market_date=normalized_market_date.isoformat(),
                request_run_id=request_run_id,
                role=role,
                symbol=symbol,
                error=error,
                retryable=retryable,
                buffered_feedback_count=len(snapshot.buffered_feedback_rows) if snapshot else None,
                buffered_failure_count=len(snapshot.buffered_failures) if snapshot else None,
                pending_feedback_count=len(snapshot.pending_feedback_rows) if snapshot else None,
                pending_failure_count=len(snapshot.pending_failures) if snapshot else None,
                failed_symbols_by_role={k: len(v) for k, v in snapshot.failed_symbols_by_role.items()} if snapshot else None,
            )

        all_items: list[dict[str, Any]] = []
        all_failures: list[dict[str, Any]] = []
        symbol_context = {'items': []}
        response_payload: dict[str, Any] | None = None
        raised_exception: Exception | None = None
        finalization_error: Exception | None = None
        final_status = 'failed'
        final_flush_result: dict[str, Any] | None = None

        try:
            if not dispatch_symbols:
                final_status = 'completed'
                response_payload = {
                    'request': request_payload,
                    'summary': {
                        'status': 'skipped',
                        'completed': 0,
                        'failed': 0,
                        'skipped': len(skipped_symbols),
                        'flushed_to_db': 0,
                    },
                    'items': [],
                    'failures': [],
                }
            else:
                reuse_decisions = {}
                if reuse_existing:
                    reuse_decisions = agent_context_reuse_service.build_latest_decisions(
                        market_date=normalized_market_date,
                        symbols=dispatch_symbols,
                        roles=[spec.role for spec in specs],
                    )
                upstream_reuse_decisions = {}
                if any(spec.role == SIGNAL_DECIDER_AGENT_NAME for spec in specs):
                    upstream_reuse_decisions = agent_context_reuse_service.build_latest_decisions(
                        market_date=normalized_market_date,
                        symbols=dispatch_symbols,
                        roles=UPSTREAM_SUMMARY_ROLES,
                    )
                symbols_requiring_context = dispatch_symbols
                if reuse_decisions:
                    symbols_requiring_context = [
                        symbol
                        for symbol in dispatch_symbols
                        if any(
                            decision is None or decision.should_dispatch
                            for decision in (
                                reuse_decisions.get(symbol, {}).get(spec.role)
                                for spec in specs
                            )
                        )
                    ]
                if symbols_requiring_context:
                    symbol_context = agent_context_read_service.get_symbol_day_context(
                        market_date=normalized_market_date,
                        symbols=symbols_requiring_context,
                        fs_div=fs_div,
                        disclosures_limit=disclosures_limit,
                    )
                self._checkpoint_run_state(
                    market_date=normalized_market_date,
                    request_run_id=request_run_id,
                    request_payload=request_payload,
                    lifecycle_stage='dispatch_initialized',
                    current_role=state.current_role,
                    status='running',
                )

                for spec in specs:
                    role = spec.role

                    def _set_current_role(current: RuntimeFeedbackRunState) -> None:
                        current.current_role = role
                        current.status = 'running'
                        current.last_activity_at = datetime.now(timezone.utc)

                    runtime_feedback_state_store.update_run(
                        normalized_market_date,
                        _set_current_role,
                        run_id=request_run_id,
                    )
                    self._checkpoint_run_state(
                        market_date=normalized_market_date,
                        request_run_id=request_run_id,
                        request_payload=request_payload,
                        lifecycle_stage='role_started',
                        current_role=role,
                        status='running',
                    )

                    reused_symbols = [
                        symbol
                        for symbol in dispatch_symbols
                        if (reuse_decisions.get(symbol, {}).get(role) is not None)
                        and not reuse_decisions[symbol][role].should_dispatch
                    ]
                    mark_reused(role, reused_symbols)

                    dispatch_items = self._prepare_role_dispatch_items(
                        role=role,
                        market_date=normalized_market_date,
                        request_run_id=request_run_id,
                        symbol_context_items=symbol_context['items'],
                        reuse_decisions=reuse_decisions,
                        upstream_reuse_decisions=upstream_reuse_decisions,
                        all_failures=all_failures,
                        buffer_failure=buffer_failure,
                    )
                    self._flush_and_checkpoint_pending_run_buffers(
                        market_date=normalized_market_date,
                        request_run_id=request_run_id,
                        request_payload=request_payload,
                        reason=f'role_prepared:{role}',
                        lifecycle_stage='batch_persisted',
                        current_role=role,
                        status='running',
                    )
                    indexed_batches = [dispatch_items[i:i + batch_size] for i in range(0, len(dispatch_items), batch_size)]
                    queue: list[tuple[int, list[dict[str, Any]]]] = []
                    for batch_index, batch in enumerate(indexed_batches):
                        slot = (batch_index % role_parallelism) + 1
                        stable_session_key = f"agent:{spec.agent_name}:{_feedback_session_key(agent_name=spec.agent_name, slot=slot)}"
                        batch_with_session = [
                            {
                                **batch_item,
                                'session_slot': slot,
                                'session_key': stable_session_key,
                            }
                            for batch_item in batch
                        ]
                        queue.append((slot, batch_with_session))
                        _feedback_runtime_log(
                            'queue_batch',
                            market_date=normalized_market_date.isoformat(),
                            request_run_id=request_run_id,
                            role=role,
                            slot=slot,
                            session_key=stable_session_key,
                            batch_symbols=[str(item.get('symbol')) for item in batch_with_session],
                        )

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
                                should_submit_next = False
                                staged_batch_outcomes = False
                                try:
                                    batch_result = future.result()
                                    decoded_items = batch_result.get('decoded', {}).get('items', [])
                                    by_symbol = {
                                        item['symbol']: item for item in decoded_items if isinstance(item, dict) and item.get('symbol')
                                    }
                                    for source_item in batch:
                                        symbol = source_item['symbol']
                                        feedback_candidate = by_symbol.get(symbol, {}).get('feedback')
                                        if not feedback_candidate:
                                            failure = {
                                                'role': role,
                                                'agent_name': spec.agent_name,
                                                'symbol': symbol,
                                                'status': 'failed',
                                                'retryable': True,
                                                'error': 'missing_batch_feedback',
                                            }
                                            all_failures.append(failure)
                                            buffer_failure(
                                                role,
                                                symbol,
                                                'missing_batch_feedback',
                                                True,
                                                context=source_item,
                                                agent_name=spec.agent_name,
                                            )
                                            _feedback_runtime_log(
                                                'buffer_failure',
                                                market_date=normalized_market_date.isoformat(),
                                                request_run_id=request_run_id,
                                                role=role,
                                                agent_name=spec.agent_name,
                                                symbol=symbol,
                                                error='missing_batch_feedback',
                                                batch_result=batch_result,
                                            )
                                            staged_batch_outcomes = True
                                            continue
                                        normalized_feedback = normalize_agent_response(
                                            role=spec.role,
                                            output_text=json.dumps(feedback_candidate, ensure_ascii=False),
                                            raw_response=feedback_candidate if isinstance(feedback_candidate, dict) else {'feedback': feedback_candidate},
                                            market_date=source_item.get('market_date') or normalized_market_date.isoformat(),
                                            symbol=symbol,
                                            agent_name=spec.agent_name,
                                        )
                                        response_payload_item = _build_response_storage_payload(
                                            spec=spec,
                                            batch_result=batch_result,
                                            normalized_feedback=normalized_feedback,
                                            collector_mode='role_sequential_runtime_buffered',
                                        )
                                        buffer_success(spec.agent_name, symbol, source_item, response_payload_item)
                                        _feedback_runtime_log(
                                            'buffer_success',
                                            market_date=normalized_market_date.isoformat(),
                                            request_run_id=request_run_id,
                                            role=role,
                                            agent_name=spec.agent_name,
                                            symbol=symbol,
                                            response_payload=response_payload_item,
                                            source_item=source_item,
                                        )
                                        all_items.append(
                                            {
                                                'role': role,
                                                'agent_name': spec.agent_name,
                                                'symbol': symbol,
                                                'status': 'completed',
                                                'response': {'normalized_feedback': normalized_feedback},
                                            }
                                        )
                                    staged_batch_outcomes = True
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
                                        buffer_failure(
                                            role,
                                            source_item['symbol'],
                                            f'{type(exc).__name__}: {exc}',
                                            True,
                                            context=source_item,
                                            agent_name=spec.agent_name,
                                        )
                                        _feedback_runtime_log(
                                            'buffer_exception',
                                            market_date=normalized_market_date.isoformat(),
                                            request_run_id=request_run_id,
                                            role=role,
                                            agent_name=spec.agent_name,
                                            symbol=source_item['symbol'],
                                            error_type=type(exc).__name__,
                                            error=str(exc),
                                        )
                                    staged_batch_outcomes = True
                                try:
                                    if staged_batch_outcomes:
                                        self._flush_and_checkpoint_pending_run_buffers(
                                            market_date=normalized_market_date,
                                            request_run_id=request_run_id,
                                            request_payload=request_payload,
                                            reason=f'batch_completed:{role}:slot-{slot}',
                                            lifecycle_stage='batch_persisted',
                                            current_role=role,
                                            status='running',
                                        )
                                    should_submit_next = True
                                finally:
                                    mark_activity(role, slot, done=True)
                                if should_submit_next:
                                    submit_next()

                    role_flush_result = self._flush_pending_run_buffers(
                        market_date=normalized_market_date,
                        request_run_id=request_run_id,
                        reason=f'role_completed:{role}',
                    )
                    self._cleanup_persisted_run_sessions(
                        market_date=normalized_market_date,
                        request_run_id=request_run_id,
                        role=role,
                    )
                    self._checkpoint_run_state(
                        market_date=normalized_market_date,
                        request_run_id=request_run_id,
                        request_payload=request_payload,
                        lifecycle_stage='role_completed',
                        current_role=role,
                        status='running',
                        flush_result=role_flush_result,
                    )

                final_snapshot = runtime_feedback_state_store.get_run(normalized_market_date, run_id=request_run_id) or state
                completed_count = len(final_snapshot.buffered_feedback_rows)
                failed_count = len(final_snapshot.buffered_failures)
                final_status = 'completed' if failed_count == 0 else ('partial' if completed_count > 0 else 'failed')
                response_payload = {
                    'request': request_payload,
                    'summary': {
                        'status': final_status,
                        'completed': completed_count,
                        'failed': failed_count,
                        'skipped': len(skipped_symbols),
                        'flushed_to_db': final_snapshot.flushed_feedback_count,
                    },
                    'items': all_items,
                    'failures': all_failures,
                }
        except Exception as exc:  # noqa: BLE001
            raised_exception = exc
        finally:
            final_snapshot = runtime_feedback_state_store.get_run(normalized_market_date, run_id=request_run_id) or state
            try:
                final_flush_result = self._flush_pending_run_buffers(
                    market_date=normalized_market_date,
                    request_run_id=request_run_id,
                    reason='finalize',
                )
            except Exception as exc:  # noqa: BLE001
                finalization_error = exc
                _feedback_runtime_log(
                    'finalize_failed',
                    market_date=normalized_market_date.isoformat(),
                    request_run_id=request_run_id,
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
            finally:
                try:
                    self._cleanup_persisted_run_sessions(
                        market_date=normalized_market_date,
                        request_run_id=request_run_id,
                    )
                except Exception as cleanup_exc:  # noqa: BLE001
                    _feedback_runtime_log(
                        'cleanup_persisted_run_sessions_failed',
                        market_date=normalized_market_date.isoformat(),
                        request_run_id=request_run_id,
                        error_type=type(cleanup_exc).__name__,
                        error=str(cleanup_exc),
                    )
            final_snapshot = runtime_feedback_state_store.get_run(normalized_market_date, run_id=request_run_id) or final_snapshot
            completed_count = len(final_snapshot.buffered_feedback_rows) if final_snapshot is not None else 0
            failed_count = len(final_snapshot.buffered_failures) if final_snapshot is not None else 0
            if raised_exception is not None or finalization_error is not None:
                final_status = 'partial' if (completed_count > 0 or failed_count > 0) else 'failed'
            elif dispatch_symbols:
                final_status = 'completed' if failed_count == 0 else ('partial' if completed_count > 0 else 'failed')
            else:
                final_status = 'completed'

            def _finish(current: RuntimeFeedbackRunState) -> None:
                current.status = final_status
                current.completed_at = datetime.now(timezone.utc)
                current.last_activity_at = current.completed_at
                for role_slots in current.in_flight_slots_by_role.values():
                    role_slots.clear()

            runtime_feedback_state_store.update_run(
                normalized_market_date,
                _finish,
                run_id=request_run_id,
            )
            final_snapshot = runtime_feedback_state_store.get_run(normalized_market_date, run_id=request_run_id) or final_snapshot or state
            _feedback_runtime_log(
                'run_finish_pending',
                market_date=normalized_market_date.isoformat(),
                request_run_id=request_run_id,
                final_status=final_status,
                completed_count=completed_count,
                failed_count=failed_count,
                final_flush_result=final_flush_result,
                raised_exception=(
                    {
                        'type': type(raised_exception).__name__,
                        'error': str(raised_exception),
                    }
                    if raised_exception is not None
                    else None
                ),
            )
            try:
                agent_context_feedback_run_state_service.finish_run(
                    run_id=request_run_id,
                    status=final_status,
                    completed_count=int(final_snapshot.to_progress_summary().get('completed_feedback_count') or 0),
                    failed_count=int(final_snapshot.to_progress_summary().get('failed_items') or 0),
                    summary_payload=self._build_run_summary_payload(
                        market_date=normalized_market_date,
                        request_payload=request_payload,
                        state=final_snapshot,
                        lifecycle_stage='finished',
                        current_role=final_snapshot.current_role,
                        flush_result=final_flush_result,
                        fatal_error=(
                            None
                            if raised_exception is None and finalization_error is None
                            else str(finalization_error or raised_exception)
                        ),
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                if finalization_error is None:
                    finalization_error = exc
            _feedback_runtime_log(
                'run_finished',
                market_date=normalized_market_date.isoformat(),
                request_run_id=request_run_id,
                final_status=final_status,
                completed_at=final_snapshot.completed_at.isoformat() if final_snapshot and final_snapshot.completed_at else None,
                run_summary=final_snapshot.to_progress_summary() if final_snapshot else None,
            )

            if response_payload is not None and dispatch_symbols:
                response_payload['summary']['status'] = final_status
                response_payload['summary']['completed'] = completed_count
                response_payload['summary']['failed'] = failed_count
                response_payload['summary']['flushed_to_db'] = final_snapshot.flushed_feedback_count if final_snapshot else 0

        if finalization_error is not None:
            if raised_exception is not None:
                raise finalization_error from raised_exception
            raise finalization_error
        if raised_exception is not None:
            raise raised_exception

        if response_payload is None:
            raise RuntimeError(f'No response payload generated for {request_run_id}')

        return response_payload

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
        requested_symbols = _normalize_requested_symbols(symbols)
        dispatch_symbols, skipped_symbols = _split_requested_symbols_by_piotroski_passed_filter(
            market_date=normalized_market_date,
            requested_symbols=requested_symbols,
        )
        specs = self._select_specs(roles=roles)
        if any(spec.role == SIGNAL_DECIDER_AGENT_NAME for spec in specs):
            raise ValueError('signal-decider is not supported via subagent fanout; use dispatch_for_symbols so upstream summaries are available in-sequence')
        request_run_id = f'subagent-symbol-run-{normalized_market_date.isoformat()}-{uuid.uuid4().hex[:10]}'
        if not dispatch_symbols:
            return {
                'request': {
                    'market_date': normalized_market_date.isoformat(),
                    'symbols': [],
                    'requested_symbols': requested_symbols,
                    'request_run_id': request_run_id,
                    'roles': [spec.role for spec in specs],
                    'fs_div': fs_div,
                    'disclosures_limit': disclosures_limit,
                    'per_role_parallelism': max(1, int(per_role_parallelism)),
                    'batch_size': 2,
                    'total_parallel_subtasks': max(1, len(specs) * max(1, int(per_role_parallelism))),
                    'scope': 'piotroski_passed_symbols_only',
                    'skipped_symbols': skipped_symbols,
                    'mode': 'slot_parent_subagent_fanout',
                },
                'summary': {
                    'status': 'skipped',
                    'completed': 0,
                    'failed': 0,
                    'skipped': len(skipped_symbols),
                },
                'items': [],
                'failures': [],
            }
        symbol_context = agent_context_read_service.get_symbol_day_context(
            market_date=normalized_market_date,
            symbols=dispatch_symbols,
            fs_div=fs_div,
            disclosures_limit=disclosures_limit,
        )
        items: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        batch_size = 2
        role_worker_count = max(1, int(per_role_parallelism))
        total_max_workers = max(1, len(specs) * role_worker_count)

        role_batches: list[tuple[AgentDispatchSpec, list[dict[str, Any]]]] = []
        for spec in specs:
            dispatch_items = [{**item} for item in symbol_context['items']]
            for batch_index, start in enumerate(range(0, len(dispatch_items), batch_size)):
                slot = (batch_index % role_worker_count) + 1
                batch = dispatch_items[start:start + batch_size]
                batch_with_session = [
                    {
                        **batch_item,
                        'session_slot': slot,
                        'session_key': _feedback_session_key(agent_name=spec.agent_name, slot=slot),
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
                        feedback_candidate = by_symbol.get(symbol, {}).get('feedback')
                        if not feedback_candidate:
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
                        normalized_feedback = normalize_agent_response(
                            role=spec.role,
                            output_text=json.dumps(feedback_candidate, ensure_ascii=False),
                            raw_response=feedback_candidate if isinstance(feedback_candidate, dict) else {'feedback': feedback_candidate},
                            market_date=source_item.get('market_date') or normalized_market_date.isoformat(),
                            symbol=symbol,
                            agent_name=spec.agent_name,
                        )
                        stored = self._store_feedback(
                            market_date=normalized_market_date,
                            agent_name=spec.agent_name,
                            run_id=request_run_id,
                            payload={'role': spec.role, 'symbol': symbol},
                            response=_build_response_storage_payload(
                                spec=spec,
                                batch_result=batch_result,
                                normalized_feedback=normalized_feedback,
                                collector_mode='slot_parent_subagent_fanout',
                            ),
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
                'symbols': list(dispatch_symbols),
                'requested_symbols': requested_symbols,
                'request_run_id': request_run_id,
                'roles': [spec.role for spec in specs],
                'fs_div': fs_div,
                'disclosures_limit': disclosures_limit,
                'per_role_parallelism': role_worker_count,
                'batch_size': batch_size,
                'total_parallel_subtasks': total_max_workers,
                'scope': 'piotroski_passed_symbols_only',
                'skipped_symbols': skipped_symbols,
                'mode': 'slot_parent_subagent_fanout',
            },
            'summary': {
                'status': status,
                'completed': len(items),
                'failed': len(failures),
                'skipped': len(skipped_symbols),
            },
            'items': items,
            'failures': failures,
        }


agent_feedback_orchestrator = AgentFeedbackOrchestrator()
