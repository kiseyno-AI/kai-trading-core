from __future__ import annotations

import concurrent.futures
import json
import logging
from pathlib import Path
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

from app.agent_context.decision_delta import build_previous_decision_context
from app.agent_context.feedback_service import agent_context_feedback_service
from app.agent_context.prerequisite_service import agent_feedback_prerequisite_service
from app.agent_context.eligibility import (
    AGENT_FEEDBACK_ELIGIBILITY_FILTERS,
    AGENT_FEEDBACK_ELIGIBILITY_RULE,
    build_agent_feedback_role_funnel,
    resolve_agent_feedback_scope,
    split_requested_symbols_by_agent_feedback_eligibility_with_diagnostics,
)
from app.agent_context.feedback_failure_service import agent_context_feedback_failure_service
from app.agent_context.feedback_failure_taxonomy import FeedbackFailureKind, classify_feedback_failure
from app.agent_context.orchestrator_helpers import (
    build_completed_dispatch_item as _build_completed_dispatch_item,
    build_failed_dispatch_item as _build_failed_dispatch_item,
    build_response_storage_payload as _build_response_storage_payload,
    build_signal_decider_payload as _build_signal_decider_payload_helper,
    build_upstream_feedback_bundle as _build_upstream_feedback_bundle_helper,
    coerce_decoded_batch_response as _coerce_decoded_batch_response,
    correlate_decoded_batch_response as _correlate_decoded_batch_response,
    extract_json_object as _extract_json_object,
    extract_response_mismatch_flags as _extract_response_mismatch_flags,
    repair_json_tail as _repair_json_tail,
    failure_log_fields as _failure_log_fields,
    feedback_row_lookup as _feedback_row_lookup,
    feedback_session_key as _feedback_session_key,
    fresh_feedback_session_key as _fresh_feedback_session_key,
    is_abortlike_error as _is_abortlike_error,
    normalize_requested_symbols as _normalize_requested_symbols,
    request_scope_from_items as _request_scope_from_items,
    resolve_dispatch_status as _resolve_dispatch_status,
    compressed_batch_item_for_prompt as _compressed_batch_item_for_prompt,
    slot_from_items as _slot_from_items,
    strip_runtime_session_fields as _strip_runtime_session_fields,
    subagent_test_session_key as _subagent_test_session_key,
    trim_batch_item_for_prompt as _trim_batch_item_for_prompt,
)
from app.agent_context.orchestrator_runtime_helpers import (
    build_buffer_counts as _build_buffer_counts,
    build_pending_failure_row as _build_pending_failure_row,
    build_pending_feedback_row as _build_pending_feedback_row,
    dispatch_and_store as _dispatch_and_store_helper,
    dispatch_for_symbols_via_subagents as _dispatch_for_symbols_via_subagents_helper,
    normalize_symbol_feedback as _normalize_symbol_feedback,
    resolve_batch_dispatch_context as _resolve_batch_dispatch_context,
)
from app.agent_context.logging import (
    build_normalized_feedback_snapshot,
    build_request_snapshot,
    build_response_snapshot,
    log_agent_feedback_event,
    make_attempt_id,
)
from app.agent_context.runtime_state import RuntimeFeedbackRunState, runtime_feedback_state_store
from app.agent_context.payload_builder import agent_payload_builder
from app.agent_context.read_service import agent_context_read_service
from app.agent_context.roles import DEFAULT_AGENT_NAMES, DECISION_SUPPORT_AGENT_NAMES, SHORT_TERM_DECIDER_AGENT_NAME, SIGNAL_DECIDER_AGENT_NAME, SUMMARIZER_AGENT_NAMES
from app.agent_context.run_state_service import agent_context_feedback_run_state_service
from app.agent_context.response_contract import build_contract_prompt_suffix, normalize_agent_response
from app.agent_context.reuse_service import (
    FUNDAMENTAL_SUMMARIZER_AGENT_NAME,
    FUNDAMENTAL_SUMMARY_REUSE_POLICY_VERSION,
    agent_context_reuse_service,
)
from app.agent_context.session_cleanup_service import SessionCleanupTarget, agent_feedback_session_cleanup_service
from app.agent_context.signal_decider_market_context import signal_decider_market_context_builder
from app.agent_context.signal_decider_loop_service import signal_decider_loop_service
from app.core.settings import settings
from app.db import session_scope
from app.db_models import AgentContextFeedback, EODCollectionRun

logger = logging.getLogger(__name__)


class AgentDispatchError(RuntimeError):
    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.details = details or {}


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
        role=SHORT_TERM_DECIDER_AGENT_NAME,
        agent_name=SHORT_TERM_DECIDER_AGENT_NAME,
        instruction=(
            'Synthesize the specialist summaries, market context, flow context, momentum, volume, price-action, and data-quality inputs into short-term tactical decision support only, '
            'classifying each symbol as scalp_buy_candidate, watch_for_trigger, avoid_chase, wait, or reject for a 1-2 trading day horizon. '
            'Prioritize liquidity, fresh participation, clean trigger/invalidation levels, and chase-risk control. '
            'Every watch_for_trigger, avoid_chase, or wait decision must name one primary wait_reason taxonomy blocker plus a concise actionable explanation. '
            'If previous_decision_context is supplied, summarize the delta instead of repeating the earlier wait explanation: state what changed, what stayed the same, whether the setup is persistent or improving or worsening, and the concrete trigger that upgrades the setup. '
            'Account for market_regime_context posture when choosing scalp_buy_candidate, watch_for_trigger, wait, or reject: risk_off defaults to wait/reject unless the candidate shows exceptional market/sector leadership; caution requires stronger evidence, trigger confirmation, and reduced-size language; weak sector context requires sector leadership or clear confirmation. '
            'Treat missing news/disclosure detail and missing detailed quarterly/cash-flow detail as auxiliary by default, not standalone blockers, unless the setup explicitly depends on them. '
            'Write all operator-facing feedback in Korean while staying JSON-only, and avoid execution logic, leverage, sizing, or fake precision.'
        ),
    ),
    AgentDispatchSpec(
        role=SIGNAL_DECIDER_AGENT_NAME,
        agent_name=SIGNAL_DECIDER_AGENT_NAME,
        instruction=(
            'Synthesize the three specialist summaries into conservative next-trading-day decision support only, '
            'choosing buy, wait, or reject while surfacing evidence, disagreement, invalidation, data-quality warnings, '
            'and explicit realistic target and stop-loss levels for every symbol. '
            'Every wait decision must name one primary wait_reason taxonomy blocker plus a concise actionable explanation. '
            'If previous_decision_context is supplied, summarize the delta instead of repeating the earlier wait explanation: state what changed, what stayed the same, whether the setup is persistent or improving or worsening, and the concrete trigger that upgrades the setup to buy. '
            'Account for market_regime_context posture when choosing buy, wait, or reject: risk_off defaults to wait/reject unless the candidate shows exceptional market/sector leadership; caution requires stronger evidence and reduced-size language; weak sector context requires sector leadership or clear confirmation. '
            'Treat missing news/disclosure detail and missing detailed quarterly/cash-flow detail as auxiliary by default, not standalone blockers, unless the thesis explicitly depends on them. '
            'Write all operator-facing feedback in Korean while staying JSON-only, and avoid execution logic, leverage, sizing, or fake precision.'
        ),
    ),
)


def _split_requested_symbols_by_agent_feedback_eligibility(
    *,
    market_date: date,
    requested_symbols: Sequence[str],
) -> tuple[list[str], list[str]]:
    eligible_symbols, skipped_symbols, _ = _split_requested_symbols_by_agent_feedback_eligibility_with_diagnostics(
        market_date=market_date,
        requested_symbols=requested_symbols,
    )
    return eligible_symbols, skipped_symbols


def _split_requested_symbols_by_agent_feedback_eligibility_with_diagnostics(
    *,
    market_date: date,
    requested_symbols: Sequence[str],
) -> tuple[list[str], list[str], dict[str, Any]]:
    return split_requested_symbols_by_agent_feedback_eligibility_with_diagnostics(
        market_date=market_date,
        requested_symbols=requested_symbols,
    )


def _public_eligibility_diagnostics(diagnostics: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in diagnostics.items()
        if key != 'records_by_symbol'
    }


def _attach_agent_feedback_eligibility_context(
    *,
    items: Sequence[dict[str, Any]],
    diagnostics: dict[str, Any],
) -> None:
    records_by_symbol = diagnostics.get('records_by_symbol') if isinstance(diagnostics, dict) else None
    if not isinstance(records_by_symbol, dict):
        return
    for item in items:
        symbol = str(item.get('symbol') or '').strip().upper()
        record = records_by_symbol.get(symbol)
        if isinstance(record, dict):
            item['agent_feedback_eligibility'] = record


def _normalize_role_symbol_scopes(
    *,
    roles: Sequence[str],
    symbols: Sequence[str],
    role_symbol_scopes: dict[str, Sequence[str]] | None,
) -> dict[str, list[str]]:
    fallback_symbols = _normalize_requested_symbols(symbols)
    normalized: dict[str, list[str]] = {}
    for role in roles:
        source = role_symbol_scopes.get(role) if isinstance(role_symbol_scopes, dict) else None
        if source is None:
            normalized[role] = list(fallback_symbols)
        else:
            normalized[role] = _normalize_requested_symbols(source)
    return normalized


def _ordered_union(values: Sequence[Sequence[str]]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for group in values:
        for symbol in group:
            normalized = str(symbol or '').strip().upper()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            ordered.append(normalized)
    return ordered


DEFAULT_OPENCLAW_AGENT_TIMEOUT_SECONDS = 120.0


class OpenClawAgentClient:
    def __init__(self) -> None:
        self.base_url = settings.openclaw_base_url.rstrip('/')
        self.gateway_token = settings.openclaw_gateway_token
        configured_timeout = float(settings.openclaw_agent_timeout_seconds or 0)
        self.timeout_seconds = (
            configured_timeout if configured_timeout > 0 else DEFAULT_OPENCLAW_AGENT_TIMEOUT_SECONDS
        )
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
        response_id = parsed.get('id')
        session_id = None
        if isinstance(parsed.get('session'), dict):
            session_id = parsed['session'].get('id') or parsed['session'].get('session_id')
        if not session_id and isinstance(parsed.get('metadata'), dict):
            session_id = parsed['metadata'].get('session_id')
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
        if not session_id and isinstance(result, dict):
            session_id = result.get('childSessionId') or result.get('sessionId')
        if not session_id and details:
            session_id = details.get('childSessionId') or details.get('sessionId')
        if session_key or session_id or response_id:
            return {
                'session_key': session_key,
                'session_id': session_id,
                'response_id': response_id,
            }
        return None

    @staticmethod
    def _message_text(message: dict[str, Any], *, allowed_types: set[str]) -> str:
        text_chunks: list[str] = []
        content = message.get('content')
        if isinstance(content, str) and content.strip():
            return content.strip()
        if not isinstance(content, list):
            return ''
        for item in content:
            if not isinstance(item, dict):
                continue
            if item.get('type') not in allowed_types:
                continue
            text = item.get('text')
            if isinstance(text, str) and text.strip():
                text_chunks.append(text)
        return '\n'.join(text_chunks).strip()

    @staticmethod
    def _decode_candidate_json(candidate_text: str | None) -> tuple[dict[str, Any] | None, str | None]:
        normalized_text = _extract_json_object(candidate_text)
        if not normalized_text:
            repaired_raw_text = _repair_json_tail(candidate_text)
            if not repaired_raw_text:
                return None, 'json_object_not_found'
            normalized_text = repaired_raw_text
        try:
            candidate_decoded = json.loads(normalized_text)
        except json.JSONDecodeError:
            repaired_text = _repair_json_tail(normalized_text)
            if not repaired_text:
                return None, 'json_decode_error'
            try:
                candidate_decoded = json.loads(repaired_text)
            except json.JSONDecodeError:
                return None, 'json_decode_error'
        if isinstance(candidate_decoded, list):
            return {'items': candidate_decoded}, None
        if isinstance(candidate_decoded, dict):
            return candidate_decoded, None
        return None, 'json_shape_not_object'

    @staticmethod
    def _failure_class_for_mode(failure_mode: str | None) -> str:
        if failure_mode in {
            None,
            'json_object_not_found',
            'json_decode_error',
            'json_shape_not_object',
            'session_history_assistant_non_json_after_marker',
            'trajectory_assistant_non_json',
        }:
            return 'invalid_json'
        return str(failure_mode)

    @staticmethod
    def _response_text_excerpt(response: httpx.Response | None, *, limit: int = 1000) -> str | None:
        if response is None:
            return None
        try:
            text = str(response.text or '')
        except Exception:  # noqa: BLE001
            return None
        text = text.strip()
        if not text:
            return None
        return text[:limit]

    def _build_dispatch_exception_details(
        self,
        *,
        exc: Exception,
        response: httpx.Response | None,
        request_run_id: str | None,
        dispatch_id: str,
        attempt_id: str | None,
        request_session_key: str,
        request_marker: str,
        slot: int | None,
        batch_symbols: Sequence[str],
        session_reuse_mode: str | None,
    ) -> dict[str, Any]:
        error_type = type(exc).__name__
        response_text_excerpt = self._response_text_excerpt(response)
        response_status_code = response.status_code if response is not None else None
        response_json_decode_error = isinstance(exc, json.JSONDecodeError) or error_type == 'JSONDecodeError'
        summary = {
            'origin': 'openclaw_batch_dispatch',
            'exception_type': error_type,
            'error': str(exc),
            'response_status_code': response_status_code,
            'response_json_decode_error': response_json_decode_error,
        }
        context = {
            'request_run_id': request_run_id,
            'dispatch_id': dispatch_id,
            'attempt_id': attempt_id,
            'session_key': request_session_key,
            'request_marker': request_marker,
            'slot': slot,
            'batch_symbols': list(batch_symbols),
            'session_reuse_mode': session_reuse_mode,
            'response_status_code': response_status_code,
            'response_text_excerpt': response_text_excerpt,
        }
        classification = classify_feedback_failure(
            failure_kind=(
                FeedbackFailureKind.OPENCLAW_RESPONSE_INVALID_JSON.value
                if response_json_decode_error
                else None
            ),
            summary=summary,
            context=context,
            error=str(exc),
            exception_type=error_type,
        )
        return {
            **context,
            'failure_kind': classification.failure_kind,
            'failure_status': classification.failure_status,
            'failure_category': classification.failure_category,
            'failure_classification': classification.as_payload(),
            'failure_mode': classification.failure_kind,
            'exception_type': error_type,
            'error_message': str(exc),
            'response_json_decode_error': response_json_decode_error,
        }

    @staticmethod
    def _build_structured_fallback_batch_response(
        *,
        spec: AgentDispatchSpec,
        market_date: date,
        batch_items: Sequence[dict[str, Any]],
        output_text: str | None,
        failure_class: str,
        failure_mode: str | None,
        json_failure_class: str | None,
        response_recovery: dict[str, Any],
    ) -> dict[str, Any]:
        summary = (
            '에이전트가 구조화 JSON을 반환하지 않아 Core가 보수적 fallback 피드백을 생성했습니다.'
        )
        data_warning = (
            f'agent_response_contract_violation: {failure_class}'
            + (f':{failure_mode}' if failure_mode and failure_mode != failure_class else '')
        )
        raw_output = str(output_text or '').strip()
        raw_output_excerpt = raw_output[:1000] if raw_output else None

        def _base_feedback(symbol: str) -> dict[str, Any]:
            details = {
                'fallback_generated_by_core': True,
                'fallback_reason': 'agent_response_not_parseable_json',
                'failure_class': failure_class,
                'failure_mode': failure_mode,
                'json_failure_class': json_failure_class,
                'response_recovery': response_recovery,
            }
            if raw_output_excerpt:
                details['raw_output_excerpt'] = raw_output_excerpt
            return {
                'role': spec.role,
                'agent_name': spec.agent_name,
                'symbol': symbol,
                'market_date': market_date.isoformat(),
                'summary': summary,
                'confidence': 0.0,
                'risk_flags': ['unstructured_response', 'agent_json_contract_violation'],
                'details': details,
                'improvement_notes': [
                    '에이전트는 raw JSON object만 반환해야 합니다.',
                    'Core fallback이 생성된 row는 매수 확신도에 긍정적으로 사용하면 안 됩니다.',
                ],
            }

        def _decision_feedback(symbol: str) -> dict[str, Any]:
            feedback = _base_feedback(symbol)
            wait_reason = {
                'code': 'core_execution_evidence_gap',
                'summary': '에이전트 응답이 JSON 계약을 위반해 구조화 판단 근거를 신뢰할 수 없습니다.',
            }
            return {
                **feedback,
                'preferred_action': 'wait',
                'next_day_open_action': 'wait',
                'wait_reason': wait_reason,
                'entry_price_basis': 'next_open',
                'target_price': None,
                'expected_return_pct': None,
                'stop_loss_price': None,
                'suggested_holding_period': None,
                'confidence_score_10': 1.0,
                'conviction_score_10': 1.0,
                'thesis_summary': summary,
                'supporting_evidence': [],
                'decision_risks': ['agent_json_contract_violation'],
                'decision_delta': {},
                'invalidation_conditions': [],
                'data_quality_warnings': [data_warning],
                'specialist_disagreement': [],
                'traceability': {
                    'agent_name': spec.agent_name,
                    'symbol': symbol,
                    'market_date': market_date.isoformat(),
                    'fallback_generated_by_core': True,
                },
            }

        def _short_term_feedback(symbol: str) -> dict[str, Any]:
            feedback = _base_feedback(symbol)
            wait_reason = {
                'code': 'core_execution_evidence_gap',
                'summary': '에이전트 응답이 JSON 계약을 위반해 단기 진입 판단 근거를 신뢰할 수 없습니다.',
            }
            return {
                **feedback,
                'tactical_action': 'wait',
                'preferred_action': 'wait',
                'wait_reason': wait_reason,
                'trigger_price': None,
                'chase_limit_price': None,
                'target_price': None,
                'stop_loss_price': None,
                'expected_holding_period': None,
                'confidence_score_10': 1.0,
                'conviction_score_10': 1.0,
                'thesis_summary': summary,
                'trigger_conditions': [],
                'supporting_evidence': [],
                'tactical_risks': ['agent_json_contract_violation'],
                'decision_delta': {},
                'invalidation_conditions': [],
                'data_quality_warnings': [data_warning],
                'traceability': {
                    'agent_name': spec.agent_name,
                    'symbol': symbol,
                    'market_date': market_date.isoformat(),
                    'fallback_generated_by_core': True,
                },
            }

        items = []
        for batch_item in batch_items:
            symbol = str(batch_item.get('symbol') or '').strip()
            if not symbol:
                continue
            if spec.role == SIGNAL_DECIDER_AGENT_NAME:
                feedback = _decision_feedback(symbol)
            elif spec.role == SHORT_TERM_DECIDER_AGENT_NAME:
                feedback = _short_term_feedback(symbol)
            else:
                feedback = _base_feedback(symbol)
            items.append({'symbol': symbol, 'feedback': feedback})
        return {'items': items}

    def _recover_from_session_history(
        self,
        *,
        session_history: list[dict[str, Any]],
        request_marker: str,
    ) -> dict[str, Any]:
        diagnostics: dict[str, Any] = {
            'checked': True,
            'message_count': len(session_history),
            'request_marker': request_marker,
            'marker_found': False,
            'assistant_messages_after_marker': 0,
            'assistant_text_messages_after_marker': 0,
            'failure_mode': None,
            'json_failure_class': None,
        }
        if not session_history:
            diagnostics['failure_mode'] = 'session_history_missing_or_empty'
            return {
                'decoded': None,
                'output_text': None,
                'diagnostics': diagnostics,
            }

        marker_index = None
        for idx, message in enumerate(session_history):
            if str(message.get('role') or '') != 'user':
                continue
            candidate_text = self._message_text(
                message,
                allowed_types={'text', 'input_text', 'output_text'},
            )
            if request_marker in candidate_text:
                marker_index = idx

        diagnostics['marker_found'] = marker_index is not None
        if marker_index is None:
            diagnostics['failure_mode'] = 'session_history_marker_not_found'
            return {
                'decoded': None,
                'output_text': None,
                'diagnostics': diagnostics,
            }

        last_json_failure = None
        for message in session_history[marker_index + 1:]:
            if str(message.get('role') or '') != 'assistant':
                continue
            diagnostics['assistant_messages_after_marker'] += 1
            candidate_text = self._message_text(
                message,
                allowed_types={'text', 'output_text'},
            )
            if not candidate_text:
                continue
            diagnostics['assistant_text_messages_after_marker'] += 1
            decoded, json_failure_class = self._decode_candidate_json(candidate_text)
            if decoded is not None:
                diagnostics['json_failure_class'] = None
                return {
                    'decoded': decoded,
                    'output_text': candidate_text,
                    'diagnostics': diagnostics,
                }
            last_json_failure = json_failure_class

        diagnostics['json_failure_class'] = last_json_failure
        if diagnostics['assistant_messages_after_marker'] == 0 or diagnostics['assistant_text_messages_after_marker'] == 0:
            diagnostics['failure_mode'] = 'session_history_assistant_missing_after_marker'
        else:
            diagnostics['failure_mode'] = 'session_history_assistant_non_json_after_marker'
        return {
            'decoded': None,
            'output_text': None,
            'diagnostics': diagnostics,
        }

    def _session_artifacts_dir(self, *, agent_name: str) -> Path:
        return Path(settings.openclaw_state_dir).expanduser() / 'agents' / agent_name / 'sessions'

    def _find_trajectory_file(
        self,
        *,
        agent_name: str,
        session_key: str,
    ) -> tuple[Path | None, dict[str, Any]]:
        sessions_dir = self._session_artifacts_dir(agent_name=agent_name)
        diagnostics: dict[str, Any] = {
            'sessions_dir': str(sessions_dir),
            'lookup_mode': None,
            'session_id': None,
            'trajectory_file': None,
        }
        if not sessions_dir.is_dir():
            diagnostics['failure_mode'] = 'trajectory_sessions_dir_missing'
            return None, diagnostics

        registry_path = sessions_dir / 'sessions.json'
        if registry_path.is_file():
            try:
                registry = json.loads(registry_path.read_text(encoding='utf-8'))
            except (OSError, json.JSONDecodeError) as exc:
                diagnostics['registry_error'] = f'{type(exc).__name__}: {exc}'
            else:
                entry = registry.get(session_key) if isinstance(registry, dict) else None
                if isinstance(entry, dict):
                    session_id = entry.get('sessionId')
                    diagnostics['session_id'] = session_id
                    if isinstance(session_id, str) and session_id.strip():
                        trajectory_path = sessions_dir / f'{session_id}.trajectory.jsonl'
                        if trajectory_path.is_file():
                            diagnostics['lookup_mode'] = 'sessions_registry'
                            diagnostics['trajectory_file'] = str(trajectory_path)
                            return trajectory_path, diagnostics

        for trajectory_path in sessions_dir.glob('*.trajectory.jsonl'):
            try:
                with trajectory_path.open('r', encoding='utf-8') as handle:
                    for _ in range(3):
                        line = handle.readline()
                        if not line:
                            break
                        if session_key not in line:
                            continue
                        try:
                            event = json.loads(line)
                        except json.JSONDecodeError:
                            event = {}
                        diagnostics['lookup_mode'] = 'trajectory_scan'
                        diagnostics['trajectory_file'] = str(trajectory_path)
                        diagnostics['session_id'] = event.get('sessionId')
                        return trajectory_path, diagnostics
            except OSError:
                continue

        diagnostics['failure_mode'] = 'trajectory_artifacts_not_found'
        return None, diagnostics

    def _recover_from_trajectory_artifacts(
        self,
        *,
        agent_name: str,
        session_key: str,
        response_id: str | None,
    ) -> dict[str, Any]:
        trajectory_path, diagnostics = self._find_trajectory_file(
            agent_name=agent_name,
            session_key=session_key,
        )
        diagnostics = {
            **diagnostics,
            'checked': True,
            'requested_response_id': response_id,
            'matched_response_id': False,
            'candidate_run_ids': [],
            'matched_event_type': None,
            'trajectory_run_id': None,
            'trajectory_final_status': None,
            'assistant_text_count': 0,
            'status': 'not_recovered',
            'failure_mode': diagnostics.get('failure_mode'),
            'json_failure_class': None,
        }
        if trajectory_path is None:
            return {
                'decoded': None,
                'output_text': None,
                'diagnostics': diagnostics,
            }

        candidate_events: list[dict[str, Any]] = []
        try:
            with trajectory_path.open('r', encoding='utf-8') as handle:
                for line in handle:
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if str(event.get('sessionKey') or '') != session_key:
                        continue
                    if diagnostics.get('session_id') is None and isinstance(event.get('sessionId'), str):
                        diagnostics['session_id'] = event.get('sessionId')
                    if str(event.get('type') or '') not in {'trace.artifacts', 'model.completed'}:
                        continue
                    data = event.get('data') if isinstance(event.get('data'), dict) else {}
                    assistant_texts = data.get('assistantTexts')
                    if not isinstance(assistant_texts, list) or not assistant_texts:
                        continue
                    candidate_events.append(
                        {
                            'type': str(event.get('type') or ''),
                            'run_id': str(event.get('runId') or ''),
                            'session_id': event.get('sessionId'),
                            'final_status': data.get('finalStatus') or data.get('status'),
                            'assistant_texts': [text for text in assistant_texts if isinstance(text, str) and text.strip()],
                        }
                    )
        except OSError as exc:
            diagnostics['failure_mode'] = 'trajectory_artifacts_unreadable'
            diagnostics['read_error'] = f'{type(exc).__name__}: {exc}'
            return {
                'decoded': None,
                'output_text': None,
                'diagnostics': diagnostics,
            }

        if not candidate_events:
            diagnostics['failure_mode'] = 'trajectory_assistant_texts_missing'
            return {
                'decoded': None,
                'output_text': None,
                'diagnostics': diagnostics,
            }

        diagnostics['candidate_run_ids'] = [event['run_id'] for event in candidate_events if event['run_id']]
        if response_id:
            preferred_events = [event for event in candidate_events if event['run_id'] == response_id]
            diagnostics['matched_response_id'] = bool(preferred_events)
            if not preferred_events:
                diagnostics['failure_mode'] = 'trajectory_response_id_mismatch'
                return {
                    'decoded': None,
                    'output_text': None,
                    'diagnostics': diagnostics,
                }
        else:
            preferred_events = candidate_events
        preferred_events.sort(
            key=lambda event: (
                0 if event['type'] == 'trace.artifacts' else 1,
                0 if response_id and event['run_id'] == response_id else 1,
            )
        )

        last_json_failure = None
        for event in preferred_events:
            assistant_texts = list(reversed(event['assistant_texts']))
            diagnostics['assistant_text_count'] = len(assistant_texts)
            diagnostics['matched_event_type'] = event['type']
            diagnostics['trajectory_run_id'] = event['run_id'] or None
            diagnostics['trajectory_final_status'] = event['final_status']
            if diagnostics.get('session_id') is None:
                diagnostics['session_id'] = event.get('session_id')
            for candidate_text in assistant_texts:
                decoded, json_failure_class = self._decode_candidate_json(candidate_text)
                if decoded is not None:
                    diagnostics['status'] = 'recovered'
                    diagnostics['failure_mode'] = None
                    diagnostics['json_failure_class'] = None
                    return {
                        'decoded': decoded,
                        'output_text': candidate_text,
                        'diagnostics': diagnostics,
                    }
                last_json_failure = json_failure_class

        diagnostics['json_failure_class'] = last_json_failure
        diagnostics['failure_mode'] = 'trajectory_assistant_non_json'
        return {
            'decoded': None,
            'output_text': None,
            'diagnostics': diagnostics,
        }

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
        assigned_symbols = [
            str(item.get('symbol') or '').strip()
            for item in batch_items
            if str(item.get('symbol') or '').strip()
        ]
        assigned_symbols_json = json.dumps(assigned_symbols, ensure_ascii=False)

        def _signal_decider_batch_rules(symbols: Sequence[str]) -> str:
            if spec.role not in DECISION_SUPPORT_AGENT_NAMES:
                return ''
            symbols_json = json.dumps(list(symbols), ensure_ascii=False)
            symbol_count = len(symbols)
            return (
                "Signal-decider hard batch contract:\n"
                f"- Assigned symbols are exactly {symbols_json}; output exactly {symbol_count} item(s), no more and no fewer.\n"
                "- The only top-level JSON shape allowed is {\"items\":[...]}; never return a bare feedback object or a top-level array.\n"
                "- Every item.symbol is mandatory and must exactly match one assigned symbol string.\n"
                "- Every feedback.symbol and feedback.traceability.symbol is mandatory and must exactly match its item.symbol.\n"
                "- One-symbol requests still require the same batch envelope with exactly one item; never collapse the response to a single feedback object.\n"
                "- All operator-facing natural-language fields must be written in Korean, while the overall output remains raw JSON only.\n"
                "- Every signal-decider item must include explicit target_price and stop_loss_price values; do not leave them unspecified unless the payload makes a justified level impossible, and in that rare case explain the limitation in data_quality_warnings.\n"
                "- Every wait-like conservative action must include a wait_reason object that names the primary normalized blocker instead of vague missing-information text.\n"
                "- If previous_decision_context is present for a symbol, every decision-support item must include decision_delta with prior-vs-current metadata, summarize changes instead of repeating the old wait explanation, and name the concrete upgrade trigger.\n"
                "- If context is weak, missing, conflicting, or truncated, emit a complete conservative wait or reject item instead of prose, placeholders, omissions, or malformed JSON.\n"
                "- Self-check before final answer: JSON parses; top-level object has items; item count matches assigned symbols; assigned symbols are exact and unique; no assigned symbol is missing; no unexpected symbol appears; required feedback fields exist.\n"
                "- Immediately before sending, validate the final raw JSON text again and fix any extra or missing closing braces/brackets so the response begins with {, ends with }, and parses successfully with no trailing prose.\n"
            )

        if spec.role == SHORT_TERM_DECIDER_AGENT_NAME:
            example_feedback = {
                'role': spec.role,
                'agent_name': spec.agent_name,
                'symbol': example_symbol,
                'market_date': market_date.isoformat(),
                'tactical_action': 'watch_for_trigger',
                'wait_reason': {
                    'code': 'unsuitable_execution_timing',
                    'summary': '돌파 트리거 확인 전이라 진입 시점이 아직 이릅니다.',
                },
                'trigger_price': 10300,
                'chase_limit_price': 10500,
                'target_price': 10900,
                'stop_loss_price': 9900,
                'expected_holding_period': '1-2 trading days',
                'confidence_score_10': 6,
                'conviction_score_10': 5,
                'thesis_summary': 'brief short-term tactical thesis',
                'trigger_conditions': ['example trigger condition'],
                'supporting_evidence': ['example evidence'],
                'tactical_risks': ['example chase or reversal risk'],
                'decision_delta': {
                    'has_previous_decision': False,
                    'transition': None,
                    'persistence_trend': 'new',
                    'upgrade_trigger': None,
                },
                'invalidation_conditions': ['example invalidation'],
                'data_quality_warnings': ['example warning'],
                'traceability': {
                    'agent_name': spec.agent_name,
                    'symbol': example_symbol,
                    'market_date': market_date.isoformat(),
                },
                'summary': 'brief short-term tactical thesis',
                'confidence': 0.6,
                'risk_flags': ['example_warning'],
                'details': {
                    'tactical_decision_support': {'decision_support_only': True},
                    'traceability': {
                        'agent_name': spec.agent_name,
                        'symbol': example_symbol,
                        'market_date': market_date.isoformat(),
                    },
                },
                'improvement_notes': ['example note about missing intraday, order-book, or flow data'],
            }
        elif spec.role == SIGNAL_DECIDER_AGENT_NAME:
            example_feedback = {
                'role': spec.role,
                'agent_name': spec.agent_name,
                'symbol': example_symbol,
                'market_date': market_date.isoformat(),
                'preferred_action': 'wait',
                'next_day_open_action': 'wait',
                'wait_reason': {
                    'code': 'stale_quote_orderbook',
                    'summary': '실시간 호가 신선도가 떨어져 진입 가격 신뢰가 부족합니다.',
                },
                'entry_price_basis': 'next_open',
                'target_price': 10500,
                'expected_return_pct': None,
                'stop_loss_price': 9800,
                'suggested_holding_period': '3-10 trading days',
                'confidence_score_10': 6,
                'conviction_score_10': 5,
                'thesis_summary': 'brief conservative decision-support thesis',
                'supporting_evidence': ['example evidence'],
                'decision_risks': ['example decision risk'],
                'decision_delta': {
                    'has_previous_decision': False,
                    'transition': None,
                    'persistence_trend': 'new',
                    'upgrade_trigger': None,
                },
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
                'details': {
                    'decision_support': {'decision_support_only': True},
                    'traceability': {
                        'agent_name': spec.agent_name,
                        'symbol': example_symbol,
                        'market_date': market_date.isoformat(),
                    },
                },
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
        signal_decider_batch_rules = _signal_decider_batch_rules(assigned_symbols)
        base_message = (
            f"Request marker: {request_marker}\n"
            f"Role: {spec.role}\n"
            f"Instruction: {spec.instruction}\n"
            f"Assigned symbols (exact authoritative strings): {assigned_symbols_json}\n"
            "Analyze each assigned symbol directly using only the supplied payload.\n"
            "Return one aggregated JSON object for the whole batch.\n"
            "Return JSON only with exactly one top-level object in this batch envelope: {\"items\": [{\"symbol\": \"...\", \"feedback\": {...}}]}.\n"
            "items length must equal the number of assigned symbols. Each assigned symbol must appear exactly once. Do not omit or invent symbols.\n"
            "Each item.symbol must exactly match one assigned symbol string.\n"
            "For each symbol, feedback must include role, summary, confidence, risk_flags, details, improvement_notes, and every additional role-specific field required by the contract.\n"
            "Inside details, do not repeat raw payload facts or echo numeric/source fields already present in the request.\n"
            "Use details only for compact interpretation, relationships, anomalies, conservative takeaways, and missing-context notes that add new value beyond the payload.\n"
            "Prefer derived judgments over restating inputs; if a field is already in the payload, reference its implication instead of copying it.\n"
            "If some inputs are weak or missing, still return conservative structured feedback for that symbol instead of prose or placeholders.\n"
            "All operator-facing natural-language fields inside the JSON must be written in Korean.\n"
            "For decision-support outputs, provide explicit target_price and stop_loss_price levels for each symbol whenever possible; short-term-decider must also provide trigger_price and chase_limit_price. If a level is genuinely not supportable, keep the key and explain the limitation in data_quality_warnings.\n"
            "If previous_decision_context is present, use it to populate decision_delta, avoid copy-pasting the prior wait narrative, and explain both the stable blockers and the concrete upgrade trigger.\n"
            "Interpret decider_data_quality_context.decision_priority literally: core_execution gaps can justify conservative wait/reject, while auxiliary_context gaps are confidence reducers only by default.\n"
            "Interpret decision_input_quality_context.final_judgment and execution_feasibility as the source-attributed buy-quality guardrail; hard_blocks should prevent buy/watch upgrades unless directly contradicted by fresher supplied evidence.\n"
            "Interpret market_regime_context as the authoritative market/sector posture guardrail: risk_off should default to wait/reject unless exceptional relative/sector leadership is evident; caution should require stronger evidence and reduced-size language; weak sector context should block fresh exposure unless leadership or confirmation is explicit.\n"
            "Empty recent news means no captured recent news and is neutral unless separate risk evidence exists.\n"
            "Do not forecast price reaction from news alone; use external news only as source-backed material/noise context, and do not treat headline-only, paywalled, or fetch-failed items as full-text summaries.\n"
            "Missing detailed quarterly/cash-flow context should usually reduce confidence only, not make the symbol untradeable on its own.\n"
            f"{signal_decider_batch_rules}"
            "Do not mention subagents, children, spawning, waiting, or internal orchestration.\n"
            "Do not output tool_code, python, print(...), json.dumps(...), markdown fences, code blocks, intent tags, analysis, reasoning, or explanatory prose.\n"
            "Do not prepend labels like [intent:...], do not wrap JSON in tags, and do not add any text before or after the JSON.\n"
            "Do not restate the request, do not describe what you will do, and do not explain the schema. Output the final JSON only.\n"
            "Immediately before sending, re-check the final JSON formatting one last time. If any closing brace/bracket count is wrong, fix it before answering.\n"
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
                f"Assigned symbols (exact authoritative strings): {assigned_symbols_json}\n"
                "Analyze each assigned symbol directly and return aggregated JSON only.\n"
                "Each assigned symbol must appear exactly once in {\"items\": [...]} with structured feedback; do not omit or invent symbols.\n"
                "item.symbol must exactly match an assigned symbol string.\n"
                f"{signal_decider_batch_rules}"
                "No tool_code, no print(json.dumps(...)), no markdown fences, no intent tags, no prose, no request restatement, raw JSON object only.\n"
                "Immediately before sending, re-check the final JSON formatting one last time and fix any extra or missing closing braces/brackets.\n"
                "All operator-facing natural-language fields inside the JSON must be written in Korean.\n"
                "For decision-support outputs, provide explicit target_price and stop_loss_price levels for each symbol whenever possible; short-term-decider must also provide trigger_price and chase_limit_price. If a level is genuinely not supportable, keep the key and explain the limitation in data_quality_warnings.\n"
                "Interpret decider_data_quality_context.decision_priority literally: core_execution gaps can justify conservative wait/reject, while auxiliary_context gaps are confidence reducers only by default.\n"
                "Interpret decision_input_quality_context.final_judgment and execution_feasibility as the source-attributed buy-quality guardrail; hard_blocks should prevent buy/watch upgrades unless directly contradicted by fresher supplied evidence.\n"
                "Interpret market_regime_context as the authoritative market/sector posture guardrail: risk_off should default to wait/reject unless exceptional relative/sector leadership is evident; caution should require stronger evidence and reduced-size language; weak sector context should block fresh exposure unless leadership or confirmation is explicit.\n"
                "Empty recent news means no captured recent news and is neutral unless separate risk evidence exists.\n"
                "Do not forecast price reaction from news alone; use external news only as source-backed material/noise context, and do not treat headline-only, paywalled, or fetch-failed items as full-text summaries.\n"
                "Missing detailed quarterly/cash-flow context should usually reduce confidence only, not make the symbol untradeable on its own.\n"
                f"{build_contract_prompt_suffix(role=spec.role)}\n"
                f"Market date: {market_date.isoformat()}\n"
                f"Assigned batch payload:\n{json.dumps(list(prepared_items), ensure_ascii=False)}"
            )
            if max_chars <= 0 or len(candidate) <= max_chars:
                return candidate
        fallback_item = _compressed_batch_item_for_prompt(batch_items[0])
        fallback_symbol = str(fallback_item.get('symbol') or example_symbol).strip() or '000000'
        fallback_assigned_symbols = [fallback_symbol]
        fallback_assigned_symbols_json = json.dumps(fallback_assigned_symbols, ensure_ascii=False)
        return (
            f"Role: {spec.role}\n"
            f"Instruction: {spec.instruction}\n"
            f"Assigned symbols (exact authoritative strings): {fallback_assigned_symbols_json}\n"
            "Analyze the assigned symbol directly and return aggregated JSON only.\n"
            "Return {\"items\": [{\"symbol\": \"...\", \"feedback\": {...}}]} with exactly one item.\n"
            "The one item.symbol must exactly match the assigned symbol string; do not omit feedback.symbol when the role contract requires it.\n"
            f"{_signal_decider_batch_rules(fallback_assigned_symbols)}"
            "No tool_code, no print(json.dumps(...)), no markdown fences, no intent tags, no prose, no request restatement, raw JSON object only.\n"
            "All operator-facing natural-language fields inside the JSON must be written in Korean.\n"
            "For decision-support outputs, provide explicit target_price and stop_loss_price levels for the assigned symbol whenever possible; short-term-decider must also provide trigger_price and chase_limit_price. If a level is genuinely not supportable, keep the key and explain the limitation in data_quality_warnings.\n"
            "Interpret decider_data_quality_context.decision_priority literally: core_execution gaps can justify conservative wait/reject, while auxiliary_context gaps are confidence reducers only by default.\n"
            "Interpret decision_input_quality_context.final_judgment and execution_feasibility as the source-attributed buy-quality guardrail; hard_blocks should prevent buy/watch upgrades unless directly contradicted by fresher supplied evidence.\n"
            "Interpret market_regime_context as the authoritative market/sector posture guardrail: risk_off should default to wait/reject unless exceptional relative/sector leadership is evident; caution should require stronger evidence and reduced-size language; weak sector context should block fresh exposure unless leadership or confirmation is explicit.\n"
            "Empty recent news means no captured recent news and is neutral unless separate risk evidence exists.\n"
            "Do not forecast price reaction from news alone; use external news only as source-backed material/noise context, and do not treat headline-only, paywalled, or fetch-failed items as full-text summaries.\n"
            "Missing detailed quarterly/cash-flow context should usually reduce confidence only, not make the symbol untradeable on its own.\n"
            f"{build_contract_prompt_suffix(role=spec.role)}\n"
            f"Market date: {market_date.isoformat()}\n"
            f"Assigned batch payload:\n{json.dumps([fallback_item], ensure_ascii=False)}"
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
        last_error_details: dict[str, Any] | None = None
        while attempt <= self.max_retries:
            attempt += 1
            started_at = datetime.now(timezone.utc)
            try:
                headers = self._headers() | {
                    'x-openclaw-session-key': request_session_key,
                    'x-openclaw-agent-id': spec.agent_name,
                }
                log_agent_feedback_event(
                    'dispatch_single_http_target',
                    status='started',
                    agent_name=spec.agent_name,
                    role=spec.role,
                    session_key=request_session_key,
                    http_target={'url': url, 'headers': headers},
                    request_snapshot=build_request_snapshot(body),
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
            except AgentDispatchTerminalError as exc:
                log_agent_feedback_event(
                    'dispatch_single_response_error',
                    status='terminal',
                    market_date=str(payload.get('market_date') or payload.get('trade_date') or '') or None,
                    role=spec.role,
                    agent_name=spec.agent_name,
                    symbol=str(payload.get('symbol') or '') or None,
                    session_key=request_session_key,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                raise
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                log_agent_feedback_event(
                    'dispatch_single_response_error',
                    status='retryable' if attempt <= self.max_retries else 'failed',
                    market_date=str(payload.get('market_date') or payload.get('trade_date') or '') or None,
                    role=spec.role,
                    agent_name=spec.agent_name,
                    symbol=str(payload.get('symbol') or '') or None,
                    session_key=request_session_key,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
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
            "Do not return free-form prose, explanations, or partial notes outside the JSON.\n"
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
            except AgentDispatchTerminalError as exc:
                log_agent_feedback_event(
                    'subagent_spawn_error',
                    status='terminal',
                    request_run_id=request_run_id,
                    agent_name=spec.agent_name,
                    role=spec.role,
                    market_date=market_date.isoformat(),
                    symbol=symbol,
                    session_key=child_session_key,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    spawn_target={
                        'url': url,
                        'child_label': child_label,
                    },
                )
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
        request_run_id = _request_scope_from_items(batch_items, 'request_run_id')
        batch_symbols = [str(item.get('symbol') or '').strip() for item in batch_items if str(item.get('symbol') or '').strip()]
        slot = _slot_from_items(batch_items)
        request_session_key = _request_scope_from_items(batch_items, 'session_key')
        request_session_key = str(request_session_key or f"feedback/{spec.agent_name}/{market_date.isoformat()}/{uuid.uuid4().hex}")
        fixed_session_key = request_session_key
        session_reuse_mode = _request_scope_from_items(batch_items, 'session_reuse_mode')
        if session_reuse_mode is None:
            session_reuse_mode = 'fresh_per_batch' if request_run_id and request_run_id in request_session_key else 'slot_reused'
        dispatch_id = _request_scope_from_items(batch_items, 'dispatch_id')
        if dispatch_id is None:
            slot_token = slot if slot is not None else 0
            dispatch_id = f'{request_run_id or "adhoc-run"}:{spec.role}:slot-{slot_token}:{uuid.uuid4().hex[:8]}'
        attempt = 0
        last_error: Exception | None = None
        while attempt <= self.max_retries:
            attempt += 1
            started_at = datetime.now(timezone.utc)
            response: httpx.Response | None = None
            attempt_id = make_attempt_id(dispatch_id=dispatch_id, attempt=attempt)
            request_marker = f"batch-request::{attempt_id or dispatch_id}"
            body = {
                'model': f'openclaw/{spec.agent_name}',
                'input': self._build_batch_message(
                    spec=spec,
                    market_date=market_date,
                    batch_items=batch_items,
                    request_marker=request_marker,
                ),
            }
            try:
                request_session_key = fixed_session_key
                headers = self._headers() | {
                    'x-openclaw-session-key': request_session_key,
                    'x-openclaw-agent-id': spec.agent_name,
                }
                log_agent_feedback_event(
                    'dispatch_batch_request',
                    status='started',
                    request_run_id=request_run_id,
                    dispatch_id=dispatch_id,
                    attempt_id=attempt_id,
                    agent_name=spec.agent_name,
                    role=spec.role,
                    market_date=market_date.isoformat(),
                    session_key=request_session_key,
                    batch_symbols=batch_symbols,
                    slot=slot,
                    session_reuse_mode=session_reuse_mode,
                    request_marker=request_marker,
                    request_snapshot=build_request_snapshot(body),
                    http_target={'url': url, 'headers': headers},
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
                log_agent_feedback_event(
                    'dispatch_batch_response',
                    status='received',
                    request_run_id=request_run_id,
                    dispatch_id=dispatch_id,
                    attempt_id=attempt_id,
                    agent_name=spec.agent_name,
                    role=spec.role,
                    market_date=market_date.isoformat(),
                    session_key=request_session_key,
                    batch_symbols=batch_symbols,
                    slot=slot,
                    session_reuse_mode=session_reuse_mode,
                    response_snapshot=build_response_snapshot(
                        parsed,
                        output_text=output_text,
                        status_code=response.status_code,
                    ),
                )
                if parsed.get('status') in {'failed', 'aborted', 'cancelled', 'canceled'}:
                    raise AgentDispatchRetryableError(
                        f"OpenClaw batch ended with status={parsed.get('status')}"
                    )
                decoded, json_failure_class = self._decode_candidate_json(output_text)
                decoded_source = 'output_text'
                session_ref = self._extract_session_ref(parsed, explicit_session_key=request_session_key) or {
                    'session_key': request_session_key,
                    'session_id': None,
                    'response_id': parsed.get('id'),
                }
                if session_ref.get('response_id') is None:
                    session_ref['response_id'] = parsed.get('id')
                response_id = (
                    str(session_ref.get('response_id')).strip()
                    if session_ref.get('response_id') is not None
                    else None
                ) or None
                session_history: list[dict[str, Any]] | None = None
                session_history_diagnostics: dict[str, Any] = {'checked': False, 'request_marker': request_marker}
                trajectory_diagnostics: dict[str, Any] = {'checked': False}
                history_marker_found = None
                failure_mode = json_failure_class
                if decoded is None and request_session_key:
                    session_history = self.fetch_session_history(session_key=request_session_key, limit=80, include_tools=True)
                    history_recovery = self._recover_from_session_history(
                        session_history=session_history,
                        request_marker=request_marker,
                    )
                    session_history_diagnostics = history_recovery.get('diagnostics') or session_history_diagnostics
                    history_marker_found = session_history_diagnostics.get('marker_found')
                    if history_recovery.get('decoded') is not None:
                        decoded = history_recovery['decoded']
                        decoded_source = 'session_history_after_marker'
                        json_failure_class = None
                        failure_mode = None
                        output_text = history_recovery.get('output_text')
                    else:
                        failure_mode = session_history_diagnostics.get('failure_mode') or failure_mode
                        trajectory_recovery = self._recover_from_trajectory_artifacts(
                            agent_name=spec.agent_name,
                            session_key=request_session_key,
                            response_id=response_id,
                        )
                        trajectory_diagnostics = trajectory_recovery.get('diagnostics') or trajectory_diagnostics
                        if trajectory_diagnostics.get('session_id') and not session_ref.get('session_id'):
                            session_ref['session_id'] = trajectory_diagnostics.get('session_id')
                        if trajectory_recovery.get('decoded') is not None:
                            decoded = trajectory_recovery['decoded']
                            decoded_source = 'trajectory_artifacts'
                            json_failure_class = None
                            failure_mode = None
                            output_text = trajectory_recovery.get('output_text')
                        else:
                            trajectory_failure_mode = trajectory_diagnostics.get('failure_mode')
                            if trajectory_failure_mode in {'trajectory_assistant_non_json', 'trajectory_response_id_mismatch'}:
                                failure_mode = trajectory_failure_mode
                            if trajectory_diagnostics.get('json_failure_class'):
                                json_failure_class = trajectory_diagnostics.get('json_failure_class')
                if decoded is None:
                    failure_class = self._failure_class_for_mode(failure_mode)
                    response_recovery = {
                        'session_key': request_session_key,
                        'session_id': session_ref.get('session_id'),
                        'response_id': response_id,
                        'request_marker': request_marker,
                        'history': session_history_diagnostics,
                        'trajectory': trajectory_diagnostics,
                    }
                    response_correlation = {
                        'expected_symbols': batch_symbols,
                        'parsed_symbols': [],
                        'response_symbols': [],
                        'missing_symbols': batch_symbols,
                        'unexpected_symbols': [],
                        'fallback_attributions': [],
                        'attribution_status': 'not_decoded',
                        'failure_class': failure_class,
                        'failure_mode': failure_mode,
                        'json_failure_class': json_failure_class,
                        'decoded_source': decoded_source,
                        'history_checked': bool(session_history_diagnostics.get('checked')),
                        'history_marker_found': history_marker_found,
                        'session_reuse_mode': session_reuse_mode,
                        'response_recovery': response_recovery,
                    }
                    log_agent_feedback_event(
                        'batch_response_parse_failed',
                        status='retryable',
                        request_run_id=request_run_id,
                        dispatch_id=dispatch_id,
                        attempt_id=attempt_id,
                        agent_name=spec.agent_name,
                        role=spec.role,
                        market_date=market_date.isoformat(),
                        session_key=request_session_key,
                        session_id=session_ref.get('session_id'),
                        response_id=response_id,
                        request_marker=request_marker,
                        batch_symbols=batch_symbols,
                        slot=slot,
                        session_reuse_mode=session_reuse_mode,
                        failure_class=failure_class,
                        failure_mode=failure_mode,
                        json_failure_class=json_failure_class,
                        response_correlation=response_correlation,
                    )
                    decoded = self._build_structured_fallback_batch_response(
                        spec=spec,
                        market_date=market_date,
                        batch_items=batch_items,
                        output_text=output_text,
                        failure_class=failure_class,
                        failure_mode=failure_mode,
                        json_failure_class=json_failure_class,
                        response_recovery=response_recovery,
                    )
                    decoded_source = 'core_structured_fallback'
                    log_agent_feedback_event(
                        'batch_response_structured_fallback',
                        status='accepted_with_fallback',
                        request_run_id=request_run_id,
                        dispatch_id=dispatch_id,
                        attempt_id=attempt_id,
                        agent_name=spec.agent_name,
                        role=spec.role,
                        market_date=market_date.isoformat(),
                        session_key=request_session_key,
                        session_id=session_ref.get('session_id'),
                        response_id=response_id,
                        request_marker=request_marker,
                        batch_symbols=batch_symbols,
                        slot=slot,
                        session_reuse_mode=session_reuse_mode,
                        failure_class=failure_class,
                        failure_mode=failure_mode,
                        json_failure_class=json_failure_class,
                        response_recovery=response_recovery,
                    )
                decoded = _coerce_decoded_batch_response(decoded)
                top_level_object_fallback = False
                if isinstance(decoded, dict) and not isinstance(decoded.get('items'), list):
                    top_level_object_fallback = True
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
                decoded, response_correlation = _correlate_decoded_batch_response(
                    decoded=decoded,
                    expected_symbols=batch_symbols,
                    session_reuse_mode=session_reuse_mode,
                )
                response_correlation = {
                    **response_correlation,
                    'decoded_source': decoded_source,
                    'history_checked': bool(session_history_diagnostics.get('checked')),
                    'history_marker_found': history_marker_found,
                    'top_level_object_fallback': top_level_object_fallback,
                    'json_failure_class': json_failure_class,
                    'response_recovery': {
                        'session_key': request_session_key,
                        'session_id': session_ref.get('session_id'),
                        'response_id': response_id,
                        'request_marker': request_marker,
                        'history': session_history_diagnostics,
                        'trajectory': trajectory_diagnostics,
                    },
                }
                response_symbols = response_correlation.get('response_symbols') or []
                unexpected_symbols = response_correlation.get('unexpected_symbols') or []
                missing_symbols = response_correlation.get('missing_symbols') or []
                fallback_attributions = response_correlation.get('fallback_attributions') or []
                if fallback_attributions:
                    log_agent_feedback_event(
                        'batch_response_symbol_attribution_fallback',
                        status='accepted_with_fallback',
                        request_run_id=request_run_id,
                        dispatch_id=dispatch_id,
                        attempt_id=attempt_id,
                        agent_name=spec.agent_name,
                        role=spec.role,
                        market_date=market_date.isoformat(),
                        session_key=request_session_key,
                        batch_symbols=batch_symbols,
                        slot=slot,
                        session_reuse_mode=session_reuse_mode,
                        response_symbols=response_symbols,
                        fallback_attributions=fallback_attributions,
                        response_correlation=response_correlation,
                    )
                if unexpected_symbols or missing_symbols:
                    log_agent_feedback_event(
                        'batch_response_symbol_mismatch',
                        status='warning',
                        request_run_id=request_run_id,
                        dispatch_id=dispatch_id,
                        attempt_id=attempt_id,
                        agent_name=spec.agent_name,
                        role=spec.role,
                        market_date=market_date.isoformat(),
                        session_key=request_session_key,
                        batch_symbols=batch_symbols,
                        slot=slot,
                        response_symbols=response_symbols,
                        unexpected_symbols=unexpected_symbols,
                        missing_symbols=missing_symbols,
                        history_checked=bool(session_history_diagnostics.get('checked')),
                        failure_class=response_correlation.get('failure_class') or 'batch_response_attribution_mismatch',
                        session_reuse_mode=session_reuse_mode,
                        response_correlation=response_correlation,
                        reuse_signal='stale_or_ambiguous_session_reuse',
                    )
                return {
                    'ok': True,
                    'attempt': attempt,
                    'attempt_id': attempt_id,
                    'dispatch_id': dispatch_id,
                    'request_run_id': request_run_id,
                    'request_marker': request_marker,
                    'market_date': market_date.isoformat(),
                    'role': spec.role,
                    'agent_name': spec.agent_name,
                    'batch_symbols': batch_symbols,
                    'slot': slot,
                    'started_at': started_at.isoformat(),
                    'completed_at': datetime.now(timezone.utc).isoformat(),
                    'response': parsed,
                    'output_text': output_text,
                    'session_ref': session_ref,
                    'decoded': decoded,
                    'history_checked': bool(session_history_diagnostics.get('checked')),
                    'response_correlation': response_correlation,
                    'session_reuse_mode': session_reuse_mode,
                }
            except AgentDispatchTerminalError:
                raise
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                error_type = type(exc).__name__
                error_message = str(exc)
                raw_error_details = exc.details if isinstance(exc, AgentDispatchError) else {}
                error_details = (
                    raw_error_details
                    if isinstance(raw_error_details, dict) and raw_error_details.get('failure_kind')
                    else self._build_dispatch_exception_details(
                        exc=exc,
                        response=response,
                        request_run_id=request_run_id,
                        dispatch_id=dispatch_id,
                        attempt_id=attempt_id,
                        request_session_key=request_session_key,
                        request_marker=request_marker,
                        slot=slot,
                        batch_symbols=batch_symbols,
                        session_reuse_mode=session_reuse_mode,
                    )
                )
                last_error_details = error_details
                log_agent_feedback_event(
                    'dispatch_batch_response_error',
                    status='retryable' if attempt <= self.max_retries else 'failed',
                    request_run_id=request_run_id,
                    dispatch_id=dispatch_id,
                    attempt_id=attempt_id,
                    agent_name=spec.agent_name,
                    role=spec.role,
                    market_date=market_date.isoformat(),
                    session_key=request_session_key,
                    session_id=error_details.get('session_id') if isinstance(error_details, dict) else None,
                    response_id=error_details.get('response_id') if isinstance(error_details, dict) else None,
                    request_marker=error_details.get('request_marker') if isinstance(error_details, dict) else None,
                    batch_symbols=batch_symbols,
                    slot=slot,
                    session_reuse_mode=session_reuse_mode,
                    error_type=error_type,
                    error_message=error_message,
                    failure_kind=error_details.get('failure_kind') if isinstance(error_details, dict) else None,
                    failure_mode=error_details.get('failure_mode') if isinstance(error_details, dict) else None,
                    failure_diagnostics=(
                        error_details.get('response_recovery')
                        if isinstance(error_details, dict)
                        else None
                    ),
                    response_snapshot=(
                        None
                        if response is None
                        else {
                            'status_code': response.status_code,
                            'text': response.text,
                        }
                    ),
                )
                logger.warning(
                    'OpenClaw batch dispatch attempt failed',
                    extra={
                        'agent_name': spec.agent_name,
                        'role': spec.role,
                        'attempt': attempt,
                        'max_retries': self.max_retries,
                        'batch_size': len(batch_items),
                        'session_key': request_session_key,
                        'session_id': error_details.get('session_id') if isinstance(error_details, dict) else None,
                        'response_id': error_details.get('response_id') if isinstance(error_details, dict) else None,
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
        if isinstance(last_error, AgentDispatchError) and last_error.details:
            raise last_error
        raise AgentDispatchRetryableError(
            str(last_error) if last_error else 'unknown batch dispatch failure',
            details=last_error_details or {},
        )


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
        return _build_upstream_feedback_bundle_helper(
            symbol=symbol,
            market_date=market_date,
            buffered_lookup=buffered_lookup,
            upstream_reuse_decisions=upstream_reuse_decisions,
            upstream_summary_roles=UPSTREAM_SUMMARY_ROLES,
        )

    @staticmethod
    def _build_signal_decider_payload(
        *,
        item: dict[str, Any],
        market_date: date,
        request_run_id: str,
        market_context: dict[str, Any] | None,
        upstream_summaries: dict[str, dict[str, Any]],
        upstream_traceability: dict[str, dict[str, Any]],
        signal_decider_policy: dict[str, Any] | None = None,
        agent_name: str = SIGNAL_DECIDER_AGENT_NAME,
        previous_decision_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = _build_signal_decider_payload_helper(
            item=item,
            market_date=market_date,
            request_run_id=request_run_id,
            market_context=market_context,
            upstream_summaries=upstream_summaries,
            upstream_traceability=upstream_traceability,
            signal_decider_agent_name=agent_name,
            signal_decider_policy=signal_decider_policy,
            previous_decision_context=previous_decision_context,
        )
        payload['orchestration']['required_upstream_roles'] = list(UPSTREAM_SUMMARY_ROLES)
        return payload

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
        active_signal_decider_policy = None
        market_context = None
        if role in DECISION_SUPPORT_AGENT_NAMES:
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
            active_signal_decider_policy = (
                signal_decider_loop_service.get_active_policy_context()
                if role == SIGNAL_DECIDER_AGENT_NAME
                else {}
            )

        dispatch_items: list[dict[str, Any]] = []
        for item in symbol_context_items:
            symbol = str(item.get('symbol') or '')
            decision = reuse_decisions.get(symbol, {}).get(role)
            if decision is not None and not decision.should_dispatch:
                continue
            if role not in DECISION_SUPPORT_AGENT_NAMES:
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
                failure_kind = FeedbackFailureKind.DEPENDENCY_MISSING_UPSTREAM.value
                dependency_context = {
                    'dependency_status': 'blocked_missing_upstream_summaries',
                    'failure_status': 'dependency/missing-upstream',
                    'failure_kind': failure_kind,
                    'dependency_policy': 'record_failure_and_continue',
                    'missing_roles': missing_roles,
                }
                failure = _build_failed_dispatch_item(
                    role=role,
                    agent_name=role,
                    symbol=symbol,
                    error=error,
                    retryable=True,
                    failure_kind=failure_kind,
                    context=dependency_context,
                )
                all_failures.append(failure)
                buffer_failure(role, symbol, error, True, failure_kind=failure_kind, context=dependency_context)
                log_agent_feedback_event(
                    'decider_prerequisite_missing',
                    status='blocked',
                    market_date=market_date.isoformat(),
                    request_run_id=request_run_id,
                    role=role,
                    agent_name=role,
                    symbol=symbol,
                    error_type='MissingUpstreamSummaries',
                    error_message=error,
                    missing_roles=missing_roles,
                    dependency_policy=dependency_context['dependency_policy'],
                )
                continue
            if market_context is None:
                market_context = signal_decider_market_context_builder.build(market_date=market_date)
            previous_decision_context = build_previous_decision_context(
                role=role,
                latest_feedback=getattr(decision, 'latest_feedback', None) if decision is not None else None,
            )
            dispatch_items.append(
                self._build_signal_decider_payload(
                    item=item,
                    market_date=market_date,
                    request_run_id=request_run_id,
                    market_context=market_context,
                    upstream_summaries=upstream_summaries,
                    upstream_traceability=upstream_traceability,
                    signal_decider_policy=active_signal_decider_policy,
                    agent_name=role,
                    previous_decision_context=previous_decision_context,
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

    @staticmethod
    def _session_key_from_failure(failure: dict[str, Any]) -> str | None:
        context = failure.get('context') if isinstance(failure.get('context'), dict) else {}
        session_key = failure.get('session_key') or context.get('session_key')
        if not isinstance(session_key, str) or not session_key.strip():
            return None
        return session_key.strip()

    def _cleanup_session_key(
        self,
        *,
        market_date: date,
        symbol: str,
        agent_name: str,
        run_id: str | None,
        session_key: str,
    ) -> dict[str, Any]:
        return agent_feedback_session_cleanup_service.delete_now(
            target=SessionCleanupTarget(
                market_date=market_date,
                symbol=symbol,
                agent_name=agent_name,
                run_id=run_id,
                session_key=session_key,
            )
        )

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
        return self._cleanup_session_key(
            market_date=market_date,
            symbol=symbol,
            agent_name=agent_name,
            run_id=run_id,
            session_key=session_key,
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
        max_seconds: float = 10.0,
    ) -> list[dict[str, Any]]:
        state = runtime_feedback_state_store.get_run(market_date, run_id=request_run_id)
        if state is None:
            return []

        rows_to_cleanup: list[dict[str, Any]] = []
        failure_cleanup_targets: list[dict[str, Any]] = []
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

        for failure in state.buffered_failures:
            agent_name = str(failure.get('agent_name') or failure.get('role') or '')
            if role is not None and agent_name != role:
                continue
            session_key = self._session_key_from_failure(failure)
            if session_key is None or session_key in cleaned_session_keys:
                continue
            failure_cleanup_targets.append(
                {
                    'market_date': market_date,
                    'symbol': str(failure.get('symbol') or ''),
                    'agent_name': agent_name,
                    'run_id': request_run_id,
                    'session_key': session_key,
                }
            )
            cleaned_session_keys.add(session_key)

        if not rows_to_cleanup and not failure_cleanup_targets:
            return []

        started_at = time.monotonic()
        deadline = started_at + max(0.0, float(max_seconds))
        cleanup_results: list[dict[str, Any]] = []
        deferred_session_keys: set[str] = set()

        def _deadline_exceeded() -> bool:
            return time.monotonic() >= deadline

        seen_session_keys: set[str] = set()
        for row in rows_to_cleanup:
            response = row.get('summary') if isinstance(row.get('summary'), dict) else None
            session_key = self._session_key_from_response(response)
            if session_key is None or session_key in seen_session_keys:
                continue
            if _deadline_exceeded():
                deferred_session_keys.add(session_key)
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

        for target in failure_cleanup_targets:
            session_key = target.get('session_key')
            if _deadline_exceeded():
                if isinstance(session_key, str):
                    deferred_session_keys.add(session_key)
                continue
            cleanup_results.append(self._cleanup_session_key(**target))
        session_keys = {
            session_key
            for session_key in (
                self._session_key_from_response(row.get('summary') if isinstance(row.get('summary'), dict) else None)
                for row in rows_to_cleanup
            )
            if session_key is not None
        }
        session_keys.update(
            target['session_key']
            for target in failure_cleanup_targets
            if isinstance(target.get('session_key'), str)
        )
        if deferred_session_keys:
            cleanup_results.append(
                {
                    'status': 'deferred',
                    'reason': 'cleanup_time_budget_exhausted',
                    'deferred_count': len(deferred_session_keys),
                    'max_seconds': max_seconds,
                }
            )
            log_agent_feedback_event(
                'cleanup_persisted_run_sessions_deferred',
                status='deferred',
                market_date=market_date.isoformat(),
                request_run_id=request_run_id,
                role=role,
                persistence_result={
                    'cleaned_count': len(session_keys - deferred_session_keys),
                    'deferred_count': len(deferred_session_keys),
                    'max_seconds': max_seconds,
                },
            )
        session_keys = session_keys - deferred_session_keys

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
        cleanup_result = None
        try:
            cleanup_result = self._cleanup_feedback_session(
                market_date=market_date,
                symbol=str(payload.get('symbol') or ''),
                agent_name=agent_name,
                run_id=run_id,
                response=response,
            )
        except Exception as exc:  # noqa: BLE001
            cleanup_result = {
                'status': 'deferred',
                'reason': 'cleanup_failed_after_feedback_persisted',
                'error_type': type(exc).__name__,
                'error': str(exc),
            }
            logger.warning(
                'agent feedback session cleanup failed after feedback persisted for %s/%s/%s: %s',
                market_date,
                agent_name,
                payload.get('symbol'),
                exc,
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
        log_agent_feedback_event(
            'role_checkpoint_summary',
            status=status,
            market_date=market_date.isoformat(),
            request_run_id=request_run_id,
            role=current_role,
            agent_name=current_role,
            batch_symbols=request_payload.get('symbols') if isinstance(request_payload.get('symbols'), list) else None,
            persistence_result={
                'completed_count': completed_count,
                'failed_count': failed_count,
            },
            lifecycle_stage=lifecycle_stage,
            progress_summary=summary_payload.get('progress_summary'),
            flush_result=flush_result,
            fatal_error=fatal_error,
        )
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
            for row in pending_feedback_rows:
                summary = row.get('summary') if isinstance(row.get('summary'), dict) else {}
                log_agent_feedback_event(
                    'feedback_persistence_attempt',
                    status='attempting',
                    market_date=market_date.isoformat(),
                    request_run_id=request_run_id,
                    dispatch_id=summary.get('dispatch_id'),
                    attempt_id=summary.get('attempt_id'),
                    role=row.get('agent_name'),
                    agent_name=row.get('agent_name'),
                    symbol=row.get('symbol'),
                    batch_symbols=summary.get('batch_symbols') if isinstance(summary.get('batch_symbols'), list) else None,
                    slot=summary.get('slot'),
                    session_key=summary.get('session_key'),
                    persistence_target={
                        'table': 'agent_context_feedback',
                        'operation': 'upsert',
                        'reason': reason,
                    },
                )
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
            try:
                feedback_result = agent_context_feedback_service.bulk_submit_feedback(items=flush_payload)
            except Exception as exc:  # noqa: BLE001
                flush_result['feedback_error'] = f'{type(exc).__name__}: {exc}'
                flush_result['status'] = 'failed'
                for row in pending_feedback_rows:
                    summary = row.get('summary') if isinstance(row.get('summary'), dict) else {}
                    log_agent_feedback_event(
                        'feedback_persistence_failure',
                        status='failed',
                        market_date=market_date.isoformat(),
                        request_run_id=request_run_id,
                        dispatch_id=summary.get('dispatch_id'),
                        attempt_id=summary.get('attempt_id'),
                        role=row.get('agent_name'),
                        agent_name=row.get('agent_name'),
                        symbol=row.get('symbol'),
                        batch_symbols=summary.get('batch_symbols') if isinstance(summary.get('batch_symbols'), list) else None,
                        slot=summary.get('slot'),
                        session_key=summary.get('session_key'),
                        persistence_target={
                            'table': 'agent_context_feedback',
                            'operation': 'upsert',
                            'reason': reason,
                        },
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                raise

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
            feedback_results_by_key = {
                (item.get('symbol'), item.get('agent', {}).get('name')): item
                for item in feedback_result.get('items', [])
                if isinstance(item, dict)
            }
            for row in pending_feedback_rows:
                summary = row.get('summary') if isinstance(row.get('summary'), dict) else {}
                persisted_item = feedback_results_by_key.get((row.get('symbol'), row.get('agent_name')))
                log_agent_feedback_event(
                    'feedback_persistence_success',
                    status='succeeded',
                    market_date=market_date.isoformat(),
                    request_run_id=request_run_id,
                    dispatch_id=summary.get('dispatch_id'),
                    attempt_id=summary.get('attempt_id'),
                    role=row.get('agent_name'),
                    agent_name=row.get('agent_name'),
                    symbol=row.get('symbol'),
                    batch_symbols=summary.get('batch_symbols') if isinstance(summary.get('batch_symbols'), list) else None,
                    slot=summary.get('slot'),
                    session_key=summary.get('session_key'),
                    persistence_target={
                        'table': 'agent_context_feedback',
                        'operation': 'upsert',
                        'reason': reason,
                    },
                    persistence_result={
                        'record_id': persisted_item.get('id') if isinstance(persisted_item, dict) else None,
                        'received_at': persisted_item.get('received_at') if isinstance(persisted_item, dict) else None,
                    },
                )

        if pending_failures:
            persisted_failures = []
            try:
                for failure in pending_failures:
                    failure_log_fields = _failure_log_fields(failure)
                    failure_kind = str(failure.get('failure_kind') or failure.get('error') or 'dispatch_failed')
                    original_failure = {
                        'origin': 'agent_dispatch_failure',
                        'failure_kind': failure_kind,
                        'error_message': failure.get('error'),
                        'retryable': failure.get('retryable', True),
                        'role': failure.get('role'),
                        'agent_name': failure.get('agent_name') or failure.get('role'),
                        'symbol': failure.get('symbol'),
                    }
                    log_agent_feedback_event(
                        'failure_persistence_attempt',
                        status='attempting',
                        market_date=market_date.isoformat(),
                        request_run_id=request_run_id,
                        dispatch_id=failure_log_fields['dispatch_id'],
                        attempt_id=failure_log_fields['attempt_id'],
                        role=failure.get('role'),
                        agent_name=failure.get('agent_name') or failure.get('role'),
                        symbol=failure.get('symbol'),
                        batch_symbols=failure_log_fields['batch_symbols'],
                        slot=failure_log_fields['slot'],
                        session_key=failure_log_fields['session_key'],
                        session_id=failure_log_fields['session_id'],
                        response_id=failure_log_fields['response_id'],
                        request_marker=failure_log_fields['request_marker'],
                        persistence_target={
                            'table': 'agent_context_feedback_failures',
                            'operation': 'upsert',
                            'reason': reason,
                        },
                        failure_origin='agent_dispatch_failure',
                        original_failure=original_failure,
                        failure_diagnostics=failure_log_fields['diagnostics'],
                    )
                    try:
                        persisted_failure = agent_context_feedback_failure_service.record_failure(
                            market_date=market_date,
                            symbol=str(failure.get('symbol') or ''),
                            agent_name=str(failure.get('agent_name') or failure.get('role') or ''),
                            run_id=request_run_id,
                            failure_kind=failure_kind,
                            retryable=bool(failure.get('retryable', True)),
                            summary={
                                'origin': 'agent_dispatch_failure',
                                'role': failure.get('role'),
                                'status': failure.get('status'),
                                'error': failure.get('error'),
                                'retryable': failure.get('retryable', True),
                                'failure_kind': failure_kind,
                                'dispatch_id': failure_log_fields['dispatch_id'],
                                'attempt_id': failure_log_fields['attempt_id'],
                                'slot': failure_log_fields['slot'],
                                'session_key': failure_log_fields['session_key'],
                                'session_id': failure_log_fields['session_id'],
                                'response_id': failure_log_fields['response_id'],
                                'request_marker': failure_log_fields['request_marker'],
                                'batch_symbols': failure_log_fields['batch_symbols'],
                                'failure_mode': failure.get('failure_kind'),
                                'failure_diagnostics': failure_log_fields['diagnostics'],
                            },
                            context=failure.get('context') if isinstance(failure.get('context'), dict) else None,
                        )
                    except Exception as exc:
                        log_agent_feedback_event(
                            'failure_persistence_failure',
                            status='failed',
                            market_date=market_date.isoformat(),
                            request_run_id=request_run_id,
                            dispatch_id=failure_log_fields['dispatch_id'],
                            attempt_id=failure_log_fields['attempt_id'],
                            role=failure.get('role'),
                            agent_name=failure.get('agent_name') or failure.get('role'),
                            symbol=failure.get('symbol'),
                            batch_symbols=failure_log_fields['batch_symbols'],
                            slot=failure_log_fields['slot'],
                            session_key=failure_log_fields['session_key'],
                            session_id=failure_log_fields['session_id'],
                            response_id=failure_log_fields['response_id'],
                            request_marker=failure_log_fields['request_marker'],
                            persistence_target={
                                'table': 'agent_context_feedback_failures',
                                'operation': 'upsert',
                                'reason': reason,
                            },
                            failure_origin='agent_dispatch_failure',
                            original_failure=original_failure,
                            failure_diagnostics=failure_log_fields['diagnostics'],
                            persistence_error_origin='failure_row_persistence',
                            error_type=type(exc).__name__,
                            error_message=str(exc),
                        )
                        raise
                    persisted_failures.append(persisted_failure)
                    log_agent_feedback_event(
                        'failure_persistence_success',
                        status='succeeded',
                        market_date=market_date.isoformat(),
                        request_run_id=request_run_id,
                        dispatch_id=failure_log_fields['dispatch_id'],
                        attempt_id=failure_log_fields['attempt_id'],
                        role=failure.get('role'),
                        agent_name=failure.get('agent_name') or failure.get('role'),
                        symbol=failure.get('symbol'),
                        batch_symbols=failure_log_fields['batch_symbols'],
                        slot=failure_log_fields['slot'],
                        session_key=failure_log_fields['session_key'],
                        session_id=failure_log_fields['session_id'],
                        response_id=failure_log_fields['response_id'],
                        request_marker=failure_log_fields['request_marker'],
                        persistence_target={
                            'table': 'agent_context_feedback_failures',
                            'operation': 'upsert',
                            'reason': reason,
                        },
                        persistence_result={
                            'record_id': persisted_failure.get('id') if isinstance(persisted_failure, dict) else None,
                            'received_at': persisted_failure.get('received_at') if isinstance(persisted_failure, dict) else None,
                            'failure_kind': persisted_failure.get('failure_kind') if isinstance(persisted_failure, dict) else None,
                        },
                        failure_origin='agent_dispatch_failure',
                        original_failure=original_failure,
                        failure_diagnostics=failure_log_fields['diagnostics'],
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
        return _dispatch_and_store_helper(
            self,
            market_date=market_date,
            request_run_id=request_run_id,
            specs=specs,
            payloads=payloads,
            symbol=symbol,
        )

    def _select_specs(self, *, roles: Sequence[str] | None, available_roles: Sequence[str] | None = None) -> list[AgentDispatchSpec]:
        requested = {role.strip() for role in roles} if roles else set(DEFAULT_AGENT_NAMES)
        available = set(available_roles) if available_roles else None
        specs = [
            spec
            for spec in DISPATCH_SPECS
            if spec.role in requested and (available is None or spec.role in available)
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

        status = _resolve_dispatch_status(completed=len(items), failed=len(failures))
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
        request_run_id: str | None = None,
        force: bool = False,
        force_recompute: bool = False,
        auto_scope: bool = False,
        role_symbol_scopes: dict[str, Sequence[str]] | None = None,
        funnel_diagnostics: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = market_date if isinstance(market_date, date) else date.fromisoformat(market_date)
        specs = self._select_specs(roles=roles)
        requested_role_names = [spec.role for spec in specs]
        requested_symbols = _normalize_requested_symbols(symbols)
        prerequisites = agent_feedback_prerequisite_service.inspect_feedback_prerequisites(
            market_date=normalized_market_date,
            fs_div=fs_div,
        )
        if not force and not prerequisites['ready']:
            return {
                'request': {
                    'market_date': normalized_market_date.isoformat(),
                    'symbols': requested_symbols,
                    'requested_symbols': requested_symbols,
                    'request_run_id': None,
                    'roles': [spec.role for spec in specs],
                    'fs_div': fs_div,
                    'disclosures_limit': disclosures_limit,
                    'reuse_existing': reuse_existing,
                    'force_recompute': force_recompute,
                    'scope': resolve_agent_feedback_scope(requested_symbols=None if auto_scope else requested_symbols),
                    'auto_scope': auto_scope,
                    'eligibility_rule': AGENT_FEEDBACK_ELIGIBILITY_RULE,
                    'eligibility_filters': list(AGENT_FEEDBACK_ELIGIBILITY_FILTERS),
                    'skipped_symbols': [],
                    'funnel_diagnostics': funnel_diagnostics,
                    'mode': 'role_sequential_runtime_buffered',
                },
                'summary': {
                    'status': prerequisites['status'],
                    'completed': 0,
                    'failed': 0,
                    'skipped': 0,
                    'flushed_to_db': 0,
                    'gated': True,
                },
                'items': [],
                'failures': [],
                'prerequisites': prerequisites,
            }
        dispatch_symbols, skipped_symbols, eligibility_diagnostics = (
            _split_requested_symbols_by_agent_feedback_eligibility_with_diagnostics(
                market_date=normalized_market_date,
                requested_symbols=requested_symbols,
            )
        )
        public_eligibility_diagnostics = _public_eligibility_diagnostics(eligibility_diagnostics)
        soft_penalized_symbols = list(public_eligibility_diagnostics.get('soft_penalized_symbols') or [])
        if funnel_diagnostics is None:
            records_by_symbol = eligibility_diagnostics.get('records_by_symbol') if isinstance(eligibility_diagnostics, dict) else {}
            funnel_diagnostics = build_agent_feedback_role_funnel(
                records=list(records_by_symbol.values()) if isinstance(records_by_symbol, dict) else [],
                roles=requested_role_names,
                requested_symbols=None if auto_scope else requested_symbols,
                candidate_symbols=dispatch_symbols,
            )
        feedback_scope_mode = (
            str(funnel_diagnostics.get('feedback_scope_mode')).strip()
            if isinstance(funnel_diagnostics, dict) and funnel_diagnostics.get('feedback_scope_mode') is not None
            else None
        )
        normalized_role_symbol_scopes = _normalize_role_symbol_scopes(
            roles=requested_role_names,
            symbols=dispatch_symbols,
            role_symbol_scopes=role_symbol_scopes
            if role_symbol_scopes is not None
            else (
                funnel_diagnostics.get('role_symbols')
                if isinstance(funnel_diagnostics.get('role_symbols'), dict)
                else None
            ),
        )
        scoped_dispatch_symbols = _ordered_union(
            [normalized_role_symbol_scopes.get(role, []) for role in requested_role_names]
        )
        request_run_id = request_run_id or f'symbol-run-{normalized_market_date.isoformat()}-{uuid.uuid4().hex[:10]}'
        batch_size = max(1, settings.openclaw_agent_symbol_chunk_size)
        role_parallelism = max(1, settings.openclaw_agent_parallel_subtasks)
        request_payload = {
            'market_date': normalized_market_date.isoformat(),
            'symbols': list(scoped_dispatch_symbols),
            'broad_symbols': list(dispatch_symbols),
            'requested_symbols': requested_symbols,
            'request_run_id': request_run_id,
            'roles': requested_role_names,
            'fs_div': fs_div,
            'disclosures_limit': disclosures_limit,
            'symbol_chunk_size': batch_size,
            'parallel_subtasks': role_parallelism,
            'role_parallel_subtasks': role_parallelism,
            'total_parallel_subtasks': role_parallelism,
            'reuse_existing': reuse_existing,
            'force_recompute': force_recompute,
            'scope': resolve_agent_feedback_scope(requested_symbols=None if auto_scope else requested_symbols),
            'auto_scope': auto_scope,
            'eligibility_rule': AGENT_FEEDBACK_ELIGIBILITY_RULE,
            'eligibility_filters': list(AGENT_FEEDBACK_ELIGIBILITY_FILTERS),
            'skipped_symbols': skipped_symbols,
            'soft_penalized_symbols': soft_penalized_symbols,
            'eligibility_diagnostics': public_eligibility_diagnostics,
            'funnel_diagnostics': funnel_diagnostics,
            'role_symbol_scopes': normalized_role_symbol_scopes,
            'role_symbol_counts': {
                role: len(symbols)
                for role, symbols in normalized_role_symbol_scopes.items()
            },
            'mode': 'role_sequential_runtime_buffered',
            'feedback_scope_mode': feedback_scope_mode,
        }
        state = RuntimeFeedbackRunState(
            run_id=request_run_id,
            market_date=normalized_market_date,
            requested_symbols=dispatch_symbols,
            requested_roles=requested_role_names,
            role_sequence=requested_role_names,
            parallel_per_role=role_parallelism,
            batch_size=batch_size,
            started_at=datetime.now(timezone.utc),
            current_role=specs[0].role if specs else None,
            role_symbol_scopes=normalized_role_symbol_scopes,
            funnel_diagnostics=funnel_diagnostics,
            feedback_scope_mode=feedback_scope_mode,
        )
        runtime_feedback_state_store.start_run(state)
        reuse_decisions: dict[str, dict[str, Any]] = {}
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
        log_agent_feedback_event(
            'run_started',
            status='started',
            market_date=normalized_market_date.isoformat(),
            request_run_id=request_run_id,
            batch_symbols=scoped_dispatch_symbols,
            requested_symbols=dispatch_symbols,
            skipped_symbols=skipped_symbols,
            requested_roles=requested_role_names,
            request_payload=request_payload,
            runtime_limits={
                'batch_size': batch_size,
                'parallel_subtasks': role_parallelism,
            },
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
            log_agent_feedback_event(
                'slot_activity',
                status='released' if done else 'active',
                market_date=normalized_market_date.isoformat(),
                request_run_id=request_run_id,
                role=role,
                agent_name=role,
                slot=slot,
                batch_symbols=batch_symbols or [],
                slot_state='released' if done else 'active',
            )

        def mark_reused(role: str, reuse_items: dict[str, Any]) -> None:
            reused_symbols = list(reuse_items.keys())
            if not reused_symbols:
                return

            def _update(current: RuntimeFeedbackRunState) -> None:
                current.current_role = role
                current.status = 'running'
                current.last_activity_at = datetime.now(timezone.utc)
                current.completed_symbols_by_role.setdefault(role, set()).update(reused_symbols)
                current.reused_symbols_by_role.setdefault(role, set()).update(reused_symbols)

            runtime_feedback_state_store.update_run(normalized_market_date, _update, run_id=request_run_id)
            for symbol, decision in reuse_items.items():
                log_agent_feedback_event(
                    'reuse_applied',
                    status='reused',
                    market_date=normalized_market_date.isoformat(),
                    request_run_id=request_run_id,
                    role=role,
                    agent_name=getattr(decision, 'agent_name', role),
                    symbol=symbol,
                    persistence_result={
                        'latest_feedback_id': getattr(decision, 'latest_feedback_id', None),
                        'latest_feedback_received_at': getattr(decision, 'latest_feedback_received_at', None),
                        'input_fingerprint': getattr(decision, 'input_fingerprint', None),
                        'stored_input_fingerprint': getattr(decision, 'stored_input_fingerprint', None),
                    },
                    reuse_reason=getattr(decision, 'reason', None),
                    reuse_policy_version=getattr(decision, 'reuse_policy_version', None),
                )

        def buffer_success(role: str, symbol: str, context: dict[str, Any], response: dict[str, Any]) -> None:
            normalized_feedback = response.get('normalized_feedback')
            storage_context = context
            storage_response = response
            decision = reuse_decisions.get(symbol, {}).get(role) if isinstance(reuse_decisions, dict) else None
            input_fingerprint_payload = getattr(decision, 'input_fingerprint_payload', None)
            if role == FUNDAMENTAL_SUMMARIZER_AGENT_NAME and not isinstance(input_fingerprint_payload, dict):
                try:
                    fingerprint_decision = agent_context_reuse_service.build_latest_decisions(
                        market_date=normalized_market_date,
                        symbols=[symbol],
                        roles=[role],
                        force_recompute=True,
                    ).get(symbol, {}).get(role)
                    input_fingerprint_payload = getattr(fingerprint_decision, 'input_fingerprint_payload', None)
                except Exception:  # noqa: BLE001
                    input_fingerprint_payload = None
            if role == FUNDAMENTAL_SUMMARIZER_AGENT_NAME and isinstance(input_fingerprint_payload, dict):
                reuse_metadata = {
                    'status': 'computed',
                    'reuse_policy_version': FUNDAMENTAL_SUMMARY_REUSE_POLICY_VERSION,
                    'input_fingerprint': input_fingerprint_payload,
                    'force_recompute': force_recompute,
                }
                storage_context = {
                    **context,
                    'fundamental_summary_input_fingerprint': input_fingerprint_payload,
                    'reuse_metadata': reuse_metadata,
                }
                storage_response = {
                    **response,
                    'fundamental_summary_input_fingerprint': input_fingerprint_payload,
                    'reuse_metadata': reuse_metadata,
                }

            def _update(current: RuntimeFeedbackRunState) -> None:
                current.last_activity_at = datetime.now(timezone.utc)
                current.pending_feedback_rows.append(
                    _build_pending_feedback_row(
                        market_date=normalized_market_date,
                        symbol=symbol,
                        agent_name=role,
                        run_id=request_run_id,
                        response=storage_response,
                        context=storage_context,
                    )
                )

            snapshot = runtime_feedback_state_store.update_run(
                normalized_market_date,
                _update,
                run_id=request_run_id,
            )
            log_agent_feedback_event(
                'feedback_buffered',
                status='buffered',
                market_date=normalized_market_date.isoformat(),
                request_run_id=request_run_id,
                role=role,
                agent_name=role,
                symbol=symbol,
                dispatch_id=response.get('dispatch_id'),
                attempt_id=response.get('attempt_id'),
                batch_symbols=response.get('batch_symbols'),
                slot=response.get('slot'),
                session_key=response.get('session_key'),
                normalized_feedback_snapshot=build_normalized_feedback_snapshot(
                    normalized_feedback if isinstance(normalized_feedback, dict) else None
                ),
                **_build_buffer_counts(snapshot),
                response_summary={
                    'collector_mode': response.get('collector_mode'),
                    'completed_at': response.get('completed_at'),
                },
            )

        def buffer_failure(
            role: str,
            symbol: str,
            error: str,
            retryable: bool,
            *,
            failure_kind: str | None = None,
            context: dict[str, Any] | None = None,
            agent_name: str | None = None,
            dispatch_id: str | None = None,
            attempt_id: str | None = None,
            slot: int | None = None,
            session_key: str | None = None,
            session_id: str | None = None,
            response_id: str | None = None,
            request_marker: str | None = None,
            diagnostics: dict[str, Any] | None = None,
            batch_symbols: Sequence[str] | None = None,
        ) -> None:
            def _update(current: RuntimeFeedbackRunState) -> None:
                current.last_activity_at = datetime.now(timezone.utc)
                current.pending_failures.append(
                    _build_pending_failure_row(
                        role=role,
                        symbol=symbol,
                        error=error,
                        retryable=retryable,
                        agent_name=agent_name,
                        failure_kind=failure_kind,
                        context=context,
                        dispatch_id=dispatch_id,
                        attempt_id=attempt_id,
                        slot=slot,
                        session_key=session_key,
                        session_id=session_id,
                        response_id=response_id,
                        request_marker=request_marker,
                        diagnostics=diagnostics,
                        batch_symbols=batch_symbols,
                    )
                )

            snapshot = runtime_feedback_state_store.update_run(
                normalized_market_date,
                _update,
                run_id=request_run_id,
            )
            log_agent_feedback_event(
                'failure_buffered',
                status='buffered',
                market_date=normalized_market_date.isoformat(),
                request_run_id=request_run_id,
                role=role,
                agent_name=agent_name or role,
                symbol=symbol,
                dispatch_id=dispatch_id,
                attempt_id=attempt_id,
                batch_symbols=list(batch_symbols) if batch_symbols is not None else None,
                slot=slot,
                session_key=session_key,
                session_id=session_id,
                response_id=response_id,
                request_marker=request_marker,
                error_message=error,
                retryable=retryable,
                **_build_buffer_counts(snapshot),
                failure_kind=failure_kind,
                failure_diagnostics=diagnostics,
                failure_context=context,
            )

        def buffer_incomplete_terminal_failures(snapshot: RuntimeFeedbackRunState) -> list[dict[str, Any]]:
            buffered: list[dict[str, Any]] = []
            for role in requested_role_names:
                role_symbols = normalized_role_symbol_scopes.get(role, list(scoped_dispatch_symbols))
                completed_symbols = snapshot.completed_symbols_by_role.get(role, set())
                failed_symbols = snapshot.failed_symbols_by_role.get(role, set())
                terminal_symbols = completed_symbols | failed_symbols
                for symbol in role_symbols:
                    normalized_symbol = str(symbol or '').strip().upper()
                    if not normalized_symbol or normalized_symbol in terminal_symbols:
                        continue
                    context = {
                        'origin': 'final_terminal_reconciliation',
                        'role': role,
                        'symbol': normalized_symbol,
                        'request_run_id': request_run_id,
                        'expected_role_symbols': len(role_symbols),
                        'completed_symbols': len(completed_symbols),
                        'failed_symbols': len(failed_symbols),
                    }
                    failure = _build_failed_dispatch_item(
                        role=role,
                        agent_name=role,
                        symbol=normalized_symbol,
                        error='dispatch_incomplete: terminal feedback missing at final reconciliation',
                        retryable=True,
                        failure_kind=FeedbackFailureKind.DISPATCH_INCOMPLETE.value,
                        context=context,
                        batch_symbols=[normalized_symbol],
                    )
                    all_failures.append(failure)
                    buffered.append(failure)
                    buffer_failure(
                        role,
                        normalized_symbol,
                        'dispatch_incomplete: terminal feedback missing at final reconciliation',
                        True,
                        failure_kind=FeedbackFailureKind.DISPATCH_INCOMPLETE.value,
                        context=context,
                        agent_name=role,
                        batch_symbols=[normalized_symbol],
                    )
                    log_agent_feedback_event(
                        'terminal_feedback_missing',
                        status='retryable_failure_buffered',
                        market_date=normalized_market_date.isoformat(),
                        request_run_id=request_run_id,
                        role=role,
                        agent_name=role,
                        symbol=normalized_symbol,
                        batch_symbols=[normalized_symbol],
                        failure_kind=FeedbackFailureKind.DISPATCH_INCOMPLETE.value,
                        failure_context=context,
                    )
            return buffered

        all_items: list[dict[str, Any]] = []
        all_failures: list[dict[str, Any]] = []
        symbol_context = {'items': []}
        response_payload: dict[str, Any] | None = None
        raised_exception: Exception | None = None
        finalization_error: Exception | None = None
        final_status = 'failed'
        final_flush_result: dict[str, Any] | None = None

        try:
            if not scoped_dispatch_symbols:
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
                        symbols=scoped_dispatch_symbols,
                        roles=requested_role_names,
                        force_recompute=force_recompute,
                    )
                upstream_reuse_decisions = {}
                if any(spec.role in DECISION_SUPPORT_AGENT_NAMES for spec in specs):
                    upstream_reuse_decisions = agent_context_reuse_service.build_latest_decisions(
                        market_date=normalized_market_date,
                        symbols=scoped_dispatch_symbols,
                        roles=UPSTREAM_SUMMARY_ROLES,
                        force_recompute=force_recompute,
                    )
                symbols_requiring_context = scoped_dispatch_symbols
                if reuse_decisions:
                    symbols_requiring_context = [
                        symbol
                        for symbol in scoped_dispatch_symbols
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
                    _attach_agent_feedback_eligibility_context(
                        items=symbol_context.get('items') or [],
                        diagnostics=eligibility_diagnostics,
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
                    log_agent_feedback_event(
                        'role_started',
                        status='started',
                        market_date=normalized_market_date.isoformat(),
                        request_run_id=request_run_id,
                        role=role,
                        agent_name=spec.agent_name,
                        batch_symbols=normalized_role_symbol_scopes.get(role, []),
                    )

                    role_scope_symbols = normalized_role_symbol_scopes.get(role, list(scoped_dispatch_symbols))
                    role_scope_set = set(role_scope_symbols)
                    role_reuse_items = {
                        symbol: reuse_decisions[symbol][role]
                        for symbol in role_scope_symbols
                        if (reuse_decisions.get(symbol, {}).get(role) is not None)
                        and not reuse_decisions[symbol][role].should_dispatch
                    }
                    for symbol in role_scope_symbols:
                        decision = reuse_decisions.get(symbol, {}).get(role)
                        if decision is None or not decision.should_dispatch:
                            continue
                        decision_reason = getattr(decision, 'reason', 'redispatch_required')
                        signal_kind = (
                            'stale_feedback'
                            if decision_reason == 'stale_market_feedback'
                            else (
                                'ambiguous_reuse'
                                if decision_reason in {'retryable_feedback_gap', 'latest_failure_pending_retry', 'missing_feedback', 'redispatch_required'}
                                else 'source_updated'
                            )
                        )
                        log_agent_feedback_event(
                            'reuse_signal',
                            status='redispatch_required',
                            market_date=normalized_market_date.isoformat(),
                            request_run_id=request_run_id,
                            role=role,
                            agent_name=getattr(decision, 'agent_name', role),
                            symbol=symbol,
                            persistence_result={
                                'latest_feedback_id': getattr(decision, 'latest_feedback_id', None),
                                'latest_feedback_received_at': getattr(decision, 'latest_feedback_received_at', None),
                                'latest_source_updated_at': getattr(decision, 'latest_source_updated_at', None),
                            },
                            reuse_reason=decision_reason,
                            reuse_signal=signal_kind,
                        )
                    mark_reused(role, role_reuse_items)

                    dispatch_items = self._prepare_role_dispatch_items(
                        role=role,
                        market_date=normalized_market_date,
                        request_run_id=request_run_id,
                        symbol_context_items=[
                            item
                            for item in symbol_context['items']
                            if str(item.get('symbol') or '').strip().upper() in role_scope_set
                        ],
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
                        session_reuse_mode = 'fresh_per_batch'
                        stable_session_key = f"agent:{spec.agent_name}:{_fresh_feedback_session_key(agent_name=spec.agent_name, request_run_id=request_run_id, slot=slot)}"
                        dispatch_id = f'{request_run_id}:{role}:slot-{slot}:batch-{batch_index + 1}'
                        batch_with_session = [
                            {
                                **batch_item,
                                'session_slot': slot,
                                'session_key': stable_session_key,
                                'session_reuse_mode': session_reuse_mode,
                                'request_run_id': request_run_id,
                                'dispatch_id': dispatch_id,
                            }
                            for batch_item in batch
                        ]
                        queue.append((slot, batch_with_session))
                        log_agent_feedback_event(
                            'queue_batch',
                            status='queued',
                            market_date=normalized_market_date.isoformat(),
                            request_run_id=request_run_id,
                            dispatch_id=dispatch_id,
                            role=role,
                            agent_name=spec.agent_name,
                            slot=slot,
                            session_key=stable_session_key,
                            session_reuse_mode=session_reuse_mode,
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
                                        str(item['symbol']).strip().upper(): item
                                        for item in decoded_items
                                        if isinstance(item, dict) and item.get('symbol')
                                    }
                                    response_symbols = sorted(by_symbol.keys())
                                    response_correlation = (
                                        batch_result.get('response_correlation')
                                        if isinstance(batch_result.get('response_correlation'), dict)
                                        else {}
                                    )
                                    for source_item in batch:
                                        symbol = source_item['symbol']
                                        symbol_key = str(symbol).strip().upper()
                                        dispatch_context = _resolve_batch_dispatch_context(
                                            batch_result=batch_result,
                                            source_item=source_item,
                                            batch=batch,
                                            slot=slot,
                                        )
                                        dispatch_id = dispatch_context['dispatch_id']
                                        attempt_id = dispatch_context['attempt_id']
                                        batch_symbols = dispatch_context['batch_symbols']
                                        session_key = dispatch_context['session_key']
                                        session_id = dispatch_context.get('session_id')
                                        response_id = dispatch_context.get('response_id')
                                        request_marker = dispatch_context.get('request_marker')
                                        feedback_candidate = by_symbol.get(symbol_key, {}).get('feedback')
                                        if not feedback_candidate:
                                            failure_class = (
                                                response_correlation.get('failure_class')
                                                if response_correlation.get('failure_class')
                                                else 'missing_batch_feedback'
                                            )
                                            failure_mode = response_correlation.get('failure_mode')
                                            failure_context = {
                                                **source_item,
                                                'response_correlation': response_correlation or None,
                                            }
                                            failure = _build_failed_dispatch_item(
                                                role=role,
                                                agent_name=spec.agent_name,
                                                symbol=symbol,
                                                error=str(failure_class),
                                                retryable=True,
                                                failure_kind=str(failure_mode or failure_class),
                                                context=failure_context,
                                                dispatch_id=dispatch_id,
                                                attempt_id=attempt_id,
                                                slot=slot,
                                                session_key=session_key,
                                                session_id=session_id,
                                                response_id=response_id,
                                                request_marker=request_marker,
                                                diagnostics=(
                                                    response_correlation.get('response_recovery')
                                                    if isinstance(response_correlation.get('response_recovery'), dict)
                                                    else None
                                                ),
                                                batch_symbols=batch_symbols,
                                            )
                                            all_failures.append(failure)
                                            log_agent_feedback_event(
                                                'normalized_response_rejected',
                                                status='rejected',
                                                market_date=normalized_market_date.isoformat(),
                                                request_run_id=request_run_id,
                                                dispatch_id=dispatch_id,
                                                attempt_id=attempt_id,
                                                role=role,
                                                agent_name=spec.agent_name,
                                                symbol=symbol,
                                                batch_symbols=batch_symbols,
                                                slot=slot,
                                                session_key=session_key,
                                                session_id=session_id,
                                                response_id=response_id,
                                                request_marker=request_marker,
                                                error_type='MissingBatchFeedback',
                                                error_message=str(failure_class),
                                                failure_class=failure_class,
                                                failure_mode=failure_mode,
                                                response_symbols=response_symbols,
                                                response_correlation=response_correlation,
                                            )
                                            buffer_failure(
                                                role,
                                                symbol,
                                                str(failure_class),
                                                True,
                                                failure_kind=str(failure_mode or failure_class),
                                                context=failure_context,
                                                agent_name=spec.agent_name,
                                                dispatch_id=dispatch_id,
                                                attempt_id=attempt_id,
                                                slot=slot,
                                                session_key=session_key,
                                                session_id=session_id,
                                                response_id=response_id,
                                                request_marker=request_marker,
                                                diagnostics=(
                                                    response_correlation.get('response_recovery')
                                                    if isinstance(response_correlation.get('response_recovery'), dict)
                                                    else None
                                                ),
                                                batch_symbols=batch_symbols,
                                            )
                                            staged_batch_outcomes = True
                                            continue
                                        normalized_feedback = _normalize_symbol_feedback(
                                            normalize_agent_response=normalize_agent_response,
                                            role=spec.role,
                                            agent_name=spec.agent_name,
                                            feedback_candidate=feedback_candidate,
                                            market_date=source_item.get('market_date') or normalized_market_date.isoformat(),
                                            symbol=symbol,
                                        )
                                        mismatch_flags = _extract_response_mismatch_flags(normalized_feedback)
                                        normalization_status = 'accepted_with_warnings' if mismatch_flags else 'accepted'
                                        log_agent_feedback_event(
                                            'normalized_response_accepted',
                                            status=normalization_status,
                                            market_date=normalized_market_date.isoformat(),
                                            request_run_id=request_run_id,
                                            dispatch_id=dispatch_id,
                                            attempt_id=attempt_id,
                                            role=role,
                                            agent_name=spec.agent_name,
                                            symbol=symbol,
                                            batch_symbols=batch_symbols,
                                            slot=slot,
                                            session_key=session_key,
                                            session_reuse_mode=batch_result.get('session_reuse_mode'),
                                            normalized_feedback_snapshot=build_normalized_feedback_snapshot(normalized_feedback),
                                            mismatch_flags=mismatch_flags,
                                            attribution_status=response_correlation.get('attribution_status'),
                                            response_correlation=response_correlation or None,
                                            response_symbol=(
                                                feedback_candidate.get('symbol')
                                                if isinstance(feedback_candidate, dict)
                                                else None
                                            ),
                                        )
                                        if mismatch_flags:
                                            log_agent_feedback_event(
                                                'normalized_response_mismatch',
                                                status='warning',
                                                market_date=normalized_market_date.isoformat(),
                                                request_run_id=request_run_id,
                                                dispatch_id=dispatch_id,
                                                attempt_id=attempt_id,
                                                role=role,
                                                agent_name=spec.agent_name,
                                                symbol=symbol,
                                                batch_symbols=batch_symbols,
                                                slot=slot,
                                                session_key=session_key,
                                                session_reuse_mode=batch_result.get('session_reuse_mode'),
                                                mismatch_flags=mismatch_flags,
                                                attribution_status=response_correlation.get('attribution_status'),
                                                response_correlation=response_correlation or None,
                                                normalized_feedback_snapshot=build_normalized_feedback_snapshot(normalized_feedback),
                                            )
                                        response_payload_item = _build_response_storage_payload(
                                            spec=spec,
                                            batch_result=batch_result,
                                            normalized_feedback=normalized_feedback,
                                            collector_mode='role_sequential_runtime_buffered',
                                        )
                                        buffer_success(spec.agent_name, symbol, source_item, response_payload_item)
                                        all_items.append(
                                            _build_completed_dispatch_item(
                                                role=role,
                                                agent_name=spec.agent_name,
                                                symbol=symbol,
                                                normalized_feedback=normalized_feedback,
                                            )
                                        )
                                    staged_batch_outcomes = True
                                except Exception as exc:
                                    error_details = exc.details if isinstance(exc, AgentDispatchError) else {}
                                    for source_item in batch:
                                        dispatch_context = _resolve_batch_dispatch_context(
                                            batch_result=None,
                                            source_item=source_item,
                                            batch=batch,
                                            slot=slot,
                                        )
                                        dispatch_id = dispatch_context['dispatch_id']
                                        batch_symbols = dispatch_context['batch_symbols']
                                        failure_context = dict(source_item)
                                        if isinstance(error_details, dict) and error_details:
                                            failure_context['dispatch_diagnostics'] = error_details
                                        failure = _build_failed_dispatch_item(
                                            role=role,
                                            agent_name=spec.agent_name,
                                            symbol=source_item['symbol'],
                                            error=f'{type(exc).__name__}: {exc}',
                                            retryable=True,
                                            failure_kind=(
                                                str(error_details.get('failure_kind'))
                                                if isinstance(error_details, dict) and error_details.get('failure_kind')
                                                else None
                                            ),
                                            context=failure_context,
                                            dispatch_id=dispatch_id,
                                            attempt_id=(
                                                error_details.get('attempt_id')
                                                if isinstance(error_details, dict)
                                                else None
                                            ),
                                            slot=slot,
                                            session_key=(
                                                error_details.get('session_key')
                                                if isinstance(error_details, dict) and error_details.get('session_key')
                                                else dispatch_context['session_key']
                                            ),
                                            session_id=(
                                                error_details.get('session_id')
                                                if isinstance(error_details, dict)
                                                else None
                                            ),
                                            response_id=(
                                                error_details.get('response_id')
                                                if isinstance(error_details, dict)
                                                else None
                                            ),
                                            request_marker=(
                                                error_details.get('request_marker')
                                                if isinstance(error_details, dict)
                                                else None
                                            ),
                                            diagnostics=(
                                                error_details.get('response_recovery')
                                                if isinstance(error_details, dict) and isinstance(error_details.get('response_recovery'), dict)
                                                else None
                                            ),
                                            batch_symbols=batch_symbols,
                                        )
                                        all_failures.append(failure)
                                        buffer_failure(
                                            role,
                                            source_item['symbol'],
                                            f'{type(exc).__name__}: {exc}',
                                            True,
                                            failure_kind=(
                                                str(error_details.get('failure_kind'))
                                                if isinstance(error_details, dict) and error_details.get('failure_kind')
                                                else None
                                            ),
                                            context=failure_context,
                                            agent_name=spec.agent_name,
                                            dispatch_id=dispatch_id,
                                            attempt_id=(
                                                error_details.get('attempt_id')
                                                if isinstance(error_details, dict)
                                                else None
                                            ),
                                            slot=slot,
                                            session_key=(
                                                error_details.get('session_key')
                                                if isinstance(error_details, dict) and error_details.get('session_key')
                                                else dispatch_context['session_key']
                                            ),
                                            session_id=(
                                                error_details.get('session_id')
                                                if isinstance(error_details, dict)
                                                else None
                                            ),
                                            response_id=(
                                                error_details.get('response_id')
                                                if isinstance(error_details, dict)
                                                else None
                                            ),
                                            request_marker=(
                                                error_details.get('request_marker')
                                                if isinstance(error_details, dict)
                                                else None
                                            ),
                                            diagnostics=(
                                                error_details.get('response_recovery')
                                                if isinstance(error_details, dict) and isinstance(error_details.get('response_recovery'), dict)
                                                else None
                                            ),
                                            batch_symbols=batch_symbols,
                                        )
                                        log_agent_feedback_event(
                                            'normalized_response_rejected',
                                            status='rejected',
                                            market_date=normalized_market_date.isoformat(),
                                            request_run_id=request_run_id,
                                            dispatch_id=dispatch_id,
                                            role=role,
                                            agent_name=spec.agent_name,
                                            symbol=source_item['symbol'],
                                            batch_symbols=batch_symbols,
                                            slot=slot,
                                            session_key=(
                                                error_details.get('session_key')
                                                if isinstance(error_details, dict) and error_details.get('session_key')
                                                else dispatch_context['session_key']
                                            ),
                                            session_id=(
                                                error_details.get('session_id')
                                                if isinstance(error_details, dict)
                                                else None
                                            ),
                                            response_id=(
                                                error_details.get('response_id')
                                                if isinstance(error_details, dict)
                                                else None
                                            ),
                                            request_marker=(
                                                error_details.get('request_marker')
                                                if isinstance(error_details, dict)
                                                else None
                                            ),
                                            error_type=type(exc).__name__,
                                            error_message=str(exc),
                                            failure_kind=(
                                                error_details.get('failure_kind')
                                                if isinstance(error_details, dict)
                                                else None
                                            ),
                                            failure_diagnostics=(
                                                error_details.get('response_recovery')
                                                if isinstance(error_details, dict)
                                                else None
                                            ),
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
                    try:
                        role_cleanup_result = self._cleanup_persisted_run_sessions(
                            market_date=normalized_market_date,
                            request_run_id=request_run_id,
                            role=role,
                        )
                    except Exception as cleanup_exc:  # noqa: BLE001
                        role_cleanup_result = [
                            {
                                'status': 'deferred',
                                'reason': 'role_cleanup_failed_after_role_persisted',
                                'error_type': type(cleanup_exc).__name__,
                                'error': str(cleanup_exc),
                            }
                        ]
                        logger.warning(
                            'agent feedback role session cleanup failed after role persisted for %s/%s/%s: %s',
                            normalized_market_date,
                            request_run_id,
                            role,
                            cleanup_exc,
                        )
                    self._checkpoint_run_state(
                        market_date=normalized_market_date,
                        request_run_id=request_run_id,
                        request_payload=request_payload,
                        lifecycle_stage='role_completed',
                        current_role=role,
                        status='running',
                        flush_result={
                            **role_flush_result,
                            'session_cleanup': role_cleanup_result,
                        },
                    )
                    log_agent_feedback_event(
                        'role_completed',
                        status='completed',
                        market_date=normalized_market_date.isoformat(),
                        request_run_id=request_run_id,
                        role=role,
                        agent_name=spec.agent_name,
                        batch_symbols=role_scope_symbols,
                        persistence_result={
                            'feedback_rows_persisted': role_flush_result.get('feedback_rows_persisted'),
                            'failure_rows_persisted': role_flush_result.get('failure_rows_persisted'),
                        },
                        flush_result=role_flush_result,
                    )

                final_snapshot = runtime_feedback_state_store.get_run(normalized_market_date, run_id=request_run_id) or state
                completed_count = len(final_snapshot.buffered_feedback_rows)
                failed_count = len(final_snapshot.buffered_failures)
                progress_counts = final_snapshot.to_progress_summary()
                final_status = _resolve_dispatch_status(completed=completed_count, failed=failed_count)
                response_payload = {
                    'request': request_payload,
                    'summary': {
                        'status': final_status,
                        'completed': completed_count,
                        'computed_count': int(progress_counts.get('computed_feedback_count') or completed_count),
                        'reused_count': int(progress_counts.get('reused_feedback_count') or 0),
                        'stale_count': int(progress_counts.get('stale_feedback_count') or 0),
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
                final_snapshot = runtime_feedback_state_store.get_run(normalized_market_date, run_id=request_run_id) or final_snapshot
                if final_snapshot is not None and (final_snapshot.pending_feedback_rows or final_snapshot.pending_failures):
                    drain_flush_result = self._flush_pending_run_buffers(
                        market_date=normalized_market_date,
                        request_run_id=request_run_id,
                        reason='finalize_drain_pending',
                    )
                    final_flush_result = {
                        'status': 'drained',
                        'initial_flush': final_flush_result,
                        'drain_flush': drain_flush_result,
                    }
                    final_snapshot = runtime_feedback_state_store.get_run(normalized_market_date, run_id=request_run_id) or final_snapshot
                if (
                    final_snapshot is not None
                    and scoped_dispatch_symbols
                    and raised_exception is None
                    and finalization_error is None
                ):
                    incomplete_failures = buffer_incomplete_terminal_failures(final_snapshot)
                    if incomplete_failures:
                        incomplete_flush_result = self._flush_pending_run_buffers(
                            market_date=normalized_market_date,
                            request_run_id=request_run_id,
                            reason='finalize_missing_terminal_feedback',
                        )
                        final_flush_result = {
                            'status': 'terminal_reconciled',
                            'previous_flush': final_flush_result,
                            'incomplete_failure_count': len(incomplete_failures),
                            'incomplete_flush': incomplete_flush_result,
                        }
            except Exception as exc:  # noqa: BLE001
                finalization_error = exc
                log_agent_feedback_event(
                    'finalize_failed',
                    status='failed',
                    market_date=normalized_market_date.isoformat(),
                    request_run_id=request_run_id,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            finally:
                try:
                    self._cleanup_persisted_run_sessions(
                        market_date=normalized_market_date,
                        request_run_id=request_run_id,
                    )
                except Exception as cleanup_exc:  # noqa: BLE001
                    log_agent_feedback_event(
                        'cleanup_persisted_run_sessions_failed',
                        status='failed',
                        market_date=normalized_market_date.isoformat(),
                        request_run_id=request_run_id,
                        error_type=type(cleanup_exc).__name__,
                        error_message=str(cleanup_exc),
                    )
            final_snapshot = runtime_feedback_state_store.get_run(normalized_market_date, run_id=request_run_id) or final_snapshot
            completed_count = len(final_snapshot.buffered_feedback_rows) if final_snapshot is not None else 0
            failed_count = len(final_snapshot.buffered_failures) if final_snapshot is not None else 0
            if raised_exception is not None or finalization_error is not None:
                final_status = 'partial' if (completed_count > 0 or failed_count > 0) else 'failed'
            elif scoped_dispatch_symbols:
                final_status = _resolve_dispatch_status(completed=completed_count, failed=failed_count)
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
            log_agent_feedback_event(
                'final_reconciliation',
                status='reconciled',
                market_date=normalized_market_date.isoformat(),
                request_run_id=request_run_id,
                batch_symbols=scoped_dispatch_symbols,
                terminal_status=final_status,
                persistence_result={
                    'completed_count': completed_count,
                    'failed_count': failed_count,
                    'flushed_feedback_count': final_snapshot.flushed_feedback_count if final_snapshot else None,
                    'flushed_failure_count': final_snapshot.flushed_failure_count if final_snapshot else None,
                },
                final_flush_result=final_flush_result,
                final_status=final_status,
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
            log_agent_feedback_event(
                'run_finished',
                status='finished',
                market_date=normalized_market_date.isoformat(),
                request_run_id=request_run_id,
                batch_symbols=scoped_dispatch_symbols,
                terminal_status=final_status,
                completed_at=final_snapshot.completed_at.isoformat() if final_snapshot and final_snapshot.completed_at else None,
                persistence_result={
                    'completed_count': int(final_snapshot.to_progress_summary().get('completed_feedback_count') or 0) if final_snapshot else 0,
                    'failed_count': int(final_snapshot.to_progress_summary().get('failed_items') or 0) if final_snapshot else 0,
                    'flushed_feedback_count': final_snapshot.flushed_feedback_count if final_snapshot else None,
                    'flushed_failure_count': final_snapshot.flushed_failure_count if final_snapshot else None,
                },
                run_summary=final_snapshot.to_progress_summary() if final_snapshot else None,
            )

            if response_payload is not None and scoped_dispatch_symbols:
                progress_counts = final_snapshot.to_progress_summary() if final_snapshot else {}
                response_payload['summary']['status'] = final_status
                response_payload['summary']['completed'] = completed_count
                response_payload['summary']['computed_count'] = int(progress_counts.get('computed_feedback_count') or completed_count)
                response_payload['summary']['reused_count'] = int(progress_counts.get('reused_feedback_count') or 0)
                response_payload['summary']['stale_count'] = int(progress_counts.get('stale_feedback_count') or 0)
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
        return _dispatch_for_symbols_via_subagents_helper(
            self,
            market_date=market_date,
            symbols=symbols,
            roles=roles,
            fs_div=fs_div,
            disclosures_limit=disclosures_limit,
            per_role_parallelism=per_role_parallelism,
            signal_decider_agent_name=SIGNAL_DECIDER_AGENT_NAME,
            normalize_requested_symbols=_normalize_requested_symbols,
            split_requested_symbols_by_agent_feedback_eligibility=_split_requested_symbols_by_agent_feedback_eligibility,
            agent_context_read_service=agent_context_read_service,
            normalize_agent_response=normalize_agent_response,
            build_response_storage_payload=_build_response_storage_payload,
        )


agent_feedback_orchestrator = AgentFeedbackOrchestrator()
