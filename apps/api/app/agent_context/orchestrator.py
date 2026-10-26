from __future__ import annotations

import concurrent.futures
from contextlib import contextmanager
from email.utils import parsedate_to_datetime
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import quote, urlparse
from zoneinfo import ZoneInfo

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
    load_agent_feedback_eligibility_records,
    resolve_agent_feedback_scope,
    split_requested_symbols_by_agent_feedback_eligibility_with_diagnostics,
)
from app.agent_context.feedback_failure_service import agent_context_feedback_failure_service
from app.agent_context.feedback_failure_taxonomy import FeedbackFailureKind, classify_feedback_failure
from app.agent_context.orchestrator_helpers import (
    build_completed_dispatch_item as _build_completed_dispatch_item,
    build_failed_dispatch_item as _build_failed_dispatch_item,
    build_response_storage_payload as _build_response_storage_payload,
    build_decider_input_generation as _build_decider_input_generation,
    build_durable_decider_input_generation as _build_durable_decider_input_generation,
    build_decider_source_context_generation as _build_decider_source_context_generation,
    durable_decider_source_context_fingerprint_matches as _durable_decider_source_context_fingerprint_matches,
    build_signal_decider_payload as _build_signal_decider_payload_helper,
    build_upstream_feedback_bundle as _build_upstream_feedback_bundle_helper,
    coerce_decoded_batch_response as _coerce_decoded_batch_response,
    correlate_decoded_batch_response as _correlate_decoded_batch_response,
    extract_response_mismatch_flags as _extract_response_mismatch_flags,
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
    batch_response_contract_failure as _batch_response_contract_failure,
    build_buffer_counts as _build_buffer_counts,
    build_pending_failure_row as _build_pending_failure_row,
    build_pending_feedback_row as _build_pending_feedback_row,
    dispatch_and_store as _dispatch_and_store_helper,
    dispatch_for_symbols_via_subagents as _dispatch_for_symbols_via_subagents_helper,
    normalize_symbol_feedback as _normalize_symbol_feedback,
    normalized_feedback_contract_failure as _normalized_feedback_contract_failure,
    response_failure_audit as _response_failure_audit,
    resolve_batch_dispatch_context as _resolve_batch_dispatch_context,
)
from app.agent_context.logging import (
    build_normalized_feedback_snapshot,
    build_request_snapshot,
    build_response_snapshot,
    log_agent_feedback_event,
    make_attempt_id,
)
from app.agent_context.model_identity import (
    apply_decider_model_identity_fence,
    build_dispatch_runtime_contract_provenance,
    build_requested_model_identity,
    build_observed_model_identity,
    build_runtime_contract_provenance,
    expected_decider_model,
)
from app.agent_context.openclaw_storage_admission import (
    inspect_openclaw_storage_admission,
)
from app.agent_context.runtime_state import RuntimeFeedbackRunState, runtime_feedback_state_store
from app.agent_context.run_modes import (
    OPENCLAW_SHARED_DISPATCH_CIRCUIT_BREAKER_SCHEMA_VERSION,
    OPENCLAW_SHARED_DISPATCH_ORCHESTRATOR_SOURCE,
)
from app.agent_context.safe_file import (
    SafeFileReadError,
    fingerprint_regular_file,
)
from app.agent_context.payload_builder import agent_payload_builder
from app.agent_context.read_service import (
    agent_context_read_service,
    resolve_external_news_knowledge_cutoff_for_symbols,
)
from app.agent_context.roles import DEFAULT_AGENT_NAMES, DECISION_SUPPORT_AGENT_NAMES, SHORT_TERM_DECIDER_AGENT_NAME, SIGNAL_DECIDER_AGENT_NAME, SUMMARIZER_AGENT_NAMES
from app.agent_context.run_state_service import agent_context_feedback_run_state_service
from app.agent_context.response_contract import (
    build_agent_contract_provenance,
    build_contract_prompt_suffix,
    decode_strict_agent_json_object,
    normalize_agent_response,
)
from app.agent_context.reuse_service import (
    FUNDAMENTAL_SUMMARIZER_AGENT_NAME,
    FUNDAMENTAL_SUMMARY_REUSE_POLICY_VERSION,
    agent_context_reuse_service,
    build_fundamental_input_fingerprint_payload_from_context,
)
from app.agent_context.session_cleanup_service import SessionCleanupTarget, agent_feedback_session_cleanup_service
from app.agent_context.signal_decider_calibration_service import signal_decider_calibration_service
from app.agent_context.signal_decider_loop_service import (
    DEFAULT_SIGNAL_DECIDER_EVALUATION_LOOKBACK_DAYS,
    DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION,
    DEFAULT_SIGNAL_DECIDER_REVIEW_PRIMARY_HORIZON,
    signal_decider_loop_service,
)
from app.agent_context.signal_decider_market_context import signal_decider_market_context_builder
from app.agent_context.signal_decider_performance_criteria import (
    compact_guidance,
    compact_performance_buckets,
    compact_rule_candidates,
)
from app.agent_context.signal_decider_weekly_learning_service import (
    signal_decider_weekly_learning_service,
)
from app.core.settings import settings
from app.db import session_scope, session_timeout_budget
from app.db_models import AgentContextFeedback, EODCollectionRun
from app.external_news.store import list_external_news_items

logger = logging.getLogger(__name__)

OPENCLAW_MODEL_IDENTITY_OBSERVATION_RETRY_DELAYS_SECONDS = (0.05, 0.1, 0.2)
OPENCLAW_AGENT_SQLITE_SCHEMA_VERSION = 25
PERSISTED_SNAPSHOT_JSON_MAX_BYTES = 1024 * 1024
POST_DECISION_NEWS_SCAN_LIMIT = 50
_OPENCLAW_SESSION_REGISTRY_OBSERVATION_LOCK = threading.Lock()
_MAX_OPENCLAW_RATE_LIMIT_COOLDOWN_SECONDS = 7 * 24 * 60 * 60


def _openclaw_rate_limit_retry_not_before(
    details: Mapping[str, Any],
) -> datetime | None:
    raw_value = str(details.get('retry_not_before') or '').strip()
    if not raw_value:
        return None
    try:
        parsed = datetime.fromisoformat(raw_value.replace('Z', '+00:00'))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@dataclass
class _OpenClawGatewayCircuitState:
    error: tuple[str, dict[str, Any]] | None = None
    run_id: str | None = None
    reprobe_run_id: str | None = None


class _OpenClawDispatchCoordinator:
    """Coordinate model dispatches and shared outage state per gateway."""

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._clock = clock
        self._condition = threading.Condition()
        self._active_keys: set[str] = set()
        self._circuit_states: dict[str, _OpenClawGatewayCircuitState] = {}

    @staticmethod
    def _normalized_key(key: str) -> str:
        return str(key or 'openclaw').strip() or 'openclaw'

    @contextmanager
    def admit(self, *, key: str):
        normalized_key = self._normalized_key(key)
        wait_started_at = self._clock()
        with self._condition:
            while normalized_key in self._active_keys:
                self._condition.wait()
            self._active_keys.add(normalized_key)
        admission = {
            'coordination_mode': 'completion_single_flight',
            'wait_seconds': max(0.0, self._clock() - wait_started_at),
            'max_in_flight': 1,
        }
        try:
            yield admission
        finally:
            with self._condition:
                self._active_keys.discard(normalized_key)
                self._condition.notify_all()

    def check_circuit(
        self,
        *,
        key: str,
        request_run_id: str | None,
        allow_reserved_reprobe: bool,
    ) -> bool:
        normalized_key = self._normalized_key(key)
        normalized_run_id = str(request_run_id or '').strip() or None
        blocked_error: tuple[str, dict[str, Any]] | None = None
        with self._condition:
            state = self._circuit_states.get(normalized_key)
            stored = state.error if state is not None else None
            if stored is None:
                return False
            message, stored_details = stored
            if (
                allow_reserved_reprobe
                and normalized_run_id is not None
                and state is not None
                and state.reprobe_run_id == normalized_run_id
            ):
                return True
            failure_kind = str(
                stored_details.get('failure_kind') or ''
            ).strip()
            retry_not_before = (
                _openclaw_rate_limit_retry_not_before(stored_details)
                if failure_kind
                == FeedbackFailureKind.OPENCLAW_PROVIDER_RATE_LIMITED.value
                else None
            )
            now = datetime.now(timezone.utc)
            rate_limit_cooldown_active = bool(
                retry_not_before is not None and now < retry_not_before
            )
            if rate_limit_cooldown_active:
                blocked_details = dict(stored_details)
                blocked_details['retry_after_seconds'] = max(
                    1,
                    math.ceil((retry_not_before - now).total_seconds()),
                )
                blocked_error = (message, blocked_details)
            else:
                rate_limit_cooldown_expired = bool(
                    failure_kind
                    == FeedbackFailureKind.OPENCLAW_PROVIDER_RATE_LIMITED.value
                    and retry_not_before is not None
                )
                independent_run = bool(
                    normalized_run_id is not None
                    and state is not None
                    and normalized_run_id != state.run_id
                )
                if (
                    (rate_limit_cooldown_expired or independent_run)
                    and normalized_run_id is not None
                    and state is not None
                    and state.reprobe_run_id is None
                ):
                    state.reprobe_run_id = normalized_run_id
                    return True
                blocked_error = (message, dict(stored_details))
        if blocked_error is not None:
            message, details = blocked_error
            raise AgentDispatchTerminalError(message, details=details)
        return False

    def trip_circuit(
        self,
        *,
        key: str,
        error: AgentDispatchError,
        request_run_id: str | None,
    ) -> None:
        normalized_key = self._normalized_key(key)
        with self._condition:
            existing_state = self._circuit_states.get(normalized_key)
            existing_details = (
                existing_state.error[1]
                if existing_state is not None
                and existing_state.error is not None
                else None
            )
            existing_rate_limit_deadline = (
                _openclaw_rate_limit_retry_not_before(existing_details)
                if isinstance(existing_details, dict)
                and existing_details.get('failure_kind')
                == FeedbackFailureKind.OPENCLAW_PROVIDER_RATE_LIMITED.value
                else None
            )
            incoming_rate_limit_deadline = (
                _openclaw_rate_limit_retry_not_before(error.details)
                if error.details.get('failure_kind')
                == FeedbackFailureKind.OPENCLAW_PROVIDER_RATE_LIMITED.value
                else None
            )
            now = datetime.now(timezone.utc)
            if (
                existing_rate_limit_deadline is not None
                and existing_rate_limit_deadline > now
                and (
                    incoming_rate_limit_deadline is None
                    or incoming_rate_limit_deadline
                    <= existing_rate_limit_deadline
                )
            ):
                return
            self._circuit_states[normalized_key] = (
                _OpenClawGatewayCircuitState(
                    error=(str(error), dict(error.details)),
                    run_id=str(request_run_id or '').strip() or None,
                )
            )

    def finish_reprobe(
        self,
        *,
        key: str,
        request_run_id: str | None,
        recovered: bool,
    ) -> None:
        normalized_key = self._normalized_key(key)
        normalized_run_id = str(request_run_id or '').strip() or None
        with self._condition:
            state = self._circuit_states.get(normalized_key)
            if state is None or state.reprobe_run_id != normalized_run_id:
                return
            if recovered:
                self._circuit_states.pop(normalized_key, None)
                return
            state.run_id = normalized_run_id
            state.reprobe_run_id = None


class _OpenClawDispatchPacer:
    """Serialize model-start admissions across clients in this API process."""

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._next_dispatch_at: dict[str, float] = {}

    def wait(
        self,
        *,
        key: str,
        minimum_interval_seconds: float,
    ) -> dict[str, Any]:
        interval = max(0.0, float(minimum_interval_seconds or 0.0))
        if interval <= 0:
            return {
                'wait_seconds': 0.0,
                'minimum_interval_seconds': 0.0,
            }

        normalized_key = str(key or 'openclaw').strip() or 'openclaw'
        with self._lock:
            now = self._clock()
            scheduled_at = max(
                now,
                self._next_dispatch_at.get(normalized_key, now),
            )
            self._next_dispatch_at[normalized_key] = scheduled_at + interval
        wait_seconds = max(0.0, scheduled_at - now)
        if wait_seconds > 0:
            self._sleep(wait_seconds)
        return {
            'wait_seconds': wait_seconds,
            'minimum_interval_seconds': interval,
        }

_OPENCLAW_DISPATCH_COORDINATOR = _OpenClawDispatchCoordinator()
_OPENCLAW_DISPATCH_PACER = _OpenClawDispatchPacer()


def _parse_persisted_snapshot_json_object(
    value: Any,
) -> tuple[dict[str, Any], str | None]:
    if isinstance(value, dict):
        try:
            payload_size_bytes = len(
                json.dumps(
                    value,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(',', ':'),
                ).encode('utf-8')
            )
        except (RecursionError, TypeError, UnicodeEncodeError, ValueError):
            return {}, 'json_decode_error'
        if payload_size_bytes > PERSISTED_SNAPSHOT_JSON_MAX_BYTES:
            return {}, 'json_payload_too_large'
        return dict(value), None
    if not isinstance(value, str):
        return {}, 'json_shape_not_object'
    try:
        payload_size_bytes = len(value.encode('utf-8'))
    except UnicodeEncodeError:
        return {}, 'json_decode_error'
    if payload_size_bytes > PERSISTED_SNAPSHOT_JSON_MAX_BYTES:
        return {}, 'json_payload_too_large'
    try:
        parsed, failure = decode_strict_agent_json_object(value)
    except RecursionError:
        return {}, 'json_decode_error'
    if failure is not None or not isinstance(parsed, dict):
        return {}, failure or 'json_shape_not_object'
    return parsed, None


def _can_reuse_decider_input_revalidation(
    *,
    snapshot: dict[str, Any] | None,
    dispatch_started_monotonic: float,
    dispatch_completed_monotonic: float | None,
) -> bool:
    if snapshot is None or dispatch_completed_monotonic is None:
        return False
    validation_started_monotonic = snapshot.get(
        'validation_started_monotonic'
    )
    try:
        validation_started_monotonic = float(
            validation_started_monotonic
        )
        dispatch_completed_monotonic = float(
            dispatch_completed_monotonic
        )
    except (TypeError, ValueError):
        return False
    return bool(
        dispatch_started_monotonic <= dispatch_completed_monotonic
        < validation_started_monotonic
    )


class AgentDispatchError(RuntimeError):
    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.details = details or {}


class AgentDispatchRetryableError(AgentDispatchError):
    pass


class AgentDispatchTerminalError(AgentDispatchError):
    pass


def _is_retryable_dispatch_exception(exc: Exception) -> bool:
    if not isinstance(exc, AgentDispatchTerminalError):
        return True
    details = exc.details if isinstance(exc.details, dict) else {}
    return details.get('retryable') is True


@dataclass(frozen=True)
class AgentDispatchSpec:
    role: str
    agent_name: str
    instruction: str


UPSTREAM_SUMMARY_ROLES: tuple[str, ...] = SUMMARIZER_AGENT_NAMES
OPENCLAW_BATCH_RESPONSE_TOOL_NAME = 'submit_agent_feedback_batch'


DISPATCH_SPECS: tuple[AgentDispatchSpec, ...] = (
    AgentDispatchSpec(
        role='eod-summarizer',
        agent_name='eod-summarizer',
        instruction=(
            'Assess each supplied symbol independently for end-of-day data readiness, emphasizing per-symbol completeness, freshness, anomalies, and data gaps. '
            'Do not infer market-wide breadth, leaders, or laggards from the bounded dispatch batch.'
        ),
    ),
    AgentDispatchSpec(
        role='technical-summarizer',
        agent_name='technical-summarizer',
        instruction='Summarize the latest technical setup conservatively using only the supplied inputs, emphasizing trend, momentum, support/resistance, invalidation risks, anomalies, and data gaps.',
    ),
    AgentDispatchSpec(
        role='fundamental-summarizer',
        agent_name='fundamental-summarizer',
        instruction=(
            'Summarize the latest fundamentals conservatively using the supplied market and source metadata: KRX inputs may come from DART filings/XBRL, while US inputs may come from SEC EDGAR filings/company facts. '
            'Emphasize business quality, balance-sheet risks, cash-flow traits, anomalies, and data gaps without assuming every payload is DART-specific.'
        ),
    ),
    AgentDispatchSpec(
        role=SHORT_TERM_DECIDER_AGENT_NAME,
        agent_name=SHORT_TERM_DECIDER_AGENT_NAME,
        instruction=(
            'Synthesize the specialist summaries, market context, flow context, momentum, volume, price-action, and data-quality inputs into short-term tactical decision support only, '
            'classifying each symbol as scalp_buy_candidate, watch_for_trigger, avoid_chase, wait, or reject for a 1-3 trading day horizon. '
            'Prioritize liquidity, fresh participation, clean trigger/invalidation levels, and chase-risk control. '
            'Every watch_for_trigger, avoid_chase, or wait decision must name one primary wait_reason taxonomy blocker plus a concise actionable explanation. '
            'If previous_decision_context is supplied, summarize the delta instead of repeating the earlier wait explanation: state what changed, what stayed the same, whether the setup is persistent or improving or worsening, and the concrete trigger that upgrades the setup. '
            'Account for market_regime_context when choosing scalp_buy_candidate, watch_for_trigger, wait, or reject: defensive/risk_off defaults to wait/reject unless the candidate shows exceptional market/sector leadership; caution/normal require setup confirmation; aggressive_open and trend_day can support faster or continuation-oriented entries only inside the supplied bounded window and without sizing or safety-gate claims. '
            'Treat missing news/disclosure detail and missing detailed quarterly/cash-flow detail as auxiliary by default, not standalone blockers, unless the setup explicitly depends on them. '
            'When intraday_wait_recheck_context is supplied, treat its immutable fresh best-ask observation only as a request to reassess the prior WAIT; the touch is not buy authority, does not override any blocker, and may be promoted only to watch_for_trigger after a fresh conservative review. '
            'Write all operator-facing feedback in Korean while staying JSON-only, and avoid execution logic, leverage, sizing, or fake precision.'
        ),
    ),
    AgentDispatchSpec(
        role=SIGNAL_DECIDER_AGENT_NAME,
        agent_name=SIGNAL_DECIDER_AGENT_NAME,
        instruction=(
            'Operate as an independent swing, position, and long-horizon lane rather than a duplicate of or subordinate filter for the short-term lane, which normally holds 2-3 trading days, may exit earlier on invalidation, stop, or target, and never exceeds 3 trading sessions. '
            'Synthesize the three specialist summaries into conservative decision support using this lane\'s own entry, target, stop, invalidation, and holding-horizon evidence, '
            'choosing buy, wait, or reject while surfacing evidence, disagreement, invalidation, data-quality warnings, '
            'and explicit realistic target and stop-loss levels for every symbol. '
            'Every wait decision must name one primary wait_reason taxonomy blocker plus a concise actionable explanation. '
            'If previous_decision_context is supplied, summarize the delta instead of repeating the earlier wait explanation: state what changed, what stayed the same, whether the setup is persistent or improving or worsening, and the concrete trigger that upgrades the setup to buy. '
            'When intraday_wait_recheck_context is supplied, treat its immutable fresh best-ask observation only as a request to reassess this lane\'s prior WAIT; the touch is not buy authority, does not override any blocker, and may be promoted only to buy with complete fresh trigger, chase-limit, target, and stop geometry after a conservative review. '
            'Account for market_regime_context when choosing buy, wait, or reject: defensive/risk_off defaults to wait/reject unless the candidate shows exceptional market/sector leadership; caution/normal require setup confirmation; aggressive_open and trend_day can support faster or continuation-oriented entries only inside the supplied bounded window and without sizing or safety-gate claims. '
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


def _requested_scope_eligibility_collapse(
    *,
    requested_symbols: Sequence[str],
    dispatch_symbols: Sequence[str],
    diagnostics: dict[str, Any],
) -> dict[str, Any] | None:
    requested_count = len(_normalize_requested_symbols(requested_symbols))
    if requested_count < 10:
        return None
    records_by_symbol = diagnostics.get('records_by_symbol') if isinstance(diagnostics, dict) else {}
    if not isinstance(records_by_symbol, dict):
        return None
    missing_market_risk_symbols: list[str] = []
    for symbol, record in records_by_symbol.items():
        hard_block_reasons = record.get('hard_block_reasons') if isinstance(record, dict) else []
        if not isinstance(hard_block_reasons, list):
            continue
        if any(
            isinstance(reason, dict) and reason.get('code') == 'missing_market_risk_filter'
            for reason in hard_block_reasons
        ):
            missing_market_risk_symbols.append(str(symbol))
    missing_ratio = len(missing_market_risk_symbols) / requested_count if requested_count else 0.0
    dispatch_ratio = len(dispatch_symbols) / requested_count if requested_count else 1.0
    if not missing_market_risk_symbols:
        return None
    if missing_ratio < 0.25:
        return None
    return {
        'status': 'blocked',
        'ready': False,
        'reason': 'requested_scope_missing_market_risk_filter_coverage',
        'requested_symbol_count': requested_count,
        'dispatch_symbol_count': len(dispatch_symbols),
        'dispatch_ratio': round(dispatch_ratio, 6),
        'missing_market_risk_symbol_count': len(missing_market_risk_symbols),
        'missing_market_risk_ratio': round(missing_ratio, 6),
        'missing_market_risk_symbols_sample': missing_market_risk_symbols[:20],
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


DEFAULT_OPENCLAW_AGENT_TIMEOUT_SECONDS = 300.0
DEFAULT_OPENCLAW_AGENT_BATCH_TIMEOUT_SECONDS = 300.0
RUN_LEVEL_FAILURE_SYMBOL = '__RUN__'
OPENCLAW_GATEWAY_AGENT_NAME = 'openclaw-gateway'


def _is_shared_openclaw_transport_outage(
    *,
    exc: BaseException | None = None,
    error: str | None = None,
) -> bool:
    if isinstance(exc, (httpx.ConnectError, httpx.ConnectTimeout, httpx.ProxyError)):
        return True
    normalized = ' '.join(
        part.strip().lower()
        for part in (type(exc).__name__ if exc is not None else '', str(exc or ''), str(error or ''))
        if part and part.strip()
    )
    return any(
        marker in normalized
        for marker in (
            'connecterror',
            'connecttimeout',
            'proxyerror',
            'remoteprotocolerror',
            'connection refused',
            'connection reset',
            'connection aborted',
            'server disconnected',
            'all connection attempts failed',
            'name or service not known',
            'temporary failure in name resolution',
            'nodename nor servname provided',
            'no route to host',
            'network is unreachable',
            'errno 61',
            'errno 111',
        )
    )


def _has_openclaw_provider_rate_limit_marker(
    *,
    error: str | None = None,
) -> bool:
    normalized = str(error or '').strip().lower()
    return any(
        marker in normalized
        for marker in (
            'openclaw_provider_rate_limited',
            'provider/rate-limit',
            'rate_limit',
            'rate limit',
            'too many requests',
            'http 429',
            'error 429',
            'status: 429',
        )
    )


def _is_shared_openclaw_dispatch_outage(
    *,
    exc: BaseException | None = None,
    error: str | None = None,
    response_status_code: int | None = None,
) -> bool:
    if _is_shared_openclaw_transport_outage(exc=exc, error=error):
        return True
    if response_status_code == 429:
        return True
    return _has_openclaw_provider_rate_limit_marker(
        error=' '.join(
            part.strip()
            for part in (str(exc or ''), str(error or ''))
            if part and part.strip()
        ),
    )


class OpenClawAgentClient:
    def __init__(self) -> None:
        self.base_url = settings.openclaw_base_url.rstrip('/')
        self.gateway_token = settings.openclaw_gateway_token
        configured_timeout = float(settings.openclaw_agent_timeout_seconds or 0)
        self.timeout_seconds = (
            configured_timeout if configured_timeout > 0 else DEFAULT_OPENCLAW_AGENT_TIMEOUT_SECONDS
        )
        self.http_timeout_seconds = self.timeout_seconds
        configured_batch_timeout = float(getattr(settings, 'openclaw_agent_batch_timeout_seconds', 0) or 0)
        self.batch_http_timeout_seconds = (
            min(self.http_timeout_seconds, configured_batch_timeout, DEFAULT_OPENCLAW_AGENT_BATCH_TIMEOUT_SECONDS)
            if configured_batch_timeout > 0
            else min(self.http_timeout_seconds, DEFAULT_OPENCLAW_AGENT_BATCH_TIMEOUT_SECONDS)
        )
        self.max_retries = max(0, settings.openclaw_agent_max_retries)
        self.retry_backoff_seconds = max(0, settings.openclaw_agent_retry_backoff_seconds)
        self.minimum_dispatch_interval_seconds = max(
            0.0,
            float(
                getattr(
                    settings,
                    'openclaw_agent_min_dispatch_interval_seconds',
                    0.0,
                )
                or 0.0
            ),
        )
        self.rate_limit_fallback_cooldown_seconds = min(
            _MAX_OPENCLAW_RATE_LIMIT_COOLDOWN_SECONDS,
            max(
                1,
                int(
                    getattr(
                        settings,
                        'openclaw_agent_rate_limit_fallback_cooldown_seconds',
                        900,
                    )
                    or 900
                ),
            ),
        )
        self._dispatch_coordinator = _OPENCLAW_DISPATCH_COORDINATOR
        self._dispatch_pacer = _OPENCLAW_DISPATCH_PACER

    @staticmethod
    def _plugin_state_capacity_error(
        response: httpx.Response,
    ) -> AgentDispatchTerminalError | None:
        response_text = response.text[:1000]
        normalized = response_text.lower()
        if (
            'app-server-thread-bindings' not in normalized
            or 'row limit' not in normalized
        ):
            return None
        return AgentDispatchTerminalError(
            'OpenClaw Codex app-server binding state capacity is exhausted',
            details={
                'failure_kind': (
                    FeedbackFailureKind.OPENCLAW_PLUGIN_STATE_CAPACITY_EXHAUSTED.value
                ),
                'failure_status': 'capacity/exhausted',
                'failure_category': 'capacity',
                'response_status_code': response.status_code,
                'response_text_excerpt': response_text[:300],
                'retryable': False,
                'shared_dispatch_outage': True,
            },
        )

    def _provider_rate_limit_error(
        self,
        response: httpx.Response,
        *,
        operation: str,
    ) -> AgentDispatchRetryableError | None:
        wrapped_provider_rate_limit = bool(
            response.status_code >= 400
            and _has_openclaw_provider_rate_limit_marker(
                error=response.text[:1000],
            )
        )
        if response.status_code != 429 and not wrapped_provider_rate_limit:
            return None
        now = datetime.now(timezone.utc)
        retry_after_seconds = self.rate_limit_fallback_cooldown_seconds
        retry_after_source = 'configured-fallback'
        response_headers = getattr(response, 'headers', None) or {}
        raw_retry_after = str(
            response_headers.get('Retry-After', '') or ''
        ).strip()
        if raw_retry_after:
            try:
                parsed_seconds = float(raw_retry_after)
            except (TypeError, ValueError):
                try:
                    parsed_deadline = parsedate_to_datetime(raw_retry_after)
                    if parsed_deadline.tzinfo is None:
                        parsed_deadline = parsed_deadline.replace(
                            tzinfo=timezone.utc
                        )
                    parsed_seconds = (
                        parsed_deadline.astimezone(timezone.utc) - now
                    ).total_seconds()
                    retry_after_source = 'retry-after-http-date'
                except (AttributeError, TypeError, ValueError, OverflowError):
                    parsed_seconds = None
            else:
                retry_after_source = 'retry-after-seconds'
            if (
                parsed_seconds is not None
                and math.isfinite(parsed_seconds)
                and (
                    retry_after_source != 'retry-after-seconds'
                    or parsed_seconds >= 0
                )
            ):
                retry_after_seconds = max(1, math.ceil(parsed_seconds))
            else:
                retry_after_source = 'configured-fallback'
        retry_after_seconds = min(
            _MAX_OPENCLAW_RATE_LIMIT_COOLDOWN_SECONDS,
            max(1, int(retry_after_seconds)),
        )
        retry_not_before = now + timedelta(seconds=retry_after_seconds)
        return AgentDispatchRetryableError(
            f'OpenClaw {operation} provider rate limit '
            f'({response.status_code}): {response.text[:300]}',
            details={
                'failure_kind': (
                    FeedbackFailureKind.OPENCLAW_PROVIDER_RATE_LIMITED.value
                ),
                'failure_status': 'provider/rate-limit',
                'failure_category': 'provider',
                'response_status_code': response.status_code,
                'response_text_excerpt': response.text[:300],
                'retryable': True,
                'shared_dispatch_outage': True,
                'shared_transport_outage': False,
                'retry_after_seconds': retry_after_seconds,
                'retry_not_before': retry_not_before.isoformat(),
                'retry_after_source': retry_after_source,
            },
        )

    @staticmethod
    def _rate_limit_retry_not_before(
        details: Mapping[str, Any],
    ) -> datetime | None:
        return _openclaw_rate_limit_retry_not_before(details)

    def _wait_for_dispatch_slot(self) -> dict[str, Any]:
        return self._dispatch_pacer.wait(
            key=self.base_url,
            minimum_interval_seconds=self.minimum_dispatch_interval_seconds,
        )

    @contextmanager
    def _admit_model_dispatch(self):
        with self._dispatch_coordinator.admit(
            key=self.base_url
        ) as completion_admission:
            pacing_admission = self._wait_for_dispatch_slot()
            completion_wait_seconds = float(
                completion_admission.get('wait_seconds') or 0.0
            )
            pacing_wait_seconds = float(
                pacing_admission.get('wait_seconds') or 0.0
            )
            yield {
                **pacing_admission,
                **completion_admission,
                'wait_seconds': (
                    completion_wait_seconds + pacing_wait_seconds
                ),
                'completion_wait_seconds': completion_wait_seconds,
                'fixed_pacing_wait_seconds': pacing_wait_seconds,
            }

    def _raise_if_circuit_broken(
        self,
        *,
        request_run_id: str | None,
        allow_reserved_reprobe: bool = False,
    ) -> bool:
        normalized_run_id = str(request_run_id or '').strip() or None
        storage_admission = inspect_openclaw_storage_admission(
            state_dir=getattr(settings, 'openclaw_state_dir', '~/.openclaw'),
            min_free_bytes=getattr(
                settings,
                'openclaw_dispatch_min_free_bytes',
                0,
            ),
        )
        if storage_admission.get('admitted') is not True:
            error = AgentDispatchTerminalError(
                'OpenClaw dispatch blocked by local storage admission guard',
                details={
                    'failure_kind': 'openclaw_storage_admission_blocked',
                    'failure_status': 'capacity/storage-admission-blocked',
                    'failure_category': 'capacity',
                    'retryable': True,
                    'shared_dispatch_outage': True,
                    'storage_admission': storage_admission,
                },
            )
            self._trip_circuit_breaker(
                error,
                request_run_id=normalized_run_id,
            )
            raise error

        return self._dispatch_coordinator.check_circuit(
            key=self.base_url,
            request_run_id=normalized_run_id,
            allow_reserved_reprobe=allow_reserved_reprobe,
        )

    def _trip_circuit_breaker(
        self,
        error: AgentDispatchError,
        *,
        request_run_id: str | None,
    ) -> None:
        self._dispatch_coordinator.trip_circuit(
            key=self.base_url,
            error=error,
            request_run_id=request_run_id,
        )

    def _finish_circuit_breaker_reprobe(
        self,
        *,
        request_run_id: str | None,
        recovered: bool,
    ) -> None:
        self._dispatch_coordinator.finish_reprobe(
            key=self.base_url,
            request_run_id=request_run_id,
            recovered=recovered,
        )

    def _shared_transport_circuit_error(
        self,
        *,
        exc: Exception,
        operation: str,
    ) -> AgentDispatchRetryableError | None:
        if not _is_shared_openclaw_transport_outage(exc=exc):
            return None
        classification = classify_feedback_failure(
            failure_kind=None,
            summary={
                'origin': f'openclaw_{operation}',
                'exception_type': type(exc).__name__,
                'error': str(exc),
            },
            error=str(exc),
            exception_type=type(exc).__name__,
        )
        return AgentDispatchRetryableError(
            f'OpenClaw {operation} shared transport outage: {exc}',
            details={
                'failure_kind': classification.failure_kind,
                'failure_status': classification.failure_status,
                'failure_category': classification.failure_category,
                'failure_classification': classification.as_payload(),
                'failure_mode': classification.failure_kind,
                'retryable': True,
                'shared_transport_outage': True,
                'shared_dispatch_outage': True,
                'exception_type': type(exc).__name__,
                'error_message': str(exc),
            },
        )

    @staticmethod
    def _shared_transport_response_circuit_error(
        response: httpx.Response,
        *,
        operation: str,
    ) -> AgentDispatchRetryableError | None:
        response_text = response.text[:1000]
        if (
            response.status_code < 500
            or not _is_shared_openclaw_transport_outage(error=response_text)
        ):
            return None
        error_message = (
            f'OpenClaw {operation} HTTP {response.status_code}: '
            f'{response_text[:300]}'
        )
        classification = classify_feedback_failure(
            failure_kind=None,
            summary={
                'origin': f'openclaw_{operation}',
                'response_status_code': response.status_code,
                'error': error_message,
            },
            context={'response_status_code': response.status_code},
            error=error_message,
            exception_type='HTTPStatusError',
        )
        return AgentDispatchRetryableError(
            f'OpenClaw {operation} shared transport outage: '
            f'{response_text[:300]}',
            details={
                'failure_kind': classification.failure_kind,
                'failure_status': classification.failure_status,
                'failure_category': classification.failure_category,
                'failure_classification': classification.as_payload(),
                'failure_mode': classification.failure_kind,
                'retryable': True,
                'shared_transport_outage': True,
                'shared_dispatch_outage': True,
                'exception_type': 'HTTPStatusError',
                'response_status_code': response.status_code,
                'response_text_excerpt': response_text[:300],
                'error_message': error_message,
            },
        )

    def _post_model_request(
        self,
        *,
        role: str,
        operation: str,
        url: str,
        body: dict[str, Any],
        headers: dict[str, str],
        timeout_seconds: float,
        request_run_id: str | None,
        capacity_reprobe: bool,
        retry_contract_anchor: dict[str, Any] | None,
        retry_contract_change_message: str,
        failed_contract_change_message: str,
        started_event: str,
        started_log_fields: dict[str, Any],
    ) -> tuple[
        httpx.Response,
        dict[str, Any] | None,
        dict[str, Any],
        dict[str, Any],
        bool,
    ]:
        with self._admit_model_dispatch() as dispatch_admission:
            try:
                log_agent_feedback_event(
                    started_event,
                    status='started',
                    dispatch_rate_admission=dispatch_admission,
                    http_target={'url': url, 'headers': headers},
                    request_snapshot=build_request_snapshot(body),
                    **started_log_fields,
                )
                pre_runtime_contract = build_runtime_contract_provenance(
                    role,
                    openclaw_state_dir=getattr(
                        settings,
                        'openclaw_state_dir',
                        '~/.openclaw',
                    ),
                )
                if retry_contract_anchor is None:
                    retry_contract_anchor = pre_runtime_contract
                else:
                    retry_contract_proof = (
                        build_dispatch_runtime_contract_provenance(
                            pre_dispatch=retry_contract_anchor,
                            post_dispatch=pre_runtime_contract,
                        )
                    )
                    if (
                        retry_contract_proof is not None
                        and retry_contract_proof.get('verified') is not True
                    ):
                        raise AgentDispatchTerminalError(
                            retry_contract_change_message,
                            details={
                                'runtime_contract': retry_contract_proof
                            },
                        )
                capacity_reprobe = self._raise_if_circuit_broken(
                    request_run_id=request_run_id,
                    allow_reserved_reprobe=capacity_reprobe,
                )
                try:
                    with httpx.Client(
                        timeout=timeout_seconds,
                        headers=headers,
                    ) as client:
                        response = client.post(url, json=body)
                except Exception as exc:
                    transport_error = self._shared_transport_circuit_error(
                        exc=exc,
                        operation=operation,
                    )
                    if transport_error is not None:
                        self._trip_circuit_breaker(
                            transport_error,
                            request_run_id=request_run_id,
                        )
                    raise

                capacity_error = self._plugin_state_capacity_error(response)
                if capacity_error is not None:
                    self._trip_circuit_breaker(
                        capacity_error,
                        request_run_id=request_run_id,
                    )
                    raise capacity_error
                rate_limit_error = self._provider_rate_limit_error(
                    response,
                    operation=operation,
                )
                if rate_limit_error is not None:
                    self._trip_circuit_breaker(
                        rate_limit_error,
                        request_run_id=request_run_id,
                    )
                    raise rate_limit_error
                transport_response_error = (
                    self._shared_transport_response_circuit_error(
                        response,
                        operation=operation,
                    )
                )
                if transport_response_error is not None:
                    self._trip_circuit_breaker(
                        transport_response_error,
                        request_run_id=request_run_id,
                    )
                    raise transport_response_error

                post_runtime_contract = build_runtime_contract_provenance(
                    role,
                    openclaw_state_dir=getattr(
                        settings,
                        'openclaw_state_dir',
                        '~/.openclaw',
                    ),
                )
                runtime_contract = (
                    build_dispatch_runtime_contract_provenance(
                        pre_dispatch=pre_runtime_contract,
                        post_dispatch=post_runtime_contract,
                    )
                )
                if (
                    response.status_code >= 400
                    and runtime_contract is not None
                    and runtime_contract.get('verified') is not True
                ):
                    raise AgentDispatchTerminalError(
                        failed_contract_change_message,
                        details={
                            'runtime_contract': runtime_contract,
                            'response_status_code': response.status_code,
                            'response_text_excerpt': response.text[:300],
                        },
                    )
                if capacity_reprobe and response.status_code < 400:
                    self._finish_circuit_breaker_reprobe(
                        request_run_id=request_run_id,
                        recovered=True,
                    )
                    capacity_reprobe = False
                return (
                    response,
                    runtime_contract,
                    dispatch_admission,
                    retry_contract_anchor,
                    capacity_reprobe,
                )
            except Exception:
                if capacity_reprobe:
                    self._finish_circuit_breaker_reprobe(
                        request_run_id=request_run_id,
                        recovered=False,
                    )
                raise

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

    def preflight_dispatch_auth(self) -> dict[str, Any]:
        if not self.gateway_token:
            return {
                'ok': False,
                'status': 'failed',
                'failure_kind': FeedbackFailureKind.OPENCLAW_AUTH_MISSING.value,
                'failure_status': 'auth/missing',
                'failure_category': 'auth',
                'retryable': False,
                'error': 'OPENCLAW_GATEWAY_TOKEN is not configured',
                'token_configured': False,
            }

        try:
            url = self._sessions_history_url()
        except AgentDispatchTerminalError as exc:
            classification = classify_feedback_failure(
                failure_kind=None,
                summary={'origin': 'openclaw_auth_preflight'},
                error=str(exc),
                exception_type=type(exc).__name__,
            )
            return {
                'ok': False,
                'status': 'failed',
                'reason': 'openclaw_base_url_invalid',
                'failure_kind': classification.failure_kind,
                'failure_status': classification.failure_status,
                'failure_category': classification.failure_category,
                'retryable': False,
                'error_type': type(exc).__name__,
                'error': str(exc),
                'token_configured': True,
            }

        body = {
            'tool': 'sessions_history',
            'args': {
                'sessionKey': f'core-auth-preflight/{uuid.uuid4().hex[:12]}',
                'limit': 1,
                'includeTools': False,
            },
        }
        timeout_seconds = max(1.0, min(float(self.http_timeout_seconds or 0) or 1.0, 5.0))
        try:
            with httpx.Client(timeout=timeout_seconds, headers=self._headers()) as client:
                response = client.post(url, json=body)
        except Exception as exc:  # noqa: BLE001
            classification = classify_feedback_failure(
                failure_kind=None,
                summary={'origin': 'openclaw_auth_preflight'},
                error=str(exc),
                exception_type=type(exc).__name__,
            )
            return {
                'ok': False,
                'status': 'failed',
                'reason': 'openclaw_auth_preflight_transport_unavailable',
                'failure_kind': classification.failure_kind,
                'failure_status': classification.failure_status,
                'failure_category': classification.failure_category,
                'retryable': classification.failure_category == 'transport',
                'shared_transport_outage': _is_shared_openclaw_transport_outage(
                    exc=exc,
                ),
                'shared_dispatch_outage': _is_shared_openclaw_dispatch_outage(
                    exc=exc,
                ),
                'error_type': type(exc).__name__,
                'error': str(exc),
                'token_configured': True,
                'timeout_seconds': timeout_seconds,
            }

        response_text_excerpt = self._response_text_excerpt(response, limit=300)
        if response.status_code in (401, 403):
            return {
                'ok': False,
                'status': 'failed',
                'failure_kind': FeedbackFailureKind.OPENCLAW_AUTH_REJECTED.value,
                'failure_status': 'auth/rejected',
                'failure_category': 'auth',
                'retryable': False,
                'error': f'OpenClaw gateway rejected auth with HTTP {response.status_code}',
                'token_configured': True,
                'response_status_code': response.status_code,
                'response_text_excerpt': response_text_excerpt,
            }
        provider_rate_limit_error = self._provider_rate_limit_error(
            response,
            operation='auth preflight',
        )
        if provider_rate_limit_error is not None:
            self._trip_circuit_breaker(
                provider_rate_limit_error,
                request_run_id=None,
            )
            return {
                'ok': False,
                'status': 'failed',
                'reason': 'openclaw_auth_preflight_provider_rate_limited',
                'error': str(provider_rate_limit_error),
                'token_configured': True,
                **provider_rate_limit_error.details,
            }
        if response.status_code >= 400:
            classification = classify_feedback_failure(
                failure_kind=None,
                summary={
                    'origin': 'openclaw_auth_preflight',
                    'response_status_code': response.status_code,
                },
                context={'response_status_code': response.status_code},
                error=(
                    f'OpenClaw auth preflight HTTP {response.status_code}'
                ),
                exception_type='HTTPStatusError',
            )
            return {
                'ok': False,
                'status': 'failed',
                'reason': 'openclaw_auth_preflight_non_auth_http_error',
                'failure_kind': classification.failure_kind,
                'failure_status': classification.failure_status,
                'failure_category': classification.failure_category,
                'retryable': (
                    response.status_code in {408, 409, 425, 429}
                    or response.status_code >= 500
                ),
                'shared_dispatch_outage': _is_shared_openclaw_dispatch_outage(
                    response_status_code=response.status_code,
                    error=response_text_excerpt,
                ),
                'token_configured': True,
                'response_status_code': response.status_code,
                'response_text_excerpt': response_text_excerpt,
            }
        return {
            'ok': True,
            'status': 'ready',
            'token_configured': True,
            'response_status_code': response.status_code,
        }

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
        return decode_strict_agent_json_object(candidate_text)

    @staticmethod
    def _build_batch_response_tool(
        *,
        spec: AgentDispatchSpec,
        batch_symbols: Sequence[str],
    ) -> dict[str, Any]:
        symbols = list(dict.fromkeys(str(symbol).strip() for symbol in batch_symbols if str(symbol).strip()))
        item_count = len(symbols)
        return {
            'type': 'function',
            'name': OPENCLAW_BATCH_RESPONSE_TOOL_NAME,
            'description': (
                'Submit exactly one canonical feedback envelope for every assigned symbol. '
                'Do not omit, duplicate, or add symbols.'
            ),
            'parameters': {
                'type': 'object',
                'additionalProperties': False,
                'required': ['items'],
                'properties': {
                    'items': {
                        'type': 'array',
                        'minItems': item_count,
                        'maxItems': item_count,
                        'items': {
                            'type': 'object',
                            'additionalProperties': False,
                            'required': ['symbol', 'feedback'],
                            'properties': {
                                'symbol': {
                                    'type': 'string',
                                    'enum': symbols,
                                },
                                'feedback': {
                                    'type': 'object',
                                    'additionalProperties': True,
                                    'required': [
                                        'role',
                                        'summary',
                                        'confidence',
                                        'risk_flags',
                                        'details',
                                        'improvement_notes',
                                    ],
                                    'properties': {
                                        'role': {'type': 'string', 'enum': [spec.role]},
                                        'summary': {'type': 'string'},
                                        'confidence': {'type': 'number', 'minimum': 0, 'maximum': 1},
                                        'risk_flags': {
                                            'type': 'array',
                                            'items': {'type': 'string'},
                                        },
                                        'details': {'type': 'object'},
                                        'improvement_notes': {
                                            'type': 'array',
                                            'items': {'type': 'string'},
                                        },
                                    },
                                },
                            },
                        },
                    }
                },
            },
        }

    @staticmethod
    def _extract_batch_function_call_arguments(
        parsed: dict[str, Any],
    ) -> tuple[str | None, str | None, bool]:
        output = parsed.get('output')
        output_items = output if isinstance(output, list) else []
        function_calls = [
            item
            for item in output_items
            if isinstance(item, dict) and item.get('type') == 'function_call'
        ]
        if not function_calls:
            return None, None, False
        if len(function_calls) != 1:
            return None, 'function_call_cardinality_mismatch', True
        function_call = function_calls[0]
        if function_call.get('name') != OPENCLAW_BATCH_RESPONSE_TOOL_NAME:
            return None, 'function_call_name_mismatch', True
        arguments = function_call.get('arguments')
        if isinstance(arguments, dict):
            return json.dumps(arguments, ensure_ascii=False), None, True
        if isinstance(arguments, str) and arguments.strip():
            return arguments.strip(), None, True
        return None, 'function_call_arguments_missing', True

    @staticmethod
    def _failure_class_for_mode(failure_mode: str | None) -> str:
        if failure_mode in {
            None,
            'json_object_not_found',
            'json_decode_error',
            'json_duplicate_key',
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
        shared_transport_outage = bool(
            classification.failure_category == 'transport'
            and _is_shared_openclaw_transport_outage(
                exc=exc,
                error=str(exc),
            )
        )
        shared_dispatch_outage = _is_shared_openclaw_dispatch_outage(
            exc=exc,
            error=str(exc),
            response_status_code=response_status_code,
        )
        return {
            **context,
            'failure_kind': classification.failure_kind,
            'failure_status': classification.failure_status,
            'failure_category': classification.failure_category,
            'failure_classification': classification.as_payload(),
            'failure_mode': classification.failure_kind,
            'retryable': True,
            'shared_transport_outage': shared_transport_outage,
            'shared_dispatch_outage': shared_dispatch_outage,
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

    def _session_sqlite_store_path(self, *, agent_name: str) -> Path:
        return (
            Path(settings.openclaw_state_dir).expanduser()
            / 'agents'
            / agent_name
            / 'agent'
            / 'openclaw-agent.sqlite'
        )

    def _read_sqlite_session_identity_entry(
        self,
        *,
        agent_name: str,
        session_key: str,
    ) -> tuple[str, dict[str, Any], dict[str, int] | None]:
        store_path = self._session_sqlite_store_path(agent_name=agent_name)
        try:
            store_path.lstat()
        except FileNotFoundError:
            return 'missing', {}, None
        except OSError:
            return 'invalid', {}, None

        try:
            before_fingerprint = fingerprint_regular_file(store_path)
            if (
                before_fingerprint['uid'] != os.getuid()
                or before_fingerprint['mode']
                & (stat.S_IWGRP | stat.S_IWOTH)
            ):
                return 'invalid', {}, None
            absolute_store_path = store_path.expanduser().absolute()
            # The store is owned by the same local OS account as Core and
            # OpenClaw. sqlite3 cannot consume the verified descriptor while
            # retaining WAL semantics, so schema ownership and a post-query
            # inode check close the trusted-local path boundary.
            connection = sqlite3.connect(
                f'{absolute_store_path.as_uri()}?mode=ro',
                uri=True,
                timeout=0.25,
            )
            try:
                connection.row_factory = sqlite3.Row
                connection.execute('PRAGMA query_only = ON')
                connection.execute('BEGIN')
                user_version_row = connection.execute(
                    'PRAGMA user_version'
                ).fetchone()
                schema_rows = connection.execute(
                    '''
                    SELECT role, schema_version, agent_id
                    FROM schema_meta
                    WHERE meta_key = 'primary'
                    LIMIT 2
                    '''
                ).fetchall()
                rows = connection.execute(
                    '''
                    SELECT
                        n.current_session_id,
                        n.entry_valid,
                        n.status AS node_status,
                        n.updated_at AS node_updated_at,
                        w.session_id,
                        w.session_key AS window_session_key,
                        w.status AS window_status,
                        w.started_at,
                        w.ended_at,
                        w.updated_at AS window_updated_at,
                        w.model_provider,
                        w.model,
                        w.agent_harness_id
                    FROM session_nodes AS n
                    JOIN session_windows AS w
                      ON w.session_id = n.current_session_id
                     AND w.session_key = n.session_key
                    WHERE n.session_key = ?
                    LIMIT 2
                    ''',
                    (session_key,),
                ).fetchall()
            finally:
                connection.close()
            after_fingerprint = fingerprint_regular_file(store_path)
        except (SafeFileReadError, OSError, sqlite3.Error, ValueError):
            return 'invalid', {}, None

        user_version = (
            user_version_row[0]
            if user_version_row is not None and len(user_version_row) == 1
            else None
        )
        schema_row = schema_rows[0] if len(schema_rows) == 1 else None
        if (
            user_version != OPENCLAW_AGENT_SQLITE_SCHEMA_VERSION
            or schema_row is None
            or schema_row['role'] != 'agent'
            or schema_row['schema_version']
            != OPENCLAW_AGENT_SQLITE_SCHEMA_VERSION
            or schema_row['agent_id'] != agent_name
            or len(rows) != 1
            or rows[0]['agent_harness_id'] != 'codex'
            or any(
                before_fingerprint[field] != after_fingerprint[field]
                for field in ('dev', 'ino')
            )
        ):
            return 'invalid', {}, None
        row = rows[0]
        entry = {
            'sessionId': row['current_session_id'],
            'entryValid': row['entry_valid'],
            'nodeStatus': row['node_status'],
            'nodeUpdatedAt': row['node_updated_at'],
            'status': row['window_status'],
            'startedAt': row['started_at'],
            'endedAt': row['ended_at'],
            'updatedAt': row['node_updated_at'],
            'modelProvider': row['model_provider'],
            'model': row['model'],
        }
        fingerprint = {
            **before_fingerprint,
            'node_updated_at': int(row['node_updated_at'] or 0),
            'session_updated_at': int(row['node_updated_at'] or 0),
            'window_updated_at': int(row['window_updated_at'] or 0),
        }
        return 'present', entry, fingerprint

    def _observe_post_dispatch_model_identity(
        self,
        *,
        role: str,
        agent_name: str,
        session_key: str,
        expected_model: str | None,
        dispatch_started_at: datetime,
        session_id: str | None = None,
    ) -> dict[str, Any] | None:
        if expected_model is None:
            return build_observed_model_identity(
                role=role,
                expected_model=expected_model,
                actual_provider=None,
                actual_model=None,
            )
        started_ms = int(dispatch_started_at.timestamp() * 1000)
        entry: dict[str, Any] = {}
        source_fingerprint: dict[str, int] | None = None
        retry_delays = (0.0, *OPENCLAW_MODEL_IDENTITY_OBSERVATION_RETRY_DELAYS_SECONDS)
        with _OPENCLAW_SESSION_REGISTRY_OBSERVATION_LOCK:
            for retry_delay in retry_delays:
                if retry_delay > 0:
                    time.sleep(retry_delay)
                sqlite_state, candidate, candidate_fingerprint = (
                    self._read_sqlite_session_identity_entry(
                        agent_name=agent_name,
                        session_key=session_key,
                    )
                )
                if sqlite_state != 'present':
                    continue

                status = str(candidate.get('status') or '').strip().lower()
                if status not in {'completed', 'done'}:
                    continue
                if (
                    candidate.get('entryValid') != 1
                    or str(candidate.get('nodeStatus') or '').strip().lower()
                    != 'done'
                ):
                    continue

                entry_started_ms = candidate.get('startedAt')
                entry_ended_ms = candidate.get('endedAt')
                entry_updated_ms = candidate.get('updatedAt')
                timing_fields_complete = all(
                    isinstance(value, (int, float)) and not isinstance(value, bool)
                    for value in (entry_started_ms, entry_ended_ms, entry_updated_ms)
                )
                if not timing_fields_complete:
                    continue
                observed_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                timing_valid = bool(
                    entry_started_ms >= started_ms - 5_000
                    and entry_started_ms <= observed_ms + 5_000
                    and entry_ended_ms >= entry_started_ms
                    and entry_ended_ms <= observed_ms + 5_000
                    and entry_updated_ms >= entry_ended_ms
                    and entry_updated_ms <= observed_ms + 5_000
                )
                if not timing_valid:
                    continue

                observed_session_id = str(candidate.get('sessionId') or '').strip() or None
                if observed_session_id is None:
                    continue
                if session_id is not None and observed_session_id != session_id:
                    continue
                if not str(candidate.get('modelProvider') or '').strip() or not str(
                    candidate.get('model') or ''
                ).strip():
                    continue
                entry = candidate
                source_fingerprint = candidate_fingerprint
                break
        return build_observed_model_identity(
            role=role,
            expected_model=expected_model,
            actual_provider=str(entry.get('modelProvider') or '') or None,
            actual_model=str(entry.get('model') or '') or None,
            actual_source_fingerprint=source_fingerprint,
            actual_identity_source='openclaw_session_store_sqlite',
        )

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
        contract_prompt_suffix = build_contract_prompt_suffix(role=spec.role)
        is_us_batch = any(
            str((item.get('symbol_day_context') or {}).get('market') or '').upper() == 'US'
            for item in batch_items
            if isinstance(item, dict)
        )
        short_term_example_levels = (
            (103.0, 105.0, 109.0, 99.0)
            if is_us_batch
            else (10300, 10500, 10900, 9900)
        )
        signal_example_levels = (105.0, 98.0) if is_us_batch else (10500, 9800)
        signal_wait_reason = (
            {
                'code': 'insufficient_risk_reward',
                'summary': '전일 종가 기준 목표수익 대비 손실위험이 충분히 유리하지 않습니다.',
            }
            if is_us_batch
            else {
                'code': 'stale_quote_orderbook',
                'summary': '실시간 호가 신선도가 떨어져 진입 가격 신뢰가 부족합니다.',
            }
        )

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
                'preferred_action': 'watch_for_trigger',
                'wait_reason': {
                    'code': 'unsuitable_execution_timing',
                    'summary': '돌파 트리거 확인 전이라 진입 시점이 아직 이릅니다.',
                },
                'trigger_price': short_term_example_levels[0],
                'chase_limit_price': short_term_example_levels[1],
                'target_price': short_term_example_levels[2],
                'stop_loss_price': short_term_example_levels[3],
                'expected_holding_period': '1-3 trading days',
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
                'wait_reason': signal_wait_reason,
                'entry_price_basis': 'next_open',
                'target_price': signal_example_levels[0],
                'expected_return_pct': None,
                'stop_loss_price': signal_example_levels[1],
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
            "Interpret signal_decider_policy.performance_criteria as auditable replay context only when usage_policy.model_guidance_eligible=true. It may support conservative review or tighter buy thresholds; when manual_review_only=true it must never relax wait/reject into buy or authorize a buy. Deterministic execution may consume it only when usage_policy.deterministic_gate_eligible=true, and that eligibility never activates rule candidates or grants buy authority.\n"
            "Interpret market_regime_context as the authoritative market/sector regime guardrail: defensive/risk_off should default to wait/reject unless exceptional relative/sector leadership is evident; normal preserves existing sizing/entry rules; aggressive_open and trend_day may support faster or continuation-oriented entries only inside the supplied bounded window, while quantity_multiplier remains an exec-side capped hint and never bypasses stop-loss or account/symbol/notional safety gates.\n"
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
            f"{contract_prompt_suffix}\n"
            f"JSON example:\n{json.dumps(example, ensure_ascii=False)}\n\n"
            f"Market date: {market_date.isoformat()}\n"
            f"Assigned batch payload:\n{json.dumps(list(prepared_items), ensure_ascii=False)}"
        )
        max_chars = int(settings.openclaw_agent_max_prompt_chars or 0)
        if max_chars <= 0 or len(base_message) <= max_chars:
            return base_message
        compressed_items = [_compressed_batch_item_for_prompt(item) for item in batch_items]
        compact_message = (
            f"Request marker: {request_marker}\n"
            f"Role: {spec.role}\n"
            f"Instruction: {spec.instruction}\n"
            f"Assigned symbols (exact authoritative strings): {assigned_symbols_json}\n"
            "Analyze every assigned symbol directly and return one aggregated JSON object.\n"
            "Return {\"items\": [{\"symbol\": \"...\", \"feedback\": {...}}]} with exactly one item per assigned symbol.\n"
            "Every item.symbol must exactly match one assigned symbol string; do not omit feedback.symbol when the role contract requires it.\n"
            f"{signal_decider_batch_rules}"
            "No tool_code, no print(json.dumps(...)), no markdown fences, no intent tags, no prose, no request restatement, raw JSON object only.\n"
            "All operator-facing natural-language fields inside the JSON must be written in Korean.\n"
            "For decision-support outputs, provide explicit target_price and stop_loss_price levels for every assigned symbol whenever possible; short-term-decider must also provide trigger_price and chase_limit_price. If a level is genuinely not supportable, keep the key and explain the limitation in data_quality_warnings.\n"
            "Interpret decider_data_quality_context.decision_priority literally: core_execution gaps can justify conservative wait/reject, while auxiliary_context gaps are confidence reducers only by default.\n"
            "Interpret decision_input_quality_context.final_judgment and execution_feasibility as the source-attributed buy-quality guardrail; hard_blocks should prevent buy/watch upgrades unless directly contradicted by fresher supplied evidence.\n"
            "Interpret signal_decider_policy.performance_criteria as auditable replay context only when usage_policy.model_guidance_eligible=true. It may support conservative review or tighter buy thresholds; when manual_review_only=true it must never relax wait/reject into buy or authorize a buy. Deterministic execution may consume it only when usage_policy.deterministic_gate_eligible=true, and that eligibility never activates rule candidates or grants buy authority.\n"
            "Interpret market_regime_context as the authoritative market/sector regime guardrail: defensive/risk_off should default to wait/reject unless exceptional relative/sector leadership is evident; normal preserves existing sizing/entry rules; aggressive_open and trend_day may support faster or continuation-oriented entries only inside the supplied bounded window, while quantity_multiplier remains an exec-side capped hint and never bypasses stop-loss or account/symbol/notional safety gates.\n"
            "Empty recent news means no captured recent news and is neutral unless separate risk evidence exists.\n"
            "Do not forecast price reaction from news alone; use external news only as source-backed material/noise context, and do not treat headline-only, paywalled, or fetch-failed items as full-text summaries.\n"
            "Missing detailed quarterly/cash-flow context should usually reduce confidence only, not make the symbol untradeable on its own.\n"
            f"{contract_prompt_suffix}\n"
            f"Market date: {market_date.isoformat()}\n"
            f"Assigned batch payload:\n{json.dumps(compressed_items, ensure_ascii=False)}"
        )
        if len(compact_message) <= max_chars:
            return compact_message
        raise AgentDispatchTerminalError(
            'OpenClaw prompt budget is too small to encode the requested batch without dropping assigned symbols',
            details={
                'failure_kind': 'prompt_budget_exceeded_without_symbol_loss',
                'assigned_symbols': assigned_symbols,
                'assigned_symbol_count': len(assigned_symbols),
                'prompt_chars': len(compact_message),
                'max_prompt_chars': max_chars,
            },
        )

    def dispatch(self, *, spec: AgentDispatchSpec, payload: dict[str, Any]) -> dict[str, Any]:
        url = f'{self.base_url}/v1/responses'
        request_session_key = str(payload.get('session_key') or f"agent:{spec.agent_name}:feedback/{spec.agent_name}/{payload.get('request_id') or uuid.uuid4().hex}")
        request_run_id = (
            str(payload.get('request_run_id') or request_session_key).strip() or None
        )
        expected_model = expected_decider_model(spec.role, settings_obj=settings)
        requested_model_identity = build_requested_model_identity(
            role=spec.role,
            expected_model=expected_model,
        )
        body = {
            'model': f'openclaw/{spec.agent_name}',
            'input': self._build_message(spec=spec, payload=payload),
        }
        attempt = 0
        last_error: Exception | None = None
        last_error_details: dict[str, Any] | None = None
        retry_contract_anchor: dict[str, Any] | None = None
        dispatch_started_at: datetime | None = None
        capacity_reprobe = False
        dispatch_rate_admission: dict[str, Any] | None = None
        # Reposting a full prompt under the same OpenClaw session key appends it
        # to the same native thread. Retries therefore belong to the durable
        # outer retry cycle, which allocates a fresh per-batch session key.
        in_session_max_retries = 0
        while attempt <= in_session_max_retries:
            attempt += 1
            started_at = datetime.now(timezone.utc)
            if dispatch_started_at is None:
                dispatch_started_at = started_at
            try:
                if not capacity_reprobe:
                    capacity_reprobe = self._raise_if_circuit_broken(
                        request_run_id=request_run_id,
                    )
                headers = self._headers() | {
                    'x-openclaw-session-key': request_session_key,
                    'x-openclaw-agent-id': spec.agent_name,
                }
                if expected_model is not None:
                    headers['x-openclaw-model'] = expected_model
                (
                    response,
                    runtime_contract,
                    dispatch_rate_admission,
                    retry_contract_anchor,
                    capacity_reprobe,
                ) = self._post_model_request(
                    role=spec.role,
                    operation='dispatch',
                    url=url,
                    body=body,
                    headers=headers,
                    timeout_seconds=self.timeout_seconds,
                    request_run_id=request_run_id,
                    capacity_reprobe=capacity_reprobe,
                    retry_contract_anchor=retry_contract_anchor,
                    retry_contract_change_message=(
                        'OpenClaw runtime contract changed between retry attempts'
                    ),
                    failed_contract_change_message=(
                        'OpenClaw runtime contract changed during failed '
                        'dispatch attempt'
                    ),
                    started_event='dispatch_single_http_target',
                    started_log_fields={
                        'agent_name': spec.agent_name,
                        'role': spec.role,
                        'session_key': request_session_key,
                    },
                )
                if response.status_code >= 500:
                    raise AgentDispatchRetryableError(
                        f'OpenClaw server error {response.status_code}: '
                        f'{response.text[:300]}'
                    )
                if response.status_code in (408, 409):
                    raise AgentDispatchRetryableError(
                        f'OpenClaw retryable error {response.status_code}: '
                        f'{response.text[:300]}'
                    )
                if response.status_code >= 400:
                    raise AgentDispatchTerminalError(
                        f'OpenClaw dispatch failed {response.status_code}: '
                        f'{response.text[:300]}'
                    )
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
                normalized_feedback = normalize_agent_response(
                    role=spec.role,
                    output_text=output_text,
                    raw_response=parsed,
                    market_date=payload.get('market_date') or payload.get('trade_date'),
                    symbol=str(payload.get('symbol') or '') or None,
                    agent_name=spec.agent_name,
                )
                model_identity = self._observe_post_dispatch_model_identity(
                    role=spec.role,
                    agent_name=spec.agent_name,
                    session_key=request_session_key,
                    expected_model=expected_model,
                    dispatch_started_at=dispatch_started_at,
                    session_id=(
                        str(parsed.get('session_id')).strip()
                        if parsed.get('session_id') is not None
                        else None
                    ),
                )
                normalized_feedback = apply_decider_model_identity_fence(
                    role=spec.role,
                    normalized_feedback=normalized_feedback,
                    model_identity=model_identity,
                    expected_model=expected_model,
                    runtime_contract=runtime_contract,
                )
                if capacity_reprobe:
                    self._finish_circuit_breaker_reprobe(
                        request_run_id=request_run_id,
                        recovered=True,
                    )
                    capacity_reprobe = False
                return {
                    'ok': True,
                    'attempt': attempt,
                    'started_at': started_at.isoformat(),
                    'completed_at': datetime.now(timezone.utc).isoformat(),
                    'response': parsed,
                    'output_text': output_text,
                    'session_ref': self._extract_session_ref(parsed, explicit_session_key=request_session_key),
                    'normalized_feedback': normalized_feedback,
                    'model_identity': model_identity,
                    'request_provenance': {
                        'model': body['model'],
                        'role': spec.role,
                        'requested_model_identity': requested_model_identity,
                        'model_identity': model_identity,
                        'runtime_contract': runtime_contract,
                        'dispatch_rate_admission': dispatch_rate_admission,
                    },
                }
            except AgentDispatchTerminalError as exc:
                if capacity_reprobe:
                    self._finish_circuit_breaker_reprobe(
                        request_run_id=request_run_id,
                        recovered=False,
                    )
                    capacity_reprobe = False
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
                if capacity_reprobe:
                    self._finish_circuit_breaker_reprobe(
                        request_run_id=request_run_id,
                        recovered=False,
                    )
                    capacity_reprobe = False
                last_error = exc
                log_agent_feedback_event(
                    'dispatch_single_response_error',
                    status='retryable' if attempt <= in_session_max_retries else 'failed',
                    market_date=str(payload.get('market_date') or payload.get('trade_date') or '') or None,
                    role=spec.role,
                    agent_name=spec.agent_name,
                    symbol=str(payload.get('symbol') or '') or None,
                    session_key=request_session_key,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                if (
                    isinstance(exc, AgentDispatchError)
                    and exc.details.get('shared_dispatch_outage') is True
                ):
                    break
                if attempt > in_session_max_retries:
                    break
                if self.retry_backoff_seconds > 0:
                    time.sleep(self.retry_backoff_seconds * attempt)
        if isinstance(last_error, AgentDispatchError) and last_error.details:
            raise last_error
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
                self._wait_for_dispatch_slot()
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
        expected_model = expected_decider_model(spec.role, settings_obj=settings)
        requested_model_identity = build_requested_model_identity(
            role=spec.role,
            expected_model=expected_model,
        )
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
        circuit_breaker_run_id = str(request_run_id or dispatch_id).strip() or None
        attempt = 0
        last_error: Exception | None = None
        retry_contract_anchor: dict[str, Any] | None = None
        dispatch_started_at: datetime | None = None
        capacity_reprobe = False
        dispatch_rate_admission: dict[str, Any] | None = None
        # Keep each OpenClaw thread single-shot. The durable outer retry cycle
        # owns retries and supplies a fresh per-batch session key.
        in_session_max_retries = 0
        while attempt <= in_session_max_retries:
            attempt += 1
            started_at = datetime.now(timezone.utc)
            if dispatch_started_at is None:
                dispatch_started_at = started_at
            response: httpx.Response | None = None
            attempt_id = make_attempt_id(dispatch_id=dispatch_id, attempt=attempt)
            request_marker = f"batch-request::{attempt_id or dispatch_id}"
            model_name = f'openclaw/{spec.agent_name}'
            prompt_text = self._build_batch_message(
                spec=spec,
                market_date=market_date,
                batch_items=batch_items,
                request_marker=request_marker,
            )
            request_provenance = {
                **build_agent_contract_provenance(role=spec.role),
                'model': model_name,
                'prompt_sha256': f"sha256:{hashlib.sha256(prompt_text.encode('utf-8')).hexdigest()}",
                'prompt_chars': len(prompt_text),
                'requested_model_identity': requested_model_identity,
                'model_identity': None,
                'runtime_contract': None,
            }
            body = {
                'model': model_name,
                'input': prompt_text,
                'instructions': (
                    'Return the batch by calling '
                    f'{OPENCLAW_BATCH_RESPONSE_TOOL_NAME} exactly once with '
                    'the canonical items envelope. If this runtime cannot '
                    'emit a client function call, return only that same '
                    'canonical JSON object as plain output text; do not add '
                    'Markdown or explanatory prose.'
                ),
                'tools': [
                    self._build_batch_response_tool(
                        spec=spec,
                        batch_symbols=batch_symbols,
                    )
                ],
                # Some installed Codex-backed OpenClaw agents currently
                # serialize the requested envelope as JSON text even when a
                # client function is pinned. OpenClaw turns that otherwise
                # valid response into HTTP 502. ``auto`` keeps the schema
                # visible and accepts a real function call when supported,
                # while the existing strict JSON/correlation checks retain a
                # fail-closed text fallback without consuming retry capacity.
                'tool_choice': 'auto',
            }
            try:
                if not capacity_reprobe:
                    capacity_reprobe = self._raise_if_circuit_broken(
                        request_run_id=circuit_breaker_run_id,
                    )
                request_session_key = fixed_session_key
                headers = self._headers() | {
                    'x-openclaw-session-key': request_session_key,
                    'x-openclaw-agent-id': spec.agent_name,
                }
                if expected_model is not None:
                    headers['x-openclaw-model'] = expected_model
                (
                    response,
                    runtime_contract,
                    dispatch_rate_admission,
                    retry_contract_anchor,
                    capacity_reprobe,
                ) = self._post_model_request(
                    role=spec.role,
                    operation='batch',
                    url=url,
                    body=body,
                    headers=headers,
                    timeout_seconds=self.batch_http_timeout_seconds,
                    request_run_id=circuit_breaker_run_id,
                    capacity_reprobe=capacity_reprobe,
                    retry_contract_anchor=retry_contract_anchor,
                    retry_contract_change_message=(
                        'OpenClaw runtime contract changed between batch '
                        'retry attempts'
                    ),
                    failed_contract_change_message=(
                        'OpenClaw runtime contract changed during failed '
                        'batch dispatch attempt'
                    ),
                    started_event='dispatch_batch_request',
                    started_log_fields={
                        'request_run_id': request_run_id,
                        'dispatch_id': dispatch_id,
                        'attempt_id': attempt_id,
                        'agent_name': spec.agent_name,
                        'role': spec.role,
                        'market_date': market_date.isoformat(),
                        'session_key': request_session_key,
                        'batch_symbols': batch_symbols,
                        'slot': slot,
                        'session_reuse_mode': session_reuse_mode,
                        'request_marker': request_marker,
                    },
                )
                request_provenance[
                    'dispatch_rate_admission'
                ] = dispatch_rate_admission
                request_provenance['runtime_contract'] = runtime_contract
                if response.status_code >= 500:
                    raise AgentDispatchRetryableError(
                        f'OpenClaw batch server error '
                        f'{response.status_code}: {response.text[:300]}'
                    )
                if response.status_code in (408, 409):
                    raise AgentDispatchRetryableError(
                        f'OpenClaw batch retryable error '
                        f'{response.status_code}: {response.text[:300]}'
                    )
                if response.status_code >= 400:
                    raise AgentDispatchTerminalError(
                        f'OpenClaw batch failed {response.status_code}: '
                        f'{response.text[:300]}'
                    )
                parsed = response.json()
                message_output_text = None
                for item in parsed.get('output', []):
                    if not isinstance(item, dict) or item.get('type') != 'message':
                        continue
                    for content in item.get('content', []):
                        if isinstance(content, dict) and content.get('type') == 'output_text':
                            message_output_text = content.get('text')
                            break
                    if message_output_text:
                        break
                (
                    function_call_arguments,
                    function_call_failure_mode,
                    function_call_seen,
                ) = self._extract_batch_function_call_arguments(parsed)
                output_text = (
                    function_call_arguments
                    if function_call_seen
                    else message_output_text
                )
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
                decoded_source = (
                    'function_call_arguments'
                    if function_call_seen
                    else 'output_text'
                )
                session_ref = self._extract_session_ref(parsed, explicit_session_key=request_session_key) or {
                    'session_key': request_session_key,
                    'session_id': None,
                    'response_id': parsed.get('id'),
                }
                if session_ref.get('response_id') is None:
                    session_ref['response_id'] = parsed.get('id')
                model_identity = self._observe_post_dispatch_model_identity(
                    role=spec.role,
                    agent_name=spec.agent_name,
                    session_key=request_session_key,
                    expected_model=expected_model,
                    dispatch_started_at=dispatch_started_at,
                    session_id=(
                        str(session_ref.get('session_id')).strip()
                        if session_ref.get('session_id') is not None
                        else None
                    ),
                )
                request_provenance['model_identity'] = model_identity
                response_id = (
                    str(session_ref.get('response_id')).strip()
                    if session_ref.get('response_id') is not None
                    else None
                ) or None
                session_history: list[dict[str, Any]] | None = None
                session_history_diagnostics: dict[str, Any] = {'checked': False, 'request_marker': request_marker}
                trajectory_diagnostics: dict[str, Any] = {'checked': False}
                history_marker_found = None
                failure_mode = function_call_failure_mode or json_failure_class
                if (
                    decoded is None
                    and request_session_key
                    and not function_call_seen
                ):
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
                        '_coerced_from_top_level_object': True,
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
                if capacity_reprobe:
                    self._finish_circuit_breaker_reprobe(
                        request_run_id=circuit_breaker_run_id,
                        recovered=True,
                    )
                    capacity_reprobe = False
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
                    'request_provenance': request_provenance,
                    'model_identity': model_identity,
                }
            except AgentDispatchTerminalError:
                if capacity_reprobe:
                    self._finish_circuit_breaker_reprobe(
                        request_run_id=circuit_breaker_run_id,
                        recovered=False,
                    )
                    capacity_reprobe = False
                raise
            except Exception as exc:  # noqa: BLE001
                if capacity_reprobe:
                    self._finish_circuit_breaker_reprobe(
                        request_run_id=circuit_breaker_run_id,
                        recovered=False,
                    )
                    capacity_reprobe = False
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
                    status='retryable' if attempt <= in_session_max_retries else 'failed',
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
                if (
                    isinstance(error_details, dict)
                    and error_details.get('shared_dispatch_outage') is True
                ):
                    break
                if attempt > in_session_max_retries:
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
        source_knowledge_cutoff_at: str | None = None,
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
        payload['decision_input_generation'] = _build_decider_input_generation(
            source_context=item,
            market_context=(
                payload.get('market_context')
                if isinstance(payload.get('market_context'), dict)
                else {}
            ),
            policy_context=(
                payload.get('signal_decider_policy')
                if isinstance(payload.get('signal_decider_policy'), dict)
                else {}
            ),
            previous_decision_context=(
                payload.get('previous_decision_context')
                if isinstance(payload.get('previous_decision_context'), dict)
                else {}
            ),
            upstream_summaries=(
                payload.get('upstream_summaries')
                if isinstance(payload.get('upstream_summaries'), dict)
                else {}
            ),
            upstream_traceability=(
                payload.get('upstream_traceability')
                if isinstance(payload.get('upstream_traceability'), dict)
                else {}
            ),
        )
        payload['durable_decision_input_generation'] = (
            _build_durable_decider_input_generation(
                source_context=item,
                source_knowledge_cutoff_at=source_knowledge_cutoff_at,
                policy_context=(
                    payload.get('signal_decider_policy')
                    if isinstance(payload.get('signal_decider_policy'), dict)
                    else {}
                ),
                previous_decision_context=(
                    payload.get('previous_decision_context')
                    if isinstance(payload.get('previous_decision_context'), dict)
                    else {}
                ),
                upstream_summaries=(
                    payload.get('upstream_summaries')
                    if isinstance(payload.get('upstream_summaries'), dict)
                    else {}
                ),
                upstream_traceability=(
                    payload.get('upstream_traceability')
                    if isinstance(payload.get('upstream_traceability'), dict)
                    else {}
                ),
            )
        )
        return payload

    def _revalidate_decider_dispatch_inputs(
        self,
        *,
        market_date: date,
        policy_context_as_of_date: date | None = None,
        request_run_id: str,
        source_items: Sequence[dict[str, Any]],
        source_context_scope_symbols: Sequence[str],
        eligibility_scope_symbols: Sequence[str] | None,
        fs_div: str | None,
        disclosures_limit: int,
        allow_current_run_feedback: bool = False,
        revalidate_market_context: bool = True,
        allow_persisted_current_run_upstream: bool = False,
        durable_source_generation: bool = False,
        source_knowledge_cutoff_at: str | None = None,
        authoritative_summary_run_id: str | None = None,
        revalidate_post_decision_material_context: bool = False,
        post_decision_check_at: datetime | None = None,
    ) -> dict[tuple[str, str], dict[str, Any]]:
        symbols = _normalize_requested_symbols(
            [str(item.get('symbol') or '') for item in source_items]
        )
        context_scope_symbols = _normalize_requested_symbols(
            source_context_scope_symbols
        )
        source_keys = {
            (
                str(
                    item.get('role') or item.get('agent_name') or ''
                ).strip(),
                str(item.get('symbol') or '').strip().upper(),
            )
            for item in source_items
            if str(item.get('symbol') or '').strip()
        }
        failures: dict[tuple[str, str], dict[str, Any]] = {}

        try:
            refreshed_context = agent_context_read_service.get_symbol_day_context(
                market_date=market_date,
                symbols=context_scope_symbols,
                fs_div=fs_div,
                disclosures_limit=disclosures_limit,
                knowledge_cutoff_at=source_knowledge_cutoff_at,
            )
            refreshed_by_symbol = {
                str(item.get('symbol') or '').strip().upper(): item
                for item in (refreshed_context.get('items') or [])
                if isinstance(item, dict) and item.get('symbol')
            }
            refreshed_request = refreshed_context.get('request')
            if not isinstance(refreshed_request, dict):
                refreshed_request = {}
            refreshed_fs_div = refreshed_request.get('fs_div', fs_div)
            refreshed_disclosures_limit = int(
                refreshed_request.get(
                    'disclosures_limit',
                    disclosures_limit,
                )
            )
            refreshed_knowledge_cutoff_at = refreshed_request.get(
                'knowledge_cutoff_at',
                source_knowledge_cutoff_at,
            )
        except Exception as exc:  # noqa: BLE001
            for key in source_keys:
                failures[key] = {
                    'reason': 'source_context_revalidation_failed',
                    'error_type': type(exc).__name__,
                }
            return failures

        refreshed_market_context: dict[str, Any] | None = None
        if revalidate_market_context:
            try:
                refreshed_market_context = (
                    signal_decider_market_context_builder.build(
                        market_date=market_date
                    )
                )
            except Exception as exc:  # noqa: BLE001
                for key in source_keys:
                    failures[key] = {
                        'reason': 'market_context_revalidation_failed',
                        'error_type': type(exc).__name__,
                    }
                return failures

        try:
            if eligibility_scope_symbols is None:
                refreshed_records = load_agent_feedback_eligibility_records(
                    market_date=market_date,
                    requested_symbols=None,
                )
                refreshed_records_by_symbol = {
                    str(record.get('symbol') or '').strip().upper(): record
                    for record in refreshed_records
                    if str(record.get('symbol') or '').strip()
                }
                refreshed_eligible_set = {
                    symbol
                    for symbol, record in refreshed_records_by_symbol.items()
                    if not bool(record.get('hard_blocked'))
                    and not bool(record.get('cap_excluded'))
                }
                refreshed_eligibility = {
                    'records_by_symbol': refreshed_records_by_symbol,
                }
            else:
                refreshed_eligible_symbols, _, refreshed_eligibility = (
                    _split_requested_symbols_by_agent_feedback_eligibility_with_diagnostics(
                        market_date=market_date,
                        requested_symbols=eligibility_scope_symbols,
                    )
                )
                refreshed_eligible_set = set(refreshed_eligible_symbols)
        except Exception as exc:  # noqa: BLE001
            refreshed_eligible_set = set()
            refreshed_eligibility = {
                'lookup_error_type': type(exc).__name__,
            }

        state = runtime_feedback_state_store.get_run(
            market_date,
            run_id=request_run_id,
        )
        buffered_lookup = _feedback_row_lookup(
            state.buffered_feedback_rows if state is not None else []
        )
        persisted_source_symbols = {
            str(item.get('symbol') or '').strip().upper()
            for item in source_items
            if any(
                isinstance(trace, dict)
                and (
                    str(trace.get('source') or '').startswith('persisted_feedback')
                    or (
                        allow_persisted_current_run_upstream
                        and str(trace.get('source') or '') == 'current_run_buffer'
                    )
                )
                for trace in (
                    item.get('upstream_traceability', {}).values()
                    if isinstance(item.get('upstream_traceability'), dict)
                    else []
                )
            )
        }
        persisted_decisions: dict[str, dict[str, Any]] = {}
        persisted_lookup_error: str | None = None
        if persisted_source_symbols:
            try:
                refreshed_fundamental_fingerprints = {
                    symbol: build_fundamental_input_fingerprint_payload_from_context(
                        market_date=market_date,
                        symbol=symbol,
                        context=refreshed_by_symbol[symbol],
                        fs_div=refreshed_fs_div,
                        disclosures_limit=refreshed_disclosures_limit,
                        knowledge_cutoff_at=refreshed_knowledge_cutoff_at,
                    )
                    for symbol in sorted(persisted_source_symbols)
                    if symbol in refreshed_by_symbol
                }
                persisted_decisions = (
                    agent_context_reuse_service.build_latest_decisions(
                        market_date=market_date,
                        symbols=sorted(persisted_source_symbols),
                        roles=UPSTREAM_SUMMARY_ROLES,
                        force_recompute=False,
                        knowledge_cutoff_at=(
                            refreshed_knowledge_cutoff_at
                        ),
                        fs_div=refreshed_fs_div,
                        disclosures_limit=refreshed_disclosures_limit,
                        fundamental_input_fingerprint_payloads={
                            symbol: refreshed_fundamental_fingerprints[symbol]
                            for symbol in sorted(persisted_source_symbols)
                            if symbol in refreshed_fundamental_fingerprints
                        },
                        authoritative_summary_run_id=(
                            authoritative_summary_run_id
                        ),
                    )
                )
            except Exception as exc:  # noqa: BLE001
                persisted_lookup_error = type(exc).__name__

        decision_roles = sorted(
            {
                str(item.get('role') or item.get('agent_name') or '').strip()
                for item in source_items
                if str(item.get('role') or item.get('agent_name') or '').strip()
                in DECISION_SUPPORT_AGENT_NAMES
            }
        )
        current_decisions: dict[str, dict[str, Any]] = {}
        current_decision_lookup_error: str | None = None
        if decision_roles:
            try:
                current_decisions = (
                    agent_context_reuse_service.build_latest_decisions(
                        market_date=market_date,
                        symbols=symbols,
                        roles=decision_roles,
                        force_recompute=False,
                        knowledge_cutoff_at=source_knowledge_cutoff_at,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                current_decision_lookup_error = type(exc).__name__

        for source_item in source_items:
            symbol = str(source_item.get('symbol') or '').strip().upper()
            decision_role = str(
                source_item.get('role') or source_item.get('agent_name') or ''
            ).strip()
            reasons: list[dict[str, Any]] = []
            if symbol not in refreshed_eligible_set:
                record = (
                    refreshed_eligibility.get('records_by_symbol', {}).get(
                        symbol
                    )
                    if isinstance(
                        refreshed_eligibility.get('records_by_symbol'),
                        dict,
                    )
                    else None
                )
                reasons.append(
                    {
                        'code': 'eligibility_changed_or_unavailable',
                        'hard_block_reasons': (
                            record.get('hard_block_reasons')
                            if isinstance(record, dict)
                            else None
                        ),
                        'lookup_error_type': refreshed_eligibility.get(
                            'lookup_error_type'
                        ),
                    }
                )
            expected_generation = source_item.get(
                'durable_decision_input_generation'
                if durable_source_generation
                else 'decision_input_generation'
            )
            raw_refreshed_item = refreshed_by_symbol.get(symbol)
            refreshed_item = (
                dict(raw_refreshed_item)
                if isinstance(raw_refreshed_item, dict)
                else None
            )
            refreshed_eligibility_record = (
                refreshed_eligibility.get('records_by_symbol', {}).get(symbol)
                if isinstance(
                    refreshed_eligibility.get('records_by_symbol'),
                    dict,
                )
                else None
            )
            expected_symbol_context = source_item.get('symbol_day_context')
            eligibility_was_part_of_generation = bool(
                isinstance(expected_symbol_context, dict)
                and 'agent_feedback_eligibility' in expected_symbol_context
            )
            if isinstance(refreshed_item, dict) and eligibility_was_part_of_generation:
                if isinstance(refreshed_eligibility_record, dict):
                    refreshed_item['agent_feedback_eligibility'] = (
                        refreshed_eligibility_record
                    )
                else:
                    refreshed_item.pop('agent_feedback_eligibility', None)
            expected_wait_recheck_context = (
                expected_symbol_context.get('intraday_wait_recheck_context')
                if isinstance(expected_symbol_context, dict)
                and isinstance(
                    expected_symbol_context.get(
                        'intraday_wait_recheck_context'
                    ),
                    dict,
                )
                else None
            )
            if expected_wait_recheck_context is not None:
                from app.agent_context.wait_recheck_service import (
                    load_validated_wait_recheck_context,
                )

                try:
                    wait_recheck_event_id = int(
                        expected_wait_recheck_context.get('event_id')
                    )
                except (TypeError, ValueError):
                    wait_recheck_event_id = None
                validated_wait_recheck_context = (
                    load_validated_wait_recheck_context(
                        event_id=wait_recheck_event_id,
                        symbol=symbol,
                        agent_name=decision_role,
                    )
                    if wait_recheck_event_id is not None
                    else None
                )
                if (
                    validated_wait_recheck_context is None
                    or validated_wait_recheck_context
                    != expected_wait_recheck_context
                ):
                    reasons.append(
                        {'code': 'intraday_wait_recheck_context_changed'}
                    )
                elif isinstance(refreshed_item, dict):
                    refreshed_item['intraday_wait_recheck_context'] = (
                        validated_wait_recheck_context
                    )
            refreshed_policy_context: dict[str, Any] = {}
            try:
                if decision_role in DECISION_SUPPORT_AGENT_NAMES:
                    refreshed_policy_context = self._build_decider_policy_context(
                        role=decision_role,
                        market_date=market_date,
                        policy_context_as_of_date=policy_context_as_of_date,
                    )
            except Exception as exc:  # noqa: BLE001
                reasons.append(
                    {
                        'code': 'policy_context_revalidation_failed',
                        'error_type': type(exc).__name__,
                    }
                )

            expected_previous_context = (
                source_item.get('previous_decision_context')
                if isinstance(source_item.get('previous_decision_context'), dict)
                else {}
            )
            refreshed_previous_context = expected_previous_context
            current_decision = current_decisions.get(symbol, {}).get(
                decision_role
            )
            current_feedback = (
                getattr(current_decision, 'latest_feedback', None)
                if current_decision is not None
                else None
            )
            current_feedback_run_id = (
                str(current_feedback.get('run_id') or '').strip()
                if isinstance(current_feedback, dict)
                else ''
            )
            current_run_feedback_is_authoritative = bool(
                allow_current_run_feedback
                and current_feedback_run_id == request_run_id
                and not bool(
                    getattr(current_decision, 'should_dispatch', True)
                )
            )
            if current_decision_lookup_error is not None:
                reasons.append(
                    {
                        'code': 'previous_decision_revalidation_failed',
                        'error_type': current_decision_lookup_error,
                    }
                )
            elif not current_run_feedback_is_authoritative:
                refreshed_previous_context = (
                    build_previous_decision_context(
                        role=decision_role,
                        latest_feedback=(
                            current_feedback
                            if isinstance(current_feedback, dict)
                            else None
                        ),
                    )
                    or {}
                )
                if bool(
                    current_decision is not None
                    and getattr(current_decision, 'should_dispatch', False)
                    and current_feedback_run_id == request_run_id
                ):
                    reasons.append(
                        {
                            'code': 'current_run_decision_failure_detected',
                            'reuse_reason': getattr(
                                current_decision,
                                'reason',
                                None,
                            ),
                        }
                    )

            generation_inputs = {
                'source_context': refreshed_item,
                'policy_context': refreshed_policy_context,
                'previous_decision_context': refreshed_previous_context,
                'upstream_summaries': (
                    source_item.get('upstream_summaries')
                    if isinstance(source_item.get('upstream_summaries'), dict)
                    else {}
                ),
                'upstream_traceability': (
                    source_item.get('upstream_traceability')
                    if isinstance(source_item.get('upstream_traceability'), dict)
                    else {}
                ),
            }
            if isinstance(refreshed_item, dict):
                if durable_source_generation:
                    observed_generation = (
                        _build_durable_decider_input_generation(
                            source_knowledge_cutoff_at=(
                                source_knowledge_cutoff_at
                            ),
                            **generation_inputs
                        )
                    )
                else:
                    observed_generation = _build_decider_input_generation(
                        **generation_inputs,
                        market_context=(
                            refreshed_market_context
                            if revalidate_market_context
                            and isinstance(refreshed_market_context, dict)
                            else (
                                source_item.get('market_context')
                                if isinstance(
                                    source_item.get('market_context'), dict
                                )
                                else {}
                            )
                        ),
                    )
            else:
                observed_generation = None
            if not isinstance(expected_generation, dict):
                reasons.append({'code': 'source_generation_missing'})
            elif observed_generation is None:
                reasons.append({'code': 'source_context_missing'})
            else:
                component_reason_by_key = {
                    'source_context_fingerprint': 'source_context_changed',
                    'policy_context_fingerprint': 'policy_context_changed',
                    'previous_decision_context_fingerprint': (
                        'previous_decision_context_changed'
                    ),
                }
                if revalidate_market_context:
                    component_reason_by_key[
                        'market_context_fingerprint'
                    ] = 'market_context_changed'
                for component_key, reason_code in component_reason_by_key.items():
                    expected_component = expected_generation.get(component_key)
                    observed_component = observed_generation.get(component_key)
                    legacy_cutoff_marker_match = bool(
                        durable_source_generation
                        and component_key == 'source_context_fingerprint'
                        and isinstance(refreshed_item, dict)
                        and _durable_decider_source_context_fingerprint_matches(
                            expected_fingerprint=expected_component,
                            observed_fingerprint=observed_component,
                            refreshed_source_context=refreshed_item,
                            expected_source_context=source_item,
                        )
                    )
                    if (
                        expected_component != observed_component
                        and not legacy_cutoff_marker_match
                    ):
                        reasons.append({'code': reason_code})

            upstream_traceability = source_item.get('upstream_traceability')
            upstream_summaries = source_item.get('upstream_summaries')
            if not isinstance(upstream_traceability, dict):
                upstream_traceability = {}
            if not isinstance(upstream_summaries, dict):
                upstream_summaries = {}
            for upstream_role in UPSTREAM_SUMMARY_ROLES:
                trace = upstream_traceability.get(upstream_role)
                expected_summary = upstream_summaries.get(upstream_role)
                if not isinstance(trace, dict) or not isinstance(
                    expected_summary,
                    dict,
                ):
                    reasons.append(
                        {
                            'code': 'upstream_trace_missing',
                            'role': upstream_role,
                        }
                    )
                    continue
                source = str(trace.get('source') or '')
                if source == 'current_run_buffer':
                    buffered_row = buffered_lookup.get((symbol, upstream_role))
                    buffered_feedback = (
                        buffered_row.get('feedback')
                        if isinstance(buffered_row, dict)
                        and isinstance(buffered_row.get('feedback'), dict)
                        else None
                    )
                    buffered_identity_matches = bool(
                        buffered_feedback == expected_summary
                        and str((buffered_row or {}).get('run_id') or '')
                        == str(trace.get('run_id') or '')
                    )
                    if buffered_identity_matches:
                        continue
                    if allow_persisted_current_run_upstream:
                        if persisted_lookup_error is not None:
                            reasons.append(
                                {
                                    'code': (
                                        'persisted_current_run_upstream_'
                                        'revalidation_failed'
                                    ),
                                    'role': upstream_role,
                                    'error_type': persisted_lookup_error,
                                }
                            )
                            continue
                        decision = persisted_decisions.get(symbol, {}).get(
                            upstream_role
                        )
                        latest_feedback = (
                            getattr(decision, 'latest_feedback', None)
                            if decision is not None
                            else None
                        )
                        persisted_identity_matches = bool(
                            decision is not None
                            and not bool(
                                getattr(decision, 'should_dispatch', True)
                            )
                            and isinstance(latest_feedback, dict)
                            and str(latest_feedback.get('run_id') or '')
                            == str(trace.get('run_id') or '')
                            and latest_feedback.get('feedback_payload')
                            == expected_summary
                            and (
                                trace.get('feedback_id') is None
                                or str(latest_feedback.get('id'))
                                == str(trace.get('feedback_id'))
                            )
                            and (
                                trace.get('received_at') is None
                                or str(latest_feedback.get('received_at') or '')
                                == str(trace.get('received_at') or '')
                            )
                        )
                        if persisted_identity_matches:
                            continue
                        reasons.append(
                            {
                                'code': 'persisted_current_run_upstream_changed',
                                'role': upstream_role,
                                'reuse_reason': (
                                    getattr(decision, 'reason', None)
                                    if decision is not None
                                    else None
                                ),
                            }
                        )
                    else:
                        reasons.append(
                            {
                                'code': 'current_run_upstream_changed',
                                'role': upstream_role,
                            }
                        )
                    continue
                if not source.startswith('persisted_feedback'):
                    reasons.append(
                        {
                            'code': 'upstream_trace_source_invalid',
                            'role': upstream_role,
                        }
                    )
                    continue
                if persisted_lookup_error is not None:
                    reasons.append(
                        {
                            'code': 'persisted_upstream_revalidation_failed',
                            'role': upstream_role,
                            'error_type': persisted_lookup_error,
                        }
                    )
                    continue
                decision = persisted_decisions.get(symbol, {}).get(upstream_role)
                latest_feedback = (
                    getattr(decision, 'latest_feedback', None)
                    if decision is not None
                    else None
                )
                identity_matches = bool(
                    decision is not None
                    and not bool(getattr(decision, 'should_dispatch', True))
                    and isinstance(latest_feedback, dict)
                    and str(latest_feedback.get('run_id') or '')
                    == str(trace.get('run_id') or '')
                    and latest_feedback.get('feedback_payload') == expected_summary
                    and (
                        trace.get('feedback_id') is None
                        or str(latest_feedback.get('id'))
                        == str(trace.get('feedback_id'))
                    )
                    and (
                        trace.get('received_at') is None
                        or str(latest_feedback.get('received_at') or '')
                        == str(trace.get('received_at') or '')
                    )
                )
                if not identity_matches:
                    reasons.append(
                        {
                            'code': 'persisted_upstream_changed',
                            'role': upstream_role,
                            'reuse_reason': (
                                getattr(decision, 'reason', None)
                                if decision is not None
                                else None
                            ),
                        }
                    )

            if reasons:
                failures[(decision_role, symbol)] = {
                    'reason': 'decider_inputs_changed_during_dispatch',
                    'expected_generation': expected_generation,
                    'observed_generation': observed_generation,
                    'reasons': reasons,
                }
        if (
            revalidate_post_decision_material_context
            and source_knowledge_cutoff_at
        ):
            post_decision_failures = (
                self._load_post_decision_material_context_failures(
                    market_date=market_date,
                    source_items=source_items,
                    source_knowledge_cutoff_at=source_knowledge_cutoff_at,
                    disclosures_limit=disclosures_limit,
                    check_at=post_decision_check_at,
                )
            )
            for key, post_decision_failure in post_decision_failures.items():
                if key in failures:
                    failures[key]['post_decision_material_context'] = (
                        post_decision_failure
                    )
                else:
                    failures[key] = post_decision_failure
        return failures

    @staticmethod
    def _post_decision_material_reasons(
        *,
        market_date: date,
        news_items: Sequence[dict[str, Any]],
        disclosures: Sequence[dict[str, Any]],
        source_knowledge_cutoff_at: datetime | None = None,
    ) -> list[dict[str, Any]]:
        reasons: list[dict[str, Any]] = []
        concerning_news = []
        for item in news_items:
            if not isinstance(item, dict):
                continue
            classification = (
                item.get('classification')
                if isinstance(item.get('classification'), dict)
                else {}
            )
            materiality = str(
                item.get('materiality')
                or classification.get('materiality')
                or 'unknown'
            ).strip().lower()
            if materiality == 'noise':
                continue
            concerning_news.append(
                {
                    'external_news_item_id': item.get('id'),
                    'provider': item.get('provider'),
                    'provider_item_id': item.get('provider_item_id'),
                    'materiality': materiality,
                    'published_at': item.get('published_at'),
                    'collected_at': item.get('collected_at'),
                    'associated_at': item.get('associated_at'),
                    'classified_at': (
                        item.get('classification_provenance') or {}
                    ).get('classified_at'),
                }
            )
        if concerning_news:
            reasons.append(
                {
                    'code': 'post_decision_news_requires_redecision',
                    'event_count': len(concerning_news),
                    'events': concerning_news[:20],
                }
            )

        new_disclosures = []
        for item in disclosures:
            if not isinstance(item, dict):
                continue
            raw_receipt_date = item.get('receipt_date')
            try:
                receipt_date = (
                    raw_receipt_date.date()
                    if isinstance(raw_receipt_date, datetime)
                    else raw_receipt_date
                    if isinstance(raw_receipt_date, date)
                    else date.fromisoformat(str(raw_receipt_date))
                )
            except (TypeError, ValueError):
                # An unparseable new disclosure cannot safely authorize reuse.
                receipt_date = None

            raw_observed_at = item.get('created_at')
            try:
                observed_at = (
                    raw_observed_at
                    if isinstance(raw_observed_at, datetime)
                    else datetime.fromisoformat(
                        str(raw_observed_at).replace('Z', '+00:00')
                    )
                )
            except (TypeError, ValueError):
                observed_at = None
            if (
                observed_at is not None
                and observed_at.tzinfo is not None
                and observed_at.utcoffset() is not None
            ):
                observed_at = observed_at.astimezone(timezone.utc)
            else:
                observed_at = None

            observed_after_cutoff = bool(
                source_knowledge_cutoff_at is not None
                and observed_at is not None
                and observed_at >= source_knowledge_cutoff_at
            )
            same_day_observation_unavailable = bool(
                source_knowledge_cutoff_at is not None
                and receipt_date == market_date
                and observed_at is None
            )
            if (
                receipt_date is not None
                and receipt_date <= market_date
                and not observed_after_cutoff
                and not same_day_observation_unavailable
            ):
                continue
            new_disclosures.append(
                {
                    'rcept_no': item.get('rcept_no'),
                    'receipt_date': (
                        receipt_date.isoformat()
                        if receipt_date is not None
                        else raw_receipt_date
                    ),
                    'observed_at': (
                        observed_at.isoformat()
                        if observed_at is not None
                        else raw_observed_at
                    ),
                    'filing_category': item.get('filing_category'),
                    'is_financial_report': item.get('is_financial_report'),
                }
            )
        if new_disclosures:
            reasons.append(
                {
                    'code': 'post_decision_disclosure_requires_redecision',
                    'event_count': len(new_disclosures),
                    'events': new_disclosures[:20],
                }
            )
        return reasons

    def _load_post_decision_material_context_failures(
        self,
        *,
        market_date: date,
        source_items: Sequence[dict[str, Any]],
        source_knowledge_cutoff_at: str,
        disclosures_limit: int,
        check_at: datetime | None = None,
    ) -> dict[tuple[str, str], dict[str, Any]]:
        source_keys = {
            (
                str(item.get('role') or item.get('agent_name') or '').strip(),
                str(item.get('symbol') or '').strip().upper(),
            )
            for item in source_items
            if str(item.get('symbol') or '').strip()
        }
        if not source_keys:
            return {}
        try:
            cutoff = datetime.fromisoformat(
                str(source_knowledge_cutoff_at).replace('Z', '+00:00')
            )
        except (TypeError, ValueError):
            cutoff = None
        if (
            cutoff is None
            or cutoff.tzinfo is None
            or cutoff.utcoffset() is None
        ):
            return {
                key: {
                    'reason': 'post_decision_material_context_unavailable',
                    'reasons': [
                        {'code': 'source_knowledge_cutoff_missing_or_invalid'}
                    ],
                }
                for key in source_keys
            }
        cutoff = cutoff.astimezone(timezone.utc)
        authority_check_at = (check_at or datetime.now(timezone.utc))
        if (
            authority_check_at.tzinfo is None
            or authority_check_at.utcoffset() is None
        ):
            authority_check_at = authority_check_at.replace(
                tzinfo=timezone.utc
            )
        else:
            authority_check_at = authority_check_at.astimezone(timezone.utc)
        if authority_check_at <= cutoff:
            return {}

        symbols = sorted({symbol for _, symbol in source_keys})
        try:
            news_by_symbol: dict[str, list[dict[str, Any]]] = {}
            capped_news_symbols: set[str] = set()
            for symbol in symbols:
                news_items = list_external_news_items(
                    symbols=[symbol],
                    limit=POST_DECISION_NEWS_SCAN_LIMIT + 1,
                    published_before=authority_check_at,
                    collected_before=authority_check_at,
                    observed_after=cutoff,
                )
                if len(news_items) > POST_DECISION_NEWS_SCAN_LIMIT:
                    capped_news_symbols.add(symbol)
                news_by_symbol[symbol] = news_items[
                    :POST_DECISION_NEWS_SCAN_LIMIT
                ]
        except Exception as exc:  # noqa: BLE001
            return {
                key: {
                    'reason': 'post_decision_material_context_unavailable',
                    'reasons': [
                        {
                            'code': 'post_decision_news_lookup_failed',
                            'error_type': type(exc).__name__,
                        }
                    ],
                }
                for key in source_keys
            }

        try:
            disclosure_cutoff_date = authority_check_at.astimezone(
                ZoneInfo('Asia/Seoul')
            ).date()
            disclosures_by_symbol = (
                agent_context_read_service.disclosure_repository
                .list_recent_events_by_stock_codes(
                    stock_codes=symbols,
                    receipt_date_to=disclosure_cutoff_date,
                    observed_at_or_after=cutoff,
                    observed_before=authority_check_at,
                    limit_per_symbol=max(5, int(disclosures_limit)),
                )
            )
        except Exception as exc:  # noqa: BLE001
            return {
                key: {
                    'reason': 'post_decision_material_context_unavailable',
                    'reasons': [
                        {
                            'code': 'post_decision_disclosure_lookup_failed',
                            'error_type': type(exc).__name__,
                        }
                    ],
                }
                for key in source_keys
            }

        failures: dict[tuple[str, str], dict[str, Any]] = {}
        for key in source_keys:
            _, symbol = key
            if symbol in capped_news_symbols:
                failures[key] = {
                    'reason': 'post_decision_material_context_unavailable',
                    'reasons': [
                        {
                            'code': 'post_decision_news_scan_limit_exceeded',
                            'scan_limit': POST_DECISION_NEWS_SCAN_LIMIT,
                        }
                    ],
                }
                continue
            reasons = self._post_decision_material_reasons(
                market_date=market_date,
                news_items=news_by_symbol.get(symbol, []),
                disclosures=disclosures_by_symbol.get(symbol, []),
                source_knowledge_cutoff_at=cutoff,
            )
            if reasons:
                failures[key] = {
                    'reason': 'post_decision_material_context_changed',
                    'source_knowledge_cutoff_at': cutoff.isoformat(),
                    'reasons': reasons,
                }
        return failures

    def revalidate_persisted_decider_snapshots(
        self,
        *,
        market_date: str | date,
        snapshots: Sequence[Any],
        full_source_revalidation_actionable_only: bool = False,
        policy_context_as_of_date: str | date | None = None,
    ) -> dict[str, Any]:
        """Revalidate immutable decision inputs before next-open execution use.

        Non-actionable snapshots created before the durable generation fence remain
        readable for migration compatibility. An actionable legacy snapshot fails
        closed until a decider retry regenerates it with fenced inputs. Persisted
        payload strings must decode to bounded JSON objects before either path is
        classified. Newly fenced snapshots fail closed when their source-day EOD
        data, eligibility, policy, prior decision, or exact upstream lineage no
        longer matches. Current-session quote/order-book context is intentionally
        delegated to the next-open live market authority gate.

        Callers may limit the expensive durable source comparison to actionable
        snapshots. Payload, fence, and durable metadata validation still applies
        to every snapshot. The default retains full source revalidation for all
        fenced snapshots.
        """

        normalized_market_date = (
            market_date
            if isinstance(market_date, date)
            else date.fromisoformat(market_date)
        )
        normalized_policy_context_as_of_date = (
            policy_context_as_of_date.date()
            if isinstance(policy_context_as_of_date, datetime)
            else policy_context_as_of_date
            if isinstance(policy_context_as_of_date, date)
            else date.fromisoformat(policy_context_as_of_date)
            if policy_context_as_of_date is not None
            else None
        )
        validation_by_snapshot_id: dict[int, dict[str, Any]] = {}
        grouped: dict[str, dict[str, Any]] = {}

        for snapshot in snapshots:
            snapshot_id = int(getattr(snapshot, 'id'))
            request_payload, request_payload_error = (
                _parse_persisted_snapshot_json_object(
                    getattr(snapshot, 'request_payload', None)
                )
            )
            response_payload, response_payload_error = (
                _parse_persisted_snapshot_json_object(
                    getattr(snapshot, 'response_payload', None)
                )
            )
            payload_errors = {
                name: error
                for name, error in (
                    ('request_payload', request_payload_error),
                    ('response_payload', response_payload_error),
                )
                if error is not None
            }
            if payload_errors:
                validation_by_snapshot_id[snapshot_id] = {
                    'status': 'invalid',
                    'reason': 'persisted_snapshot_payload_invalid',
                    'payload_errors': payload_errors,
                }
                continue

            action_values = {
                str(value or '').strip().lower()
                for value in (
                    getattr(snapshot, 'decision_action', None),
                    getattr(snapshot, 'next_day_open_action', None),
                    response_payload.get('preferred_action'),
                    response_payload.get('tactical_action'),
                )
            }
            actionable_snapshot = bool(
                action_values
                & {'buy', 'scalp_buy_candidate', 'watch_for_trigger'}
            )

            fence = request_payload.get('decision_input_generation_fence')
            if (
                not isinstance(fence, dict)
                or fence.get('enforcement') != 'required'
            ):
                validation_by_snapshot_id[snapshot_id] = (
                    {
                        'status': 'invalid',
                        'reason': (
                            'actionable_snapshot_missing_durable_generation_fence'
                        ),
                    }
                    if actionable_snapshot
                    else {
                        'status': 'legacy_unenforced',
                        'reason': 'durable_generation_fence_not_present',
                    }
                )
                continue

            revalidation_request = request_payload.get(
                'source_revalidation_request'
            )
            durable_generation = request_payload.get(
                'durable_decision_input_generation'
            )
            decision_run_id = str(
                getattr(snapshot, 'decision_run_id', None) or ''
            ).strip()
            if (
                not isinstance(revalidation_request, dict)
                or not decision_run_id
                or 'eligibility_scope_symbols' not in revalidation_request
                or not isinstance(
                    revalidation_request.get('source_context_scope_symbols'),
                    list,
                )
                or not revalidation_request.get('source_context_scope_symbols')
                or not isinstance(
                    durable_generation,
                    dict,
                )
            ):
                validation_by_snapshot_id[snapshot_id] = {
                    'status': 'invalid',
                    'reason': 'durable_generation_metadata_missing',
                }
                continue

            if (
                durable_generation.get('schema_version')
                != 'durable_decider_input_generation_v1'
                or durable_generation.get('durable_reuse_eligible') is not True
            ):
                validation_by_snapshot_id[snapshot_id] = {
                    'status': 'invalid',
                    'reason': 'snapshot_source_not_durable',
                    'non_durable_reasons': list(
                        durable_generation.get('non_durable_reasons') or []
                    ),
                }
                continue

            raw_eligibility_scope = revalidation_request.get(
                'eligibility_scope_symbols'
            )
            if raw_eligibility_scope is not None and not isinstance(
                raw_eligibility_scope,
                list,
            ):
                validation_by_snapshot_id[snapshot_id] = {
                    'status': 'invalid',
                    'reason': 'durable_generation_metadata_invalid',
                }
                continue
            try:
                disclosures_limit = max(
                    0,
                    int(revalidation_request.get('disclosures_limit', 5)),
                )
            except (TypeError, ValueError):
                validation_by_snapshot_id[snapshot_id] = {
                    'status': 'invalid',
                    'reason': 'durable_generation_metadata_invalid',
                }
                continue
            raw_knowledge_cutoff = revalidation_request.get(
                'knowledge_cutoff_at'
            )
            try:
                parsed_knowledge_cutoff = datetime.fromisoformat(
                    str(raw_knowledge_cutoff).replace('Z', '+00:00')
                )
            except (TypeError, ValueError):
                parsed_knowledge_cutoff = None
            if (
                parsed_knowledge_cutoff is None
                or parsed_knowledge_cutoff.tzinfo is None
                or parsed_knowledge_cutoff.utcoffset() is None
            ):
                validation_by_snapshot_id[snapshot_id] = {
                    'status': 'invalid',
                    'reason': 'durable_knowledge_cutoff_missing_or_invalid',
                }
                continue
            normalized_knowledge_cutoff = (
                parsed_knowledge_cutoff.astimezone(timezone.utc).isoformat()
            )
            raw_stored_policy_context_as_of_date = (
                revalidation_request.get('policy_context_as_of_date')
            )
            try:
                stored_policy_context_as_of_date = (
                    date.fromisoformat(
                        str(raw_stored_policy_context_as_of_date)
                    )
                    if raw_stored_policy_context_as_of_date is not None
                    else normalized_market_date
                )
            except (TypeError, ValueError):
                validation_by_snapshot_id[snapshot_id] = {
                    'status': 'invalid',
                    'reason': 'durable_policy_context_date_invalid',
                }
                continue
            effective_policy_context_as_of_date = (
                normalized_policy_context_as_of_date
                or stored_policy_context_as_of_date
            )
            if effective_policy_context_as_of_date < normalized_market_date:
                validation_by_snapshot_id[snapshot_id] = {
                    'status': 'invalid',
                    'reason': 'durable_policy_context_date_invalid',
                }
                continue
            if (
                full_source_revalidation_actionable_only
                and not actionable_snapshot
            ):
                validation_by_snapshot_id[snapshot_id] = {
                    'status': 'valid',
                    'reason': 'non_actionable_durable_metadata_valid',
                }
                continue
            group_material = {
                'decision_run_id': decision_run_id,
                'fs_div': revalidation_request.get('fs_div'),
                'disclosures_limit': disclosures_limit,
                'source_context_scope_symbols': revalidation_request[
                    'source_context_scope_symbols'
                ],
                'eligibility_scope_symbols': raw_eligibility_scope,
                'source_knowledge_cutoff_at': normalized_knowledge_cutoff,
                'policy_context_as_of_date': (
                    effective_policy_context_as_of_date.isoformat()
                ),
                'authoritative_summary_run_id': str(
                    revalidation_request.get('upstream_summary_run_id')
                    or ''
                ).strip()
                or None,
            }
            group_key = json.dumps(
                group_material,
                ensure_ascii=True,
                sort_keys=True,
                separators=(',', ':'),
            )
            group = grouped.setdefault(
                group_key,
                {
                    'material': group_material,
                    'snapshots': [],
                    'source_items': [],
                },
            )
            group['snapshots'].append(snapshot)
            group['source_items'].append(request_payload)

        for group in grouped.values():
            material = group['material']
            try:
                with session_timeout_budget(
                    statement_timeout_ms=(
                        settings.signal_decider_db_statement_timeout_ms
                    ),
                    lock_timeout_ms=settings.signal_decider_db_lock_timeout_ms,
                ):
                    failures = self._revalidate_decider_dispatch_inputs(
                        market_date=normalized_market_date,
                        policy_context_as_of_date=date.fromisoformat(
                            material['policy_context_as_of_date']
                        ),
                        request_run_id=material['decision_run_id'],
                        source_items=group['source_items'],
                        source_context_scope_symbols=material[
                            'source_context_scope_symbols'
                        ],
                        eligibility_scope_symbols=material[
                            'eligibility_scope_symbols'
                        ],
                        fs_div=material['fs_div'],
                        disclosures_limit=material['disclosures_limit'],
                        allow_current_run_feedback=True,
                        revalidate_market_context=False,
                        allow_persisted_current_run_upstream=True,
                        durable_source_generation=True,
                        revalidate_post_decision_material_context=True,
                        source_knowledge_cutoff_at=material[
                            'source_knowledge_cutoff_at'
                        ],
                        authoritative_summary_run_id=material[
                            'authoritative_summary_run_id'
                        ],
                    )
            except Exception as exc:  # noqa: BLE001
                failures = {
                    (
                        str(getattr(snapshot, 'agent_name', None) or '').strip(),
                        str(getattr(snapshot, 'symbol', None) or '')
                        .strip()
                        .upper(),
                    ): {
                        'reason': 'durable_generation_revalidation_failed',
                        'error_type': type(exc).__name__,
                    }
                    for snapshot in group['snapshots']
                }

            for snapshot in group['snapshots']:
                snapshot_id = int(getattr(snapshot, 'id'))
                scope_key = (
                    str(getattr(snapshot, 'agent_name', None) or '').strip(),
                    str(getattr(snapshot, 'symbol', None) or '').strip().upper(),
                )
                failure = failures.get(scope_key)
                validation_by_snapshot_id[snapshot_id] = (
                    {
                        'status': 'invalid',
                        'reason': 'durable_decider_inputs_changed',
                        'details': failure,
                    }
                    if isinstance(failure, dict)
                    else {
                        'status': 'valid',
                        'reason': 'durable_decider_inputs_unchanged',
                    }
                )

        valid_snapshot_ids = sorted(
            snapshot_id
            for snapshot_id, validation in validation_by_snapshot_id.items()
            if validation.get('status') in {'valid', 'legacy_unenforced'}
        )
        invalid_snapshot_ids = sorted(
            set(validation_by_snapshot_id).difference(valid_snapshot_ids)
        )
        snapshot_identity_by_id = {
            int(getattr(snapshot, 'id')): {
                'symbol': str(getattr(snapshot, 'symbol', '') or '')
                .strip()
                .upper(),
                'agent_name': str(
                    getattr(snapshot, 'agent_name', '') or ''
                ).strip(),
            }
            for snapshot in snapshots
        }
        generation_material = {
            'schema_version': 'persisted_decider_generation_validation_v1',
            'market_date': normalized_market_date.isoformat(),
            'valid_snapshot_ids': valid_snapshot_ids,
            'invalid_snapshot_ids': invalid_snapshot_ids,
            'validations': [
                {
                    'snapshot_id': snapshot_id,
                    **snapshot_identity_by_id.get(snapshot_id, {}),
                    **validation_by_snapshot_id[snapshot_id],
                }
                for snapshot_id in sorted(validation_by_snapshot_id)
            ],
        }
        encoded = json.dumps(
            jsonable_encoder(generation_material),
            ensure_ascii=True,
            sort_keys=True,
            separators=(',', ':'),
        )
        return {
            **generation_material,
            'generation_fingerprint': (
                'sha256:' + hashlib.sha256(encoded.encode('utf-8')).hexdigest()
            ),
        }

    def _prepare_role_dispatch_items(
        self,
        *,
        role: str,
        market_date: date,
        policy_context_as_of_date: date,
        expected_weekly_policy_binding: Mapping[str, Any] | None = None,
        request_run_id: str,
        symbol_context_items: Sequence[dict[str, Any]],
        source_context_scope_symbols: Sequence[str],
        eligibility_scope_symbols: Sequence[str] | None,
        fs_div: str | None,
        disclosures_limit: int,
        source_knowledge_cutoff_at: str | None,
        upstream_summary_run_id: str | None,
        reuse_decisions: dict[str, dict[str, Any]],
        previous_decision_context_decisions: dict[str, dict[str, Any]],
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
            active_signal_decider_policy = self._build_decider_policy_context(
                role=role,
                market_date=market_date,
                policy_context_as_of_date=policy_context_as_of_date,
                expected_weekly_policy_binding=(
                    expected_weekly_policy_binding
                ),
            )

        dispatch_items: list[dict[str, Any]] = []
        for item in symbol_context_items:
            symbol = str(item.get('symbol') or '')
            decision = reuse_decisions.get(symbol, {}).get(role)
            if decision is not None and not decision.should_dispatch:
                continue
            if role not in DECISION_SUPPORT_AGENT_NAMES:
                dispatch_item = {**item}
                if role == FUNDAMENTAL_SUMMARIZER_AGENT_NAME:
                    fingerprint_error_type: str | None = None
                    try:
                        input_fingerprint_payload = (
                            build_fundamental_input_fingerprint_payload_from_context(
                                market_date=market_date,
                                symbol=symbol,
                                context=dispatch_item,
                                fs_div=fs_div,
                                disclosures_limit=disclosures_limit,
                                knowledge_cutoff_at=(
                                    source_knowledge_cutoff_at
                                ),
                            )
                        )
                    except Exception as exc:  # noqa: BLE001
                        input_fingerprint_payload = None
                        fingerprint_error_type = type(exc).__name__
                    fingerprint_value = (
                        str(
                            input_fingerprint_payload.get('fingerprint')
                            or ''
                        ).strip()
                        if isinstance(input_fingerprint_payload, dict)
                        else ''
                    )
                    if not fingerprint_value:
                        failure_kind = (
                            FeedbackFailureKind
                            .FUNDAMENTAL_INPUT_FINGERPRINT_UNAVAILABLE
                            .value
                        )
                        failure_context = {
                            'origin': (
                                'fundamental_summary_dispatch_guard'
                            ),
                            'dependency_status': (
                                'blocked_missing_input_fingerprint'
                            ),
                            'failure_status': (
                                'dependency/'
                                'fundamental-input-fingerprint-unavailable'
                            ),
                            'failure_kind': failure_kind,
                            'fingerprint_error_type': (
                                fingerprint_error_type
                            ),
                        }
                        failure = _build_failed_dispatch_item(
                            role=role,
                            agent_name=role,
                            symbol=symbol,
                            error=failure_kind,
                            retryable=True,
                            failure_kind=failure_kind,
                            context=failure_context,
                        )
                        all_failures.append(failure)
                        buffer_failure(
                            role,
                            symbol,
                            failure_kind,
                            True,
                            failure_kind=failure_kind,
                            context=failure_context,
                            agent_name=role,
                        )
                        log_agent_feedback_event(
                            'fundamental_input_fingerprint_rejected',
                            status='retryable_failure_buffered',
                            market_date=market_date.isoformat(),
                            request_run_id=request_run_id,
                            role=role,
                            agent_name=role,
                            symbol=symbol,
                            failure_kind=failure_kind,
                            failure_context=failure_context,
                        )
                        continue
                    dispatch_item[
                        'fundamental_summary_input_fingerprint'
                    ] = input_fingerprint_payload
                dispatch_items.append(dispatch_item)
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
            previous_decision = previous_decision_context_decisions.get(
                symbol,
                {},
            ).get(role)
            previous_decision_context = build_previous_decision_context(
                role=role,
                latest_feedback=(
                    getattr(previous_decision, 'latest_feedback', None)
                    if previous_decision is not None
                    else None
                ),
            )
            payload = self._build_signal_decider_payload(
                item=item,
                market_date=market_date,
                request_run_id=request_run_id,
                market_context=market_context,
                upstream_summaries=upstream_summaries,
                upstream_traceability=upstream_traceability,
                signal_decider_policy=active_signal_decider_policy,
                agent_name=role,
                previous_decision_context=previous_decision_context,
                source_knowledge_cutoff_at=source_knowledge_cutoff_at,
            )
            payload['source_revalidation_request'] = {
                'schema_version': 'decider_source_revalidation_request_v1',
                'source_context_scope_symbols': list(
                    source_context_scope_symbols
                ),
                'eligibility_scope_symbols': (
                    list(eligibility_scope_symbols)
                    if eligibility_scope_symbols is not None
                    else None
                ),
                'fs_div': fs_div,
                'disclosures_limit': int(disclosures_limit),
                'knowledge_cutoff_at': source_knowledge_cutoff_at,
                'policy_context_as_of_date': (
                    policy_context_as_of_date.isoformat()
                ),
                'upstream_summary_run_id': upstream_summary_run_id,
                'intraday_wait_recheck_event_id': (
                    item.get('intraday_wait_recheck_context', {}).get(
                        'event_id'
                    )
                    if isinstance(
                        item.get('intraday_wait_recheck_context'),
                        dict,
                    )
                    else None
                ),
            }
            payload['decision_input_generation_fence'] = {
                'schema_version': 'decision_input_generation_fence_v1',
                'enforcement': 'required',
                'durable_revalidation_required': True,
                'market_context_authority': 'next_open_live_market_gate',
            }
            dispatch_items.append(payload)
        return dispatch_items

    def _attach_signal_decider_performance_criteria(
        self,
        *,
        policy_context: dict[str, Any],
        market_date: date,
        policy_context_as_of_date: date | None = None,
        expected_weekly_policy_binding: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        criteria = self._build_signal_decider_performance_criteria(
            policy_context=policy_context,
            market_date=market_date,
            policy_context_as_of_date=policy_context_as_of_date,
            expected_weekly_policy_binding=expected_weekly_policy_binding,
        )
        if not criteria:
            return policy_context
        return {
            **policy_context,
            'performance_criteria': criteria,
        }

    def _build_decider_policy_context(
        self,
        *,
        role: str,
        market_date: date,
        policy_context_as_of_date: date | None = None,
        expected_weekly_policy_binding: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        resolved_policy_context_as_of_date = (
            policy_context_as_of_date or market_date
        )
        if role == SIGNAL_DECIDER_AGENT_NAME:
            return self._attach_signal_decider_performance_criteria(
                policy_context=(
                    signal_decider_loop_service.get_active_policy_context()
                ),
                market_date=market_date,
                policy_context_as_of_date=(
                    resolved_policy_context_as_of_date
                ),
                expected_weekly_policy_binding=(
                    expected_weekly_policy_binding
                ),
            )
        if role == SHORT_TERM_DECIDER_AGENT_NAME:
            criteria = self._load_published_weekly_performance_criteria(
                market_date=resolved_policy_context_as_of_date,
                expected_weekly_policy_binding=(
                    expected_weekly_policy_binding
                ),
            )
            return (
                {'performance_criteria': criteria}
                if isinstance(criteria, dict)
                else {}
            )
        return {}

    @staticmethod
    def _load_published_weekly_performance_criteria(
        *,
        market_date: date,
        expected_weekly_policy_binding: Mapping[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        try:
            criteria = (
                signal_decider_weekly_learning_service
                .get_latest_performance_criteria(as_of_date=market_date)
            )
        except Exception as exc:
            if expected_weekly_policy_binding is not None:
                raise RuntimeError(
                    'bound weekly performance criteria unavailable'
                ) from exc
            logger.warning(
                'signal_decider_weekly_learning_unavailable: %s',
                exc,
            )
            return None
        if expected_weekly_policy_binding is None:
            return criteria
        if not isinstance(criteria, Mapping):
            raise RuntimeError(
                'bound weekly performance criteria unavailable'
            )
        artifact = criteria.get('artifact')
        if not isinstance(artifact, Mapping):
            raise RuntimeError(
                'bound weekly performance criteria artifact missing'
            )
        expected_artifact_version = str(
            expected_weekly_policy_binding.get('artifact_version') or ''
        ).strip()
        expected_content_sha256 = str(
            expected_weekly_policy_binding.get('content_sha256') or ''
        ).strip()
        if not expected_artifact_version or not expected_content_sha256:
            raise ValueError(
                'expected weekly policy binding identity is incomplete'
            )
        if (
            str(artifact.get('artifact_version') or '').strip()
            != expected_artifact_version
            or str(artifact.get('content_sha256') or '').strip()
            != expected_content_sha256
        ):
            raise RuntimeError(
                'bound weekly performance criteria identity changed'
            )
        return dict(criteria)

    def _build_signal_decider_performance_criteria(
        self,
        *,
        policy_context: dict[str, Any],
        market_date: date,
        policy_context_as_of_date: date | None = None,
        expected_weekly_policy_binding: Mapping[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        published = self._load_published_weekly_performance_criteria(
            market_date=policy_context_as_of_date or market_date,
            expected_weekly_policy_binding=expected_weekly_policy_binding,
        )
        if published is not None:
            return published
        policy_version = str(policy_context.get('current_version') or '').strip() or None
        market_date_to = market_date - timedelta(days=1)
        market_date_from = market_date_to - timedelta(days=DEFAULT_SIGNAL_DECIDER_EVALUATION_LOOKBACK_DAYS)
        try:
            report = signal_decider_calibration_service.build_report(
                market_date_from=market_date_from,
                market_date_to=market_date_to,
                policy_version=policy_version,
                horizon_days=DEFAULT_SIGNAL_DECIDER_REVIEW_PRIMARY_HORIZON,
                evaluation_version=DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION,
                min_sample_size=3,
                cluster_limit=6,
            )
        except Exception as exc:  # Calibration context should degrade dispatch, not block the decider.
            logger.warning('signal_decider_performance_criteria_unavailable: %s', exc)
            return {
                'schema_version': 'signal_decider_performance_criteria_v1',
                'status': 'unavailable',
                'reason': 'calibration_report_unavailable',
                'auto_apply_policy_changes': False,
                'manual_review_only': True,
            }

        summary = report.get('summary') if isinstance(report.get('summary'), dict) else {}
        if summary.get('status') not in {'ready', 'insufficient_sample'}:
            return {
                'schema_version': 'signal_decider_performance_criteria_v1',
                'status': summary.get('status') or 'empty',
                'request': report.get('request') or {},
                'sample_count': int(summary.get('mature_computed_count') or 0),
                'auto_apply_policy_changes': False,
                'manual_review_only': True,
            }

        sample_count = int(summary.get('mature_computed_count') or 0)
        operator_review_ready = summary.get('ready_for_operator_review')
        replay_eligible = (
            summary.get('status') == 'ready'
            and sample_count >= 3
            and operator_review_ready is True
        )
        return {
            'schema_version': 'signal_decider_performance_criteria_v1',
            'status': summary.get('status'),
            'request': report.get('request') or {},
            'sample_count': sample_count,
            'overall': {
                'decision_action_counts': summary.get('decision_action_counts') or {},
                'grade_label_counts': summary.get('grade_label_counts') or {},
                'positive_outcome_rate': summary.get('positive_outcome_rate'),
                'negative_outcome_rate': summary.get('negative_outcome_rate'),
                'coverage_gap_count': summary.get('coverage_gap_count'),
            },
            'performance_buckets': self._compact_signal_decider_performance_buckets(
                report.get('group_summaries') if isinstance(report.get('group_summaries'), dict) else {}
            ),
            'rule_candidates': self._compact_signal_decider_rule_candidates(
                report.get('rule_candidates') if isinstance(report.get('rule_candidates'), dict) else {}
            ),
            'guidance': self._compact_signal_decider_guidance(
                report.get('calibration_hints') if isinstance(report.get('calibration_hints'), dict) else {}
            ),
            'usage_policy': {
                'model_guidance_eligible': replay_eligible,
                'deterministic_gate_eligible': replay_eligible,
                'use_for_buy_wait_reject_thresholds': False,
                'tighten_buy_when_matching_bucket_has_bad_buy_or_negative_return': True,
                'relax_wait_or_reject_when_matching_bucket_has_missed_opportunity': False,
                'allow_buy_authorization': False,
                'reward_risk_and_outcome_path_override_name_only_reasoning': True,
                'auto_apply_policy_changes': False,
                'manual_review_only': True,
            },
        }

    @staticmethod
    def _compact_signal_decider_performance_buckets(group_summaries: dict[str, Any]) -> list[dict[str, Any]]:
        return compact_performance_buckets(group_summaries)

    @staticmethod
    def _compact_signal_decider_rule_candidates(rule_candidates: dict[str, Any]) -> list[dict[str, Any]]:
        return compact_rule_candidates(rule_candidates)

    @staticmethod
    def _compact_signal_decider_guidance(guidance: dict[str, Any]) -> dict[str, Any]:
        return compact_guidance(guidance)

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

    def _preflight_openclaw_dispatch_auth(
        self,
        *,
        market_date: date,
        request_run_id: str,
        requested_roles: Sequence[str],
        scoped_dispatch_symbols: Sequence[str],
    ) -> dict[str, Any]:
        client = self._get_client()
        preflight = getattr(client, 'preflight_dispatch_auth', None)
        if not callable(preflight):
            return {
                'ok': True,
                'status': 'skipped',
                'reason': 'client_without_preflight_support',
            }
        try:
            result = preflight()
        except Exception as exc:  # noqa: BLE001
            classification = classify_feedback_failure(
                failure_kind=None,
                summary={'origin': 'openclaw_auth_preflight'},
                error=str(exc),
                exception_type=type(exc).__name__,
            )
            result = {
                'ok': False,
                'status': 'failed',
                'reason': 'openclaw_auth_preflight_exception',
                'failure_kind': classification.failure_kind,
                'failure_status': classification.failure_status,
                'failure_category': classification.failure_category,
                'retryable': classification.failure_category == 'transport',
                'shared_transport_outage': _is_shared_openclaw_transport_outage(
                    exc=exc,
                ),
                'error_type': type(exc).__name__,
                'error': str(exc),
            }
        if not isinstance(result, dict):
            result = {
                'ok': False,
                'status': 'failed',
                'reason': 'client_preflight_returned_non_mapping',
                'failure_kind': FeedbackFailureKind.DISPATCH_FAILED.value,
                'failure_status': 'dispatch/error',
                'failure_category': 'dispatch',
                'retryable': False,
            }
        log_agent_feedback_event(
            'openclaw_dispatch_auth_preflight',
            status=str(result.get('status') or 'unknown'),
            market_date=market_date.isoformat(),
            request_run_id=request_run_id,
            agent_name=OPENCLAW_GATEWAY_AGENT_NAME,
            role=OPENCLAW_GATEWAY_AGENT_NAME,
            batch_symbols=list(scoped_dispatch_symbols),
            requested_roles=list(requested_roles),
            failure_kind=result.get('failure_kind'),
            failure_context={
                key: value
                for key, value in result.items()
                if key not in {'response_text_excerpt'}
            },
        )
        return result

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

        cleanup_targets: list[SessionCleanupTarget] = []
        cleaned_session_keys = set(state.cleaned_session_keys)
        for row in state.buffered_feedback_rows:
            agent_name = str(row.get('agent_name') or '')
            if role is not None and agent_name != role:
                continue
            response = row.get('summary') if isinstance(row.get('summary'), dict) else None
            session_key = self._session_key_from_response(response)
            if session_key is None or session_key in cleaned_session_keys:
                continue
            cleanup_targets.append(
                SessionCleanupTarget(
                    market_date=market_date,
                    symbol=str(row.get('symbol') or ''),
                    agent_name=agent_name,
                    run_id=row.get('run_id'),
                    session_key=session_key,
                )
            )
            cleaned_session_keys.add(session_key)

        for failure in state.buffered_failures:
            agent_name = str(failure.get('agent_name') or failure.get('role') or '')
            if role is not None and agent_name != role:
                continue
            session_key = self._session_key_from_failure(failure)
            if session_key is None or session_key in cleaned_session_keys:
                continue
            cleanup_targets.append(
                SessionCleanupTarget(
                    market_date=market_date,
                    symbol=str(failure.get('symbol') or ''),
                    agent_name=agent_name,
                    run_id=request_run_id,
                    session_key=session_key,
                )
            )
            cleaned_session_keys.add(session_key)

        if not cleanup_targets:
            return []

        started_at = time.monotonic()
        cleanup_budget_seconds = max(0.0, float(max_seconds))
        cleanup_deadline = started_at + cleanup_budget_seconds
        # Durable prequeueing uses a fresh, unpooled connection, so its budget
        # is independent of the shared QueuePool and has no pre-ping phase.
        # Reserve the configured connect bound plus a small SQL window. Actual
        # statement/lock limits are recomputed after connect; the caller's
        # cleanup/delete budget remains unchanged.
        persistence_budget_seconds = max(
            cleanup_budget_seconds,
            float(getattr(settings, 'db_connect_timeout_seconds', 0.0)) + 1.0,
        )
        persistence_deadline = started_at + persistence_budget_seconds
        cleanup_results: list[dict[str, Any]] = []
        deferred_session_keys: set[str] = set()
        try:
            retry_queue = agent_feedback_session_cleanup_service.enqueue_deferred(
                targets=cleanup_targets,
                reason='cleanup_time_budget_exhausted',
                deadline_monotonic=persistence_deadline,
            )
        except Exception as exc:  # noqa: BLE001
            retry_queue = {
                'status': 'failed',
                'queued': None,
                'inserted': None,
                'already_queued': None,
                'requested_count': len(cleanup_targets),
                'pending_count': len(cleanup_targets),
                'persistence_confirmed': False,
                'error_type': type(exc).__name__,
                'error': str(exc),
            }
            deferred_session_keys.update(target.session_key for target in cleanup_targets)
            log_agent_feedback_event(
                'cleanup_persisted_run_sessions_prequeue_failed',
                status='failed',
                market_date=market_date.isoformat(),
                request_run_id=request_run_id,
                role=role,
                error_type=type(exc).__name__,
                error_message=str(exc),
                requested_count=len(cleanup_targets),
            )
        if retry_queue.get('persistence_confirmed') is not True:
            deferred_session_keys.update(target.session_key for target in cleanup_targets)
        else:
            for index, target in enumerate(cleanup_targets):
                if cleanup_deadline - time.monotonic() <= 0:
                    deferred_session_keys.update(
                        item.session_key for item in cleanup_targets[index:]
                    )
                    break
                completed = threading.Event()
                outcome: dict[str, Any] = {}

                def _delete_target(
                    cleanup_target: SessionCleanupTarget = target,
                ) -> None:
                    try:
                        outcome['result'] = (
                            agent_feedback_session_cleanup_service.delete_now(
                                target=cleanup_target,
                                deadline_monotonic=cleanup_deadline,
                            )
                        )
                    except Exception as exc:  # noqa: BLE001
                        outcome['error'] = exc
                    finally:
                        completed.set()

                threading.Thread(
                    target=_delete_target,
                    name='agent-feedback-session-cleanup',
                    daemon=True,
                ).start()
                remaining_seconds = max(
                    0.0,
                    cleanup_deadline - time.monotonic(),
                )
                if not completed.wait(timeout=remaining_seconds):
                    deferred_session_keys.update(
                        item.session_key for item in cleanup_targets[index:]
                    )
                    break
                error = outcome.get('error')
                if isinstance(error, Exception):
                    raise error
                result = outcome.get('result')
                if isinstance(result, dict):
                    cleanup_results.append(result)

        session_keys = {target.session_key for target in cleanup_targets}
        if deferred_session_keys:
            cleanup_results.append(
                {
                    'status': 'deferred',
                    'reason': 'cleanup_time_budget_exhausted',
                    'deferred_count': len(deferred_session_keys),
                    'max_seconds': max_seconds,
                    'persistence_max_seconds': persistence_budget_seconds,
                    'retry_queue': retry_queue,
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
                    'persistence_max_seconds': persistence_budget_seconds,
                    'retry_queue': retry_queue,
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
        cleanup_session: bool = True,
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
        if cleanup_session:
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
            'completed_symbols_by_role': {
                role: sorted(symbols)
                for role, symbols in state.completed_symbols_by_role.items()
            },
            'failed_symbols_by_role': {
                role: sorted(symbols)
                for role, symbols in state.failed_symbols_by_role.items()
            },
            'stale_symbols_by_role': {
                role: sorted(symbols)
                for role, symbols in state.stale_symbols_by_role.items()
            },
            'retry_pending_symbols_by_role': {
                role: sorted(symbols)
                for role, symbols in state.retry_pending_symbols_by_role.items()
            },
            'reused_feedback_evidence_by_role': {
                role: {
                    symbol: dict(evidence)
                    for symbol, evidence in sorted(symbol_evidence.items())
                }
                for role, symbol_evidence in sorted(
                    state.reused_feedback_evidence_by_role.items()
                )
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
        persistence_guard: Callable[..., Any] | None = None,
    ) -> dict[str, Any]:
        flush_result = self._flush_pending_run_buffers(
            market_date=market_date,
            request_run_id=request_run_id,
            reason=reason,
            persistence_guard=persistence_guard,
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
        persistence_guard: Callable[..., Any] | None = None,
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

        if persistence_guard is not None:
            persistence_guard(
                stage='before_feedback_persistence',
                market_date=market_date,
                request_run_id=request_run_id,
                reason=reason,
                feedback_rows=len(pending_feedback_rows),
                failure_rows=len(pending_failures),
            )

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
                if persistence_guard is None:
                    feedback_result = (
                        agent_context_feedback_service.bulk_submit_feedback(
                            items=flush_payload,
                        )
                    )
                else:
                    feedback_result = (
                        agent_context_feedback_service.bulk_submit_feedback(
                            items=flush_payload,
                            before_commit_guard=persistence_guard,
                        )
                    )
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
        knowledge_cutoff_at: datetime | str | None = None,
        policy_context_as_of_date: str | date | None = None,
        reuse_existing: bool = True,
        request_run_id: str | None = None,
        parent_run_id: str | None = None,
        force: bool = False,
        force_recompute: bool = False,
        auto_scope: bool = False,
        role_symbol_scopes: dict[str, Sequence[str]] | None = None,
        funnel_diagnostics: dict[str, Any] | None = None,
        upstream_summary_run_id: str | None = None,
        persistence_guard: Callable[..., Any] | None = None,
        expected_weekly_policy_binding: Mapping[str, Any] | None = None,
        source_context_overlays: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = market_date if isinstance(market_date, date) else date.fromisoformat(market_date)
        resolved_policy_context_as_of_date = (
            policy_context_as_of_date.date()
            if isinstance(policy_context_as_of_date, datetime)
            else policy_context_as_of_date
            if isinstance(policy_context_as_of_date, date)
            else date.fromisoformat(policy_context_as_of_date)
            if policy_context_as_of_date is not None
            else normalized_market_date
        )
        if resolved_policy_context_as_of_date < normalized_market_date:
            raise ValueError(
                'policy_context_as_of_date must be on or after market_date'
            )
        normalized_expected_weekly_policy_binding = None
        if expected_weekly_policy_binding is not None:
            if not isinstance(expected_weekly_policy_binding, Mapping):
                raise ValueError(
                    'expected_weekly_policy_binding must be an object'
                )
            artifact_version = str(
                expected_weekly_policy_binding.get('artifact_version') or ''
            ).strip()
            content_sha256 = str(
                expected_weekly_policy_binding.get('content_sha256') or ''
            ).strip()
            binding_policy_date = str(
                expected_weekly_policy_binding.get(
                    'policy_context_as_of_date'
                )
                or ''
            ).strip()
            if (
                not artifact_version
                or not content_sha256
                or binding_policy_date
                != resolved_policy_context_as_of_date.isoformat()
            ):
                raise ValueError(
                    'expected weekly policy binding does not match dispatch'
                )
            normalized_expected_weekly_policy_binding = {
                'artifact_version': artifact_version,
                'content_sha256': content_sha256,
                'policy_context_as_of_date': binding_policy_date,
            }
        effective_knowledge_cutoff_at, _knowledge_cutoff_source = (
            resolve_external_news_knowledge_cutoff_for_symbols(
                market_date=normalized_market_date,
                symbols=symbols,
                knowledge_cutoff_at=knowledge_cutoff_at,
            )
        )
        requested_knowledge_cutoff_at = effective_knowledge_cutoff_at.isoformat()
        normalized_parent_run_id = str(parent_run_id or '').strip() or None
        normalized_upstream_summary_run_id = str(
            upstream_summary_run_id or ''
        ).strip() or None
        specs = self._select_specs(roles=roles)
        requested_role_names = [spec.role for spec in specs]
        requested_symbols = _normalize_requested_symbols(symbols)
        normalized_source_context_overlays: dict[str, dict[str, Any]] = {}
        if source_context_overlays is not None:
            if (
                len(requested_role_names) != 1
                or requested_role_names[0]
                not in {
                    SIGNAL_DECIDER_AGENT_NAME,
                    SHORT_TERM_DECIDER_AGENT_NAME,
                }
            ):
                raise ValueError(
                    'source_context_overlays require one supported decider role'
                )
            overlay_agent_name = requested_role_names[0]
            from app.agent_context.wait_recheck_service import (
                load_validated_wait_recheck_context,
            )

            for raw_symbol, raw_context in source_context_overlays.items():
                symbol = str(raw_symbol or '').strip().upper()
                if symbol not in set(requested_symbols):
                    raise ValueError(
                        'source_context_overlays must match requested symbols'
                    )
                if not isinstance(raw_context, Mapping):
                    raise ValueError('source_context_overlays must contain objects')
                try:
                    event_id = int(raw_context.get('event_id'))
                except (TypeError, ValueError):
                    raise ValueError(
                        'source_context_overlays require a durable event_id'
                    ) from None
                validated_context = load_validated_wait_recheck_context(
                    event_id=event_id,
                    symbol=symbol,
                    agent_name=overlay_agent_name,
                )
                if validated_context is None or validated_context != dict(
                    raw_context
                ):
                    raise ValueError(
                        'source_context_overlays do not match durable WAIT recheck evidence'
                    )
                normalized_source_context_overlays[symbol] = validated_context
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
                    'parent_run_id': normalized_parent_run_id,
                    'roles': [spec.role for spec in specs],
                    'fs_div': fs_div,
                    'disclosures_limit': disclosures_limit,
                    'knowledge_cutoff_at': requested_knowledge_cutoff_at,
                    'policy_context_as_of_date': (
                        resolved_policy_context_as_of_date.isoformat()
                    ),
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
        uses_precomputed_auto_scope = bool(
            auto_scope
            and role_symbol_scopes is not None
            and funnel_diagnostics is not None
        )
        if uses_precomputed_auto_scope:
            dispatch_symbols = _ordered_union(role_symbol_scopes.values())
            skipped_symbols = []
            eligibility_diagnostics = {}
            public_eligibility_diagnostics = {}
            eligibility_scope_symbols_for_revalidation = None
        else:
            dispatch_symbols, skipped_symbols, eligibility_diagnostics = (
                _split_requested_symbols_by_agent_feedback_eligibility_with_diagnostics(
                    market_date=normalized_market_date,
                    requested_symbols=requested_symbols,
                )
            )
            eligibility_scope_symbols_for_revalidation = requested_symbols
            public_eligibility_diagnostics = _public_eligibility_diagnostics(eligibility_diagnostics)
            eligibility_collapse = _requested_scope_eligibility_collapse(
                requested_symbols=requested_symbols,
                dispatch_symbols=dispatch_symbols,
                diagnostics=eligibility_diagnostics,
            )
            if not force and eligibility_collapse is not None:
                return {
                    'request': {
                        'market_date': normalized_market_date.isoformat(),
                        'symbols': dispatch_symbols,
                        'requested_symbols': requested_symbols,
                        'request_run_id': None,
                        'parent_run_id': normalized_parent_run_id,
                        'roles': requested_role_names,
                        'fs_div': fs_div,
                        'disclosures_limit': disclosures_limit,
                        'knowledge_cutoff_at': requested_knowledge_cutoff_at,
                        'policy_context_as_of_date': (
                            resolved_policy_context_as_of_date.isoformat()
                        ),
                        'reuse_existing': reuse_existing,
                        'force_recompute': force_recompute,
                        'scope': resolve_agent_feedback_scope(requested_symbols=requested_symbols),
                        'auto_scope': auto_scope,
                        'eligibility_rule': AGENT_FEEDBACK_ELIGIBILITY_RULE,
                        'eligibility_filters': list(AGENT_FEEDBACK_ELIGIBILITY_FILTERS),
                        'skipped_symbols': skipped_symbols,
                        'eligibility_diagnostics': public_eligibility_diagnostics,
                        'funnel_diagnostics': funnel_diagnostics,
                        'mode': 'role_sequential_runtime_buffered',
                    },
                    'summary': {
                        'status': 'blocked',
                        'completed': 0,
                        'failed': 0,
                        'skipped': len(skipped_symbols),
                        'flushed_to_db': 0,
                        'gated': True,
                        'gate_reason': eligibility_collapse['reason'],
                    },
                    'items': [],
                    'failures': [],
                    'prerequisites': {
                        'status': 'blocked',
                        'ready': False,
                        'blocked_stage_ids': ['market_risk_filter'],
                        'missing_prerequisites': [
                            {
                                'stage': 'market_risk_filter',
                                'gate_status': 'blocked',
                                'reason': eligibility_collapse['reason'],
                                'details': eligibility_collapse,
                            }
                        ],
                        'eligibility_scope_gate': eligibility_collapse,
                    },
                }
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
            'parent_run_id': normalized_parent_run_id,
            'roles': requested_role_names,
            'fs_div': fs_div,
            'disclosures_limit': disclosures_limit,
            'knowledge_cutoff_at': requested_knowledge_cutoff_at,
            'policy_context_as_of_date': (
                resolved_policy_context_as_of_date.isoformat()
            ),
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
            'upstream_summary_run_id': (
                normalized_upstream_summary_run_id
            ),
            'expected_weekly_policy_binding': (
                normalized_expected_weekly_policy_binding
            ),
        }
        if normalized_source_context_overlays:
            request_payload['source_context_overlay_event_ids'] = {
                symbol: context['event_id']
                for symbol, context in normalized_source_context_overlays.items()
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

        def reuse_evidence_for_decision(
            decision: Any,
        ) -> dict[str, Any] | None:
            latest_feedback = getattr(decision, 'latest_feedback', None)
            feedback_run_id = (
                str(latest_feedback.get('run_id') or '').strip()
                if isinstance(latest_feedback, dict)
                else ''
            )
            feedback_received_at = str(
                getattr(decision, 'latest_feedback_received_at', None)
                or (
                    latest_feedback.get('received_at')
                    if isinstance(latest_feedback, dict)
                    else None
                )
                or ''
            ).strip()
            if not feedback_run_id or not feedback_received_at:
                return None
            feedback_id = getattr(decision, 'latest_feedback_id', None)
            return {
                'schema_version': 'agent_feedback_reuse_evidence_v1',
                'feedback_id': (
                    int(feedback_id) if feedback_id is not None else None
                ),
                'feedback_run_id': feedback_run_id,
                'feedback_received_at': feedback_received_at,
                'decision_snapshot_id': getattr(
                    decision,
                    'latest_decision_snapshot_id',
                    None,
                ),
                'decision_snapshot_fingerprint': getattr(
                    decision,
                    'latest_decision_snapshot_fingerprint',
                    None,
                ),
                'decision_snapshot_observed_at': getattr(
                    decision,
                    'latest_decision_snapshot_observed_at',
                    None,
                ),
            }

        def mark_reused(role: str, reuse_items: dict[str, Any]) -> None:
            reuse_evidence = {
                symbol: evidence
                for symbol, decision in reuse_items.items()
                if (evidence := reuse_evidence_for_decision(decision)) is not None
                and (
                    role not in DECISION_SUPPORT_AGENT_NAMES
                    or bool(evidence.get('decision_snapshot_id'))
                    or bool(evidence.get('decision_snapshot_fingerprint'))
                )
            }
            reused_symbols = list(reuse_evidence)
            if not reused_symbols:
                return

            def _update(current: RuntimeFeedbackRunState) -> None:
                current.current_role = role
                current.status = 'running'
                current.last_activity_at = datetime.now(timezone.utc)
                current.completed_symbols_by_role.setdefault(role, set()).update(reused_symbols)
                current.reused_symbols_by_role.setdefault(role, set()).update(reused_symbols)
                current.reused_feedback_evidence_by_role.setdefault(
                    role,
                    {},
                ).update(reuse_evidence)

            runtime_feedback_state_store.update_run(normalized_market_date, _update, run_id=request_run_id)
            for symbol in reused_symbols:
                decision = reuse_items[symbol]
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

        def buffer_success(
            role: str,
            symbol: str,
            context: dict[str, Any],
            response: dict[str, Any],
        ) -> bool:
            normalized_feedback = response.get('normalized_feedback')
            storage_context = context
            storage_response = response
            input_fingerprint_payload = context.get(
                'fundamental_summary_input_fingerprint'
            )
            fingerprint_value = (
                str(input_fingerprint_payload.get('fingerprint') or '').strip()
                if isinstance(input_fingerprint_payload, dict)
                else ''
            )
            if role == FUNDAMENTAL_SUMMARIZER_AGENT_NAME and not fingerprint_value:
                failure_kind = (
                    FeedbackFailureKind
                    .FUNDAMENTAL_INPUT_FINGERPRINT_UNAVAILABLE
                    .value
                )
                failure_context = {
                    'origin': 'fundamental_summary_persistence_guard',
                    'dependency_status': 'blocked_missing_input_fingerprint',
                    'failure_status': (
                        'dependency/fundamental-input-fingerprint-unavailable'
                    ),
                    'failure_kind': failure_kind,
                    'fingerprint_error_type': None,
                }
                batch_symbols = response.get('batch_symbols')
                failure = _build_failed_dispatch_item(
                    role=role,
                    agent_name=role,
                    symbol=symbol,
                    error=failure_kind,
                    retryable=True,
                    failure_kind=failure_kind,
                    context=failure_context,
                    dispatch_id=response.get('dispatch_id'),
                    attempt_id=response.get('attempt_id'),
                    slot=response.get('slot'),
                    session_key=response.get('session_key'),
                    session_id=response.get('session_id'),
                    response_id=response.get('response_id'),
                    request_marker=response.get('request_marker'),
                    batch_symbols=(
                        batch_symbols if isinstance(batch_symbols, list) else [symbol]
                    ),
                )
                all_failures.append(failure)
                buffer_failure(
                    role,
                    symbol,
                    failure_kind,
                    True,
                    failure_kind=failure_kind,
                    context=failure_context,
                    agent_name=role,
                    dispatch_id=response.get('dispatch_id'),
                    attempt_id=response.get('attempt_id'),
                    slot=response.get('slot'),
                    session_key=response.get('session_key'),
                    session_id=response.get('session_id'),
                    response_id=response.get('response_id'),
                    request_marker=response.get('request_marker'),
                    batch_symbols=(
                        batch_symbols if isinstance(batch_symbols, list) else [symbol]
                    ),
                )
                log_agent_feedback_event(
                    'fundamental_input_fingerprint_rejected',
                    status='retryable_failure_buffered',
                    market_date=normalized_market_date.isoformat(),
                    request_run_id=request_run_id,
                    role=role,
                    agent_name=role,
                    symbol=symbol,
                    failure_kind=failure_kind,
                    failure_context=failure_context,
                )
                return False
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
            return True

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
                # Failure scope is part of the durable run verdict, not merely
                # a side effect of writing the detail row.  Record it when the
                # tombstone is buffered so a failure-table outage cannot leave
                # a mixed/partial child summary looking fully successful to
                # next-open lineage validation.
                normalized_role = str(role or agent_name or '').strip()
                normalized_symbol = str(symbol or '').strip().upper()
                if normalized_role and normalized_symbol:
                    current.failed_symbols_by_role.setdefault(
                        normalized_role,
                        set(),
                    ).add(normalized_symbol)

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
        accepted_decider_source_items: dict[
            tuple[str, str],
            dict[str, Any],
        ] = {}
        symbol_context = {'items': []}
        response_payload: dict[str, Any] | None = None
        raised_exception: Exception | None = None
        finalization_error: Exception | None = None
        final_status = 'failed'
        final_flush_result: dict[str, Any] | None = None
        skip_terminal_reconciliation = False
        openclaw_dispatch_preflight_checked = False
        openclaw_shared_dispatch_outage: dict[str, Any] | None = None

        def run_openclaw_dispatch_preflight(live_dispatch_symbols: Sequence[str]) -> bool:
            nonlocal openclaw_dispatch_preflight_checked
            nonlocal response_payload
            nonlocal skip_terminal_reconciliation

            preflight_symbols = _ordered_union([list(live_dispatch_symbols)])
            if not preflight_symbols:
                return True
            if openclaw_dispatch_preflight_checked:
                return True

            openclaw_dispatch_preflight_checked = True
            openclaw_dispatch_preflight = self._preflight_openclaw_dispatch_auth(
                market_date=normalized_market_date,
                request_run_id=request_run_id,
                requested_roles=requested_role_names,
                scoped_dispatch_symbols=preflight_symbols,
            )
            sanitized_openclaw_dispatch_preflight = {
                key: value
                for key, value in openclaw_dispatch_preflight.items()
                if key not in {'response_text_excerpt'}
            }
            request_payload['openclaw_dispatch_preflight'] = sanitized_openclaw_dispatch_preflight
            if openclaw_dispatch_preflight.get('ok') is not False:
                return True

            skip_terminal_reconciliation = True
            failure_kind = str(
                openclaw_dispatch_preflight.get('failure_kind')
                or FeedbackFailureKind.DISPATCH_FAILED.value
            )
            failure_error = str(
                openclaw_dispatch_preflight.get('error')
                or openclaw_dispatch_preflight.get('reason')
                or 'OpenClaw dispatch auth preflight failed'
            )
            failure_context = {
                'origin': 'openclaw_dispatch_auth_preflight',
                'request_run_id': request_run_id,
                'affected_symbols': preflight_symbols,
                'affected_roles': list(requested_role_names),
                'preflight': sanitized_openclaw_dispatch_preflight,
            }
            failure = _build_failed_dispatch_item(
                role=OPENCLAW_GATEWAY_AGENT_NAME,
                agent_name=OPENCLAW_GATEWAY_AGENT_NAME,
                symbol=RUN_LEVEL_FAILURE_SYMBOL,
                error=failure_error,
                retryable=bool(openclaw_dispatch_preflight.get('retryable', False)),
                failure_kind=failure_kind,
                context=failure_context,
                batch_symbols=preflight_symbols,
            )
            all_failures.append(failure)
            buffer_failure(
                OPENCLAW_GATEWAY_AGENT_NAME,
                RUN_LEVEL_FAILURE_SYMBOL,
                failure_error,
                bool(openclaw_dispatch_preflight.get('retryable', False)),
                failure_kind=failure_kind,
                context=failure_context,
                agent_name=OPENCLAW_GATEWAY_AGENT_NAME,
                batch_symbols=preflight_symbols,
            )
            preflight_flush_result = self._flush_and_checkpoint_pending_run_buffers(
                market_date=normalized_market_date,
                request_run_id=request_run_id,
                request_payload=request_payload,
                reason='openclaw_auth_preflight_failed',
                lifecycle_stage='run_level_preflight_failed',
                current_role=OPENCLAW_GATEWAY_AGENT_NAME,
                status='failed',
                persistence_guard=persistence_guard,
            )
            response_payload = {
                'request': request_payload,
                'summary': {
                    'status': 'failed',
                    'completed': 0,
                    'computed_count': 0,
                    'reused_count': 0,
                    'stale_count': 0,
                    'failed': 1,
                    'skipped': len(skipped_symbols),
                    'flushed_to_db': 0,
                    'run_level_failure': True,
                    'failure_kind': failure_kind,
                    'openclaw_dispatch_preflight': request_payload['openclaw_dispatch_preflight'],
                    'failure_rows_persisted': preflight_flush_result.get('failure_rows_persisted'),
                },
                'items': [],
                'failures': all_failures,
            }
            return False

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
                source_knowledge_cutoff_at: str | None = (
                    requested_knowledge_cutoff_at
                )
                source_fs_div = fs_div
                source_disclosures_limit = disclosures_limit
                context_materialized = False
                fundamental_input_fingerprint_payloads: dict[
                    str,
                    dict[str, Any],
                ] = {}
                upstream_reuse_enabled = bool(
                    getattr(
                        settings,
                        'openclaw_feedback_upstream_reuse_enabled',
                        True,
                    )
                )
                needs_fundamental_context_before_reuse = bool(
                    not skip_terminal_reconciliation
                    and (
                        FUNDAMENTAL_SUMMARIZER_AGENT_NAME
                        in requested_role_names
                        or (
                            upstream_reuse_enabled
                            and any(
                                spec.role in DECISION_SUPPORT_AGENT_NAMES
                                for spec in specs
                            )
                        )
                    )
                )
                if needs_fundamental_context_before_reuse:
                    symbol_context = (
                        agent_context_read_service.get_symbol_day_context(
                            market_date=normalized_market_date,
                            symbols=scoped_dispatch_symbols,
                            fs_div=fs_div,
                            disclosures_limit=disclosures_limit,
                            knowledge_cutoff_at=(
                                requested_knowledge_cutoff_at
                            ),
                        )
                    )
                    for context_item in symbol_context.get('items') or []:
                        overlay = normalized_source_context_overlays.get(
                            str(context_item.get('symbol') or '').strip().upper()
                        )
                        if overlay is not None:
                            context_item['intraday_wait_recheck_context'] = overlay
                    context_materialized = True
                    _attach_agent_feedback_eligibility_context(
                        items=symbol_context.get('items') or [],
                        diagnostics=eligibility_diagnostics,
                    )
                    symbol_context_request = symbol_context.get('request')
                    if isinstance(symbol_context_request, dict):
                        source_fs_div = symbol_context_request.get(
                            'fs_div',
                            source_fs_div,
                        )
                        source_disclosures_limit = int(
                            symbol_context_request.get(
                                'disclosures_limit',
                                source_disclosures_limit,
                            )
                        )
                        raw_knowledge_cutoff = symbol_context_request.get(
                            'knowledge_cutoff_at'
                        )
                        if (
                            raw_knowledge_cutoff is not None
                            and str(raw_knowledge_cutoff).strip()
                        ):
                            source_knowledge_cutoff_at = str(
                                raw_knowledge_cutoff
                            ).strip()
                    for context_item in symbol_context.get('items') or []:
                        symbol = str(
                            context_item.get('symbol') or ''
                        ).strip().upper()
                        if not symbol:
                            continue
                        fundamental_input_fingerprint_payloads[symbol] = (
                            build_fundamental_input_fingerprint_payload_from_context(
                                market_date=normalized_market_date,
                                symbol=symbol,
                                context=context_item,
                                fs_div=source_fs_div,
                                disclosures_limit=(
                                    source_disclosures_limit
                                ),
                                knowledge_cutoff_at=(
                                    source_knowledge_cutoff_at
                                ),
                            )
                        )
                reuse_decisions = {}
                previous_decision_context_decisions = {}
                previous_decision_context_loaded = False
                decision_context_roles = [
                    role
                    for role in requested_role_names
                    if role in DECISION_SUPPORT_AGENT_NAMES
                ]
                if not skip_terminal_reconciliation and reuse_existing:
                    reuse_decisions = agent_context_reuse_service.build_latest_decisions(
                        market_date=normalized_market_date,
                        symbols=scoped_dispatch_symbols,
                        roles=requested_role_names,
                        force_recompute=force_recompute,
                        knowledge_cutoff_at=requested_knowledge_cutoff_at,
                        fs_div=source_fs_div,
                        disclosures_limit=source_disclosures_limit,
                        fundamental_input_fingerprint_payloads=(
                            fundamental_input_fingerprint_payloads
                            if needs_fundamental_context_before_reuse
                            else None
                        ),
                    )
                    if (
                        not force_recompute
                        and source_knowledge_cutoff_at
                        == requested_knowledge_cutoff_at
                    ):
                        previous_decision_context_decisions = {
                            symbol: {
                                role: decision
                                for role, decision in decisions_by_role.items()
                                if role in decision_context_roles
                            }
                            for symbol, decisions_by_role in reuse_decisions.items()
                        }
                        previous_decision_context_loaded = True
                    reuse_decisions = {
                        symbol: {
                            role: decision
                            for role, decision in decisions_by_role.items()
                            if bool(decision.should_dispatch)
                            or (
                                (
                                    evidence := reuse_evidence_for_decision(
                                        decision
                                    )
                                )
                                is not None
                                and (
                                    role not in DECISION_SUPPORT_AGENT_NAMES
                                    or bool(evidence.get('decision_snapshot_id'))
                                    or bool(
                                        evidence.get(
                                            'decision_snapshot_fingerprint'
                                        )
                                    )
                                )
                            )
                        }
                        for symbol, decisions_by_role in reuse_decisions.items()
                    }
                if (
                    not skip_terminal_reconciliation
                    and decision_context_roles
                    and not previous_decision_context_loaded
                ):
                    previous_decision_context_decisions = (
                        agent_context_reuse_service.build_latest_decisions(
                            market_date=normalized_market_date,
                            symbols=scoped_dispatch_symbols,
                            roles=decision_context_roles,
                            force_recompute=False,
                            knowledge_cutoff_at=source_knowledge_cutoff_at,
                            fs_div=source_fs_div,
                            disclosures_limit=source_disclosures_limit,
                            fundamental_input_fingerprint_payloads=(
                                fundamental_input_fingerprint_payloads
                                if needs_fundamental_context_before_reuse
                                else None
                            ),
                        )
                    )
                upstream_reuse_decisions = {}
                if (
                    not skip_terminal_reconciliation
                    and upstream_reuse_enabled
                    and any(spec.role in DECISION_SUPPORT_AGENT_NAMES for spec in specs)
                ):
                    upstream_reuse_decisions = agent_context_reuse_service.build_latest_decisions(
                        market_date=normalized_market_date,
                        symbols=scoped_dispatch_symbols,
                        roles=UPSTREAM_SUMMARY_ROLES,
                        force_recompute=False,
                        knowledge_cutoff_at=requested_knowledge_cutoff_at,
                        fs_div=source_fs_div,
                        disclosures_limit=source_disclosures_limit,
                        fundamental_input_fingerprint_payloads=(
                            fundamental_input_fingerprint_payloads
                        ),
                        authoritative_summary_run_id=(
                            normalized_upstream_summary_run_id
                        ),
                    )
                    upstream_reuse_decisions = {
                        symbol: {
                            role: decision
                            for role, decision in decisions_by_role.items()
                            if bool(decision.should_dispatch)
                            or reuse_evidence_for_decision(decision) is not None
                        }
                        for symbol, decisions_by_role in upstream_reuse_decisions.items()
                    }
                symbols_requiring_context = [] if skip_terminal_reconciliation else scoped_dispatch_symbols
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
                if symbols_requiring_context and not run_openclaw_dispatch_preflight(symbols_requiring_context):
                    symbols_requiring_context = []
                source_context_scope_symbols_for_revalidation = list(
                    symbols_requiring_context
                )
                if symbols_requiring_context and not context_materialized:
                    symbol_context = agent_context_read_service.get_symbol_day_context(
                        market_date=normalized_market_date,
                        symbols=symbols_requiring_context,
                        fs_div=fs_div,
                        disclosures_limit=disclosures_limit,
                        knowledge_cutoff_at=requested_knowledge_cutoff_at,
                    )
                    for context_item in symbol_context.get('items') or []:
                        overlay = normalized_source_context_overlays.get(
                            str(context_item.get('symbol') or '').strip().upper()
                        )
                        if overlay is not None:
                            context_item['intraday_wait_recheck_context'] = overlay
                    _attach_agent_feedback_eligibility_context(
                        items=symbol_context.get('items') or [],
                        diagnostics=eligibility_diagnostics,
                    )
                    symbol_context_request = symbol_context.get('request')
                    if isinstance(symbol_context_request, dict):
                        source_fs_div = symbol_context_request.get(
                            'fs_div',
                            source_fs_div,
                        )
                        source_disclosures_limit = int(
                            symbol_context_request.get(
                                'disclosures_limit',
                                source_disclosures_limit,
                            )
                        )
                        raw_knowledge_cutoff = symbol_context_request.get(
                            'knowledge_cutoff_at'
                        )
                        source_knowledge_cutoff_at = (
                            str(raw_knowledge_cutoff).strip()
                            if raw_knowledge_cutoff is not None
                            and str(raw_knowledge_cutoff).strip()
                            else None
                        )
                if not skip_terminal_reconciliation:
                    self._checkpoint_run_state(
                        market_date=normalized_market_date,
                        request_run_id=request_run_id,
                        request_payload=request_payload,
                        lifecycle_stage='dispatch_initialized',
                        current_role=state.current_role,
                        status='running',
                    )

                for spec in ([] if skip_terminal_reconciliation else specs):
                    if openclaw_shared_dispatch_outage is not None:
                        break
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
                        policy_context_as_of_date=(
                            resolved_policy_context_as_of_date
                        ),
                        expected_weekly_policy_binding=(
                            normalized_expected_weekly_policy_binding
                        ),
                        request_run_id=request_run_id,
                        symbol_context_items=[
                            item
                            for item in symbol_context['items']
                            if str(item.get('symbol') or '').strip().upper() in role_scope_set
                        ],
                        source_context_scope_symbols=(
                            source_context_scope_symbols_for_revalidation
                        ),
                        eligibility_scope_symbols=(
                            eligibility_scope_symbols_for_revalidation
                        ),
                        fs_div=source_fs_div,
                        disclosures_limit=source_disclosures_limit,
                        source_knowledge_cutoff_at=(
                            source_knowledge_cutoff_at
                        ),
                        upstream_summary_run_id=(
                            normalized_upstream_summary_run_id
                        ),
                        reuse_decisions=reuse_decisions,
                        previous_decision_context_decisions=(
                            previous_decision_context_decisions
                        ),
                        upstream_reuse_decisions=upstream_reuse_decisions,
                        all_failures=all_failures,
                        buffer_failure=buffer_failure,
                    )
                    input_revalidation_snapshot: dict[str, Any] | None = None
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
                        stable_session_key = f"agent:{spec.agent_name}:{_fresh_feedback_session_key(agent_name=spec.agent_name, request_run_id=request_run_id, batch_index=batch_index + 1, slot=slot)}"
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

                    executor = concurrent.futures.ThreadPoolExecutor(max_workers=role_parallelism)
                    try:
                        future_map: dict[
                            concurrent.futures.Future[Any],
                            tuple[int, list[dict[str, Any]]],
                        ] = {}
                        future_started_at: dict[
                            concurrent.futures.Future[Any],
                            float,
                        ] = {}
                        client_batch_timeout = float(
                            getattr(
                                self._get_client(),
                                'batch_http_timeout_seconds',
                                getattr(settings, 'openclaw_agent_batch_timeout_seconds', 0)
                                or DEFAULT_OPENCLAW_AGENT_BATCH_TIMEOUT_SECONDS,
                            )
                            or DEFAULT_OPENCLAW_AGENT_BATCH_TIMEOUT_SECONDS
                        )
                        configured_batch_timeout = float(
                            getattr(settings, 'openclaw_agent_batch_timeout_seconds', 0)
                            or DEFAULT_OPENCLAW_AGENT_BATCH_TIMEOUT_SECONDS
                        )
                        batch_timeout_seconds = max(
                            1.0,
                            min(
                                client_batch_timeout,
                                configured_batch_timeout,
                                DEFAULT_OPENCLAW_AGENT_BATCH_TIMEOUT_SECONDS,
                            ),
                        )
                        dispatch_admission_allowance_seconds = max(
                            0.0,
                            float(
                                getattr(
                                    self._get_client(),
                                    'minimum_dispatch_interval_seconds',
                                    0.0,
                                )
                                or 0.0
                            ),
                        ) * max(1, role_parallelism)
                        batch_future_timeout_seconds = (
                            batch_timeout_seconds
                            + dispatch_admission_allowance_seconds
                        )
                        poll_seconds = max(0.25, float(getattr(settings, 'openclaw_agent_subtask_poll_seconds', 2.0) or 2.0))

                        def dispatch_batch_with_completion(
                            *,
                            spec: AgentDispatchSpec,
                            market_date: date,
                            batch_items: list[dict[str, Any]],
                        ) -> tuple[dict[str, Any], float]:
                            result = self._get_client().dispatch_batch(
                                spec=spec,
                                market_date=market_date,
                                batch_items=batch_items,
                            )
                            return result, time.monotonic()

                        def submit_next() -> None:
                            if openclaw_shared_dispatch_outage is not None or not queue:
                                return
                            slot, batch = queue.pop(0)
                            mark_activity(role, slot, [item['symbol'] for item in batch], done=False)
                            submitted_at = time.monotonic()
                            future = executor.submit(
                                dispatch_batch_with_completion,
                                spec=spec,
                                market_date=normalized_market_date,
                                batch_items=batch,
                            )
                            future_map[future] = (slot, batch)
                            future_started_at[future] = submitted_at

                        for _ in range(min(role_parallelism, len(queue))):
                            submit_next()

                        while future_map:
                            done, not_done = concurrent.futures.wait(
                                list(future_map.keys()),
                                timeout=poll_seconds,
                                return_when=concurrent.futures.FIRST_COMPLETED,
                            )
                            now_monotonic = time.monotonic()
                            timed_out = {
                                future
                                for future, started_at in future_started_at.items()
                                if (
                                    future in future_map
                                    and future in not_done
                                    and now_monotonic - started_at
                                    >= batch_future_timeout_seconds
                                )
                            }
                            done = set(done) | timed_out
                            for future in done:
                                slot, batch = future_map.pop(future)
                                started_at = future_started_at.pop(future, now_monotonic)
                                completed_at: float | None = None
                                should_submit_next = False
                                staged_batch_outcomes = False
                                try:
                                    if future in timed_out:
                                        future.cancel()
                                        raise AgentDispatchRetryableError(
                                            'OpenClaw batch dispatch timed out '
                                            'after model and rate-admission budgets',
                                            details={
                                                'failure_kind': FeedbackFailureKind.OPENCLAW_TRANSPORT_TIMEOUT.value,
                                                'failure_mode': FeedbackFailureKind.OPENCLAW_TRANSPORT_TIMEOUT.value,
                                                'timeout_seconds': batch_future_timeout_seconds,
                                                'model_timeout_seconds': batch_timeout_seconds,
                                                'dispatch_admission_allowance_seconds': dispatch_admission_allowance_seconds,
                                                'elapsed_seconds': round(now_monotonic - started_at, 3),
                                            },
                                        )
                                    raw_future_result = future.result()
                                    if (
                                        isinstance(raw_future_result, tuple)
                                        and len(raw_future_result) == 2
                                        and isinstance(raw_future_result[0], dict)
                                        and isinstance(raw_future_result[1], (int, float))
                                        and not isinstance(raw_future_result[1], bool)
                                    ):
                                        batch_result = raw_future_result[0]
                                        completed_at = float(raw_future_result[1])
                                    else:
                                        batch_result = raw_future_result
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
                                    contract_failure = _batch_response_contract_failure(batch_result)
                                    contract_mismatch_flags: list[str] = []
                                    contract_snapshot: dict[str, Any] | None = None
                                    contract_model_identity: dict[str, Any] | None = None
                                    staged_feedback: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
                                    if contract_failure is None:
                                        for source_item in batch:
                                            symbol = source_item['symbol']
                                            symbol_key = str(symbol).strip().upper()
                                            feedback_candidate = by_symbol.get(symbol_key, {}).get('feedback')
                                            if not isinstance(feedback_candidate, dict):
                                                contract_failure = 'response_contract_invalid:feedback_missing'
                                                break
                                            normalized_feedback = _normalize_symbol_feedback(
                                                normalize_agent_response=normalize_agent_response,
                                                role=spec.role,
                                                agent_name=spec.agent_name,
                                                feedback_candidate=feedback_candidate,
                                                market_date=(
                                                    source_item.get('market_date')
                                                    or normalized_market_date.isoformat()
                                                ),
                                                symbol=symbol,
                                            )
                                            normalized_feedback = apply_decider_model_identity_fence(
                                                role=spec.role,
                                                normalized_feedback=normalized_feedback,
                                                model_identity=(
                                                    batch_result.get('model_identity')
                                                    if isinstance(batch_result.get('model_identity'), dict)
                                                    else None
                                                ),
                                                expected_model=expected_decider_model(
                                                    spec.role,
                                                    settings_obj=settings,
                                                ),
                                                runtime_contract=(
                                                    batch_result.get('request_provenance', {}).get(
                                                        'runtime_contract'
                                                    )
                                                    if isinstance(
                                                        batch_result.get('request_provenance'),
                                                        dict,
                                                    )
                                                    else None
                                                ),
                                            )
                                            normalized_contract_failure = _normalized_feedback_contract_failure(
                                                normalized_feedback
                                            )
                                            if normalized_contract_failure is not None:
                                                contract_failure = normalized_contract_failure
                                                contract_mismatch_flags = _extract_response_mismatch_flags(
                                                    normalized_feedback
                                                )
                                                contract_snapshot = build_normalized_feedback_snapshot(
                                                    normalized_feedback
                                                )
                                                contract_model_identity = (
                                                    normalized_feedback.get('model_identity')
                                                    if isinstance(
                                                        normalized_feedback.get('model_identity'),
                                                        dict,
                                                    )
                                                    else None
                                                )
                                                break
                                            staged_feedback.append(
                                                (source_item, feedback_candidate, normalized_feedback)
                                            )

                                    input_generation_failures: dict[
                                        tuple[str, str],
                                        dict[str, Any],
                                    ] = {}
                                    if (
                                        contract_failure is None
                                        and role in DECISION_SUPPORT_AGENT_NAMES
                                        and staged_feedback
                                    ):
                                        can_reuse_input_revalidation = (
                                            _can_reuse_decider_input_revalidation(
                                                snapshot=(
                                                    input_revalidation_snapshot
                                                ),
                                                dispatch_started_monotonic=(
                                                    started_at
                                                ),
                                                dispatch_completed_monotonic=(
                                                    completed_at
                                                ),
                                            )
                                        )
                                        if can_reuse_input_revalidation:
                                            input_generation_failures = dict(
                                                input_revalidation_snapshot[
                                                    'failures'
                                                ]
                                            )
                                        else:
                                            validation_started_monotonic = (
                                                time.monotonic()
                                            )
                                            input_generation_failures = (
                                                self._revalidate_decider_dispatch_inputs(
                                                    market_date=normalized_market_date,
                                                    policy_context_as_of_date=(
                                                        resolved_policy_context_as_of_date
                                                    ),
                                                    request_run_id=request_run_id,
                                                    source_items=dispatch_items,
                                                    source_context_scope_symbols=(
                                                        source_context_scope_symbols_for_revalidation
                                                    ),
                                                    eligibility_scope_symbols=(
                                                        eligibility_scope_symbols_for_revalidation
                                                    ),
                                                    fs_div=fs_div,
                                                    disclosures_limit=disclosures_limit,
                                                    source_knowledge_cutoff_at=(
                                                        source_knowledge_cutoff_at
                                                    ),
                                                    authoritative_summary_run_id=(
                                                        normalized_upstream_summary_run_id
                                                    ),
                                                )
                                            )
                                            input_revalidation_snapshot = {
                                                'validation_started_monotonic': (
                                                    validation_started_monotonic
                                                ),
                                                'failures': dict(
                                                    input_generation_failures
                                                ),
                                            }

                                    if contract_failure is not None:
                                        failure_class = (
                                            'response_identity_mismatch'
                                            if contract_failure.startswith('response_identity_mismatch:')
                                            else contract_failure
                                        )
                                        failure_kind = failure_class
                                        failure_mode = response_correlation.get('failure_mode') or failure_class
                                        diagnostics = (
                                            response_correlation.get('response_recovery')
                                            if isinstance(response_correlation.get('response_recovery'), dict)
                                            else None
                                        )
                                        for source_item in batch:
                                            symbol = source_item['symbol']
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
                                            failure_context = {
                                                **source_item,
                                                'contract_failure': contract_failure,
                                                'response_correlation': response_correlation or None,
                                                'response_audit': _response_failure_audit(batch_result),
                                                'model_identity': contract_model_identity,
                                            }
                                            if contract_mismatch_flags:
                                                failure_context['mismatch_flags'] = contract_mismatch_flags
                                            if contract_snapshot is not None:
                                                failure_context['normalized_feedback_snapshot'] = contract_snapshot
                                            failure = _build_failed_dispatch_item(
                                                role=role,
                                                agent_name=spec.agent_name,
                                                symbol=symbol,
                                                error=str(failure_class),
                                                retryable=True,
                                                failure_kind=str(failure_kind),
                                                context=failure_context,
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
                                                error_type=(
                                                    'ResponseIdentityMismatch'
                                                    if failure_class == 'response_identity_mismatch'
                                                    else 'ResponseContractInvalid'
                                                ),
                                                error_message=str(failure_class),
                                                failure_class=failure_class,
                                                failure_mode=failure_mode,
                                                contract_failure=contract_failure,
                                                mismatch_flags=contract_mismatch_flags,
                                                response_symbols=response_symbols,
                                                attribution_status=response_correlation.get('attribution_status'),
                                                response_correlation=response_correlation or None,
                                                normalized_feedback_snapshot=contract_snapshot,
                                            )
                                            buffer_failure(
                                                role,
                                                symbol,
                                                str(failure_class),
                                                True,
                                                failure_kind=str(failure_kind),
                                                context=failure_context,
                                                agent_name=spec.agent_name,
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
                                    else:
                                        for source_item, feedback_candidate, normalized_feedback in staged_feedback:
                                            symbol = source_item['symbol']
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
                                            input_generation_failure = (
                                                input_generation_failures.get(
                                                    (role, symbol)
                                                )
                                            )
                                            if input_generation_failure is not None:
                                                failure_kind = (
                                                    FeedbackFailureKind
                                                    .DEPENDENCY_UPSTREAM_CHANGED_DURING_DISPATCH
                                                    .value
                                                )
                                                failure_context = {
                                                    **source_item,
                                                    'dependency_status': (
                                                        'blocked_upstream_changed_during_dispatch'
                                                    ),
                                                    'failure_status': (
                                                        'dependency/upstream-changed-during-dispatch'
                                                    ),
                                                    'failure_kind': failure_kind,
                                                    'input_generation_revalidation': (
                                                        input_generation_failure
                                                    ),
                                                    'response_correlation': (
                                                        response_correlation or None
                                                    ),
                                                }
                                                failure = _build_failed_dispatch_item(
                                                    role=role,
                                                    agent_name=spec.agent_name,
                                                    symbol=symbol,
                                                    error=failure_kind,
                                                    retryable=True,
                                                    failure_kind=failure_kind,
                                                    context=failure_context,
                                                    dispatch_id=dispatch_id,
                                                    attempt_id=attempt_id,
                                                    slot=slot,
                                                    session_key=session_key,
                                                    session_id=dispatch_context.get(
                                                        'session_id'
                                                    ),
                                                    response_id=dispatch_context.get(
                                                        'response_id'
                                                    ),
                                                    request_marker=dispatch_context.get(
                                                        'request_marker'
                                                    ),
                                                    batch_symbols=batch_symbols,
                                                )
                                                all_failures.append(failure)
                                                log_agent_feedback_event(
                                                    'normalized_response_rejected',
                                                    status='rejected',
                                                    market_date=(
                                                        normalized_market_date.isoformat()
                                                    ),
                                                    request_run_id=request_run_id,
                                                    dispatch_id=dispatch_id,
                                                    attempt_id=attempt_id,
                                                    role=role,
                                                    agent_name=spec.agent_name,
                                                    symbol=symbol,
                                                    batch_symbols=batch_symbols,
                                                    slot=slot,
                                                    session_key=session_key,
                                                    error_type=(
                                                        'DecisionInputGenerationChanged'
                                                    ),
                                                    error_message=failure_kind,
                                                    failure_kind=failure_kind,
                                                    failure_context=(
                                                        input_generation_failure
                                                    ),
                                                )
                                                buffer_failure(
                                                    role,
                                                    symbol,
                                                    failure_kind,
                                                    True,
                                                    failure_kind=failure_kind,
                                                    context=failure_context,
                                                    agent_name=spec.agent_name,
                                                    dispatch_id=dispatch_id,
                                                    attempt_id=attempt_id,
                                                    slot=slot,
                                                    session_key=session_key,
                                                    session_id=dispatch_context.get(
                                                        'session_id'
                                                    ),
                                                    response_id=dispatch_context.get(
                                                        'response_id'
                                                    ),
                                                    request_marker=dispatch_context.get(
                                                        'request_marker'
                                                    ),
                                                    batch_symbols=batch_symbols,
                                                )
                                                continue
                                            mismatch_flags = _extract_response_mismatch_flags(normalized_feedback)
                                            log_agent_feedback_event(
                                                'normalized_response_accepted',
                                                status='accepted',
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
                                                normalized_feedback_snapshot=build_normalized_feedback_snapshot(
                                                    normalized_feedback
                                                ),
                                                mismatch_flags=mismatch_flags,
                                                attribution_status=response_correlation.get('attribution_status'),
                                                response_correlation=response_correlation or None,
                                                response_symbol=feedback_candidate.get('symbol'),
                                            )
                                            response_payload_item = _build_response_storage_payload(
                                                spec=spec,
                                                batch_result=batch_result,
                                                normalized_feedback=normalized_feedback,
                                                collector_mode='role_sequential_runtime_buffered',
                                            )
                                            success_buffered = buffer_success(
                                                spec.agent_name,
                                                symbol,
                                                source_item,
                                                response_payload_item,
                                            )
                                            if not success_buffered:
                                                continue
                                            all_items.append(
                                                _build_completed_dispatch_item(
                                                    role=role,
                                                    agent_name=spec.agent_name,
                                                    symbol=symbol,
                                                    normalized_feedback=normalized_feedback,
                                                )
                                            )
                                            if role in DECISION_SUPPORT_AGENT_NAMES:
                                                accepted_decider_source_items[
                                                    (role, symbol)
                                                ] = dict(source_item)
                                    staged_batch_outcomes = True
                                except Exception as exc:
                                    error_details = exc.details if isinstance(exc, AgentDispatchError) else {}
                                    retryable_dispatch_error = (
                                        _is_retryable_dispatch_exception(exc)
                                    )
                                    if bool(
                                        isinstance(error_details, dict)
                                        and (
                                            error_details.get(
                                                'shared_dispatch_outage'
                                            )
                                            or error_details.get(
                                                'shared_transport_outage'
                                            )
                                        )
                                    ):
                                        if openclaw_shared_dispatch_outage is None:
                                            openclaw_shared_dispatch_outage = {
                                                'schema_version': (
                                                    OPENCLAW_SHARED_DISPATCH_CIRCUIT_BREAKER_SCHEMA_VERSION
                                                ),
                                                'failure_kind': str(
                                                    error_details.get(
                                                        'failure_kind'
                                                    )
                                                    or FeedbackFailureKind
                                                    .OPENCLAW_TRANSPORT_ERROR
                                                    .value
                                                ),
                                                'failure_status': str(
                                                    error_details.get(
                                                        'failure_status'
                                                    )
                                                    or 'transport/error'
                                                ),
                                                'role': role,
                                                'slot': slot,
                                                'error_type': type(exc).__name__,
                                                'retryable': (
                                                    error_details.get('retryable')
                                                    is True
                                                ),
                                                'source': (
                                                    OPENCLAW_SHARED_DISPATCH_ORCHESTRATOR_SOURCE
                                                ),
                                            }
                                            retry_after_seconds = (
                                                error_details.get(
                                                    'retry_after_seconds'
                                                )
                                            )
                                            if (
                                                isinstance(
                                                    retry_after_seconds,
                                                    (int, float),
                                                )
                                                and not isinstance(
                                                    retry_after_seconds,
                                                    bool,
                                                )
                                                and math.isfinite(
                                                    retry_after_seconds
                                                )
                                                and retry_after_seconds > 0
                                            ):
                                                openclaw_shared_dispatch_outage[
                                                    'retry_after_seconds'
                                                ] = math.ceil(
                                                    retry_after_seconds
                                                )
                                            for optional_field in (
                                                'retry_not_before',
                                                'retry_after_source',
                                            ):
                                                optional_value = (
                                                    error_details.get(
                                                        optional_field
                                                    )
                                                )
                                                if (
                                                    isinstance(
                                                        optional_value,
                                                        str,
                                                    )
                                                    and optional_value.strip()
                                                ):
                                                    openclaw_shared_dispatch_outage[
                                                        optional_field
                                                    ] = optional_value.strip()
                                            request_payload[
                                                'openclaw_dispatch_circuit_breaker'
                                            ] = dict(
                                                openclaw_shared_dispatch_outage
                                            )
                                            log_agent_feedback_event(
                                                'openclaw_dispatch_circuit_opened',
                                                status='opened',
                                                market_date=(
                                                    normalized_market_date
                                                    .isoformat()
                                                ),
                                                request_run_id=request_run_id,
                                                role=role,
                                                agent_name=spec.agent_name,
                                                slot=slot,
                                                batch_symbols=[
                                                    str(
                                                        item.get('symbol')
                                                        or ''
                                                    )
                                                    for item in batch
                                                ],
                                                failure_kind=(
                                                    openclaw_shared_dispatch_outage[
                                                        'failure_kind'
                                                    ]
                                                ),
                                                failure_context={
                                                    'remaining_queued_batches': (
                                                        len(queue)
                                                    ),
                                                    'in_flight_batches': (
                                                        len(future_map)
                                                    ),
                                                },
                                            )
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
                                            retryable=retryable_dispatch_error,
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
                                            retryable_dispatch_error,
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
                                            persistence_guard=persistence_guard,
                                        )
                                    should_submit_next = (
                                        openclaw_shared_dispatch_outage is None
                                    )
                                finally:
                                    mark_activity(role, slot, done=True)
                                if should_submit_next:
                                    submit_next()
                    finally:
                        shutdown = getattr(executor, 'shutdown', None)
                        if shutdown is not None:
                            shutdown(wait=False, cancel_futures=True)

                    role_flush_result = self._flush_pending_run_buffers(
                        market_date=normalized_market_date,
                        request_run_id=request_run_id,
                        reason=f'role_completed:{role}',
                        persistence_guard=persistence_guard,
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
                    if openclaw_shared_dispatch_outage is not None:
                        break

                if not skip_terminal_reconciliation:
                    if accepted_decider_source_items:
                        final_generation_failures = (
                            self._revalidate_decider_dispatch_inputs(
                                market_date=normalized_market_date,
                                policy_context_as_of_date=(
                                    resolved_policy_context_as_of_date
                                ),
                                request_run_id=request_run_id,
                                source_items=list(
                                    accepted_decider_source_items.values()
                                ),
                                source_context_scope_symbols=(
                                    source_context_scope_symbols_for_revalidation
                                ),
                                eligibility_scope_symbols=(
                                    eligibility_scope_symbols_for_revalidation
                                ),
                                fs_div=fs_div,
                                disclosures_limit=disclosures_limit,
                                source_knowledge_cutoff_at=(
                                    source_knowledge_cutoff_at
                                ),
                                authoritative_summary_run_id=(
                                    normalized_upstream_summary_run_id
                                ),
                                revalidate_post_decision_material_context=True,
                                allow_current_run_feedback=True,
                            )
                        )
                        invalidated_keys = set(
                            accepted_decider_source_items
                        ).intersection(final_generation_failures)
                        for role, symbol in sorted(invalidated_keys):
                            source_item = accepted_decider_source_items[
                                (role, symbol)
                            ]
                            validation_failure = final_generation_failures[
                                (role, symbol)
                            ]
                            failure_kind = (
                                FeedbackFailureKind
                                .DEPENDENCY_UPSTREAM_CHANGED_DURING_DISPATCH
                                .value
                            )
                            failure_context = {
                                **source_item,
                                'dependency_status': (
                                    'blocked_upstream_changed_before_finalization'
                                ),
                                'failure_status': (
                                    'dependency/upstream-changed-during-dispatch'
                                ),
                                'failure_kind': failure_kind,
                                'input_generation_revalidation': validation_failure,
                            }
                            failure = _build_failed_dispatch_item(
                                role=role,
                                agent_name=role,
                                symbol=symbol,
                                error=failure_kind,
                                retryable=True,
                                failure_kind=failure_kind,
                                context=failure_context,
                                dispatch_id=source_item.get('dispatch_id'),
                                slot=source_item.get('session_slot'),
                                session_key=source_item.get('session_key'),
                                batch_symbols=[symbol],
                            )
                            all_failures.append(failure)
                            buffer_failure(
                                role,
                                symbol,
                                failure_kind,
                                True,
                                failure_kind=failure_kind,
                                context=failure_context,
                                agent_name=role,
                                dispatch_id=source_item.get('dispatch_id'),
                                slot=source_item.get('session_slot'),
                                session_key=source_item.get('session_key'),
                                batch_symbols=[symbol],
                            )
                            log_agent_feedback_event(
                                'decision_input_generation_finalization_rejected',
                                status='rejected',
                                market_date=normalized_market_date.isoformat(),
                                request_run_id=request_run_id,
                                dispatch_id=source_item.get('dispatch_id'),
                                role=role,
                                agent_name=role,
                                symbol=symbol,
                                batch_symbols=[symbol],
                                failure_kind=failure_kind,
                                failure_context=validation_failure,
                            )

                        if invalidated_keys:
                            def _invalidate_decider_successes(
                                current: RuntimeFeedbackRunState,
                            ) -> None:
                                for invalid_role, invalid_symbol in invalidated_keys:
                                    current.completed_symbols_by_role.setdefault(
                                        invalid_role,
                                        set(),
                                    ).discard(invalid_symbol)
                                    current.stale_symbols_by_role.setdefault(
                                        invalid_role,
                                        set(),
                                    ).add(invalid_symbol)
                                    current.retry_pending_symbols_by_role.setdefault(
                                        invalid_role,
                                        set(),
                                    ).add(invalid_symbol)

                            runtime_feedback_state_store.update_run(
                                normalized_market_date,
                                _invalidate_decider_successes,
                                run_id=request_run_id,
                            )
                            all_items[:] = [
                                item
                                for item in all_items
                                if (
                                    str(item.get('role') or ''),
                                    str(item.get('symbol') or ''),
                                )
                                not in invalidated_keys
                            ]
                            self._flush_and_checkpoint_pending_run_buffers(
                                market_date=normalized_market_date,
                                request_run_id=request_run_id,
                                request_payload=request_payload,
                                reason='finalize_decider_input_generation',
                                lifecycle_stage='final_input_generation_checked',
                                current_role=state.current_role,
                                status='running',
                                persistence_guard=persistence_guard,
                            )
                    final_snapshot = runtime_feedback_state_store.get_run(normalized_market_date, run_id=request_run_id) or state
                    progress_counts = final_snapshot.to_progress_summary()
                    completed_count = int(progress_counts.get('completed_feedback_count') or 0)
                    failed_count = int(progress_counts.get('failed_items') or 0)
                    final_status = _resolve_dispatch_status(completed=completed_count, failed=failed_count)
                    response_payload = {
                        'request': request_payload,
                        'summary': {
                            'status': final_status,
                            'completed': completed_count,
                            'computed_count': int(progress_counts.get('computed_feedback_count') or 0),
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
                    persistence_guard=persistence_guard,
                )
                final_snapshot = runtime_feedback_state_store.get_run(normalized_market_date, run_id=request_run_id) or final_snapshot
                if final_snapshot is not None and (final_snapshot.pending_feedback_rows or final_snapshot.pending_failures):
                    drain_flush_result = self._flush_pending_run_buffers(
                        market_date=normalized_market_date,
                        request_run_id=request_run_id,
                        reason='finalize_drain_pending',
                        persistence_guard=persistence_guard,
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
                    and not skip_terminal_reconciliation
                    and raised_exception is None
                    and finalization_error is None
                ):
                    incomplete_failures = buffer_incomplete_terminal_failures(final_snapshot)
                    if incomplete_failures:
                        incomplete_flush_result = self._flush_pending_run_buffers(
                            market_date=normalized_market_date,
                            request_run_id=request_run_id,
                            reason='finalize_missing_terminal_feedback',
                            persistence_guard=persistence_guard,
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
            progress_counts = final_snapshot.to_progress_summary() if final_snapshot is not None else {}
            completed_count = int(progress_counts.get('completed_feedback_count') or 0)
            failed_count = int(progress_counts.get('failed_items') or 0)
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
                response_payload['summary']['computed_count'] = int(progress_counts.get('computed_feedback_count') or 0)
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
