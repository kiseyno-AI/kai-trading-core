from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, Mapping


class FeedbackFailureKind(str, Enum):
    DEPENDENCY_MISSING_UPSTREAM = 'dependency_missing_upstream'
    OPENCLAW_AUTH_MISSING = 'openclaw_auth_missing'
    OPENCLAW_AUTH_REJECTED = 'openclaw_auth_rejected'
    OPENCLAW_TRANSPORT_TIMEOUT = 'openclaw_transport_timeout'
    OPENCLAW_TRANSPORT_ERROR = 'openclaw_transport_error'
    OPENCLAW_RESPONSE_INVALID_JSON = 'openclaw_response_invalid_json'
    OPENCLAW_ASSISTANT_NON_JSON = 'openclaw_assistant_non_json'
    OPENCLAW_RESPONSE_CONTRACT = 'openclaw_response_contract'
    DISPATCH_INCOMPLETE = 'dispatch_incomplete'
    DISPATCH_FAILED = 'dispatch_failed'


class FeedbackFailureStatus(str, Enum):
    DEPENDENCY_MISSING_UPSTREAM = 'dependency/missing-upstream'
    AUTH_MISSING = 'auth/missing'
    AUTH_REJECTED = 'auth/rejected'
    TRANSPORT_TIMEOUT = 'transport/timeout'
    TRANSPORT_ERROR = 'transport/error'
    RESPONSE_INVALID_JSON = 'response/invalid-json'
    RESPONSE_ASSISTANT_NON_JSON = 'response/assistant-non-json'
    RESPONSE_CONTRACT = 'response/contract'
    DISPATCH_INCOMPLETE = 'dispatch/incomplete'
    DISPATCH_ERROR = 'dispatch/error'


@dataclass(frozen=True)
class FeedbackFailureClassification:
    failure_kind: str
    failure_status: str
    failure_category: str
    reason: str

    def as_payload(self) -> dict[str, str]:
        return {
            'failure_kind': self.failure_kind,
            'failure_status': self.failure_status,
            'failure_category': self.failure_category,
            'reason': self.reason,
        }


def _string_values(*values: Any) -> Iterable[str]:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str):
            if value.strip():
                yield value.strip()
            continue
        if isinstance(value, Mapping):
            for key in (
                'failure_kind',
                'raw_failure_kind',
                'failure_status',
                'failure_category',
                'failure_class',
                'failure_mode',
                'error',
                'error_message',
                'exception_type',
                'dependency_status',
                'status',
            ):
                nested = value.get(key)
                if isinstance(nested, str) and nested.strip():
                    yield nested.strip()
            nested_context = value.get('dispatch_diagnostics') or value.get('failure_classification')
            if isinstance(nested_context, Mapping):
                yield from _string_values(nested_context)
            continue
        text = str(value).strip()
        if text:
            yield text


def _coerce_status_code(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        value = value.strip()
        if value.isdigit():
            return int(value)
    return None


def _status_code_values(*values: Any) -> Iterable[int]:
    for value in values:
        if not isinstance(value, Mapping):
            continue
        for key in ('response_status_code', 'status_code', 'http_status', 'http_status_code'):
            status_code = _coerce_status_code(value.get(key))
            if status_code is not None:
                yield status_code
        nested_context = value.get('dispatch_diagnostics') or value.get('failure_classification')
        if isinstance(nested_context, Mapping):
            yield from _status_code_values(nested_context)


def classify_feedback_failure(
    *,
    failure_kind: str | None,
    summary: Mapping[str, Any] | None = None,
    context: Mapping[str, Any] | None = None,
    error: str | None = None,
    exception_type: str | None = None,
) -> FeedbackFailureClassification:
    texts = list(_string_values(failure_kind, summary, context, error, exception_type))
    haystack = ' '.join(texts).lower()
    context_payload = context if isinstance(context, Mapping) else {}
    summary_payload = summary if isinstance(summary, Mapping) else {}
    status_codes = set(_status_code_values(summary_payload, context_payload))

    missing_roles = context_payload.get('missing_roles')
    if (
        'dependency_missing_upstream' in haystack
        or failure_kind == FeedbackFailureKind.DEPENDENCY_MISSING_UPSTREAM.value
        or failure_kind == FeedbackFailureStatus.DEPENDENCY_MISSING_UPSTREAM.value
        or 'missing_decider_upstream_summaries' in haystack
        or 'blocked_missing_upstream_summaries' in haystack
        or (isinstance(missing_roles, list) and missing_roles)
    ):
        return FeedbackFailureClassification(
            failure_kind=FeedbackFailureKind.DEPENDENCY_MISSING_UPSTREAM.value,
            failure_status=FeedbackFailureStatus.DEPENDENCY_MISSING_UPSTREAM.value,
            failure_category='dependency',
            reason='missing upstream summarizer feedback required by decider role',
        )

    if (
        'openclaw_auth_missing' in haystack
        or 'openclaw_gateway_token is not configured' in haystack
        or 'openclaw_gateway_token missing' in haystack
        or 'gateway token is not configured' in haystack
    ):
        return FeedbackFailureClassification(
            failure_kind=FeedbackFailureKind.OPENCLAW_AUTH_MISSING.value,
            failure_status=FeedbackFailureStatus.AUTH_MISSING.value,
            failure_category='auth',
            reason='OpenClaw gateway token is not configured locally',
        )

    if (
        'openclaw_auth_rejected' in haystack
        or 'unauthorized' in haystack
        or 'forbidden' in haystack
        or 401 in status_codes
        or 403 in status_codes
    ):
        return FeedbackFailureClassification(
            failure_kind=FeedbackFailureKind.OPENCLAW_AUTH_REJECTED.value,
            failure_status=FeedbackFailureStatus.AUTH_REJECTED.value,
            failure_category='auth',
            reason='OpenClaw gateway rejected the configured bearer token',
        )

    if 'dispatch_incomplete' in haystack or 'terminal feedback missing' in haystack:
        return FeedbackFailureClassification(
            failure_kind=FeedbackFailureKind.DISPATCH_INCOMPLETE.value,
            failure_status=FeedbackFailureStatus.DISPATCH_INCOMPLETE.value,
            failure_category='dispatch',
            reason='dispatch completed without terminal feedback or failure for the requested symbol',
        )

    if (
        'readtimeout' in haystack
        or 'connecttimeout' in haystack
        or 'writetimeout' in haystack
        or 'pooltimeout' in haystack
        or 'timeoutexception' in haystack
        or 'timed out' in haystack
        or 408 in status_codes
    ):
        return FeedbackFailureClassification(
            failure_kind=FeedbackFailureKind.OPENCLAW_TRANSPORT_TIMEOUT.value,
            failure_status=FeedbackFailureStatus.TRANSPORT_TIMEOUT.value,
            failure_category='transport',
            reason='OpenClaw HTTP request timed out before a usable response was received',
        )

    if (
        'jsondecodeerror' in haystack
        or 'response_invalid_json' in haystack
        or 'invalid_json_response' in haystack
        or 'openclaw_response_invalid_json' in haystack
        or summary_payload.get('response_json_decode_error') is True
    ):
        return FeedbackFailureClassification(
            failure_kind=FeedbackFailureKind.OPENCLAW_RESPONSE_INVALID_JSON.value,
            failure_status=FeedbackFailureStatus.RESPONSE_INVALID_JSON.value,
            failure_category='response',
            reason='OpenClaw gateway returned a non-JSON or invalid JSON HTTP response',
        )

    if (
        'assistant_non_json' in haystack
        or 'session_history_assistant_non_json_after_marker' in haystack
        or 'trajectory_assistant_non_json' in haystack
        or 'json_object_not_found' in haystack
        or 'json_decode_error' in haystack
        or failure_kind == 'invalid_json'
    ):
        return FeedbackFailureClassification(
            failure_kind=FeedbackFailureKind.OPENCLAW_ASSISTANT_NON_JSON.value,
            failure_status=FeedbackFailureStatus.RESPONSE_ASSISTANT_NON_JSON.value,
            failure_category='response',
            reason='OpenClaw assistant output did not satisfy the JSON-only feedback contract',
        )

    if (
        'batch_response_missing_items' in haystack
        or 'batch_response_attribution_mismatch' in haystack
        or 'missing_batch_feedback' in haystack
    ):
        return FeedbackFailureClassification(
            failure_kind=FeedbackFailureKind.OPENCLAW_RESPONSE_CONTRACT.value,
            failure_status=FeedbackFailureStatus.RESPONSE_CONTRACT.value,
            failure_category='response',
            reason='OpenClaw response JSON did not map cleanly to the requested symbol batch',
        )

    if 'http' in haystack or 'network' in haystack or 'transport' in haystack:
        return FeedbackFailureClassification(
            failure_kind=FeedbackFailureKind.OPENCLAW_TRANSPORT_ERROR.value,
            failure_status=FeedbackFailureStatus.TRANSPORT_ERROR.value,
            failure_category='transport',
            reason='OpenClaw HTTP transport failed before feedback could be stored',
        )

    return FeedbackFailureClassification(
        failure_kind=FeedbackFailureKind.DISPATCH_FAILED.value,
        failure_status=FeedbackFailureStatus.DISPATCH_ERROR.value,
        failure_category='dispatch',
        reason='agent feedback dispatch failed without a more specific classifier',
    )


def build_failure_groups(failures: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for failure in failures:
        summary = failure.get('summary') if isinstance(failure.get('summary'), Mapping) else failure.get('summary_payload')
        context = failure.get('context') if isinstance(failure.get('context'), Mapping) else failure.get('context_payload')
        classification_payload = None
        if isinstance(summary, Mapping):
            classification_payload = summary.get('failure_classification')
        classification = (
            FeedbackFailureClassification(
                failure_kind=str(classification_payload.get('failure_kind')),
                failure_status=str(classification_payload.get('failure_status')),
                failure_category=str(classification_payload.get('failure_category')),
                reason=str(classification_payload.get('reason') or ''),
            )
            if isinstance(classification_payload, Mapping)
            and classification_payload.get('failure_kind')
            and classification_payload.get('failure_status')
            and classification_payload.get('failure_category')
            else classify_feedback_failure(
                failure_kind=str(failure.get('failure_kind') or ''),
                summary=summary if isinstance(summary, Mapping) else None,
                context=context if isinstance(context, Mapping) else None,
                error=str(failure.get('error') or '') or None,
                exception_type=str(failure.get('exception_type') or '') or None,
            )
        )
        status = classification.failure_status
        group = groups.setdefault(
            status,
            {
                'failure_status': status,
                'failure_category': classification.failure_category,
                'count': 0,
                'retryable_count': 0,
                'agents': {},
                'symbols_sample': [],
                'failure_kinds': [],
            },
        )
        group['count'] += 1
        if bool(failure.get('retryable', True)):
            group['retryable_count'] += 1
        agent_name = str(failure.get('agent_name') or failure.get('role') or '').strip()
        if agent_name:
            group['agents'][agent_name] = int(group['agents'].get(agent_name, 0)) + 1
        symbol = str(failure.get('symbol') or '').strip()
        if symbol and symbol not in group['symbols_sample'] and len(group['symbols_sample']) < 10:
            group['symbols_sample'].append(symbol)
        kind = str(failure.get('failure_kind') or classification.failure_kind).strip()
        if kind and kind not in group['failure_kinds']:
            group['failure_kinds'].append(kind)

    for group in groups.values():
        group['agents'] = dict(sorted(group['agents'].items()))
        group['failure_kinds'] = sorted(group['failure_kinds'])
    return dict(sorted(groups.items()))
