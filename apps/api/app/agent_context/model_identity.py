from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Sequence

from app.agent_context.roles import (
    SHORT_TERM_DECIDER_AGENT_NAME,
    SIGNAL_DECIDER_AGENT_NAME,
)
from app.agent_context.safe_file import SafeFileReadError, read_bounded_regular_file


DEFAULT_DECIDER_EXPECTED_MODEL = 'openai/gpt-5.6-sol'
DECIDER_MODEL_IDENTITY_SCHEMA_VERSION = 'decider_model_identity_v1'
DECIDER_ROLES = frozenset(
    {SIGNAL_DECIDER_AGENT_NAME, SHORT_TERM_DECIDER_AGENT_NAME}
)
RUNTIME_CONTRACT_VERSION = 'v1'


def _normalize_model_ref(value: Any) -> str | None:
    normalized = str(value or '').strip()
    if not normalized or '/' not in normalized:
        return None
    provider, model = normalized.split('/', 1)
    if not provider.strip() or not model.strip():
        return None
    return f'{provider.strip()}/{model.strip()}'


def _split_model_ref(value: str | None) -> tuple[str | None, str | None]:
    normalized = _normalize_model_ref(value)
    if normalized is None:
        return None, None
    provider, model = normalized.split('/', 1)
    return provider, model


def expected_decider_model(role: str, *, settings_obj: Any) -> str | None:
    if role == SIGNAL_DECIDER_AGENT_NAME:
        configured = getattr(
            settings_obj,
            'openclaw_signal_decider_expected_model',
            DEFAULT_DECIDER_EXPECTED_MODEL,
        )
    elif role == SHORT_TERM_DECIDER_AGENT_NAME:
        configured = getattr(
            settings_obj,
            'openclaw_short_term_decider_expected_model',
            DEFAULT_DECIDER_EXPECTED_MODEL,
        )
    else:
        return None
    return _normalize_model_ref(configured)


def build_requested_model_identity(
    *,
    role: str,
    expected_model: str | None,
) -> dict[str, Any] | None:
    if role not in DECIDER_ROLES:
        return None
    normalized_expected = _normalize_model_ref(expected_model)
    if normalized_expected is None:
        return _invalid_identity(
            expected_model=None,
            actual_model_ref=None,
            status='expected_model_invalid',
            reason='configured expected provider/model identity is invalid',
        )
    return {
        'schema_version': DECIDER_MODEL_IDENTITY_SCHEMA_VERSION,
        'expected_model': normalized_expected,
        'requested_model': normalized_expected,
        'actual_provider': None,
        'actual_model': None,
        'actual_model_ref': None,
        'actual_identity_source': None,
        'actual_observed_from_response': False,
        'verified': False,
        'status': 'actual_model_missing',
        'reason': 'exact model override was requested but actual runtime identity is not yet observed',
        'verification_basis': 'gateway_exact_model_override_requested',
        'cohort': None,
    }


def build_observed_model_identity(
    *,
    role: str,
    expected_model: str | None,
    actual_provider: str | None,
    actual_model: str | None,
    actual_source_fingerprint: dict[str, int] | None = None,
) -> dict[str, Any] | None:
    if role not in DECIDER_ROLES:
        return None
    actual_ref = _normalize_model_ref(f'{actual_provider or ""}/{actual_model or ""}')
    provider, model = _split_model_ref(actual_ref)
    return {
        'schema_version': DECIDER_MODEL_IDENTITY_SCHEMA_VERSION,
        'expected_model': _normalize_model_ref(expected_model),
        'requested_model': _normalize_model_ref(expected_model),
        'actual_provider': provider,
        'actual_model': model,
        'actual_model_ref': actual_ref,
        'actual_identity_source': 'openclaw_session_registry',
        'actual_source_fingerprint': actual_source_fingerprint,
        'actual_observed_from_response': False,
        'verified': actual_ref is not None,
        'status': 'observed' if actual_ref is not None else 'actual_model_missing',
        'reason': None if actual_ref is not None else 'post-dispatch session model identity is missing',
        'verification_basis': 'post_dispatch_session_observation',
        'cohort': None,
    }


def _invalid_identity(
    *,
    expected_model: str | None,
    actual_model_ref: str | None,
    status: str,
    reason: str,
    verification_basis: str | None = None,
    actual_identity_source: str | None = None,
    actual_observed_from_response: bool = False,
    actual_source_fingerprint: dict[str, int] | None = None,
) -> dict[str, Any]:
    provider, model = _split_model_ref(actual_model_ref)
    return {
        'schema_version': DECIDER_MODEL_IDENTITY_SCHEMA_VERSION,
        'expected_model': _normalize_model_ref(expected_model),
        'requested_model': _normalize_model_ref(expected_model),
        'actual_provider': provider,
        'actual_model': model,
        'actual_model_ref': _normalize_model_ref(actual_model_ref),
        'actual_identity_source': actual_identity_source,
        'actual_source_fingerprint': actual_source_fingerprint,
        'actual_observed_from_response': actual_observed_from_response,
        'verified': False,
        'status': status,
        'reason': reason,
        'verification_basis': verification_basis,
        'cohort': None,
    }


def validated_decider_model_identity(
    *,
    role: str,
    model_identity: dict[str, Any] | None,
    expected_model: str | None,
) -> dict[str, Any] | None:
    if role not in DECIDER_ROLES:
        return None
    normalized_expected = _normalize_model_ref(expected_model)
    if normalized_expected is None:
        return _invalid_identity(
            expected_model=expected_model,
            actual_model_ref=None,
            status='expected_model_invalid',
            reason='configured expected provider/model identity is invalid',
        )
    identity = model_identity if isinstance(model_identity, dict) else {}
    actual_ref = _normalize_model_ref(identity.get('actual_model_ref'))
    if actual_ref is None:
        provider = str(identity.get('actual_provider') or '').strip()
        model = str(identity.get('actual_model') or '').strip()
        actual_ref = _normalize_model_ref(f'{provider}/{model}') if provider or model else None
    if actual_ref is None:
        return _invalid_identity(
            expected_model=normalized_expected,
            actual_model_ref=None,
            status='actual_model_missing',
            reason='trusted actual provider/model identity is missing',
            verification_basis=identity.get('verification_basis'),
            actual_identity_source=identity.get('actual_identity_source'),
            actual_observed_from_response=bool(identity.get('actual_observed_from_response')),
            actual_source_fingerprint=identity.get('actual_source_fingerprint'),
        )
    if actual_ref != normalized_expected:
        return _invalid_identity(
            expected_model=normalized_expected,
            actual_model_ref=actual_ref,
            status='actual_model_mismatch',
            reason=f'actual model {actual_ref} does not match expected model {normalized_expected}',
            verification_basis=identity.get('verification_basis'),
            actual_identity_source=identity.get('actual_identity_source'),
            actual_observed_from_response=bool(identity.get('actual_observed_from_response')),
            actual_source_fingerprint=identity.get('actual_source_fingerprint'),
        )
    if identity.get('verified') is not True:
        return _invalid_identity(
            expected_model=normalized_expected,
            actual_model_ref=actual_ref,
            status='actual_model_unverified',
            reason='actual provider/model identity was not verified by the dispatch boundary',
            verification_basis=identity.get('verification_basis'),
            actual_identity_source=identity.get('actual_identity_source'),
            actual_observed_from_response=bool(identity.get('actual_observed_from_response')),
            actual_source_fingerprint=identity.get('actual_source_fingerprint'),
        )
    if identity.get('verification_basis') != 'post_dispatch_session_observation':
        return _invalid_identity(
            expected_model=normalized_expected,
            actual_model_ref=actual_ref,
            status='actual_model_unverified',
            reason='model identity was not independently observed after dispatch',
            verification_basis=identity.get('verification_basis'),
            actual_identity_source=identity.get('actual_identity_source'),
            actual_observed_from_response=bool(identity.get('actual_observed_from_response')),
            actual_source_fingerprint=identity.get('actual_source_fingerprint'),
        )
    provider, model = _split_model_ref(actual_ref)
    return {
        'schema_version': DECIDER_MODEL_IDENTITY_SCHEMA_VERSION,
        'expected_model': normalized_expected,
        'requested_model': normalized_expected,
        'actual_provider': provider,
        'actual_model': model,
        'actual_model_ref': actual_ref,
        'actual_identity_source': identity.get('actual_identity_source'),
        'actual_source_fingerprint': identity.get('actual_source_fingerprint'),
        'actual_observed_from_response': bool(
            identity.get('actual_observed_from_response')
        ),
        'verified': True,
        'status': 'verified',
        'reason': None,
        'verification_basis': identity.get('verification_basis'),
        'cohort': f'{role}|{actual_ref}',
    }


def apply_decider_model_identity_fence(
    *,
    role: str,
    normalized_feedback: dict[str, Any],
    model_identity: dict[str, Any] | None,
    expected_model: str | None,
    runtime_contract: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if role not in DECIDER_ROLES:
        return normalized_feedback
    validated = validated_decider_model_identity(
        role=role,
        model_identity=model_identity,
        expected_model=expected_model,
    )
    contract = runtime_contract if isinstance(runtime_contract, dict) else {}
    fenced = {
        **normalized_feedback,
        'model_identity': validated,
        'runtime_contract': contract or None,
    }
    identity_verified = bool(validated and validated['verified'])
    contract_verified = contract.get('verified') is True
    if identity_verified and contract_verified:
        return fenced
    if identity_verified and not contract_verified:
        risk_flags = [str(item) for item in fenced.get('risk_flags') or [] if str(item)]
        if 'runtime_contract_unverified' not in risk_flags:
            risk_flags.append('runtime_contract_unverified')
        fenced.update(
            {
                'response_contract_valid': False,
                'risk_flags': risk_flags,
                'preferred_action': 'reject',
                'next_day_open_action': 'reject',
                'tactical_action': 'reject',
            }
        )
        return fenced
    status = str((validated or {}).get('status') or 'actual_model_unverified')
    flag = {
        'actual_model_missing': 'runtime_model_identity_missing',
        'actual_model_mismatch': 'runtime_model_identity_mismatch',
        'expected_model_invalid': 'runtime_expected_model_invalid',
    }.get(status, 'runtime_model_identity_unverified')
    risk_flags = [str(item) for item in fenced.get('risk_flags') or [] if str(item)]
    if flag not in risk_flags:
        risk_flags.append(flag)
    fenced.update(
        {
            'response_contract_valid': False,
            'risk_flags': risk_flags,
            'preferred_action': 'reject',
            'next_day_open_action': 'reject',
            'tactical_action': 'reject',
        }
    )
    return fenced


def build_runtime_contract_provenance(
    role: str,
    *,
    openclaw_state_dir: str | Path | None = None,
) -> dict[str, Any] | None:
    if role not in DECIDER_ROLES:
        return None
    repository_root = Path(__file__).resolve().parents[4]
    contract_path = (
        repository_root
        / 'runtime_contracts'
        / 'openclaw'
        / RUNTIME_CONTRACT_VERSION
        / role
        / 'AGENTS.md'
    )
    try:
        canonical_snapshot = read_bounded_regular_file(contract_path, max_bytes=1024 * 1024)
    except SafeFileReadError:
        return {
            'role': role,
            'version': RUNTIME_CONTRACT_VERSION,
            'expected_sha256': None,
            'live_sha256': None,
            'verified': False,
            'status': 'canonical_contract_missing',
            'reason': 'canonical runtime contract is missing',
        }
    content = canonical_snapshot.content
    expected_sha256 = f"sha256:{hashlib.sha256(content).hexdigest()}"
    provenance = {
        'role': role,
        'version': RUNTIME_CONTRACT_VERSION,
        'expected_sha256': expected_sha256,
        'expected_file_fingerprint': canonical_snapshot.fingerprint,
        'expected_path_fingerprint': canonical_snapshot.path_fingerprint,
        'live_sha256': None,
        'live_file_fingerprint': None,
        'live_path_fingerprint': None,
        'verified': False,
        'status': 'live_contract_unchecked',
        'reason': 'live OpenClaw workspace contract was not checked',
    }
    if openclaw_state_dir is None:
        return provenance
    live_path = Path(openclaw_state_dir).expanduser() / f'workspace-{role}' / 'AGENTS.md'
    try:
        live_snapshot = read_bounded_regular_file(live_path, max_bytes=1024 * 1024)
    except SafeFileReadError:
        return {**provenance, 'status': 'live_contract_unreadable', 'reason': 'live OpenClaw workspace contract cannot be read'}
    live_content = live_snapshot.content
    live_sha256 = f"sha256:{hashlib.sha256(live_content).hexdigest()}"
    if live_sha256 != expected_sha256:
        return {
            **provenance,
            'live_sha256': live_sha256,
            'live_file_fingerprint': live_snapshot.fingerprint,
            'live_path_fingerprint': live_snapshot.path_fingerprint,
            'status': 'live_contract_mismatch',
            'reason': 'live OpenClaw workspace contract differs from the canonical contract',
        }
    return {
        **provenance,
        'live_sha256': live_sha256,
        'live_file_fingerprint': live_snapshot.fingerprint,
        'live_path_fingerprint': live_snapshot.path_fingerprint,
        'verified': True,
        'status': 'verified',
        'reason': None,
    }


def build_dispatch_runtime_contract_provenance(
    *,
    pre_dispatch: dict[str, Any] | None,
    post_dispatch: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if pre_dispatch is None and post_dispatch is None:
        return None
    pre = pre_dispatch if isinstance(pre_dispatch, dict) else {}
    post = post_dispatch if isinstance(post_dispatch, dict) else {}
    identity_fields = (
        'role',
        'version',
        'expected_sha256',
        'expected_file_fingerprint',
        'expected_path_fingerprint',
        'live_sha256',
        'live_file_fingerprint',
        'live_path_fingerprint',
    )
    same_identity = all(pre.get(field) == post.get(field) for field in identity_fields)
    verified = pre.get('verified') is True and post.get('verified') is True and same_identity
    if pre.get('verified') is not True:
        status = 'pre_dispatch_contract_unverified'
        reason = 'live runtime contract was not verified immediately before dispatch'
    elif post.get('verified') is not True:
        status = 'post_dispatch_contract_unverified'
        reason = 'live runtime contract was not verified immediately after dispatch'
    elif not same_identity:
        status = 'live_contract_changed_during_dispatch'
        reason = 'live runtime contract identity changed during the dispatch attempt'
    else:
        status = 'verified'
        reason = None
    return {
        'role': pre.get('role') or post.get('role'),
        'version': pre.get('version') if pre.get('version') == post.get('version') else None,
        'expected_sha256': (
            pre.get('expected_sha256')
            if pre.get('expected_sha256') == post.get('expected_sha256')
            else None
        ),
        'expected_file_fingerprint': (
            pre.get('expected_file_fingerprint')
            if pre.get('expected_file_fingerprint') == post.get('expected_file_fingerprint')
            else None
        ),
        'expected_path_fingerprint': (
            pre.get('expected_path_fingerprint')
            if pre.get('expected_path_fingerprint') == post.get('expected_path_fingerprint')
            else None
        ),
        'live_sha256': (
            pre.get('live_sha256')
            if pre.get('live_sha256') == post.get('live_sha256')
            else None
        ),
        'live_file_fingerprint': (
            pre.get('live_file_fingerprint')
            if pre.get('live_file_fingerprint') == post.get('live_file_fingerprint')
            else None
        ),
        'live_path_fingerprint': (
            pre.get('live_path_fingerprint')
            if pre.get('live_path_fingerprint') == post.get('live_path_fingerprint')
            else None
        ),
        'verified': verified,
        'status': status,
        'reason': reason,
        'pre_dispatch': pre or None,
        'post_dispatch': post or None,
    }


def _stored_identity(row: Any) -> dict[str, Any] | None:
    summary = row.response_summary_payload if isinstance(row.response_summary_payload, dict) else {}
    provenance = summary.get('decision_provenance')
    normalized_feedback = summary.get('normalized_feedback')
    request_provenance = summary.get('request_provenance')
    candidates = (
        provenance.get('model_identity') if isinstance(provenance, dict) else None,
        normalized_feedback.get('model_identity') if isinstance(normalized_feedback, dict) else None,
        summary.get('model_identity'),
        request_provenance.get('model_identity') if isinstance(request_provenance, dict) else None,
    )
    return next((item for item in candidates if isinstance(item, dict)), None)


def select_model_identity_cohort_rows(
    rows: Sequence[Any],
    *,
    role: str,
    expected_model: str | None,
) -> tuple[list[Any], list[dict[str, Any]], str]:
    normalized_expected = _normalize_model_ref(expected_model)
    expected_cohort = f'{role}|{normalized_expected}' if normalized_expected else None
    stored = [(row, _stored_identity(row)) for row in rows]

    selected: list[Any] = []
    exclusions: list[dict[str, Any]] = []
    for row, identity in stored:
        validated = validated_decider_model_identity(
            role=role,
            model_identity=identity,
            expected_model=normalized_expected,
        )
        if validated and validated.get('cohort') == expected_cohort:
            selected.append(row)
            continue
        status = str((validated or {}).get('status') or '')
        exclusions.append(
            {
                'snapshot_id': row.id,
                'status_reason': (
                    'model_identity_cohort_mismatch'
                    if status == 'actual_model_mismatch'
                    else 'model_identity_unverified'
                ),
                'model_identity': validated,
            }
        )
    return selected, exclusions, expected_cohort or 'invalid_expected_model'
