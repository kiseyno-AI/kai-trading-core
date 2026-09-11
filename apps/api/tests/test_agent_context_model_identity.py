from types import SimpleNamespace
from datetime import datetime, timedelta, timezone
import json
from unittest.mock import Mock

import pytest

from app.agent_context.model_identity import (
    DEFAULT_DECIDER_EXPECTED_MODEL,
    apply_decider_model_identity_fence,
    build_requested_model_identity,
    build_observed_model_identity,
    build_dispatch_runtime_contract_provenance,
    build_runtime_contract_provenance,
    expected_decider_model,
    select_model_identity_cohort_rows,
)
from app.core.settings import Settings
from app.agent_context import orchestrator as orchestrator_module
from app.agent_context.orchestrator import OpenClawAgentClient


def _settings(**overrides):
    values = {
        'openclaw_signal_decider_expected_model': DEFAULT_DECIDER_EXPECTED_MODEL,
        'openclaw_short_term_decider_expected_model': DEFAULT_DECIDER_EXPECTED_MODEL,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _feedback(action: str = 'buy') -> dict:
    return {
        'role': 'signal-decider',
        'preferred_action': action,
        'next_day_open_action': action,
        'response_contract_valid': True,
        'risk_flags': [],
    }


def _verified_contract() -> dict:
    return {'version': 'v1', 'verified': True, 'status': 'verified'}


def _row(*, identity: dict | None):
    return SimpleNamespace(
        id=id(identity),
        agent_name='signal-decider',
        response_summary_payload={
            'decision_provenance': {'model_identity': identity}
            if identity is not None
            else {}
        },
    )


def test_exact_observed_model_identity_is_verified_and_actionable() -> None:
    expected = expected_decider_model('signal-decider', settings_obj=_settings())
    identity = build_observed_model_identity(
        role='signal-decider',
        expected_model=expected,
        actual_provider='openai',
        actual_model='gpt-5.6-sol',
    )

    fenced = apply_decider_model_identity_fence(
        role='signal-decider',
        normalized_feedback=_feedback(),
        model_identity=identity,
        expected_model=expected,
        runtime_contract=_verified_contract(),
    )

    assert fenced['model_identity'] == {
        'schema_version': 'decider_model_identity_v1',
        'expected_model': 'openai/gpt-5.6-sol',
        'requested_model': 'openai/gpt-5.6-sol',
        'actual_provider': 'openai',
        'actual_model': 'gpt-5.6-sol',
        'actual_model_ref': 'openai/gpt-5.6-sol',
        'actual_identity_source': 'openclaw_session_registry',
        'actual_source_fingerprint': None,
        'actual_observed_from_response': False,
        'verified': True,
        'status': 'verified',
        'reason': None,
        'verification_basis': 'post_dispatch_session_observation',
        'cohort': 'signal-decider|openai/gpt-5.6-sol',
    }
    assert fenced['next_day_open_action'] == 'buy'
    assert fenced['response_contract_valid'] is True


def test_missing_actual_model_identity_is_non_actionable_but_preserved() -> None:
    fenced = apply_decider_model_identity_fence(
        role='signal-decider',
        normalized_feedback=_feedback(),
        model_identity=None,
        expected_model=DEFAULT_DECIDER_EXPECTED_MODEL,
        runtime_contract=_verified_contract(),
    )

    assert fenced['preferred_action'] == 'reject'
    assert fenced['next_day_open_action'] == 'reject'
    assert fenced['response_contract_valid'] is False
    assert fenced['model_identity']['status'] == 'actual_model_missing'
    assert fenced['model_identity']['reason'] == 'trusted actual provider/model identity is missing'
    assert 'runtime_model_identity_missing' in fenced['risk_flags']


def test_mismatched_actual_model_identity_is_non_actionable() -> None:
    mismatched = {
        'schema_version': 'decider_model_identity_v1',
        'expected_model': DEFAULT_DECIDER_EXPECTED_MODEL,
        'actual_provider': 'openai-codex',
        'actual_model': 'gpt-5.5',
        'actual_model_ref': 'openai-codex/gpt-5.5',
        'verified': True,
        'status': 'verified',
        'reason': None,
        'verification_basis': 'test',
        'cohort': 'signal-decider|openai-codex/gpt-5.5',
    }

    fenced = apply_decider_model_identity_fence(
        role='signal-decider',
        normalized_feedback=_feedback(),
        model_identity=mismatched,
        expected_model=DEFAULT_DECIDER_EXPECTED_MODEL,
        runtime_contract=_verified_contract(),
    )

    assert fenced['next_day_open_action'] == 'reject'
    assert fenced['response_contract_valid'] is False
    assert fenced['model_identity']['verified'] is False
    assert fenced['model_identity']['status'] == 'actual_model_mismatch'
    assert 'runtime_model_identity_mismatch' in fenced['risk_flags']


def test_auto_discovered_exact_model_is_not_promoted_to_authority() -> None:
    discovered = {
        'expected_model': DEFAULT_DECIDER_EXPECTED_MODEL,
        'actual_model_ref': DEFAULT_DECIDER_EXPECTED_MODEL,
        'verified': True,
        'verification_basis': 'runtime_auto_discovery',
    }

    fenced = apply_decider_model_identity_fence(
        role='signal-decider',
        normalized_feedback=_feedback(),
        model_identity=discovered,
        expected_model=DEFAULT_DECIDER_EXPECTED_MODEL,
        runtime_contract=_verified_contract(),
    )

    assert fenced['next_day_open_action'] == 'reject'
    assert fenced['model_identity']['status'] == 'actual_model_unverified'
    assert 'runtime_model_identity_unverified' in fenced['risk_flags']


def test_non_decider_feedback_path_is_unchanged() -> None:
    original = {'role': 'eod-summarizer', 'response_contract_valid': True, 'risk_flags': []}
    assert apply_decider_model_identity_fence(
        role='eod-summarizer',
        normalized_feedback=original,
        model_identity=None,
        expected_model=None,
    ) == original


def test_model_cohort_selection_does_not_mix_verified_actual_models() -> None:
    expected_identity = build_observed_model_identity(
        role='signal-decider',
        expected_model=DEFAULT_DECIDER_EXPECTED_MODEL,
        actual_provider='openai',
        actual_model='gpt-5.6-sol',
    )
    other_identity = {
        **expected_identity,
        'expected_model': 'openai/gpt-5.7',
        'actual_model': 'gpt-5.7',
        'actual_model_ref': 'openai/gpt-5.7',
        'cohort': 'signal-decider|openai/gpt-5.7',
    }
    expected_row = _row(identity=expected_identity)
    other_row = _row(identity=other_identity)
    legacy_row = _row(identity=None)

    selected, exclusions, cohort = select_model_identity_cohort_rows(
        [expected_row, other_row, legacy_row],
        role='signal-decider',
        expected_model=DEFAULT_DECIDER_EXPECTED_MODEL,
    )

    assert selected == [expected_row]
    assert cohort == 'signal-decider|openai/gpt-5.6-sol'
    assert [item['snapshot_id'] for item in exclusions] == [other_row.id, legacy_row.id]
    assert {item['status_reason'] for item in exclusions} == {
        'model_identity_cohort_mismatch',
        'model_identity_unverified',
    }


def test_legacy_unattributed_rows_are_excluded_from_profitability_cohorts() -> None:
    first = _row(identity=None)
    second = _row(identity=None)

    selected, exclusions, cohort = select_model_identity_cohort_rows(
        [first, second],
        role='signal-decider',
        expected_model=DEFAULT_DECIDER_EXPECTED_MODEL,
    )

    assert selected == []
    assert cohort == 'signal-decider|openai/gpt-5.6-sol'
    assert [item['status_reason'] for item in exclusions] == [
        'model_identity_unverified',
        'model_identity_unverified',
    ]


def test_model_cohort_selection_reads_normalized_single_dispatch_provenance() -> None:
    identity = build_observed_model_identity(
        role='signal-decider',
        expected_model=DEFAULT_DECIDER_EXPECTED_MODEL,
        actual_provider='openai',
        actual_model='gpt-5.6-sol',
    )
    row = SimpleNamespace(
        id=1,
        response_summary_payload={
            'normalized_feedback': {
                'model_identity': {
                    **identity,
                    'status': 'verified',
                    'cohort': 'signal-decider|openai/gpt-5.6-sol',
                }
            }
        },
    )

    selected, exclusions, cohort = select_model_identity_cohort_rows(
        [row],
        role='signal-decider',
        expected_model=DEFAULT_DECIDER_EXPECTED_MODEL,
    )

    assert selected == [row]
    assert exclusions == []
    assert cohort == 'signal-decider|openai/gpt-5.6-sol'


def test_gateway_override_is_requested_only_until_post_dispatch_observation() -> None:
    identity = build_requested_model_identity(
        role='signal-decider',
        expected_model=DEFAULT_DECIDER_EXPECTED_MODEL,
    )

    assert identity['actual_model_ref'] is None
    assert identity['verified'] is False
    assert identity['status'] == 'actual_model_missing'


def test_runtime_contract_provenance_verifies_live_workspace(tmp_path) -> None:
    canonical = build_runtime_contract_provenance('signal-decider')
    target = tmp_path / 'workspace-signal-decider' / 'AGENTS.md'
    target.parent.mkdir(parents=True)
    repo_root = __import__('pathlib').Path(__file__).resolve().parents[3]
    source = repo_root / 'runtime_contracts/openclaw/v1/signal-decider/AGENTS.md'
    target.write_bytes(source.read_bytes())

    provenance = build_runtime_contract_provenance(
        'signal-decider',
        openclaw_state_dir=tmp_path,
    )

    assert provenance['version'] == 'v1'
    assert provenance['expected_sha256'].startswith('sha256:')
    assert provenance['live_sha256'] == provenance['expected_sha256']
    assert provenance['verified'] is True
    assert provenance['role'] == 'signal-decider'


def test_runtime_contract_mismatch_is_non_actionable(tmp_path) -> None:
    target = tmp_path / 'workspace-signal-decider' / 'AGENTS.md'
    target.parent.mkdir(parents=True)
    target.write_text('stale runtime contract\n', encoding='utf-8')
    contract = build_runtime_contract_provenance(
        'signal-decider',
        openclaw_state_dir=tmp_path,
    )
    identity = build_observed_model_identity(
        role='signal-decider',
        expected_model=DEFAULT_DECIDER_EXPECTED_MODEL,
        actual_provider='openai',
        actual_model='gpt-5.6-sol',
    )

    fenced = apply_decider_model_identity_fence(
        role='signal-decider',
        normalized_feedback=_feedback(),
        model_identity=identity,
        expected_model=DEFAULT_DECIDER_EXPECTED_MODEL,
        runtime_contract=contract,
    )

    assert contract['status'] == 'live_contract_mismatch'
    assert fenced['next_day_open_action'] == 'reject'
    assert 'runtime_contract_unverified' in fenced['risk_flags']


def test_dispatch_runtime_contract_rejects_same_hash_with_changed_file_fingerprint() -> None:
    pre = {
        'role': 'signal-decider',
        'version': 'v1',
        'expected_sha256': 'sha256:canonical',
        'expected_file_fingerprint': {'dev': 1, 'ino': 2, 'size': 3, 'mtime_ns': 4, 'ctime_ns': 5},
        'live_sha256': 'sha256:canonical',
        'live_file_fingerprint': {'dev': 6, 'ino': 7, 'size': 3, 'mtime_ns': 8, 'ctime_ns': 9},
        'verified': True,
        'status': 'verified',
    }
    post = {
        **pre,
        'live_file_fingerprint': {
            **pre['live_file_fingerprint'],
            'ctime_ns': 10,
        },
    }

    proof = build_dispatch_runtime_contract_provenance(
        pre_dispatch=pre,
        post_dispatch=post,
    )

    assert proof['verified'] is False
    assert proof['status'] == 'live_contract_changed_during_dispatch'
    assert proof['live_sha256'] == 'sha256:canonical'
    assert proof['live_file_fingerprint'] is None


def test_dispatch_runtime_contract_rejects_restored_file_after_parent_chain_change() -> None:
    directory = {
        'dev': 1,
        'ino': 2,
        'mode': 16877,
        'size': 96,
        'mtime_ns': 4,
        'ctime_ns': 5,
    }
    pre = {
        'role': 'signal-decider',
        'version': 'v1',
        'expected_sha256': 'sha256:canonical',
        'expected_file_fingerprint': {'dev': 1, 'ino': 3, 'size': 3, 'mtime_ns': 4, 'ctime_ns': 5},
        'expected_path_fingerprint': [directory],
        'live_sha256': 'sha256:canonical',
        'live_file_fingerprint': {'dev': 1, 'ino': 4, 'size': 3, 'mtime_ns': 4, 'ctime_ns': 5},
        'live_path_fingerprint': [directory],
        'verified': True,
        'status': 'verified',
    }
    post = {
        **pre,
        'live_path_fingerprint': [{**directory, 'ctime_ns': 6}],
    }

    proof = build_dispatch_runtime_contract_provenance(
        pre_dispatch=pre,
        post_dispatch=post,
    )

    assert proof['verified'] is False
    assert proof['status'] == 'live_contract_changed_during_dispatch'
    assert proof['live_file_fingerprint'] == pre['live_file_fingerprint']
    assert proof['live_path_fingerprint'] is None


@pytest.mark.parametrize('symlink_component', ['state-root', 'workspace'])
def test_runtime_contract_provenance_rejects_symlinked_scope(
    tmp_path,
    symlink_component,
) -> None:
    repo_root = __import__('pathlib').Path(__file__).resolve().parents[3]
    canonical = repo_root / 'runtime_contracts/openclaw/v1/signal-decider/AGENTS.md'
    real_state = tmp_path / 'real-state'
    real_workspace = real_state / 'workspace-signal-decider'
    real_workspace.mkdir(parents=True)
    (real_workspace / 'AGENTS.md').write_bytes(canonical.read_bytes())
    if symlink_component == 'state-root':
        state_root = tmp_path / 'linked-state'
        state_root.symlink_to(real_state, target_is_directory=True)
    else:
        state_root = tmp_path / 'state'
        state_root.mkdir()
        (state_root / 'workspace-signal-decider').symlink_to(
            real_workspace,
            target_is_directory=True,
        )

    provenance = build_runtime_contract_provenance(
        'signal-decider',
        openclaw_state_dir=state_root,
    )

    assert provenance['verified'] is False
    assert provenance['live_sha256'] is None


def test_decider_expected_model_settings_default_to_explicit_frontier_identity() -> None:
    configured = Settings(_env_file=None)

    assert configured.openclaw_signal_decider_expected_model == 'openai/gpt-5.6-sol'
    assert configured.openclaw_short_term_decider_expected_model == 'openai/gpt-5.6-sol'


def test_post_dispatch_session_observation_accepts_done_and_rejects_stale(tmp_path, monkeypatch) -> None:
    session_key = 'feedback/signal-decider/test-run'
    started_at = datetime.now(timezone.utc)
    started_ms = int(started_at.timestamp() * 1000)
    registry = tmp_path / 'agents' / 'signal-decider' / 'sessions' / 'sessions.json'
    registry.parent.mkdir(parents=True)
    registry.write_text(
        json.dumps(
            {
                session_key: {
                    'sessionId': 'session-1',
                    'status': 'done',
                    'startedAt': started_ms,
                    'endedAt': started_ms + 10,
                    'updatedAt': started_ms + 10,
                    'modelProvider': 'openai',
                    'model': 'gpt-5.6-sol',
                }
            }
        ),
        encoding='utf-8',
    )
    fake_settings = SimpleNamespace(
        openclaw_base_url='http://127.0.0.1:18789',
        openclaw_gateway_token=None,
        openclaw_agent_timeout_seconds=30,
        openclaw_agent_max_retries=0,
        openclaw_agent_retry_backoff_seconds=0,
        openclaw_state_dir=str(tmp_path),
    )
    monkeypatch.setattr(orchestrator_module, 'settings', fake_settings)
    client = OpenClawAgentClient()

    observed = client._observe_post_dispatch_model_identity(
        role='signal-decider',
        agent_name='signal-decider',
        session_key=session_key,
        expected_model='openai/gpt-5.6-sol',
        dispatch_started_at=started_at,
        session_id='session-1',
    )
    stale = client._observe_post_dispatch_model_identity(
        role='signal-decider',
        agent_name='signal-decider',
        session_key=session_key,
        expected_model='openai/gpt-5.6-sol',
        dispatch_started_at=started_at + timedelta(minutes=1),
        session_id='session-1',
    )
    future_ms = started_ms + int(timedelta(minutes=10).total_seconds() * 1000)
    registry.write_text(
        json.dumps(
            {
                session_key: {
                    'sessionId': 'session-1',
                    'status': 'done',
                    'startedAt': future_ms,
                    'endedAt': future_ms + 10,
                    'updatedAt': future_ms + 10,
                    'modelProvider': 'openai',
                    'model': 'gpt-5.6-sol',
                }
            }
        ),
        encoding='utf-8',
    )
    future = client._observe_post_dispatch_model_identity(
        role='signal-decider',
        agent_name='signal-decider',
        session_key=session_key,
        expected_model='openai/gpt-5.6-sol',
        dispatch_started_at=started_at,
        session_id='session-1',
    )

    assert observed['actual_model_ref'] == 'openai/gpt-5.6-sol'
    assert observed['verification_basis'] == 'post_dispatch_session_observation'
    assert stale['actual_model_ref'] is None
    assert future['actual_model_ref'] is None


def test_post_dispatch_session_registry_warns_before_bounded_fail_closed_limit(tmp_path, monkeypatch) -> None:
    session_key = 'feedback/short-term-decider/test-run'
    started_at = datetime.now(timezone.utc)
    started_ms = int(started_at.timestamp() * 1000)
    registry = tmp_path / 'agents' / 'short-term-decider' / 'sessions' / 'sessions.json'
    registry.parent.mkdir(parents=True)
    registry.write_text(
        json.dumps(
            {
                session_key: {
                    'sessionId': 'session-1',
                    'status': 'done',
                    'startedAt': started_ms,
                    'endedAt': started_ms + 10,
                    'updatedAt': started_ms + 10,
                    'modelProvider': 'openai',
                    'model': 'gpt-5.6-sol',
                }
            }
        )
        + (' ' * 512),
        encoding='utf-8',
    )
    fake_settings = SimpleNamespace(
        openclaw_base_url='http://127.0.0.1:18789',
        openclaw_gateway_token=None,
        openclaw_agent_timeout_seconds=30,
        openclaw_agent_max_retries=0,
        openclaw_agent_retry_backoff_seconds=0,
        openclaw_state_dir=str(tmp_path),
    )
    warning = Mock()
    monkeypatch.setattr(orchestrator_module, 'settings', fake_settings)
    monkeypatch.setattr(orchestrator_module, 'OPENCLAW_SESSION_REGISTRY_WARNING_BYTES', 256)
    monkeypatch.setattr(orchestrator_module, 'OPENCLAW_SESSION_REGISTRY_MAX_BYTES', 1_024)
    monkeypatch.setattr(orchestrator_module.logger, 'warning', warning)
    client = OpenClawAgentClient()

    observed = client._observe_post_dispatch_model_identity(
        role='short-term-decider',
        agent_name='short-term-decider',
        session_key=session_key,
        expected_model='openai/gpt-5.6-sol',
        dispatch_started_at=started_at,
        session_id='session-1',
    )
    assert observed['actual_model_ref'] == 'openai/gpt-5.6-sol'
    warning.assert_called_once()

    monkeypatch.setattr(orchestrator_module, 'OPENCLAW_SESSION_REGISTRY_MAX_BYTES', 512)
    rejected = client._observe_post_dispatch_model_identity(
        role='short-term-decider',
        agent_name='short-term-decider',
        session_key=session_key,
        expected_model='openai/gpt-5.6-sol',
        dispatch_started_at=started_at,
        session_id='session-1',
    )
    assert rejected['actual_model_ref'] is None


@pytest.mark.parametrize('symlink_component', ['state-root', 'agents', 'agent', 'sessions', 'registry'])
def test_post_dispatch_model_observation_rejects_symlinked_registry_scope(
    tmp_path,
    monkeypatch,
    symlink_component,
) -> None:
    real_state = tmp_path / 'real-state'
    real_sessions = real_state / 'agents' / 'signal-decider' / 'sessions'
    real_sessions.mkdir(parents=True)
    (real_sessions / 'sessions.json').write_text('{}', encoding='utf-8')
    state_root = tmp_path / 'state'
    if symlink_component == 'state-root':
        state_root.symlink_to(real_state, target_is_directory=True)
    elif symlink_component == 'agents':
        state_root.mkdir()
        (state_root / 'agents').symlink_to(real_state / 'agents', target_is_directory=True)
    elif symlink_component == 'agent':
        (state_root / 'agents').mkdir(parents=True)
        (state_root / 'agents' / 'signal-decider').symlink_to(
            real_state / 'agents' / 'signal-decider',
            target_is_directory=True,
        )
    elif symlink_component == 'sessions':
        agent_dir = state_root / 'agents' / 'signal-decider'
        agent_dir.mkdir(parents=True)
        (agent_dir / 'sessions').symlink_to(real_sessions, target_is_directory=True)
    else:
        sessions = state_root / 'agents' / 'signal-decider' / 'sessions'
        sessions.mkdir(parents=True)
        (sessions / 'sessions.json').symlink_to(real_sessions / 'sessions.json')
    fake_settings = SimpleNamespace(
        openclaw_base_url='http://127.0.0.1:18789',
        openclaw_gateway_token=None,
        openclaw_agent_timeout_seconds=30,
        openclaw_agent_max_retries=0,
        openclaw_agent_retry_backoff_seconds=0,
        openclaw_state_dir=str(state_root),
    )
    monkeypatch.setattr(orchestrator_module, 'settings', fake_settings)

    observed = OpenClawAgentClient()._observe_post_dispatch_model_identity(
        role='signal-decider',
        agent_name='signal-decider',
        session_key='feedback/signal-decider/test-run',
        expected_model='openai/gpt-5.6-sol',
        dispatch_started_at=datetime.now(timezone.utc),
    )

    assert observed['actual_model_ref'] is None
    assert observed['actual_source_fingerprint'] is None
