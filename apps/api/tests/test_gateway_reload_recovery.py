import json
from datetime import date
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from app.agent_context import orchestrator
from app.agent_context.feedback_failure_taxonomy import classify_feedback_failure


@pytest.fixture
def gateway(monkeypatch, tmp_path):
    settings = SimpleNamespace(
        openclaw_base_url=f'https://{uuid4().hex}.example',
        openclaw_gateway_token='test-token', openclaw_agent_timeout_seconds=30,
        openclaw_agent_max_retries=9, openclaw_agent_retry_backoff_seconds=0,
        openclaw_agent_max_prompt_chars=0, openclaw_state_dir=str(tmp_path),
    )
    monkeypatch.setattr(orchestrator, 'settings', settings)
    monkeypatch.setattr(orchestrator, 'build_runtime_contract_provenance', lambda *a, **kw: None)
    monkeypatch.setattr(orchestrator, 'log_agent_feedback_event', lambda *a, **kw: None)
    monkeypatch.setattr(orchestrator.OpenClawAgentClient, '_observe_post_dispatch_model_identity', lambda *a, **kw: None)
    real_client = httpx.Client
    calls = []
    feedback = {
        'role': 'eod-summarizer', 'symbol': '037950', 'market_date': '2026-11-13',
        'summary': 'Original completed feedback', 'confidence': 0.7,
        'risk_flags': [], 'details': {}, 'improvement_notes': [],
    }

    def run(*, status=403, message='ToolAuthorizationError: Gateway requester authority changed', marker='batch-request::reload-dispatch:attempt-1', output=None, history_status=200, following_marker=None):
        def handle(req):
            body = json.loads(req.content)
            calls.append((req.url.path, body, req.headers))
            if req.url.path == '/v1/responses':
                return httpx.Response(status, json={'status': 'aborted'} if status == 200 else {'error': {'message': message}})
            assert body['tool'] == 'sessions_history'
            assert body['args']['sessionKey'] == 'agent:eod-summarizer:feedback-fresh/reload-run/037950'
            return httpx.Response(history_status, json={'result': {'details': {'messages': [
                {'role': 'user', 'content': [{'type': 'text', 'text': marker}]},
                *([{'role': 'user', 'content': [{'type': 'text', 'text': following_marker}]}] if following_marker else []),
                {'role': 'assistant', 'content': [{'type': 'text', 'text': output if output is not None else json.dumps({'items': [{'symbol': '037950', 'feedback': feedback}]})}]},
            ]}}})

        monkeypatch.setattr(orchestrator.httpx, 'Client', lambda **kw: real_client(transport=httpx.MockTransport(handle), **kw))
        client = orchestrator.OpenClawAgentClient()
        return client.dispatch_batch(
            spec=orchestrator.DISPATCH_SPECS[0], market_date=date(2026, 11, 13),
            batch_items=[{'symbol': '037950', 'market_date': '2026-11-13',
                          'request_run_id': 'reload-run', 'dispatch_id': 'reload-dispatch',
                          'session_key': 'agent:eod-summarizer:feedback-fresh/reload-run/037950',
                          'session_reuse_mode': 'fresh_per_batch'}],
        )

    return run, calls


def test_authority_reload_recovers_original_matching_reply_without_model_repost(gateway):
    run, calls = gateway
    result = run()
    assert result['decoded']['items'][0]['feedback']['summary'] == 'Original completed feedback'
    assert result['response_correlation']['decoded_source'] == 'session_history_after_marker'
    assert result['request_marker'] == 'batch-request::reload-dispatch:attempt-1'
    assert [path for path, _, _ in calls] == ['/v1/responses', '/tools/invoke']


@pytest.mark.parametrize('message', ['Forbidden: invalid bearer token', 'Forbidden: previous Gateway requester authority changed'])
def test_ordinary_forbidden_remains_terminal_without_history_or_repost(gateway, message):
    run, calls = gateway
    with pytest.raises(orchestrator.AgentDispatchTerminalError):
        run(message=message)
    assert [path for path, _, _ in calls] == ['/v1/responses']


@pytest.mark.parametrize('case', ['wrong_marker', 'prefix_marker', 'later_request', 'invalid_json', 'wrong_symbol', 'history_unavailable'])
def test_reload_without_valid_matching_reply_is_bounded_retryable_outage(gateway, case):
    run, calls = gateway
    args = {'following_marker': 'batch-request::other-dispatch:attempt-1'} if case == 'later_request' else (
        {'marker': 'batch-request::reload-dispatch:attempt-10'} if case == 'prefix_marker' else (
        {'marker': 'batch-request::other-dispatch:attempt-1'} if case == 'wrong_marker' else (
        {'output': 'not valid JSON'} if case == 'invalid_json' else (
            {'output': '{"items":[{"symbol":"005930","feedback":{}}]}'} if case == 'wrong_symbol' else {'history_status': 503}
        )
    )))
    with pytest.raises(orchestrator.AgentDispatchRetryableError) as raised:
        run(**args)
    details = raised.value.details
    assert details['failure_kind'] == 'openclaw_transport_error'
    assert details['failure_mode'] == 'gateway_requester_authority_changed'
    assert details['response_status_code'] == 403
    assert details['request_marker'] == 'batch-request::reload-dispatch:attempt-1'
    assert details['shared_dispatch_outage'] is True
    assert [path for path, _, _ in calls] == ['/v1/responses', '/tools/invoke']


def test_aborted_inflight_response_recovers_original_matching_reply(gateway):
    run, calls = gateway
    result = run(status=200)
    assert result['decoded']['items'][0]['feedback']['summary'] == 'Original completed feedback'
    assert [path for path, _, _ in calls] == ['/v1/responses', '/tools/invoke']


def test_unresolved_reload_blocks_same_run_from_reposting(gateway):
    run, calls = gateway
    with pytest.raises(orchestrator.AgentDispatchRetryableError):
        run(marker='batch-request::other-dispatch:attempt-1')
    with pytest.raises(orchestrator.AgentDispatchTerminalError) as raised:
        run()
    assert raised.value.details['retryable'] is True
    assert [path for path, _, _ in calls] == ['/v1/responses', '/tools/invoke']


@pytest.mark.parametrize('mode,message,expected', [
    ('gateway_requester_authority_changed', 'ToolAuthorizationError: Gateway requester authority changed', 'openclaw_transport_error'),
    ('gateway_requester_authority_changed', 'Forbidden: invalid bearer token', 'openclaw_auth_rejected'),
    (None, 'ToolAuthorizationError: Gateway requester authority changed', 'openclaw_auth_rejected'),
])
def test_only_confirmed_local_authority_reload_403_is_transport(mode, message, expected):
    classification = classify_feedback_failure(
        failure_kind='openclaw_transport_error',
        context={'failure_mode': mode, 'response_status_code': 403, 'error_message': message},
    )
    assert classification.failure_kind == expected


def test_authority_reload_classification_survives_persisted_dispatch_diagnostics():
    classification = classify_feedback_failure(
        failure_kind='openclaw_transport_error',
        summary={'dispatch_diagnostics': {
            'failure_mode': 'gateway_requester_authority_changed',
            'response_status_code': 403,
            'response_text_excerpt': '{"error":{"message":"Gateway requester authority changed","type":"forbidden"}}',
        }},
    )
    assert classification.failure_kind == 'openclaw_transport_error'
