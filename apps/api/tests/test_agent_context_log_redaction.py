import json

import pytest

from app.agent_context import logging as feedback_logging


@pytest.mark.parametrize('container', [dict, list])
def test_depth_limited_feedback_logs_do_not_stringify_nested_secrets(tmp_path, monkeypatch, container):
    credentials = {'access_token': 'dummy-private-credential'}
    deepest = credentials if container is dict else [credentials]
    diagnostics = {'one': {'two': {'three': {'four': deepest}}}}
    monkeypatch.setattr(
        feedback_logging,
        '__file__',
        str(tmp_path / 'app' / 'agent_context' / 'logging.py'),
    )

    feedback_logging.log_agent_feedback_event(
        'test_nested_redaction',
        request_run_id='audit-test-run',
        status='failed',
        diagnostics=diagnostics,
        safe_context={'symbol': '005930', 'count': 1},
        auth_token='dummy-top-level-credential',
    )

    log_text = (tmp_path / 'var' / 'log' / 'agent-feedback-runtime.log').read_text()
    assert 'dummy-private-credential' not in log_text
    assert 'dummy-top-level-credential' not in log_text
    event = json.loads(log_text)
    assert event['stage'] == 'test_nested_redaction'
    assert event['request_run_id'] == 'audit-test-run'
    assert event['safe_context'] == {'symbol': '005930', 'count': 1}
    assert event['auth_token'] == '[REDACTED]'
