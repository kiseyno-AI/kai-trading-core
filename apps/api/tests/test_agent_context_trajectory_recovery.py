import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from app.agent_context import orchestrator as orchestrator_module
from app.agent_context.orchestrator import OpenClawAgentClient


@pytest.mark.parametrize('use_registry', [False, True])
@pytest.mark.parametrize(
    'malformed_event',
    [None, [], ['dummy-session-key'], 'dummy-session-key', 7, True],
)
def test_trajectory_recovery_skips_non_object_records(
    tmp_path, use_registry, malformed_event,
):
    session_key = 'dummy-session-key'
    session_id = 'dummy-session-id'
    response_id = 'dummy-response-id'
    sessions_dir = tmp_path / 'agents' / 'eod-summarizer' / 'sessions'
    sessions_dir.mkdir(parents=True)
    if use_registry:
        (sessions_dir / 'sessions.json').write_text(
            json.dumps({session_key: {'sessionId': session_id}}),
            encoding='utf-8',
        )
    decoded = {'items': [{'symbol': '005930', 'feedback': {'summary': 'Recovered'}}]}
    event = {
        'type': 'trace.artifacts',
        'sessionId': session_id,
        'sessionKey': session_key,
        'runId': response_id,
        'data': {
            'assistantTexts': [json.dumps(decoded)],
            'finalStatus': 'success',
        },
    }
    (sessions_dir / f'{session_id}.trajectory.jsonl').write_text(
        '\n'.join(json.dumps(row) for row in [malformed_event, event]) + '\n',
        encoding='utf-8',
    )
    client = object.__new__(OpenClawAgentClient)
    with patch.object(
        orchestrator_module,
        'settings',
        SimpleNamespace(openclaw_state_dir=str(tmp_path)),
    ):
        result = client._recover_from_trajectory_artifacts(
            agent_name='eod-summarizer',
            session_key=session_key,
            response_id=response_id,
        )

    assert result['decoded'] == decoded
    diagnostics = result['diagnostics']
    assert diagnostics['status'] == 'recovered'
    assert diagnostics['matched_response_id'] is True
    assert diagnostics['candidate_run_ids'] == [response_id]
    assert diagnostics['lookup_mode'] == (
        'sessions_registry' if use_registry else 'trajectory_scan'
    )
