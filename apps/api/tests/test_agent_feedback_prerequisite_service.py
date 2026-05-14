from app.agent_context.prerequisite_service import AgentFeedbackPrerequisiteService


def test_high_coverage_terminal_partial_prerequisite_is_progressable_ready():
    payload = {
        'status': 'partial',
        'total': 2809,
        'succeeded': 2808,
        'processed': 2808,
        'details': {
            'rows_persisted': 2808,
            'expected_rows': 2809,
            'remaining_retryable_events': 0,
        },
    }

    assert AgentFeedbackPrerequisiteService._classify_stage_gate_status(payload) == 'ready'


def test_terminal_partial_prerequisite_waits_when_retry_cycles_remain():
    payload = {
        'status': 'partial',
        'total': 2809,
        'succeeded': 2808,
        'processed': 2808,
        'details': {
            'rows_persisted': 2808,
            'expected_rows': 2809,
            'retry_cycles_requested': 3,
            'retry_cycles_completed': 1,
            'remaining_retryable_events': 1,
        },
    }

    assert AgentFeedbackPrerequisiteService._classify_stage_gate_status(payload) == 'blocked'


def test_low_coverage_partial_prerequisite_remains_blocked():
    payload = {
        'status': 'partial',
        'total': 2809,
        'succeeded': 2400,
        'processed': 2400,
        'details': {
            'rows_persisted': 2400,
            'expected_rows': 2809,
            'remaining_retryable_events': 0,
        },
    }

    assert AgentFeedbackPrerequisiteService._classify_stage_gate_status(payload) == 'blocked'


def test_partial_prerequisite_with_blocking_dependency_remains_blocked():
    payload = {
        'status': 'partial',
        'total': 2809,
        'succeeded': 2808,
        'processed': 2808,
        'details': {
            'rows_persisted': 2808,
            'expected_rows': 2809,
            'blocking_stages': ['fundamentals_daily_refresh'],
            'remaining_retryable_events': 0,
        },
    }

    assert AgentFeedbackPrerequisiteService._classify_stage_gate_status(payload) == 'blocked'
