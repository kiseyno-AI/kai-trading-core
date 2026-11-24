"""A processed failure must never become a successful agent publication."""

import pytest

from app.eod.pykrx_pipeline_runner import _extract_result_counts


@pytest.mark.parametrize('stage_id', ['agent_feedback_summaries', 'signal_decider'])
def test_zero_success_does_not_fall_back_to_processed_failures(stage_id):
    result = {
        'status': 'partial',
        'progress': {
            'summary': {
                'completed_feedback_count': 0,
                'completed_symbols': 0,
                'processed_symbols': 60,
                'failed_items': 60,
                'expected_feedback_count': 157,
            },
        },
    }

    assert _extract_result_counts(result, stage_id=stage_id) == {
        'processed': 60,
        'total': 157,
        'succeeded': 0,
        'failed': 60,
    }


def test_legacy_completed_symbol_zero_is_authoritative():
    result = {
        'status': 'partial',
        'progress': {'summary': {
            'completed_symbols': 0,
            'processed_symbols': 3,
            'failed_items': 3,
            'expected_feedback_count': 5,
        }},
    }

    assert _extract_result_counts(result)['succeeded'] == 0
