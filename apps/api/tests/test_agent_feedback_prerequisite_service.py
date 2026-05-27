from unittest.mock import patch

from app.agent_context.prerequisite_service import (
    FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER,
    PYKRX_DECIDER_ENRICHMENT_REQUIRED_CATEGORIES,
    AgentFeedbackPrerequisiteService,
)


def _completed_stage(stage_id: str) -> dict:
    return {
        'task': stage_id,
        'label': stage_id.replace('_', ' '),
        'status': 'completed',
        'current_activity': f'Completed {stage_id}',
        'details': {},
    }


def _pykrx_enrichment_stage(
    *,
    category_overrides: dict[str, dict],
    status: str = 'completed',
) -> dict:
    categories = {
        category: {
            'scope_type': 'market' if category in {'market_breadth', 'investor_flow_market'} else 'symbol',
            'present_rows': 3 if category not in {'market_breadth', 'investor_flow_market'} else 2,
            'expected_rows': 3 if category not in {'market_breadth', 'investor_flow_market'} else 2,
            'coverage_ratio': 1.0,
            'coverage_status': 'complete',
            'latest_captured_at': '2026-05-26T15:40:00+09:00',
        }
        for category in PYKRX_DECIDER_ENRICHMENT_REQUIRED_CATEGORIES
    }
    for category, override in category_overrides.items():
        categories[category] = {
            **categories.get(category, {'scope_type': 'symbol'}),
            **override,
        }
    return {
        'task': 'pykrx_enrichment',
        'label': 'pykrx enrichment collect',
        'status': status,
        'current_activity': 'Completed pykrx enrichment collect',
        'total': len(categories),
        'succeeded': sum(1 for item in categories.values() if int(item.get('present_rows') or 0) > 0),
        'processed': len(categories),
        'details': {
            'requested_symbol_count': 3,
            'requested_market_count': 2,
            'categories': categories,
        },
    }


def _pipeline_status(*, pykrx_stage: dict) -> dict:
    return {
        'stages': {
            stage_id: pykrx_stage if stage_id == 'pykrx_enrichment' else _completed_stage(stage_id)
            for stage_id in FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER
        }
    }


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


def test_inspect_feedback_prerequisites_blocks_missing_pykrx_enrichment_category_rows():
    pykrx_stage = _pykrx_enrichment_stage(
        status='partial',
        category_overrides={
            'investor_flow_symbol': {
                'present_rows': 0,
                'expected_rows': 3,
                'coverage_ratio': 0.0,
                'coverage_status': 'missing',
                'latest_captured_at': None,
            },
        },
    )

    with patch(
        'app.eod.pipeline_status.eod_pipeline_status_service.read',
        return_value=_pipeline_status(pykrx_stage=pykrx_stage),
    ):
        result = AgentFeedbackPrerequisiteService().inspect_feedback_prerequisites(
            market_date='2026-05-26',
            fs_div='CFS',
        )

    assert result['status'] == 'blocked'
    assert result['ready'] is False
    assert 'pykrx_enrichment' in result['required_stage_ids']
    assert result['blocked_stage_ids'] == ['pykrx_enrichment']
    missing = result['missing_prerequisites'][0]
    assert missing['stage'] == 'pykrx_enrichment'
    blocking_categories = missing['details']['decider_blocking_categories']
    assert blocking_categories == [
        {
            'category': 'investor_flow_symbol',
            'scope_type': 'symbol',
            'gate_status': 'blocked',
            'coverage_status': 'missing',
            'expected_rows': 3,
            'present_rows': 0,
            'missing_rows': 3,
            'coverage_ratio': 0.0,
            'min_coverage_ratio': 0.99,
            'reason': 'missing_required_pykrx_enrichment_category',
        }
    ]


def test_inspect_feedback_prerequisites_blocks_material_partial_pykrx_enrichment_coverage():
    pykrx_stage = _pykrx_enrichment_stage(
        status='completed',
        category_overrides={
            'short_selling': {
                'present_rows': 2,
                'expected_rows': 3,
                'coverage_ratio': 0.666667,
                'coverage_status': 'partial',
            },
        },
    )

    with patch(
        'app.eod.pipeline_status.eod_pipeline_status_service.read',
        return_value=_pipeline_status(pykrx_stage=pykrx_stage),
    ):
        result = AgentFeedbackPrerequisiteService().inspect_feedback_prerequisites(
            market_date='2026-05-26',
            fs_div='CFS',
        )

    assert result['status'] == 'blocked'
    assert result['blocked_stage_ids'] == ['pykrx_enrichment']
    blocking_categories = result['stages']['pykrx_enrichment']['details']['decider_blocking_categories']
    assert blocking_categories[0]['category'] == 'short_selling'
    assert blocking_categories[0]['expected_rows'] == 3
    assert blocking_categories[0]['present_rows'] == 2
    assert blocking_categories[0]['missing_rows'] == 1
    assert blocking_categories[0]['reason'] == 'required_pykrx_enrichment_category_below_min_coverage'
