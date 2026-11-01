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


def _pipeline_status(*, pykrx_stage: dict, stage_overrides: dict[str, dict] | None = None) -> dict:
    stages = {
        stage_id: pykrx_stage if stage_id == 'pykrx_enrichment' else _completed_stage(stage_id)
        for stage_id in FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER
    }
    stages.update(stage_overrides or {})
    return {'stages': stages}


def _price_collection_stage(*, rows_persisted: int) -> dict:
    return {
        'task': 'price_collection',
        'status': 'completed',
        'processed': rows_persisted,
        'total': rows_persisted,
        'succeeded': rows_persisted,
        'details': {
            'rows_persisted': rows_persisted,
            'markets': ['KOSPI', 'KOSDAQ'],
        },
    }


def test_current_run_can_require_signal_features_without_optional_context():
    required_stage_ids = (
        *FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER,
        'signal_features',
    )
    assert 'signal_features' not in FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER
    assert 'external_news_collect' not in FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER
    assert 'external_news_enrich' not in FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER
    assert 'macro_context_refresh' not in FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER
    pipeline_status = _pipeline_status(
        pykrx_stage=_pykrx_enrichment_stage(category_overrides={}),
        stage_overrides={
            'signal_features': {
                'task': 'signal_features',
                'status': 'pending',
                'current_activity': 'Pending signal features',
                'details': {},
            },
        },
    )

    with patch(
        'app.eod.pipeline_status.eod_pipeline_status_service.read',
        return_value=pipeline_status,
    ):
        result = AgentFeedbackPrerequisiteService().inspect_feedback_prerequisites(
            market_date='2026-09-18',
            fs_div='CFS',
            required_stage_ids=required_stage_ids,
        )

    assert result['ready'] is False
    assert result['status'] == 'waiting'
    assert result['waiting_stage_ids'] == ['signal_features']
    assert result['required_stage_ids'] == list(required_stage_ids)


def test_default_point_in_time_prerequisites_ignore_current_signal_features():
    pipeline_status = _pipeline_status(
        pykrx_stage=_pykrx_enrichment_stage(category_overrides={}),
        stage_overrides={
            'signal_features': {
                'task': 'signal_features',
                'status': 'pending',
                'current_activity': 'Pending current-only signal features',
                'details': {},
            },
        },
    )

    with patch(
        'app.eod.pipeline_status.eod_pipeline_status_service.read',
        return_value=pipeline_status,
    ):
        result = AgentFeedbackPrerequisiteService().inspect_feedback_prerequisites(
            market_date='2026-09-18',
            fs_div='CFS',
        )

    assert result['ready'] is True
    assert result['required_stage_ids'] == list(
        FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER
    )


def test_prerequisites_include_technical_inputs_and_apply_requested_scope():
    assert FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER[:2] == (
        'technical_snapshots',
        'action_summaries',
    )
    pipeline_status = _pipeline_status(
        pykrx_stage=_pykrx_enrichment_stage(category_overrides={}),
    )

    with patch(
        'app.eod.pipeline_status.eod_pipeline_status_service.read',
        return_value=pipeline_status,
    ) as read_status:
        result = AgentFeedbackPrerequisiteService().inspect_feedback_prerequisites(
            market_date='2026-09-18',
            fs_div='CFS',
            symbols=['005930', '000660', '005930'],
        )

    assert result['ready'] is True
    assert result['scope_symbols'] == ['005930', '000660']
    assert result['scope_symbol_count'] == 2
    assert 'technical_snapshots' in result['ready_stage_ids']
    assert 'action_summaries' in result['ready_stage_ids']
    assert read_status.call_args.kwargs['scope_symbols'] == ['005930', '000660']


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


def test_processed_count_does_not_substitute_for_missing_success_evidence():
    payload = {
        'status': 'partial',
        'total': 100,
        'processed': 100,
        'details': {
            'expected_rows': 100,
            'remaining_retryable_events': 0,
        },
    }

    assert AgentFeedbackPrerequisiteService._classify_stage_gate_status(payload) == 'blocked'


def test_explicit_zero_success_or_persistence_remains_authoritative():
    cases = (
        {
            'status': 'partial',
            'total': 100,
            'processed': 100,
            'succeeded': 0,
            'details': {
                'expected_rows': 100,
                'rows_persisted': 100,
                'remaining_retryable_events': 0,
            },
        },
        {
            'status': 'partial',
            'total': 100,
            'processed': 100,
            'succeeded': 100,
            'details': {
                'expected_rows': 100,
                'rows_persisted': 0,
                'remaining_retryable_events': 0,
            },
        },
    )

    for payload in cases:
        assert AgentFeedbackPrerequisiteService._classify_stage_gate_status(payload) == 'blocked'


def test_completed_explicit_zero_blocks_but_absent_legacy_counts_remain_ready():
    cases = (
        {
            'status': 'completed',
            'total': 100,
            'succeeded': 0,
            'details': {'expected_rows': 100, 'rows_persisted': 100},
        },
        {
            'status': 'completed',
            'total': 100,
            'succeeded': 100,
            'details': {'expected_rows': 100, 'rows_persisted': 0},
        },
    )

    for payload in cases:
        assert AgentFeedbackPrerequisiteService._classify_stage_gate_status(payload) == 'blocked'

    assert AgentFeedbackPrerequisiteService._classify_stage_gate_status(
        {'status': 'completed', 'details': {}}
    ) == 'ready'


def test_partial_coverage_uses_largest_reported_denominator():
    for total, expected_rows in ((1, 100), (100, 1)):
        payload = {
            'status': 'partial',
            'total': total,
            'succeeded': 1,
            'details': {
                'rows_persisted': 1,
                'expected_rows': expected_rows,
                'remaining_retryable_events': 0,
            },
        }

        assert AgentFeedbackPrerequisiteService._classify_stage_gate_status(payload) == 'blocked'


def test_high_coverage_partial_is_blocked_when_no_ready_row_persisted():
    payload = {
        'task': 'momentum_factors',
        'status': 'partial',
        'total': 2678,
        'succeeded': 2677,
        'processed': 2678,
        'details': {
            'rows_persisted': 2677,
            'expected_rows': 2678,
            'remaining_retryable_events': 0,
            'result_summary': {
                'results_persisted': 2677,
                'ready_count': 1,
                'unready_count': 2677,
                'persisted_ready_count': 0,
                'persisted_unready_count': 2677,
            },
        },
    }

    assert AgentFeedbackPrerequisiteService._classify_stage_gate_status(payload) == 'blocked'


def test_konex_only_pykrx_categories_satisfy_feedback_prerequisites():
    not_applicable = {
        category: {
            'present_rows': 0,
            'expected_rows': 0,
            'coverage_ratio': None,
            'coverage_status': 'not_applicable',
            'expectation_status': 'not_applicable',
            'latest_captured_at': None,
        }
        for category in PYKRX_DECIDER_ENRICHMENT_REQUIRED_CATEGORIES
    }
    pipeline_status = _pipeline_status(
        pykrx_stage=_pykrx_enrichment_stage(
            category_overrides=not_applicable,
        )
    )

    with patch(
        'app.eod.pipeline_status.eod_pipeline_status_service.read',
        return_value=pipeline_status,
    ):
        result = AgentFeedbackPrerequisiteService().inspect_feedback_prerequisites(
            market_date='2026-09-18',
            fs_div='CFS',
        )

    assert result['status'] == 'ready'
    assert result['ready'] is True
    assert result['blocked_stage_ids'] == []
    assert result['waiting_stage_ids'] == []
    details = result['stages']['pykrx_enrichment']['details']
    assert details['decider_category_gate_status'] == 'ready'
    assert details['decider_blocking_categories'] == []
    assert details['decider_waiting_categories'] == []
    assert {
        item['gate_status']
        for item in details['decider_category_gates']
    } == {'not_applicable'}


def test_zero_expected_rows_without_explicit_not_applicable_remains_waiting():
    pipeline_status = _pipeline_status(
        pykrx_stage=_pykrx_enrichment_stage(
            category_overrides={
                'investor_flow_symbol': {
                    'present_rows': 0,
                    'expected_rows': 0,
                    'coverage_ratio': None,
                    'coverage_status': 'missing',
                    'expectation_status': 'available',
                    'latest_captured_at': None,
                },
            },
        )
    )

    with patch(
        'app.eod.pipeline_status.eod_pipeline_status_service.read',
        return_value=pipeline_status,
    ):
        result = AgentFeedbackPrerequisiteService().inspect_feedback_prerequisites(
            market_date='2026-09-18',
            fs_div='CFS',
        )

    assert result['status'] == 'waiting'
    assert result['ready'] is False
    details = result['stages']['pykrx_enrichment']['details']
    assert details['decider_category_gate_status'] == 'waiting'
    assert details['decider_not_applicable_categories'] == []
    assert details['decider_waiting_categories'] == [
        {
            'category': 'investor_flow_symbol',
            'scope_type': 'symbol',
            'gate_status': 'waiting',
            'coverage_status': 'missing',
            'expected_rows': 0,
            'present_rows': 0,
            'missing_rows': 0,
            'coverage_ratio': None,
            'reason': 'coverage_expectation_unavailable',
        }
    ]


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


def test_explicit_pipeline_gate_status_overrides_partial_coverage_heuristic():
    payload = {
        'status': 'partial',
        'total': 2806,
        'succeeded': 2805,
        'processed': 2805,
        'gate_status': 'ready',
        'details': {
            'gate_status': 'ready',
            'blocking_status': 'non_blocking',
            'gate_reason': 'missing_rows_outside_active_feedback_scope',
            'rows_persisted': 2805,
            'expected_rows': 2806,
            'missing_count': 1,
            'missing_symbols_sample': ['176750'],
        },
    }

    assert AgentFeedbackPrerequisiteService._classify_stage_gate_status(payload) == 'ready'


def test_explicit_blocked_gate_overrides_full_persisted_coverage():
    payload = {
        'task': 'low_risk_factors',
        'status': 'completed',
        'total': 2_678,
        'succeeded': 2_678,
        'processed': 2_678,
        'gate_status': 'blocked',
        'details': {
            'gate_status': 'blocked',
            'blocking_status': 'blocking',
            'gate_reason': 'no_decision_ready_rows',
            'rows_persisted': 2_678,
            'expected_rows': 2_678,
        },
    }

    assert AgentFeedbackPrerequisiteService._classify_stage_gate_status(payload) == 'blocked'


def test_inspect_feedback_prerequisites_preserves_non_blocking_market_risk_gate():
    pykrx_stage = _pykrx_enrichment_stage(category_overrides={})
    market_risk_stage = {
        'task': 'market_risk_filter',
        'label': 'KIS market-risk filter compute',
        'status': 'partial',
        'gate_status': 'ready',
        'severity': 'degraded',
        'blocking_status': 'non_blocking',
        'current_activity': 'Stored 2805/2806 rows for KIS market-risk filter compute',
        'details': {
            'gate_status': 'ready',
            'severity': 'degraded',
            'blocking_status': 'non_blocking',
            'gate_reason': 'missing_rows_outside_active_feedback_scope',
            'rows_persisted': 2805,
            'expected_rows': 2806,
            'missing_count': 1,
            'missing_symbols_sample': ['176750'],
        },
    }

    with patch(
        'app.eod.pipeline_status.eod_pipeline_status_service.read',
        return_value=_pipeline_status(
            pykrx_stage=pykrx_stage,
            stage_overrides={'market_risk_filter': market_risk_stage},
        ),
    ):
        result = AgentFeedbackPrerequisiteService().inspect_feedback_prerequisites(
            market_date='2026-05-27',
            fs_div='CFS',
        )

    assert result['ready'] is True
    assert result['status'] == 'ready'
    assert 'market_risk_filter' not in result['blocked_stage_ids']
    stage = result['stages']['market_risk_filter']
    assert stage['gate_status'] == 'ready'
    assert stage['severity'] == 'degraded'
    assert stage['blocking_status'] == 'non_blocking'
    assert stage['gate_reason'] == 'missing_rows_outside_active_feedback_scope'
    assert stage['details']['missing_symbols_sample'] == ['176750']


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
            'reason': 'missing_required_pykrx_enrichment_category',
        }
    ]


def test_inspect_feedback_prerequisites_warns_on_pykrx_provider_unavailable_without_coverage_expectation():
    provider_error = 'PyKRX stock client is unavailable: Expecting value: line 16 column 3 (char 36)'
    categories = {
        category: {
            'category': category,
            'scope_type': 'market' if category in {'market_breadth', 'investor_flow_market'} else 'symbol',
            'rows_prepared': 0,
            'rows_persisted': 0,
            'errors': [] if category == 'market_breadth' else [{'market': 'KOSPI', 'error': provider_error}],
            'status': 'partial',
        }
        for category in PYKRX_DECIDER_ENRICHMENT_REQUIRED_CATEGORIES
    }
    pykrx_stage = {
        'task': 'pykrx_enrichment',
        'label': 'pykrx enrichment collect',
        'status': 'partial',
        'current_activity': 'Completed with errors pykrx enrichment collect',
        'details': {
            'result_summary': {
                'categories': categories,
            },
        },
    }

    with patch(
        'app.eod.pipeline_status.eod_pipeline_status_service.read',
        return_value=_pipeline_status(pykrx_stage=pykrx_stage),
    ):
        result = AgentFeedbackPrerequisiteService().inspect_feedback_prerequisites(
            market_date='2026-07-24',
            fs_div='CFS',
        )

    assert result['status'] == 'ready'
    assert result['ready'] is True
    assert result['blocked_stage_ids'] == []
    warning_categories = result['stages']['pykrx_enrichment']['details']['decider_warning_categories']
    assert len(warning_categories) == len(PYKRX_DECIDER_ENRICHMENT_REQUIRED_CATEGORIES)
    assert {
        item['reason']
        for item in warning_categories
    } == {'pykrx_provider_unavailable_optional_enrichment_degraded'}


def test_inspect_feedback_prerequisites_blocks_short_shells_without_decision_core():
    pykrx_stage = _pykrx_enrichment_stage(
        status='completed',
        category_overrides={
            'short_selling': {
                'present_rows': 2767,
                'expected_rows': 2767,
                'coverage_ratio': 1.0,
                'coverage_status': 'complete',
                'decision_usable_present_rows': 0,
                'decision_usable_ratio': 0.0,
                'decision_usable_status': 'missing',
                'complete_present_rows': 0,
                'completeness_ratio': 0.0,
                'completeness_status': 'missing',
            },
        },
    )

    with patch(
        'app.eod.pipeline_status.eod_pipeline_status_service.read',
        return_value=_pipeline_status(pykrx_stage=pykrx_stage),
    ):
        result = AgentFeedbackPrerequisiteService().inspect_feedback_prerequisites(
            market_date='2026-10-30',
            fs_div='CFS',
        )

    assert result['status'] == 'blocked'
    assert result['ready'] is False
    assert result['blocked_stage_ids'] == ['pykrx_enrichment']
    details = result['stages']['pykrx_enrichment']['details']
    assert details['decider_category_gate_status'] == 'blocked'
    assert [
        item['category']
        for item in details['decider_blocking_categories']
    ] == ['short_selling']
    short_gate = details['decider_blocking_categories'][0]
    assert short_gate['reason'] == (
        'missing_required_pykrx_enrichment_decision_evidence'
    )
    assert short_gate['decision_usable_rows'] == 0
    assert short_gate['missing_decision_usable_rows'] == 2767
    assert short_gate['missing_complete_rows'] == 2767


def test_inspect_feedback_prerequisites_warns_but_allows_short_core_without_balance():
    pykrx_stage = _pykrx_enrichment_stage(
        status='completed',
        category_overrides={
            'short_selling': {
                'present_rows': 2767,
                'expected_rows': 2767,
                'coverage_ratio': 1.0,
                'coverage_status': 'complete',
                'decision_usable_present_rows': 2767,
                'decision_usable_ratio': 1.0,
                'decision_usable_status': 'complete',
                'complete_present_rows': 0,
                'completeness_ratio': 0.0,
                'completeness_status': 'missing',
            },
        },
    )

    with patch(
        'app.eod.pipeline_status.eod_pipeline_status_service.read',
        return_value=_pipeline_status(pykrx_stage=pykrx_stage),
    ):
        result = AgentFeedbackPrerequisiteService().inspect_feedback_prerequisites(
            market_date='2026-10-30',
            fs_div='CFS',
        )

    assert result['status'] == 'ready'
    assert result['ready'] is True
    assert result['blocked_stage_ids'] == []
    details = result['stages']['pykrx_enrichment']['details']
    assert details['decider_category_gate_status'] == 'ready'
    assert details['decider_blocking_categories'] == []
    assert [
        item['category']
        for item in details['decider_warning_categories']
    ] == ['short_selling']


def test_inspect_feedback_prerequisites_blocks_low_partial_pykrx_enrichment_coverage():
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
    assert result['ready'] is False
    assert result['blocked_stage_ids'] == ['pykrx_enrichment']
    blocking_categories = result['stages']['pykrx_enrichment']['details']['decider_blocking_categories']
    assert blocking_categories[0]['category'] == 'short_selling'
    assert blocking_categories[0]['expected_rows'] == 3
    assert blocking_categories[0]['present_rows'] == 2
    assert blocking_categories[0]['missing_rows'] == 1
    assert blocking_categories[0]['reason'] == 'incomplete_required_pykrx_enrichment_category'


def test_inspect_feedback_prerequisites_allows_pykrx_enrichment_above_minimum_coverage():
    pykrx_stage = _pykrx_enrichment_stage(
        status='completed',
        category_overrides={
            'investor_flow_symbol': {
                'present_rows': 2780,
                'expected_rows': 2808,
                'coverage_ratio': 0.990028,
                'coverage_status': 'partial',
            },
            'valuation_snapshot': {
                'present_rows': 2780,
                'expected_rows': 2808,
                'coverage_ratio': 0.990028,
                'coverage_status': 'partial',
            },
        },
    )

    with patch(
        'app.eod.pipeline_status.eod_pipeline_status_service.read',
        return_value=_pipeline_status(pykrx_stage=pykrx_stage),
    ):
        result = AgentFeedbackPrerequisiteService().inspect_feedback_prerequisites(
            market_date='2026-05-29',
            fs_div='CFS',
        )

    assert result['status'] == 'ready'
    assert result['ready'] is True
    assert result['blocked_stage_ids'] == []
    warning_categories = result['stages']['pykrx_enrichment']['details']['decider_warning_categories']
    assert [item['category'] for item in warning_categories] == ['investor_flow_symbol', 'valuation_snapshot']


def test_pykrx_enrichment_uses_lower_valuation_coverage_floor_only_for_valuation():
    category_payloads = _pykrx_enrichment_stage(
        status='completed',
        category_overrides={
            'investor_flow_symbol': {
                'present_rows': 2734,
                'expected_rows': 2762,
                'coverage_ratio': 0.989862,
                'coverage_status': 'partial',
            },
            'valuation_snapshot': {
                'present_rows': 2714,
                'expected_rows': 2762,
                'coverage_ratio': 0.982621,
                'coverage_status': 'partial',
            },
        },
    )['details']['categories']

    details = AgentFeedbackPrerequisiteService.build_pykrx_enrichment_gate_details(
        category_payloads=category_payloads,
    )

    gates = {
        item['category']: item
        for item in details['decider_category_gates']
    }
    assert gates['valuation_snapshot']['gate_status'] == 'ready'
    assert gates['valuation_snapshot']['reason'] == 'required_pykrx_enrichment_category_near_complete'
    assert gates['investor_flow_symbol']['gate_status'] == 'blocked'
    assert gates['investor_flow_symbol']['reason'] == 'incomplete_required_pykrx_enrichment_category'


def test_pykrx_category_row_counts_block_when_price_bar_universe_is_partial():
    stage = {
        'task': 'pykrx_enrichment',
        'status': 'completed',
        'details': {
            'category_row_counts': {
                'sector_classification': 400,
                'market_breadth': 2,
                'investor_flow_market': 2,
                'investor_flow_symbol': 400,
                'short_selling': 400,
                'valuation_snapshot': 400,
            },
        },
    }
    pipeline_status = _pipeline_status(pykrx_stage=stage)
    pipeline_status['stages']['price_collection'] = _price_collection_stage(
        rows_persisted=3547,
    )

    with patch(
        'app.eod.pipeline_status.eod_pipeline_status_service.read',
        return_value=pipeline_status,
    ):
        result = AgentFeedbackPrerequisiteService().inspect_feedback_prerequisites(
            market_date='2026-09-14',
            fs_div='CFS',
        )

    assert result['status'] == 'blocked'
    assert result['ready'] is False
    assert result['blocked_stage_ids'] == ['pykrx_enrichment']
    details = result['stages']['pykrx_enrichment']['details']
    assert details['decider_category_gate_status'] == 'blocked'
    blocking_by_category = {
        item['category']: item
        for item in details['decider_blocking_categories']
    }
    assert set(blocking_by_category) == {
        'sector_classification',
        'investor_flow_symbol',
        'short_selling',
        'valuation_snapshot',
    }
    sector_gate = blocking_by_category['sector_classification']
    assert sector_gate['coverage_status'] == 'partial'
    assert sector_gate['expected_rows'] == 3547
    assert sector_gate['present_rows'] == 400
    assert sector_gate['missing_rows'] == 3147
    assert sector_gate['coverage_ratio'] == round(400 / 3547, 6)
    assert sector_gate['reason'] == 'incomplete_required_pykrx_enrichment_category'


def test_pykrx_category_row_counts_are_ready_at_full_price_bar_coverage():
    stage = {
        'task': 'pykrx_enrichment',
        'status': 'completed',
        'details': {
            'category_row_counts': {
                'sector_classification': 3547,
                'market_breadth': 2,
                'investor_flow_market': 2,
                'investor_flow_symbol': 3547,
                'short_selling': 3547,
                'valuation_snapshot': 3547,
            },
        },
    }
    pipeline_status = _pipeline_status(pykrx_stage=stage)
    pipeline_status['stages']['price_collection'] = _price_collection_stage(
        rows_persisted=3547,
    )

    with patch(
        'app.eod.pipeline_status.eod_pipeline_status_service.read',
        return_value=pipeline_status,
    ):
        result = AgentFeedbackPrerequisiteService().inspect_feedback_prerequisites(
            market_date='2026-09-14',
            fs_div='CFS',
        )

    assert result['status'] == 'ready'
    assert result['ready'] is True
    assert result['blocked_stage_ids'] == []
    details = result['stages']['pykrx_enrichment']['details']
    assert details['decider_category_gate_status'] == 'ready'
    assert details['decider_blocking_categories'] == []
    assert details['decider_waiting_categories'] == []
    gates = {
        item['category']: item
        for item in details['decider_category_gates']
    }
    assert gates['sector_classification']['expected_rows'] == 3547
    assert gates['sector_classification']['coverage_status'] == 'complete'
    assert gates['market_breadth']['expected_rows'] == 2
    assert gates['market_breadth']['coverage_status'] == 'complete'


def test_pykrx_prerequisite_preserves_existing_gate_details():
    stage = {
        'task': 'pykrx_enrichment',
        'status': 'partial',
        'details': {
            'decider_category_gate_status': 'ready',
            'decider_category_gates': [{'category': 'sector_classification'}],
        },
    }

    augmented = AgentFeedbackPrerequisiteService._augment_pykrx_enrichment_prerequisite_stage(stage)

    assert augmented['details']['decider_category_gate_status'] == 'ready'
    assert augmented['details']['decider_category_gates'] == [{'category': 'sector_classification'}]
