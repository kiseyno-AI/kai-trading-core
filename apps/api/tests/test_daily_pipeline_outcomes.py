from __future__ import annotations

from copy import deepcopy
from unittest.mock import patch

import pytest

from app.eod.daily_pipeline_outcomes import (
    daily_pipeline_outcome_is_satisfied,
    daily_pipeline_prefix_outcome_is_satisfied,
    terminal_continuation_feedback_lease_loss_run_id,
)
from app.eod.pykrx_backfill import PykrxBackfillService


def _partial_outcome(
    *,
    succeeded: int,
    total: int,
    details: dict | None = None,
) -> dict:
    return {
        'stage': 'momentum_factors',
        'status': 'partial',
        'succeeded': succeeded,
        'total': total,
        'details': details or {},
    }


def _ready_enrichment_warning_outcome() -> dict:
    categories = {
        'sector_classification': {
            'status': 'completed',
            'rows_persisted': 10,
            'errors': [],
        },
        'investor_flow_symbol': {
            'status': 'completed',
            'rows_persisted': 10,
            'errors': [],
        },
        'short_selling': {
            'status': 'partial',
            'rows_persisted': 10,
            'errors': ['empty provider frame'],
        },
    }
    return {
        'stage': 'pykrx_enrichment',
        'status': 'completed',
        'processed': 3,
        'total': 3,
        'succeeded': 2,
        'failed': 1,
        'remaining': 0,
        'completed_at': '2026-04-03T08:00:00+00:00',
        'error_message': None,
        'details': {
            'result_summary': {
                'status': 'completed',
                'point_in_time_recovery': True,
                'categories_requested': list(categories),
                'categories': categories,
                'downstream_preflight': {
                    'allowed': True,
                    'status': 'ready',
                    'required_categories': list(categories),
                    'category_row_counts': {
                        category: 10 for category in categories
                    },
                    'failed_categories': [],
                    'blocked_categories': [],
                    'waiting_categories': [],
                    'zero_coverage_categories': [],
                },
            }
        },
    }


@pytest.mark.parametrize('malformed_resume_count', ['0', 'corrupt', True])
def test_feedback_lease_loss_reclaim_rejects_malformed_resume_count(
    malformed_resume_count: object,
) -> None:
    child_run_id = 'feedback-cycle-2026-09-18-deadbeef01'
    error = (
        f'feedback retry lease ownership lost for {child_run_id} at '
        'before_parent_finalization: heartbeat_refresh_grace_exceeded'
    )
    pipeline_run = {
        'status': 'failed',
        'current_stage': 'signal_decider',
        'market_date': '2026-09-18',
        'completed_at': '2026-09-20T01:01:00+00:00',
        'error_message': error,
        'run_metadata': {
            'terminal_continuation_count': 1,
            'terminal_continuation_version': 1,
            'terminal_continuation_reason': (
                'summary_knowledge_cutoff_repair'
            ),
            'resume_from_stage': 'agent_feedback_summaries',
            'terminal_continuation_claimed_at': (
                '2026-09-20T01:00:00+00:00'
            ),
            'terminal_continuation_evidence': {},
            'terminal_continuation_resume_count': malformed_resume_count,
        },
    }
    stage = {
        'stage_id': 'signal_decider',
        'status': 'failed',
        'processed': 20,
        'total': 160,
        'succeeded': 0,
        'failed': 20,
        'remaining': 140,
        'current_step': 'signal_decider_failed',
        'completed_at': '2026-09-20T01:01:00+00:00',
        'error_message': error,
        'source_run_ids': {'agent_feedback_run_id': child_run_id},
    }

    assert terminal_continuation_feedback_lease_loss_run_id(
        pipeline_run=pipeline_run,
        stage=stage,
    ) is None


def test_terminal_high_coverage_partial_is_satisfied() -> None:
    outcome = _partial_outcome(
        succeeded=999,
        total=1_000,
        details={'remaining_retryable_events': 0},
    )

    assert daily_pipeline_outcome_is_satisfied(outcome) is True


def test_ready_enrichment_warning_is_only_satisfied_as_prefix_evidence(
) -> None:
    outcome = _ready_enrichment_warning_outcome()

    assert daily_pipeline_outcome_is_satisfied(outcome) is False
    assert daily_pipeline_prefix_outcome_is_satisfied(outcome) is True


@pytest.mark.parametrize(
    ('path', 'value'),
    [
        (('downstream_preflight', 'allowed'), False),
        (('downstream_preflight', 'blocked_categories'), ['short_selling']),
        (
            ('downstream_preflight', 'category_row_counts'),
            {
                'sector_classification': 10,
                'investor_flow_symbol': 10,
                'short_selling': 0,
            },
        ),
        (('downstream_preflight', 'required_categories'), []),
        (('outcome', 'error_message'), 'provider failed'),
    ],
)
def test_enrichment_warning_prefix_evidence_fails_closed(
    path: tuple[str, str],
    value: object,
) -> None:
    outcome = deepcopy(_ready_enrichment_warning_outcome())
    if path[0] == 'outcome':
        outcome[path[1]] = value
    else:
        outcome['details']['result_summary'][path[0]][path[1]] = value

    assert daily_pipeline_prefix_outcome_is_satisfied(outcome) is False


@pytest.mark.parametrize(
    'outcome',
    [
        _partial_outcome(
            succeeded=989,
            total=1_000,
            details={'remaining_retryable_events': 0},
        ),
        _partial_outcome(
            succeeded=999,
            total=1_000,
            details={
                'retry_cycles_requested': 3,
                'retry_cycles_completed': 1,
                'remaining_retryable_events': 1,
            },
        ),
        _partial_outcome(
            succeeded=999,
            total=1_000,
            details={'blocking_stages': ['fundamentals_daily_refresh']},
        ),
        {'status': 'partial'},
    ],
)
def test_unproven_or_retryable_partial_fails_closed(outcome: dict) -> None:
    assert daily_pipeline_outcome_is_satisfied(outcome) is False


def test_explicit_ready_partial_is_satisfied() -> None:
    outcome = {
        'stage': 'market_risk_filter',
        'status': 'partial',
        'details': {
            'gate_status': 'ready',
            'blocking_status': 'non_blocking',
        },
    }

    assert daily_pipeline_outcome_is_satisfied(outcome) is True


def test_conflicting_blocked_gate_overrides_ready_partial() -> None:
    outcome = {
        'stage': 'market_risk_filter',
        'status': 'partial',
        'gate_status': 'ready',
        'details': {'gate_status': 'blocked'},
    }

    assert daily_pipeline_outcome_is_satisfied(outcome) is False


@pytest.mark.parametrize(
    'outcome',
    [
        {
            'stage': 'market_risk_filter',
            'status': 'completed',
            'details': {'gate_status': 'blocked'},
        },
        {
            'stage': 'technical_snapshots',
            'status': 'completed',
            'snapshots_persisted': 100,
            'snapshots_failed': 1,
        },
        {
            'stage': 'momentum_factors',
            'status': 'completed',
            'details': {'remaining_retryable_events': 1},
        },
    ],
)
def test_completed_outcome_with_conflicting_failure_evidence_fails_closed(
    outcome: dict,
) -> None:
    assert daily_pipeline_outcome_is_satisfied(outcome) is False


def test_grouped_partial_requires_every_reported_coverage_family() -> None:
    outcome = {
        'stage': 'technical_snapshots',
        'status': 'partial',
        'result': {
            'snapshots_persisted': 100,
            'snapshots_failed': 0,
            'contexts_persisted': 98,
            'contexts_failed': 2,
            'remaining_retryable_events': 0,
        },
    }

    assert daily_pipeline_outcome_is_satisfied(outcome) is False


def test_fundamentals_partial_caused_only_by_separate_piotroski_is_satisfied() -> None:
    outcome = {
        'stage': 'fundamentals_daily_refresh',
        'status': 'partial',
        'result': {
            'status': 'partial',
            'symbols_failed': 0,
            'events_failed': 0,
            'remaining_retryable_events': 0,
            'partial_reasons': ['piotroski_filter_followup_failed'],
        },
    }

    assert daily_pipeline_outcome_is_satisfied(outcome) is True


def test_feedback_partial_is_not_satisfied_by_generic_coverage() -> None:
    outcome = {
        'stage': 'agent_feedback_summaries',
        'status': 'partial',
        'succeeded': 999,
        'total': 1_000,
        'details': {'remaining_retryable_events': 0},
    }

    assert daily_pipeline_outcome_is_satisfied(outcome) is False


def test_factor_group_partial_requires_every_nested_stage_to_be_satisfied() -> None:
    outcome = {
        'status': 'partial',
        'stages': {
            'momentum_factors': {
                'status': 'partial',
                'result': {
                    'results_persisted': 999,
                    'results_failed': 1,
                },
            },
            'value_factors': {'status': 'completed', 'result': {}},
        },
    }

    assert daily_pipeline_outcome_is_satisfied(outcome) is True

    outcome['stages']['momentum_factors']['result'] = {
        'results_persisted': 989,
        'results_failed': 11,
    }
    assert daily_pipeline_outcome_is_satisfied(outcome) is False


def test_ready_aggregate_gate_cannot_hide_failed_nested_stage() -> None:
    outcome = {
        'status': 'partial',
        'gate_status': 'ready',
        'stages': {
            'momentum_factors': {'status': 'completed'},
            'value_factors': {'status': 'failed'},
        },
    }

    assert daily_pipeline_outcome_is_satisfied(outcome) is False


def test_unknown_or_conflicting_skip_reason_fails_closed() -> None:
    assert daily_pipeline_outcome_is_satisfied(
        {
            'status': 'skipped',
            'reason': 'already_completed',
            'details': {'result_summary': {'reason': 'provider_unavailable'}},
        }
    ) is False


def test_expired_next_open_window_is_a_terminal_safe_skip() -> None:
    assert daily_pipeline_outcome_is_satisfied(
        {
            'status': 'skipped',
            'reason': 'next_open_window_expired',
        }
    ) is True


def test_safe_partial_recovery_clears_retry_scope_without_losing_partial_audit(
) -> None:
    latest_eod_run = {
        'id': 7,
        'trade_date': '2026-04-03',
        'status': 'completed_with_errors',
        'symbols_total': 1,
        'symbols_succeeded': 1,
        'symbols_failed': 0,
        'run_metadata': {
            'requested_symbols': ['005930'],
            'failed_symbols': [],
            'unattempted_symbols': [],
            'downstream_retry_symbols': ['005930'],
            'downstream_retry_resume_from_stage': 'momentum_factors',
        },
    }
    recovery_result = {
        'status': 'partial',
        'pipeline_run_id': 92,
        'stages': {
            'momentum_factors': {
                'status': 'partial',
                'result': {
                    'results_persisted': 99,
                    'results_failed': 1,
                    'remaining_retryable_events': 0,
                },
            },
            'signal_features': {
                'status': 'completed',
                'result': {},
            },
        },
    }

    with (
        patch(
            'app.eod.pykrx_backfill.count_eod_run_persisted_symbols',
            return_value=1,
        ),
        patch(
            'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
            return_value=['005930'],
        ),
        patch(
            'app.eod.pykrx_backfill.complete_eod_collection_run'
        ) as complete_run,
    ):
        result = PykrxBackfillService()._persist_successful_followup_recovery(
            latest_eod_run=latest_eod_run,
            recovery_result=recovery_result,
        )

    assert result['updated'] is True
    assert result['status'] == 'completed'
    completion_metadata = complete_run.call_args.kwargs['run_metadata']
    assert completion_metadata['downstream_retry_symbols'] == []
    assert completion_metadata['downstream_recovery_pipeline_run_id'] == 92
