from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.eod.daily_pipeline_state import (
    DailyPipelineRunStateService,
    _build_counts,
)
from app.eod.daily_pipeline_stages import DailyPipelineStageService
from app.eod.pykrx_pipeline_runner import _extract_result_counts


def test_build_counts_repairs_out_of_order_and_negative_values() -> None:
    assert _build_counts(
        processed=7,
        total=5,
        succeeded=12,
        failed=-4,
        remaining=-10,
    ) == {
        'processed': 12,
        'total': 12,
        'succeeded': 12,
        'failed': 0,
        'remaining': 0,
    }


def test_build_counts_preserves_combined_outcome_and_total_invariants() -> None:
    assert _build_counts(
        processed=7,
        total=7,
        succeeded=6,
        failed=6,
        remaining=4,
    ) == {
        'processed': 12,
        'total': 12,
        'succeeded': 6,
        'failed': 6,
        'remaining': 0,
    }


def test_terminal_result_counts_overwrite_row_and_record_mismatch() -> None:
    now = datetime(2026, 10, 30, 17, 0, tzinfo=timezone.utc)
    run = SimpleNamespace(
        id=598,
        status='running',
        current_stage='external_news_collect',
        updated_at=now - timedelta(minutes=1),
    )
    stage = SimpleNamespace(
        id=1_598,
        pipeline_run_id=598,
        market_date=date(2026, 10, 30),
        stage_id='external_news_collect',
        status='running',
        processed=0,
        total=201,
        succeeded=0,
        failed=0,
        remaining=201,
        current_step='external_news_collect_running',
        current_activity='Running external news collection',
        current_symbol=None,
        current_period=None,
        started_at=now - timedelta(minutes=5),
        updated_at=now - timedelta(minutes=1),
        completed_at=None,
        error_message=None,
        source_run_ids={},
        details={},
    )
    run_result = MagicMock()
    run_result.scalar_one_or_none.return_value = run
    stage_result = MagicMock()
    stage_result.scalar_one_or_none.return_value = stage
    session = MagicMock()
    session.execute.side_effect = [run_result, stage_result]
    session_context = MagicMock()
    session_context.__enter__.return_value = session

    with (
        patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ),
        patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
    ):
        result = DailyPipelineRunStateService().update_stage(
            pipeline_run_id=598,
            stage_id='external_news_collect',
            market_date='2026-10-30',
            status='completed',
            processed=201,
            total=201,
            succeeded=118,
            failed=0,
            details={
                'terminal_count_authority': 'result',
                'result_summary': {'status': 'ok'},
            },
        )

    assert result['processed'] == 201
    assert result['total'] == 201
    assert result['succeeded'] == 118
    assert result['failed'] == 0
    assert result['remaining'] == 0
    assert result['details']['terminal_count_reconciliation'] == {
        'schema_version': 'daily_pipeline_terminal_count_reconciliation_v1',
        'authority': 'result',
        'mismatch_fields': ['processed', 'succeeded', 'remaining'],
        'previous': {
            'processed': 0,
            'total': 201,
            'succeeded': 0,
            'failed': 0,
            'remaining': 201,
        },
        'result': {
            'processed': 201,
            'total': 201,
            'succeeded': 118,
            'failed': 0,
            'remaining': 0,
        },
    }


def test_fundamentals_terminal_counts_prefer_actual_captured_events() -> None:
    counts = _extract_result_counts(
        {
            'status': 'completed',
            'symbols_refreshed': 0,
            'symbols_failed': 0,
            'events_detected': 2_780,
            'events_captured': 2_780,
            'events_failed': 0,
        },
        stage_id='fundamentals_daily_refresh',
    )

    assert counts == {
        'processed': 2_780,
        'total': 2_780,
        'succeeded': 2_780,
        'failed': 0,
    }


def test_news_terminal_counts_do_not_treat_stale_symbols_as_success() -> None:
    counts = _extract_result_counts(
        {
            'status': 'completed',
            'summary': {
                'eligible_symbol_count': 201,
                'collected_symbol_count': 201,
                'failed_symbol_count': 0,
                'skipped_symbol_count': 0,
                'recent_news_symbol_count': 118,
                'stale_news_symbol_count': 83,
                'no_result_symbol_count': 0,
            },
        },
        stage_id='external_news_collect',
    )

    assert counts == {
        'processed': 201,
        'total': 201,
        'succeeded': 118,
        'failed': 0,
    }


def test_result_count_extraction_repairs_success_above_reported_total() -> None:
    counts = _extract_result_counts(
        {
            'status': 'completed',
            'features_persisted': 9,
            'features_failed': 0,
            'items_total': 3,
        },
        stage_id='signal_features',
    )

    assert counts == {
        'processed': 9,
        'total': 9,
        'succeeded': 9,
        'failed': 0,
    }


def test_stage_orchestrator_uses_fundamentals_event_counts() -> None:
    counts = DailyPipelineStageService._extract_stage_counts(
        stage_id='fundamentals_daily_refresh',
        result={
            'status': 'completed',
            'symbols_refreshed': 0,
            'symbols_failed': 0,
            'events_detected': 2_780,
            'events_captured': 2_780,
            'events_failed': 0,
        },
    )

    assert counts == {
        'processed': 2_780,
        'total': 2_780,
        'succeeded': 2_780,
        'failed': 0,
    }


def test_stage_orchestrator_news_counts_use_recent_success_only() -> None:
    counts = DailyPipelineStageService._extract_stage_counts(
        stage_id='external_news_collect',
        result={
            'status': 'completed',
            'summary': {
                'eligible_symbol_count': 201,
                'collected_symbol_count': 201,
                'failed_symbol_count': 0,
                'skipped_symbol_count': 0,
                'recent_news_symbol_count': 118,
                'stale_news_symbol_count': 83,
                'no_result_symbol_count': 0,
            },
        },
    )

    assert counts == {
        'processed': 201,
        'total': 201,
        'succeeded': 118,
        'failed': 0,
    }


def test_stage_result_details_mark_result_counts_as_terminal_authority() -> None:
    details = DailyPipelineStageService._build_stage_run_details(
        stage_id='external_news_collect',
        result={'status': 'completed', 'summary': {'eligible_symbol_count': 0}},
    )

    assert details['terminal_count_authority'] == 'result'
