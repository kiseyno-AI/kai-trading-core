import asyncio
from unittest.mock import AsyncMock, MagicMock

from app.eod.daily_pipeline_tracking import (
    _market_risk_filter_coverage_gate,
    run_tracked_market_risk_filter,
)


def test_market_risk_filter_coverage_gate_blocks_collapsed_requested_scope() -> None:
    result = {
        'requested_symbols': [f'{index:06d}' for index in range(1, 81)],
        'results_persisted': 1,
        'missing_symbols': [f'{index:06d}' for index in range(2, 81)],
    }

    gate = _market_risk_filter_coverage_gate(result)

    assert gate['gate_status'] == 'blocked'
    assert gate['gate_reason'] == 'market_risk_filter_coverage_below_minimum'
    assert gate['requested_symbol_count'] == 80
    assert gate['results_persisted'] == 1


def test_market_risk_filter_coverage_gate_allows_high_requested_scope_coverage() -> None:
    result = {
        'requested_symbols': [f'{index:06d}' for index in range(1, 81)],
        'results_persisted': 79,
        'missing_symbols': ['000080'],
    }

    gate = _market_risk_filter_coverage_gate(result)

    assert gate['gate_status'] == 'ready'
    assert gate['blocking_status'] == 'non_blocking'


def test_direct_market_risk_stage_does_not_run_or_finish_deduplicated_admission() -> None:
    run_state_service = MagicMock()
    run_state_service.start_run.return_value = {
        'id': 744,
        'status': 'running',
        'deduplicated': True,
        'deduplication_reason': 'overlapping_run_already_active',
    }
    filter_service = MagicMock()
    filter_service.rebuild_for_market_date = AsyncMock()

    result = asyncio.run(
        run_tracked_market_risk_filter(
            market_date='2026-08-28',
            symbols=['005930'],
            trigger='manual_api',
            entrypoint='test_direct_stage',
            filter_service=filter_service,
            run_state_service=run_state_service,
        )
    )

    assert result['triggered'] is False
    assert result['pipeline_run_id'] == 744
    filter_service.rebuild_for_market_date.assert_not_awaited()
    run_state_service.start_stage.assert_not_called()
    run_state_service.finish_run.assert_not_called()
