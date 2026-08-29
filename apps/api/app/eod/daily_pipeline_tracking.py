from __future__ import annotations

from typing import Any, Sequence

from app.eod.daily_pipeline_state import daily_pipeline_run_state_service
from app.eod.daily_pipeline_stages import DailyPipelineStageService, STAGE_DEFINITIONS
from app.signals.market_risk_filter import (
    MARKET_RISK_FILTER_SOURCE,
    daily_market_risk_filter_service,
)
from app.signals.market_risk_snapshot import DEFAULT_MARKET_CODE

MIN_BLOCKING_MARKET_RISK_COVERAGE_RATIO = 0.5


def _market_risk_filter_coverage_gate(result: dict[str, Any]) -> dict[str, Any]:
    requested_symbols = list(result.get('requested_symbols') or [])
    requested_count = len(requested_symbols)
    if requested_count <= 1:
        return {
            'gate_status': 'ready',
            'coverage_ratio': 1.0,
            'requested_symbol_count': requested_count,
            'results_persisted': int(result.get('results_persisted') or 0),
            'missing_count': len(result.get('missing_symbols') or []),
        }
    results_persisted = int(result.get('results_persisted') or 0)
    coverage_ratio = min(results_persisted, requested_count) / requested_count
    missing_symbols = list(result.get('missing_symbols') or [])
    blocking = coverage_ratio < MIN_BLOCKING_MARKET_RISK_COVERAGE_RATIO
    return {
        'gate_status': 'blocked' if blocking else 'ready',
        'blocking_status': 'blocking' if blocking else 'non_blocking',
        'severity': 'blocking' if blocking else 'degraded',
        'gate_reason': (
            'market_risk_filter_coverage_below_minimum'
            if blocking
            else 'market_risk_filter_coverage_above_minimum'
        ),
        'coverage_ratio': round(coverage_ratio, 6),
        'minimum_coverage_ratio': MIN_BLOCKING_MARKET_RISK_COVERAGE_RATIO,
        'requested_symbol_count': requested_count,
        'results_persisted': results_persisted,
        'missing_count': max(requested_count - results_persisted, len(missing_symbols)),
        'missing_symbols_sample': missing_symbols[:20],
    }


async def run_tracked_market_risk_filter(
    *,
    market_date: str,
    symbols: Sequence[str] | None = None,
    source: str = MARKET_RISK_FILTER_SOURCE,
    market_code: str = DEFAULT_MARKET_CODE,
    trigger: str,
    entrypoint: str,
    filter_service=None,
    run_state_service=None,
) -> dict[str, Any]:
    stage_id = 'market_risk_filter'
    definition = STAGE_DEFINITIONS[stage_id]
    normalized_symbols = list(symbols or [])
    resolved_filter_service = filter_service or daily_market_risk_filter_service
    resolved_run_state_service = run_state_service or daily_pipeline_run_state_service
    pipeline_run = resolved_run_state_service.start_run(
        market_date=market_date,
        trigger=trigger,
        requested_stages=[stage_id],
        resolved_stages=[stage_id],
        run_metadata={
            'entrypoint': entrypoint,
            'symbols': normalized_symbols or None,
            'source': source,
            'market_code': market_code,
            'resume_from_stage': stage_id,
        },
    )
    pipeline_run_id = int(pipeline_run['id'])
    if pipeline_run.get('deduplicated'):
        return {
            'triggered': False,
            'reason': pipeline_run.get('deduplication_reason') or 'overlapping_run_already_active',
            'pipeline_run_id': pipeline_run_id,
            'pipeline_run': pipeline_run,
        }
    resolved_run_state_service.start_stage(
        pipeline_run_id=pipeline_run_id,
        stage_id=stage_id,
        market_date=market_date,
        current_step='collecting_market_risk_snapshots_and_filters',
        current_activity='Collecting daily KIS risk snapshots and evaluating market risk filter',
        processed=0,
        total=len(normalized_symbols),
        succeeded=0,
        failed=0,
        details={
            'group': definition.group,
            'depends_on': list(definition.depends_on),
            'output_tables': list(definition.output_tables),
            'inspect_routes': list(definition.inspect_routes),
        },
    )

    def _progress(**payload: Any) -> None:
        resolved_run_state_service.update_stage(
            pipeline_run_id=pipeline_run_id,
            stage_id=stage_id,
            market_date=market_date,
            status=str(payload.get('status') or 'running'),
            current_step=payload.get('current_step'),
            current_activity=payload.get('current_activity'),
            processed=payload.get('processed'),
            total=payload.get('total'),
            succeeded=payload.get('succeeded'),
            failed=payload.get('failed'),
            remaining=payload.get('remaining'),
            current_symbol=payload.get('current_symbol'),
            current_period=payload.get('current_period'),
            error_message=payload.get('error_message'),
            source_run_ids=payload.get('source_run_ids'),
            details=payload.get('details'),
        )

    try:
        result = await resolved_filter_service.rebuild_for_market_date(
            market_date=market_date,
            symbols=normalized_symbols or None,
            source=source,
            market_code=market_code,
            progress_callback=_progress,
        )
        coverage_gate = _market_risk_filter_coverage_gate(result)
        status = DailyPipelineStageService._resolve_result_status(result)
        if coverage_gate['gate_status'] == 'blocked' and status == 'completed':
            status = 'partial'
        counts = DailyPipelineStageService._extract_stage_counts(stage_id=stage_id, result=result)
        resolved_run_state_service.finish_stage(
            pipeline_run_id=pipeline_run_id,
            stage_id=stage_id,
            market_date=market_date,
            status=status,
            processed=counts['processed'],
            total=counts['total'],
            succeeded=counts['succeeded'],
            failed=counts['failed'],
            current_step=f'{stage_id}_{status}',
            current_activity=f'{DailyPipelineStageService._status_activity_prefix(status)} {definition.label}',
            error_message=str(result.get('error') or result.get('error_message') or '') or None,
            source_run_ids=DailyPipelineStageService._extract_stage_source_run_ids(result),
            details={
                **DailyPipelineStageService._build_stage_run_details(stage_id=stage_id, result=result),
                **coverage_gate,
            },
        )
        final_run = resolved_run_state_service.finish_run(
            pipeline_run_id=pipeline_run_id,
            status=status,
            run_metadata={'stage_statuses': {stage_id: status}},
        )
        return {
            **result,
            'pipeline_run_id': pipeline_run_id,
            'pipeline_run': final_run,
        }
    except Exception as exc:
        resolved_run_state_service.finish_stage(
            pipeline_run_id=pipeline_run_id,
            stage_id=stage_id,
            market_date=market_date,
            status='failed',
            current_step='market_risk_filter_failed',
            current_activity='Daily KIS market risk filter failed',
            error_message=str(exc),
        )
        resolved_run_state_service.finish_run(
            pipeline_run_id=pipeline_run_id,
            status='failed',
            error_message=str(exc),
        )
        raise
