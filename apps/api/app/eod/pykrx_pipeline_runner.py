from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Sequence

from app.eod.pykrx_enrichment import DEFAULT_PYKRX_ENRICHMENT_CATEGORIES
from app.eod.pykrx_enrichment_gate import (
    REQUIRED_EXEC_DECIDER_PYKRX_ENRICHMENT_CATEGORIES,
    build_pykrx_enrichment_preflight,
)
from app.eod.pykrx_pipeline_support import (
    append_stage_error,
    build_stage_failure_result,
    make_stage_progress_callback,
)
from app.signals.market_risk_filter import MARKET_RISK_FILTER_SOURCE

logger = logging.getLogger(__name__)

PYKRX_DAILY_STAGE_ORDER: tuple[str, ...] = (
    'price_collection',
    'technical_snapshots',
    'action_summaries',
    'trending_ranks',
    'signal_features',
    'market_risk_filter',
)

FACTOR_FOLLOWUP_STAGE_ORDER: tuple[str, ...] = (
    'momentum_factors',
    'value_factors',
    'low_risk_factors',
    'beta_factors',
    'advanced_value_factors',
    'unified_value_factors',
    'quality_factors',
    'composite_factors',
)
AUTO_DAILY_FOLLOWUP_STAGE_ORDER: tuple[str, ...] = (
    'fundamentals_daily_refresh',
    *FACTOR_FOLLOWUP_STAGE_ORDER,
    'external_news_collect',
    'agent_feedback_summaries',
    'signal_decider',
    'signal_decider_evaluations',
)
AUTO_DAILY_TRACKED_STAGE_ORDER: tuple[str, ...] = (
    'price_collection',
    'pykrx_enrichment',
    'technical_snapshots',
    'action_summaries',
    'trending_ranks',
    'signal_features',
    'market_risk_filter',
    *AUTO_DAILY_FOLLOWUP_STAGE_ORDER,
)
MAX_NON_BLOCKING_STAGE_MISSING_RATIO = 0.10
PYKRX_PROVIDER_OUTAGE_EARLY_ABORT_FAILURES = 20


@dataclass(frozen=True)
class PykrxPipelineDependencies:
    log_fetch: Callable[..., None]
    create_eod_collection_run: Callable[..., int]
    update_eod_collection_run_progress: Callable[..., None]
    complete_eod_collection_run: Callable[..., None]
    persist_daily_price_bars: Callable[..., int]
    get_market_ohlcv_by_date: Callable[..., Any]
    get_market_ohlcv_by_ticker: Callable[..., Any] | None
    price_context_service: Any
    technical_snapshot_service: Any
    action_summary_service: Any
    compute_trending_ranks: Callable[..., dict]
    pykrx_enrichment_service: Any
    signal_feature_service: Any
    market_risk_filter_service: Any
    dart_fundamentals_capture_service: Any
    settings: Any
    normalize_yyyymmdd: Callable[..., str]
    update_run_stage_metadata: Callable[..., dict]
    daily_pipeline_run_state_service: Any | None = None


def _normalize_stage_status(value: Any) -> str:
    status = str(value or '').strip().lower()
    if status == 'ok':
        return 'completed'
    if status in {'completed_with_errors', 'warning'}:
        return 'partial'
    if status in {'disabled', 'not_configured'}:
        return 'skipped'
    return status or 'completed'


def _status_activity_prefix(status: str) -> str:
    if status == 'completed':
        return 'Completed'
    if status == 'partial':
        return 'Completed with errors'
    if status == 'failed':
        return 'Failed'
    if status == 'waiting':
        return 'Waiting on'
    if status == 'blocked':
        return 'Blocked'
    if status == 'skipped':
        return 'Skipped'
    return status.replace('_', ' ').capitalize()


def _missing_ratio(*, failed: int, total: int) -> float | None:
    if total <= 0:
        return None
    return max(failed, 0) / total


def _is_non_blocking_gap(*, failed: int, total: int) -> bool:
    ratio = _missing_ratio(failed=failed, total=total)
    return failed > 0 and ratio is not None and ratio < MAX_NON_BLOCKING_STAGE_MISSING_RATIO


def _completion_status_for_gap(*, failed: int, total: int) -> str:
    if failed <= 0:
        return 'completed'
    return 'completed' if _is_non_blocking_gap(failed=failed, total=total) else 'partial'


def _gap_policy_details(*, failed: int, total: int) -> dict[str, Any]:
    ratio = _missing_ratio(failed=failed, total=total)
    return {
        'max_non_blocking_missing_ratio': MAX_NON_BLOCKING_STAGE_MISSING_RATIO,
        'missing_ratio': round(ratio, 6) if ratio is not None else None,
        'minor_missing_gap_treated_as_success': _is_non_blocking_gap(failed=failed, total=total),
    }


def _resolve_result_status(result: dict[str, Any]) -> str:
    if result.get('triggered') is False:
        if _is_already_completed_followup_result(result):
            return 'completed'
        status = _normalize_stage_status(result.get('status'))
        return status if status in {'waiting', 'blocked', 'failed', 'partial'} else 'skipped'
    status = _normalize_stage_status(result.get('status'))
    if status != 'completed' or result.get('status'):
        return status
    failed_keys = (
        'symbols_failed',
        'events_failed',
        'results_failed',
        'features_failed',
        'snapshots_failed',
        'action_summaries_failed',
        'failed_count',
        'errors_count',
    )
    if any(int(result.get(key) or 0) > 0 for key in failed_keys):
        return 'partial'
    if result.get('error') or result.get('error_message'):
        return 'failed'
    return 'completed'


def _is_already_completed_followup_result(result: dict[str, Any]) -> bool:
    if result.get('triggered') is not False:
        return False
    if str(result.get('reason') or '').strip().lower() != 'already_completed':
        return False
    existing_run = result.get('existing_run') if isinstance(result.get('existing_run'), dict) else {}
    existing_status = str(existing_run.get('status') or result.get('status') or '').strip().lower()
    return existing_status in {'completed', 'partial', 'completed_with_errors'}


def _is_satisfied_followup_result(result: dict[str, Any]) -> bool:
    status = _resolve_result_status(result)
    return status in {'completed', 'partial'}


def _extract_result_counts(result: dict[str, Any], *, default_total: int = 0) -> dict[str, int]:
    def _int(value: Any) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    count_pairs = (
        ('symbols_refreshed', 'symbols_failed'),
        ('events_captured', 'events_failed'),
        ('results_persisted', 'results_failed'),
        ('features_persisted', 'features_failed'),
        ('snapshots_persisted', 'snapshots_failed'),
        ('action_summaries_persisted', 'action_summaries_failed'),
        ('evaluations_upserted', 'failed_count'),
        ('outcomes_upserted', 'failed_count'),
        ('completed_count', 'failed_count'),
    )
    for succeeded_key, failed_key in count_pairs:
        if succeeded_key not in result and failed_key not in result:
            continue
        succeeded = _int(result.get(succeeded_key))
        failed = _int(result.get(failed_key))
        total = _int(result.get('total') or result.get('symbols_total') or result.get('requested_count'))
        if total <= 0:
            total = default_total or succeeded + failed
        return {
            'processed': min(succeeded + failed, total) if total else succeeded + failed,
            'total': total,
            'succeeded': succeeded,
            'failed': failed,
        }

    summary = result.get('summary') if isinstance(result.get('summary'), dict) else {}
    if summary:
        succeeded = _int(
            summary.get('completed_count')
            or summary.get('computed_count')
            or summary.get('evaluated_count')
            or summary.get('passed_count')
        )
        failed = _int(summary.get('failed_count'))
        total = _int(summary.get('total') or summary.get('matched_symbol_count') or succeeded + failed)
        return {
            'processed': min(succeeded + failed, total) if total else succeeded + failed,
            'total': total,
            'succeeded': succeeded,
            'failed': failed,
        }

    progress = result.get('progress') if isinstance(result.get('progress'), dict) else {}
    progress_summary = progress.get('summary') if isinstance(progress.get('summary'), dict) else {}
    if progress_summary:
        succeeded = _int(
            progress_summary.get('completed_feedback_count')
            or progress_summary.get('completed_symbols')
            or progress_summary.get('processed_symbols')
        )
        failed = _int(progress_summary.get('failed_items') or progress_summary.get('failed_symbols'))
        total = _int(
            progress_summary.get('expected_feedback_count')
            or progress_summary.get('total_expected_feedback')
            or progress_summary.get('total_symbols')
        )
        return {
            'processed': min(succeeded + failed, total) if total else succeeded + failed,
            'total': total,
            'succeeded': succeeded,
            'failed': failed,
        }

    succeeded = _int(
        result.get('persisted_count')
        or result.get('trending_ranks_persisted')
        or result.get('items_persisted')
        or result.get('classified_items')
    )
    total = default_total or succeeded
    return {
        'processed': min(succeeded, total) if total else succeeded,
        'total': total,
        'succeeded': succeeded,
        'failed': 0,
    }


def _compact_result_summary(result: dict[str, Any]) -> dict[str, Any]:
    omitted_keys = {'items', 'rows', 'payloads', 'results'}
    return {
        key: value
        for key, value in result.items()
        if key not in omitted_keys and not key.endswith('_payload')
    }


def _resolve_auto_daily_tracked_stages(
    *,
    selected_stages: Sequence[str] | None,
    chain_fundamentals_followup: bool,
) -> list[str]:
    requested = list(selected_stages or PYKRX_DAILY_STAGE_ORDER)
    if chain_fundamentals_followup:
        requested.extend(AUTO_DAILY_FOLLOWUP_STAGE_ORDER)
    requested_set = {str(stage_id or '').strip() for stage_id in requested if str(stage_id or '').strip()}
    return [stage_id for stage_id in AUTO_DAILY_TRACKED_STAGE_ORDER if stage_id in requested_set]


class _OfficialDailyPipelineTracker:
    def __init__(self, *, service: Any | None, log_fetch: Callable[..., None]) -> None:
        self.service = service
        self.log_fetch = log_fetch
        self.pipeline_run_id: int | None = None
        self.market_date: str | None = None
        self.eod_run_id: int | None = None
        self.stage_statuses: dict[str, str] = {}
        self.enabled = service is not None

    def start(
        self,
        *,
        market_date: str,
        trigger: str,
        requested_stages: Sequence[str],
        resolved_stages: Sequence[str],
        eod_run_id: int,
        run_metadata: dict[str, Any],
    ) -> None:
        if not self.enabled or self.service is None:
            return
        try:
            run = self.service.start_run(
                market_date=market_date,
                trigger=trigger,
                requested_stages=requested_stages,
                resolved_stages=resolved_stages,
                run_metadata={
                    'entrypoint': 'PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due',
                    'eod_collection_run_id': eod_run_id,
                    **run_metadata,
                },
            )
            self.pipeline_run_id = int(run['id'])
            self.market_date = market_date
            self.eod_run_id = eod_run_id
        except Exception as exc:  # pragma: no cover - defensive production guard
            self.enabled = False
            logger.warning('daily pipeline tracker start failed for auto EOD run %s: %s', eod_run_id, exc)
            self.log_fetch(
                'daily_pipeline_tracking',
                'failed',
                stage='start_run',
                run_id=eod_run_id,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )

    def update_stage(
        self,
        *,
        stage: str,
        status: str,
        current_step: str,
        current_activity: str,
        processed: int | None = None,
        total: int | None = None,
        succeeded: int | None = None,
        failed: int | None = None,
        current_symbol: str | None = None,
        current_period: str | None = None,
        source_run_ids: dict[str, Any] | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        if not self.enabled or self.service is None or self.pipeline_run_id is None or self.market_date is None:
            return
        normalized_status = _normalize_stage_status(status)
        self.stage_statuses[stage] = normalized_status
        merged_source_run_ids = {'eod_run_id': self.eod_run_id} if self.eod_run_id is not None else {}
        if source_run_ids:
            merged_source_run_ids.update(source_run_ids)
        merged_details = {
            'mode': 'auto_eod_pipeline_tracking',
            'progress_source': 'auto_eod_stage_progress',
            **dict(details or {}),
        }
        try:
            self.service.update_stage(
                pipeline_run_id=self.pipeline_run_id,
                stage_id=stage,
                market_date=self.market_date,
                status=normalized_status,
                current_step=current_step,
                current_activity=current_activity,
                processed=processed,
                total=total,
                succeeded=succeeded,
                failed=failed,
                current_symbol=current_symbol,
                current_period=current_period,
                source_run_ids=merged_source_run_ids,
                details=merged_details,
            )
        except Exception as exc:  # pragma: no cover - defensive production guard
            logger.warning('daily pipeline tracker update failed for run %s stage %s: %s', self.eod_run_id, stage, exc)
            self.log_fetch(
                'daily_pipeline_tracking',
                'failed',
                stage=stage,
                run_id=self.eod_run_id,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )

    def finish_run(self, *, status: str | None = None, error_message: str | None = None) -> None:
        if not self.enabled or self.service is None or self.pipeline_run_id is None:
            return
        final_status = status or self.resolve_overall_status()
        try:
            self.service.finish_run(
                pipeline_run_id=self.pipeline_run_id,
                status=final_status,
                error_message=error_message,
                run_metadata={
                    'stage_statuses': dict(self.stage_statuses),
                    'eod_collection_run_id': self.eod_run_id,
                },
            )
        except Exception as exc:  # pragma: no cover - defensive production guard
            logger.warning('daily pipeline tracker finish failed for run %s: %s', self.eod_run_id, exc)
            self.log_fetch(
                'daily_pipeline_tracking',
                'failed',
                stage='finish_run',
                run_id=self.eod_run_id,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )

    def resolve_overall_status(self) -> str:
        statuses = set(self.stage_statuses.values())
        if not statuses:
            return 'completed'
        if 'failed' in statuses:
            return 'failed'
        if 'blocked' in statuses:
            return 'blocked'
        if 'waiting' in statuses:
            return 'waiting'
        if 'running' in statuses:
            return 'running'
        if 'partial' in statuses:
            return 'partial'
        if statuses <= {'completed', 'skipped'}:
            return 'completed'
        return 'partial'


def _run_daily_factor_followup(*, market_date: str) -> dict[str, Any]:
    from app.signals.advanced_value_factor import ADVANCED_VALUE_FACTOR_SOURCE, daily_advanced_value_factor_service
    from app.signals.beta_factor import BETA_FACTOR_SOURCE, daily_beta_factor_service
    from app.signals.composite_factor import COMPOSITE_FACTOR_SOURCE, daily_composite_factor_service
    from app.signals.low_risk_factor import LOW_RISK_SOURCE, daily_low_risk_factor_service
    from app.signals.momentum_factor import daily_momentum_factor_service
    from app.signals.quality_factor import QUALITY_FACTOR_SOURCE, daily_quality_factor_service
    from app.signals.unified_value_factor import UNIFIED_VALUE_FACTOR_SOURCE, daily_unified_value_factor_service
    from app.signals.value_factor import VALUE_FACTOR_SOURCE, daily_value_factor_service

    stage_calls: dict[str, Callable[[], dict[str, Any]]] = {
        'momentum_factors': lambda: daily_momentum_factor_service.rebuild_for_market_date(market_date=market_date),
        'value_factors': lambda: daily_value_factor_service.rebuild_for_market_date(
            market_date=market_date,
            source=VALUE_FACTOR_SOURCE,
        ),
        'low_risk_factors': lambda: daily_low_risk_factor_service.rebuild_for_market_date(
            market_date=market_date,
            source=LOW_RISK_SOURCE,
        ),
        'beta_factors': lambda: daily_beta_factor_service.rebuild_for_market_date(
            market_date=market_date,
            source=BETA_FACTOR_SOURCE,
        ),
        'advanced_value_factors': lambda: daily_advanced_value_factor_service.rebuild_for_market_date(
            market_date=market_date,
            source=ADVANCED_VALUE_FACTOR_SOURCE,
        ),
        'unified_value_factors': lambda: daily_unified_value_factor_service.rebuild_for_market_date(
            market_date=market_date,
            source=UNIFIED_VALUE_FACTOR_SOURCE,
        ),
        'quality_factors': lambda: daily_quality_factor_service.rebuild_for_market_date(
            market_date=market_date,
            source=QUALITY_FACTOR_SOURCE,
        ),
        'composite_factors': lambda: daily_composite_factor_service.rebuild_for_market_date(
            market_date=market_date,
            source=COMPOSITE_FACTOR_SOURCE,
        ),
    }
    stages: dict[str, dict[str, Any]] = {}
    overall_status = 'completed'
    for stage_id in FACTOR_FOLLOWUP_STAGE_ORDER:
        try:
            result = stage_calls[stage_id]()
            failed = int(
                result.get('results_failed')
                or result.get('failed')
                or result.get('symbols_failed')
                or 0
            )
            stage_status = 'completed' if failed == 0 else 'partial'
            if failed:
                overall_status = 'partial'
            stages[stage_id] = {
                'status': stage_status,
                'result': result,
            }
        except Exception as exc:  # pragma: no cover - defensive production guard
            logger.exception('Daily factor follow-up failed for %s on %s', stage_id, market_date)
            overall_status = 'partial'
            stages[stage_id] = {
                'status': 'failed',
                'error_type': type(exc).__name__,
                'error_message': str(exc),
            }
    return {
        'triggered': True,
        'market_date': market_date,
        'status': overall_status,
        'stages': stages,
    }


def _run_daily_external_news_followup(*, market_date: str, enabled: bool) -> dict[str, Any]:
    if not enabled:
        return {
            'triggered': False,
            'reason': 'followup_disabled',
            'market_date': market_date,
        }

    from app.core.settings import settings
    from app.external_news.service import external_news_service

    result = asyncio.run(
        external_news_service.collect_for_agent_feedback_candidates(
            market_date=market_date,
            display=settings.external_news_default_display,
        )
    )
    return {
        'triggered': True,
        **result,
    }


def _frame_value(frame: Any, symbol: str, column: str) -> Any:
    if frame is None or getattr(frame, 'empty', False):
        return None
    try:
        if symbol in frame.index and column in frame.columns:
            return frame.loc[symbol, column]
    except Exception:
        pass
    try:
        row = frame.loc[symbol]
        if hasattr(row, 'get'):
            return row.get(column)
    except Exception:
        return None
    return None


def _to_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if hasattr(value, 'item'):
            value = value.item()
        converted = int(value)
    except (TypeError, ValueError):
        return None
    return converted if converted > 0 else None


def _enrich_bars_with_market_turnover(
    *,
    bars: list[dict[str, Any]],
    symbol: str,
    market: str,
    dependencies: PykrxPipelineDependencies,
    market_turnover_cache: dict[tuple[str, str], Any],
    run_id: int | None,
) -> list[dict[str, Any]]:
    getter = dependencies.get_market_ohlcv_by_ticker
    if getter is None or not bars:
        return bars
    enriched: list[dict[str, Any]] = []
    for bar in bars:
        trade_date = str(bar.get('trade_date'))
        pykrx_date = trade_date.replace('-', '')
        cache_key = (pykrx_date, market)
        if cache_key not in market_turnover_cache:
            try:
                market_turnover_cache[cache_key] = getter(pykrx_date, market)
            except Exception as exc:
                market_turnover_cache[cache_key] = None
                dependencies.log_fetch(
                    'fetch_market_ohlcv_turnover',
                    'failed',
                    run_id=run_id,
                    market=market,
                    trade_date=trade_date,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
        provider_turnover = _to_int_or_none(_frame_value(market_turnover_cache.get(cache_key), symbol, '거래대금'))
        if provider_turnover is not None:
            updated = dict(bar)
            updated['turnover'] = provider_turnover
            enriched.append(updated)
            continue
        enriched.append(bar)
    return enriched


def _run_daily_intraday_snapshot_followup(*, market_date: str, enabled: bool) -> dict[str, Any]:
    if not enabled:
        return {'triggered': False, 'reason': 'followup_disabled', 'market_date': market_date}
    from datetime import date

    from app.agent_context.eligibility import load_agent_feedback_eligible_symbols
    from app.core.settings import settings
    from app.market.kis_intraday_snapshots import kis_intraday_snapshot_service

    symbols = load_agent_feedback_eligible_symbols(market_date=date.fromisoformat(market_date))
    if not symbols:
        return {'triggered': False, 'reason': 'no_agent_feedback_eligible_symbols', 'market_date': market_date}
    result = asyncio.run(
        kis_intraday_snapshot_service.collect_snapshots(
            symbols=symbols,
            market_code=settings.kis_intraday_snapshot_market_code,
            max_ranking_pages=settings.kis_intraday_snapshot_max_ranking_pages,
            max_news_items=settings.kis_intraday_snapshot_news_limit,
            include_news=False,
        )
    )
    return {'triggered': True, 'market_date': market_date, **result}


def _run_daily_feedback_followup(
    *,
    market_date: str,
    enabled: bool,
    progress_callback: Callable[..., None] | None = None,
) -> dict[str, Any]:
    from app.agent_context.roles import SUMMARIZER_AGENT_NAMES
    from app.agent_context.run_modes import FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH

    return _run_daily_agent_feedback_followup(
        market_date=market_date,
        enabled=enabled,
        roles=SUMMARIZER_AGENT_NAMES,
        feedback_scope_mode=FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
        progress_callback=progress_callback,
    )


def _run_daily_signal_decider_followup(
    *,
    market_date: str,
    enabled: bool,
    progress_callback: Callable[..., None] | None = None,
) -> dict[str, Any]:
    from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES
    from app.agent_context.run_modes import FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH

    return _run_daily_agent_feedback_followup(
        market_date=market_date,
        enabled=enabled,
        roles=DECISION_SUPPORT_AGENT_NAMES,
        feedback_scope_mode=FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
        progress_callback=progress_callback,
    )


def _run_daily_agent_feedback_followup(
    *,
    market_date: str,
    enabled: bool,
    roles: tuple[str, ...],
    feedback_scope_mode: str,
    progress_callback: Callable[..., None] | None = None,
) -> dict[str, Any]:
    if not enabled:
        return {
            'triggered': False,
            'reason': 'followup_disabled',
            'market_date': market_date,
        }

    from datetime import date

    from app.agent_context.prerequisite_service import agent_feedback_prerequisite_service
    from app.agent_context.retry_service import FeedbackRetryCycleConfig, agent_feedback_retry_service
    from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES
    from app.agent_context.run_modes import FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
    from app.core.settings import settings

    prerequisites = agent_feedback_prerequisite_service.inspect_feedback_prerequisites(
        market_date=market_date,
    )
    bypass_prerequisite_gate_enabled = (
        bool(settings.openclaw_feedback_fast_path_bypass_prerequisite_gate)
        and feedback_scope_mode == FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
        and set(roles).issubset(set(DECISION_SUPPORT_AGENT_NAMES))
    )
    prerequisites_ready = bool(prerequisites['ready'])
    bypass_prerequisite_gate = bypass_prerequisite_gate_enabled and not prerequisites_ready
    if not prerequisites_ready and not bypass_prerequisite_gate:
        return {
            'triggered': False,
            'reason': 'feedback_prerequisites_not_ready',
            'status': prerequisites['status'],
            'market_date': market_date,
            'prerequisites': prerequisites,
        }

    result = agent_feedback_retry_service.run_retry_cycles(
        config=FeedbackRetryCycleConfig(
            market_date=date.fromisoformat(market_date),
            roles=roles,
            feedback_scope_mode=feedback_scope_mode,
            max_cycles=settings.openclaw_feedback_retry_max_cycles,
            outer_batch_size=settings.openclaw_feedback_retry_outer_batch_size,
            reuse_existing=True,
            progress_callback=progress_callback,
        ),
        bypass_prerequisite_gate=bypass_prerequisite_gate,
    )
    return {
        'triggered': True,
        'prerequisite_gate_bypassed': bypass_prerequisite_gate,
        'prerequisites': prerequisites if bypass_prerequisite_gate else None,
        **result,
    }

def _run_daily_signal_decider_evaluation_followup(*, market_date: str, enabled: bool) -> dict[str, Any]:
    if not enabled:
        return {
            'triggered': False,
            'reason': 'followup_disabled',
            'market_date': market_date,
        }

    from app.agent_context.signal_decider_evaluation_maintenance_service import (
        signal_decider_evaluation_maintenance_service,
    )

    return signal_decider_evaluation_maintenance_service.refresh_recent_coverage(
        as_of_market_date=market_date,
        refresh_calendar=True,
        # Cover the longest 10-session outcome horizon plus weekends/holidays
        # without rebuilding the full 30-day evaluation window every day.
        calendar_lookback_days=21,
        trigger='eod_pykrx_followup',
    )


def run_market_data_pipeline(
    service,
    *,
    dependencies: PykrxPipelineDependencies,
    start_date: str,
    end_date: str,
    markets: Sequence[str],
    trigger: str,
    refresh_universe: bool,
    mode: str,
    limit: int | None = None,
    symbol: str | None = None,
    symbols_override: Sequence[Any] | None = None,
    retry_context: dict | None = None,
    selected_stages: Sequence[str] | None = None,
    chain_fundamentals_followup: bool = True,
    track_daily_pipeline: bool = False,
) -> dict:
    normalized_start = dependencies.normalize_yyyymmdd(start_date)
    normalized_end = dependencies.normalize_yyyymmdd(end_date)
    requested_stage_set = set(selected_stages or PYKRX_DAILY_STAGE_ORDER)
    if 'price_collection' not in requested_stage_set:
        raise ValueError('price_collection stage is required for pykrx pipeline execution')
    dependencies.log_fetch(
        'run_collection',
        'started',
        mode=mode,
        trigger=trigger,
        start_date=normalized_start,
        end_date=normalized_end,
        markets=list(markets),
        limit=limit,
        symbol=symbol,
        symbols_override_count=len(symbols_override) if symbols_override is not None else None,
        refresh_universe=refresh_universe,
        retry=retry_context,
        selected_stages=list(selected_stages or PYKRX_DAILY_STAGE_ORDER),
    )

    effective_refresh_universe = refresh_universe and symbols_override is None
    if symbols_override is not None:
        universe_snapshot = None
        symbols = list(symbols_override)
    elif effective_refresh_universe:
        snapshot = service.fetch_recent_universe_snapshot(markets=markets, limit=limit)
        symbols = snapshot['symbols']
        deactivate_missing = limit is None and symbol is None
        persisted = service._persist_universe(symbols, deactivate_missing=deactivate_missing)
        universe_snapshot = {
            'source': 'pykrx',
            'markets': list(markets),
            'symbol_count': len(symbols),
            'persisted_count': persisted,
            'latest_trade_date': snapshot['latest_trade_date'],
            'universe_source': snapshot['universe_source'],
            'deactivate_missing': deactivate_missing,
        }
    else:
        universe_snapshot = None
        symbols = service.load_active_universe(markets=markets, symbol=symbol, limit=limit)

    if symbol:
        symbols = [item for item in symbols if item.symbol == symbol]
        if not symbols:
            matched = service.find_symbol(symbol, markets=markets)
            if matched is not None:
                symbols = [matched]
    if not symbols:
        raise RuntimeError('No active pykrx symbols are available for backfill.')

    latest_trade_date = service.resolve_latest_trade_date(end_date=normalized_end)
    run_metadata = {
        'mode': mode,
        'requested_start_date': normalized_start,
        'requested_end_date': normalized_end,
        'latest_available_trade_date': latest_trade_date,
        'markets': list(markets),
        'symbol': symbol,
        'limited_run': limit is not None,
        'universe_refreshed': effective_refresh_universe,
        'universe_snapshot': universe_snapshot,
    }
    if retry_context is not None:
        run_metadata['retry'] = retry_context
        run_metadata['requested_symbols'] = [item.symbol for item in symbols]
    first_symbol = symbols[0].symbol if symbols else None
    run_metadata = dependencies.update_run_stage_metadata(
        run_metadata,
        stage='price_collection',
        status='running',
        current_step='fetch_symbol_ohlcv',
        current_activity=(
            f'Collecting pykrx daily prices for {first_symbol}'
            if first_symbol
            else 'Collecting pykrx daily prices'
        ),
        processed=0,
        total=len(symbols),
        succeeded=0,
        failed=0,
        current_symbol=first_symbol,
        extra={'bars_upserted': 0},
    )
    run_id = dependencies.create_eod_collection_run(
        market='KRX',
        trade_date=latest_trade_date,
        symbols_total=len(symbols),
        trigger=trigger,
        source='pykrx',
        run_metadata=run_metadata,
    )
    tracked_stages = _resolve_auto_daily_tracked_stages(
        selected_stages=selected_stages,
        chain_fundamentals_followup=chain_fundamentals_followup,
    )
    daily_pipeline_tracker = _OfficialDailyPipelineTracker(
        service=dependencies.daily_pipeline_run_state_service if track_daily_pipeline else None,
        log_fetch=dependencies.log_fetch,
    )
    daily_pipeline_tracker.start(
        market_date=latest_trade_date,
        trigger=trigger,
        requested_stages=tracked_stages,
        resolved_stages=tracked_stages,
        eod_run_id=run_id,
        run_metadata={
            'mode': mode,
            'trigger': trigger,
            'markets': list(markets),
            'selected_stages': list(selected_stages or PYKRX_DAILY_STAGE_ORDER),
            'chain_fundamentals_followup': chain_fundamentals_followup,
            'retry': retry_context,
        },
    )

    symbols_succeeded = 0
    symbols_failed = 0
    failed_symbols: list[str] = []
    bars_upserted = 0

    def persist_stage_progress(
        *,
        stage: str,
        status: str,
        current_step: str,
        current_activity: str,
        processed: int | None = None,
        total: int | None = None,
        succeeded: int | None = None,
        failed: int | None = None,
        current_symbol: str | None = None,
        current_period: str | None = None,
        source_run_ids: dict[str, Any] | None = None,
        extra: dict | None = None,
    ) -> None:
        nonlocal run_metadata
        metadata_extra = dict(extra or {})
        if source_run_ids:
            metadata_extra['source_run_ids'] = dict(source_run_ids)
        run_metadata = dependencies.update_run_stage_metadata(
            run_metadata,
            stage=stage,
            status=status,
            current_step=current_step,
            current_activity=current_activity,
            processed=processed,
            total=total,
            succeeded=succeeded,
            failed=failed,
            current_symbol=current_symbol,
            current_period=current_period,
            extra=metadata_extra,
        )
        daily_pipeline_tracker.update_stage(
            stage=stage,
            status=status,
            current_step=current_step,
            current_activity=current_activity,
            processed=processed,
            total=total,
            succeeded=succeeded,
            failed=failed,
            current_symbol=current_symbol,
            current_period=current_period,
            source_run_ids=source_run_ids,
            details=metadata_extra,
        )
        try:
            dependencies.update_eod_collection_run_progress(
                run_id,
                symbols_succeeded=symbols_succeeded,
                symbols_failed=symbols_failed,
                run_metadata=run_metadata,
            )
        except Exception as exc:
            logger.warning('pykrx progress update failed for run %s during %s: %s', run_id, stage, exc)
            dependencies.log_fetch(
                'update_collection_progress',
                'failed',
                run_id=run_id,
                stage=stage,
                current_step=current_step,
                current_symbol=current_symbol,
                current_period=current_period,
                symbols_succeeded=symbols_succeeded,
                symbols_failed=symbols_failed,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )

    persist_stage_progress(
        stage='price_collection',
        status='running',
        current_step='fetch_symbol_ohlcv',
        current_activity=(
            f'Collecting pykrx daily prices for {first_symbol}'
            if first_symbol
            else 'Collecting pykrx daily prices'
        ),
        processed=0,
        total=len(symbols),
        succeeded=0,
        failed=0,
        current_symbol=first_symbol,
        extra={'bars_upserted': 0},
    )

    try:
        market_turnover_cache: dict[tuple[str, str], Any] = {}
        for index, item in enumerate(symbols):
            symbol_started_at = time.perf_counter()
            try:
                frame = dependencies.get_market_ohlcv_by_date(normalized_start, normalized_end, item.symbol)
                if frame.empty:
                    raise RuntimeError('No OHLCV rows returned.')
                fundamentals_frame = None
                if mode not in {'daily_auto', 'daily_stage_run'}:
                    fundamentals_frame = service._load_symbol_fundamentals(
                        symbol=item.symbol,
                        start_date=normalized_start,
                        end_date=normalized_end,
                    )
                bars = service._map_symbol_bars(frame, fundamentals_frame=fundamentals_frame)
                bars = _enrich_bars_with_market_turnover(
                    bars=bars,
                    symbol=item.symbol,
                    market=item.market,
                    dependencies=dependencies,
                    market_turnover_cache=market_turnover_cache,
                    run_id=run_id,
                )
                persisted_count = dependencies.persist_daily_price_bars(
                    item.symbol,
                    bars,
                    market=item.market,
                    source='pykrx',
                    run_id=run_id,
                    name_ko=item.name_ko,
                    standard_code=item.standard_code,
                )
                bars_upserted += persisted_count
                symbols_succeeded += 1
                dependencies.log_fetch(
                    'fetch_symbol_ohlcv',
                    'completed',
                    run_id=run_id,
                    symbol=item.symbol,
                    market=item.market,
                    start_date=normalized_start,
                    end_date=normalized_end,
                    bar_count=len(bars),
                    persisted_count=persisted_count,
                    duration_ms=round((time.perf_counter() - symbol_started_at) * 1000, 3),
                )
            except Exception as exc:
                symbols_failed += 1
                failed_symbols.append(item.symbol)
                logger.warning('pykrx backfill failed for %s: %s', item.symbol, exc)
                dependencies.log_fetch(
                    'fetch_symbol_ohlcv',
                    'failed',
                    run_id=run_id,
                    symbol=item.symbol,
                    market=item.market,
                    start_date=normalized_start,
                    end_date=normalized_end,
                    duration_ms=round((time.perf_counter() - symbol_started_at) * 1000, 3),
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                if (
                    mode == 'daily_auto'
                    and symbols_succeeded == 0
                    and symbols_failed >= min(PYKRX_PROVIDER_OUTAGE_EARLY_ABORT_FAILURES, len(symbols))
                ):
                    dependencies.log_fetch(
                        'run_collection',
                        'failed',
                        run_id=run_id,
                        mode=mode,
                        trigger=trigger,
                        start_date=normalized_start,
                        end_date=normalized_end,
                        latest_available_trade_date=latest_trade_date,
                        symbols_total=len(symbols),
                        symbols_succeeded=symbols_succeeded,
                        symbols_failed=symbols_failed,
                        bars_upserted=bars_upserted,
                        error_type='PykrxProviderUnavailable',
                        error_message=(
                            'Aborting daily PyKRX price collection after initial consecutive failures; '
                            'provider appears unavailable.'
                        ),
                    )
                    break
            try:
                next_symbol = symbols[index + 1].symbol if index + 1 < len(symbols) else None
                persist_stage_progress(
                    stage='price_collection',
                    status='running'
                    if next_symbol is not None
                    else _completion_status_for_gap(failed=symbols_failed, total=len(symbols)),
                    current_step='fetch_symbol_ohlcv' if next_symbol is not None else 'price_collection_completed',
                    current_activity=(
                        f'Collecting pykrx daily prices for {next_symbol}'
                        if next_symbol is not None
                        else 'Completed pykrx daily price collection'
                    ),
                    processed=symbols_succeeded + symbols_failed,
                    total=len(symbols),
                    succeeded=symbols_succeeded,
                    failed=symbols_failed,
                    current_symbol=next_symbol,
                    extra={
                        'bars_upserted': bars_upserted,
                        **_gap_policy_details(failed=symbols_failed, total=len(symbols)),
                    },
                )
            except Exception as exc:
                logger.warning('pykrx progress update failed for run %s after %s: %s', run_id, item.symbol, exc)
                dependencies.log_fetch(
                    'update_collection_progress',
                    'failed',
                    run_id=run_id,
                    symbol=item.symbol,
                    symbols_succeeded=symbols_succeeded,
                    symbols_failed=symbols_failed,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            if service.symbol_pause_seconds:
                time.sleep(service.symbol_pause_seconds)
    except Exception as exc:
        dependencies.log_fetch(
            'run_collection',
            'failed',
            run_id=run_id,
            mode=mode,
            trigger=trigger,
            start_date=normalized_start,
            end_date=normalized_end,
            latest_available_trade_date=latest_trade_date,
            symbols_total=len(symbols),
            symbols_succeeded=symbols_succeeded,
            symbols_failed=symbols_failed,
            bars_upserted=bars_upserted,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
        run_metadata = dependencies.update_run_stage_metadata(
            run_metadata,
            stage='price_collection',
            status='failed',
            current_step='price_collection_failed',
            current_activity='pykrx daily price collection failed',
            processed=symbols_succeeded + symbols_failed,
            total=len(symbols),
            succeeded=symbols_succeeded,
            failed=symbols_failed,
            current_symbol=None,
            extra={'bars_upserted': bars_upserted},
        )
        if failed_symbols:
            run_metadata = service._run_metadata_with_failed_symbols(run_metadata, failed_symbols)
        dependencies.complete_eod_collection_run(
            run_id,
            status='failed',
            symbols_succeeded=symbols_succeeded,
            symbols_failed=symbols_failed,
            error_message=str(exc),
            run_metadata=run_metadata,
        )
        daily_pipeline_tracker.finish_run(status='failed', error_message=str(exc))
        raise

    if mode == 'daily_auto' and symbols_succeeded == 0 and symbols_failed > 0:
        error_message = (
            'PyKRX daily price collection failed for every requested symbol; '
            'provider appears unavailable, so downstream EOD stages were not run.'
        )
        dependencies.log_fetch(
            'run_collection',
            'failed',
            run_id=run_id,
            mode=mode,
            trigger=trigger,
            start_date=normalized_start,
            end_date=normalized_end,
            latest_available_trade_date=latest_trade_date,
            symbols_total=len(symbols),
            symbols_succeeded=symbols_succeeded,
            symbols_failed=symbols_failed,
            bars_upserted=bars_upserted,
            error_type='PykrxProviderUnavailable',
            error_message=error_message,
        )
        persist_stage_progress(
            stage='price_collection',
            status='failed',
            current_step='price_collection_provider_unavailable',
            current_activity=error_message,
            processed=symbols_failed,
            total=len(symbols),
            succeeded=0,
            failed=symbols_failed,
            current_symbol=None,
            extra={
                'bars_upserted': bars_upserted,
                'provider_outage_suspected': True,
                'downstream_stages_skipped': True,
            },
        )
        run_metadata = service._run_metadata_with_failed_symbols(run_metadata, failed_symbols)
        dependencies.complete_eod_collection_run(
            run_id,
            status='failed',
            symbols_succeeded=symbols_succeeded,
            symbols_failed=symbols_failed,
            error_message=error_message,
            run_metadata=run_metadata,
        )
        daily_pipeline_tracker.finish_run(status='failed', error_message=error_message)
        return {
            'run_id': run_id,
            'source': 'pykrx',
            'status': 'failed',
            'selected_stages': list(selected_stages or PYKRX_DAILY_STAGE_ORDER),
            'markets': list(markets),
            'start_date': normalized_start,
            'requested_end_date': normalized_end,
            'latest_available_trade_date': latest_trade_date,
            'symbols_total': len(symbols),
            'symbols_succeeded': symbols_succeeded,
            'symbols_failed': symbols_failed,
            'failed_symbols': failed_symbols[:50],
            'bars_upserted': bars_upserted,
            'error_type': 'PykrxProviderUnavailable',
            'error_message': error_message,
            'refresh_universe': effective_refresh_universe,
            'universe_snapshot': universe_snapshot,
        }

    failed_symbol_set = set(failed_symbols)
    price_context_symbols = [item.symbol for item in symbols if item.symbol not in failed_symbol_set]
    price_context_result: dict
    price_context_error_message: str | None = None
    if mode != 'daily_stage_run':
        try:
            price_context_result = dependencies.price_context_service.rebuild_for_market_date(
                market_date=latest_trade_date,
                symbols=price_context_symbols,
                source='pykrx',
                run_id=run_id,
            )
        except Exception as exc:
            price_context_result = build_stage_failure_result(
                market_date=latest_trade_date,
                source='pykrx',
                error=exc,
                totals={
                    'requested_symbols': len(price_context_symbols),
                    'contexts_persisted': 0,
                    'contexts_failed': len(price_context_symbols),
                },
                failed_symbols=price_context_symbols,
                items_key='items',
            )
            price_context_error_message = f'Price context persistence failed: {exc}'
    else:
        price_context_result = {
            'skipped': True,
            'reason': 'stage_orchestrator_skipped_implicit_price_contexts',
            'market_date': latest_trade_date,
            'source': 'pykrx',
        }

    pykrx_enrichment_result: dict
    pykrx_enrichment_error_message: str | None = None
    if 'pykrx_enrichment' in requested_stage_set:
        try:
            persist_stage_progress(
                stage='pykrx_enrichment',
                status='running',
                current_step='collecting_pykrx_enrichment',
                current_activity='Collecting PyKRX enrichment categories',
                processed=0,
                total=len(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES),
                succeeded=0,
                failed=0,
                extra={
                    'categories_requested': list(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES),
                    'required_downstream_categories': list(REQUIRED_EXEC_DECIDER_PYKRX_ENRICHMENT_CATEGORIES),
                },
            )
            pykrx_enrichment_result = dependencies.pykrx_enrichment_service.collect_for_market_date(
                market_date=latest_trade_date,
                markets=markets,
                symbols=[item.symbol for item in symbols],
            )
            preflight = build_pykrx_enrichment_preflight(
                market_date=latest_trade_date,
                coverage=dict(pykrx_enrichment_result.get('coverage') or {}),
                categories=REQUIRED_EXEC_DECIDER_PYKRX_ENRICHMENT_CATEGORIES,
            )
            pykrx_enrichment_result['downstream_preflight'] = preflight
            category_results = (
                pykrx_enrichment_result.get('categories')
                if isinstance(pykrx_enrichment_result.get('categories'), dict)
                else {}
            )
            collection_failed_categories = [
                category
                for category, details in category_results.items()
                if isinstance(details, dict) and details.get('errors')
            ]
            failed_categories = sorted(
                {
                    *collection_failed_categories,
                    *(preflight.get('failed_categories') or []),
                }
            )
            category_row_counts = {
                category: int((details or {}).get('rows_persisted') or 0)
                for category, details in category_results.items()
                if isinstance(details, dict)
            }
            coverage_categories = (
                pykrx_enrichment_result.get('coverage', {}).get('categories')
                if isinstance(pykrx_enrichment_result.get('coverage'), dict)
                and isinstance(pykrx_enrichment_result.get('coverage', {}).get('categories'), dict)
                else {}
            )
            blocking_failed_categories: list[str] = []
            tolerated_failed_categories: list[str] = []
            for category in failed_categories:
                coverage_payload = (
                    dict(coverage_categories.get(category))
                    if isinstance(coverage_categories.get(category), dict)
                    else {}
                )
                expected_rows = int(coverage_payload.get('expected_rows') or 0)
                present_rows = int(coverage_payload.get('present_rows') or 0)
                missing_rows = max(expected_rows - present_rows, 0)
                if (
                    expected_rows > 0
                    and missing_rows > 0
                    and _is_non_blocking_gap(failed=missing_rows, total=expected_rows)
                    and preflight.get('allowed')
                ):
                    tolerated_failed_categories.append(category)
                else:
                    blocking_failed_categories.append(category)
            stage_status = (
                'completed'
                if pykrx_enrichment_result.get('status') == 'completed'
                and preflight.get('allowed')
                and not blocking_failed_categories
                else 'partial'
            )
            if blocking_failed_categories:
                pykrx_enrichment_error_message = (
                    'PyKRX enrichment incomplete for categories: ' + ','.join(blocking_failed_categories)
                )
            persist_stage_progress(
                stage='pykrx_enrichment',
                status=stage_status,
                current_step='pykrx_enrichment_completed',
                current_activity=(
                    'PyKRX enrichment collected'
                    if stage_status == 'completed'
                    else 'PyKRX enrichment collected with coverage gaps'
                ),
                processed=len(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES),
                total=len(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES),
                succeeded=max(len(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES) - len(blocking_failed_categories), 0),
                failed=len(blocking_failed_categories),
                extra={
                    'categories_requested': pykrx_enrichment_result.get('categories_requested'),
                    'category_row_counts': category_row_counts,
                    'failed_categories': failed_categories,
                    'blocking_failed_categories': blocking_failed_categories,
                    'tolerated_failed_categories': tolerated_failed_categories,
                    'minor_missing_gap_policy': {
                        'max_non_blocking_missing_ratio': MAX_NON_BLOCKING_STAGE_MISSING_RATIO,
                    },
                    'downstream_preflight_status': preflight.get('status'),
                    'downstream_allowed': bool(preflight.get('allowed')),
                    'zero_coverage_categories': preflight.get('zero_coverage_categories'),
                },
            )
            dependencies.log_fetch(
                'pykrx_enrichment',
                stage_status,
                run_id=run_id,
                market_date=latest_trade_date,
                categories_requested=pykrx_enrichment_result.get('categories_requested'),
                category_row_counts=category_row_counts,
                failed_categories=failed_categories,
                blocking_failed_categories=blocking_failed_categories,
                tolerated_failed_categories=tolerated_failed_categories,
                downstream_preflight_status=preflight.get('status'),
                downstream_allowed=bool(preflight.get('allowed')),
            )
        except Exception as exc:
            pykrx_enrichment_result = build_stage_failure_result(
                market_date=latest_trade_date,
                source='pykrx_enrichment',
                error=exc,
                totals={
                    'categories_requested': list(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES),
                    'categories_failed': len(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES),
                },
            )
            pykrx_enrichment_error_message = f'PyKRX enrichment collection failed: {exc}'
            persist_stage_progress(
                stage='pykrx_enrichment',
                status='failed',
                current_step='pykrx_enrichment_failed',
                current_activity='PyKRX enrichment collection failed',
                processed=0,
                total=len(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES),
                succeeded=0,
                failed=len(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES),
                extra={
                    'categories_requested': list(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES),
                    'required_downstream_categories': list(REQUIRED_EXEC_DECIDER_PYKRX_ENRICHMENT_CATEGORIES),
                },
            )
    else:
        pykrx_enrichment_result = {
            'skipped': True,
            'reason': 'stage_not_requested',
            'market_date': latest_trade_date,
            'source': 'pykrx_enrichment',
        }

    technical_snapshot_result: dict
    snapshot_error_message: str | None = None
    if 'technical_snapshots' in requested_stage_set:
        try:
            technical_snapshot_result = dependencies.technical_snapshot_service.rebuild_for_market_date(
                market_date=latest_trade_date,
                symbols=[item.symbol for item in symbols],
                source='pykrx',
                run_id=run_id,
                progress_callback=make_stage_progress_callback(
                    persist_stage_progress,
                    stage='technical_snapshots',
                    default_step='computing_symbol_snapshot',
                    default_activity='Computing daily technical snapshots',
                    include_current_symbol=True,
                ),
            )
            technical_failed = int(technical_snapshot_result.get('snapshots_failed') or 0)
            technical_succeeded = int(technical_snapshot_result.get('snapshots_persisted') or 0)
            technical_total = technical_succeeded + technical_failed
            technical_status = _completion_status_for_gap(failed=technical_failed, total=technical_total)
            persist_stage_progress(
                stage='technical_snapshots',
                status=technical_status,
                current_step='technical_snapshots_completed',
                current_activity='Daily technical snapshots computed'
                if technical_status == 'completed'
                else 'Daily technical snapshots completed with errors',
                processed=technical_total,
                total=len(symbols),
                succeeded=technical_succeeded,
                failed=technical_failed,
                extra=_gap_policy_details(failed=technical_failed, total=technical_total),
            )
        except Exception as exc:
            technical_snapshot_result = build_stage_failure_result(
                market_date=latest_trade_date,
                source='pykrx',
                error=exc,
                totals={
                    'requested_symbols': len(symbols),
                    'snapshots_persisted': 0,
                    'snapshots_failed': len(symbols),
                },
                failed_symbols=[item.symbol for item in symbols],
                items_key='items',
            )
            snapshot_error_message = f'Technical snapshot persistence failed: {exc}'
            persist_stage_progress(
                stage='technical_snapshots',
                status='failed',
                current_step='technical_snapshots_failed',
                current_activity='Daily technical snapshot persistence failed',
                processed=0,
                total=len(symbols),
                succeeded=0,
                failed=len(symbols),
            )
    else:
        technical_snapshot_result = {
            'skipped': True,
            'reason': 'stage_not_requested',
            'market_date': latest_trade_date,
            'source': 'pykrx',
        }

    action_summary_result: dict
    action_summary_error_message: str | None = None
    if 'action_summaries' in requested_stage_set:
        try:
            action_summary_result = dependencies.action_summary_service.rebuild_for_market_date(
                market_date=latest_trade_date,
                symbols=[item.symbol for item in symbols],
                source='pykrx',
                run_id=run_id,
                progress_callback=make_stage_progress_callback(
                    persist_stage_progress,
                    stage='action_summaries',
                    default_step='computing_action_summary',
                    default_activity='Computing daily action summaries',
                    include_current_symbol=True,
                ),
            )
            action_failed = int(action_summary_result.get('action_summaries_failed') or 0)
            action_succeeded = int(action_summary_result.get('action_summaries_persisted') or 0)
            action_total = action_succeeded + action_failed
            action_status = _completion_status_for_gap(failed=action_failed, total=action_total)
            persist_stage_progress(
                stage='action_summaries',
                status=action_status,
                current_step='action_summaries_completed',
                current_activity='Daily action summaries computed'
                if action_status == 'completed'
                else 'Daily action summaries completed with errors',
                processed=action_total,
                total=len(symbols),
                succeeded=action_succeeded,
                failed=action_failed,
                extra=_gap_policy_details(failed=action_failed, total=action_total),
            )
        except Exception as exc:
            action_summary_result = build_stage_failure_result(
                market_date=latest_trade_date,
                source='pykrx',
                error=exc,
                totals={
                    'requested_symbols': len(symbols),
                    'action_summaries_persisted': 0,
                    'action_summaries_failed': len(symbols),
                },
                failed_symbols=[item.symbol for item in symbols],
                items_key='items',
            )
            action_summary_error_message = f'Action summary persistence failed: {exc}'
            persist_stage_progress(
                stage='action_summaries',
                status='failed',
                current_step='action_summaries_failed',
                current_activity='Daily action summary persistence failed',
                processed=0,
                total=len(symbols),
                succeeded=0,
                failed=len(symbols),
            )
    else:
        action_summary_result = {
            'skipped': True,
            'reason': 'stage_not_requested',
            'market_date': latest_trade_date,
            'source': 'pykrx',
        }

    trending_rank_result: dict
    trending_rank_error_message: str | None = None
    if 'trending_ranks' in requested_stage_set:
        try:
            trending_rank_result = dependencies.compute_trending_ranks(
                market_date=latest_trade_date,
                source='pykrx',
                run_id=run_id,
                progress_callback=make_stage_progress_callback(
                    persist_stage_progress,
                    stage='trending_ranks',
                    default_step='computing_trending_period',
                    default_activity='Computing daily trending ranks',
                    include_current_period=True,
                ),
            )
            trending_failed = int(trending_rank_result.get('failed_count') or trending_rank_result.get('periods_failed') or 0)
            trending_succeeded = int(
                trending_rank_result.get('periods_persisted')
                or trending_rank_result.get('periods_completed')
                or (1 if int(trending_rank_result.get('trending_ranks_persisted') or 0) > 0 else 0)
            )
            trending_total = int(trending_rank_result.get('total') or trending_succeeded + trending_failed or 0)
            trending_status = _completion_status_for_gap(failed=trending_failed, total=trending_total)
            persist_stage_progress(
                stage='trending_ranks',
                status=trending_status,
                current_step='trending_ranks_completed',
                current_activity='Daily trending ranks computed'
                if trending_status == 'completed'
                else 'Daily trending ranks completed with errors',
                processed=trending_succeeded + trending_failed,
                total=trending_total,
                succeeded=trending_succeeded,
                failed=trending_failed,
                extra=_gap_policy_details(failed=trending_failed, total=trending_total),
            )
        except Exception as exc:
            trending_rank_result = build_stage_failure_result(
                market_date=latest_trade_date,
                source='pykrx',
                error=exc,
                totals={
                    'periods': [],
                    'trending_ranks_persisted': 0,
                },
            )
            trending_rank_error_message = f'Trending rank persistence failed: {exc}'
            persist_stage_progress(
                stage='trending_ranks',
                status='failed',
                current_step='trending_ranks_failed',
                current_activity='Daily trending rank persistence failed',
                processed=0,
                total=0,
                succeeded=0,
                failed=1,
                extra={
                    'error_type': type(exc).__name__,
                    'error_message': str(exc),
                },
            )
    else:
        trending_rank_result = {
            'skipped': True,
            'reason': 'stage_not_requested',
            'market_date': latest_trade_date,
            'source': 'pykrx',
        }

    signal_feature_result: dict
    signal_feature_error_message: str | None = None
    if 'signal_features' in requested_stage_set:
        try:
            persist_stage_progress(
                stage='signal_features',
                status='running',
                current_step='collecting_daily_signal_features',
                current_activity='Collecting daily KIS signal features',
                processed=0,
                total=3,
                succeeded=0,
                failed=0,
            )
            signal_feature_result = asyncio.run(
                dependencies.signal_feature_service.refresh_for_market_date(
                    market_date=latest_trade_date,
                )
            )
            signal_total = int(signal_feature_result.get('features_persisted', 0)) + int(
                signal_feature_result.get('features_failed', 0)
            )
            signal_source_errors = signal_feature_result.get('source_errors') or {}
            signal_failed = int(signal_feature_result.get('features_failed') or 0)
            signal_status = _completion_status_for_gap(failed=signal_failed, total=signal_total)
            if signal_source_errors:
                signal_status = 'partial'
            persist_stage_progress(
                stage='signal_features',
                status=signal_status,
                current_step='signal_features_completed',
                current_activity='Daily KIS signal features collected',
                processed=signal_total,
                total=signal_total,
                succeeded=signal_feature_result.get('features_persisted'),
                failed=signal_feature_result.get('features_failed'),
                extra={
                    'sources': signal_feature_result.get('sources'),
                    'source_counts': signal_feature_result.get('source_counts'),
                    'source_errors': signal_source_errors,
                    'coverage': signal_feature_result.get('coverage'),
                    **_gap_policy_details(failed=signal_failed, total=signal_total),
                },
            )
        except Exception as exc:
            signal_feature_result = build_stage_failure_result(
                market_date=latest_trade_date,
                source='kis_ranking',
                error=exc,
                totals={
                    'sources': [],
                    'source_counts': {},
                    'features_persisted': 0,
                    'features_failed': 0,
                },
                failed_symbols=[],
                items_key='items',
                extra={'participation_context': {'error': str(exc)}},
            )
            signal_feature_error_message = f'Daily signal feature collection failed: {exc}'
            persist_stage_progress(
                stage='signal_features',
                status='failed',
                current_step='signal_features_failed',
                current_activity='Daily KIS signal feature collection failed',
                processed=0,
                total=3,
                succeeded=0,
                failed=1,
                error_message=signal_feature_error_message,
                extra={
                    'error_type': type(exc).__name__,
                    'error_message': str(exc),
                },
            )
    else:
        signal_feature_result = {
            'skipped': True,
            'reason': 'stage_not_requested',
            'market_date': latest_trade_date,
            'source': 'kis_ranking',
        }

    market_risk_filter_result: dict
    market_risk_filter_error_message: str | None = None
    if 'market_risk_filter' in requested_stage_set:
        try:
            persist_stage_progress(
                stage='market_risk_filter',
                status='running',
                current_step='collecting_market_risk_snapshots_and_filters',
                current_activity='Collecting daily KIS risk snapshots and evaluating market risk filter',
                processed=0,
                total=len(price_context_symbols),
                succeeded=0,
                failed=0,
            )
            market_risk_filter_result = asyncio.run(
                dependencies.market_risk_filter_service.rebuild_for_market_date(
                    market_date=latest_trade_date,
                    symbols=price_context_symbols,
                    source=MARKET_RISK_FILTER_SOURCE,
                    run_id=run_id,
                )
            )
            market_risk_total = len(market_risk_filter_result.get('matched_symbols') or [])
            market_risk_failed = int(market_risk_filter_result.get('results_failed') or 0)
            market_risk_status = _completion_status_for_gap(failed=market_risk_failed, total=market_risk_total)
            persist_stage_progress(
                stage='market_risk_filter',
                status=market_risk_status,
                current_step='market_risk_filter_completed',
                current_activity='Daily KIS market risk filter computed',
                processed=(int(market_risk_filter_result.get('results_persisted') or 0) + market_risk_failed),
                total=market_risk_total,
                succeeded=market_risk_filter_result.get('results_persisted'),
                failed=market_risk_failed,
                extra={
                    'snapshot_results_failed': market_risk_filter_result.get('snapshot_collection', {}).get(
                        'snapshots_failed'
                    ),
                    'passed_count': market_risk_filter_result.get('summary', {}).get('passed_count'),
                    'watchlist_count': market_risk_filter_result.get('summary', {}).get('watchlist_count'),
                    **_gap_policy_details(failed=market_risk_failed, total=market_risk_total),
                },
            )
        except Exception as exc:
            market_risk_filter_result = build_stage_failure_result(
                market_date=latest_trade_date,
                source=MARKET_RISK_FILTER_SOURCE,
                error=exc,
                totals={
                    'results_persisted': 0,
                    'results_failed': len(price_context_symbols),
                },
                failed_symbols=price_context_symbols,
                items_key='items',
            )
            market_risk_filter_error_message = f'Market risk filter persistence failed: {exc}'
            persist_stage_progress(
                stage='market_risk_filter',
                status='failed',
                current_step='market_risk_filter_failed',
                current_activity='Daily KIS market risk filter persistence failed',
                processed=0,
                total=len(price_context_symbols),
                succeeded=0,
                failed=len(price_context_symbols),
            )
    else:
        market_risk_filter_result = {
            'skipped': True,
            'reason': 'stage_not_requested',
            'market_date': latest_trade_date,
            'source': MARKET_RISK_FILTER_SOURCE,
        }

    run_errors: list[str] = []
    if failed_symbols and not _is_non_blocking_gap(failed=len(failed_symbols), total=len(symbols)):
        run_errors.append(f'Failed symbols: {",".join(failed_symbols[:20])}')
    append_stage_error(
        run_errors,
        direct_error=price_context_error_message,
        result=price_context_result,
        failed_key='contexts_failed',
        incomplete_prefix='Price context persistence incomplete for symbols: ',
        exclude_symbols=failed_symbol_set,
        total_keys=('requested_symbols', 'contexts_persisted'),
        max_non_blocking_missing_ratio=MAX_NON_BLOCKING_STAGE_MISSING_RATIO,
    )
    append_stage_error(run_errors, direct_error=pykrx_enrichment_error_message)
    append_stage_error(
        run_errors,
        direct_error=snapshot_error_message,
        result=technical_snapshot_result,
        failed_key='snapshots_failed',
        incomplete_prefix='Technical snapshot persistence incomplete for symbols: ',
        total_keys=('requested_symbols', 'snapshots_persisted'),
        max_non_blocking_missing_ratio=MAX_NON_BLOCKING_STAGE_MISSING_RATIO,
    )
    append_stage_error(
        run_errors,
        direct_error=action_summary_error_message,
        result=action_summary_result,
        failed_key='action_summaries_failed',
        incomplete_prefix='Action summary persistence incomplete for symbols: ',
        total_keys=('requested_symbols', 'action_summaries_persisted'),
        max_non_blocking_missing_ratio=MAX_NON_BLOCKING_STAGE_MISSING_RATIO,
    )
    append_stage_error(run_errors, direct_error=trending_rank_error_message)
    append_stage_error(
        run_errors,
        direct_error=signal_feature_error_message,
        result=signal_feature_result,
        failed_key='features_failed',
        incomplete_prefix='Daily signal feature persistence incomplete for symbols: ',
        total_keys=('features_total', 'features_persisted'),
        max_non_blocking_missing_ratio=MAX_NON_BLOCKING_STAGE_MISSING_RATIO,
    )
    append_stage_error(
        run_errors,
        direct_error=market_risk_filter_error_message,
        result=market_risk_filter_result,
        failed_key='results_failed',
        incomplete_prefix='Market risk filter persistence incomplete for symbols: ',
        total_keys=('matched_symbols', 'matched_symbol_count', 'requested_symbol_count', 'results_persisted'),
        max_non_blocking_missing_ratio=MAX_NON_BLOCKING_STAGE_MISSING_RATIO,
    )

    status = 'completed' if not run_errors else 'completed_with_errors'
    if failed_symbols:
        run_metadata = service._run_metadata_with_failed_symbols(run_metadata, failed_symbols)
    dependencies.complete_eod_collection_run(
        run_id,
        status=status,
        symbols_succeeded=symbols_succeeded,
        symbols_failed=symbols_failed,
        error_message=' | '.join(run_errors) if run_errors else None,
        run_metadata=run_metadata,
    )
    result = {
        'run_id': run_id,
        'source': 'pykrx',
        'status': status,
        'selected_stages': list(selected_stages or PYKRX_DAILY_STAGE_ORDER),
        'markets': list(markets),
        'start_date': normalized_start,
        'requested_end_date': normalized_end,
        'latest_available_trade_date': latest_trade_date,
        'symbols_total': len(symbols),
        'symbols_succeeded': symbols_succeeded,
        'symbols_failed': symbols_failed,
        'failed_symbols': failed_symbols[:50],
        'bars_upserted': bars_upserted,
        'price_contexts': price_context_result,
        'pykrx_enrichment': pykrx_enrichment_result,
        'technical_snapshots': technical_snapshot_result,
        'action_summaries': action_summary_result,
        'trending_ranks': trending_rank_result,
        'signal_features': signal_feature_result,
        'market_risk_filter': market_risk_filter_result,
        'refresh_universe': effective_refresh_universe,
        'universe_snapshot': universe_snapshot,
    }

    def record_followup_start(stage_id: str, activity: str) -> None:
        persist_stage_progress(
            stage=stage_id,
            status='running',
            current_step=f'{stage_id}_running',
            current_activity=activity,
            processed=0,
            total=0,
            succeeded=0,
            failed=0,
        )

    def record_followup_result(
        stage_id: str,
        stage_result: dict[str, Any],
        *,
        default_total: int = 0,
    ) -> None:
        stage_status = _resolve_result_status(stage_result)
        counts = _extract_result_counts(stage_result, default_total=default_total)
        persist_stage_progress(
            stage=stage_id,
            status=stage_status,
            current_step=f'{stage_id}_{stage_status}',
            current_activity=f'{_status_activity_prefix(stage_status)} {stage_id.replace("_", " ")}',
            processed=counts['processed'],
            total=counts['total'],
            succeeded=counts['succeeded'],
            failed=counts['failed'],
            source_run_ids=(
                stage_result.get('source_run_ids')
                if isinstance(stage_result.get('source_run_ids'), dict)
                else None
            ),
            extra={'result_summary': _compact_result_summary(stage_result)},
        )

    def record_followup_failure(stage_id: str, exc: Exception) -> None:
        persist_stage_progress(
            stage=stage_id,
            status='failed',
            current_step=f'{stage_id}_failed',
            current_activity=f'{stage_id.replace("_", " ")} failed: {exc}',
            processed=0,
            total=0,
            succeeded=0,
            failed=1,
            extra={
                'error_type': type(exc).__name__,
                'error_message': str(exc),
            },
        )

    def run_followup_stage(
        stage_id: str,
        *,
        activity: str,
        call: Callable[[], dict[str, Any]],
        default_total: int = 0,
    ) -> dict[str, Any]:
        record_followup_start(stage_id, activity)
        try:
            stage_result = call()
        except Exception as exc:
            record_followup_failure(stage_id, exc)
            raise
        record_followup_result(stage_id, stage_result, default_total=default_total)
        return stage_result

    def fundamentals_progress_callback(**payload: Any) -> None:
        details = payload.get('details') if isinstance(payload.get('details'), dict) else {}
        source_run_ids = payload.get('source_run_ids') if isinstance(payload.get('source_run_ids'), dict) else None
        persist_stage_progress(
            stage='fundamentals_daily_refresh',
            status=str(payload.get('status') or 'running'),
            current_step=str(payload.get('current_step') or 'fundamentals_daily_refresh_running'),
            current_activity=str(payload.get('current_activity') or 'Running DART fundamentals daily refresh'),
            processed=payload.get('processed'),
            total=payload.get('total'),
            succeeded=payload.get('succeeded'),
            failed=payload.get('failed'),
            current_symbol=payload.get('current_symbol'),
            current_period=payload.get('current_period'),
            source_run_ids=source_run_ids,
            extra=details,
        )

    def feedback_progress_callback(**payload: Any) -> None:
        details = payload.get('details') if isinstance(payload.get('details'), dict) else {}
        source_run_ids = payload.get('source_run_ids') if isinstance(payload.get('source_run_ids'), dict) else None
        persist_stage_progress(
            stage='agent_feedback_summaries',
            status=str(payload.get('status') or 'running'),
            current_step=str(payload.get('current_step') or 'agent_feedback_summaries_running'),
            current_activity=str(payload.get('current_activity') or 'Running agent feedback summary follow-up'),
            processed=payload.get('processed'),
            total=payload.get('total'),
            succeeded=payload.get('succeeded'),
            failed=payload.get('failed'),
            current_period=payload.get('current_period'),
            source_run_ids=source_run_ids,
            extra=details,
        )

    def signal_decider_progress_callback(**payload: Any) -> None:
        details = payload.get('details') if isinstance(payload.get('details'), dict) else {}
        source_run_ids = payload.get('source_run_ids') if isinstance(payload.get('source_run_ids'), dict) else None
        persist_stage_progress(
            stage='signal_decider',
            status=str(payload.get('status') or 'running'),
            current_step=str(payload.get('current_step') or 'signal_decider_running'),
            current_activity=str(payload.get('current_activity') or 'Running signal-decider follow-up'),
            processed=payload.get('processed'),
            total=payload.get('total'),
            succeeded=payload.get('succeeded'),
            failed=payload.get('failed'),
            current_period=payload.get('current_period'),
            source_run_ids=source_run_ids,
            extra=details,
        )

    def record_factor_followup_result(factor_result: dict[str, Any]) -> None:
        stage_results = factor_result.get('stages') if isinstance(factor_result.get('stages'), dict) else {}
        for stage_id in FACTOR_FOLLOWUP_STAGE_ORDER:
            raw_stage_result = stage_results.get(stage_id) if isinstance(stage_results.get(stage_id), dict) else {}
            nested_result = (
                raw_stage_result.get('result')
                if isinstance(raw_stage_result.get('result'), dict)
                else {}
            )
            stage_status = (
                _normalize_stage_status(raw_stage_result.get('status'))
                if raw_stage_result
                else _resolve_result_status(factor_result)
            )
            if factor_result.get('triggered') is True and not raw_stage_result:
                stage_status = 'failed'
                raw_stage_result = {
                    'status': 'failed',
                    'reason': 'factor_followup_missing_stage_result',
                    'market_date': factor_result.get('market_date'),
                }
            counts = _extract_result_counts(nested_result, default_total=0)
            if stage_status == 'failed' and counts['failed'] <= 0:
                counts['failed'] = 1
                counts['processed'] = max(counts['processed'], 1)
                counts['total'] = max(counts['total'], 1)
            persist_stage_progress(
                stage=stage_id,
                status=stage_status,
                current_step=f'{stage_id}_{stage_status}',
                current_activity=f'{_status_activity_prefix(stage_status)} {stage_id.replace("_", " ")}',
                processed=counts['processed'],
                total=counts['total'],
                succeeded=counts['succeeded'],
                failed=counts['failed'],
                extra={
                    'factor_scoring_followup_status': factor_result.get('status'),
                    'result_summary': _compact_result_summary(nested_result or raw_stage_result),
                },
            )

    def build_blocked_followup_result(stage_id: str, reason: str, *, upstream_status: str | None = None) -> dict[str, Any]:
        return {
            'triggered': False,
            'status': 'blocked',
            'reason': reason,
            'market_date': latest_trade_date,
            'upstream_status': upstream_status,
            'stage': stage_id,
        }

    def run_factor_followup() -> dict[str, Any]:
        record_followup_start('momentum_factors', 'Running daily factor scoring follow-up')
        try:
            factor_result = _run_daily_factor_followup(market_date=latest_trade_date)
        except Exception as exc:
            record_followup_failure('momentum_factors', exc)
            raise
        record_factor_followup_result(factor_result)
        return factor_result

    dart_auto_followup_enabled = bool((dependencies.settings.dart_api_key or '').strip())
    should_chain_fundamentals = (
        chain_fundamentals_followup
        and 'market_risk_filter' in requested_stage_set
        and dart_auto_followup_enabled
        and mode == 'daily_auto'
        and status in {'completed', 'completed_with_errors'}
    )
    try:
        if should_chain_fundamentals:
            result['fundamentals_followup'] = run_followup_stage(
                'fundamentals_daily_refresh',
                activity='Running DART fundamentals daily refresh follow-up',
                call=lambda: asyncio.run(
                    dependencies.dart_fundamentals_capture_service.run_daily_incremental_refresh(
                        market_date=latest_trade_date,
                        capture_limit=500,
                        trigger='eod_pykrx_followup',
                        run_feedback_followup=False,
                        progress_callback=fundamentals_progress_callback,
                    )
                ),
            )
            fundamentals_status = _resolve_result_status(result['fundamentals_followup'])
            if _is_satisfied_followup_result(result['fundamentals_followup']):
                result['factor_scoring_followup'] = run_factor_followup()
                result['external_news_followup'] = run_followup_stage(
                    'external_news_collect',
                    activity='Running external news collection follow-up',
                    call=lambda: _run_daily_external_news_followup(
                        market_date=latest_trade_date,
                        enabled=True,
                    ),
                )
            else:
                result['factor_scoring_followup'] = {
                    'triggered': False,
                    'status': 'blocked',
                    'reason': 'fundamentals_followup_incomplete',
                    'market_date': latest_trade_date,
                    'fundamentals_status': fundamentals_status or None,
                }
                record_factor_followup_result(result['factor_scoring_followup'])
                result['external_news_followup'] = {
                    'triggered': False,
                    'reason': 'factor_scoring_followup_incomplete',
                    'market_date': latest_trade_date,
                    'fundamentals_status': fundamentals_status or None,
                }
                record_followup_result('external_news_collect', result['external_news_followup'])
            factor_status = _resolve_result_status(result.get('factor_scoring_followup') or {})
            result['intraday_snapshot_followup'] = _run_daily_intraday_snapshot_followup(
                market_date=latest_trade_date,
                enabled=bool(dependencies.settings.kis_intraday_snapshot_enabled),
            )
            if factor_status in {'completed', 'partial'}:
                result['feedback_followup'] = run_followup_stage(
                    'agent_feedback_summaries',
                    activity='Running agent feedback summary follow-up',
                    call=lambda: _run_daily_feedback_followup(
                        market_date=latest_trade_date,
                        enabled=bool(dependencies.settings.openclaw_feedback_trigger_after_fundamentals_refresh),
                        progress_callback=feedback_progress_callback,
                    ),
                )
                feedback_status = _resolve_result_status(result['feedback_followup'])
                if feedback_status in {'completed', 'partial'}:
                    result['signal_decider_followup'] = run_followup_stage(
                        'signal_decider',
                        activity='Running signal-decider follow-up',
                        call=lambda: _run_daily_signal_decider_followup(
                            market_date=latest_trade_date,
                            enabled=bool(dependencies.settings.openclaw_feedback_trigger_after_fundamentals_refresh),
                            progress_callback=signal_decider_progress_callback,
                        ),
                    )
                    signal_decider_status = _resolve_result_status(result['signal_decider_followup'])
                else:
                    result['signal_decider_followup'] = build_blocked_followup_result(
                        'signal_decider',
                        'feedback_followup_incomplete',
                        upstream_status=feedback_status,
                    )
                    record_followup_result('signal_decider', result['signal_decider_followup'])
                    signal_decider_status = 'blocked'
                if signal_decider_status in {'completed', 'partial'}:
                    result['signal_decider_evaluation_followup'] = run_followup_stage(
                        'signal_decider_evaluations',
                        activity='Running signal-decider evaluation follow-up',
                        call=lambda: _run_daily_signal_decider_evaluation_followup(
                            market_date=latest_trade_date,
                            enabled=True,
                        ),
                    )
                else:
                    result['signal_decider_evaluation_followup'] = build_blocked_followup_result(
                        'signal_decider_evaluations',
                        'signal_decider_followup_incomplete',
                        upstream_status=signal_decider_status,
                    )
                    record_followup_result(
                        'signal_decider_evaluations',
                        result['signal_decider_evaluation_followup'],
                    )
            else:
                result['feedback_followup'] = build_blocked_followup_result(
                    'agent_feedback_summaries',
                    'factor_scoring_followup_incomplete',
                    upstream_status=factor_status,
                )
                record_followup_result('agent_feedback_summaries', result['feedback_followup'])
                result['signal_decider_followup'] = build_blocked_followup_result(
                    'signal_decider',
                    'factor_scoring_followup_incomplete',
                    upstream_status=factor_status,
                )
                record_followup_result('signal_decider', result['signal_decider_followup'])
                result['signal_decider_evaluation_followup'] = build_blocked_followup_result(
                    'signal_decider_evaluations',
                    'factor_scoring_followup_incomplete',
                    upstream_status=factor_status,
                )
                record_followup_result(
                    'signal_decider_evaluations',
                    result['signal_decider_evaluation_followup'],
                )
        elif chain_fundamentals_followup and mode == 'daily_auto' and status in {'completed', 'completed_with_errors'}:
            result['fundamentals_followup'] = {
                'triggered': False,
                'reason': 'dart_api_key_not_configured',
                'market_date': latest_trade_date,
            }
            record_followup_result('fundamentals_daily_refresh', result['fundamentals_followup'])
            result['factor_scoring_followup'] = {
                'triggered': False,
                'status': 'blocked',
                'reason': 'dart_api_key_not_configured',
                'market_date': latest_trade_date,
            }
            record_factor_followup_result(result['factor_scoring_followup'])
            result['external_news_followup'] = run_followup_stage(
                'external_news_collect',
                activity='Running external news collection follow-up',
                call=lambda: _run_daily_external_news_followup(
                    market_date=latest_trade_date,
                    enabled=True,
                ),
            )
            result['intraday_snapshot_followup'] = _run_daily_intraday_snapshot_followup(
                market_date=latest_trade_date,
                enabled=bool(dependencies.settings.kis_intraday_snapshot_enabled),
            )
            result['feedback_followup'] = build_blocked_followup_result(
                'agent_feedback_summaries',
                'factor_scoring_followup_incomplete',
                upstream_status='blocked',
            )
            record_followup_result('agent_feedback_summaries', result['feedback_followup'])
            result['signal_decider_followup'] = build_blocked_followup_result(
                'signal_decider',
                'factor_scoring_followup_incomplete',
                upstream_status='blocked',
            )
            record_followup_result('signal_decider', result['signal_decider_followup'])
            result['signal_decider_evaluation_followup'] = build_blocked_followup_result(
                'signal_decider_evaluations',
                'factor_scoring_followup_incomplete',
                upstream_status='blocked',
            )
            record_followup_result(
                'signal_decider_evaluations',
                result['signal_decider_evaluation_followup'],
            )
    except Exception as exc:
        daily_pipeline_tracker.finish_run(status='failed', error_message=str(exc))
        raise
    daily_pipeline_tracker.finish_run()
    if retry_context is not None:
        result['retry'] = retry_context
    dependencies.log_fetch('run_collection', status, **{key: value for key, value in result.items() if key != 'status'})
    return result
