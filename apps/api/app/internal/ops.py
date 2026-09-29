from __future__ import annotations

import argparse
import asyncio
import json
from datetime import date
from pathlib import Path
from typing import Any, Iterable

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / '.env', override=False)

from app.agent_context.derived_contexts import derived_context_backfill_service
from app.agent_context.missing_data_report_service import agent_context_missing_data_report_service
from app.agent_context.orchestrator import agent_feedback_orchestrator
from app.agent_context.prerequisite_service import agent_feedback_prerequisite_service
from app.agent_context.retry_service import EXPECTED_ROLES, FeedbackRetryCycleConfig, agent_feedback_retry_service
from app.agent_context.run_modes import FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH, FEEDBACK_SCOPE_MODES
from app.agent_context.session_cleanup_service import agent_feedback_session_cleanup_service
from app.agent_context.signal_decider_evaluation_maintenance_service import (
    signal_decider_evaluation_maintenance_service,
)
from app.agent_context.signal_decider_calendar_review_service import signal_decider_calendar_review_service
from app.agent_context.decider_replay_service import decider_replay_service
from app.agent_context.signal_decider_loop_service import (
    DEFAULT_SIGNAL_DECIDER_EVALUATION_LOOKBACK_DAYS,
    signal_decider_loop_service,
)
from app.core.settings import settings
from app.dart.fundamentals_capture_service import dart_fundamentals_capture_service
from app.dart.signals_service import dart_signals_service
from app.db import init_database
from app.eod.action_summaries import daily_action_summary_service
from app.eod.auto_pykrx import auto_pykrx_eod_scheduler
from app.eod.daily_pipeline_stages import daily_pipeline_stage_service
from app.eod.daily_pipeline_tracking import (
    build_prior_date_current_only_stage_skip,
    run_tracked_market_risk_filter,
)
from app.eod.master import refresh_master_if_due
from app.eod.master_state import master_refresh_state
from app.eod.pykrx_backfill import DEFAULT_MARKETS, PYKRX_BACKFILL_SERVICE
from app.eod.pykrx_enrichment import pykrx_enrichment_service
from app.eod.state import job_state
from app.eod.technical_snapshots import daily_technical_snapshot_service
from app.external_news.service import external_news_service
from app.internal.deploy_hygiene import collect_schema_drift_preflight
from app.market.clock import get_market_state
from app.market.kis_intraday_snapshots import KIS_INTRADAY_SNAPSHOT_SOURCE, kis_intraday_snapshot_service
from app.market_data_store import (
    compute_and_persist_daily_trending_ranks,
    load_tracked_symbols,
    persist_market_status_snapshot,
)
from app.orders.exec_payload_slimming import cleanup_signal_decider_exec_submission_payloads
from app.orders.exec_integration import signal_decider_exec_integration_service
from app.orders.intraday_auto_sell import signal_decider_intraday_auto_sell_service
from app.orders.next_open_execution import signal_decider_next_open_execution_service
from app.orders.reconciliation import trading_order_reconciliation_service
from app.orders.signal_decider_execution_outcomes import signal_decider_execution_outcome_service
from app.quotes.service import QuoteService
from app.signals.advanced_value_factor import (
    ADVANCED_VALUE_FACTOR_SOURCE,
    daily_advanced_value_factor_service,
)
from app.signals.beta_factor import BETA_FACTOR_SOURCE, daily_beta_factor_service
from app.signals.composite_factor import COMPOSITE_FACTOR_SOURCE, daily_composite_factor_service
from app.signals.low_risk_factor import LOW_RISK_SOURCE, daily_low_risk_factor_service
from app.signals.market_risk_filter import (
    MARKET_RISK_FILTER_SOURCE,
    daily_market_risk_filter_service,
)
from app.signals.market_risk_snapshot import (
    MARKET_RISK_SNAPSHOT_SOURCE,
    daily_market_risk_snapshot_service,
)
from app.signals.momentum_factor import daily_momentum_factor_service
from app.signals.piotroski_filter import daily_piotroski_filter_service
from app.signals.quality_factor import QUALITY_FACTOR_SOURCE, daily_quality_factor_service
from app.signals.unified_value_factor import (
    UNIFIED_VALUE_FACTOR_SOURCE,
    daily_unified_value_factor_service,
)
from app.signals.value_factor import VALUE_FACTOR_SOURCE, daily_value_factor_service
from app.scanner.state import scanner_state
from app.signals.daily_signal_features import daily_signal_feature_service


def bootstrap_runtime() -> None:
    init_database()
    scanner_state.hydrate(load_tracked_symbols())


def get_market_status() -> dict[str, Any]:
    state = get_market_state()
    persist_market_status_snapshot(state, source='internal_ops')
    return state.__dict__


async def get_tracked_quotes() -> dict[str, Any]:
    bootstrap_runtime()
    return await QuoteService().get_tracked_quotes()


def get_tracked_symbols() -> dict[str, Any]:
    bootstrap_runtime()
    return scanner_state.snapshot()


def get_eod_status() -> dict[str, Any]:
    bootstrap_runtime()
    return {
        'job': job_state.snapshot(),
        'master': master_refresh_state.snapshot(),
        'pykrx_auto': auto_pykrx_eod_scheduler.snapshot(),
    }


def slim_signal_decider_exec_payloads(
    *,
    dry_run: bool = True,
    target_trade_date: str | None = None,
    min_payload_bytes: int = 64 * 1024,
    max_payload_bytes: int = 24 * 1024,
    batch_size: int = 100,
    limit: int | None = None,
) -> dict[str, Any]:
    bootstrap_runtime()
    return cleanup_signal_decider_exec_submission_payloads(
        dry_run=dry_run,
        target_trade_date=target_trade_date,
        min_payload_bytes=min_payload_bytes,
        max_payload_bytes=max_payload_bytes,
        batch_size=batch_size,
        limit=limit,
    )


def run_pykrx_daily_if_due(
    *,
    market_date: str | None = None,
    markets: str | None = None,
    force: bool = False,
    refresh_universe: bool | None = None,
) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_markets = tuple(item.strip().upper() for item in (markets or settings.eod_auto_pykrx_markets).split(',') if item.strip())
    return PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due(
        market_date=market_date or get_market_state().server_time[:10],
        markets=parsed_markets or DEFAULT_MARKETS,
        trigger='internal_ops_cli',
        refresh_universe=settings.eod_auto_pykrx_refresh_universe if refresh_universe is None else refresh_universe,
        force=force,
    )


def backfill_pykrx_daily_bar_fundamentals(
    *,
    start_date: str,
    end_date: str | None = None,
    markets: str | None = None,
    limit: int | None = None,
    symbol: str | None = None,
) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_markets = tuple(item.strip().upper() for item in (markets or settings.eod_auto_pykrx_markets).split(',') if item.strip())
    return PYKRX_BACKFILL_SERVICE.backfill_daily_bar_fundamentals(
        start_date=start_date,
        end_date=end_date,
        markets=parsed_markets or DEFAULT_MARKETS,
        limit=limit,
        symbol=symbol,
    )


def collect_pykrx_enrichment(
    *,
    market_date: str,
    markets: str | None = None,
    symbols: str | None = None,
    categories: str | None = None,
) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_markets = [item.strip().upper() for item in (markets or settings.eod_auto_pykrx_markets).split(',') if item.strip()]
    parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
    parsed_categories = [item.strip() for item in categories.split(',') if item.strip()] if categories else None
    return pykrx_enrichment_service.collect_for_market_date(
        market_date=market_date,
        markets=parsed_markets or list(DEFAULT_MARKETS),
        symbols=parsed_symbols,
        categories=parsed_categories,
    )


def inspect_pykrx_enrichment(
    *,
    market_date: str,
    markets: str | None = None,
    symbols: str | None = None,
    categories: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_markets = [item.strip().upper() for item in markets.split(',') if item.strip()] if markets else None
    parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
    parsed_categories = [item.strip() for item in categories.split(',') if item.strip()] if categories else None
    return pykrx_enrichment_service.inspect(
        market_date=market_date,
        markets=parsed_markets,
        symbols=parsed_symbols,
        categories=parsed_categories,
        limit=limit,
    )


def run_dart_fundamentals_daily_if_due(
    *,
    market_date: str | None = None,
    symbol_limit: int | None = None,
    symbol_offset: int = 0,
    capture_limit: int = 500,
    retry_cycles: int | None = None,
    force: bool = False,
    fs_div: str | None = None,
) -> dict[str, Any]:
    bootstrap_runtime()
    return asyncio.run(
        dart_fundamentals_capture_service.run_daily_incremental_refresh(
            market_date=market_date,
            symbol_limit=symbol_limit,
            symbol_offset=symbol_offset,
            capture_limit=capture_limit,
            retry_cycles=retry_cycles,
            force=force,
            fs_div=fs_div,
            trigger='internal_ops_cli',
        )
    )


def capture_dart_signals(*, rcept_no: str) -> dict[str, Any]:
    bootstrap_runtime()
    return asyncio.run(dart_signals_service.capture_signals_for_event(rcept_no=rcept_no))


def backfill_dart_audit_signals(
    *,
    market_date: str,
    limit: int = 25,
    after_event_id: int = 0,
    dry_run: bool = True,
) -> dict[str, Any]:
    bootstrap_runtime()
    return asyncio.run(
        dart_signals_service.backfill_missing_annual_audit_signals(
            market_date=date.fromisoformat(market_date),
            limit=limit,
            after_event_id=after_event_id,
            dry_run=dry_run,
        )
    )


def collect_kis_daily_signal_features(
    *,
    market_date: str | None = None,
    sources: str | None = None,
    max_pages: int = 10,
    overtime_volume_rank_sort_cls_code: str = '2',
    overtime_fluctuation_div_cls_code: str = '2',
) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_sources = [item.strip() for item in sources.split(',') if item.strip()] if sources else None
    return asyncio.run(
        daily_signal_feature_service.refresh_for_market_date(
            market_date=market_date or get_market_state().server_time[:10],
            sources=parsed_sources,
            max_pages=max_pages,
            overtime_volume_rank_sort_cls_code=overtime_volume_rank_sort_cls_code,
            overtime_fluctuation_div_cls_code=overtime_fluctuation_div_cls_code,
        )
    )


def collect_kis_intraday_snapshots(
    *,
    symbols: str | None = None,
    market_code: str | None = None,
    source: str = KIS_INTRADAY_SNAPSHOT_SOURCE,
    max_ranking_pages: int | None = None,
    max_news_items: int | None = None,
    include_news: bool = True,
) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
    return asyncio.run(
        kis_intraday_snapshot_service.collect_snapshots(
            symbols=parsed_symbols,
            market_code=market_code,
            source=source,
            max_ranking_pages=max_ranking_pages,
            max_news_items=max_news_items,
            include_news=include_news,
        )
    )


def collect_external_news(
    *,
    symbols: str,
    display: int | None = None,
) -> dict[str, Any]:
    bootstrap_runtime()
    return asyncio.run(external_news_service.collect_for_symbols(symbols=symbols, display=display))


def list_external_news(
    *,
    symbols: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    bootstrap_runtime()
    return external_news_service.list_news(symbols=symbols, limit=limit)


def collect_daily_piotroski_filter_results(
    *,
    market_date: str | None = None,
) -> dict[str, Any]:
    bootstrap_runtime()
    return daily_piotroski_filter_service.rebuild_for_market_date(
        market_date=market_date or get_market_state().server_time[:10],
    )


def list_daily_pipeline_stages() -> dict[str, Any]:
    bootstrap_runtime()
    return daily_pipeline_stage_service.describe()


def run_daily_pipeline_stages(
    *,
    market_date: str | None = None,
    stages: str | None = None,
    symbols: str | None = None,
    markets: str | None = None,
    force: bool = False,
    refresh_universe: bool | None = None,
    fs_div: str | None = None,
    symbol_limit: int | None = None,
    symbol_offset: int = 0,
    capture_limit: int = 500,
    retry_cycles: int | None = None,
    feedback_max_cycles: int = 3,
    feedback_outer_batch_size: int = 20,
    reuse_existing_feedback: bool = True,
    resume_from_stage: str | None = None,
) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_stages = [item.strip() for item in stages.split(',') if item.strip()] if stages else None
    parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
    parsed_markets = [item.strip() for item in markets.split(',') if item.strip()] if markets else None
    return daily_pipeline_stage_service.run(
        market_date=market_date,
        stages=parsed_stages,
        symbols=parsed_symbols,
        markets=parsed_markets,
        force=force,
        refresh_universe=refresh_universe,
        fs_div=fs_div,
        symbol_limit=symbol_limit,
        symbol_offset=symbol_offset,
        capture_limit=capture_limit,
        retry_cycles=retry_cycles,
        feedback_max_cycles=feedback_max_cycles,
        feedback_outer_batch_size=feedback_outer_batch_size,
        reuse_existing_feedback=reuse_existing_feedback,
        resume_from_stage=resume_from_stage,
        trigger='internal_ops_cli',
    )


def compute_signal_decider_evaluations(
    *,
    market_date: str,
    lookback_days: int = DEFAULT_SIGNAL_DECIDER_EVALUATION_LOOKBACK_DAYS,
    symbols: str | None = None,
    horizons: str | None = None,
    evaluation_version: str | None = None,
    force_recompute: bool = False,
) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
    parsed_horizons = [int(item.strip()) for item in horizons.split(',') if item.strip()] if horizons else None
    prerequisites = agent_feedback_prerequisite_service.inspect_feedback_prerequisites(
        market_date=market_date,
    )
    if not force_recompute and not prerequisites['ready']:
        return {
            'request': {
                'as_of_market_date': market_date,
                'lookback_days': lookback_days,
                'symbols': parsed_symbols,
                'horizons': parsed_horizons,
                'evaluation_version': evaluation_version,
                'force_recompute': force_recompute,
            },
            'summary': {
                'status': prerequisites['status'],
                'gated': True,
                'outcomes_upserted': 0,
                'evaluations_upserted': 0,
            },
            'items': [],
            'prerequisites': prerequisites,
        }
    return signal_decider_loop_service.compute_recent_evaluations(
        as_of_market_date=market_date,
        lookback_days=lookback_days,
        symbols=parsed_symbols,
        horizons=parsed_horizons,
        evaluation_version=evaluation_version,
        force_recompute=force_recompute,
    )


def refresh_signal_decider_calendar_reviews(
    *,
    market_date: str,
    lookback_days: int = DEFAULT_SIGNAL_DECIDER_EVALUATION_LOOKBACK_DAYS,
    symbols: str | None = None,
    horizons: str | None = None,
    primary_horizons: str | None = None,
    evaluation_version: str | None = None,
    force_recompute: bool = False,
) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
    parsed_horizons = [int(item.strip()) for item in horizons.split(',') if item.strip()] if horizons else None
    parsed_primary_horizons = (
        [int(item.strip()) for item in primary_horizons.split(',') if item.strip()]
        if primary_horizons
        else None
    )
    return signal_decider_calendar_review_service.refresh_recent_reviews(
        as_of_market_date=market_date,
        lookback_days=lookback_days,
        symbols=parsed_symbols,
        horizons=parsed_horizons,
        primary_horizons=parsed_primary_horizons,
        evaluation_version=evaluation_version,
        force_recompute=force_recompute,
        trigger='internal_ops_cli',
    )


def repair_signal_decider_evaluation_coverage(
    *,
    market_date_from: str,
    market_date_to: str,
    symbols: str | None = None,
    horizons: str | None = None,
    primary_horizons: str | None = None,
    evaluation_version: str | None = None,
    force_recompute: bool = True,
    refresh_calendar: bool = True,
    fail_on_mature_gaps: bool = True,
) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
    parsed_horizons = [int(item.strip()) for item in horizons.split(',') if item.strip()] if horizons else None
    parsed_primary_horizons = (
        [int(item.strip()) for item in primary_horizons.split(',') if item.strip()]
        if primary_horizons
        else None
    )
    return signal_decider_evaluation_maintenance_service.repair_coverage_range(
        market_date_from=market_date_from,
        market_date_to=market_date_to,
        symbols=parsed_symbols,
        horizons=parsed_horizons,
        primary_horizons=parsed_primary_horizons,
        evaluation_version=evaluation_version,
        force_recompute=force_recompute,
        refresh_calendar=refresh_calendar,
        fail_on_mature_gaps=fail_on_mature_gaps,
        trigger='internal_ops_cli',
    )


def replay_decider_candidate_rules(
    *,
    market_date_from: str,
    market_date_to: str,
    agent_name: str,
    rule_config_json: str | None = None,
    rule_config_path: str | None = None,
    symbols: str | None = None,
    policy_versions: str | None = None,
    fee_bps: str | float | int = 0,
    slippage_bps: str | float | int = 0,
    notional_krw: str | float | int = 1_000_000,
    horizon_days: int = 5,
    top_examples_limit: int = 10,
) -> dict[str, Any]:
    bootstrap_runtime()
    if rule_config_path:
        rule_config = Path(rule_config_path).read_text()
    else:
        rule_config = rule_config_json or '{}'
    parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
    parsed_policy_versions = (
        [item.strip() for item in policy_versions.split(',') if item.strip()]
        if policy_versions
        else None
    )
    return decider_replay_service.replay(
        market_date_from=market_date_from,
        market_date_to=market_date_to,
        agent_name=agent_name,
        symbols=parsed_symbols,
        policy_versions=parsed_policy_versions,
        rule_config=rule_config,
        fee_bps=fee_bps,
        slippage_bps=slippage_bps,
        notional_krw=notional_krw,
        horizon_days=horizon_days,
        top_examples_limit=top_examples_limit,
    )


def replay_short_term_wait_conflict_gate(
    *,
    market_date_from: str | None = None,
    market_date_to: str | None = None,
    completed_weeks: int = 4,
    as_of_date_kst: str | None = None,
    symbols: str | None = None,
    fee_bps: str | float | int = 0,
    slippage_bps: str | float | int = 0,
    notional_krw: str | float | int = 1_000_000,
    gate_mode: str = 'hard_block',
    probe_quantity: int = 1,
    top_examples_limit: int = 10,
) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
    return decider_replay_service.replay_wait_conflict_gate(
        market_date_from=market_date_from,
        market_date_to=market_date_to,
        completed_weeks=completed_weeks,
        as_of_date_kst=as_of_date_kst,
        symbols=parsed_symbols,
        fee_bps=fee_bps,
        slippage_bps=slippage_bps,
        notional_krw=notional_krw,
        gate_mode=gate_mode,
        probe_quantity=probe_quantity,
        top_examples_limit=top_examples_limit,
    )


def collect_market_risk_snapshots(
    *,
    market_date: str,
    symbols: str | None = None,
    source: str = MARKET_RISK_SNAPSHOT_SOURCE,
    market_code: str = 'J',
) -> dict[str, Any]:
    bootstrap_runtime()
    market_state = get_market_state()
    safety_skip = build_prior_date_current_only_stage_skip(
        stage_id='market_risk_snapshots',
        market_date=market_date,
        current_market_date=str(market_state.server_time)[:10],
        market=market_state,
    )
    if safety_skip is not None:
        return safety_skip
    parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
    return asyncio.run(
        daily_market_risk_snapshot_service.collect_for_market_date(
            market_date=market_date,
            symbols=parsed_symbols,
            source=source,
            market_code=market_code,
        )
    )


def compute_market_risk_filters(
    *,
    market_date: str,
    symbols: str | None = None,
    source: str = MARKET_RISK_FILTER_SOURCE,
    market_code: str = 'J',
) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
    return asyncio.run(
        run_tracked_market_risk_filter(
            market_date=market_date,
            symbols=parsed_symbols,
            source=source,
            market_code=market_code,
            trigger='internal_ops_cli_stage',
            entrypoint='python -m app.internal.ops eod-market-risk-filters-compute',
            filter_service=daily_market_risk_filter_service,
        )
    )


def compute_low_risk_factors(*, market_date: str, symbols: str | None = None) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
    return daily_low_risk_factor_service.rebuild_for_market_date(
        market_date=market_date,
        symbols=parsed_symbols,
        source=LOW_RISK_SOURCE,
    )


def compute_beta_factors(*, market_date: str, symbols: str | None = None) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
    return daily_beta_factor_service.rebuild_for_market_date(
        market_date=market_date,
        symbols=parsed_symbols,
        source=BETA_FACTOR_SOURCE,
    )


def compute_momentum_factors(*, market_date: str, symbols: str | None = None) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
    return daily_momentum_factor_service.rebuild_for_market_date(
        market_date=market_date,
        symbols=parsed_symbols,
    )


def compute_value_factors(*, market_date: str, symbols: str | None = None) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
    return daily_value_factor_service.rebuild_for_market_date(
        market_date=market_date,
        symbols=parsed_symbols,
        source=VALUE_FACTOR_SOURCE,
    )


def compute_advanced_value_factors(*, market_date: str, symbols: str | None = None) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
    return daily_advanced_value_factor_service.rebuild_for_market_date(
        market_date=market_date,
        symbols=parsed_symbols,
        source=ADVANCED_VALUE_FACTOR_SOURCE,
    )


def compute_unified_value_factors(*, market_date: str, symbols: str | None = None) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
    return daily_unified_value_factor_service.rebuild_for_market_date(
        market_date=market_date,
        symbols=parsed_symbols,
        source=UNIFIED_VALUE_FACTOR_SOURCE,
    )


def compute_quality_factors(*, market_date: str, symbols: str | None = None) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
    return daily_quality_factor_service.rebuild_for_market_date(
        market_date=market_date,
        symbols=parsed_symbols,
        source=QUALITY_FACTOR_SOURCE,
    )


def compute_composite_factors(*, market_date: str, symbols: str | None = None) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
    return daily_composite_factor_service.rebuild_for_market_date(
        market_date=market_date,
        symbols=parsed_symbols,
        source=COMPOSITE_FACTOR_SOURCE,
    )


def refresh_master_if_due_internal(*, force: bool = False) -> dict[str, Any]:
    bootstrap_runtime()
    symbols = refresh_master_if_due(force=force)
    return {
        'refreshed': True,
        'symbol_count': len(symbols),
        'master': master_refresh_state.snapshot(),
    }


def compute_technical_snapshots(*, market_date: str, symbols: str | None = None) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
    return daily_technical_snapshot_service.rebuild_for_market_date(
        market_date=market_date,
        symbols=parsed_symbols,
        source='internal_ops_cli',
    )


def compute_action_summaries(*, market_date: str, symbols: str | None = None) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
    return daily_action_summary_service.rebuild_for_market_date(
        market_date=market_date,
        symbols=parsed_symbols,
        source='internal_ops_cli',
    )


def compute_trending_ranks(*, market_date: str, periods: str | None = None) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_periods = [item.strip().lower() for item in periods.split(',') if item.strip()] if periods else None
    return compute_and_persist_daily_trending_ranks(
        market_date=market_date,
        periods=parsed_periods,
        source='internal_ops_cli',
    )


def run_agent_feedback_retry_cycles(
    *,
    market_date: str,
    symbols: str | None = None,
    roles: str | None = None,
    feedback_scope_mode: str = FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
    max_cycles: int = 3,
    outer_batch_size: int = 20,
    reuse_existing: bool = True,
    force: bool = False,
    force_recompute: bool = False,
) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_symbols = tuple(item.strip() for item in symbols.split(',') if item.strip()) if symbols else None
    parsed_roles = tuple(item.strip() for item in roles.split(',') if item.strip()) if roles else EXPECTED_ROLES
    return agent_feedback_retry_service.run_retry_cycles(
        config=FeedbackRetryCycleConfig(
            market_date=__import__('datetime').date.fromisoformat(market_date),
            roles=parsed_roles,
            symbols=parsed_symbols,
            feedback_scope_mode=feedback_scope_mode,
            max_cycles=max_cycles,
            outer_batch_size=outer_batch_size,
            reuse_existing=reuse_existing,
            force_recompute=force_recompute,
        ),
        force=force,
    )


def get_agent_feedback_data_gap_summary(
    *,
    market_date: str | None = None,
    market_date_from: str | None = None,
    market_date_to: str | None = None,
    symbols: str | None = None,
    agent_name: str | None = None,
    categories: str | None = None,
    persist_missing_categories: bool = False,
) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_symbols = tuple(item.strip() for item in symbols.split(',') if item.strip()) if symbols else None
    parsed_categories = tuple(item.strip() for item in categories.split(',') if item.strip()) if categories else None
    return agent_context_missing_data_report_service.get_summary(
        market_date=market_date,
        market_date_from=market_date_from,
        market_date_to=market_date_to,
        symbols=parsed_symbols,
        agent_name=agent_name,
        categories=parsed_categories,
        persist_missing_categories=persist_missing_categories,
    )


def run_agent_feedback_symbol_cleanup_smoke(
    *,
    market_date: str,
    symbols: str,
    roles: str | None = None,
    fs_div: str | None = None,
    disclosures_limit: int = 5,
    cleanup_retry_limit: int = 100,
    dump_json_path: str | None = None,
) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()]
    parsed_roles = [item.strip() for item in roles.split(',') if item.strip()] if roles else None
    dispatch_result = agent_feedback_orchestrator.dispatch_for_symbols(
        market_date=market_date,
        symbols=parsed_symbols,
        roles=parsed_roles,
        fs_div=fs_div,
        disclosures_limit=disclosures_limit,
        reuse_existing=False,
    )
    cleanup_result = agent_feedback_session_cleanup_service.run_pending_cleanup(limit=cleanup_retry_limit)
    result = {
        'dispatch': dispatch_result,
        'cleanup_retry': cleanup_result,
    }
    if dump_json_path:
        dump_path = Path(dump_json_path)
        dump_path.parent.mkdir(parents=True, exist_ok=True)
        dump_path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return result


def backfill_recent_derived_contexts(
    *,
    end_date: str | None = None,
    trading_days: int = 7,
    market: str = 'KRX',
) -> dict[str, Any]:
    bootstrap_runtime()
    return derived_context_backfill_service.backfill_recent_history(
        end_date=end_date,
        trading_days=trading_days,
        market=market,
    )


def reconcile_trading_orders(
    *,
    account_reference: str | None = None,
    client_order_id: str | None = None,
    symbol: str | None = None,
    limit: int | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    bootstrap_runtime()
    if client_order_id:
        if not account_reference:
            raise ValueError('account_reference is required when client_order_id is provided')
        return asyncio.run(
            trading_order_reconciliation_service.reconcile_single_order(
                account_reference=account_reference,
                client_order_id=client_order_id,
                trigger='internal_ops_cli',
                reason=reason,
            )
        )
    return asyncio.run(
        trading_order_reconciliation_service.reconcile_recent_orders(
            account_reference=account_reference,
            symbol=symbol,
            limit=limit,
            trigger='internal_ops_cli',
            reason=reason,
            include_orders=True,
        )
    )


def run_next_open_execution_if_due(
    *,
    target_trade_date: str | None = None,
    dry_run: bool | None = None,
    force: bool = False,
) -> dict[str, Any]:
    bootstrap_runtime()
    resolved_target_trade_date = signal_decider_next_open_execution_service._resolve_target_trade_date(
        target_trade_date,
    )
    if (
        signal_decider_exec_integration_service.should_delegate_execution()
        or signal_decider_exec_integration_service.should_manage_delegated_authority(
            target_trade_date=resolved_target_trade_date,
        )
    ):
        return asyncio.run(
            signal_decider_exec_integration_service.dispatch_next_open_plan(
                target_trade_date=resolved_target_trade_date,
                refresh_plan=True,
                dry_run=dry_run,
                force=force,
                trigger='internal_ops_cli',
            )
        )
    return asyncio.run(
        signal_decider_next_open_execution_service.run_market_open_execution_if_due(
            target_trade_date=resolved_target_trade_date,
            dry_run=dry_run,
            force=force,
            trigger='internal_ops_cli',
        )
    )


def run_intraday_auto_sell_if_due(
    *,
    account_reference: str | None = None,
    symbol: str | None = None,
    dry_run: bool | None = None,
    force: bool = False,
) -> dict[str, Any]:
    bootstrap_runtime()
    if signal_decider_exec_integration_service.should_delegate_execution():
        return asyncio.run(
            signal_decider_exec_integration_service.list_intraday_exec_watch(
                account_reference=account_reference,
                symbol=symbol,
                refresh=True,
                trigger='internal_ops_cli',
            )
        )
    return asyncio.run(
        signal_decider_intraday_auto_sell_service.run_intraday_auto_sell_if_due(
            account_reference=account_reference,
            symbol=symbol,
            dry_run=dry_run,
            force=force,
            trigger='internal_ops_cli',
        )
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Internal kai-trading-core operations without self-HTTP.')
    subparsers = parser.add_subparsers(dest='command', required=True)

    subparsers.add_parser('market-status', help='Return /market/status directly from services')
    subparsers.add_parser('quotes-tracked', help='Return /quotes/tracked directly from services')
    subparsers.add_parser('scanner-tracked-symbols', help='Return /scanner/tracked-symbols directly from services')
    subparsers.add_parser('eod-status', help='Return /eod/status directly from services')
    subparsers.add_parser('eod-pipeline-stages', help='List the canonical daily pipeline stage catalog')
    subparsers.add_parser(
        'schema-drift-preflight',
        help='Verify runtime-critical tables and constraints before write-heavy workflows are treated as healthy',
    )

    pipeline_run_parser = subparsers.add_parser(
        'eod-pipeline-run',
        help='Run an explicit subset of the daily pipeline stages in canonical order',
    )
    pipeline_run_parser.add_argument('--market-date')
    pipeline_run_parser.add_argument('--stages')
    pipeline_run_parser.add_argument('--symbols')
    pipeline_run_parser.add_argument('--markets')
    pipeline_run_parser.add_argument('--force', action='store_true')
    pipeline_run_parser.add_argument('--refresh-universe', choices=('true', 'false'))
    pipeline_run_parser.add_argument('--fs-div')
    pipeline_run_parser.add_argument('--symbol-limit', type=int)
    pipeline_run_parser.add_argument('--symbol-offset', type=int, default=0)
    pipeline_run_parser.add_argument('--capture-limit', type=int, default=500)
    pipeline_run_parser.add_argument('--retry-cycles', type=int)
    pipeline_run_parser.add_argument('--feedback-max-cycles', type=int, default=3)
    pipeline_run_parser.add_argument('--feedback-outer-batch-size', type=int, default=20)
    pipeline_run_parser.add_argument('--reuse-existing-feedback', choices=('true', 'false'), default='true')
    pipeline_run_parser.add_argument('--resume-from-stage')

    refresh_master_parser = subparsers.add_parser('eod-master-refresh-if-due', help='Run master refresh directly')
    refresh_master_parser.add_argument('--force', action='store_true')

    pykrx_daily_parser = subparsers.add_parser('eod-pykrx-daily-run-if-due', help='Run pykrx daily EOD directly')
    pykrx_daily_parser.add_argument('--market-date')
    pykrx_daily_parser.add_argument('--markets')
    pykrx_daily_parser.add_argument('--force', action='store_true')
    pykrx_daily_parser.add_argument('--refresh-universe', choices=('true', 'false'))

    pykrx_fundamentals_backfill_parser = subparsers.add_parser(
        'eod-pykrx-daily-bar-fundamentals-backfill',
        help='Backfill pykrx fundamentals columns onto existing daily_price_bars rows',
    )
    pykrx_fundamentals_backfill_parser.add_argument('--start-date', required=True)
    pykrx_fundamentals_backfill_parser.add_argument('--end-date')
    pykrx_fundamentals_backfill_parser.add_argument('--markets')
    pykrx_fundamentals_backfill_parser.add_argument('--limit', type=int)
    pykrx_fundamentals_backfill_parser.add_argument('--symbol')

    pykrx_enrichment_collect_parser = subparsers.add_parser(
        'eod-pykrx-enrichment-collect',
        help='Collect persisted PyKRX enrichment rows for one market date without live broker credentials',
    )
    pykrx_enrichment_collect_parser.add_argument('--market-date', required=True)
    pykrx_enrichment_collect_parser.add_argument('--markets')
    pykrx_enrichment_collect_parser.add_argument('--symbols')
    pykrx_enrichment_collect_parser.add_argument('--categories')

    pykrx_enrichment_inspect_parser = subparsers.add_parser(
        'eod-pykrx-enrichment-inspect',
        help='Inspect persisted PyKRX enrichment rows and per-category coverage for one market date',
    )
    pykrx_enrichment_inspect_parser.add_argument('--market-date', required=True)
    pykrx_enrichment_inspect_parser.add_argument('--markets')
    pykrx_enrichment_inspect_parser.add_argument('--symbols')
    pykrx_enrichment_inspect_parser.add_argument('--categories')
    pykrx_enrichment_inspect_parser.add_argument('--limit', type=int, default=200)

    dart_daily_parser = subparsers.add_parser(
        'dart-fundamentals-daily-run-if-due',
        help='Run DART fundamentals daily incremental refresh directly',
    )
    dart_daily_parser.add_argument('--market-date')
    dart_daily_parser.add_argument('--symbol-limit', type=int)
    dart_daily_parser.add_argument('--symbol-offset', type=int, default=0)
    dart_daily_parser.add_argument('--capture-limit', type=int, default=500)
    dart_daily_parser.add_argument('--retry-cycles', type=int)
    dart_daily_parser.add_argument('--force', action='store_true')
    dart_daily_parser.add_argument('--fs-div')

    dart_signal_parser = subparsers.add_parser(
        'dart-signals-capture',
        help='Capture audit opinion and dilution signals for a DART disclosure event',
    )
    dart_signal_parser.add_argument('--rcept-no', required=True)

    dart_audit_backfill_parser = subparsers.add_parser(
        'dart-audit-signals-backfill',
        help='Backfill missing audit opinions for captured annual DART fundamentals only',
    )
    dart_audit_backfill_parser.add_argument('--market-date', required=True)
    dart_audit_backfill_parser.add_argument('--limit', type=int, default=25)
    dart_audit_backfill_parser.add_argument('--after-event-id', type=int, default=0)
    dart_audit_backfill_mode = dart_audit_backfill_parser.add_mutually_exclusive_group()
    dart_audit_backfill_mode.add_argument(
        '--apply',
        action='store_false',
        dest='dry_run',
        help='Persist captured audit signals. The default is a read-only dry run.',
    )
    dart_audit_backfill_mode.add_argument(
        '--dry-run',
        action='store_true',
        dest='dry_run',
        help='Inspect the bounded candidate page without writing (default).',
    )
    dart_audit_backfill_parser.set_defaults(dry_run=True)

    signal_features_parser = subparsers.add_parser(
        'kis-daily-signal-features-collect',
        help='Collect daily KIS ranking signal features directly',
    )
    signal_features_parser.add_argument('--market-date')
    signal_features_parser.add_argument('--sources')
    signal_features_parser.add_argument('--max-pages', type=int, default=10)
    signal_features_parser.add_argument('--overtime-volume-rank-sort-cls-code', default='2')
    signal_features_parser.add_argument('--overtime-fluctuation-div-cls-code', default='2')

    intraday_snapshot_parser = subparsers.add_parser(
        'kis-intraday-snapshots-collect',
        help='Collect current KIS intraday execution-context snapshots directly',
    )
    intraday_snapshot_parser.add_argument('--symbols')
    intraday_snapshot_parser.add_argument('--market-code')
    intraday_snapshot_parser.add_argument('--source', default=KIS_INTRADAY_SNAPSHOT_SOURCE)
    intraday_snapshot_parser.add_argument('--max-ranking-pages', type=int)
    intraday_snapshot_parser.add_argument('--max-news-items', type=int)
    intraday_snapshot_parser.add_argument('--include-news', choices=('true', 'false'), default='true')

    external_news_collect_parser = subparsers.add_parser(
        'external-news-collect',
        help='Collect optional external news items for symbols via configured provider credentials',
    )
    external_news_collect_parser.add_argument('--symbols', required=True)
    external_news_collect_parser.add_argument('--display', type=int)

    external_news_list_parser = subparsers.add_parser(
        'external-news-list',
        help='List persisted external news items for symbols',
    )
    external_news_list_parser.add_argument('--symbols')
    external_news_list_parser.add_argument('--limit', type=int, default=100)

    piotroski_filter_parser = subparsers.add_parser(
        'eod-piotroski-filter-collect',
        help='Compute and persist daily Piotroski filter results directly',
    )
    piotroski_filter_parser.add_argument('--market-date')

    market_risk_filter_parser = subparsers.add_parser(
        'eod-market-risk-filters-compute',
        help='Compute and persist daily KIS market risk filter rows directly',
    )
    market_risk_filter_parser.add_argument('--market-date', required=True)
    market_risk_filter_parser.add_argument('--symbols')
    market_risk_filter_parser.add_argument('--source', default=MARKET_RISK_FILTER_SOURCE)
    market_risk_filter_parser.add_argument('--market-code', default='J')

    market_risk_snapshot_parser = subparsers.add_parser(
        'eod-market-risk-snapshots-collect',
        help='Collect daily KIS market risk snapshots from inquire-price-2',
    )
    market_risk_snapshot_parser.add_argument('--market-date', required=True)
    market_risk_snapshot_parser.add_argument('--symbols')
    market_risk_snapshot_parser.add_argument('--source', default=MARKET_RISK_SNAPSHOT_SOURCE)
    market_risk_snapshot_parser.add_argument('--market-code', default='J')

    low_risk_factor_parser = subparsers.add_parser(
        'eod-low-risk-factors-compute',
        help='Compute and persist daily low-risk factor results directly',
    )
    low_risk_factor_parser.add_argument('--market-date', required=True)
    low_risk_factor_parser.add_argument('--symbols')

    beta_factor_parser = subparsers.add_parser(
        'eod-beta-factors-compute',
        help='Compute and persist daily beta factor results directly',
    )
    beta_factor_parser.add_argument('--market-date', required=True)
    beta_factor_parser.add_argument('--symbols')

    technical_parser = subparsers.add_parser('eod-technical-snapshots-compute', help='Compute technical snapshots directly')
    technical_parser.add_argument('--market-date', required=True)
    technical_parser.add_argument('--symbols')

    action_parser = subparsers.add_parser('eod-action-summaries-compute', help='Compute action summaries directly')
    action_parser.add_argument('--market-date', required=True)
    action_parser.add_argument('--symbols')

    momentum_parser = subparsers.add_parser('eod-momentum-factors-compute', help='Compute momentum factor rows directly')
    momentum_parser.add_argument('--market-date', required=True)
    momentum_parser.add_argument('--symbols')

    value_parser = subparsers.add_parser('eod-value-factors-compute', help='Compute value factor rows directly')
    value_parser.add_argument('--market-date', required=True)
    value_parser.add_argument('--symbols')

    advanced_value_parser = subparsers.add_parser(
        'eod-advanced-value-factors-compute',
        help='Compute advanced value factor rows directly',
    )
    advanced_value_parser.add_argument('--market-date', required=True)
    advanced_value_parser.add_argument('--symbols')

    unified_value_parser = subparsers.add_parser(
        'eod-unified-value-factors-compute',
        help='Compute unified value factor rows directly',
    )
    unified_value_parser.add_argument('--market-date', required=True)
    unified_value_parser.add_argument('--symbols')

    quality_parser = subparsers.add_parser(
        'eod-quality-factors-compute',
        help='Compute practical quality factor rows directly',
    )
    quality_parser.add_argument('--market-date', required=True)
    quality_parser.add_argument('--symbols')

    composite_parser = subparsers.add_parser(
        'eod-composite-factors-compute',
        help='Compute integrated composite factor rows directly',
    )
    composite_parser.add_argument('--market-date', required=True)
    composite_parser.add_argument('--symbols')

    trending_parser = subparsers.add_parser(
        'eod-trending-ranks-compute',
        help='Compute stored day-based trending ranks directly',
    )
    trending_parser.add_argument('--market-date', required=True)
    trending_parser.add_argument('--periods')

    feedback_retry_parser = subparsers.add_parser(
        'agent-feedback-retry-cycles',
        help='Run the same agent feedback retry-cycle engine used by fundamentals follow-up',
    )
    feedback_retry_parser.add_argument('--market-date', required=True)
    feedback_retry_parser.add_argument('--symbols')
    feedback_retry_parser.add_argument('--roles')
    feedback_retry_parser.add_argument(
        '--feedback-scope-mode',
        choices=FEEDBACK_SCOPE_MODES,
        default=FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
    )
    feedback_retry_parser.add_argument('--max-cycles', type=int, default=3)
    feedback_retry_parser.add_argument('--outer-batch-size', type=int, default=20)
    feedback_retry_parser.add_argument('--reuse-existing', choices=('true', 'false'), default='true')
    feedback_retry_parser.add_argument('--force', action='store_true')
    feedback_retry_parser.add_argument('--force-recompute', action='store_true')

    feedback_data_gap_summary_parser = subparsers.add_parser(
        'agent-feedback-data-gap-summary',
        help='Generate the operator data-gap feedback quality report',
    )
    feedback_data_gap_summary_parser.add_argument('--market-date')
    feedback_data_gap_summary_parser.add_argument('--market-date-from')
    feedback_data_gap_summary_parser.add_argument('--market-date-to')
    feedback_data_gap_summary_parser.add_argument('--symbols')
    feedback_data_gap_summary_parser.add_argument('--agent-name')
    feedback_data_gap_summary_parser.add_argument('--categories')
    feedback_data_gap_summary_parser.add_argument(
        '--persist-missing-categories',
        choices=('true', 'false'),
        default='false',
    )
    feedback_data_gap_quality_report_parser = subparsers.add_parser(
        'agent-feedback-data-gap-quality-report',
        help='Alias for the operator data-gap feedback quality report',
    )
    feedback_data_gap_quality_report_parser.add_argument('--market-date')
    feedback_data_gap_quality_report_parser.add_argument('--market-date-from')
    feedback_data_gap_quality_report_parser.add_argument('--market-date-to')
    feedback_data_gap_quality_report_parser.add_argument('--symbols')
    feedback_data_gap_quality_report_parser.add_argument('--agent-name')
    feedback_data_gap_quality_report_parser.add_argument('--categories')
    feedback_data_gap_quality_report_parser.add_argument(
        '--persist-missing-categories',
        choices=('true', 'false'),
        default='false',
    )

    signal_decider_evaluations_parser = subparsers.add_parser(
        'agent-signal-decider-evaluations-compute',
        help='Refresh recent signal-decider realized outcomes and verdict rows directly',
    )
    signal_decider_evaluations_parser.add_argument('--market-date', required=True)
    signal_decider_evaluations_parser.add_argument(
        '--lookback-days',
        type=int,
        default=DEFAULT_SIGNAL_DECIDER_EVALUATION_LOOKBACK_DAYS,
    )
    signal_decider_evaluations_parser.add_argument('--symbols')
    signal_decider_evaluations_parser.add_argument('--horizons')
    signal_decider_evaluations_parser.add_argument('--evaluation-version')
    signal_decider_evaluations_parser.add_argument('--force-recompute', action='store_true')

    signal_decider_calendar_parser = subparsers.add_parser(
        'agent-signal-decider-calendar-refresh',
        help='Refresh the dedicated persisted calendar-review read model directly',
    )
    signal_decider_calendar_parser.add_argument('--market-date', required=True)
    signal_decider_calendar_parser.add_argument(
        '--lookback-days',
        type=int,
        default=DEFAULT_SIGNAL_DECIDER_EVALUATION_LOOKBACK_DAYS,
    )
    signal_decider_calendar_parser.add_argument('--symbols')
    signal_decider_calendar_parser.add_argument('--horizons')
    signal_decider_calendar_parser.add_argument('--primary-horizons')
    signal_decider_calendar_parser.add_argument('--evaluation-version')
    signal_decider_calendar_parser.add_argument('--force-recompute', action='store_true')

    signal_decider_repair_parser = subparsers.add_parser(
        'agent-signal-decider-evaluations-repair',
        help='Repair signal-decider evaluation coverage over an explicit market-date range',
    )
    signal_decider_repair_parser.add_argument('--market-date-from', required=True)
    signal_decider_repair_parser.add_argument('--market-date-to', required=True)
    signal_decider_repair_parser.add_argument('--symbols')
    signal_decider_repair_parser.add_argument('--horizons')
    signal_decider_repair_parser.add_argument('--primary-horizons')
    signal_decider_repair_parser.add_argument('--evaluation-version')
    signal_decider_repair_parser.add_argument('--force-recompute', choices=('true', 'false'), default='true')
    signal_decider_repair_parser.add_argument('--refresh-calendar', choices=('true', 'false'), default='true')
    signal_decider_repair_parser.add_argument('--fail-on-mature-gaps', choices=('true', 'false'), default='true')

    execution_outcomes_parser = subparsers.add_parser(
        'agent-signal-decider-execution-outcomes-backfill',
        help='Backfill actual signal-decider execution outcomes from persisted core order/fill and exec readback data',
    )
    execution_outcomes_parser.add_argument('--market-date-from', required=True)
    execution_outcomes_parser.add_argument('--market-date-to', required=True)
    execution_outcomes_parser.add_argument('--symbol')
    execution_outcomes_parser.add_argument('--account-reference')
    execution_outcomes_parser.add_argument('--policy-version')
    execution_outcomes_parser.add_argument('--limit', type=int)

    decider_replay_parser = subparsers.add_parser(
        'agent-decider-replay',
        help='Replay candidate decider hard-gate rules offline against persisted decision snapshots and outcomes',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            'Example:\n'
            '  python3.12 -m app.internal.ops agent-decider-replay \\\n'
            '    --market-date-from 2026-06-15 --market-date-to 2026-06-19 \\\n'
            '    --agent-name short-term-decider \\\n'
            '    --rule-config-json \'{"rules":[{"name":"stale_watch_veto",'
            '"predicates":[{"type":"trigger_price_drift","max_below_trigger_pct":0.03}]}]}\' \\\n'
            '    --fee-bps 10 --slippage-bps 5 --output-format markdown\n\n'
            'Predicates are evaluated only against persisted snapshot-time request, response, traceability, '
            'and input-feature payloads. Realized outcomes are used only for replay P/L comparison.'
        ),
    )
    decider_replay_parser.add_argument('--market-date-from', required=True)
    decider_replay_parser.add_argument('--market-date-to', required=True)
    decider_replay_parser.add_argument(
        '--agent-name',
        required=True,
        choices=('signal-decider', 'short-term-decider'),
        help='Decider snapshot stream to replay',
    )
    decider_replay_parser.add_argument('--symbols', help='Optional comma-separated symbol list')
    decider_replay_parser.add_argument(
        '--policy-versions',
        help='Optional comma-separated decision policy versions to include',
    )
    decider_replay_parser.add_argument(
        '--rule-config-json',
        help=(
            'Inline JSON rule config. Example: '
            '\'{"rules":[{"name":"stale_watch_veto","predicates":[{"type":"trigger_price_drift","max_below_trigger_pct":0.03}]}]}\''
        ),
    )
    decider_replay_parser.add_argument('--rule-config-path', help='Path to a JSON rule config file')
    decider_replay_parser.add_argument('--fee-bps', default='0', help='Per-side fee in basis points')
    decider_replay_parser.add_argument('--slippage-bps', default='0', help='Per-side slippage in basis points')
    decider_replay_parser.add_argument(
        '--notional-krw',
        default='1000000',
        help='Fallback simulated notional for evaluation-only outcomes',
    )
    decider_replay_parser.add_argument('--horizon-days', type=int, default=5)
    decider_replay_parser.add_argument('--top-examples-limit', type=int, default=10)
    decider_replay_parser.add_argument(
        '--output-format',
        choices=('json', 'markdown'),
        default='json',
        help='json returns machine-readable metrics including markdown_summary; markdown prints the concise summary only',
    )

    wait_conflict_replay_parser = subparsers.add_parser(
        'agent-short-term-wait-conflict-replay',
        help='Replay actual short-term executions against the signal-decider WAIT-conflict candidate gate',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            'Example:\n'
            '  python3.12 -m app.internal.ops agent-short-term-wait-conflict-replay \\\n'
            '    --completed-weeks 4 --as-of-date-kst 2026-09-06 \\\n'
            '    --gate-mode downsize_probe --probe-quantity 3 \\\n'
            '    --output-format markdown\n\n'
            'By default, the window is the last four completed Monday-Friday KST trading weeks. '
            'An explicit --market-date-from/--market-date-to pair overrides the rolling window. '
            'The market-date window is the short-term decision/source market date, not the next-open '
            'target execution date. '
            'This report uses actual short-term execution outcome rows only. It does not fall back to synthetic '
            'calendar/evaluation outcomes.'
        ),
    )
    wait_conflict_replay_parser.add_argument(
        '--market-date-from',
        help='Inclusive source decision market date lower bound; requires --market-date-to',
    )
    wait_conflict_replay_parser.add_argument(
        '--market-date-to',
        help='Inclusive source decision market date upper bound; requires --market-date-from',
    )
    wait_conflict_replay_parser.add_argument(
        '--completed-weeks',
        type=int,
        default=4,
        help='Rolling completed Monday-Friday KST weeks; minimum and default is 4',
    )
    wait_conflict_replay_parser.add_argument(
        '--as-of-date-kst',
        help='Optional reproducible KST as-of date for rolling-window resolution',
    )
    wait_conflict_replay_parser.add_argument('--symbols', help='Optional comma-separated symbol list')
    wait_conflict_replay_parser.add_argument('--fee-bps', default='0', help='Per-side fee in basis points')
    wait_conflict_replay_parser.add_argument('--slippage-bps', default='0', help='Per-side slippage in basis points')
    wait_conflict_replay_parser.add_argument(
        '--notional-krw',
        default='1000000',
        help='Fallback notional if an actual outcome row lacks entry notional',
    )
    wait_conflict_replay_parser.add_argument(
        '--gate-mode',
        choices=('hard_block', 'downsize_probe', 'report_only'),
        default='hard_block',
        help='Candidate gate mode for replay comparison only; this does not alter live runtime settings',
    )
    wait_conflict_replay_parser.add_argument(
        '--probe-quantity',
        type=int,
        default=1,
        help='One replay-level probe quantity applied to every historical trade; valid range is 1 through 100',
    )
    wait_conflict_replay_parser.add_argument('--top-examples-limit', type=int, default=10)
    wait_conflict_replay_parser.add_argument(
        '--output-format',
        choices=('json', 'markdown'),
        default='json',
        help='json returns machine-readable metrics including markdown_summary; markdown prints the concise summary only',
    )

    feedback_smoke_parser = subparsers.add_parser(
        'agent-feedback-symbol-cleanup-smoke',
        help='Dispatch symbol feedback, store rows, attempt immediate session deletion, then retry failed deletions once',
    )
    feedback_smoke_parser.add_argument('--market-date', required=True)
    feedback_smoke_parser.add_argument('--symbols', required=True)
    feedback_smoke_parser.add_argument('--roles')
    feedback_smoke_parser.add_argument('--fs-div')
    feedback_smoke_parser.add_argument('--disclosures-limit', type=int, default=5)
    feedback_smoke_parser.add_argument('--cleanup-retry-limit', type=int, default=100)
    feedback_smoke_parser.add_argument('--dump-json-path')

    derived_context_backfill_parser = subparsers.add_parser(
        'agent-derived-contexts-backfill-recent',
        help='Backfill persisted price and participation context rows for the recent trading window ending yesterday',
    )
    derived_context_backfill_parser.add_argument('--end-date')
    derived_context_backfill_parser.add_argument('--trading-days', type=int, default=7)
    derived_context_backfill_parser.add_argument('--market', default='KRX')

    trading_reconcile_parser = subparsers.add_parser(
        'trading-orders-reconcile',
        help='Reconcile recent or targeted KIS trading orders directly from persistence and broker inquiry truth',
    )
    trading_reconcile_parser.add_argument('--account-reference')
    trading_reconcile_parser.add_argument('--client-order-id')
    trading_reconcile_parser.add_argument('--symbol')
    trading_reconcile_parser.add_argument('--limit', type=int)
    trading_reconcile_parser.add_argument('--reason')

    next_open_execution_parser = subparsers.add_parser(
        'trading-next-open-execution-run-if-due',
        help='Refresh and execute the signal-decider next-open buy plan directly',
    )
    next_open_execution_parser.add_argument('--target-trade-date')
    next_open_execution_parser.add_argument('--dry-run', choices=('true', 'false'))
    next_open_execution_parser.add_argument('--force', action='store_true')

    slim_exec_payloads_parser = subparsers.add_parser(
        'trading-signal-decider-slim-exec-payloads',
        help='Slim oversized per-item signal-decider exec dispatch payloads; dry-run by default',
    )
    slim_exec_payloads_parser.add_argument('--dry-run', choices=('true', 'false'), default='true')
    slim_exec_payloads_parser.add_argument('--target-trade-date')
    slim_exec_payloads_parser.add_argument('--min-payload-bytes', type=int, default=64 * 1024)
    slim_exec_payloads_parser.add_argument('--max-payload-bytes', type=int, default=24 * 1024)
    slim_exec_payloads_parser.add_argument('--batch-size', type=int, default=100)
    slim_exec_payloads_parser.add_argument('--limit', type=int)

    intraday_auto_sell_parser = subparsers.add_parser(
        'trading-intraday-auto-sell-run-if-due',
        help='Evaluate linked next-open positions and execute signal-decider intraday auto-sells directly',
    )
    intraday_auto_sell_parser.add_argument('--account-reference')
    intraday_auto_sell_parser.add_argument('--symbol')
    intraday_auto_sell_parser.add_argument('--dry-run', choices=('true', 'false'))
    intraday_auto_sell_parser.add_argument('--force', action='store_true')
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command == 'market-status':
        payload = get_market_status()
    elif args.command == 'quotes-tracked':
        payload = asyncio.run(get_tracked_quotes())
    elif args.command == 'scanner-tracked-symbols':
        payload = get_tracked_symbols()
    elif args.command == 'eod-status':
        payload = get_eod_status()
    elif args.command == 'eod-pipeline-stages':
        payload = list_daily_pipeline_stages()
    elif args.command == 'schema-drift-preflight':
        payload, exit_code = collect_schema_drift_preflight()
        print(json.dumps(payload, ensure_ascii=False))
        return exit_code
    elif args.command == 'eod-pipeline-run':
        payload = run_daily_pipeline_stages(
            market_date=args.market_date,
            stages=args.stages,
            symbols=args.symbols,
            markets=args.markets,
            force=args.force,
            refresh_universe=None if args.refresh_universe is None else args.refresh_universe == 'true',
            fs_div=args.fs_div,
            symbol_limit=args.symbol_limit,
            symbol_offset=args.symbol_offset,
            capture_limit=args.capture_limit,
            retry_cycles=args.retry_cycles,
            feedback_max_cycles=args.feedback_max_cycles,
            feedback_outer_batch_size=args.feedback_outer_batch_size,
            reuse_existing_feedback=args.reuse_existing_feedback == 'true',
            resume_from_stage=args.resume_from_stage,
        )
    elif args.command == 'eod-master-refresh-if-due':
        payload = refresh_master_if_due_internal(force=args.force)
    elif args.command == 'eod-pykrx-daily-run-if-due':
        payload = run_pykrx_daily_if_due(
            market_date=args.market_date,
            markets=args.markets,
            force=args.force,
            refresh_universe=None if args.refresh_universe is None else args.refresh_universe == 'true',
        )
    elif args.command == 'eod-pykrx-daily-bar-fundamentals-backfill':
        payload = backfill_pykrx_daily_bar_fundamentals(
            start_date=args.start_date,
            end_date=args.end_date,
            markets=args.markets,
            limit=args.limit,
            symbol=args.symbol,
        )
    elif args.command == 'eod-pykrx-enrichment-collect':
        payload = collect_pykrx_enrichment(
            market_date=args.market_date,
            markets=args.markets,
            symbols=args.symbols,
            categories=args.categories,
        )
    elif args.command == 'eod-pykrx-enrichment-inspect':
        payload = inspect_pykrx_enrichment(
            market_date=args.market_date,
            markets=args.markets,
            symbols=args.symbols,
            categories=args.categories,
            limit=args.limit,
        )
    elif args.command == 'dart-fundamentals-daily-run-if-due':
        payload = run_dart_fundamentals_daily_if_due(
            market_date=args.market_date,
            symbol_limit=args.symbol_limit,
            symbol_offset=args.symbol_offset,
            capture_limit=args.capture_limit,
            retry_cycles=args.retry_cycles,
            force=args.force,
            fs_div=args.fs_div,
        )
    elif args.command == 'dart-signals-capture':
        payload = capture_dart_signals(rcept_no=args.rcept_no)
    elif args.command == 'dart-audit-signals-backfill':
        payload = backfill_dart_audit_signals(
            market_date=args.market_date,
            limit=args.limit,
            after_event_id=args.after_event_id,
            dry_run=args.dry_run,
        )
    elif args.command == 'kis-daily-signal-features-collect':
        payload = collect_kis_daily_signal_features(
            market_date=args.market_date,
            sources=args.sources,
            max_pages=args.max_pages,
            overtime_volume_rank_sort_cls_code=args.overtime_volume_rank_sort_cls_code,
            overtime_fluctuation_div_cls_code=args.overtime_fluctuation_div_cls_code,
        )
    elif args.command == 'kis-intraday-snapshots-collect':
        payload = collect_kis_intraday_snapshots(
            symbols=args.symbols,
            market_code=args.market_code,
            source=args.source,
            max_ranking_pages=args.max_ranking_pages,
            max_news_items=args.max_news_items,
            include_news=args.include_news == 'true',
        )
    elif args.command == 'external-news-collect':
        payload = collect_external_news(symbols=args.symbols, display=args.display)
    elif args.command == 'external-news-list':
        payload = list_external_news(symbols=args.symbols, limit=args.limit)
    elif args.command == 'eod-piotroski-filter-collect':
        payload = collect_daily_piotroski_filter_results(market_date=args.market_date)
    elif args.command == 'eod-market-risk-filters-compute':
        payload = compute_market_risk_filters(
            market_date=args.market_date,
            symbols=args.symbols,
            source=args.source,
            market_code=args.market_code,
        )
    elif args.command == 'eod-market-risk-snapshots-collect':
        payload = collect_market_risk_snapshots(
            market_date=args.market_date,
            symbols=args.symbols,
            source=args.source,
            market_code=args.market_code,
        )
    elif args.command == 'eod-low-risk-factors-compute':
        payload = compute_low_risk_factors(market_date=args.market_date, symbols=args.symbols)
    elif args.command == 'eod-beta-factors-compute':
        payload = compute_beta_factors(market_date=args.market_date, symbols=args.symbols)
    elif args.command == 'eod-technical-snapshots-compute':
        payload = compute_technical_snapshots(market_date=args.market_date, symbols=args.symbols)
    elif args.command == 'eod-action-summaries-compute':
        payload = compute_action_summaries(market_date=args.market_date, symbols=args.symbols)
    elif args.command == 'eod-momentum-factors-compute':
        payload = compute_momentum_factors(market_date=args.market_date, symbols=args.symbols)
    elif args.command == 'eod-value-factors-compute':
        payload = compute_value_factors(market_date=args.market_date, symbols=args.symbols)
    elif args.command == 'eod-advanced-value-factors-compute':
        payload = compute_advanced_value_factors(market_date=args.market_date, symbols=args.symbols)
    elif args.command == 'eod-unified-value-factors-compute':
        payload = compute_unified_value_factors(market_date=args.market_date, symbols=args.symbols)
    elif args.command == 'eod-quality-factors-compute':
        payload = compute_quality_factors(market_date=args.market_date, symbols=args.symbols)
    elif args.command == 'eod-composite-factors-compute':
        payload = compute_composite_factors(market_date=args.market_date, symbols=args.symbols)
    elif args.command == 'eod-trending-ranks-compute':
        payload = compute_trending_ranks(market_date=args.market_date, periods=args.periods)
    elif args.command == 'agent-feedback-retry-cycles':
        payload = run_agent_feedback_retry_cycles(
            market_date=args.market_date,
            symbols=args.symbols,
            roles=args.roles,
            feedback_scope_mode=args.feedback_scope_mode,
            max_cycles=args.max_cycles,
            outer_batch_size=args.outer_batch_size,
            reuse_existing=args.reuse_existing == 'true',
            force=args.force,
            force_recompute=args.force_recompute,
        )
    elif args.command in {'agent-feedback-data-gap-summary', 'agent-feedback-data-gap-quality-report'}:
        payload = get_agent_feedback_data_gap_summary(
            market_date=args.market_date,
            market_date_from=args.market_date_from,
            market_date_to=args.market_date_to,
            symbols=args.symbols,
            agent_name=args.agent_name,
            categories=args.categories,
            persist_missing_categories=args.persist_missing_categories == 'true',
        )
    elif args.command == 'agent-signal-decider-evaluations-compute':
        payload = compute_signal_decider_evaluations(
            market_date=args.market_date,
            lookback_days=args.lookback_days,
            symbols=args.symbols,
            horizons=args.horizons,
            evaluation_version=args.evaluation_version,
            force_recompute=args.force_recompute,
        )
    elif args.command == 'agent-signal-decider-calendar-refresh':
        payload = refresh_signal_decider_calendar_reviews(
            market_date=args.market_date,
            lookback_days=args.lookback_days,
            symbols=args.symbols,
            horizons=args.horizons,
            primary_horizons=args.primary_horizons,
            evaluation_version=args.evaluation_version,
            force_recompute=args.force_recompute,
        )
    elif args.command == 'agent-signal-decider-evaluations-repair':
        payload = repair_signal_decider_evaluation_coverage(
            market_date_from=args.market_date_from,
            market_date_to=args.market_date_to,
            symbols=args.symbols,
            horizons=args.horizons,
            primary_horizons=args.primary_horizons,
            evaluation_version=args.evaluation_version,
            force_recompute=args.force_recompute == 'true',
            refresh_calendar=args.refresh_calendar == 'true',
            fail_on_mature_gaps=args.fail_on_mature_gaps == 'true',
        )
    elif args.command == 'agent-signal-decider-execution-outcomes-backfill':
        payload = signal_decider_execution_outcome_service.backfill_outcomes(
            market_date_from=args.market_date_from,
            market_date_to=args.market_date_to,
            symbol=args.symbol,
            account_reference=args.account_reference,
            policy_version=args.policy_version,
            limit=args.limit,
        )
    elif args.command == 'agent-decider-replay':
        payload = replay_decider_candidate_rules(
            market_date_from=args.market_date_from,
            market_date_to=args.market_date_to,
            agent_name=args.agent_name,
            rule_config_json=args.rule_config_json,
            rule_config_path=args.rule_config_path,
            symbols=args.symbols,
            policy_versions=args.policy_versions,
            fee_bps=args.fee_bps,
            slippage_bps=args.slippage_bps,
            notional_krw=args.notional_krw,
            horizon_days=args.horizon_days,
            top_examples_limit=args.top_examples_limit,
        )
    elif args.command == 'agent-short-term-wait-conflict-replay':
        payload = replay_short_term_wait_conflict_gate(
            market_date_from=args.market_date_from,
            market_date_to=args.market_date_to,
            completed_weeks=args.completed_weeks,
            as_of_date_kst=args.as_of_date_kst,
            symbols=args.symbols,
            fee_bps=args.fee_bps,
            slippage_bps=args.slippage_bps,
            notional_krw=args.notional_krw,
            gate_mode=args.gate_mode,
            probe_quantity=args.probe_quantity,
            top_examples_limit=args.top_examples_limit,
        )
    elif args.command == 'agent-feedback-symbol-cleanup-smoke':
        payload = run_agent_feedback_symbol_cleanup_smoke(
            market_date=args.market_date,
            symbols=args.symbols,
            roles=args.roles,
            fs_div=args.fs_div,
            disclosures_limit=args.disclosures_limit,
            cleanup_retry_limit=args.cleanup_retry_limit,
            dump_json_path=args.dump_json_path,
        )
    elif args.command == 'agent-derived-contexts-backfill-recent':
        payload = backfill_recent_derived_contexts(
            end_date=args.end_date,
            trading_days=args.trading_days,
            market=args.market,
        )
    elif args.command == 'trading-orders-reconcile':
        payload = reconcile_trading_orders(
            account_reference=args.account_reference,
            client_order_id=args.client_order_id,
            symbol=args.symbol,
            limit=args.limit,
            reason=args.reason,
        )
    elif args.command == 'trading-next-open-execution-run-if-due':
        payload = run_next_open_execution_if_due(
            target_trade_date=args.target_trade_date,
            dry_run=None if args.dry_run is None else args.dry_run == 'true',
            force=args.force,
        )
    elif args.command == 'trading-signal-decider-slim-exec-payloads':
        payload = slim_signal_decider_exec_payloads(
            dry_run=args.dry_run == 'true',
            target_trade_date=args.target_trade_date,
            min_payload_bytes=args.min_payload_bytes,
            max_payload_bytes=args.max_payload_bytes,
            batch_size=args.batch_size,
            limit=args.limit,
        )
    elif args.command == 'trading-intraday-auto-sell-run-if-due':
        payload = run_intraday_auto_sell_if_due(
            account_reference=args.account_reference,
            symbol=args.symbol,
            dry_run=None if args.dry_run is None else args.dry_run == 'true',
            force=args.force,
        )
    else:
        parser.error(f'Unsupported command: {args.command}')

    if args.command in {'agent-decider-replay', 'agent-short-term-wait-conflict-replay'} and args.output_format == 'markdown':
        print(payload['markdown_summary'])
    else:
        print(json.dumps(payload, ensure_ascii=False))
    if (
        args.command == 'dart-audit-signals-backfill'
        and payload.get('status') in {'partial', 'completed_with_gaps', 'failed'}
    ):
        return 2
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
