from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Sequence

from app.agent_context.retry_service import FeedbackRetryCycleConfig, agent_feedback_retry_service
from app.agent_context.prerequisite_service import agent_feedback_prerequisite_service
from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES, SUMMARIZER_AGENT_NAMES
from app.agent_context.run_modes import (
    FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
)
from app.agent_context.signal_decider_loop_service import (
    DEFAULT_SIGNAL_DECIDER_EVALUATION_LOOKBACK_DAYS,
    signal_decider_loop_service,
)
from app.core.settings import settings
from app.dart.fundamentals_capture_service import dart_fundamentals_capture_service
from app.eod.action_summaries import daily_action_summary_service
from app.eod.daily_pipeline_state import daily_pipeline_run_state_service
from app.eod.master import refresh_master_if_due
from app.eod.pykrx_backfill import DEFAULT_MARKETS, PYKRX_BACKFILL_SERVICE
from app.eod.pykrx_enrichment import pykrx_enrichment_service
from app.eod.pykrx_enrichment_gate import (
    REQUIRED_EXEC_DECIDER_PYKRX_ENRICHMENT_CATEGORIES,
    build_pykrx_enrichment_preflight,
)
from app.eod.run_stage_support import extract_next_open_plan_stage_counts
from app.eod.technical_snapshots import daily_technical_snapshot_service
from app.external_news.service import external_news_service
from app.macro_context.service import macro_context_service
from app.market.clock import get_market_state
from app.market_data_store import compute_and_persist_daily_trending_ranks
from app.signals.advanced_value_factor import (
    ADVANCED_VALUE_FACTOR_SOURCE,
    daily_advanced_value_factor_service,
)
from app.signals.beta_factor import BETA_FACTOR_SOURCE, daily_beta_factor_service
from app.signals.composite_factor import COMPOSITE_FACTOR_SOURCE, daily_composite_factor_service
from app.signals.daily_signal_features import daily_signal_feature_service
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

PYKRX_PIPELINE_STAGE_ORDER: tuple[str, ...] = (
    'price_collection',
    'technical_snapshots',
    'action_summaries',
    'trending_ranks',
    'signal_features',
    'market_risk_filter',
)
FACTOR_STAGE_ORDER: tuple[str, ...] = (
    'momentum_factors',
    'value_factors',
    'low_risk_factors',
    'beta_factors',
    'advanced_value_factors',
    'unified_value_factors',
    'quality_factors',
    'composite_factors',
)
DAILY_STAGE_ORDER: tuple[str, ...] = (
    'master_refresh',
    'pykrx_universe_refresh',
    'price_collection',
    'pykrx_enrichment',
    *(stage_id for stage_id in PYKRX_PIPELINE_STAGE_ORDER if stage_id != 'price_collection'),
    'market_risk_snapshots',
    'fundamentals_daily_refresh',
    'piotroski_filter',
    *FACTOR_STAGE_ORDER,
    'external_news_collect',
    'external_news_enrich',
    'macro_context_refresh',
    'agent_feedback_summaries',
    'signal_decider',
    'next_open_execution_plan',
    'signal_decider_evaluations',
)
DEFAULT_DAILY_STAGE_ORDER: tuple[str, ...] = (
    'master_refresh',
    'pykrx_universe_refresh',
    'price_collection',
    'pykrx_enrichment',
    *(stage_id for stage_id in PYKRX_PIPELINE_STAGE_ORDER if stage_id != 'price_collection'),
    'fundamentals_daily_refresh',
    'piotroski_filter',
    *FACTOR_STAGE_ORDER,
    'external_news_collect',
    'external_news_enrich',
    'macro_context_refresh',
    'agent_feedback_summaries',
    'signal_decider',
    'next_open_execution_plan',
    'signal_decider_evaluations',
)


@dataclass(frozen=True)
class DailyStageDefinition:
    id: str
    label: str
    group: str
    description: str
    depends_on: tuple[str, ...] = ()
    supports_market_date: bool = True
    supports_symbols: bool = False
    supports_markets: bool = False
    supports_force: bool = False
    supports_symbol_limit: bool = False
    supports_capture_limit: bool = False
    inspect_routes: tuple[str, ...] = ()
    output_tables: tuple[str, ...] = ()
    default_selected: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            'id': self.id,
            'label': self.label,
            'group': self.group,
            'description': self.description,
            'depends_on': list(self.depends_on),
            'supports_market_date': self.supports_market_date,
            'supports_symbols': self.supports_symbols,
            'supports_markets': self.supports_markets,
            'supports_force': self.supports_force,
            'supports_symbol_limit': self.supports_symbol_limit,
            'supports_capture_limit': self.supports_capture_limit,
            'inspect_routes': list(self.inspect_routes),
            'output_tables': list(self.output_tables),
            'default_selected': self.default_selected,
        }


STAGE_DEFINITIONS: dict[str, DailyStageDefinition] = {
    'master_refresh': DailyStageDefinition(
        id='master_refresh',
        label='master refresh',
        group='market_data',
        description='Refresh the KIS-backed master symbol universe when due.',
        supports_market_date=False,
        supports_force=True,
        inspect_routes=('/eod/status', '/eod/master/refresh-if-due'),
        output_tables=('symbols',),
        default_selected=True,
    ),
    'pykrx_universe_refresh': DailyStageDefinition(
        id='pykrx_universe_refresh',
        label='pykrx universe refresh',
        group='market_data',
        description='Refresh the active pykrx universe used by daily price collection.',
        supports_market_date=False,
        supports_markets=True,
        supports_symbol_limit=True,
        inspect_routes=('/eod/pykrx/universe/refresh',),
        output_tables=('symbols',),
        default_selected=True,
    ),
    'price_collection': DailyStageDefinition(
        id='price_collection',
        label='pykrx daily price collection',
        group='market_data',
        description='Persist daily pykrx price bars for the selected market date.',
        supports_symbols=True,
        supports_markets=True,
        supports_force=True,
        inspect_routes=('/eod/pipeline/status', '/eod/pykrx/daily/run-if-due'),
        output_tables=('daily_price_bars', 'eod_collection_runs'),
        default_selected=True,
    ),
    'pykrx_enrichment': DailyStageDefinition(
        id='pykrx_enrichment',
        label='pykrx enrichment collect',
        group='market_data',
        description='Persist PyKRX EOD enrichment rows for sector, breadth, investor flow, short-selling, and valuation context.',
        depends_on=('price_collection',),
        supports_symbols=True,
        supports_markets=True,
        inspect_routes=('/eod/pykrx/enrichment', '/eod/pipeline/status'),
        output_tables=('daily_pykrx_enrichment_rows',),
        default_selected=True,
    ),
    'technical_snapshots': DailyStageDefinition(
        id='technical_snapshots',
        label='technical snapshot compute',
        group='market_data',
        description='Rebuild persisted daily technical snapshots from stored price bars.',
        depends_on=('price_collection',),
        supports_symbols=True,
        inspect_routes=('/eod/technical-snapshots',),
        output_tables=('daily_technical_snapshots',),
        default_selected=True,
    ),
    'action_summaries': DailyStageDefinition(
        id='action_summaries',
        label='action summary compute',
        group='market_data',
        description='Rebuild persisted daily action summaries from stored price bars.',
        depends_on=('price_collection',),
        supports_symbols=True,
        inspect_routes=('/eod/action-summaries',),
        output_tables=('daily_action_summaries',),
        default_selected=True,
    ),
    'trending_ranks': DailyStageDefinition(
        id='trending_ranks',
        label='stored trending rank rebuild',
        group='market_data',
        description='Rebuild day-based persisted trending ranks from stored price bars.',
        depends_on=('price_collection',),
        inspect_routes=('/scanner/trending-stocks',),
        output_tables=('daily_trending_ranks',),
        default_selected=True,
    ),
    'signal_features': DailyStageDefinition(
        id='signal_features',
        label='KIS daily signal feature collect',
        group='market_data',
        description='Collect ranking-based KIS daily signal features and participation context.',
        depends_on=('price_collection',),
        inspect_routes=('/eod/signal-features',),
        output_tables=('daily_signal_features', 'daily_market_participation_contexts'),
        default_selected=True,
    ),
    'market_risk_snapshots': DailyStageDefinition(
        id='market_risk_snapshots',
        label='KIS market-risk snapshot collect',
        group='market_data',
        description='Collect persisted KIS market-risk snapshots for the selected symbol scope.',
        depends_on=('price_collection',),
        supports_symbols=True,
        inspect_routes=('/eod/market-risk-snapshots',),
        output_tables=('daily_market_risk_snapshots',),
    ),
    'market_risk_filter': DailyStageDefinition(
        id='market_risk_filter',
        label='KIS market-risk filter compute',
        group='market_data',
        description='Compute persisted KIS market-risk filter results from collected snapshots.',
        depends_on=('price_collection',),
        supports_symbols=True,
        inspect_routes=('/eod/market-risk-filters',),
        output_tables=('daily_market_risk_snapshots', 'daily_market_risk_filter_results'),
        default_selected=True,
    ),
    'fundamentals_daily_refresh': DailyStageDefinition(
        id='fundamentals_daily_refresh',
        label='DART fundamentals daily refresh',
        group='fundamentals',
        description='Run the incremental DART fundamentals refresh for the selected market date.',
        supports_force=True,
        supports_symbol_limit=True,
        supports_capture_limit=True,
        inspect_routes=('/disclosures/dart/fundamentals/daily/inspect', '/eod/pipeline/status'),
        output_tables=('dart_fundamentals_snapshots', 'dart_fundamentals_rows'),
        default_selected=True,
    ),
    'piotroski_filter': DailyStageDefinition(
        id='piotroski_filter',
        label='Piotroski filter compute',
        group='fundamentals',
        description='Rebuild persisted daily Piotroski filter results from stored fundamentals.',
        depends_on=('fundamentals_daily_refresh',),
        inspect_routes=('/eod/piotroski-filter-results',),
        output_tables=('daily_piotroski_filter_results',),
        default_selected=True,
    ),
    'momentum_factors': DailyStageDefinition(
        id='momentum_factors',
        label='momentum factor compute',
        group='factors',
        description='Rebuild persisted daily momentum factor rows from stored price bars.',
        depends_on=('price_collection',),
        supports_symbols=True,
        inspect_routes=('/eod/momentum-factors',),
        output_tables=('daily_momentum_factor_results',),
        default_selected=True,
    ),
    'value_factors': DailyStageDefinition(
        id='value_factors',
        label='value factor compute',
        group='factors',
        description='Rebuild persisted daily value factor rows from stored price bars.',
        depends_on=('price_collection',),
        supports_symbols=True,
        inspect_routes=('/eod/value-factors',),
        output_tables=('daily_value_factor_results',),
        default_selected=True,
    ),
    'low_risk_factors': DailyStageDefinition(
        id='low_risk_factors',
        label='low-risk factor compute',
        group='factors',
        description='Rebuild persisted daily low-risk factor rows from stored price bars.',
        depends_on=('price_collection',),
        supports_symbols=True,
        inspect_routes=('/eod/low-risk-factors',),
        output_tables=('daily_low_risk_factor_results',),
        default_selected=True,
    ),
    'beta_factors': DailyStageDefinition(
        id='beta_factors',
        label='beta factor compute',
        group='factors',
        description='Rebuild persisted daily beta factor rows from stored price bars.',
        depends_on=('price_collection',),
        supports_symbols=True,
        inspect_routes=('/eod/beta-factors',),
        output_tables=('daily_beta_factor_results',),
        default_selected=True,
    ),
    'advanced_value_factors': DailyStageDefinition(
        id='advanced_value_factors',
        label='advanced value factor compute',
        group='factors',
        description='Rebuild persisted daily advanced value factor rows from stored price bars and annual fundamentals.',
        depends_on=('price_collection', 'fundamentals_daily_refresh'),
        supports_symbols=True,
        inspect_routes=('/eod/advanced-value-factors',),
        output_tables=('daily_advanced_value_factor_results',),
        default_selected=True,
    ),
    'unified_value_factors': DailyStageDefinition(
        id='unified_value_factors',
        label='unified value factor compute',
        group='factors',
        description='Rebuild persisted full-universe unified value factor rows from stored price bars and annual fundamentals.',
        depends_on=('price_collection', 'fundamentals_daily_refresh'),
        supports_symbols=True,
        inspect_routes=('/eod/unified-value-factors', '/eod/screening-results'),
        output_tables=('daily_unified_value_factor_results',),
        default_selected=True,
    ),
    'quality_factors': DailyStageDefinition(
        id='quality_factors',
        label='quality factor compute',
        group='factors',
        description='Rebuild persisted practical quality factor rows from stored price bars and annual fundamentals.',
        depends_on=('price_collection', 'fundamentals_daily_refresh'),
        supports_symbols=True,
        inspect_routes=('/eod/quality-factors',),
        output_tables=('daily_quality_factor_results',),
        default_selected=True,
    ),
    'composite_factors': DailyStageDefinition(
        id='composite_factors',
        label='composite factor compute',
        group='factors',
        description='Rebuild persisted daily composite factor rows from stored factor inputs after the component factor stages finish.',
        depends_on=(
            'momentum_factors',
            'value_factors',
            'low_risk_factors',
            'beta_factors',
            'advanced_value_factors',
        ),
        supports_symbols=True,
        inspect_routes=('/eod/composite-factors',),
        output_tables=('daily_composite_factor_results',),
        default_selected=True,
    ),
    'external_news_collect': DailyStageDefinition(
        id='external_news_collect',
        label='external news collect',
        group='external_data',
        description='Collect recent external news for the strategy-aware feedback prefilter universe before summarizer/decider dispatch.',
        depends_on=('market_risk_filter', 'piotroski_filter', *FACTOR_STAGE_ORDER),
        supports_symbols=True,
        supports_symbol_limit=True,
        inspect_routes=('/external/news/health', '/external/news'),
        output_tables=('external_news_items', 'external_news_item_symbols'),
        default_selected=True,
    ),
    'external_news_enrich': DailyStageDefinition(
        id='external_news_enrich',
        label='external news context enrich',
        group='external_data',
        description='Append a timestamped classifier version for legacy immutable news observations without rewriting their collected payload.',
        depends_on=('external_news_collect',),
        supports_symbols=True,
        supports_symbol_limit=True,
        inspect_routes=('/external/news',),
        output_tables=('external_news_item_classifications',),
        default_selected=True,
    ),
    'macro_context_refresh': DailyStageDefinition(
        id='macro_context_refresh',
        label='macro context refresh',
        group='external_data',
        description='Refresh persisted slow-moving macro context from free/no-key sources and explicitly label unavailable keyed providers.',
        depends_on=('external_news_enrich',),
        supports_force=True,
        inspect_routes=('/macro/context', '/macro/context/refresh'),
        output_tables=('macro_context_observations',),
        default_selected=True,
    ),
    'agent_feedback_summaries': DailyStageDefinition(
        id='agent_feedback_summaries',
        label='agent feedback summarizer dispatch',
        group='agent_feedback',
        description='Run the persisted symbol-level summarizer stages over the symbol universe that passes both active pre-filters.',
        depends_on=(
            'market_risk_filter',
            'piotroski_filter',
            *FACTOR_STAGE_ORDER,
            'external_news_collect',
            'external_news_enrich',
            'macro_context_refresh',
        ),
        supports_symbols=True,
        supports_capture_limit=True,
        inspect_routes=('/agent-context/feedback/progress', '/agent-context/feedback/status'),
        output_tables=('agent_context_feedback', 'agent_context_feedback_runs'),
        default_selected=True,
    ),
    'signal_decider': DailyStageDefinition(
        id='signal_decider',
        label='decision support decider dispatch',
        group='agent_feedback',
        description='Run the persisted decision-support decider stages over the same active-pre-filter intersection using stored summaries.',
        depends_on=('agent_feedback_summaries',),
        supports_symbols=True,
        supports_capture_limit=True,
        inspect_routes=('/agent-context/feedback/progress', '/agent-context/feedback/latest'),
        output_tables=('agent_context_feedback', 'agent_context_feedback_runs'),
        default_selected=True,
    ),
    'signal_decider_evaluations': DailyStageDefinition(
        id='signal_decider_evaluations',
        label='signal decider evaluation refresh',
        group='agent_feedback',
        description='Refresh realized outcomes and verdict rows for recent immutable signal-decider decision snapshots.',
        depends_on=('signal_decider',),
        supports_symbols=True,
        supports_force=True,
        inspect_routes=('/agent-context/signal-decider/evaluations', '/agent-context/signal-decider/diagnostics'),
        output_tables=('signal_decider_realized_outcomes', 'signal_decider_evaluations'),
        default_selected=True,
    ),
    'next_open_execution_plan': DailyStageDefinition(
        id='next_open_execution_plan',
        label='next-open execution plan refresh',
        group='execution_planning',
        description='Materialize persisted next-open execution plan rows immediately after decision-support deciders complete.',
        depends_on=('signal_decider',),
        supports_force=True,
        inspect_routes=('/trading/orders/next-open-execution',),
        output_tables=('signal_decider_execution_plan_items',),
        default_selected=True,
    ),
}


class DailyPipelineStageService:
    def __init__(
        self,
        *,
        pykrx_service=PYKRX_BACKFILL_SERVICE,
        pykrx_enrichment_service_ref=pykrx_enrichment_service,
        feedback_retry_service=agent_feedback_retry_service,
        decision_loop_service=signal_decider_loop_service,
        run_state_service=None,
    ) -> None:
        self.pykrx_service = pykrx_service
        self.pykrx_enrichment_service = pykrx_enrichment_service_ref
        self.feedback_retry_service = feedback_retry_service
        self.decision_loop_service = decision_loop_service
        self.run_state_service = run_state_service or daily_pipeline_run_state_service

    def describe(self) -> dict[str, Any]:
        return {
            'stage_order': list(DAILY_STAGE_ORDER),
            'default_daily_stage_order': list(DEFAULT_DAILY_STAGE_ORDER),
            'pykrx_pipeline_stage_order': list(PYKRX_PIPELINE_STAGE_ORDER),
            'stages': [STAGE_DEFINITIONS[stage_id].to_dict() for stage_id in DAILY_STAGE_ORDER],
        }

    def validate_run_request(
        self,
        *,
        market_date: str | date | None,
        stages: Sequence[str] | None,
        resume_from_stage: str | None,
    ) -> dict[str, Any]:
        requested_stages = self._resolve_requested_stages_for_resume(
            requested_stages=list(stages or DEFAULT_DAILY_STAGE_ORDER),
            resume_from_stage=resume_from_stage,
        )
        return {
            'market_date': self._normalize_market_date(market_date),
            'requested_stages': requested_stages,
            'resolved_stages': self._resolve_stage_ids(requested_stages),
        }

    def run(
        self,
        *,
        market_date: str | date | None = None,
        stages: Sequence[str] | None = None,
        symbols: Sequence[str] | None = None,
        markets: Sequence[str] | None = None,
        force: bool = False,
        bypass_prerequisite_gate: bool = False,
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
        trigger: str = 'stage_orchestrator',
    ) -> dict[str, Any]:
        validated_request = self.validate_run_request(
            market_date=market_date,
            stages=stages,
            resume_from_stage=resume_from_stage,
        )
        requested_stages = validated_request['requested_stages']
        resolved_stages = validated_request['resolved_stages']
        resolved_market_date = validated_request['market_date']
        resolved_symbols = self._normalize_symbols(symbols)
        resolved_markets = self._normalize_markets(markets)
        resolved_fs_div = (fs_div or settings.dart_fundamentals_default_fs_div).strip().upper()

        stage_outcomes: dict[str, dict[str, Any]] = {}
        handled: set[str] = set()
        pipeline_run = self.run_state_service.start_run(
            market_date=resolved_market_date,
            trigger=trigger,
            requested_stages=requested_stages,
            resolved_stages=resolved_stages,
            run_metadata={
                'entrypoint': 'daily_pipeline_stage_service.run',
                'resume_from_stage': resume_from_stage,
                'symbols': resolved_symbols,
                'markets': resolved_markets,
                'force': force,
                'fs_div': resolved_fs_div,
                'symbol_limit': symbol_limit,
                'symbol_offset': symbol_offset,
                'capture_limit': capture_limit,
                'reuse_existing_feedback': reuse_existing_feedback,
                'feedback_force_recompute': bool(force and not reuse_existing_feedback),
                'bypass_prerequisite_gate': bypass_prerequisite_gate,
            },
        )
        pipeline_run_id = int(pipeline_run['id'])
        if pipeline_run.get('deduplicated'):
            return {
                'triggered': False,
                'reason': pipeline_run.get('deduplication_reason') or 'duplicate_recovery',
                'pipeline_run_id': pipeline_run_id,
                'pipeline_run': pipeline_run,
                'market_date': resolved_market_date,
                'requested_stages': requested_stages,
                'resolved_stages': resolved_stages,
                'status': pipeline_run.get('status'),
                'stages': pipeline_run.get('stages') or {},
            }

        pykrx_requested = [stage_id for stage_id in resolved_stages if stage_id in PYKRX_PIPELINE_STAGE_ORDER]
        if 'price_collection' in pykrx_requested:
            try:
                pykrx_result = self.pykrx_service.run_daily_pipeline_stages(
                    market_date=resolved_market_date,
                    stages=pykrx_requested,
                    markets=resolved_markets,
                    trigger=trigger,
                    refresh_universe=(
                        settings.eod_auto_pykrx_refresh_universe if refresh_universe is None else refresh_universe
                    ),
                    force=force,
                    symbols=resolved_symbols,
                )
            except Exception as exc:
                self.run_state_service.finish_run(
                    pipeline_run_id=pipeline_run_id,
                    status='failed',
                    error_message=str(exc),
                )
                raise
            for stage_id in pykrx_requested:
                stage_result = self._extract_pykrx_stage_result(stage_id, pykrx_result)
                stage_outcomes[stage_id] = self._build_stage_outcome(stage_id, stage_result)
                self._finish_tracked_stage(
                    pipeline_run_id=pipeline_run_id,
                    stage_id=stage_id,
                    market_date=resolved_market_date,
                    outcome=stage_outcomes[stage_id],
                    result=stage_result,
                )
                handled.add(stage_id)

        for stage_id in resolved_stages:
            if stage_id in handled:
                continue
            self._start_tracked_stage(
                pipeline_run_id=pipeline_run_id,
                stage_id=stage_id,
                market_date=resolved_market_date,
                symbols=resolved_symbols,
            )
            incomplete_dependencies = [
                dependency
                for dependency in STAGE_DEFINITIONS[stage_id].depends_on
                if dependency in resolved_stages
                and str((stage_outcomes.get(dependency) or {}).get('status') or '').lower() != 'completed'
            ]
            if incomplete_dependencies and stage_id != 'next_open_execution_plan':
                stage_result = {
                    'triggered': False,
                    'status': 'blocked',
                    'reason': 'declared_dependencies_incomplete',
                    'market_date': resolved_market_date,
                    'incomplete_dependencies': incomplete_dependencies,
                }
                stage_outcomes[stage_id] = self._build_stage_outcome(stage_id, stage_result)
                self._finish_tracked_stage(
                    pipeline_run_id=pipeline_run_id,
                    stage_id=stage_id,
                    market_date=resolved_market_date,
                    outcome=stage_outcomes[stage_id],
                    result=stage_result,
                )
                continue
            if stage_id == 'next_open_execution_plan':
                decider_outcome = stage_outcomes.get('signal_decider')
                decider_status = str((decider_outcome or {}).get('status') or '').strip().lower()
                if decider_outcome is not None and decider_status != 'completed':
                    stage_result = {
                        'triggered': False,
                        'status': 'blocked',
                        'reason': 'signal_decider_stage_incomplete',
                        'market_date': resolved_market_date,
                        'upstream_status': decider_status or 'unknown',
                    }
                    stage_outcomes[stage_id] = self._build_stage_outcome(stage_id, stage_result)
                    self._finish_tracked_stage(
                        pipeline_run_id=pipeline_run_id,
                        stage_id=stage_id,
                        market_date=resolved_market_date,
                        outcome=stage_outcomes[stage_id],
                        result=stage_result,
                    )
                    continue
            try:
                stage_result = self._execute_stage(
                    stage_id,
                    market_date=resolved_market_date,
                    symbols=resolved_symbols,
                    markets=resolved_markets,
                    force=force,
                    bypass_prerequisite_gate=bypass_prerequisite_gate,
                    fs_div=resolved_fs_div,
                    symbol_limit=symbol_limit,
                    symbol_offset=symbol_offset,
                    capture_limit=capture_limit,
                    retry_cycles=retry_cycles,
                    feedback_max_cycles=feedback_max_cycles,
                    feedback_outer_batch_size=feedback_outer_batch_size,
                    reuse_existing_feedback=reuse_existing_feedback,
                    progress_callback=self._build_progress_callback(
                        pipeline_run_id=pipeline_run_id,
                        stage_id=stage_id,
                        market_date=resolved_market_date,
                    ),
                )
                stage_outcomes[stage_id] = self._build_stage_outcome(stage_id, stage_result)
                self._finish_tracked_stage(
                    pipeline_run_id=pipeline_run_id,
                    stage_id=stage_id,
                    market_date=resolved_market_date,
                    outcome=stage_outcomes[stage_id],
                    result=stage_result,
                )
            except Exception as exc:
                self.run_state_service.finish_stage(
                    pipeline_run_id=pipeline_run_id,
                    stage_id=stage_id,
                    market_date=resolved_market_date,
                    status='failed',
                    current_step=f'{stage_id}_failed',
                    current_activity=f'{STAGE_DEFINITIONS[stage_id].label} failed',
                    error_message=str(exc),
                )
                self.run_state_service.finish_run(
                    pipeline_run_id=pipeline_run_id,
                    status='failed',
                    error_message=str(exc),
                )
                raise

        overall_status = self._resolve_overall_status(stage_outcomes.values())
        final_run = self.run_state_service.finish_run(
            pipeline_run_id=pipeline_run_id,
            status=overall_status,
            run_metadata={
                'stage_statuses': {
                    stage_id: outcome.get('status')
                    for stage_id, outcome in stage_outcomes.items()
                },
            },
        )

        return {
            'pipeline_run_id': pipeline_run_id,
            'pipeline_run': final_run,
            'market_date': resolved_market_date,
            'requested_stages': requested_stages,
            'resolved_stages': resolved_stages,
            'status': overall_status,
            'stages': stage_outcomes,
        }

    def _start_tracked_stage(
        self,
        *,
        pipeline_run_id: int,
        stage_id: str,
        market_date: str,
        symbols: list[str] | None,
    ) -> None:
        definition = STAGE_DEFINITIONS[stage_id]
        self.run_state_service.start_stage(
            pipeline_run_id=pipeline_run_id,
            stage_id=stage_id,
            market_date=market_date,
            current_step=f'{stage_id}_started',
            current_activity=f'Running {definition.label}',
            processed=0,
            total=len(symbols or []),
            succeeded=0,
            failed=0,
            details={
                'group': definition.group,
                'depends_on': list(definition.depends_on),
                'output_tables': list(definition.output_tables),
                'inspect_routes': list(definition.inspect_routes),
            },
        )

    def _build_progress_callback(
        self,
        *,
        pipeline_run_id: int,
        stage_id: str,
        market_date: str,
    ) -> Callable[..., None]:
        def _checkpoint(**payload: Any) -> None:
            self.run_state_service.update_stage(
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

        return _checkpoint

    def _finish_tracked_stage(
        self,
        *,
        pipeline_run_id: int,
        stage_id: str,
        market_date: str,
        outcome: dict[str, Any],
        result: dict[str, Any],
    ) -> None:
        counts = self._extract_stage_counts(stage_id=stage_id, result=result)
        status = str(outcome.get('status') or 'completed')
        self.run_state_service.finish_stage(
            pipeline_run_id=pipeline_run_id,
            stage_id=stage_id,
            market_date=market_date,
            status=status,
            processed=counts['processed'],
            total=counts['total'],
            succeeded=counts['succeeded'],
            failed=counts['failed'],
            current_step=f'{stage_id}_{status}',
            current_activity=f'{self._status_activity_prefix(status)} {STAGE_DEFINITIONS[stage_id].label}',
            error_message=str(result.get('error') or result.get('error_message') or '') or None,
            source_run_ids=self._extract_stage_source_run_ids(result),
            details=self._build_stage_run_details(stage_id=stage_id, result=result),
        )

    @staticmethod
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

    @staticmethod
    def _extract_stage_counts(*, stage_id: str, result: dict[str, Any]) -> dict[str, int]:
        def _int(value: Any) -> int:
            try:
                return int(value or 0)
            except (TypeError, ValueError):
                return 0

        if stage_id == 'price_collection':
            total = _int(result.get('symbols_total'))
            succeeded = _int(result.get('symbols_succeeded'))
            failed = _int(result.get('symbols_failed'))
            processed = succeeded + failed
            return {
                'processed': processed,
                'total': total,
                'succeeded': succeeded,
                'failed': failed,
            }
        if stage_id == 'market_risk_filter':
            succeeded = _int(result.get('results_persisted'))
            failed = _int(result.get('results_failed'))
            total = len(result.get('matched_symbols') or []) or succeeded + failed
            return {
                'processed': succeeded + failed,
                'total': total,
                'succeeded': succeeded,
                'failed': failed,
            }
        if stage_id == 'market_risk_snapshots':
            succeeded = _int(result.get('snapshots_persisted'))
            failed = _int(result.get('snapshots_failed'))
            total = len(result.get('matched_symbols') or []) or succeeded + failed
            return {
                'processed': succeeded + failed,
                'total': total,
                'succeeded': succeeded,
                'failed': failed,
            }
        if stage_id == 'pykrx_enrichment':
            category_results = result.get('categories') if isinstance(result.get('categories'), dict) else {}
            categories_requested = result.get('categories_requested') or list(category_results)
            failed_categories = {
                category
                for category, details in category_results.items()
                if isinstance(details, dict) and details.get('errors')
            }
            downstream_preflight = (
                result.get('downstream_preflight')
                if isinstance(result.get('downstream_preflight'), dict)
                else {}
            )
            failed_categories.update(
                str(category)
                for category in downstream_preflight.get('failed_categories') or []
            )
            succeeded = sum(
                1
                for category, details in category_results.items()
                if isinstance(details, dict)
                and category not in failed_categories
                and int(details.get('rows_persisted') or 0) > 0
            )
            failed = len(failed_categories)
            total = len(categories_requested) or succeeded + failed
            return {
                'processed': min(succeeded + failed, total) if total else succeeded + failed,
                'total': total,
                'succeeded': succeeded,
                'failed': failed,
            }
        if stage_id == 'macro_context_refresh':
            succeeded = _int(result.get('observations_upserted'))
            failed = _int(result.get('observations_failed'))
            total = succeeded + failed
            return {
                'processed': total,
                'total': total,
                'succeeded': succeeded,
                'failed': failed,
            }
        if stage_id == 'external_news_collect':
            summary = result.get('summary') if isinstance(result.get('summary'), dict) else {}
            total_key = (
                'eligible_symbol_count'
                if 'eligible_symbol_count' in summary
                else 'requested_symbol_count'
            )
            succeeded_key = (
                'collected_symbol_count'
                if 'collected_symbol_count' in summary
                else 'matched_symbol_count'
            )
            total = _int(summary.get(total_key))
            succeeded = _int(summary.get(succeeded_key))
            failed = max(total - succeeded, 0)
            return {
                'processed': min(succeeded + failed, total) if total else succeeded + failed,
                'total': total,
                'succeeded': succeeded,
                'failed': failed,
            }
        if stage_id == 'external_news_enrich':
            inspected = _int(result.get('items_inspected'))
            missing_item_ids = result.get('missing_item_ids')
            remaining = _int(result.get('items_remaining_at_least'))
            failed_preview_count = (
                len(set(missing_item_ids))
                if isinstance(missing_item_ids, list)
                else 0
            )
            failed = max(
                failed_preview_count,
                _int(result.get('items_missing_classification')) - remaining,
            )
            return {
                'processed': inspected,
                'total': inspected + remaining,
                'succeeded': max(inspected - failed, 0),
                'failed': min(failed, inspected),
            }
        if stage_id == 'next_open_execution_plan':
            counts = extract_next_open_plan_stage_counts(result)
            return {
                'processed': counts.processed,
                'total': counts.total,
                'succeeded': counts.succeeded,
                'failed': counts.failed,
            }

        count_pairs = (
            ('snapshots_persisted', 'snapshots_failed'),
            ('action_summaries_persisted', 'action_summaries_failed'),
            ('features_persisted', 'features_failed'),
            ('results_persisted', 'results_failed'),
            ('symbols_refreshed', 'symbols_failed'),
            ('events_captured', 'events_failed'),
            ('persisted_count', 'failed_count'),
            ('completed_count', 'failed_count'),
            ('evaluations_upserted', 'failed_count'),
            ('outcomes_upserted', 'failed_count'),
        )
        for succeeded_key, failed_key in count_pairs:
            if succeeded_key not in result and failed_key not in result:
                continue
            succeeded = _int(result.get(succeeded_key))
            failed = _int(result.get(failed_key))
            total = _int(result.get('total') or result.get('symbols_total') or result.get('requested_count'))
            if total <= 0:
                total = succeeded + failed
            return {
                'processed': succeeded + failed,
                'total': total,
                'succeeded': succeeded,
                'failed': failed,
            }

        summary = result.get('summary') if isinstance(result.get('summary'), dict) else {}
        if summary:
            succeeded = _int(summary.get('completed_count') or summary.get('evaluated_count') or summary.get('passed_count'))
            failed = _int(summary.get('failed_count'))
            total = _int(summary.get('matched_symbol_count') or summary.get('total') or succeeded + failed)
            return {
                'processed': min(succeeded + failed, total) if total else succeeded + failed,
                'total': total,
                'succeeded': succeeded,
                'failed': failed,
            }

        return {'processed': 0, 'total': 0, 'succeeded': 0, 'failed': 0}

    @staticmethod
    def _extract_stage_count_semantics(
        *,
        stage_id: str,
        result: dict[str, Any],
    ) -> dict[str, Any]:
        if stage_id != 'external_news_enrich':
            return {}
        try:
            remaining_at_least = int(result.get('items_remaining_at_least') or 0)
        except (TypeError, ValueError):
            remaining_at_least = 0
        if remaining_at_least <= 0:
            return {}
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id=stage_id,
            result=result,
        )
        return {
            'total': 'lower_bound',
            'total_at_least': counts['total'],
            'remaining': 'lower_bound',
            'remaining_at_least': remaining_at_least,
        }

    @staticmethod
    def _has_explicit_zero_stage_scope(
        *,
        stage_id: str,
        result: dict[str, Any],
    ) -> bool:
        def _is_explicit_zero(payload: dict[str, Any], key: str) -> bool:
            if key not in payload:
                return False
            value = payload.get(key)
            if isinstance(value, bool) or value is None:
                return False
            if isinstance(value, int):
                return value == 0
            if isinstance(value, float):
                return value == 0.0
            if not isinstance(value, str):
                return False
            try:
                return int(value.strip()) == 0
            except ValueError:
                return False

        if stage_id == 'external_news_collect':
            summary = result.get('summary') if isinstance(result.get('summary'), dict) else {}
            total_key = (
                'eligible_symbol_count'
                if 'eligible_symbol_count' in summary
                else 'requested_symbol_count'
            )
            return _is_explicit_zero(summary, total_key)
        if stage_id == 'external_news_enrich':
            return _is_explicit_zero(
                result,
                'items_inspected',
            ) and _is_explicit_zero(result, 'items_remaining_at_least')
        return False

    @staticmethod
    def _extract_stage_source_run_ids(result: dict[str, Any]) -> dict[str, Any]:
        source_run_ids = result.get('source_run_ids') if isinstance(result.get('source_run_ids'), dict) else {}
        extracted = dict(source_run_ids)
        if result.get('run_id') is not None:
            extracted.setdefault('run_id', result.get('run_id'))
        if result.get('capture_run_id') is not None:
            extracted.setdefault('capture_run_id', result.get('capture_run_id'))
        latest_run = result.get('latest_run') if isinstance(result.get('latest_run'), dict) else None
        if latest_run and latest_run.get('id') is not None:
            extracted.setdefault('latest_run_id', latest_run.get('id'))
        return extracted

    @staticmethod
    def _build_stage_run_details(*, stage_id: str, result: dict[str, Any]) -> dict[str, Any]:
        definition = STAGE_DEFINITIONS[stage_id]
        omitted_keys = {'items', 'rows', 'payloads'}
        result_summary = {
            key: value
            for key, value in result.items()
            if key not in omitted_keys and not key.endswith('_payload')
        }
        details = {
            'group': definition.group,
            'depends_on': list(definition.depends_on),
            'output_tables': list(definition.output_tables),
            'inspect_routes': list(definition.inspect_routes),
            'result_summary': result_summary,
        }
        count_semantics = DailyPipelineStageService._extract_stage_count_semantics(
            stage_id=stage_id,
            result=result,
        )
        if count_semantics:
            details['count_semantics'] = count_semantics
        return details

    def _execute_stage(
        self,
        stage_id: str,
        *,
        market_date: str,
        symbols: list[str] | None,
        markets: list[str],
        force: bool,
        bypass_prerequisite_gate: bool,
        fs_div: str,
        symbol_limit: int | None,
        symbol_offset: int,
        capture_limit: int,
        retry_cycles: int | None,
        feedback_max_cycles: int,
        feedback_outer_batch_size: int,
        reuse_existing_feedback: bool,
        progress_callback: Callable[..., Any] | None = None,
    ) -> dict[str, Any]:
        if stage_id == 'master_refresh':
            symbols_refreshed = refresh_master_if_due(force=force)
            return {
                'refreshed': True,
                'symbol_count': len(symbols_refreshed),
            }
        if stage_id == 'pykrx_universe_refresh':
            return self.pykrx_service.refresh_recent_universe(
                markets=tuple(markets),
                limit=symbol_limit,
                deactivate_missing=symbol_limit is None,
            )
        if stage_id == 'pykrx_enrichment':
            result = self.pykrx_enrichment_service.collect_for_market_date(
                market_date=market_date,
                markets=markets,
                symbols=symbols,
            )
            preflight = build_pykrx_enrichment_preflight(
                market_date=market_date,
                coverage=dict(result.get('coverage') or {}),
                categories=REQUIRED_EXEC_DECIDER_PYKRX_ENRICHMENT_CATEGORIES,
            )
            result['downstream_preflight'] = preflight
            if (
                not preflight.get('allowed')
                and str(result.get('status') or '').strip().lower()
                not in {'failed', 'blocked', 'waiting'}
            ):
                result['status'] = 'partial'
            return result
        if stage_id == 'technical_snapshots':
            return daily_technical_snapshot_service.rebuild_for_market_date(
                market_date=market_date,
                symbols=symbols,
                source='stage_orchestrator',
            )
        if stage_id == 'action_summaries':
            return daily_action_summary_service.rebuild_for_market_date(
                market_date=market_date,
                symbols=symbols,
                source='stage_orchestrator',
            )
        if stage_id == 'trending_ranks':
            return compute_and_persist_daily_trending_ranks(
                market_date=market_date,
                source='stage_orchestrator',
            )
        if stage_id == 'signal_features':
            return asyncio.run(
                daily_signal_feature_service.refresh_for_market_date(
                    market_date=market_date,
                )
            )
        if stage_id == 'market_risk_snapshots':
            return asyncio.run(
                daily_market_risk_snapshot_service.collect_for_market_date(
                    market_date=market_date,
                    symbols=symbols,
                    source=MARKET_RISK_SNAPSHOT_SOURCE,
                    market_code='J',
                    progress_callback=progress_callback,
                )
            )
        if stage_id == 'market_risk_filter':
            return asyncio.run(
                daily_market_risk_filter_service.rebuild_for_market_date(
                    market_date=market_date,
                    symbols=symbols,
                    source=MARKET_RISK_FILTER_SOURCE,
                    market_code='J',
                    progress_callback=progress_callback,
                )
            )
        if stage_id == 'fundamentals_daily_refresh':
            return asyncio.run(
                dart_fundamentals_capture_service.run_daily_incremental_refresh(
                    market_date=market_date,
                    symbol_limit=symbol_limit,
                    symbol_offset=symbol_offset,
                    capture_limit=capture_limit,
                    retry_cycles=retry_cycles,
                    force=force,
                    fs_div=fs_div,
                    trigger='stage_orchestrator',
                    run_piotroski_followup=False,
                    run_feedback_followup=False,
                    progress_callback=progress_callback,
                )
            )
        if stage_id == 'piotroski_filter':
            return daily_piotroski_filter_service.rebuild_for_market_date(market_date=market_date)
        if stage_id == 'momentum_factors':
            return daily_momentum_factor_service.rebuild_for_market_date(
                market_date=market_date,
                symbols=symbols,
            )
        if stage_id == 'value_factors':
            return daily_value_factor_service.rebuild_for_market_date(
                market_date=market_date,
                symbols=symbols,
                source=VALUE_FACTOR_SOURCE,
            )
        if stage_id == 'low_risk_factors':
            return daily_low_risk_factor_service.rebuild_for_market_date(
                market_date=market_date,
                symbols=symbols,
                source=LOW_RISK_SOURCE,
            )
        if stage_id == 'beta_factors':
            return daily_beta_factor_service.rebuild_for_market_date(
                market_date=market_date,
                symbols=symbols,
                source=BETA_FACTOR_SOURCE,
            )
        if stage_id == 'advanced_value_factors':
            return daily_advanced_value_factor_service.rebuild_for_market_date(
                market_date=market_date,
                symbols=symbols,
                source=ADVANCED_VALUE_FACTOR_SOURCE,
            )
        if stage_id == 'unified_value_factors':
            return daily_unified_value_factor_service.rebuild_for_market_date(
                market_date=market_date,
                symbols=symbols,
                source=UNIFIED_VALUE_FACTOR_SOURCE,
            )
        if stage_id == 'quality_factors':
            return daily_quality_factor_service.rebuild_for_market_date(
                market_date=market_date,
                symbols=symbols,
                source=QUALITY_FACTOR_SOURCE,
            )
        if stage_id == 'composite_factors':
            return daily_composite_factor_service.rebuild_for_market_date(
                market_date=market_date,
                symbols=symbols,
                source=COMPOSITE_FACTOR_SOURCE,
            )
        if stage_id == 'external_news_collect':
            return asyncio.run(
                external_news_service.collect_for_agent_feedback_candidates(
                    market_date=market_date,
                    symbols=symbols,
                    display=settings.external_news_default_display,
                    symbol_limit=symbol_limit,
                )
            )
        if stage_id == 'external_news_enrich':
            return external_news_service.enrich_recent_classification(
                symbols=symbols,
                lookback_days=settings.external_news_context_lookback_days,
                limit=symbol_limit or 1000,
            )
        if stage_id == 'macro_context_refresh':
            return macro_context_service.refresh_context(
                force=force,
                trigger='daily_pipeline_stage',
            )
        if stage_id in {'agent_feedback_summaries', 'signal_decider'}:
            prerequisites = agent_feedback_prerequisite_service.inspect_feedback_prerequisites(
                market_date=market_date,
                fs_div=fs_div,
            )
            if not bypass_prerequisite_gate and not prerequisites['ready']:
                return self._build_prerequisite_gate_result(
                    stage_id=stage_id,
                    market_date=market_date,
                    prerequisites=prerequisites,
                )
            roles = (
                SUMMARIZER_AGENT_NAMES
                if stage_id == 'agent_feedback_summaries'
                else DECISION_SUPPORT_AGENT_NAMES
            )
            return self.feedback_retry_service.run_retry_cycles(
                config=FeedbackRetryCycleConfig(
                    market_date=date.fromisoformat(market_date),
                    roles=roles,
                    symbols=tuple(symbols) if symbols else None,
                    # A production retry of a completed source day must see
                    # information collected after that session close. The
                    # resolved instant remains fixed for every child cycle.
                    knowledge_cutoff_at=datetime.now(timezone.utc),
                    feedback_scope_mode=(
                        FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE
                        if stage_id == 'agent_feedback_summaries'
                        else FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                    ),
                    max_cycles=feedback_max_cycles,
                    outer_batch_size=feedback_outer_batch_size,
                    reuse_existing=reuse_existing_feedback,
                    progress_callback=progress_callback,
                ),
                # A forced pipeline recovery must bypass stale prerequisite
                # gates, but it must not silently turn an idempotent resume
                # into a full model recompute when reuse was requested.
                force=force and not reuse_existing_feedback,
                bypass_prerequisite_gate=bypass_prerequisite_gate,
            )
        if stage_id == 'next_open_execution_plan':
            from app.orders.next_open_execution import signal_decider_next_open_execution_service

            target_trade_date = (date.fromisoformat(market_date) + timedelta(days=1)).isoformat()
            return signal_decider_next_open_execution_service.refresh_plan_items(
                target_trade_date=target_trade_date,
                trigger='daily_pipeline_post_decider',
                force=force,
            )
        if stage_id == 'signal_decider_evaluations':
            prerequisites = agent_feedback_prerequisite_service.inspect_feedback_prerequisites(
                market_date=market_date,
                fs_div=fs_div,
            )
            if not bypass_prerequisite_gate and not prerequisites['ready']:
                return self._build_prerequisite_gate_result(
                    stage_id=stage_id,
                    market_date=market_date,
                    prerequisites=prerequisites,
                )
            return self.decision_loop_service.compute_recent_evaluations(
                as_of_market_date=market_date,
                lookback_days=DEFAULT_SIGNAL_DECIDER_EVALUATION_LOOKBACK_DAYS,
                symbols=symbols,
                force_recompute=force,
            )
        raise ValueError(f'Unsupported daily stage: {stage_id}')

    @staticmethod
    def _build_prerequisite_gate_result(
        *,
        stage_id: str,
        market_date: str,
        prerequisites: dict[str, Any],
    ) -> dict[str, Any]:
        gate_status = str(prerequisites.get('status') or 'waiting').strip().lower()
        if gate_status not in {'waiting', 'blocked'}:
            gate_status = 'waiting'
        return {
            'triggered': False,
            'status': gate_status,
            'reason': 'feedback_prerequisites_not_ready',
            'market_date': market_date,
            'stage': stage_id,
            'prerequisites': prerequisites,
            'missing_prerequisites': prerequisites.get('missing_prerequisites') or [],
        }

    def _extract_pykrx_stage_result(self, stage_id: str, result: dict[str, Any]) -> dict[str, Any]:
        if result.get('skipped') or result.get('triggered') is False:
            payload = dict(result)
            payload.setdefault('market_date', result.get('latest_available_trade_date'))
            return payload
        if stage_id == 'price_collection':
            try:
                total = max(int(result.get('symbols_total') or 0), 0)
                succeeded = max(int(result.get('symbols_succeeded') or 0), 0)
                failed = max(int(result.get('symbols_failed') or 0), 0)
            except (TypeError, ValueError):
                total = succeeded = failed = 0
            aggregate_status = str(result.get('status') or '').strip().lower()
            if failed > 0:
                price_status = 'partial' if succeeded > 0 else 'failed'
            elif total > 0 and succeeded < total:
                price_status = 'partial'
            elif total <= 0 and aggregate_status in {
                'failed',
                'blocked',
                'waiting',
                'interrupted',
                'abandoned',
            }:
                price_status = aggregate_status
            else:
                price_status = 'completed'
            return {
                'run_id': result.get('run_id'),
                'status': price_status,
                'market_date': result.get('latest_available_trade_date'),
                'symbols_total': result.get('symbols_total'),
                'symbols_succeeded': result.get('symbols_succeeded'),
                'symbols_failed': result.get('symbols_failed'),
                'failed_symbols': result.get('failed_symbols'),
                'bars_upserted': result.get('bars_upserted'),
            }
        payload = dict(result.get(stage_id) or {})
        payload.setdefault('run_id', result.get('run_id'))
        payload.setdefault('market_date', result.get('latest_available_trade_date'))
        return payload

    def _build_stage_outcome(self, stage_id: str, result: dict[str, Any]) -> dict[str, Any]:
        definition = STAGE_DEFINITIONS[stage_id]
        return {
            'stage': stage_id,
            'label': definition.label,
            'group': definition.group,
            'status': self._resolve_result_status(result),
            'result': result,
        }

    def _resolve_stage_ids(self, requested_stages: Sequence[str]) -> list[str]:
        requested_set: set[str] = set()
        normalized: list[str] = []
        for raw_stage in requested_stages:
            stage_id = str(raw_stage or '').strip()
            if not stage_id:
                continue
            if stage_id not in STAGE_DEFINITIONS:
                raise ValueError(f'Unknown daily stage: {stage_id}')
            if stage_id in requested_set:
                continue
            requested_set.add(stage_id)
        for stage_id in DAILY_STAGE_ORDER:
            if stage_id in requested_set:
                normalized.append(stage_id)
        if not normalized:
            raise ValueError('At least one daily stage is required.')
        return normalized

    def _resolve_requested_stages_for_resume(
        self,
        *,
        requested_stages: Sequence[str],
        resume_from_stage: str | None,
    ) -> list[str]:
        normalized_requested = [str(stage_id or '').strip() for stage_id in requested_stages if str(stage_id or '').strip()]
        if not resume_from_stage:
            return normalized_requested

        resume_stage = str(resume_from_stage or '').strip()
        if resume_stage not in STAGE_DEFINITIONS:
            raise ValueError(f'Unknown resume_from_stage: {resume_stage}')
        resume_index = DAILY_STAGE_ORDER.index(resume_stage)
        if not normalized_requested:
            return list(DAILY_STAGE_ORDER[resume_index:])
        requested_set = set(normalized_requested)
        requested_set.add(resume_stage)
        return [
            stage_id
            for stage_id in DAILY_STAGE_ORDER[resume_index:]
            if stage_id in requested_set
        ] or [resume_stage]

    @staticmethod
    def _normalize_symbols(symbols: Sequence[str] | None) -> list[str] | None:
        if not symbols:
            return None
        items: list[str] = []
        seen: set[str] = set()
        for raw_symbol in symbols:
            symbol = str(raw_symbol or '').strip().upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            items.append(symbol)
        return items or None

    def _normalize_markets(self, markets: Sequence[str] | None) -> list[str]:
        if markets:
            normalized = [str(item).strip().upper() for item in markets if str(item).strip()]
            return normalized or list(DEFAULT_MARKETS)
        configured = [item.strip().upper() for item in settings.eod_auto_pykrx_markets.split(',') if item.strip()]
        return configured or list(DEFAULT_MARKETS)

    def _normalize_market_date(self, market_date: str | date | None) -> str:
        if market_date is None:
            return str(get_market_state().server_time)[:10]
        if isinstance(market_date, date):
            return market_date.isoformat()
        return date.fromisoformat(str(market_date)).isoformat()

    @staticmethod
    def _resolve_result_status(result: dict[str, Any]) -> str:
        def _failure_count(value: Any) -> int:
            if isinstance(value, (list, tuple, set, dict)):
                return len(value)
            try:
                return int(value or 0)
            except (TypeError, ValueError):
                return 0

        failed_keys = (
            'failed',
            'failed_count',
            'failed_items',
            'results_failed',
            'snapshots_failed',
            'action_summaries_failed',
            'features_failed',
            'symbols_failed',
            'contexts_failed',
        )
        if result.get('skipped') or result.get('triggered') is False:
            raw_gate_status = str(result.get('status') or '').strip().lower()
            if raw_gate_status in {'waiting', 'blocked'}:
                return raw_gate_status
            return 'skipped'
        nested_payloads = [result]
        summary_payload = result.get('summary')
        if isinstance(summary_payload, dict):
            nested_payloads.append(summary_payload)
        progress_payload = result.get('progress')
        if isinstance(progress_payload, dict):
            nested_payloads.append(progress_payload)
            progress_summary = progress_payload.get('summary')
            if isinstance(progress_summary, dict):
                nested_payloads.append(progress_summary)
        has_reported_failures = False
        for payload in nested_payloads:
            for failed_key in failed_keys:
                if _failure_count(payload.get(failed_key)) > 0:
                    has_reported_failures = True
                    break
            if has_reported_failures:
                break

        raw_status = str(result.get('status') or '').strip().lower()
        normalized_status = {
            'ok': 'completed',
            'recovered': 'completed',
            'completed_with_errors': 'partial',
            'warning': 'partial',
            'unavailable': 'partial',
            'disabled': 'skipped',
            'not_configured': 'skipped',
        }.get(raw_status, raw_status)
        if raw_status:
            if normalized_status == 'completed' and has_reported_failures:
                return 'partial'
            return normalized_status
        if 'completed' in result and 'final_pending' in result:
            return 'completed' if bool(result.get('completed')) else 'partial'
        if result.get('error') or result.get('error_message'):
            if has_reported_failures:
                return 'partial'
            if DailyPipelineStageService._has_successful_work(result):
                return 'partial'
            return 'failed'

        for failed_key in failed_keys:
            if failed_key in result:
                return 'completed' if _failure_count(result.get(failed_key)) == 0 else 'partial'
        return 'completed'

    @staticmethod
    def _has_successful_work(result: dict[str, Any]) -> bool:
        success_suffixes = ('_persisted', '_succeeded', '_refreshed', '_fresh', '_upserted')
        success_keys = {'bars_upserted', 'persisted_count'}
        for key, value in result.items():
            if isinstance(value, bool):
                continue
            if not isinstance(value, int):
                continue
            if value <= 0:
                continue
            if key in success_keys or key.endswith(success_suffixes):
                return True
        return False

    @staticmethod
    def _resolve_overall_status(stage_outcomes: Sequence[dict[str, Any]]) -> str:
        statuses = [str(item.get('status') or 'completed') for item in stage_outcomes]
        if any(status == 'blocked' for status in statuses):
            return 'blocked'
        if any(status == 'waiting' for status in statuses):
            return 'waiting'
        if any(status == 'failed' for status in statuses):
            return 'failed'
        if any(status == 'running' for status in statuses):
            return 'running'
        if any(status == 'partial' for status in statuses):
            return 'partial'
        if statuses and all(status == 'skipped' for status in statuses):
            return 'skipped'
        return 'completed'


daily_pipeline_stage_service = DailyPipelineStageService()
