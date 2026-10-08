from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Sequence

from app.agent_context.derived_contexts import daily_price_context_service
from app.agent_context.retry_service import (
    FeedbackRetryCycleConfig,
    agent_feedback_retry_service,
    resolve_latest_completed_summary_knowledge_cutoff,
)
from app.agent_context.prerequisite_service import (
    FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER,
    agent_feedback_prerequisite_service,
)
from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES, SUMMARIZER_AGENT_NAMES
from app.agent_context.run_modes import (
    FEEDBACK_SCOPE_AUTHORITY_COMPLETED_SUMMARY_INTERSECTION,
    FEEDBACK_SCOPE_AUTHORITY_SCHEMA_VERSION,
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
from app.eod.daily_pipeline_outcomes import (
    daily_pipeline_outcome_is_satisfied,
    daily_pipeline_result_has_no_ready_content,
    daily_pipeline_zero_scope_gate_status,
    pykrx_enrichment_preflight_is_ready,
)
from app.eod.daily_pipeline_state import daily_pipeline_run_state_service
from app.eod.summary_cutoff_contract import (
    LEGACY_CUTOFF_REPAIR_STAGE_IDS,
    SUMMARY_CUTOFF_CONTRACT_REPAIR_VERSION,
    completed_summary_scope_matches_parent,
    summary_cutoff_proof_from_stage,
)
from app.eod.master import refresh_master_if_due
from app.eod.pykrx_backfill import DEFAULT_MARKETS, PYKRX_BACKFILL_SERVICE
from app.eod.pykrx_enrichment import pykrx_enrichment_service
from app.eod.pykrx_enrichment_gate import (
    REQUIRED_EXEC_DECIDER_PYKRX_ENRICHMENT_CATEGORIES,
    build_pykrx_enrichment_preflight,
)
from app.eod.pykrx_pipeline_runner import (
    POINT_IN_TIME_RECOVERY_CURRENT_ONLY_STAGE_IDS,
    POINT_IN_TIME_RECOVERY_BOUNDARY_TRIGGERS,
    resolve_point_in_time_recovery,
    should_skip_point_in_time_current_only_stage,
)
from app.eod.run_stage_support import (
    EXPIRED_DECISION_REPLAY_STAGE_IDS,
    build_expired_decision_replay_result,
    build_next_open_window_expired_result,
    extract_next_open_plan_stage_counts,
)
from app.eod.stage_heartbeat import PeriodicStageHeartbeat
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

logger = logging.getLogger(__name__)

PYKRX_PIPELINE_STAGE_ORDER: tuple[str, ...] = (
    'price_collection',
    'technical_snapshots',
    'action_summaries',
    'trending_ranks',
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
    'signal_features',
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
    'signal_features',
    'external_news_collect',
    'external_news_enrich',
    'macro_context_refresh',
    'agent_feedback_summaries',
    'signal_decider',
    'next_open_execution_plan',
    'signal_decider_evaluations',
)
TERMINAL_FEEDBACK_CONTINUATION_STAGE_ORDER: tuple[str, ...] = (
    'agent_feedback_summaries',
    'signal_decider',
    'next_open_execution_plan',
    'signal_decider_evaluations',
)
DEFAULT_FEEDBACK_MAX_CYCLES = 3
DEFAULT_FEEDBACK_OUTER_BATCH_SIZE = 20
FACTOR_STAGE_HEARTBEAT_INTERVAL_SECONDS = 60.0
DAILY_PIPELINE_FINALIZATION_ATTEMPTS = 3
CURRENT_FEEDBACK_PREREQUISITE_STAGE_ORDER: tuple[str, ...] = (
    *FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER,
    'signal_features',
)
# External news collect/enrich and macro refresh are deliberately absent from
# the hard gate. daily_pipeline_outcomes classifies them as ordering-only
# optional context whose terminal degradation may lower confidence but must
# not block feedback. Historical point-in-time recovery also excludes current-
# only signal features and retains the date-bounded numeric prerequisite set.

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
        description='Rebuild persisted daily price contexts and technical snapshots from stored price bars.',
        depends_on=('price_collection',),
        supports_symbols=True,
        inspect_routes=('/eod/technical-snapshots',),
        output_tables=('daily_price_contexts', 'daily_technical_snapshots'),
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
        description='Collect ranking-based KIS daily signal features and participation context after same-day filters and factor scores are ready.',
        depends_on=(
            'market_risk_filter',
            'piotroski_filter',
            *FACTOR_STAGE_ORDER,
        ),
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
        description=(
            'Rebuild persisted daily Piotroski results from stored DART fundamentals, '
            'PyKRX PBR, and price momentum.'
        ),
        depends_on=('fundamentals_daily_refresh', 'pykrx_enrichment'),
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
        description=(
            'Rebuild persisted daily value factor rows from same-day PyKRX valuation snapshots.'
        ),
        depends_on=('price_collection', 'pykrx_enrichment'),
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
        description=(
            'Rebuild persisted daily advanced value rows from price bars, same-day PyKRX '
            'market caps, and annual DART fundamentals.'
        ),
        depends_on=('price_collection', 'pykrx_enrichment', 'fundamentals_daily_refresh'),
        supports_symbols=True,
        inspect_routes=('/eod/advanced-value-factors',),
        output_tables=('daily_advanced_value_factor_results',),
        default_selected=True,
    ),
    'unified_value_factors': DailyStageDefinition(
        id='unified_value_factors',
        label='unified value factor compute',
        group='factors',
        description=(
            'Rebuild persisted full-universe value rows from price bars, same-day PyKRX '
            'valuation, and annual DART fundamentals.'
        ),
        depends_on=('price_collection', 'pykrx_enrichment', 'fundamentals_daily_refresh'),
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
            'signal_features',
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
        output_tables=(
            'agent_context_feedback',
            'agent_context_feedback_runs',
            'signal_decider_decision_snapshots',
        ),
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
            'resolved_stages': self._resolve_stage_ids(
                requested_stages,
                inject_next_open_dependency=resume_from_stage is None,
            ),
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
        feedback_max_cycles: int = DEFAULT_FEEDBACK_MAX_CYCLES,
        feedback_outer_batch_size: int = (
            DEFAULT_FEEDBACK_OUTER_BATCH_SIZE
        ),
        reuse_existing_feedback: bool = True,
        resume_from_stage: str | None = None,
        trigger: str = 'stage_orchestrator',
        reused_eod_collection_run_id: int | None = None,
        point_in_time_recovery: bool = False,
        existing_pipeline_run_id: int | None = None,
        inherit_feedback_prerequisite_admission: bool = False,
        provider_admission_fence_owned: bool = False,
        summary_cutoff_proof: Mapping[str, Any] | None = None,
        summary_cutoff_contract_repair: bool = False,
    ) -> dict[str, Any]:
        validated_request = self.validate_run_request(
            market_date=market_date,
            stages=stages,
            resume_from_stage=resume_from_stage,
        )
        requested_stages = validated_request['requested_stages']
        resolved_stages = validated_request['resolved_stages']
        resolved_market_date = validated_request['market_date']
        point_in_time_recovery = resolve_point_in_time_recovery(
            market_date=resolved_market_date,
            requested=point_in_time_recovery,
            recovery_context=(
                trigger in POINT_IN_TIME_RECOVERY_BOUNDARY_TRIGGERS
            ),
            market=(
                get_market_state()
                if not point_in_time_recovery
                and trigger in POINT_IN_TIME_RECOVERY_BOUNDARY_TRIGGERS
                else None
            ),
        )
        resolved_symbols = self._normalize_symbols(symbols)
        resolved_markets = self._normalize_markets(markets)
        resolved_fs_div = (fs_div or settings.dart_fundamentals_default_fs_div).strip().upper()
        feedback_knowledge_cutoff_at: datetime | None = None
        feedback_cutoff_resolution: dict[str, Any] | None = None
        terminal_summary_source_run_id: str | None = None
        normalized_summary_cutoff_proof = (
            dict(summary_cutoff_proof)
            if isinstance(summary_cutoff_proof, Mapping)
            else None
        )
        if summary_cutoff_contract_repair and not (
            trigger == 'auto_eod_followup_resume'
            and tuple(resolved_stages) == LEGACY_CUTOFF_REPAIR_STAGE_IDS
            and resume_from_stage == LEGACY_CUTOFF_REPAIR_STAGE_IDS[0]
            and reused_eod_collection_run_id is not None
            and isinstance(normalized_summary_cutoff_proof, dict)
            and str(
                normalized_summary_cutoff_proof.get('source_run_id') or ''
            ).strip()
            and str(
                normalized_summary_cutoff_proof.get(
                    'feedback_scope_mode'
                )
                or ''
            ).strip()
            == FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
        ):
            raise ValueError(
                'summary cutoff contract repair requires the exact '
                'automatic decider suffix and FAST summary proof'
            )
        feedback_prerequisite_stage_ids = (
            self._feedback_prerequisite_stage_ids(
                point_in_time_recovery=point_in_time_recovery,
            )
        )
        persisted_feedback_prerequisite_stage_ids = tuple(
            stage_id
            for stage_id in feedback_prerequisite_stage_ids
            if stage_id not in resolved_stages
        )
        if inherit_feedback_prerequisite_admission and not (
            trigger == 'auto_eod_followup_resume'
            and tuple(resolved_stages)
            == TERMINAL_FEEDBACK_CONTINUATION_STAGE_ORDER
            and resume_from_stage
            == TERMINAL_FEEDBACK_CONTINUATION_STAGE_ORDER[0]
            and reused_eod_collection_run_id is not None
            and point_in_time_recovery
            and reuse_existing_feedback
            and not force
            and existing_pipeline_run_id is None
        ):
            raise ValueError(
                'feedback prerequisite admission inheritance requires the '
                'exact point-in-time automatic feedback suffix retry'
            )
        if (
            existing_pipeline_run_id is None
            and not inherit_feedback_prerequisite_admission
            and self._requires_persisted_feedback_prerequisite_preflight(
                resolved_stages,
                required_stage_ids=feedback_prerequisite_stage_ids,
            )
        ):
            prerequisites = (
                agent_feedback_prerequisite_service.inspect_feedback_prerequisites(
                    market_date=resolved_market_date,
                    fs_div=resolved_fs_div,
                    required_stage_ids=(
                        persisted_feedback_prerequisite_stage_ids
                    ),
                )
            )
            if not prerequisites['ready']:
                gated_stage_id = next(
                    stage_id
                    for stage_id in (
                        'agent_feedback_summaries',
                        'signal_decider',
                    )
                    if stage_id in resolved_stages
                )
                gate_result = self._build_prerequisite_gate_result(
                    stage_id=gated_stage_id,
                    market_date=resolved_market_date,
                    prerequisites=prerequisites,
                )
                return {
                    'triggered': False,
                    'reason': gate_result['reason'],
                    'pipeline_run_id': None,
                    'pipeline_run': None,
                    'market_date': resolved_market_date,
                    'requested_stages': requested_stages,
                    'resolved_stages': resolved_stages,
                    'status': gate_result['status'],
                    'stages': {
                        gated_stage_id: self._build_stage_outcome(
                            gated_stage_id,
                            gate_result,
                        ),
                    },
                    'prerequisites': prerequisites,
                }

        stage_outcomes: dict[str, dict[str, Any]] = {}
        handled: set[str] = set()
        if (
            existing_pipeline_run_id is not None
            and summary_cutoff_contract_repair
        ):
            repair_claim = (
                self.run_state_service
                .claim_summary_cutoff_contract_repair_resume(
                    pipeline_run_id=int(existing_pipeline_run_id),
                    market_date=resolved_market_date,
                    reused_eod_collection_run_id=int(
                        reused_eod_collection_run_id
                    ),
                )
            )
            if not repair_claim.get('claimed'):
                return {
                    'triggered': False,
                    'reason': (
                        repair_claim.get('reason')
                        or 'cutoff_contract_repair_not_claimed'
                    ),
                    'pipeline_run_id': int(existing_pipeline_run_id),
                    'pipeline_run': repair_claim.get('pipeline_run'),
                    'market_date': resolved_market_date,
                    'requested_stages': requested_stages,
                    'resolved_stages': resolved_stages,
                    'status': 'blocked',
                    'stages': {},
                }
            pipeline_run = repair_claim.get('pipeline_run')
            try:
                if not isinstance(pipeline_run, dict):
                    raise RuntimeError(
                        'cutoff contract repair claim did not return pipeline '
                        'state'
                    )
                repair_context = (
                    self._cutoff_contract_repair_execution_context(
                        pipeline_run
                    )
                )
                resolved_symbols = repair_context['symbols']
                resolved_markets = repair_context['markets']
                force = repair_context['force']
                bypass_prerequisite_gate = repair_context[
                    'bypass_prerequisite_gate'
                ]
                resolved_fs_div = repair_context['fs_div']
                symbol_limit = repair_context['symbol_limit']
                symbol_offset = repair_context['symbol_offset']
                capture_limit = repair_context['capture_limit']
                retry_cycles = repair_context['retry_cycles']
                feedback_max_cycles = repair_context[
                    'feedback_max_cycles'
                ]
                feedback_outer_batch_size = repair_context[
                    'feedback_outer_batch_size'
                ]
                reuse_existing_feedback = repair_context[
                    'reuse_existing_feedback'
                ]
                point_in_time_recovery = repair_context[
                    'point_in_time_recovery'
                ]
                normalized_summary_cutoff_proof = repair_context[
                    'summary_cutoff_proof'
                ]
                claimed_cutoff_resolution = repair_claim.get(
                    'knowledge_cutoff_resolution'
                )
                claimed_cutoff_symbols = (
                    claimed_cutoff_resolution.get('requested_symbols')
                    if isinstance(claimed_cutoff_resolution, dict)
                    else None
                )
                if not (
                    isinstance(claimed_cutoff_resolution, dict)
                    and claimed_cutoff_resolution.get('ready') is True
                    and isinstance(
                        claimed_cutoff_resolution.get(
                            'knowledge_cutoff_at'
                        ),
                        datetime,
                    )
                    and claimed_cutoff_resolution.get('source_run_id')
                    == normalized_summary_cutoff_proof.get(
                        'source_run_id'
                    )
                    and claimed_cutoff_resolution.get(
                        'feedback_scope_mode'
                    )
                    == normalized_summary_cutoff_proof.get(
                        'feedback_scope_mode'
                    )
                    and isinstance(claimed_cutoff_symbols, list)
                    and claimed_cutoff_symbols
                    and (
                        resolved_symbols is None
                        or completed_summary_scope_matches_parent(
                            feedback_scope_mode=(
                                claimed_cutoff_resolution.get(
                                    'feedback_scope_mode'
                                )
                            ),
                            completed_symbols=claimed_cutoff_symbols,
                            completed_scope_proof=(
                                claimed_cutoff_resolution.get(
                                    'completed_scope_proof'
                                )
                            ),
                            parent_symbols=resolved_symbols,
                        )
                    )
                ):
                    raise RuntimeError(
                        'cutoff contract repair claim returned invalid '
                        'summary cutoff proof'
                    )
                feedback_cutoff_resolution = dict(
                    claimed_cutoff_resolution
                )
                feedback_knowledge_cutoff_at = (
                    claimed_cutoff_resolution['knowledge_cutoff_at']
                )
            except Exception as exc:
                self._finish_run_preserving_primary_error(
                    pipeline_run_id=int(existing_pipeline_run_id),
                    status='failed',
                    error_message=str(exc),
                )
                raise
        elif existing_pipeline_run_id is not None:
            if trigger != 'auto_eod_followup_resume':
                raise ValueError(
                    'existing_pipeline_run_id is restricted to auto EOD follow-up recovery'
                )
            if reused_eod_collection_run_id is None:
                raise ValueError(
                    'reused_eod_collection_run_id is required for terminal continuation'
                )
            if tuple(resolved_stages) != TERMINAL_FEEDBACK_CONTINUATION_STAGE_ORDER:
                raise ValueError(
                    'terminal continuation must run the exact feedback suffix'
                )
            continuation_claim = (
                self.run_state_service.claim_terminal_continuation(
                    pipeline_run_id=int(existing_pipeline_run_id),
                    market_date=resolved_market_date,
                    reused_eod_collection_run_id=int(
                        reused_eod_collection_run_id
                    ),
                    resume_from_stage='agent_feedback_summaries',
                    reason='summary_knowledge_cutoff_repair',
                )
            )
            if not continuation_claim.get('claimed'):
                return {
                    'triggered': False,
                    'reason': (
                        continuation_claim.get('reason')
                        or 'terminal_continuation_not_claimed'
                    ),
                    'pipeline_run_id': int(existing_pipeline_run_id),
                    'pipeline_run': continuation_claim.get('pipeline_run'),
                    'market_date': resolved_market_date,
                    'requested_stages': requested_stages,
                    'resolved_stages': resolved_stages,
                    'status': 'blocked',
                    'stages': {},
                    'knowledge_cutoff_resolution': (
                        continuation_claim.get(
                            'knowledge_cutoff_resolution'
                        )
                    ),
                }
            pipeline_run = continuation_claim.get('pipeline_run')
            if not isinstance(pipeline_run, dict):
                raise RuntimeError(
                    'terminal continuation claim did not return pipeline state'
                )
            try:
                continuation_context = (
                    self._terminal_continuation_execution_context(
                        pipeline_run
                    )
                )
            except Exception as exc:
                self._finish_run_preserving_primary_error(
                    pipeline_run_id=int(existing_pipeline_run_id),
                    status='failed',
                    error_message=str(exc),
                )
                raise
            resolved_symbols = continuation_context['symbols']
            resolved_markets = continuation_context['markets']
            force = continuation_context['force']
            bypass_prerequisite_gate = continuation_context[
                'bypass_prerequisite_gate'
            ]
            resolved_fs_div = continuation_context['fs_div']
            symbol_limit = continuation_context['symbol_limit']
            symbol_offset = continuation_context['symbol_offset']
            capture_limit = continuation_context['capture_limit']
            retry_cycles = continuation_context['retry_cycles']
            feedback_max_cycles = continuation_context[
                'feedback_max_cycles'
            ]
            feedback_outer_batch_size = continuation_context[
                'feedback_outer_batch_size'
            ]
            reuse_existing_feedback = continuation_context[
                'reuse_existing_feedback'
            ]
            point_in_time_recovery = continuation_context[
                'point_in_time_recovery'
            ]
            terminal_summary_source_run_id = continuation_context[
                'summary_source_run_id'
            ]
            feedback_cutoff_resolution = continuation_claim.get(
                'knowledge_cutoff_resolution'
            )
            raw_persisted_stage_outcomes = continuation_claim.get(
                'persisted_stage_outcomes',
                {},
            )
            if not isinstance(raw_persisted_stage_outcomes, dict):
                self._finish_run_preserving_primary_error(
                    pipeline_run_id=int(existing_pipeline_run_id),
                    status='failed',
                    error_message=(
                        'terminal continuation claim returned invalid '
                        'persisted stage outcomes'
                    ),
                )
                raise RuntimeError(
                    'terminal continuation claim returned invalid '
                    'persisted stage outcomes'
                )
            persisted_stage_ids = list(raw_persisted_stage_outcomes)
            if persisted_stage_ids != resolved_stages[
                : len(persisted_stage_ids)
            ]:
                self._finish_run_preserving_primary_error(
                    pipeline_run_id=int(existing_pipeline_run_id),
                    status='failed',
                    error_message=(
                        'terminal continuation claim returned a '
                        'noncontiguous persisted stage prefix'
                    ),
                )
                raise RuntimeError(
                    'terminal continuation claim returned a '
                    'noncontiguous persisted stage prefix'
                )
            effective_summary_source_run_id = (
                terminal_summary_source_run_id
            )
            effective_summary_scope_mode: str | None = None
            if persisted_stage_ids[:1] == ['agent_feedback_summaries']:
                persisted_summary_outcome = raw_persisted_stage_outcomes[
                    'agent_feedback_summaries'
                ]
                if str(
                    persisted_summary_outcome.get('status') or ''
                ).strip().lower() == 'completed':
                    persisted_summary_proof = (
                        summary_cutoff_proof_from_stage(
                            persisted_summary_outcome
                        )
                    )
                    if persisted_summary_proof is None:
                        self._finish_run_preserving_primary_error(
                            pipeline_run_id=int(existing_pipeline_run_id),
                            status='failed',
                            error_message=(
                                'terminal continuation claim returned invalid '
                                'persisted summary evidence'
                            ),
                        )
                        raise RuntimeError(
                            'terminal continuation claim returned invalid '
                            'persisted summary evidence'
                        )
                    effective_summary_source_run_id = str(
                        persisted_summary_proof['source_run_id']
                    ).strip()
                    effective_summary_scope_mode = str(
                        persisted_summary_proof['feedback_scope_mode']
                    ).strip()
            terminal_cutoff_symbols = (
                feedback_cutoff_resolution.get('requested_symbols')
                if isinstance(feedback_cutoff_resolution, dict)
                else None
            )
            if not (
                isinstance(feedback_cutoff_resolution, dict)
                and feedback_cutoff_resolution.get('ready')
                and feedback_cutoff_resolution.get(
                    'knowledge_cutoff_at'
                )
                is not None
                and feedback_cutoff_resolution.get('source_run_id')
                == effective_summary_source_run_id
                and (
                    effective_summary_scope_mode is None
                    or feedback_cutoff_resolution.get(
                        'feedback_scope_mode'
                    )
                    == effective_summary_scope_mode
                )
                and isinstance(terminal_cutoff_symbols, list)
                and terminal_cutoff_symbols
                and (
                    resolved_symbols is None
                    or completed_summary_scope_matches_parent(
                        feedback_scope_mode=(
                            feedback_cutoff_resolution.get(
                                'feedback_scope_mode'
                            )
                        ),
                        completed_symbols=terminal_cutoff_symbols,
                        completed_scope_proof=(
                            feedback_cutoff_resolution.get(
                                'completed_scope_proof'
                            )
                        ),
                        parent_symbols=resolved_symbols,
                    )
                )
            ):
                self._finish_run_preserving_primary_error(
                    pipeline_run_id=int(existing_pipeline_run_id),
                    status='failed',
                    error_message=(
                        'terminal continuation claim returned invalid '
                        'summary cutoff proof'
                    ),
                )
                raise RuntimeError(
                    'terminal continuation claim returned invalid '
                    'summary cutoff proof'
                )
            feedback_knowledge_cutoff_at = (
                feedback_cutoff_resolution['knowledge_cutoff_at']
            )
            for stage_id in persisted_stage_ids:
                persisted_outcome = raw_persisted_stage_outcomes[
                    stage_id
                ]
                if not (
                    isinstance(persisted_outcome, dict)
                    and daily_pipeline_outcome_is_satisfied(
                        persisted_outcome
                    )
                ):
                    self._finish_run_preserving_primary_error(
                        pipeline_run_id=int(existing_pipeline_run_id),
                        status='failed',
                        error_message=(
                            'terminal continuation claim returned an '
                            'unsatisfied persisted stage outcome'
                        ),
                    )
                    raise RuntimeError(
                        'terminal continuation claim returned an '
                        'unsatisfied persisted stage outcome'
                    )
                persisted_details = persisted_outcome.get('details')
                persisted_result = (
                    persisted_details.get('result_summary')
                    if isinstance(persisted_details, dict)
                    and isinstance(
                        persisted_details.get('result_summary'),
                        dict,
                    )
                    else {}
                )
                restored_outcome = self._build_stage_outcome(
                    stage_id,
                    {
                        **persisted_result,
                        'status': persisted_outcome['status'],
                        'resumed_from_persisted_stage': True,
                    },
                )
                restored_outcome['status'] = persisted_outcome['status']
                stage_outcomes[stage_id] = restored_outcome
                handled.add(stage_id)
        else:
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
                    'retry_cycles': retry_cycles,
                    'feedback_max_cycles': feedback_max_cycles,
                    'feedback_outer_batch_size': (
                        feedback_outer_batch_size
                    ),
                    'reuse_existing_feedback': reuse_existing_feedback,
                    'feedback_force_recompute': bool(force and not reuse_existing_feedback),
                    'bypass_prerequisite_gate': bypass_prerequisite_gate,
                    'inherited_feedback_prerequisite_admission': (
                        inherit_feedback_prerequisite_admission
                    ),
                    'reused_eod_collection_run_id': (
                        int(reused_eod_collection_run_id)
                        if reused_eod_collection_run_id is not None
                        else None
                    ),
                    'point_in_time_recovery': point_in_time_recovery,
                    **(
                        {
                            'summary_cutoff_contract_repair_version': (
                                SUMMARY_CUTOFF_CONTRACT_REPAIR_VERSION
                            ),
                            'summary_cutoff_contract_repair_proof': (
                                normalized_summary_cutoff_proof
                            ),
                        }
                        if summary_cutoff_contract_repair
                        else {}
                    ),
                },
                provider_admission_fence_owned=(
                    provider_admission_fence_owned
                ),
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
                pykrx_kwargs = {
                    'market_date': resolved_market_date,
                    'stages': pykrx_requested,
                    'markets': resolved_markets,
                    'trigger': trigger,
                    'refresh_universe': (
                        settings.eod_auto_pykrx_refresh_universe
                        if refresh_universe is None
                        else refresh_universe
                    ),
                    'force': force,
                    'symbols': resolved_symbols,
                }
                if point_in_time_recovery:
                    pykrx_kwargs['point_in_time_recovery'] = True
                pykrx_result = self.pykrx_service.run_daily_pipeline_stages(
                    **pykrx_kwargs,
                )
            except Exception as exc:
                self._finish_run_preserving_primary_error(
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
            stage_symbols = resolved_symbols
            if (
                stage_id == 'signal_decider'
                and isinstance(feedback_cutoff_resolution, dict)
                and feedback_cutoff_resolution.get('ready') is True
                and str(
                    feedback_cutoff_resolution.get('source_run_id') or ''
                ).strip()
            ):
                completed_summary_symbols = (
                    feedback_cutoff_resolution.get('requested_symbols')
                )
                if (
                    isinstance(completed_summary_symbols, list)
                    and completed_summary_symbols
                    and completed_summary_scope_matches_parent(
                        feedback_scope_mode=(
                            feedback_cutoff_resolution.get(
                                'feedback_scope_mode'
                            )
                        ),
                        completed_symbols=completed_summary_symbols,
                        completed_scope_proof=(
                            feedback_cutoff_resolution.get(
                                'completed_scope_proof'
                            )
                        ),
                        parent_symbols=resolved_symbols,
                    )
                ):
                    stage_symbols = list(completed_summary_symbols)
                else:
                    feedback_knowledge_cutoff_at = None
                    feedback_cutoff_resolution = {
                        **feedback_cutoff_resolution,
                        'ready': False,
                        'reason': (
                            'summary_run_symbol_scope_mismatch'
                            if isinstance(completed_summary_symbols, list)
                            and completed_summary_symbols
                            else 'summary_run_symbol_scope_unavailable'
                        ),
                    }
            if (
                existing_pipeline_run_id is not None
                and stage_id == 'signal_decider'
                and isinstance(feedback_cutoff_resolution, dict)
                and feedback_cutoff_resolution.get('ready') is True
                and str(
                    feedback_cutoff_resolution.get('source_run_id') or ''
                ).strip()
                and isinstance(
                    feedback_cutoff_resolution.get('requested_symbols'),
                    list,
                )
                and feedback_cutoff_resolution['requested_symbols']
            ):
                stage_symbols = list(
                    feedback_cutoff_resolution['requested_symbols']
                )
            try:
                self._start_tracked_stage(
                    pipeline_run_id=pipeline_run_id,
                    stage_id=stage_id,
                    market_date=resolved_market_date,
                    symbols=resolved_symbols,
                )
            except Exception as exc:
                self._finish_run_after_stage_start_failure(
                    pipeline_run_id=pipeline_run_id,
                    error_message=str(exc),
                )
                raise
            incomplete_dependencies = [
                dependency
                for dependency in STAGE_DEFINITIONS[stage_id].depends_on
                if dependency in resolved_stages
                and not self._dependency_outcome_is_satisfied(
                    stage_outcomes.get(dependency)
                )
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
                if decider_outcome is not None and not self._dependency_outcome_is_satisfied(
                    decider_outcome
                ):
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
            stage_heartbeat: PeriodicStageHeartbeat | None = None
            try:
                if stage_id in {
                    'agent_feedback_summaries',
                    'signal_decider',
                } and feedback_knowledge_cutoff_at is None and not (
                    stage_id == 'signal_decider'
                    and isinstance(feedback_cutoff_resolution, dict)
                    and str(
                        feedback_cutoff_resolution.get('source_run_id') or ''
                    ).strip()
                    and feedback_cutoff_resolution.get('ready') is False
                ):
                    if (
                        stage_id == 'agent_feedback_summaries'
                        and existing_pipeline_run_id is None
                    ):
                        feedback_knowledge_cutoff_at = datetime.now(
                            timezone.utc
                        )
                        feedback_cutoff_resolution = {
                            'ready': True,
                            'reason': 'current_pipeline_feedback_boundary',
                            'knowledge_cutoff_at': (
                                feedback_knowledge_cutoff_at
                            ),
                            'source_run_id': None,
                            'feedback_scope_mode': (
                                FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE
                            ),
                        }
                    else:
                        proof_source_run_id = str(
                            (
                                normalized_summary_cutoff_proof or {}
                            ).get('source_run_id')
                            or terminal_summary_source_run_id
                            or ''
                        ).strip()
                        proof_scope_mode = str(
                            (
                                normalized_summary_cutoff_proof or {}
                            ).get('feedback_scope_mode')
                            or ''
                        ).strip()
                        proof_requested_symbols = (
                            (normalized_summary_cutoff_proof or {}).get(
                                'requested_symbols'
                            )
                            if normalized_summary_cutoff_proof is not None
                            else (
                                resolved_symbols
                                if terminal_summary_source_run_id is not None
                                else None
                            )
                        )
                        proof_is_valid = bool(
                            proof_source_run_id
                            and proof_scope_mode
                            in {
                                FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
                                FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
                            }
                            and (
                                proof_requested_symbols is None
                                or isinstance(
                                    proof_requested_symbols,
                                    (list, tuple),
                                )
                            )
                        )
                        if (
                            trigger == 'auto_eod_followup_resume'
                            and existing_pipeline_run_id is None
                            and not proof_is_valid
                        ):
                            feedback_cutoff_resolution = {
                                'ready': False,
                                'reason': 'summary_cutoff_proof_unavailable',
                                'knowledge_cutoff_at': None,
                                'source_run_id': (
                                    proof_source_run_id or None
                                ),
                                'feedback_scope_mode': (
                                    proof_scope_mode or None
                                ),
                            }
                        else:
                            feedback_cutoff_resolution = (
                                resolve_latest_completed_summary_knowledge_cutoff(
                                    market_date=date.fromisoformat(
                                        resolved_market_date
                                    ),
                                    feedback_scope_mode=(
                                        proof_scope_mode
                                        or FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE
                                    ),
                                    fs_div=resolved_fs_div,
                                    source_run_id=(
                                        proof_source_run_id or None
                                    ),
                                    requested_symbols=(
                                        list(proof_requested_symbols)
                                        if isinstance(
                                            proof_requested_symbols,
                                            (list, tuple),
                                        )
                                        else None
                                    ),
                                )
                            )
                            if (
                                stage_id == 'signal_decider'
                                and not proof_source_run_id
                                and feedback_cutoff_resolution.get('ready')
                                is True
                                and str(
                                    feedback_cutoff_resolution.get(
                                        'source_run_id'
                                    )
                                    or ''
                                ).strip()
                                and not isinstance(
                                    feedback_cutoff_resolution.get(
                                        'completed_scope_proof'
                                    ),
                                    dict,
                                )
                            ):
                                latest_resolution = dict(
                                    feedback_cutoff_resolution
                                )
                                latest_source_run_id = str(
                                    latest_resolution['source_run_id']
                                ).strip()
                                latest_scope_mode = str(
                                    latest_resolution.get(
                                        'feedback_scope_mode'
                                    )
                                    or ''
                                ).strip()
                                latest_admitted_symbols = (
                                    latest_resolution.get(
                                        'requested_symbols'
                                    )
                                )
                                if not (
                                    latest_scope_mode
                                    in {
                                        FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
                                        FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
                                    }
                                    and isinstance(
                                        latest_admitted_symbols,
                                        list,
                                    )
                                    and latest_admitted_symbols
                                ):
                                    feedback_cutoff_resolution = {
                                        **latest_resolution,
                                        'ready': False,
                                        'reason': (
                                            'summary_run_symbol_scope_unavailable'
                                        ),
                                    }
                                else:
                                    exact_resolution = (
                                        resolve_latest_completed_summary_knowledge_cutoff(
                                            market_date=date.fromisoformat(
                                                resolved_market_date
                                            ),
                                            feedback_scope_mode=(
                                                latest_scope_mode
                                            ),
                                            fs_div=resolved_fs_div,
                                            source_run_id=(
                                                latest_source_run_id
                                            ),
                                            requested_symbols=list(
                                                latest_admitted_symbols
                                            ),
                                        )
                                    )
                                    exact_source_run_id = str(
                                        exact_resolution.get(
                                            'source_run_id'
                                        )
                                        or ''
                                    ).strip()
                                    exact_scope_mode = str(
                                        exact_resolution.get(
                                            'feedback_scope_mode'
                                        )
                                        or ''
                                    ).strip()
                                    if not (
                                        exact_resolution.get('ready')
                                        is True
                                        and exact_source_run_id
                                        == latest_source_run_id
                                        and exact_scope_mode
                                        == latest_scope_mode
                                        and exact_resolution.get(
                                            'knowledge_cutoff_at'
                                        )
                                        == latest_resolution.get(
                                            'knowledge_cutoff_at'
                                        )
                                    ):
                                        feedback_cutoff_resolution = {
                                            **exact_resolution,
                                            'ready': False,
                                            'reason': (
                                                exact_resolution.get(
                                                    'reason'
                                                )
                                                or (
                                                    'summary_exact_source_mismatch'
                                                )
                                            ),
                                        }
                                    else:
                                        feedback_cutoff_resolution = (
                                            exact_resolution
                                        )
                        if feedback_cutoff_resolution.get('ready'):
                            feedback_knowledge_cutoff_at = (
                                feedback_cutoff_resolution.get(
                                    'knowledge_cutoff_at'
                                )
                            )
                            resolved_proof_symbols = (
                                feedback_cutoff_resolution.get(
                                    'requested_symbols'
                                )
                            )
                            resolved_proof_source = str(
                                feedback_cutoff_resolution.get(
                                    'source_run_id'
                                )
                                or ''
                            ).strip()
                            resolved_proof_scope = str(
                                feedback_cutoff_resolution.get(
                                    'feedback_scope_mode'
                                )
                                or ''
                            ).strip()
                            exact_decider_scope = bool(
                                stage_id == 'signal_decider'
                                and resolved_proof_source
                            )
                            proof_scope_is_valid = bool(
                                isinstance(resolved_proof_symbols, list)
                                and resolved_proof_symbols
                                and completed_summary_scope_matches_parent(
                                    feedback_scope_mode=(
                                        resolved_proof_scope
                                    ),
                                    completed_symbols=(
                                        resolved_proof_symbols
                                    ),
                                    completed_scope_proof=(
                                        feedback_cutoff_resolution.get(
                                            'completed_scope_proof'
                                        )
                                    ),
                                    parent_symbols=resolved_symbols,
                                )
                            )
                            if exact_decider_scope and not proof_scope_is_valid:
                                feedback_knowledge_cutoff_at = None
                                feedback_cutoff_resolution = {
                                    **feedback_cutoff_resolution,
                                    'ready': False,
                                    'reason': (
                                        'summary_run_symbol_scope_mismatch'
                                        if isinstance(
                                            resolved_proof_symbols,
                                            list,
                                        )
                                        and resolved_proof_symbols
                                        else (
                                            'summary_run_symbol_scope_unavailable'
                                        )
                                    ),
                                }
                            elif exact_decider_scope:
                                stage_symbols = list(
                                    resolved_proof_symbols
                                )
                if (
                    stage_id == 'agent_feedback_summaries'
                    and isinstance(feedback_cutoff_resolution, dict)
                    and not feedback_cutoff_resolution.get('ready')
                ):
                    stage_result = {
                        'triggered': False,
                        'status': 'blocked',
                        'reason': 'summary_knowledge_cutoff_unavailable',
                        'market_date': resolved_market_date,
                        'stage': stage_id,
                        'knowledge_cutoff_resolution': {
                            key: value
                            for key, value in (
                                feedback_cutoff_resolution.items()
                            )
                            if key != 'knowledge_cutoff_at'
                        },
                    }
                else:
                    def execute_current_stage() -> dict[str, Any]:
                        return self._execute_stage(
                            stage_id,
                            market_date=resolved_market_date,
                            symbols=stage_symbols,
                            markets=resolved_markets,
                            force=force,
                            bypass_prerequisite_gate=(
                                bypass_prerequisite_gate
                                or inherit_feedback_prerequisite_admission
                            ),
                            fs_div=resolved_fs_div,
                            symbol_limit=symbol_limit,
                            symbol_offset=symbol_offset,
                            capture_limit=capture_limit,
                            retry_cycles=retry_cycles,
                            feedback_max_cycles=feedback_max_cycles,
                            feedback_outer_batch_size=(
                                feedback_outer_batch_size
                            ),
                            reuse_existing_feedback=(
                                reuse_existing_feedback
                            ),
                            feedback_knowledge_cutoff_at=(
                                feedback_knowledge_cutoff_at
                            ),
                            feedback_cutoff_resolution=(
                                feedback_cutoff_resolution
                            ),
                            point_in_time_recovery=(
                                point_in_time_recovery
                            ),
                            provider_admission_fence_owned=(
                                provider_admission_fence_owned
                            ),
                            run_id=reused_eod_collection_run_id,
                            progress_callback=(
                                self._build_progress_callback(
                                    pipeline_run_id=pipeline_run_id,
                                    stage_id=stage_id,
                                    market_date=resolved_market_date,
                                )
                            ),
                        )

                    if stage_id in FACTOR_STAGE_ORDER:
                        stage_heartbeat = PeriodicStageHeartbeat(
                            lambda: self.run_state_service.heartbeat_stage(
                                pipeline_run_id=pipeline_run_id,
                                stage_id=stage_id,
                                market_date=resolved_market_date,
                            ),
                            interval_seconds=(
                                FACTOR_STAGE_HEARTBEAT_INTERVAL_SECONDS
                            ),
                            thread_name=(
                                'daily-pipeline-'
                                f'{pipeline_run_id}-{stage_id}-heartbeat'
                            ),
                        )
                        if not stage_heartbeat.check_now():
                            stage_result = {
                                'triggered': False,
                                'status': 'interrupted',
                                'reason': 'pipeline_run_ownership_lost',
                                'market_date': resolved_market_date,
                                'stage': stage_id,
                            }
                            stage_outcomes[stage_id] = (
                                self._build_stage_outcome(
                                    stage_id,
                                    stage_result,
                                )
                            )
                            return {
                                'triggered': False,
                                'reason': 'pipeline_run_ownership_lost',
                                'pipeline_run_id': pipeline_run_id,
                                'pipeline_run': pipeline_run,
                                'market_date': resolved_market_date,
                                'requested_stages': requested_stages,
                                'resolved_stages': resolved_stages,
                                'status': 'interrupted',
                                'stages': stage_outcomes,
                            }
                        with stage_heartbeat:
                            stage_result = execute_current_stage()
                        if stage_heartbeat.ownership_lost:
                            stage_outcomes[stage_id] = (
                                self._build_stage_outcome(
                                    stage_id,
                                    stage_result,
                                )
                            )
                            return {
                                'triggered': False,
                                'reason': 'pipeline_run_ownership_lost',
                                'pipeline_run_id': pipeline_run_id,
                                'pipeline_run': pipeline_run,
                                'market_date': resolved_market_date,
                                'requested_stages': requested_stages,
                                'resolved_stages': resolved_stages,
                                'status': 'interrupted',
                                'stages': stage_outcomes,
                            }
                    else:
                        stage_result = execute_current_stage()
                stage_outcomes[stage_id] = self._build_stage_outcome(stage_id, stage_result)
                self._finish_tracked_stage(
                    pipeline_run_id=pipeline_run_id,
                    stage_id=stage_id,
                    market_date=resolved_market_date,
                    outcome=stage_outcomes[stage_id],
                    result=stage_result,
                )
                if (
                    stage_id == 'agent_feedback_summaries'
                    and 'signal_decider' in resolved_stages
                    and self._dependency_outcome_is_satisfied(
                        stage_outcomes[stage_id]
                    )
                ):
                    summary_source_run_id = str(
                        stage_result.get('run_id') or ''
                    ).strip()
                    summary_scope_mode = str(
                        stage_result.get('feedback_scope_mode') or ''
                    ).strip()
                    summary_requested_symbols = stage_result.get(
                        'requested_symbols'
                    )
                    if not (
                        summary_source_run_id
                        and summary_scope_mode
                        in {
                            FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
                            FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
                        }
                        and (
                            summary_requested_symbols is None
                            or isinstance(summary_requested_symbols, list)
                        )
                    ):
                        feedback_cutoff_resolution = {
                            'ready': False,
                            'reason': 'summary_cutoff_proof_unavailable',
                            'knowledge_cutoff_at': None,
                            'source_run_id': summary_source_run_id or None,
                            'feedback_scope_mode': summary_scope_mode or None,
                        }
                        feedback_knowledge_cutoff_at = None
                    else:
                        same_run_cutoff_resolution = (
                            resolve_latest_completed_summary_knowledge_cutoff(
                                market_date=date.fromisoformat(
                                    resolved_market_date
                                ),
                                feedback_scope_mode=summary_scope_mode,
                                fs_div=resolved_fs_div,
                                source_run_id=summary_source_run_id,
                                requested_symbols=(
                                    list(summary_requested_symbols)
                                    if summary_requested_symbols is not None
                                    else None
                                ),
                            )
                        )
                        if (
                            same_run_cutoff_resolution.get('ready')
                            and same_run_cutoff_resolution.get(
                                'knowledge_cutoff_at'
                            )
                            != feedback_knowledge_cutoff_at
                        ):
                            same_run_cutoff_resolution = {
                                **same_run_cutoff_resolution,
                                'ready': False,
                                'reason': 'summary_cutoff_mismatch',
                                'knowledge_cutoff_at': None,
                            }
                        feedback_cutoff_resolution = (
                            same_run_cutoff_resolution
                        )
                        feedback_knowledge_cutoff_at = (
                            same_run_cutoff_resolution.get(
                                'knowledge_cutoff_at'
                            )
                            if same_run_cutoff_resolution.get('ready')
                            else None
                        )
            except Exception as exc:
                if (
                    stage_heartbeat is not None
                    and stage_heartbeat.ownership_lost
                ):
                    stage_result = {
                        'triggered': False,
                        'status': 'interrupted',
                        'reason': 'pipeline_run_ownership_lost',
                        'market_date': resolved_market_date,
                        'stage': stage_id,
                        'error_type': type(exc).__name__,
                        'error_message': str(exc),
                    }
                    stage_outcomes[stage_id] = self._build_stage_outcome(
                        stage_id,
                        stage_result,
                    )
                    return {
                        'triggered': False,
                        'reason': 'pipeline_run_ownership_lost',
                        'pipeline_run_id': pipeline_run_id,
                        'pipeline_run': pipeline_run,
                        'market_date': resolved_market_date,
                        'requested_stages': requested_stages,
                        'resolved_stages': resolved_stages,
                        'status': 'interrupted',
                        'stages': stage_outcomes,
                    }
                try:
                    self.run_state_service.finish_stage(
                        pipeline_run_id=pipeline_run_id,
                        stage_id=stage_id,
                        market_date=resolved_market_date,
                        status='failed',
                        current_step=f'{stage_id}_failed',
                        current_activity=(
                            f'{STAGE_DEFINITIONS[stage_id].label} failed'
                        ),
                        error_message=str(exc),
                    )
                except Exception as cleanup_exc:
                    logger.error(
                        'Unable to mark failed stage %s for pipeline run '
                        '%s: %s',
                        stage_id,
                        pipeline_run_id,
                        cleanup_exc,
                    )
                self._finish_run_preserving_primary_error(
                    pipeline_run_id=pipeline_run_id,
                    status='failed',
                    error_message=str(exc),
                    run_metadata=self._feedback_cutoff_run_metadata(
                        feedback_knowledge_cutoff_at,
                        feedback_cutoff_resolution,
                    ),
                )
                raise

        existing_run_metadata = (
            pipeline_run.get('run_metadata')
            if isinstance(pipeline_run.get('run_metadata'), dict)
            else {}
        )
        existing_stage_statuses = (
            existing_run_metadata.get('stage_statuses')
            if isinstance(existing_run_metadata.get('stage_statuses'), dict)
            else {}
        )
        merged_stage_statuses = {
            **existing_stage_statuses,
            **{
                stage_id: outcome.get('status')
                for stage_id, outcome in stage_outcomes.items()
            },
        }
        overall_status = self._resolve_overall_status(
            [
                {'status': stage_status}
                for stage_status in merged_stage_statuses.values()
            ]
        )
        run_metadata = {
            'stage_statuses': merged_stage_statuses,
            **self._feedback_cutoff_run_metadata(
                feedback_knowledge_cutoff_at,
                feedback_cutoff_resolution,
            ),
        }
        if overall_status == 'partial' and all(
            daily_pipeline_outcome_is_satisfied(outcome)
            for outcome in stage_outcomes.values()
        ):
            run_metadata['resumable'] = False
        final_run = self._finish_run_with_retry(
            pipeline_run_id=pipeline_run_id,
            status=overall_status,
            run_metadata=run_metadata,
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

    @staticmethod
    def _feedback_cutoff_run_metadata(
        knowledge_cutoff_at: datetime | None,
        resolution: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if knowledge_cutoff_at is None and not isinstance(resolution, dict):
            return {}
        return {
            'feedback_knowledge_cutoff': {
                'knowledge_cutoff_at': (
                    knowledge_cutoff_at.isoformat()
                    if knowledge_cutoff_at is not None
                    else None
                ),
                'ready': bool(
                    resolution.get('ready')
                    if isinstance(resolution, dict)
                    else knowledge_cutoff_at is not None
                ),
                'reason': (
                    resolution.get('reason')
                    if isinstance(resolution, dict)
                    else None
                ),
                'source_run_id': (
                    resolution.get('source_run_id')
                    if isinstance(resolution, dict)
                    else None
                ),
                'feedback_scope_mode': (
                    resolution.get('feedback_scope_mode')
                    if isinstance(resolution, dict)
                    else None
                ),
                'requested_symbols': (
                    list(resolution['requested_symbols'])
                    if isinstance(resolution, dict)
                    and isinstance(
                        resolution.get('requested_symbols'),
                        (list, tuple),
                    )
                    else None
                ),
            },
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

    def _finish_run_after_stage_start_failure(
        self,
        *,
        pipeline_run_id: int,
        error_message: str,
    ) -> None:
        """Bound terminalization when an admitted run cannot checkpoint a stage."""

        self._finish_run_preserving_primary_error(
            pipeline_run_id=pipeline_run_id,
            status='failed',
            error_message=error_message,
        )

    def _finish_run_with_retry(self, **kwargs: Any) -> dict[str, Any]:
        last_error: Exception | None = None
        for _attempt in range(DAILY_PIPELINE_FINALIZATION_ATTEMPTS):
            try:
                return self.run_state_service.finish_run(**kwargs)
            except Exception as exc:
                last_error = exc
        assert last_error is not None
        raise last_error

    def _finish_run_preserving_primary_error(self, **kwargs: Any) -> None:
        try:
            self._finish_run_with_retry(**kwargs)
        except Exception as exc:
            logger.error(
                'Unable to terminalize daily pipeline run %s after worker '
                'failure: %s',
                kwargs.get('pipeline_run_id'),
                exc,
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
        finish_kwargs = {
            'pipeline_run_id': pipeline_run_id,
            'stage_id': stage_id,
            'market_date': market_date,
            'status': status,
            'processed': counts['processed'],
            'total': counts['total'],
            'succeeded': counts['succeeded'],
            'failed': counts['failed'],
            'current_step': f'{stage_id}_{status}',
            'current_activity': (
                f'{self._status_activity_prefix(status)} '
                f'{STAGE_DEFINITIONS[stage_id].label}'
            ),
            'error_message': (
                str(
                    result.get('error')
                    or result.get('error_message')
                    or ''
                )
                or None
            ),
            'source_run_ids': self._extract_stage_source_run_ids(result),
            'details': self._build_stage_run_details(
                stage_id=stage_id,
                result=result,
            ),
        }
        last_error: Exception | None = None
        for _attempt in range(DAILY_PIPELINE_FINALIZATION_ATTEMPTS):
            try:
                self.run_state_service.finish_stage(**finish_kwargs)
                return
            except Exception as exc:
                last_error = exc
        assert last_error is not None
        self._finish_run_preserving_primary_error(
            pipeline_run_id=pipeline_run_id,
            status='failed',
            error_message=str(last_error),
        )
        raise last_error

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
    def _extract_role_progress(result: dict[str, Any]) -> dict[str, dict[str, Any]]:
        progress = result.get('progress')
        progress = progress if isinstance(progress, dict) else {}
        progress_summary = progress.get('summary')
        summary = result.get('summary')
        candidates = (
            progress_summary if isinstance(progress_summary, dict) else {},
            progress,
            summary if isinstance(summary, dict) else {},
            result,
        )
        for payload in candidates:
            role_progress = payload.get('role_progress')
            if isinstance(role_progress, dict):
                return {
                    str(role): state
                    for role, state in role_progress.items()
                    if isinstance(state, dict)
                }
        return {}

    @staticmethod
    def _extract_stage_counts(*, stage_id: str, result: dict[str, Any]) -> dict[str, int]:
        def _int(value: Any) -> int:
            try:
                return int(value or 0)
            except (TypeError, ValueError):
                return 0

        def _strict_non_negative_int(value: Any) -> int | None:
            if isinstance(value, bool) or value is None:
                return None
            if isinstance(value, int):
                return value if value >= 0 else None
            return None

        def _failed_contract_counts() -> dict[str, int]:
            return {
                'processed': 1,
                'total': 1,
                'succeeded': 0,
                'failed': 1,
            }

        if stage_id == 'agent_feedback_summaries':
            role_progress = DailyPipelineStageService._extract_role_progress(result)
            if role_progress:
                total = processed = succeeded = failed = 0
                for role_state in role_progress.values():
                    role_total = max(_int(role_state.get('total_symbols')), 0)
                    role_failed = max(_int(role_state.get('failed_items')), 0)
                    role_processed = max(
                        _int(role_state.get('processed_symbols')),
                        0,
                    )
                    if 'processed_symbols' not in role_state:
                        role_processed = max(
                            _int(role_state.get('completed_symbols')) + role_failed,
                            0,
                        )
                    if 'completed_symbols' in role_state:
                        role_succeeded = max(
                            _int(role_state.get('completed_symbols')),
                            0,
                        )
                    elif str(role_state.get('status') or '').strip().lower() in {
                        'complete',
                        'completed',
                    }:
                        role_succeeded = max(role_processed - role_failed, 0)
                    else:
                        role_succeeded = 0
                    total += role_total
                    processed += role_processed
                    succeeded += role_succeeded
                    failed += role_failed
                return {
                    'processed': processed,
                    'total': total,
                    'succeeded': succeeded,
                    'failed': failed,
                }
        if stage_id == 'signal_decider':
            progress = (
                result.get('progress')
                if isinstance(result.get('progress'), dict)
                else {}
            )
            summary = (
                progress.get('summary')
                if isinstance(progress.get('summary'), dict)
                else {}
            )
            result_payload = (
                progress.get('result')
                if isinstance(progress.get('result'), dict)
                else {}
            )
            role_results = (
                result_payload.get('roles')
                if isinstance(result_payload.get('roles'), dict)
                else {}
            )
            has_summary_counts = any(
                key in summary
                for key in (
                    'expected_feedback_count',
                    'completed_feedback_count',
                    'failed_items',
                )
            )
            role_total = role_succeeded = role_failed = 0
            has_valid_role_denominator = bool(role_results)
            if has_valid_role_denominator:
                for role_result in role_results.values():
                    if not isinstance(role_result, dict):
                        has_valid_role_denominator = False
                        break
                    requested_count = _strict_non_negative_int(
                        role_result.get('requested_count')
                    )
                    completed_count = _strict_non_negative_int(
                        role_result.get('completed_count')
                    )
                    error_count = _strict_non_negative_int(
                        role_result.get('error_count')
                    )
                    if (
                        requested_count is None
                        or requested_count <= 0
                        or completed_count is None
                        or completed_count > requested_count
                        or error_count is None
                        or completed_count + error_count > requested_count
                    ):
                        has_valid_role_denominator = False
                        break
                    role_total += requested_count
                    role_succeeded += completed_count
                    role_failed += error_count
            if has_summary_counts or role_results:
                if not has_valid_role_denominator or role_total <= 0:
                    return _failed_contract_counts()
                succeeded_candidates: list[int] = []
                if 'completed_feedback_count' in summary:
                    succeeded_candidates.append(
                        max(
                            _int(summary.get('completed_feedback_count')),
                            0,
                        )
                    )
                succeeded_candidates.append(role_succeeded)
                succeeded = (
                    min(succeeded_candidates)
                    if succeeded_candidates
                    else 0
                )
                summary_failed = (
                    max(_int(summary.get('failed_items')), 0)
                    if 'failed_items' in summary
                    else 0
                )
                failed = max(summary_failed, role_failed)
                processed = succeeded + failed
                summary_expected = (
                    max(_int(summary.get('expected_feedback_count')), 0)
                    if 'expected_feedback_count' in summary
                    else 0
                )
                return {
                    'processed': processed,
                    'total': max(summary_expected, role_total, processed),
                    'succeeded': succeeded,
                    'failed': failed,
                }
        if stage_id == 'signal_decider_evaluations':
            request = (
                result.get('request')
                if isinstance(result.get('request'), dict)
                else {}
            )
            summary = (
                result.get('summary')
                if isinstance(result.get('summary'), dict)
                else {}
            )
            if (
                'evaluations_upserted' in summary
                or 'outcomes_upserted' in summary
            ):
                evaluations = max(
                    _int(summary.get('evaluations_upserted')),
                    0,
                )
                outcomes = max(_int(summary.get('outcomes_upserted')), 0)
                explicit_failed = max(
                    _int(summary.get('failed_count')),
                    _int(summary.get('failed_items')),
                    _int(summary.get('not_computed_count')),
                    0,
                )
                mismatch = (
                    abs(outcomes - evaluations)
                    if 'evaluations_upserted' in summary
                    and 'outcomes_upserted' in summary
                    else 0
                )
                failed = max(explicit_failed, mismatch)
                succeeded = min(evaluations, outcomes)
                if 'outcomes_upserted' not in summary:
                    succeeded = evaluations
                elif 'evaluations_upserted' not in summary:
                    succeeded = outcomes
                horizons = request.get('horizons')
                valid_horizons = bool(
                    isinstance(horizons, (list, tuple))
                    and horizons
                    and all(
                        isinstance(horizon, int)
                        and not isinstance(horizon, bool)
                        and horizon > 0
                        for horizon in horizons
                    )
                    and len(set(horizons)) == len(horizons)
                )
                decisions_considered = (
                    _strict_non_negative_int(
                        summary.get('decisions_considered')
                    )
                    or 0
                )
                request_total = (
                    decisions_considered * len(horizons)
                    if valid_horizons and decisions_considered > 0
                    else 0
                )
                coverage = (
                    summary.get('coverage')
                    if isinstance(summary.get('coverage'), dict)
                    else {}
                )
                if request_total <= 0:
                    return _failed_contract_counts()
                coverage_count = _strict_non_negative_int(
                    coverage.get('count')
                )
                if 'count' in coverage and coverage_count != request_total:
                    return _failed_contract_counts()
                coverage_component_keys = (
                    'computed_count',
                    'pending_maturity_count',
                    'insufficient_price_data_count',
                    'not_computed_count',
                )
                present_coverage_components = tuple(
                    key for key in coverage_component_keys if key in coverage
                )
                if present_coverage_components:
                    component_counts = [
                        _strict_non_negative_int(coverage.get(key))
                        for key in coverage_component_keys
                    ]
                    if (
                        len(present_coverage_components)
                        != len(coverage_component_keys)
                        or any(count is None for count in component_counts)
                        or sum(
                            count
                            for count in component_counts
                            if count is not None
                        )
                        != request_total
                    ):
                        return _failed_contract_counts()
                coverage_total = coverage_count or 0
                total = max(
                    request_total,
                    coverage_total,
                    evaluations,
                    outcomes,
                    succeeded + failed,
                )
                return {
                    'processed': succeeded + failed,
                    'total': total,
                    'succeeded': succeeded,
                    'failed': failed,
                }
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
        if stage_id == 'technical_snapshots' and (
            'contexts_persisted' in result or 'contexts_failed' in result
        ):
            snapshot_succeeded = _int(result.get('snapshots_persisted'))
            snapshot_failed = _int(result.get('snapshots_failed'))
            context_succeeded = _int(result.get('contexts_persisted'))
            context_failed = _int(result.get('contexts_failed'))
            total = max(
                snapshot_succeeded + snapshot_failed,
                context_succeeded + context_failed,
            )
            failed = max(snapshot_failed, context_failed)
            return {
                'processed': total,
                'total': total,
                'succeeded': max(total - failed, 0),
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
            downstream_preflight = (
                result.get('downstream_preflight')
                if isinstance(result.get('downstream_preflight'), dict)
                else {}
            )
            failed_categories = {
                str(category)
                for category in downstream_preflight.get('failed_categories') or []
            }
            preflight_ready = pykrx_enrichment_preflight_is_ready(
                downstream_preflight
            )
            failed_categories.update(
                category
                for category, details in category_results.items()
                if isinstance(details, dict)
                and (
                    str(details.get('status') or '').strip().lower()
                    == 'failed'
                    or (
                        bool(details.get('errors'))
                        and not (
                            preflight_ready
                            and int(details.get('rows_persisted') or 0) > 0
                            and str(details.get('status') or '')
                            .strip()
                            .lower()
                            in {'completed', 'partial'}
                        )
                    )
                )
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
        if stage_id == 'trending_ranks' and 'periods_persisted' in result:
            succeeded = _int(result.get('periods_persisted'))
            failed = _int(result.get('periods_failed'))
            total = _int(result.get('periods_total')) or succeeded + failed
            return {
                'processed': succeeded + failed,
                'total': total,
                'succeeded': succeeded,
                'failed': failed,
            }

        count_pairs = (
            ('snapshots_persisted', 'snapshots_failed'),
            ('action_summaries_persisted', 'action_summaries_failed'),
            ('trending_ranks_persisted', 'trending_ranks_failed'),
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
    def compact_stage_result_summary(
        result: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
        """Remove bulk evidence while retaining the operational result summary."""

        omitted_keys = {
            'cycles',
            'eligible_symbols',
            'funnel_diagnostics',
            'items',
            'matched_symbols',
            'payloads',
            'requested_symbols',
            'role_symbol_scopes',
            'rows',
            'stages',
        }
        max_list_items = 100
        omissions: dict[str, dict[str, Any]] = {}

        def _describe(value: Any) -> dict[str, Any]:
            if isinstance(value, dict):
                return {'type': 'object', 'item_count': len(value)}
            if isinstance(value, (list, tuple, set)):
                return {'type': 'list', 'item_count': len(value)}
            if isinstance(value, str):
                return {'type': 'string', 'character_count': len(value)}
            return {'type': type(value).__name__}

        def _compact(value: Any, *, path: str) -> tuple[bool, Any]:
            if isinstance(value, dict):
                compacted: dict[str, Any] = {}
                for raw_key, nested_value in value.items():
                    key = str(raw_key)
                    nested_path = f'{path}.{key}' if path else key
                    is_named_bulk_field = key in omitted_keys or key.endswith('_payload')
                    is_bulk_value = isinstance(
                        nested_value,
                        (dict, list, tuple, set),
                    ) or (
                        isinstance(nested_value, str)
                        and len(nested_value) > 1_000
                    )
                    if is_named_bulk_field and is_bulk_value:
                        omissions[nested_path] = _describe(nested_value)
                        continue
                    keep, nested_compacted = _compact(nested_value, path=nested_path)
                    if keep:
                        compacted[key] = nested_compacted
                return True, compacted
            if isinstance(value, (list, tuple, set)):
                if len(value) > max_list_items:
                    omissions[path] = _describe(value)
                    return False, None
                compacted_items = []
                for index, nested_value in enumerate(value):
                    keep, nested_compacted = _compact(
                        nested_value,
                        path=f'{path}[{index}]',
                    )
                    if keep:
                        compacted_items.append(nested_compacted)
                return True, compacted_items
            return True, value

        _, compacted_result = _compact(result, path='')
        return compacted_result, omissions

    @staticmethod
    def _build_stage_run_details(*, stage_id: str, result: dict[str, Any]) -> dict[str, Any]:
        definition = STAGE_DEFINITIONS[stage_id]
        result_summary, result_summary_omissions = (
            DailyPipelineStageService.compact_stage_result_summary(result)
        )
        details = {
            'group': definition.group,
            'depends_on': list(definition.depends_on),
            'output_tables': list(definition.output_tables),
            'inspect_routes': list(definition.inspect_routes),
            'result_summary': result_summary,
        }
        if result_summary_omissions:
            details['result_summary_omissions'] = result_summary_omissions
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
        feedback_knowledge_cutoff_at: datetime | None = None,
        feedback_cutoff_resolution: dict[str, Any] | None = None,
        point_in_time_recovery: bool = False,
        provider_admission_fence_owned: bool = False,
        run_id: int | None = None,
        progress_callback: Callable[..., Any] | None = None,
    ) -> dict[str, Any]:
        if (
            point_in_time_recovery
            and stage_id in POINT_IN_TIME_RECOVERY_CURRENT_ONLY_STAGE_IDS
            and should_skip_point_in_time_current_only_stage(
                stage_id=stage_id,
                market_date=market_date,
                market=get_market_state(),
            )
        ):
            return {
                'triggered': False,
                'skipped': True,
                'safe_skip': True,
                'status': 'skipped',
                'reason': 'point_in_time_recovery_current_only_stage',
                'market_date': market_date,
                'stage': stage_id,
            }
        if stage_id in EXPIRED_DECISION_REPLAY_STAGE_IDS:
            expired_decision_result = build_expired_decision_replay_result(
                stage_id=stage_id,
                market_date=market_date,
                market=get_market_state(),
            )
            if expired_decision_result is not None:
                return expired_decision_result
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
            enrichment_kwargs: dict[str, Any] = {
                'market_date': market_date,
                'markets': markets,
                'symbols': symbols,
                'use_full_market_coverage': True,
            }
            if point_in_time_recovery:
                enrichment_kwargs['point_in_time_recovery'] = True
            if provider_admission_fence_owned:
                enrichment_kwargs['daily_price_bar_lock_owned'] = True
            result = self.pykrx_enrichment_service.collect_for_market_date(
                **enrichment_kwargs
            )
            preflight = build_pykrx_enrichment_preflight(
                market_date=market_date,
                coverage=dict(result.get('coverage') or {}),
                categories=REQUIRED_EXEC_DECIDER_PYKRX_ENRICHMENT_CATEGORIES,
            )
            result['downstream_preflight'] = preflight
            result_status = str(
                result.get('status') or ''
            ).strip().lower()
            enrichment_counts = self._extract_stage_counts(
                stage_id='pykrx_enrichment',
                result=result,
            )
            if (
                result_status == 'partial'
                and pykrx_enrichment_preflight_is_ready(preflight)
                and enrichment_counts['total'] > 0
                and enrichment_counts['processed']
                == enrichment_counts['total']
                and enrichment_counts['succeeded']
                == enrichment_counts['total']
                and enrichment_counts['failed'] == 0
            ):
                result['status'] = 'completed'
            elif (
                not preflight.get('allowed')
                and result_status not in {'failed', 'blocked', 'waiting'}
            ):
                result['status'] = 'partial'
            return result
        if stage_id == 'technical_snapshots':
            price_context_result = daily_price_context_service.rebuild_for_market_date(
                market_date=market_date,
                symbols=symbols,
                source='stage_orchestrator',
                run_id=run_id,
            )
            technical_result = daily_technical_snapshot_service.rebuild_for_market_date(
                market_date=market_date,
                symbols=symbols,
                source='stage_orchestrator',
                run_id=run_id,
            )
            return {
                **technical_result,
                'contexts_persisted': int(
                    price_context_result.get('contexts_persisted') or 0
                ),
                'contexts_failed': int(
                    price_context_result.get('contexts_failed') or 0
                ),
                'price_contexts': {
                    key: value
                    for key, value in price_context_result.items()
                    if key != 'items'
                },
            }
        if stage_id == 'action_summaries':
            return daily_action_summary_service.rebuild_for_market_date(
                market_date=market_date,
                symbols=symbols,
                source='stage_orchestrator',
                run_id=run_id,
            )
        if stage_id == 'trending_ranks':
            return compute_and_persist_daily_trending_ranks(
                market_date=market_date,
                source='stage_orchestrator',
                run_id=run_id,
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
                market_date=market_date,
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
                required_stage_ids=self._feedback_prerequisite_stage_ids(
                    point_in_time_recovery=point_in_time_recovery,
                ),
            )
            if (
                not bypass_prerequisite_gate
                and not prerequisites['ready']
            ):
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
            if (
                stage_id == 'signal_decider'
                and isinstance(feedback_cutoff_resolution, dict)
                and not feedback_cutoff_resolution.get('ready')
            ):
                return {
                    'triggered': False,
                    'status': 'blocked',
                    'reason': 'summary_knowledge_cutoff_unavailable',
                    'market_date': market_date,
                    'stage': stage_id,
                    'knowledge_cutoff_resolution': {
                        key: value
                        for key, value in feedback_cutoff_resolution.items()
                        if key != 'knowledge_cutoff_at'
                    },
                }
            resolved_feedback_cutoff = (
                feedback_knowledge_cutoff_at
                or datetime.now(timezone.utc)
            )
            scope_authority: dict[str, Any] | None = None
            if (
                stage_id == 'signal_decider'
                and isinstance(feedback_cutoff_resolution, dict)
                and feedback_cutoff_resolution.get('ready') is True
                and str(
                    feedback_cutoff_resolution.get('source_run_id') or ''
                ).strip()
                and completed_summary_scope_matches_parent(
                    feedback_scope_mode=(
                        feedback_cutoff_resolution.get(
                            'feedback_scope_mode'
                        )
                    ),
                    completed_symbols=symbols,
                    completed_scope_proof=(
                        feedback_cutoff_resolution.get(
                            'completed_scope_proof'
                        )
                    ),
                    parent_symbols=None,
                )
            ):
                scope_authority = {
                    'schema_version': (
                        FEEDBACK_SCOPE_AUTHORITY_SCHEMA_VERSION
                    ),
                    'kind': (
                        FEEDBACK_SCOPE_AUTHORITY_COMPLETED_SUMMARY_INTERSECTION
                    ),
                    'source_summary_run_id': str(
                        feedback_cutoff_resolution['source_run_id']
                    ).strip(),
                    'source_feedback_scope_mode': str(
                        feedback_cutoff_resolution.get(
                            'feedback_scope_mode'
                        )
                        or ''
                    ).strip(),
                    'decision_feedback_scope_mode': (
                        FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                    ),
                    'knowledge_cutoff_at': (
                        resolved_feedback_cutoff.isoformat()
                        if hasattr(
                            resolved_feedback_cutoff,
                            'isoformat',
                        )
                        else str(resolved_feedback_cutoff)
                    ),
                    'fs_div': fs_div,
                    'completed_scope_proof': dict(
                        feedback_cutoff_resolution[
                            'completed_scope_proof'
                        ]
                    ),
                }
            result = self.feedback_retry_service.run_retry_cycles(
                config=FeedbackRetryCycleConfig(
                    market_date=date.fromisoformat(market_date),
                    roles=roles,
                    symbols=tuple(symbols) if symbols else None,
                    fs_div=fs_div,
                    # A production retry of a completed source day must see
                    # information collected after that session close. The
                    # resolved instant remains fixed for every child cycle.
                    knowledge_cutoff_at=resolved_feedback_cutoff,
                    feedback_scope_mode=(
                        FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE
                        if stage_id == 'agent_feedback_summaries'
                        else FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                    ),
                    max_cycles=feedback_max_cycles,
                    outer_batch_size=feedback_outer_batch_size,
                    reuse_existing=reuse_existing_feedback,
                    scope_authority=scope_authority,
                    progress_callback=progress_callback,
                ),
                # A forced pipeline recovery must bypass stale prerequisite
                # gates, but it must not silently turn an idempotent resume
                # into a full model recompute when reuse was requested.
                force=force and not reuse_existing_feedback,
                bypass_prerequisite_gate=bypass_prerequisite_gate,
            )
            expired_after_completion = build_expired_decision_replay_result(
                stage_id=stage_id,
                market_date=market_date,
                market=get_market_state(),
            )
            if expired_after_completion is not None:
                return {
                    **expired_after_completion,
                    'completed_after_action_window': True,
                }
            return result
        if stage_id == 'next_open_execution_plan':
            from app.orders.next_open_execution import signal_decider_next_open_execution_service

            expired_result = build_next_open_window_expired_result(
                market_date=market_date,
                market=get_market_state(),
            )
            if expired_result is not None:
                return {**expired_result, 'stage': stage_id}
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
            result = self.decision_loop_service.compute_recent_evaluations(
                as_of_market_date=market_date,
                lookback_days=DEFAULT_SIGNAL_DECIDER_EVALUATION_LOOKBACK_DAYS,
                symbols=symbols,
                force_recompute=force,
            )
            expired_after_completion = build_expired_decision_replay_result(
                stage_id=stage_id,
                market_date=market_date,
                market=get_market_state(),
            )
            if expired_after_completion is not None:
                return {
                    **expired_after_completion,
                    'completed_after_action_window': True,
                }
            return result
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

    @staticmethod
    def _feedback_prerequisite_stage_ids(
        *,
        point_in_time_recovery: bool,
    ) -> tuple[str, ...]:
        if point_in_time_recovery:
            return FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER
        return CURRENT_FEEDBACK_PREREQUISITE_STAGE_ORDER

    @staticmethod
    def _requires_persisted_feedback_prerequisite_preflight(
        resolved_stages: Sequence[str],
        *,
        required_stage_ids: Sequence[str],
    ) -> bool:
        resolved_stage_set = set(resolved_stages)
        if not resolved_stage_set.intersection(
            {'agent_feedback_summaries', 'signal_decider'}
        ):
            return False
        return not set(required_stage_ids).issubset(
            resolved_stage_set
        )

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
        if stage_id == 'technical_snapshots':
            price_context_result = (
                result.get('price_contexts')
                if isinstance(result.get('price_contexts'), dict)
                else {}
            )
            if price_context_result:
                payload['contexts_persisted'] = int(
                    price_context_result.get('contexts_persisted') or 0
                )
                payload['contexts_failed'] = int(
                    price_context_result.get('contexts_failed') or 0
                )
                payload['price_contexts'] = {
                    key: value
                    for key, value in price_context_result.items()
                    if key != 'items'
                }
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

    @staticmethod
    def _dependency_outcome_is_satisfied(outcome: dict[str, Any] | None) -> bool:
        return daily_pipeline_outcome_is_satisfied(outcome)

    def _resolve_stage_ids(
        self,
        requested_stages: Sequence[str],
        *,
        inject_next_open_dependency: bool = True,
    ) -> list[str]:
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
        if (
            inject_next_open_dependency
            and 'next_open_execution_plan' in requested_set
        ):
            # A direct plan refresh must be backed by a decider result from this
            # run. Resume requests intentionally retain their persisted-output
            # boundary and let the plan service validate stored candidates.
            requested_set.add('signal_decider')
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

    def _cutoff_contract_repair_execution_context(
        self,
        pipeline_run: dict[str, Any],
    ) -> dict[str, Any]:
        metadata = pipeline_run.get('run_metadata')
        if not isinstance(metadata, dict):
            raise RuntimeError(
                'cutoff contract repair run metadata is unavailable'
            )
        if (
            metadata.get('summary_cutoff_contract_repair_version')
            != SUMMARY_CUTOFF_CONTRACT_REPAIR_VERSION
        ):
            raise RuntimeError('cutoff contract repair marker is invalid')

        raw_symbols = metadata.get('symbols')
        if raw_symbols is not None and not isinstance(
            raw_symbols,
            (list, tuple),
        ):
            raise RuntimeError('cutoff contract repair symbol scope is invalid')
        resolved_symbols = self._normalize_symbols(raw_symbols)

        raw_markets = metadata.get('markets')
        if not isinstance(raw_markets, (list, tuple)):
            raise RuntimeError(
                'cutoff contract repair market scope is unavailable'
            )
        resolved_markets = list(
            dict.fromkeys(
                str(market or '').strip().upper()
                for market in raw_markets
                if str(market or '').strip()
            )
        )
        if not resolved_markets:
            raise RuntimeError('cutoff contract repair market scope is empty')

        resolved_fs_div = str(metadata.get('fs_div') or '').strip().upper()
        if resolved_fs_div not in {'CFS', 'OFS'}:
            raise RuntimeError('cutoff contract repair fs_div is invalid')

        def required_bool(key: str) -> bool:
            value = metadata.get(key)
            if not isinstance(value, bool):
                raise RuntimeError(
                    f'cutoff contract repair {key} is unavailable'
                )
            return value

        def optional_int(
            key: str,
            *,
            default: int | None,
            minimum: int,
        ) -> int | None:
            value = metadata.get(key, default)
            if value is None and default is None:
                return None
            if isinstance(value, bool):
                raise RuntimeError(
                    f'cutoff contract repair {key} is invalid'
                )
            try:
                parsed = int(value)
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f'cutoff contract repair {key} is invalid'
                ) from exc
            if parsed < minimum:
                raise RuntimeError(
                    f'cutoff contract repair {key} is invalid'
                )
            return parsed

        proof = metadata.get('summary_cutoff_contract_repair_proof')
        if not (
            isinstance(proof, dict)
            and str(proof.get('source_run_id') or '').strip()
            and str(proof.get('feedback_scope_mode') or '').strip()
            == FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
            and (
                proof.get('requested_symbols') is None
                or isinstance(proof.get('requested_symbols'), list)
            )
        ):
            raise RuntimeError(
                'cutoff contract repair summary proof is invalid'
            )

        return {
            'symbols': resolved_symbols,
            'markets': resolved_markets,
            'force': required_bool('force'),
            'bypass_prerequisite_gate': required_bool(
                'bypass_prerequisite_gate'
            ),
            'fs_div': resolved_fs_div,
            'symbol_limit': optional_int(
                'symbol_limit',
                default=None,
                minimum=1,
            ),
            'symbol_offset': optional_int(
                'symbol_offset',
                default=0,
                minimum=0,
            ),
            'capture_limit': optional_int(
                'capture_limit',
                default=500,
                minimum=1,
            ),
            'retry_cycles': optional_int(
                'retry_cycles',
                default=None,
                minimum=1,
            ),
            'feedback_max_cycles': optional_int(
                'feedback_max_cycles',
                default=DEFAULT_FEEDBACK_MAX_CYCLES,
                minimum=1,
            ),
            'feedback_outer_batch_size': optional_int(
                'feedback_outer_batch_size',
                default=DEFAULT_FEEDBACK_OUTER_BATCH_SIZE,
                minimum=1,
            ),
            'reuse_existing_feedback': required_bool(
                'reuse_existing_feedback'
            ),
            'point_in_time_recovery': required_bool(
                'point_in_time_recovery'
            ),
            'summary_cutoff_proof': dict(proof),
        }

    def _terminal_continuation_execution_context(
        self,
        pipeline_run: dict[str, Any],
    ) -> dict[str, Any]:
        """Restore the immutable execution inputs of a claimed continuation.

        The scheduler request is only a wake-up signal. Once the existing row
        is claimed, every input that can change feedback scope or decisions is
        recovered from that row instead of trusting current settings or caller
        arguments. Legacy rows predate persisted retry tuning, so those two
        values use the historical fixed defaults rather than caller input.
        """

        metadata = pipeline_run.get('run_metadata')
        if not isinstance(metadata, dict):
            raise RuntimeError(
                'terminal continuation run metadata is unavailable'
            )

        raw_symbols = metadata.get('symbols')
        if not isinstance(raw_symbols, (list, tuple)):
            raise RuntimeError(
                'terminal continuation symbol scope is unavailable'
            )
        resolved_symbols = self._normalize_symbols(raw_symbols)
        if not resolved_symbols:
            raise RuntimeError(
                'terminal continuation symbol scope is empty'
            )

        raw_markets = metadata.get('markets')
        if not isinstance(raw_markets, (list, tuple)):
            raise RuntimeError(
                'terminal continuation market scope is unavailable'
            )
        resolved_markets = list(
            dict.fromkeys(
                str(market or '').strip().upper()
                for market in raw_markets
                if str(market or '').strip()
            )
        )
        if not resolved_markets:
            raise RuntimeError(
                'terminal continuation market scope is empty'
            )

        resolved_fs_div = str(metadata.get('fs_div') or '').strip().upper()
        if not resolved_fs_div:
            raise RuntimeError(
                'terminal continuation fs_div is unavailable'
            )

        def required_bool(key: str) -> bool:
            value = metadata.get(key)
            if not isinstance(value, bool):
                raise RuntimeError(
                    f'terminal continuation {key} is unavailable'
                )
            return value

        def optional_int(
            key: str,
            *,
            default: int | None,
            minimum: int,
        ) -> int | None:
            value = metadata.get(key, default)
            if value is None and default is None:
                return None
            if isinstance(value, bool):
                raise RuntimeError(
                    f'terminal continuation {key} is invalid'
                )
            try:
                parsed = int(value)
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f'terminal continuation {key} is invalid'
                ) from exc
            if parsed < minimum:
                raise RuntimeError(
                    f'terminal continuation {key} is invalid'
                )
            return parsed

        evidence = metadata.get('terminal_continuation_evidence')
        suffix_stages = (
            evidence.get('suffix_stages')
            if isinstance(evidence, dict)
            and isinstance(evidence.get('suffix_stages'), dict)
            else {}
        )
        summary_evidence = (
            suffix_stages.get('agent_feedback_summaries')
            if isinstance(
                suffix_stages.get('agent_feedback_summaries'),
                dict,
            )
            else {}
        )
        source_run_ids = (
            summary_evidence.get('source_run_ids')
            if isinstance(summary_evidence.get('source_run_ids'), dict)
            else {}
        )
        summary_source_run_ids = {
            str(source_run_ids.get(key) or '').strip()
            for key in ('agent_feedback_run_id', 'run_id')
            if str(source_run_ids.get(key) or '').strip()
        }
        summary_details = (
            summary_evidence.get('details')
            if isinstance(summary_evidence.get('details'), Mapping)
            else {}
        )
        summary_result = (
            summary_details.get('result_summary')
            if isinstance(summary_details.get('result_summary'), Mapping)
            else {}
        )
        summary_result_run_id = str(
            summary_result.get('run_id') or ''
        ).strip()
        if summary_result_run_id:
            summary_source_run_ids.add(summary_result_run_id)
        if len(summary_source_run_ids) != 1:
            raise RuntimeError(
                'terminal continuation summary source run is unavailable'
            )

        return {
            'symbols': resolved_symbols,
            'markets': resolved_markets,
            'force': required_bool('force'),
            'bypass_prerequisite_gate': required_bool(
                'bypass_prerequisite_gate'
            ),
            'fs_div': resolved_fs_div,
            'symbol_limit': optional_int(
                'symbol_limit',
                default=None,
                minimum=1,
            ),
            'symbol_offset': optional_int(
                'symbol_offset',
                default=0,
                minimum=0,
            ),
            'capture_limit': optional_int(
                'capture_limit',
                default=500,
                minimum=1,
            ),
            'retry_cycles': optional_int(
                'retry_cycles',
                default=None,
                minimum=1,
            ),
            'feedback_max_cycles': optional_int(
                'feedback_max_cycles',
                default=DEFAULT_FEEDBACK_MAX_CYCLES,
                minimum=1,
            ),
            'feedback_outer_batch_size': optional_int(
                'feedback_outer_batch_size',
                default=DEFAULT_FEEDBACK_OUTER_BATCH_SIZE,
                minimum=1,
            ),
            'reuse_existing_feedback': required_bool(
                'reuse_existing_feedback'
            ),
            'point_in_time_recovery': required_bool(
                'point_in_time_recovery'
            ),
            'summary_source_run_id': next(
                iter(summary_source_run_ids)
            ),
        }

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
        if result.get('safe_skip') is True:
            return 'completed'
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
        has_incomplete_role = False
        incomplete_role_statuses = {
            'abandoned',
            'blocked',
            'completed_with_failures',
            'failed',
            'incomplete',
            'interrupted',
            'partial',
            'pending',
            'running',
            'waiting',
        }
        for role_state in DailyPipelineStageService._extract_role_progress(result).values():
            role_status = str(role_state.get('status') or '').strip().lower()
            boundary_status = str(
                role_state.get('boundary_status') or ''
            ).strip().lower()
            if (
                role_status in incomplete_role_statuses
                or boundary_status in incomplete_role_statuses
                or role_state.get('incomplete') is True
            ):
                has_incomplete_role = True
                break
            for key in (
                'failed_items',
                'in_progress_symbols',
                'pending_symbols',
                'retry_pending_symbols',
            ):
                try:
                    if int(role_state.get(key) or 0) > 0:
                        has_incomplete_role = True
                        break
                except (TypeError, ValueError):
                    continue
            if has_incomplete_role:
                break
            total = role_state.get('total_symbols')
            for completed_key in ('processed_symbols', 'completed_symbols'):
                completed = role_state.get(completed_key)
                if total is None or completed is None:
                    continue
                try:
                    if int(completed) < int(total):
                        has_incomplete_role = True
                        break
                except (TypeError, ValueError):
                    continue
            if has_incomplete_role:
                break
        has_reported_failures = False
        for payload in nested_payloads:
            for failed_key in failed_keys:
                if _failure_count(payload.get(failed_key)) > 0:
                    has_reported_failures = True
                    break
            if has_reported_failures:
                break
        has_no_ready_content = daily_pipeline_result_has_no_ready_content(
            result
        )
        zero_scope_gate_status = daily_pipeline_zero_scope_gate_status(result)

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
            if normalized_status == 'completed' and zero_scope_gate_status:
                return zero_scope_gate_status
            if normalized_status == 'completed' and (
                has_reported_failures
                or has_incomplete_role
                or has_no_ready_content
            ):
                return 'partial'
            return normalized_status
        if 'completed' in result and 'final_pending' in result:
            return (
                'completed'
                if bool(result.get('completed')) and not has_incomplete_role
                else 'partial'
            )
        if result.get('error') or result.get('error_message'):
            if has_reported_failures:
                return 'partial'
            if DailyPipelineStageService._has_successful_work(result):
                return 'partial'
            return 'failed'

        if has_no_ready_content:
            return 'partial'
        if zero_scope_gate_status:
            return zero_scope_gate_status

        if any(failed_key in result for failed_key in failed_keys):
            return 'partial' if has_reported_failures else 'completed'
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
