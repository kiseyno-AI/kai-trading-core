from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Sequence


from app.agent_context.derived_contexts import daily_price_context_service
from app.agent_context.retry_service import (
    FeedbackRetryCycleConfig,
    agent_feedback_retry_service,
    resolve_latest_completed_summary_knowledge_cutoff,
    resolve_terminal_role_success_intersection,
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
    daily_pipeline_warning_metadata,
    daily_pipeline_zero_scope_gate_status,
    pykrx_enrichment_preflight_is_ready,
)
from app.eod.daily_pipeline_state import daily_pipeline_run_state_service
from app.eod.summary_cutoff_contract import (
    LEGACY_CUTOFF_REPAIR_STAGE_IDS,
    SUMMARY_CUTOFF_CONTRACT_REPAIR_VERSION,
    completed_summary_scope_matches_parent,
    normalize_symbol_codes,
    summary_cutoff_proof_from_stage,
)
from app.eod.master import refresh_master_if_due
from app.eod.market_membership import market_membership_sha256
from app.eod.pykrx_backfill import (
    DEFAULT_MARKETS,
    FEEDBACK_RETRY_PARENT_CONTRACT_VERSION,
    PYKRX_BACKFILL_SERVICE,
)
from app.eod.pykrx_enrichment import pykrx_enrichment_service
from app.eod.pykrx_enrichment_gate import (
    REQUIRED_EXEC_DECIDER_PYKRX_ENRICHMENT_CATEGORIES,
    build_pykrx_enrichment_preflight,
)
from app.eod.pykrx_pipeline_runner import (
    POINT_IN_TIME_RECOVERY_CURRENT_ONLY_STAGE_IDS,
    POINT_IN_TIME_RECOVERY_BOUNDARY_TRIGGERS,
    _build_pykrx_enrichment_symbol_scope_outcome,
    resolve_point_in_time_recovery,
    should_skip_point_in_time_current_only_stage,
    _finalize_daily_stage_scope,
    _validated_exact_scope_outcome,
    _validated_exact_piotroski_membership_symbols,
)
from app.eod.run_stage_support import (
    EXPIRED_DECISION_REPLAY_STAGE_IDS,
    build_nonblocking_downstream_scope_proof,
    NONBLOCKING_FEEDBACK_RETRY_DEBT_SCHEMA_VERSION,
    NONBLOCKING_PARTIAL_PROGRESSION_SCHEMA_VERSION,
    admits_previous_session_preopen_auto_recovery,
    admits_same_day_intraday_watch_continuation,
    build_expired_decision_replay_result,
    build_next_open_window_expired_result,
    extract_next_open_plan_stage_counts,
    is_exact_auto_pitr_intraday_watch_recovery_request,
    nonblocking_partial_progression_digest,
)
from app.eod.scope_outcomes import (
    EXACT_SCOPE_OUTCOME_MANIFEST_VERSIONS,
    build_exact_symbol_scope_outcome,
)
from app.eod.stage_heartbeat import PeriodicStageHeartbeat
from app.eod.technical_snapshots import daily_technical_snapshot_service
from app.external_news.service import external_news_service
from app.macro_context.service import macro_context_service
from app.market.clock import get_market_state
from app.market_data_store import (
    DAY_BASED_TRENDING_PERIODS,
    compute_and_persist_daily_trending_ranks,
)
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
EXACT_MEMBERSHIP_BOUND_STAGE_IDS = frozenset(
    {
        'pykrx_enrichment',
        'trending_ranks',
        'market_risk_filter',
        'fundamentals_daily_refresh',
        'piotroski_filter',
        *FACTOR_STAGE_ORDER,
        'signal_features',
    }
)
EXACT_FINALIZATION_STAGE_IDS = frozenset(
    {
        'trending_ranks',
        'market_risk_filter',
        'piotroski_filter',
        *FACTOR_STAGE_ORDER,
        'signal_features',
    }
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
EXACT_SCOPE_OUTCOME_PROOF_SCHEMA_VERSION = 'exact_scope_outcome_proof_v1'
STAGE_COMPLETION_SCOPE_PROOF_SCHEMA_VERSION = (
    'stage_completion_scope_v1'
)
PYKRX_INVOCATION_SCOPE_MANIFEST_VERSION = (
    'pykrx_daily_invocation_scope_v1'
)
CURRENT_FEEDBACK_PREREQUISITE_STAGE_ORDER: tuple[str, ...] = (
    *FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER,
    'signal_features',
)
# External news collect/enrich and macro refresh are deliberately absent from
# the hard gate. daily_pipeline_outcomes classifies them as ordering-only
# optional context whose terminal degradation may lower confidence but must
# not block feedback. Historical point-in-time recovery also excludes current-
# only signal features and retains the date-bounded numeric prerequisite set.


class DailyPipelineStageCheckpointError(RuntimeError):
    """A stage progress checkpoint could not be durably persisted."""


class DailyPipelineScopeFinalizationError(RuntimeError):
    """An exact-scope persistence proof could not be durably finalized."""


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
        depends_on=('master_refresh',),
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
        depends_on=('pykrx_universe_refresh',),
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
        description='Rebuild persisted daily action summaries from technical snapshots.',
        depends_on=('technical_snapshots',),
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
        supports_symbols=True,
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
        supports_symbols=True,
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
        supports_symbols=True,
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
        supports_symbols=True,
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
        include_provider_dependency_closure: bool = True,
    ) -> dict[str, Any]:
        stages_were_explicit = stages is not None
        requested_stages = self._resolve_requested_stages_for_resume(
            requested_stages=list(stages or DEFAULT_DAILY_STAGE_ORDER),
            resume_from_stage=resume_from_stage,
        )
        provider_prefix_requested = any(
            stage_id in {'pykrx_universe_refresh', 'price_collection'}
            for stage_id in requested_stages
        )
        stages_to_resolve = list(requested_stages)
        if (
            stages_were_explicit
            and include_provider_dependency_closure
            and provider_prefix_requested
        ):
            # An explicit price/provider request owns only its acquisition
            # control prefix.  Do not recursively pull the prerequisites of
            # unrelated requested suffix stages: those stages validate their
            # persisted inputs and fail closed when evidence is absent.
            stages_to_resolve.append('master_refresh')
            if 'price_collection' in requested_stages:
                stages_to_resolve.append('pykrx_universe_refresh')
        return {
            'market_date': self._normalize_market_date(market_date),
            'requested_stages': requested_stages,
            'resolved_stages': self._resolve_stage_ids(
                stages_to_resolve,
                # Explicit suffix/recovery requests consume persisted
                # prerequisites.  They must never turn a stored-data repair
                # into a new decider or provider run merely because a stage
                # definition advertises its normal full-process predecessor.
                inject_next_open_dependency=(
                    resume_from_stage is None and not stages_were_explicit
                ),
                # Only an explicitly requested provider control stage (or the
                # default official process, which contains price_collection)
                # owns acquisition of its master -> universe prefix.  Derived
                # and terminal suffixes validate/reuse persisted inputs.
                include_dependency_closure=(
                    resume_from_stage is None
                    and include_provider_dependency_closure
                    and not stages_were_explicit
                ),
            ),
        }

    def run(
        self,
        *,
        market_date: str | date | None = None,
        stages: Sequence[str] | None = None,
        symbols: Sequence[str] | None = None,
        exact_membership_symbols: Sequence[str] | None = None,
        markets: Sequence[str] | None = None,
        force: bool = False,
        bypass_prerequisite_gate: bool = False,
        allow_expired_analysis_replay: bool = False,
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
        inherited_feedback_knowledge_cutoff_at: datetime | None = None,
        inherited_feedback_source_run_id: str | None = None,
        inherited_feedback_retry_config: Mapping[str, Any] | None = None,
        provider_admission_fence_owned: bool = False,
        recovery_parent_scope_outcome: Mapping[str, Any] | None = None,
        summary_cutoff_proof: Mapping[str, Any] | None = None,
        summary_cutoff_contract_repair: bool = False,
        same_day_intraday_watch_rearm_resume: bool = False,
    ) -> dict[str, Any]:
        normalized_market_date = self._normalize_market_date(market_date)
        point_in_time_recovery = resolve_point_in_time_recovery(
            market_date=normalized_market_date,
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
        validated_request = self.validate_run_request(
            market_date=normalized_market_date,
            stages=stages,
            resume_from_stage=resume_from_stage,
            include_provider_dependency_closure=not point_in_time_recovery,
        )
        requested_stages = validated_request['requested_stages']
        resolved_stages = validated_request['resolved_stages']
        resolved_market_date = validated_request['market_date']
        resolved_symbols = self._normalize_symbols(symbols)
        resolved_exact_membership_symbols = self._normalize_symbols(
            exact_membership_symbols
        )
        recovery_parent_partition: dict[str, Any] | None = None
        resolved_markets = self._normalize_markets(markets)
        if exact_membership_symbols is not None:
            if (
                resolved_exact_membership_symbols is None
                or not point_in_time_recovery
                or reused_eod_collection_run_id is None
            ):
                raise ValueError(
                    'exact_membership_symbols requires a validated '
                    'point-in-time recovery parent'
                )
            outside_membership = sorted(
                set(resolved_symbols or ())
                - set(resolved_exact_membership_symbols)
            )
            if outside_membership:
                raise ValueError(
                    'requested recovery symbols must be contained in exact '
                    'membership: ' + ','.join(outside_membership[:20])
                )
            if recovery_parent_scope_outcome is not None:
                parent_retry_debt = recovery_parent_scope_outcome.get(
                    'retry_debt_stock_codes'
                )
                parent_status = (
                    'partial'
                    if isinstance(parent_retry_debt, list)
                    and parent_retry_debt
                    else 'completed'
                )
                recovery_parent_partition = (
                    _validated_exact_scope_outcome(
                        result=recovery_parent_scope_outcome,
                        invocation_symbols=(
                            resolved_exact_membership_symbols
                        ),
                        status=parent_status,
                    )
                )
                if recovery_parent_partition is None:
                    raise ValueError(
                        'recovery parent scope outcome is not an exact '
                        'membership-bound partition'
                    )
        resolved_fs_div = (fs_div or settings.dart_fundamentals_default_fs_div).strip().upper()
        feedback_knowledge_cutoff_at: datetime | None = None
        feedback_cutoff_resolution: dict[str, Any] | None = None
        terminal_summary_source_run_id: str | None = None
        normalized_inherited_feedback_retry_config: dict[str, Any] | None = (
            None
        )
        feedback_retry_parent_contract: dict[str, Any] | None = None
        normalized_summary_cutoff_proof = (
            dict(summary_cutoff_proof)
            if isinstance(summary_cutoff_proof, Mapping)
            else None
        )
        summary_cutoff_expected_at_provided = bool(
            isinstance(normalized_summary_cutoff_proof, dict)
            and 'knowledge_cutoff_at' in normalized_summary_cutoff_proof
        )
        expected_summary_cutoff_at: datetime | None = None
        if summary_cutoff_expected_at_provided:
            raw_expected_cutoff_at = normalized_summary_cutoff_proof.get(
                'knowledge_cutoff_at'
            )
            try:
                parsed_expected_cutoff_at = (
                    raw_expected_cutoff_at
                    if isinstance(raw_expected_cutoff_at, datetime)
                    else datetime.fromisoformat(
                        str(raw_expected_cutoff_at or '')
                    )
                )
            except (TypeError, ValueError):
                parsed_expected_cutoff_at = None
            if (
                parsed_expected_cutoff_at is not None
                and parsed_expected_cutoff_at.tzinfo is not None
                and parsed_expected_cutoff_at.utcoffset() is not None
            ):
                expected_summary_cutoff_at = (
                    parsed_expected_cutoff_at.astimezone(timezone.utc)
                )
        expected_completed_symbols_provided = bool(
            isinstance(normalized_summary_cutoff_proof, dict)
            and 'expected_completed_symbols'
            in normalized_summary_cutoff_proof
        )
        expected_completed_symbols: list[str] | None = None
        if expected_completed_symbols_provided:
            raw_expected_completed_symbols = (
                normalized_summary_cutoff_proof.get(
                    'expected_completed_symbols'
                )
            )
            if isinstance(raw_expected_completed_symbols, list):
                normalized_expected_completed_symbols = (
                    normalize_symbol_codes(
                        raw_expected_completed_symbols
                    )
                )
                if (
                    normalized_expected_completed_symbols
                    and normalized_expected_completed_symbols
                    == raw_expected_completed_symbols
                ):
                    expected_completed_symbols = (
                        normalized_expected_completed_symbols
                    )
        proof_fs_div = str(
            (normalized_summary_cutoff_proof or {}).get('fs_div') or ''
        ).strip().upper()
        proof_fs_div_provided = bool(
            isinstance(normalized_summary_cutoff_proof, dict)
            and 'fs_div' in normalized_summary_cutoff_proof
        )
        canonical_intraday_summary_cutoff_proof = bool(
            isinstance(normalized_summary_cutoff_proof, dict)
            and str(
                normalized_summary_cutoff_proof.get('source_run_id') or ''
            ).strip()
            and summary_cutoff_expected_at_provided
            and expected_summary_cutoff_at is not None
            and expected_completed_symbols_provided
            and expected_completed_symbols is not None
            and proof_fs_div_provided
            and proof_fs_div == resolved_fs_div
        )
        intraday_watch_candidate = bool(
            allow_expired_analysis_replay is False
            and existing_pipeline_run_id is None
            and summary_cutoff_contract_repair is False
            and canonical_intraday_summary_cutoff_proof
            and is_exact_auto_pitr_intraday_watch_recovery_request(
                trigger=trigger,
                point_in_time_recovery=point_in_time_recovery,
                reused_eod_collection_run_id=reused_eod_collection_run_id,
                force=force,
                reuse_existing_feedback=reuse_existing_feedback,
                resume_from_stage=resume_from_stage,
                resolved_stages=resolved_stages,
            )
        )
        intraday_watch_rearm_resume_candidate = bool(
            same_day_intraday_watch_rearm_resume is True
            and allow_expired_analysis_replay is False
            and existing_pipeline_run_id is not None
            and summary_cutoff_contract_repair is False
            and canonical_intraday_summary_cutoff_proof
            and is_exact_auto_pitr_intraday_watch_recovery_request(
                trigger=trigger,
                point_in_time_recovery=point_in_time_recovery,
                reused_eod_collection_run_id=reused_eod_collection_run_id,
                force=force,
                reuse_existing_feedback=reuse_existing_feedback,
                resume_from_stage=resume_from_stage,
                resolved_stages=resolved_stages,
            )
        )
        if same_day_intraday_watch_rearm_resume and not (
            intraday_watch_rearm_resume_candidate
        ):
            raise ValueError(
                'same-day intraday-watch rearm resume requires the exact '
                'admitted PITR suffix and existing pipeline run'
            )
        intraday_watch_market = (
            get_market_state()
            if intraday_watch_candidate
            or intraday_watch_rearm_resume_candidate
            else None
        )
        allow_same_day_intraday_watch_continuation = bool(
            (
                intraday_watch_candidate
                or intraday_watch_rearm_resume_candidate
            )
            and admits_same_day_intraday_watch_continuation(
                market_date=resolved_market_date,
                market=intraday_watch_market,
                allow_existing_recovery_resume=(
                    intraday_watch_rearm_resume_candidate
                ),
            )
        )
        previous_session_preopen_candidate = bool(
            trigger == 'auto_eod_followup_resume'
            and point_in_time_recovery is True
            and force is False
            and reuse_existing_feedback is True
            and provider_admission_fence_owned is True
            and existing_pipeline_run_id is None
            and tuple(resolved_stages)
            in {
                (
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ),
                (
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ),
            }
        )
        previous_session_preopen_market = (
            get_market_state()
            if previous_session_preopen_candidate
            else None
        )
        allow_previous_session_preopen_recovery = bool(
            previous_session_preopen_candidate
            and admits_previous_session_preopen_auto_recovery(
                market_date=resolved_market_date,
                market=previous_session_preopen_market,
                trigger=trigger,
                point_in_time_recovery=point_in_time_recovery,
                reused_eod_collection_run_id=(
                    reused_eod_collection_run_id
                ),
                force=force,
                reuse_existing_feedback=reuse_existing_feedback,
                resume_from_stage=resume_from_stage,
                resolved_stages=resolved_stages,
                provider_admission_fence_owned=(
                    provider_admission_fence_owned
                ),
                existing_pipeline_run_id=existing_pipeline_run_id,
            )
        )
        if (
            intraday_watch_rearm_resume_candidate
            and not allow_same_day_intraday_watch_continuation
        ):
            raise ValueError(
                'same-day intraday-watch rearm resume is outside its '
                'target PREOPEN/OPEN session'
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
        exact_feedback_retry_suffix = bool(
            trigger == 'auto_eod_followup_resume'
            and tuple(resolved_stages)
            == TERMINAL_FEEDBACK_CONTINUATION_STAGE_ORDER
            and resume_from_stage
            == TERMINAL_FEEDBACK_CONTINUATION_STAGE_ORDER[0]
            and reused_eod_collection_run_id is not None
            and reuse_existing_feedback
            and not force
            and existing_pipeline_run_id is None
        )
        if inherited_feedback_retry_config is not None:
            if not (
                exact_feedback_retry_suffix
                and isinstance(inherited_feedback_retry_config, Mapping)
            ):
                raise ValueError(
                    'inherited feedback retry config requires the exact '
                    'automatic feedback suffix'
                )
            normalized_inherited_feedback_retry_config = dict(
                inherited_feedback_retry_config
            )
            inherited_feedback_source_run_id = str(
                normalized_inherited_feedback_retry_config.get(
                    'source_run_id'
                )
                or ''
            ).strip()
            raw_config_cutoff = (
                normalized_inherited_feedback_retry_config.get(
                    'knowledge_cutoff_at'
                )
            )
            try:
                inherited_feedback_knowledge_cutoff_at = (
                    raw_config_cutoff
                    if isinstance(raw_config_cutoff, datetime)
                    else datetime.fromisoformat(
                        str(raw_config_cutoff or '')
                        .strip()
                        .replace('Z', '+00:00')
                    )
                )
            except ValueError as exc:
                raise ValueError(
                    'inherited feedback retry config has an invalid cutoff'
                ) from exc
            if not (
                inherited_feedback_source_run_id
                and inherited_feedback_knowledge_cutoff_at.tzinfo is not None
                and inherited_feedback_knowledge_cutoff_at.utcoffset()
                is not None
            ):
                raise ValueError(
                    'inherited feedback retry config requires an exact '
                    'parent and aware cutoff'
                )
            feedback_knowledge_cutoff_at = (
                inherited_feedback_knowledge_cutoff_at.astimezone(
                    timezone.utc
                )
            )
            feedback_scope_mode = str(
                normalized_inherited_feedback_retry_config.get(
                    'feedback_scope_mode'
                )
                or ''
            ).strip()
            if feedback_scope_mode not in {
                FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
                FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
            }:
                raise ValueError(
                    'inherited feedback retry config requires an exact '
                    'scope mode'
                )
            feedback_retry_parent_contract = {
                'schema_version': (
                    FEEDBACK_RETRY_PARENT_CONTRACT_VERSION
                ),
                'source_run_id': inherited_feedback_source_run_id,
                'knowledge_cutoff_at': (
                    feedback_knowledge_cutoff_at.isoformat()
                ),
                'feedback_scope_mode': feedback_scope_mode,
            }
        elif (
            inherited_feedback_knowledge_cutoff_at is not None
            or inherited_feedback_source_run_id is not None
        ) and not inherit_feedback_prerequisite_admission:
            raise ValueError(
                'inherited feedback knowledge cutoff requires prerequisite '
                'admission inheritance'
            )
        if inherit_feedback_prerequisite_admission and not (
            exact_feedback_retry_suffix
            and point_in_time_recovery
            and normalized_inherited_feedback_retry_config is not None
        ):
            raise ValueError(
                'feedback prerequisite admission inheritance requires the '
                'exact point-in-time automatic feedback suffix retry'
            )
        if inherit_feedback_prerequisite_admission:
            inherited_feedback_source_run_id = str(
                inherited_feedback_source_run_id or ''
            ).strip()
            if not (
                isinstance(inherited_feedback_knowledge_cutoff_at, datetime)
                and inherited_feedback_knowledge_cutoff_at.tzinfo is not None
                and inherited_feedback_knowledge_cutoff_at.utcoffset()
                is not None
            ):
                raise ValueError(
                    'feedback prerequisite admission inheritance requires '
                    'an aware parent knowledge cutoff'
                )
            if not inherited_feedback_source_run_id:
                raise ValueError(
                    'feedback prerequisite admission inheritance requires '
                    'an exact parent feedback run id'
                )
            feedback_knowledge_cutoff_at = (
                inherited_feedback_knowledge_cutoff_at.astimezone(
                    timezone.utc
                )
            )
        if (
            existing_pipeline_run_id is None
            and not inherit_feedback_prerequisite_admission
            and not (
                bypass_prerequisite_gate
                and allow_expired_analysis_replay
            )
            and self._requires_persisted_feedback_prerequisite_preflight(
                resolved_stages,
                required_stage_ids=feedback_prerequisite_stage_ids,
            )
        ):
            prerequisites = (
                agent_feedback_prerequisite_service.inspect_feedback_prerequisites(
                    market_date=resolved_market_date,
                    fs_div=resolved_fs_div,
                    symbols=resolved_symbols,
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
        stage_invocation_symbols: dict[str, list[str] | None] = {}
        handled: set[str] = set()
        if (
            existing_pipeline_run_id is not None
            and same_day_intraday_watch_rearm_resume
        ):
            target_trade_date = str(
                getattr(intraday_watch_market, 'server_time', '') or ''
            )[:10]
            rearm_claim = (
                self.run_state_service
                .claim_same_day_intraday_watch_rearm_resume(
                    pipeline_run_id=int(existing_pipeline_run_id),
                    market_date=resolved_market_date,
                    reused_eod_collection_run_id=int(
                        reused_eod_collection_run_id
                    ),
                    target_trade_date=target_trade_date,
                    expected_summary_cutoff_proof=(
                        normalized_summary_cutoff_proof
                    ),
                )
            )
            if not rearm_claim.get('claimed'):
                return {
                    'triggered': False,
                    'reason': (
                        rearm_claim.get('reason')
                        or 'same_day_intraday_watch_rearm_not_claimed'
                    ),
                    'pipeline_run_id': int(existing_pipeline_run_id),
                    'pipeline_run': rearm_claim.get('pipeline_run'),
                    'market_date': resolved_market_date,
                    'requested_stages': requested_stages,
                    'resolved_stages': resolved_stages,
                    'status': 'blocked',
                    'stages': {},
                }
            pipeline_run = rearm_claim.get('pipeline_run')
            if not isinstance(pipeline_run, dict):
                raise RuntimeError(
                    'same-day intraday-watch rearm claim did not return '
                    'pipeline state'
                )
            persisted_metadata = pipeline_run.get('run_metadata')
            if not isinstance(persisted_metadata, dict):
                raise RuntimeError(
                    'same-day intraday-watch rearm claim returned invalid '
                    'pipeline metadata'
                )
            resolved_symbols = self._normalize_symbols(
                persisted_metadata.get('symbols')
            )
            persisted_markets = persisted_metadata.get('markets')
            if not isinstance(persisted_markets, list) or not (
                persisted_markets
            ):
                raise RuntimeError(
                    'same-day intraday-watch rearm markets are unavailable'
                )
            resolved_markets = [
                str(market).strip().upper()
                for market in persisted_markets
            ]
            resolved_fs_div = str(
                persisted_metadata.get('fs_div') or ''
            ).strip().upper()
            force = False
            reuse_existing_feedback = True
            point_in_time_recovery = True
            allow_same_day_intraday_watch_continuation = True
            normalized_summary_cutoff_proof = dict(
                rearm_claim['summary_cutoff_proof']
            )
            raw_persisted_stage_outcomes = rearm_claim.get(
                'persisted_stage_outcomes',
                {},
            )
            if not isinstance(raw_persisted_stage_outcomes, dict):
                raise RuntimeError(
                    'same-day intraday-watch rearm claim returned invalid '
                    'persisted stage outcomes'
                )
            persisted_stage_ids = list(raw_persisted_stage_outcomes)
            if persisted_stage_ids != resolved_stages[
                : len(persisted_stage_ids)
            ]:
                raise RuntimeError(
                    'same-day intraday-watch rearm claim returned a '
                    'noncontiguous persisted stage prefix'
                )
            for stage_id in persisted_stage_ids:
                persisted_outcome = raw_persisted_stage_outcomes[stage_id]
                if not (
                    isinstance(persisted_outcome, dict)
                    and daily_pipeline_outcome_is_satisfied(
                        persisted_outcome
                    )
                ):
                    raise RuntimeError(
                        'same-day intraday-watch rearm claim returned an '
                        'unsatisfied persisted stage outcome'
                    )
                (
                    restored_outcome,
                    restored_invocation,
                ) = self._restore_persisted_stage_outcome(
                    stage_id=stage_id,
                    persisted_outcome=persisted_outcome,
                )
                stage_outcomes[stage_id] = restored_outcome
                if restored_invocation is not None:
                    stage_invocation_symbols[stage_id] = (
                        restored_invocation
                    )
                handled.add(stage_id)
        elif (
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
                allow_same_day_intraday_watch_continuation = (
                    repair_context[
                        'same_day_intraday_watch_continuation_admitted'
                    ]
                )
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
            allow_same_day_intraday_watch_continuation = (
                continuation_context[
                    'same_day_intraday_watch_continuation_admitted'
                ]
            )
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
                try:
                    (
                        restored_outcome,
                        restored_invocation,
                    ) = self._restore_persisted_stage_outcome(
                        stage_id=stage_id,
                        persisted_outcome=persisted_outcome,
                    )
                except RuntimeError as exc:
                    self._finish_run_preserving_primary_error(
                        pipeline_run_id=int(existing_pipeline_run_id),
                        status='failed',
                        error_message=str(exc),
                    )
                    raise
                stage_outcomes[stage_id] = restored_outcome
                if restored_invocation is not None:
                    stage_invocation_symbols[stage_id] = (
                        restored_invocation
                    )
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
                    'exact_membership_symbol_count': len(
                        resolved_exact_membership_symbols or ()
                    ),
                    'exact_membership_sha256': (
                        market_membership_sha256(
                            resolved_exact_membership_symbols
                        )
                        if resolved_exact_membership_symbols is not None
                        else None
                    ),
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
                    'allow_expired_analysis_replay': (
                        allow_expired_analysis_replay
                    ),
                    'inherited_feedback_prerequisite_admission': (
                        inherit_feedback_prerequisite_admission
                    ),
                    'inherited_feedback_knowledge_cutoff_at': (
                        feedback_knowledge_cutoff_at.isoformat()
                        if normalized_inherited_feedback_retry_config
                        and feedback_knowledge_cutoff_at is not None
                        else None
                    ),
                    'inherited_feedback_source_run_id': (
                        inherited_feedback_source_run_id
                        if normalized_inherited_feedback_retry_config
                        else None
                    ),
                    **(
                        {
                            'feedback_retry_parent_contract': (
                                feedback_retry_parent_contract
                            )
                        }
                        if feedback_retry_parent_contract is not None
                        else {}
                    ),
                    'reused_eod_collection_run_id': (
                        int(reused_eod_collection_run_id)
                        if reused_eod_collection_run_id is not None
                        else None
                    ),
                    'point_in_time_recovery': point_in_time_recovery,
                    'same_day_intraday_watch_continuation_admitted': (
                        allow_same_day_intraday_watch_continuation
                    ),
                    'previous_session_preopen_recovery_admitted': (
                        allow_previous_session_preopen_recovery
                    ),
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

        pykrx_requested = [
            stage_id
            for stage_id in resolved_stages
            if stage_id in PYKRX_PIPELINE_STAGE_ORDER
        ]
        grouped_pykrx_requested = 'price_collection' in pykrx_requested
        official_membership_symbols: list[str] | None = (
            list(resolved_exact_membership_symbols)
            if resolved_exact_membership_symbols is not None
            else (
                list(resolved_symbols)
                if resolved_symbols is not None
                and not grouped_pykrx_requested
                else None
            )
        )

        # The grouped PyKRX runner owns the authoritative universe acquisition
        # used by price collection.  Run the advertised control prefix first so
        # a failed master refresh cannot be followed by price or derived writes.
        if (
            grouped_pykrx_requested
            and 'master_refresh' in resolved_stages
            and 'master_refresh' not in handled
        ):
            self._start_tracked_stage(
                pipeline_run_id=pipeline_run_id,
                stage_id='master_refresh',
                market_date=resolved_market_date,
                symbols=None,
            )
            try:
                master_result = self._execute_stage(
                    'master_refresh',
                    market_date=resolved_market_date,
                    symbols=None,
                    exact_scope_symbols=official_membership_symbols,
                    markets=resolved_markets,
                    force=force,
                    bypass_prerequisite_gate=bypass_prerequisite_gate,
                    allow_expired_analysis_replay=(
                        allow_expired_analysis_replay
                    ),
                    fs_div=resolved_fs_div,
                    symbol_limit=symbol_limit,
                    symbol_offset=symbol_offset,
                    capture_limit=capture_limit,
                    retry_cycles=retry_cycles,
                    feedback_max_cycles=feedback_max_cycles,
                    feedback_outer_batch_size=feedback_outer_batch_size,
                    reuse_existing_feedback=reuse_existing_feedback,
                    point_in_time_recovery=point_in_time_recovery,
                    allow_same_day_intraday_watch_continuation=(
                        allow_same_day_intraday_watch_continuation
                    ),
                    allow_previous_session_preopen_recovery=(
                        allow_previous_session_preopen_recovery
                    ),
                    provider_admission_fence_owned=(
                        provider_admission_fence_owned
                    ),
                    run_id=reused_eod_collection_run_id,
                )
            except Exception as exc:
                failed_result = {
                    'status': 'failed',
                    'reason': 'master_refresh_failed',
                    'error_message': str(exc),
                    'market_date': resolved_market_date,
                }
                failed_outcome = self._build_stage_outcome(
                    'master_refresh',
                    failed_result,
                )
                self._finish_tracked_stage(
                    pipeline_run_id=pipeline_run_id,
                    stage_id='master_refresh',
                    market_date=resolved_market_date,
                    outcome=failed_outcome,
                    result=failed_result,
                )
                self._finish_run_preserving_primary_error(
                    pipeline_run_id=pipeline_run_id,
                    status='failed',
                    error_message=str(exc),
                )
                raise
            stage_outcomes['master_refresh'] = self._build_stage_outcome(
                'master_refresh',
                master_result,
            )
            self._finish_tracked_stage(
                pipeline_run_id=pipeline_run_id,
                stage_id='master_refresh',
                market_date=resolved_market_date,
                outcome=stage_outcomes['master_refresh'],
                result=master_result,
            )
            handled.add('master_refresh')

        grouped_control_prefix_ready = bool(
            grouped_pykrx_requested
            and (
                'master_refresh' not in resolved_stages
                or self._dependency_outcome_is_satisfied(
                    stage_outcomes.get('master_refresh')
                )
            )
        )
        if grouped_pykrx_requested and not grouped_control_prefix_ready:
            blocked_stage_ids = [
                stage_id
                for stage_id in (
                    'pykrx_universe_refresh',
                    *pykrx_requested,
                )
                if stage_id in resolved_stages and stage_id not in handled
            ]
            for blocked_stage_id in blocked_stage_ids:
                blocked_result = {
                    'triggered': False,
                    'status': 'blocked',
                    'reason': 'control_prefix_incomplete',
                    'market_date': resolved_market_date,
                    'incomplete_dependencies': ['master_refresh'],
                }
                self._start_tracked_stage(
                    pipeline_run_id=pipeline_run_id,
                    stage_id=blocked_stage_id,
                    market_date=resolved_market_date,
                    symbols=resolved_symbols,
                )
                stage_outcomes[blocked_stage_id] = (
                    self._build_stage_outcome(
                        blocked_stage_id,
                        blocked_result,
                    )
                )
                self._finish_tracked_stage(
                    pipeline_run_id=pipeline_run_id,
                    stage_id=blocked_stage_id,
                    market_date=resolved_market_date,
                    outcome=stage_outcomes[blocked_stage_id],
                    result=blocked_result,
                    invocation_symbols=resolved_symbols,
                )
                handled.add(blocked_stage_id)

        if grouped_control_prefix_ready:
            universe_stage_started = bool(
                'pykrx_universe_refresh' in resolved_stages
                and 'pykrx_universe_refresh' not in handled
            )
            if universe_stage_started:
                self._start_tracked_stage(
                    pipeline_run_id=pipeline_run_id,
                    stage_id='pykrx_universe_refresh',
                    market_date=resolved_market_date,
                    symbols=None,
                )
            try:
                pykrx_kwargs = {
                    'market_date': resolved_market_date,
                    'stages': pykrx_requested,
                    'markets': resolved_markets,
                    'trigger': trigger,
                    # An explicitly requested universe stage is fulfilled by
                    # this one grouped acquisition.  The frozen snapshot it
                    # returns is reused as downstream membership authority.
                    'refresh_universe': (
                        True
                        if universe_stage_started
                        else (
                            settings.eod_auto_pykrx_refresh_universe
                            if refresh_universe is None
                            else refresh_universe
                        )
                    ),
                    'force': force,
                    'symbols': resolved_symbols,
                    'owning_pipeline_run_id': pipeline_run_id,
                }
                if point_in_time_recovery:
                    pykrx_kwargs['point_in_time_recovery'] = True
                pykrx_result = self.pykrx_service.run_daily_pipeline_stages(
                    **pykrx_kwargs,
                )
            except Exception as exc:
                if universe_stage_started:
                    universe_failure = {
                        'status': 'failed',
                        'reason': 'authoritative_universe_acquisition_failed',
                        'error_message': str(exc),
                        'market_date': resolved_market_date,
                    }
                    universe_failure_outcome = self._build_stage_outcome(
                        'pykrx_universe_refresh',
                        universe_failure,
                    )
                    self._finish_tracked_stage(
                        pipeline_run_id=pipeline_run_id,
                        stage_id='pykrx_universe_refresh',
                        market_date=resolved_market_date,
                        outcome=universe_failure_outcome,
                        result=universe_failure,
                    )
                self._finish_run_preserving_primary_error(
                    pipeline_run_id=pipeline_run_id,
                    status='failed',
                    error_message=str(exc),
                )
                raise
            pykrx_invocation_symbols = (
                list(resolved_symbols)
                if resolved_symbols is not None
                else self._validated_pykrx_invocation_scope(pykrx_result)
            )
            validated_snapshot_membership = (
                _validated_exact_piotroski_membership_symbols(
                    universe_snapshot=pykrx_result.get('universe_snapshot'),
                    market_date=resolved_market_date,
                    markets=resolved_markets,
                    # A frozen PITR parent remains authoritative during a
                    # limited retry.  Still validate any complete snapshot
                    # returned by that retry so conflicting authority cannot
                    # silently replace the frozen scope.
                    limited_run=(
                        resolved_exact_membership_symbols is None
                        and (
                            resolved_symbols is not None
                            or symbol_limit is not None
                        )
                    ),
                )
            )
            if resolved_exact_membership_symbols is not None:
                if (
                    validated_snapshot_membership is not None
                    and set(validated_snapshot_membership)
                    != set(resolved_exact_membership_symbols)
                ):
                    error_message = (
                        'validated PyKRX membership conflicts with the '
                        'frozen point-in-time recovery membership'
                    )
                    self._finish_run_preserving_primary_error(
                        pipeline_run_id=pipeline_run_id,
                        status='failed',
                        error_message=error_message,
                    )
                    raise RuntimeError(error_message)
                official_membership_symbols = list(
                    resolved_exact_membership_symbols
                )
            else:
                official_membership_symbols = (
                    validated_snapshot_membership
                )
            if universe_stage_started:
                universe_snapshot = pykrx_result.get('universe_snapshot')
                grouped_run_skipped = bool(
                    pykrx_result.get('skipped')
                    or pykrx_result.get('triggered') is False
                )
                universe_authority_available = bool(
                    official_membership_symbols
                    and (
                        validated_snapshot_membership is not None
                        or (
                            point_in_time_recovery
                            and resolved_exact_membership_symbols is not None
                        )
                    )
                )
                universe_result: dict[str, Any] = {
                    'status': (
                        'skipped'
                        if grouped_run_skipped
                        else 'completed'
                        if universe_authority_available
                        else 'blocked'
                    ),
                    'reason': (
                        str(
                            pykrx_result.get('reason')
                            or 'grouped_pykrx_run_skipped'
                        )
                        if grouped_run_skipped
                        else 'frozen_point_in_time_membership_reused'
                        if universe_authority_available
                        and validated_snapshot_membership is None
                        else 'authoritative_universe_snapshot_acquired'
                        if universe_authority_available
                        else 'authoritative_universe_membership_unavailable'
                    ),
                    'market_date': resolved_market_date,
                    'triggered': (
                        False if grouped_run_skipped else True
                    ),
                    'skipped': grouped_run_skipped,
                    'source': 'pykrx',
                    'markets': list(resolved_markets),
                    'symbol_count': len(official_membership_symbols or ()),
                    'persisted_count': (
                        int(universe_snapshot.get('persisted_count') or 0)
                        if isinstance(universe_snapshot, Mapping)
                        else 0
                    ),
                    'membership_sha256': (
                        market_membership_sha256(
                            official_membership_symbols
                        )
                        if official_membership_symbols
                        else None
                    ),
                    'frozen_for_pipeline': universe_authority_available,
                    'universe_snapshot': (
                        dict(universe_snapshot)
                        if isinstance(universe_snapshot, Mapping)
                        else None
                    ),
                }
                stage_outcomes['pykrx_universe_refresh'] = (
                    self._build_stage_outcome(
                        'pykrx_universe_refresh',
                        universe_result,
                    )
                )
                self._finish_tracked_stage(
                    pipeline_run_id=pipeline_run_id,
                    stage_id='pykrx_universe_refresh',
                    market_date=resolved_market_date,
                    outcome=stage_outcomes['pykrx_universe_refresh'],
                    result=universe_result,
                    invocation_symbols=official_membership_symbols,
                )
                handled.add('pykrx_universe_refresh')
            pykrx_admitted_symbols: dict[
                str, list[str] | None
            ] = {}
            for stage_id in pykrx_requested:
                stage_result = self._extract_pykrx_stage_result(stage_id, pykrx_result)
                pykrx_dependencies = [
                    dependency
                    for dependency in STAGE_DEFINITIONS[stage_id].depends_on
                    if dependency in pykrx_requested
                ]
                if stage_id == 'price_collection' or not pykrx_dependencies:
                    bound_invocation_symbols = (
                        list(pykrx_invocation_symbols)
                        if pykrx_invocation_symbols is not None
                        else None
                    )
                elif any(
                    pykrx_admitted_symbols.get(dependency) is None
                    for dependency in pykrx_dependencies
                ):
                    bound_invocation_symbols = None
                else:
                    dependency_admitted_sets = [
                        set(pykrx_admitted_symbols[dependency] or [])
                        for dependency in pykrx_dependencies
                    ]
                    bound_invocation_symbols = [
                        symbol
                        for symbol in pykrx_invocation_symbols or []
                        if all(
                            symbol in admitted_set
                            for admitted_set in dependency_admitted_sets
                        )
                    ]
                stage_invocation_symbols[stage_id] = (
                    bound_invocation_symbols
                )
                stage_outcomes[stage_id] = self._build_stage_outcome(stage_id, stage_result)
                exact_stage_scope = self._exact_partial_success_scope(
                    outcome=stage_outcomes[stage_id],
                    invocation_symbols=bound_invocation_symbols,
                )
                has_scope_outcome_claim = any(
                    key in stage_result
                    for key in (
                        'scope_outcome_manifest_version',
                        'selected_stock_codes',
                        'successful_stock_codes',
                        'retry_debt_stock_codes',
                        'scope_outcome_manifest',
                    )
                )
                if exact_stage_scope is not None:
                    pykrx_admitted_symbols[stage_id] = list(
                        exact_stage_scope['successful_stock_codes']
                    )
                elif (
                    bound_invocation_symbols is not None
                    and not has_scope_outcome_claim
                    and self._dependency_outcome_is_satisfied(
                        stage_outcomes[stage_id]
                    )
                ):
                    pykrx_admitted_symbols[stage_id] = list(
                        bound_invocation_symbols
                    )
                else:
                    pykrx_admitted_symbols[stage_id] = None
                self._finish_tracked_stage(
                    pipeline_run_id=pipeline_run_id,
                    stage_id=stage_id,
                    market_date=resolved_market_date,
                    outcome=stage_outcomes[stage_id],
                    result=stage_result,
                    invocation_symbols=stage_invocation_symbols.get(
                        stage_id
                    ),
                )
                handled.add(stage_id)

        for stage_id in resolved_stages:
            if stage_id in handled:
                continue
            stage_symbols = resolved_symbols
            if (
                stage_id in EXACT_MEMBERSHIP_BOUND_STAGE_IDS
                and official_membership_symbols is not None
                and (
                    stage_symbols is None
                    or (
                        point_in_time_recovery
                        and resolved_exact_membership_symbols is not None
                        and stage_id in EXACT_FINALIZATION_STAGE_IDS
                    )
                )
            ):
                stage_symbols = list(official_membership_symbols)
            terminal_decider_progression_proof = (
                self._terminal_decider_progression_proof(
                    stage_outcomes.get('signal_decider'),
                    expected_decider_symbols=(
                        feedback_cutoff_resolution.get(
                            'requested_symbols'
                        )
                        if isinstance(
                            feedback_cutoff_resolution,
                            dict,
                        )
                        else None
                    ),
                    feedback_cutoff_resolution=(
                        feedback_cutoff_resolution
                    ),
                )
                if stage_id in {
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                }
                else None
            )
            terminal_decider_symbols = (
                list(
                    terminal_decider_progression_proof[
                        'completed_symbols'
                    ]
                )
                if isinstance(
                    terminal_decider_progression_proof,
                    Mapping,
                )
                else None
            )
            if (
                stage_id in {
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                }
                and terminal_decider_symbols
            ):
                stage_symbols = list(terminal_decider_symbols)
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
            exact_partial_dependency_proofs: list[dict[str, Any]] = []
            exact_partial_dependency_candidates: list[
                dict[str, Any]
            ] = []
            incomplete_dependencies = [
                dependency
                for dependency in STAGE_DEFINITIONS[stage_id].depends_on
                if dependency in resolved_stages
                and not self._dependency_outcome_is_satisfied(
                    stage_outcomes.get(dependency)
                )
            ]
            if (
                stage_id == 'signal_decider'
                and 'agent_feedback_summaries' in incomplete_dependencies
                and self._partial_summary_dependency_is_proven(
                    outcome=stage_outcomes.get(
                        'agent_feedback_summaries'
                    ),
                    cutoff_resolution=feedback_cutoff_resolution,
                    parent_symbols=resolved_symbols,
                )
            ):
                incomplete_dependencies.remove(
                    'agent_feedback_summaries'
                )
            if (
                stage_id == 'signal_decider_evaluations'
                and 'signal_decider' in incomplete_dependencies
                and terminal_decider_symbols
            ):
                incomplete_dependencies.remove('signal_decider')
            if (
                stage_id == 'signal_decider_evaluations'
                and stage_outcomes.get('signal_decider') is not None
                and not terminal_decider_symbols
                and str(
                    stage_outcomes['signal_decider'].get('status') or ''
                ).strip().lower()
                == 'completed'
                and 'signal_decider' not in incomplete_dependencies
            ):
                incomplete_dependencies.append('signal_decider')
            if (
                stage_id in {
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                }
                and terminal_decider_symbols is None
                and stage_outcomes.get('signal_decider') is None
                and (
                    resume_from_stage is None
                    or same_day_intraday_watch_rearm_resume
                )
                and 'signal_decider' not in incomplete_dependencies
            ):
                # An ordinary direct suffix resume may validate a persisted
                # boundary in the downstream service.  Same-day rearm must
                # carry same-run terminal proof; never speculate from a
                # proofless persisted completion during an open session.
                incomplete_dependencies.append('signal_decider')
            if STAGE_DEFINITIONS[stage_id].supports_symbols:
                for dependency in STAGE_DEFINITIONS[stage_id].depends_on:
                    if dependency not in resolved_stages:
                        continue
                    exact_scope = self._exact_partial_success_scope(
                        outcome=stage_outcomes.get(dependency),
                        invocation_symbols=stage_invocation_symbols.get(
                            dependency
                        ),
                    )
                    if exact_scope is None:
                        continue
                    admitted_symbols = exact_scope[
                        'successful_stock_codes'
                    ]
                    if not admitted_symbols:
                        if dependency not in incomplete_dependencies:
                            incomplete_dependencies.append(dependency)
                        continue
                    if stage_symbols is None:
                        stage_symbols = list(admitted_symbols)
                    else:
                        admitted_set = set(admitted_symbols)
                        stage_symbols = [
                            symbol
                            for symbol in stage_symbols
                            if symbol in admitted_set
                        ]
                    exact_partial_dependency_candidates.append(
                        {
                            'dependency': dependency,
                            **exact_scope,
                        }
                    )
            if exact_partial_dependency_candidates:
                if stage_symbols:
                    for proof in exact_partial_dependency_candidates:
                        dependency = str(proof['dependency'])
                        if dependency in incomplete_dependencies:
                            incomplete_dependencies.remove(dependency)
                        exact_partial_dependency_proofs.append(
                            {
                                **proof,
                                'admitted_stock_codes': list(
                                    stage_symbols
                                ),
                            }
                        )
                else:
                    for proof in exact_partial_dependency_candidates:
                        dependency = str(proof['dependency'])
                        if dependency not in incomplete_dependencies:
                            incomplete_dependencies.append(dependency)
            if (
                stage_id in EXACT_FINALIZATION_STAGE_IDS
                and official_membership_symbols is not None
                and point_in_time_recovery
                and resolved_exact_membership_symbols is not None
                and resolved_symbols is not None
                and set(resolved_symbols)
                != set(official_membership_symbols)
            ):
                recovery_scope = set(resolved_symbols or ())
                parent_retry_debt = set(
                    (
                        recovery_parent_partition or {}
                    ).get('retry_debt_stock_codes')
                    or []
                )
                if (
                    recovery_parent_partition is None
                    or not recovery_scope <= parent_retry_debt
                ):
                    if (
                        'recovery_parent_scope_outcome'
                        not in incomplete_dependencies
                    ):
                        incomplete_dependencies.append(
                            'recovery_parent_scope_outcome'
                        )
                else:
                    # Only a durable parent partition may preserve successes
                    # outside the retry subset.  Never infer that complement
                    # as ``official - retry``: a stale same-day row can exist
                    # for a symbol whose current attempt is still debt.
                    previously_successful = set(
                        recovery_parent_partition[
                            'successful_stock_codes'
                        ]
                    )
                    currently_admitted = set(stage_symbols or ())
                    if not currently_admitted <= recovery_scope:
                        incomplete_dependencies.append(
                            'recovery_current_scope_outcome'
                        )
                    else:
                        stage_symbols = [
                            symbol
                            for symbol in official_membership_symbols
                            if symbol in previously_successful
                            or symbol in currently_admitted
                        ]
            try:
                self._start_tracked_stage(
                    pipeline_run_id=pipeline_run_id,
                    stage_id=stage_id,
                    market_date=resolved_market_date,
                    symbols=stage_symbols,
                )
            except Exception as exc:
                self._finish_run_after_stage_start_failure(
                    pipeline_run_id=pipeline_run_id,
                    error_message=str(exc),
                )
                raise
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
                    invocation_symbols=stage_symbols,
                )
                continue
            if stage_id == 'next_open_execution_plan':
                decider_outcome = stage_outcomes.get('signal_decider')
                decider_status = str((decider_outcome or {}).get('status') or '').strip().lower()
                if (
                    not terminal_decider_symbols
                    and (
                        resume_from_stage is None
                        or same_day_intraday_watch_rearm_resume
                    )
                    and (
                        decider_outcome is None
                        or decider_status == 'completed'
                        or not self._dependency_outcome_is_satisfied(
                            decider_outcome
                        )
                    )
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
                        invocation_symbols=stage_symbols,
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
                            and (
                                not summary_cutoff_expected_at_provided
                                or expected_summary_cutoff_at is not None
                            )
                            and (
                                not expected_completed_symbols_provided
                                or expected_completed_symbols is not None
                            )
                            and (
                                not proof_fs_div_provided
                                or proof_fs_div == resolved_fs_div
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
                                feedback_cutoff_resolution.get('ready')
                                is True
                                and expected_summary_cutoff_at is not None
                                and feedback_cutoff_resolution.get(
                                    'knowledge_cutoff_at'
                                )
                                != expected_summary_cutoff_at
                            ):
                                feedback_cutoff_resolution = {
                                    **feedback_cutoff_resolution,
                                    'ready': False,
                                    'reason': (
                                        'summary_knowledge_cutoff_mismatch'
                                    ),
                                }
                            resolved_completed_symbols = (
                                feedback_cutoff_resolution.get(
                                    'requested_symbols'
                                )
                            )
                            if (
                                feedback_cutoff_resolution.get('ready')
                                is True
                                and expected_completed_symbols is not None
                                and resolved_completed_symbols
                                != expected_completed_symbols
                            ):
                                feedback_cutoff_resolution = {
                                    **feedback_cutoff_resolution,
                                    'ready': False,
                                    'reason': (
                                        'summary_completed_scope_mismatch'
                                    ),
                                }
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
                        stage_invocation_symbols[stage_id] = (
                            list(stage_symbols)
                            if stage_symbols is not None
                            else None
                        )
                        stage_result = self._execute_stage(
                            stage_id,
                            market_date=resolved_market_date,
                            symbols=stage_symbols,
                            exact_scope_symbols=official_membership_symbols,
                            markets=resolved_markets,
                            force=force,
                            bypass_prerequisite_gate=(
                                bypass_prerequisite_gate
                                or inherit_feedback_prerequisite_admission
                            ),
                            allow_expired_analysis_replay=(
                                allow_expired_analysis_replay
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
                            feedback_reuse_parent_run_id=(
                                inherited_feedback_source_run_id
                                if normalized_inherited_feedback_retry_config
                                is not None
                                else None
                            ),
                            feedback_retry_config_override=(
                                normalized_inherited_feedback_retry_config
                                if stage_id == 'agent_feedback_summaries'
                                else None
                            ),
                            feedback_cutoff_resolution=(
                                feedback_cutoff_resolution
                            ),
                            completion_parent_proof=(
                                terminal_decider_progression_proof
                                if stage_id
                                in {
                                    'next_open_execution_plan',
                                    'signal_decider_evaluations',
                                }
                                else None
                            ),
                            point_in_time_recovery=(
                                point_in_time_recovery
                            ),
                            allow_same_day_intraday_watch_continuation=(
                                allow_same_day_intraday_watch_continuation
                            ),
                            allow_previous_session_preopen_recovery=(
                                allow_previous_session_preopen_recovery
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
                            complete_price_scope_proven=(
                                self._complete_price_scope_is_proven(
                                    exact_scope_symbols=(
                                        official_membership_symbols
                                    ),
                                    admitted_symbols=stage_symbols,
                                    price_outcome=stage_outcomes.get(
                                        'price_collection'
                                    ),
                                    price_invocation_symbols=(
                                        stage_invocation_symbols.get(
                                            'price_collection'
                                        )
                                    ),
                                )
                                if stage_id == 'trending_ranks'
                                else False
                            ),
                        )
                        return stage_result

                    if stage_id in FACTOR_STAGE_ORDER:
                        factor_heartbeat_errors: list[Exception] = []

                        def fail_closed_factor_heartbeat() -> bool:
                            try:
                                return self.run_state_service.heartbeat_stage(
                                    pipeline_run_id=pipeline_run_id,
                                    stage_id=stage_id,
                                    market_date=resolved_market_date,
                                )
                            except Exception as exc:
                                factor_heartbeat_errors.append(exc)
                                return False

                        stage_heartbeat = PeriodicStageHeartbeat(
                            fail_closed_factor_heartbeat,
                            interval_seconds=(
                                FACTOR_STAGE_HEARTBEAT_INTERVAL_SECONDS
                            ),
                            thread_name=(
                                'daily-pipeline-'
                                f'{pipeline_run_id}-{stage_id}-heartbeat'
                            ),
                        )
                        if not stage_heartbeat.check_now():
                            if factor_heartbeat_errors:
                                heartbeat_error = factor_heartbeat_errors[0]
                                raise DailyPipelineStageCheckpointError(
                                    'unable to persist factor heartbeat for '
                                    f'stage {stage_id}: {heartbeat_error}'
                                ) from heartbeat_error
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
                        if factor_heartbeat_errors:
                            heartbeat_error = factor_heartbeat_errors[0]
                            raise DailyPipelineStageCheckpointError(
                                'unable to persist factor heartbeat for '
                                f'stage {stage_id}: {heartbeat_error}'
                            ) from heartbeat_error
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
                if exact_partial_dependency_proofs:
                    stage_result = {
                        **stage_result,
                        'exact_partial_dependency_proofs': (
                            exact_partial_dependency_proofs
                        ),
                        'requested_symbols': list(stage_symbols or []),
                    }
                stage_outcomes[stage_id] = self._build_stage_outcome(stage_id, stage_result)
                stage_completion_authority = (
                    self._terminal_decider_progression_proof(
                        stage_outcomes[stage_id],
                        expected_decider_symbols=(
                            feedback_cutoff_resolution.get(
                                'requested_symbols'
                            )
                            if isinstance(
                                feedback_cutoff_resolution,
                                Mapping,
                            )
                            else None
                        ),
                        feedback_cutoff_resolution=(
                            feedback_cutoff_resolution
                        ),
                    )
                    if stage_id == 'signal_decider'
                    else terminal_decider_progression_proof
                    if stage_id in {
                        'next_open_execution_plan',
                        'signal_decider_evaluations',
                    }
                    else None
                )
                self._finish_tracked_stage(
                    pipeline_run_id=pipeline_run_id,
                    stage_id=stage_id,
                    market_date=resolved_market_date,
                    outcome=stage_outcomes[stage_id],
                    result=stage_result,
                    invocation_symbols=stage_invocation_symbols.get(
                        stage_id
                    ),
                    completion_parent_proof=(
                        stage_completion_authority
                    ),
                )
                if (
                    stage_id == 'agent_feedback_summaries'
                    and 'signal_decider' in resolved_stages
                    and str(
                        stage_outcomes[stage_id].get('status') or ''
                    ).strip().lower() in {'completed', 'partial'}
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
                    and not isinstance(
                        exc,
                        DailyPipelineStageCheckpointError,
                    )
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
        merged_stage_outcomes = {
            stage_id: {
                'stage': stage_id,
                'status': stage_status,
            }
            for stage_id, stage_status in merged_stage_statuses.items()
        }
        merged_stage_outcomes.update(stage_outcomes)
        warning_metadata = daily_pipeline_warning_metadata(
            merged_stage_outcomes,
        )
        warning_stage_ids = set(warning_metadata['warning_stage_ids'])
        overall_status = self._resolve_overall_status(
            [
                outcome
                for stage_id, outcome in merged_stage_outcomes.items()
                if stage_id not in warning_stage_ids
            ]
        )
        run_metadata = {
            'stage_statuses': merged_stage_statuses,
            **warning_metadata,
            **self._feedback_cutoff_run_metadata(
                feedback_knowledge_cutoff_at,
                feedback_cutoff_resolution,
            ),
        }
        nonblocking_partial_progression = (
            self._build_nonblocking_partial_progression_marker(
                market_date=resolved_market_date,
                pipeline_run_id=pipeline_run_id,
                reused_eod_collection_run_id=(
                    reused_eod_collection_run_id
                ),
                trigger=trigger,
                stage_outcomes=stage_outcomes,
                feedback_cutoff_resolution=feedback_cutoff_resolution,
            )
        )
        if nonblocking_partial_progression is not None:
            overall_status = 'partial'
            run_metadata.update(
                {
                    'nonblocking_partial_progression': (
                        nonblocking_partial_progression
                    ),
                    'resumable': False,
                }
            )
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
    def _build_nonblocking_partial_progression_marker(
        *,
        market_date: str,
        pipeline_run_id: int,
        reused_eod_collection_run_id: int | None,
        trigger: str,
        stage_outcomes: Mapping[str, dict[str, Any]],
        feedback_cutoff_resolution: Mapping[str, Any] | None,
    ) -> dict[str, Any] | None:
        """Build proof that isolated feedback failures were excluded."""

        if not (
            trigger == 'auto_eod_followup_resume'
            and type(pipeline_run_id) is int
            and pipeline_run_id > 0
            and type(reused_eod_collection_run_id) is int
            and reused_eod_collection_run_id > 0
            and isinstance(feedback_cutoff_resolution, Mapping)
            and feedback_cutoff_resolution.get('ready') is True
        ):
            return None

        completed_scope_proof = feedback_cutoff_resolution.get(
            'completed_scope_proof'
        )
        completed_summary_symbols = feedback_cutoff_resolution.get(
            'requested_symbols'
        )
        if not (
            isinstance(completed_summary_symbols, list)
            and completed_summary_symbols
            and completed_summary_scope_matches_parent(
                feedback_scope_mode=feedback_cutoff_resolution.get(
                    'feedback_scope_mode'
                ),
                completed_symbols=completed_summary_symbols,
                completed_scope_proof=completed_scope_proof,
                parent_symbols=None,
            )
        ):
            return None
        summary_admitted_symbols = list(
            completed_scope_proof['admitted_symbols']
        )
        summary_terminal_proof = feedback_cutoff_resolution.get(
            'terminal_role_scope_proof'
        )
        summary_failed_symbols_by_role = {
            role: [] for role in SUMMARIZER_AGENT_NAMES
        }
        if isinstance(summary_terminal_proof, Mapping):
            raw_summary_failures = summary_terminal_proof.get(
                'failed_symbols_by_role'
            )
            if not (
                summary_terminal_proof.get('schema_version')
                == 'terminal_role_success_intersection_v1'
                and isinstance(raw_summary_failures, Mapping)
                and set(raw_summary_failures)
                == set(SUMMARIZER_AGENT_NAMES)
            ):
                return None
            summary_failed_symbols_by_role = {
                role: list(raw_summary_failures[role])
                for role in SUMMARIZER_AGENT_NAMES
            }
        summary_failed_symbols = set().union(
            *(
                set(symbols)
                for symbols in summary_failed_symbols_by_role.values()
            )
        )
        if set(completed_summary_symbols) != (
            set(summary_admitted_symbols) - summary_failed_symbols
        ):
            return None

        decider_proof = (
            DailyPipelineStageService._terminal_decider_progression_proof(
                stage_outcomes.get('signal_decider'),
                expected_decider_symbols=completed_summary_symbols,
                feedback_cutoff_resolution=feedback_cutoff_resolution,
            )
        )
        if not isinstance(decider_proof, Mapping):
            return None
        decider_failed_symbols_by_role = decider_proof.get(
            'failed_symbols_by_role'
        )
        if not (
            isinstance(decider_failed_symbols_by_role, Mapping)
            and set(decider_failed_symbols_by_role)
            == set(DECISION_SUPPORT_AGENT_NAMES)
        ):
            return None
        decider_failed_symbols_by_role = {
            role: list(decider_failed_symbols_by_role[role])
            for role in DECISION_SUPPORT_AGENT_NAMES
        }
        actionable_symbols = list(decider_proof['completed_symbols'])
        plan_outcome = stage_outcomes.get('next_open_execution_plan')
        evaluation_outcome = stage_outcomes.get(
            'signal_decider_evaluations'
        )
        plan_scope_proof = build_nonblocking_downstream_scope_proof(
            'next_open_execution_plan',
            (plan_outcome or {}).get('result'),
        )
        evaluation_scope_proof = (
            build_nonblocking_downstream_scope_proof(
                'signal_decider_evaluations',
                (evaluation_outcome or {}).get('result'),
            )
        )
        decider_completion_scope_proof = (
            DailyPipelineStageService
            .validated_stage_completion_scope_proof(
                'signal_decider',
                decider_proof,
            )
        )
        plan_completion_scope_proof = (
            DailyPipelineStageService
            ._build_next_open_completion_scope_proof(
                (plan_outcome or {}).get('result') or {},
                parent_scope_proof=decider_completion_scope_proof,
            )
        )
        evaluation_completion_scope_proof = (
            DailyPipelineStageService
            ._build_evaluations_completion_scope_proof(
                (evaluation_outcome or {}).get('result') or {},
                parent_scope_proof=decider_completion_scope_proof,
            )
        )
        if not (
            str((plan_outcome or {}).get('status') or '').strip().lower()
            == 'completed'
            and str(
                (evaluation_outcome or {}).get('status') or ''
            ).strip().lower()
            == 'completed'
            and actionable_symbols
            and isinstance(plan_scope_proof, Mapping)
            and isinstance(evaluation_scope_proof, Mapping)
            and isinstance(decider_completion_scope_proof, Mapping)
            and isinstance(plan_completion_scope_proof, Mapping)
            and isinstance(evaluation_completion_scope_proof, Mapping)
            and evaluation_scope_proof.get('requested_symbols')
            == actionable_symbols
            and set(plan_scope_proof.get('active_symbols') or [])
            <= set(actionable_symbols)
            and (
                summary_failed_symbols
                or any(decider_failed_symbols_by_role.values())
            )
        ):
            return None

        cutoff_at = feedback_cutoff_resolution.get(
            'knowledge_cutoff_at'
        )
        if not isinstance(cutoff_at, datetime):
            try:
                cutoff_at = datetime.fromisoformat(str(cutoff_at or ''))
            except ValueError:
                return None
        if cutoff_at.tzinfo is None or cutoff_at.utcoffset() is None:
            return None
        cutoff_at = cutoff_at.astimezone(timezone.utc)

        marker: dict[str, Any] = {
            'schema_version': (
                NONBLOCKING_PARTIAL_PROGRESSION_SCHEMA_VERSION
            ),
            'blocking_status': 'non_blocking',
            'market_date': market_date,
            'pipeline_run_id': pipeline_run_id,
            'reused_eod_collection_run_id': (
                reused_eod_collection_run_id
            ),
            'summary': {
                'source_run_id': str(
                    feedback_cutoff_resolution.get('source_run_id') or ''
                ).strip(),
                'knowledge_cutoff_at': cutoff_at.isoformat(),
                'feedback_scope_mode': str(
                    feedback_cutoff_resolution.get(
                        'feedback_scope_mode'
                    )
                    or ''
                ).strip(),
                'admitted_symbols': summary_admitted_symbols,
                'completed_symbols': list(completed_summary_symbols),
                'failed_symbols_by_role': (
                    summary_failed_symbols_by_role
                ),
                'total_items': summary_terminal_proof.get(
                    'total_items'
                ) if isinstance(summary_terminal_proof, Mapping) else (
                    len(summary_admitted_symbols)
                    * len(SUMMARIZER_AGENT_NAMES)
                ),
                'completed_items': summary_terminal_proof.get(
                    'completed_items'
                ) if isinstance(summary_terminal_proof, Mapping) else (
                    len(summary_admitted_symbols)
                    * len(SUMMARIZER_AGENT_NAMES)
                ),
                'failed_items': summary_terminal_proof.get(
                    'failed_items'
                ) if isinstance(summary_terminal_proof, Mapping) else 0,
            },
            'decider': {
                'source_run_id': decider_proof['source_run_id'],
                'source_summary_run_id': decider_proof[
                    'source_summary_run_id'
                ],
                'admitted_symbols': list(
                    decider_proof['admitted_symbols']
                ),
                'completed_symbols': actionable_symbols,
                'failed_symbols_by_role': (
                    decider_failed_symbols_by_role
                ),
                'total_items': decider_proof['total_items'],
                'completed_items': decider_proof['completed_items'],
                'failed_items': decider_proof['failed_items'],
            },
            'downstream': {
                'plan_status': 'completed',
                'evaluation_status': 'completed',
                'actionable_symbols': actionable_symbols,
                'plan_scope_proof': plan_scope_proof,
                'evaluation_scope_proof': evaluation_scope_proof,
            },
            'completion_authority': {
                'schema_version': (
                    'nonblocking_strict_completion_authority_v1'
                ),
                'parent_run_id': decider_completion_scope_proof[
                    'source_run_id'
                ],
                'parent_summary_run_id': (
                    decider_completion_scope_proof[
                        'source_summary_run_id'
                    ]
                ),
                'completed_symbols': list(actionable_symbols),
                'decision_identities': list(
                    decider_completion_scope_proof[
                        'decision_identities'
                    ]
                ),
                'signal_decider_proof_digest': (
                    decider_completion_scope_proof['proof_digest']
                ),
                'next_open_execution_plan_proof_digest': (
                    plan_completion_scope_proof['proof_digest']
                ),
                'signal_decider_evaluations_proof_digest': (
                    evaluation_completion_scope_proof['proof_digest']
                ),
                'next_open_manifest_digest': (
                    plan_completion_scope_proof['manifest_digest']
                ),
            },
            'retry_debt': {
                'schema_version': (
                    NONBLOCKING_FEEDBACK_RETRY_DEBT_SCHEMA_VERSION
                ),
                'summary_failed_symbols_by_role': (
                    summary_failed_symbols_by_role
                ),
                'decider_failed_symbols_by_role': (
                    decider_failed_symbols_by_role
                ),
            },
        }
        if not marker['summary']['source_run_id']:
            return None
        marker['proof_digest'] = nonblocking_partial_progression_digest(
            marker
        )
        return marker

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
                'completed_scope_proof': (
                    dict(resolution['completed_scope_proof'])
                    if isinstance(resolution, dict)
                    and isinstance(
                        resolution.get('completed_scope_proof'),
                        Mapping,
                    )
                    else None
                ),
                'terminal_role_scope_proof': (
                    dict(resolution['terminal_role_scope_proof'])
                    if isinstance(resolution, dict)
                    and isinstance(
                        resolution.get('terminal_role_scope_proof'),
                        Mapping,
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
            try:
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
            except Exception as exc:
                raise DailyPipelineStageCheckpointError(
                    f'unable to persist progress for stage {stage_id}: '
                    f'{exc}'
                ) from exc

        return _checkpoint

    def _finish_tracked_stage(
        self,
        *,
        pipeline_run_id: int,
        stage_id: str,
        market_date: str,
        outcome: dict[str, Any],
        result: dict[str, Any],
        invocation_symbols: Sequence[str] | None = None,
        completion_parent_proof: Mapping[str, Any] | None = None,
    ) -> None:
        counts = self._extract_stage_counts(stage_id=stage_id, result=result)
        status = str(outcome.get('status') or 'completed')
        details = self._build_stage_run_details(
            stage_id=stage_id,
            result=result,
            invocation_symbols=invocation_symbols,
            completion_parent_proof=completion_parent_proof,
        )
        stage_reason = self._extract_stage_reason(result)
        if stage_reason:
            details.setdefault('reason', stage_reason)
            if status not in {'completed', 'skipped'}:
                details.setdefault('failure_reason', stage_reason)
        source_run_ids = self._extract_stage_source_run_ids(result)
        completion_scope_proof = details.get('completion_scope_proof')
        missing_completion_authority = bool(
            status == 'completed'
            and (
                (
                    stage_id == 'signal_decider'
                    and not isinstance(
                        completion_scope_proof,
                        Mapping,
                    )
                )
                or (
                    stage_id == 'next_open_execution_plan'
                    and not isinstance(
                        completion_scope_proof,
                        Mapping,
                    )
                )
                or (
                    stage_id == 'signal_decider_evaluations'
                    and not isinstance(
                        completion_scope_proof,
                        Mapping,
                    )
                )
            )
        )
        if missing_completion_authority:
            status = 'partial'
            outcome['status'] = status
            details['completion_authority_status'] = 'invalid'
            details['completion_authority_reason'] = (
                'terminal_progression_proof_unavailable'
            )
        if (
            stage_id in {
                'signal_decider',
                'next_open_execution_plan',
                'signal_decider_evaluations',
            }
            and isinstance(completion_scope_proof, Mapping)
        ):
            if stage_id == 'signal_decider':
                source_run_ids.update(
                    {
                        'signal_decider_run_id': (
                            completion_scope_proof['source_run_id']
                        ),
                        'source_summary_run_id': (
                            completion_scope_proof[
                                'source_summary_run_id'
                            ]
                        ),
                    }
                )
            else:
                source_run_ids.update({
                    'parent_signal_decider_run_id': (
                        completion_scope_proof['parent_run_id']
                    ),
                    'source_summary_run_id': (
                        completion_scope_proof[
                            'parent_summary_run_id'
                        ]
                    ),
                })
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
                + (f': {stage_reason}' if stage_reason else '')
            ),
            'error_message': (
                str(
                    result.get('error')
                    or result.get('error_message')
                    or (
                        stage_reason
                        if status not in {'completed', 'skipped'}
                        else None
                    )
                    or ''
                )
                or None
            ),
            'source_run_ids': source_run_ids,
            'details': details,
        }
        last_error: Exception | None = None
        for _attempt in range(DAILY_PIPELINE_FINALIZATION_ATTEMPTS):
            try:
                self.run_state_service.finish_stage(**finish_kwargs)
                return
            except Exception as exc:
                last_error = exc
        assert last_error is not None
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
            selected_scope = result.get('selected_stock_codes')
            successful_scope = result.get('successful_stock_codes')
            retry_scope = result.get('retry_debt_stock_codes')
            if (
                result.get('scope_outcome_manifest_version')
                == 'technical_snapshots_scope_outcomes_v1'
                and isinstance(selected_scope, list)
                and isinstance(successful_scope, list)
                and isinstance(retry_scope, list)
                and len(selected_scope) == len(set(selected_scope))
                and len(successful_scope) == len(set(successful_scope))
                and len(retry_scope) == len(set(retry_scope))
                and not set(successful_scope).intersection(retry_scope)
                and set(selected_scope)
                == set(successful_scope).union(retry_scope)
            ):
                return {
                    'processed': len(selected_scope),
                    'total': len(selected_scope),
                    'succeeded': len(successful_scope),
                    'failed': len(retry_scope),
                }
            snapshot_succeeded = _int(result.get('snapshots_persisted'))
            snapshot_failed = _int(result.get('snapshots_failed'))
            context_succeeded = _int(result.get('contexts_persisted'))
            context_failed = _int(result.get('contexts_failed'))
            total = max(
                snapshot_succeeded + snapshot_failed,
                context_succeeded + context_failed,
            )
            if (
                result.get('scope_outcome_contract_status') == 'invalid'
            ):
                retryable_symbols = result.get('retryable_symbols')
                retryable_count = _int(
                    result.get('retryable_symbol_count')
                )
                if isinstance(retryable_symbols, list):
                    retryable_count = max(
                        retryable_count,
                        len(retryable_symbols),
                    )
                total = max(total, retryable_count)
                return {
                    'processed': total,
                    'total': total,
                    'succeeded': 0,
                    'failed': total,
                }
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
            total = (
                len(result.get('selected_stock_codes') or [])
                or len(result.get('requested_symbols') or [])
                or len(result.get('matched_symbols') or [])
                or succeeded + failed
            )
            return {
                'processed': succeeded + failed,
                'total': total,
                'succeeded': succeeded,
                'failed': failed,
            }
        if stage_id == 'market_risk_snapshots':
            succeeded = _int(result.get('snapshots_persisted'))
            failed = _int(result.get('snapshots_failed'))
            requested_symbols = result.get('requested_symbols')
            if isinstance(requested_symbols, list):
                total = len(requested_symbols)
                failed = max(failed, total - succeeded)
            else:
                total = (
                    len(result.get('matched_symbols') or [])
                    or succeeded + failed
                )
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
        if stage_id == 'fundamentals_daily_refresh':
            has_symbol_scope = any(
                key in result
                for key in (
                    'symbols_checked',
                    'symbols_selected',
                    'successful_stock_codes',
                    'retry_debt_stock_codes',
                    'retry_debt_count',
                )
            )
            if has_symbol_scope:
                successful_stock_codes = result.get(
                    'successful_stock_codes'
                )
                succeeded = (
                    len(successful_stock_codes)
                    if isinstance(successful_stock_codes, list)
                    else _int(result.get('symbols_fresh'))
                    + _int(result.get('symbols_refreshed'))
                )
                retry_debt_count = _int(result.get('retry_debt_count'))
                if retry_debt_count <= 0 and isinstance(
                    result.get('retry_debt_stock_codes'),
                    list,
                ):
                    retry_debt_count = len(
                        result['retry_debt_stock_codes']
                    )
                selected_stock_codes = result.get('selected_stock_codes')
                total = (
                    len(selected_stock_codes)
                    if isinstance(selected_stock_codes, list)
                    else _int(
                        result.get('symbols_checked')
                        or result.get('symbols_selected')
                    )
                )
                failed = (
                    0
                    if retry_debt_count > 0
                    else _int(result.get('symbols_failed'))
                )
                if total <= 0:
                    total = succeeded + retry_debt_count + failed
            else:
                succeeded = _int(result.get('events_captured'))
                failed = _int(result.get('events_failed'))
                total = (
                    _int(result.get('events_detected'))
                    or succeeded + failed
                )
            return {
                'processed': min(succeeded + failed, total),
                'total': max(total, succeeded + failed),
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
            total = _int(summary.get(total_key))
            failed = _int(summary.get('failed_symbol_count')) + _int(
                summary.get('skipped_symbol_count')
            )
            if 'collected_symbol_count' in summary:
                collected = _int(summary.get('collected_symbol_count'))
            elif 'matched_symbol_count' in summary:
                collected = _int(summary.get('matched_symbol_count'))
            elif any(
                _int(summary.get(key)) > 0
                for key in (
                    'items_fetched',
                    'items_inserted',
                    'items_updated',
                    'symbols_with_any_news',
                )
            ):
                collected = max(total - failed, 0)
            else:
                collected = 0
            # A successful collection attempt is terminal work even when the
            # provider returns no recent article.  Fresh/stale/no-result is a
            # data-quality distinction, not a task outcome; using only
            # ``recent_news_symbol_count`` made successful no-result and stale
            # symbols disappear from both success and failure totals.
            succeeded = collected
            total = max(total, collected + failed)
            return {
                'processed': min(collected + failed, total) if total else collected + failed,
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
            failed = _int(result.get('periods_failed')) + _int(
                result.get('periods_unavailable')
            )
            total = _int(result.get('periods_total')) or succeeded + failed
            return {
                'processed': succeeded + failed,
                'total': total,
                'succeeded': succeeded,
                'failed': failed,
            }
        if stage_id == 'signal_features':
            selected_scope = result.get('selected_stock_codes')
            if isinstance(selected_scope, list):
                scope_status = str(
                    result.get('status') or ''
                ).strip().lower()
                if scope_status not in {'completed', 'partial'}:
                    scope_status = (
                        'partial'
                        if result.get('retry_debt_stock_codes')
                        else 'completed'
                    )
                partition = _validated_exact_scope_outcome(
                    result=result,
                    invocation_symbols=selected_scope,
                    status=scope_status,
                )
                if partition is not None:
                    succeeded = len(
                        partition['successful_stock_codes']
                    ) + len(
                        partition['terminal_not_applicable_stock_codes']
                    )
                    failed = len(partition['retry_debt_stock_codes'])
                    total = len(partition['selected_stock_codes'])
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
        if stage_id == 'fundamentals_daily_refresh':
            try:
                retry_debt_count = int(
                    result.get('retry_debt_count') or 0
                )
            except (TypeError, ValueError):
                retry_debt_count = 0
            if retry_debt_count <= 0 and isinstance(
                result.get('retry_debt_stock_codes'),
                list,
            ):
                retry_debt_count = len(result['retry_debt_stock_codes'])
            if retry_debt_count <= 0:
                return {}
            return {
                'remaining': 'retry_debt',
                'retry_debt_count': retry_debt_count,
            }
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
    def _extract_stage_reason(result: Mapping[str, Any]) -> str | None:
        payloads: list[Mapping[str, Any]] = []
        pending: list[Mapping[str, Any]] = [result]
        seen_ids: set[int] = set()
        while pending:
            payload = pending.pop(0)
            if id(payload) in seen_ids:
                continue
            seen_ids.add(id(payload))
            payloads.append(payload)
            for key in ('result', 'result_summary', 'summary', 'details'):
                nested = payload.get(key)
                if isinstance(nested, Mapping):
                    pending.append(nested)

        for key in (
            'failure_reason',
            'reason',
            'error',
            'error_message',
        ):
            for payload in payloads:
                value = str(payload.get(key) or '').strip()
                if value:
                    return value
        for payload in payloads:
            partial_reasons = payload.get('partial_reasons')
            if not isinstance(partial_reasons, (list, tuple)):
                continue
            for reason in partial_reasons:
                value = str(reason or '').strip()
                if value:
                    return value
        return None

    @staticmethod
    def _build_exact_scope_outcome_proof(
        result: Mapping[str, Any],
        *,
        invocation_symbols: Sequence[str] | None,
    ) -> dict[str, Any] | None:
        selected = result.get('selected_stock_codes')
        if not isinstance(selected, list) or invocation_symbols is None:
            return None
        normalized_selected = normalize_symbol_codes(selected)
        normalized_invocation = normalize_symbol_codes(invocation_symbols)
        if (
            not normalized_selected
            or len(normalized_selected) != len(selected)
            or len(normalized_invocation) != len(invocation_symbols)
            or set(normalized_selected) != set(normalized_invocation)
        ):
            return None
        canonical_invocation = sorted(normalized_invocation)
        status = str(result.get('status') or '').strip().lower()
        if status not in {'completed', 'partial'}:
            status = DailyPipelineStageService._resolve_result_status(
                dict(result)
            )
        partition = _validated_exact_scope_outcome(
            result=result,
            invocation_symbols=selected,
            status=status,
        )
        if partition is None:
            return None
        successful = partition['successful_stock_codes']
        return {
            'schema_version': EXACT_SCOPE_OUTCOME_PROOF_SCHEMA_VERSION,
            'manifest_version': partition['manifest_version'],
            # Keep the invocation identities outside the compact result
            # summary.  The count/hash binds the list to the invocation that
            # actually ran, while the retry/terminal sets make the successful
            # complement recoverable after a process restart.
            'selected_stock_codes': list(canonical_invocation),
            'selected_symbol_count': len(canonical_invocation),
            'selected_symbol_sha256': market_membership_sha256(
                canonical_invocation
            ),
            'successful_symbol_count': len(successful),
            'successful_symbol_sha256': market_membership_sha256(
                successful
            ),
            'retry_debt_stock_codes': list(
                partition['retry_debt_stock_codes']
            ),
            'terminal_not_applicable_stock_codes': list(
                partition['terminal_not_applicable_stock_codes']
            ),
        }

    @staticmethod
    def _stage_completion_scope_proof_digest(
        proof: Mapping[str, Any],
    ) -> str:
        payload = {
            str(key): value
            for key, value in proof.items()
            if str(key) != 'proof_digest'
        }
        serialized = json.dumps(
            payload,
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=True,
        )
        return hashlib.sha256(serialized.encode('utf-8')).hexdigest()

    @staticmethod
    def _build_feedback_completion_scope_proof(
        result: Mapping[str, Any],
        *,
        invocation_symbols: Sequence[str] | None,
    ) -> dict[str, Any] | None:
        if str(result.get('status') or '').strip().lower() != 'completed':
            return None
        roles = result.get('roles')
        progress = result.get('progress')
        summary = (
            progress.get('summary')
            if isinstance(progress, Mapping)
            and isinstance(progress.get('summary'), Mapping)
            else None
        )
        role_progress = (
            summary.get('role_progress')
            if isinstance(summary, Mapping)
            else None
        )
        role_scopes = (
            summary.get('role_symbol_scopes')
            if isinstance(summary, Mapping)
            else None
        )
        source_run_id = str(result.get('run_id') or '').strip()
        raw_knowledge_cutoff_at = result.get('knowledge_cutoff_at')
        try:
            knowledge_cutoff_at = (
                raw_knowledge_cutoff_at
                if isinstance(raw_knowledge_cutoff_at, datetime)
                else datetime.fromisoformat(
                    str(raw_knowledge_cutoff_at or '').strip()
                )
            )
        except (TypeError, ValueError):
            return None
        if (
            knowledge_cutoff_at.tzinfo is None
            or knowledge_cutoff_at.utcoffset() is None
        ):
            return None
        knowledge_cutoff_at_iso = knowledge_cutoff_at.astimezone(
            timezone.utc
        ).isoformat()
        feedback_scope_mode = str(
            result.get('feedback_scope_mode') or ''
        ).strip()
        expected_roles = list(SUMMARIZER_AGENT_NAMES)
        if not (
            isinstance(roles, list)
            and len(roles) == len(expected_roles)
            and set(roles) == set(expected_roles)
            and isinstance(summary, Mapping)
            and isinstance(role_progress, Mapping)
            and set(role_progress) == set(expected_roles)
            and isinstance(role_scopes, Mapping)
            and set(role_scopes) == set(expected_roles)
            and source_run_id
            and str(summary.get('run_id') or source_run_id).strip()
            == source_run_id
            and feedback_scope_mode
            in {
                FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
                FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
            }
        ):
            return None

        def strict_count(value: Any) -> int | None:
            if isinstance(value, bool) or not isinstance(value, int):
                return None
            return value if value >= 0 else None

        canonical_scopes: dict[str, list[str]] = {}
        role_counts: dict[str, int] = {}
        for role in expected_roles:
            raw_scope = role_scopes.get(role)
            state = role_progress.get(role)
            if not isinstance(raw_scope, list) or not isinstance(
                state,
                Mapping,
            ):
                return None
            normalized_scope = normalize_symbol_codes(raw_scope)
            if (
                len(normalized_scope) != len(raw_scope)
                or len(set(normalized_scope)) != len(normalized_scope)
            ):
                return None
            normalized_scope = sorted(normalized_scope)
            counts = {
                key: strict_count(state.get(key))
                for key in (
                    'total_symbols',
                    'processed_symbols',
                    'completed_symbols',
                    'in_progress_symbols',
                    'pending_symbols',
                    'failed_items',
                    'retry_pending_symbols',
                )
            }
            if not (
                str(state.get('role') or '').strip() == role
                and str(state.get('status') or '').strip().lower()
                == 'completed'
                and str(
                    state.get('boundary_status') or ''
                ).strip().lower()
                == 'completed'
                and counts['total_symbols'] == len(normalized_scope)
                and counts['processed_symbols'] == len(normalized_scope)
                and counts['completed_symbols'] == len(normalized_scope)
                and counts['in_progress_symbols'] == 0
                and counts['pending_symbols'] == 0
                and counts['failed_items'] == 0
                and counts['retry_pending_symbols'] == 0
            ):
                return None
            canonical_scopes[role] = normalized_scope
            role_counts[role] = len(normalized_scope)

        total_item_count = sum(role_counts.values())
        if not (
            total_item_count > 0
            and strict_count(summary.get('expected_feedback_count'))
            == total_item_count
            and strict_count(summary.get('completed_feedback_count'))
            == total_item_count
            and strict_count(summary.get('failed_items')) == 0
        ):
            return None
        scope_symbols = sorted(
            {
                symbol
                for role_scope in canonical_scopes.values()
                for symbol in role_scope
            }
        )
        if invocation_symbols is None:
            requested_scope_kind = 'resolved_role_scope_union'
            requested_symbols = list(scope_symbols)
        else:
            requested_symbols = sorted(
                normalize_symbol_codes(invocation_symbols)
            )
            if not (
                requested_symbols
                and len(requested_symbols) == len(invocation_symbols)
                and len(set(requested_symbols)) == len(requested_symbols)
                and set(scope_symbols) <= set(requested_symbols)
            ):
                return None
            requested_scope_kind = 'explicit_invocation'
        proof: dict[str, Any] = {
            'schema_version': (
                STAGE_COMPLETION_SCOPE_PROOF_SCHEMA_VERSION
            ),
            'kind': 'agent_feedback_summaries_role_scope',
            'source_run_id': source_run_id,
            'knowledge_cutoff_at': knowledge_cutoff_at_iso,
            'feedback_scope_mode': feedback_scope_mode,
            'roles': expected_roles,
            'requested_scope_kind': requested_scope_kind,
            'requested_symbols': requested_symbols,
            'scope_symbols': scope_symbols,
            'role_symbol_scopes': canonical_scopes,
            'role_symbol_counts': role_counts,
            'role_scope_manifest_fingerprint': (
                DailyPipelineStageService
                ._stage_completion_scope_proof_digest(
                    {
                        'requested_symbols': requested_symbols,
                        'role_symbol_scopes': canonical_scopes,
                    }
                )
            ),
            'total_item_count': total_item_count,
        }
        proof['proof_digest'] = (
            DailyPipelineStageService
            ._stage_completion_scope_proof_digest(proof)
        )
        return proof

    @staticmethod
    def _build_next_open_completion_scope_proof(
        result: Mapping[str, Any],
        *,
        parent_scope_proof: Mapping[str, Any] | None,
    ) -> dict[str, Any] | None:
        parent_scope_proof = (
            DailyPipelineStageService
            .validated_stage_completion_scope_proof(
                'signal_decider',
                parent_scope_proof,
            )
        )
        if parent_scope_proof is None:
            return None
        parent_run_id = str(
            parent_scope_proof.get('source_run_id') or ''
        ).strip()
        parent_summary_run_id = str(
            parent_scope_proof.get('source_summary_run_id') or ''
        ).strip()
        raw_admitted_symbols = parent_scope_proof.get(
            'admitted_symbols'
        )
        raw_completed_symbols = parent_scope_proof.get(
            'completed_symbols'
        )
        if not (
            parent_run_id
            and parent_summary_run_id
            and isinstance(raw_admitted_symbols, list)
            and isinstance(raw_completed_symbols, list)
        ):
            return None
        parent_admitted_symbols = sorted(
            normalize_symbol_codes(raw_admitted_symbols)
        )
        parent_completed_symbols = sorted(
            normalize_symbol_codes(raw_completed_symbols)
        )
        raw_parent_identities = parent_scope_proof.get(
            'decision_identities'
        )
        if not (
            parent_admitted_symbols
            and parent_completed_symbols
            and len(parent_admitted_symbols)
            == len(raw_admitted_symbols)
            and len(parent_completed_symbols)
            == len(raw_completed_symbols)
            and set(parent_completed_symbols)
            <= set(parent_admitted_symbols)
            and isinstance(raw_parent_identities, list)
        ):
            return None
        parent_identities_by_key = {
            (
                str(identity['symbol']),
                str(identity['decision_agent_name']),
            ): (
                int(identity['decision_snapshot_id']),
                str(identity['decision_run_id']),
            )
            for identity in raw_parent_identities
        }
        manifest = result.get('completion_manifest')
        items = result.get('items')
        if not (
            str(result.get('status') or '').strip().lower()
            == 'completed'
            and isinstance(manifest, Mapping)
            and isinstance(items, list)
        ):
            return None
        raw_manifest_digest = str(
            manifest.get('manifest_digest') or ''
        ).strip()
        manifest_payload = {
            str(key): value
            for key, value in manifest.items()
            if str(key) != 'manifest_digest'
        }
        manifest_digest = hashlib.sha256(
            json.dumps(
                manifest_payload,
                sort_keys=True,
                separators=(',', ':'),
                ensure_ascii=True,
            ).encode('utf-8')
        ).hexdigest()
        role_symbol_scopes = manifest.get('role_symbol_scopes')
        partition = manifest.get('decision_partition')
        if not (
            manifest.get('schema_version')
            == 'next_open_completion_manifest_v1'
            and raw_manifest_digest == manifest_digest
            and str(manifest.get('parent_run_id') or '').strip()
            == parent_run_id
            and str(
                manifest.get('authority_fingerprint') or ''
            ).strip()
            and str(
                manifest.get('generation_fingerprint') or ''
            ).strip()
            and isinstance(role_symbol_scopes, Mapping)
            and set(role_symbol_scopes)
            == set(DECISION_SUPPORT_AGENT_NAMES)
            and isinstance(partition, list)
        ):
            return None
        manifest_role_scopes: dict[str, list[str]] = {}
        for role in DECISION_SUPPORT_AGENT_NAMES:
            raw_scope = role_symbol_scopes.get(role)
            if not (
                isinstance(raw_scope, list)
                and normalize_symbol_codes(raw_scope) == raw_scope
                and sorted(set(raw_scope)) == raw_scope
                and raw_scope == parent_completed_symbols
            ):
                return None
            manifest_role_scopes[role] = list(raw_scope)
        canonical_role_scopes = {
            role: list(parent_completed_symbols)
            for role in DECISION_SUPPORT_AGENT_NAMES
        }
        expected_decisions = {
            (symbol, role)
            for role in DECISION_SUPPORT_AGENT_NAMES
            for symbol in parent_completed_symbols
        }
        manifest_decisions = {
            (symbol, role)
            for role, symbols in manifest_role_scopes.items()
            for symbol in symbols
        }

        canonical_partition: list[dict[str, Any]] = []
        result_item_identity: set[
            tuple[int, str, str, str, int, str]
        ] = set()
        for item in items:
            if not isinstance(item, Mapping):
                return None
            plan_item_id = item.get('plan_item_id')
            account_reference = str(
                item.get('account_reference') or ''
            ).strip()
            normalized_symbol = normalize_symbol_codes(
                [item.get('symbol')]
            )
            decision_agent_name = str(
                item.get('decision_agent_name')
                or item.get('agent_name')
                or ''
            ).strip()
            decision_snapshot_id = item.get('decision_snapshot_id')
            decision_run_id = str(
                item.get('decision_run_id') or ''
            ).strip()
            identity = (
                plan_item_id,
                account_reference,
                normalized_symbol[0] if normalized_symbol else '',
                decision_agent_name,
                decision_snapshot_id,
                decision_run_id,
            )
            if not (
                type(plan_item_id) is int
                and plan_item_id > 0
                and account_reference
                and len(normalized_symbol) == 1
                and decision_agent_name
                in DECISION_SUPPORT_AGENT_NAMES
                and type(decision_snapshot_id) is int
                and decision_snapshot_id > 0
                and decision_run_id
                and identity not in result_item_identity
            ):
                return None
            result_item_identity.add(identity)

        manifest_item_identity: set[
            tuple[int, str, str, str, int, str]
        ] = set()
        seen_decisions: set[tuple[str, str]] = set()
        included_decision_count = 0
        excluded_decision_count = 0
        active_item_count = 0
        manifest_included_decision_count = 0
        manifest_excluded_decision_count = 0
        manifest_incomplete_decision_count = 0
        for raw_entry in partition:
            if not isinstance(raw_entry, Mapping):
                return None
            normalized_symbol = normalize_symbol_codes(
                [raw_entry.get('symbol')]
            )
            role = str(
                raw_entry.get('decision_agent_name') or ''
            ).strip()
            decision_run_id = str(
                raw_entry.get('decision_run_id') or ''
            ).strip()
            snapshot_id = raw_entry.get('decision_snapshot_id')
            canonical_action = str(
                raw_entry.get('canonical_action') or ''
            ).strip().lower()
            authority_fingerprint = str(
                raw_entry.get('authority_fingerprint') or ''
            ).strip()
            generation_fingerprint = str(
                raw_entry.get('generation_fingerprint') or ''
            ).strip()
            disposition = str(
                raw_entry.get('disposition') or ''
            ).strip().lower()
            included_items = raw_entry.get('included_items')
            exclusion = raw_entry.get('exclusion')
            decision_key = (
                normalized_symbol[0] if normalized_symbol else '',
                role,
            )
            actionable = bool(
                (role == 'signal-decider' and canonical_action == 'buy')
                or (
                    role == 'short-term-decider'
                    and canonical_action
                    in {'scalp_buy_candidate', 'watch_for_trigger'}
                )
            )
            if not (
                len(normalized_symbol) == 1
                and role in DECISION_SUPPORT_AGENT_NAMES
                and decision_key in manifest_decisions
                and decision_key not in seen_decisions
                and disposition
                in {'included', 'non_actionable', 'incomplete'}
                and isinstance(included_items, list)
            ):
                return None
            seen_decisions.add(decision_key)
            if disposition == 'included':
                manifest_included_decision_count += 1
            elif disposition == 'non_actionable':
                manifest_excluded_decision_count += 1
            else:
                manifest_incomplete_decision_count += 1

            expected_identity = parent_identities_by_key.get(
                decision_key
            )
            if not (
                expected_identity is not None
                and decision_run_id
                and (snapshot_id, decision_run_id)
                == expected_identity
                and type(snapshot_id) is int
                and snapshot_id > 0
                and canonical_action
                and authority_fingerprint
                and generation_fingerprint
                and disposition in {'included', 'non_actionable'}
            ):
                return None
            canonical_items: list[dict[str, Any]] = []
            if disposition == 'included':
                if not actionable or not included_items or exclusion is not None:
                    return None
                included_decision_count += 1
                for raw_item in included_items:
                    if not isinstance(raw_item, Mapping):
                        return None
                    plan_item_id = raw_item.get('plan_item_id')
                    account_reference = str(
                        raw_item.get('account_reference') or ''
                    ).strip()
                    eligibility_status = str(
                        raw_item.get('eligibility_status') or ''
                    ).strip().lower()
                    handoff_active = raw_item.get('handoff_active')
                    exec_monitoring_active = raw_item.get(
                        'exec_monitoring_active'
                    )
                    row_authority_fingerprint = str(
                        raw_item.get('row_authority_fingerprint')
                        or ''
                    ).strip()
                    item_identity = (
                        plan_item_id,
                        account_reference,
                        normalized_symbol[0],
                        role,
                        snapshot_id,
                        decision_run_id,
                    )
                    if not (
                        type(plan_item_id) is int
                        and plan_item_id > 0
                        and account_reference
                        and eligibility_status
                        and type(handoff_active) is bool
                        and type(exec_monitoring_active) is bool
                        and row_authority_fingerprint
                        and item_identity not in manifest_item_identity
                    ):
                        return None
                    manifest_item_identity.add(item_identity)
                    active_item_count += int(
                        handoff_active or exec_monitoring_active
                    )
                    canonical_items.append(
                        {
                            'plan_item_id': plan_item_id,
                            'account_reference': account_reference,
                            'eligibility_status': eligibility_status,
                            'handoff_active': handoff_active,
                            'exec_monitoring_active': (
                                exec_monitoring_active
                            ),
                            'row_authority_fingerprint': (
                                row_authority_fingerprint
                            ),
                        }
                    )
                canonical_items.sort(
                    key=lambda item: (
                        item['account_reference'],
                        item['plan_item_id'],
                    )
                )
                if canonical_items != included_items:
                    return None
                canonical_exclusion = None
            else:
                if not (
                    not actionable
                    and not included_items
                    and isinstance(exclusion, Mapping)
                    and set(exclusion) == {'kind', 'reason_code'}
                    and exclusion.get('kind') == 'non_actionable'
                    and exclusion.get('reason_code')
                    == 'latest_snapshot_not_actionable'
                ):
                    return None
                excluded_decision_count += 1
                canonical_exclusion = dict(exclusion)
            canonical_partition.append(
                {
                    'symbol': normalized_symbol[0],
                    'decision_agent_name': role,
                    'decision_run_id': decision_run_id,
                    'decision_snapshot_id': snapshot_id,
                    'canonical_action': canonical_action,
                    'authority_fingerprint': authority_fingerprint,
                    'generation_fingerprint': generation_fingerprint,
                    'disposition': disposition,
                    'included_items': canonical_items,
                    'exclusion': canonical_exclusion,
                }
            )
        canonical_partition.sort(
            key=lambda entry: (
                entry['symbol'],
                entry['decision_agent_name'],
            )
        )
        included_item_count = len(manifest_item_identity)
        if not (
            manifest_decisions == expected_decisions
            and seen_decisions == expected_decisions
            and len(canonical_partition) == len(expected_decisions)
            and result_item_identity == manifest_item_identity
            and manifest.get('decision_count')
            == len(manifest_decisions)
            and manifest.get('included_decision_count')
            == manifest_included_decision_count
            and manifest.get('excluded_decision_count')
            == manifest_excluded_decision_count
            and manifest.get('incomplete_decision_count')
            == manifest_incomplete_decision_count
            == 0
            and manifest.get('included_item_count')
            == included_item_count
        ):
            return None
        proof: dict[str, Any] = {
            'schema_version': (
                STAGE_COMPLETION_SCOPE_PROOF_SCHEMA_VERSION
            ),
            'kind': 'next_open_execution_plan_parent_scope',
            'parent_run_id': parent_run_id,
            'parent_summary_run_id': parent_summary_run_id,
            'parent_admitted_symbols': parent_admitted_symbols,
            'parent_completed_symbols': parent_completed_symbols,
            'authority_fingerprint': str(
                manifest['authority_fingerprint']
            ),
            'generation_fingerprint': str(
                manifest['generation_fingerprint']
            ),
            'role_symbol_scopes': canonical_role_scopes,
            'decision_partition': canonical_partition,
            'decision_count': len(expected_decisions),
            'included_decision_count': included_decision_count,
            'excluded_decision_count': excluded_decision_count,
            'item_count': included_item_count,
            'active_item_count': active_item_count,
            'blocked_item_count': (
                included_item_count - active_item_count
            ),
            'manifest_digest': manifest_digest,
        }
        proof['proof_digest'] = (
            DailyPipelineStageService
            ._stage_completion_scope_proof_digest(proof)
        )
        return proof

    @staticmethod
    def _build_evaluations_completion_scope_proof(
        result: Mapping[str, Any],
        *,
        parent_scope_proof: Mapping[str, Any] | None,
    ) -> dict[str, Any] | None:
        """Bind an evaluation refresh to one exact terminal decider scope."""

        parent_scope_proof = (
            DailyPipelineStageService
            .validated_stage_completion_scope_proof(
                'signal_decider',
                parent_scope_proof,
            )
        )
        if parent_scope_proof is None:
            return None
        parent_run_id = str(
            parent_scope_proof.get('source_run_id') or ''
        ).strip()
        parent_summary_run_id = str(
            parent_scope_proof.get('source_summary_run_id') or ''
        ).strip()
        raw_parent_symbols = parent_scope_proof.get('completed_symbols')
        raw_parent_identities = parent_scope_proof.get(
            'decision_identities'
        )
        if not (
            parent_run_id
            and parent_summary_run_id
            and isinstance(raw_parent_symbols, list)
            and isinstance(raw_parent_identities, list)
        ):
            return None
        parent_symbols = sorted(normalize_symbol_codes(raw_parent_symbols))
        signal_identity_by_symbol = {
            str(identity['symbol']): (
                int(identity['decision_snapshot_id']),
                str(identity['decision_run_id']),
            )
            for identity in raw_parent_identities
            if identity.get('decision_agent_name') == 'signal-decider'
        }
        request = result.get('request')
        summary = result.get('summary')
        items = result.get('items')
        if not (
            parent_symbols
            and len(parent_symbols) == len(raw_parent_symbols)
            and set(signal_identity_by_symbol) == set(parent_symbols)
            and str(result.get('status') or '').strip().lower()
            == 'completed'
            and isinstance(request, Mapping)
            and isinstance(summary, Mapping)
            and isinstance(items, list)
        ):
            return None
        raw_request_symbols = request.get('symbols')
        raw_horizons = request.get('horizons')
        if not (
            isinstance(raw_request_symbols, list)
            and normalize_symbol_codes(raw_request_symbols)
            == raw_request_symbols
            and raw_request_symbols == parent_symbols
            and isinstance(raw_horizons, list)
            and raw_horizons
            and all(
                type(horizon) is int and horizon > 0
                for horizon in raw_horizons
            )
            and len(set(raw_horizons)) == len(raw_horizons)
        ):
            return None
        horizons = sorted(raw_horizons)
        evaluation_version = str(
            request.get('evaluation_version') or ''
        ).strip()
        evaluation_rule_set_version = str(
            request.get('evaluation_rule_set_version') or ''
        ).strip()
        if (
            horizons != raw_horizons
            or not evaluation_version
            or not evaluation_rule_set_version
        ):
            return None

        def strict_count(value: Any) -> int | None:
            return (
                value
                if type(value) is int and value >= 0
                else None
            )

        decisions_considered = strict_count(
            summary.get('decisions_considered')
        )
        expected_item_count = len(items) * len(horizons)
        if not (
            str(summary.get('status') or '').strip().lower()
            == 'completed'
            and decisions_considered == len(items)
            and decisions_considered > 0
            and strict_count(summary.get('outcomes_upserted'))
            == expected_item_count
            and strict_count(summary.get('evaluations_upserted'))
            == expected_item_count
        ):
            return None

        decision_horizon_scope: list[dict[str, Any]] = []
        seen_snapshots: set[int] = set()
        seen_symbols: set[str] = set()
        for item in items:
            if not isinstance(item, Mapping):
                return None
            snapshot_id = item.get('snapshot_id')
            feedback_id = item.get('feedback_id')
            decision_run_id = str(
                item.get('decision_run_id') or ''
            ).strip()
            agent_name = str(item.get('agent_name') or '').strip()
            normalized_symbol = normalize_symbol_codes(
                [item.get('symbol')]
            )
            market_date = str(item.get('market_date') or '').strip()
            policy_version = str(
                item.get('policy_version') or ''
            ).strip()
            decision_payload = item.get('decision')
            decision_action = str(
                decision_payload.get('action')
                if isinstance(decision_payload, Mapping)
                else ''
            ).strip().lower()
            outcomes = item.get('outcomes')
            evaluations = item.get('evaluations')
            symbol = normalized_symbol[0] if normalized_symbol else ''
            expected_identity = signal_identity_by_symbol.get(symbol)
            if not (
                type(snapshot_id) is int
                and snapshot_id > 0
                and type(feedback_id) is int
                and feedback_id > 0
                and snapshot_id not in seen_snapshots
                and decision_run_id
                and agent_name == 'signal-decider'
                and len(normalized_symbol) == 1
                and symbol in parent_symbols
                and symbol not in seen_symbols
                and expected_identity
                == (snapshot_id, decision_run_id)
                and market_date
                and policy_version
                and decision_action
                and isinstance(outcomes, list)
                and isinstance(evaluations, list)
            ):
                return None
            seen_snapshots.add(snapshot_id)
            seen_symbols.add(symbol)
            outcomes_by_horizon: dict[int, Mapping[str, Any]] = {}
            evaluations_by_horizon: dict[int, Mapping[str, Any]] = {}
            for raw_entry, target in (
                (outcomes, outcomes_by_horizon),
                (evaluations, evaluations_by_horizon),
            ):
                for entry in raw_entry:
                    if not isinstance(entry, Mapping):
                        return None
                    horizon = entry.get('horizon_days')
                    if not (
                        type(horizon) is int
                        and horizon in horizons
                        and horizon not in target
                        and entry.get('decision_snapshot_id')
                        == snapshot_id
                    ):
                        return None
                    target[horizon] = entry
            if (
                sorted(outcomes_by_horizon) != horizons
                or sorted(evaluations_by_horizon) != horizons
            ):
                return None
            for horizon in horizons:
                outcome = outcomes_by_horizon[horizon]
                evaluation = evaluations_by_horizon[horizon]
                outcome_id = outcome.get('id')
                evaluation_id = evaluation.get('id')
                entry_evaluation_version = str(
                    evaluation.get('evaluation_version') or ''
                ).strip()
                outcome_action = str(
                    outcome.get('decision_action') or ''
                ).strip().lower()
                evaluation_action = str(
                    evaluation.get('decision_action') or ''
                ).strip().lower()
                if not (
                    type(outcome_id) is int
                    and outcome_id > 0
                    and type(evaluation_id) is int
                    and evaluation_id > 0
                    and entry_evaluation_version == evaluation_version
                    and outcome_action == decision_action
                    and evaluation_action == decision_action
                ):
                    return None
                decision_horizon_scope.append(
                    {
                        'snapshot_id': snapshot_id,
                        'feedback_id': feedback_id,
                        'decision_run_id': decision_run_id,
                        'decision_agent_name': agent_name,
                        'symbol': normalized_symbol[0],
                        'market_date': market_date,
                        'decision_action': decision_action,
                        'policy_version': policy_version,
                        'horizon_days': horizon,
                        'outcome_id': outcome_id,
                        'evaluation_id': evaluation_id,
                        'evaluation_version': entry_evaluation_version,
                        'evaluation_rule_set_version': (
                            evaluation_rule_set_version
                        ),
                        'evaluation_status': str(
                            evaluation.get('evaluation_status') or ''
                        ).strip().lower(),
                    }
                )
        decision_horizon_scope.sort(
            key=lambda entry: (
                entry['market_date'],
                entry['symbol'],
                entry['decision_run_id'],
                entry['snapshot_id'],
                entry['horizon_days'],
            )
        )
        evaluated_symbols = sorted(
            {
                entry['symbol']
                for entry in decision_horizon_scope
            }
        )
        if not (
            len(decision_horizon_scope) == expected_item_count
            and evaluated_symbols == parent_symbols
            and seen_symbols == set(parent_symbols)
            and len(items) == len(parent_symbols)
        ):
            return None
        proof: dict[str, Any] = {
            'schema_version': STAGE_COMPLETION_SCOPE_PROOF_SCHEMA_VERSION,
            'kind': 'signal_decider_evaluations_parent_scope',
            'parent_run_id': parent_run_id,
            'parent_summary_run_id': parent_summary_run_id,
            'parent_completed_symbols': parent_symbols,
            'request_symbols': list(raw_request_symbols),
            'horizons': horizons,
            'evaluation_version': evaluation_version,
            'evaluation_rule_set_version': evaluation_rule_set_version,
            'decision_horizon_scope': decision_horizon_scope,
            'decision_count': len(items),
            'item_count': expected_item_count,
        }
        proof['proof_digest'] = (
            DailyPipelineStageService
            ._stage_completion_scope_proof_digest(proof)
        )
        return proof

    @staticmethod
    def validated_stage_completion_scope_proof(
        stage_id: str,
        proof: object,
    ) -> dict[str, Any] | None:
        if not isinstance(proof, Mapping):
            return None
        proof = dict(proof)
        if not (
            proof.get('schema_version')
            == STAGE_COMPLETION_SCOPE_PROOF_SCHEMA_VERSION
            and str(proof.get('proof_digest') or '')
            == DailyPipelineStageService
            ._stage_completion_scope_proof_digest(proof)
        ):
            return None

        if stage_id == 'signal_decider':
            expected_keys = {
                'schema_version',
                'kind',
                'roles',
                'source_run_id',
                'source_summary_run_id',
                'knowledge_cutoff_at',
                'admitted_symbols',
                'completed_symbols',
                'failed_symbols_by_role',
                'decision_identities',
                'total_items',
                'completed_items',
                'failed_items',
                'proof_digest',
            }
            roles = proof.get('roles')
            admitted_symbols = proof.get('admitted_symbols')
            completed_symbols = proof.get('completed_symbols')
            failed_by_role = proof.get('failed_symbols_by_role')
            decision_identities = proof.get('decision_identities')
            try:
                knowledge_cutoff_at = datetime.fromisoformat(
                    str(proof.get('knowledge_cutoff_at') or '').strip()
                )
            except (TypeError, ValueError):
                return None
            if not (
                set(proof) == expected_keys
                and proof.get('kind')
                == 'signal_decider_terminal_progression'
                and roles == list(DECISION_SUPPORT_AGENT_NAMES)
                and str(proof.get('source_run_id') or '').strip()
                and str(
                    proof.get('source_summary_run_id') or ''
                ).strip()
                and knowledge_cutoff_at.tzinfo is not None
                and knowledge_cutoff_at.utcoffset() is not None
                and proof.get('knowledge_cutoff_at')
                == knowledge_cutoff_at.astimezone(timezone.utc).isoformat()
                and isinstance(admitted_symbols, list)
                and admitted_symbols
                and normalize_symbol_codes(admitted_symbols)
                == admitted_symbols
                and sorted(set(admitted_symbols)) == admitted_symbols
                and isinstance(completed_symbols, list)
                and completed_symbols
                and normalize_symbol_codes(completed_symbols)
                == completed_symbols
                and sorted(set(completed_symbols)) == completed_symbols
                and set(completed_symbols) <= set(admitted_symbols)
                and isinstance(failed_by_role, Mapping)
                and set(failed_by_role)
                == set(DECISION_SUPPORT_AGENT_NAMES)
                and isinstance(decision_identities, list)
            ):
                return None
            total_items = proof.get('total_items')
            completed_items = proof.get('completed_items')
            failed_items = proof.get('failed_items')
            canonical_failed_by_role: dict[str, list[str]] = {}
            for role in DECISION_SUPPORT_AGENT_NAMES:
                failed_symbols = failed_by_role.get(role)
                if not (
                    isinstance(failed_symbols, list)
                    and normalize_symbol_codes(failed_symbols)
                    == failed_symbols
                    and sorted(set(failed_symbols)) == failed_symbols
                    and set(failed_symbols) <= set(admitted_symbols)
                ):
                    return None
                canonical_failed_by_role[role] = list(failed_symbols)
            canonical_identities: list[dict[str, Any]] = []
            seen_identity_keys: set[tuple[str, str]] = set()
            for raw_identity in decision_identities:
                if not isinstance(raw_identity, Mapping):
                    return None
                identity = dict(raw_identity)
                if set(identity) != {
                    'symbol',
                    'decision_agent_name',
                    'decision_snapshot_id',
                    'decision_run_id',
                }:
                    return None
                normalized_symbol = normalize_symbol_codes(
                    [identity.get('symbol')]
                )
                role = str(
                    identity.get('decision_agent_name') or ''
                ).strip()
                snapshot_id = identity.get('decision_snapshot_id')
                decision_run_id = str(
                    identity.get('decision_run_id') or ''
                ).strip()
                key = (
                    normalized_symbol[0]
                    if normalized_symbol
                    else '',
                    role,
                )
                if not (
                    len(normalized_symbol) == 1
                    and identity.get('symbol')
                    == normalized_symbol[0]
                    and normalized_symbol[0] in completed_symbols
                    and role in DECISION_SUPPORT_AGENT_NAMES
                    and type(snapshot_id) is int
                    and snapshot_id > 0
                    and decision_run_id
                    and key not in seen_identity_keys
                ):
                    return None
                seen_identity_keys.add(key)
                canonical_identities.append(
                    {
                        'symbol': normalized_symbol[0],
                        'decision_agent_name': role,
                        'decision_snapshot_id': snapshot_id,
                        'decision_run_id': decision_run_id,
                    }
                )
            canonical_identities.sort(
                key=lambda identity: (
                    identity['symbol'],
                    identity['decision_agent_name'],
                )
            )
            expected_identity_keys = {
                (symbol, role)
                for symbol in completed_symbols
                for role in DECISION_SUPPORT_AGENT_NAMES
            }
            if not (
                type(total_items) is int
                and total_items == (
                    len(admitted_symbols)
                    * len(DECISION_SUPPORT_AGENT_NAMES)
                )
                and type(completed_items) is int
                and type(failed_items) is int
                and completed_items + failed_items == total_items
                and failed_items
                == sum(
                    len(symbols)
                    for symbols in canonical_failed_by_role.values()
                )
                and set(completed_symbols)
                == set(admitted_symbols).difference(
                    {
                        symbol
                        for symbols in canonical_failed_by_role.values()
                        for symbol in symbols
                    }
                )
                and seen_identity_keys == expected_identity_keys
                and decision_identities == canonical_identities
            ):
                return None
            return proof

        if stage_id == 'agent_feedback_summaries':
            expected_keys = {
                'schema_version',
                'kind',
                'source_run_id',
                'knowledge_cutoff_at',
                'feedback_scope_mode',
                'roles',
                'requested_scope_kind',
                'requested_symbols',
                'scope_symbols',
                'role_symbol_scopes',
                'role_symbol_counts',
                'role_scope_manifest_fingerprint',
                'total_item_count',
                'proof_digest',
            }
            roles = proof.get('roles')
            role_scopes = proof.get('role_symbol_scopes')
            role_counts = proof.get('role_symbol_counts')
            requested_symbols = proof.get('requested_symbols')
            scope_symbols = proof.get('scope_symbols')
            try:
                knowledge_cutoff_at = datetime.fromisoformat(
                    str(proof.get('knowledge_cutoff_at') or '').strip()
                )
            except (TypeError, ValueError):
                return None
            if not (
                set(proof) == expected_keys
                and proof.get('kind')
                == 'agent_feedback_summaries_role_scope'
                and proof.get('feedback_scope_mode')
                in {
                    FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
                    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
                }
                and str(proof.get('source_run_id') or '').strip()
                and knowledge_cutoff_at.tzinfo is not None
                and knowledge_cutoff_at.utcoffset() is not None
                and proof.get('knowledge_cutoff_at')
                == knowledge_cutoff_at.astimezone(timezone.utc).isoformat()
                and roles == list(SUMMARIZER_AGENT_NAMES)
                and proof.get('requested_scope_kind')
                in {
                    'explicit_invocation',
                    'resolved_role_scope_union',
                }
                and isinstance(role_scopes, Mapping)
                and set(role_scopes) == set(SUMMARIZER_AGENT_NAMES)
                and isinstance(role_counts, Mapping)
                and set(role_counts) == set(SUMMARIZER_AGENT_NAMES)
                and isinstance(requested_symbols, list)
                and requested_symbols
                and normalize_symbol_codes(requested_symbols)
                == requested_symbols
                and isinstance(scope_symbols, list)
                and scope_symbols
                and normalize_symbol_codes(scope_symbols) == scope_symbols
                and set(scope_symbols) <= set(requested_symbols)
            ):
                return None
            union_symbols: set[str] = set()
            total_item_count = 0
            for role in SUMMARIZER_AGENT_NAMES:
                role_scope = role_scopes.get(role)
                role_count = role_counts.get(role)
                if not (
                    isinstance(role_scope, list)
                    and normalize_symbol_codes(role_scope) == role_scope
                    and type(role_count) is int
                    and role_count == len(role_scope)
                ):
                    return None
                union_symbols.update(role_scope)
                total_item_count += role_count
            if not (
                sorted(union_symbols) == scope_symbols
                and type(proof.get('total_item_count')) is int
                and proof.get('total_item_count') == total_item_count
                and total_item_count > 0
                and proof.get('role_scope_manifest_fingerprint')
                == DailyPipelineStageService
                ._stage_completion_scope_proof_digest(
                    {
                        'requested_symbols': requested_symbols,
                        'role_symbol_scopes': dict(role_scopes),
                    }
                )
            ):
                return None
            return proof

        if stage_id == 'next_open_execution_plan':
            expected_keys = {
                'schema_version',
                'kind',
                'parent_run_id',
                'parent_summary_run_id',
                'parent_admitted_symbols',
                'parent_completed_symbols',
                'authority_fingerprint',
                'generation_fingerprint',
                'role_symbol_scopes',
                'decision_partition',
                'decision_count',
                'included_decision_count',
                'excluded_decision_count',
                'item_count',
                'active_item_count',
                'blocked_item_count',
                'manifest_digest',
                'proof_digest',
            }
            admitted_symbols = proof.get('parent_admitted_symbols')
            completed_symbols = proof.get('parent_completed_symbols')
            role_symbol_scopes = proof.get('role_symbol_scopes')
            partition = proof.get('decision_partition')
            decision_count = proof.get('decision_count')
            included_decision_count = proof.get(
                'included_decision_count'
            )
            excluded_decision_count = proof.get(
                'excluded_decision_count'
            )
            item_count = proof.get('item_count')
            active_item_count = proof.get('active_item_count')
            blocked_item_count = proof.get('blocked_item_count')
            if not (
                set(proof) == expected_keys
                and proof.get('kind')
                == 'next_open_execution_plan_parent_scope'
                and str(proof.get('parent_run_id') or '').strip()
                and str(
                    proof.get('parent_summary_run_id') or ''
                ).strip()
                and isinstance(admitted_symbols, list)
                and admitted_symbols
                and normalize_symbol_codes(admitted_symbols)
                == admitted_symbols
                and isinstance(completed_symbols, list)
                and completed_symbols
                and normalize_symbol_codes(completed_symbols)
                == completed_symbols
                and set(completed_symbols) <= set(admitted_symbols)
                and str(
                    proof.get('authority_fingerprint') or ''
                ).strip()
                and str(
                    proof.get('generation_fingerprint') or ''
                ).strip()
                and isinstance(role_symbol_scopes, Mapping)
                and set(role_symbol_scopes)
                == set(DECISION_SUPPORT_AGENT_NAMES)
                and all(
                    role_symbol_scopes.get(role) == completed_symbols
                    for role in DECISION_SUPPORT_AGENT_NAMES
                )
                and isinstance(partition, list)
                and all(
                    type(value) is int and value >= 0
                    for value in (
                        decision_count,
                        included_decision_count,
                        excluded_decision_count,
                        item_count,
                        active_item_count,
                        blocked_item_count,
                    )
                )
                and decision_count
                == len(completed_symbols)
                * len(DECISION_SUPPORT_AGENT_NAMES)
                and len(partition) == decision_count
                and included_decision_count
                + excluded_decision_count
                == decision_count
                and active_item_count + blocked_item_count == item_count
                and len(str(proof.get('manifest_digest') or '')) == 64
            ):
                return None
            canonical_partition: list[dict[str, Any]] = []
            seen_decisions: set[tuple[str, str]] = set()
            seen_plan_items: set[tuple[int, str]] = set()
            actual_included_decisions = 0
            actual_excluded_decisions = 0
            actual_active_items = 0
            for entry in partition:
                if not (
                    isinstance(entry, Mapping)
                    and set(entry)
                    == {
                        'symbol',
                        'decision_agent_name',
                        'decision_run_id',
                        'decision_snapshot_id',
                        'canonical_action',
                        'authority_fingerprint',
                        'generation_fingerprint',
                        'disposition',
                        'included_items',
                        'exclusion',
                    }
                ):
                    return None
                normalized_symbol = normalize_symbol_codes(
                    [entry.get('symbol')]
                )
                role = str(
                    entry.get('decision_agent_name') or ''
                ).strip()
                canonical_action = str(
                    entry.get('canonical_action') or ''
                ).strip().lower()
                disposition = str(
                    entry.get('disposition') or ''
                ).strip().lower()
                included_items = entry.get('included_items')
                exclusion = entry.get('exclusion')
                decision_key = (
                    normalized_symbol[0] if normalized_symbol else '',
                    role,
                )
                actionable = bool(
                    (
                        role == 'signal-decider'
                        and canonical_action == 'buy'
                    )
                    or (
                        role == 'short-term-decider'
                        and canonical_action
                        in {
                            'scalp_buy_candidate',
                            'watch_for_trigger',
                        }
                    )
                )
                if not (
                    len(normalized_symbol) == 1
                    and normalized_symbol[0] in completed_symbols
                    and role in DECISION_SUPPORT_AGENT_NAMES
                    and decision_key not in seen_decisions
                    and str(
                        entry.get('decision_run_id') or ''
                    ).strip()
                    and type(entry.get('decision_snapshot_id')) is int
                    and entry.get('decision_snapshot_id') > 0
                    and canonical_action
                    and str(
                        entry.get('authority_fingerprint') or ''
                    ).strip()
                    and str(
                        entry.get('generation_fingerprint') or ''
                    ).strip()
                    and disposition in {
                        'included',
                        'non_actionable',
                    }
                    and isinstance(included_items, list)
                ):
                    return None
                seen_decisions.add(decision_key)
                canonical_items: list[dict[str, Any]] = []
                if disposition == 'included':
                    if not (
                        actionable
                        and included_items
                        and exclusion is None
                    ):
                        return None
                    actual_included_decisions += 1
                    for item in included_items:
                        if not (
                            isinstance(item, Mapping)
                            and set(item)
                            == {
                                'plan_item_id',
                                'account_reference',
                                'eligibility_status',
                                'handoff_active',
                                'exec_monitoring_active',
                                'row_authority_fingerprint',
                            }
                        ):
                            return None
                        plan_item_id = item.get('plan_item_id')
                        account_reference = str(
                            item.get('account_reference') or ''
                        ).strip()
                        item_identity = (
                            plan_item_id,
                            account_reference,
                        )
                        if not (
                            type(plan_item_id) is int
                            and plan_item_id > 0
                            and account_reference
                            and item_identity not in seen_plan_items
                            and str(
                                item.get('eligibility_status') or ''
                            ).strip()
                            and type(item.get('handoff_active')) is bool
                            and type(
                                item.get('exec_monitoring_active')
                            ) is bool
                            and str(
                                item.get('row_authority_fingerprint')
                                or ''
                            ).strip()
                        ):
                            return None
                        seen_plan_items.add(item_identity)
                        actual_active_items += int(
                            item['handoff_active']
                            or item['exec_monitoring_active']
                        )
                        canonical_items.append(dict(item))
                    canonical_items.sort(
                        key=lambda item: (
                            item['account_reference'],
                            item['plan_item_id'],
                        )
                    )
                    if canonical_items != included_items:
                        return None
                    canonical_exclusion = None
                else:
                    if not (
                        not actionable
                        and not included_items
                        and isinstance(exclusion, Mapping)
                        and dict(exclusion)
                        == {
                            'kind': 'non_actionable',
                            'reason_code': (
                                'latest_snapshot_not_actionable'
                            ),
                        }
                    ):
                        return None
                    actual_excluded_decisions += 1
                    canonical_exclusion = dict(exclusion)
                canonical_partition.append(
                    {
                        **{
                            key: entry[key]
                            for key in (
                                'symbol',
                                'decision_agent_name',
                                'decision_run_id',
                                'decision_snapshot_id',
                                'canonical_action',
                                'authority_fingerprint',
                                'generation_fingerprint',
                                'disposition',
                            )
                        },
                        'included_items': canonical_items,
                        'exclusion': canonical_exclusion,
                    }
                )
            canonical_partition.sort(
                key=lambda entry: (
                    entry['symbol'],
                    entry['decision_agent_name'],
                )
            )
            if not (
                seen_decisions
                == {
                    (symbol, role)
                    for role in DECISION_SUPPORT_AGENT_NAMES
                    for symbol in completed_symbols
                }
                and canonical_partition == partition
                and actual_included_decisions
                == included_decision_count
                and actual_excluded_decisions
                == excluded_decision_count
                and len(seen_plan_items) == item_count
                and actual_active_items == active_item_count
            ):
                return None
            return proof
        if stage_id == 'signal_decider_evaluations':
            expected_keys = {
                'schema_version',
                'kind',
                'parent_run_id',
                'parent_summary_run_id',
                'parent_completed_symbols',
                'request_symbols',
                'horizons',
                'evaluation_version',
                'evaluation_rule_set_version',
                'decision_horizon_scope',
                'decision_count',
                'item_count',
                'proof_digest',
            }
            parent_symbols = proof.get('parent_completed_symbols')
            request_symbols = proof.get('request_symbols')
            horizons = proof.get('horizons')
            scope = proof.get('decision_horizon_scope')
            decision_count = proof.get('decision_count')
            item_count = proof.get('item_count')
            evaluation_version = str(
                proof.get('evaluation_version') or ''
            ).strip()
            evaluation_rule_set_version = str(
                proof.get('evaluation_rule_set_version') or ''
            ).strip()
            if not (
                set(proof) == expected_keys
                and proof.get('kind')
                == 'signal_decider_evaluations_parent_scope'
                and str(proof.get('parent_run_id') or '').strip()
                and str(
                    proof.get('parent_summary_run_id') or ''
                ).strip()
                and isinstance(parent_symbols, list)
                and parent_symbols
                and normalize_symbol_codes(parent_symbols)
                == parent_symbols
                and request_symbols == parent_symbols
                and isinstance(horizons, list)
                and horizons
                and all(
                    type(horizon) is int and horizon > 0
                    for horizon in horizons
                )
                and sorted(set(horizons)) == horizons
                and evaluation_version
                and evaluation_rule_set_version
                and isinstance(scope, list)
                and type(decision_count) is int
                and decision_count > 0
                and type(item_count) is int
                and item_count == decision_count * len(horizons)
                and len(scope) == item_count
            ):
                return None
            canonical_scope: list[dict[str, Any]] = []
            for entry in scope:
                if not (
                    isinstance(entry, Mapping)
                    and set(entry)
                    == {
                        'snapshot_id',
                        'feedback_id',
                        'decision_run_id',
                        'decision_agent_name',
                        'symbol',
                        'market_date',
                        'decision_action',
                        'policy_version',
                        'horizon_days',
                        'outcome_id',
                        'evaluation_id',
                        'evaluation_version',
                        'evaluation_rule_set_version',
                        'evaluation_status',
                    }
                    and type(entry.get('snapshot_id')) is int
                    and entry.get('snapshot_id') > 0
                    and type(entry.get('feedback_id')) is int
                    and entry.get('feedback_id') > 0
                    and str(
                        entry.get('decision_run_id') or ''
                    ).strip()
                    and entry.get('decision_agent_name')
                    == 'signal-decider'
                    and entry.get('symbol') in parent_symbols
                    and str(entry.get('market_date') or '').strip()
                    and str(
                        entry.get('decision_action') or ''
                    ).strip()
                    and str(
                        entry.get('policy_version') or ''
                    ).strip()
                    and entry.get('horizon_days') in horizons
                    and type(entry.get('outcome_id')) is int
                    and entry.get('outcome_id') > 0
                    and type(entry.get('evaluation_id')) is int
                    and entry.get('evaluation_id') > 0
                    and str(
                        entry.get('evaluation_version') or ''
                    ).strip() == evaluation_version
                    and str(
                        entry.get('evaluation_rule_set_version') or ''
                    ).strip() == evaluation_rule_set_version
                    and str(
                        entry.get('evaluation_status') or ''
                    ).strip()
                ):
                    return None
                canonical_scope.append(dict(entry))
            canonical_scope.sort(
                key=lambda entry: (
                    entry['market_date'],
                    entry['symbol'],
                    entry['decision_run_id'],
                    entry['snapshot_id'],
                    entry['horizon_days'],
                )
            )
            if canonical_scope != scope:
                return None
            identity_keys = {
                (
                    entry['snapshot_id'],
                    entry['horizon_days'],
                )
                for entry in scope
            }
            if len(identity_keys) != item_count:
                return None
            return proof
        return None

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
    def _build_stage_run_details(
        *,
        stage_id: str,
        result: dict[str, Any],
        invocation_symbols: Sequence[str] | None = None,
        completion_parent_proof: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        definition = STAGE_DEFINITIONS[stage_id]
        result_summary, result_summary_omissions = (
            DailyPipelineStageService.compact_stage_result_summary(result)
        )
        details = {
            'group': definition.group,
            'depends_on': list(definition.depends_on),
            'output_tables': list(definition.output_tables),
            'inspect_routes': list(definition.inspect_routes),
            'terminal_count_authority': 'result',
            'result_summary': result_summary,
        }
        if result_summary_omissions:
            details['result_summary_omissions'] = result_summary_omissions
        exact_scope_outcome_proof = (
            DailyPipelineStageService._build_exact_scope_outcome_proof(
                result,
                invocation_symbols=invocation_symbols,
            )
        )
        if exact_scope_outcome_proof is not None:
            details['exact_scope_outcome_proof'] = (
                exact_scope_outcome_proof
            )
        nonblocking_scope_proof = (
            build_nonblocking_downstream_scope_proof(stage_id, result)
        )
        if nonblocking_scope_proof is not None:
            details['nonblocking_scope_proof'] = nonblocking_scope_proof
        completion_scope_proof = (
            DailyPipelineStageService
            ._build_feedback_completion_scope_proof(
                result,
                invocation_symbols=invocation_symbols,
            )
            if stage_id == 'agent_feedback_summaries'
            else DailyPipelineStageService
            .validated_stage_completion_scope_proof(
                'signal_decider',
                completion_parent_proof,
            )
            if stage_id == 'signal_decider'
            else DailyPipelineStageService
            ._build_next_open_completion_scope_proof(
                result,
                parent_scope_proof=completion_parent_proof,
            )
            if stage_id == 'next_open_execution_plan'
            else DailyPipelineStageService
            ._build_evaluations_completion_scope_proof(
                result,
                parent_scope_proof=completion_parent_proof,
            )
            if stage_id == 'signal_decider_evaluations'
            else None
        )
        if completion_scope_proof is not None:
            details['completion_scope_proof'] = (
                completion_scope_proof
            )
        count_semantics = DailyPipelineStageService._extract_stage_count_semantics(
            stage_id=stage_id,
            result=result,
        )
        if count_semantics:
            details['count_semantics'] = count_semantics
        return details

    @staticmethod
    def _execute_exact_scoped_derived_stage(
        *,
        stage_id: str,
        market_date: str,
        exact_scope_symbols: Sequence[str] | None,
        call: Callable[[], dict[str, Any]],
        atomic_finalization: bool = False,
    ) -> dict[str, Any]:
        try:
            result = call()
        except Exception:
            if exact_scope_symbols is not None and not atomic_finalization:
                DailyPipelineStageService._finalize_exact_scope_or_raise(
                    market_date=market_date,
                    stage_id=stage_id,
                    exact_symbols=exact_scope_symbols,
                    result=None,
                )
            raise
        if atomic_finalization:
            finalization = result.get('scope_finalization')
            if exact_scope_symbols is not None and not (
                isinstance(finalization, Mapping)
                and finalization.get('ready') is True
            ):
                raise DailyPipelineScopeFinalizationError(
                    f'{stage_id} atomic exact-scope finalization proof '
                    'is unavailable'
                )
            return result
        if exact_scope_symbols is not None:
            result['scope_finalization'] = (
                DailyPipelineStageService._finalize_exact_scope_or_raise(
                    market_date=market_date,
                    stage_id=stage_id,
                    exact_symbols=exact_scope_symbols,
                    result=result,
                )
            )
        return result

    @staticmethod
    def _finalize_exact_scope_or_raise(
        *,
        market_date: str,
        stage_id: str,
        exact_symbols: Sequence[str],
        result: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        try:
            return _finalize_daily_stage_scope(
                market_date=market_date,
                stage_id=stage_id,
                exact_symbols=exact_symbols,
                result=result,
            )
        except Exception as exc:
            raise DailyPipelineScopeFinalizationError(
                f'{stage_id} exact-scope finalization failed: {exc}'
            ) from exc

    @staticmethod
    def _execute_optional_context_stage(
        *,
        stage_id: str,
        call: Callable[[], dict[str, Any]],
    ) -> dict[str, Any]:
        del stage_id
        return call()

    @staticmethod
    def _combine_technical_scope_outcomes(
        *,
        price_context_result: Mapping[str, Any],
        technical_result: Mapping[str, Any],
        invocation_symbols: Sequence[str] | None,
    ) -> dict[str, Any]:
        """Return only a proven context-and-snapshot success partition."""

        combined_result = dict(technical_result)
        exact_scope_keys = (
            'scope_outcome_manifest_version',
            'selected_stock_codes',
            'successful_stock_codes',
            'retry_debt_stock_codes',
            'terminal_not_applicable_stock_codes',
            'scope_outcome_manifest',
        )
        for key in exact_scope_keys:
            combined_result.pop(key, None)

        def fail_closed_scope_contract(
            *,
            reason: str,
            retry_symbols: Sequence[str],
            retry_count: int | None = None,
        ) -> dict[str, Any]:
            normalized_retry_symbols = normalize_symbol_codes(
                retry_symbols
            )
            resolved_retry_count = max(
                int(retry_count or 0),
                len(normalized_retry_symbols),
            )
            raw_status = str(
                combined_result.get('status') or ''
            ).strip().lower()
            if raw_status not in {
                'blocked',
                'failed',
                'interrupted',
                'waiting',
            }:
                combined_result['status'] = 'partial'
            combined_result.update(
                {
                    'reason': (
                        'technical_scope_outcome_contract_invalid'
                    ),
                    'gate_status': 'blocked',
                    'blocking_status': 'blocking',
                    'scope_outcome_contract_status': 'invalid',
                    'scope_outcome_contract_reason': reason,
                    'retryable_symbols': normalized_retry_symbols,
                    'retryable_symbol_count': resolved_retry_count,
                    'remaining_retryable_events': resolved_retry_count,
                    'retry_required': True,
                }
            )
            retry_metadata = (
                dict(combined_result['_retry_metadata'])
                if isinstance(
                    combined_result.get('_retry_metadata'),
                    Mapping,
                )
                else {}
            )
            retry_metadata.update(
                {
                    'failed_symbols': normalized_retry_symbols,
                    'failed_symbols_count': resolved_retry_count,
                    'retry_required': True,
                }
            )
            combined_result['_retry_metadata'] = retry_metadata
            return combined_result

        def reported_invocation_count(
            result: Mapping[str, Any],
        ) -> int:
            value = result.get('requested_symbols')
            if isinstance(value, bool):
                return 0
            try:
                return max(int(value or 0), 0)
            except (TypeError, ValueError):
                return 0

        if invocation_symbols is None:
            context_selected = price_context_result.get(
                'selected_stock_codes'
            )
            snapshot_selected = technical_result.get(
                'selected_stock_codes'
            )
            if not (
                isinstance(context_selected, list)
                and isinstance(snapshot_selected, list)
                and set(normalize_symbol_codes(context_selected))
                == set(normalize_symbol_codes(snapshot_selected))
            ):
                candidate_symbols = normalize_symbol_codes(
                    [
                        *(
                            context_selected
                            if isinstance(context_selected, list)
                            else []
                        ),
                        *(
                            snapshot_selected
                            if isinstance(snapshot_selected, list)
                            else []
                        ),
                    ]
                )
                return fail_closed_scope_contract(
                    reason='invocation_scope_identity_unavailable',
                    retry_symbols=candidate_symbols,
                    retry_count=max(
                        reported_invocation_count(
                            price_context_result
                        ),
                        reported_invocation_count(technical_result),
                    ),
                )
            expected_symbols: Sequence[str] = context_selected
        else:
            expected_symbols = normalize_symbol_codes(
                invocation_symbols
            )

        expected_symbol_count = len(expected_symbols)
        for child_name, child_result in (
            ('price_context', price_context_result),
            ('technical_snapshot', technical_result),
        ):
            raw_requested_count = child_result.get(
                'requested_symbols'
            )
            if raw_requested_count is None:
                continue
            reported_count = (
                raw_requested_count
                if type(raw_requested_count) is int
                and raw_requested_count >= 0
                else 0
            )
            if (
                type(raw_requested_count) is not int
                or raw_requested_count < 0
                or reported_count != expected_symbol_count
            ):
                return fail_closed_scope_contract(
                    reason=(
                        f'{child_name}_requested_scope_count_mismatch'
                    ),
                    retry_symbols=expected_symbols,
                    retry_count=max(
                        expected_symbol_count,
                        reported_count,
                    ),
                )
        if invocation_symbols is None and expected_symbol_count == 0:
            return fail_closed_scope_contract(
                reason='derived_empty_scope_not_authoritative',
                retry_symbols=[],
                retry_count=0,
            )

        context_scope = _validated_exact_scope_outcome(
            result=price_context_result,
            invocation_symbols=expected_symbols,
            status=DailyPipelineStageService._resolve_result_status(
                dict(price_context_result)
            ),
        )
        snapshot_scope = _validated_exact_scope_outcome(
            result=technical_result,
            invocation_symbols=expected_symbols,
            status=DailyPipelineStageService._resolve_result_status(
                dict(technical_result)
            ),
        )
        if context_scope is None:
            return fail_closed_scope_contract(
                reason='price_context_scope_outcome_invalid',
                retry_symbols=expected_symbols,
            )
        if snapshot_scope is None:
            return fail_closed_scope_contract(
                reason='technical_snapshot_scope_outcome_invalid',
                retry_symbols=expected_symbols,
            )
        if (
            context_scope['manifest_version']
            != 'price_contexts_scope_outcomes_v1'
        ):
            return fail_closed_scope_contract(
                reason='price_context_scope_manifest_version_invalid',
                retry_symbols=expected_symbols,
            )
        if (
            snapshot_scope['manifest_version']
            != 'technical_snapshots_scope_outcomes_v1'
        ):
            return fail_closed_scope_contract(
                reason=(
                    'technical_snapshot_scope_manifest_version_invalid'
                ),
                retry_symbols=expected_symbols,
            )

        def partition_matches_counts(
            *,
            scope: Mapping[str, Any],
            result: Mapping[str, Any],
            persisted_key: str,
            failed_key: str,
        ) -> bool:
            persisted = result.get(persisted_key)
            failed = result.get(failed_key)
            if (
                isinstance(persisted, bool)
                or isinstance(failed, bool)
            ):
                return False
            try:
                persisted_count = int(persisted)
                failed_count = int(failed)
            except (TypeError, ValueError):
                return False
            successful_count = len(
                scope['successful_stock_codes']
            )
            unsuccessful_count = len(
                scope['retry_debt_stock_codes']
            ) + len(scope['terminal_not_applicable_stock_codes'])
            return (
                persisted_count >= 0
                and failed_count >= 0
                and persisted_count == successful_count
                and failed_count == unsuccessful_count
                and persisted_count + failed_count
                == len(scope['selected_stock_codes'])
            )

        if not partition_matches_counts(
            scope=context_scope,
            result=price_context_result,
            persisted_key='contexts_persisted',
            failed_key='contexts_failed',
        ):
            return fail_closed_scope_contract(
                reason='price_context_scope_count_mismatch',
                retry_symbols=expected_symbols,
            )
        if not partition_matches_counts(
            scope=snapshot_scope,
            result=technical_result,
            persisted_key='snapshots_persisted',
            failed_key='snapshots_failed',
        ):
            return fail_closed_scope_contract(
                reason='technical_snapshot_scope_count_mismatch',
                retry_symbols=expected_symbols,
            )

        selected_symbols = context_scope['selected_stock_codes']
        if set(selected_symbols) != set(
            snapshot_scope['selected_stock_codes']
        ):
            return fail_closed_scope_contract(
                reason='child_scope_identity_mismatch',
                retry_symbols=expected_symbols,
            )
        context_successes = set(
            context_scope['successful_stock_codes']
        )
        snapshot_successes = set(
            snapshot_scope['successful_stock_codes']
        )
        successful_symbols = [
            symbol
            for symbol in selected_symbols
            if symbol in context_successes
            and symbol in snapshot_successes
        ]
        successful_set = set(successful_symbols)
        retry_debt_symbols = [
            symbol
            for symbol in selected_symbols
            if symbol not in successful_set
        ]
        combined_result.update(
            build_exact_symbol_scope_outcome(
                manifest_version=(
                    'technical_snapshots_scope_outcomes_v1'
                ),
                selected_symbols=selected_symbols,
                successful_symbols=successful_symbols,
                retry_debt_symbols=retry_debt_symbols,
            )
        )
        combined_result['snapshots_persisted'] = len(successful_symbols)
        combined_result['snapshots_failed'] = len(retry_debt_symbols)
        combined_result['failed_symbols'] = retry_debt_symbols[:50]
        combined_result['items'] = [
            item
            for item in (combined_result.get('items') or [])
            if isinstance(item, Mapping)
            and str(item.get('symbol') or '').strip() in successful_set
        ]
        combined_result['_retry_metadata'] = {
            'failed_symbols': list(retry_debt_symbols),
        }
        if retry_debt_symbols:
            combined_result['status'] = 'partial'
        return combined_result

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
        feedback_reuse_parent_run_id: str | None = None,
        feedback_retry_config_override: Mapping[str, Any] | None = None,
        feedback_cutoff_resolution: dict[str, Any] | None = None,
        completion_parent_proof: Mapping[str, Any] | None = None,
        point_in_time_recovery: bool = False,
        allow_same_day_intraday_watch_continuation: bool = False,
        allow_previous_session_preopen_recovery: bool = False,
        provider_admission_fence_owned: bool = False,
        run_id: int | None = None,
        progress_callback: Callable[..., Any] | None = None,
        allow_expired_analysis_replay: bool = False,
        exact_scope_symbols: list[str] | None = None,
        complete_price_scope_proven: bool = False,
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
        if (
            stage_id in EXPIRED_DECISION_REPLAY_STAGE_IDS
            and not allow_expired_analysis_replay
        ):
            expired_decision_result = build_expired_decision_replay_result(
                stage_id=stage_id,
                market_date=market_date,
                market=get_market_state(),
                allow_same_day_intraday_watch_continuation=bool(
                    allow_same_day_intraday_watch_continuation
                ),
                allow_previous_session_preopen_recovery=bool(
                    allow_previous_session_preopen_recovery
                ),
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

            coverage_payload = (
                result.get('coverage')
                if isinstance(result.get('coverage'), dict)
                else {}
            )
            coverage_categories = (
                coverage_payload.get('categories')
                if isinstance(coverage_payload.get('categories'), dict)
                else {}
            )
            raw_requested_symbols = (
                symbols
                if symbols is not None
                else result.get('symbols_requested')
            )
            exact_scope_symbols = (
                normalize_symbol_codes(raw_requested_symbols)
                if isinstance(raw_requested_symbols, (list, tuple))
                else []
            )
            if exact_scope_symbols:
                (
                    exact_scope_outcome,
                    exact_scope_retry_reasons,
                ) = _build_pykrx_enrichment_symbol_scope_outcome(
                    selected_symbols=exact_scope_symbols,
                    coverage_categories=coverage_categories,
                )
                result.update(exact_scope_outcome)
                result['exact_scope_retry_reasons'] = (
                    exact_scope_retry_reasons
                )
                exact_retry_symbols = list(
                    exact_scope_outcome['retry_debt_stock_codes']
                )
                if exact_retry_symbols:
                    if str(result.get('status') or '').strip().lower() not in {
                        'failed',
                        'blocked',
                        'waiting',
                    }:
                        result['status'] = 'partial'
                    retry_metadata = (
                        dict(result.get('_retry_metadata'))
                        if isinstance(result.get('_retry_metadata'), dict)
                        else {}
                    )
                    retry_metadata['failed_symbols'] = exact_retry_symbols
                    result['_retry_metadata'] = retry_metadata
            elif str(result.get('status') or '').strip().lower() not in {
                'failed',
                'blocked',
                'waiting',
            }:
                # Exact downstream narrowing requires a durable invocation
                # identity.  Aggregate category readiness cannot prove which
                # symbols are safe to advance.
                result['status'] = 'partial'
                result['exact_scope_status'] = 'unavailable'
                result['exact_scope_retry_reasons'] = {
                    '__scope__': ['missing_exact_invocation_symbols']
                }
            return result
        if stage_id == 'technical_snapshots':
            price_context_result = self._execute_exact_scoped_derived_stage(
                stage_id='price_contexts',
                market_date=market_date,
                exact_scope_symbols=exact_scope_symbols,
                call=lambda: daily_price_context_service.rebuild_for_market_date(
                    market_date=market_date,
                    symbols=symbols,
                    exact_scope_symbols=exact_scope_symbols,
                    source='stage_orchestrator',
                    run_id=run_id,
                ),
                atomic_finalization=True,
            )
            price_scope = _validated_exact_scope_outcome(
                result=price_context_result,
                invocation_symbols=exact_scope_symbols or symbols,
                status=self._resolve_result_status(price_context_result),
            )
            price_context_successes = (
                list(price_scope['successful_stock_codes'])
                if price_scope is not None
                and price_scope['manifest_version']
                == 'price_contexts_scope_outcomes_v1'
                else []
            )
            technical_result = self._execute_exact_scoped_derived_stage(
                stage_id='technical_snapshots',
                market_date=market_date,
                exact_scope_symbols=exact_scope_symbols,
                call=lambda: daily_technical_snapshot_service.rebuild_for_market_date(
                    market_date=market_date,
                    symbols=price_context_successes,
                    exact_scope_symbols=exact_scope_symbols,
                    source='stage_orchestrator',
                    run_id=run_id,
                ),
                atomic_finalization=True,
            )
            combined_technical_result = (
                self._combine_technical_scope_outcomes(
                    price_context_result=price_context_result,
                    technical_result=technical_result,
                    invocation_symbols=exact_scope_symbols or symbols,
                )
            )
            return {
                **combined_technical_result,
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
            return self._execute_exact_scoped_derived_stage(
                stage_id=stage_id,
                market_date=market_date,
                exact_scope_symbols=exact_scope_symbols,
                call=lambda: daily_action_summary_service.rebuild_for_market_date(
                    market_date=market_date,
                    symbols=symbols,
                    exact_scope_symbols=exact_scope_symbols,
                    source='stage_orchestrator',
                    run_id=run_id,
                ),
                atomic_finalization=True,
            )
        if stage_id == 'trending_ranks':
            normalized_exact_scope = normalize_symbol_codes(
                exact_scope_symbols or []
            )
            normalized_admitted_scope = normalize_symbol_codes(
                symbols or []
            )
            exact_scope_admitted = bool(
                normalized_exact_scope
                and normalized_admitted_scope
                and len(normalized_exact_scope)
                == len(normalized_admitted_scope)
                and set(normalized_exact_scope)
                == set(normalized_admitted_scope)
            )
            authoritative_scope_ready = bool(
                exact_scope_admitted
                and complete_price_scope_proven
                and {'KOSPI', 'KOSDAQ'}.issubset(set(markets))
            )
            if not authoritative_scope_ready:
                retry_debt_scope = (
                    normalized_exact_scope
                    or normalized_admitted_scope
                )
                blocked_result: dict[str, Any] = {
                    'status': 'blocked',
                    'reason': (
                        'authoritative_trending_membership_unavailable'
                        if not normalized_exact_scope
                        or not {'KOSPI', 'KOSDAQ'}.issubset(set(markets))
                        else 'complete_price_scope_authority_unavailable'
                    ),
                    'market_date': market_date,
                    'periods_persisted': 0,
                    'periods_unavailable': len(
                        DAY_BASED_TRENDING_PERIODS
                    ),
                    'periods_total': len(DAY_BASED_TRENDING_PERIODS),
                    'admitted_stock_codes': normalized_admitted_scope,
                    '_retry_metadata': {
                        'failed_symbols': retry_debt_scope,
                    },
                }
                if retry_debt_scope:
                    blocked_result.update(
                        build_exact_symbol_scope_outcome(
                            manifest_version=(
                                'trending_ranks_scope_outcomes_v1'
                            ),
                            selected_symbols=retry_debt_scope,
                            successful_symbols=[],
                            retry_debt_symbols=retry_debt_scope,
                        )
                    )
                return blocked_result
            return compute_and_persist_daily_trending_ranks(
                market_date=market_date,
                source='stage_orchestrator',
                run_id=run_id,
                # Trending ranks are a global cross-sectional artifact.  A
                # narrowed recovery subset must never replace the whole
                # date's ranks; only a proven full membership is authoritative.
                symbols=normalized_exact_scope,
                authoritative_scope=True,
            )
        if stage_id == 'signal_features':
            return asyncio.run(
                daily_signal_feature_service.refresh_for_market_date(
                    market_date=market_date,
                    include_retry_metadata=True,
                    exact_symbols=exact_scope_symbols,
                    admitted_symbols=symbols,
                )
            )
        if stage_id == 'market_risk_snapshots':
            return self._execute_exact_scoped_derived_stage(
                stage_id=stage_id,
                market_date=market_date,
                exact_scope_symbols=exact_scope_symbols,
                call=lambda: asyncio.run(
                    daily_market_risk_snapshot_service.collect_for_market_date(
                        market_date=market_date,
                        symbols=symbols,
                        source=MARKET_RISK_SNAPSHOT_SOURCE,
                        market_code='J',
                        progress_callback=progress_callback,
                        **(
                            {
                                'exact_scope_symbols': exact_scope_symbols,
                            }
                            if exact_scope_symbols is not None
                            else {}
                        ),
                    )
                ),
                atomic_finalization=True,
            )
        if stage_id == 'market_risk_filter':
            return self._execute_exact_scoped_derived_stage(
                stage_id=stage_id,
                market_date=market_date,
                exact_scope_symbols=exact_scope_symbols,
                call=lambda: asyncio.run(
                    daily_market_risk_filter_service.rebuild_for_market_date(
                        market_date=market_date,
                        symbols=symbols,
                        source=MARKET_RISK_FILTER_SOURCE,
                        market_code='J',
                        progress_callback=progress_callback,
                        **(
                            {'exact_scope_symbols': exact_scope_symbols}
                            if exact_scope_symbols is not None
                            else {}
                        ),
                    )
                ),
                atomic_finalization=True,
            )
        if stage_id == 'fundamentals_daily_refresh':
            return asyncio.run(
                dart_fundamentals_capture_service.run_daily_incremental_refresh(
                    market_date=market_date,
                    symbols=symbols,
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
            return self._execute_exact_scoped_derived_stage(
                stage_id=stage_id,
                market_date=market_date,
                exact_scope_symbols=exact_scope_symbols,
                call=lambda: daily_piotroski_filter_service.rebuild_for_market_date(
                    market_date=market_date,
                    symbols=symbols,
                    **(
                        {
                            'ranking_symbols': exact_scope_symbols,
                            'exact_scope_symbols': exact_scope_symbols,
                        }
                        if exact_scope_symbols is not None
                        else {}
                    ),
                ),
                atomic_finalization=True,
            )
        if stage_id == 'momentum_factors':
            return self._execute_exact_scoped_derived_stage(
                stage_id=stage_id,
                market_date=market_date,
                exact_scope_symbols=exact_scope_symbols,
                call=lambda: daily_momentum_factor_service.rebuild_for_market_date(
                    market_date=market_date,
                    symbols=symbols,
                    **(
                        {
                            'ranking_symbols': exact_scope_symbols,
                            'exact_scope_symbols': exact_scope_symbols,
                        }
                        if exact_scope_symbols is not None
                        else {}
                    ),
                ),
                atomic_finalization=True,
            )
        if stage_id == 'value_factors':
            return self._execute_exact_scoped_derived_stage(
                stage_id=stage_id,
                market_date=market_date,
                exact_scope_symbols=exact_scope_symbols,
                call=lambda: daily_value_factor_service.rebuild_for_market_date(
                    market_date=market_date,
                    symbols=symbols,
                    source=VALUE_FACTOR_SOURCE,
                    **(
                        {
                            'ranking_symbols': exact_scope_symbols,
                            'exact_scope_symbols': exact_scope_symbols,
                        }
                        if exact_scope_symbols is not None
                        else {}
                    ),
                ),
                atomic_finalization=True,
            )
        if stage_id == 'low_risk_factors':
            return self._execute_exact_scoped_derived_stage(
                stage_id=stage_id,
                market_date=market_date,
                exact_scope_symbols=exact_scope_symbols,
                call=lambda: daily_low_risk_factor_service.rebuild_for_market_date(
                    market_date=market_date,
                    symbols=symbols,
                    source=LOW_RISK_SOURCE,
                    **(
                        {
                            'ranking_symbols': exact_scope_symbols,
                            'exact_scope_symbols': exact_scope_symbols,
                        }
                        if exact_scope_symbols is not None
                        else {}
                    ),
                ),
                atomic_finalization=True,
            )
        if stage_id == 'beta_factors':
            return self._execute_exact_scoped_derived_stage(
                stage_id=stage_id,
                market_date=market_date,
                exact_scope_symbols=exact_scope_symbols,
                call=lambda: daily_beta_factor_service.rebuild_for_market_date(
                    market_date=market_date,
                    symbols=symbols,
                    source=BETA_FACTOR_SOURCE,
                    **(
                        {
                            'ranking_symbols': exact_scope_symbols,
                            'exact_scope_symbols': exact_scope_symbols,
                        }
                        if exact_scope_symbols is not None
                        else {}
                    ),
                ),
                atomic_finalization=True,
            )
        if stage_id == 'advanced_value_factors':
            return self._execute_exact_scoped_derived_stage(
                stage_id=stage_id,
                market_date=market_date,
                exact_scope_symbols=exact_scope_symbols,
                call=lambda: daily_advanced_value_factor_service.rebuild_for_market_date(
                    market_date=market_date,
                    symbols=symbols,
                    source=ADVANCED_VALUE_FACTOR_SOURCE,
                    **(
                        {
                            'ranking_symbols': exact_scope_symbols,
                            'exact_scope_symbols': exact_scope_symbols,
                        }
                        if exact_scope_symbols is not None
                        else {}
                    ),
                ),
                atomic_finalization=True,
            )
        if stage_id == 'unified_value_factors':
            return self._execute_exact_scoped_derived_stage(
                stage_id=stage_id,
                market_date=market_date,
                exact_scope_symbols=exact_scope_symbols,
                call=lambda: daily_unified_value_factor_service.rebuild_for_market_date(
                    market_date=market_date,
                    symbols=symbols,
                    source=UNIFIED_VALUE_FACTOR_SOURCE,
                    **(
                        {
                            'ranking_symbols': exact_scope_symbols,
                            'exact_scope_symbols': exact_scope_symbols,
                        }
                        if exact_scope_symbols is not None
                        else {}
                    ),
                ),
                atomic_finalization=True,
            )
        if stage_id == 'quality_factors':
            return self._execute_exact_scoped_derived_stage(
                stage_id=stage_id,
                market_date=market_date,
                exact_scope_symbols=exact_scope_symbols,
                call=lambda: daily_quality_factor_service.rebuild_for_market_date(
                    market_date=market_date,
                    symbols=symbols,
                    source=QUALITY_FACTOR_SOURCE,
                    **(
                        {
                            'ranking_symbols': exact_scope_symbols,
                            'exact_scope_symbols': exact_scope_symbols,
                        }
                        if exact_scope_symbols is not None
                        else {}
                    ),
                ),
                atomic_finalization=True,
            )
        if stage_id == 'composite_factors':
            return self._execute_exact_scoped_derived_stage(
                stage_id=stage_id,
                market_date=market_date,
                exact_scope_symbols=exact_scope_symbols,
                call=lambda: daily_composite_factor_service.rebuild_for_market_date(
                    market_date=market_date,
                    symbols=symbols,
                    source=COMPOSITE_FACTOR_SOURCE,
                    **(
                        {
                            'ranking_symbols': exact_scope_symbols,
                            'exact_scope_symbols': exact_scope_symbols,
                        }
                        if exact_scope_symbols is not None
                        else {}
                    ),
                ),
                atomic_finalization=True,
            )
        if stage_id == 'external_news_collect':
            return self._execute_optional_context_stage(
                stage_id=stage_id,
                call=lambda: asyncio.run(
                    external_news_service.collect_for_agent_feedback_candidates(
                        market_date=market_date,
                        symbols=symbols,
                        display=settings.external_news_default_display,
                        symbol_limit=symbol_limit,
                    )
                ),
            )
        if stage_id == 'external_news_enrich':
            return self._execute_optional_context_stage(
                stage_id=stage_id,
                call=lambda: external_news_service.enrich_recent_classification(
                    symbols=symbols,
                    market_date=market_date,
                    lookback_days=(
                        settings.external_news_context_lookback_days
                    ),
                    limit=symbol_limit or 1000,
                ),
            )
        if stage_id == 'macro_context_refresh':
            return self._execute_optional_context_stage(
                stage_id=stage_id,
                call=lambda: macro_context_service.refresh_context(
                    force=force,
                    trigger='daily_pipeline_stage',
                ),
            )
        if stage_id in {'agent_feedback_summaries', 'signal_decider'}:
            prerequisites = agent_feedback_prerequisite_service.inspect_feedback_prerequisites(
                market_date=market_date,
                fs_div=fs_div,
                symbols=symbols,
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
            retry_symbols = tuple(symbols) if symbols else None
            retry_policy_context_as_of_date: date | str | None = None
            retry_idempotency_key: str | None = None
            retry_role_symbol_scopes: Mapping[
                str,
                Sequence[str],
            ] | None = None
            retry_feedback_scope_mode = (
                FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE
                if stage_id == 'agent_feedback_summaries'
                else FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
            )
            retry_outer_batch_size = feedback_outer_batch_size
            if (
                stage_id == 'agent_feedback_summaries'
                and isinstance(feedback_retry_config_override, Mapping)
            ):
                roles = tuple(feedback_retry_config_override['roles'])
                retry_symbols = tuple(
                    feedback_retry_config_override['requested_symbols']
                )
                retry_policy_context_as_of_date = (
                    feedback_retry_config_override[
                        'policy_context_as_of_date'
                    ]
                )
                retry_idempotency_key = (
                    feedback_retry_config_override.get('idempotency_key')
                )
                retry_role_symbol_scopes = (
                    feedback_retry_config_override['role_symbol_scopes']
                )
                retry_feedback_scope_mode = str(
                    feedback_retry_config_override[
                        'feedback_scope_mode'
                    ]
                )
                retry_outer_batch_size = int(
                    feedback_retry_config_override['outer_batch_size']
                )
                inherited_scope_authority = (
                    feedback_retry_config_override.get('scope_authority')
                )
                scope_authority = (
                    dict(inherited_scope_authority)
                    if isinstance(inherited_scope_authority, Mapping)
                    else None
                )
            result = self.feedback_retry_service.run_retry_cycles(
                config=FeedbackRetryCycleConfig(
                    market_date=date.fromisoformat(market_date),
                    roles=roles,
                    symbols=retry_symbols,
                    policy_context_as_of_date=(
                        retry_policy_context_as_of_date
                    ),
                    idempotency_key=retry_idempotency_key,
                    fs_div=fs_div,
                    # A production retry of a completed source day must see
                    # information collected after that session close. The
                    # resolved instant remains fixed for every child cycle.
                    knowledge_cutoff_at=resolved_feedback_cutoff,
                    feedback_scope_mode=retry_feedback_scope_mode,
                    max_cycles=feedback_max_cycles,
                    outer_batch_size=retry_outer_batch_size,
                    reuse_existing=reuse_existing_feedback,
                    point_in_time_recovery=point_in_time_recovery,
                    reuse_parent_run_id=(
                        feedback_reuse_parent_run_id
                        if stage_id == 'agent_feedback_summaries'
                        else None
                    ),
                    scope_authority=scope_authority,
                    role_symbol_scopes=retry_role_symbol_scopes,
                    progress_callback=progress_callback,
                ),
                # A forced pipeline recovery must bypass stale prerequisite
                # gates, but it must not silently turn an idempotent resume
                # into a full model recompute when reuse was requested.
                force=force and not reuse_existing_feedback,
                bypass_prerequisite_gate=bypass_prerequisite_gate,
            )
            expired_after_completion = (
                None
                if allow_expired_analysis_replay
                else build_expired_decision_replay_result(
                    stage_id=stage_id,
                    market_date=market_date,
                    market=get_market_state(),
                    allow_same_day_intraday_watch_continuation=(
                        allow_same_day_intraday_watch_continuation
                    ),
                    allow_previous_session_preopen_recovery=(
                        allow_previous_session_preopen_recovery
                    ),
                )
            )
            if expired_after_completion is not None:
                return {
                    **expired_after_completion,
                    'completed_after_action_window': True,
                }
            if stage_id == 'signal_decider':
                from app.orders.next_open_execution import (
                    signal_decider_next_open_execution_service,
                )

                parent_run_id = str(result.get('run_id') or '').strip()
                requested_identity_symbols = result.get(
                    'requested_symbols'
                )
                existing_decision_identities = result.get(
                    'decision_identities'
                )
                identity_resolution = (
                    {
                        'status': 'completed',
                        'parent_run_id': parent_run_id,
                        'symbols': list(requested_identity_symbols),
                        'decision_identities': list(
                            existing_decision_identities
                        ),
                    }
                    if isinstance(existing_decision_identities, list)
                    and existing_decision_identities
                    else
                    signal_decider_next_open_execution_service
                    .resolve_completion_decision_identities(
                        source_market_date=market_date,
                        parent_run_id=parent_run_id,
                        symbols=requested_identity_symbols,
                    )
                    if parent_run_id
                    and isinstance(requested_identity_symbols, list)
                    and requested_identity_symbols
                    else {
                        'status': 'blocked',
                        'reason': 'completion_identity_scope_unavailable',
                        'decision_identities': [],
                    }
                )
                result = {
                    **result,
                    'decision_identity_resolution': identity_resolution,
                    'decision_identities': list(
                        identity_resolution.get('decision_identities')
                        or []
                    ),
                }
            return result
        if stage_id == 'next_open_execution_plan':
            from app.orders.next_open_execution import signal_decider_next_open_execution_service

            expired_result = build_next_open_window_expired_result(
                market_date=market_date,
                market=get_market_state(),
                allow_same_day_intraday_watch_continuation=(
                    allow_same_day_intraday_watch_continuation
                ),
                allow_previous_session_preopen_recovery=(
                    allow_previous_session_preopen_recovery
                ),
            )
            if expired_result is not None:
                return {**expired_result, 'stage': stage_id}
            target_trade_date = (date.fromisoformat(market_date) + timedelta(days=1)).isoformat()
            return signal_decider_next_open_execution_service.refresh_plan_items(
                target_trade_date=target_trade_date,
                trigger='daily_pipeline_post_decider',
                force=force,
                completion_parent_proof=completion_parent_proof,
            )
        if stage_id == 'signal_decider_evaluations':
            prerequisites = agent_feedback_prerequisite_service.inspect_feedback_prerequisites(
                market_date=market_date,
                fs_div=fs_div,
                symbols=symbols,
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
            expired_after_completion = (
                None
                if allow_expired_analysis_replay
                else build_expired_decision_replay_result(
                    stage_id=stage_id,
                    market_date=market_date,
                    market=get_market_state(),
                    allow_same_day_intraday_watch_continuation=(
                        allow_same_day_intraday_watch_continuation
                    ),
                    allow_previous_session_preopen_recovery=(
                        allow_previous_session_preopen_recovery
                    ),
                )
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
        source_run_ids = (
            dict(result['source_run_ids'])
            if isinstance(result.get('source_run_ids'), dict)
            else {}
        )
        if stage_id == 'price_collection' and result.get('run_id') is not None:
            source_run_ids.setdefault('eod_run_id', result['run_id'])
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
                'source_run_ids': source_run_ids,
                'status': price_status,
                'market_date': result.get('latest_available_trade_date'),
                'symbols_total': result.get('symbols_total'),
                'symbols_succeeded': result.get('symbols_succeeded'),
                'symbols_failed': result.get('symbols_failed'),
                'failed_symbols': result.get('failed_symbols'),
                'bars_upserted': result.get('bars_upserted'),
                'scope_outcome_manifest_version': result.get(
                    'scope_outcome_manifest_version'
                ),
                'selected_stock_codes': result.get(
                    'selected_stock_codes'
                ),
                'successful_stock_codes': result.get(
                    'successful_stock_codes'
                ),
                'retry_debt_stock_codes': result.get(
                    'retry_debt_stock_codes'
                ),
                'terminal_not_applicable_stock_codes': result.get(
                    'terminal_not_applicable_stock_codes'
                ),
                'scope_outcome_manifest': result.get(
                    'scope_outcome_manifest'
                ),
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
        payload_source_run_ids = (
            dict(payload['source_run_ids'])
            if isinstance(payload.get('source_run_ids'), dict)
            else {}
        )
        payload_source_run_ids.update(source_run_ids)
        payload['source_run_ids'] = payload_source_run_ids
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

    def _restore_persisted_stage_outcome(
        self,
        *,
        stage_id: str,
        persisted_outcome: Mapping[str, Any],
    ) -> tuple[dict[str, Any], list[str] | None]:
        persisted_details = persisted_outcome.get('details')
        persisted_result = (
            persisted_details.get('result_summary')
            if isinstance(persisted_details, Mapping)
            and isinstance(
                persisted_details.get('result_summary'),
                Mapping,
            )
            else {}
        )
        persisted_status = str(
            persisted_outcome.get('status') or ''
        ).strip().lower()
        restored_outcome = self._build_stage_outcome(
            stage_id,
            {
                **dict(persisted_result),
                'status': persisted_status,
                'resumed_from_persisted_stage': True,
            },
        )
        restored_outcome['status'] = persisted_status
        restored_invocation: list[str] | None = None
        if isinstance(persisted_details, Mapping):
            restored_outcome['details'] = dict(persisted_details)
            if 'exact_scope_outcome_proof' in persisted_details:
                proof_only_outcome = {
                    'status': persisted_status,
                    'result': {'status': persisted_status},
                    'details': dict(persisted_details),
                }
                restored_scope = self._exact_partial_success_scope(
                    outcome=proof_only_outcome,
                    invocation_symbols=None,
                )
                if restored_scope is None:
                    raise RuntimeError(
                        'persisted exact scope outcome proof is invalid for '
                        f'{stage_id}'
                    )
                restored_invocation = list(
                    restored_scope['selected_stock_codes']
                )
        return restored_outcome, restored_invocation

    @staticmethod
    def _dependency_outcome_is_satisfied(outcome: dict[str, Any] | None) -> bool:
        return daily_pipeline_outcome_is_satisfied(outcome)

    @classmethod
    def _complete_price_scope_is_proven(
        cls,
        *,
        exact_scope_symbols: Sequence[str] | None,
        admitted_symbols: Sequence[str] | None,
        price_outcome: dict[str, Any] | None,
        price_invocation_symbols: Sequence[str] | None,
    ) -> bool:
        """Require an exhaustive same-run price partition for global ranks."""

        exact_scope = normalize_symbol_codes(exact_scope_symbols or [])
        admitted_scope = normalize_symbol_codes(admitted_symbols or [])
        if not (
            exact_scope
            and admitted_scope
            and len(exact_scope) == len(admitted_scope)
            and set(exact_scope) == set(admitted_scope)
        ):
            return False
        price_scope = cls._exact_partial_success_scope(
            outcome=price_outcome,
            invocation_symbols=price_invocation_symbols,
        )
        return bool(
            price_scope is not None
            and not price_scope['retry_debt_stock_codes']
            and len(price_scope['selected_stock_codes'])
            == len(exact_scope)
            and set(price_scope['selected_stock_codes'])
            == set(exact_scope)
            and len(price_scope['successful_stock_codes'])
            == len(exact_scope)
            and set(price_scope['successful_stock_codes'])
            == set(exact_scope)
        )

    @staticmethod
    def _exact_partial_success_scope(
        *,
        outcome: dict[str, Any] | None,
        invocation_symbols: Sequence[str] | None,
    ) -> dict[str, Any] | None:
        """Return a partial-success partition bound to its exact invocation.

        A well-formed partition for only part of a larger requested scope is
        not exhaustive evidence.  The orchestrator therefore binds the
        manifest to the symbols it actually passed to that dependency.
        """

        if not (
            isinstance(outcome, dict)
            and str(outcome.get('status') or '').strip().lower()
            in {'completed', 'partial'}
        ):
            return None
        result = outcome.get('result')
        if not isinstance(result, dict):
            return None
        details = outcome.get('details')
        proof = (
            details.get('exact_scope_outcome_proof')
            if isinstance(details, Mapping)
            else None
        )
        if (
            not isinstance(result.get('selected_stock_codes'), list)
            and isinstance(proof, Mapping)
        ):
            proof_selected = proof.get('selected_stock_codes')
            if isinstance(proof_selected, list):
                normalized_invocation = normalize_symbol_codes(
                    proof_selected
                )
                if normalized_invocation != proof_selected:
                    return None
                if invocation_symbols is not None:
                    bound_invocation = normalize_symbol_codes(
                        invocation_symbols
                    )
                    if (
                        bound_invocation != list(invocation_symbols)
                        or bound_invocation != normalized_invocation
                    ):
                        return None
            elif invocation_symbols is not None:
                normalized_invocation = normalize_symbol_codes(
                    invocation_symbols
                )
                if normalized_invocation != list(invocation_symbols):
                    return None
            else:
                return None
            retry_debt = normalize_symbol_codes(
                proof.get('retry_debt_stock_codes') or []
            )
            terminal_not_applicable = normalize_symbol_codes(
                proof.get('terminal_not_applicable_stock_codes') or []
            )
            retry_set = set(retry_debt)
            terminal_set = set(terminal_not_applicable)
            invocation_set = set(normalized_invocation)
            if (
                proof.get('schema_version')
                != EXACT_SCOPE_OUTCOME_PROOF_SCHEMA_VERSION
                or str(proof.get('manifest_version') or '').strip()
                not in EXACT_SCOPE_OUTCOME_MANIFEST_VERSIONS
                or any(not value for value in normalized_invocation)
                or len(normalized_invocation)
                != len(invocation_set)
                or retry_debt
                != list(proof.get('retry_debt_stock_codes') or [])
                or terminal_not_applicable
                != list(
                    proof.get(
                        'terminal_not_applicable_stock_codes'
                    )
                    or []
                )
                or retry_set & terminal_set
                or not (retry_set | terminal_set) <= invocation_set
                or proof.get('selected_symbol_count')
                != len(normalized_invocation)
                or str(proof.get('selected_symbol_sha256') or '')
                != market_membership_sha256(normalized_invocation)
            ):
                return None
            successful = [
                symbol
                for symbol in normalized_invocation
                if symbol not in retry_set | terminal_set
            ]
            if (
                proof.get('successful_symbol_count') != len(successful)
                or str(proof.get('successful_symbol_sha256') or '')
                != market_membership_sha256(successful)
            ):
                return None
            outcome_status = str(
                outcome.get('status') or ''
            ).strip().lower()
            if outcome_status == 'completed' and retry_debt:
                return None
            return {
                'manifest_version': proof['manifest_version'],
                'selected_stock_codes': normalized_invocation,
                'successful_stock_codes': successful,
                'retry_debt_stock_codes': retry_debt,
                'terminal_not_applicable_stock_codes': (
                    terminal_not_applicable
                ),
            }
        version = str(
            result.get('scope_outcome_manifest_version') or ''
        ).strip()
        if version not in EXACT_SCOPE_OUTCOME_MANIFEST_VERSIONS:
            return None

        def normalize_list(key: str) -> list[str] | None:
            raw = result.get(key)
            if not isinstance(raw, list):
                return None
            normalized = [
                (
                    str(value or '').strip().zfill(6)
                    if str(value or '').strip().isdigit()
                    else str(value or '').strip()
                )
                for value in raw
            ]
            if any(not value for value in normalized):
                return None
            if len(normalized) != len(set(normalized)):
                return None
            return normalized

        selected = normalize_list('selected_stock_codes')
        successful = normalize_list('successful_stock_codes')
        retry_debt = normalize_list('retry_debt_stock_codes')
        terminal_not_applicable = normalize_list(
            'terminal_not_applicable_stock_codes'
        )
        if terminal_not_applicable is None:
            terminal_not_applicable = []
        if selected is None or successful is None or retry_debt is None:
            return None
        selected_set = set(selected)
        successful_set = set(successful)
        retry_debt_set = set(retry_debt)
        terminal_set = set(terminal_not_applicable)
        if (
            successful_set & retry_debt_set
            or successful_set & terminal_set
            or retry_debt_set & terminal_set
            or selected_set
            != successful_set | retry_debt_set | terminal_set
        ):
            return None
        outcome_status = str(outcome.get('status') or '').strip().lower()
        if outcome_status == 'completed' and retry_debt_set:
            return None
        if invocation_symbols is None:
            return None
        normalized_invocation = [
            (
                str(value or '').strip().zfill(6)
                if str(value or '').strip().isdigit()
                else str(value or '').strip()
            )
            for value in invocation_symbols
        ]
        if (
            not normalized_invocation
            or any(not value for value in normalized_invocation)
            or len(normalized_invocation) != len(set(normalized_invocation))
            or selected_set != set(normalized_invocation)
        ):
            return None

        raw_manifest = result.get('scope_outcome_manifest')
        if not isinstance(raw_manifest, list):
            return None
        manifest_outcomes: dict[str, str] = {}
        for item in raw_manifest:
            if not isinstance(item, dict):
                return None
            raw_stock_code = str(item.get('stock_code') or '').strip()
            stock_code = (
                raw_stock_code.zfill(6)
                if raw_stock_code.isdigit()
                else raw_stock_code
            )
            outcome_value = str(item.get('outcome') or '').strip()
            if (
                not stock_code
                or stock_code in manifest_outcomes
                or outcome_value
                not in {'success', 'retry_debt', 'terminal_not_applicable'}
            ):
                return None
            manifest_outcomes[stock_code] = outcome_value
        expected_manifest_outcomes = {
            **{stock_code: 'success' for stock_code in successful_set},
            **{stock_code: 'retry_debt' for stock_code in retry_debt_set},
            **{
                stock_code: 'terminal_not_applicable'
                for stock_code in terminal_set
            },
        }
        if manifest_outcomes != expected_manifest_outcomes:
            return None
        return {
            'manifest_version': version,
            'selected_stock_codes': selected,
            'successful_stock_codes': successful,
            'retry_debt_stock_codes': retry_debt,
            'terminal_not_applicable_stock_codes': terminal_not_applicable,
        }

    @staticmethod
    def _validated_pykrx_invocation_scope(
        result: Mapping[str, Any],
    ) -> list[str] | None:
        if (
            str(result.get('invocation_scope_manifest_version') or '')
            != PYKRX_INVOCATION_SCOPE_MANIFEST_VERSION
        ):
            return None
        raw_symbols = result.get('invocation_scope_stock_codes')
        if not isinstance(raw_symbols, list):
            return None
        normalized = [
            str(value or '').strip().zfill(6)
            if str(value or '').strip().isdigit()
            else str(value or '').strip()
            for value in raw_symbols
        ]
        if (
            not normalized
            or any(not value for value in normalized)
            or len(normalized) != len(set(normalized))
        ):
            return None
        try:
            declared_count = int(result.get('invocation_scope_count'))
        except (TypeError, ValueError):
            return None
        if declared_count != len(normalized):
            return None
        canonical = json.dumps(
            normalized,
            ensure_ascii=False,
            separators=(',', ':'),
        )
        expected_sha256 = hashlib.sha256(
            canonical.encode('utf-8')
        ).hexdigest()
        if (
            str(result.get('invocation_scope_sha256') or '').strip()
            != expected_sha256
        ):
            return None
        return normalized

    @staticmethod
    def _partial_summary_dependency_is_proven(
        *,
        outcome: dict[str, Any] | None,
        cutoff_resolution: dict[str, Any] | None,
        parent_symbols: Sequence[str] | None,
    ) -> bool:
        """Accept only an exact finalized success intersection as input."""

        if not (
            isinstance(outcome, dict)
            and str(outcome.get('status') or '').strip().lower()
            == 'partial'
            and isinstance(cutoff_resolution, dict)
            and cutoff_resolution.get('ready') is True
            and cutoff_resolution.get('knowledge_cutoff_at') is not None
        ):
            return False
        result = outcome.get('result')
        if not isinstance(result, dict):
            return False
        source_run_id = str(
            cutoff_resolution.get('source_run_id') or ''
        ).strip()
        completed_symbols = cutoff_resolution.get('requested_symbols')
        return bool(
            source_run_id
            and source_run_id == str(result.get('run_id') or '').strip()
            and isinstance(completed_symbols, list)
            and completed_symbols
            and completed_summary_scope_matches_parent(
                feedback_scope_mode=cutoff_resolution.get(
                    'feedback_scope_mode'
                ),
                completed_symbols=completed_symbols,
                completed_scope_proof=cutoff_resolution.get(
                    'completed_scope_proof'
                ),
                parent_symbols=parent_symbols,
            )
        )

    @staticmethod
    def _terminal_decider_progression_proof(
        outcome: dict[str, Any] | None,
        *,
        expected_decider_symbols: Sequence[str] | None,
        feedback_cutoff_resolution: Mapping[str, Any] | None,
    ) -> dict[str, Any] | None:
        """Return terminal decider evidence bound to one summary lineage."""

        if not (
            isinstance(outcome, dict)
            and str(outcome.get('status') or '').strip().lower()
            in {'completed', 'partial'}
        ):
            return None
        result = outcome.get('result')
        progress = (
            result.get('progress')
            if isinstance(result, dict)
            and isinstance(result.get('progress'), dict)
            else {}
        )
        progress_summary = (
            progress.get('summary')
            if isinstance(progress.get('summary'), dict)
            else {}
        )
        roles = result.get('roles') if isinstance(result, dict) else None
        normalized_expected_symbols = (
            normalize_symbol_codes(expected_decider_symbols)
            if isinstance(expected_decider_symbols, (list, tuple))
            else []
        )
        cutoff_requested_symbols = (
            feedback_cutoff_resolution.get('requested_symbols')
            if isinstance(feedback_cutoff_resolution, Mapping)
            else None
        )
        normalized_cutoff_symbols = (
            normalize_symbol_codes(cutoff_requested_symbols)
            if isinstance(cutoff_requested_symbols, list)
            else []
        )
        result_requested_symbols = (
            result.get('requested_symbols')
            if isinstance(result, dict)
            else None
        )
        scope_authority = (
            result.get('scope_authority')
            if isinstance(result, dict)
            and isinstance(result.get('scope_authority'), Mapping)
            else {}
        )
        cutoff_source_run_id = str(
            (
                feedback_cutoff_resolution.get('source_run_id')
                if isinstance(feedback_cutoff_resolution, Mapping)
                else None
            )
            or ''
        ).strip()
        cutoff_scope_mode = str(
            (
                feedback_cutoff_resolution.get('feedback_scope_mode')
                if isinstance(feedback_cutoff_resolution, Mapping)
                else None
            )
            or ''
        ).strip()
        cutoff_at = (
            feedback_cutoff_resolution.get('knowledge_cutoff_at')
            if isinstance(feedback_cutoff_resolution, Mapping)
            else None
        )
        try:
            normalized_cutoff_at = (
                cutoff_at
                if isinstance(cutoff_at, datetime)
                else datetime.fromisoformat(str(cutoff_at or ''))
            )
        except (TypeError, ValueError):
            normalized_cutoff_at = None
        if (
            normalized_cutoff_at is not None
            and normalized_cutoff_at.tzinfo is not None
            and normalized_cutoff_at.utcoffset() is not None
        ):
            normalized_cutoff_at = normalized_cutoff_at.astimezone(
                timezone.utc
            )
        else:
            normalized_cutoff_at = None
        if not (
            isinstance(roles, list)
            and set(roles) == set(DECISION_SUPPORT_AGENT_NAMES)
            and len(roles) == len(DECISION_SUPPORT_AGENT_NAMES)
            and str(result.get('run_id') or '').strip()
            == str(progress_summary.get('run_id') or '').strip()
            and normalized_expected_symbols
            and normalized_expected_symbols
            == list(expected_decider_symbols or ())
            and normalized_cutoff_symbols == normalized_expected_symbols
            and result_requested_symbols == normalized_expected_symbols
            and result.get('feedback_scope_mode')
            == FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
            and isinstance(feedback_cutoff_resolution, Mapping)
            and feedback_cutoff_resolution.get('ready') is True
            and cutoff_source_run_id
            and normalized_cutoff_at is not None
            and scope_authority.get('schema_version')
            == FEEDBACK_SCOPE_AUTHORITY_SCHEMA_VERSION
            and scope_authority.get('kind')
            == FEEDBACK_SCOPE_AUTHORITY_COMPLETED_SUMMARY_INTERSECTION
            and scope_authority.get('source_summary_run_id')
            == cutoff_source_run_id
            and scope_authority.get('source_feedback_scope_mode')
            == cutoff_scope_mode
            and scope_authority.get('decision_feedback_scope_mode')
            == FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
            and scope_authority.get('knowledge_cutoff_at')
            == normalized_cutoff_at.isoformat()
            and scope_authority.get('completed_scope_proof')
            == feedback_cutoff_resolution.get('completed_scope_proof')
        ):
            return None
        resolution = resolve_terminal_role_success_intersection(
            progress_summary=progress_summary,
            roles=DECISION_SUPPORT_AGENT_NAMES,
            expected_role_symbol_scopes={
                role: normalized_expected_symbols
                for role in DECISION_SUPPORT_AGENT_NAMES
            },
        )
        completed_symbols = resolution.get('completed_symbols')
        if not (
            resolution.get('ready') is True
            and isinstance(completed_symbols, list)
            and completed_symbols
        ):
            return None
        raw_decision_identities = (
            result.get('decision_identities')
            if isinstance(result, Mapping)
            else None
        )
        if not isinstance(raw_decision_identities, list):
            return None
        completed_symbol_set = set(completed_symbols)
        decision_identities_by_key: dict[
            tuple[str, str],
            dict[str, Any],
        ] = {}
        for raw_identity in raw_decision_identities:
            if not isinstance(raw_identity, Mapping):
                return None
            identity = dict(raw_identity)
            if set(identity) != {
                'symbol',
                'decision_agent_name',
                'decision_snapshot_id',
                'decision_run_id',
            }:
                return None
            normalized_symbol = normalize_symbol_codes(
                [identity.get('symbol')]
            )
            role = str(
                identity.get('decision_agent_name') or ''
            ).strip()
            snapshot_id = identity.get('decision_snapshot_id')
            decision_run_id = str(
                identity.get('decision_run_id') or ''
            ).strip()
            key = (
                normalized_symbol[0] if normalized_symbol else '',
                role,
            )
            if key[0] not in completed_symbol_set:
                # Identities from failed retry-debt scopes are intentionally
                # omitted from terminal progression authority.
                continue
            if not (
                len(normalized_symbol) == 1
                and identity.get('symbol') == normalized_symbol[0]
                and role in DECISION_SUPPORT_AGENT_NAMES
                and type(snapshot_id) is int
                and snapshot_id > 0
                and decision_run_id
                and key not in decision_identities_by_key
            ):
                return None
            decision_identities_by_key[key] = {
                'symbol': normalized_symbol[0],
                'decision_agent_name': role,
                'decision_snapshot_id': snapshot_id,
                'decision_run_id': decision_run_id,
            }
        expected_identity_keys = {
            (symbol, role)
            for symbol in completed_symbols
            for role in DECISION_SUPPORT_AGENT_NAMES
        }
        if set(decision_identities_by_key) != expected_identity_keys:
            return None
        decision_identities = sorted(
            decision_identities_by_key.values(),
            key=lambda identity: (
                identity['symbol'],
                identity['decision_agent_name'],
            ),
        )
        proof: dict[str, Any] = {
            'schema_version': STAGE_COMPLETION_SCOPE_PROOF_SCHEMA_VERSION,
            'kind': 'signal_decider_terminal_progression',
            'roles': list(DECISION_SUPPORT_AGENT_NAMES),
            'source_run_id': str(result.get('run_id') or '').strip(),
            'source_summary_run_id': cutoff_source_run_id,
            'knowledge_cutoff_at': normalized_cutoff_at.isoformat(),
            'admitted_symbols': list(normalized_expected_symbols),
            'completed_symbols': list(completed_symbols),
            'failed_symbols_by_role': {
                role: list(
                    resolution['failed_symbols_by_role'].get(role, [])
                )
                for role in DECISION_SUPPORT_AGENT_NAMES
            },
            'decision_identities': decision_identities,
            'total_items': resolution['total_items'],
            'completed_items': resolution['completed_items'],
            'failed_items': resolution['failed_items'],
        }
        proof['proof_digest'] = (
            DailyPipelineStageService
            ._stage_completion_scope_proof_digest(proof)
        )
        return (
            DailyPipelineStageService
            .validated_stage_completion_scope_proof(
                'signal_decider',
                proof,
            )
        )

    @staticmethod
    def _terminal_decider_completed_symbols(
        outcome: dict[str, Any] | None,
        *,
        expected_decider_symbols: Sequence[str] | None,
        feedback_cutoff_resolution: Mapping[str, Any] | None,
    ) -> list[str] | None:
        """Return the exact terminal two-decider success intersection."""

        if not (
            isinstance(outcome, dict)
            and str(outcome.get('status') or '').strip().lower()
            in {'completed', 'partial'}
        ):
            return None
        proof = (
            DailyPipelineStageService._terminal_decider_progression_proof(
                outcome,
                expected_decider_symbols=expected_decider_symbols,
                feedback_cutoff_resolution=feedback_cutoff_resolution,
            )
        )
        completed_symbols = (
            proof.get('completed_symbols')
            if isinstance(proof, Mapping)
            else None
        )
        return (
            list(completed_symbols)
            if isinstance(completed_symbols, list)
            and completed_symbols
            else None
        )

    def _resolve_stage_ids(
        self,
        requested_stages: Sequence[str],
        *,
        inject_next_open_dependency: bool = True,
        include_dependency_closure: bool = True,
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
        if include_dependency_closure:
            pending = list(requested_set)
            while pending:
                stage_id = pending.pop()
                for dependency in STAGE_DEFINITIONS[stage_id].depends_on:
                    if dependency in requested_set:
                        continue
                    requested_set.add(dependency)
                    pending.append(dependency)
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
            'same_day_intraday_watch_continuation_admitted': (
                metadata.get(
                    'same_day_intraday_watch_continuation_admitted'
                )
                is True
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
            'same_day_intraday_watch_continuation_admitted': (
                metadata.get(
                    'same_day_intraday_watch_continuation_admitted'
                )
                is True
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
        statuses = [
            str(item.get('status') or '').strip().lower()
            for item in stage_outcomes
        ]
        if not statuses:
            return 'completed'
        if any(status == 'running' for status in statuses):
            return 'running'
        if all(status == 'pending' for status in statuses):
            return 'pending'
        if any(status == 'failed' for status in statuses):
            if any(
                status in {'completed', 'partial'}
                for status in statuses
            ):
                return 'partial'
            return 'failed'
        for specific_status in (
            'blocked',
            'waiting',
            'interrupted',
            'abandoned',
        ):
            if all(status == specific_status for status in statuses):
                return specific_status
        if all(
            (
                status == 'completed'
                and daily_pipeline_outcome_is_satisfied(outcome)
            )
            or (
                status == 'skipped'
                and (
                    outcome.get('safe_skip') is True
                    or daily_pipeline_outcome_is_satisfied(outcome)
                )
            )
            for status, outcome in zip(statuses, stage_outcomes)
        ):
            return 'completed'
        return 'partial'


daily_pipeline_stage_service = DailyPipelineStageService()
