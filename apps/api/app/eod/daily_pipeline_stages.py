from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date
from typing import Any, Sequence

from app.agent_context.retry_service import FeedbackRetryCycleConfig, agent_feedback_retry_service
from app.agent_context.roles import SIGNAL_DECIDER_AGENT_NAME, SUMMARIZER_AGENT_NAMES
from app.core.settings import settings
from app.dart.fundamentals_capture_service import dart_fundamentals_capture_service
from app.eod.action_summaries import daily_action_summary_service
from app.eod.master import refresh_master_if_due
from app.eod.pykrx_backfill import DEFAULT_MARKETS, PYKRX_BACKFILL_SERVICE
from app.eod.technical_snapshots import daily_technical_snapshot_service
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
    'composite_factors',
)
DAILY_STAGE_ORDER: tuple[str, ...] = (
    'master_refresh',
    'pykrx_universe_refresh',
    *PYKRX_PIPELINE_STAGE_ORDER,
    'market_risk_snapshots',
    'fundamentals_daily_refresh',
    'piotroski_filter',
    *FACTOR_STAGE_ORDER,
    'agent_feedback_summaries',
    'signal_decider',
)
DEFAULT_DAILY_STAGE_ORDER: tuple[str, ...] = (
    'master_refresh',
    'pykrx_universe_refresh',
    *PYKRX_PIPELINE_STAGE_ORDER,
    'fundamentals_daily_refresh',
    'piotroski_filter',
    'agent_feedback_summaries',
    'signal_decider',
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
    ),
    'advanced_value_factors': DailyStageDefinition(
        id='advanced_value_factors',
        label='advanced value factor compute',
        group='factors',
        description='Rebuild persisted daily advanced value factor rows from stored price bars.',
        depends_on=('price_collection',),
        supports_symbols=True,
        inspect_routes=('/eod/advanced-value-factors',),
        output_tables=('daily_advanced_value_factor_results',),
    ),
    'composite_factors': DailyStageDefinition(
        id='composite_factors',
        label='composite factor compute',
        group='factors',
        description='Rebuild persisted daily composite factor rows from stored price bars and stored factor inputs.',
        depends_on=('price_collection',),
        supports_symbols=True,
        inspect_routes=('/eod/composite-factors',),
        output_tables=('daily_composite_factor_results',),
    ),
    'agent_feedback_summaries': DailyStageDefinition(
        id='agent_feedback_summaries',
        label='agent feedback summarizer dispatch',
        group='agent_feedback',
        description='Run the persisted symbol-level summarizer stages over the eligible symbol universe.',
        depends_on=('piotroski_filter',),
        supports_symbols=True,
        supports_capture_limit=True,
        inspect_routes=('/agent-context/feedback/progress', '/agent-context/feedback/status'),
        output_tables=('agent_context_feedback', 'agent_context_feedback_runs'),
        default_selected=True,
    ),
    'signal_decider': DailyStageDefinition(
        id='signal_decider',
        label='signal decider dispatch',
        group='agent_feedback',
        description='Run the persisted signal-decider stage over the eligible symbol universe using stored summaries.',
        depends_on=('agent_feedback_summaries',),
        supports_symbols=True,
        supports_capture_limit=True,
        inspect_routes=('/agent-context/feedback/progress', '/agent-context/feedback/latest'),
        output_tables=('agent_context_feedback', 'agent_context_feedback_runs'),
        default_selected=True,
    ),
}


class DailyPipelineStageService:
    def __init__(
        self,
        *,
        pykrx_service=PYKRX_BACKFILL_SERVICE,
        feedback_retry_service=agent_feedback_retry_service,
    ) -> None:
        self.pykrx_service = pykrx_service
        self.feedback_retry_service = feedback_retry_service

    def describe(self) -> dict[str, Any]:
        return {
            'stage_order': list(DAILY_STAGE_ORDER),
            'default_daily_stage_order': list(DEFAULT_DAILY_STAGE_ORDER),
            'pykrx_pipeline_stage_order': list(PYKRX_PIPELINE_STAGE_ORDER),
            'stages': [STAGE_DEFINITIONS[stage_id].to_dict() for stage_id in DAILY_STAGE_ORDER],
        }

    def run(
        self,
        *,
        market_date: str | date | None = None,
        stages: Sequence[str] | None = None,
        symbols: Sequence[str] | None = None,
        markets: Sequence[str] | None = None,
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
    ) -> dict[str, Any]:
        requested_stages = list(stages or DEFAULT_DAILY_STAGE_ORDER)
        resolved_stages = self._resolve_stage_ids(requested_stages)
        resolved_market_date = self._normalize_market_date(market_date)
        resolved_symbols = self._normalize_symbols(symbols)
        resolved_markets = self._normalize_markets(markets)
        resolved_fs_div = (fs_div or settings.dart_fundamentals_default_fs_div).strip().upper()

        stage_outcomes: dict[str, dict[str, Any]] = {}
        handled: set[str] = set()

        pykrx_requested = [stage_id for stage_id in resolved_stages if stage_id in PYKRX_PIPELINE_STAGE_ORDER]
        if 'price_collection' in pykrx_requested:
            pykrx_result = self.pykrx_service.run_daily_pipeline_stages(
                market_date=resolved_market_date,
                stages=pykrx_requested,
                markets=resolved_markets,
                trigger='stage_orchestrator',
                refresh_universe=(
                    settings.eod_auto_pykrx_refresh_universe if refresh_universe is None else refresh_universe
                ),
                force=force,
                symbols=resolved_symbols,
            )
            for stage_id in pykrx_requested:
                stage_outcomes[stage_id] = self._build_stage_outcome(
                    stage_id,
                    self._extract_pykrx_stage_result(stage_id, pykrx_result),
                )
                handled.add(stage_id)

        for stage_id in resolved_stages:
            if stage_id in handled:
                continue
            stage_outcomes[stage_id] = self._build_stage_outcome(
                stage_id,
                self._execute_stage(
                    stage_id,
                    market_date=resolved_market_date,
                    symbols=resolved_symbols,
                    markets=resolved_markets,
                    force=force,
                    fs_div=resolved_fs_div,
                    symbol_limit=symbol_limit,
                    symbol_offset=symbol_offset,
                    capture_limit=capture_limit,
                    retry_cycles=retry_cycles,
                    feedback_max_cycles=feedback_max_cycles,
                    feedback_outer_batch_size=feedback_outer_batch_size,
                    reuse_existing_feedback=reuse_existing_feedback,
                ),
            )

        return {
            'market_date': resolved_market_date,
            'requested_stages': requested_stages,
            'resolved_stages': resolved_stages,
            'status': self._resolve_overall_status(stage_outcomes.values()),
            'stages': stage_outcomes,
        }

    def _execute_stage(
        self,
        stage_id: str,
        *,
        market_date: str,
        symbols: list[str] | None,
        markets: list[str],
        force: bool,
        fs_div: str,
        symbol_limit: int | None,
        symbol_offset: int,
        capture_limit: int,
        retry_cycles: int | None,
        feedback_max_cycles: int,
        feedback_outer_batch_size: int,
        reuse_existing_feedback: bool,
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
                )
            )
        if stage_id == 'market_risk_filter':
            return asyncio.run(
                daily_market_risk_filter_service.rebuild_for_market_date(
                    market_date=market_date,
                    symbols=symbols,
                    source=MARKET_RISK_FILTER_SOURCE,
                    market_code='J',
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
        if stage_id == 'composite_factors':
            return daily_composite_factor_service.rebuild_for_market_date(
                market_date=market_date,
                symbols=symbols,
                source=COMPOSITE_FACTOR_SOURCE,
            )
        if stage_id in {'agent_feedback_summaries', 'signal_decider'}:
            roles = (
                SUMMARIZER_AGENT_NAMES
                if stage_id == 'agent_feedback_summaries'
                else (SIGNAL_DECIDER_AGENT_NAME,)
            )
            return self.feedback_retry_service.run_retry_cycles(
                config=FeedbackRetryCycleConfig(
                    market_date=date.fromisoformat(market_date),
                    roles=roles,
                    symbols=tuple(symbols) if symbols else None,
                    max_cycles=feedback_max_cycles,
                    outer_batch_size=feedback_outer_batch_size,
                    reuse_existing=reuse_existing_feedback,
                )
            )
        raise ValueError(f'Unsupported daily stage: {stage_id}')

    def _extract_pykrx_stage_result(self, stage_id: str, result: dict[str, Any]) -> dict[str, Any]:
        if stage_id == 'price_collection':
            return {
                'run_id': result.get('run_id'),
                'status': result.get('status'),
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
        if result.get('skipped') or result.get('triggered') is False:
            return 'skipped'
        raw_status = str(result.get('status') or '').strip()
        if raw_status:
            return 'partial' if raw_status == 'completed_with_errors' else raw_status
        if 'completed' in result and 'final_pending' in result:
            return 'completed' if bool(result.get('completed')) else 'partial'

        for failed_key in (
            'results_failed',
            'snapshots_failed',
            'action_summaries_failed',
            'features_failed',
            'symbols_failed',
            'contexts_failed',
        ):
            if failed_key in result:
                return 'completed' if int(result.get(failed_key) or 0) == 0 else 'partial'
        return 'completed'

    @staticmethod
    def _resolve_overall_status(stage_outcomes: Sequence[dict[str, Any]]) -> str:
        statuses = [str(item.get('status') or 'completed') for item in stage_outcomes]
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
