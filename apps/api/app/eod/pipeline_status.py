from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import aliased

from app.agent_context.prerequisite_service import AgentFeedbackPrerequisiteService
from app.agent_context.roles import SUMMARIZER_AGENT_NAMES
from app.agent_context.run_modes import DAILY_RETRY_CYCLE_MODE
from app.core.settings import settings
from app.db import session_scope
from app.db_models import (
    AgentContextFeedback,
    DailyActionSummary,
    DailyMarketRiskFilterResult,
    DailyPiotroskiFilterResult,
    DailyAdvancedValueFactorResult,
    DailyBetaFactorResult,
    DailyCompositeFactorResult,
    DailyLowRiskFactorResult,
    DailyMomentumFactorResult,
    DailyPipelineRun,
    DailyPipelineStageRun,
    DailyPriceBar,
    DailyPriceContext,
    DailyQualityFactorResult,
    DailySignalFeature,
    DailyTechnicalSnapshot,
    DailyUnifiedValueFactorResult,
    DailyValueFactorResult,
    ExternalNewsItem,
    ExternalNewsItemClassification,
    ExternalNewsItemSymbol,
    SignalDeciderDecisionSnapshot,
    Symbol,
)
from app.dart.fundamentals_capture_service import dart_fundamentals_capture_service
from app.eod.daily_pipeline_stages import (
    DAILY_STAGE_ORDER,
    FACTOR_STAGE_ORDER,
    STAGE_DEFINITIONS,
    DailyPipelineStageService,
)
from app.eod.daily_pipeline_outcomes import (
    daily_pipeline_nonblocking_warning_stage_ids,
    daily_pipeline_result_has_no_ready_content,
    daily_pipeline_warning_metadata,
    normalize_daily_pipeline_stage_status,
)
from app.eod.daily_pipeline_state import daily_pipeline_run_state_service
from app.eod.pykrx_backfill import DEFAULT_MARKETS
from app.eod.pykrx_enrichment import DEFAULT_PYKRX_ENRICHMENT_CATEGORIES, pykrx_enrichment_service
from app.eod.market_membership import validate_market_membership_snapshot
from app.eod.pipeline_status_support import (
    PYKRX_STAGE_ORDER,
    STAGE_LABELS,
    _build_fundamentals_activity,
    _default_activity,
    _infer_pykrx_stage,
    _normalize_status,
    _progress_extras,
    _stage_index,
)
from app.eod.run_stage_support import (
    build_stage_counts,
    extract_fundamentals_refresh_metrics,
    get_run_metadata,
    resolve_run_updated_at,
    select_preferred_eod_run,
    validated_nonblocking_downstream_scope_proof,
)
from app.eod.summary_cutoff_contract import (
    summary_cutoff_proof_from_stage,
)
from app.external_news.classifier import (
    CLASSIFIER_VERSION,
    external_news_classification_has_schema_contract,
)
from app.macro_context.service import macro_context_service
from app.market.clock import get_market_state
from app.market.session import build_market_session_metadata
from app.market_data_store import (
    DAY_BASED_TRENDING_PERIODS,
    count_daily_action_summaries_for_live_symbols,
    count_daily_price_bars_for_date,
    count_daily_price_contexts_for_live_symbols,
    count_daily_technical_snapshots_for_live_symbols,
    count_live_symbols,
    load_eod_run_stage_counts,
)
from app.repositories.eod_run_repository import list_eod_collection_runs

DEFAULT_FACTOR_STAGE_ORDER = tuple(
    stage_id
    for stage_id in FACTOR_STAGE_ORDER
    if STAGE_DEFINITIONS[stage_id].default_selected
)
FILTER_STAGE_ORDER = (
    'market_risk_filter',
    'piotroski_filter',
)
PIPELINE_STAGE_ORDER = (
    tuple(stage_id for stage_id in DAILY_STAGE_ORDER if stage_id != 'market_risk_snapshots')
)
LIVE_EOD_RUN_PROGRESS_FALLBACK_STAGE_IDS = tuple(
    stage_id
    for stage_id in PIPELINE_STAGE_ORDER
    if stage_id not in {*PYKRX_STAGE_ORDER, 'pykrx_enrichment'}
)
FACTOR_STAGE_MODELS = {
    'momentum_factors': DailyMomentumFactorResult,
    'value_factors': DailyValueFactorResult,
    'low_risk_factors': DailyLowRiskFactorResult,
    'beta_factors': DailyBetaFactorResult,
    'advanced_value_factors': DailyAdvancedValueFactorResult,
    'unified_value_factors': DailyUnifiedValueFactorResult,
    'quality_factors': DailyQualityFactorResult,
    'composite_factors': DailyCompositeFactorResult,
}
ROW_COUNT_STAGE_MODELS = {
    'market_risk_filter': DailyMarketRiskFilterResult,
    'piotroski_filter': DailyPiotroskiFilterResult,
    **FACTOR_STAGE_MODELS,
}
PREREQUISITE_SCOPE_STAGE_MODELS = {
    'price_collection': DailyPriceBar,
    'technical_snapshots': DailyTechnicalSnapshot,
    'action_summaries': DailyActionSummary,
    'signal_features': DailySignalFeature,
    **ROW_COUNT_STAGE_MODELS,
}
PREREQUISITE_SCOPE_STAGE_REQUIRED_MODELS = {
    'technical_snapshots': (
        DailyPriceContext,
        DailyTechnicalSnapshot,
    ),
    'action_summaries': (
        DailyPriceContext,
        DailyTechnicalSnapshot,
        DailyActionSummary,
    ),
}
ROW_COUNT_STAGE_MISSING_SAMPLE_LIMIT = 10
PARTIAL_ROW_COUNT_MIN_NON_BLOCKING_COVERAGE_RATIO = 0.99
NON_DATE_SCOPED_PREOPEN_STAGE_IDS = (
    'master_refresh',
    'pykrx_universe_refresh',
)
SIGNAL_DECIDER_AGENT_NAME = 'signal-decider'
SEOUL_TZ = ZoneInfo('Asia/Seoul')
PERSISTED_OUTPUT_FALLBACK_MODES = {
    'derived_from_daily_signal_features',
    'derived_from_external_news_items',
    'derived_from_signal_decider_outputs',
}
RECOVERABLE_PERSISTED_OUTPUT_STAGE_IDS = {
    'signal_features',
    'external_news_collect',
    'external_news_enrich',
}
RECOVERABLE_STAGE_HISTORY_IDS = {
    *RECOVERABLE_PERSISTED_OUTPUT_STAGE_IDS,
    'agent_feedback_summaries',
    'signal_decider',
    'signal_decider_evaluations',
    'next_open_execution_plan',
}
EXACT_COMPLETED_RECOVERY_STAGE_HISTORY_IDS = {
    'agent_feedback_summaries',
    'signal_decider',
    'signal_decider_evaluations',
    'next_open_execution_plan',
}
OFFICIAL_STAGE_HISTORY_INCOMPLETE_STATUSES = {
    'running',
    'partial',
    'failed',
    'blocked',
    'interrupted',
    'abandoned',
}
TERMINAL_OFFICIAL_RUN_STATUSES = {
    'abandoned',
    'blocked',
    'cancelled',
    'completed',
    'completed_with_errors',
    'failed',
    'interrupted',
    'partial',
    'skipped',
    'waiting',
}


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_pipeline_pykrx_run_rows(*, trade_date: str) -> list[dict[str, Any]]:
    daily_rows = list_eod_collection_runs(
        market='KRX',
        trade_date=trade_date,
        source='pykrx',
        excluded_run_metadata_modes=('historical_backfill',),
        limit=10,
    )
    if daily_rows:
        return daily_rows
    return list_eod_collection_runs(
        market='KRX',
        trade_date=trade_date,
        source='pykrx',
        limit=1,
    )


def _select_pipeline_pykrx_run(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    return select_preferred_eod_run(rows)


def _load_latest_pipeline_pykrx_run(*, trade_date: str) -> dict[str, Any] | None:
    # Status reads must remain observational. Orphan reconciliation mutates
    # EOD run state and is only safe from the scheduler path while it holds the
    # provider advisory lock.
    return _select_pipeline_pykrx_run(
        _load_pipeline_pykrx_run_rows(trade_date=trade_date)
    )


def _load_latest_fundamentals_refresh_run(*, market_date: str, fs_div: str) -> dict[str, Any] | None:
    payload = dart_fundamentals_capture_service.inspect_daily_refresh_status(
        runs_limit=100,
        market_date=date.fromisoformat(market_date),
        fs_div=fs_div,
    )
    latest_run = payload.get('latest_run')
    if not isinstance(latest_run, dict):
        return None
    return {
        **latest_run,
        '_scope_evidence_runs': [
            item for item in payload.get('runs') or [] if isinstance(item, dict)
        ],
    }


def _load_latest_daily_pipeline_run(*, market_date: str | None) -> dict[str, Any] | None:
    try:
        return daily_pipeline_run_state_service.get_latest_run(market_date=market_date)
    except SQLAlchemyError:
        return None


def _noop_factor_stage_counts_loader(*, market_date: str) -> dict[str, dict[str, Any]]:
    return {}


def _noop_row_count_stage_counts_loader(*, market_date: str) -> dict[str, dict[str, Any]]:
    return {}


def _noop_price_bar_count_loader(*args, market_date: str | None = None, **kwargs) -> int:
    return 0


def _noop_feedback_run_loader(*, market_date: str) -> dict[str, Any] | None:
    return None


def _noop_stage_history_loader(*, market_date: str) -> dict[str, dict[str, Any]]:
    return {}


def _noop_pykrx_enrichment_loader(
    *,
    market_date: str,
    symbols: list[str] | None = None,
) -> dict[str, Any]:
    return {}


def _noop_persisted_output_counts_loader(*, market_date: str) -> dict[str, Any]:
    return {}


def _noop_row_count_scope_gap_loader(
    *,
    stage_id: str,
    market_date: str,
    symbols: list[str],
) -> dict[str, Any]:
    return {
        'missing_count': None,
        'missing_symbols_sample': [],
        'missing_symbol_details_sample': [],
    }


def _load_pykrx_enrichment_coverage(
    *,
    market_date: str,
    symbols: list[str] | None = None,
) -> dict[str, Any]:
    return pykrx_enrichment_service.inspect(
        market_date=market_date,
        categories=DEFAULT_PYKRX_ENRICHMENT_CATEGORIES,
        symbols=symbols,
        limit=1,
    ).get('coverage') or {}


def _load_daily_pipeline_stage_history(*, market_date: str) -> dict[str, dict[str, Any]]:
    normalized_market_date = date.fromisoformat(market_date)
    latest_by_stage: dict[str, dict[str, Any]] = {}
    with session_scope() as session:
        rows = session.execute(
            select(DailyPipelineRun, DailyPipelineStageRun)
            .join(DailyPipelineStageRun, DailyPipelineStageRun.pipeline_run_id == DailyPipelineRun.id)
            .where(DailyPipelineRun.market_date == normalized_market_date)
            .where(DailyPipelineStageRun.market_date == normalized_market_date)
            .where(DailyPipelineStageRun.stage_id.in_(PIPELINE_STAGE_ORDER))
            .order_by(
                DailyPipelineStageRun.updated_at.desc().nullslast(),
                DailyPipelineRun.started_at.desc().nullslast(),
                DailyPipelineStageRun.id.desc(),
            )
        ).all()
        for run_row, stage_row in rows:
            if stage_row.stage_id in latest_by_stage:
                continue
            latest_by_stage[stage_row.stage_id] = {
                'run': daily_pipeline_run_state_service._serialize_run(run_row),
                'stage': daily_pipeline_run_state_service._serialize_stage(stage_row),
            }
    return latest_by_stage


def _load_factor_stage_counts(*, market_date: str) -> dict[str, dict[str, Any]]:
    normalized_market_date = date.fromisoformat(market_date)
    counts: dict[str, dict[str, Any]] = {}
    with session_scope() as session:
        for stage_id, model in FACTOR_STAGE_MODELS.items():
            persisted, updated_at = session.execute(
                select(
                    func.count(model.id),
                    func.max(model.captured_at),
                )
                .select_from(model)
                .where(model.market_date == normalized_market_date)
            ).one()
            counts[stage_id] = {
                'persisted': int(persisted or 0),
                'updated_at': updated_at.isoformat() if updated_at else None,
            }
    return counts


def _load_signal_feature_counts(*, market_date: str) -> dict[str, Any]:
    normalized_market_date = date.fromisoformat(market_date)
    with session_scope() as session:
        rows = session.execute(
            select(
                DailySignalFeature.signal_count,
                DailySignalFeature.feature_payload,
                DailySignalFeature.captured_at,
            )
            .select_from(DailySignalFeature)
            .where(DailySignalFeature.market_date == normalized_market_date)
        ).all()
    semantic_ready_count = sum(
        1
        for row in rows
        if _signal_feature_row_is_semantically_ready(
            signal_count=row.signal_count,
            feature_payload=row.feature_payload,
        )
    )
    direct_signal_rows = sum(1 for row in rows if int(row.signal_count or 0) > 0)
    fallback_usable_rows = sum(
        1
        for row in rows
        if isinstance(row.feature_payload, dict)
        and _payload_has_content(row.feature_payload.get('fallback_features'))
    )
    updated_at = max(
        (row.captured_at for row in rows if row.captured_at is not None),
        default=None,
    )
    return {
        'persisted': len(rows),
        'semantic_ready_count': semantic_ready_count,
        'semantic_unready_count': len(rows) - semantic_ready_count,
        'direct_signal_rows': direct_signal_rows,
        'fallback_usable_rows': fallback_usable_rows,
        'fallback_only_rows': sum(
            1
            for row in rows
            if int(row.signal_count or 0) <= 0
            and isinstance(row.feature_payload, dict)
            and _payload_has_content(
                row.feature_payload.get('fallback_features')
            )
        ),
        'updated_at': updated_at.isoformat() if updated_at else None,
    }


def _signal_feature_row_is_semantically_ready(
    *,
    signal_count: Any,
    feature_payload: Any,
) -> bool:
    if int(signal_count or 0) > 0:
        return True
    if not isinstance(feature_payload, dict):
        return False
    fallback_features = feature_payload.get('fallback_features')
    if not isinstance(fallback_features, dict):
        return False
    return _payload_has_content(fallback_features)


def _payload_has_content(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, dict):
        return any(_payload_has_content(item) for item in value.values())
    if isinstance(value, list):
        return any(_payload_has_content(item) for item in value)
    return True


def _load_macro_context_counts(*, market_date: str) -> dict[str, Any]:
    context = macro_context_service.get_context(as_of_date=market_date)
    series = context.get('series') if isinstance(context.get('series'), list) else []
    available_series = [
        item
        for item in series
        if isinstance(item, dict) and str(item.get('status') or '').strip().lower() in {'available', 'stale'}
    ]
    missing_series = [
        item.get('series_id')
        for item in series
        if isinstance(item, dict) and str(item.get('status') or '').strip().lower() == 'missing'
    ]
    stale_series = [
        item.get('series_id')
        for item in series
        if isinstance(item, dict) and str(item.get('status') or '').strip().lower() == 'stale'
    ]
    return {
        'persisted': len(available_series),
        'required_series_count': len(series),
        'updated_at': context.get('latest_fetched_at'),
        'context_status': context.get('status'),
        'source_statuses': context.get('source_statuses') or {},
        'macro_regime_summary': context.get('macro_regime_summary') or {},
        'usage_policy': context.get('usage_policy') or {},
        'missing_series': [series_id for series_id in missing_series if series_id],
        'stale_series': [series_id for series_id in stale_series if series_id],
        'freshness': context.get('freshness') or {},
    }


def _market_date_utc_bounds(market_date: str) -> tuple[datetime, datetime]:
    normalized_market_date = date.fromisoformat(market_date)
    start = datetime.combine(normalized_market_date, time.min, tzinfo=SEOUL_TZ).astimezone(timezone.utc)
    return start, start + timedelta(days=1)


def _load_external_news_counts(*, market_date: str) -> dict[str, Any]:
    start, end = _market_date_utc_bounds(market_date)
    with session_scope() as session:
        rows = session.execute(
            select(
                ExternalNewsItem.id,
                ExternalNewsItemSymbol.symbol_id,
                ExternalNewsItemSymbol.associated_at,
                ExternalNewsItem.payload,
            )
            .join(
                ExternalNewsItemSymbol,
                ExternalNewsItemSymbol.external_news_item_id == ExternalNewsItem.id,
            )
            .where(ExternalNewsItemSymbol.associated_at >= start)
            .where(ExternalNewsItemSymbol.associated_at < end)
        ).all()
        item_ids = {int(row.id) for row in rows}
        classification_rows = (
            session.execute(
                select(
                    ExternalNewsItemClassification.id,
                    ExternalNewsItemClassification.external_news_item_id,
                    ExternalNewsItemClassification.classifier_version,
                    ExternalNewsItemClassification.classified_at,
                    ExternalNewsItemClassification.classification_payload,
                ).where(
                    ExternalNewsItemClassification.external_news_item_id.in_(item_ids)
                ).order_by(
                    ExternalNewsItemClassification.external_news_item_id,
                    ExternalNewsItemClassification.classified_at.desc(),
                    ExternalNewsItemClassification.id.desc(),
                )
            ).all()
            if item_ids
            else []
        )
    symbol_ids = {int(row.symbol_id) for row in rows}
    contract_classified_ids: set[int] = set()
    legacy_classified_ids: set[int] = set()
    outdated_classified_ids: set[int] = set()
    for row in rows:
        payload = row.payload if isinstance(row.payload, dict) else {}
        classification = payload.get('classification')
        # Embedded classifier output has no trustworthy observation timestamp;
        # report it as legacy until enrichment creates a timestamped sidecar.
        if isinstance(classification, dict) and classification:
            legacy_classified_ids.add(int(row.id))
    latest_classification_rows: dict[int, Any] = {}
    latest_current_classification_rows: dict[int, Any] = {}
    for row in classification_rows:
        item_id = int(row.external_news_item_id)
        latest_classification_rows.setdefault(item_id, row)
        if row.classifier_version == CLASSIFIER_VERSION:
            latest_current_classification_rows.setdefault(item_id, row)
        else:
            outdated_classified_ids.add(item_id)
    for item_id, row in latest_current_classification_rows.items():
        classification = row.classification_payload
        if (
            isinstance(classification, dict)
            and row.classifier_version == CLASSIFIER_VERSION
            and classification.get('classifier') == CLASSIFIER_VERSION
            and external_news_classification_has_schema_contract(classification)
        ):
            contract_classified_ids.add(item_id)
            legacy_classified_ids.discard(item_id)
            outdated_classified_ids.discard(item_id)
    for item_id, row in latest_classification_rows.items():
        if item_id in contract_classified_ids:
            continue
        classification = row.classification_payload
        if isinstance(classification, dict) and classification:
            legacy_classified_ids.add(item_id)
    latest_updated_at = max(
        (
            timestamp
            for timestamp in (
                *(row.associated_at for row in rows),
                *(row.classified_at for row in classification_rows),
            )
            if timestamp is not None
        ),
        default=None,
    )
    return {
        'items': len(item_ids),
        'symbols': len(symbol_ids),
        'classified_items': len(contract_classified_ids),
        'contract_classified_items': len(contract_classified_ids),
        'legacy_classified_items': len(legacy_classified_ids),
        'outdated_classified_items': len(outdated_classified_ids),
        'classifier_version': CLASSIFIER_VERSION,
        'sample_limited': False,
        'sample_limit': None,
        'updated_at': latest_updated_at.isoformat() if latest_updated_at else None,
    }


def _load_signal_decider_output_counts(*, market_date: str) -> dict[str, Any]:
    normalized_market_date = date.fromisoformat(market_date)
    with session_scope() as session:
        feedback_rows, feedback_symbols, latest_feedback_at = session.execute(
            select(
                func.count(AgentContextFeedback.id),
                func.count(func.distinct(AgentContextFeedback.symbol)),
                func.max(AgentContextFeedback.received_at),
            )
            .select_from(AgentContextFeedback)
            .where(AgentContextFeedback.market_date == normalized_market_date)
            .where(AgentContextFeedback.agent_name == SIGNAL_DECIDER_AGENT_NAME)
        ).one()
        snapshot_rows, snapshot_symbols, latest_snapshot_at = session.execute(
            select(
                func.count(SignalDeciderDecisionSnapshot.id),
                func.count(func.distinct(SignalDeciderDecisionSnapshot.symbol)),
                func.max(SignalDeciderDecisionSnapshot.created_at),
            )
            .select_from(SignalDeciderDecisionSnapshot)
            .where(SignalDeciderDecisionSnapshot.market_date == normalized_market_date)
            .where(SignalDeciderDecisionSnapshot.agent_name == SIGNAL_DECIDER_AGENT_NAME)
        ).one()
    updated_at_candidates = [value for value in (latest_feedback_at, latest_snapshot_at) if value is not None]
    updated_at = max(updated_at_candidates).isoformat() if updated_at_candidates else None
    return {
        'feedback_rows': int(feedback_rows or 0),
        'feedback_symbols': int(feedback_symbols or 0),
        'decision_snapshots': int(snapshot_rows or 0),
        'decision_snapshot_symbols': int(snapshot_symbols or 0),
        'updated_at': updated_at,
    }


def _load_row_count_stage_counts(
    *,
    market_date: str,
    include_missing_diagnostics: bool = True,
) -> dict[str, dict[str, Any]]:
    normalized_market_date = date.fromisoformat(market_date)
    counts: dict[str, dict[str, Any]] = {}
    with session_scope() as session:
        for stage_id, model in ROW_COUNT_STAGE_MODELS.items():
            aggregate_columns = [
                func.count(model.id),
                func.max(model.captured_at),
            ]
            if stage_id == 'low_risk_factors':
                decision_ready = model.low_risk_classification.in_(
                    (
                        'eligible',
                        'watchlist',
                        'ineligible',
                        'liquidity_excluded',
                    )
                )
                aggregate_columns.extend(
                    (
                        func.count(model.id).filter(decision_ready),
                        func.count(model.id).filter(
                            model.low_risk_classification
                            == 'insufficient_history'
                        ),
                        func.max(model.captured_at).filter(decision_ready),
                    )
                )
            aggregate = session.execute(
                select(*aggregate_columns)
                .select_from(model)
                .where(model.market_date == normalized_market_date)
            ).one()
            persisted, updated_at = aggregate[:2]
            persisted_count = int(persisted or 0)
            if not include_missing_diagnostics:
                missing_diagnostics = {
                    'missing_count': None,
                    'missing_symbols_sample': [],
                    'missing_symbol_details_sample': [],
                    'missing_sample_limit': ROW_COUNT_STAGE_MISSING_SAMPLE_LIMIT,
                    'missing_diagnostics_omitted': True,
                }
            else:
                missing_diagnostics = _load_row_count_missing_diagnostics_for_model(
                    session=session,
                    model=model,
                    market_date=normalized_market_date,
                    limit=ROW_COUNT_STAGE_MISSING_SAMPLE_LIMIT,
                )
            counts[stage_id] = {
                'persisted': persisted_count,
                'updated_at': updated_at.isoformat() if updated_at else None,
                **missing_diagnostics,
            }
            if stage_id == 'low_risk_factors':
                ready_count, insufficient_history_count, latest_ready_at = (
                    aggregate[2:]
                )
                counts[stage_id].update(
                    {
                        'decision_ready_count': int(ready_count or 0),
                        'insufficient_history_count': int(
                            insufficient_history_count or 0
                        ),
                        'latest_decision_ready_at': (
                            latest_ready_at.isoformat()
                            if latest_ready_at
                            else None
                        ),
                    }
                )
    return counts


def _load_row_count_missing_diagnostics_for_model(
    *,
    session: Any,
    model: Any,
    market_date: date,
    limit: int,
) -> dict[str, Any]:
    price_symbol_ids = set(
        session.execute(
            select(DailyPriceBar.symbol_id)
            .where(DailyPriceBar.trade_date == market_date)
        ).scalars().all()
    )
    persisted_symbol_ids = set(
        session.execute(
            select(model.symbol_id)
            .where(model.market_date == market_date)
        ).scalars().all()
    )
    present_symbol_ids = price_symbol_ids & persisted_symbol_ids
    missing_symbol_ids = sorted(price_symbol_ids - persisted_symbol_ids)
    missing_count = len(missing_symbol_ids)
    sample_rows = session.execute(
        select(Symbol.symbol, Symbol.name_ko, Symbol.market)
        .where(Symbol.id.in_(missing_symbol_ids[:limit]))
        .order_by(Symbol.symbol.asc())
    ).all() if missing_symbol_ids else []
    details_sample = [
        {
            'symbol': row.symbol,
            'name_ko': row.name_ko,
            'market': row.market,
        }
        for row in sample_rows
    ]
    return {
        'membership_evidence_version': 'row_count_scope_gap_v1',
        'membership_authoritative': True,
        'membership_requested_count': len(price_symbol_ids),
        'membership_present_count': len(present_symbol_ids),
        'membership_extra_count': len(persisted_symbol_ids - price_symbol_ids),
        'missing_count': int(missing_count),
        'missing_symbols_sample': [item['symbol'] for item in details_sample],
        'missing_symbol_details_sample': details_sample,
        'missing_sample_limit': limit,
    }


def _load_row_count_stage_scope_gap(
    *,
    stage_id: str,
    market_date: str,
    symbols: list[str],
) -> dict[str, Any]:
    model = PREREQUISITE_SCOPE_STAGE_MODELS.get(stage_id)
    required_models = PREREQUISITE_SCOPE_STAGE_REQUIRED_MODELS.get(
        stage_id,
        (model,) if model is not None else (),
    )
    normalized_symbols = [str(symbol or '').strip().upper() for symbol in symbols if str(symbol or '').strip()]
    if model is None or not normalized_symbols:
        return {
            'evidence_version': 'row_count_scope_gap_v1',
            'authoritative': True,
            'requested_count': len(set(normalized_symbols)),
            'present_count': len(set(normalized_symbols)),
            'missing_count': 0,
            'missing_symbols_sample': [],
            'missing_symbol_details_sample': [],
        }

    normalized_market_date = date.fromisoformat(market_date)
    with session_scope() as session:
        requested_symbol_rows = session.execute(
            select(Symbol.id, Symbol.symbol)
            .where(Symbol.symbol.in_(normalized_symbols))
        ).all()
        symbol_by_id = {
            int(row.id): str(row.symbol).strip().upper()
            for row in requested_symbol_rows
        }
        requested_symbol_ids = set(symbol_by_id)
        date_column = (
            model.trade_date
            if stage_id == 'price_collection'
            else model.market_date
        )
        if stage_id == 'signal_features':
            persisted_rows = (
                session.execute(
                    select(
                        model.symbol_id,
                        model.signal_count,
                        model.feature_payload,
                    )
                    .where(date_column == normalized_market_date)
                    .where(model.symbol_id.in_(requested_symbol_ids))
                ).all()
                if requested_symbol_ids
                else []
            )
            persisted_symbol_ids = {
                int(row.symbol_id)
                for row in persisted_rows
                if _signal_feature_row_is_semantically_ready(
                    signal_count=row.signal_count,
                    feature_payload=row.feature_payload,
                )
            }
        elif stage_id in PREREQUISITE_SCOPE_STAGE_REQUIRED_MODELS:
            required_model_symbol_ids = [
                set(
                    session.execute(
                        select(required_model.symbol_id)
                        .where(
                            required_model.market_date
                            == normalized_market_date
                        )
                        .where(
                            required_model.symbol_id.in_(
                                requested_symbol_ids
                            )
                        )
                    ).scalars().all()
                )
                for required_model in required_models
            ]
            persisted_symbol_ids = (
                set.intersection(*required_model_symbol_ids)
                if required_model_symbol_ids
                else set()
            )
        else:
            persisted_symbol_ids = set(
                session.execute(
                    select(model.symbol_id)
                    .where(date_column == normalized_market_date)
                    .where(model.symbol_id.in_(requested_symbol_ids))
                ).scalars().all()
            ) if requested_symbol_ids else set()
        missing_symbols = sorted(
            {
                *(
                    symbol_by_id[symbol_id]
                    for symbol_id in requested_symbol_ids - persisted_symbol_ids
                ),
                *(set(normalized_symbols) - set(symbol_by_id.values())),
            }
        )
        missing_count = len(missing_symbols)
        sample_rows = session.execute(
            select(Symbol.symbol, Symbol.name_ko, Symbol.market)
            .where(Symbol.symbol.in_(missing_symbols[:ROW_COUNT_STAGE_MISSING_SAMPLE_LIMIT]))
            .order_by(Symbol.symbol.asc())
        ).all() if missing_symbols else []
    details_sample = [
        {
            'symbol': row.symbol,
            'name_ko': row.name_ko,
            'market': row.market,
        }
        for row in sample_rows
    ]
    detail_symbols = {item['symbol'] for item in details_sample}
    for symbol in missing_symbols[:ROW_COUNT_STAGE_MISSING_SAMPLE_LIMIT]:
        if symbol not in detail_symbols:
            details_sample.append(
                {'symbol': symbol, 'name_ko': None, 'market': None}
            )
    return {
        'evidence_version': 'row_count_scope_gap_v1',
        'authoritative': True,
        'missing_count': int(missing_count or 0),
        'present_count': max(len(set(normalized_symbols)) - missing_count, 0),
        'requested_count': len(set(normalized_symbols)),
        'coverage_ratio': round(
            max(len(set(normalized_symbols)) - missing_count, 0)
            / len(set(normalized_symbols)),
            6,
        ),
        'missing_symbols_sample': missing_symbols[:ROW_COUNT_STAGE_MISSING_SAMPLE_LIMIT],
        'missing_symbol_details_sample': details_sample,
        'semantic_readiness_required': stage_id == 'signal_features',
        'required_row_components': [
            required_model.__tablename__
            for required_model in required_models
        ],
    }


def _coerce_int(*values: Any, default: int = 0) -> int:
    for value in values:
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return default


def _strict_nonnegative_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _validated_row_count_scope_gap_evidence(
    value: Any,
    *,
    requested_count: int,
) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    evidence_requested_count = _strict_nonnegative_int(
        value.get('requested_count')
    )
    present_count = _strict_nonnegative_int(value.get('present_count'))
    missing_count = _strict_nonnegative_int(value.get('missing_count'))
    if (
        value.get('authoritative') is not True
        or value.get('evidence_version') != 'row_count_scope_gap_v1'
        or evidence_requested_count != requested_count
        or present_count is None
        or missing_count is None
        or present_count + missing_count != requested_count
    ):
        return None
    return dict(value)


def _resolve_exact_run_universe_symbols(
    run: dict[str, Any] | None,
) -> list[str]:
    metadata = get_run_metadata(run)
    universe_snapshot = (
        metadata.get('universe_snapshot')
        if isinstance(metadata.get('universe_snapshot'), dict)
        else {}
    )
    requested_symbols = EODPipelineStatusService._normalize_symbol_list(
        metadata.get('requested_symbols')
    )
    raw_markets = metadata.get('markets')
    if not isinstance(raw_markets, (list, tuple)):
        raw_markets = universe_snapshot.get('markets')
    markets = [
        str(item or '').strip().upper()
        for item in raw_markets or []
        if str(item or '').strip()
    ]
    validated = validate_market_membership_snapshot(
        universe_snapshot.get('market_membership'),
        requested_symbols=requested_symbols,
        markets=markets,
    )
    if validated is None:
        return []
    return sorted(
        symbol
        for market_symbols in validated.values()
        for symbol in market_symbols
    )


def _strict_non_negative_int(
    payload: dict[str, Any],
    key: str,
) -> int | None:
    if key not in payload:
        return None
    value = payload.get(key)
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    return None


def _strict_positive_unique_ints(value: Any) -> tuple[int, ...] | None:
    if not isinstance(value, (list, tuple)) or not value:
        return None
    normalized: list[int] = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, int) or item <= 0:
            return None
        normalized.append(item)
    if len(set(normalized)) != len(normalized):
        return None
    return tuple(normalized)


def _recovery_payload_has_rejected_control_marker(
    *payloads: dict[str, Any],
) -> bool:
    for payload in payloads:
        safe_skip = payload.get('safe_skip')
        skipped = payload.get('skipped')
        triggered = payload.get('triggered')
        if safe_skip is True or str(safe_skip).strip().lower() == 'true':
            return True
        if skipped is True or str(skipped).strip().lower() == 'true':
            return True
        if triggered is False or str(triggered).strip().lower() == 'false':
            return True
    return False


def _recovery_payload_has_invalid_optional_controls(
    payload: dict[str, Any],
) -> bool:
    expected_booleans = {
        'safe_skip': False,
        'skipped': False,
        'triggered': True,
    }
    for key, expected in expected_booleans.items():
        if key in payload and payload.get(key) is not expected:
            return True
    if (
        'warning_count' in payload
        and _strict_non_negative_int(payload, 'warning_count') != 0
    ):
        return True
    if 'warnings' in payload:
        warnings = payload.get('warnings')
        if not isinstance(warnings, (list, tuple, set, dict)) or bool(
            warnings
        ):
            return True
    return False


def _recovery_optional_counts_are_strict_zero(
    payload: dict[str, Any],
    keys: tuple[str, ...],
) -> bool:
    return all(
        key not in payload or _strict_non_negative_int(payload, key) == 0
        for key in keys
    )


def _recovery_optional_collection_is_strict_empty(
    payload: dict[str, Any],
    key: str,
) -> bool:
    if key not in payload:
        return True
    value = payload.get(key)
    return isinstance(value, (list, tuple, set, dict)) and not value


def _recovery_payload_has_reported_failure(payload: dict[str, Any]) -> bool:
    for key in (
        'failed',
        'failed_count',
        'failed_items',
        'error_count',
        'errors',
        'error',
        'error_message',
        'not_computed_count',
    ):
        if key not in payload:
            continue
        value = payload.get(key)
        if value is None or value is False:
            continue
        if isinstance(value, (dict, list, tuple, set)):
            if value:
                return True
            continue
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                continue
            try:
                if int(stripped) != 0:
                    return True
            except ValueError:
                return True
            continue
        try:
            if int(value) != 0:
                return True
        except (TypeError, ValueError):
            return True
    return False


def _coerce_iso_timestamp(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return None


def _parse_iso_timestamp(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _latest_iso_timestamp(*values: Any) -> str | None:
    latest_text: str | None = None
    latest_value: datetime | None = None
    for value in values:
        timestamp = _coerce_iso_timestamp(value)
        if timestamp is None:
            continue
        parsed = _parse_iso_timestamp(timestamp)
        if parsed is None:
            if latest_value is None and (latest_text is None or timestamp > latest_text):
                latest_text = timestamp
            continue
        if latest_value is None or parsed > latest_value:
            latest_value = parsed
            latest_text = timestamp
    return latest_text


def _progress_summary_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    progress_summary = payload.get('progress_summary')
    if isinstance(progress_summary, dict):
        return dict(progress_summary)
    return dict(payload)


def _is_summarizer_feedback_runtime_state(state: Any) -> bool:
    requested_roles = getattr(state, 'requested_roles', None)
    if not isinstance(requested_roles, (list, tuple)):
        return False
    normalized_roles = sorted(
        str(role or '').strip()
        for role in requested_roles
        if str(role or '').strip()
    )
    return normalized_roles == sorted(SUMMARIZER_AGENT_NAMES)


def _load_latest_feedback_run_summary(*, market_date: str) -> dict[str, Any] | None:
    normalized_market_date = date.fromisoformat(market_date)
    try:
        from app.agent_context.run_state_service import agent_context_feedback_run_state_service
        from app.agent_context.runtime_state import runtime_feedback_state_store
    except Exception:
        return None

    runtime_state = runtime_feedback_state_store.get_run(
        normalized_market_date,
        logical=True,
    )
    if not _is_summarizer_feedback_runtime_state(runtime_state):
        runtime_state = runtime_feedback_state_store.get_run(normalized_market_date)
    if not _is_summarizer_feedback_runtime_state(runtime_state):
        runtime_state = None
    if runtime_state is not None:
        summary = runtime_state.to_progress_summary()
        requested_symbols = getattr(runtime_state, 'requested_symbols', None)
        return {
            'source': 'runtime_feedback_state',
            'run_id': getattr(runtime_state, 'run_id', None) or summary.get('run_id'),
            'status': summary.get('run_status') or getattr(runtime_state, 'status', None),
            'requested_symbols': list(requested_symbols or []),
            'summary': summary,
        }

    run_snapshot = agent_context_feedback_run_state_service.find_latest_run(
        market_date=normalized_market_date,
        requested_roles=list(SUMMARIZER_AGENT_NAMES),
        request_modes=[DAILY_RETRY_CYCLE_MODE],
    )
    if run_snapshot is None:
        run_snapshot = agent_context_feedback_run_state_service.find_latest_run(
            market_date=normalized_market_date,
            requested_roles=list(SUMMARIZER_AGENT_NAMES),
        )
    if run_snapshot is None:
        return None

    requested_symbols = agent_context_feedback_run_state_service.resolve_run_requested_symbols(
        run_snapshot=run_snapshot,
    )
    try:
        from app.agent_context.progress_service import agent_context_progress_service

        progress = agent_context_progress_service.get_feedback_progress(
            market_date=normalized_market_date,
            run_id=run_snapshot.get('run_id'),
        )
    except Exception:
        progress = None
    if isinstance(progress, dict) and isinstance(progress.get('summary'), dict):
        summary = dict(progress['summary'])
        status = str(summary.get('run_status') or run_snapshot.get('status') or '').strip().lower()
        if not status:
            status = 'running' if run_snapshot.get('completed_at') is None else 'partial'
        return {
            'source': 'agent_context_feedback_runs',
            'run_id': run_snapshot.get('run_id'),
            'status': status,
            'requested_symbols': requested_symbols or [],
            'summary': summary,
            'run_snapshot': run_snapshot,
        }

    summary_payload = (
        dict(run_snapshot.get('summary_payload'))
        if isinstance(run_snapshot.get('summary_payload'), dict)
        else {}
    )
    summary = _progress_summary_from_payload(summary_payload)
    status = str(summary.get('run_status') or run_snapshot.get('status') or '').strip().lower()
    if not status:
        status = 'running' if run_snapshot.get('completed_at') is None else 'partial'
    summary.setdefault('run_status', status)
    summary.setdefault('run_table_status', run_snapshot.get('status'))
    summary.setdefault('completed_feedback_count', run_snapshot.get('completed_count'))
    summary.setdefault('failed_items', run_snapshot.get('failed_count'))
    return {
        'source': 'agent_context_feedback_runs',
        'run_id': run_snapshot.get('run_id'),
        'status': status,
        'requested_symbols': requested_symbols or [],
        'summary': summary,
        'run_snapshot': run_snapshot,
    }


def _stage_label(stage_id: str) -> str:
    if stage_id in STAGE_LABELS:
        return STAGE_LABELS[stage_id]
    definition = STAGE_DEFINITIONS.get(stage_id)
    return definition.label if definition is not None else stage_id.replace('_', ' ')


@dataclass
class EODPipelineStatusService:
    market_state_loader: Callable = get_market_state
    pykrx_run_loader: Callable = _load_latest_pipeline_pykrx_run
    pykrx_stage_counts_loader: Callable = load_eod_run_stage_counts
    live_symbol_count_loader: Callable = count_live_symbols
    fundamentals_refresh_loader: Callable = _load_latest_fundamentals_refresh_run
    factor_stage_counts_loader: Callable = _noop_factor_stage_counts_loader
    row_count_stage_counts_loader: Callable = _noop_row_count_stage_counts_loader
    price_bar_count_loader: Callable = _noop_price_bar_count_loader
    price_context_count_loader: Callable | None = None
    technical_snapshot_count_loader: Callable = _noop_price_bar_count_loader
    action_summary_count_loader: Callable = _noop_price_bar_count_loader
    pykrx_enrichment_loader: Callable = _noop_pykrx_enrichment_loader
    official_run_loader: Callable = _load_latest_daily_pipeline_run
    official_stage_history_loader: Callable = _noop_stage_history_loader
    feedback_run_loader: Callable = _noop_feedback_run_loader
    signal_feature_counts_loader: Callable = _noop_persisted_output_counts_loader
    external_news_counts_loader: Callable = _noop_persisted_output_counts_loader
    macro_context_counts_loader: Callable = _noop_persisted_output_counts_loader
    signal_decider_counts_loader: Callable = _noop_persisted_output_counts_loader
    row_count_scope_gap_loader: Callable = _noop_row_count_scope_gap_loader

    def read(
        self,
        *,
        market_date: str | None = None,
        fs_div: str | None = None,
        include_feedback_context: bool = True,
        feedback_run_context: dict[str, Any] | None = None,
        include_feedback_stage: bool = True,
        include_row_count_diagnostics: bool = True,
        include_pykrx_enrichment_coverage: bool = True,
        scope_symbols: list[str] | tuple[str, ...] | None = None,
    ) -> dict[str, Any]:
        market_state = self.market_state_loader()
        explicit_market_date = (
            date.fromisoformat(market_date).isoformat()
            if market_date is not None
            else None
        )
        official_run = self.official_run_loader(market_date=explicit_market_date)
        official_market_date = (
            (official_run or {}).get('market_date')
            if explicit_market_date is None
            else None
        )
        try:
            latest_persisted_market_date = (
                date.fromisoformat(str(official_market_date)[:10]).isoformat()
                if official_market_date is not None
                else None
            )
        except ValueError:
            latest_persisted_market_date = None
        resolved_market_date = (
            explicit_market_date
            or latest_persisted_market_date
            or date.fromisoformat(str(market_state.server_time)[:10]).isoformat()
        )
        market_session = build_market_session_metadata(
            requested_date=resolved_market_date,
            current_state=market_state,
        )
        resolved_fs_div = (fs_div or settings.dart_fundamentals_default_fs_div).strip().upper()
        pykrx_run = self.pykrx_run_loader(trade_date=resolved_market_date)
        official_stage_history = dict(
            self.official_stage_history_loader(market_date=resolved_market_date) or {}
        )
        feedback_run = feedback_run_context
        if feedback_run is None and include_feedback_context:
            feedback_run = self.feedback_run_loader(market_date=resolved_market_date)
        live_symbols_total = int(self.live_symbol_count_loader() or 0)
        normalized_scope_symbols = self._normalize_symbol_list(
            list(scope_symbols) if scope_symbols is not None else []
        )
        exact_run_universe_symbols = _resolve_exact_run_universe_symbols(pykrx_run)
        run_metadata = get_run_metadata(pykrx_run)
        unverified_run_symbols = self._normalize_symbol_list(
            run_metadata.get('requested_symbols')
        )
        pykrx_stage_counts = self.pykrx_stage_counts_loader(pykrx_run['id']) if pykrx_run else {}
        pykrx_stages = self._build_pykrx_stages(
            market_date=resolved_market_date,
            run=pykrx_run,
            stage_counts=pykrx_stage_counts,
            live_symbols_total=live_symbols_total,
        )
        if include_pykrx_enrichment_coverage:
            try:
                pykrx_enrichment_coverage = dict(
                    self.pykrx_enrichment_loader(
                        market_date=resolved_market_date,
                        symbols=normalized_scope_symbols or None,
                    )
                    or {}
                )
            except TypeError:
                pykrx_enrichment_coverage = dict(
                    self.pykrx_enrichment_loader(
                        market_date=resolved_market_date
                    )
                    or {}
                )
        else:
            pykrx_enrichment_coverage = {}
        pykrx_enrichment_stage = self._build_pykrx_enrichment_stage(
            market_date=resolved_market_date,
            coverage=pykrx_enrichment_coverage,
            run=pykrx_run,
            scope_symbols=normalized_scope_symbols,
        )
        fundamentals_stage = self._build_fundamentals_stage(
            market_date=resolved_market_date,
            fs_div=resolved_fs_div,
            scope_symbols=normalized_scope_symbols,
        )
        try:
            row_count_stage_counts = dict(
                self.row_count_stage_counts_loader(
                    market_date=resolved_market_date,
                    include_missing_diagnostics=include_row_count_diagnostics,
                )
                or {}
            )
        except TypeError:
            row_count_stage_counts = dict(
                self.row_count_stage_counts_loader(market_date=resolved_market_date) or {}
            )
        if any(stage_id not in row_count_stage_counts for stage_id in FACTOR_STAGE_MODELS):
            legacy_factor_stage_counts = dict(
                self.factor_stage_counts_loader(market_date=resolved_market_date) or {}
            )
            for stage_id in FACTOR_STAGE_MODELS:
                if stage_id in row_count_stage_counts:
                    continue
                row_count_stage_counts[stage_id] = dict(
                    legacy_factor_stage_counts.get(stage_id) or {}
                )
        stored_price_rows = int(self.price_bar_count_loader(market_date=resolved_market_date) or 0)
        if stored_price_rows <= 0:
            stored_price_rows = int(dict(pykrx_stage_counts.get('price_collection') or {}).get('persisted') or 0)
        stored_snapshot_rows = int(
            self.technical_snapshot_count_loader(resolved_market_date) or 0
        )
        stored_action_only_rows = int(
            self.action_summary_count_loader(resolved_market_date) or 0
        )
        if self.price_context_count_loader is None:
            stored_technical_rows = max(
                stored_snapshot_rows,
                int(
                    dict(
                        pykrx_stage_counts.get('technical_snapshots') or {}
                    ).get('persisted')
                    or 0
                ),
            )
            stored_action_rows = max(
                stored_action_only_rows,
                int(
                    dict(
                        pykrx_stage_counts.get('action_summaries') or {}
                    ).get('persisted')
                    or 0
                ),
            )
        else:
            stored_price_context_rows = int(
                self.price_context_count_loader(resolved_market_date) or 0
            )
            stored_technical_rows = min(
                stored_price_context_rows,
                stored_snapshot_rows,
            )
            stored_action_rows = min(
                stored_price_context_rows,
                stored_snapshot_rows,
                stored_action_only_rows,
            )
        if exact_run_universe_symbols:
            expected_price_rows = len(exact_run_universe_symbols)
            expected_price_rows_source = 'validated_eod_run_market_membership'
        elif unverified_run_symbols:
            expected_price_rows = len(unverified_run_symbols)
            expected_price_rows_source = (
                'eod_run_requested_symbol_count_unverified'
            )
        elif _coerce_int((pykrx_run or {}).get('symbols_total')) > 0:
            expected_price_rows = _coerce_int(
                (pykrx_run or {}).get('symbols_total')
            )
            expected_price_rows_source = 'eod_run_symbol_total_unverified'
        elif live_symbols_total > 0:
            expected_price_rows = live_symbols_total
            expected_price_rows_source = 'live_symbol_count_unverified'
        else:
            expected_price_rows = stored_price_rows
            expected_price_rows_source = 'stored_rows_unverified'
        exact_universe_coverage = (
            {
                stage_id: self.row_count_scope_gap_loader(
                    stage_id=stage_id,
                    market_date=resolved_market_date,
                    symbols=exact_run_universe_symbols,
                )
                for stage_id in (
                    'price_collection',
                    'technical_snapshots',
                    'action_summaries',
                    'signal_features',
                    *ROW_COUNT_STAGE_MODELS,
                )
            }
            if exact_run_universe_symbols
            else {}
        )
        pykrx_stages = {
            **pykrx_stages,
            'price_collection': self._normalize_stored_pykrx_coverage_stage(
                pykrx_stages['price_collection'],
                persisted=stored_price_rows,
                expected_total=expected_price_rows,
                exact_scope_gap=exact_universe_coverage.get(
                    'price_collection'
                ),
                expected_total_source=expected_price_rows_source,
            ),
            'technical_snapshots': self._normalize_stored_pykrx_coverage_stage(
                pykrx_stages['technical_snapshots'],
                persisted=stored_technical_rows,
                expected_total=expected_price_rows,
                exact_scope_gap=exact_universe_coverage.get(
                    'technical_snapshots'
                ),
                expected_total_source=expected_price_rows_source,
            ),
            'action_summaries': self._normalize_stored_pykrx_coverage_stage(
                pykrx_stages['action_summaries'],
                persisted=stored_action_rows,
                expected_total=expected_price_rows,
                exact_scope_gap=exact_universe_coverage.get(
                    'action_summaries'
                ),
                expected_total_source=expected_price_rows_source,
            ),
        }
        for scope_stage_id in (
            'price_collection',
            'technical_snapshots',
            'action_summaries',
        ):
            pykrx_stages[scope_stage_id] = self._apply_scope_coverage_gate(
                stage_id=scope_stage_id,
                market_date=resolved_market_date,
                stage=pykrx_stages[scope_stage_id],
                scope_symbols=normalized_scope_symbols,
            )
        stages = {
            **pykrx_stages,
            'pykrx_enrichment': pykrx_enrichment_stage,
            'fundamentals_daily_refresh': fundamentals_stage,
        }
        price_stage = stages['price_collection']
        for stage_id in FILTER_STAGE_ORDER:
            stages[stage_id] = self._build_row_count_stage(
                stage_id=stage_id,
                market_date=resolved_market_date,
                price_stage=price_stage,
                price_rows_persisted=stored_price_rows,
                prior_stages=stages,
                aggregate=dict(row_count_stage_counts.get(stage_id) or {}),
                live_symbols_total=live_symbols_total,
                feedback_run=feedback_run,
                scope_symbols=normalized_scope_symbols,
            )
        for stage_id in DEFAULT_FACTOR_STAGE_ORDER:
            stages[stage_id] = self._build_row_count_stage(
                stage_id=stage_id,
                market_date=resolved_market_date,
                price_stage=price_stage,
                price_rows_persisted=stored_price_rows,
                prior_stages=stages,
                aggregate=dict(row_count_stage_counts.get(stage_id) or {}),
                live_symbols_total=live_symbols_total,
                feedback_run=feedback_run,
                scope_symbols=normalized_scope_symbols,
            )
        stages['signal_features'] = self._build_signal_features_stage(
            market_date=resolved_market_date,
            aggregate=dict(self.signal_feature_counts_loader(market_date=resolved_market_date) or {}),
            expected_total=expected_price_rows,
            scope_symbols=normalized_scope_symbols,
        )
        external_news_counts = dict(self.external_news_counts_loader(market_date=resolved_market_date) or {})
        stages['external_news_collect'] = self._build_external_news_collect_stage(
            market_date=resolved_market_date,
            aggregate=external_news_counts,
        )
        stages['external_news_enrich'] = self._build_external_news_enrich_stage(
            market_date=resolved_market_date,
            aggregate=external_news_counts,
            collect_stage=stages['external_news_collect'],
        )
        stages['macro_context_refresh'] = self._build_macro_context_stage(
            market_date=resolved_market_date,
            aggregate=dict(self.macro_context_counts_loader(market_date=resolved_market_date) or {}),
        )
        stages['signal_decider'] = self._build_signal_decider_stage(
            market_date=resolved_market_date,
            aggregate=dict(self.signal_decider_counts_loader(market_date=resolved_market_date) or {}),
        )
        for stage_id, coverage in exact_universe_coverage.items():
            if stage_id not in stages or not isinstance(coverage, dict):
                continue
            stages[stage_id] = self._apply_exact_universe_coverage_evidence(
                stage=stages[stage_id],
                coverage=coverage,
                expected_count=len(exact_run_universe_symbols),
                source_run_id=(pykrx_run or {}).get('id'),
            )
        stored_row_count_stage_overrides = {
            stage_id: {
                **stages[stage_id],
                'details': dict(stages[stage_id].get('details') or {}),
            }
            for stage_id in ROW_COUNT_STAGE_MODELS
            if stage_id in stages
            and int(dict(row_count_stage_counts.get(stage_id) or {}).get('persisted') or 0) > 0
        }
        for stage_id in PIPELINE_STAGE_ORDER:
            if stage_id in stages:
                continue
            stages[stage_id] = self._build_generic_pending_stage(
                stage_id=stage_id,
                market_date=resolved_market_date,
            )
        if include_feedback_stage and feedback_run is not None:
            stages['agent_feedback_summaries'] = self._build_agent_feedback_stage(
                market_date=resolved_market_date,
                feedback_run=feedback_run,
                fallback_stage=stages.get('agent_feedback_summaries') or {},
            )
        stages = self._overlay_live_eod_run_stage_progress(
            stages=stages,
            run=pykrx_run,
            market_date=resolved_market_date,
        )
        stages = {
            stage_id: stages[stage_id]
            for stage_id in PIPELINE_STAGE_ORDER
        }
        if official_stage_history:
            stages = self._overlay_official_stage_history(
                stages=stages,
                stage_history=official_stage_history,
                market_date=resolved_market_date,
            )
        if official_run is not None:
            stages = self._overlay_official_run_stages(
                stages=stages,
                official_run=official_run,
                market_date=resolved_market_date,
            )
        for stage_id, stored_stage in stored_row_count_stage_overrides.items():
            if stage_id in stages:
                current_stage = stages[stage_id]
                current_details = (
                    current_stage.get('details')
                    if isinstance(current_stage.get('details'), dict)
                    else {}
                )
                current_is_official = bool(
                    current_details.get('mode')
                    == 'official_daily_pipeline_run_state'
                    or current_details.get('historical_stage_state') is True
                )
                if (
                    current_is_official
                    and self._stage_reports_incomplete_authoritative_state(
                        current_stage
                    )
                    and not self._stored_stage_has_exact_universe_proof(
                        stored_stage=stored_stage,
                        official_stage=current_stage,
                    )
                ):
                    continue
                if self._stored_dependency_wait_is_stale(
                    stored_stage=stored_stage,
                    current_stage=current_stage,
                    current_stages=stages,
                ):
                    continue
                if daily_pipeline_result_has_no_ready_content(
                    current_details.get('result_summary')
                ) and not self._should_preserve_stored_coverage_stage(
                    stage_id=stage_id,
                    fallback_stage=stored_stage,
                    official_stage=stages[stage_id],
                ):
                    continue
                stages[stage_id] = stored_stage
        for stage_id, coverage in exact_universe_coverage.items():
            if stage_id not in stages or not isinstance(coverage, dict):
                continue
            stages[stage_id] = self._apply_exact_universe_coverage_evidence(
                stage=stages[stage_id],
                coverage=coverage,
                expected_count=len(exact_run_universe_symbols),
                source_run_id=(pykrx_run or {}).get('id'),
            )
        if normalized_scope_symbols:
            for stage_id in PREREQUISITE_SCOPE_STAGE_MODELS:
                if stage_id not in stages:
                    continue
                stages[stage_id] = self._apply_scope_coverage_gate(
                    stage_id=stage_id,
                    market_date=resolved_market_date,
                    stage=stages[stage_id],
                    scope_symbols=normalized_scope_symbols,
                )
        stages = self._mark_preopen_stage_skips(stages=stages)
        stages = self._normalize_stage_contract(stages)
        warning_metadata = daily_pipeline_warning_metadata(stages)
        overall_status = self._resolve_overall_status(stages)
        if self._is_current_date_waiting_for_official_run(
            stages=stages,
            official_run=official_run,
            pykrx_run=pykrx_run,
            market_session=market_session,
        ):
            overall_status = 'pending'
        official_run_status = str((official_run or {}).get('status') or '').strip().lower()
        if official_run_status in {'running', 'interrupted', 'abandoned'}:
            overall_status = official_run_status
        summary = self._build_stage_summary(stages=stages)
        official_current_stage = str((official_run or {}).get('current_stage') or '').strip()
        if official_run_status in {'running', 'interrupted', 'abandoned'} and official_current_stage in stages:
            summary['current_stage'] = official_current_stage
            summary['current_stage_status'] = stages[official_current_stage].get('status')
        gate_summary = self._build_gate_summary(stages=stages)
        return {
            'market_date': resolved_market_date,
            'generated_at': _utcnow_iso(),
            'market': {
                'market': getattr(market_state, 'market', None),
                'state': getattr(market_state, 'state', None),
                'is_trading_day': getattr(market_state, 'is_trading_day', None),
                'is_open': getattr(market_state, 'is_open', None),
                'reason': getattr(market_state, 'reason', None),
                'server_time': getattr(market_state, 'server_time', None),
                'current_calendar_date': getattr(market_state, 'current_calendar_date', None),
                'latest_valid_trading_date': getattr(market_state, 'latest_valid_trading_date', None),
                'market_session': getattr(market_state, 'market_session', None),
            },
            'market_session': market_session,
            'fs_div': resolved_fs_div,
            'status': overall_status,
            **warning_metadata,
            'stage_order': list(PIPELINE_STAGE_ORDER),
            **summary,
            'gate_summary': gate_summary,
            'stages': stages,
            'official_run': self._build_official_run_summary(official_run),
        }

    @staticmethod
    def _stored_dependency_wait_is_stale(
        *,
        stored_stage: dict[str, Any],
        current_stage: dict[str, Any],
        current_stages: dict[str, dict[str, Any]],
    ) -> bool:
        stored_details = (
            stored_stage.get('details')
            if isinstance(stored_stage.get('details'), dict)
            else {}
        )
        current_details = (
            current_stage.get('details')
            if isinstance(current_stage.get('details'), dict)
            else {}
        )
        blocking_stages = stored_details.get('blocking_stages')
        persisted = int(
            stored_details.get('rows_persisted')
            or stored_stage.get('succeeded')
            or 0
        )
        expected = int(
            stored_details.get('expected_rows')
            or stored_stage.get('total')
            or 0
        )
        current_mode = str(current_details.get('mode') or '').strip()
        if (
            str(stored_stage.get('status') or '').strip().lower() != 'pending'
            or str(stored_stage.get('gate_status') or '').strip().lower()
            not in {'waiting', 'blocked'}
            or not isinstance(blocking_stages, list)
            or not blocking_stages
            or persisted <= 0
            or expected <= 0
            or persisted < expected
            or str(current_stage.get('status') or '').strip().lower()
            != 'completed'
            or current_mode
            not in {
                'official_daily_pipeline_run_state',
                'eod_collection_run_stage_progress',
            }
            or daily_pipeline_result_has_no_ready_content(
                current_details.get('result_summary')
            )
        ):
            return False
        return all(
            str((current_stages.get(stage_id) or {}).get('status') or '')
            .strip()
            .lower()
            == 'completed'
            for stage_id in blocking_stages
        )

    @staticmethod
    def _normalize_stage_contract(stages: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
        normalized: dict[str, dict[str, Any]] = {}
        for index, stage_id in enumerate(PIPELINE_STAGE_ORDER, start=1):
            definition = STAGE_DEFINITIONS[stage_id]
            stage = dict(stages.get(stage_id) or {})
            details = dict(stage.get('details') or {})
            details.setdefault('depends_on', list(definition.depends_on))
            details.setdefault('inspect_routes', list(definition.inspect_routes))
            details.setdefault('output_tables', list(definition.output_tables))
            details.setdefault('default_selected', definition.default_selected)
            if details.get('rows_persisted') is None and stage.get('succeeded') is not None:
                details['rows_persisted'] = int(stage.get('succeeded') or 0)
            if details.get('expected_rows') is None and stage.get('total') is not None:
                details['expected_rows'] = int(stage.get('total') or 0)
            if details.get('progress_source') is None:
                details['progress_source'] = EODPipelineStatusService._infer_stage_progress_source(stage)
            if details.get('reason') is None:
                details['reason'] = EODPipelineStatusService._infer_stage_reason(stage, details)
            raw_status = str(
                stage.get('status') or 'pending'
            ).strip().lower()
            normalized_status = normalize_daily_pipeline_stage_status(
                stage_id=stage_id,
                status=raw_status,
                outcome={**stage, 'details': details},
            )
            if normalized_status != raw_status:
                stage['status'] = normalized_status
                stage['source_status'] = (
                    stage.get('source_status') or raw_status
                )
                details.setdefault(
                    'status_canonicalized_from',
                    raw_status,
                )
                details.setdefault(
                    'status_canonicalization_reason',
                    'terminal_optional_context_unavailable',
                )

            normalized[stage_id] = {
                **stage,
                'task': stage.get('task') or stage_id,
                'stage_id': stage_id,
                'label': stage.get('label') or definition.label,
                'group': definition.group,
                'order': index,
                'depends_on': list(definition.depends_on),
                'inspect_routes': list(definition.inspect_routes),
                'output_tables': list(definition.output_tables),
                'default_selected': definition.default_selected,
                'details': details,
            }
        signal_decider_stage = normalized.get('signal_decider', {})
        signal_decider_status = str(
            signal_decider_stage.get('status') or ''
        ).strip().lower()
        signal_decider_proof = (
            DailyPipelineStageService
            .validated_stage_completion_scope_proof(
                'signal_decider',
                dict(signal_decider_stage.get('details') or {}).get(
                    'completion_scope_proof'
                ),
            )
        )
        next_open_stage = normalized.get('next_open_execution_plan')
        next_open_proof = (
            DailyPipelineStageService
            .validated_stage_completion_scope_proof(
                'next_open_execution_plan',
                dict(next_open_stage.get('details') or {}).get(
                    'completion_scope_proof'
                ),
            )
            if next_open_stage is not None
            else None
        )
        next_open_is_bound_to_parent = bool(
            signal_decider_proof is not None
            and next_open_proof is not None
            and next_open_proof['parent_run_id']
            == signal_decider_proof['source_run_id']
            and next_open_proof['parent_summary_run_id']
            == signal_decider_proof['source_summary_run_id']
            and next_open_proof['parent_admitted_symbols']
            == signal_decider_proof['admitted_symbols']
            and next_open_proof['parent_completed_symbols']
            == signal_decider_proof['completed_symbols']
        )
        if (
            next_open_stage is not None
            and (
                str(next_open_stage.get('status') or '')
                .strip()
                .lower()
                == 'completed'
                or str(next_open_stage.get('source_status') or '')
                .strip()
                .lower()
                == 'completed'
            )
            and not next_open_is_bound_to_parent
            and dict(next_open_stage.get('details') or {}).get(
                'status_repair_reason'
            )
            != 'incomplete_result_summary'
        ):
            repaired_details = dict(next_open_stage.get('details') or {})
            repaired_details['status_repaired_from'] = 'completed'
            repaired_details['status_repair_reason'] = (
                'signal_decider_completion_proof_unavailable_or_mismatched'
            )
            repaired_details['upstream_status'] = signal_decider_status or 'unknown'
            normalized['next_open_execution_plan'] = {
                **next_open_stage,
                'status': 'blocked',
                'current_activity': 'Blocked because the signal decider cohort is incomplete.',
                'details': repaired_details,
            }
        return normalized

    @staticmethod
    def _infer_stage_progress_source(stage: dict[str, Any]) -> str:
        mode = str(dict(stage.get('details') or {}).get('mode') or '').strip()
        if mode == 'official_daily_pipeline_run_state':
            return 'official_run'
        source_status = str(stage.get('source_status') or '').strip()
        if source_status == 'stored_rows_present' or source_status.startswith('stored_'):
            return 'stored_rows'
        if stage.get('source_run_ids'):
            return 'live_run_metadata'
        return 'fallback_inference'

    @staticmethod
    def _infer_stage_reason(stage: dict[str, Any], details: dict[str, Any]) -> str | None:
        for key in ('reason', 'message', 'failure_reason', 'error_message', 'gate_reason'):
            value = details.get(key) or stage.get(key)
            if value:
                return str(value)
        status = str(stage.get('status') or '').strip().lower()
        if status == 'skipped':
            return str(stage.get('current_activity') or 'Stage is not applicable to this market-date status view.')
        if status == 'pending':
            return str(stage.get('current_activity') or 'No official run state or persisted output rows were found.')
        if status in {'partial', 'blocked', 'failed', 'running'} and stage.get('current_activity'):
            return str(stage.get('current_activity'))
        missing_count = int(details.get('missing_count') or 0)
        if missing_count > 0:
            return f'{missing_count} symbols are missing persisted rows for this stage.'
        return None

    @staticmethod
    def _build_stage_summary(*, stages: dict[str, dict[str, Any]]) -> dict[str, Any]:
        status_counts: dict[str, int] = {}
        for stage in stages.values():
            status = str(stage.get('status') or 'pending').strip().lower()
            status_counts[status] = status_counts.get(status, 0) + 1

        current_stage = EODPipelineStatusService._resolve_current_stage(stages=stages)
        completed_count = status_counts.get('completed', 0)
        skipped_count = status_counts.get('skipped', 0)
        return {
            'total_stages': len(stages),
            'completed_stages': completed_count,
            'skipped_stages': skipped_count,
            'display_completed_stages': completed_count + skipped_count,
            'partial_stages': status_counts.get('partial', 0),
            'failed_stages': status_counts.get('failed', 0),
            'blocked_stages': status_counts.get('blocked', 0),
            'running_stages': status_counts.get('running', 0),
            'pending_stages': status_counts.get('pending', 0),
            'stage_status_counts': status_counts,
            'current_stage': current_stage.get('task') if current_stage else None,
            'current_stage_status': current_stage.get('status') if current_stage else None,
        }

    @staticmethod
    def _build_gate_summary(*, stages: dict[str, dict[str, Any]]) -> dict[str, Any]:
        stage_gates: dict[str, dict[str, Any]] = {}
        ready_stage_ids: list[str] = []
        waiting_stage_ids: list[str] = []
        blocked_stage_ids: list[str] = []
        for stage_id, stage in stages.items():
            gate_status = str(stage.get('gate_status') or '').strip().lower()
            if gate_status not in {'ready', 'waiting', 'blocked'}:
                continue
            details = dict(stage.get('details') or {})
            gate_item = {
                'stage': stage_id,
                'status': stage.get('status'),
                'gate_status': gate_status,
                'severity': stage.get('severity'),
                'blocking_status': stage.get('blocking_status'),
                'gate_reason': details.get('gate_reason') or details.get('reason'),
            }
            if stage_id == 'pykrx_enrichment':
                gate_item.update(
                    {
                        'required_categories': details.get('decider_required_categories') or [],
                        'min_coverage_ratio': details.get('decider_min_coverage_ratio'),
                        'blocking_categories': details.get('decider_blocking_categories') or [],
                        'waiting_categories': details.get('decider_waiting_categories') or [],
                        'warning_categories': details.get('decider_warning_categories') or [],
                    }
                )
            stage_gates[stage_id] = gate_item
            if gate_status == 'ready':
                ready_stage_ids.append(stage_id)
            elif gate_status == 'blocked':
                blocked_stage_ids.append(stage_id)
            else:
                waiting_stage_ids.append(stage_id)

        if blocked_stage_ids:
            status = 'blocked'
        elif waiting_stage_ids:
            status = 'waiting'
        elif ready_stage_ids:
            status = 'ready'
        else:
            status = None
        return {
            'status': status,
            'ready_stage_ids': ready_stage_ids,
            'waiting_stage_ids': waiting_stage_ids,
            'blocked_stage_ids': blocked_stage_ids,
            'stages': stage_gates,
        }

    @staticmethod
    def _resolve_current_stage(*, stages: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
        for preferred_status in ('running', 'partial', 'blocked', 'failed', 'interrupted', 'abandoned', 'pending'):
            for stage in stages.values():
                if str(stage.get('status') or '').strip().lower() == preferred_status:
                    return stage
        return None

    def _build_generic_pending_stage(
        self,
        *,
        stage_id: str,
        market_date: str,
    ) -> dict[str, Any]:
        definition = STAGE_DEFINITIONS[stage_id]
        counts = build_stage_counts(total=0, succeeded=0, failed=0, processed=0)
        return {
            'task': stage_id,
            'label': definition.label,
            'status': 'pending',
            'source_status': None,
            'market_date': market_date,
            'started_at': None,
            'updated_at': None,
            'completed_at': None,
            'processed': counts.processed,
            'total': counts.total,
            'succeeded': counts.succeeded,
            'failed': counts.failed,
            'remaining': counts.remaining,
            'current_step': None,
            'current_activity': _default_activity(definition.label, 'pending'),
            'current_symbol': None,
            'current_period': None,
            'source_run_ids': {},
            'details': {
                'mode': 'pending_until_official_run_or_persisted_rows',
                'depends_on': list(definition.depends_on),
                'output_tables': list(definition.output_tables),
                'inspect_routes': list(definition.inspect_routes),
                'default_selected': definition.default_selected,
            },
        }

    def _overlay_live_eod_run_stage_progress(
        self,
        *,
        stages: dict[str, dict[str, Any]],
        run: dict[str, Any] | None,
        market_date: str,
    ) -> dict[str, dict[str, Any]]:
        if not run:
            return stages
        run_metadata = get_run_metadata(run)
        stage_progress = run_metadata.get('stage_progress') if isinstance(run_metadata.get('stage_progress'), dict) else {}
        if not stage_progress:
            return stages
        overlaid = dict(stages)
        for stage_id in LIVE_EOD_RUN_PROGRESS_FALLBACK_STAGE_IDS:
            progress = stage_progress.get(stage_id)
            if not isinstance(progress, dict):
                continue
            fallback_stage = stages.get(stage_id) or {}
            if not self._should_overlay_live_eod_run_stage_progress(
                stage_id=stage_id,
                fallback_stage=fallback_stage,
                progress=progress,
            ):
                continue
            overlaid[stage_id] = self._build_live_eod_run_progress_stage(
                stage_id=stage_id,
                market_date=market_date,
                run=run,
                run_metadata=run_metadata,
                progress=progress,
            )
        return overlaid

    @staticmethod
    def _should_overlay_live_eod_run_stage_progress(
        *,
        stage_id: str,
        fallback_stage: dict[str, Any],
        progress: dict[str, Any],
    ) -> bool:
        progress_status = _normalize_status(str(progress.get('status') or '')) or 'pending'
        fallback_status = str(fallback_stage.get('status') or '').strip().lower()
        if EODPipelineStatusService._recovery_state_is_newer(
            stage_id=stage_id,
            candidate=fallback_stage,
            current=progress,
        ):
            return False
        if EODPipelineStatusService._should_preserve_stored_coverage_stage(
            stage_id=stage_id,
            fallback_stage=fallback_stage,
        ):
            return False
        if progress_status == 'running':
            return fallback_status != 'running'
        if fallback_status == 'pending':
            return True
        if progress_status in OFFICIAL_STAGE_HISTORY_INCOMPLETE_STATUSES:
            fallback_details = fallback_stage.get('details') if isinstance(fallback_stage.get('details'), dict) else {}
            fallback_mode = str(fallback_details.get('mode') or '').strip()
            return fallback_mode in PERSISTED_OUTPUT_FALLBACK_MODES
        return False

    def _build_live_eod_run_progress_stage(
        self,
        *,
        stage_id: str,
        market_date: str,
        run: dict[str, Any],
        run_metadata: dict[str, Any],
        progress: dict[str, Any],
    ) -> dict[str, Any]:
        definition = STAGE_DEFINITIONS[stage_id]
        status = _normalize_status(str(progress.get('status') or '')) or 'pending'
        counts = build_stage_counts(
            total=progress.get('total'),
            succeeded=progress.get('succeeded'),
            failed=progress.get('failed'),
            processed=progress.get('processed'),
        )
        progress_source_run_ids = progress.get('source_run_ids') if isinstance(progress.get('source_run_ids'), dict) else {}
        source_run_ids = {
            'eod_run_id': run.get('id'),
            **progress_source_run_ids,
        }
        last_progress_at = run_metadata.get('last_progress_at') or progress.get('updated_at')
        stale_reason = run_metadata.get('stale_failure_reason')
        current_activity = progress.get('current_activity')
        if status == 'failed' and stale_reason:
            current_activity = stale_reason
        details = {
            **_progress_extras(progress),
            'mode': 'eod_collection_run_stage_progress',
            'progress_source': 'live_run_metadata',
            'eod_run_id': run.get('id'),
            'eod_run_status': run.get('status'),
            'eod_run_trigger': run.get('trigger'),
            'eod_run_started_at': run.get('started_at'),
            'last_progress_at': last_progress_at,
            'stale_running_threshold_minutes': settings.eod_auto_pykrx_stale_run_minutes,
        }
        if stale_reason:
            details['stale_failure_reason'] = stale_reason
            details['stale_failed_at'] = run_metadata.get('stale_failed_at')
        completed_at = progress.get('completed_at')
        if completed_at is None and status in {'completed', 'partial', 'failed', 'blocked', 'waiting', 'skipped'}:
            completed_at = progress.get('updated_at')
        return {
            'task': stage_id,
            'label': definition.label,
            'status': status,
            'source_status': progress.get('status') or run.get('status'),
            'market_date': market_date,
            'started_at': progress.get('started_at') or run.get('started_at'),
            'updated_at': progress.get('updated_at') or last_progress_at or run.get('started_at'),
            'completed_at': completed_at,
            'processed': counts.processed,
            'total': counts.total,
            'succeeded': counts.succeeded,
            'failed': counts.failed,
            'remaining': counts.remaining,
            'current_step': progress.get('current_step'),
            'current_activity': current_activity or _default_activity(definition.label, status),
            'current_symbol': progress.get('current_symbol'),
            'current_period': progress.get('current_period'),
            'source_run_ids': source_run_ids,
            'details': details,
        }

    def _build_skipped_stage(
        self,
        *,
        stage_id: str,
        market_date: str,
        reason: str,
    ) -> dict[str, Any]:
        definition = STAGE_DEFINITIONS[stage_id]
        counts = build_stage_counts(total=0, succeeded=0, failed=0, processed=0)
        activity = 'Skipped: ' + reason.replace('_', ' ')
        return {
            'task': stage_id,
            'label': definition.label,
            'status': 'skipped',
            'source_status': 'not_applicable',
            'market_date': market_date,
            'started_at': None,
            'updated_at': None,
            'completed_at': None,
            'processed': counts.processed,
            'total': counts.total,
            'succeeded': counts.succeeded,
            'failed': counts.failed,
            'remaining': counts.remaining,
            'current_step': None,
            'current_activity': activity,
            'current_symbol': None,
            'current_period': None,
            'source_run_ids': {},
            'details': {
                'mode': 'not_applicable_to_market_date_status',
                'reason': reason,
                'depends_on': list(definition.depends_on),
                'output_tables': list(definition.output_tables),
                'inspect_routes': list(definition.inspect_routes),
                'default_selected': definition.default_selected,
            },
        }

    def _mark_preopen_stage_skips(
        self,
        *,
        stages: dict[str, dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        if not self._has_date_scoped_activity(stages=stages):
            return stages
        overlaid = dict(stages)
        for stage_id in NON_DATE_SCOPED_PREOPEN_STAGE_IDS:
            stage = dict(overlaid.get(stage_id) or {})
            if str(stage.get('status') or '').strip().lower() != 'pending':
                continue
            overlaid[stage_id] = self._build_skipped_stage(
                stage_id=stage_id,
                market_date=str(stage.get('market_date') or ''),
                reason='preopen_global_stage_not_required_for_resumed_market_date_status',
            )
        return overlaid

    @staticmethod
    def _has_date_scoped_activity(*, stages: dict[str, dict[str, Any]]) -> bool:
        active_statuses = {
            'completed',
            'partial',
            'running',
            'failed',
            'blocked',
            'waiting',
            'interrupted',
            'abandoned',
        }
        for stage_id, stage in stages.items():
            if stage_id in NON_DATE_SCOPED_PREOPEN_STAGE_IDS:
                continue
            if str(stage.get('status') or '').strip().lower() in active_statuses:
                return True
            if int(stage.get('succeeded') or 0) > 0:
                return True
        return False

    def _overlay_official_run_stages(
        self,
        *,
        stages: dict[str, dict[str, Any]],
        official_run: dict[str, Any],
        market_date: str,
    ) -> dict[str, dict[str, Any]]:
        official_stages = official_run.get('stages') if isinstance(official_run.get('stages'), dict) else {}
        if not official_stages:
            return stages
        overlaid = dict(stages)
        for stage_id in PIPELINE_STAGE_ORDER:
            official_stage = official_stages.get(stage_id)
            if not isinstance(official_stage, dict):
                continue
            fallback_stage = stages.get(stage_id) or {}
            if self._recovery_state_is_newer(
                stage_id=stage_id,
                candidate=fallback_stage,
                current=official_stage,
            ):
                continue
            if self._should_preserve_stored_coverage_stage(
                stage_id=stage_id,
                fallback_stage=fallback_stage,
                official_stage=official_stage,
            ):
                continue
            overlaid[stage_id] = self._build_official_stage(
                stage_id=stage_id,
                market_date=market_date,
                official_run=official_run,
                official_stage=official_stage,
                fallback_stage=fallback_stage,
            )
        return overlaid

    def _overlay_official_stage_history(
        self,
        *,
        stages: dict[str, dict[str, Any]],
        stage_history: dict[str, dict[str, Any]],
        market_date: str,
    ) -> dict[str, dict[str, Any]]:
        overlaid = dict(stages)
        for stage_id in PIPELINE_STAGE_ORDER:
            fallback_stage = stages.get(stage_id) or {}
            history_item = stage_history.get(stage_id)
            if not isinstance(history_item, dict):
                continue
            official_stage = history_item.get('stage') if isinstance(history_item.get('stage'), dict) else None
            official_run = history_item.get('run') if isinstance(history_item.get('run'), dict) else None
            if not isinstance(official_stage, dict) or not isinstance(official_run, dict):
                continue
            if self._should_preserve_stored_coverage_stage(
                stage_id=stage_id,
                fallback_stage=fallback_stage,
                official_stage=official_stage,
            ):
                continue
            if not self._should_overlay_official_stage_history(
                stage_id=stage_id,
                fallback_stage=fallback_stage,
                official_stage=official_stage,
            ):
                continue
            overlaid[stage_id] = self._build_official_stage(
                stage_id=stage_id,
                market_date=market_date,
                official_run=official_run,
                official_stage=official_stage,
                fallback_stage=fallback_stage,
            )
            details = dict(overlaid[stage_id].get('details') or {})
            details['historical_stage_state'] = True
            details['progress_source'] = 'official_stage_history'
            overlaid[stage_id]['details'] = details
        return overlaid

    @staticmethod
    def _should_preserve_stored_coverage_stage(
        *,
        stage_id: str,
        fallback_stage: dict[str, Any],
        official_stage: dict[str, Any] | None = None,
    ) -> bool:
        details = fallback_stage.get('details') if isinstance(fallback_stage.get('details'), dict) else {}
        if stage_id == 'pykrx_enrichment':
            return False
        if stage_id not in FACTOR_STAGE_MODELS and stage_id not in {'composite_factors'}:
            return False
        if (
            EODPipelineStatusService._stage_reports_incomplete_authoritative_state(
                official_stage
            )
            and not EODPipelineStatusService._stored_stage_has_exact_universe_proof(
                stored_stage=fallback_stage,
                official_stage=official_stage,
            )
        ):
            return False
        official_details = (
            official_stage.get('details')
            if isinstance(official_stage, dict)
            and isinstance(official_stage.get('details'), dict)
            else {}
        )
        latest_decision_ready_at = _parse_iso_timestamp(
            _coerce_iso_timestamp(details.get('latest_decision_ready_at')) or ''
        )
        official_updated_at = _parse_iso_timestamp(
            _coerce_iso_timestamp(
                (official_stage or {}).get('updated_at')
                or (official_stage or {}).get('completed_at')
            )
            or ''
        )
        fresher_stored_low_risk_rows_are_ready = bool(
            stage_id == 'low_risk_factors'
            and latest_decision_ready_at is not None
            and official_updated_at is not None
            and latest_decision_ready_at > official_updated_at
            and _coerce_int(details.get('decision_ready_count')) > 0
        )
        if fresher_stored_low_risk_rows_are_ready:
            return (
                str(fallback_stage.get('status') or '').strip().lower()
                == 'completed'
                and int(
                    details.get('rows_persisted')
                    or fallback_stage.get('succeeded')
                    or 0
                )
                > 0
            )
        if daily_pipeline_result_has_no_ready_content(
            official_details.get('result_summary')
        ):
            return False
        return (
            str(fallback_stage.get('status') or '').strip().lower() == 'completed'
            and int(details.get('rows_persisted') or fallback_stage.get('succeeded') or 0) > 0
        )

    @staticmethod
    def _stage_reports_incomplete_authoritative_state(
        stage: dict[str, Any] | None,
    ) -> bool:
        if not isinstance(stage, dict):
            return False
        status = str(stage.get('status') or '').strip().lower()
        return status in {
            'partial',
            'blocked',
            'failed',
            'waiting',
            'interrupted',
            'abandoned',
        }

    @staticmethod
    def _stored_stage_has_exact_universe_proof(
        *,
        stored_stage: dict[str, Any],
        official_stage: dict[str, Any] | None,
    ) -> bool:
        details = (
            stored_stage.get('details')
            if isinstance(stored_stage.get('details'), dict)
            else {}
        )
        coverage = details.get('exact_universe_coverage')
        requested_count = (
            _strict_nonnegative_int(coverage.get('requested_count'))
            if isinstance(coverage, dict)
            else None
        )
        validated_coverage = (
            _validated_row_count_scope_gap_evidence(
                coverage,
                requested_count=requested_count,
            )
            if requested_count is not None
            else None
        )
        source_run_id = details.get('exact_universe_source_run_id')
        if (
            details.get('exact_universe_verified') is not True
            or details.get('coverage_verification_status') != 'verified'
            or details.get('expected_rows_source')
            != 'validated_eod_run_market_membership'
            or validated_coverage is None
            or int(validated_coverage['missing_count']) != 0
            or source_run_id is None
            or not str(source_run_id).strip()
        ):
            return False

        official_source_run_ids: set[str] = set()
        if isinstance(official_stage, dict):
            source_payloads = [official_stage.get('source_run_ids')]
            official_details = (
                official_stage.get('details')
                if isinstance(official_stage.get('details'), dict)
                else {}
            )
            result_summary = official_details.get('result_summary')
            if isinstance(result_summary, dict):
                source_payloads.append(result_summary.get('source_run_ids'))
            for payload in source_payloads:
                if not isinstance(payload, dict):
                    continue
                for key in ('eod_run_id', 'pykrx_run_id', 'source_eod_run_id'):
                    value = payload.get(key)
                    if value is not None and str(value).strip():
                        official_source_run_ids.add(str(value).strip())
        return (
            not official_source_run_ids
            or str(source_run_id).strip() in official_source_run_ids
        )

    @staticmethod
    def _should_overlay_official_stage_history(
        *,
        stage_id: str,
        fallback_stage: dict[str, Any],
        official_stage: dict[str, Any],
    ) -> bool:
        official_details = (
            official_stage.get('details')
            if isinstance(official_stage.get('details'), dict)
            else {}
        )
        if EODPipelineStatusService._recovery_state_is_newer(
            stage_id=stage_id,
            candidate=fallback_stage,
            current=official_stage,
        ):
            return False
        fallback_details = (
            fallback_stage.get('details')
            if isinstance(fallback_stage.get('details'), dict)
            else {}
        )
        official_status = str(
            official_stage.get('status') or ''
        ).strip().lower()
        if (
            stage_id in RECOVERABLE_STAGE_HISTORY_IDS
            and (
                official_status == 'completed'
                or (
                    stage_id != 'agent_feedback_summaries'
                    and official_status == 'partial'
                )
            )
            and EODPipelineStatusService._recovery_state_is_newer(
                stage_id=stage_id,
                candidate=official_stage,
                current=fallback_stage,
                candidate_is_history=True,
            )
        ):
            return True
        if daily_pipeline_result_has_no_ready_content(
            official_details.get('result_summary')
        ):
            return True
        fallback_status = str(fallback_stage.get('status') or '').strip().lower()
        result_summary = (
            official_details.get('result_summary')
            if isinstance(official_details.get('result_summary'), dict)
            else {}
        )
        if (
            fallback_status == 'pending'
            and result_summary
            and DailyPipelineStageService._resolve_result_status(
                result_summary
            )
            in {'partial', 'blocked', 'waiting'}
        ):
            return True
        if (
            stage_id in EXACT_COMPLETED_RECOVERY_STAGE_HISTORY_IDS
            and official_status == 'completed'
            and not EODPipelineStatusService._history_stage_has_authoritative_completion(
                stage_id=stage_id,
                candidate=official_stage,
            )
        ):
            return False
        if fallback_status == 'pending':
            return True
        if official_status not in OFFICIAL_STAGE_HISTORY_INCOMPLETE_STATUSES:
            return False
        fallback_mode = str(fallback_details.get('mode') or '').strip()
        return fallback_mode in PERSISTED_OUTPUT_FALLBACK_MODES

    @staticmethod
    def _signal_decider_has_legacy_completion_proof(
        result: dict[str, Any],
    ) -> bool:
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
        progress_result = (
            progress.get('result')
            if isinstance(progress.get('result'), dict)
            else {}
        )
        role_results = (
            progress_result.get('roles')
            if isinstance(progress_result.get('roles'), dict)
            else {}
        )
        role_progress = (
            summary.get('role_progress')
            if isinstance(summary.get('role_progress'), dict)
            else {}
        )
        cycle_status = summary.get('cycle_status')
        if (
            _recovery_payload_has_rejected_control_marker(
                result,
                progress_result,
                summary,
            )
            or _recovery_payload_has_invalid_optional_controls(result)
            or _recovery_payload_has_invalid_optional_controls(
                progress_result
            )
            or _recovery_payload_has_invalid_optional_controls(summary)
            or str(result.get('status') or '').strip().lower() != 'completed'
            or result.get('completed') is not True
            or str(summary.get('run_status') or '').strip().lower()
            != 'completed'
            or str(summary.get('run_table_status') or '').strip().lower()
            != 'completed'
            or not isinstance(cycle_status, str)
            or cycle_status.strip().lower() != 'completed'
            or _strict_non_negative_int(result, 'final_pending') != 0
            or _recovery_payload_has_reported_failure(result)
            or _recovery_payload_has_reported_failure(progress_result)
            or _recovery_payload_has_reported_failure(summary)
            or not role_progress
            or not role_results
            or set(role_results) != set(role_progress)
        ):
            return False

        expected = _strict_non_negative_int(
            summary,
            'expected_feedback_count',
        )
        completed = _strict_non_negative_int(
            summary,
            'completed_feedback_count',
        )
        failed = _strict_non_negative_int(summary, 'failed_items')
        if (
            expected is None
            or completed is None
            or failed != 0
            or expected <= 0
            or completed != expected
        ):
            return False
        if not _recovery_optional_counts_are_strict_zero(
            summary,
            (
                'pending_failure_count',
                'pending_feedback_count',
                'buffered_failure_count',
                'flushed_failure_count',
            ),
        ):
            return False
        if not _recovery_optional_collection_is_strict_empty(
            summary,
            'failure_groups',
        ):
            return False
        for pending_key in (
            'in_progress_symbols',
            'pending_symbols',
            'retry_pending_symbols',
            'partial_symbols',
        ):
            if (
                pending_key in summary
                and _strict_non_negative_int(summary, pending_key) != 0
            ):
                return False
        if not _recovery_optional_collection_is_strict_empty(
            summary,
            'remaining_roles',
        ):
            return False

        completed_roles = summary.get('completed_roles')
        if not isinstance(completed_roles, (list, tuple, set)):
            return False
        if (
            any(
                not isinstance(role, str) or not role.strip()
                for role in completed_roles
            )
            or len(completed_roles) != len(set(completed_roles))
            or set(completed_roles) != set(role_results)
        ):
            return False

        if 'is_live' in summary and summary.get('is_live') is not False:
            return False
        if 'current_role' in summary and summary.get('current_role') is not None:
            return False
        if (
            'terminal_feedback_count' in summary
            and _strict_non_negative_int(summary, 'terminal_feedback_count')
            != completed
        ):
            return False
        computed_feedback_count = _strict_non_negative_int(
            summary,
            'computed_feedback_count',
        )
        if 'reused_feedback_count' in summary:
            reused_feedback_count = _strict_non_negative_int(
                summary,
                'reused_feedback_count',
            )
            if (
                computed_feedback_count is None
                or reused_feedback_count is None
                or computed_feedback_count + reused_feedback_count
                != completed
            ):
                return False
        elif (
            'computed_feedback_count' in summary
            and computed_feedback_count != completed
        ):
            return False
        if (
            'stale_feedback_count' in summary
            and _strict_non_negative_int(summary, 'stale_feedback_count') != 0
        ):
            return False
        for failure_timestamp_key in (
            'first_failed_feedback_at',
            'latest_failed_feedback_at',
        ):
            if (
                failure_timestamp_key in summary
                and summary.get(failure_timestamp_key) is not None
            ):
                return False
        if 'run_completed_at' in summary:
            run_completed_at = summary.get('run_completed_at')
            if not isinstance(run_completed_at, str) or not run_completed_at.strip():
                return False

        role_expected = 0
        role_completed = 0
        for role, role_state in role_progress.items():
            role_result = role_results.get(role)
            if not isinstance(role_state, dict) or not isinstance(
                role_result,
                dict,
            ):
                return False
            total_symbols = _strict_non_negative_int(
                role_state,
                'total_symbols',
            )
            processed_symbols = _strict_non_negative_int(
                role_state,
                'processed_symbols',
            )
            completed_symbols = _strict_non_negative_int(
                role_state,
                'completed_symbols',
            )
            requested_count = _strict_non_negative_int(
                role_result,
                'requested_count',
            )
            completed_count = _strict_non_negative_int(
                role_result,
                'completed_count',
            )
            error_count = _strict_non_negative_int(
                role_result,
                'error_count',
            )
            if (
                str(role_state.get('status') or '').strip().lower()
                != 'completed'
                or str(role_state.get('boundary_status') or '')
                .strip()
                .lower()
                != 'completed'
                or _recovery_payload_has_reported_failure(role_state)
                or _recovery_payload_has_reported_failure(role_result)
                or (
                    'failed_items' in role_state
                    and _strict_non_negative_int(
                        role_state,
                        'failed_items',
                    )
                    != 0
                )
                or total_symbols is None
                or total_symbols <= 0
                or processed_symbols != total_symbols
                or completed_symbols != total_symbols
                or requested_count != total_symbols
                or completed_count != completed_symbols
                or error_count != 0
            ):
                return False
            if 'reused_symbols' in role_state:
                computed_symbols = _strict_non_negative_int(
                    role_state,
                    'computed_symbols',
                )
                reused_symbols = _strict_non_negative_int(
                    role_state,
                    'reused_symbols',
                )
                if (
                    computed_symbols is None
                    or reused_symbols is None
                    or computed_symbols + reused_symbols != completed_symbols
                ):
                    return False
            if (
                'stale_symbols' in role_state
                and _strict_non_negative_int(role_state, 'stale_symbols') != 0
            ):
                return False
            for pending_key in (
                'in_progress_symbols',
                'pending_symbols',
                'retry_pending_symbols',
                'partial_symbols',
            ):
                if (
                    pending_key in role_state
                    and _strict_non_negative_int(
                        role_state,
                        pending_key,
                    )
                    != 0
                ):
                    return False
            role_expected += total_symbols
            role_completed += completed_symbols
        return role_expected == expected and role_completed == completed

    @staticmethod
    def _evaluation_refresh_completion_signature(
        *,
        request: dict[str, Any],
        summary: dict[str, Any],
    ) -> dict[str, Any] | None:
        coverage = (
            summary.get('coverage')
            if isinstance(summary.get('coverage'), dict)
            else {}
        )
        if (
            str(summary.get('status') or '').strip().lower() != 'completed'
            or _recovery_payload_has_rejected_control_marker(summary)
            or _recovery_payload_has_invalid_optional_controls(summary)
            or _recovery_payload_has_reported_failure(summary)
            or _recovery_payload_has_reported_failure(coverage)
        ):
            return None

        horizons = _strict_positive_unique_ints(request.get('horizons'))
        decisions_considered = _strict_non_negative_int(
            summary,
            'decisions_considered',
        )
        if (
            horizons is None
            or decisions_considered is None
            or decisions_considered <= 0
        ):
            return None
        expected = decisions_considered * len(horizons)
        outcomes = _strict_non_negative_int(summary, 'outcomes_upserted')
        evaluations = _strict_non_negative_int(
            summary,
            'evaluations_upserted',
        )
        coverage_count = _strict_non_negative_int(coverage, 'count')
        if (
            outcomes != expected
            or evaluations != expected
            or coverage_count != expected
        ):
            return None

        component_keys = (
            'computed_count',
            'pending_maturity_count',
            'insufficient_price_data_count',
            'not_computed_count',
        )
        components: dict[str, int] = {}
        for key in component_keys:
            coverage_value = _strict_non_negative_int(coverage, key)
            summary_value = _strict_non_negative_int(summary, key)
            if coverage_value is None or summary_value != coverage_value:
                return None
            components[key] = coverage_value
        if sum(components.values()) != expected:
            return None
        if components['computed_count'] <= 0:
            return None

        graded_count = _strict_non_negative_int(summary, 'graded_count')
        if 'graded_count' in summary and graded_count != components[
            'computed_count'
        ]:
            return None
        pending_count = _strict_non_negative_int(summary, 'pending_count')
        if (
            'pending_count' in summary
            and pending_count != expected - components['computed_count']
        ):
            return None
        return {
            'horizons': horizons,
            'decisions_considered': decisions_considered,
            'expected': expected,
            **components,
        }

    @staticmethod
    def _signal_decider_evaluations_has_legacy_wrapper_completion_proof(
        *,
        result: dict[str, Any],
        candidate: dict[str, Any],
    ) -> bool:
        outer_summary = (
            result.get('summary')
            if isinstance(result.get('summary'), dict)
            else {}
        )
        direct_refresh = (
            result.get('evaluation_refresh')
            if isinstance(result.get('evaluation_refresh'), dict)
            else {}
        )
        refresh_request = (
            direct_refresh.get('request')
            if isinstance(direct_refresh.get('request'), dict)
            else {}
        )
        direct_refresh_summary = (
            direct_refresh.get('summary')
            if isinstance(direct_refresh.get('summary'), dict)
            else {}
        )
        nested_refresh_summary = (
            outer_summary.get('evaluation_refresh')
            if isinstance(outer_summary.get('evaluation_refresh'), dict)
            else {}
        )
        if (
            str(result.get('status') or '').strip().lower() != 'completed'
            or str(outer_summary.get('status') or '').strip().lower()
            != 'completed'
            or not direct_refresh_summary
            or not nested_refresh_summary
            or _recovery_payload_has_rejected_control_marker(
                result,
                outer_summary,
                direct_refresh,
                direct_refresh_summary,
                nested_refresh_summary,
            )
            or _recovery_payload_has_invalid_optional_controls(result)
            or _recovery_payload_has_invalid_optional_controls(outer_summary)
            or _recovery_payload_has_invalid_optional_controls(direct_refresh)
            or _recovery_payload_has_reported_failure(result)
            or _recovery_payload_has_reported_failure(outer_summary)
            or _recovery_payload_has_reported_failure(direct_refresh)
        ):
            return False

        direct_signature = EODPipelineStatusService._evaluation_refresh_completion_signature(
            request=refresh_request,
            summary=direct_refresh_summary,
        )
        nested_signature = EODPipelineStatusService._evaluation_refresh_completion_signature(
            request=refresh_request,
            summary=nested_refresh_summary,
        )
        if direct_signature is None or nested_signature != direct_signature:
            return False
        for key in (
            'computed_count',
            'pending_maturity_count',
            'insufficient_price_data_count',
            'not_computed_count',
        ):
            if _strict_non_negative_int(outer_summary, key) != direct_signature[
                key
            ]:
                return False

        calendar_refresh = result.get('calendar_refresh')
        if calendar_refresh is not None:
            if not isinstance(calendar_refresh, dict):
                return False
            if (
                _recovery_payload_has_invalid_optional_controls(
                    calendar_refresh
                )
                or _recovery_payload_has_reported_failure(calendar_refresh)
            ):
                return False
            calendar_request = (
                calendar_refresh.get('request')
                if isinstance(calendar_refresh.get('request'), dict)
                else {}
            )
            calendar_summary = (
                calendar_refresh.get('summary')
                if isinstance(calendar_refresh.get('summary'), dict)
                else {}
            )
            if _recovery_payload_has_invalid_optional_controls(
                calendar_summary
            ):
                return False
            calendar_evaluation_summary = (
                calendar_summary.get('evaluation_refresh')
                if isinstance(calendar_summary.get('evaluation_refresh'), dict)
                else {}
            )
            calendar_signature = EODPipelineStatusService._evaluation_refresh_completion_signature(
                request=calendar_request,
                summary=calendar_evaluation_summary,
            )
            if calendar_signature != direct_signature:
                return False
            primary_horizon_count = _strict_non_negative_int(
                calendar_summary,
                'primary_horizon_count',
            )
            if (
                'primary_horizon_count' in calendar_summary
                and primary_horizon_count
                != len(direct_signature['horizons'])
            ):
                return False

            calendar_run = calendar_refresh.get('run')
            if calendar_run is not None:
                if not isinstance(calendar_run, dict):
                    return False
                run_summary = (
                    calendar_run.get('summary')
                    if isinstance(calendar_run.get('summary'), dict)
                    else {}
                )
                if _recovery_payload_has_invalid_optional_controls(
                    run_summary
                ):
                    return False
                run_evaluation_summary = (
                    run_summary.get('evaluation_refresh')
                    if isinstance(run_summary.get('evaluation_refresh'), dict)
                    else {}
                )
                run_request = {'horizons': calendar_run.get('horizons')}
                run_signature = EODPipelineStatusService._evaluation_refresh_completion_signature(
                    request=run_request,
                    summary=run_evaluation_summary,
                )
                if (
                    str(calendar_run.get('status') or '')
                    .strip()
                    .lower()
                    != 'completed'
                    or _recovery_payload_has_invalid_optional_controls(
                        calendar_run
                    )
                    or _recovery_payload_has_reported_failure(calendar_run)
                    or run_signature != direct_signature
                ):
                    return False

        outer_calendar = outer_summary.get('calendar_refresh')
        if outer_calendar is not None:
            if not isinstance(outer_calendar, dict):
                return False
            if _recovery_payload_has_invalid_optional_controls(
                outer_calendar
            ):
                return False
            outer_calendar_evaluation = outer_calendar.get(
                'evaluation_refresh'
            )
            if not isinstance(outer_calendar_evaluation, dict):
                return False
            outer_calendar_signature = EODPipelineStatusService._evaluation_refresh_completion_signature(
                request=refresh_request,
                summary=outer_calendar_evaluation,
            )
            if outer_calendar_signature != direct_signature:
                return False

        computed_count = direct_signature['computed_count']
        return bool(
            _strict_non_negative_int(candidate, 'processed') == computed_count
            and _strict_non_negative_int(candidate, 'total') == computed_count
            and _strict_non_negative_int(candidate, 'succeeded')
            == computed_count
            and _strict_non_negative_int(candidate, 'failed') == 0
            and _strict_non_negative_int(candidate, 'remaining') == 0
        )

    @staticmethod
    def _signal_decider_evaluations_has_legacy_completion_proof(
        result: dict[str, Any],
    ) -> bool:
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
        raw_result_status = str(result.get('status') or '').strip().lower()
        if (
            not summary
            or _recovery_payload_has_rejected_control_marker(result)
            or _recovery_payload_has_invalid_optional_controls(result)
            or (
                raw_result_status
                and raw_result_status != 'completed'
            )
            or _recovery_payload_has_reported_failure(result)
        ):
            return False
        return (
            EODPipelineStatusService._evaluation_refresh_completion_signature(
                request=request,
                summary=summary,
            )
            is not None
        )

    @staticmethod
    def _agent_feedback_summaries_has_completion_proof(
        *,
        candidate: dict[str, Any],
        result: dict[str, Any],
    ) -> bool:
        cutoff_proof = summary_cutoff_proof_from_stage(candidate)
        details = (
            candidate.get('details')
            if isinstance(candidate.get('details'), dict)
            else {}
        )
        scope_proof = (
            DailyPipelineStageService
            .validated_stage_completion_scope_proof(
                'agent_feedback_summaries',
                details.get('completion_scope_proof'),
            )
        )
        roles = result.get('roles')
        raw_knowledge_cutoff_at = result.get('knowledge_cutoff_at')
        try:
            knowledge_cutoff_at = datetime.fromisoformat(
                str(raw_knowledge_cutoff_at or '').strip()
            )
        except (TypeError, ValueError):
            knowledge_cutoff_at = None
        if not (
            cutoff_proof is not None
            and scope_proof is not None
            and knowledge_cutoff_at is not None
            and knowledge_cutoff_at.tzinfo is not None
            and knowledge_cutoff_at.utcoffset() is not None
            and scope_proof['knowledge_cutoff_at']
            == knowledge_cutoff_at.astimezone(timezone.utc).isoformat()
            and isinstance(roles, list)
            and len(roles) == len(SUMMARIZER_AGENT_NAMES)
            and set(roles) == set(SUMMARIZER_AGENT_NAMES)
            and scope_proof['source_run_id']
            == cutoff_proof['source_run_id']
            and scope_proof['feedback_scope_mode']
            == cutoff_proof['feedback_scope_mode']
        ):
            return False
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
        role_progress = summary.get('role_progress')
        if not (
            isinstance(role_progress, dict)
            and set(role_progress) == set(SUMMARIZER_AGENT_NAMES)
        ):
            return False

        def strict_count(value: Any) -> int | None:
            if isinstance(value, bool) or not isinstance(value, int):
                return None
            return value if value >= 0 else None

        role_total = 0
        for role in SUMMARIZER_AGENT_NAMES:
            state = role_progress.get(role)
            expected_count = scope_proof['role_symbol_counts'][role]
            if not (
                isinstance(state, dict)
                and str(state.get('role') or '').strip() == role
                and str(state.get('status') or '').strip().lower()
                == 'completed'
                and str(
                    state.get('boundary_status') or ''
                ).strip().lower()
                == 'completed'
                and strict_count(state.get('total_symbols'))
                == expected_count
                and strict_count(state.get('processed_symbols'))
                == expected_count
                and strict_count(state.get('completed_symbols'))
                == expected_count
                and strict_count(state.get('in_progress_symbols')) == 0
                and strict_count(state.get('pending_symbols')) == 0
                and strict_count(state.get('failed_items')) == 0
                and strict_count(state.get('retry_pending_symbols')) == 0
            ):
                return False
            role_total += expected_count
        semantic_counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='agent_feedback_summaries',
            result=result,
        )
        total = _coerce_int(candidate.get('total'))
        return bool(
            total > 0
            and role_total == total
            and scope_proof['total_item_count'] == total
            and strict_count(summary.get('expected_feedback_count'))
            == total
            and strict_count(summary.get('completed_feedback_count'))
            == total
            and strict_count(summary.get('failed_items')) == 0
            and semantic_counts['total'] == total
            and semantic_counts['processed'] == total
            and semantic_counts['succeeded'] == total
            and semantic_counts['failed'] == 0
        )

    @staticmethod
    def _next_open_execution_plan_has_completion_proof(
        *,
        candidate: dict[str, Any],
        details: dict[str, Any],
    ) -> bool:
        scope_proof = validated_nonblocking_downstream_scope_proof(
            'next_open_execution_plan',
            details.get('nonblocking_scope_proof'),
        )
        completion_proof = (
            DailyPipelineStageService
            .validated_stage_completion_scope_proof(
                'next_open_execution_plan',
                details.get('completion_scope_proof'),
            )
        )
        source_run_ids = (
            candidate.get('source_run_ids')
            if isinstance(candidate.get('source_run_ids'), dict)
            else {}
        )
        total = _coerce_int(candidate.get('total'))
        completion_active_scope = sorted(
            {
                (
                    entry['symbol'],
                    entry['decision_agent_name'],
                    entry['decision_run_id'],
                )
                for entry in (
                    completion_proof.get('decision_partition', [])
                    if isinstance(completion_proof, dict)
                    else []
                )
                if any(
                    item.get('handoff_active') is True
                    or item.get('exec_monitoring_active') is True
                    for item in entry.get('included_items', [])
                    if isinstance(item, dict)
                )
            }
        )
        persisted_active_scope = sorted(
            (
                entry['symbol'],
                entry['decision_agent_name'],
                entry['decision_run_id'],
            )
            for entry in (
                scope_proof.get('active_scope', [])
                if isinstance(scope_proof, dict)
                else []
            )
        )
        return bool(
            completion_proof is not None
            and total >= 0
            and completion_proof['decision_count'] > 0
            and completion_proof['item_count'] == total
            and _coerce_int(candidate.get('processed')) == total
            and _coerce_int(candidate.get('succeeded')) == total
            and _coerce_int(candidate.get('failed')) == 0
            and _coerce_int(candidate.get('remaining')) == 0
            and (
                (
                    total == 0
                    and completion_proof['active_item_count'] == 0
                    and not completion_active_scope
                    and scope_proof is not None
                    and scope_proof['item_count'] == 0
                    and scope_proof['active_item_count'] == 0
                    and scope_proof['blocked_item_count'] == 0
                    and not scope_proof['active_symbols']
                    and not persisted_active_scope
                )
                or (
                    total > 0
                    and scope_proof is not None
                    and scope_proof['item_count'] == total
                    and completion_proof['active_item_count']
                    == scope_proof['active_item_count']
                    and completion_proof['blocked_item_count']
                    == scope_proof['blocked_item_count']
                    and completion_active_scope
                    == persisted_active_scope
                )
            )
            and source_run_ids.get('parent_signal_decider_run_id')
            == completion_proof['parent_run_id']
            and source_run_ids.get('source_summary_run_id')
            == completion_proof['parent_summary_run_id']
        )

    @staticmethod
    def _signal_decider_evaluations_has_completion_proof(
        *,
        candidate: dict[str, Any],
        details: dict[str, Any],
    ) -> bool:
        completion_proof = (
            DailyPipelineStageService
            .validated_stage_completion_scope_proof(
                'signal_decider_evaluations',
                details.get('completion_scope_proof'),
            )
        )
        source_run_ids = (
            candidate.get('source_run_ids')
            if isinstance(candidate.get('source_run_ids'), dict)
            else {}
        )
        total = _coerce_int(candidate.get('total'))
        return bool(
            completion_proof is not None
            and total > 0
            and completion_proof['item_count'] == total
            and _coerce_int(candidate.get('processed')) == total
            and _coerce_int(candidate.get('succeeded')) == total
            and _coerce_int(candidate.get('failed')) == 0
            and _coerce_int(candidate.get('remaining')) == 0
            and source_run_ids.get('parent_signal_decider_run_id')
            == completion_proof['parent_run_id']
            and source_run_ids.get('source_summary_run_id')
            == completion_proof['parent_summary_run_id']
        )

    @staticmethod
    def _signal_decider_has_completion_proof(
        *,
        candidate: dict[str, Any],
        details: dict[str, Any],
    ) -> bool:
        completion_proof = (
            DailyPipelineStageService
            .validated_stage_completion_scope_proof(
                'signal_decider',
                details.get('completion_scope_proof'),
            )
        )
        if completion_proof is None:
            return False
        total = _coerce_int(candidate.get('total'))
        return bool(
            total > 0
            and total == completion_proof['total_items']
            and _coerce_int(candidate.get('processed')) == total
            and _coerce_int(candidate.get('succeeded'))
            == completion_proof['completed_items']
            and _coerce_int(candidate.get('failed'))
            == completion_proof['failed_items']
            and _coerce_int(candidate.get('remaining')) == 0
        )

    @staticmethod
    def _history_stage_has_authoritative_completion(
        *,
        stage_id: str,
        candidate: dict[str, Any],
    ) -> bool:
        if str(candidate.get('status') or '').strip().lower() != 'completed':
            return False
        details = (
            candidate.get('details')
            if isinstance(candidate.get('details'), dict)
            else {}
        )
        has_result_summary = 'result_summary' in details
        result = (
            details.get('result_summary')
            if isinstance(details.get('result_summary'), dict)
            else {}
        )
        nested_summary = (
            result.get('summary')
            if isinstance(result.get('summary'), dict)
            else {}
        )
        if _recovery_payload_has_rejected_control_marker(
            result,
            nested_summary,
        ):
            return False
        for payload in (result, nested_summary):
            status = str(payload.get('status') or '').strip().lower()
            if status and status not in {'complete', 'completed', 'ok'}:
                return False

        total = _coerce_int(candidate.get('total'))
        if stage_id == 'next_open_execution_plan':
            return (
                EODPipelineStatusService
                ._next_open_execution_plan_has_completion_proof(
                    candidate=candidate,
                    details=details,
                )
            )
        has_exact_counts = bool(
            total > 0
            and _coerce_int(candidate.get('processed')) == total
            and _coerce_int(candidate.get('succeeded')) == total
            and _coerce_int(candidate.get('failed')) == 0
            and _coerce_int(candidate.get('remaining')) == 0
        )
        if has_exact_counts:
            if stage_id == 'agent_feedback_summaries':
                return bool(
                    has_result_summary
                    and EODPipelineStatusService._agent_feedback_summaries_has_completion_proof(
                        candidate=candidate,
                        result=result,
                    )
                )
            if stage_id == 'signal_decider':
                return EODPipelineStatusService._signal_decider_has_completion_proof(
                    candidate=candidate,
                    details=details,
                )
            if stage_id == 'signal_decider_evaluations':
                return EODPipelineStatusService._signal_decider_evaluations_has_completion_proof(
                    candidate=candidate,
                    details=details,
                )
            return True
        has_legacy_zero_counts = all(
            _coerce_int(candidate.get(key)) == 0
            for key in (
                'processed',
                'total',
                'succeeded',
                'failed',
                'remaining',
            )
        )
        if not has_legacy_zero_counts or not result:
            return False
        if stage_id == 'signal_decider':
            return EODPipelineStatusService._signal_decider_has_legacy_completion_proof(
                result
            )
        if stage_id == 'signal_decider_evaluations':
            return EODPipelineStatusService._signal_decider_evaluations_has_legacy_completion_proof(
                result
            )
        return False

    @staticmethod
    def _recovery_state_is_newer(
        *,
        stage_id: str,
        candidate: dict[str, Any],
        current: dict[str, Any],
        candidate_is_history: bool = False,
    ) -> bool:
        details = (
            candidate.get('details')
            if isinstance(candidate.get('details'), dict)
            else {}
        )
        source_run_ids = (
            candidate.get('source_run_ids')
            if isinstance(candidate.get('source_run_ids'), dict)
            else {}
        )
        candidate_status = str(candidate.get('status') or '').strip().lower()
        history_evidence = (
            candidate_is_history
            or str(details.get('progress_source') or '').strip()
            == 'official_stage_history'
        )
        if history_evidence:
            evidence_is_recovery = (
                stage_id in RECOVERABLE_STAGE_HISTORY_IDS
                and candidate_status == 'completed'
            ) or (
                stage_id in RECOVERABLE_PERSISTED_OUTPUT_STAGE_IDS
                and candidate_status == 'partial'
            )
            if stage_id in EXACT_COMPLETED_RECOVERY_STAGE_HISTORY_IDS:
                evidence_is_recovery = bool(
                    evidence_is_recovery
                    and EODPipelineStatusService._history_stage_has_authoritative_completion(
                        stage_id=stage_id,
                        candidate=candidate,
                    )
                )
        elif (
            stage_id in RECOVERABLE_PERSISTED_OUTPUT_STAGE_IDS
            and str(details.get('mode') or '').strip()
            in PERSISTED_OUTPUT_FALLBACK_MODES
        ):
            persisted_count = {
                'signal_features': details.get('rows_persisted'),
                'external_news_collect': details.get('items_persisted'),
                'external_news_enrich': details.get('items_persisted'),
            }.get(stage_id)
            evidence_is_recovery = _coerce_int(persisted_count) > 0
        elif (
            stage_id == 'agent_feedback_summaries'
            and str(details.get('mode') or '').strip()
            == 'agent_feedback_run_state'
            and candidate_status == 'completed'
        ):
            total = _coerce_int(candidate.get('total'))
            evidence_is_recovery = bool(
                str(source_run_ids.get('feedback_run_id') or '').strip()
                and total > 0
                and _coerce_int(candidate.get('processed')) == total
                and _coerce_int(candidate.get('succeeded')) == total
                and _coerce_int(candidate.get('failed')) == 0
                and _coerce_int(candidate.get('remaining')) == 0
            )
        else:
            evidence_is_recovery = False
        if not evidence_is_recovery:
            return False
        if stage_id in EXACT_COMPLETED_RECOVERY_STAGE_HISTORY_IDS:
            candidate_total = (
                EODPipelineStatusService._completion_scope_total(
                    stage_id=stage_id,
                    stage=candidate,
                )
            )
            current_total = _coerce_int(current.get('total'))
            if current_total > 0 and candidate_total < current_total:
                return False
            if history_evidence and current_total > 0:
                candidate_scope_identity = (
                    EODPipelineStatusService
                    ._completion_scope_identity(
                        stage_id=stage_id,
                        stage=candidate,
                    )
                )
                current_scope_identity = (
                    EODPipelineStatusService
                    ._completion_scope_identity(
                        stage_id=stage_id,
                        stage=current,
                    )
                )
                if not (
                    candidate_total == current_total
                    and candidate_scope_identity is not None
                    and candidate_scope_identity
                    == current_scope_identity
                ):
                    return False
        candidate_updated_at = _parse_iso_timestamp(
            _coerce_iso_timestamp(
                candidate.get('updated_at')
                or candidate.get('completed_at')
            )
            or ''
        )
        current_updated_at = _parse_iso_timestamp(
            _coerce_iso_timestamp(
                current.get('updated_at')
                or current.get('completed_at')
            )
            or ''
        )
        return (
            candidate_updated_at is not None
            and current_updated_at is not None
            and candidate_updated_at > current_updated_at
        )

    @staticmethod
    def _completion_scope_total(
        *,
        stage_id: str,
        stage: dict[str, Any],
    ) -> int:
        stored_total = _coerce_int(stage.get('total'))
        if stored_total > 0:
            return stored_total
        details = (
            stage.get('details')
            if isinstance(stage.get('details'), dict)
            else {}
        )
        result = (
            details.get('result_summary')
            if isinstance(details.get('result_summary'), dict)
            else {}
        )
        semantic_counts = DailyPipelineStageService._extract_stage_counts(
            stage_id=stage_id,
            result=result,
        )
        return _coerce_int(semantic_counts.get('total'))

    @staticmethod
    def _completion_scope_identity(
        *,
        stage_id: str,
        stage: dict[str, Any],
    ) -> tuple[Any, ...] | None:
        details = (
            stage.get('details')
            if isinstance(stage.get('details'), dict)
            else {}
        )
        if stage_id in {
            'agent_feedback_summaries',
            'signal_decider',
            'next_open_execution_plan',
            'signal_decider_evaluations',
        }:
            proof = (
                DailyPipelineStageService
                .validated_stage_completion_scope_proof(
                    stage_id,
                    details.get('completion_scope_proof'),
                )
            )
            if proof is None:
                return None
        if stage_id == 'agent_feedback_summaries':
            return (
                stage_id,
                proof['proof_digest'],
            )
        if stage_id == 'signal_decider':
            return (
                stage_id,
                proof['proof_digest'],
            )
        if stage_id == 'next_open_execution_plan':
            return (
                stage_id,
                proof['proof_digest'],
            )
        if stage_id == 'signal_decider_evaluations':
            return (
                stage_id,
                proof['proof_digest'],
            )
        # Legacy decision summaries retain counts, but not an exact symbol or
        # decision-item manifest. They may recover zero-scope legacy state,
        # but must not supersede a newer positive authoritative scope.
        return None

    def _build_official_stage(
        self,
        *,
        stage_id: str,
        market_date: str,
        official_run: dict[str, Any],
        official_stage: dict[str, Any],
        fallback_stage: dict[str, Any],
    ) -> dict[str, Any]:
        definition = STAGE_DEFINITIONS[stage_id]
        status = str(official_stage.get('status') or 'pending').strip().lower()
        stored_status = status
        parent_run_status = str(official_run.get('status') or '').strip().lower()
        orphaned_running_stage = bool(
            stored_status == 'running'
            and parent_run_status in TERMINAL_OFFICIAL_RUN_STATUSES
        )
        if orphaned_running_stage:
            status = 'interrupted'
        fallback_status = str(fallback_stage.get('status') or '').strip().lower()
        use_fallback_progress = (
            status == 'running'
            and fallback_status == 'running'
            and int(official_stage.get('processed') or 0) <= 0
            and int(fallback_stage.get('processed') or 0) > 0
        )
        progress_source = fallback_stage if use_fallback_progress else official_stage
        counts = build_stage_counts(
            total=progress_source.get('total'),
            succeeded=progress_source.get('succeeded'),
            failed=progress_source.get('failed'),
            processed=progress_source.get('processed'),
        )
        official_details = dict(official_stage.get('details') or {})
        has_result_summary = 'result_summary' in official_details
        result_summary = (
            official_details.get('result_summary')
            if isinstance(official_details.get('result_summary'), dict)
            else {}
        )
        result_summary_status = (
            DailyPipelineStageService._resolve_result_status(result_summary)
            if result_summary
            else status
        )
        status_repaired_from_result_summary = (
            stored_status == 'completed'
            and result_summary_status in {'partial', 'blocked', 'waiting'}
        )
        no_ready_content = daily_pipeline_result_has_no_ready_content(
            result_summary
        )
        if status_repaired_from_result_summary:
            status = result_summary_status
        status_repaired_for_non_authoritative_result = (
            stored_status == 'completed'
            and stage_id in EXACT_COMPLETED_RECOVERY_STAGE_HISTORY_IDS
            and not self._history_stage_has_authoritative_completion(
                stage_id=stage_id,
                candidate=official_stage,
            )
        )
        if status_repaired_for_non_authoritative_result:
            status = (
                'blocked'
                if stage_id == 'next_open_execution_plan'
                else 'partial'
            )
        should_repair_legacy_counts = (
            counts.processed == 0
            and counts.succeeded == 0
            and counts.failed == 0
        )
        if (
            stage_id in {'external_news_collect', 'external_news_enrich'}
            and result_summary
            and should_repair_legacy_counts
        ):
            result_counts = DailyPipelineStageService._extract_stage_counts(
                stage_id=stage_id,
                result=result_summary,
            )
            if (
                result_counts['total'] > 0
                or result_counts['processed'] > 0
                or DailyPipelineStageService._has_explicit_zero_stage_scope(
                    stage_id=stage_id,
                    result=result_summary,
                )
            ):
                counts = build_stage_counts(**result_counts)
        if (
            stage_id == 'agent_feedback_summaries'
            and stored_status == 'completed'
            and result_summary
        ):
            feedback_counts = DailyPipelineStageService._extract_stage_counts(
                stage_id=stage_id,
                result=result_summary,
            )
            if feedback_counts['total'] > 0:
                counts = build_stage_counts(**feedback_counts)
        if (
            stage_id in {'signal_decider', 'signal_decider_evaluations'}
            and stored_status == 'completed'
            and result_summary
            and should_repair_legacy_counts
            and self._history_stage_has_authoritative_completion(
                stage_id=stage_id,
                candidate=official_stage,
            )
        ):
            semantic_counts = DailyPipelineStageService._extract_stage_counts(
                stage_id=stage_id,
                result=result_summary,
            )
            if (
                semantic_counts['total'] > 0
                and semantic_counts['processed'] == semantic_counts['total']
                and semantic_counts['succeeded'] == semantic_counts['total']
                and semantic_counts['failed'] == 0
            ):
                counts = build_stage_counts(**semantic_counts)
        status_repaired_for_incomplete_counts = (
            stored_status == 'completed'
            and (counts.failed > 0 or counts.remaining > 0)
        )
        if status_repaired_for_incomplete_counts:
            status = 'partial'
        fallback_details = dict(fallback_stage.get('details') or {})
        uses_fallback_coverage_diagnostics = (
            stage_id == 'pykrx_enrichment'
            and bool(fallback_details.get('categories'))
        )
        details = (
            {**fallback_details, **official_details}
            if uses_fallback_coverage_diagnostics or no_ready_content
            else official_details
        )
        if uses_fallback_coverage_diagnostics:
            authoritative_coverage_details = {
                key: value
                for key, value in fallback_details.items()
                if key
                in {
                    'requested_symbol_count',
                    'requested_market_count',
                    'categories',
                    'missing_categories',
                    'not_applicable_categories',
                    'coverage_warning_categories',
                }
                or key.startswith('decider_')
            }
            details.update(authoritative_coverage_details)
        if orphaned_running_stage:
            details.update(
                {
                    'orphaned_running_stage': True,
                    'orphan_reason': 'terminal_parent_with_running_stage',
                    'parent_run_status': parent_run_status,
                }
            )
        if no_ready_content:
            details.update(
                {
                    'gate_status': 'blocked',
                    'severity': 'blocked',
                    'blocking_status': 'blocking',
                    'gate_reason': 'no_decision_ready_rows',
                }
            )
        if result_summary:
            compacted_result_summary, read_time_omissions = (
                DailyPipelineStageService.compact_stage_result_summary(result_summary)
            )
            details['result_summary'] = compacted_result_summary
            if read_time_omissions:
                existing_omissions = details.get('result_summary_omissions')
                details['result_summary_omissions'] = {
                    **(
                        dict(existing_omissions)
                        if isinstance(existing_omissions, dict)
                        else {}
                    ),
                    **read_time_omissions,
                }
        count_semantics = DailyPipelineStageService._extract_stage_count_semantics(
            stage_id=stage_id,
            result=result_summary,
        )
        if count_semantics:
            details['count_semantics'] = count_semantics
        if uses_fallback_coverage_diagnostics:
            details['coverage_diagnostics_source'] = 'stage_specific_fallback'
            details['coverage_diagnostics_updated_at'] = fallback_stage.get('updated_at')
        if (
            status_repaired_for_incomplete_counts
            or status_repaired_from_result_summary
            or status_repaired_for_non_authoritative_result
        ):
            details['status_repaired_from'] = 'completed'
            details['status_repair_reason'] = (
                'incomplete_terminal_counts'
                if status_repaired_for_incomplete_counts
                else (
                    'incomplete_result_summary'
                    if status_repaired_from_result_summary
                    else 'non_authoritative_terminal_result'
                )
            )
        details['mode'] = 'official_daily_pipeline_run_state'
        details['pipeline_run_id'] = official_run.get('id')
        details['pipeline_trigger'] = official_run.get('trigger')
        details['run_metadata'] = official_run.get('run_metadata') or {}
        if use_fallback_progress:
            details['progress_source'] = 'stage_specific_fallback_run'
            details['fallback_details'] = fallback_stage.get('details') or {}
        current_activity = official_stage.get('current_activity')
        if use_fallback_progress:
            current_activity = fallback_stage.get('current_activity') or current_activity
        if (
            status_repaired_for_incomplete_counts
            or status_repaired_from_result_summary
            or status_repaired_for_non_authoritative_result
        ):
            current_activity = (
                f'Completed with {counts.failed} failed items.'
                if counts.failed > 0
                else (
                    f'Incomplete with {counts.remaining} '
                    f'{"item" if counts.remaining == 1 else "items"} remaining.'
                    if counts.remaining > 0
                    else 'Incomplete role progress reported.'
                )
            )
        if status in {'failed', 'interrupted', 'abandoned'} and official_stage.get('error_message'):
            current_activity = official_stage.get('error_message')
        if orphaned_running_stage:
            current_activity = (
                'Stage was still marked running after its parent pipeline run '
                f'became {parent_run_status}; treating it as interrupted.'
            )
        completed_at = official_stage.get('completed_at')
        if orphaned_running_stage and completed_at is None:
            completed_at = official_run.get('completed_at') or official_run.get(
                'updated_at'
            )
        stage_payload = {
            'task': stage_id,
            'label': definition.label,
            'status': status,
            'source_status': official_stage.get('status'),
            'market_date': official_stage.get('market_date') or market_date,
            'started_at': official_stage.get('started_at'),
            'updated_at': progress_source.get('updated_at') or official_stage.get('updated_at') or official_run.get('updated_at'),
            'completed_at': completed_at,
            'processed': counts.processed,
            'total': counts.total,
            'succeeded': counts.succeeded,
            'failed': counts.failed,
            'remaining': counts.remaining,
            'current_step': fallback_stage.get('current_step') if use_fallback_progress else official_stage.get('current_step'),
            'current_activity': current_activity or _default_activity(definition.label, status),
            'current_symbol': fallback_stage.get('current_symbol') if use_fallback_progress else official_stage.get('current_symbol'),
            'current_period': fallback_stage.get('current_period') if use_fallback_progress else official_stage.get('current_period'),
            'source_run_ids': (
                {**(fallback_stage.get('source_run_ids') or {}), **(official_stage.get('source_run_ids') or {})}
                if use_fallback_progress
                else official_stage.get('source_run_ids') or {}
            ),
            'details': details,
        }
        if stage_id == 'pykrx_enrichment':
            for field_name in ('gate_status', 'severity', 'blocking_status'):
                if fallback_stage.get(field_name) is not None:
                    stage_payload[field_name] = fallback_stage[field_name]
        if no_ready_content:
            stage_payload.update(
                {
                    'gate_status': 'blocked',
                    'severity': 'blocked',
                    'blocking_status': 'blocking',
                }
            )
        return stage_payload

    def _build_agent_feedback_stage(
        self,
        *,
        market_date: str,
        feedback_run: dict[str, Any],
        fallback_stage: dict[str, Any],
    ) -> dict[str, Any]:
        definition = STAGE_DEFINITIONS['agent_feedback_summaries']
        summary = dict(feedback_run.get('summary') if isinstance(feedback_run.get('summary'), dict) else {})
        run_status = str(summary.get('run_status') or feedback_run.get('status') or 'pending').strip().lower()
        failed_items = _coerce_int(summary.get('failed_items'), summary.get('buffered_failure_count'))
        completed_feedback_count = _coerce_int(
            summary.get('completed_feedback_count'),
            summary.get('flushed_feedback_count'),
        )
        expected_feedback_count = _coerce_int(summary.get('expected_feedback_count'))
        if expected_feedback_count <= 0:
            requested_symbol_count = len(feedback_run.get('requested_symbols') or [])
            remaining_roles = summary.get('remaining_roles') if isinstance(summary.get('remaining_roles'), list) else []
            completed_roles = summary.get('completed_roles') if isinstance(summary.get('completed_roles'), list) else []
            expected_feedback_count = requested_symbol_count * max(len(remaining_roles) + len(completed_roles), 1)

        if run_status == 'running':
            status = 'running'
        elif run_status in {'blocked', 'waiting'}:
            status = run_status
        elif failed_items > 0 and completed_feedback_count <= 0:
            status = 'failed'
        elif failed_items > 0:
            status = 'partial'
        elif expected_feedback_count > 0 and completed_feedback_count >= expected_feedback_count:
            status = 'completed'
        elif completed_feedback_count > 0:
            status = 'partial'
        else:
            status = run_status if run_status in {'failed', 'partial', 'completed'} else 'pending'

        processed = min(
            expected_feedback_count,
            max(0, completed_feedback_count + failed_items),
        ) if expected_feedback_count > 0 else max(0, completed_feedback_count + failed_items)
        counts = build_stage_counts(
            total=expected_feedback_count,
            succeeded=completed_feedback_count,
            failed=failed_items,
            processed=processed,
        )
        current_role = summary.get('current_role')
        activity = self._build_agent_feedback_activity(
            status=status,
            current_role=str(current_role or '').strip() or None,
            failed_items=failed_items,
        )
        requested_symbols = list(feedback_run.get('requested_symbols') or [])
        seconds_since_last_feedback = summary.get('seconds_since_last_feedback')
        stalled_threshold_seconds = summary.get('stalled_threshold_seconds')
        stalled = False
        try:
            stalled = (
                status == 'running'
                and seconds_since_last_feedback is not None
                and stalled_threshold_seconds is not None
                and int(seconds_since_last_feedback) > int(stalled_threshold_seconds)
            )
        except (TypeError, ValueError):
            stalled = False
        role_progress = summary.get('role_progress') if isinstance(summary.get('role_progress'), dict) else {}
        current_role_name = str(current_role or '').strip()
        active_roles = [
            role
            for role, role_payload in role_progress.items()
            if isinstance(role_payload, dict)
            and str(role_payload.get('status') or '').strip().lower() in {'running', 'in_progress'}
        ]
        if current_role_name and current_role_name not in active_roles:
            active_roles.insert(0, current_role_name)
        target_symbols = _coerce_int(
            summary.get('total_symbols'),
            len(requested_symbols),
        )
        completed_symbols = _coerce_int(summary.get('completed_symbols'))
        partial_symbols = _coerce_int(summary.get('partial_symbols'))
        in_progress_symbols = _coerce_int(summary.get('in_progress_symbols'))
        pending_symbols = _coerce_int(summary.get('pending_symbols'))
        latest_successful_feedback_at = summary.get('latest_feedback_at')
        latest_failed_feedback_at = summary.get('latest_failed_feedback_at')
        latest_child_activity_at = _latest_iso_timestamp(
            latest_successful_feedback_at,
            latest_failed_feedback_at,
            summary.get('last_activity_at'),
            fallback_stage.get('updated_at'),
        )
        details = {
            'mode': 'agent_feedback_run_state',
            'depends_on': list(definition.depends_on),
            'output_tables': list(definition.output_tables),
            'inspect_routes': list(definition.inspect_routes),
            'default_selected': definition.default_selected,
            'run_id': feedback_run.get('run_id'),
            'run_state_source': feedback_run.get('source'),
            'run_status': run_status,
            'run_table_status': summary.get('run_table_status'),
            'current_role': current_role,
            'current_active_roles': active_roles,
            'requested_symbol_count': len(requested_symbols),
            'target_symbols': target_symbols,
            'completed_symbols': completed_symbols,
            'partial_symbols': partial_symbols,
            'in_progress_symbols': in_progress_symbols,
            'pending_symbols': pending_symbols,
            'failed_symbols_count': failed_items,
            'failed_batches': summary.get('failure_groups') if isinstance(summary.get('failure_groups'), dict) else {},
            'failed_items': failed_items,
            'completed_feedback_count': completed_feedback_count,
            'expected_feedback_count': expected_feedback_count,
            'role_progress': role_progress,
            'role_symbol_counts': summary.get('role_symbol_counts') if isinstance(summary.get('role_symbol_counts'), dict) else {},
            'completed_roles': summary.get('completed_roles') if isinstance(summary.get('completed_roles'), list) else [],
            'remaining_roles': summary.get('remaining_roles') if isinstance(summary.get('remaining_roles'), list) else [],
            'latest_successful_feedback_at': latest_successful_feedback_at,
            'latest_feedback_at': latest_successful_feedback_at,
            'latest_failed_feedback_at': latest_failed_feedback_at,
            'latest_child_activity_at': latest_child_activity_at,
            'active_feedback_running': status == 'running',
            'feedback_failure_status': 'failures_present' if failed_items > 0 else 'no_failures',
            'feedback_health': 'stalled' if stalled else status,
            'seconds_since_last_feedback': seconds_since_last_feedback,
            'stalled_threshold_seconds': stalled_threshold_seconds,
        }
        return {
            'task': 'agent_feedback_summaries',
            'label': definition.label,
            'status': status,
            'source_status': run_status,
            'market_date': market_date,
            'started_at': summary.get('run_started_at') or fallback_stage.get('started_at'),
            'updated_at': latest_child_activity_at,
            'completed_at': summary.get('run_completed_at') if status != 'running' else None,
            'processed': counts.processed,
            'total': counts.total,
            'succeeded': counts.succeeded,
            'failed': counts.failed,
            'remaining': counts.remaining,
            'current_step': 'agent_feedback_running' if status == 'running' else None,
            'current_activity': activity,
            'current_symbol': None,
            'current_period': current_role,
            'source_run_ids': {'feedback_run_id': feedback_run.get('run_id')},
            'gate_status': 'waiting' if status == 'running' else ('ready' if status == 'completed' else status),
            'severity': 'degraded' if status == 'partial' else status,
            'blocking_status': 'blocking' if status in {'failed', 'blocked'} else 'non_blocking',
            'details': details,
        }

    @staticmethod
    def _build_agent_feedback_activity(
        *,
        status: str,
        current_role: str | None,
        failed_items: int,
    ) -> str:
        if status == 'running' and current_role:
            return f'Running agent feedback for {current_role}'
        if status == 'running':
            return 'Running agent feedback'
        if failed_items > 0:
            return f'Agent feedback has {failed_items} failed item(s)'
        return _default_activity(_stage_label('agent_feedback_summaries'), status)

    def _build_signal_features_stage(
        self,
        *,
        market_date: str,
        aggregate: dict[str, Any],
        expected_total: int = 0,
        scope_symbols: list[str] | None = None,
    ) -> dict[str, Any]:
        persisted = int(
            aggregate.get('persisted') or aggregate.get('rows_persisted') or 0
        )
        semantic_ready = int(
            aggregate.get('semantic_ready_count')
            if aggregate.get('semantic_ready_count') is not None
            else persisted
        )
        normalized_total = max(int(expected_total or 0), semantic_ready)
        if semantic_ready <= 0:
            status = 'pending'
        elif normalized_total > 0 and semantic_ready < normalized_total:
            status = 'partial'
        else:
            status = 'completed'
        coverage_ratio = (
            semantic_ready / normalized_total if normalized_total > 0 else 0.0
        )
        partial_is_progressable = bool(
            status == 'partial'
            and coverage_ratio
            >= PARTIAL_ROW_COUNT_MIN_NON_BLOCKING_COVERAGE_RATIO
        )
        counts = build_stage_counts(
            total=normalized_total,
            succeeded=semantic_ready,
            failed=max(normalized_total - semantic_ready, 0),
            processed=min(semantic_ready, normalized_total),
        )
        stage = {
            'task': 'signal_features',
            'label': _stage_label('signal_features'),
            'status': status,
            'source_status': (
                'stored_signal_features_present'
                if semantic_ready > 0
                else None
            ),
            'market_date': market_date,
            'started_at': None,
            'updated_at': aggregate.get('updated_at'),
            'completed_at': (
                aggregate.get('updated_at')
                if status in {'completed', 'partial'}
                else None
            ),
            'processed': counts.processed,
            'total': counts.total,
            'succeeded': counts.succeeded,
            'failed': counts.failed,
            'remaining': counts.remaining,
            'current_step': None,
            'current_activity': _default_activity(
                _stage_label('signal_features'), status
            ),
            'current_symbol': None,
            'current_period': None,
            'source_run_ids': {},
            'gate_status': (
                'ready'
                if status == 'completed' or partial_is_progressable
                else 'blocked' if status == 'partial' else 'waiting'
            ),
            'severity': (
                'ready'
                if status == 'completed'
                else 'degraded'
                if partial_is_progressable
                else 'blocked' if status == 'partial' else 'waiting'
            ),
            'blocking_status': (
                'non_blocking'
                if status == 'completed' or partial_is_progressable
                else 'blocking' if status == 'partial' else 'waiting'
            ),
            'details': {
                'mode': 'derived_from_daily_signal_features',
                'reason': (
                    'semantic_signal_features_available'
                    if status == 'completed'
                    else 'incomplete_semantic_signal_feature_coverage'
                    if status == 'partial'
                    else 'no_semantic_signal_features_for_market_date'
                ),
                'rows_persisted': persisted,
                'semantic_ready_rows': semantic_ready,
                'semantic_unready_rows': int(
                    aggregate.get('semantic_unready_count')
                    if aggregate.get('semantic_unready_count') is not None
                    else max(persisted - semantic_ready, 0)
                ),
                'direct_signal_rows': int(
                    aggregate.get('direct_signal_rows') or 0
                ),
                'fallback_usable_rows': int(
                    aggregate.get('fallback_usable_rows') or 0
                ),
                'fallback_only_rows': int(
                    aggregate.get('fallback_only_rows') or 0
                ),
                'expected_rows': normalized_total,
                'coverage_ratio': round(coverage_ratio, 6),
                'min_non_blocking_coverage_ratio': (
                    PARTIAL_ROW_COUNT_MIN_NON_BLOCKING_COVERAGE_RATIO
                ),
                'semantic_readiness_rule': (
                    'signal_count_positive_or_owned_fallback_features_present'
                ),
            },
        }
        return self._apply_scope_coverage_gate(
            stage_id='signal_features',
            market_date=market_date,
            stage=stage,
            scope_symbols=list(scope_symbols or []),
        )

    def _build_external_news_collect_stage(
        self,
        *,
        market_date: str,
        aggregate: dict[str, Any],
    ) -> dict[str, Any]:
        item_count = int(aggregate.get('items') or aggregate.get('items_persisted') or 0)
        return self._build_persisted_count_stage(
            stage_id='external_news_collect',
            market_date=market_date,
            persisted=item_count,
            updated_at=aggregate.get('updated_at'),
            source_status='stored_external_news_present',
            mode='derived_from_external_news_items',
            available_reason='persisted_external_news_items_available',
            pending_reason='no_external_news_items_collected_for_market_date',
            extra_details={
                'items_persisted': item_count,
                'symbols_with_news': int(aggregate.get('symbols') or aggregate.get('symbol_count') or 0),
                'sample_limited': bool(aggregate.get('sample_limited')),
                'sample_limit': aggregate.get('sample_limit'),
            },
        )

    def _build_external_news_enrich_stage(
        self,
        *,
        market_date: str,
        aggregate: dict[str, Any],
        collect_stage: dict[str, Any],
    ) -> dict[str, Any]:
        definition = STAGE_DEFINITIONS['external_news_enrich']
        item_count = int(aggregate.get('items') or aggregate.get('items_persisted') or 0)
        classified_count = int(
            aggregate.get('contract_classified_items')
            if aggregate.get('contract_classified_items') is not None
            else aggregate.get('classified_items')
            or aggregate.get('classified')
            or 0
        )
        legacy_classified_count = int(aggregate.get('legacy_classified_items') or 0)
        if item_count <= 0:
            status = 'pending'
            source_status = None
        elif classified_count >= item_count:
            status = 'completed'
            source_status = 'stored_external_news_classified'
        elif classified_count > 0:
            status = 'partial'
            source_status = 'stored_external_news_partially_classified'
        else:
            status = 'pending'
            source_status = 'stored_external_news_unclassified'

        counts = build_stage_counts(
            total=max(item_count, classified_count),
            succeeded=classified_count,
            failed=0,
            processed=classified_count,
        )
        coverage_ratio = (
            round(classified_count / item_count, 6)
            if item_count > 0
            else None
        )
        details = {
            'mode': 'derived_from_external_news_items',
            'depends_on': list(definition.depends_on),
            'blocking_stages': (
                ['external_news_collect']
                if str(collect_stage.get('status') or '').strip().lower() == 'pending' and item_count <= 0
                else []
            ),
            'items_persisted': item_count,
            'classified_items': classified_count,
            'contract_classified_items': classified_count,
            'legacy_classified_items': legacy_classified_count,
            'classification_coverage_ratio': coverage_ratio,
            'symbols_with_news': int(aggregate.get('symbols') or aggregate.get('symbol_count') or 0),
            'sample_limited': bool(aggregate.get('sample_limited')),
            'sample_limit': aggregate.get('sample_limit'),
            'reason': (
                'persisted_external_news_classification_available'
                if status == 'completed'
                else 'external_news_items_need_classification'
                if item_count > 0
                else 'no_external_news_items_collected_for_market_date'
            ),
        }
        return {
            'task': 'external_news_enrich',
            'label': definition.label,
            'status': status,
            'source_status': source_status,
            'market_date': market_date,
            'started_at': None,
            'updated_at': aggregate.get('updated_at'),
            'completed_at': aggregate.get('updated_at') if status in {'completed', 'partial'} else None,
            'processed': counts.processed,
            'total': counts.total,
            'succeeded': counts.succeeded,
            'failed': counts.failed,
            'remaining': counts.remaining,
            'current_step': None,
            'current_activity': self._build_external_news_enrich_activity(
                status=status,
                item_count=item_count,
                classified_count=classified_count,
            ),
            'current_symbol': None,
            'current_period': None,
            'source_run_ids': {},
            'details': details,
        }

    def _build_macro_context_stage(
        self,
        *,
        market_date: str,
        aggregate: dict[str, Any],
    ) -> dict[str, Any]:
        definition = STAGE_DEFINITIONS['macro_context_refresh']
        persisted = int(aggregate.get('persisted') or 0)
        required_series_count = int(aggregate.get('required_series_count') or persisted or 0)
        context_status = str(aggregate.get('context_status') or 'unavailable').strip().lower()
        missing_series = list(aggregate.get('missing_series') or [])
        stale_series = list(aggregate.get('stale_series') or [])
        if context_status == 'available':
            status = 'completed'
            source_status = 'stored_macro_context_fresh'
        elif context_status in {'partial', 'stale'} and persisted > 0:
            status = 'partial'
            source_status = 'stored_macro_context_partial' if context_status == 'partial' else 'stored_macro_context_stale'
        else:
            status = 'pending'
            source_status = None
        counts = build_stage_counts(
            total=max(required_series_count, persisted + len(missing_series)),
            succeeded=persisted,
            failed=0,
            processed=persisted,
        )
        reason = (
            'persisted_macro_context_available'
            if status == 'completed'
            else 'persisted_macro_context_stale_or_partial'
            if status == 'partial'
            else 'no_persisted_macro_context_available'
        )
        return {
            'task': 'macro_context_refresh',
            'label': definition.label,
            'status': status,
            'source_status': source_status,
            'market_date': market_date,
            'started_at': None,
            'updated_at': aggregate.get('updated_at'),
            'completed_at': aggregate.get('updated_at') if status in {'completed', 'partial'} else None,
            'processed': counts.processed,
            'total': counts.total,
            'succeeded': counts.succeeded,
            'failed': counts.failed,
            'remaining': counts.remaining,
            'current_step': None,
            'current_activity': _default_activity(definition.label, status),
            'current_symbol': None,
            'current_period': None,
            'source_run_ids': {},
            'details': {
                'mode': 'derived_from_macro_context_observations',
                'depends_on': list(definition.depends_on),
                'reason': reason,
                'context_status': context_status,
                'required_series_count': required_series_count,
                'persisted_series_count': persisted,
                'missing_series': missing_series,
                'stale_series': stale_series,
                'source_statuses': aggregate.get('source_statuses') or {},
                'macro_regime_summary': aggregate.get('macro_regime_summary') or {},
                'freshness': aggregate.get('freshness') or {},
                'usage_policy': {
                    **dict(aggregate.get('usage_policy') or {}),
                    'contextual_input_only': True,
                    'direct_buy_sell_signal': False,
                    'llm_generated': False,
                },
                'output_tables': list(definition.output_tables),
                'inspect_routes': list(definition.inspect_routes),
                'default_selected': definition.default_selected,
            },
        }

    @staticmethod
    def _build_external_news_enrich_activity(
        *,
        status: str,
        item_count: int,
        classified_count: int,
    ) -> str:
        label = _stage_label('external_news_enrich')
        if item_count > 0 and status == 'partial':
            return f'Classified {classified_count}/{item_count} external news item(s)'
        if item_count > 0 and status == 'pending':
            return f'Waiting to classify {item_count} external news item(s)'
        return _default_activity(label, status)

    def _build_signal_decider_stage(
        self,
        *,
        market_date: str,
        aggregate: dict[str, Any],
    ) -> dict[str, Any]:
        feedback_rows = int(aggregate.get('feedback_rows') or 0)
        snapshot_rows = int(aggregate.get('decision_snapshots') or aggregate.get('snapshot_rows') or 0)
        persisted = max(feedback_rows, snapshot_rows)
        return self._build_persisted_count_stage(
            stage_id='signal_decider',
            market_date=market_date,
            persisted=persisted,
            updated_at=aggregate.get('updated_at'),
            source_status='stored_signal_decider_outputs_present',
            mode='derived_from_signal_decider_outputs',
            available_reason='persisted_signal_decider_outputs_available',
            pending_reason='no_signal_decider_feedback_or_snapshots_for_market_date',
            extra_details={
                'feedback_rows': feedback_rows,
                'feedback_symbols': int(aggregate.get('feedback_symbols') or 0),
                'decision_snapshots': snapshot_rows,
                'decision_snapshot_symbols': int(aggregate.get('decision_snapshot_symbols') or 0),
            },
        )

    def _build_persisted_count_stage(
        self,
        *,
        stage_id: str,
        market_date: str,
        persisted: int,
        updated_at: Any,
        source_status: str,
        mode: str,
        available_reason: str,
        pending_reason: str,
        extra_details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        definition = STAGE_DEFINITIONS[stage_id]
        persisted_count = int(persisted or 0)
        status = 'completed' if persisted_count > 0 else 'pending'
        counts = build_stage_counts(
            total=persisted_count,
            succeeded=persisted_count,
            failed=0,
            processed=persisted_count,
        )
        return {
            'task': stage_id,
            'label': definition.label,
            'status': status,
            'source_status': source_status if persisted_count > 0 else None,
            'market_date': market_date,
            'started_at': None,
            'updated_at': updated_at,
            'completed_at': updated_at if status == 'completed' else None,
            'processed': counts.processed,
            'total': counts.total,
            'succeeded': counts.succeeded,
            'failed': counts.failed,
            'remaining': counts.remaining,
            'current_step': None,
            'current_activity': _default_activity(definition.label, status),
            'current_symbol': None,
            'current_period': None,
            'source_run_ids': {},
            'details': {
                'mode': mode,
                'depends_on': list(definition.depends_on),
                'available': persisted_count > 0,
                'reason': available_reason if persisted_count > 0 else pending_reason,
                'output_tables': list(definition.output_tables),
                'inspect_routes': list(definition.inspect_routes),
                'default_selected': definition.default_selected,
                **dict(extra_details or {}),
            },
        }

    @staticmethod
    def _build_official_run_summary(official_run: dict[str, Any] | None) -> dict[str, Any] | None:
        if official_run is None:
            return None
        return {
            'id': official_run.get('id'),
            'market_date': official_run.get('market_date'),
            'trigger': official_run.get('trigger'),
            'status': official_run.get('status'),
            'current_stage': official_run.get('current_stage'),
            'started_at': official_run.get('started_at'),
            'updated_at': official_run.get('updated_at'),
            'completed_at': official_run.get('completed_at'),
            'error_message': official_run.get('error_message'),
            'run_metadata': official_run.get('run_metadata') or {},
        }

    def _build_pykrx_enrichment_stage(
        self,
        *,
        market_date: str,
        coverage: dict[str, Any],
        run: dict[str, Any] | None = None,
        scope_symbols: list[str] | None = None,
    ) -> dict[str, Any]:
        category_rows = dict(coverage.get('categories') or {})
        run_metadata = get_run_metadata(run) if run else {}
        stage_progress = run_metadata.get('stage_progress') if isinstance(run_metadata.get('stage_progress'), dict) else {}
        progress = dict(stage_progress.get('pykrx_enrichment') or {})
        total = len(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES)
        latest_captured_at = None
        for item in category_rows.values():
            candidate = item.get('latest_captured_at')
            if candidate and (latest_captured_at is None or str(candidate) > str(latest_captured_at)):
                latest_captured_at = candidate
        applicable_category_rows = [
            item
            for item in category_rows.values()
            if str(item.get('coverage_status') or '').strip().lower()
            != 'not_applicable'
        ]
        all_missing = bool(applicable_category_rows) and all(
            int(item.get('present_rows') or 0) <= 0
            for item in applicable_category_rows
        )
        present_count = sum(1 for item in category_rows.values() if int(item.get('present_rows') or 0) > 0)
        not_applicable_categories = [
            category
            for category, item in category_rows.items()
            if str(item.get('coverage_status') or '').strip().lower()
            == 'not_applicable'
        ]
        satisfied_count = present_count + len(not_applicable_categories)
        missing_categories = [
            category
            for category, item in category_rows.items()
            if str(item.get('coverage_status') or '').strip().lower()
            != 'not_applicable'
            and (
                str(item.get('coverage_status') or 'missing') == 'missing'
                or int(item.get('present_rows') or 0) <= 0
            )
        ]
        coverage_warning_categories = [
            category
            for category, item in category_rows.items()
            if category not in missing_categories
            and category not in not_applicable_categories
            and str(item.get('coverage_status') or '') != 'complete'
        ]
        failed_count = len(missing_categories)
        coverage_complete = bool(category_rows) and satisfied_count >= total and failed_count == 0
        source_status = None
        if not category_rows or (all_missing and latest_captured_at is None):
            counts = build_stage_counts(total=total, succeeded=0, failed=0, processed=0)
            status = 'pending'
        else:
            counts = build_stage_counts(
                total=total,
                succeeded=total if coverage_complete else satisfied_count,
                failed=failed_count,
                processed=(
                    total
                    if coverage_complete
                    else min(satisfied_count + failed_count, total)
                ),
            )
            if coverage_complete:
                status = 'completed'
                source_status = 'completed_with_coverage_warnings' if coverage_warning_categories else 'completed'
            else:
                status = 'partial'
                source_status = 'partial'
        progress_status = _normalize_status(progress.get('status'))
        if progress_status in {'running', 'failed'}:
            status = progress_status
            source_status = progress.get('status') or source_status
            counts = build_stage_counts(
                total=progress.get('total') or total,
                succeeded=progress.get('succeeded'),
                failed=progress.get('failed'),
                processed=progress.get('processed'),
            )
        elif progress_status in {'completed', 'partial'} and status == 'pending':
            status = progress_status
            source_status = progress.get('status') or source_status
            counts = build_stage_counts(
                total=progress.get('total') or total,
                succeeded=progress.get('succeeded'),
                failed=progress.get('failed'),
                processed=progress.get('processed'),
            )
        progress_extras = _progress_extras(progress)
        updated_at = progress.get('updated_at') or latest_captured_at
        completed_at = progress.get('completed_at') or (latest_captured_at if status in {'completed', 'partial'} else None)
        source_run_ids = {'eod_run_id': run.get('id')} if run and progress else {}
        gate_details = AgentFeedbackPrerequisiteService.build_pykrx_enrichment_gate_details(
            category_payloads=category_rows,
        )
        gate_status = str(gate_details.get('decider_category_gate_status') or 'waiting')
        decision_warning_categories = [
            str(item.get('category') or '').strip()
            for item in gate_details.get('decider_warning_categories') or []
            if isinstance(item, dict)
            and str(item.get('category') or '').strip()
        ]
        if decision_warning_categories:
            coverage_warning_categories = list(
                dict.fromkeys(
                    [
                        *coverage_warning_categories,
                        *decision_warning_categories,
                    ]
                )
            )
            if status == 'completed':
                source_status = 'completed_with_coverage_warnings'
        decision_evidence_blocking_categories = [
            str(item.get('category') or '').strip()
            for item in gate_details.get('decider_blocking_categories') or []
            if isinstance(item, dict)
            and str(item.get('category') or '').strip()
            and item.get('decision_usable_evidence_available') is True
        ]
        if decision_evidence_blocking_categories:
            missing_categories = list(
                dict.fromkeys(
                    [
                        *missing_categories,
                        *decision_evidence_blocking_categories,
                    ]
                )
            )
            coverage_warning_categories = [
                category
                for category in coverage_warning_categories
                if category not in decision_evidence_blocking_categories
            ]
            if status in {'completed', 'partial'}:
                status = 'partial'
                source_status = 'partial'
                failed_count = len(missing_categories)
                counts = build_stage_counts(
                    total=total,
                    succeeded=max(total - failed_count, 0),
                    failed=failed_count,
                    processed=total,
                )
        normalized_scope_symbols = self._normalize_symbol_list(
            scope_symbols or []
        )
        requested_scope_count = len(normalized_scope_symbols)
        exact_scope_version = str(
            progress_extras.get('scope_outcome_manifest_version') or ''
        ).strip()

        def exact_scope_list(key: str) -> tuple[list[str], bool]:
            raw = progress_extras.get(key)
            if not isinstance(raw, list):
                return [], False
            normalized = self._normalize_symbol_list(raw)
            return normalized, len(raw) == len(normalized)

        selected_scope, selected_valid = exact_scope_list(
            'selected_stock_codes'
        )
        successful_scope, successful_valid = exact_scope_list(
            'successful_stock_codes'
        )
        retry_scope, retry_valid = exact_scope_list(
            'retry_debt_stock_codes'
        )
        terminal_scope, terminal_valid = exact_scope_list(
            'terminal_not_applicable_stock_codes'
        )
        selected_scope_set = set(selected_scope)
        successful_scope_set = set(successful_scope)
        retry_scope_set = set(retry_scope)
        terminal_scope_set = set(terminal_scope)
        raw_scope_manifest = progress_extras.get('scope_outcome_manifest')
        manifest_outcomes: dict[str, str] = {}
        manifest_valid = isinstance(raw_scope_manifest, list)
        if manifest_valid:
            for item in raw_scope_manifest:
                if not isinstance(item, dict):
                    manifest_valid = False
                    break
                stock_code = str(item.get('stock_code') or '').strip().upper()
                outcome = str(item.get('outcome') or '').strip().lower()
                if (
                    not stock_code
                    or stock_code in manifest_outcomes
                    or outcome
                    not in {
                        'success',
                        'retry_debt',
                        'terminal_not_applicable',
                    }
                ):
                    manifest_valid = False
                    break
                manifest_outcomes[stock_code] = outcome
        expected_manifest_outcomes = {
            **{stock_code: 'success' for stock_code in successful_scope},
            **{stock_code: 'retry_debt' for stock_code in retry_scope},
            **{
                stock_code: 'terminal_not_applicable'
                for stock_code in terminal_scope
            },
        }
        exact_partition_valid = (
            exact_scope_version == 'pykrx_enrichment_scope_outcomes_v1'
            and selected_valid
            and successful_valid
            and retry_valid
            and terminal_valid
            and not (successful_scope_set & retry_scope_set)
            and not (successful_scope_set & terminal_scope_set)
            and not (retry_scope_set & terminal_scope_set)
            and selected_scope_set
            == successful_scope_set | retry_scope_set | terminal_scope_set
            and manifest_valid
            and manifest_outcomes == expected_manifest_outcomes
        )
        normalized_scope_set = set(normalized_scope_symbols)
        exact_scope_loaded = (
            requested_scope_count > 0
            and exact_partition_valid
            and normalized_scope_set.issubset(selected_scope_set)
        )
        missing_scope_symbols = [
            stock_code
            for stock_code in normalized_scope_symbols
            if stock_code not in successful_scope_set
        ]
        requested_scope_coverage = None
        if requested_scope_count:
            scope_ready = (
                status in {'completed', 'partial'}
                and exact_scope_loaded
                and not missing_scope_symbols
                and gate_status == 'ready'
            )
            scope_waiting = status in {'pending', 'running', 'waiting'}
            requested_scope_coverage = {
                'evidence_version': 'pykrx_enrichment_scope_coverage_v1',
                'authoritative': exact_scope_loaded,
                'requested_count': requested_scope_count,
                'present_count': (
                    requested_scope_count - len(missing_scope_symbols)
                    if exact_scope_loaded
                    else 0
                ),
                'missing_count': (
                    len(missing_scope_symbols)
                    if exact_scope_loaded
                    else requested_scope_count
                ),
                'missing_symbols_sample': (
                    missing_scope_symbols
                    if exact_scope_loaded
                    else normalized_scope_symbols
                )[:ROW_COUNT_STAGE_MISSING_SAMPLE_LIMIT],
            }
            if not scope_ready:
                gate_status = 'waiting' if scope_waiting else 'blocked'
        severity = 'blocked' if gate_status == 'blocked' else gate_status
        blocking_status = 'blocking' if gate_status == 'blocked' else gate_status
        return {
            'task': 'pykrx_enrichment',
            'label': STAGE_LABELS['pykrx_enrichment'],
            'status': status,
            'source_status': source_status if (category_rows or progress) else None,
            'gate_status': gate_status,
            'severity': severity,
            'blocking_status': blocking_status if gate_status != 'ready' else 'non_blocking',
            'market_date': market_date,
            'started_at': progress.get('started_at'),
            'updated_at': updated_at,
            'completed_at': completed_at,
            'processed': counts.processed,
            'total': counts.total,
            'succeeded': counts.succeeded,
            'failed': counts.failed,
            'remaining': counts.remaining,
            'current_step': progress.get('current_step'),
            'current_activity': progress.get('current_activity') or _default_activity(STAGE_LABELS['pykrx_enrichment'], status),
            'current_symbol': None,
            'current_period': None,
            'source_run_ids': source_run_ids,
            'details': {
                **progress_extras,
                'requested_symbol_count': coverage.get('requested_symbol_count'),
                'requested_market_count': coverage.get('requested_market_count'),
                'categories': category_rows,
                'missing_categories': missing_categories,
                'not_applicable_categories': not_applicable_categories,
                'coverage_warning_categories': coverage_warning_categories,
                'scope_verification_required': bool(requested_scope_count),
                'requested_scope_coverage': requested_scope_coverage,
                'progress_source': 'live_run_metadata' if progress else 'stored_rows',
                **gate_details,
            },
        }

    def _build_pykrx_stages(
        self,
        *,
        market_date: str,
        run: dict[str, Any] | None,
        stage_counts: dict[str, dict[str, Any]],
        live_symbols_total: int,
    ) -> dict[str, dict[str, Any]]:
        if not run:
            return {
                stage_name: self._pending_pykrx_stage(
                    stage_name=stage_name,
                    market_date=market_date,
                    live_symbols_total=live_symbols_total,
                )
                for stage_name in PYKRX_STAGE_ORDER
            }

        run_metadata = get_run_metadata(run)
        stage_progress = run_metadata.get('stage_progress') if isinstance(run_metadata.get('stage_progress'), dict) else {}
        current_stage = _infer_pykrx_stage(
            run_status=str(run.get('status') or ''),
            run_metadata=run_metadata,
            stage_counts=stage_counts,
        )
        current_stage_index = _stage_index(current_stage)
        return {
            stage_name: self._build_pykrx_stage(
                stage_name=stage_name,
                market_date=market_date,
                run=run,
                run_metadata=run_metadata,
                progress=dict(stage_progress.get(stage_name) or {}),
                aggregate=dict(stage_counts.get(stage_name) or {}),
                current_stage=current_stage,
                current_stage_index=current_stage_index,
                live_symbols_total=live_symbols_total,
            )
            for stage_name in PYKRX_STAGE_ORDER
        }

    def _pending_pykrx_stage(
        self,
        *,
        stage_name: str,
        market_date: str,
        live_symbols_total: int,
    ) -> dict[str, Any]:
        total = live_symbols_total if stage_name != 'trending_ranks' else len(DAY_BASED_TRENDING_PERIODS)
        counts = build_stage_counts(total=total, succeeded=0, failed=0, processed=0)
        return {
            'task': stage_name,
            'label': STAGE_LABELS[stage_name],
            'status': 'pending',
            'source_status': None,
            'market_date': market_date,
            'started_at': None,
            'updated_at': None,
            'completed_at': None,
            'processed': counts.processed,
            'total': counts.total,
            'succeeded': counts.succeeded,
            'failed': counts.failed,
            'remaining': counts.remaining,
            'current_step': None,
            'current_activity': _default_activity(STAGE_LABELS[stage_name], 'pending'),
            'current_symbol': None,
            'current_period': None,
            'source_run_ids': {},
            'details': {
                'mode': 'daily_auto' if stage_name == 'price_collection' else 'derived_from_pykrx_run',
            },
        }

    def _build_pykrx_stage(
        self,
        *,
        stage_name: str,
        market_date: str,
        run: dict[str, Any],
        run_metadata: dict[str, Any],
        progress: dict[str, Any],
        aggregate: dict[str, Any],
        current_stage: str | None,
        current_stage_index: int | None,
        live_symbols_total: int,
    ) -> dict[str, Any]:
        stage_index = PYKRX_STAGE_ORDER.index(stage_name)
        progress_status = _normalize_status(progress.get('status'))
        run_status = str(run.get('status') or '')
        if progress_status == 'running' and run_status != 'running':
            progress_status = 'failed' if run_status == 'failed' else None

        if stage_name == 'price_collection':
            counts = build_stage_counts(
                total=progress.get('total') or run.get('symbols_total') or live_symbols_total,
                succeeded=max(
                    int(progress.get('succeeded') or 0),
                    int(run.get('symbols_succeeded') or 0),
                    int(aggregate.get('persisted') or 0),
                ),
                failed=int(progress['failed']) if 'failed' in progress else int(run.get('symbols_failed') or 0),
                processed=int(progress['processed']) if 'processed' in progress else None,
            )
            started = True
        elif stage_name in {'technical_snapshots', 'action_summaries'}:
            total = int(progress.get('total') or run.get('symbols_total') or 0)
            succeeded = max(
                int(progress.get('succeeded') or 0),
                int(aggregate.get('persisted') or 0),
            )
            inferred_failed = max(total - succeeded, 0) if total else 0
            failed = int(progress['failed']) if 'failed' in progress else (
                inferred_failed
                if (
                    succeeded > 0
                    or progress_status in {'completed', 'partial', 'failed'}
                    or (current_stage_index is not None and stage_index < current_stage_index)
                )
                else 0
            )
            counts = build_stage_counts(
                total=total,
                succeeded=succeeded,
                failed=failed,
                processed=(
                    int(progress['processed'])
                    if 'processed' in progress
                    else min(succeeded + failed, total) if total else succeeded + failed
                ),
            )
            started = bool(progress or succeeded or failed or (current_stage_index is not None and stage_index < current_stage_index))
        else:
            succeeded = max(
                int(progress.get('succeeded') or 0),
                int(aggregate.get('periods_persisted') or 0),
            )
            failed = int(progress['failed']) if 'failed' in progress else 0
            total = int(progress.get('total') or len(DAY_BASED_TRENDING_PERIODS))
            counts = build_stage_counts(
                total=total,
                succeeded=succeeded,
                failed=failed,
                processed=int(progress['processed']) if 'processed' in progress else min(succeeded + failed, total),
            )
            started = bool(progress or succeeded or failed)

        if progress_status:
            status = progress_status
        elif run_status == 'running' and current_stage == stage_name:
            status = 'running'
        # Later pykrx stages remain pending until they actually start. Earlier completed counts alone
        # must not imply that downstream stages are already running or done for the current live run.
        elif current_stage_index is not None and stage_index > current_stage_index and not started:
            status = 'pending'
        elif run_status == 'failed' and current_stage == stage_name and not started:
            status = 'failed'
        elif not started:
            status = 'pending'
        elif counts.total and counts.succeeded >= counts.total and counts.failed == 0:
            status = 'completed'
        elif counts.failed > 0:
            status = 'partial' if counts.succeeded > 0 or run_status in {'completed', 'completed_with_errors'} else 'failed'
        elif run_status == 'completed_with_errors':
            status = 'partial'
        else:
            status = 'completed'

        started_at = progress.get('started_at') or run.get('started_at')
        updated_at = (
            progress.get('updated_at')
            or aggregate.get('updated_at')
            or run.get('completed_at')
            or run.get('started_at')
        )
        completed_at = progress.get('completed_at') or (
            None
            if status in {'pending', 'running'}
            else run.get('completed_at') or aggregate.get('updated_at')
        )
        current_step = (
            run_metadata.get('current_step')
            if status == 'running' and current_stage == stage_name
            else progress.get('current_step')
        )
        current_activity = (
            run_metadata.get('current_activity')
            if status == 'running' and current_stage == stage_name
            else progress.get('current_activity')
        )
        if status == 'failed' and run.get('error_message'):
            current_activity = str(run.get('error_message'))
        current_activity = current_activity or _default_activity(STAGE_LABELS[stage_name], status)
        current_symbol = (
            run_metadata.get('current_symbol')
            if status == 'running' and current_stage == stage_name
            else progress.get('current_symbol')
        )
        current_period = (
            run_metadata.get('current_period')
            if status == 'running' and current_stage == stage_name
            else progress.get('current_period')
        )

        details = {
            **_progress_extras(progress),
            'mode': run_metadata.get('mode'),
            'trigger': run.get('trigger'),
            'latest_available_trade_date': run_metadata.get('latest_available_trade_date'),
            'requested_start_date': run_metadata.get('requested_start_date'),
            'requested_end_date': run_metadata.get('requested_end_date'),
            'markets': run_metadata.get('markets') or list(DEFAULT_MARKETS),
        }
        if stage_name == 'price_collection':
            details['bars_upserted'] = _progress_extras(progress).get('bars_upserted', aggregate.get('persisted'))
            details['universe_refreshed'] = bool(run_metadata.get('universe_refreshed'))
            details['failed_symbols_count'] = int(run_metadata.get('failed_symbols_count') or 0)
        elif stage_name == 'technical_snapshots':
            details['persisted_symbols'] = int(aggregate.get('persisted') or 0)
        elif stage_name == 'action_summaries':
            details['persisted_symbols'] = int(aggregate.get('persisted') or 0)
        else:
            details['periods_persisted'] = int(_progress_extras(progress).get('periods_persisted') or aggregate.get('periods_persisted') or 0)
            details['rows_persisted'] = int(_progress_extras(progress).get('rows_persisted') or aggregate.get('persisted') or 0)
            details['periods_total'] = len(DAY_BASED_TRENDING_PERIODS)

        return {
            'task': stage_name,
            'label': STAGE_LABELS[stage_name],
            'status': status,
            'source_status': progress.get('status') or run_status,
            'market_date': market_date,
            'started_at': started_at,
            'updated_at': updated_at,
            'completed_at': completed_at,
            'processed': counts.processed,
            'total': counts.total,
            'succeeded': counts.succeeded,
            'failed': counts.failed,
            'remaining': counts.remaining,
            'current_step': current_step,
            'current_activity': current_activity,
            'current_symbol': current_symbol,
            'current_period': current_period,
            'source_run_ids': {
                'eod_run_id': run.get('id'),
            },
            'details': details,
        }

    def _build_fundamentals_stage(
        self,
        *,
        market_date: str,
        fs_div: str,
        scope_symbols: list[str] | None = None,
    ) -> dict[str, Any]:
        run = self.fundamentals_refresh_loader(market_date=market_date, fs_div=fs_div)
        if not run:
            counts = build_stage_counts(total=0, succeeded=0, failed=0, processed=0)
            return {
                'task': 'fundamentals_daily_refresh',
                'label': STAGE_LABELS['fundamentals_daily_refresh'],
                'status': 'pending',
                'source_status': None,
                'market_date': market_date,
                'started_at': None,
                'updated_at': None,
                'completed_at': None,
                'processed': counts.processed,
                'total': counts.total,
                'succeeded': counts.succeeded,
                'failed': counts.failed,
                'remaining': counts.remaining,
                'current_step': None,
                'current_activity': _default_activity(STAGE_LABELS['fundamentals_daily_refresh'], 'pending'),
                'current_symbol': None,
                'current_period': None,
                'source_run_ids': {},
                'details': {
                    'fs_div': fs_div,
                },
            }

        metadata = get_run_metadata(run)
        metrics = extract_fundamentals_refresh_metrics(run)
        status = _normalize_status(str(run.get('status') or '')) or 'pending'
        requested_scope_coverage = (
            self._build_fundamentals_requested_scope_coverage(
                run=run,
                scope_symbols=scope_symbols or [],
            )
            if scope_symbols
            else None
        )
        if requested_scope_coverage is not None:
            scope_ready = status in {'completed', 'partial'} and (
                _coerce_int(requested_scope_coverage.get('missing_count')) == 0
            )
            scope_waiting = status in {'pending', 'running', 'waiting'}
            gate_status = (
                'ready' if scope_ready else ('waiting' if scope_waiting else 'blocked')
            )
            severity = gate_status
            blocking_status = (
                'non_blocking'
                if scope_ready
                else ('waiting' if scope_waiting else 'blocking')
            )
        else:
            gate_status = None
            severity = None
            blocking_status = None
        current_step = str(metadata.get('stage') or '') or None
        current_activity = (
            str(run.get('error_message'))
            if status == 'failed' and run.get('error_message')
            else _build_fundamentals_activity(metadata, status)
        )
        source_run_ids = {
            'refresh_run_id': run.get('id'),
            'sync_run_ids': [
                item.get('run_id')
                for item in metadata.get('sync_runs', [])
                if isinstance(item, dict) and item.get('run_id') is not None
            ],
            'capture_run_id': metadata.get('capture_run_id'),
            'capture_run_ids': [
                value
                for value in metadata.get('capture_run_ids', [])
                if value is not None
            ],
        }
        stage = {
            'task': 'fundamentals_daily_refresh',
            'label': STAGE_LABELS['fundamentals_daily_refresh'],
            'status': status,
            'source_status': run.get('status'),
            'market_date': market_date,
            'started_at': run.get('started_at'),
            'updated_at': resolve_run_updated_at(run),
            'completed_at': run.get('completed_at'),
            'processed': metrics.processed,
            'total': metrics.total,
            'succeeded': metrics.succeeded,
            'failed': metrics.failed,
            'remaining': metrics.remaining,
            'current_step': current_step,
            'current_activity': current_activity,
            'current_symbol': None,
            'current_period': None,
            'source_run_ids': source_run_ids,
            'details': {
                'fs_div': run.get('fs_div') or fs_div,
                'trigger': run.get('trigger'),
                'force': bool(run.get('force')),
                'symbol_limit': run.get('symbol_limit'),
                'symbol_offset': run.get('symbol_offset'),
                'symbols_missing': int(run.get('symbols_missing') or 0),
                'symbols_stale': int(run.get('symbols_stale') or 0),
                'symbols_fresh': metrics.fresh,
                'symbols_refreshed': metrics.refreshed,
                'events_detected': metrics.events_detected,
                'events_captured': metrics.events_captured,
                'events_failed': metrics.events_failed,
                'remaining_retryable_events': int(metadata.get('remaining_candidate_events') or 0),
                'retry_cycles_requested': metadata.get('retry_cycles_requested'),
                'retry_cycles_completed': metadata.get('retry_cycles_completed'),
                'current_sync_group': metadata.get('current_sync_group'),
                'pending_event_count': metadata.get('pending_event_count'),
                'capture_event_count': metadata.get('capture_event_count') or metadata.get('candidate_event_count'),
                'partial_reasons': metadata.get('partial_reasons'),
                'scope_verification_required': bool(scope_symbols),
                'requested_scope_coverage': requested_scope_coverage,
            },
        }
        if gate_status is not None:
            stage.update(
                {
                    'gate_status': gate_status,
                    'severity': severity,
                    'blocking_status': blocking_status,
                }
            )
        return stage

    @classmethod
    def _build_fundamentals_requested_scope_coverage(
        cls,
        *,
        run: dict[str, Any],
        scope_symbols: list[str],
    ) -> dict[str, Any]:
        requested_symbols = cls._normalize_symbol_list(scope_symbols)
        unresolved = set(requested_symbols)
        successful: set[str] = set()
        source_run_ids_by_symbol: dict[str, Any] = {}
        valid_evidence_run_ids: list[Any] = []
        evidence_runs = run.get('_scope_evidence_runs')
        if not isinstance(evidence_runs, list):
            evidence_runs = [run]
        for evidence_run in evidence_runs:
            if not unresolved or not isinstance(evidence_run, dict):
                break
            evidence_metadata = get_run_metadata(evidence_run)
            evidence_version = str(
                evidence_metadata.get('scope_outcome_manifest_version')
                or ''
            ).strip()
            if evidence_version not in {
                'dart_fundamentals_scope_outcomes_v1',
                'dart_daily_data_scope_outcomes_v1',
            }:
                continue
            raw_selected = evidence_metadata.get('selected_stock_codes')
            raw_successful = evidence_metadata.get(
                'successful_stock_codes'
            )
            raw_retry_debt = evidence_metadata.get(
                'retry_debt_stock_codes'
            )
            raw_terminal = evidence_metadata.get(
                'terminal_not_applicable_stock_codes', []
            )
            if not all(
                isinstance(value, list)
                for value in (
                    raw_selected,
                    raw_successful,
                    raw_retry_debt,
                    raw_terminal,
                )
            ):
                continue
            normalized_selected = cls._normalize_symbol_list(raw_selected)
            normalized_successful = cls._normalize_symbol_list(
                raw_successful
            )
            normalized_retry_debt = cls._normalize_symbol_list(
                raw_retry_debt
            )
            normalized_terminal = cls._normalize_symbol_list(raw_terminal)
            if any(
                len(normalized) != len(raw)
                for normalized, raw in (
                    (normalized_selected, raw_selected),
                    (normalized_successful, raw_successful),
                    (normalized_retry_debt, raw_retry_debt),
                    (normalized_terminal, raw_terminal),
                )
            ):
                continue
            selected = set(normalized_selected)
            run_successful = set(normalized_successful)
            run_retry_debt = set(normalized_retry_debt)
            run_terminal = set(normalized_terminal)
            if (
                run_successful & run_retry_debt
                or run_successful & run_terminal
                or run_retry_debt & run_terminal
                or selected
                != run_successful | run_retry_debt | run_terminal
            ):
                continue
            manifest = evidence_metadata.get('scope_outcome_manifest')
            if not isinstance(manifest, list):
                continue
            manifest_outcomes: dict[str, str] = {}
            manifest_valid = True
            for item in manifest:
                if not isinstance(item, dict):
                    manifest_valid = False
                    break
                normalized_manifest_symbols = cls._normalize_symbol_list(
                    [item.get('stock_code')]
                )
                stock_code = (
                    normalized_manifest_symbols[0]
                    if len(normalized_manifest_symbols) == 1
                    else None
                )
                outcome = str(item.get('outcome') or '').strip()
                if (
                    not stock_code
                    or stock_code in manifest_outcomes
                    or outcome
                    not in {
                        'success',
                        'retry_debt',
                        'terminal_not_applicable',
                    }
                ):
                    manifest_valid = False
                    break
                manifest_outcomes[stock_code] = outcome
            expected_manifest = {
                **{symbol: 'success' for symbol in run_successful},
                **{symbol: 'retry_debt' for symbol in run_retry_debt},
                **{
                    symbol: 'terminal_not_applicable'
                    for symbol in run_terminal
                },
            }
            if not manifest_valid or manifest_outcomes != expected_manifest:
                continue
            covered = unresolved & selected
            if not covered:
                continue
            run_status = str(evidence_run.get('status') or '').strip().lower()
            if run_status not in {'completed', 'partial'}:
                continue
            if run_status == 'completed' and run_retry_debt:
                continue
            valid_evidence_run_ids.append(evidence_run.get('id'))
            for symbol in covered:
                source_run_ids_by_symbol[symbol] = evidence_run.get('id')
                if run_status in {'completed', 'partial'} and symbol in run_successful:
                    successful.add(symbol)
            unresolved -= covered
        missing = [
            symbol for symbol in requested_symbols if symbol not in successful
        ]
        return {
            'evidence_version': 'dart_daily_data_scope_outcomes_v1',
            'authoritative': bool(valid_evidence_run_ids),
            'requested_count': len(requested_symbols),
            'present_count': len(successful),
            'missing_count': len(missing),
            'missing_symbols_sample': missing[:ROW_COUNT_STAGE_MISSING_SAMPLE_LIMIT],
            'source_run_ids_by_symbol': source_run_ids_by_symbol,
        }

    def _build_row_count_stage(
        self,
        *,
        stage_id: str,
        market_date: str,
        price_stage: dict[str, Any],
        price_rows_persisted: int,
        prior_stages: dict[str, dict[str, Any]],
        aggregate: dict[str, Any],
        live_symbols_total: int,
        feedback_run: dict[str, Any] | None,
        scope_symbols: list[str],
    ) -> dict[str, Any]:
        definition = STAGE_DEFINITIONS[stage_id]
        label = definition.label
        persisted = int(aggregate.get('persisted') or 0)
        expected_total, expected_total_source = self._resolve_factor_expected_total(
            price_stage=price_stage,
            price_rows_persisted=price_rows_persisted,
            live_symbols_total=live_symbols_total,
            persisted=persisted,
        )
        membership_requested_count = _strict_nonnegative_int(
            aggregate.get('membership_requested_count')
        )
        membership_present_count = _strict_nonnegative_int(
            aggregate.get('membership_present_count')
        )
        membership_missing_count = _strict_nonnegative_int(
            aggregate.get('missing_count')
        )
        exact_current_membership_verified = bool(
            aggregate.get('membership_authoritative') is True
            and aggregate.get('membership_evidence_version')
            == 'row_count_scope_gap_v1'
            and membership_requested_count is not None
            and membership_requested_count > 0
            and membership_present_count is not None
            and membership_missing_count is not None
            and membership_present_count + membership_missing_count
            == membership_requested_count
        )
        scope_rows_persisted = persisted
        if exact_current_membership_verified:
            expected_total = int(membership_requested_count or 0)
            expected_total_source = 'daily_price_bar_symbol_membership'
            scope_rows_persisted = int(membership_present_count or 0)
        price_stage_expected_total = self._resolve_price_stage_metadata_total(price_stage=price_stage)
        blocking_stages = [
            dependency
            for dependency in definition.depends_on
            if dependency in prior_stages
            and not self._dependency_is_ready_for_scope(
                stage=prior_stages[dependency],
                scope_symbols=scope_symbols,
            )
        ]
        failed_blocking_stages = [
            dependency
            for dependency in blocking_stages
            if str(prior_stages[dependency].get('status') or 'pending') == 'failed'
        ]
        status = 'pending'
        if not blocking_stages:
            if (
                exact_current_membership_verified
                and int(membership_missing_count or 0) > 0
                and persisted > 0
            ):
                status = 'partial'
            elif (
                exact_current_membership_verified
                and scope_rows_persisted >= expected_total
                and expected_total > 0
            ):
                status = 'completed'
            elif persisted > 0 and expected_total and persisted < expected_total:
                status = 'partial'
            elif persisted > 0:
                status = 'completed'
        counts = build_stage_counts(
            total=(
                expected_total
                if exact_current_membership_verified
                else max(expected_total, persisted)
            ),
            succeeded=scope_rows_persisted,
            failed=0,
            processed=scope_rows_persisted,
        )
        diagnostics = self._build_row_count_stage_diagnostics(
            stage_id=stage_id,
            status=status,
            market_date=market_date,
            persisted=scope_rows_persisted,
            expected_total=counts.total,
            blocking_stages=blocking_stages,
            failed_blocking_stages=failed_blocking_stages,
            aggregate=aggregate,
            feedback_run=feedback_run,
            scope_symbols=scope_symbols,
        )
        return {
            'task': stage_id,
            'label': label,
            'status': status,
            'source_status': 'stored_rows_present' if persisted > 0 else None,
            'gate_status': diagnostics['gate_status'],
            'severity': diagnostics['severity'],
            'blocking_status': diagnostics['blocking_status'],
            'market_date': market_date,
            'started_at': None,
            'updated_at': aggregate.get('updated_at'),
            'completed_at': aggregate.get('updated_at') if status in {'completed', 'partial'} else None,
            'processed': counts.processed,
            'total': counts.total,
            'succeeded': counts.succeeded,
            'failed': counts.failed,
            'remaining': counts.remaining,
            'current_step': None,
            'current_activity': self._build_row_count_stage_activity(
                stage_id=stage_id,
                status=status,
                persisted=scope_rows_persisted,
                expected_total=counts.total,
                blocking_stages=blocking_stages,
                failed_blocking_stages=failed_blocking_stages,
            ),
            'current_symbol': None,
            'current_period': None,
            'source_run_ids': {},
            'details': {
                'mode': 'derived_from_factor_rows',
                'depends_on': list(definition.depends_on),
                'blocking_stages': blocking_stages,
                'failed_blocking_stages': failed_blocking_stages,
                'rows_persisted': persisted,
                'rows_in_exact_membership': (
                    scope_rows_persisted
                    if exact_current_membership_verified
                    else None
                ),
                'off_scope_rows_persisted': (
                    _coerce_int(aggregate.get('membership_extra_count'))
                    if exact_current_membership_verified
                    else None
                ),
                'exact_current_membership_verified': (
                    exact_current_membership_verified
                ),
                'expected_rows': counts.total,
                'expected_rows_source': expected_total_source,
                'price_bars_persisted': price_rows_persisted or None,
                'price_stage_expected_rows': price_stage_expected_total or None,
                'no_rows_present': persisted == 0,
                'output_tables': list(definition.output_tables),
                'inspect_routes': list(definition.inspect_routes),
                'default_selected': definition.default_selected,
                **diagnostics,
            },
        }

    def _build_row_count_stage_diagnostics(
        self,
        *,
        stage_id: str,
        status: str,
        market_date: str,
        persisted: int,
        expected_total: int,
        blocking_stages: list[str],
        failed_blocking_stages: list[str],
        aggregate: dict[str, Any],
        feedback_run: dict[str, Any] | None,
        scope_symbols: list[str],
    ) -> dict[str, Any]:
        missing_count = self._resolve_missing_count(
            aggregate=aggregate,
            persisted=persisted,
            expected_total=expected_total,
        )
        missing_symbols_sample = self._normalize_symbol_list(aggregate.get('missing_symbols_sample'))
        missing_symbol_details_sample = (
            list(aggregate.get('missing_symbol_details_sample'))
            if isinstance(aggregate.get('missing_symbol_details_sample'), list)
            else []
        )
        coverage_ratio = (
            round(min(persisted, expected_total) / expected_total, 6)
            if expected_total > 0
            else None
        )
        gate_status, severity, blocking_status, gate_reason = self._default_row_count_gate_contract(
            status=status,
            coverage_ratio=coverage_ratio,
            blocking_stages=blocking_stages,
            failed_blocking_stages=failed_blocking_stages,
        )
        membership_requested_count = _strict_nonnegative_int(
            aggregate.get('membership_requested_count')
        )
        membership_present_count = _strict_nonnegative_int(
            aggregate.get('membership_present_count')
        )
        exact_current_membership_verified = bool(
            aggregate.get('membership_authoritative') is True
            and aggregate.get('membership_evidence_version')
            == 'row_count_scope_gap_v1'
            and membership_requested_count is not None
            and membership_requested_count > 0
            and membership_present_count is not None
            and membership_present_count + missing_count
            == membership_requested_count
        )
        if (
            exact_current_membership_verified
            and missing_count > 0
            and not blocking_stages
            and not failed_blocking_stages
        ):
            gate_status = 'blocked'
            severity = 'blocked'
            blocking_status = 'blocking'
            gate_reason = 'missing_rows_intersect_daily_price_membership'
        feedback_scope_gap = None
        if scope_symbols or (
            status == 'partial' and feedback_run is not None
        ):
            active_contract = self._resolve_feedback_scope_gate_contract(
                stage_id=stage_id,
                market_date=market_date,
                feedback_run=feedback_run,
                scope_symbols=scope_symbols,
            )
            if active_contract is not None:
                feedback_scope_gap = active_contract['feedback_scope_gap']
                if (
                    active_contract['gate_status'] == 'blocked'
                    or (
                        not blocking_stages
                        and not failed_blocking_stages
                        and status in {'completed', 'partial'}
                    )
                ):
                    gate_status = active_contract['gate_status']
                    severity = active_contract['severity']
                    blocking_status = active_contract['blocking_status']
                    gate_reason = active_contract['gate_reason']

        return {
            'gate_status': gate_status,
            'severity': severity,
            'blocking_status': blocking_status,
            'gate_reason': gate_reason,
            'coverage_ratio': coverage_ratio,
            'min_non_blocking_coverage_ratio': PARTIAL_ROW_COUNT_MIN_NON_BLOCKING_COVERAGE_RATIO,
            'missing_count': missing_count,
            'missing_symbols_sample': missing_symbols_sample,
            'missing_symbol_details_sample': missing_symbol_details_sample,
            'missing_sample_limit': aggregate.get('missing_sample_limit') or ROW_COUNT_STAGE_MISSING_SAMPLE_LIMIT,
            'exact_current_membership_verified': (
                exact_current_membership_verified
            ),
            'membership_requested_count': membership_requested_count,
            'membership_present_count': membership_present_count,
            'membership_extra_count': (
                _strict_nonnegative_int(
                    aggregate.get('membership_extra_count')
                )
            ),
            'feedback_scope_gap': feedback_scope_gap,
            'decision_ready_count': (
                _coerce_int(aggregate.get('decision_ready_count'))
                if stage_id == 'low_risk_factors'
                else None
            ),
            'insufficient_history_count': (
                _coerce_int(aggregate.get('insufficient_history_count'))
                if stage_id == 'low_risk_factors'
                else None
            ),
            'latest_decision_ready_at': (
                _coerce_iso_timestamp(aggregate.get('latest_decision_ready_at'))
                if stage_id == 'low_risk_factors'
                else None
            ),
        }

    @staticmethod
    def _resolve_missing_count(
        *,
        aggregate: dict[str, Any],
        persisted: int,
        expected_total: int,
    ) -> int:
        if aggregate.get('missing_count') is not None:
            return _coerce_int(aggregate.get('missing_count'))
        return max(int(expected_total or 0) - int(persisted or 0), 0)

    @staticmethod
    def _dependency_is_ready_for_scope(
        *,
        stage: dict[str, Any],
        scope_symbols: list[str],
    ) -> bool:
        status = str(stage.get('status') or 'pending').strip().lower()
        gate_status = str(stage.get('gate_status') or '').strip().lower()
        blocking_status = str(stage.get('blocking_status') or '').strip().lower()
        if status not in {'completed', 'partial'}:
            return False
        if gate_status not in {'', 'ready'}:
            return False
        if blocking_status not in {'', 'non_blocking'}:
            return False
        if not scope_symbols:
            return status == 'completed'
        requested_scope_count = len(
            EODPipelineStatusService._normalize_symbol_list(scope_symbols)
        )
        details = dict(stage.get('details') or {})
        accepted_evidence_versions = {
            'dart_daily_data_scope_outcomes_v1',
            'dart_fundamentals_scope_outcomes_v1',
            'pykrx_enrichment_scope_coverage_v1',
            'row_count_scope_gap_v1',
        }
        for evidence_key in (
            'requested_scope_coverage',
            'feedback_scope_gap',
        ):
            evidence = details.get(evidence_key)
            if not isinstance(evidence, dict):
                continue
            if evidence.get('evidence_version') == 'row_count_scope_gap_v1':
                validated_evidence = _validated_row_count_scope_gap_evidence(
                    evidence,
                    requested_count=requested_scope_count,
                )
                if (
                    validated_evidence is not None
                    and validated_evidence['missing_count'] == 0
                ):
                    return True
                continue
            if (
                evidence.get('authoritative') is True
                and evidence.get('evidence_version')
                in accepted_evidence_versions
                and _strict_nonnegative_int(
                    evidence.get('requested_count')
                ) == requested_scope_count
                and _strict_nonnegative_int(
                    evidence.get('missing_count')
                ) == 0
            ):
                return True
        if status == 'completed' and not details.get(
            'scope_verification_required'
        ):
            return True
        return False

    @staticmethod
    def _normalize_symbol_list(value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        normalized: list[str] = []
        seen: set[str] = set()
        for item in value:
            symbol = str(item or '').strip().upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            normalized.append(symbol)
        return normalized

    @staticmethod
    def _default_row_count_gate_contract(
        *,
        status: str,
        coverage_ratio: float | None,
        blocking_stages: list[str],
        failed_blocking_stages: list[str],
    ) -> tuple[str, str, str, str | None]:
        if failed_blocking_stages:
            return 'blocked', 'blocked', 'blocking', 'failed_dependency'
        if blocking_stages:
            return 'waiting', 'waiting', 'waiting', 'waiting_on_dependency'
        if status == 'completed':
            return 'ready', 'ready', 'non_blocking', None
        if status == 'partial':
            if (coverage_ratio or 0.0) >= PARTIAL_ROW_COUNT_MIN_NON_BLOCKING_COVERAGE_RATIO:
                return 'ready', 'degraded', 'non_blocking', 'row_count_partial_above_min_coverage'
            return 'blocked', 'blocked', 'blocking', 'row_count_partial_below_min_coverage'
        if status == 'failed':
            return 'blocked', 'failed', 'blocking', 'stage_failed'
        return 'waiting', 'waiting', 'waiting', None

    def _resolve_feedback_scope_gate_contract(
        self,
        *,
        stage_id: str,
        market_date: str,
        feedback_run: dict[str, Any] | None,
        scope_symbols: list[str],
    ) -> dict[str, Any] | None:
        requested_symbols = self._normalize_symbol_list(scope_symbols)
        explicit_scope = bool(requested_symbols)
        if not requested_symbols and feedback_run is not None:
            summary = dict(
                feedback_run.get('summary')
                if isinstance(feedback_run.get('summary'), dict)
                else {}
            )
            run_status = str(
                summary.get('run_status') or feedback_run.get('status') or ''
            ).strip().lower()
            failed_items = _coerce_int(
                summary.get('failed_items'),
                feedback_run.get('failed_count'),
            )
            if run_status != 'running' or failed_items > 0:
                return None
            requested_symbols = self._normalize_symbol_list(
                feedback_run.get('requested_symbols')
            )
        if not requested_symbols:
            return None

        feedback_scope_gap = self.row_count_scope_gap_loader(
            stage_id=stage_id,
            market_date=market_date,
            symbols=requested_symbols,
        )
        validated_scope_gap = _validated_row_count_scope_gap_evidence(
            feedback_scope_gap,
            requested_count=len(requested_symbols),
        )
        if validated_scope_gap is None:
            return {
                'gate_status': 'blocked',
                'severity': 'blocked',
                'blocking_status': 'blocking',
                'gate_reason': 'requested_scope_coverage_evidence_invalid',
                'feedback_scope_gap': feedback_scope_gap,
            }
        missing_scope_count = int(validated_scope_gap['missing_count'])
        if missing_scope_count > 0:
            return {
                'gate_status': 'blocked',
                'severity': 'blocked',
                'blocking_status': 'blocking',
                'gate_reason': (
                    'missing_rows_intersect_requested_scope'
                    if explicit_scope
                    else 'missing_rows_intersect_active_feedback_scope'
                ),
                'feedback_scope_gap': feedback_scope_gap,
            }
        return {
            'gate_status': 'ready',
            'severity': 'degraded',
            'blocking_status': 'non_blocking',
            'gate_reason': (
                'requested_scope_rows_complete'
                if explicit_scope
                else 'missing_rows_outside_active_feedback_scope'
            ),
            'feedback_scope_gap': feedback_scope_gap,
        }

    def _apply_scope_coverage_gate(
        self,
        *,
        stage_id: str,
        market_date: str,
        stage: dict[str, Any],
        scope_symbols: list[str],
    ) -> dict[str, Any]:
        if not scope_symbols:
            return stage
        scope_gap = self.row_count_scope_gap_loader(
            stage_id=stage_id,
            market_date=market_date,
            symbols=scope_symbols,
        )
        details = dict(stage.get('details') or {})
        details['requested_scope_coverage'] = scope_gap
        validated_scope_gap = _validated_row_count_scope_gap_evidence(
            scope_gap,
            requested_count=len(self._normalize_symbol_list(scope_symbols)),
        )
        if validated_scope_gap is None:
            return {
                **stage,
                'gate_status': 'blocked',
                'severity': 'blocked',
                'blocking_status': 'blocking',
                'current_activity': (
                    'Requested-scope coverage proof is missing or invalid'
                ),
                'details': {
                    **details,
                    'gate_reason': (
                        'requested_scope_coverage_evidence_invalid'
                    ),
                },
            }
        missing_count = int(validated_scope_gap['missing_count'])
        if missing_count <= 0:
            existing_gate_status = str(
                stage.get('gate_status')
                or details.get('gate_status')
                or ''
            ).strip().lower()
            existing_status = str(stage.get('status') or '').strip().lower()
            exact_universe_coverage = details.get('exact_universe_coverage')
            scope_can_isolate_aggregate_gap = (
                existing_status == 'partial'
                and isinstance(exact_universe_coverage, dict)
                and _coerce_int(exact_universe_coverage.get('missing_count')) > 0
            )
            if (
                (
                    existing_gate_status in {'blocked', 'waiting'}
                    and not scope_can_isolate_aggregate_gap
                )
                or existing_status
                in {
                    'failed',
                    'blocked',
                    'waiting',
                    'running',
                    'pending',
                    'interrupted',
                    'abandoned',
                }
            ):
                return {**stage, 'details': details}
            if scope_can_isolate_aggregate_gap:
                details['aggregate_gate_status'] = existing_gate_status
                details['aggregate_gate_reason'] = details.get('gate_reason')
                details['scope_gate_override'] = (
                    'requested_scope_complete_outside_scope_gap_isolated'
                )
            details['gate_reason'] = 'requested_scope_rows_complete'
            return {
                **stage,
                'gate_status': 'ready',
                'severity': (
                    'ready'
                    if str(stage.get('status') or '').strip().lower()
                    == 'completed'
                    else 'degraded'
                ),
                'blocking_status': 'non_blocking',
                'details': details,
            }
        return {
            **stage,
            'gate_status': 'blocked',
            'severity': 'blocked',
            'blocking_status': 'blocking',
            'current_activity': (
                f'{missing_count} requested symbol(s) lack decision-usable '
                f'{_stage_label(stage_id)} rows'
            ),
            'details': {
                **details,
                'gate_reason': 'missing_rows_intersect_requested_scope',
            },
        }

    @staticmethod
    def _apply_exact_universe_coverage_evidence(
        *,
        stage: dict[str, Any],
        coverage: dict[str, Any],
        expected_count: int,
        source_run_id: Any = None,
    ) -> dict[str, Any]:
        details = dict(stage.get('details') or {})
        details['exact_universe_coverage'] = coverage
        details['exact_universe_source_run_id'] = source_run_id
        validated_coverage = _validated_row_count_scope_gap_evidence(
            coverage,
            requested_count=expected_count,
        )
        if validated_coverage is None:
            details['exact_universe_verified'] = False
            details['coverage_verification_status'] = 'invalid'
            details['expected_rows_source'] = (
                'validated_eod_run_market_membership'
            )
            return {
                **stage,
                'gate_status': 'blocked',
                'severity': 'blocked',
                'blocking_status': 'blocking',
                'details': {
                    **details,
                    'gate_reason': (
                        'exact_run_universe_coverage_evidence_invalid'
                    ),
                },
            }
        missing_count = int(validated_coverage['missing_count'])
        details['exact_universe_verified'] = missing_count == 0
        details['coverage_verification_status'] = 'verified'
        details['expected_rows_source'] = (
            'validated_eod_run_market_membership'
        )
        if missing_count <= 0:
            return {**stage, 'details': details}
        return {
            **stage,
            'gate_status': 'blocked',
            'severity': 'blocked',
            'blocking_status': 'blocking',
            'details': {
                **details,
                'gate_reason': 'missing_rows_intersect_exact_run_universe',
            },
        }

    @staticmethod
    def _normalize_stored_pykrx_coverage_stage(
        stage: dict[str, Any],
        *,
        persisted: int,
        expected_total: int,
        exact_scope_gap: dict[str, Any] | None = None,
        expected_total_source: str | None = None,
    ) -> dict[str, Any]:
        # Persisted row coverage can prove that rows exist, but cannot prove
        # that the source run which produced this stage completed safely.  In
        # particular, never let a zero-gap row count erase an official
        # failure/interruption or an upstream block before the requested-scope
        # gate gets a chance to preserve that status.
        if str(stage.get('status') or '').strip().lower() in {
            'running',
            'failed',
            'blocked',
            'partial',
            'interrupted',
            'abandoned',
        }:
            return stage
        validated_scope_gap = _validated_row_count_scope_gap_evidence(
            exact_scope_gap,
            requested_count=int(expected_total or 0),
        )
        coverage_supplied = exact_scope_gap is not None
        coverage_verified = validated_scope_gap is not None
        if coverage_supplied and not coverage_verified:
            details = dict(stage.get('details') or {})
            details['exact_universe_coverage'] = exact_scope_gap
            details['coverage_verification_status'] = 'invalid'
            details['exact_universe_verified'] = False
            details['gate_reason'] = (
                'exact_run_universe_coverage_evidence_invalid'
            )
            return {
                **stage,
                'gate_status': 'blocked',
                'severity': 'blocked',
                'blocking_status': 'blocking',
                'details': details,
            }
        if persisted <= 0 and not coverage_verified:
            return stage
        if persisted <= 0 and expected_total <= 0:
            return stage
        normalized_total = (
            int(expected_total or 0)
            if coverage_verified and int(expected_total or 0) > 0
            else persisted
        )
        exact_missing_count = (
            int(validated_scope_gap['missing_count'])
            if coverage_verified
            else 0
        )
        exact_present_count = (
            int(validated_scope_gap['present_count'])
            if coverage_verified
            else min(persisted, normalized_total)
        )
        counts = build_stage_counts(
            total=normalized_total,
            succeeded=min(exact_present_count, normalized_total),
            failed=max(exact_missing_count, normalized_total - persisted, 0),
            processed=min(persisted, normalized_total),
        )
        if coverage_verified:
            status = (
                'completed'
                if counts.failed == 0 and counts.succeeded >= counts.total
                else 'partial'
            )
            gate_status = 'ready' if status == 'completed' else 'blocked'
            severity = 'ready' if status == 'completed' else 'blocked'
            blocking_status = (
                'non_blocking' if status == 'completed' else 'blocking'
            )
        elif persisted > 0:
            # Aggregate-only evidence preserves the historical pipeline status,
            # but remains explicitly unverified. Candidate admission always
            # supplies a requested scope and is checked symbol-by-symbol.
            status = 'completed'
            gate_status = 'ready'
            severity = 'degraded'
            blocking_status = 'non_blocking'
        else:
            status = 'pending'
            gate_status = 'waiting'
            severity = 'waiting'
            blocking_status = 'waiting'
        details = dict(stage.get('details') or {})
        details['stored_rows_present'] = persisted
        details['stored_rows_coverage_override'] = True
        details['expected_rows_source'] = expected_total_source
        details['unverified_expected_rows'] = (
            int(expected_total or 0) if not coverage_verified else None
        )
        details['coverage_verification_status'] = (
            'verified' if coverage_verified else 'unverified'
        )
        details['coverage_unverified_reason'] = (
            None
            if coverage_verified
            else 'validated_expected_universe_membership_unavailable'
        )
        details['exact_universe_verified'] = (
            coverage_verified and exact_missing_count == 0
        )
        if coverage_verified:
            details['exact_universe_coverage'] = exact_scope_gap
        return {
            **stage,
            'status': status,
            'source_status': stage.get('source_status') or 'stored_rows_present',
            'gate_status': gate_status,
            'severity': severity,
            'blocking_status': blocking_status,
            'processed': counts.processed,
            'total': counts.total,
            'succeeded': counts.succeeded,
            'failed': counts.failed,
            'remaining': counts.remaining,
            'completed_at': (
                (stage.get('completed_at') or stage.get('updated_at'))
                if status in {'completed', 'partial'}
                else None
            ),
            'current_activity': (
                _default_activity(
                    str(stage.get('label') or stage.get('task') or ''),
                    status,
                )
                if coverage_verified
                else (
                    'Stored row count exists, but exact universe coverage is unverified'
                    if persisted > 0
                    else 'Waiting for exact universe coverage and stored rows'
                )
            ),
            'details': details,
        }

    @staticmethod
    def _resolve_factor_expected_total(
        *,
        price_stage: dict[str, Any],
        price_rows_persisted: int,
        live_symbols_total: int,
        persisted: int,
    ) -> tuple[int, str]:
        expected_total = int(price_rows_persisted or 0)
        if expected_total > 0:
            return max(expected_total, persisted), 'daily_price_bars'

        expected_total = EODPipelineStatusService._resolve_price_stage_metadata_total(price_stage=price_stage)
        if expected_total <= 0:
            expected_total = int(live_symbols_total or 0)
            return max(expected_total, persisted), 'live_symbol_count'
        return max(expected_total, persisted), 'price_stage_metadata'

    @staticmethod
    def _resolve_price_stage_metadata_total(*, price_stage: dict[str, Any]) -> int:
        expected_total = int(price_stage.get('succeeded') or 0)
        if expected_total <= 0:
            expected_total = int(price_stage.get('total') or 0)
        return expected_total

    @staticmethod
    def _build_row_count_stage_activity(
        *,
        stage_id: str,
        status: str,
        persisted: int,
        expected_total: int,
        blocking_stages: list[str],
        failed_blocking_stages: list[str],
    ) -> str:
        label = _stage_label(stage_id)
        if failed_blocking_stages:
            failed_labels = ', '.join(_stage_label(item) for item in failed_blocking_stages)
            return f'Blocked by failed {failed_labels}'
        if blocking_stages:
            blocking_labels = ', '.join(_stage_label(item) for item in blocking_stages)
            return f'Waiting on {blocking_labels}'
        if persisted > 0:
            if status == 'partial' and expected_total > 0:
                return f'Stored {persisted}/{expected_total} rows for {label}'
            return _default_activity(label, status)
        return _default_activity(label, status)

    @staticmethod
    def _is_current_date_waiting_for_official_run(
        *,
        stages: dict[str, dict[str, Any]],
        official_run: dict[str, Any] | None,
        pykrx_run: dict[str, Any] | None,
        market_session: dict[str, Any],
    ) -> bool:
        if official_run is not None or pykrx_run is not None:
            return False
        if not bool(market_session.get('requested_date_is_current_calendar_date')):
            return False
        if not bool(market_session.get('requested_date_is_trading_day')):
            return False

        statuses = {
            str(stage.get('status') or 'pending').strip().lower()
            for stage in stages.values()
        }
        return 'pending' in statuses and statuses <= {'pending', 'completed', 'skipped'}

    @staticmethod
    def _resolve_overall_status(stages: dict[str, dict[str, Any]]) -> str:
        warning_stage_ids = set(
            daily_pipeline_nonblocking_warning_stage_ids(stages)
        )
        statuses = [
            EODPipelineStatusService._effective_stage_status(item)
            for stage_id, item in stages.items()
            if stage_id not in warning_stage_ids
        ]
        if not statuses:
            return 'completed' if warning_stage_ids else 'pending'
        if any(status == 'running' for status in statuses):
            return 'running'
        if any(status == 'interrupted' for status in statuses):
            return 'interrupted'
        if any(status == 'abandoned' for status in statuses):
            return 'abandoned'
        if any(status == 'blocked' for status in statuses):
            return 'blocked'
        if any(status == 'waiting' for status in statuses):
            # ``waiting`` describes a pipeline that has not made durable
            # progress yet.  Once an upstream stage is already complete (or
            # has a durable partial result), downstream missing output is a
            # partial pipeline, not an active wait.  Reporting ``waiting`` in
            # that state hid terminal gaps such as a completed collection and
            # fundamentals refresh with no factor rows.
            if any(
                status in {'completed', 'partial', 'skipped'}
                for status in statuses
            ):
                return 'partial'
            return 'waiting'
        if all(status == 'pending' for status in statuses):
            return 'pending'
        if all(status == 'completed' for status in statuses):
            return 'completed'
        if statuses and all(status in {'completed', 'skipped'} for status in statuses):
            return 'completed' if any(status == 'completed' for status in statuses) else 'skipped'
        if statuses and all(status == 'skipped' for status in statuses):
            return 'skipped'
        if any(status == 'failed' for status in statuses) and not any(status in {'completed', 'partial'} for status in statuses):
            return 'failed'
        return 'partial'

    @staticmethod
    def _effective_stage_status(stage: dict[str, Any]) -> str:
        details = (
            stage.get('details')
            if isinstance(stage.get('details'), dict)
            else {}
        )
        status = str(stage.get('status') or 'pending').strip().lower()
        gate_status = str(
            stage.get('gate_status') or details.get('gate_status') or ''
        ).strip().lower()
        blocking_status = str(
            stage.get('blocking_status')
            or details.get('blocking_status')
            or ''
        ).strip().lower()
        if (
            status == 'failed'
            or gate_status == 'failed'
            or blocking_status == 'failed'
        ):
            return 'failed'
        if (
            status == 'blocked'
            or gate_status == 'blocked'
            or blocking_status in {'blocked', 'blocking'}
        ):
            return 'blocked'
        if status in {
            'running',
            'interrupted',
            'abandoned',
            'waiting',
            'pending',
            'completed',
            'partial',
            'skipped',
        }:
            if (
                status in {'completed', 'partial', 'skipped'}
                and (
                    gate_status == 'waiting'
                    or blocking_status == 'waiting'
                )
            ):
                return 'waiting'
            return status
        if gate_status == 'waiting' or blocking_status == 'waiting':
            return 'waiting'
        return 'pending'


eod_pipeline_status_service = EODPipelineStatusService(
    factor_stage_counts_loader=_load_factor_stage_counts,
    row_count_stage_counts_loader=_load_row_count_stage_counts,
    pykrx_enrichment_loader=_load_pykrx_enrichment_coverage,
    official_stage_history_loader=_load_daily_pipeline_stage_history,
    signal_feature_counts_loader=_load_signal_feature_counts,
    external_news_counts_loader=_load_external_news_counts,
    macro_context_counts_loader=_load_macro_context_counts,
    signal_decider_counts_loader=_load_signal_decider_output_counts,
    row_count_scope_gap_loader=_load_row_count_stage_scope_gap,
    price_bar_count_loader=lambda *, market_date: count_daily_price_bars_for_date(market_date),
    price_context_count_loader=lambda market_date: count_daily_price_contexts_for_live_symbols(market_date),
    technical_snapshot_count_loader=lambda market_date: count_daily_technical_snapshots_for_live_symbols(market_date),
    action_summary_count_loader=lambda market_date: count_daily_action_summaries_for_live_symbols(market_date),
    feedback_run_loader=_load_latest_feedback_run_summary,
)
