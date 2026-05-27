from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Callable

from sqlalchemy import and_, func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import aliased

from app.core.settings import settings
from app.db import session_scope
from app.db_models import (
    DailyMarketRiskFilterResult,
    DailyPiotroskiFilterResult,
    DailyAdvancedValueFactorResult,
    DailyBetaFactorResult,
    DailyCompositeFactorResult,
    DailyLowRiskFactorResult,
    DailyMomentumFactorResult,
    DailyPriceBar,
    DailyQualityFactorResult,
    DailyUnifiedValueFactorResult,
    DailyValueFactorResult,
    Symbol,
)
from app.dart.fundamentals_capture_service import dart_fundamentals_capture_service
from app.eod.daily_pipeline_stages import DAILY_STAGE_ORDER, FACTOR_STAGE_ORDER, STAGE_DEFINITIONS
from app.eod.daily_pipeline_state import daily_pipeline_run_state_service
from app.eod.pykrx_backfill import DEFAULT_MARKETS, PYKRX_BACKFILL_SERVICE
from app.eod.pykrx_enrichment import DEFAULT_PYKRX_ENRICHMENT_CATEGORIES, pykrx_enrichment_service
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
)
from app.market.clock import get_market_state
from app.market.session import build_market_session_metadata
from app.market_data_store import (
    DAY_BASED_TRENDING_PERIODS,
    count_daily_action_summaries_for_live_symbols,
    count_daily_price_bars_for_date,
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
ROW_COUNT_STAGE_MISSING_SAMPLE_LIMIT = 10
PARTIAL_ROW_COUNT_MIN_NON_BLOCKING_COVERAGE_RATIO = 0.99


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_pipeline_pykrx_run_rows(*, trade_date: str) -> list[dict[str, Any]]:
    return list_eod_collection_runs(
        market='KRX',
        trade_date=trade_date,
        source='pykrx',
        limit=10,
    )


def _select_pipeline_pykrx_run(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    return select_preferred_eod_run(rows)


def _load_latest_pipeline_pykrx_run(*, trade_date: str) -> dict[str, Any] | None:
    preferred = _select_pipeline_pykrx_run(_load_pipeline_pykrx_run_rows(trade_date=trade_date))
    if preferred is None:
        return None

    if preferred.get('status') == 'running':
        PYKRX_BACKFILL_SERVICE.reconcile_latest_daily_run_status(latest_run=preferred)
        preferred = _select_pipeline_pykrx_run(_load_pipeline_pykrx_run_rows(trade_date=trade_date))

    return preferred


def _load_latest_fundamentals_refresh_run(*, market_date: str, fs_div: str) -> dict[str, Any] | None:
    payload = dart_fundamentals_capture_service.inspect_daily_refresh_status(
        market_date=date.fromisoformat(market_date),
        fs_div=fs_div,
    )
    return payload.get('latest_run')


def _load_latest_daily_pipeline_run(*, market_date: str) -> dict[str, Any] | None:
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


def _load_pykrx_enrichment_coverage(*, market_date: str) -> dict[str, Any]:
    return pykrx_enrichment_service.inspect(
        market_date=market_date,
        categories=DEFAULT_PYKRX_ENRICHMENT_CATEGORIES,
        limit=1,
    ).get('coverage') or {}


def _load_factor_stage_counts(*, market_date: str) -> dict[str, dict[str, Any]]:
    normalized_market_date = date.fromisoformat(market_date)
    counts: dict[str, dict[str, Any]] = {}
    with session_scope() as session:
        for stage_id, model in FACTOR_STAGE_MODELS.items():
            persisted, updated_at = session.execute(
                select(
                    func.count(func.distinct(model.symbol_id)),
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


def _load_row_count_stage_counts(*, market_date: str) -> dict[str, dict[str, Any]]:
    normalized_market_date = date.fromisoformat(market_date)
    counts: dict[str, dict[str, Any]] = {}
    with session_scope() as session:
        for stage_id, model in ROW_COUNT_STAGE_MODELS.items():
            persisted, updated_at = session.execute(
                select(
                    func.count(func.distinct(model.symbol_id)),
                    func.max(model.captured_at),
                )
                .select_from(model)
                .where(model.market_date == normalized_market_date)
            ).one()
            missing_diagnostics = _load_row_count_missing_diagnostics_for_model(
                session=session,
                model=model,
                market_date=normalized_market_date,
                limit=ROW_COUNT_STAGE_MISSING_SAMPLE_LIMIT,
            )
            counts[stage_id] = {
                'persisted': int(persisted or 0),
                'updated_at': updated_at.isoformat() if updated_at else None,
                **missing_diagnostics,
            }
    return counts


def _load_row_count_missing_diagnostics_for_model(
    *,
    session: Any,
    model: Any,
    market_date: date,
    limit: int,
) -> dict[str, Any]:
    target = aliased(model)
    join_condition = and_(
        target.symbol_id == DailyPriceBar.symbol_id,
        target.market_date == market_date,
    )
    missing_count = session.execute(
        select(func.count(func.distinct(DailyPriceBar.symbol_id)))
        .select_from(DailyPriceBar)
        .outerjoin(target, join_condition)
        .where(DailyPriceBar.trade_date == market_date)
        .where(target.symbol_id.is_(None))
    ).scalar_one()
    sample_rows = session.execute(
        select(Symbol.symbol, Symbol.name_ko, Symbol.market)
        .select_from(DailyPriceBar)
        .join(Symbol, Symbol.id == DailyPriceBar.symbol_id)
        .outerjoin(target, join_condition)
        .where(DailyPriceBar.trade_date == market_date)
        .where(target.symbol_id.is_(None))
        .order_by(Symbol.symbol.asc())
        .limit(limit)
    ).all()
    details_sample = [
        {
            'symbol': row.symbol,
            'name_ko': row.name_ko,
            'market': row.market,
        }
        for row in sample_rows
    ]
    return {
        'missing_count': int(missing_count or 0),
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
    model = ROW_COUNT_STAGE_MODELS.get(stage_id)
    normalized_symbols = [str(symbol or '').strip().upper() for symbol in symbols if str(symbol or '').strip()]
    if model is None or not normalized_symbols:
        return {
            'missing_count': 0,
            'missing_symbols_sample': [],
            'missing_symbol_details_sample': [],
        }

    normalized_market_date = date.fromisoformat(market_date)
    target = aliased(model)
    join_condition = and_(
        target.symbol_id == Symbol.id,
        target.market_date == normalized_market_date,
    )
    with session_scope() as session:
        missing_count = session.execute(
            select(func.count(func.distinct(Symbol.id)))
            .select_from(Symbol)
            .join(DailyPriceBar, DailyPriceBar.symbol_id == Symbol.id)
            .outerjoin(target, join_condition)
            .where(DailyPriceBar.trade_date == normalized_market_date)
            .where(Symbol.symbol.in_(normalized_symbols))
            .where(target.symbol_id.is_(None))
        ).scalar_one()
        sample_rows = session.execute(
            select(Symbol.symbol, Symbol.name_ko, Symbol.market)
            .select_from(Symbol)
            .join(DailyPriceBar, DailyPriceBar.symbol_id == Symbol.id)
            .outerjoin(target, join_condition)
            .where(DailyPriceBar.trade_date == normalized_market_date)
            .where(Symbol.symbol.in_(normalized_symbols))
            .where(target.symbol_id.is_(None))
            .order_by(Symbol.symbol.asc())
            .limit(ROW_COUNT_STAGE_MISSING_SAMPLE_LIMIT)
        ).all()
    details_sample = [
        {
            'symbol': row.symbol,
            'name_ko': row.name_ko,
            'market': row.market,
        }
        for row in sample_rows
    ]
    return {
        'missing_count': int(missing_count or 0),
        'missing_symbols_sample': [item['symbol'] for item in details_sample],
        'missing_symbol_details_sample': details_sample,
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


def _progress_summary_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    progress_summary = payload.get('progress_summary')
    if isinstance(progress_summary, dict):
        return dict(progress_summary)
    return dict(payload)


def _load_latest_feedback_run_summary(*, market_date: str) -> dict[str, Any] | None:
    normalized_market_date = date.fromisoformat(market_date)
    try:
        from app.agent_context.run_state_service import agent_context_feedback_run_state_service
        from app.agent_context.runtime_state import runtime_feedback_state_store
    except Exception:
        return None

    runtime_state = (
        runtime_feedback_state_store.get_run(normalized_market_date, logical=True)
        or runtime_feedback_state_store.get_run(normalized_market_date)
    )
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
    )
    if run_snapshot is None:
        return None

    summary_payload = (
        dict(run_snapshot.get('summary_payload'))
        if isinstance(run_snapshot.get('summary_payload'), dict)
        else {}
    )
    summary = _progress_summary_from_payload(summary_payload)
    requested_symbols = agent_context_feedback_run_state_service.resolve_run_requested_symbols(
        run_snapshot=run_snapshot,
    )
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
    technical_snapshot_count_loader: Callable = _noop_price_bar_count_loader
    action_summary_count_loader: Callable = _noop_price_bar_count_loader
    pykrx_enrichment_loader: Callable = _load_pykrx_enrichment_coverage
    official_run_loader: Callable = _load_latest_daily_pipeline_run
    feedback_run_loader: Callable = _noop_feedback_run_loader
    row_count_scope_gap_loader: Callable = _noop_row_count_scope_gap_loader

    def read(
        self,
        *,
        market_date: str | None = None,
        fs_div: str | None = None,
        include_feedback_context: bool = True,
        feedback_run_context: dict[str, Any] | None = None,
        include_feedback_stage: bool = True,
    ) -> dict[str, Any]:
        market_state = self.market_state_loader()
        # The default operator view stays on the current Seoul calendar date.
        # Callers must pass market_date explicitly when they want a previous trade date on weekends or break-days.
        resolved_market_date = date.fromisoformat(market_date or str(market_state.server_time)[:10]).isoformat()
        market_session = build_market_session_metadata(
            requested_date=resolved_market_date,
            current_state=market_state,
        )
        resolved_fs_div = (fs_div or settings.dart_fundamentals_default_fs_div).strip().upper()
        pykrx_run = self.pykrx_run_loader(trade_date=resolved_market_date)
        official_run = self.official_run_loader(market_date=resolved_market_date)
        feedback_run = feedback_run_context
        if feedback_run is None and include_feedback_context:
            feedback_run = self.feedback_run_loader(market_date=resolved_market_date)
        live_symbols_total = int(self.live_symbol_count_loader() or 0)
        pykrx_stage_counts = self.pykrx_stage_counts_loader(pykrx_run['id']) if pykrx_run else {}
        pykrx_stages = self._build_pykrx_stages(
            market_date=resolved_market_date,
            run=pykrx_run,
            stage_counts=pykrx_stage_counts,
            live_symbols_total=live_symbols_total,
        )
        pykrx_enrichment_stage = self._build_pykrx_enrichment_stage(
            market_date=resolved_market_date,
            coverage=dict(self.pykrx_enrichment_loader(market_date=resolved_market_date) or {}),
        )
        fundamentals_stage = self._build_fundamentals_stage(
            market_date=resolved_market_date,
            fs_div=resolved_fs_div,
        )
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
        stored_technical_rows = max(
            int(self.technical_snapshot_count_loader(resolved_market_date) or 0),
            int(dict(pykrx_stage_counts.get('technical_snapshots') or {}).get('persisted') or 0),
        )
        stored_action_rows = max(
            int(self.action_summary_count_loader(resolved_market_date) or 0),
            int(dict(pykrx_stage_counts.get('action_summaries') or {}).get('persisted') or 0),
        )
        pykrx_stages = {
            **pykrx_stages,
            'price_collection': self._normalize_stored_pykrx_coverage_stage(
                pykrx_stages['price_collection'],
                persisted=stored_price_rows,
                expected_total=stored_price_rows,
            ),
            'technical_snapshots': self._normalize_stored_pykrx_coverage_stage(
                pykrx_stages['technical_snapshots'],
                persisted=stored_technical_rows,
                expected_total=stored_price_rows,
            ),
            'action_summaries': self._normalize_stored_pykrx_coverage_stage(
                pykrx_stages['action_summaries'],
                persisted=stored_action_rows,
                expected_total=stored_price_rows,
            ),
        }
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
            )
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
        stages = {
            stage_id: stages[stage_id]
            for stage_id in PIPELINE_STAGE_ORDER
        }
        if official_run is not None:
            stages = self._overlay_official_run_stages(
                stages=stages,
                official_run=official_run,
                market_date=resolved_market_date,
            )
        stages = self._normalize_stage_contract(stages)
        overall_status = self._resolve_overall_status(stages)
        official_run_status = str((official_run or {}).get('status') or '').strip().lower()
        if official_run_status in {'running', 'interrupted', 'abandoned'}:
            overall_status = official_run_status
        summary = self._build_stage_summary(stages=stages)
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
            'stage_order': list(PIPELINE_STAGE_ORDER),
            **summary,
            'stages': stages,
            'official_run': self._build_official_run_summary(official_run),
        }

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
        return normalized

    @staticmethod
    def _infer_stage_progress_source(stage: dict[str, Any]) -> str:
        mode = str(dict(stage.get('details') or {}).get('mode') or '').strip()
        if mode == 'official_daily_pipeline_run_state':
            return 'official_run'
        source_status = str(stage.get('source_status') or '').strip()
        if source_status == 'stored_rows_present':
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
            overlaid[stage_id] = self._build_official_stage(
                stage_id=stage_id,
                market_date=market_date,
                official_run=official_run,
                official_stage=official_stage,
                fallback_stage=stages.get(stage_id) or {},
            )
        return overlaid

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
        details = dict(official_stage.get('details') or {})
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
        if status in {'failed', 'interrupted', 'abandoned'} and official_stage.get('error_message'):
            current_activity = official_stage.get('error_message')
        return {
            'task': stage_id,
            'label': definition.label,
            'status': status,
            'source_status': official_stage.get('status'),
            'market_date': official_stage.get('market_date') or market_date,
            'started_at': official_stage.get('started_at'),
            'updated_at': progress_source.get('updated_at') or official_stage.get('updated_at') or official_run.get('updated_at'),
            'completed_at': official_stage.get('completed_at'),
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
            'requested_symbol_count': len(requested_symbols),
            'failed_items': failed_items,
            'completed_feedback_count': completed_feedback_count,
            'expected_feedback_count': expected_feedback_count,
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
            'updated_at': summary.get('latest_feedback_at') or fallback_stage.get('updated_at'),
            'completed_at': summary.get('run_completed_at') if status != 'running' else None,
            'processed': counts.processed,
            'total': counts.total,
            'succeeded': counts.succeeded,
            'failed': counts.failed,
            'remaining': counts.remaining,
            'current_step': 'agent_feedback_running' if status == 'running' else None,
            'current_activity': activity,
            'current_symbol': None,
            'current_period': None,
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
    ) -> dict[str, Any]:
        category_rows = dict(coverage.get('categories') or {})
        total = len(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES)
        latest_captured_at = None
        for item in category_rows.values():
            candidate = item.get('latest_captured_at')
            if candidate and (latest_captured_at is None or str(candidate) > str(latest_captured_at)):
                latest_captured_at = candidate
        all_missing = bool(category_rows) and all(int(item.get('present_rows') or 0) <= 0 for item in category_rows.values())
        present_count = sum(1 for item in category_rows.values() if int(item.get('present_rows') or 0) > 0)
        missing_categories = [
            category
            for category, item in category_rows.items()
            if str(item.get('coverage_status') or 'missing') == 'missing'
            or int(item.get('present_rows') or 0) <= 0
        ]
        coverage_warning_categories = [
            category
            for category, item in category_rows.items()
            if category not in missing_categories and str(item.get('coverage_status') or '') != 'complete'
        ]
        failed_count = len(missing_categories)
        source_status = None
        if not category_rows or (all_missing and latest_captured_at is None):
            counts = build_stage_counts(total=total, succeeded=0, failed=0, processed=0)
            status = 'pending'
        else:
            soft_complete = present_count >= total and failed_count == 0
            counts = build_stage_counts(
                total=total,
                succeeded=total if soft_complete else present_count,
                failed=failed_count,
                processed=total if soft_complete else present_count + failed_count,
            )
            if soft_complete:
                status = 'completed'
                source_status = 'completed_with_coverage_warnings' if coverage_warning_categories else 'completed'
            else:
                status = 'partial'
                source_status = 'partial'
        return {
            'task': 'pykrx_enrichment',
            'label': STAGE_LABELS['pykrx_enrichment'],
            'status': status,
            'source_status': source_status if category_rows else None,
            'market_date': market_date,
            'started_at': None,
            'updated_at': latest_captured_at,
            'completed_at': latest_captured_at if status in {'completed', 'partial'} else None,
            'processed': counts.processed,
            'total': counts.total,
            'succeeded': counts.succeeded,
            'failed': counts.failed,
            'remaining': counts.remaining,
            'current_step': None,
            'current_activity': _default_activity(STAGE_LABELS['pykrx_enrichment'], status),
            'current_symbol': None,
            'current_period': None,
            'source_run_ids': {},
            'details': {
                'requested_symbol_count': coverage.get('requested_symbol_count'),
                'requested_market_count': coverage.get('requested_market_count'),
                'categories': category_rows,
                'missing_categories': missing_categories,
                'coverage_warning_categories': coverage_warning_categories,
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
        return {
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
            },
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
        price_stage_expected_total = self._resolve_price_stage_metadata_total(price_stage=price_stage)
        blocking_stages = [
            dependency
            for dependency in definition.depends_on
            if dependency in prior_stages and str(prior_stages[dependency].get('status') or 'pending') != 'completed'
        ]
        failed_blocking_stages = [
            dependency
            for dependency in blocking_stages
            if str(prior_stages[dependency].get('status') or 'pending') == 'failed'
        ]
        status = 'pending'
        if not blocking_stages:
            if persisted > 0 and expected_total and persisted < expected_total:
                status = 'partial'
            elif persisted > 0:
                status = 'completed'
        counts = build_stage_counts(
            total=max(expected_total, persisted),
            succeeded=persisted,
            failed=0,
            processed=persisted,
        )
        diagnostics = self._build_row_count_stage_diagnostics(
            stage_id=stage_id,
            status=status,
            market_date=market_date,
            persisted=persisted,
            expected_total=counts.total,
            blocking_stages=blocking_stages,
            failed_blocking_stages=failed_blocking_stages,
            aggregate=aggregate,
            feedback_run=feedback_run,
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
                persisted=persisted,
                expected_total=max(expected_total, persisted),
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
                'expected_rows': max(expected_total, persisted),
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
        feedback_scope_gap = None
        if (
            stage_id == 'market_risk_filter'
            and status == 'partial'
            and feedback_run is not None
        ):
            active_contract = self._resolve_active_feedback_market_risk_gate_contract(
                market_date=market_date,
                feedback_run=feedback_run,
            )
            if active_contract is not None:
                gate_status = active_contract['gate_status']
                severity = active_contract['severity']
                blocking_status = active_contract['blocking_status']
                gate_reason = active_contract['gate_reason']
                feedback_scope_gap = active_contract['feedback_scope_gap']

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
            'feedback_scope_gap': feedback_scope_gap,
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

    def _resolve_active_feedback_market_risk_gate_contract(
        self,
        *,
        market_date: str,
        feedback_run: dict[str, Any],
    ) -> dict[str, Any] | None:
        summary = dict(feedback_run.get('summary') if isinstance(feedback_run.get('summary'), dict) else {})
        run_status = str(summary.get('run_status') or feedback_run.get('status') or '').strip().lower()
        failed_items = _coerce_int(summary.get('failed_items'), feedback_run.get('failed_count'))
        requested_symbols = self._normalize_symbol_list(feedback_run.get('requested_symbols'))
        if run_status != 'running' or failed_items > 0 or not requested_symbols:
            return None

        feedback_scope_gap = self.row_count_scope_gap_loader(
            stage_id='market_risk_filter',
            market_date=market_date,
            symbols=requested_symbols,
        )
        missing_scope_count = (
            _coerce_int(feedback_scope_gap.get('missing_count'))
            if isinstance(feedback_scope_gap, dict) and feedback_scope_gap.get('missing_count') is not None
            else 0
        )
        if missing_scope_count > 0:
            return {
                'gate_status': 'blocked',
                'severity': 'blocked',
                'blocking_status': 'blocking',
                'gate_reason': 'missing_rows_intersect_active_feedback_scope',
                'feedback_scope_gap': feedback_scope_gap,
            }
        return {
            'gate_status': 'ready',
            'severity': 'degraded',
            'blocking_status': 'non_blocking',
            'gate_reason': 'missing_rows_outside_active_feedback_scope',
            'feedback_scope_gap': feedback_scope_gap,
        }

    @staticmethod
    def _normalize_stored_pykrx_coverage_stage(
        stage: dict[str, Any],
        *,
        persisted: int,
        expected_total: int,
    ) -> dict[str, Any]:
        if persisted <= 0 or str(stage.get('status') or '') == 'running':
            return stage
        normalized_total = max(int(expected_total or 0), persisted)
        counts = build_stage_counts(
            total=normalized_total,
            succeeded=persisted,
            failed=max(normalized_total - persisted, 0),
            processed=persisted,
        )
        status = 'completed' if counts.failed == 0 and counts.succeeded >= counts.total else 'partial'
        details = dict(stage.get('details') or {})
        details['stored_rows_present'] = persisted
        details['stored_rows_coverage_override'] = True
        return {
            **stage,
            'status': status,
            'source_status': stage.get('source_status') or 'stored_rows_present',
            'processed': counts.processed,
            'total': counts.total,
            'succeeded': counts.succeeded,
            'failed': counts.failed,
            'remaining': counts.remaining,
            'completed_at': stage.get('completed_at') or stage.get('updated_at'),
            'current_activity': _default_activity(str(stage.get('label') or stage.get('task') or ''), status),
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
    def _resolve_overall_status(stages: dict[str, dict[str, Any]]) -> str:
        statuses = [str(item.get('status') or 'pending') for item in stages.values()]
        if any(status == 'running' for status in statuses):
            return 'running'
        if any(status == 'interrupted' for status in statuses):
            return 'interrupted'
        if any(status == 'abandoned' for status in statuses):
            return 'abandoned'
        if any(status == 'blocked' for status in statuses):
            return 'blocked'
        if any(status == 'waiting' for status in statuses):
            return 'waiting'
        if all(status == 'pending' for status in statuses):
            return 'pending'
        if all(status == 'completed' for status in statuses):
            return 'completed'
        if statuses and all(status == 'skipped' for status in statuses):
            return 'skipped'
        if any(status == 'failed' for status in statuses) and not any(status in {'completed', 'partial'} for status in statuses):
            return 'failed'
        return 'partial'


eod_pipeline_status_service = EODPipelineStatusService(
    factor_stage_counts_loader=_load_factor_stage_counts,
    row_count_stage_counts_loader=_load_row_count_stage_counts,
    row_count_scope_gap_loader=_load_row_count_stage_scope_gap,
    price_bar_count_loader=lambda *, market_date: count_daily_price_bars_for_date(market_date),
    technical_snapshot_count_loader=lambda market_date: count_daily_technical_snapshots_for_live_symbols(market_date),
    action_summary_count_loader=lambda market_date: count_daily_action_summaries_for_live_symbols(market_date),
    feedback_run_loader=_load_latest_feedback_run_summary,
)
