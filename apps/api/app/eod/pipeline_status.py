from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Callable

from sqlalchemy import func, select

from app.core.settings import settings
from app.db import session_scope
from app.db_models import (
    DailyAdvancedValueFactorResult,
    DailyBetaFactorResult,
    DailyCompositeFactorResult,
    DailyLowRiskFactorResult,
    DailyMomentumFactorResult,
    DailyQualityFactorResult,
    DailyUnifiedValueFactorResult,
    DailyValueFactorResult,
)
from app.dart.fundamentals_capture_service import dart_fundamentals_capture_service
from app.eod.daily_pipeline_stages import FACTOR_STAGE_ORDER, STAGE_DEFINITIONS
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
from app.market_data_store import (
    DAY_BASED_TRENDING_PERIODS,
    count_daily_price_bars_for_date,
    count_live_symbols,
    load_eod_run_stage_counts,
)
from app.repositories.eod_run_repository import list_eod_collection_runs

DEFAULT_FACTOR_STAGE_ORDER = tuple(
    stage_id
    for stage_id in FACTOR_STAGE_ORDER
    if STAGE_DEFINITIONS[stage_id].default_selected
)
PIPELINE_STAGE_ORDER = (
    ('price_collection', 'pykrx_enrichment')
    + tuple(stage_id for stage_id in PYKRX_STAGE_ORDER if stage_id != 'price_collection')
    + ('fundamentals_daily_refresh',)
    + DEFAULT_FACTOR_STAGE_ORDER
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


def _noop_factor_stage_counts_loader(*, market_date: str) -> dict[str, dict[str, Any]]:
    return {}


def _noop_price_bar_count_loader(*, market_date: str) -> int:
    return 0


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
    price_bar_count_loader: Callable = _noop_price_bar_count_loader
    pykrx_enrichment_loader: Callable = _load_pykrx_enrichment_coverage

    def read(
        self,
        *,
        market_date: str | None = None,
        fs_div: str | None = None,
    ) -> dict[str, Any]:
        market_state = self.market_state_loader()
        # The default operator view stays on the current Seoul calendar date.
        # Callers must pass market_date explicitly when they want a previous trade date on weekends or break-days.
        resolved_market_date = date.fromisoformat(market_date or str(market_state.server_time)[:10]).isoformat()
        resolved_fs_div = (fs_div or settings.dart_fundamentals_default_fs_div).strip().upper()
        pykrx_run = self.pykrx_run_loader(trade_date=resolved_market_date)
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
        factor_stage_counts = self.factor_stage_counts_loader(market_date=resolved_market_date)
        stored_price_rows = int(self.price_bar_count_loader(market_date=resolved_market_date) or 0)
        if stored_price_rows <= 0:
            stored_price_rows = int(dict(pykrx_stage_counts.get('price_collection') or {}).get('persisted') or 0)
        stages = {
            **pykrx_stages,
            'pykrx_enrichment': pykrx_enrichment_stage,
            'fundamentals_daily_refresh': fundamentals_stage,
        }
        price_stage = stages['price_collection']
        for stage_id in DEFAULT_FACTOR_STAGE_ORDER:
            stages[stage_id] = self._build_factor_stage(
                stage_id=stage_id,
                market_date=resolved_market_date,
                price_stage=price_stage,
                price_rows_persisted=stored_price_rows,
                prior_stages=stages,
                aggregate=dict(factor_stage_counts.get(stage_id) or {}),
                live_symbols_total=live_symbols_total,
            )
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
            },
            'fs_div': resolved_fs_div,
            'status': self._resolve_overall_status(stages),
            'stage_order': list(PIPELINE_STAGE_ORDER),
            'stages': stages,
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
        failed_count = sum(1 for item in category_rows.values() if str(item.get('coverage_status') or 'missing') == 'missing')
        if not category_rows or (all_missing and latest_captured_at is None):
            counts = build_stage_counts(total=total, succeeded=0, failed=0, processed=0)
            status = 'pending'
        else:
            counts = build_stage_counts(
                total=total,
                succeeded=present_count,
                failed=failed_count,
                processed=present_count + failed_count,
            )
            if all(str(item.get('coverage_status') or '') == 'complete' for item in category_rows.values()):
                status = 'completed'
            else:
                status = 'partial'
        return {
            'task': 'pykrx_enrichment',
            'label': STAGE_LABELS['pykrx_enrichment'],
            'status': status,
            'source_status': status if category_rows else None,
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

    def _build_factor_stage(
        self,
        *,
        stage_id: str,
        market_date: str,
        price_stage: dict[str, Any],
        price_rows_persisted: int,
        prior_stages: dict[str, dict[str, Any]],
        aggregate: dict[str, Any],
        live_symbols_total: int,
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
        return {
            'task': stage_id,
            'label': label,
            'status': status,
            'source_status': 'stored_rows_present' if persisted > 0 else None,
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
            'current_activity': self._build_factor_activity(
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
            },
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
    def _build_factor_activity(
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
        if all(status == 'pending' for status in statuses):
            return 'pending'
        if all(status == 'completed' for status in statuses):
            return 'completed'
        if any(status == 'failed' for status in statuses) and not any(status in {'completed', 'partial'} for status in statuses):
            return 'failed'
        return 'partial'


eod_pipeline_status_service = EODPipelineStatusService(
    factor_stage_counts_loader=_load_factor_stage_counts,
    price_bar_count_loader=lambda *, market_date: count_daily_price_bars_for_date(market_date),
)
