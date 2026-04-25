from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Callable

from app.core.settings import settings
from app.dart.fundamentals_capture_service import dart_fundamentals_capture_service
from app.eod.pykrx_backfill import DEFAULT_MARKETS, PYKRX_BACKFILL_SERVICE
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
from app.market_data_store import DAY_BASED_TRENDING_PERIODS, count_live_symbols, load_eod_run_stage_counts
from app.repositories.eod_run_repository import list_eod_collection_runs

PIPELINE_STAGE_ORDER = PYKRX_STAGE_ORDER + ('fundamentals_daily_refresh',)


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


@dataclass
class EODPipelineStatusService:
    market_state_loader: Callable = get_market_state
    pykrx_run_loader: Callable = _load_latest_pipeline_pykrx_run
    pykrx_stage_counts_loader: Callable = load_eod_run_stage_counts
    live_symbol_count_loader: Callable = count_live_symbols
    fundamentals_refresh_loader: Callable = _load_latest_fundamentals_refresh_run

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
        fundamentals_stage = self._build_fundamentals_stage(
            market_date=resolved_market_date,
            fs_div=resolved_fs_div,
        )
        stages = {
            **pykrx_stages,
            'fundamentals_daily_refresh': fundamentals_stage,
        }
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


eod_pipeline_status_service = EODPipelineStatusService()
