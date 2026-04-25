from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Callable

from sqlalchemy import select

from app.db import session_scope
from app.db_models import EODCollectionRun
from app.eod.freshness_repository import (
    LATEST_AVAILABLE_FUNDAMENTALS_RUN_STATUSES,
    _load_daily_fundamentals_stage_counts,
    _load_latest_available_fundamentals_refresh_run,
    _load_latest_fundamentals_refresh_run,
    _load_latest_price_bar_updated_at,
    _load_today_fundamentals_refresh_run,
)
from app.market.clock import get_market_state
from app.market_data_store import (
    count_daily_action_summaries_for_live_symbols,
    count_daily_price_bars_for_date,
    count_daily_price_bars_for_live_symbols,
    count_daily_technical_snapshots_for_live_symbols,
    count_live_symbols,
    load_latest_available_daily_action_summary_date,
    load_latest_available_daily_price_trade_date,
    load_latest_available_daily_technical_snapshot_date,
    load_latest_eod_collection_run,
)

def _normalize_fundamentals_refresh_status(value: str | None) -> str | None:
    if not value:
        return None
    if value == 'completed_with_errors':
        return 'partial'
    if value == 'skipped':
        return 'pending'
    return value


@dataclass
class EODFreshnessService:
    market_state_loader: Callable = get_market_state
    latest_run_loader: Callable = load_latest_eod_collection_run
    live_symbol_count_loader: Callable = count_live_symbols
    daily_price_bar_count_loader: Callable = count_daily_price_bars_for_live_symbols
    daily_technical_snapshot_count_loader: Callable = count_daily_technical_snapshots_for_live_symbols
    daily_action_summary_count_loader: Callable = count_daily_action_summaries_for_live_symbols
    daily_fundamentals_count_loader: Callable = _load_daily_fundamentals_stage_counts
    fundamentals_today_refresh_loader: Callable = _load_today_fundamentals_refresh_run
    latest_fundamentals_refresh_loader: Callable = _load_latest_available_fundamentals_refresh_run
    latest_price_trade_date_loader: Callable = load_latest_available_daily_price_trade_date
    latest_price_bar_count_loader: Callable = count_daily_price_bars_for_date
    latest_price_bar_updated_at_loader: Callable = _load_latest_price_bar_updated_at
    latest_technical_market_date_loader: Callable = load_latest_available_daily_technical_snapshot_date
    latest_action_market_date_loader: Callable = load_latest_available_daily_action_summary_date

    def read(self) -> dict:
        trade_date = self.market_state_loader().server_time[:10]
        latest_price_trade_date = self.latest_price_trade_date_loader()
        price_bar_count = self.daily_price_bar_count_loader(trade_date)
        technical_snapshot_count = self.daily_technical_snapshot_count_loader(trade_date)
        action_summary_count = self.daily_action_summary_count_loader(trade_date)
        fundamentals_counts = self.daily_fundamentals_count_loader(trade_date)
        fundamentals_today_run = self.fundamentals_today_refresh_loader(trade_date)
        latest_available_fundamentals_run = self.latest_fundamentals_refresh_loader()

        if latest_price_trade_date:
            latest_price_run = self._select_freshness_run(
                trade_date=latest_price_trade_date,
                statuses=['completed', 'completed_with_errors'],
            ) or self._select_freshness_run(trade_date=latest_price_trade_date)
            latest_price_bar_count = self.latest_price_bar_count_loader(latest_price_trade_date)
        else:
            latest_price_run = None
            latest_price_bar_count = 0

        latest_summary = self._build_latest_summary(
            trade_date=latest_price_trade_date,
            latest_run=latest_price_run,
            price_bar_count=latest_price_bar_count,
        )
        latest_eod_status = self._resolve_eod_status(
            latest_run=latest_price_run,
            price_bar_count=latest_price_bar_count,
        )

        today_run = self._select_freshness_run(
            trade_date=trade_date,
            statuses=['completed', 'completed_with_errors'],
        ) or self._select_freshness_run(trade_date=trade_date)
        live_symbols_total = int((today_run or {}).get('symbols_total') or 0)
        if live_symbols_total == 0 and latest_price_run:
            live_symbols_total = int(latest_price_run.get('symbols_total') or 0)
        if live_symbols_total == 0:
            live_symbols_total = int(self.live_symbol_count_loader() or 0)

        return {
            'today': {
                'trade_date': trade_date,
                'price': self._build_live_symbol_stage(
                    symbols_total=live_symbols_total,
                    symbols_succeeded=price_bar_count,
                ),
                'technical_analysis': self._build_live_symbol_stage(
                    symbols_total=live_symbols_total,
                    symbols_succeeded=technical_snapshot_count,
                ),
                'action_summary': self._build_live_symbol_stage(
                    symbols_total=live_symbols_total,
                    symbols_succeeded=action_summary_count,
                ),
                'fundamentals': self._build_fundamentals_stage(
                    counts=fundamentals_counts,
                    current_run=fundamentals_today_run,
                ),
            },
            'latest': {
                'trade_date': latest_price_trade_date,
                'price_trade_date': latest_price_trade_date,
                'eod_status': latest_eod_status,
                'updated_at': latest_summary['updated_at'],
                'succeeded': latest_summary['succeeded'],
                'failed': latest_summary['failed'],
                'total': latest_summary['total'],
                'summary': latest_summary,
                'symbols_total': latest_price_run.get('symbols_total') if latest_price_run else None,
                'symbols_succeeded': latest_price_run.get('symbols_succeeded') if latest_price_run else None,
                'symbols_failed': latest_price_run.get('symbols_failed') if latest_price_run else None,
                'technical_market_date': self.latest_technical_market_date_loader(),
                'action_market_date': self.latest_action_market_date_loader(),
                'fundamentals': self._build_fundamentals_refresh_summary(
                    latest_available_fundamentals_run,
                    basis='latest_available_refresh_run',
                ),
            },
        }

    def _select_freshness_run(
        self,
        *,
        trade_date: str,
        statuses: list[str] | None = None,
    ) -> dict | None:
        candidate = self.latest_run_loader(market='KRX', trade_date=trade_date, statuses=statuses)
        if not candidate:
            return None
        candidate_total = int(candidate.get('symbols_total') or 0)
        candidate_succeeded = int(candidate.get('symbols_succeeded') or 0)
        latest_bar_count = int(self.latest_price_bar_count_loader(trade_date) or 0)
        if latest_bar_count > 0 and candidate_total > 0 and candidate_total < latest_bar_count and candidate_succeeded == 0:
            with session_scope() as session:
                fallback_row = session.execute(
                    select(EODCollectionRun)
                    .where(EODCollectionRun.market == 'KRX')
                    .where(EODCollectionRun.trade_date == date.fromisoformat(trade_date))
                    .where(EODCollectionRun.status.in_(statuses or ['completed', 'completed_with_errors', 'failed', 'running']))
                    .where(EODCollectionRun.symbols_succeeded > 0)
                    .order_by(EODCollectionRun.symbols_succeeded.desc(), EODCollectionRun.symbols_total.desc(), EODCollectionRun.started_at.desc(), EODCollectionRun.id.desc())
                    .limit(1)
                ).scalars().first()
                if fallback_row is not None:
                    return {
                        'id': fallback_row.id,
                        'market': fallback_row.market,
                        'trade_date': fallback_row.trade_date.isoformat(),
                        'source': fallback_row.source,
                        'trigger': fallback_row.trigger,
                        'status': fallback_row.status,
                        'symbols_total': fallback_row.symbols_total,
                        'symbols_succeeded': fallback_row.symbols_succeeded,
                        'symbols_failed': fallback_row.symbols_failed,
                        'error_message': fallback_row.error_message,
                        'started_at': fallback_row.started_at.isoformat() if fallback_row.started_at else None,
                        'completed_at': fallback_row.completed_at.isoformat() if fallback_row.completed_at else None,
                        'run_metadata': fallback_row.run_metadata,
                    }
        return candidate

    @staticmethod
    def _build_live_symbol_stage(
        *,
        symbols_total: int,
        symbols_succeeded: int,
    ) -> dict:
        if symbols_succeeded > 0:
            status = 'ready'
            symbols_failed = max(symbols_total - symbols_succeeded, 0)
        else:
            status = 'missing'
            symbols_failed = 0

        return {
            'status': status,
            'symbols_total': symbols_total,
            'symbols_succeeded': symbols_succeeded,
            'symbols_failed': symbols_failed,
        }

    def _build_fundamentals_stage(self, *, counts: dict[str, int], current_run: dict | None) -> dict:
        symbols_succeeded = int(counts.get('symbols_succeeded') or 0)
        symbols_failed = int(counts.get('symbols_failed') or 0)
        symbols_total = int(counts.get('symbols_total') or 0)

        if current_run and _normalize_fundamentals_refresh_status(str(current_run.get('status') or '')) == 'running':
            status = 'running'
        elif symbols_succeeded > 0:
            status = 'ready'
        else:
            status = 'missing'

        return {
            'status': status,
            'basis': 'same_day_snapshot_updates',
            'symbols_total': symbols_total,
            'symbols_succeeded': symbols_succeeded,
            'symbols_failed': symbols_failed,
            'current_run': self._build_fundamentals_refresh_summary(
                current_run,
                basis='market_date_refresh_run',
            ),
        }

    def _build_fundamentals_refresh_summary(
        self,
        run: dict | None,
        *,
        basis: str,
    ) -> dict:
        if run is None:
            return {
                'basis': None,
                'market_date': None,
                'status': None,
                'fs_div': None,
                'trigger': None,
                'updated_at': None,
                'completed_at': None,
                'symbols_total': None,
                'symbols_checked': None,
                'symbols_succeeded': None,
                'symbols_fresh': None,
                'symbols_refreshed': None,
                'symbols_failed': None,
                'symbols_remaining': None,
                'events_detected': None,
                'events_captured': None,
                'events_failed': None,
                'error_message': None,
            }

        metadata = run.get('run_metadata') if isinstance(run.get('run_metadata'), dict) else {}
        symbols_total = int(metadata.get('symbols_selected') or run.get('symbols_total') or 0)
        symbols_checked = int(run.get('symbols_checked') or 0)
        symbols_fresh = int(run.get('symbols_fresh') or 0)
        symbols_refreshed = int(run.get('symbols_refreshed') or 0)
        symbols_failed = int(run.get('symbols_failed') or 0)

        return {
            'basis': basis,
            'market_date': run.get('market_date'),
            'status': _normalize_fundamentals_refresh_status(str(run.get('status') or '')),
            'fs_div': run.get('fs_div'),
            'trigger': run.get('trigger'),
            'updated_at': self._resolve_latest_run_updated_at(run),
            'completed_at': run.get('completed_at'),
            'symbols_total': symbols_total,
            'symbols_checked': symbols_checked,
            'symbols_succeeded': symbols_fresh + symbols_refreshed,
            'symbols_fresh': symbols_fresh,
            'symbols_refreshed': symbols_refreshed,
            'symbols_failed': symbols_failed,
            'symbols_remaining': max(symbols_total - symbols_checked, 0),
            'events_detected': int(run.get('events_detected') or 0),
            'events_captured': int(run.get('events_captured') or 0),
            'events_failed': int(run.get('events_failed') or 0),
            'error_message': run.get('error_message'),
        }

    def _build_latest_summary(
        self,
        *,
        trade_date: str | None,
        latest_run: dict | None,
        price_bar_count: int,
    ) -> dict:
        # latest.summary is intentionally scoped to the selected latest trade date.
        # When an EOD run row exists, its symbol universe is the summary basis because
        # that best answers whether the latest EOD patch completed for that run.
        if latest_run:
            return {
                'basis': 'latest_eod_run_symbol_universe',
                'updated_at': self._resolve_latest_run_updated_at(latest_run),
                'succeeded': int(latest_run.get('symbols_succeeded') or 0),
                'failed': int(latest_run.get('symbols_failed') or 0),
                'total': int(latest_run.get('symbols_total') or 0),
            }

        if trade_date and price_bar_count > 0:
            return {
                'basis': 'stored_price_bars_for_latest_trade_date',
                'updated_at': self.latest_price_bar_updated_at_loader(trade_date),
                'succeeded': int(price_bar_count),
                'failed': 0,
                'total': int(price_bar_count),
            }

        return {
            'basis': None,
            'updated_at': None,
            'succeeded': None,
            'failed': None,
            'total': None,
        }

    @staticmethod
    def _resolve_latest_run_updated_at(latest_run: dict) -> str | None:
        metadata = latest_run.get('run_metadata') if isinstance(latest_run.get('run_metadata'), dict) else {}
        return metadata.get('last_progress_at') or latest_run.get('completed_at') or latest_run.get('started_at')

    @staticmethod
    def _resolve_eod_status(*, latest_run: dict | None, price_bar_count: int) -> str:
        if latest_run:
            status = latest_run.get('status')
            if status == 'running':
                return 'running'
            if status == 'completed_with_errors':
                return 'completed_with_errors'
            if status == 'completed':
                return 'completed_with_errors' if (latest_run.get('symbols_failed') or 0) > 0 else 'completed'
            if status == 'failed':
                return 'completed_with_errors' if price_bar_count > 0 else 'missing'
        if price_bar_count > 0:
            return 'completed'
        return 'missing'


eod_freshness_service = EODFreshnessService()
