from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from app.eod.freshness_repository import (
    LATEST_AVAILABLE_FUNDAMENTALS_RUN_STATUSES,
    _load_daily_fundamentals_stage_counts,
    _load_latest_available_fundamentals_refresh_run,
    _load_latest_fundamentals_refresh_run,
    _load_latest_price_bar_updated_at,
    _load_today_fundamentals_refresh_run,
)
from app.market.clock import get_market_state
from app.market.session import build_market_session_metadata
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
from app.eod.run_stage_support import (
    build_stage_counts,
    extract_fundamentals_refresh_metrics,
    extract_run_stage_counts,
    normalize_run_status,
    resolve_run_updated_at,
    select_preferred_eod_run,
)
from app.repositories.eod_run_repository import (
    list_eod_collection_runs,
    load_most_complete_eod_collection_run,
)

DEFAULT_FRESHNESS_FALLBACK_STATUSES = (
    'completed',
    'completed_with_errors',
    'failed',
    'running',
)


def _load_freshness_run_rows(
    *,
    trade_date: str,
    statuses: list[str] | None = None,
) -> list[dict[str, Any]]:
    return list_eod_collection_runs(
        market='KRX',
        trade_date=trade_date,
        statuses=statuses,
        limit=10,
    )


@dataclass
class EODFreshnessService:
    market_state_loader: Callable = get_market_state
    latest_run_loader: Callable = load_latest_eod_collection_run
    latest_run_rows_loader: Callable | None = None
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
    best_run_loader: Callable = load_most_complete_eod_collection_run

    def read(self) -> dict:
        # Keep "today" bound to the current Seoul calendar date.
        # Weekend and break-day fallback lives under latest.*, not by backshifting today.trade_date.
        market_state = self.market_state_loader()
        trade_date = market_state.server_time[:10]
        market_session = build_market_session_metadata(
            requested_date=trade_date,
            current_state=market_state,
        )
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
            'market_session': market_session,
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
        candidate = (
            select_preferred_eod_run(
                self.latest_run_rows_loader(trade_date=trade_date, statuses=statuses)
            )
            if self.latest_run_rows_loader is not None
            else self.latest_run_loader(market='KRX', trade_date=trade_date, statuses=statuses)
        )
        if not candidate:
            return None
        candidate_counts = extract_run_stage_counts(candidate)
        latest_bar_count = int(self.latest_price_bar_count_loader(trade_date) or 0)
        if (
            latest_bar_count > 0
            and candidate_counts.total > 0
            and candidate_counts.total < latest_bar_count
            and candidate_counts.succeeded == 0
        ):
            fallback = self.best_run_loader(
                market='KRX',
                trade_date=trade_date,
                statuses=statuses or DEFAULT_FRESHNESS_FALLBACK_STATUSES,
            )
            if fallback is not None:
                return fallback
        return candidate

    @staticmethod
    def _build_live_symbol_stage(
        *,
        symbols_total: int,
        symbols_succeeded: int,
    ) -> dict:
        # EOD retries can be scoped to only failed/remaining symbols, so the latest run's
        # symbols_total may be smaller than the already persisted row count. Freshness is
        # a stored-data coverage view; never publish impossible ratios such as 3,683 / 877.
        normalized_total = max(int(symbols_total or 0), int(symbols_succeeded or 0))
        counts = build_stage_counts(
            total=normalized_total,
            succeeded=symbols_succeeded,
            failed=max(normalized_total - symbols_succeeded, 0) if symbols_succeeded > 0 else 0,
        )
        if symbols_succeeded > 0:
            status = 'ready'
        else:
            status = 'missing'

        return {
            'status': status,
            'symbols_total': counts.total,
            'symbols_succeeded': counts.succeeded,
            'symbols_failed': counts.failed,
        }

    def _build_fundamentals_stage(self, *, counts: dict[str, int], current_run: dict | None) -> dict:
        stage_counts = build_stage_counts(
            total=counts.get('symbols_total'),
            succeeded=counts.get('symbols_succeeded'),
            failed=counts.get('symbols_failed'),
        )

        # today.fundamentals is an activity view, but a live refresh run should still surface as running
        # even before same-day snapshot updates are fully reflected in the stored counts.
        if current_run and normalize_run_status(str(current_run.get('status') or '')) == 'running':
            status = 'running'
        elif stage_counts.succeeded > 0:
            status = 'ready'
        else:
            status = 'missing'

        return {
            'status': status,
            'basis': 'same_day_snapshot_updates',
            'symbols_total': stage_counts.total,
            'symbols_succeeded': stage_counts.succeeded,
            'symbols_failed': stage_counts.failed,
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

        metrics = extract_fundamentals_refresh_metrics(run)

        return {
            'basis': basis,
            'market_date': run.get('market_date'),
            'status': normalize_run_status(str(run.get('status') or '')),
            'fs_div': run.get('fs_div'),
            'trigger': run.get('trigger'),
            'updated_at': resolve_run_updated_at(run),
            'completed_at': run.get('completed_at'),
            'symbols_total': metrics.total,
            'symbols_checked': metrics.processed,
            'symbols_succeeded': metrics.succeeded,
            'symbols_fresh': metrics.fresh,
            'symbols_refreshed': metrics.refreshed,
            'symbols_failed': metrics.failed,
            'symbols_remaining': metrics.remaining,
            'events_detected': metrics.events_detected,
            'events_captured': metrics.events_captured,
            'events_failed': metrics.events_failed,
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
            latest_counts = extract_run_stage_counts(latest_run)
            return {
                'basis': 'latest_eod_run_symbol_universe',
                'updated_at': resolve_run_updated_at(latest_run),
                'succeeded': latest_counts.succeeded,
                'failed': latest_counts.failed,
                'total': latest_counts.total,
            }

        if trade_date and price_bar_count > 0:
            latest_counts = build_stage_counts(
                total=price_bar_count,
                succeeded=price_bar_count,
                failed=0,
            )
            return {
                'basis': 'stored_price_bars_for_latest_trade_date',
                'updated_at': self.latest_price_bar_updated_at_loader(trade_date),
                'succeeded': latest_counts.succeeded,
                'failed': latest_counts.failed,
                'total': latest_counts.total,
            }

        return {
            'basis': None,
            'updated_at': None,
            'succeeded': None,
            'failed': None,
            'total': None,
        }

    @staticmethod
    def _resolve_eod_status(*, latest_run: dict | None, price_bar_count: int) -> str:
        if latest_run:
            status = latest_run.get('status')
            latest_counts = extract_run_stage_counts(latest_run)
            if status == 'running':
                return 'running'
            if status == 'completed_with_errors':
                return 'completed_with_errors'
            if status == 'completed':
                return 'completed_with_errors' if latest_counts.failed > 0 else 'completed'
            if status == 'failed':
                return 'completed_with_errors' if price_bar_count > 0 else 'missing'
        if price_bar_count > 0:
            return 'completed'
        return 'missing'

eod_freshness_service = EODFreshnessService(latest_run_rows_loader=_load_freshness_run_rows)
