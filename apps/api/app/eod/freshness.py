from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Callable

from app.core.settings import settings
from app.dart.fundamentals_store import dart_fundamentals_repository
from app.market.clock import get_market_state
from app.market_data_store import (
    count_daily_action_summaries_for_live_symbols,
    count_daily_price_bars_for_live_symbols,
    count_daily_technical_snapshots_for_live_symbols,
    count_live_symbols,
    load_latest_available_daily_action_summary_date,
    load_latest_available_daily_price_trade_date,
    load_latest_available_daily_technical_snapshot_date,
    load_latest_eod_collection_run,
)


def _load_latest_fundamentals_refresh_run(market_date: str) -> dict | None:
    return dart_fundamentals_repository.get_latest_refresh_run(
        market_date=date.fromisoformat(market_date),
        fs_div=settings.dart_fundamentals_default_fs_div.strip().upper(),
    )


@dataclass
class EODFreshnessService:
    market_state_loader: Callable = get_market_state
    latest_run_loader: Callable = load_latest_eod_collection_run
    live_symbol_count_loader: Callable = count_live_symbols
    daily_price_bar_count_loader: Callable = count_daily_price_bars_for_live_symbols
    daily_technical_snapshot_count_loader: Callable = count_daily_technical_snapshots_for_live_symbols
    daily_action_summary_count_loader: Callable = count_daily_action_summaries_for_live_symbols
    latest_fundamentals_run_loader: Callable = _load_latest_fundamentals_refresh_run
    latest_price_trade_date_loader: Callable = load_latest_available_daily_price_trade_date
    latest_technical_market_date_loader: Callable = load_latest_available_daily_technical_snapshot_date
    latest_action_market_date_loader: Callable = load_latest_available_daily_action_summary_date

    def read(self) -> dict:
        trade_date = self.market_state_loader().server_time[:10]
        live_symbols_total = self.live_symbol_count_loader()
        latest_price_trade_date = self.latest_price_trade_date_loader()
        today_run = self.latest_run_loader(market='KRX', trade_date=trade_date)
        price_bar_count = self.daily_price_bar_count_loader(trade_date)
        technical_snapshot_count = self.daily_technical_snapshot_count_loader(trade_date)
        action_summary_count = self.daily_action_summary_count_loader(trade_date)
        fundamentals_run = self.latest_fundamentals_run_loader(trade_date)

        if latest_price_trade_date == trade_date:
            latest_price_run = today_run
            latest_price_bar_count = price_bar_count
        elif latest_price_trade_date:
            latest_price_run = self.latest_run_loader(market='KRX', trade_date=latest_price_trade_date)
            latest_price_bar_count = self.daily_price_bar_count_loader(latest_price_trade_date)
        else:
            latest_price_run = None
            latest_price_bar_count = 0

        return {
            'today': {
                'trade_date': trade_date,
                'price': self._build_live_symbol_stage(
                    latest_run=today_run,
                    symbols_total=live_symbols_total,
                    symbols_succeeded=price_bar_count,
                    known_symbols_failed=today_run.get('symbols_failed') if today_run else 0,
                ),
                'technical_analysis': self._build_live_symbol_stage(
                    latest_run=today_run,
                    symbols_total=live_symbols_total,
                    symbols_succeeded=technical_snapshot_count,
                ),
                'action_summary': self._build_live_symbol_stage(
                    latest_run=today_run,
                    symbols_total=live_symbols_total,
                    symbols_succeeded=action_summary_count,
                ),
                'fundamentals': self._build_fundamentals_stage(fundamentals_run),
            },
            'latest': {
                'price_trade_date': latest_price_trade_date,
                'eod_status': self._resolve_eod_status(
                    latest_run=latest_price_run,
                    price_bar_count=latest_price_bar_count,
                ),
                'symbols_total': latest_price_run.get('symbols_total') if latest_price_run else None,
                'symbols_succeeded': latest_price_run.get('symbols_succeeded') if latest_price_run else None,
                'symbols_failed': latest_price_run.get('symbols_failed') if latest_price_run else None,
                'technical_market_date': self.latest_technical_market_date_loader(),
                'action_market_date': self.latest_action_market_date_loader(),
            },
        }

    @staticmethod
    def _build_live_symbol_stage(
        *,
        latest_run: dict | None,
        symbols_total: int,
        symbols_succeeded: int,
        known_symbols_failed: int | None = 0,
    ) -> dict:
        run_status = latest_run.get('status') if latest_run else None
        if run_status == 'running':
            return {
                'status': 'running',
                'symbols_total': symbols_total,
                'symbols_succeeded': symbols_succeeded,
                'symbols_failed': int(known_symbols_failed or 0),
            }

        if symbols_succeeded > 0:
            status = 'ready'
            symbols_failed = max(symbols_total - symbols_succeeded, int(known_symbols_failed or 0), 0)
        elif latest_run and run_status in {'completed', 'completed_with_errors', 'failed'}:
            status = 'missing'
            symbols_failed = max(symbols_total, int(known_symbols_failed or 0), 0)
        else:
            status = 'missing'
            symbols_failed = 0

        return {
            'status': status,
            'symbols_total': symbols_total,
            'symbols_succeeded': symbols_succeeded,
            'symbols_failed': symbols_failed,
        }

    @staticmethod
    def _build_fundamentals_stage(latest_run: dict | None) -> dict:
        if latest_run is None:
            return {
                'status': 'missing',
                'symbols_total': 0,
                'symbols_succeeded': 0,
                'symbols_failed': 0,
            }

        symbols_refreshed = int(latest_run.get('symbols_refreshed') or 0)
        symbols_failed = int(latest_run.get('symbols_failed') or 0)
        status = 'running' if latest_run.get('status') == 'running' else 'ready'
        if latest_run.get('status') == 'failed' and symbols_refreshed == 0:
            status = 'missing'

        return {
            'status': status,
            'symbols_total': symbols_refreshed,
            'symbols_succeeded': symbols_refreshed,
            'symbols_failed': symbols_failed,
        }

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
