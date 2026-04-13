from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from app.market.clock import get_market_state
from app.market_data_store import (
    count_daily_price_bars_for_date,
    load_latest_available_daily_action_summary_date,
    load_latest_available_daily_price_trade_date,
    load_latest_available_daily_technical_snapshot_date,
    load_latest_eod_collection_run,
)


@dataclass
class EODFreshnessService:
    market_state_loader: Callable = get_market_state
    latest_run_loader: Callable = load_latest_eod_collection_run
    daily_price_bar_count_loader: Callable = count_daily_price_bars_for_date
    latest_price_trade_date_loader: Callable = load_latest_available_daily_price_trade_date
    latest_technical_market_date_loader: Callable = load_latest_available_daily_technical_snapshot_date
    latest_action_market_date_loader: Callable = load_latest_available_daily_action_summary_date

    def read(self) -> dict:
        trade_date = self.market_state_loader().server_time[:10]
        latest_price_trade_date = self.latest_price_trade_date_loader()
        latest_run = self.latest_run_loader(market='KRX', trade_date=trade_date)
        price_bar_count = self.daily_price_bar_count_loader(trade_date)

        return {
            'today': {
                'trade_date': trade_date,
                'eod_status': self._resolve_eod_status(latest_run=latest_run, price_bar_count=price_bar_count),
                'symbols_total': latest_run.get('symbols_total') if latest_run else None,
                'symbols_succeeded': latest_run.get('symbols_succeeded') if latest_run else None,
                'symbols_failed': latest_run.get('symbols_failed') if latest_run else None,
                'received': price_bar_count > 0,
            },
            'latest': {
                'price_trade_date': latest_price_trade_date,
                'technical_market_date': self.latest_technical_market_date_loader(),
                'action_market_date': self.latest_action_market_date_loader(),
            },
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
