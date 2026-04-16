from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Callable
from zoneinfo import ZoneInfo

from sqlalchemy import func, select

from app.core.settings import settings
from app.db import session_scope
from app.db_models import DartDisclosureEvent, DartFundamentalsSnapshot
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

SEOUL_TZ = ZoneInfo('Asia/Seoul')
FUNDAMENTALS_CAPTURE_FAILED = 'failed'


def _seoul_day_bounds(value: str | date) -> tuple[datetime, datetime]:
    market_date = date.fromisoformat(value) if isinstance(value, str) else value
    start = datetime.combine(market_date, time.min, tzinfo=SEOUL_TZ)
    return start, start + timedelta(days=1)


def _load_daily_fundamentals_stage_counts(market_date: str) -> dict[str, int]:
    start_at, end_at = _seoul_day_bounds(market_date)
    fs_div = settings.dart_fundamentals_default_fs_div.strip().upper()

    with session_scope() as session:
        succeeded_codes = set(
            session.execute(
                select(func.distinct(DartFundamentalsSnapshot.stock_code))
                .select_from(DartFundamentalsSnapshot)
                .where(DartFundamentalsSnapshot.fs_div == fs_div)
                .where(DartFundamentalsSnapshot.stock_code.is_not(None))
                .where(DartFundamentalsSnapshot.updated_at >= start_at)
                .where(DartFundamentalsSnapshot.updated_at < end_at)
            ).scalars()
        )
        failed_codes = set(
            session.execute(
                select(func.distinct(DartDisclosureEvent.stock_code))
                .select_from(DartDisclosureEvent)
                .where(DartDisclosureEvent.is_financial_report.is_(True))
                .where(DartDisclosureEvent.stock_code.is_not(None))
                .where(DartDisclosureEvent.fundamentals_capture_status == FUNDAMENTALS_CAPTURE_FAILED)
                .where(DartDisclosureEvent.fundamentals_last_attempt_at >= start_at)
                .where(DartDisclosureEvent.fundamentals_last_attempt_at < end_at)
            ).scalars()
        )

    return {
        'symbols_succeeded': len(succeeded_codes),
        'symbols_failed': len(failed_codes),
        'symbols_total': len(succeeded_codes | failed_codes),
    }


@dataclass
class EODFreshnessService:
    market_state_loader: Callable = get_market_state
    latest_run_loader: Callable = load_latest_eod_collection_run
    live_symbol_count_loader: Callable = count_live_symbols
    daily_price_bar_count_loader: Callable = count_daily_price_bars_for_live_symbols
    daily_technical_snapshot_count_loader: Callable = count_daily_technical_snapshots_for_live_symbols
    daily_action_summary_count_loader: Callable = count_daily_action_summaries_for_live_symbols
    daily_fundamentals_count_loader: Callable = _load_daily_fundamentals_stage_counts
    latest_price_trade_date_loader: Callable = load_latest_available_daily_price_trade_date
    latest_price_bar_count_loader: Callable = count_daily_price_bars_for_date
    latest_technical_market_date_loader: Callable = load_latest_available_daily_technical_snapshot_date
    latest_action_market_date_loader: Callable = load_latest_available_daily_action_summary_date

    def read(self) -> dict:
        trade_date = self.market_state_loader().server_time[:10]
        live_symbols_total = self.live_symbol_count_loader()
        latest_price_trade_date = self.latest_price_trade_date_loader()
        price_bar_count = self.daily_price_bar_count_loader(trade_date)
        technical_snapshot_count = self.daily_technical_snapshot_count_loader(trade_date)
        action_summary_count = self.daily_action_summary_count_loader(trade_date)
        fundamentals_counts = self.daily_fundamentals_count_loader(trade_date)

        if latest_price_trade_date:
            latest_price_run = self.latest_run_loader(market='KRX', trade_date=latest_price_trade_date)
            latest_price_bar_count = self.latest_price_bar_count_loader(latest_price_trade_date)
        else:
            latest_price_run = None
            latest_price_bar_count = 0

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
                'fundamentals': self._build_fundamentals_stage(fundamentals_counts),
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

    @staticmethod
    def _build_fundamentals_stage(counts: dict[str, int]) -> dict:
        symbols_succeeded = int(counts.get('symbols_succeeded') or 0)
        symbols_failed = int(counts.get('symbols_failed') or 0)
        status = 'ready' if symbols_succeeded > 0 else 'missing'

        return {
            'status': status,
            'symbols_total': int(counts.get('symbols_total') or 0),
            'symbols_succeeded': symbols_succeeded,
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
