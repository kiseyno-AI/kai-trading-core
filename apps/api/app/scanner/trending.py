from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Callable, Sequence

from app.market_data_store import (
    load_latest_available_daily_action_summary_date,
    load_latest_available_daily_price_trade_date,
    load_latest_available_daily_technical_snapshot_date,
    load_trending_stocks,
)

SUPPORTED_PERIODS = {'1d', '5d', '20d', 'action_score'}
SUPPORTED_DIRECTIONS = {'up', 'down', 'both'}


def _normalize_list(values: Sequence[str] | None) -> list[str] | None:
    if not values:
        return None
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_value in values:
        for piece in (raw_value or '').split(','):
            value = piece.strip().upper()
            if not value or value in seen:
                continue
            seen.add(value)
            normalized.append(value)
    return normalized or None


@dataclass
class TrendingStocksService:
    latest_price_trade_date_loader: Callable[[], str | None] = load_latest_available_daily_price_trade_date
    latest_technical_market_date_loader: Callable[[], str | None] = load_latest_available_daily_technical_snapshot_date
    latest_action_market_date_loader: Callable[[], str | None] = load_latest_available_daily_action_summary_date
    trending_loader: Callable = load_trending_stocks

    def _resolve_market_date(self, *, period: str, market_date: date | None) -> str | None:
        if market_date is not None:
            return market_date.isoformat()
        if period == '1d':
            return self.latest_price_trade_date_loader()
        if period == 'action_score':
            return self.latest_action_market_date_loader()
        return self.latest_technical_market_date_loader()

    def get_trending_stocks(
        self,
        *,
        market_date: date | None = None,
        period: str = '5d',
        direction: str = 'both',
        limit: int = 20,
        markets: Sequence[str] | None = None,
    ) -> dict:
        normalized_period = period.strip().lower()
        if normalized_period not in SUPPORTED_PERIODS:
            raise ValueError('period must be one of 1d, 5d, 20d, or action_score')

        normalized_direction = direction.strip().lower()
        if normalized_direction not in SUPPORTED_DIRECTIONS:
            raise ValueError('direction must be one of up, down, or both')

        normalized_markets = _normalize_list(markets)
        resolved_market_date = self._resolve_market_date(
            period=normalized_period,
            market_date=market_date,
        )
        if resolved_market_date is None:
            return {
                'request': {
                    'market_date': market_date.isoformat() if market_date else None,
                    'resolved_market_date': None,
                    'period': normalized_period,
                    'direction': normalized_direction,
                    'limit': limit,
                    'markets': normalized_markets,
                },
                'summary': {
                    'count': 0,
                    'up_count': 0,
                    'down_count': 0,
                    'flat_count': 0,
                    'data_available': False,
                },
                'items': [],
            }

        rows = self.trending_loader(
            market_date=resolved_market_date,
            period=normalized_period,
            direction=normalized_direction,
            limit=limit,
            markets=normalized_markets,
        )
        items = [
            {
                'rank': index + 1,
                **row,
            }
            for index, row in enumerate(rows)
        ]
        return {
            'request': {
                'market_date': market_date.isoformat() if market_date else None,
                'resolved_market_date': resolved_market_date,
                'period': normalized_period,
                'direction': normalized_direction,
                'limit': limit,
                'markets': normalized_markets,
            },
            'summary': {
                'count': len(items),
                'up_count': sum(1 for item in items if item['trend_direction'] == 'up'),
                'down_count': sum(1 for item in items if item['trend_direction'] == 'down'),
                'flat_count': sum(1 for item in items if item['trend_direction'] == 'flat'),
                'data_available': True,
            },
            'items': items,
        }


trending_stocks_service = TrendingStocksService()
