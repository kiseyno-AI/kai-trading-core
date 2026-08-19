from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import math
from statistics import median
from typing import Any, Iterable

from app.us_equities.screening import USPriceScreenMetrics


@dataclass(frozen=True, slots=True)
class USPriceBar:
    symbol: str
    trade_date: date
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: int
    turnover: Decimal


def _decimal(value: Any) -> Decimal | None:
    normalized = str(value or '').replace(',', '').strip()
    if not normalized:
        return None
    try:
        return Decimal(normalized)
    except InvalidOperation:
        return None


def _integer(value: Any) -> int | None:
    parsed = _decimal(value)
    if parsed is None:
        return None
    try:
        return int(parsed)
    except (ValueError, OverflowError):
        return None


def parse_kis_daily_price_payload(payload: dict[str, Any], *, symbol: str) -> list[USPriceBar]:
    raw_rows = payload.get('output2')
    if isinstance(raw_rows, dict):
        raw_rows = [raw_rows]
    if not isinstance(raw_rows, list):
        return []
    normalized_symbol = symbol.strip().upper()
    bars_by_date: dict[date, USPriceBar] = {}
    for row in raw_rows:
        if not isinstance(row, dict):
            continue
        date_text = str(row.get('xymd') or '').strip()
        try:
            trade_date = datetime.strptime(date_text, '%Y%m%d').date()
        except ValueError:
            continue
        open_price = _decimal(row.get('open'))
        high_price = _decimal(row.get('high'))
        low_price = _decimal(row.get('low'))
        close_price = _decimal(row.get('clos'))
        volume = _integer(row.get('tvol'))
        turnover = _decimal(row.get('tamt'))
        if None in {open_price, high_price, low_price, close_price, volume, turnover}:
            continue
        if min(open_price, high_price, low_price, close_price) < 0 or volume < 0 or turnover < 0:
            continue
        bars_by_date.setdefault(
            trade_date,
            USPriceBar(
                symbol=normalized_symbol,
                trade_date=trade_date,
                open_price=open_price,
                high_price=high_price,
                low_price=low_price,
                close_price=close_price,
                volume=volume,
                turnover=turnover,
            ),
        )
    return [bars_by_date[trade_date] for trade_date in sorted(bars_by_date)]


def _simple_return(end: Decimal, start: Decimal) -> Decimal | None:
    if start <= 0:
        return None
    return (end / start) - Decimal('1')


def _annualized_volatility(closes: list[Decimal]) -> Decimal | None:
    if len(closes) < 2:
        return None
    returns = [float(value) for value in (_simple_return(closes[index], closes[index - 1]) for index in range(1, len(closes))) if value is not None]
    if len(returns) < 2:
        return None
    mean = sum(returns) / len(returns)
    variance = sum((value - mean) ** 2 for value in returns) / (len(returns) - 1)
    return Decimal(str(math.sqrt(variance) * math.sqrt(252)))


def _max_drawdown(closes: list[Decimal]) -> Decimal | None:
    if not closes:
        return None
    peak = closes[0]
    worst = Decimal('0')
    for close in closes:
        peak = max(peak, close)
        if peak > 0:
            worst = min(worst, (close / peak) - Decimal('1'))
    return worst


def compute_us_price_screen_metrics(
    symbol: str,
    bars: Iterable[USPriceBar],
) -> USPriceScreenMetrics:
    ordered = sorted(bars, key=lambda item: item.trade_date)
    closes = [item.close_price for item in ordered]
    latest_price = closes[-1] if closes else None
    trailing_20 = ordered[-20:]

    def trailing_return(periods: int) -> Decimal | None:
        if len(closes) <= periods:
            return None
        return _simple_return(closes[-1], closes[-periods - 1])

    return USPriceScreenMetrics(
        symbol=symbol.strip().upper(),
        latest_price=latest_price,
        history_days=len(ordered),
        median_dollar_volume_20d=(median([item.turnover for item in trailing_20]) if trailing_20 else None),
        median_volume_20d=(median([Decimal(item.volume) for item in trailing_20]) if trailing_20 else None),
        volatility_60d=_annualized_volatility(closes[-61:]),
        max_drawdown_120d=_max_drawdown(closes[-120:]),
        momentum_6m=trailing_return(126),
        momentum_12m=trailing_return(252),
        return_5d=trailing_return(5),
    )
