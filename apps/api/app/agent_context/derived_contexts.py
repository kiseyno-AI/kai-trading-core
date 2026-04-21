from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any, Sequence

from app.market_data_store import (
    list_symbols_with_daily_bar_on_date,
    load_daily_signal_feature_context_rows,
    load_recent_price_bars_by_symbol,
    upsert_daily_market_participation_context,
    upsert_daily_price_context,
)


SIGNAL_PARTICIPATION_SOURCES: tuple[tuple[str, str, str], ...] = (
    ('volume-power', 'volume_power_rank', 'volume_power'),
    ('overtime-volume', 'overtime_volume_rank', 'overtime_volume'),
    ('overtime-fluctuation', 'overtime_fluctuation_rank', 'overtime_fluctuation_rate'),
)
PARTICIPATION_CONTEXT_LEADER_LIMIT = 20


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _round_float(value: float | None, *, digits: int = 4) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def _ratio(value: int | float | None, total: int | float | None) -> float | None:
    if value is None or total in (None, 0):
        return None
    return round(float(value) / float(total), 4)


def _percent(value: int | float | None, base: int | float | None) -> float | None:
    if value is None or base in (None, 0):
        return None
    return _round_float((float(value) / float(base)) * 100.0)


def _classify_body_size(body_percent_of_range: float | None) -> str | None:
    if body_percent_of_range is None:
        return None
    if body_percent_of_range <= 10.0:
        return 'small'
    if body_percent_of_range <= 40.0:
        return 'medium'
    return 'large'


def _classify_wick_bias(*, upper_wick_points: int, lower_wick_points: int) -> str:
    if upper_wick_points == 0 and lower_wick_points == 0:
        return 'none'
    larger = max(upper_wick_points, lower_wick_points)
    smaller = min(upper_wick_points, lower_wick_points)
    if smaller == 0:
        return 'upper' if upper_wick_points > lower_wick_points else 'lower'
    if larger / smaller < 1.25:
        return 'balanced'
    return 'upper' if upper_wick_points > lower_wick_points else 'lower'


def _classify_close_position(position: float | None) -> str | None:
    if position is None:
        return None
    if position <= 0.2:
        return 'near_low'
    if position <= 0.4:
        return 'lower_half'
    if position < 0.6:
        return 'middle'
    if position < 0.8:
        return 'upper_half'
    return 'near_high'


def build_price_context(price_bars: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not price_bars:
        return None

    current = price_bars[0]
    previous = price_bars[1] if len(price_bars) > 1 else None

    open_price = current.get('open_price')
    high_price = current.get('high_price')
    low_price = current.get('low_price')
    close_price = current.get('close_price')
    if None in (open_price, high_price, low_price, close_price):
        return None

    range_points = max(int(high_price) - int(low_price), 0)
    body_points_signed = int(close_price) - int(open_price)
    body_points = abs(body_points_signed)
    upper_wick_points = max(int(high_price) - max(int(open_price), int(close_price)), 0)
    lower_wick_points = max(min(int(open_price), int(close_price)) - int(low_price), 0)
    close_position_in_range = (
        _round_float((int(close_price) - int(low_price)) / range_points) if range_points > 0 else None
    )
    body_percent_of_range = _percent(body_points, range_points)
    previous_close_price = previous.get('close_price') if previous else None
    gap_points = int(open_price) - int(previous_close_price) if previous_close_price is not None else None
    gap_percent = _percent(gap_points, previous_close_price) if gap_points is not None else None
    gap_filled_intraday = None
    if gap_points is not None and gap_points > 0:
        gap_filled_intraday = int(low_price) <= int(previous_close_price)
    elif gap_points is not None and gap_points < 0:
        gap_filled_intraday = int(high_price) >= int(previous_close_price)

    if body_points_signed > 0:
        candle_direction = 'bullish'
    elif body_points_signed < 0:
        candle_direction = 'bearish'
    else:
        candle_direction = 'flat'

    if gap_points is None:
        gap_direction = None
    elif gap_points > 0:
        gap_direction = 'up'
    elif gap_points < 0:
        gap_direction = 'down'
    else:
        gap_direction = 'flat'

    return {
        'trade_date': current.get('trade_date'),
        'source': current.get('source'),
        'captured_at': current.get('captured_at'),
        'open_price': int(open_price),
        'high_price': int(high_price),
        'low_price': int(low_price),
        'close_price': int(close_price),
        'volume': current.get('volume'),
        'turnover': current.get('turnover'),
        'candle': {
            'direction': candle_direction,
            'body_points': body_points,
            'body_percent_of_open': _percent(body_points, open_price),
            'body_percent_of_range': body_percent_of_range,
            'body_size': _classify_body_size(body_percent_of_range),
            'upper_wick_points': upper_wick_points,
            'lower_wick_points': lower_wick_points,
            'wick_bias': _classify_wick_bias(
                upper_wick_points=upper_wick_points,
                lower_wick_points=lower_wick_points,
            ),
        },
        'session_range': {
            'points': range_points,
            'percent_of_open': _percent(range_points, open_price),
            'close_position_in_range': close_position_in_range,
            'close_position_label': _classify_close_position(close_position_in_range),
        },
        'gap': {
            'previous_trade_date': previous.get('trade_date') if previous else None,
            'previous_close_price': int(previous_close_price) if previous_close_price is not None else None,
            'gap_points': gap_points,
            'gap_percent': gap_percent,
            'direction': gap_direction,
            'filled_intraday': gap_filled_intraday,
        },
    }


def build_signal_participation_context(
    rows: list[dict[str, Any]],
    *,
    limit: int,
) -> dict[str, Any]:
    distribution: dict[str, int] = {}
    for row in rows:
        signal_count = row.get('signal_count')
        if signal_count is None:
            continue
        key = str(int(signal_count))
        distribution[key] = distribution.get(key, 0) + 1

    source_counts = {
        source: sum(1 for row in rows if row.get(rank_key) is not None)
        for source, rank_key, _ in SIGNAL_PARTICIPATION_SOURCES
    }

    leaders_by_source: dict[str, list[dict[str, Any]]] = {}
    for source, rank_key, value_key in SIGNAL_PARTICIPATION_SOURCES:
        source_rows = [
            {
                'symbol': row['symbol'],
                'name_ko': row['name_ko'],
                'market': row['market'],
                'rank': row[rank_key],
                'metric_name': value_key,
                'metric_value': _to_float(row[value_key]),
                'signal_count': row['signal_count'],
            }
            for row in rows
            if row.get(rank_key) is not None
        ]
        source_rows.sort(key=lambda item: (item['rank'], item['symbol']))
        leaders_by_source[source] = source_rows[:limit]

    return {
        'captured_symbol_count': len(rows),
        'signal_count_distribution': dict(sorted(distribution.items())),
        'source_counts': source_counts,
        'leaders_by_source': leaders_by_source,
    }


def apply_participation_run_coverage(
    context: dict[str, Any] | None,
    *,
    symbols_total: int | None,
    symbols_succeeded: int | None,
) -> dict[str, Any]:
    base = dict(context or {})
    captured_symbol_count = int(base.get('captured_symbol_count') or 0)
    source_counts = {
        source: int((base.get('source_counts') or {}).get(source) or 0)
        for source, _, _ in SIGNAL_PARTICIPATION_SOURCES
    }
    leaders_by_source = {
        source: list((base.get('leaders_by_source') or {}).get(source) or [])
        for source, _, _ in SIGNAL_PARTICIPATION_SOURCES
    }

    return {
        'captured_symbol_count': captured_symbol_count,
        'coverage_ratio_vs_run_total': _ratio(captured_symbol_count, symbols_total),
        'coverage_ratio_vs_run_succeeded': _ratio(captured_symbol_count, symbols_succeeded),
        'signal_count_distribution': dict(base.get('signal_count_distribution') or {}),
        'source_counts': source_counts,
        'source_coverage_ratio_vs_run_succeeded': {
            source: _ratio(count, symbols_succeeded)
            for source, count in source_counts.items()
        },
        'leaders_by_source': leaders_by_source,
    }


class DailyPriceContextService:
    def compute_and_persist_for_market_date(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
        source: str = 'pykrx',
        run_id: int | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = market_date if isinstance(market_date, date) else date.fromisoformat(market_date)
        target_symbols = list(dict.fromkeys(symbols or list_symbols_with_daily_bar_on_date(normalized_market_date.isoformat())))
        price_bars_by_symbol = load_recent_price_bars_by_symbol(
            market_date=normalized_market_date,
            symbols=target_symbols,
        )

        items: list[dict[str, Any]] = []
        failed_symbols: list[str] = []
        items: list[dict[str, Any]] = []
        failed_symbols: list[str] = []
        expected_trade_date = normalized_market_date.isoformat()
        for symbol in target_symbols:
            price_bars = price_bars_by_symbol.get(symbol, [])
            if not price_bars or price_bars[0].get('trade_date') != expected_trade_date:
                failed_symbols.append(symbol)
                continue

            context_payload = build_price_context(price_bars)
            if context_payload is None:
                failed_symbols.append(symbol)
                continue
            if context_payload is None:
                failed_symbols.append(symbol)
                continue
            upsert_daily_price_context(
                {
                    'symbol': symbol,
                    'market_date': normalized_market_date.isoformat(),
                    'source': source,
                    'context_payload': context_payload,
                },
                run_id=run_id,
            )
            items.append(
                {
                    'symbol': symbol,
                    'market_date': normalized_market_date.isoformat(),
                    'price_context': context_payload,
                }
            )

        return {
            'market_date': normalized_market_date.isoformat(),
            'source': source,
            'requested_symbols': len(target_symbols),
            'contexts_persisted': len(items),
            'contexts_failed': len(failed_symbols),
            'failed_symbols': failed_symbols[:50],
            'items': items,
        }


class DailyMarketParticipationContextService:
    def compute_and_persist_for_market_date(
        self,
        *,
        market_date: str | date,
        market: str = 'KRX',
        source: str = 'kis_ranking',
        leader_limit: int = PARTICIPATION_CONTEXT_LEADER_LIMIT,
    ) -> dict[str, Any]:
        normalized_market_date = market_date if isinstance(market_date, date) else date.fromisoformat(market_date)
        rows = load_daily_signal_feature_context_rows(market_date=normalized_market_date)
        context_payload = build_signal_participation_context(rows, limit=leader_limit)
        upsert_daily_market_participation_context(
            market_date=normalized_market_date,
            market=market,
            source=source,
            context_payload=context_payload,
        )
        return {
            'market': market,
            'market_date': normalized_market_date.isoformat(),
            'source': source,
            'captured_symbol_count': context_payload['captured_symbol_count'],
            'context_payload': context_payload,
        }


daily_price_context_service = DailyPriceContextService()
daily_market_participation_context_service = DailyMarketParticipationContextService()
