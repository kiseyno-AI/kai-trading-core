from __future__ import annotations

from datetime import datetime
from statistics import mean, median, pstdev
from typing import Any

from app.crypto.market_data import (
    ASSET_CLASS,
    INSTRUMENT_TYPE,
    PROVIDER,
    QUOTE_CURRENCY,
    USAGE_SCOPE,
    VENUE,
    UpbitCryptoMarketDataService,
    crypto_market_data_service,
    _split_market,
)

STRATEGY_CONTEXT_VERSION = 'crypto_strategy_context_v1'
MIN_STRATEGY_CANDLES = 5


class CryptoStrategyContextService:
    """Deterministic read-only crypto strategy context built from public Upbit data."""

    def __init__(self, market_data_service: UpbitCryptoMarketDataService | None = None) -> None:
        self.market_data_service = market_data_service or crypto_market_data_service

    async def get_strategy_context(
        self,
        *,
        markets: str | list[str] | None = None,
        count: int = 30,
    ) -> dict[str, Any]:
        selected_markets = self.market_data_service._select_markets(markets)
        ticker_payload = await self.market_data_service.get_tickers(markets=selected_markets)
        tickers_by_market = {
            str(item.get('market') or '').upper(): item
            for item in ticker_payload.get('items', [])
            if isinstance(item, dict)
        }

        items = []
        for market in selected_markets:
            candles_payload = await self.market_data_service.get_candles(
                market=market,
                interval='day',
                count=count,
            )
            candles = [
                item
                for item in candles_payload.get('items', [])
                if isinstance(item, dict)
            ]
            items.append(
                build_crypto_strategy_context_item(
                    market=market,
                    ticker=tickers_by_market.get(market),
                    candles=candles,
                    requested_count=count,
                )
            )

        statuses = [item['status'] for item in items]
        if statuses and all(status == 'available' for status in statuses):
            status = 'available'
        elif statuses and all(status == 'unavailable' for status in statuses):
            status = 'unavailable'
        else:
            status = 'partial'

        return {
            'provider': PROVIDER,
            'asset_class': ASSET_CLASS,
            'venue': VENUE,
            'quote_currency': QUOTE_CURRENCY,
            'usage_scope': USAGE_SCOPE,
            'strategy_context_version': STRATEGY_CONTEXT_VERSION,
            'status': status,
            'trading_enabled_in_core': False,
            'decision_support_only': True,
            'private_api_used': False,
            'account_data_used': False,
            'requested_markets': selected_markets,
            'requested_count': count,
            'count': len(items),
            'source': _strategy_source(),
            'items': items,
        }


def build_crypto_strategy_context_item(
    *,
    market: str,
    ticker: dict[str, Any] | None,
    candles: list[dict[str, Any]],
    requested_count: int,
) -> dict[str, Any]:
    quote_currency, base_currency = _split_market(market)
    valid_candles, incomplete_candle_count = _valid_candles(candles)
    closes = [_to_float(candle.get('close')) for candle in valid_candles]
    closes = [value for value in closes if value is not None and value > 0]
    latest_close = closes[-1] if closes else _to_float((ticker or {}).get('price'))

    missing_fields = _missing_fields(
        ticker=ticker,
        raw_candle_count=len(candles),
        valid_candle_count=len(valid_candles),
        incomplete_candle_count=incomplete_candle_count,
        requested_count=requested_count,
    )
    status = _data_status(valid_candle_count=len(valid_candles), missing_fields=missing_fields)

    returns = _returns(closes)
    window_return_pct = _percent_change(closes[0], closes[-1]) if len(closes) >= 2 else None
    recent_return_pct = _percent_change(closes[-min(len(closes), 7)], closes[-1]) if len(closes) >= 2 else None
    simple_average = mean(closes) if closes else None
    sma_distance_pct = _percent_change(simple_average, latest_close) if simple_average and latest_close else None

    trend_state = _trend_state(
        valid_candle_count=len(valid_candles),
        window_return_pct=window_return_pct,
        sma_distance_pct=sma_distance_pct,
    )
    momentum_score = _momentum_score(
        valid_candle_count=len(valid_candles),
        window_return_pct=window_return_pct,
        recent_return_pct=recent_return_pct,
        sma_distance_pct=sma_distance_pct,
    )

    volatility_pct = round(pstdev(returns) * 100, 4) if returns else None
    range_proxy_pct = _median_range_proxy_pct(valid_candles)
    volatility_tier = _volatility_tier(volatility_pct)
    liquidity = _liquidity_context(valid_candles=valid_candles, ticker=ticker, range_proxy_pct=range_proxy_pct)
    drawdown = _drawdown_context(closes)
    session_flags = _session_flags(valid_candles)
    risk_tier = _risk_tier(
        trend_state=trend_state,
        volatility_tier=volatility_tier,
        liquidity_tier=liquidity['liquidity_tier'],
        max_drawdown_pct=drawdown['max_drawdown_pct'],
    )
    levels = _decision_support_levels(valid_candles=valid_candles, latest_close=latest_close, range_proxy_pct=range_proxy_pct)

    reasons = _reasons(
        status=status,
        missing_fields=missing_fields,
        trend_state=trend_state,
        momentum_score=momentum_score,
        volatility_tier=volatility_tier,
        volatility_pct=volatility_pct,
        liquidity_tier=liquidity['liquidity_tier'],
        risk_tier=risk_tier,
        session_flags=session_flags,
    )

    return {
        'symbol': market,
        'market': market,
        'asset_class': ASSET_CLASS,
        'instrument_type': INSTRUMENT_TYPE,
        'provider': PROVIDER,
        'venue': VENUE,
        'quote_currency': quote_currency,
        'base_currency': base_currency,
        'usage_scope': USAGE_SCOPE,
        'strategy_context_version': STRATEGY_CONTEXT_VERSION,
        'status': status,
        'trading_enabled_in_core': False,
        'decision_support_only': True,
        'private_api_used': False,
        'account_data_used': False,
        'observed_window': _observed_window(
            candles=valid_candles,
            requested_count=requested_count,
            raw_candle_count=len(candles),
            incomplete_candle_count=incomplete_candle_count,
        ),
        'trend_state': trend_state,
        'momentum_score': momentum_score,
        'momentum': {
            'window_return_pct': _round(window_return_pct),
            'recent_return_pct': _round(recent_return_pct),
            'sma_distance_pct': _round(sma_distance_pct),
            'latest_close': _round_price(latest_close),
        },
        'volatility_tier': volatility_tier,
        'volatility': {
            'realized_daily_volatility_pct': volatility_pct,
            'range_spread_proxy_pct': range_proxy_pct,
            'range_spread_proxy_source': 'daily_high_low_range_pct',
        },
        'liquidity_tier': liquidity['liquidity_tier'],
        'liquidity': liquidity,
        'drawdown': drawdown,
        'risk_tier': risk_tier,
        'session_flags': session_flags,
        'reasons': reasons,
        'missing_fields': missing_fields,
        'invalidation_level': levels['invalidation_level'],
        'target_level': levels['target_level'],
        'source': _strategy_source(),
        'source_payload': {
            'ticker': ticker,
            'candles': candles,
        },
    }


def _strategy_source() -> dict[str, Any]:
    return {
        'provider': PROVIDER,
        'venue': VENUE,
        'api_family': 'quotation_derived_strategy_context',
        'endpoints': ['/v1/ticker', '/v1/candles/days'],
        'auth_required': False,
        'read_only': True,
        'order_submission_enabled': False,
        'private_api_used': False,
        'account_data_used': False,
        'rule_version': STRATEGY_CONTEXT_VERSION,
    }


def _valid_candles(candles: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    valid = []
    incomplete = 0
    for candle in candles:
        close = _to_float(candle.get('close'))
        high = _to_float(candle.get('high'))
        low = _to_float(candle.get('low'))
        if close is None or close <= 0 or high is None or low is None:
            incomplete += 1
            continue
        valid.append(candle)
    valid.sort(key=lambda item: str(item.get('candle_date_time_utc') or item.get('candle_date_time_kst') or ''))
    return valid, incomplete


def _missing_fields(
    *,
    ticker: dict[str, Any] | None,
    raw_candle_count: int,
    valid_candle_count: int,
    incomplete_candle_count: int,
    requested_count: int,
) -> list[str]:
    missing = []
    if ticker is None:
        missing.append('ticker')
    if raw_candle_count == 0:
        missing.append('candles')
    if incomplete_candle_count:
        missing.append('complete_candles')
    if valid_candle_count < MIN_STRATEGY_CANDLES:
        missing.append('minimum_strategy_window')
    elif valid_candle_count < requested_count:
        missing.append('requested_strategy_window')
    return missing


def _data_status(*, valid_candle_count: int, missing_fields: list[str]) -> str:
    if valid_candle_count == 0:
        return 'unavailable'
    if missing_fields:
        return 'partial'
    return 'available'


def _returns(closes: list[float]) -> list[float]:
    return [
        (current / previous) - 1
        for previous, current in zip(closes, closes[1:])
        if previous > 0
    ]


def _percent_change(start: float | None, end: float | None) -> float | None:
    if start is None or end is None or start == 0:
        return None
    return ((end / start) - 1) * 100


def _trend_state(
    *,
    valid_candle_count: int,
    window_return_pct: float | None,
    sma_distance_pct: float | None,
) -> str:
    if valid_candle_count < MIN_STRATEGY_CANDLES or window_return_pct is None or sma_distance_pct is None:
        return 'unavailable'
    if window_return_pct >= 3 and sma_distance_pct >= 1:
        return 'uptrend'
    if window_return_pct <= -3 and sma_distance_pct <= -1:
        return 'downtrend'
    return 'sideways'


def _momentum_score(
    *,
    valid_candle_count: int,
    window_return_pct: float | None,
    recent_return_pct: float | None,
    sma_distance_pct: float | None,
) -> float | None:
    if (
        valid_candle_count < MIN_STRATEGY_CANDLES
        or window_return_pct is None
        or recent_return_pct is None
        or sma_distance_pct is None
    ):
        return None
    raw_score = (window_return_pct * 2.5) + (recent_return_pct * 2.0) + (sma_distance_pct * 1.5)
    return round(max(-100, min(100, raw_score)), 2)


def _median_range_proxy_pct(candles: list[dict[str, Any]]) -> float | None:
    values = []
    for candle in candles:
        high = _to_float(candle.get('high'))
        low = _to_float(candle.get('low'))
        close = _to_float(candle.get('close'))
        if high is None or low is None or close is None or close <= 0:
            continue
        values.append(((high - low) / close) * 100)
    return round(median(values), 4) if values else None


def _volatility_tier(volatility_pct: float | None) -> str:
    if volatility_pct is None:
        return 'unavailable'
    if volatility_pct < 2:
        return 'low'
    if volatility_pct < 5:
        return 'medium'
    return 'high'


def _liquidity_context(
    *,
    valid_candles: list[dict[str, Any]],
    ticker: dict[str, Any] | None,
    range_proxy_pct: float | None,
) -> dict[str, Any]:
    trade_values = [
        value
        for value in (_to_float(candle.get('acc_trade_price')) for candle in valid_candles)
        if value is not None
    ]
    average_trade_value = mean(trade_values) if trade_values else None
    ticker_trade_value_24h = _to_float((ticker or {}).get('acc_trade_price_24h'))
    liquidity_basis = average_trade_value if average_trade_value is not None else ticker_trade_value_24h

    if liquidity_basis is None:
        tier = 'unavailable'
    elif liquidity_basis >= 50_000_000_000 and (range_proxy_pct is None or range_proxy_pct <= 4):
        tier = 'high'
    elif liquidity_basis >= 5_000_000_000 and (range_proxy_pct is None or range_proxy_pct <= 8):
        tier = 'medium'
    else:
        tier = 'low'

    return {
        'liquidity_tier': tier,
        'average_daily_trade_value_krw': _round(average_trade_value),
        'ticker_trade_value_24h_krw': _round(ticker_trade_value_24h),
        'range_spread_proxy_pct': range_proxy_pct,
        'range_spread_proxy_source': 'daily_high_low_range_pct',
        'orderbook_bid_ask_spread_available': False,
    }


def _drawdown_context(closes: list[float]) -> dict[str, Any]:
    if not closes:
        return {
            'max_drawdown_pct': None,
            'current_drawdown_pct': None,
        }
    peak = closes[0]
    max_drawdown = 0.0
    for close in closes:
        peak = max(peak, close)
        drawdown = (close / peak) - 1
        max_drawdown = min(max_drawdown, drawdown)
    current_peak = max(closes)
    current_drawdown = (closes[-1] / current_peak) - 1 if current_peak else None
    return {
        'max_drawdown_pct': round(max_drawdown * 100, 4),
        'current_drawdown_pct': round(current_drawdown * 100, 4) if current_drawdown is not None else None,
    }


def _session_flags(candles: list[dict[str, Any]]) -> dict[str, Any]:
    kst_datetimes = [
        parsed
        for parsed in (_parse_datetime(candle.get('candle_date_time_kst')) for candle in candles)
        if parsed is not None
    ]
    latest = kst_datetimes[-1] if kst_datetimes else None
    weekend_count = sum(1 for value in kst_datetimes if value.weekday() >= 5)
    return {
        'market_session_model': 'continuous_24_7',
        'continuous_session': True,
        'weekend_session': latest.weekday() >= 5 if latest else None,
        'observed_weekend_candle_count': weekend_count,
        'krx_market_close_applies': False,
        'dart_fundamentals_applicable': False,
    }


def _risk_tier(
    *,
    trend_state: str,
    volatility_tier: str,
    liquidity_tier: str,
    max_drawdown_pct: float | None,
) -> str:
    if volatility_tier == 'unavailable' and trend_state == 'unavailable':
        return 'unavailable'
    if volatility_tier == 'high' or liquidity_tier == 'low' or (max_drawdown_pct is not None and max_drawdown_pct <= -15):
        return 'high'
    if volatility_tier == 'medium' or trend_state == 'downtrend' or (max_drawdown_pct is not None and max_drawdown_pct <= -8):
        return 'medium'
    return 'low'


def _decision_support_levels(
    *,
    valid_candles: list[dict[str, Any]],
    latest_close: float | None,
    range_proxy_pct: float | None,
) -> dict[str, Any]:
    if latest_close is None or not valid_candles:
        return {
            'invalidation_level': None,
            'target_level': None,
        }
    recent_candles = valid_candles[-min(10, len(valid_candles)):]
    lows = [_to_float(candle.get('low')) for candle in recent_candles]
    highs = [_to_float(candle.get('high')) for candle in recent_candles]
    lows = [value for value in lows if value is not None and value > 0]
    highs = [value for value in highs if value is not None and value > 0]
    invalidation = min(lows) if lows else None
    target = max(highs) if highs else None
    if target is not None and target <= latest_close:
        buffer_pct = max(3.0, range_proxy_pct or 0)
        target = latest_close * (1 + (buffer_pct / 100))
    return {
        'invalidation_level': _round_price(invalidation),
        'target_level': _round_price(target),
    }


def _observed_window(
    *,
    candles: list[dict[str, Any]],
    requested_count: int,
    raw_candle_count: int,
    incomplete_candle_count: int,
) -> dict[str, Any]:
    first = candles[0] if candles else {}
    latest = candles[-1] if candles else {}
    return {
        'interval': 'day',
        'requested_count': requested_count,
        'raw_count': raw_candle_count,
        'observed_count': len(candles),
        'incomplete_candle_count': incomplete_candle_count,
        'start_time_utc': first.get('candle_date_time_utc'),
        'end_time_utc': latest.get('candle_date_time_utc'),
        'start_time_kst': first.get('candle_date_time_kst'),
        'end_time_kst': latest.get('candle_date_time_kst'),
        'latest_captured_at': latest.get('captured_at'),
    }


def _reasons(
    *,
    status: str,
    missing_fields: list[str],
    trend_state: str,
    momentum_score: float | None,
    volatility_tier: str,
    volatility_pct: float | None,
    liquidity_tier: str,
    risk_tier: str,
    session_flags: dict[str, Any],
) -> list[str]:
    reasons = []
    if status != 'available':
        reasons.append(f'data_state={status}; missing={",".join(missing_fields) or "none"}')
    if trend_state != 'unavailable':
        reasons.append(f'trend_state={trend_state}; momentum_score={momentum_score}')
    else:
        reasons.append('trend_state unavailable because the strategy candle window is incomplete')
    if volatility_tier != 'unavailable':
        reasons.append(f'volatility_tier={volatility_tier}; realized_daily_volatility_pct={volatility_pct}')
    if liquidity_tier != 'unavailable':
        reasons.append(f'liquidity_tier={liquidity_tier}; spread proxy uses daily high-low range, not orderbook')
    if session_flags.get('weekend_session') is True:
        reasons.append('latest observed candle is a weekend crypto session; KRX close assumptions are not applied')
    reasons.append(f'risk_tier={risk_tier}')
    return reasons


def _to_float(value: Any) -> float | None:
    if value in (None, ''):
        return None
    return float(value)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _round(value: float | None) -> float | None:
    return round(value, 4) if value is not None else None


def _round_price(value: float | None) -> float | None:
    return round(value, 8) if value is not None else None


crypto_strategy_context_service = CryptoStrategyContextService()
