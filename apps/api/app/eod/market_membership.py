from __future__ import annotations

import hashlib
from collections.abc import Sequence
from typing import Any


MARKET_MEMBERSHIP_SCHEMA_VERSION = 'pykrx_market_membership_v1'
MARKET_MEMBERSHIP_BASIS = 'end_date_snapshot'


def _normalized_text(value: Any) -> str:
    return str(value or '').strip().upper()


def market_membership_sha256(symbols: Sequence[str]) -> str:
    normalized = sorted(_normalized_text(symbol) for symbol in symbols)
    return hashlib.sha256('\n'.join(normalized).encode('utf-8')).hexdigest()


def build_market_membership_snapshot(
    *,
    symbols: Sequence[Any],
    markets: Sequence[str],
) -> dict[str, Any]:
    normalized_markets = list(
        dict.fromkeys(_normalized_text(market) for market in markets)
    )
    if not normalized_markets or any(not market for market in normalized_markets):
        raise ValueError('point-in-time market membership requires explicit markets')

    symbols_by_market: dict[str, list[str]] = {
        market: [] for market in normalized_markets
    }
    seen_symbols: set[str] = set()
    for item in symbols:
        symbol = _normalized_text(getattr(item, 'symbol', None))
        market = _normalized_text(getattr(item, 'market', None))
        if not symbol or market not in symbols_by_market:
            raise ValueError('point-in-time symbol identity is outside requested markets')
        if symbol in seen_symbols:
            raise ValueError('point-in-time market membership contains duplicate symbol')
        seen_symbols.add(symbol)
        symbols_by_market[market].append(symbol)

    return {
        'schema_version': MARKET_MEMBERSHIP_SCHEMA_VERSION,
        'membership_basis': MARKET_MEMBERSHIP_BASIS,
        'symbol_count': len(seen_symbols),
        'markets': {
            market: {
                'symbols': sorted(market_symbols),
                'symbol_count': len(market_symbols),
                'membership_sha256': market_membership_sha256(market_symbols),
            }
            for market, market_symbols in symbols_by_market.items()
        },
    }


def validate_market_membership_snapshot(
    payload: Any,
    *,
    requested_symbols: Sequence[str],
    markets: Sequence[str],
) -> dict[str, list[str]] | None:
    if not isinstance(payload, dict):
        return None
    normalized_requested_symbols = [
        _normalized_text(symbol) for symbol in requested_symbols
    ]
    normalized_markets = list(
        dict.fromkeys(_normalized_text(market) for market in markets)
    )
    if (
        not normalized_requested_symbols
        or any(not symbol for symbol in normalized_requested_symbols)
        or len(normalized_requested_symbols) != len(set(normalized_requested_symbols))
        or not normalized_markets
        or any(not market for market in normalized_markets)
        or payload.get('schema_version') != MARKET_MEMBERSHIP_SCHEMA_VERSION
        or payload.get('membership_basis') != MARKET_MEMBERSHIP_BASIS
        or isinstance(payload.get('symbol_count'), bool)
        or payload.get('symbol_count') != len(normalized_requested_symbols)
    ):
        return None

    raw_markets = payload.get('markets')
    if not isinstance(raw_markets, dict) or set(raw_markets) != set(normalized_markets):
        return None

    symbols_by_market: dict[str, list[str]] = {}
    all_members: list[str] = []
    for market in normalized_markets:
        entry = raw_markets.get(market)
        if not isinstance(entry, dict) or not isinstance(entry.get('symbols'), list):
            return None
        market_symbols = [_normalized_text(symbol) for symbol in entry['symbols']]
        if (
            any(not symbol for symbol in market_symbols)
            or len(market_symbols) != len(set(market_symbols))
            or isinstance(entry.get('symbol_count'), bool)
            or entry.get('symbol_count') != len(market_symbols)
            or entry.get('membership_sha256')
            != market_membership_sha256(market_symbols)
        ):
            return None
        symbols_by_market[market] = market_symbols
        all_members.extend(market_symbols)

    if (
        len(all_members) != len(set(all_members))
        or set(all_members) != set(normalized_requested_symbols)
    ):
        return None
    return symbols_by_market
