from __future__ import annotations

import logging

from app.brokers.kis.provider import kis_client
from app.market_data_store import load_current_quote, persist_quote_snapshot
from app.market.clock import get_market_state
from app.quotes.cache import quote_cache
from app.quotes.names import SymbolNameService
from app.scanner.state import scanner_state

logger = logging.getLogger(__name__)


def normalize_kis_quote(symbol: str, raw: dict, market_state: str) -> dict:
    output = raw.get('output', {}) if isinstance(raw, dict) else {}
    return {
        'symbol': symbol,
        'company_name': symbol,
        'price': int(output.get('stck_prpr', 0) or 0),
        'change': int(output.get('prdy_vrss', 0) or 0),
        'change_pct': float(output.get('prdy_ctrt', 0) or 0),
        'volume': int(output.get('acml_vol', 0) or 0),
        'turnover': int(output.get('acml_tr_pbmn', 0) or 0),
        'as_of': output.get('stck_cntg_hour'),
        'market_state': market_state,
        'source': 'kis',
        'fallback': False,
        'is_live': market_state == 'OPEN',
    }


class QuoteService:
    def __init__(self) -> None:
        self.client = kis_client
        self.name_service = SymbolNameService()

    async def get_quote(self, symbol: str) -> dict:
        market = get_market_state()
        if not market.is_open:
            cached = quote_cache.get(symbol)
            if cached:
                cached = dict(cached)
                cached['market_state'] = market.state
                cached['source'] = 'cache'
                cached['fallback'] = True
                cached['is_live'] = False
                cached['cached_at'] = quote_cache.get_updated_at(symbol)
                return cached
            try:
                persisted = load_current_quote(symbol)
            except Exception as exc:
                logger.warning('Quote cache lookup failed for %s: %s', symbol, exc)
            else:
                if persisted:
                    persisted['market_state'] = market.state
                    persisted['source'] = 'db_cache'
                    persisted['fallback'] = True
                    persisted['is_live'] = False
                    return persisted

        raw = await self.client.fetch_domestic_quote(symbol=symbol)
        normalized = normalize_kis_quote(symbol=symbol, raw=raw, market_state=market.state)
        normalized['company_name'] = await self.name_service.resolve(symbol)
        quote_cache.set(symbol, normalized)
        try:
            persist_quote_snapshot(normalized)
        except Exception as exc:
            logger.warning('Quote persistence failed for %s: %s', symbol, exc)
        normalized['cached_at'] = quote_cache.get_updated_at(symbol)
        return normalized

    async def get_tracked_quotes(self) -> dict:
        market = get_market_state()
        symbols = list(scanner_state.tracked_symbols)
        items = []
        for symbol in symbols:
            try:
                items.append(await self.get_quote(symbol))
            except Exception:
                cached = quote_cache.get(symbol)
                if cached:
                    fallback = dict(cached)
                    fallback['market_state'] = market.state
                    fallback['source'] = 'cache'
                    fallback['fallback'] = True
                    fallback['is_live'] = False
                    fallback['cached_at'] = quote_cache.get_updated_at(symbol)
                    items.append(fallback)
        return {
            'market_state': market.state,
            'as_of': market.server_time,
            'items': items,
        }
