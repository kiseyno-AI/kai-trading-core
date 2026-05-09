from __future__ import annotations

import logging
from datetime import date, datetime

from app.brokers.kis.provider import kis_client
from app.market_data_store import load_current_quote, load_daily_bars_for_symbol, persist_quote_snapshot
from app.market.clock import get_market_state
from app.quotes.cache import quote_cache
from app.quotes.names import SymbolNameService
from app.scanner.state import scanner_state

logger = logging.getLogger(__name__)
MAX_RECENT_DAILY_BAR_AGE_DAYS = 3


def _parse_optional_int(value) -> int | None:
    if value in (None, ''):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


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
        'bid_price': _parse_optional_int(output.get('bidp') or output.get('bidp1')),
        'ask_price': _parse_optional_int(output.get('askp') or output.get('askp1')),
        'as_of': output.get('stck_cntg_hour'),
        'market_state': market_state,
        'source': 'kis',
        'fallback': False,
        'is_live': market_state == 'OPEN',
        'available': True,
        'fallback_reason': None,
        'unavailable_reason': None,
        'trade_date': None,
    }


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace('Z', '+00:00')
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _market_calendar_date(server_time: str) -> date:
    parsed = _parse_iso_datetime(server_time)
    if parsed is None:
        return datetime.utcnow().date()
    return parsed.date()


class QuoteService:
    def __init__(self) -> None:
        self.client = kis_client
        self.name_service = SymbolNameService()

    async def _resolve_company_name(self, symbol: str) -> str:
        try:
            return await self.name_service.resolve(symbol)
        except Exception as exc:
            logger.warning('Company name resolution failed for %s: %s', symbol, exc)
            return symbol

    def _mark_quote(
        self,
        payload: dict,
        *,
        symbol: str,
        company_name: str | None,
        market_state: str,
        source: str,
        fallback: bool,
        is_live: bool,
        available: bool = True,
        cached_at: str | None = None,
        trade_date: str | None = None,
        fallback_reason: str | None = None,
        unavailable_reason: str | None = None,
    ) -> dict:
        marked = dict(payload)
        marked['symbol'] = symbol
        marked['company_name'] = marked.get('company_name') or company_name or symbol
        marked['market_state'] = market_state
        marked['source'] = source
        marked['fallback'] = fallback
        marked['is_live'] = is_live
        marked['available'] = available
        marked['cached_at'] = cached_at
        marked['trade_date'] = trade_date or marked.get('trade_date')
        marked['fallback_reason'] = fallback_reason
        marked['unavailable_reason'] = unavailable_reason
        return marked

    def _build_unavailable_quote(self, symbol: str, market_state: str, reason: str) -> dict:
        logger.warning('No acceptable quote fallback for %s in market state %s: %s', symbol, market_state, reason)
        return self._mark_quote(
            {
                'price': None,
                'change': None,
                'change_pct': None,
                'volume': None,
                'turnover': None,
                'as_of': None,
            },
            symbol=symbol,
            company_name=None,
            market_state=market_state,
            source='unavailable',
            fallback=False,
            is_live=False,
            available=False,
            unavailable_reason=reason,
        )

    def _load_same_day_cache_quote(self, symbol: str, market_date: date, market_state: str) -> dict | None:
        cached = quote_cache.get(symbol)
        cached_at = quote_cache.get_updated_at(symbol)
        cached_timestamp = _parse_iso_datetime(cached_at)
        if cached and cached_timestamp and cached_timestamp.date() == market_date:
            return self._mark_quote(
                cached,
                symbol=symbol,
                company_name=None,
                market_state=market_state,
                source='cache',
                fallback=True,
                is_live=False,
                cached_at=cached_at,
                trade_date=market_date.isoformat(),
                fallback_reason='same_day_quote_cache',
            )
        if cached and cached_at:
            logger.warning('Ignoring stale in-memory quote cache for %s captured at %s', symbol, cached_at)

        try:
            persisted = load_current_quote(symbol)
        except Exception as exc:
            logger.warning('Quote cache lookup failed for %s: %s', symbol, exc)
            return None

        persisted_cached_at = persisted.get('cached_at') if persisted else None
        persisted_timestamp = _parse_iso_datetime(persisted_cached_at)
        if persisted and persisted_timestamp and persisted_timestamp.date() == market_date:
            return self._mark_quote(
                persisted,
                symbol=symbol,
                company_name=None,
                market_state=market_state,
                source='db_cache',
                fallback=True,
                is_live=False,
                cached_at=persisted_cached_at,
                trade_date=market_date.isoformat(),
                fallback_reason='same_day_current_quote_cache',
            )
        if persisted and persisted_cached_at:
            logger.warning('Ignoring stale current_quote_cache row for %s captured at %s', symbol, persisted_cached_at)
        return None

    def _load_daily_bar_fallback(self, symbol: str, market_date: date, market_state: str) -> dict | None:
        try:
            bars = load_daily_bars_for_symbol(symbol, end_date=market_date, limit=2)
        except Exception as exc:
            logger.warning('Daily bar lookup failed for %s: %s', symbol, exc)
            return None

        if not bars:
            return None

        latest_bar = bars[-1]
        latest_trade_date = date.fromisoformat(latest_bar['trade_date'])
        age_days = (market_date - latest_trade_date).days
        trade_date_text = latest_trade_date.isoformat()

        if latest_trade_date == market_date:
            fallback_reason = 'daily_price_bar_current_trade_date'
        elif market_state in {'PREOPEN', 'WEEKEND', 'HOLIDAY'} and 0 <= age_days <= MAX_RECENT_DAILY_BAR_AGE_DAYS:
            fallback_reason = 'daily_price_bar_recent_previous_trade_date'
        else:
            logger.warning(
                'Rejecting stale daily_price_bars fallback for %s: latest trade_date=%s market_date=%s state=%s',
                symbol,
                trade_date_text,
                market_date.isoformat(),
                market_state,
            )
            return None

        if len(bars) > 1:
            previous_close = bars[-2]['close_price']
            change = latest_bar['close_price'] - previous_close
            change_pct = 0.0 if previous_close == 0 else (change / previous_close) * 100
        else:
            change = None
            change_pct = None
        return self._mark_quote(
            {
                'price': latest_bar['close_price'],
                'change': change,
                'change_pct': change_pct,
                'volume': latest_bar['volume'],
                'turnover': latest_bar['turnover'],
                'as_of': trade_date_text,
            },
            symbol=symbol,
            company_name=None,
            market_state=market_state,
            source='daily_price_bar',
            fallback=True,
            is_live=False,
            trade_date=trade_date_text,
            fallback_reason=fallback_reason,
        )

    async def get_quote(self, symbol: str) -> dict:
        market = get_market_state()
        market_date = _market_calendar_date(market.server_time)

        if market.is_open:
            try:
                raw = await self.client.fetch_domestic_quote(symbol=symbol)
            except Exception as exc:
                logger.warning('Live quote fetch failed for %s: %s', symbol, exc)
            else:
                normalized = normalize_kis_quote(symbol=symbol, raw=raw, market_state=market.state)
                normalized['company_name'] = await self._resolve_company_name(symbol)
                normalized['trade_date'] = market_date.isoformat()
                normalized = quote_cache.set(symbol, normalized)
                try:
                    persist_quote_snapshot(normalized)
                except Exception as exc:
                    logger.warning('Quote persistence failed for %s: %s', symbol, exc)
                return self._mark_quote(
                    normalized,
                    symbol=symbol,
                    company_name=normalized.get('company_name'),
                    market_state=market.state,
                    source='kis',
                    fallback=False,
                    is_live=True,
                    cached_at=quote_cache.get_updated_at(symbol),
                    trade_date=market_date.isoformat(),
                )

        daily_bar_fallback = self._load_daily_bar_fallback(symbol, market_date, market.state)
        if daily_bar_fallback:
            return daily_bar_fallback

        same_day_cache = self._load_same_day_cache_quote(symbol, market_date, market.state)
        if same_day_cache:
            return same_day_cache

        return self._build_unavailable_quote(
            symbol,
            market.state,
            'no_live_quote_or_fresh_fallback',
        )

    async def get_tracked_quotes(self) -> dict:
        market = get_market_state()
        symbols = list(scanner_state.tracked_symbols)
        items = []
        for symbol in symbols:
            try:
                items.append(await self.get_quote(symbol))
            except Exception as exc:
                logger.warning('Tracked quote assembly failed for %s: %s', symbol, exc)
                items.append(
                    self._build_unavailable_quote(
                        symbol,
                        market.state,
                        'tracked_quote_service_error',
                    )
                )
        return {
            'market_state': market.state,
            'as_of': market.server_time,
            'items': items,
        }
