from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
from typing import Any, Sequence

from app.agent_context.eligibility import load_agent_feedback_eligible_symbols
from app.core.settings import settings
from app.external_news.provider import ExternalNewsProvider, NaverNewsSearchProvider
from app.external_news.store import enrich_recent_external_news_classification, list_external_news_items, persist_external_news_items
from app.market_data_store import load_symbols


def _normalize_symbols(symbols: Sequence[str] | str | None) -> list[str]:
    if symbols is None:
        return []
    if isinstance(symbols, str):
        values = symbols.split(',')
    else:
        values = symbols
    return [str(item).strip() for item in values if str(item).strip()]


class ExternalNewsService:
    def __init__(self, *, provider: ExternalNewsProvider | None = None) -> None:
        self.provider = provider or NaverNewsSearchProvider()

    def provider_status(self) -> dict[str, Any]:
        return self.provider.status().as_dict()

    async def collect_for_symbols(
        self,
        *,
        symbols: Sequence[str] | str,
        display: int | None = None,
    ) -> dict[str, Any]:
        requested_symbols = _normalize_symbols(symbols)
        provider_status = self.provider.status()
        summary = {
            'requested_symbol_count': len(requested_symbols),
            'matched_symbol_count': 0,
            'symbols_with_news': 0,
            'items_fetched': 0,
            'items_inserted': 0,
            'items_updated': 0,
            'items_skipped': 0,
        }
        if not provider_status.enabled:
            return {
                'status': 'disabled',
                'reason': provider_status.reason,
                'provider': provider_status.as_dict(),
                'requested_symbols': requested_symbols,
                'summary': summary,
                'items': [],
            }

        symbol_rows = load_symbols(symbols=requested_symbols, limit=max(len(requested_symbols), 1000))
        symbol_map = {row['symbol']: row for row in symbol_rows}
        summary['matched_symbol_count'] = len(symbol_map)
        items_out: list[dict[str, Any]] = []
        per_symbol_limit = display or settings.external_news_default_display
        throttle_seconds = max(0.0, float(settings.external_news_collect_throttle_seconds or 0.0))
        for index, symbol in enumerate(requested_symbols):
            row = symbol_map.get(symbol)
            if row is None:
                summary['items_skipped'] += 1
                items_out.append({'symbol': symbol, 'status': 'skipped', 'reason': 'symbol_not_found'})
                continue
            if index > 0 and throttle_seconds > 0:
                await asyncio.sleep(throttle_seconds)
            query = f"{row.get('name_ko') or symbol} {symbol}"
            fetched = await self.provider.search_news(query=query, display=per_symbol_limit)
            persist_result = persist_external_news_items(symbol=symbol, items=fetched, source=self.provider.provider_name)
            summary['items_fetched'] += len(fetched)
            summary['items_inserted'] += int(persist_result.get('inserted', 0))
            summary['items_updated'] += int(persist_result.get('updated', 0))
            summary['items_skipped'] += int(persist_result.get('skipped', 0))
            if fetched:
                summary['symbols_with_news'] += 1
            items_out.append(
                {
                    'symbol': symbol,
                    'name_ko': row.get('name_ko'),
                    'status': 'ok',
                    'query': query,
                    'fetched_count': len(fetched),
                    'persisted': persist_result,
                }
            )
        return {
            'status': 'ok',
            'provider': provider_status.as_dict(),
            'collected_at': datetime.now(timezone.utc).isoformat(),
            'requested_symbols': requested_symbols,
            'summary': summary,
            'items': items_out,
        }

    async def collect_for_agent_feedback_candidates(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | str | None = None,
        display: int | None = None,
        symbol_limit: int | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = date.fromisoformat(str(market_date)) if not isinstance(market_date, date) else market_date
        requested_symbols = _normalize_symbols(symbols)
        provider_status = self.provider.status()
        if not provider_status.enabled:
            return {
                'status': 'disabled',
                'reason': provider_status.reason,
                'provider': provider_status.as_dict(),
                'market_date': normalized_market_date.isoformat(),
                'requested_symbols': requested_symbols,
                'eligible_symbols': [],
                'summary': {
                    'requested_symbol_count': len(requested_symbols),
                    'eligible_symbol_count': 0,
                    'collected_symbol_count': 0,
                },
            }
        eligible_symbols = load_agent_feedback_eligible_symbols(
            market_date=normalized_market_date,
            requested_symbols=requested_symbols or None,
        )
        if symbol_limit is not None:
            eligible_symbols = eligible_symbols[: max(0, int(symbol_limit))]
        if not eligible_symbols:
            return {
                'status': 'skipped',
                'reason': 'no_agent_feedback_eligible_symbols',
                'market_date': normalized_market_date.isoformat(),
                'provider': self.provider_status(),
                'requested_symbols': requested_symbols,
                'eligible_symbols': [],
                'summary': {
                    'requested_symbol_count': len(requested_symbols),
                    'eligible_symbol_count': 0,
                    'collected_symbol_count': 0,
                },
            }
        result = await self.collect_for_symbols(symbols=eligible_symbols, display=display)
        result.update(
            {
                'trigger': 'agent_feedback_candidates',
                'market_date': normalized_market_date.isoformat(),
                'eligible_symbols': eligible_symbols,
            }
        )
        result.setdefault('summary', {})['eligible_symbol_count'] = len(eligible_symbols)
        result['summary']['collected_symbol_count'] = len(eligible_symbols)
        return result

    def enrich_recent_classification(
        self,
        *,
        symbols: Sequence[str] | str | None = None,
        lookback_days: int | None = None,
        limit: int = 1000,
    ) -> dict[str, Any]:
        normalized_symbols = _normalize_symbols(symbols)
        provider_status = self.provider_status()
        try:
            result = enrich_recent_external_news_classification(
                symbols=normalized_symbols or None,
                lookback_days=lookback_days or settings.external_news_context_lookback_days,
                limit=limit,
            )
        except Exception as exc:
            return {
                'status': 'unavailable',
                'reason': 'external_news_storage_unavailable',
                'error_class': exc.__class__.__name__,
                'provider': provider_status,
                'requested_symbols': normalized_symbols,
            }
        return {
            **result,
            'provider': provider_status,
            'requested_symbols': normalized_symbols,
        }

    def list_news(self, *, symbols: Sequence[str] | str | None = None, limit: int = 100) -> dict[str, Any]:
        normalized_symbols = _normalize_symbols(symbols)
        items = list_external_news_items(symbols=normalized_symbols or None, provider=self.provider.provider_name, limit=limit)
        return {
            'status': 'ok',
            'provider': self.provider_status(),
            'summary': {'count': len(items), 'requested_symbols': normalized_symbols},
            'items': items,
        }


external_news_service = ExternalNewsService()
