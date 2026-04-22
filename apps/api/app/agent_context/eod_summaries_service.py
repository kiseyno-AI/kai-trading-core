from __future__ import annotations

from datetime import date
from typing import Any

from app.agent_context.eod_summaries_store import (
    EOD_SUMMARIZER_AGENT_NAME,
    AgentContextEodSummariesStore,
    agent_context_eod_summaries_store,
)
from app.agent_context.feedback_service import _normalize_market_date, _normalize_symbol


class AgentContextEodSummariesService:
    def __init__(self, *, store: AgentContextEodSummariesStore | None = None) -> None:
        self.store = store or agent_context_eod_summaries_store

    def inspect(
        self,
        *,
        market_date: str | date,
        symbol: str | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbol = _normalize_symbol(symbol) if symbol is not None else None
        requested_symbols = [normalized_symbol] if normalized_symbol else []

        if normalized_symbol:
            item = self.store.get_for_symbol(
                market_date=normalized_market_date,
                symbol=normalized_symbol,
            )
            items = [item] if item is not None else []
        else:
            items = self.store.list_for_market_date(market_date=normalized_market_date)
            item = None

        matched_symbols = [result['symbol'] for result in items]
        matched_symbol_set = set(matched_symbols)
        summary = {
            'entries': len(items),
            'requested_symbol': normalized_symbol,
            'matched_symbols': matched_symbols,
            'missing_symbols': [candidate for candidate in requested_symbols if candidate not in matched_symbol_set],
            'risk_flagged_count': sum(1 for result in items if result.get('risk_flags')),
        }
        payload: dict[str, Any] = {
            'market_date': normalized_market_date.isoformat(),
            'agent_name': EOD_SUMMARIZER_AGENT_NAME,
            'summary': summary,
        }
        if normalized_symbol:
            payload['item'] = item
        else:
            payload['items'] = items
        return payload


agent_context_eod_summaries_service = AgentContextEodSummariesService()
