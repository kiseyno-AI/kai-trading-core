from __future__ import annotations

from datetime import date
from typing import Sequence

from app.market_data_store import (
    load_daily_market_risk_filter_results,
    load_daily_piotroski_filter_results,
)

AGENT_FEEDBACK_ELIGIBILITY_FILTERS: tuple[str, ...] = (
    'market_risk_filter',
    'piotroski_filter',
)
AGENT_FEEDBACK_ELIGIBILITY_RULE = 'market_risk_filter_passed_and_piotroski_passed'
AGENT_FEEDBACK_SCOPE_ALL = 'active_pre_filter_intersection_only'
AGENT_FEEDBACK_SCOPE_REQUESTED = 'requested_symbols_intersected_with_active_pre_filters'


def _extract_passed_symbols(rows: Sequence[dict]) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()
    for item in rows:
        symbol = str(item.get('symbol') or '').strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    return symbols


def resolve_agent_feedback_scope(*, requested_symbols: Sequence[str] | None) -> str:
    return AGENT_FEEDBACK_SCOPE_REQUESTED if requested_symbols is not None else AGENT_FEEDBACK_SCOPE_ALL


def load_agent_feedback_eligible_symbols(
    *,
    market_date: date,
    requested_symbols: Sequence[str] | None = None,
) -> list[str]:
    symbol_scope = list(requested_symbols) if requested_symbols is not None else None
    piotroski_passed_symbols = _extract_passed_symbols(
        load_daily_piotroski_filter_results(
            market_date=market_date,
            symbols=symbol_scope,
            passed_filter=True,
            limit=None,
        )
    )
    piotroski_passed_symbol_set = set(piotroski_passed_symbols)
    market_risk_passed_symbol_set = set(
        _extract_passed_symbols(
            load_daily_market_risk_filter_results(
                market_date=market_date,
                symbols=symbol_scope,
                passed_filter=True,
                limit=None,
            )
        )
    )
    if requested_symbols is not None:
        return [
            symbol
            for symbol in symbol_scope or []
            if symbol in market_risk_passed_symbol_set and symbol in piotroski_passed_symbol_set
        ]
    return [symbol for symbol in piotroski_passed_symbols if symbol in market_risk_passed_symbol_set]


def split_requested_symbols_by_agent_feedback_eligibility(
    *,
    market_date: date,
    requested_symbols: Sequence[str],
) -> tuple[list[str], list[str]]:
    eligible_symbols = load_agent_feedback_eligible_symbols(
        market_date=market_date,
        requested_symbols=requested_symbols,
    )
    eligible_symbol_set = set(eligible_symbols)
    skipped_symbols = [symbol for symbol in requested_symbols if symbol not in eligible_symbol_set]
    return eligible_symbols, skipped_symbols
