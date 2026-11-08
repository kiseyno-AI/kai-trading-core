from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Sequence

from app.core.settings import settings
from app.side_agents import market_inputs, news_inputs

_NEWS_FIELDS = (
    'title', 'description', 'source', 'published_at', 'event_tags', 'event_type', 'sentiment',
    'materiality', 'materiality_bucket', 'materiality_reason', 'access_status', 'content_basis',
    'price_forecast_prohibited',
)
FILING_LOOKBACK_DAYS = 3


def load_fx_snapshot() -> dict[str, Any] | None:
    from sqlalchemy import text

    from app.db import session_scope

    with session_scope() as session:
        row = session.execute(
            text(
                "select exchange_rate, effective_date, requested_date, fallback_applied, snapshot_status "
                "from current_fx_rate_snapshots where currency_pair = 'USD/KRW' order by id desc limit 1"
            )
        ).mappings().first()
    if row is None:
        return None
    return {
        'status': 'ok', 'usd_krw': float(row['exchange_rate']), 'effective_date': row['effective_date'].isoformat(),
        'fallback_applied': bool(row['fallback_applied']), 'provider': 'koreaexim', 'snapshot_status': row['snapshot_status'],
    }


def _calendar_is_open(day: date) -> bool:
    from app.market.session import get_krx_calendar_day

    return bool(get_krx_calendar_day(day, allow_fallback=True).is_open)


def build_market_payload(
    market_date: date,
    *,
    kis: Any | None = None,
    ecos: Any | None = None,
    now: datetime | None = None,
    breadth_loader: Callable[[date], list[dict[str, Any]]] | None = None,
    fx_loader: Callable[[], dict[str, Any] | None] | None = None,
    is_trading_day: Callable[[date], bool] | None = None,
) -> dict[str, Any]:
    if kis is None:
        from app.brokers.kis.client import KISClient

        kis = KISClient()
    if ecos is None:
        from app.integrations.bok.client import ECOSClient

        ecos = ECOSClient()
    kwargs: dict[str, Any] = {
        'kis': kis, 'ecos': ecos, 'now': now, 'fx_loader': fx_loader or load_fx_snapshot,
        'is_trading_day': is_trading_day or _calendar_is_open,
    }
    if breadth_loader is not None:
        kwargs['breadth_loader'] = breadth_loader
    return asyncio.run(market_inputs.collect_market_inputs(market_date, **kwargs))


def _trim_news_item(item: Mapping[str, Any]) -> dict[str, Any]:
    trimmed = {key: item.get(key) for key in _NEWS_FIELDS if item.get(key) is not None}
    for key in ('title', 'description'):
        if isinstance(trimmed.get(key), str):
            trimmed[key] = trimmed[key][:400]
    return trimmed


def load_recent_news(
    *, now: datetime | None = None, lookback_hours: int | None = None, loader: Callable[..., list[dict[str, Any]]] | None = None
) -> list[dict[str, Any]]:
    if loader is None:
        from app.external_news.store import list_external_news_items

        loader = list_external_news_items
    resolved_now = now or datetime.now(timezone.utc)
    since = resolved_now - timedelta(hours=lookback_hours or settings.side_agents_news_lookback_hours)
    return loader(published_since=since, published_before=resolved_now, limit=500)


def assemble_news_inputs(
    market_date: date,
    news_items: Sequence[Mapping[str, Any]],
    filings: Sequence[Mapping[str, Any]],
    *,
    max_symbols: int,
    price_loader: Callable[[Sequence[str], date], Mapping[str, Mapping[str, Any]]],
    name_loader: Callable[[Sequence[str]], Mapping[str, str]],
) -> dict[str, dict[str, Any]]:
    news_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for item in news_items:
        symbol = item.get('symbol')
        if isinstance(symbol, str) and symbol:
            news_by_symbol.setdefault(symbol, []).append(_trim_news_item(item))
    filings_by_symbol = news_inputs.group_filings(filings)
    chosen = news_inputs.select_symbols(news_by_symbol, filings_by_symbol, max_symbols=max_symbols)
    prices = price_loader(chosen, market_date)
    names = name_loader(chosen)
    return {
        symbol: {
            'name_ko': names.get(symbol),
            'news': news_by_symbol.get(symbol, [])[:5],
            'official_filings': filings_by_symbol.get(symbol, []),
            'price_context': dict(prices.get(symbol, {'status': 'unavailable'})),
        }
        for symbol in chosen
    }


def load_news_inputs(market_date: date, now: datetime) -> dict[str, dict[str, Any]]:
    items = load_recent_news(now=now)
    filings = news_inputs.load_dart_filings(market_date - timedelta(days=FILING_LOOKBACK_DAYS))
    return assemble_news_inputs(
        market_date, items, filings, max_symbols=settings.side_agents_news_max_symbols,
        price_loader=lambda codes, day: news_inputs.load_price_contexts(codes, day),
        name_loader=lambda codes: news_inputs.load_names(codes),
    )


def build_news_payloads(
    market_date: date, inputs: Mapping[str, Mapping[str, Any]], *, batch_size: int
) -> list[dict[str, Any]]:
    symbols = list(inputs)
    payloads: list[dict[str, Any]] = []
    batch_size = max(1, batch_size)
    for start in range(0, len(symbols), batch_size):
        batch = symbols[start : start + batch_size]
        payloads.append(
            {
                'task': 'news_events',
                'market_date': market_date.isoformat(),
                'symbols': batch,
                'items': [{'symbol': symbol, **inputs[symbol]} for symbol in batch],
            }
        )
    return payloads


def build_postmortem_payload(summary: Mapping[str, Any]) -> dict[str, Any]:
    return {'task': 'postmortem', 'summary': dict(summary)}
