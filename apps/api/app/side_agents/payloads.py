from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Sequence

from app.core.settings import settings

_NEWS_FIELDS = (
    'title', 'description', 'source', 'published_at', 'event_tags', 'event_type', 'sentiment',
    'materiality', 'materiality_bucket', 'materiality_reason', 'access_status', 'content_basis',
    'price_forecast_prohibited',
)


def build_market_payload(
    market_date: date,
    *,
    context_builder: Any | None = None,
    macro_service: Any | None = None,
) -> dict[str, Any]:
    if context_builder is None:
        from app.agent_context.signal_decider_market_context import SignalDeciderMarketContextBuilder

        context_builder = SignalDeciderMarketContextBuilder()
    market_context = context_builder.build(market_date=market_date)
    try:
        if macro_service is None:
            from app.macro_context.service import MacroContextService

            macro_service = MacroContextService()
        macro = macro_service.get_context(as_of_date=market_date)
    except Exception as exc:  # macro data is optional context; a gap is reported, not hidden
        macro = {'status': 'unavailable', 'reason': type(exc).__name__}
    return {
        'task': 'market_context',
        'market_date': market_date.isoformat(),
        'market_context': market_context,
        'macro_context': macro,
    }


def _trim_news_item(item: Mapping[str, Any]) -> dict[str, Any]:
    trimmed = {key: item.get(key) for key in _NEWS_FIELDS if item.get(key) is not None}
    for key in ('title', 'description'):
        if isinstance(trimmed.get(key), str):
            trimmed[key] = trimmed[key][:400]
    return trimmed


def select_news_symbols(items: Sequence[Mapping[str, Any]], *, max_symbols: int) -> dict[str, list[dict[str, Any]]]:
    """Group recent items by symbol, keeping symbols with material or unclassified news first."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        symbol = item.get('symbol')
        if not isinstance(symbol, str) or not symbol:
            continue
        grouped.setdefault(symbol, []).append(_trim_news_item(item))

    def rank(entry: tuple[str, list[dict[str, Any]]]) -> tuple[int, int, str]:
        symbol, news = entry
        material = sum(1 for n in news if n.get('materiality') == 'material')
        return (-material, -len(news), symbol)

    ordered = sorted(grouped.items(), key=rank)
    return {symbol: news[:5] for symbol, news in ordered[:max_symbols]}


def load_recent_news(
    *, now: datetime | None = None, lookback_hours: int | None = None, loader: Callable[..., list[dict[str, Any]]] | None = None
) -> list[dict[str, Any]]:
    if loader is None:
        from app.external_news.store import list_external_news_items

        loader = list_external_news_items
    resolved_now = now or datetime.now(timezone.utc)
    since = resolved_now - timedelta(hours=lookback_hours or settings.side_agents_news_lookback_hours)
    return loader(published_since=since, published_before=resolved_now, limit=500)


def build_news_payloads(
    market_date: date,
    news_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    batch_size: int,
    price_moves: Mapping[str, float] | None = None,
) -> list[dict[str, Any]]:
    symbols = list(news_by_symbol)
    payloads: list[dict[str, Any]] = []
    for start in range(0, len(symbols), max(1, batch_size)):
        batch = symbols[start : start + batch_size]
        payloads.append(
            {
                'task': 'news_events',
                'market_date': market_date.isoformat(),
                'symbols': batch,
                'items': [
                    {
                        'symbol': symbol,
                        'news': list(news_by_symbol[symbol]),
                        **(
                            {'price_move_since_publication_pct': round(float(price_moves[symbol]), 4)}
                            if price_moves and symbol in price_moves
                            else {}
                        ),
                    }
                    for symbol in batch
                ],
            }
        )
    return payloads


def build_postmortem_payload(summary: Mapping[str, Any]) -> dict[str, Any]:
    """Wrap a pre-aggregated outcome summary (for example the signal-lab JSON)."""
    return {'task': 'postmortem', 'summary': dict(summary)}
