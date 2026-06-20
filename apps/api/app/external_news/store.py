from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, Sequence

from sqlalchemy import desc, or_, select

from app.db import session_scope
from app.db_models import ExternalNewsItem, Symbol
from app.external_news.classifier import (
    classify_external_news_item,
    external_news_classification_has_contract,
    external_news_materiality_sort_key,
)
from app.external_news.provider import ExternalNewsItem as ProviderNewsItem


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _classification_from_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    classification = payload.get('classification')
    return classification if isinstance(classification, dict) else {}


def _payload_with_classification(item: ProviderNewsItem) -> dict[str, Any]:
    payload = dict(item.payload or {})
    payload['classification'] = classify_external_news_item(
        title=item.title,
        description=item.description,
        payload=payload,
        url=item.url,
        source_url=item.source_url,
        published_at=item.published_at,
    )
    return payload


def _serialize(row: ExternalNewsItem, *, symbol: str, name_ko: str | None, market: str | None) -> dict[str, Any]:
    payload = row.payload or {}
    classification = _classification_from_payload(payload)
    return {
        'id': row.id,
        'symbol': symbol,
        'name_ko': name_ko,
        'market': market,
        'provider': row.provider,
        'provider_item_id': row.provider_item_id,
        'source': row.source,
        'title': row.title,
        'description': row.description,
        'url': row.url,
        'source_url': row.source_url,
        'published_at': row.published_at.isoformat() if row.published_at else None,
        'collected_at': row.collected_at.isoformat() if row.collected_at else None,
        'event_tags': list(classification.get('event_tags') or []),
        'event_type': classification.get('event_type'),
        'sentiment': classification.get('sentiment'),
        'materiality_score': classification.get('materiality_score'),
        'materiality_bucket': classification.get('materiality_bucket'),
        'materiality': classification.get('materiality'),
        'materiality_reason': classification.get('materiality_reason'),
        'source_evidence': classification.get('source_evidence'),
        'access_status': classification.get('access_status'),
        'content_basis': classification.get('content_basis'),
        'price_forecast_prohibited': classification.get('price_forecast_prohibited'),
        'classification': classification,
        'payload': payload,
    }


def _get_symbol(session, symbol_code: str) -> Symbol | None:
    return session.execute(select(Symbol).where(Symbol.symbol == symbol_code)).scalar_one_or_none()


def persist_external_news_items(*, symbol: str, items: Sequence[ProviderNewsItem], source: str | None = None) -> dict[str, Any]:
    now = _utcnow()
    inserted = 0
    updated = 0
    skipped = 0
    with session_scope() as session:
        symbol_row = _get_symbol(session, symbol)
        if symbol_row is None:
            return {'inserted': 0, 'updated': 0, 'skipped': len(items), 'reason': 'symbol_not_found'}

        for item in items:
            if not item.url:
                skipped += 1
                continue
            dedupe_conditions = [ExternalNewsItem.url == item.url]
            if item.provider_item_id:
                dedupe_conditions.append(ExternalNewsItem.provider_item_id == item.provider_item_id)
            stmt = (
                select(ExternalNewsItem)
                .where(ExternalNewsItem.provider == item.provider)
                .where(or_(*dedupe_conditions))
            )
            existing = session.execute(stmt).scalar_one_or_none()
            payload = _payload_with_classification(item)
            if existing is None:
                session.add(
                    ExternalNewsItem(
                        symbol_id=symbol_row.id,
                        provider=item.provider,
                        provider_item_id=item.provider_item_id,
                        source=source or item.provider,
                        title=item.title,
                        description=item.description,
                        url=item.url,
                        source_url=item.source_url,
                        published_at=item.published_at,
                        collected_at=now,
                        payload=payload,
                    )
                )
                inserted += 1
                continue
            existing.symbol_id = symbol_row.id
            existing.provider_item_id = existing.provider_item_id or item.provider_item_id
            existing.source = source or item.provider
            existing.title = item.title
            existing.description = item.description
            existing.source_url = item.source_url
            existing.published_at = item.published_at or existing.published_at
            existing.collected_at = now
            existing.payload = payload
            updated += 1
    return {'inserted': inserted, 'updated': updated, 'skipped': skipped}


def list_external_news_items(
    *,
    symbols: Sequence[str] | None = None,
    provider: str | None = None,
    limit: int = 100,
    published_since: datetime | None = None,
) -> list[dict[str, Any]]:
    with session_scope() as session:
        stmt = (
            select(ExternalNewsItem, Symbol.symbol, Symbol.name_ko, Symbol.market)
            .join(Symbol, Symbol.id == ExternalNewsItem.symbol_id)
            .order_by(desc(ExternalNewsItem.published_at).nullslast(), desc(ExternalNewsItem.collected_at))
            .limit(limit)
        )
        if symbols:
            stmt = stmt.where(Symbol.symbol.in_(list(symbols)))
        if provider:
            stmt = stmt.where(ExternalNewsItem.provider == provider)
        if published_since:
            stmt = stmt.where(
                or_(ExternalNewsItem.published_at.is_(None), ExternalNewsItem.published_at >= published_since)
            )
        rows = session.execute(stmt).all()
        return [_serialize(row, symbol=symbol, name_ko=name_ko, market=market) for row, symbol, name_ko, market in rows]


def enrich_recent_external_news_classification(
    *,
    symbols: Sequence[str] | None = None,
    lookback_days: int = 14,
    limit: int = 1000,
) -> dict[str, Any]:
    since = _utcnow() - timedelta(days=lookback_days)
    inspected = 0
    updated = 0
    with session_scope() as session:
        stmt = (
            select(ExternalNewsItem)
            .join(Symbol, Symbol.id == ExternalNewsItem.symbol_id)
            .where(or_(ExternalNewsItem.published_at.is_(None), ExternalNewsItem.published_at >= since))
            .order_by(desc(ExternalNewsItem.published_at).nullslast(), desc(ExternalNewsItem.collected_at))
            .limit(limit)
        )
        if symbols:
            stmt = stmt.where(Symbol.symbol.in_(list(symbols)))
        for row in session.execute(stmt).scalars().all():
            inspected += 1
            payload = dict(row.payload or {})
            existing = payload.get('classification') if isinstance(payload.get('classification'), dict) else None
            if external_news_classification_has_contract(existing):
                continue
            payload['classification'] = classify_external_news_item(
                title=row.title,
                description=row.description,
                payload=payload,
                url=row.url,
                source_url=row.source_url,
                published_at=row.published_at,
            )
            row.payload = payload
            updated += 1
    return {
        'status': 'ok',
        'lookback_days': lookback_days,
        'symbols': list(symbols or []),
        'items_inspected': inspected,
        'items_updated': updated,
    }


def list_recent_external_news_for_symbols(
    *,
    symbols: Sequence[str],
    limit_per_symbol: int = 3,
    lookback_days: int = 14,
) -> dict[str, list[dict[str, Any]]]:
    since = _utcnow() - timedelta(days=lookback_days)
    output = {symbol: [] for symbol in symbols}
    if not symbols:
        return output
    candidate_limit_per_symbol = max(limit_per_symbol * 4, limit_per_symbol)
    rows = list_external_news_items(
        symbols=symbols,
        limit=max(len(symbols) * candidate_limit_per_symbol * 2, 100),
        published_since=since,
    )
    for row in rows:
        bucket = output.setdefault(row['symbol'], [])
        if len(bucket) < candidate_limit_per_symbol:
            bucket.append(row)
    for symbol, bucket in output.items():
        output[symbol] = sorted(bucket, key=external_news_materiality_sort_key)[:limit_per_symbol]
    return output
