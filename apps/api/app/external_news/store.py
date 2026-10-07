from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, Sequence

from sqlalchemy import and_, desc, exists, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.db import session_scope
from app.db_models import (
    ExternalNewsItem,
    ExternalNewsItemClassification,
    ExternalNewsItemSymbol,
    Symbol,
)
from app.external_news.classifier import (
    CLASSIFIER_VERSION,
    classify_external_news_item,
    external_news_classification_has_contract,
    external_news_classification_has_schema_contract,
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


def _dedupe_provider_news_items(items: Sequence[ProviderNewsItem]) -> tuple[list[ProviderNewsItem], int]:
    deduped: list[ProviderNewsItem] = []
    skipped = 0
    seen_keys: set[tuple[str, str]] = set()
    seen_urls: set[tuple[str, str]] = set()
    for item in items:
        if not item.url:
            skipped += 1
            continue
        provider_key = (item.provider, item.provider_item_id) if item.provider_item_id else None
        url_key = (item.provider, item.url)
        if provider_key is not None and provider_key in seen_keys:
            skipped += 1
            continue
        if url_key in seen_urls:
            skipped += 1
            continue
        if provider_key is not None:
            seen_keys.add(provider_key)
        seen_urls.add(url_key)
        deduped.append(item)
    return deduped, skipped


def _serialize(
    row: ExternalNewsItem,
    *,
    symbol: str,
    name_ko: str | None,
    market: str | None,
    associated_at: datetime | None = None,
    classification_observation: ExternalNewsItemClassification | None = None,
) -> dict[str, Any]:
    payload = dict(row.payload) if isinstance(row.payload, dict) else {}
    # Older code wrote classifier output into the article payload without an
    # independent observation timestamp. Never expose that field as PIT-safe
    # evidence; only the append-only sidecar can establish when it was known.
    serialized_payload = dict(payload)
    serialized_payload.pop('classification', None)
    sidecar_payload = (
        classification_observation.classification_payload
        if classification_observation is not None
        and isinstance(classification_observation.classification_payload, dict)
        else {}
    )
    sidecar_classification = (
        sidecar_payload
        if classification_observation is not None
        and sidecar_payload.get('classifier')
        == classification_observation.classifier_version
        and external_news_classification_has_schema_contract(sidecar_payload)
        else {}
    )
    classification = sidecar_classification
    if sidecar_classification:
        classification_provenance = {
            'source': 'append_only_sidecar',
            'classifier_version': classification_observation.classifier_version,
            'classified_at': (
                classification_observation.classified_at.isoformat()
                if classification_observation.classified_at
                else None
            ),
        }
    else:
        classification_provenance = {
            'source': 'unavailable',
            'classifier_version': None,
            'classified_at': None,
        }
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
        'associated_at': associated_at.isoformat() if associated_at else None,
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
        'classification_provenance': classification_provenance,
        'payload': serialized_payload,
    }


def _get_symbol(session, symbol_code: str) -> Symbol | None:
    return session.execute(select(Symbol).where(Symbol.symbol == symbol_code)).scalar_one_or_none()


def _find_existing_news_item(session, item: ProviderNewsItem) -> ExternalNewsItem | None:
    provider_item_match: ExternalNewsItem | None = None
    if item.provider_item_id:
        provider_item_match = session.execute(
            select(ExternalNewsItem).where(
                ExternalNewsItem.provider == item.provider,
                ExternalNewsItem.provider_item_id == item.provider_item_id,
            )
        ).scalar_one_or_none()
    url_match = session.execute(
        select(ExternalNewsItem).where(
            ExternalNewsItem.provider == item.provider,
            ExternalNewsItem.url == item.url,
        )
    ).scalar_one_or_none()
    if (
        provider_item_match is not None
        and url_match is not None
        and provider_item_match.id != url_match.id
    ):
        raise RuntimeError(
            'External news provider item id and URL resolve to different immutable observations.'
        )
    return provider_item_match or url_match


def _dialect_insert(model, *, dialect_name: str):
    if dialect_name == 'sqlite':
        return sqlite_insert(model)
    return pg_insert(model)


def _session_dialect_name(session) -> str:
    try:
        bind = session.get_bind()
    except (AttributeError, TypeError):
        return 'postgresql'
    return str(getattr(getattr(bind, 'dialect', None), 'name', None) or 'postgresql')


def _build_external_news_insert_statement(
    values: dict[str, Any],
    *,
    dialect_name: str = 'postgresql',
):
    # No conflict target is intentional: either the provider/url or the
    # provider/provider_item_id uniqueness contract may identify a replay.
    return (
        _dialect_insert(ExternalNewsItem, dialect_name=dialect_name)
        .values(**values)
        .on_conflict_do_nothing()
        .returning(ExternalNewsItem.id)
    )


def _build_external_news_symbol_insert_statement(
    values: dict[str, Any],
    *,
    dialect_name: str = 'postgresql',
):
    return (
        _dialect_insert(ExternalNewsItemSymbol, dialect_name=dialect_name)
        .values(**values)
        .on_conflict_do_nothing(
            index_elements=[
                ExternalNewsItemSymbol.external_news_item_id,
                ExternalNewsItemSymbol.symbol_id,
            ]
        )
    )


def _build_external_news_classification_insert_statement(
    values: dict[str, Any],
    *,
    dialect_name: str = 'postgresql',
):
    return (
        _dialect_insert(ExternalNewsItemClassification, dialect_name=dialect_name)
        .values(**values)
        .on_conflict_do_nothing(
            index_elements=[
                ExternalNewsItemClassification.external_news_item_id,
                ExternalNewsItemClassification.classifier_version,
            ]
        )
        .returning(ExternalNewsItemClassification.id)
    )


def _persist_external_news_symbol_association(
    session,
    *,
    external_news_item_id: int,
    symbol_id: int,
    associated_at: datetime,
    dialect_name: str,
) -> None:
    session.execute(
        _build_external_news_symbol_insert_statement(
            {
                'external_news_item_id': external_news_item_id,
                'symbol_id': symbol_id,
                'associated_at': associated_at,
            },
            dialect_name=dialect_name,
        )
    )


def _persist_external_news_classification(
    session,
    *,
    external_news_item_id: int,
    classification: dict[str, Any],
    classified_at: datetime,
    dialect_name: str,
) -> bool:
    classifier_version = str(classification.get('classifier') or '').strip()
    if not classifier_version or not external_news_classification_has_contract(classification):
        raise ValueError('External news classification must satisfy the current immutable contract.')
    inserted_id = session.execute(
        _build_external_news_classification_insert_statement(
            {
                'external_news_item_id': external_news_item_id,
                'classifier_version': classifier_version,
                'classified_at': classified_at,
                'classification_payload': dict(classification),
            },
            dialect_name=dialect_name,
        )
    ).scalar_one_or_none()
    return inserted_id is not None


def _load_latest_external_news_classifications(
    session,
    *,
    external_news_item_ids: Sequence[int],
    classified_before: datetime | None,
) -> dict[int, ExternalNewsItemClassification]:
    if not external_news_item_ids:
        return {}
    stmt = (
        select(ExternalNewsItemClassification)
        .where(
            ExternalNewsItemClassification.external_news_item_id.in_(
                list(dict.fromkeys(external_news_item_ids))
            )
        )
        .order_by(
            ExternalNewsItemClassification.external_news_item_id,
            desc(ExternalNewsItemClassification.classified_at),
            desc(ExternalNewsItemClassification.id),
        )
    )
    if classified_before is not None:
        stmt = stmt.where(
            ExternalNewsItemClassification.classified_at < classified_before
        )
    latest: dict[int, ExternalNewsItemClassification] = {}
    for observation in session.execute(stmt).scalars().all():
        latest.setdefault(int(observation.external_news_item_id), observation)
    return latest


def persist_external_news_items(*, symbol: str, items: Sequence[ProviderNewsItem], source: str | None = None) -> dict[str, Any]:
    now = _utcnow()
    inserted = 0
    updated = 0
    deduped_items, skipped = _dedupe_provider_news_items(items)
    with session_scope() as session:
        symbol_row = _get_symbol(session, symbol)
        if symbol_row is None:
            return {'inserted': 0, 'updated': 0, 'skipped': len(items), 'reason': 'symbol_not_found'}
        dialect_name = _session_dialect_name(session)

        for item in deduped_items:
            payload = _payload_with_classification(item)
            values = {
                'symbol_id': symbol_row.id,
                'provider': item.provider,
                'provider_item_id': item.provider_item_id,
                'source': source or item.provider,
                'title': item.title,
                'description': item.description,
                'url': item.url,
                'source_url': item.source_url,
                'published_at': item.published_at,
                'collected_at': now,
                'payload': payload,
            }
            inserted_id = session.execute(
                _build_external_news_insert_statement(values, dialect_name=dialect_name)
            ).scalar_one_or_none()
            if inserted_id is not None:
                inserted += 1
                news_item_id = int(inserted_id)
                _persist_external_news_classification(
                    session,
                    external_news_item_id=news_item_id,
                    classification=_classification_from_payload(payload),
                    classified_at=now,
                    dialect_name=dialect_name,
                )
            else:
                # ON CONFLICT waits for a concurrent writer, if any. This
                # lookup is safe for both sequential replays and collection
                # races, while the article observation remains immutable.
                existing = _find_existing_news_item(session, item)
                if existing is None:
                    raise RuntimeError('External news conflict row could not be reloaded after idempotent insert.')
                news_item_id = int(existing.id)
                skipped += 1
            # A later symbol search creates only a timestamped attribution.
            # Replaying the same article/symbol pair leaves its first-known
            # association timestamp unchanged.
            _persist_external_news_symbol_association(
                session,
                external_news_item_id=news_item_id,
                symbol_id=int(symbol_row.id),
                associated_at=now,
                dialect_name=dialect_name,
            )
    return {'inserted': inserted, 'updated': updated, 'skipped': skipped}


def list_external_news_items(
    *,
    symbols: Sequence[str] | None = None,
    provider: str | None = None,
    limit: int = 100,
    published_since: datetime | None = None,
    published_before: datetime | None = None,
    collected_before: datetime | None = None,
    observed_after: datetime | None = None,
) -> list[dict[str, Any]]:
    with session_scope() as session:
        stmt = (
            select(
                ExternalNewsItem,
                Symbol.symbol,
                Symbol.name_ko,
                Symbol.market,
                ExternalNewsItemSymbol.associated_at,
            )
            .join(
                ExternalNewsItemSymbol,
                ExternalNewsItemSymbol.external_news_item_id == ExternalNewsItem.id,
            )
            .join(Symbol, Symbol.id == ExternalNewsItemSymbol.symbol_id)
            .order_by(
                desc(ExternalNewsItem.published_at).nullslast(),
                desc(ExternalNewsItem.collected_at),
                desc(ExternalNewsItemSymbol.associated_at),
            )
            .limit(limit)
        )
        if symbols:
            stmt = stmt.where(Symbol.symbol.in_(list(symbols)))
        if provider:
            stmt = stmt.where(ExternalNewsItem.provider == provider)
        if published_since:
            stmt = stmt.where(
                or_(
                    ExternalNewsItem.published_at >= published_since,
                    and_(
                        ExternalNewsItem.published_at.is_(None),
                        ExternalNewsItem.collected_at >= published_since,
                    ),
                )
            )
        if published_before:
            stmt = stmt.where(
                or_(ExternalNewsItem.published_at.is_(None), ExternalNewsItem.published_at < published_before)
            )
        if collected_before:
            stmt = stmt.where(
                ExternalNewsItem.collected_at < collected_before,
                ExternalNewsItemSymbol.associated_at < collected_before,
            )
        if observed_after:
            classification_window = [
                ExternalNewsItemClassification.external_news_item_id
                == ExternalNewsItem.id,
                ExternalNewsItemClassification.classified_at
                >= observed_after,
            ]
            if collected_before:
                classification_window.append(
                    ExternalNewsItemClassification.classified_at
                    < collected_before
                )
            stmt = stmt.where(
                or_(
                    ExternalNewsItem.published_at >= observed_after,
                    ExternalNewsItem.collected_at >= observed_after,
                    ExternalNewsItemSymbol.associated_at >= observed_after,
                    exists(
                        select(ExternalNewsItemClassification.id).where(
                            *classification_window
                        )
                    ),
                )
            )
        rows = session.execute(stmt).all()
        classifications = _load_latest_external_news_classifications(
            session,
            external_news_item_ids=[int(row.id) for row, *_ in rows],
            classified_before=collected_before,
        )
        return [
            _serialize(
                row,
                symbol=symbol,
                name_ko=name_ko,
                market=market,
                associated_at=associated_at,
                classification_observation=classifications.get(int(row.id)),
            )
            for row, symbol, name_ko, market, associated_at in rows
        ]


def _build_external_news_enrichment_candidate_statement(
    *,
    symbols: Sequence[str] | None,
    since: datetime,
    association_start: datetime | None = None,
    association_end: datetime | None = None,
):
    if (association_start is None) != (association_end is None):
        raise ValueError('association_start and association_end must be provided together')
    if (
        association_start is not None
        and association_end is not None
        and association_end < association_start
    ):
        raise ValueError('association_end must not precede association_start')

    resolved_symbols = list(dict.fromkeys(symbols or []))
    current_version_exists = (
        select(ExternalNewsItemClassification.id)
        .where(
            ExternalNewsItemClassification.external_news_item_id
            == ExternalNewsItem.id,
            ExternalNewsItemClassification.classifier_version
            == CLASSIFIER_VERSION,
        )
        .exists()
    )
    article_is_recent = or_(
        ExternalNewsItem.published_at >= since,
        and_(
            ExternalNewsItem.published_at.is_(None),
            ExternalNewsItem.collected_at >= since,
        ),
    )

    def _association_exists(*, within_window: bool):
        association_stmt = select(
            ExternalNewsItemSymbol.external_news_item_id
        ).where(
            ExternalNewsItemSymbol.external_news_item_id == ExternalNewsItem.id,
        )
        if resolved_symbols:
            association_stmt = association_stmt.join(
                Symbol,
                Symbol.id == ExternalNewsItemSymbol.symbol_id,
            ).where(Symbol.symbol.in_(resolved_symbols))
        if within_window:
            association_stmt = association_stmt.where(
                ExternalNewsItemSymbol.associated_at >= association_start,
                ExternalNewsItemSymbol.associated_at < association_end,
            )
        return association_stmt.correlate(ExternalNewsItem).exists()

    association_window_exists = None
    eligibility = [article_is_recent]
    if association_start is not None:
        association_window_exists = _association_exists(within_window=True)
        eligibility.append(association_window_exists)
    stmt = select(ExternalNewsItem).where(
        ~current_version_exists,
        or_(*eligibility),
    )
    if resolved_symbols:
        stmt = stmt.where(_association_exists(within_window=False))
    ordering = []
    if association_window_exists is not None:
        ordering.append(desc(association_window_exists))
    ordering.extend(
        (
            desc(ExternalNewsItem.published_at).nullslast(),
            desc(ExternalNewsItem.collected_at),
        )
    )
    return stmt.order_by(*ordering)


def enrich_recent_external_news_classification(
    *,
    symbols: Sequence[str] | None = None,
    lookback_days: int = 14,
    limit: int = 1000,
    association_start: datetime | None = None,
    association_end: datetime | None = None,
) -> dict[str, Any]:
    now = _utcnow()
    since = now - timedelta(days=lookback_days)
    resolved_limit = max(1, int(limit))
    inspected = 0
    inserted_versions = 0
    failed_item_ids: list[int] = []
    with session_scope() as session:
        stmt = _build_external_news_enrichment_candidate_statement(
            symbols=symbols,
            since=since,
            association_start=association_start,
            association_end=association_end,
        )
        candidate_rows = session.execute(stmt.limit(resolved_limit + 1)).scalars().all()
        has_more = len(candidate_rows) > resolved_limit
        rows = candidate_rows[:resolved_limit]
        dialect_name = _session_dialect_name(session)
        for row in rows:
            inspected += 1
            payload = dict(row.payload) if isinstance(row.payload, dict) else {}
            existing = payload.get('classification') if isinstance(payload.get('classification'), dict) else None
            try:
                classification = (
                    dict(existing)
                    if external_news_classification_has_contract(existing)
                    else classify_external_news_item(
                        title=row.title,
                        description=row.description,
                        payload=payload,
                        url=row.url,
                        source_url=row.source_url,
                        published_at=row.published_at,
                    )
                )
                inserted_versions += int(
                    _persist_external_news_classification(
                        session,
                        external_news_item_id=int(row.id),
                        classification=classification,
                        classified_at=now,
                        dialect_name=dialect_name,
                    )
                )
            except (TypeError, ValueError):
                failed_item_ids.append(int(row.id))
    missing_count = len(failed_item_ids) + int(has_more)
    return {
        'status': 'partial' if missing_count else 'completed',
        'reason': (
            'external_news_classification_append_failed'
            if failed_item_ids
            else 'external_news_classification_batch_limit_reached'
            if has_more
            else None
        ),
        'lookback_days': lookback_days,
        'symbols': list(symbols or []),
        'items_inspected': inspected,
        'items_updated': inserted_versions,
        'classification_versions_inserted': inserted_versions,
        'items_missing_classification': missing_count,
        'items_remaining_at_least': int(has_more),
        'missing_item_ids': failed_item_ids[:50],
        'mutation_policy': 'append_only_classification_versions',
    }


def list_recent_external_news_for_symbols(
    *,
    symbols: Sequence[str],
    limit_per_symbol: int = 3,
    lookback_days: int = 14,
    published_before: datetime | None = None,
    collected_before: datetime | None = None,
) -> dict[str, list[dict[str, Any]]]:
    since = (published_before or _utcnow()) - timedelta(days=lookback_days)
    output = {symbol: [] for symbol in symbols}
    if not symbols:
        return output
    candidate_limit_per_symbol = max(limit_per_symbol * 4, limit_per_symbol)
    for symbol in symbols:
        output[symbol] = list_external_news_items(
            symbols=[symbol],
            limit=candidate_limit_per_symbol,
            published_since=since,
            published_before=published_before,
            collected_before=collected_before,
        )
    for symbol, bucket in output.items():
        output[symbol] = sorted(bucket, key=external_news_materiality_sort_key)[:limit_per_symbol]
    return output
