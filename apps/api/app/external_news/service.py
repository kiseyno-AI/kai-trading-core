from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Sequence
from zoneinfo import ZoneInfo

from app.agent_context.eligibility import load_agent_feedback_eligible_symbols
from app.core.audit_logging import exception_message
from app.core.settings import settings
from app.external_news.provider import ExternalNewsProvider, NaverNewsSearchProvider
from app.external_news.store import enrich_recent_external_news_classification, list_external_news_items, persist_external_news_items
from app.market_data_store import load_symbols


logger = logging.getLogger(__name__)
SEOUL_TZ = ZoneInfo('Asia/Seoul')
EXTERNAL_NEWS_PROVIDER_CIRCUIT_FAILURE_THRESHOLD = 2


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _association_window_for_market_date(
    market_date: str | date,
    *,
    captured_at: datetime,
) -> tuple[datetime, datetime]:
    normalized_date = (
        market_date
        if isinstance(market_date, date)
        else date.fromisoformat(str(market_date))
    )
    captured_utc = captured_at.astimezone(timezone.utc)
    captured_seoul_date = captured_utc.astimezone(SEOUL_TZ).date()
    if normalized_date > captured_seoul_date:
        raise ValueError('market_date must not be later than the current Seoul date')
    start = datetime.combine(normalized_date, time.min, tzinfo=SEOUL_TZ).astimezone(
        timezone.utc
    )
    next_midnight = (
        datetime.combine(
            normalized_date + timedelta(days=1),
            time.min,
            tzinfo=SEOUL_TZ,
        ).astimezone(timezone.utc)
    )
    end = (
        min(next_midnight, captured_utc)
        if normalized_date == captured_seoul_date
        else next_midnight
    )
    return start, end


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
            'collected_symbol_count': 0,
            'failed_symbol_count': 0,
            'skipped_symbol_count': 0,
            'provider_circuit_open': False,
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
        consecutive_provider_failures = 0
        provider_circuit_open = False
        for index, symbol in enumerate(requested_symbols):
            row = symbol_map.get(symbol)
            if row is None:
                summary['items_skipped'] += 1
                summary['skipped_symbol_count'] += 1
                items_out.append({'symbol': symbol, 'status': 'skipped', 'reason': 'symbol_not_found'})
                continue
            if provider_circuit_open:
                summary['items_skipped'] += 1
                summary['skipped_symbol_count'] += 1
                items_out.append(
                    {
                        'symbol': symbol,
                        'name_ko': row.get('name_ko'),
                        'status': 'skipped',
                        'reason': 'provider_circuit_open',
                    }
                )
                continue
            if index > 0 and throttle_seconds > 0:
                await asyncio.sleep(throttle_seconds)
            query = f"{row.get('name_ko') or symbol} {symbol}"
            try:
                fetched = await self.provider.search_news(
                    query=query,
                    display=per_symbol_limit,
                )
            except Exception as exc:
                consecutive_provider_failures += 1
                summary['failed_symbol_count'] += 1
                provider_circuit_open = (
                    consecutive_provider_failures
                    >= EXTERNAL_NEWS_PROVIDER_CIRCUIT_FAILURE_THRESHOLD
                )
                summary['provider_circuit_open'] = provider_circuit_open
                logger.warning(
                    'External news provider failed for one symbol.',
                    extra={
                        'provider': self.provider.provider_name,
                        'symbol': symbol,
                        'error_class': type(exc).__name__,
                        'consecutive_provider_failures': consecutive_provider_failures,
                        'provider_circuit_open': provider_circuit_open,
                    },
                )
                items_out.append(
                    {
                        'symbol': symbol,
                        'name_ko': row.get('name_ko'),
                        'status': 'failed',
                        'reason': 'provider_search_failed',
                        'query': query,
                        'error_type': type(exc).__name__,
                        'error_message': exception_message(exc),
                    }
                )
                continue

            consecutive_provider_failures = 0
            try:
                persist_result = persist_external_news_items(
                    symbol=symbol,
                    items=fetched,
                    source=self.provider.provider_name,
                )
            except Exception as exc:
                summary['failed_symbol_count'] += 1
                logger.warning(
                    'External news persistence failed for one symbol.',
                    extra={
                        'provider': self.provider.provider_name,
                        'symbol': symbol,
                        'error_class': type(exc).__name__,
                    },
                )
                items_out.append(
                    {
                        'symbol': symbol,
                        'name_ko': row.get('name_ko'),
                        'status': 'failed',
                        'reason': 'persistence_failed',
                        'query': query,
                        'fetched_count': len(fetched),
                        'error_type': type(exc).__name__,
                        'error_message': exception_message(exc),
                    }
                )
                continue

            summary['collected_symbol_count'] += 1
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
        status = (
            'partial'
            if summary['failed_symbol_count'] or summary['skipped_symbol_count']
            else 'ok'
        )
        result = {
            'status': status,
            'provider': provider_status.as_dict(),
            'collected_at': datetime.now(timezone.utc).isoformat(),
            'requested_symbols': requested_symbols,
            'summary': summary,
            'items': items_out,
        }
        if status == 'partial':
            result['reason'] = 'external_news_collection_partial'
        return result

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
        result['summary'].setdefault(
            'collected_symbol_count',
            max(
                len(eligible_symbols)
                - int(result['summary'].get('failed_symbol_count') or 0)
                - int(result['summary'].get('skipped_symbol_count') or 0),
                0,
            ),
        )
        return result

    def enrich_recent_classification(
        self,
        *,
        symbols: Sequence[str] | str | None = None,
        market_date: str | date | None = None,
        lookback_days: int | None = None,
        limit: int = 1000,
    ) -> dict[str, Any]:
        normalized_symbols = _normalize_symbols(symbols)
        association_start = None
        association_end = None
        normalized_market_date = None
        if market_date is not None:
            normalized_market_date = (
                market_date
                if isinstance(market_date, date)
                else date.fromisoformat(str(market_date))
            )
            association_start, association_end = _association_window_for_market_date(
                normalized_market_date,
                captured_at=_utcnow(),
            )
        provider_status = self.provider_status()
        resolved_lookback_days = (
            lookback_days or settings.external_news_context_lookback_days
        )
        try:
            result = enrich_recent_external_news_classification(
                symbols=normalized_symbols or None,
                lookback_days=resolved_lookback_days,
                limit=limit,
                association_start=association_start,
                association_end=association_end,
            )
        except Exception as exc:
            logger.error(
                'External news classification enrichment failed.',
                extra={
                    'market_date': (
                        normalized_market_date.isoformat()
                        if normalized_market_date is not None
                        else None
                    ),
                    'requested_symbol_count': len(normalized_symbols),
                    'lookback_days': resolved_lookback_days,
                    'limit': int(limit),
                    'error_class': exc.__class__.__name__,
                },
            )
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
