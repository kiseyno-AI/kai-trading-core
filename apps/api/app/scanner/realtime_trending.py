from __future__ import annotations

import copy
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from app.brokers.kis.provider import kis_client
from app.core.settings import settings

logger = logging.getLogger(__name__)

KIS_REALTIME_TRENDING_SOURCES = ('volume-rank', 'fluctuation', 'hts-top-view')
VALID_MARKET_CODES = {'J': 'KRX', 'NX': 'NXT'}

SOURCE_CONFIGS = {
    'volume-rank': {
        'label': 'volume-rank',
        'output_key': 'output',
        'metric_name': 'volume',
        'metric_unit': 'shares',
    },
    'fluctuation': {
        'label': 'fluctuation',
        'output_key': 'output',
        'metric_name': 'change_pct',
        'metric_unit': 'percent',
    },
    'hts-top-view': {
        'label': 'hts-top-view',
        'output_key': 'output1',
        'metric_name': 'view_rank',
        'metric_unit': 'rank',
    },
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat(value: datetime) -> str:
    return value.isoformat().replace('+00:00', 'Z')


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_symbol(value: Any) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None
    if text.isdigit() and len(text) < 6:
        return text.zfill(6)
    return text


def _parse_number(value: Any, *, as_int: bool = False) -> int | float | None:
    text = _clean_text(value)
    if text is None:
        return None
    normalized = text.replace(',', '').replace('%', '')
    try:
        parsed = float(normalized)
    except ValueError:
        return None
    if as_int:
        return int(parsed)
    return parsed


def _signed_number(value: Any, sign_code: Any, *, as_int: bool = False) -> int | float | None:
    parsed = _parse_number(value, as_int=as_int)
    if parsed is None:
        return None
    sign = _clean_text(sign_code)
    if sign in {'4', '5'} and parsed > 0:
        parsed = -parsed
    elif sign in {'1', '2'} and parsed < 0:
        parsed = abs(parsed)
    elif sign == '3':
        parsed = 0
    return parsed


def _first_value(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if _clean_text(value) is not None:
            return value
    return None


def parse_source_ids(sources: str | list[str] | tuple[str, ...] | None) -> list[str]:
    if sources is None:
        requested = list(KIS_REALTIME_TRENDING_SOURCES)
    elif isinstance(sources, str):
        requested = [part.strip() for part in sources.split(',')]
    else:
        requested = [str(source).strip() for source in sources]

    normalized: list[str] = []
    for source in requested:
        if not source:
            continue
        if source not in SOURCE_CONFIGS:
            allowed = ', '.join(KIS_REALTIME_TRENDING_SOURCES)
            raise ValueError(f'sources must contain only: {allowed}')
        if source not in normalized:
            normalized.append(source)

    return normalized or list(KIS_REALTIME_TRENDING_SOURCES)


@dataclass
class _CacheEntry:
    expires_at: datetime
    payload: dict[str, Any]


class RealtimeTrendingCache:
    def __init__(self) -> None:
        self._entries: dict[str, _CacheEntry] = {}

    def get(self, key: str, now: datetime) -> dict[str, Any] | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        if now >= entry.expires_at:
            self._entries.pop(key, None)
            return None
        return copy.deepcopy(entry.payload)

    def set(self, key: str, payload: dict[str, Any], now: datetime, ttl_seconds: int) -> None:
        if ttl_seconds <= 0:
            return
        self._entries[key] = _CacheEntry(
            expires_at=now + timedelta(seconds=ttl_seconds),
            payload=copy.deepcopy(payload),
        )

    def clear(self) -> None:
        self._entries.clear()


class RealtimeTrendingService:
    def __init__(self, client: Any = None, cache_ttl_seconds: int | None = None) -> None:
        self.client = client or kis_client
        self.cache_ttl_seconds = (
            settings.kis_realtime_trending_cache_ttl_seconds
            if cache_ttl_seconds is None
            else cache_ttl_seconds
        )
        self.cache = RealtimeTrendingCache()

    def _cache_key(self, *, market_code: str, limit: int, sources: list[str]) -> str:
        return f'{market_code}:{limit}:{",".join(sources)}'

    def _extract_rows(self, source_id: str, raw: dict[str, Any]) -> list[dict[str, Any]]:
        output_key = SOURCE_CONFIGS[source_id]['output_key']
        rows = raw.get(output_key, []) if isinstance(raw, dict) else []
        if isinstance(rows, dict):
            rows = [rows]
        if not isinstance(rows, list):
            return []
        return [row for row in rows if isinstance(row, dict)]

    def _normalize_source_item(
        self,
        *,
        source_id: str,
        row: dict[str, Any],
        index: int,
        market_code: str,
    ) -> dict[str, Any] | None:
        symbol = _normalize_symbol(_first_value(row, 'mksc_shrn_iscd', 'stck_shrn_iscd'))
        if symbol is None:
            return None

        config = SOURCE_CONFIGS[source_id]
        sign_code = row.get('prdy_vrss_sign')
        rank = _parse_number(row.get('data_rank'), as_int=True) or index + 1
        price = _parse_number(row.get('stck_prpr'), as_int=True)
        change = _signed_number(row.get('prdy_vrss'), sign_code, as_int=True)
        change_pct = _signed_number(row.get('prdy_ctrt'), sign_code)
        volume = _parse_number(row.get('acml_vol'), as_int=True)
        turnover = _parse_number(row.get('acml_tr_pbmn'), as_int=True)
        source_market_code = _clean_text(row.get('mrkt_div_cls_code')) or market_code

        metric_name = str(config['metric_name'])
        if metric_name == 'volume':
            metric_value: int | float | None = volume
        elif metric_name == 'change_pct':
            metric_value = change_pct
        else:
            metric_value = rank

        return {
            'source': source_id,
            'source_label': config['label'],
            'rank': rank,
            'symbol': symbol,
            'name': _clean_text(row.get('hts_kor_isnm')) or symbol,
            'market_code': source_market_code,
            'market': VALID_MARKET_CODES.get(market_code, market_code),
            'price': price,
            'change': change,
            'change_pct': change_pct,
            'volume': volume,
            'turnover': turnover,
            'metric': {
                'name': metric_name,
                'value': metric_value,
                'unit': config['metric_unit'],
            },
        }

    async def _fetch_source(self, source_id: str, market_code: str, limit: int) -> dict[str, Any]:
        try:
            if source_id == 'volume-rank':
                raw = await self.client.fetch_domestic_volume_rank(market_code=market_code)
            elif source_id == 'fluctuation':
                raw = await self.client.fetch_domestic_fluctuation_rank(market_code=market_code, count=limit)
            elif source_id == 'hts-top-view':
                raw = await self.client.fetch_domestic_hts_top_view()
            else:
                raise ValueError(f'unsupported source: {source_id}')
        except Exception as exc:
            logger.warning('KIS realtime trending source failed for %s: %s', source_id, exc)
            return {
                'source': source_id,
                'source_label': SOURCE_CONFIGS[source_id]['label'],
                'status': 'error',
                'count': 0,
                'error': 'kis_source_request_failed',
                'items': [],
            }

        items = []
        for index, row in enumerate(self._extract_rows(source_id, raw)[:limit]):
            item = self._normalize_source_item(
                source_id=source_id,
                row=row,
                index=index,
                market_code=market_code,
            )
            if item is not None:
                items.append(item)

        return {
            'source': source_id,
            'source_label': SOURCE_CONFIGS[source_id]['label'],
            'status': 'ok',
            'count': len(items),
            'items': items,
        }

    def _merge_source_items(self, buckets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        merged_by_symbol: dict[str, dict[str, Any]] = {}
        for bucket in buckets:
            for row in bucket.get('items', []):
                symbol = row['symbol']
                item = merged_by_symbol.get(symbol)
                if item is None:
                    item = {
                        'symbol': symbol,
                        'name': row.get('name') or symbol,
                        'market_code': row.get('market_code'),
                        'market': row.get('market'),
                        'price': row.get('price'),
                        'change': row.get('change'),
                        'change_pct': row.get('change_pct'),
                        'volume': row.get('volume'),
                        'turnover': row.get('turnover'),
                        'best_rank': row.get('rank'),
                        'source_count': 0,
                        'sources': [],
                    }
                    merged_by_symbol[symbol] = item

                for field in ('price', 'change', 'change_pct', 'volume', 'turnover'):
                    if item.get(field) is None and row.get(field) is not None:
                        item[field] = row[field]

                rank = row.get('rank')
                if rank is not None and (item.get('best_rank') is None or rank < item['best_rank']):
                    item['best_rank'] = rank

                item['sources'].append(
                    {
                        'source': row['source'],
                        'source_label': row['source_label'],
                        'rank': row['rank'],
                        'metric': row['metric'],
                        'market_code': row['market_code'],
                        'market': row['market'],
                    }
                )
                item['source_count'] = len(item['sources'])
        return list(merged_by_symbol.values())

    async def get_realtime_trending(
        self,
        *,
        limit: int = 20,
        market_code: str = 'J',
        sources: str | list[str] | tuple[str, ...] | None = None,
    ) -> dict[str, Any]:
        if not 1 <= limit <= 50:
            raise ValueError('limit must be between 1 and 50')

        normalized_market_code = market_code.upper()
        if normalized_market_code not in VALID_MARKET_CODES:
            allowed = ', '.join(VALID_MARKET_CODES)
            raise ValueError(f'market_code must be one of: {allowed}')

        source_ids = parse_source_ids(sources)
        now = _utc_now()
        cache_key = self._cache_key(
            market_code=normalized_market_code,
            limit=limit,
            sources=source_ids,
        )
        cached = self.cache.get(cache_key, now)
        if cached is not None:
            cached['cache']['hit'] = True
            return cached

        buckets = []
        for source_id in source_ids:
            buckets.append(await self._fetch_source(source_id, normalized_market_code, limit))

        items = self._merge_source_items(buckets)
        ok_source_count = sum(1 for bucket in buckets if bucket['status'] == 'ok')
        error_source_count = len(buckets) - ok_source_count
        status = 'ok'
        if error_source_count and ok_source_count:
            status = 'partial'
        elif error_source_count:
            status = 'unavailable'

        generated_at = _isoformat(now)
        payload = {
            'status': status,
            'source': 'kis',
            'as_of': generated_at,
            'market_code': normalized_market_code,
            'market': VALID_MARKET_CODES[normalized_market_code],
            'limit_per_source': limit,
            'sources': buckets,
            'items': items,
            'summary': {
                'source_count': len(buckets),
                'ok_source_count': ok_source_count,
                'error_source_count': error_source_count,
                'item_count': len(items),
            },
            'cache': {
                'hit': False,
                'ttl_seconds': self.cache_ttl_seconds,
                'cached_at': generated_at,
            },
        }
        self.cache.set(cache_key, payload, now, self.cache_ttl_seconds)
        return payload


realtime_trending_service = RealtimeTrendingService()
