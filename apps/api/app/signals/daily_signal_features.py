from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable, Sequence

from app.agent_context.derived_contexts import daily_market_participation_context_service
from app.brokers.kis.provider import kis_client
from app.market_data_store import load_daily_signal_features, upsert_daily_signal_feature

SIGNAL_FEATURE_VERSION = 'kis_ranking_v1'
SIGNAL_FEATURE_SOURCE = 'kis_ranking'

VOLUME_POWER = 'volume-power'
OVERTIME_VOLUME = 'overtime-volume'
OVERTIME_FLUCTUATION = 'overtime-fluctuation'
SUPPORTED_SIGNAL_SOURCES = (VOLUME_POWER, OVERTIME_VOLUME, OVERTIME_FLUCTUATION)


def _normalize_market_date(value: str | date) -> str:
    if isinstance(value, date):
        return value.isoformat()
    return date.fromisoformat(value).isoformat()


def _normalize_sources(sources: Sequence[str] | None = None) -> list[str]:
    if not sources:
        return list(SUPPORTED_SIGNAL_SOURCES)

    normalized: list[str] = []
    seen: set[str] = set()
    for item in sources:
        for piece in str(item or '').split(','):
            source = piece.strip().replace('_', '-')
            if not source:
                continue
            if source not in SUPPORTED_SIGNAL_SOURCES:
                allowed = ', '.join(SUPPORTED_SIGNAL_SOURCES)
                raise ValueError(f'Unsupported daily signal source: {source}. Allowed sources: {allowed}.')
            if source in seen:
                continue
            seen.add(source)
            normalized.append(source)
    return normalized


def _numeric_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().replace(',', '')
    if not text:
        return None
    return text


def _to_int(value: Any) -> int | None:
    text = _numeric_text(value)
    if text is None:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _to_float(value: Any) -> float | None:
    text = _numeric_text(value)
    if text is None:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _normalize_symbol(value: Any) -> str | None:
    symbol = str(value or '').strip()
    if not symbol:
        return None
    if symbol.isdigit():
        symbol = symbol.zfill(6)
    if len(symbol) != 6:
        return None
    return symbol


def _source_symbol(source: str, row: dict[str, Any]) -> str | None:
    if source == OVERTIME_FLUCTUATION:
        return _normalize_symbol(row.get('mksc_shrn_iscd') or row.get('stck_shrn_iscd'))
    return _normalize_symbol(row.get('stck_shrn_iscd') or row.get('mksc_shrn_iscd'))


def _base_feature(row: dict[str, Any], *, rank: int) -> dict[str, Any]:
    return {
        'rank': rank,
        'name_ko': row.get('hts_kor_isnm'),
        'raw': row,
    }


def _build_source_feature(source: str, row: dict[str, Any], *, rank: int) -> dict[str, Any]:
    feature = _base_feature(row, rank=rank)
    if source == VOLUME_POWER:
        feature.update(
            {
                'current_price': _to_int(row.get('stck_prpr')),
                'change': _to_int(row.get('prdy_vrss')),
                'change_sign': row.get('prdy_vrss_sign'),
                'change_pct': _to_float(row.get('prdy_ctrt')),
                'accumulated_volume': _to_int(row.get('acml_vol')),
                'volume_power': _to_float(row.get('tday_rltv')),
                'sell_execution_volume': _to_int(row.get('seln_cnqn_smtn')),
                'buy_execution_volume': _to_int(row.get('shnu_cnqn_smtn')),
            }
        )
    elif source == OVERTIME_VOLUME:
        feature.update(_build_overtime_feature(row))
    elif source == OVERTIME_FLUCTUATION:
        feature.update(_build_overtime_feature(row))
    return feature


def _build_overtime_feature(row: dict[str, Any]) -> dict[str, Any]:
    return {
        'overtime_price': _to_int(row.get('ovtm_untp_prpr')),
        'overtime_change': _to_int(row.get('ovtm_untp_prdy_vrss')),
        'overtime_change_sign': row.get('ovtm_untp_prdy_vrss_sign'),
        'overtime_change_pct': _to_float(row.get('ovtm_untp_prdy_ctrt')),
        'overtime_sell_remaining': _to_int(row.get('ovtm_untp_seln_rsqn')),
        'overtime_buy_remaining': _to_int(row.get('ovtm_untp_shnu_rsqn')),
        'overtime_volume': _to_int(row.get('ovtm_untp_vol')),
        'overtime_vs_accumulated_volume_ratio': _to_float(row.get('ovtm_vrss_acml_vol_rlim')),
        'regular_price': _to_int(row.get('stck_prpr')),
        'regular_accumulated_volume': _to_int(row.get('acml_vol')),
        'bid_price': _to_int(row.get('bidp') or row.get('ovtm_untp_bidp1')),
        'ask_price': _to_int(row.get('askp') or row.get('ovtm_untp_askp1')),
    }


def _empty_feature_row(
    *,
    symbol: str,
    market_date: str,
    source_counts: dict[str, int],
    collection: dict[str, Any],
    name_ko: str | None = None,
) -> dict[str, Any]:
    return {
        'symbol': symbol,
        'market_date': market_date,
        'source': SIGNAL_FEATURE_SOURCE,
        'name_ko': name_ko,
        'market': 'KRX',
        'signal_count': 0,
        'volume_power_rank': None,
        'volume_power': None,
        'overtime_volume_rank': None,
        'overtime_volume': None,
        'overtime_fluctuation_rank': None,
        'overtime_fluctuation_rate': None,
        'feature_payload': {
            'feature_version': SIGNAL_FEATURE_VERSION,
            'symbol': symbol,
            'market_date': market_date,
            'source': SIGNAL_FEATURE_SOURCE,
            'source_counts': source_counts,
            'collection': collection,
            'sources': {},
        },
    }


def _merge_source_feature(entry: dict[str, Any], *, source: str, feature: dict[str, Any]) -> None:
    sources = entry['feature_payload']['sources']
    current = sources.get(source)
    if current is not None and int(current.get('rank') or 0) <= int(feature['rank']):
        return

    sources[source] = feature
    entry['signal_count'] = len(sources)
    if feature.get('name_ko') and not entry.get('name_ko'):
        entry['name_ko'] = feature['name_ko']

    if source == VOLUME_POWER:
        entry['volume_power_rank'] = feature['rank']
        entry['volume_power'] = feature.get('volume_power')
    elif source == OVERTIME_VOLUME:
        entry['overtime_volume_rank'] = feature['rank']
        entry['overtime_volume'] = feature.get('overtime_volume')
    elif source == OVERTIME_FLUCTUATION:
        entry['overtime_fluctuation_rank'] = feature['rank']
        entry['overtime_fluctuation_rate'] = feature.get('overtime_change_pct')


def build_daily_signal_feature_rows(
    *,
    market_date: str | date,
    source_rows: dict[str, list[dict[str, Any]]],
    collection: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    normalized_market_date = _normalize_market_date(market_date)
    source_counts = {source: len(rows) for source, rows in source_rows.items()}
    collection_payload = collection or {}
    by_symbol: dict[str, dict[str, Any]] = {}

    for source in SUPPORTED_SIGNAL_SOURCES:
        for index, row in enumerate(source_rows.get(source, []), start=1):
            symbol = _source_symbol(source, row)
            if symbol is None:
                continue
            rank = _to_int(row.get('data_rank')) or index
            feature = _build_source_feature(source, row, rank=rank)
            entry = by_symbol.get(symbol)
            if entry is None:
                entry = _empty_feature_row(
                    symbol=symbol,
                    market_date=normalized_market_date,
                    source_counts=source_counts,
                    collection=collection_payload,
                    name_ko=feature.get('name_ko'),
                )
                by_symbol[symbol] = entry
            _merge_source_feature(entry, source=source, feature=feature)

    return sorted(
        by_symbol.values(),
        key=lambda item: (
            -int(item['signal_count']),
            item['volume_power_rank'] if item['volume_power_rank'] is not None else 999999,
            item['overtime_volume_rank'] if item['overtime_volume_rank'] is not None else 999999,
            item['overtime_fluctuation_rank'] if item['overtime_fluctuation_rank'] is not None else 999999,
            item['symbol'],
        ),
    )


@dataclass
class DailySignalFeatureCollection:
    market_date: str
    sources: list[str]
    source_counts: dict[str, int]
    items: list[dict[str, Any]]


class DailySignalFeatureService:
    def __init__(self, *, client=None) -> None:
        self.client = client or kis_client

    async def collect_features(
        self,
        *,
        market_date: str | date,
        sources: Sequence[str] | None = None,
        max_pages: int = 10,
        overtime_volume_rank_sort_cls_code: str = '2',
        overtime_fluctuation_div_cls_code: str = '2',
    ) -> DailySignalFeatureCollection:
        normalized_market_date = _normalize_market_date(market_date)
        selected_sources = _normalize_sources(sources)
        source_rows: dict[str, list[dict[str, Any]]] = {}
        collection = {
            'max_pages': max_pages,
            'overtime_volume_rank_sort_cls_code': overtime_volume_rank_sort_cls_code,
            'overtime_fluctuation_div_cls_code': overtime_fluctuation_div_cls_code,
        }

        if VOLUME_POWER in selected_sources:
            response = await self.client.fetch_domestic_ranking_volume_power(max_pages=max_pages)
            source_rows[VOLUME_POWER] = list(response.get('output') or [])
            collection[VOLUME_POWER] = {
                'page_count': response.get('page_count'),
                'max_pages_reached': response.get('max_pages_reached'),
            }

        if OVERTIME_VOLUME in selected_sources:
            response = await self.client.fetch_domestic_ranking_overtime_volume(
                rank_sort_cls_code=overtime_volume_rank_sort_cls_code,
                max_pages=max_pages,
            )
            source_rows[OVERTIME_VOLUME] = list(response.get('output2') or [])
            collection[OVERTIME_VOLUME] = {
                'page_count': response.get('page_count'),
                'max_pages_reached': response.get('max_pages_reached'),
                'summary_rows': response.get('output1') or [],
            }

        if OVERTIME_FLUCTUATION in selected_sources:
            response = await self.client.fetch_domestic_ranking_overtime_fluctuation(
                div_cls_code=overtime_fluctuation_div_cls_code,
                max_pages=max_pages,
            )
            source_rows[OVERTIME_FLUCTUATION] = list(response.get('output2') or [])
            collection[OVERTIME_FLUCTUATION] = {
                'page_count': response.get('page_count'),
                'max_pages_reached': response.get('max_pages_reached'),
                'summary_rows': response.get('output1') or [],
            }

        items = build_daily_signal_feature_rows(
            market_date=normalized_market_date,
            source_rows=source_rows,
            collection=collection,
        )
        return DailySignalFeatureCollection(
            market_date=normalized_market_date,
            sources=selected_sources,
            source_counts={source: len(source_rows.get(source, [])) for source in selected_sources},
            items=items,
        )

    async def refresh_for_market_date(
        self,
        *,
        market_date: str | date,
        sources: Sequence[str] | None = None,
        max_pages: int = 10,
        overtime_volume_rank_sort_cls_code: str = '2',
        overtime_fluctuation_div_cls_code: str = '2',
    ) -> dict[str, Any]:
        collection = await self.collect_features(
            market_date=market_date,
            sources=sources,
            max_pages=max_pages,
            overtime_volume_rank_sort_cls_code=overtime_volume_rank_sort_cls_code,
            overtime_fluctuation_div_cls_code=overtime_fluctuation_div_cls_code,
        )

        failed_symbols: list[str] = []
        persisted_items: list[dict[str, Any]] = []
        for item in collection.items:
            try:
                upsert_daily_signal_feature(item)
                persisted_items.append(item)
            except Exception:
                failed_symbols.append(item['symbol'])

        try:
            participation_context = daily_market_participation_context_service.rebuild_for_market_date(
                market_date=collection.market_date,
                market='KRX',
                source=SIGNAL_FEATURE_SOURCE,
            )
        except Exception as exc:
            participation_context = {
                'market': 'KRX',
                'market_date': collection.market_date,
                'source': SIGNAL_FEATURE_SOURCE,
                'error': str(exc),
            }

        return {
            'market_date': collection.market_date,
            'source': SIGNAL_FEATURE_SOURCE,
            'sources': collection.sources,
            'source_counts': collection.source_counts,
            'features_persisted': len(persisted_items),
            'features_failed': len(failed_symbols),
            'failed_symbols': failed_symbols[:50],
            'participation_context': participation_context,
            'items': persisted_items,
        }

    def inspect(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        requested_symbols = list(symbols or [])
        items = load_daily_signal_features(
            market_date=normalized_market_date,
            symbols=requested_symbols or None,
            limit=limit,
        )
        matched_symbols = [item['symbol'] for item in items]
        return {
            'market_date': normalized_market_date,
            'summary': {
                'entries': len(items),
                'requested_symbols': requested_symbols,
                'matched_symbols': matched_symbols,
                'missing_symbols': [symbol for symbol in requested_symbols if symbol not in set(matched_symbols)],
            },
            'items': items,
        }


daily_signal_feature_service = DailySignalFeatureService()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Collect daily KIS ranking signal features')
    parser.add_argument('--market-date', required=True, help='Market date in YYYY-MM-DD format')
    parser.add_argument('--source', action='append', dest='sources', choices=SUPPORTED_SIGNAL_SOURCES)
    parser.add_argument('--max-pages', type=int, default=10)
    parser.add_argument('--overtime-volume-rank-sort-cls-code', default='2')
    parser.add_argument('--overtime-fluctuation-div-cls-code', default='2')
    return parser


async def _run_from_args(args: argparse.Namespace) -> dict[str, Any]:
    return await daily_signal_feature_service.refresh_for_market_date(
        market_date=args.market_date,
        sources=args.sources,
        max_pages=args.max_pages,
        overtime_volume_rank_sort_cls_code=args.overtime_volume_rank_sort_cls_code,
        overtime_fluctuation_div_cls_code=args.overtime_fluctuation_div_cls_code,
    )


def main(argv: Iterable[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = asyncio.run(_run_from_args(args))
    print(result)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
