from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Sequence

from app.brokers.kis.provider import kis_client
from app.core.settings import settings
from app.market.clock import async_get_market_state
from app.market_data_store import (
    load_current_kis_intraday_snapshots,
    load_symbols,
    load_tracked_symbols,
    persist_kis_intraday_snapshot,
)
from app.scanner.state import scanner_state

KIS_INTRADAY_SNAPSHOT_SOURCE = 'kis_intraday_snapshot_v1'
DEFAULT_MARKET_CODE = 'J'
DEFAULT_MAX_RANKING_PAGES = 2
DEFAULT_MAX_NEWS_ITEMS = 5
DEFAULT_FETCH_CONCURRENCY = 5
RESULT_SYMBOL_PREVIEW_LIMIT = 50
RESULT_ERROR_MESSAGE_LIMIT = 1000
KST = timezone(timedelta(hours=9))
SURFACE_NAMES: tuple[str, ...] = (
    'quote',
    'orderbook',
    'liquidity',
    'investor_program_activity',
    'news_hints',
)
SURFACE_PROVENANCE: dict[str, dict[str, str]] = {
    'quote': {
        'endpoint': 'inquire-price',
        'tr_id': 'FHKST01010100',
    },
    'orderbook': {
        'endpoint': 'inquire-asking-price-exp-ccn',
        'tr_id': 'FHKST01010200',
    },
    'volume_power': {
        'endpoint': 'ranking/volume-power',
        'tr_id': 'FHPST01680000',
    },
    'volume_rank': {
        'endpoint': 'quotations/volume-rank',
        'tr_id': 'FHPST01710000',
    },
    'quote_balance': {
        'endpoint': 'ranking/quote-balance',
        'tr_id': 'FHPST01720000',
    },
    'program_trade': {
        'endpoint': 'program-trade-by-stock',
        'tr_id': 'FHPPG04650101',
    },
    'investor_trend_estimate': {
        'endpoint': 'investor-trend-estimate',
        'tr_id': 'HHPTJ04160200',
    },
    'news_title': {
        'endpoint': 'news-title',
        'tr_id': 'FHKST01011800',
    },
}
NEWS_EVENT_TAGS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ('earnings', ('실적', '분기', '결산', 'earnings', 'guidance')),
    ('contract', ('수주', '계약', '공급', 'supply', 'contract')),
    ('corporate_action', ('무상증자', '유상증자', '분할', '합병', '소각', 'buyback', '자사주')),
    ('investment', ('투자', '인수', 'acquisition', '지분')),
    ('risk_event', ('리콜', '소송', '조사', '회계', 'suspension', 'halt')),
)


@dataclass(slots=True)
class IntradaySymbolInput:
    symbol: str
    name_ko: str | None
    market: str | None


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


def _round_float(value: Any, *, digits: int = 6) -> float | None:
    if value is None:
        return None
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None


def _clean_mapping(values: dict[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for key, value in values.items():
        if value is None:
            continue
        if isinstance(value, dict):
            nested = _clean_mapping(value)
            if nested:
                cleaned[key] = nested
            continue
        if isinstance(value, list):
            cleaned[key] = value
            continue
        cleaned[key] = value
    return cleaned


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace('Z', '+00:00')
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _combine_market_date_and_time(market_date: str, hhmmss: str | None) -> str | None:
    normalized = str(hhmmss or '').strip()
    if not normalized:
        return None
    digits = ''.join(ch for ch in normalized if ch.isdigit())
    if len(digits) != 6:
        return None
    try:
        parsed_date = date.fromisoformat(market_date)
    except ValueError:
        return None
    combined = datetime(
        parsed_date.year,
        parsed_date.month,
        parsed_date.day,
        int(digits[0:2]),
        int(digits[2:4]),
        int(digits[4:6]),
        tzinfo=KST,
    )
    return combined.isoformat()


def _extract_dict(payload: dict[str, Any], *keys: str) -> dict[str, Any]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, dict):
            return value
    return {}


def _extract_list(payload: dict[str, Any], *keys: str) -> list[dict[str, Any]]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            return [value]
    return []


def _normalize_symbol(symbol: str) -> str:
    normalized = str(symbol or '').strip().upper()
    if not normalized:
        raise ValueError('symbol must not be blank')
    return normalized.zfill(6) if normalized.isdigit() else normalized


def _normalize_symbols(symbols: Sequence[str] | None) -> list[str] | None:
    if symbols is None:
        return None
    return list(dict.fromkeys(_normalize_symbol(symbol) for symbol in symbols))


def _normalize_market_code(value: str | None) -> str:
    normalized = str(value or '').strip().upper()
    if not normalized:
        raise ValueError('market_code must not be blank')
    return normalized


def _event_tags_for_title(title: str | None) -> list[str]:
    text = str(title or '').strip().lower()
    if not text:
        return []
    tags: list[str] = []
    for tag, keywords in NEWS_EVENT_TAGS:
        if any(keyword.lower() in text for keyword in keywords):
            tags.append(tag)
    return tags


def _status_from_counts(*, available_count: int, total_count: int) -> str:
    if total_count <= 0 or available_count <= 0:
        return 'unavailable'
    if available_count >= total_count:
        return 'available'
    return 'partial'


def _has_any_content(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, dict):
        return any(_has_any_content(item) for item in value.values())
    if isinstance(value, list):
        return any(_has_any_content(item) for item in value)
    return True


def _bounded_exception_message(exc: Exception) -> str:
    message = str(exc).strip() or type(exc).__name__
    return message[:RESULT_ERROR_MESSAGE_LIMIT]


def _now_kst() -> datetime:
    return datetime.now(KST)


def _non_trading_day_result(
    *,
    runtime: dict[str, Any],
    requested_symbols: list[str],
    market_state: str,
    market_date: str,
) -> dict[str, Any]:
    return {
        'status': 'market_closed',
        'reason': 'non_trading_day',
        'runtime': runtime,
        'market_state': market_state,
        'market_date': market_date,
        'requested_symbols': requested_symbols,
        'summary': {
            'matched_symbol_count': len(requested_symbols),
            'collected_count': 0,
            'available_count': 0,
            'partial_count': 0,
            'unavailable_count': 0,
        },
        'snapshots_persisted': 0,
        'snapshots_failed': 0,
        'failed_symbols': [],
        'failed_symbol_errors': [],
        'items': [],
    }


class KISIntradaySnapshotService:
    def __init__(self, *, client=None, fetch_concurrency: int = DEFAULT_FETCH_CONCURRENCY) -> None:
        self.client = client or kis_client
        self.fetch_concurrency = max(int(fetch_concurrency or DEFAULT_FETCH_CONCURRENCY), 1)

    def runtime_status(self) -> dict[str, Any]:
        enabled = bool(settings.kis_intraday_snapshot_enabled)
        credentials_configured = bool(settings.kis_app_key and settings.kis_app_secret)
        blocking_reasons: list[str] = []
        if not enabled:
            blocking_reasons.append('kis_intraday_snapshot_disabled')
        if not credentials_configured:
            blocking_reasons.append('kis_credentials_missing')
        return {
            'enabled': enabled,
            'ready': enabled and credentials_configured,
            'credentials_configured': credentials_configured,
            'kis_use_mock': settings.kis_use_mock,
            'market_code': settings.kis_intraday_snapshot_market_code,
            'max_ranking_pages': settings.kis_intraday_snapshot_max_ranking_pages,
            'news_limit': settings.kis_intraday_snapshot_news_limit,
            'blocking_reasons': blocking_reasons,
        }

    def _capture_universe(self, symbols: Sequence[str] | None) -> list[IntradaySymbolInput]:
        normalized_symbols = _normalize_symbols(symbols)
        if normalized_symbols is None:
            normalized_symbols = load_tracked_symbols()
            if not normalized_symbols:
                normalized_symbols = list(scanner_state.snapshot().get('tracked_symbols') or [])
        if not normalized_symbols:
            return []
        symbol_rows = load_symbols(symbols=normalized_symbols, limit=max(len(normalized_symbols), 100))
        row_map = {row['symbol']: row for row in symbol_rows}
        return [
            IntradaySymbolInput(
                symbol=symbol,
                name_ko=(row_map.get(symbol) or {}).get('name_ko'),
                market=(row_map.get(symbol) or {}).get('market') or 'KRX',
            )
            for symbol in normalized_symbols
        ]

    async def _load_shared_rankings(
        self,
        *,
        market_code: str,
        max_ranking_pages: int,
    ) -> tuple[dict[str, dict[str, dict[str, Any]]], dict[str, str], dict[str, Any]]:
        requests = {
            'volume_power': self.client.fetch_domestic_ranking_volume_power(
                market_code=market_code,
                max_pages=max_ranking_pages,
            ),
            'volume_rank': self.client.fetch_domestic_volume_rank(
                market_code=market_code,
            ),
            'quote_balance': self.client.fetch_domestic_quote_balance_ranking(
                market_code=market_code,
                max_pages=max_ranking_pages,
            ),
        }
        responses = await asyncio.gather(*requests.values(), return_exceptions=True)
        maps: dict[str, dict[str, dict[str, Any]]] = {}
        errors: dict[str, str] = {}
        metadata: dict[str, Any] = {}

        for name, response in zip(requests.keys(), responses, strict=True):
            if isinstance(response, Exception):
                errors[name] = _bounded_exception_message(response)
                continue
            rows = _extract_list(response, 'output', 'output1', 'output2')
            symbol_key = 'stck_shrn_iscd' if name == 'volume_power' else 'mksc_shrn_iscd'
            if name == 'volume_rank':
                symbol_key = 'mksc_shrn_iscd'
            maps[name] = {
                _normalize_symbol(row[symbol_key]): row
                for row in rows
                if row.get(symbol_key)
            }
            metadata[name] = {
                'row_count': len(rows),
                'page_count': response.get('page_count'),
                'max_pages_reached': response.get('max_pages_reached'),
            }
        return maps, errors, metadata

    def _normalize_quote_block(
        self,
        *,
        symbol: IntradaySymbolInput,
        market_state: str,
        market_date: str,
        response: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, str | None, str | None]:
        output = _extract_dict(response, 'output', 'output1')
        if not output:
            return None, None, None
        price = _to_int(output.get('stck_prpr'))
        bid_price = _to_int(output.get('bidp') or output.get('bidp1'))
        ask_price = _to_int(output.get('askp') or output.get('askp1'))
        spread_points = ask_price - bid_price if ask_price is not None and bid_price is not None and ask_price >= bid_price else None
        trade_date = None
        raw_trade_date = str(output.get('stck_bsop_date') or '').strip()
        if len(raw_trade_date) == 8 and raw_trade_date.isdigit():
            trade_date = f'{raw_trade_date[0:4]}-{raw_trade_date[4:6]}-{raw_trade_date[6:8]}'
        as_of_timestamp = _combine_market_date_and_time(
            trade_date or market_date,
            str(output.get('stck_cntg_hour') or '').strip() or None,
        )
        payload = _clean_mapping(
            {
                'status': 'available',
                'source': 'kis',
                'source_endpoint': SURFACE_PROVENANCE['quote']['endpoint'],
                'market_state': market_state,
                'price': price,
                'change': _to_int(output.get('prdy_vrss')),
                'change_pct': _round_float(output.get('prdy_ctrt')),
                'volume': _to_int(output.get('acml_vol')),
                'turnover': _to_int(output.get('acml_tr_pbmn')),
                'bid_price': bid_price,
                'ask_price': ask_price,
                'spread_points': spread_points,
                'spread_bps_of_price': round((spread_points / float(price)) * 10000.0, 4)
                if spread_points is not None and price not in (None, 0)
                else None,
                'trade_date': trade_date,
                'as_of_time': output.get('stck_cntg_hour'),
                'as_of_timestamp': as_of_timestamp,
                'name_ko': symbol.name_ko,
            }
        )
        return payload or None, trade_date, as_of_timestamp

    def _normalize_orderbook_block(
        self,
        *,
        response: dict[str, Any],
        fallback_market_date: str,
    ) -> tuple[dict[str, Any] | None, str | None]:
        output = _extract_dict(response, 'output1', 'output')
        expected_output = _extract_dict(response, 'output2')
        if not output:
            return None, None

        levels: list[dict[str, Any]] = []
        for level in range(1, 11):
            ask_price = _to_int(output.get(f'askp{level}'))
            bid_price = _to_int(output.get(f'bidp{level}'))
            ask_size = _to_int(output.get(f'askp_rsqn{level}'))
            bid_size = _to_int(output.get(f'bidp_rsqn{level}'))
            if all(value is None for value in (ask_price, bid_price, ask_size, bid_size)):
                continue
            levels.append(
                _clean_mapping(
                    {
                        'level': level,
                        'ask_price': ask_price,
                        'ask_size': ask_size,
                        'bid_price': bid_price,
                        'bid_size': bid_size,
                    }
                )
            )

        total_ask_depth = _to_int(output.get('total_askp_rsqn'))
        total_bid_depth = _to_int(output.get('total_bidp_rsqn'))
        net_bid_depth = _to_int(output.get('ntby_aspr_rsqn'))
        total_depth = None
        if total_ask_depth is not None and total_bid_depth is not None:
            total_depth = total_ask_depth + total_bid_depth
        imbalance = (
            round(((total_bid_depth - total_ask_depth) / float(total_depth)), 6)
            if total_depth not in (None, 0)
            else None
        )
        as_of_time = str(output.get('aspr_acpt_hour') or '').strip() or None
        as_of_timestamp = _combine_market_date_and_time(fallback_market_date, as_of_time)

        payload = _clean_mapping(
            {
                'status': 'available',
                'source': 'kis',
                'source_endpoint': SURFACE_PROVENANCE['orderbook']['endpoint'],
                'as_of_time': as_of_time,
                'as_of_timestamp': as_of_timestamp,
                'best_ask_price': levels[0].get('ask_price') if levels else None,
                'best_bid_price': levels[0].get('bid_price') if levels else None,
                'total_ask_depth': total_ask_depth,
                'total_bid_depth': total_bid_depth,
                'net_bid_depth': net_bid_depth,
                'imbalance': imbalance,
                'levels': levels,
                'expected_match': _clean_mapping(
                    {
                        'price': _to_int(expected_output.get('antc_cnpr') or output.get('antc_cnpr')),
                        'change': _to_int(expected_output.get('antc_cntg_vrss') or output.get('antc_cntg_vrss')),
                        'change_pct': _round_float(
                            expected_output.get('antc_cntg_prdy_ctrt') or output.get('antc_cntg_prdy_ctrt')
                        ),
                        'volume': _to_int(expected_output.get('antc_vol') or output.get('antc_vol')),
                    }
                ),
            }
        )
        return payload or None, as_of_timestamp

    def _normalize_liquidity_block(
        self,
        *,
        symbol: str,
        shared_maps: dict[str, dict[str, dict[str, Any]]],
        shared_errors: dict[str, str],
        shared_metadata: dict[str, Any],
    ) -> dict[str, Any] | None:
        volume_power_row = (shared_maps.get('volume_power') or {}).get(symbol)
        volume_rank_row = (shared_maps.get('volume_rank') or {}).get(symbol)
        quote_balance_row = (shared_maps.get('quote_balance') or {}).get(symbol)

        available_sources = [
            name
            for name, row in (
                ('volume_power', volume_power_row),
                ('volume_rank', volume_rank_row),
                ('quote_balance', quote_balance_row),
            )
            if row is not None
        ]
        source_rows = {
            'volume_power': volume_power_row,
            'volume_rank': volume_rank_row,
            'quote_balance': quote_balance_row,
        }
        source_statuses: dict[str, str] = {}
        for name, row in source_rows.items():
            if row is not None:
                source_statuses[name] = 'ranked'
                continue
            if name in shared_errors:
                source_statuses[name] = 'unavailable'
                continue
            metadata = shared_metadata.get(name)
            if not isinstance(metadata, dict):
                source_statuses[name] = 'unavailable'
                continue
            source_statuses[name] = (
                'unknown_due_to_truncation'
                if metadata.get('max_pages_reached') is True
                else 'not_ranked'
            )
        incomplete_source_count = sum(
            1
            for status in source_statuses.values()
            if status in {'unavailable', 'unknown_due_to_truncation'}
        )
        successful_source_count = len(source_statuses) - incomplete_source_count
        total_bid_depth = _to_int((quote_balance_row or {}).get('total_bidp_rsqn'))
        total_ask_depth = _to_int((quote_balance_row or {}).get('total_askp_rsqn'))
        net_bid_depth = _to_int((quote_balance_row or {}).get('total_ntsl_bidp_rsqn'))
        total_depth = None
        if total_bid_depth is not None and total_ask_depth is not None:
            total_depth = total_bid_depth + total_ask_depth
        payload = _clean_mapping(
            {
                'status': (
                    'available'
                    if successful_source_count == len(source_statuses)
                    else 'partial'
                    if successful_source_count > 0 or available_sources
                    else 'unavailable'
                ),
                'available_sources': available_sources,
                'source_statuses': source_statuses,
                'volume_power_rank': _to_int((volume_power_row or {}).get('data_rank')),
                'volume_power': _round_float((volume_power_row or {}).get('tday_rltv')),
                'volume_power_buy_execution': _to_int((volume_power_row or {}).get('shnu_cnqn_smtn')),
                'volume_power_sell_execution': _to_int((volume_power_row or {}).get('seln_cnqn_smtn')),
                'volume_rank': _to_int((volume_rank_row or {}).get('data_rank')),
                'quote_balance_rank': _to_int((quote_balance_row or {}).get('data_rank')),
                'quote_balance_total_bid_depth': total_bid_depth,
                'quote_balance_total_ask_depth': total_ask_depth,
                'quote_balance_net_bid_depth': net_bid_depth,
                'quote_balance_imbalance': (
                    round((net_bid_depth / float(total_depth)), 6)
                    if net_bid_depth is not None and total_depth not in (None, 0)
                    else None
                ),
                'quote_balance_bid_ratio': _round_float((quote_balance_row or {}).get('shnu_rsqn_rate')),
                'quote_balance_ask_ratio': _round_float((quote_balance_row or {}).get('seln_rsqn_rate')),
                'errors': shared_errors or None,
            }
        )
        return payload if _has_any_content(payload) else None

    def _normalize_investor_program_activity_block(
        self,
        *,
        investor_response: dict[str, Any] | None,
        program_response: dict[str, Any] | None,
        fallback_market_date: str,
    ) -> dict[str, Any] | None:
        investor_rows = _extract_list(investor_response or {}, 'output2', 'output')
        program_rows = _extract_list(program_response or {}, 'output')
        investor_row = investor_rows[-1] if investor_rows else {}
        program_row = sorted(program_rows, key=lambda item: str(item.get('bsop_hour') or ''))[-1] if program_rows else {}
        as_of_timestamp = _combine_market_date_and_time(
            fallback_market_date,
            str(program_row.get('bsop_hour') or '').strip() or None,
        )

        payload = _clean_mapping(
            {
                'status': (
                    'available'
                    if investor_row and program_row
                    else 'partial'
                    if investor_row or program_row
                    else 'unavailable'
                ),
                'as_of_timestamp': as_of_timestamp,
                'investor_trend_estimate': _clean_mapping(
                    {
                        'bucket': investor_row.get('bsop_hour_gb'),
                        'foreign_estimated_net_quantity': _to_int(investor_row.get('frgn_fake_ntby_qty')),
                        'institution_estimated_net_quantity': _to_int(investor_row.get('orgn_fake_ntby_qty')),
                        'combined_estimated_net_quantity': _to_int(investor_row.get('sum_fake_ntby_qty')),
                    }
                ),
                'program_trade': _clean_mapping(
                    {
                        'as_of_time': program_row.get('bsop_hour'),
                        'current_price': _to_int(program_row.get('stck_prpr')),
                        'volume': _to_int(program_row.get('acml_vol')),
                        'net_buy_quantity': _to_int(program_row.get('whol_smtn_ntby_qty')),
                        'net_buy_amount': _to_int(program_row.get('whol_smtn_ntby_tr_pbmn')),
                        'sell_volume': _to_int(program_row.get('whol_smtn_seln_vol')),
                        'buy_volume': _to_int(program_row.get('whol_smtn_shnu_vol')),
                        'sell_amount': _to_int(program_row.get('whol_smtn_seln_tr_pbmn')),
                        'buy_amount': _to_int(program_row.get('whol_smtn_shnu_tr_pbmn')),
                    }
                ),
            }
        )
        return payload if _has_any_content(payload) else None

    def _normalize_news_hints_block(
        self,
        *,
        symbol: str,
        response: dict[str, Any] | None,
        max_news_items: int,
        fallback_market_date: str,
    ) -> dict[str, Any] | None:
        rows = _extract_list(response or {}, 'output')
        items: list[dict[str, Any]] = []
        for row in sorted(
            rows,
            key=lambda item: f"{item.get('data_dt') or ''}{item.get('data_tm') or ''}",
            reverse=True,
        )[:max_news_items]:
            related_symbols = [
                _normalize_symbol(code)
                for code in (
                    row.get('iscd1'),
                    row.get('iscd2'),
                    row.get('iscd3'),
                    row.get('iscd4'),
                    row.get('iscd5'),
                )
                if code
            ]
            title = str(row.get('hts_pbnt_titl_cntt') or '').strip()
            items.append(
                _clean_mapping(
                    {
                        'detail_id': row.get('cntt_usiq_srno'),
                        'published_at': _combine_market_date_and_time(
                            f"{str(row.get('data_dt') or '')[0:4]}-{str(row.get('data_dt') or '')[4:6]}-{str(row.get('data_dt') or '')[6:8]}"
                            if str(row.get('data_dt') or '').isdigit() and len(str(row.get('data_dt') or '')) == 8
                            else fallback_market_date,
                            row.get('data_tm'),
                        ),
                        'title': title,
                        'provider_code': row.get('news_ofer_entp_code'),
                        'category_code': row.get('news_lrdv_code'),
                        'source_org': row.get('dorg'),
                        'related_symbols': related_symbols,
                        'event_tags': _event_tags_for_title(title),
                    }
                )
            )
        return {
            'status': 'available' if items else 'empty',
            'item_count': len(items),
            'items': items,
            'has_symbol_match': any(symbol in (item.get('related_symbols') or []) for item in items),
            'event_tags': sorted({tag for item in items for tag in (item.get('event_tags') or [])}),
        }

    async def _collect_symbol_snapshot(
        self,
        *,
        symbol_input: IntradaySymbolInput,
        market_code: str,
        market_state: str,
        market_date: str,
        shared_maps: dict[str, dict[str, dict[str, Any]]],
        shared_errors: dict[str, str],
        shared_metadata: dict[str, Any],
        max_news_items: int,
        include_news: bool,
        source: str,
    ) -> dict[str, Any]:
        quote_task = self.client.fetch_domestic_quote(symbol_input.symbol, market_code=market_code)
        orderbook_task = self.client.fetch_domestic_orderbook(symbol_input.symbol, market_code=market_code)
        program_task = self.client.fetch_domestic_program_trade_by_stock(symbol_input.symbol, market_code=market_code)
        investor_task = self.client.fetch_domestic_investor_trend_estimate(symbol_input.symbol)
        tasks = [quote_task, orderbook_task, program_task, investor_task]
        if include_news:
            tasks.append(
                self.client.fetch_domestic_news_titles(
                    symbol=symbol_input.symbol,
                    max_pages=max(1, settings.kis_intraday_snapshot_news_pages),
                )
            )
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        response_names = ['quote', 'orderbook', 'program_trade', 'investor_trend_estimate']
        if include_news:
            response_names.append('news_title')

        surface_errors: dict[str, str] = {}
        surface_responses: dict[str, dict[str, Any] | None] = {}
        for name, response in zip(response_names, responses, strict=True):
            if isinstance(response, Exception):
                surface_errors[name] = str(response)
                surface_responses[name] = None
                continue
            surface_responses[name] = response

        quote_block, quote_trade_date, quote_as_of_timestamp = self._normalize_quote_block(
            symbol=symbol_input,
            market_state=market_state,
            market_date=market_date,
            response=surface_responses.get('quote') or {},
        )
        orderbook_block, orderbook_as_of_timestamp = self._normalize_orderbook_block(
            response=surface_responses.get('orderbook') or {},
            fallback_market_date=quote_trade_date or market_date,
        )
        liquidity_block = self._normalize_liquidity_block(
            symbol=symbol_input.symbol,
            shared_maps=shared_maps,
            shared_errors=shared_errors,
            shared_metadata=shared_metadata,
        )
        investor_program_activity = self._normalize_investor_program_activity_block(
            investor_response=surface_responses.get('investor_trend_estimate'),
            program_response=surface_responses.get('program_trade'),
            fallback_market_date=quote_trade_date or market_date,
        )
        news_hints = (
            self._normalize_news_hints_block(
                symbol=symbol_input.symbol,
                response=surface_responses.get('news_title'),
                max_news_items=max_news_items,
                fallback_market_date=quote_trade_date or market_date,
            )
            if include_news
            else {'status': 'disabled', 'item_count': 0, 'items': []}
        )

        block_statuses = {
            'quote': 'available' if quote_block is not None else 'unavailable',
            'orderbook': 'available' if orderbook_block is not None else 'unavailable',
            'liquidity': (liquidity_block or {}).get('status') or 'unavailable',
            'investor_program_activity': (investor_program_activity or {}).get('status') or 'unavailable',
            'news_hints': (news_hints or {}).get('status') or 'unavailable',
        }
        enabled_surface_names = [
            name for name in SURFACE_NAMES if name != 'news_hints' or include_news
        ]
        enabled_block_statuses = {
            name: block_statuses[name] for name in enabled_surface_names
        }
        available_block_count = sum(
            1
            for status in enabled_block_statuses.values()
            if status in {'available', 'empty'}
        )
        partial_block_count = sum(
            1 for status in enabled_block_statuses.values() if status == 'partial'
        )
        coverage_status = _status_from_counts(
            available_count=available_block_count + partial_block_count,
            total_count=len(enabled_surface_names),
        )
        availability_status = (
            'available'
            if available_block_count == len(enabled_surface_names)
            else 'partial'
            if (available_block_count + partial_block_count) > 0
            else 'unavailable'
        )
        as_of_market_date = quote_trade_date or market_date
        as_of_timestamp = quote_as_of_timestamp or orderbook_as_of_timestamp
        freshness_status = 'fresh' if as_of_market_date == market_date else 'stale'

        snapshot_payload = _clean_mapping(
            {
                'symbol': symbol_input.symbol,
                'name_ko': symbol_input.name_ko,
                'market': symbol_input.market or 'KRX',
                'market_code': market_code,
                'market_state': market_state,
                'source': source,
                'feature_version': source,
                'availability_status': availability_status,
                'freshness_status': freshness_status,
                'as_of_market_date': as_of_market_date,
                'as_of_timestamp': as_of_timestamp,
                'coverage': {
                    'status': coverage_status,
                    'available_blocks': available_block_count,
                    'partial_blocks': partial_block_count,
                    'total_blocks': len(enabled_surface_names),
                    'coverage_ratio': _round_float(
                        (available_block_count + partial_block_count)
                        / float(len(enabled_surface_names))
                    ),
                    'block_statuses': block_statuses,
                    'excluded_blocks': (
                        ['news_hints'] if not include_news else []
                    ),
                },
                'availability': {
                    'quote': {'status': block_statuses['quote'], 'error': surface_errors.get('quote')},
                    'orderbook': {'status': block_statuses['orderbook'], 'error': surface_errors.get('orderbook')},
                    'liquidity': {'status': block_statuses['liquidity'], 'errors': shared_errors or None},
                    'investor_program_activity': {
                        'status': block_statuses['investor_program_activity'],
                        'errors': _clean_mapping(
                            {
                                'program_trade': surface_errors.get('program_trade'),
                                'investor_trend_estimate': surface_errors.get('investor_trend_estimate'),
                            }
                        ),
                    },
                    'news_hints': {'status': block_statuses['news_hints'], 'error': surface_errors.get('news_title')},
                },
                'freshness': {
                    'requested_market_date': market_date,
                    'trade_date_relation_to_capture_date': (
                        'same_market_date' if as_of_market_date == market_date else 'before_market_date'
                    ),
                    'captured_via': 'manual_or_runtime_pull',
                },
                'quote': quote_block,
                'orderbook': orderbook_block,
                'liquidity': liquidity_block,
                'investor_program_activity': investor_program_activity,
                'news_hints': news_hints,
                'provenance': {
                    'provider': 'kis',
                    'surfaces': {
                        name: SURFACE_PROVENANCE[name]
                        for name in (
                            'quote',
                            'orderbook',
                            'volume_power',
                            'volume_rank',
                            'quote_balance',
                            'program_trade',
                            'investor_trend_estimate',
                            'news_title',
                        )
                    },
                    'surface_errors': surface_errors,
                    'shared_ranking_errors': shared_errors,
                },
            }
        )
        return {
            'symbol': symbol_input.symbol,
            'name_ko': symbol_input.name_ko,
            'market': symbol_input.market or 'KRX',
            'market_code': market_code,
            'source': source,
            'feature_version': source,
            'availability_status': availability_status,
            'freshness_status': freshness_status,
            'as_of_market_date': as_of_market_date,
            'as_of_timestamp': as_of_timestamp,
            'snapshot_payload': snapshot_payload,
        }

    async def collect_snapshots(
        self,
        *,
        symbols: Sequence[str] | None = None,
        market_code: str | None = None,
        source: str = KIS_INTRADAY_SNAPSHOT_SOURCE,
        max_ranking_pages: int | None = None,
        max_news_items: int | None = None,
        include_news: bool = True,
    ) -> dict[str, Any]:
        runtime = self.runtime_status()
        requested_symbols = _normalize_symbols(symbols) or []
        if not runtime['ready']:
            return {
                'status': 'disabled',
                'reason': (runtime['blocking_reasons'][0] if runtime['blocking_reasons'] else 'not_ready'),
                'runtime': runtime,
                'requested_symbols': requested_symbols,
                'summary': {
                    'matched_symbol_count': len(requested_symbols),
                    'collected_count': 0,
                    'available_count': 0,
                    'partial_count': 0,
                    'unavailable_count': 0,
                },
                'snapshots_persisted': 0,
                'snapshots_failed': 0,
                'failed_symbols': [],
                'failed_symbol_errors': [],
                'items': [],
            }
        now_kst = _now_kst()
        if now_kst.weekday() >= 5:
            return _non_trading_day_result(
                runtime=runtime,
                requested_symbols=requested_symbols,
                market_state='WEEKEND',
                market_date=now_kst.date().isoformat(),
            )
        market_state = await async_get_market_state()
        market_date = market_state.server_time[:10]
        if not market_state.is_trading_day:
            return _non_trading_day_result(
                runtime=runtime,
                requested_symbols=requested_symbols,
                market_state=market_state.state,
                market_date=market_date,
            )
        symbol_inputs = self._capture_universe(symbols)
        requested_symbols = [item.symbol for item in symbol_inputs]
        if not symbol_inputs:
            return {
                'status': 'empty_universe',
                'runtime': runtime,
                'requested_symbols': [],
                'summary': {
                    'matched_symbol_count': 0,
                    'collected_count': 0,
                    'available_count': 0,
                    'partial_count': 0,
                    'unavailable_count': 0,
                },
                'snapshots_persisted': 0,
                'snapshots_failed': 0,
                'failed_symbols': [],
                'failed_symbol_errors': [],
                'items': [],
            }

        normalized_market_code = _normalize_market_code(market_code or settings.kis_intraday_snapshot_market_code or DEFAULT_MARKET_CODE)
        ranking_pages = max(1, int(max_ranking_pages or settings.kis_intraday_snapshot_max_ranking_pages or DEFAULT_MAX_RANKING_PAGES))
        news_limit = max(0, int(max_news_items or settings.kis_intraday_snapshot_news_limit or DEFAULT_MAX_NEWS_ITEMS))

        shared_maps, shared_errors, shared_metadata = await self._load_shared_rankings(
            market_code=normalized_market_code,
            max_ranking_pages=ranking_pages,
        )

        semaphore = asyncio.Semaphore(self.fetch_concurrency)

        async def _collect(item: IntradaySymbolInput) -> dict[str, Any]:
            async with semaphore:
                try:
                    return {
                        'item': await self._collect_symbol_snapshot(
                            symbol_input=item,
                            market_code=normalized_market_code,
                            market_state=market_state.state,
                            market_date=market_date,
                            shared_maps=shared_maps,
                            shared_errors=shared_errors,
                            shared_metadata=shared_metadata,
                            max_news_items=news_limit,
                            include_news=include_news,
                            source=source,
                        )
                    }
                except Exception as exc:
                    return {
                        'symbol': item.symbol,
                        'error': _bounded_exception_message(exc),
                    }

        collected = await asyncio.gather(*[_collect(item) for item in symbol_inputs])
        items: list[dict[str, Any]] = []
        failed_symbols: list[str] = []
        failed_symbol_errors: list[dict[str, str]] = []
        for result in collected:
            if result.get('item') is not None:
                items.append(result['item'])
                continue
            failed_symbols.append(result['symbol'])
            failed_symbol_errors.append({'symbol': result['symbol'], 'error': result['error']})

        persisted_items: list[dict[str, Any]] = []
        for item in items:
            try:
                persist_kis_intraday_snapshot(item)
                persisted_items.append(item['snapshot_payload'])
            except Exception as exc:
                failed_symbols.append(item['symbol'])
                failed_symbol_errors.append(
                    {
                        'symbol': item['symbol'],
                        'error': _bounded_exception_message(exc),
                    }
                )

        available_count = sum(1 for item in items if item.get('availability_status') == 'available')
        partial_count = sum(1 for item in items if item.get('availability_status') == 'partial')
        unavailable_count = sum(1 for item in items if item.get('availability_status') == 'unavailable')

        failed_symbols = list(dict.fromkeys(failed_symbols))
        partial_symbols = list(
            dict.fromkeys(
                item['symbol']
                for item in items
                if item.get('availability_status') == 'partial'
            )
        )
        unavailable_symbols = list(
            dict.fromkeys(
                item['symbol']
                for item in items
                if item.get('availability_status') == 'unavailable'
            )
        )
        pagination_truncated_sources = sorted(
            name
            for name, metadata in shared_metadata.items()
            if isinstance(metadata, dict)
            and metadata.get('max_pages_reached') is True
        )
        shared_source_incomplete = bool(
            shared_errors or pagination_truncated_sources
        )

        partial_reasons: list[str] = []
        if shared_errors:
            partial_reasons.append('shared_source_errors')
        if pagination_truncated_sources:
            partial_reasons.append('shared_source_pagination_truncated')
        if partial_symbols:
            partial_reasons.append('partial_items')
        if unavailable_symbols:
            partial_reasons.append('unavailable_items')
        if failed_symbols:
            partial_reasons.append('snapshot_failures')

        affected_symbols = list(
            dict.fromkeys(
                [*partial_symbols, *unavailable_symbols, *failed_symbols]
            )
        )
        affected_symbol_set = set(affected_symbols)
        if shared_source_incomplete:
            retry_symbols = list(requested_symbols)
            retry_scope_mode = 'all_requested_symbols'
        else:
            retry_symbols = [
                symbol
                for symbol in requested_symbols
                if symbol in affected_symbol_set
            ]
            retry_symbols.extend(
                symbol
                for symbol in affected_symbols
                if symbol not in set(requested_symbols)
            )
            retry_scope_mode = 'affected_symbols' if retry_symbols else 'none'

        status = 'partial' if partial_reasons else 'ok'
        reason = None
        if shared_source_incomplete:
            reason = 'shared_source_incomplete'
        elif partial_symbols or unavailable_symbols:
            reason = 'snapshot_items_incomplete'
        elif failed_symbols:
            reason = 'snapshot_failures'

        return {
            'status': status,
            'reason': reason,
            'partial_reasons': partial_reasons,
            'partial_details': {
                'shared_error_sources': sorted(shared_errors),
                'pagination_truncated_sources': pagination_truncated_sources,
                'partial_symbol_count': len(partial_symbols),
                'partial_symbols': partial_symbols[:RESULT_SYMBOL_PREVIEW_LIMIT],
                'unavailable_symbol_count': len(unavailable_symbols),
                'unavailable_symbols': unavailable_symbols[
                    :RESULT_SYMBOL_PREVIEW_LIMIT
                ],
                'failed_symbol_count': len(failed_symbols),
                'failed_symbols': failed_symbols[:RESULT_SYMBOL_PREVIEW_LIMIT],
            },
            'runtime': runtime,
            'market_state': market_state.state,
            'market_date': market_date,
            'market_code': normalized_market_code,
            'source': source,
            'feature_version': source,
            'requested_symbols': requested_symbols,
            'shared_source_errors': shared_errors,
            'shared_source_metadata': shared_metadata,
            'summary': {
                'matched_symbol_count': len(requested_symbols),
                'collected_count': len(items),
                'available_count': available_count,
                'partial_count': partial_count,
                'unavailable_count': unavailable_count,
            },
            'snapshots_persisted': len(persisted_items),
            'snapshots_failed': len(failed_symbols),
            'failed_symbols': failed_symbols[:RESULT_SYMBOL_PREVIEW_LIMIT],
            'failed_symbol_errors': failed_symbol_errors[
                :RESULT_SYMBOL_PREVIEW_LIMIT
            ],
            'retry_required': bool(retry_symbols),
            'retry_symbols': retry_symbols[:RESULT_SYMBOL_PREVIEW_LIMIT],
            'retry_symbols_count': len(retry_symbols),
            'retry_scope': {
                'mode': retry_scope_mode,
                'symbol_count': len(retry_symbols),
                'symbols': retry_symbols[:RESULT_SYMBOL_PREVIEW_LIMIT],
                'symbols_truncated': (
                    len(retry_symbols) > RESULT_SYMBOL_PREVIEW_LIMIT
                ),
            },
            '_retry_metadata': {'failed_symbols': retry_symbols},
            'items': persisted_items,
        }

    def inspect(
        self,
        *,
        symbols: Sequence[str] | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        normalized_symbols = _normalize_symbols(symbols)
        items = load_current_kis_intraday_snapshots(
            symbols=normalized_symbols,
            limit=max(limit, len(normalized_symbols or []), 1),
        )
        available_count = sum(1 for item in items if item.get('availability_status') == 'available')
        partial_count = sum(1 for item in items if item.get('availability_status') == 'partial')
        unavailable_count = sum(1 for item in items if item.get('availability_status') == 'unavailable')
        fresh_count = sum(1 for item in items if item.get('freshness_status') == 'fresh')
        stale_count = sum(1 for item in items if item.get('freshness_status') == 'stale')
        return {
            'runtime': self.runtime_status(),
            'requested_symbols': normalized_symbols or [],
            'summary': {
                'entries': len(items),
                'available_count': available_count,
                'partial_count': partial_count,
                'unavailable_count': unavailable_count,
                'fresh_count': fresh_count,
                'stale_count': stale_count,
            },
            'items': items[:limit],
        }


kis_intraday_snapshot_service = KISIntradaySnapshotService()
