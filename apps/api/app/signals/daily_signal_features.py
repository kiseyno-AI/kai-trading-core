from __future__ import annotations

import argparse
import asyncio
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable, Sequence

from app.agent_context.derived_contexts import build_price_context, daily_market_participation_context_service
from app.agent_context.eligibility import load_agent_feedback_eligible_symbols
from app.brokers.kis.provider import kis_client
from app.market_data_store import (
    list_symbols_with_daily_bar_on_date,
    load_current_quotes,
    load_daily_advanced_value_factor_results,
    load_daily_beta_factor_results,
    load_daily_composite_factor_results,
    load_daily_low_risk_factor_results,
    load_daily_market_risk_filter_results,
    load_daily_momentum_factor_results,
    load_daily_piotroski_filter_results,
    load_daily_quality_factor_results,
    load_daily_signal_feature_context_rows,
    load_daily_signal_features,
    load_daily_unified_value_factor_results,
    load_daily_value_factor_results,
    load_recent_price_bars_by_symbol,
    load_symbols,
    upsert_daily_signal_feature,
    upsert_daily_signal_features,
)

SIGNAL_FEATURE_VERSION = 'daily_signal_features_v2'
SIGNAL_FEATURE_SOURCE = 'kis_ranking'
SIGNAL_FEATURE_ITEM_PREVIEW_LIMIT = 100

ROW_KIND_DIRECT_ONLY = 'direct_only'
ROW_KIND_DIRECT_PLUS_FALLBACK = 'direct_plus_fallback'
ROW_KIND_FALLBACK_ONLY = 'fallback_only'

ACTIVE_UNIVERSE_LABEL = 'active_daily_bar_universe'
CANDIDATE_UNIVERSE_LABEL = 'agent_feedback_candidate_universe'
FILTER_UNIVERSE_AVAILABLE = 'available'
FILTER_UNIVERSE_PARTIAL = 'partial'
FILTER_UNIVERSE_UNAVAILABLE = 'unavailable'

VOLUME_POWER = 'volume-power'
OVERTIME_VOLUME = 'overtime-volume'
OVERTIME_FLUCTUATION = 'overtime-fluctuation'
SUPPORTED_SIGNAL_SOURCES = (VOLUME_POWER, OVERTIME_VOLUME, OVERTIME_FLUCTUATION)
SIGNAL_SOURCE_MAX_ATTEMPTS = 2
SIGNAL_SOURCE_RETRY_BASE_SECONDS = 0.25

FACTOR_SCORE_FIELD_SPECS: dict[str, dict[str, str | None]] = {
    'value': {
        'score': 'value_score',
        'rank': 'value_rank',
        'percentile': 'value_percentile',
        'status': 'value_status',
    },
    'unified_value': {
        'score': 'unified_value_score',
        'rank': 'unified_value_rank',
        'percentile': 'unified_value_percentile',
        'status': 'unified_value_status',
    },
    'quality': {
        'score': 'quality_score',
        'rank': 'quality_rank',
        'percentile': 'quality_percentile',
        'status': 'quality_status',
    },
    'momentum': {
        'score': 'momentum_percentile',
        'rank': 'momentum_rank',
        'percentile': 'momentum_percentile',
        'status': None,
    },
    'advanced_value': {
        'score': 'advanced_value_score',
        'rank': 'advanced_value_rank',
        'percentile': 'advanced_value_percentile',
        'status': 'advanced_value_classification',
    },
    'low_risk': {
        'score': 'low_risk_score',
        'rank': None,
        'percentile': None,
        'status': 'low_risk_classification',
    },
    'beta': {
        'score': 'low_beta_score',
        'rank': None,
        'percentile': None,
        'status': 'low_beta_classification',
    },
    'composite': {
        'score': 'composite_score',
        'rank': 'composite_rank',
        'percentile': 'composite_percentile',
        'status': 'composite_classification',
    },
}

FACTOR_METRIC_GROUPS: dict[str, tuple[str, ...]] = {
    'value': ('per', 'pbr', 'dividend_yield'),
    'unified_value': ('per', 'pbr', 'dividend_yield', 'fcf_yield', 'ev_to_ebitda'),
    'quality': (
        'roe',
        'roa',
        'gross_profit_to_assets',
        'operating_cash_flow_to_net_income',
        'debt_ratio',
        'accruals_ratio',
    ),
    'momentum': ('momentum_12_1', 'return_5d', 'ma_60', 'ma_120'),
    'advanced_value': ('market_cap_proxy', 'enterprise_value', 'ebitda', 'free_cash_flow', 'fcf_yield', 'ev_to_ebitda'),
    'low_risk': ('volatility_60d', 'volatility_120d', 'max_drawdown_120d', 'avg_trading_value_20d', 'liquidity_percentile'),
    'beta': ('beta_60d', 'beta_120d', 'beta_60d_percentile', 'beta_120d_percentile'),
    'composite': ('valid_factor_count', 'available_base_weight', 'has_required_inputs'),
}


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


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace('Z', '+00:00')
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _max_iso_datetime(values: Sequence[str | None]) -> str | None:
    parsed = [item for item in (_parse_iso_datetime(value) for value in values) if item is not None]
    if not parsed:
        return None
    return max(parsed).isoformat()


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
            if value:
                cleaned[key] = value
            continue
        cleaned[key] = value
    return cleaned


def _has_payload_content(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, dict):
        return any(_has_payload_content(item) for item in value.values())
    if isinstance(value, list):
        return any(_has_payload_content(item) for item in value)
    return True


def _trade_date_relation(*, raw_trade_date: str | None, market_date: str) -> str | None:
    if not raw_trade_date:
        return None
    try:
        normalized = date.fromisoformat(str(raw_trade_date)).isoformat()
    except ValueError:
        return None
    if normalized == market_date:
        return 'same_market_date'
    if normalized < market_date:
        return 'before_market_date'
    return 'after_market_date'


def _coverage_status(*, present_count: int, universe_size: int) -> str:
    if universe_size <= 0:
        return 'empty'
    if present_count <= 0:
        return 'missing'
    if present_count >= universe_size:
        return 'complete'
    return 'partial'


def _factor_status_value(row: dict[str, Any] | None, *, field_name: str | None) -> Any:
    if row is None or not field_name:
        return None
    return row.get(field_name)


def _maybe_factor_metric_group(row: dict[str, Any] | None, *, family: str) -> dict[str, Any] | None:
    if row is None:
        return None
    fields = FACTOR_METRIC_GROUPS[family]
    metrics = {field: row.get(field) for field in fields if row.get(field) is not None}
    return metrics or None


def _build_factor_score_features(
    *,
    value_row: dict[str, Any] | None,
    unified_value_row: dict[str, Any] | None,
    quality_row: dict[str, Any] | None,
    momentum_row: dict[str, Any] | None,
    advanced_value_row: dict[str, Any] | None,
    low_risk_row: dict[str, Any] | None,
    beta_row: dict[str, Any] | None,
    composite_row: dict[str, Any] | None,
) -> dict[str, Any] | None:
    row_map = {
        'value': value_row,
        'unified_value': unified_value_row,
        'quality': quality_row,
        'momentum': momentum_row,
        'advanced_value': advanced_value_row,
        'low_risk': low_risk_row,
        'beta': beta_row,
        'composite': composite_row,
    }
    items: dict[str, Any] = {}
    for family, row in row_map.items():
        if row is None:
            continue
        spec = FACTOR_SCORE_FIELD_SPECS[family]
        payload = _clean_mapping(
            {
                'source': row.get('source'),
                'feature_version': row.get('feature_version'),
                'score': _round_float(row.get(spec['score']), digits=6) if spec.get('score') else None,
                'rank': row.get(spec['rank']) if spec.get('rank') else None,
                'percentile': _round_float(row.get(spec['percentile']), digits=6) if spec.get('percentile') else None,
                'status': _factor_status_value(row, field_name=spec.get('status')),
                'eligible': (
                    row.get('is_momentum_eligible')
                    if family == 'momentum'
                    else row.get('is_advanced_value_eligible')
                    if family == 'advanced_value'
                    else row.get('is_low_risk_eligible')
                    if family == 'low_risk'
                    else row.get('is_low_beta_eligible')
                    if family == 'beta'
                    else row.get('is_composite_eligible')
                    if family == 'composite'
                    else row.get('is_quality_score_complete')
                    if family == 'quality'
                    else None
                ),
                'captured_at': row.get('captured_at'),
                'metrics': _maybe_factor_metric_group(row, family=family),
            }
        )
        if payload:
            items[family] = payload
    return items or None


def _build_volatility_features(
    *,
    low_risk_row: dict[str, Any] | None,
    beta_row: dict[str, Any] | None,
) -> dict[str, Any] | None:
    payload = _clean_mapping(
        {
            'volatility_60d': _round_float(low_risk_row.get('volatility_60d'), digits=6) if low_risk_row else None,
            'volatility_120d': _round_float(low_risk_row.get('volatility_120d'), digits=6) if low_risk_row else None,
            'max_drawdown_120d': _round_float(low_risk_row.get('max_drawdown_120d'), digits=6) if low_risk_row else None,
            'low_risk_score': _round_float(low_risk_row.get('low_risk_score'), digits=6) if low_risk_row else None,
            'low_risk_classification': low_risk_row.get('low_risk_classification') if low_risk_row else None,
            'beta_60d': _round_float(beta_row.get('beta_60d'), digits=6) if beta_row else None,
            'beta_120d': _round_float(beta_row.get('beta_120d'), digits=6) if beta_row else None,
            'low_beta_score': _round_float(beta_row.get('low_beta_score'), digits=6) if beta_row else None,
            'low_beta_classification': beta_row.get('low_beta_classification') if beta_row else None,
        }
    )
    return payload or None


def _build_quote_snapshot_features(*, quote_row: dict[str, Any] | None, market_date: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if quote_row is None:
        return None, None

    bid_price = _to_int(quote_row.get('bid_price'))
    ask_price = _to_int(quote_row.get('ask_price'))
    spread_points = ask_price - bid_price if ask_price is not None and bid_price is not None and ask_price >= bid_price else None
    trade_date = None
    raw_trade_date = quote_row.get('trade_date')
    if raw_trade_date:
        try:
            trade_date = date.fromisoformat(str(raw_trade_date)).isoformat()
        except ValueError:
            trade_date = None

    payload = _clean_mapping(
        {
            'available': bool(quote_row.get('available')) if quote_row.get('available') is not None else quote_row.get('price') is not None,
            'source': quote_row.get('source'),
            'market_state': quote_row.get('market_state'),
            'is_live': quote_row.get('is_live'),
            'fallback': quote_row.get('fallback'),
            'price': _to_int(quote_row.get('price')),
            'change': _to_int(quote_row.get('change')),
            'change_pct': _round_float(quote_row.get('change_pct'), digits=6),
            'volume': _to_int(quote_row.get('volume')),
            'turnover': _to_int(quote_row.get('turnover')),
            'trade_date': trade_date,
            'bid_price': bid_price,
            'ask_price': ask_price,
            'spread_points': spread_points,
            'spread_bps_of_price': round((spread_points / float(_to_int(quote_row.get('price')))) * 10000.0, 4)
            if spread_points is not None and _to_int(quote_row.get('price')) not in (None, 0)
            else None,
            'cached_at': quote_row.get('cached_at'),
            'captured_at': quote_row.get('captured_at'),
        }
    )
    freshness = _clean_mapping(
        {
            'trade_date': trade_date,
            'trade_date_relation_to_market_date': _trade_date_relation(raw_trade_date=trade_date, market_date=market_date),
            'cached_at': quote_row.get('cached_at'),
            'captured_at': quote_row.get('captured_at'),
        }
    )
    return (payload or None), (freshness or None)


def _build_price_action_and_return_features(
    *,
    price_bars: list[dict[str, Any]],
    market_date: str,
    momentum_row: dict[str, Any] | None,
    low_risk_row: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, dict[str, Any] | None, dict[str, Any] | None]:
    if not price_bars or str(price_bars[0].get('trade_date') or '') != market_date:
        return None, None, None, None

    price_context = build_price_context(price_bars)
    if price_context is None:
        return None, None, None, None

    current = price_bars[0]
    previous = price_bars[1] if len(price_bars) > 1 else None
    close_price = _to_int(current.get('close_price'))
    previous_close_price = _to_int(previous.get('close_price')) if previous else None
    close_return_1d_pct = None
    if close_price is not None and previous_close_price not in (None, 0):
        close_return_1d_pct = round(((float(close_price) - float(previous_close_price)) / float(previous_close_price)) * 100.0, 6)

    price_action = _clean_mapping(
        {
            'source': current.get('source'),
            'trade_date': current.get('trade_date'),
            'captured_at': current.get('captured_at'),
            'close_price': close_price,
            'previous_close_price': previous_close_price,
            'close_return_1d_pct': close_return_1d_pct,
            'candle_direction': ((price_context.get('candle') or {}).get('direction')),
            'candle_body_size': ((price_context.get('candle') or {}).get('body_size')),
            'close_location_ratio': _round_float(((price_context.get('session_range') or {}).get('close_position_in_range')), digits=6),
            'close_location_label': ((price_context.get('session_range') or {}).get('close_position_label')),
            'range_points': _to_int(((price_context.get('session_range') or {}).get('points'))),
            'range_pct': _round_float(((price_context.get('session_range') or {}).get('percent_of_open')), digits=6),
            'gap_direction': ((price_context.get('gap') or {}).get('direction')),
            'gap_vs_prev_close_pct': _round_float(((price_context.get('gap') or {}).get('gap_percent')), digits=6),
            'gap_reliable': ((price_context.get('gap') or {}).get('reliable')),
            'gap_filled_intraday': ((price_context.get('gap') or {}).get('filled_intraday')),
            'price_bar_quality': (price_context.get('price_bar_quality') or None),
        }
    )
    liquidity = _clean_mapping(
        {
            'volume': _to_int(current.get('volume')),
            'turnover': _to_int(current.get('turnover')),
            'turnover_krw': _to_int(((price_context.get('liquidity') or {}).get('turnover_krw'))),
            'turnover_source': ((price_context.get('liquidity') or {}).get('turnover_source')),
            'turnover_is_estimated': ((price_context.get('liquidity') or {}).get('turnover_is_estimated')),
            'avg_trading_value_20d': _round_float(low_risk_row.get('avg_trading_value_20d'), digits=6) if low_risk_row else None,
            'liquidity_percentile': _round_float(low_risk_row.get('liquidity_percentile'), digits=6) if low_risk_row else None,
        }
    )
    returns = _clean_mapping(
        {
            'close_return_1d_pct': close_return_1d_pct,
            'return_5d': _round_float(momentum_row.get('return_5d'), digits=6) if momentum_row else None,
            'momentum_12_1': _round_float(momentum_row.get('momentum_12_1'), digits=6) if momentum_row else None,
        }
    )
    freshness = _clean_mapping(
        {
            'trade_date': current.get('trade_date'),
            'trade_date_relation_to_market_date': _trade_date_relation(raw_trade_date=str(current.get('trade_date') or ''), market_date=market_date),
            'captured_at': current.get('captured_at'),
        }
    )
    return (
        price_action or None,
        liquidity or None,
        returns or None,
        freshness or None,
    )


def _count_present_feature_groups(groups: dict[str, Any]) -> list[str]:
    return [name for name, payload in groups.items() if _has_payload_content(payload)]


def _row_kind(*, direct_signal_source_count: int, fallback_groups_present: list[str]) -> str:
    if direct_signal_source_count > 0 and fallback_groups_present:
        return ROW_KIND_DIRECT_PLUS_FALLBACK
    if direct_signal_source_count > 0:
        return ROW_KIND_DIRECT_ONLY
    return ROW_KIND_FALLBACK_ONLY


def _candidate_universe_metadata(*, market_date: str) -> dict[str, Any]:
    piotroski_rows = load_daily_piotroski_filter_results(
        market_date=market_date,
        passed_filter=None,
        limit=1,
    )
    market_risk_rows = load_daily_market_risk_filter_results(
        market_date=market_date,
        passed_filter=None,
        limit=1,
    )
    has_piotroski_rows = bool(piotroski_rows)
    has_market_risk_rows = bool(market_risk_rows)
    if has_piotroski_rows and has_market_risk_rows:
        availability = FILTER_UNIVERSE_AVAILABLE
    elif has_piotroski_rows or has_market_risk_rows:
        availability = FILTER_UNIVERSE_PARTIAL
    else:
        availability = FILTER_UNIVERSE_UNAVAILABLE

    candidate_symbols = load_agent_feedback_eligible_symbols(market_date=date.fromisoformat(market_date))
    return {
        'availability': availability,
        'symbols': candidate_symbols,
        'has_piotroski_rows': has_piotroski_rows,
        'has_market_risk_rows': has_market_risk_rows,
    }


def _resolve_generation_universe(
    *,
    market_date: str,
    direct_symbols: Sequence[str],
) -> dict[str, Any]:
    active_symbols = list_symbols_with_daily_bar_on_date(market_date)
    try:
        candidate_metadata = _candidate_universe_metadata(market_date=market_date)
    except Exception as exc:
        candidate_metadata = {
            'availability': FILTER_UNIVERSE_UNAVAILABLE,
            'symbols': [],
            'has_piotroski_rows': False,
            'has_market_risk_rows': False,
            'error': str(exc),
        }
    candidate_symbols = list(candidate_metadata.get('symbols') or [])
    target_symbols = sorted(set(direct_symbols) | set(active_symbols) | set(candidate_symbols))
    return {
        'target_symbols': target_symbols,
        'active_symbols': active_symbols,
        'candidate_symbols': candidate_symbols,
        'candidate_universe_availability': candidate_metadata.get('availability', FILTER_UNIVERSE_UNAVAILABLE),
        'candidate_universe_inputs': {
            'has_piotroski_rows': bool(candidate_metadata.get('has_piotroski_rows')),
            'has_market_risk_rows': bool(candidate_metadata.get('has_market_risk_rows')),
            'error': candidate_metadata.get('error'),
        },
    }


def _build_feature_coverage_bucket(
    *,
    label: str,
    symbols: Sequence[str],
    persisted_symbol_set: set[str],
    availability: str = FILTER_UNIVERSE_AVAILABLE,
    reason: str | None = None,
) -> dict[str, Any]:
    ordered_symbols = list(dict.fromkeys(symbols))
    universe_size = len(ordered_symbols)
    present_symbols = [symbol for symbol in ordered_symbols if symbol in persisted_symbol_set]
    missing_symbols = [symbol for symbol in ordered_symbols if symbol not in persisted_symbol_set]
    coverage_status = _coverage_status(present_count=len(present_symbols), universe_size=universe_size)
    if availability != FILTER_UNIVERSE_AVAILABLE and universe_size == 0:
        coverage_status = availability
    return {
        'label': label,
        'availability': availability,
        'symbol_count': universe_size,
        'present_rows': len(present_symbols),
        'missing_rows': len(missing_symbols),
        'coverage_ratio': _round_float(len(present_symbols) / universe_size, digits=6) if universe_size else None,
        'coverage_status': coverage_status,
        'missing_symbols_preview': missing_symbols[:50],
        'reason': reason,
    }


def _build_feature_coverage_summary(
    *,
    market_date: str,
    persisted_rows: Sequence[dict[str, Any]],
    active_symbols: Sequence[str],
    candidate_symbols: Sequence[str],
    candidate_universe_availability: str,
    requested_symbols: Sequence[str] | None = None,
) -> dict[str, Any]:
    persisted_symbol_set = {str(row.get('symbol') or '') for row in persisted_rows if row.get('symbol')}
    row_kind_counts: dict[str, int] = {}
    direct_signal_source_rows = 0
    for row in persisted_rows:
        row_kind = str(
            (row.get('provenance') or {}).get('row_kind')
            or (row.get('feature_payload') or {}).get('row_provenance', {}).get('row_kind')
            or ROW_KIND_DIRECT_ONLY
            if int(row.get('signal_count') or 0) > 0
            else ROW_KIND_FALLBACK_ONLY
        )
        row_kind_counts[row_kind] = row_kind_counts.get(row_kind, 0) + 1
        if int(row.get('signal_count') or 0) > 0:
            direct_signal_source_rows += 1

    requested_bucket = None
    if requested_symbols:
        requested_bucket = _build_feature_coverage_bucket(
            label='requested_symbol_scope',
            symbols=requested_symbols,
            persisted_symbol_set=persisted_symbol_set,
        )

    candidate_reason = None
    if candidate_universe_availability != FILTER_UNIVERSE_AVAILABLE:
        candidate_reason = 'Candidate coverage requires both daily market-risk and Piotroski filter rows for the same market date.'

    return {
        'market_date': market_date,
        'persisted_row_count': len(persisted_rows),
        'direct_signal_source_row_count': direct_signal_source_rows,
        'direct_signal_source_coverage_ratio': (
            _round_float(direct_signal_source_rows / len(persisted_rows), digits=6)
            if persisted_rows
            else None
        ),
        'direct_signal_source_coverage_status': _coverage_status(
            present_count=direct_signal_source_rows,
            universe_size=len(persisted_rows),
        ),
        'fallback_only_row_count': row_kind_counts.get(ROW_KIND_FALLBACK_ONLY, 0),
        'row_kind_counts': row_kind_counts,
        'active_universe': _build_feature_coverage_bucket(
            label=ACTIVE_UNIVERSE_LABEL,
            symbols=active_symbols,
            persisted_symbol_set=persisted_symbol_set,
        ),
        'candidate_universe': _build_feature_coverage_bucket(
            label=CANDIDATE_UNIVERSE_LABEL,
            symbols=candidate_symbols,
            persisted_symbol_set=persisted_symbol_set,
            availability=candidate_universe_availability,
            reason=candidate_reason,
        ),
        'requested_scope': requested_bucket,
    }


def _feature_item_preview(items: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return list(items[:SIGNAL_FEATURE_ITEM_PREVIEW_LIMIT])


def _load_support_maps(
    *,
    market_date: str,
    target_symbols: Sequence[str],
) -> dict[str, Any]:
    ordered_symbols = list(dict.fromkeys(target_symbols))
    if not ordered_symbols:
        return {
            'symbol_map': {},
            'quote_map': {},
            'price_bars_by_symbol': {},
            'value_map': {},
            'unified_value_map': {},
            'quality_map': {},
            'momentum_map': {},
            'advanced_value_map': {},
            'low_risk_map': {},
            'beta_map': {},
            'composite_map': {},
        }

    symbol_rows = load_symbols(symbols=ordered_symbols, limit=None)
    quote_rows = load_current_quotes(symbols=ordered_symbols, limit=max(len(ordered_symbols), 100))
    price_bars_by_symbol = load_recent_price_bars_by_symbol(
        market_date=market_date,
        symbols=ordered_symbols,
        history_days=2,
    )
    value_rows = load_daily_value_factor_results(
        market_date=market_date,
        symbols=ordered_symbols,
        limit=None,
    )
    unified_value_rows = load_daily_unified_value_factor_results(
        market_date=market_date,
        symbols=ordered_symbols,
        limit=None,
    )
    quality_rows = load_daily_quality_factor_results(
        market_date=market_date,
        symbols=ordered_symbols,
        limit=None,
    )
    momentum_rows = load_daily_momentum_factor_results(
        market_date=market_date,
        symbols=ordered_symbols,
        limit=None,
    )
    advanced_value_rows = load_daily_advanced_value_factor_results(
        market_date=market_date,
        symbols=ordered_symbols,
        limit=None,
    )
    low_risk_rows = load_daily_low_risk_factor_results(
        market_date=market_date,
        symbols=ordered_symbols,
        limit=None,
    )
    beta_rows = load_daily_beta_factor_results(
        market_date=market_date,
        symbols=ordered_symbols,
        limit=None,
    )
    composite_rows = load_daily_composite_factor_results(
        market_date=market_date,
        symbols=ordered_symbols,
        limit=None,
    )
    return {
        'symbol_map': {item['symbol']: item for item in symbol_rows},
        'quote_map': {item['symbol']: item for item in quote_rows},
        'price_bars_by_symbol': price_bars_by_symbol,
        'value_map': {item['symbol']: item for item in value_rows},
        'unified_value_map': {item['symbol']: item for item in unified_value_rows},
        'quality_map': {item['symbol']: item for item in quality_rows},
        'momentum_map': {item['symbol']: item for item in momentum_rows},
        'advanced_value_map': {item['symbol']: item for item in advanced_value_rows},
        'low_risk_map': {item['symbol']: item for item in low_risk_rows},
        'beta_map': {item['symbol']: item for item in beta_rows},
        'composite_map': {item['symbol']: item for item in composite_rows},
    }


def _resolve_name_ko(
    *,
    entry: dict[str, Any],
    symbol_metadata: dict[str, Any] | None,
    quote_row: dict[str, Any] | None,
    fallback_rows: Sequence[dict[str, Any] | None],
) -> str | None:
    if entry.get('name_ko'):
        return entry.get('name_ko')
    if symbol_metadata and symbol_metadata.get('name_ko'):
        return symbol_metadata.get('name_ko')
    if quote_row and quote_row.get('company_name'):
        return quote_row.get('company_name')
    for row in fallback_rows:
        if row and row.get('name_ko'):
            return row.get('name_ko')
    return None


def _merge_owned_fallback_features(
    *,
    entry: dict[str, Any],
    market_date: str,
    universe: dict[str, Any],
    support_maps: dict[str, Any],
    collection_payload: dict[str, Any],
    source_errors: dict[str, str],
) -> dict[str, Any] | None:
    symbol = entry['symbol']
    symbol_metadata = (support_maps.get('symbol_map') or {}).get(symbol)
    quote_row = (support_maps.get('quote_map') or {}).get(symbol)
    price_bars = (support_maps.get('price_bars_by_symbol') or {}).get(symbol, [])
    value_row = (support_maps.get('value_map') or {}).get(symbol)
    unified_value_row = (support_maps.get('unified_value_map') or {}).get(symbol)
    quality_row = (support_maps.get('quality_map') or {}).get(symbol)
    momentum_row = (support_maps.get('momentum_map') or {}).get(symbol)
    advanced_value_row = (support_maps.get('advanced_value_map') or {}).get(symbol)
    low_risk_row = (support_maps.get('low_risk_map') or {}).get(symbol)
    beta_row = (support_maps.get('beta_map') or {}).get(symbol)
    composite_row = (support_maps.get('composite_map') or {}).get(symbol)

    factor_score_features = _build_factor_score_features(
        value_row=value_row,
        unified_value_row=unified_value_row,
        quality_row=quality_row,
        momentum_row=momentum_row,
        advanced_value_row=advanced_value_row,
        low_risk_row=low_risk_row,
        beta_row=beta_row,
        composite_row=composite_row,
    )
    volatility_features = _build_volatility_features(
        low_risk_row=low_risk_row,
        beta_row=beta_row,
    )
    price_action_features, liquidity_features, return_features, price_freshness = _build_price_action_and_return_features(
        price_bars=price_bars,
        market_date=market_date,
        momentum_row=momentum_row,
        low_risk_row=low_risk_row,
    )
    quote_snapshot_features, quote_freshness = _build_quote_snapshot_features(
        quote_row=quote_row,
        market_date=market_date,
    )

    fallback_features = _clean_mapping(
        {
            'price_action': price_action_features,
            'liquidity': liquidity_features,
            'returns': return_features,
            'volatility': volatility_features,
            'factor_scores': factor_score_features,
            'quote_snapshot': quote_snapshot_features,
        }
    )
    direct_sources = sorted(((entry.get('feature_payload') or {}).get('sources') or {}).keys())
    fallback_groups_present = _count_present_feature_groups(fallback_features)
    direct_signal_source_count = len(direct_sources)

    if direct_signal_source_count <= 0 and not fallback_groups_present:
        return None

    factor_captured_at_values = [
        (row or {}).get('captured_at')
        for row in (
            value_row,
            unified_value_row,
            quality_row,
            momentum_row,
            advanced_value_row,
            low_risk_row,
            beta_row,
            composite_row,
        )
    ]
    row_provenance = {
        'row_kind': _row_kind(
            direct_signal_source_count=direct_signal_source_count,
            fallback_groups_present=fallback_groups_present,
        ),
        'direct_signal_sources': direct_sources,
        'direct_signal_source_count': direct_signal_source_count,
        'fallback_groups_present': fallback_groups_present,
        'active_universe_member': symbol in set(universe.get('active_symbols') or []),
        'candidate_universe_member': symbol in set(universe.get('candidate_symbols') or []),
        'fallback_note': (
            'Fallback fields are derived from stored bars, stored factor rows, and cached quotes. Treat them as owned-data context, not live external ranking truth.'
            if fallback_groups_present
            else None
        ),
    }
    freshness = _clean_mapping(
        {
            'market_date': market_date,
            'direct_sources_collected_at': collection_payload.get('collected_at') if direct_signal_source_count > 0 else None,
            'price_bars': price_freshness,
            'quote_snapshot': quote_freshness,
            'factor_scores': {
                'families_present': sorted((factor_score_features or {}).keys()),
                'latest_captured_at': _max_iso_datetime(factor_captured_at_values),
            }
            if factor_score_features
            else None,
        }
    )
    payload = dict(entry['feature_payload'])
    payload['feature_version'] = SIGNAL_FEATURE_VERSION
    payload['collection'] = dict(payload.get('collection') or {})
    payload['collection']['collected_at'] = collection_payload.get('collected_at')
    payload['collection']['source_errors'] = dict(source_errors)
    payload['collection']['universe'] = {
        'target_symbol_count': len(universe.get('target_symbols') or []),
        'active_symbol_count': len(universe.get('active_symbols') or []),
        'candidate_symbol_count': len(universe.get('candidate_symbols') or []),
        'candidate_universe_availability': universe.get('candidate_universe_availability'),
    }
    payload['row_provenance'] = row_provenance
    payload['freshness'] = freshness
    payload['fallback_features'] = fallback_features

    entry['feature_payload'] = payload
    entry['name_ko'] = _resolve_name_ko(
        entry=entry,
        symbol_metadata=symbol_metadata,
        quote_row=quote_row,
        fallback_rows=(
            value_row,
            unified_value_row,
            quality_row,
            momentum_row,
            advanced_value_row,
            low_risk_row,
            beta_row,
            composite_row,
        ),
    )
    if symbol_metadata and symbol_metadata.get('market'):
        entry['market'] = symbol_metadata['market']
    return entry


def _expand_rows_with_owned_fallback(
    *,
    market_date: str,
    direct_rows: list[dict[str, Any]],
    collection_payload: dict[str, Any],
    source_errors: dict[str, str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    universe = _resolve_generation_universe(
        market_date=market_date,
        direct_symbols=[item['symbol'] for item in direct_rows],
    )
    support_maps = _load_support_maps(
        market_date=market_date,
        target_symbols=universe['target_symbols'],
    )

    direct_map = {item['symbol']: item for item in direct_rows}
    items: list[dict[str, Any]] = []
    for symbol in universe['target_symbols']:
        entry = direct_map.get(symbol)
        if entry is None:
            entry = _empty_feature_row(
                symbol=symbol,
                market_date=market_date,
                source_counts={source: int(collection_payload.get('source_counts', {}).get(source) or 0) for source in SUPPORTED_SIGNAL_SOURCES},
                collection=collection_payload,
            )
        merged = _merge_owned_fallback_features(
            entry=entry,
            market_date=market_date,
            universe=universe,
            support_maps=support_maps,
            collection_payload=collection_payload,
            source_errors=source_errors,
        )
        if merged is not None:
            items.append(merged)

    items.sort(
        key=lambda item: (
            -int(item['signal_count']),
            item['volume_power_rank'] if item['volume_power_rank'] is not None else 999999,
            item['overtime_volume_rank'] if item['overtime_volume_rank'] is not None else 999999,
            item['overtime_fluctuation_rank'] if item['overtime_fluctuation_rank'] is not None else 999999,
            item['symbol'],
        )
    )
    return items, universe


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
    source_errors: dict[str, str]
    universe: dict[str, Any]
    items: list[dict[str, Any]]


class DailySignalFeatureService:
    def __init__(self, *, client=None) -> None:
        self.client = client or kis_client

    async def _fetch_ranking_source(self, fetch, **kwargs) -> dict[str, Any]:
        """Run one read-only ranking call with one bounded retry.

        KIS client exceptions carry an explicit ``retryable`` classification.
        Restricting retries to that signal keeps request/configuration failures
        fail-fast and avoids adding retries to the generic client where order
        submissions also flow.
        """

        for attempt in range(SIGNAL_SOURCE_MAX_ATTEMPTS):
            try:
                return await fetch(**kwargs)
            except Exception as exc:
                is_final_attempt = attempt + 1 >= SIGNAL_SOURCE_MAX_ATTEMPTS
                if is_final_attempt or getattr(exc, 'retryable', False) is not True:
                    raise
                await asyncio.sleep(SIGNAL_SOURCE_RETRY_BASE_SECONDS * (2**attempt))

        raise AssertionError('unreachable signal source retry state')

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
        source_errors: dict[str, str] = {}
        collection = {
            'max_pages': max_pages,
            'overtime_volume_rank_sort_cls_code': overtime_volume_rank_sort_cls_code,
            'overtime_fluctuation_div_cls_code': overtime_fluctuation_div_cls_code,
            'collected_at': _utcnow_iso(),
        }

        if VOLUME_POWER in selected_sources:
            try:
                response = await self._fetch_ranking_source(
                    self.client.fetch_domestic_ranking_volume_power,
                    max_pages=max_pages,
                )
                source_rows[VOLUME_POWER] = list(response.get('output') or [])
                collection[VOLUME_POWER] = {
                    'page_count': response.get('page_count'),
                    'max_pages_reached': response.get('max_pages_reached'),
                }
            except Exception as exc:
                error_message = str(exc).strip() or type(exc).__name__
                source_rows[VOLUME_POWER] = []
                source_errors[VOLUME_POWER] = error_message
                collection[VOLUME_POWER] = {'error': error_message}

        if OVERTIME_VOLUME in selected_sources:
            try:
                response = await self._fetch_ranking_source(
                    self.client.fetch_domestic_ranking_overtime_volume,
                    rank_sort_cls_code=overtime_volume_rank_sort_cls_code,
                    max_pages=max_pages,
                )
                source_rows[OVERTIME_VOLUME] = list(response.get('output2') or [])
                collection[OVERTIME_VOLUME] = {
                    'page_count': response.get('page_count'),
                    'max_pages_reached': response.get('max_pages_reached'),
                    'summary_rows': response.get('output1') or [],
                }
            except Exception as exc:
                error_message = str(exc).strip() or type(exc).__name__
                source_rows[OVERTIME_VOLUME] = []
                source_errors[OVERTIME_VOLUME] = error_message
                collection[OVERTIME_VOLUME] = {'error': error_message}

        if OVERTIME_FLUCTUATION in selected_sources:
            try:
                response = await self._fetch_ranking_source(
                    self.client.fetch_domestic_ranking_overtime_fluctuation,
                    div_cls_code=overtime_fluctuation_div_cls_code,
                    max_pages=max_pages,
                )
                source_rows[OVERTIME_FLUCTUATION] = list(response.get('output2') or [])
                collection[OVERTIME_FLUCTUATION] = {
                    'page_count': response.get('page_count'),
                    'max_pages_reached': response.get('max_pages_reached'),
                    'summary_rows': response.get('output1') or [],
                }
            except Exception as exc:
                error_message = str(exc).strip() or type(exc).__name__
                source_rows[OVERTIME_FLUCTUATION] = []
                source_errors[OVERTIME_FLUCTUATION] = error_message
                collection[OVERTIME_FLUCTUATION] = {'error': error_message}

        collection['source_counts'] = {source: len(source_rows.get(source, [])) for source in selected_sources}
        direct_rows = build_daily_signal_feature_rows(
            market_date=normalized_market_date,
            source_rows=source_rows,
            collection=collection,
        )
        items, universe = _expand_rows_with_owned_fallback(
            market_date=normalized_market_date,
            direct_rows=direct_rows,
            collection_payload=collection,
            source_errors=source_errors,
        )
        return DailySignalFeatureCollection(
            market_date=normalized_market_date,
            sources=selected_sources,
            source_counts={source: len(source_rows.get(source, [])) for source in selected_sources},
            source_errors=source_errors,
            universe=universe,
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
        include_retry_metadata: bool = False,
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
        try:
            upsert_daily_signal_features(collection.items)
            persisted_items = list(collection.items)
        except Exception:
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

        coverage = _build_feature_coverage_summary(
            market_date=collection.market_date,
            persisted_rows=persisted_items,
            active_symbols=collection.universe.get('active_symbols') or [],
            candidate_symbols=collection.universe.get('candidate_symbols') or [],
            candidate_universe_availability=str(
                collection.universe.get('candidate_universe_availability') or FILTER_UNIVERSE_UNAVAILABLE
            ),
        )

        result = {
            'market_date': collection.market_date,
            'source': SIGNAL_FEATURE_SOURCE,
            'sources': collection.sources,
            'source_counts': collection.source_counts,
            'source_errors': collection.source_errors,
            'features_persisted': len(persisted_items),
            'features_failed': len(failed_symbols),
            'failed_symbols': failed_symbols[:50],
            'coverage': coverage,
            'participation_context': participation_context,
            'items_total': len(persisted_items),
            'items_returned': min(len(persisted_items), SIGNAL_FEATURE_ITEM_PREVIEW_LIMIT),
            'items': _feature_item_preview(persisted_items),
        }
        if include_retry_metadata:
            result['_retry_metadata'] = {'failed_symbols': failed_symbols}
        return result

    def rebuild_stored_owned_fallback_for_market_date(
        self,
        *,
        market_date: str | date,
    ) -> dict[str, Any]:
        """Refresh deterministic fallback fields without refetching rankings.

        Historical KIS ranking responses cannot be reproduced safely.  This
        path therefore starts from the rows already persisted for the date,
        preserves their direct ranking columns and raw ``sources`` payload,
        and replaces only the fallback fields owned by stored bars/factors.
        The stored quote fallback is preserved as well because the current
        quote cache is not valid historical evidence.
        """

        normalized_market_date = _normalize_market_date(market_date)
        stored_rows = load_daily_signal_features(
            market_date=normalized_market_date,
            symbols=None,
            limit=None,
        )
        if not stored_rows:
            return {
                'market_date': normalized_market_date,
                'stored_rows': 0,
                'features_persisted': 0,
                'preserved_direct_source_rows': 0,
            }

        stored_symbols = [str(item['symbol']) for item in stored_rows]
        universe = _resolve_generation_universe(
            market_date=normalized_market_date,
            direct_symbols=stored_symbols,
        )
        support_maps = _load_support_maps(
            market_date=normalized_market_date,
            target_symbols=stored_symbols,
        )

        # Do not let today's quote cache rewrite historical quote evidence.
        preserved_quote_map: dict[str, dict[str, Any]] = {}
        for row in stored_rows:
            feature_payload = row.get('feature_payload') or {}
            old_quote = (
                (feature_payload.get('fallback_features') or {}).get(
                    'quote_snapshot'
                )
            )
            if isinstance(old_quote, dict) and old_quote:
                preserved_quote_map[str(row['symbol'])] = deepcopy(old_quote)
        support_maps['quote_map'] = preserved_quote_map

        rebuilt_rows: list[dict[str, Any]] = []
        preserved_direct_source_rows = 0
        for stored_row in stored_rows:
            entry = {
                key: deepcopy(stored_row.get(key))
                for key in (
                    'symbol',
                    'name_ko',
                    'market',
                    'market_date',
                    'source',
                    'signal_count',
                    'volume_power_rank',
                    'volume_power',
                    'overtime_volume_rank',
                    'overtime_volume',
                    'overtime_fluctuation_rank',
                    'overtime_fluctuation_rate',
                    'feature_payload',
                )
            }
            payload = dict(entry.get('feature_payload') or {})
            entry['feature_payload'] = payload
            collection_payload = dict(payload.get('collection') or {})
            source_errors = dict(collection_payload.get('source_errors') or {})
            direct_sources_before = deepcopy(payload.get('sources') or {})
            direct_columns_before = tuple(
                entry.get(key)
                for key in (
                    'signal_count',
                    'volume_power_rank',
                    'volume_power',
                    'overtime_volume_rank',
                    'overtime_volume',
                    'overtime_fluctuation_rank',
                    'overtime_fluctuation_rate',
                )
            )
            merged = _merge_owned_fallback_features(
                entry=entry,
                market_date=normalized_market_date,
                universe=universe,
                support_maps=support_maps,
                collection_payload=collection_payload,
                source_errors=source_errors,
            )
            if merged is None:
                raise RuntimeError(
                    'stored signal feature lost every direct and deterministic '
                    f'fallback input during repair: {stored_row["symbol"]}'
                )
            direct_columns_after = tuple(
                merged.get(key)
                for key in (
                    'signal_count',
                    'volume_power_rank',
                    'volume_power',
                    'overtime_volume_rank',
                    'overtime_volume',
                    'overtime_fluctuation_rank',
                    'overtime_fluctuation_rate',
                )
            )
            if (
                direct_columns_after != direct_columns_before
                or (merged.get('feature_payload') or {}).get('sources', {})
                != direct_sources_before
            ):
                raise RuntimeError(
                    'deterministic signal-feature repair changed preserved KIS '
                    f'ranking evidence for {stored_row["symbol"]}'
                )
            if direct_sources_before:
                preserved_direct_source_rows += 1
            rebuilt_rows.append(merged)

        persisted_count = upsert_daily_signal_features(rebuilt_rows)
        if persisted_count != len(rebuilt_rows):
            raise RuntimeError(
                'deterministic signal-feature repair persistence mismatch: '
                f'expected={len(rebuilt_rows)}, observed={persisted_count}'
            )
        return {
            'market_date': normalized_market_date,
            'stored_rows': len(stored_rows),
            'features_persisted': persisted_count,
            'preserved_direct_source_rows': preserved_direct_source_rows,
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
        coverage_rows = load_daily_signal_feature_context_rows(market_date=normalized_market_date)
        coverage_universe = _resolve_generation_universe(
            market_date=normalized_market_date,
            direct_symbols=[item['symbol'] for item in coverage_rows],
        )
        matched_symbols = [item['symbol'] for item in items]
        matched_symbol_set = set(matched_symbols)
        return {
            'market_date': normalized_market_date,
            'summary': {
                'entries': len(items),
                'entries_for_market_date': len(coverage_rows),
                'requested_symbols': requested_symbols,
                'matched_symbols': matched_symbols,
                'missing_symbols': [symbol for symbol in requested_symbols if symbol not in matched_symbol_set],
                'coverage': _build_feature_coverage_summary(
                    market_date=normalized_market_date,
                    persisted_rows=coverage_rows,
                    active_symbols=coverage_universe.get('active_symbols') or [],
                    candidate_symbols=coverage_universe.get('candidate_symbols') or [],
                    candidate_universe_availability=str(
                        coverage_universe.get('candidate_universe_availability') or FILTER_UNIVERSE_UNAVAILABLE
                    ),
                    requested_symbols=requested_symbols,
                ),
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
