from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime
from typing import Any, Sequence

from app.agent_context.derived_contexts import DEFAULT_DERIVED_CONTEXT_HISTORY_DAYS, build_price_context
from app.agent_context.score_context import build_signal_decider_scores
from app.dart.fundamentals_store import DartFundamentalsRepository, dart_fundamentals_repository
from app.dart.service import FS_DIV_TO_STATEMENT_TYPE, REPORT_CODE_METADATA
from app.dart.store import DartSyncRepository, dart_sync_repository
from app.market_data_store import (
    load_daily_advanced_value_factor_results,
    load_daily_action_summaries,
    load_daily_beta_factor_results,
    load_daily_composite_factor_results,
    load_daily_low_risk_factor_results,
    load_daily_market_relative_contexts,
    load_daily_momentum_factor_results,
    load_daily_price_context_history,
    load_daily_price_contexts,
    load_daily_quality_factor_results,
    load_daily_signal_features,
    load_daily_technical_snapshots,
    load_daily_unified_value_factor_results,
    load_current_quotes,
    load_symbols,
)

PRICE_CONTEXT_HISTORY_DAYS = DEFAULT_DERIVED_CONTEXT_HISTORY_DAYS
PERSISTED_FACTOR_FAMILIES: tuple[str, ...] = (
    'unified_value',
    'quality',
    'momentum',
    'advanced_value',
    'low_risk',
    'beta',
    'composite',
)
SIGNAL_FEATURE_SUBMETRIC_FIELDS: tuple[str, ...] = (
    'signal_count',
    'volume_power_rank',
    'volume_power',
    'overtime_volume_rank',
    'overtime_volume',
    'overtime_fluctuation_rank',
    'overtime_fluctuation_rate',
)
FACTOR_ROW_FIELDS: dict[str, tuple[str, ...]] = {
    'unified_value': (
        'source',
        'feature_version',
        'market_date',
        'unified_value_score',
        'unified_value_rank',
        'unified_value_percentile',
        'components_used',
        'unified_value_status',
        'per',
        'pbr',
        'dividend_yield',
        'fcf_yield',
        'ev_to_ebitda',
        'per_status',
        'pbr_status',
        'dividend_yield_status',
        'fcf_yield_status',
        'ev_to_ebitda_status',
        'captured_at',
    ),
    'quality': (
        'source',
        'feature_version',
        'market_date',
        'quality_score',
        'quality_rank',
        'quality_percentile',
        'components_used',
        'components_possible',
        'is_quality_score_complete',
        'quality_status',
        'roe',
        'roa',
        'gross_profit_to_assets',
        'operating_cash_flow_to_net_income',
        'debt_ratio',
        'accruals_ratio',
        'roe_status',
        'roa_status',
        'gross_profit_to_assets_status',
        'operating_cash_flow_to_net_income_status',
        'debt_ratio_status',
        'accruals_ratio_status',
        'captured_at',
    ),
    'momentum': (
        'source',
        'feature_version',
        'market_date',
        'momentum_percentile',
        'momentum_rank',
        'momentum_12_1',
        'return_5d',
        'ma_60',
        'ma_120',
        'momentum_percentile_threshold',
        'passes_momentum_top_percentile',
        'is_momentum_eligible',
        'captured_at',
    ),
    'advanced_value': (
        'source',
        'feature_version',
        'market_date',
        'advanced_value_score',
        'advanced_value_rank',
        'advanced_value_percentile',
        'advanced_value_components_used',
        'advanced_value_classification',
        'is_advanced_value_eligible',
        'market_cap_proxy',
        'enterprise_value',
        'ebitda',
        'free_cash_flow',
        'fcf_yield',
        'ev_to_ebitda',
        'fcf_yield_status',
        'ev_to_ebitda_status',
        'captured_at',
    ),
    'low_risk': (
        'source',
        'feature_version',
        'market_date',
        'low_risk_score',
        'low_risk_classification',
        'is_low_risk_eligible',
        'volatility_60d',
        'volatility_120d',
        'max_drawdown_120d',
        'avg_trading_value_20d',
        'volatility_60d_percentile',
        'volatility_120d_percentile',
        'max_drawdown_120d_percentile',
        'liquidity_percentile',
        'passes_liquidity_floor',
        'captured_at',
    ),
    'beta': (
        'source',
        'feature_version',
        'market_date',
        'low_beta_score',
        'low_beta_classification',
        'is_low_beta_eligible',
        'benchmark_id',
        'benchmark_market',
        'beta_60d',
        'beta_120d',
        'beta_60d_percentile',
        'beta_120d_percentile',
        'history_days_available',
        'captured_at',
    ),
    'composite': (
        'source',
        'feature_version',
        'market_date',
        'composite_score',
        'composite_rank',
        'composite_percentile',
        'composite_classification',
        'is_composite_eligible',
        'valid_factor_count',
        'available_base_weight',
        'has_required_inputs',
        'is_hard_excluded',
        'momentum_input',
        'value_input',
        'advanced_value_input',
        'low_risk_input',
        'low_beta_input',
        'captured_at',
    ),
}
FACTOR_ROW_STATUS_FIELDS: dict[str, tuple[str, ...]] = {
    'unified_value': ('unified_value_status', 'per_status', 'pbr_status', 'dividend_yield_status', 'fcf_yield_status', 'ev_to_ebitda_status'),
    'quality': (
        'quality_status',
        'roe_status',
        'roa_status',
        'gross_profit_to_assets_status',
        'operating_cash_flow_to_net_income_status',
        'debt_ratio_status',
        'accruals_ratio_status',
    ),
    'momentum': tuple(),
    'advanced_value': ('advanced_value_classification', 'fcf_yield_status', 'ev_to_ebitda_status'),
    'low_risk': ('low_risk_classification',),
    'beta': ('low_beta_classification',),
    'composite': ('composite_classification',),
}
FACTOR_ROW_PRIMARY_SCORE_FIELDS: dict[str, str] = {
    'unified_value': 'unified_value_score',
    'quality': 'quality_score',
    'momentum': 'momentum_percentile',
    'advanced_value': 'advanced_value_score',
    'low_risk': 'low_risk_score',
    'beta': 'low_beta_score',
    'composite': 'composite_score',
}
FACTOR_ROW_SUBMETRIC_FIELDS: dict[str, tuple[str, ...]] = {
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
    'low_risk': (
        'volatility_60d',
        'volatility_120d',
        'max_drawdown_120d',
        'avg_trading_value_20d',
        'volatility_60d_percentile',
        'volatility_120d_percentile',
        'max_drawdown_120d_percentile',
        'liquidity_percentile',
    ),
    'beta': ('beta_60d', 'beta_120d', 'beta_60d_percentile', 'beta_120d_percentile'),
    'composite': (
        'momentum_input',
        'value_input',
        'advanced_value_input',
        'low_risk_input',
        'low_beta_input',
        'composite_score',
    ),
}


def _normalize_symbols(symbols: Sequence[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        for piece in (raw_symbol or '').split(','):
            symbol = piece.strip()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            normalized.append(symbol)
    if not normalized:
        raise ValueError('At least one symbol is required')
    return normalized


def _normalize_fs_div(fs_div: str | None) -> str | None:
    if fs_div is None:
        return None
    normalized = fs_div.strip().upper()
    if not normalized:
        return None
    if normalized not in FS_DIV_TO_STATEMENT_TYPE:
        raise ValueError('fs_div must be one of CFS or OFS')
    return normalized


def _build_period_end_date(*, business_year: int, period_end_month: int | None) -> date | None:
    if period_end_month is None:
        return None
    last_day = monthrange(business_year, period_end_month)[1]
    return date(business_year, period_end_month, last_day)


def _summarize_highlights(rows: list[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    ratios: dict[str, str | None] = {}
    for row in rows:
        metric_key = row['metric_key']
        if row['metric_type'] == 'ratio':
            ratios[metric_key] = row.get('ratio_value')
            continue
        result[metric_key] = {
            'statement_type': row.get('statement_type'),
            'statement_name': row.get('statement_name'),
            'account_id': row.get('account_id'),
            'account_name': row.get('account_name'),
            'current_amount': row.get('current_amount'),
            'current_cumulative_amount': row.get('current_cumulative_amount'),
            'prior_amount': row.get('prior_amount'),
            'prior_cumulative_amount': row.get('prior_cumulative_amount'),
            'pre_prior_amount': row.get('pre_prior_amount'),
            'currency': row.get('currency'),
        }
    if ratios:
        result['ratios'] = ratios
    return result


def _build_fundamentals_snapshot(item: dict[str, Any], *, period_end_date: date | None) -> dict[str, Any]:
    report_meta = REPORT_CODE_METADATA.get(item['report_code'], {})
    statement_rows = item.get('statement_rows') or []
    statement_row_count = item.get('statement_row_count')
    return {
        'filing': {
            'rcept_no': item['rcept_no'],
            'report_name': item['disclosure'].get('report_nm'),
            'filing_category': item['filing_category'],
            'receipt_date': item['disclosure'].get('receipt_date'),
            'captured_at': item.get('captured_at'),
        },
        'period': {
            'business_year': item['business_year'],
            'report_code': item['report_code'],
            'report_label': report_meta.get('label'),
            'period_type': item['period_type'],
            'period_scope': report_meta.get('scope'),
            'quarter': report_meta.get('quarter'),
            'period_end_month': item.get('period_end_month'),
            'period_end_date': period_end_date.isoformat() if period_end_date else None,
        },
        'statement': {
            'fs_div': item['fs_div'],
            'fs_label': FS_DIV_TO_STATEMENT_TYPE.get(item['fs_div']),
            'statement_type': item['statement_type'],
            'currency': item.get('currency'),
            'source_status': item.get('source_status'),
            'source_message': item.get('source_message'),
            'row_count': statement_row_count if statement_row_count is not None else len(statement_rows),
        },
        'highlights': _summarize_highlights(item.get('highlights') or []),
    }


_build_price_context = build_price_context


def _build_fundamentals_summary(items: list[dict[str, Any]], *, market_date: date) -> dict[str, Any]:
    eligible_items: list[tuple[dict[str, Any], date | None]] = []
    for item in items:
        receipt_date_raw = item['disclosure'].get('receipt_date')
        receipt_date = date.fromisoformat(receipt_date_raw) if receipt_date_raw else None
        if receipt_date is not None and receipt_date > market_date:
            continue
        period_end_date = _build_period_end_date(
            business_year=item['business_year'],
            period_end_month=item.get('period_end_month'),
        )
        eligible_items.append((item, period_end_date))

    if not eligible_items:
        return {
            'latest_snapshot': None,
            'history_summary': {
                'entries': 0,
                'latest_receipt_date': None,
                'latest_period_end_date': None,
                'available_business_years': [],
                'recent_periods': [],
            },
        }

    latest_item, latest_period_end_date = eligible_items[0]
    recent_periods: list[dict[str, Any]] = []
    business_years: list[int] = []
    seen_period_keys: set[tuple[int, str, str | None]] = set()
    for item, period_end_date in eligible_items:
        business_years.append(item['business_year'])
        period_key = (
            item['business_year'],
            item['report_code'],
            period_end_date.isoformat() if period_end_date else None,
        )
        if period_key in seen_period_keys:
            continue
        seen_period_keys.add(period_key)
        recent_periods.append(
            {
                'business_year': item['business_year'],
                'report_code': item['report_code'],
                'report_label': REPORT_CODE_METADATA.get(item['report_code'], {}).get('label'),
                'period_type': item['period_type'],
                'period_end_date': period_end_date.isoformat() if period_end_date else None,
                'receipt_date': item['disclosure'].get('receipt_date'),
                'rcept_no': item['rcept_no'],
            }
        )
        if len(recent_periods) >= 5:
            break

    latest_receipt_date = latest_item['disclosure'].get('receipt_date')
    return {
        'latest_snapshot': _build_fundamentals_snapshot(latest_item, period_end_date=latest_period_end_date),
        'history_summary': {
            'entries': len(eligible_items),
            'latest_receipt_date': latest_receipt_date,
            'latest_period_end_date': latest_period_end_date.isoformat() if latest_period_end_date else None,
            'available_business_years': sorted(set(business_years), reverse=True),
            'recent_periods': recent_periods,
        },
    }


def _serialize_disclosure(item: dict[str, Any]) -> dict[str, Any]:
    return {
        'rcept_no': item['rcept_no'],
        'corp_code': item['corp_code'],
        'corp_name': item['corp_name'],
        'report_nm': item['report_nm'],
        'filing_category': item['filing_category'],
        'is_financial_report': item['is_financial_report'],
        'receipt_date': item['receipt_date'],
        'remark': item['remark'],
        'fundamentals_capture_status': item['fundamentals_capture_status'],
    }


def _group_price_context_history(
    rows: list[dict[str, Any]],
    *,
    symbols: list[str],
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in symbols}
    for row in rows:
        symbol = row['symbol']
        if symbol not in grouped:
            grouped[symbol] = []
        grouped[symbol].append(
            {
                'market_date': row['market_date'],
                'source': row['source'],
                'captured_at': row['captured_at'],
                'price_context': row['context_payload'],
            }
        )
    return grouped


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        text = value.strip().replace(',', '')
        if not text:
            return None
        try:
            return int(float(text))
        except ValueError:
            return None
    return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip().replace(',', '')
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace('Z', '+00:00')
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _build_quote_context(row: dict[str, Any] | None, *, market_date: date) -> dict[str, Any] | None:
    if row is None:
        return None

    price = _to_int(row.get('price'))
    bid_price = _to_int(row.get('bid_price'))
    ask_price = _to_int(row.get('ask_price'))
    spread_points = ask_price - bid_price if ask_price is not None and bid_price is not None else None
    if spread_points is not None and spread_points < 0:
        spread_available = False
        spread_reason = 'negative_spread'
        spread_points = None
    elif spread_points is not None:
        spread_available = True
        spread_reason = None
    else:
        spread_available = False
        spread_reason = 'missing_bid_or_ask'

    raw_trade_date = row.get('trade_date')
    try:
        quote_trade_date = date.fromisoformat(str(raw_trade_date)).isoformat() if raw_trade_date else None
    except ValueError:
        quote_trade_date = None

    relation = 'unknown'
    if quote_trade_date is not None:
        if quote_trade_date == market_date.isoformat():
            relation = 'same_market_date'
        elif quote_trade_date > market_date.isoformat():
            relation = 'after_market_date'
        else:
            relation = 'before_market_date'

    return {
        'available': bool(row.get('available')) if row.get('available') is not None else price is not None,
        'source': row.get('source'),
        'market_state': row.get('market_state'),
        'is_live': row.get('is_live'),
        'fallback': row.get('fallback'),
        'price': price,
        'change': _to_int(row.get('change')),
        'change_pct': _to_float(row.get('change_pct')),
        'volume': _to_int(row.get('volume')),
        'turnover': _to_int(row.get('turnover')),
        'as_of': row.get('as_of'),
        'trade_date': quote_trade_date,
        'cached_at': row.get('cached_at'),
        'captured_at': row.get('captured_at'),
        'freshness': {
            'trade_date_relation_to_request': relation,
            'same_market_date_as_request': relation == 'same_market_date',
            'cached_at': row.get('cached_at'),
            'captured_at': row.get('captured_at'),
            'quote_trade_date': quote_trade_date,
            'has_cached_timestamp': _parse_iso_datetime(row.get('cached_at')) is not None,
        },
        'spread': {
            'available': spread_available,
            'bid_price': bid_price,
            'ask_price': ask_price,
            'spread_points': spread_points,
            'spread_bps_of_price': round((spread_points / price) * 10000, 4)
            if spread_points is not None and price is not None and price > 0
            else None,
            'reason': spread_reason,
        },
    }


def _ratio(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 4)


def _coverage_status(*, present_count: int, requested_count: int, partial_label: str = 'partial') -> str:
    if requested_count <= 0:
        return 'empty'
    if present_count <= 0:
        return 'missing'
    if present_count >= requested_count:
        return 'complete'
    return partial_label


def _copy_selected_fields(row: dict[str, Any] | None, *, fields: tuple[str, ...]) -> dict[str, Any] | None:
    if row is None:
        return None
    return {field: row.get(field) for field in fields}


def _build_factor_row_completeness(*, family: str, row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        return {
            'row_status': 'missing',
            'score_status': None,
            'score_field': FACTOR_ROW_PRIMARY_SCORE_FIELDS[family],
            'score_value_present': False,
            'null_submetric_fields': [],
            'null_submetric_count': 0,
            'present_submetric_fields': [],
            'present_submetric_count': 0,
            'status_fields': {},
        }

    score_field = FACTOR_ROW_PRIMARY_SCORE_FIELDS[family]
    submetric_fields = FACTOR_ROW_SUBMETRIC_FIELDS[family]
    null_submetric_fields = [field for field in submetric_fields if row.get(field) is None]
    present_submetric_fields = [field for field in submetric_fields if row.get(field) is not None]
    status_fields = {
        field: row.get(field)
        for field in FACTOR_ROW_STATUS_FIELDS[family]
        if field in row and row.get(field) is not None
    }
    primary_status = next((status_fields[field] for field in FACTOR_ROW_STATUS_FIELDS[family] if field in status_fields), None)
    return {
        'row_status': 'present',
        'score_status': primary_status,
        'score_field': score_field,
        'score_value_present': row.get(score_field) is not None,
        'null_submetric_fields': null_submetric_fields,
        'null_submetric_count': len(null_submetric_fields),
        'present_submetric_fields': present_submetric_fields,
        'present_submetric_count': len(present_submetric_fields),
        'status_fields': status_fields,
    }


def _build_signal_feature_completeness(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        return {
            'row_status': 'missing',
            'row_kind': 'missing',
            'direct_signal_source_count': 0,
            'fallback_groups_present': [],
            'null_submetric_fields': list(SIGNAL_FEATURE_SUBMETRIC_FIELDS),
            'null_submetric_count': len(SIGNAL_FEATURE_SUBMETRIC_FIELDS),
            'present_submetric_fields': [],
            'present_submetric_count': 0,
        }
    provenance = row.get('provenance') or {}
    null_submetric_fields = [field for field in SIGNAL_FEATURE_SUBMETRIC_FIELDS if row.get(field) is None]
    present_submetric_fields = [field for field in SIGNAL_FEATURE_SUBMETRIC_FIELDS if row.get(field) is not None]
    return {
        'row_status': 'present',
        'row_kind': provenance.get('row_kind') or row.get('row_kind'),
        'direct_signal_source_count': int(provenance.get('direct_signal_source_count') or row.get('signal_count') or 0),
        'fallback_groups_present': list(provenance.get('fallback_groups_present') or []),
        'null_submetric_fields': null_submetric_fields,
        'null_submetric_count': len(null_submetric_fields),
        'present_submetric_fields': present_submetric_fields,
        'present_submetric_count': len(present_submetric_fields),
    }


def _build_market_relative_completeness(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        return {
            'status': 'unavailable',
            'missing_fields': ['breadth', 'relative_strength'],
        }

    missing_fields: list[str] = []
    breadth = row.get('breadth') if isinstance(row.get('breadth'), dict) else None
    relative_strength = row.get('relative_strength') if isinstance(row.get('relative_strength'), dict) else None
    if breadth is None:
        missing_fields.append('breadth')
    if relative_strength is None:
        missing_fields.append('relative_strength')
    elif relative_strength.get('return_percentile') is None:
        missing_fields.append('relative_strength.return_percentile')
    if relative_strength is None or relative_strength.get('volume_percentile') is None:
        missing_fields.append('relative_strength.volume_percentile')
    return {
        'status': row.get('status') or ('partial' if missing_fields else 'available'),
        'missing_fields': missing_fields,
    }


class AgentContextReadService:
    def __init__(
        self,
        *,
        fundamentals_repository: DartFundamentalsRepository | None = None,
        disclosure_repository: DartSyncRepository | None = None,
    ) -> None:
        self.fundamentals_repository = fundamentals_repository or dart_fundamentals_repository
        self.disclosure_repository = disclosure_repository or dart_sync_repository

    def get_symbol_day_context(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str],
        fs_div: str | None = None,
        disclosures_limit: int = 5,
    ) -> dict[str, Any]:
        normalized_symbols = _normalize_symbols(symbols)
        normalized_market_date = market_date if isinstance(market_date, date) else date.fromisoformat(market_date)
        normalized_fs_div = _normalize_fs_div(fs_div)

        symbol_rows = load_symbols(symbols=normalized_symbols, limit=max(len(normalized_symbols), 1000))
        technical_rows = load_daily_technical_snapshots(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
            limit=max(len(normalized_symbols), 1000),
            fallback_window_days=0,
        )
        action_rows = load_daily_action_summaries(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
            limit=max(len(normalized_symbols), 1000),
        )
        signal_feature_rows = load_daily_signal_features(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
            limit=max(len(normalized_symbols), 1000),
        )
        unified_value_rows = load_daily_unified_value_factor_results(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
            limit=max(len(normalized_symbols), 1000),
        )
        quality_rows = load_daily_quality_factor_results(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
            limit=max(len(normalized_symbols), 1000),
        )
        momentum_rows = load_daily_momentum_factor_results(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
            limit=max(len(normalized_symbols), 1000),
        )
        advanced_value_rows = load_daily_advanced_value_factor_results(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
            limit=max(len(normalized_symbols), 1000),
        )
        low_risk_rows = load_daily_low_risk_factor_results(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
            limit=max(len(normalized_symbols), 1000),
        )
        beta_rows = load_daily_beta_factor_results(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
            limit=max(len(normalized_symbols), 1000),
        )
        composite_rows = load_daily_composite_factor_results(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
            limit=max(len(normalized_symbols), 1000),
        )
        quote_rows = load_current_quotes(
            symbols=normalized_symbols,
            limit=max(len(normalized_symbols), 1000),
        )
        price_context_rows = load_daily_price_contexts(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
            limit=max(len(normalized_symbols), 1000),
        )
        price_context_history_rows = load_daily_price_context_history(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
            history_days=PRICE_CONTEXT_HISTORY_DAYS,
        )
        market_relative_rows = load_daily_market_relative_contexts(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
        )
        fundamentals_rows = self.fundamentals_repository.list_stored_fundamentals_summary(
            stock_codes=normalized_symbols,
            fs_div=normalized_fs_div,
        )
        disclosures_by_symbol = self.disclosure_repository.list_recent_events_by_stock_codes(
            stock_codes=normalized_symbols,
            receipt_date_to=normalized_market_date,
            limit_per_symbol=disclosures_limit,
        )

        symbol_map = {item['symbol']: item for item in symbol_rows}
        technical_map = {item['symbol']: item for item in technical_rows}
        action_map = {item['symbol']: item for item in action_rows}
        signal_feature_map = {item['symbol']: item for item in signal_feature_rows}
        unified_value_map = {item['symbol']: item for item in unified_value_rows}
        quality_map = {item['symbol']: item for item in quality_rows}
        momentum_map = {item['symbol']: item for item in momentum_rows}
        advanced_value_map = {item['symbol']: item for item in advanced_value_rows}
        low_risk_map = {item['symbol']: item for item in low_risk_rows}
        beta_map = {item['symbol']: item for item in beta_rows}
        composite_map = {item['symbol']: item for item in composite_rows}
        quote_map = {item['symbol']: item for item in quote_rows}
        price_context_map = {item['symbol']: item['context_payload'] for item in price_context_rows}
        price_context_history_map = _group_price_context_history(
            price_context_history_rows,
            symbols=normalized_symbols,
        )
        market_relative_map = {item['symbol']: item for item in market_relative_rows}
        fundamentals_map: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in normalized_symbols}
        for item in fundamentals_rows:
            stock_code = item.get('stock_code')
            if stock_code in fundamentals_map:
                fundamentals_map[stock_code].append(item)

        items: list[dict[str, Any]] = []
        matched_symbols: list[str] = []
        missing_symbols: list[str] = []
        factor_row_counts = {family: 0 for family in PERSISTED_FACTOR_FAMILIES}

        for symbol in normalized_symbols:
            quote_context = _build_quote_context(quote_map.get(symbol), market_date=normalized_market_date)
            price_context = price_context_map.get(symbol)
            price_context_history = price_context_history_map.get(symbol, [])
            technical_snapshot = technical_map.get(symbol)
            action_summary = action_map.get(symbol)
            signal_features = signal_feature_map.get(symbol)
            market_relative_context = market_relative_map.get(symbol)
            scores = build_signal_decider_scores(
                market_date=normalized_market_date,
                unified_value_row=unified_value_map.get(symbol),
                quality_row=quality_map.get(symbol),
                momentum_row=momentum_map.get(symbol),
            )
            persisted_factor_rows = {
                'unified_value': _copy_selected_fields(
                    unified_value_map.get(symbol),
                    fields=FACTOR_ROW_FIELDS['unified_value'],
                ),
                'quality': _copy_selected_fields(
                    quality_map.get(symbol),
                    fields=FACTOR_ROW_FIELDS['quality'],
                ),
                'momentum': _copy_selected_fields(
                    momentum_map.get(symbol),
                    fields=FACTOR_ROW_FIELDS['momentum'],
                ),
                'advanced_value': _copy_selected_fields(
                    advanced_value_map.get(symbol),
                    fields=FACTOR_ROW_FIELDS['advanced_value'],
                ),
                'low_risk': _copy_selected_fields(
                    low_risk_map.get(symbol),
                    fields=FACTOR_ROW_FIELDS['low_risk'],
                ),
                'beta': _copy_selected_fields(
                    beta_map.get(symbol),
                    fields=FACTOR_ROW_FIELDS['beta'],
                ),
                'composite': _copy_selected_fields(
                    composite_map.get(symbol),
                    fields=FACTOR_ROW_FIELDS['composite'],
                ),
            }
            for family, row in persisted_factor_rows.items():
                if row is not None:
                    factor_row_counts[family] += 1
            context_completeness = {
                'factor_rows': {
                    family: _build_factor_row_completeness(family=family, row=row)
                    for family, row in persisted_factor_rows.items()
                },
                'signal_features': _build_signal_feature_completeness(signal_features),
                'market_relative_context': _build_market_relative_completeness(market_relative_context),
                'sector_peer_mapping': {
                    'status': 'unavailable',
                    'reason': 'No persisted sector or industry mapping is available for peer comparisons in the current symbol schema.',
                },
            }
            fundamentals = _build_fundamentals_summary(
                fundamentals_map.get(symbol, []),
                market_date=normalized_market_date,
            )
            recent_disclosures = [
                _serialize_disclosure(item) for item in disclosures_by_symbol.get(symbol, [])
            ]

            company_row = symbol_map.get(symbol)
            fundamentals_entries = fundamentals_map.get(symbol) or []
            latest_disclosure = recent_disclosures[0] if recent_disclosures else None
            company = {
                'symbol': symbol,
                'name_ko': company_row.get('name_ko') if company_row else None,
                'market': company_row.get('market') if company_row else None,
                'instrument_type': company_row.get('instrument_type') if company_row else None,
                'standard_code': company_row.get('standard_code') if company_row else None,
                'source': company_row.get('source') if company_row else None,
                'is_active': company_row.get('is_active') if company_row else None,
                'last_refreshed_at': (
                    company_row['last_refreshed_at'].isoformat()
                    if company_row and company_row.get('last_refreshed_at')
                    else None
                ),
                'dart': {
                    'corp_code': (
                        latest_disclosure.get('corp_code')
                        if latest_disclosure
                        else (fundamentals_entries[0].get('corp_code') if fundamentals_entries else None)
                    ),
                    'corp_name': (
                        latest_disclosure.get('corp_name')
                        if latest_disclosure
                        else (fundamentals_entries[0].get('corp_name') if fundamentals_entries else None)
                    ),
                },
            }

            availability = {
                'company': company_row is not None,
                'quote_context': quote_context is not None,
                'price_context': price_context is not None,
                'technical_snapshot': technical_snapshot is not None,
                'action_summary': action_summary is not None,
                'signal_features': signal_features is not None,
                'market_relative_context': (
                    isinstance(market_relative_context, dict)
                    and str(market_relative_context.get('status') or 'unavailable') != 'unavailable'
                ),
                'persisted_factor_rows': any(row is not None for row in persisted_factor_rows.values()),
                'fundamentals': fundamentals['latest_snapshot'] is not None,
                'recent_disclosures': bool(recent_disclosures),
            }
            if any(availability.values()):
                matched_symbols.append(symbol)
            else:
                missing_symbols.append(symbol)

            items.append(
                {
                    'symbol': symbol,
                    'market_date': normalized_market_date.isoformat(),
                    'company': company,
                    'quote_context': quote_context,
                    'price_context': price_context,
                    'price_context_history': price_context_history,
                    'technical_snapshot': technical_snapshot,
                    'action_summary': action_summary,
                    'signal_features': signal_features,
                    'market_relative_context': market_relative_context,
                    'scores': scores,
                    'persisted_factor_rows': persisted_factor_rows,
                    'fundamentals': fundamentals,
                    'recent_disclosures': {
                        'count': len(recent_disclosures),
                        'items': recent_disclosures,
                    },
                    'context_completeness': context_completeness,
                    'availability': availability,
                }
            )

        requested_symbol_count = len(normalized_symbols)
        factor_row_coverage = {
            family: {
                'present_rows': present_count,
                'missing_rows': requested_symbol_count - present_count,
                'coverage_ratio': _ratio(present_count, requested_symbol_count),
                'coverage_status': _coverage_status(
                    present_count=present_count,
                    requested_count=requested_symbol_count,
                ),
            }
            for family, present_count in factor_row_counts.items()
        }
        signal_feature_present_count = sum(1 for item in items if item['signal_features'] is not None)
        market_relative_available_count = sum(
            1
            for item in items
            if str((item.get('market_relative_context') or {}).get('status') or 'unavailable') == 'available'
        )
        market_relative_partial_count = sum(
            1
            for item in items
            if str((item.get('market_relative_context') or {}).get('status') or 'unavailable') == 'partial'
        )
        requested_symbol_coverage = {
            'requested_symbol_count': requested_symbol_count,
            'factor_rows': factor_row_coverage,
            'signal_features': {
                'present_rows': signal_feature_present_count,
                'missing_rows': requested_symbol_count - signal_feature_present_count,
                'coverage_ratio': _ratio(signal_feature_present_count, requested_symbol_count),
                'coverage_status': _coverage_status(
                    present_count=signal_feature_present_count,
                    requested_count=requested_symbol_count,
                    partial_label='sparse',
                ),
            },
            'market_relative_context': {
                'available_rows': market_relative_available_count,
                'partial_rows': market_relative_partial_count,
                'unavailable_rows': requested_symbol_count - market_relative_available_count - market_relative_partial_count,
                'coverage_ratio': _ratio(
                    market_relative_available_count + market_relative_partial_count,
                    requested_symbol_count,
                ),
                'coverage_status': (
                    'complete'
                    if market_relative_available_count == requested_symbol_count and requested_symbol_count > 0
                    else 'partial'
                    if (market_relative_available_count + market_relative_partial_count) > 0
                    else 'missing'
                ),
            },
            'sector_peer_mapping': {
                'available_rows': 0,
                'unavailable_rows': requested_symbol_count,
                'coverage_status': 'unavailable',
            },
        }
        for item in items:
            item['context_completeness']['requested_symbol_coverage'] = requested_symbol_coverage

        return {
            'request': {
                'market_date': normalized_market_date.isoformat(),
                'symbols': normalized_symbols,
                'fs_div': normalized_fs_div,
                'fs_label': FS_DIV_TO_STATEMENT_TYPE.get(normalized_fs_div) if normalized_fs_div else None,
                'disclosures_limit': disclosures_limit,
            },
            'summary': {
                'requested_symbol_count': len(normalized_symbols),
                'matched_symbol_count': len(matched_symbols),
                'missing_symbols': missing_symbols,
                'quote_context_count': sum(1 for item in items if item['quote_context'] is not None),
                'price_context_count': sum(1 for item in items if item['price_context'] is not None),
                'technical_snapshot_count': sum(1 for item in items if item['technical_snapshot'] is not None),
                'action_summary_count': sum(1 for item in items if item['action_summary'] is not None),
                'signal_feature_count': sum(1 for item in items if item['signal_features'] is not None),
                'market_relative_context_count': sum(1 for item in items if item['market_relative_context'] is not None),
                'fundamentals_count': sum(
                    1 for item in items if item['fundamentals']['latest_snapshot'] is not None
                ),
                'recent_disclosure_symbol_count': sum(
                    1 for item in items if item['recent_disclosures']['count'] > 0
                ),
                'factor_row_counts': factor_row_counts,
                'context_completeness': requested_symbol_coverage,
            },
            'items': items,
        }


agent_context_read_service = AgentContextReadService()
