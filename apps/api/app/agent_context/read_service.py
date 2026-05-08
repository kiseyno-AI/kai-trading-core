from __future__ import annotations

from calendar import monthrange
from collections import Counter
from datetime import date, datetime
from typing import Any, Sequence

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from app.agent_context.derived_contexts import DEFAULT_DERIVED_CONTEXT_HISTORY_DAYS, build_price_context
from app.agent_context.score_context import build_signal_decider_scores
from app.core.settings import settings
from app.db import session_scope
from app.db_models import (
    DailyAdvancedValueFactorResult,
    DailyBetaFactorResult,
    DailyCompositeFactorResult,
    DailyLowRiskFactorResult,
    DailyMomentumFactorResult,
    DailyQualityFactorResult,
    DailySignalFeature,
    DailyUnifiedValueFactorResult,
    DailyValueFactorResult,
    Symbol,
)
from app.dart.fundamentals_store import DartFundamentalsRepository, dart_fundamentals_repository
from app.dart.service import (
    FS_DIV_TO_STATEMENT_TYPE,
    REPORT_CODE_METADATA,
    build_fundamental_context_from_highlights,
    classify_disclosure_event,
)
from app.dart.store import DartSyncRepository, dart_sync_repository
from app.eod.pykrx_enrichment import pykrx_enrichment_service
from app.external_news.provider import NAVER_NEWS_PROVIDER, NaverNewsSearchProvider
from app.external_news.store import list_recent_external_news_for_symbols
from app.market_data_store import (
    load_current_kis_intraday_snapshots,
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
    load_daily_value_factor_results,
    load_current_quotes,
    load_symbols,
)
from app.signals.momentum_factor import enrich_momentum_audit_fields

PRICE_CONTEXT_HISTORY_DAYS = DEFAULT_DERIVED_CONTEXT_HISTORY_DAYS
FRESHNESS_STATUS_FRESH = 'fresh'
FRESHNESS_STATUS_STALE = 'stale'
FRESHNESS_STATUS_AVAILABLE_BUT_OLD = 'available_but_old'
FRESHNESS_STATUS_MISSING = 'missing'
PROVENANCE_STATUS_PROVIDER_SOURCED = 'provider_sourced'
PROVENANCE_STATUS_DERIVED = 'derived_from_provider_data'
PROVENANCE_STATUS_FALLBACK = 'fallback_from_owned_data'
PROVENANCE_STATUS_PROVIDER_PLUS_FALLBACK = 'provider_plus_fallback'
LIQUIDITY_TURNOVER_STATUS_AVAILABLE = 'available'
LIQUIDITY_TURNOVER_STATUS_DERIVED = 'derived'
LIQUIDITY_TURNOVER_STATUS_STALE = 'stale'
LIQUIDITY_TURNOVER_STATUS_MISSING_PRICE_OR_VOLUME = 'missing_price_or_volume'
LIQUIDITY_TURNOVER_STATUS_STORAGE_UNAVAILABLE = 'storage_unavailable'
PERSISTED_FACTOR_FAMILIES: tuple[str, ...] = (
    'value',
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
    'value': (
        'source',
        'feature_version',
        'market_date',
        'value_score',
        'value_rank',
        'value_percentile',
        'value_status',
        'per',
        'pbr',
        'dividend_yield',
        'per_status',
        'per_rank',
        'pbr_rank',
        'dividend_yield_rank',
        'per_percentile',
        'pbr_percentile',
        'dividend_yield_percentile',
        'captured_at',
    ),
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
        'momentum_eligibility_status',
        'momentum_percentile_threshold',
        'passes_momentum_top_percentile',
        'passes_trend_confirmation',
        'has_sufficient_cross_sectional_context',
        'is_recent_overheated',
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
    'value': ('value_status', 'per_status'),
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
    'momentum': ('momentum_eligibility_status',),
    'advanced_value': ('advanced_value_classification', 'fcf_yield_status', 'ev_to_ebitda_status'),
    'low_risk': ('low_risk_classification',),
    'beta': ('low_beta_classification',),
    'composite': ('composite_classification',),
}
FACTOR_ROW_PRIMARY_SCORE_FIELDS: dict[str, str] = {
    'value': 'value_score',
    'unified_value': 'unified_value_score',
    'quality': 'quality_score',
    'momentum': 'momentum_percentile',
    'advanced_value': 'advanced_value_score',
    'low_risk': 'low_risk_score',
    'beta': 'low_beta_score',
    'composite': 'composite_score',
}
FACTOR_ROW_SUBMETRIC_FIELDS: dict[str, tuple[str, ...]] = {
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
FACTOR_ROW_PRIMARY_TABLES: dict[str, str] = {
    'value': 'daily_value_factor_results',
    'unified_value': 'daily_unified_value_factor_results',
    'quality': 'daily_quality_factor_results',
    'momentum': 'daily_momentum_factor_results',
    'advanced_value': 'daily_advanced_value_factor_results',
    'low_risk': 'daily_low_risk_factor_results',
    'beta': 'daily_beta_factor_results',
    'composite': 'daily_composite_factor_results',
}
FACTOR_ROW_MODELS: dict[str, type[Any]] = {
    'value': DailyValueFactorResult,
    'unified_value': DailyUnifiedValueFactorResult,
    'quality': DailyQualityFactorResult,
    'momentum': DailyMomentumFactorResult,
    'advanced_value': DailyAdvancedValueFactorResult,
    'low_risk': DailyLowRiskFactorResult,
    'beta': DailyBetaFactorResult,
    'composite': DailyCompositeFactorResult,
}
FACTOR_ROW_SOURCE_PROVIDERS: dict[str, str] = {
    'value': 'internal_factor_pipeline',
    'unified_value': 'internal_factor_pipeline',
    'quality': 'internal_factor_pipeline',
    'momentum': 'internal_factor_pipeline',
    'advanced_value': 'internal_factor_pipeline',
    'low_risk': 'internal_factor_pipeline',
    'beta': 'internal_factor_pipeline',
    'composite': 'internal_factor_pipeline',
}
FACTOR_SUMMARY_SCORE_FIELDS: dict[str, str] = {
    'value': 'value_score',
    'unified_value': 'unified_value_score',
    'quality': 'quality_score',
    'momentum': 'momentum_percentile',
    'advanced_value': 'advanced_value_score',
    'low_risk': 'low_risk_score',
    'beta': 'low_beta_score',
    'composite': 'composite_score',
}
FACTOR_SUMMARY_RANK_FIELDS: dict[str, str | None] = {
    'value': 'value_rank',
    'unified_value': 'unified_value_rank',
    'quality': 'quality_rank',
    'momentum': 'momentum_rank',
    'advanced_value': 'advanced_value_rank',
    'low_risk': None,
    'beta': None,
    'composite': 'composite_rank',
}
FACTOR_SUMMARY_PERCENTILE_FIELDS: dict[str, str | None] = {
    'value': 'value_percentile',
    'unified_value': 'unified_value_percentile',
    'quality': 'quality_percentile',
    'momentum': 'momentum_percentile',
    'advanced_value': 'advanced_value_percentile',
    'low_risk': None,
    'beta': None,
    'composite': 'composite_percentile',
}
FACTOR_SUMMARY_RAW_STATUS_FIELDS: dict[str, str] = {
    'value': 'value_status',
    'unified_value': 'unified_value_status',
    'quality': 'quality_status',
    'momentum': 'momentum_eligibility_status',
    'advanced_value': 'advanced_value_classification',
    'low_risk': 'low_risk_classification',
    'beta': 'low_beta_classification',
    'composite': 'composite_classification',
}
FACTOR_SUMMARY_ELIGIBILITY_FIELDS: dict[str, str | None] = {
    'value': None,
    'unified_value': None,
    'quality': None,
    'momentum': 'is_momentum_eligible',
    'advanced_value': 'is_advanced_value_eligible',
    'low_risk': 'is_low_risk_eligible',
    'beta': 'is_low_beta_eligible',
    'composite': 'is_composite_eligible',
}
FACTOR_SUMMARY_STATUS_AVAILABLE = 'available'
FACTOR_SUMMARY_STATUS_NOT_ELIGIBLE = 'not_eligible'
FACTOR_SUMMARY_STATUS_INSUFFICIENT_HISTORY = 'insufficient_history'
FACTOR_SUMMARY_STATUS_MISSING_SOURCE_ROW = 'missing_source_row'
FACTOR_SUMMARY_STATUS_STORAGE_UNAVAILABLE = 'storage_unavailable'
FACTOR_SUMMARY_STATUS_NOT_COMPUTED_FOR_MARKET_DATE = 'not_computed_for_market_date'
FACTOR_SUMMARY_NOT_COMPUTED_RAW_STATUSES = frozenset({'missing_market_date_bar'})
FACTOR_SUMMARY_INSUFFICIENT_HISTORY_RAW_STATUSES = frozenset({'insufficient_history'})
FACTOR_SUMMARY_NOT_ELIGIBLE_RAW_STATUSES = frozenset(
    {
        'trend_not_confirmed',
        'below_top_percentile_cutoff',
        'recently_overheated',
        'watchlist',
        'ineligible',
        'liquidity_excluded',
        'hard_excluded',
        'insufficient_inputs',
        'insufficient_components',
        'insufficient_fundamentals',
    }
)
FACTOR_SUMMARY_SCORE_REQUIRED_FAMILIES = frozenset({'value', 'unified_value', 'quality'})


def _compact_provenance_summary(metadata: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(metadata, dict):
        return {}
    compact = {
        'freshness_status': metadata.get('freshness_status'),
        'provenance_status': metadata.get('provenance_status'),
        'issue_types': list(metadata.get('issue_types') or []),
        'as_of_market_date': metadata.get('as_of_market_date'),
        'latest_available_market_date': metadata.get('latest_available_market_date'),
        'source_tables': list(metadata.get('source_tables') or []),
        'reason': metadata.get('reason'),
    }
    return {key: value for key, value in compact.items() if value not in (None, [], {})}


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
    highlights = _summarize_highlights(item.get('highlights') or [])
    filing_metadata = {
        'rcept_no': item['rcept_no'],
        'receipt_date': item['disclosure'].get('receipt_date'),
        'captured_at': item.get('captured_at'),
        'report_name': item['disclosure'].get('report_nm'),
        'filing_category': item['filing_category'],
    }
    period_metadata = {
        'business_year': item['business_year'],
        'report_code': item['report_code'],
        'report_label': report_meta.get('label'),
        'period_type': item['period_type'],
        'period_scope': report_meta.get('scope'),
        'quarter': report_meta.get('quarter'),
        'period_end_month': item.get('period_end_month'),
        'period_end_date': period_end_date.isoformat() if period_end_date else None,
    }
    statement_metadata = {
        'fs_div': item['fs_div'],
        'fs_label': FS_DIV_TO_STATEMENT_TYPE.get(item['fs_div']),
        'statement_type': item['statement_type'],
        'currency': item.get('currency'),
        'source_status': item.get('source_status'),
        'source_message': item.get('source_message'),
        'dataset': 'stored_dart_fundamentals',
        'source_provider': 'dart',
    }
    disclosure_flags = classify_disclosure_event(
        report_name=item['disclosure'].get('report_nm'),
        remark=item['disclosure'].get('remark'),
        filing_category=item['filing_category'],
        is_financial_report=True,
    )
    return {
        'filing': {
            'rcept_no': item['rcept_no'],
            'report_name': item['disclosure'].get('report_nm'),
            'filing_category': item['filing_category'],
            'receipt_date': item['disclosure'].get('receipt_date'),
            'captured_at': item.get('captured_at'),
            'is_correction': disclosure_flags['is_correction'],
            'disclosure_tags': disclosure_flags['disclosure_tags'],
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
        'highlights': highlights,
        'fundamental_context': build_fundamental_context_from_highlights(
            highlights,
            filing_metadata=filing_metadata,
            period_metadata=period_metadata,
            statement_metadata=statement_metadata,
        ),
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
    disclosure_flags = classify_disclosure_event(
        report_name=item.get('report_nm'),
        remark=item.get('remark'),
        filing_category=item.get('filing_category'),
        is_financial_report=item.get('is_financial_report'),
    )
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
        'is_correction': item.get('is_correction', disclosure_flags['is_correction']),
        'is_ownership_related': item.get('is_ownership_related', disclosure_flags['is_ownership_related']),
        'is_large_holder_related': item.get('is_large_holder_related', disclosure_flags['is_large_holder_related']),
        'is_audit_related': item.get('is_audit_related', disclosure_flags['is_audit_related']),
        'disclosure_tags': item.get('disclosure_tags', disclosure_flags['disclosure_tags']),
    }


def _build_disclosure_context(
    *,
    disclosures: list[dict[str, Any]],
    latest_snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
    snapshot_rcept_no = None
    if isinstance(latest_snapshot, dict):
        snapshot_rcept_no = ((latest_snapshot.get('filing') or {}).get('rcept_no') or '').strip() or None

    financial_reports = [item for item in disclosures if item.get('is_financial_report')]
    corrections = [item for item in disclosures if item.get('is_correction')]
    ownership_related = [item for item in disclosures if item.get('is_ownership_related')]
    large_holder_related = [item for item in disclosures if item.get('is_large_holder_related')]
    audit_related = [item for item in disclosures if item.get('is_audit_related')]
    linked_snapshot_disclosure = next(
        (item for item in disclosures if snapshot_rcept_no and item.get('rcept_no') == snapshot_rcept_no),
        None,
    )
    newer_financial_reports = [
        item
        for item in financial_reports
        if snapshot_rcept_no is not None and item.get('rcept_no') != snapshot_rcept_no
    ]
    return {
        'summary': {
            'recent_disclosure_count': len(disclosures),
            'financial_report_count': len(financial_reports),
            'correction_count': len(corrections),
            'ownership_related_count': len(ownership_related),
            'large_holder_related_count': len(large_holder_related),
            'audit_related_count': len(audit_related),
            'newer_financial_report_count': len(newer_financial_reports),
        },
        'latest_snapshot_relation': {
            'snapshot_rcept_no': snapshot_rcept_no,
            'linked_recent_disclosure_rcept_no': linked_snapshot_disclosure.get('rcept_no') if linked_snapshot_disclosure else None,
            'has_linked_recent_disclosure': linked_snapshot_disclosure is not None,
        },
        'recent_corrections': corrections[:3],
        'recent_ownership_large_holder_changes': [
            item
            for item in disclosures
            if item.get('is_ownership_related') or item.get('is_large_holder_related')
        ][:3],
        'recent_financial_reports': financial_reports[:3],
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


def _maybe_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace('Z', '+00:00')
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _parse_iso_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _trade_date_relation_to_request(*, as_of_market_date: str | None, requested_market_date: date) -> str | None:
    parsed_as_of = _parse_iso_date(as_of_market_date)
    if parsed_as_of is None:
        return None
    if parsed_as_of == requested_market_date:
        return 'same_market_date'
    if parsed_as_of < requested_market_date:
        return 'before_market_date'
    return 'after_market_date'


def _resolve_turnover_candidate(price_context: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(price_context, dict):
        return None

    liquidity = _maybe_dict(price_context.get('liquidity'))
    price_bar_quality = _maybe_dict(price_context.get('price_bar_quality'))
    close_price = _to_int(price_context.get('close_price'))
    volume = _to_int(price_context.get('volume'))
    raw_turnover = _to_int(price_context.get('turnover'))
    provider_turnover_krw = _to_int(liquidity.get('provider_turnover_krw'))
    if provider_turnover_krw in (None, 0) and raw_turnover not in (None, 0):
        provider_turnover_krw = raw_turnover
    derived_turnover_krw = _to_int(liquidity.get('estimated_turnover_krw'))
    if derived_turnover_krw is None and close_price not in (None, 0) and volume not in (None, 0):
        derived_turnover_krw = close_price * volume
    turnover_krw = _to_int(liquidity.get('turnover_krw'))
    if turnover_krw in (None, 0):
        turnover_krw = provider_turnover_krw or derived_turnover_krw

    if provider_turnover_krw not in (None, 0):
        status = LIQUIDITY_TURNOVER_STATUS_AVAILABLE
        reason = None
        turnover_source = 'provider'
    elif derived_turnover_krw not in (None, 0):
        status = LIQUIDITY_TURNOVER_STATUS_DERIVED
        reason = 'derived_from_close_price_x_volume'
        turnover_source = 'derived'
    else:
        status = LIQUIDITY_TURNOVER_STATUS_MISSING_PRICE_OR_VOLUME
        turnover_source = None
        if close_price in (None, 0) and volume in (None, 0):
            reason = 'close_price_and_volume_missing_or_zero'
        elif close_price in (None, 0):
            reason = 'close_price_missing_or_zero'
        elif volume in (None, 0):
            reason = 'volume_missing_or_zero'
        else:
            reason = 'turnover_missing_without_derivable_close_volume'

    return {
        'trade_date': price_context.get('trade_date'),
        'source': price_context.get('source'),
        'captured_at': price_context.get('captured_at'),
        'close_price': close_price,
        'volume': volume,
        'turnover': raw_turnover,
        'turnover_krw': turnover_krw,
        'provider_turnover_krw': provider_turnover_krw,
        'derived_turnover_krw': derived_turnover_krw,
        'turnover_source': turnover_source,
        'status': status,
        'reason': reason,
        'price_bar_issue_codes': list(price_bar_quality.get('issue_codes') or []),
    }


def _build_turnover_context(
    *,
    requested_market_date: date,
    price_context: dict[str, Any] | None,
    price_context_history: Sequence[dict[str, Any]],
    freshness_provenance: dict[str, Any],
    storage_unavailable: bool = False,
) -> dict[str, Any]:
    if storage_unavailable:
        return {
            'status': LIQUIDITY_TURNOVER_STATUS_STORAGE_UNAVAILABLE,
            'reason': 'daily_price_context_storage_unavailable',
            'provenance': _compact_provenance_summary(freshness_provenance),
        }

    current_candidate = _resolve_turnover_candidate(price_context)
    if current_candidate is not None:
        return {
            'status': current_candidate['status'],
            'reason': current_candidate['reason'],
            'trade_date': current_candidate['trade_date'],
            'trade_date_relation_to_request': _trade_date_relation_to_request(
                as_of_market_date=current_candidate['trade_date'],
                requested_market_date=requested_market_date,
            ),
            'freshness_status': freshness_provenance.get('freshness_status'),
            'source': current_candidate['source'],
            'captured_at': current_candidate['captured_at'],
            'close_price': current_candidate['close_price'],
            'volume': current_candidate['volume'],
            'turnover': current_candidate['turnover'],
            'turnover_krw': current_candidate['turnover_krw'],
            'turnover_source': current_candidate['turnover_source'],
            'provider_turnover_krw': current_candidate['provider_turnover_krw'],
            'derived_turnover_krw': current_candidate['derived_turnover_krw'],
            'price_bar_issue_codes': current_candidate['price_bar_issue_codes'],
            'provenance': _compact_provenance_summary(freshness_provenance),
        }

    latest_history_row = price_context_history[-1] if price_context_history else None
    stale_candidate = _resolve_turnover_candidate(
        latest_history_row.get('price_context') if isinstance(latest_history_row, dict) else None
    )
    if stale_candidate is not None:
        return {
            'status': LIQUIDITY_TURNOVER_STATUS_STALE,
            'reason': 'latest_price_context_before_requested_market_date',
            'trade_date': stale_candidate['trade_date'],
            'trade_date_relation_to_request': _trade_date_relation_to_request(
                as_of_market_date=stale_candidate['trade_date'],
                requested_market_date=requested_market_date,
            ),
            'freshness_status': FRESHNESS_STATUS_STALE,
            'source': stale_candidate['source'],
            'captured_at': stale_candidate['captured_at'],
            'close_price': stale_candidate['close_price'],
            'volume': stale_candidate['volume'],
            'turnover': stale_candidate['turnover'],
            'turnover_krw': stale_candidate['turnover_krw'],
            'turnover_source': stale_candidate['turnover_source'],
            'provider_turnover_krw': stale_candidate['provider_turnover_krw'],
            'derived_turnover_krw': stale_candidate['derived_turnover_krw'],
            'stale_from_status': stale_candidate['status'],
            'price_bar_issue_codes': stale_candidate['price_bar_issue_codes'],
            'provenance': _compact_provenance_summary(freshness_provenance),
        }

    return {
        'status': LIQUIDITY_TURNOVER_STATUS_MISSING_PRICE_OR_VOLUME,
        'reason': 'no_price_context_available_for_requested_market_date',
        'freshness_status': freshness_provenance.get('freshness_status'),
        'provenance': _compact_provenance_summary(freshness_provenance),
    }


def _build_liquidity_ranking_context(
    *,
    row: dict[str, Any] | None,
    latest_prior_row: dict[str, Any] | None,
    freshness_provenance: dict[str, Any],
    current_storage_unavailable: bool = False,
    prior_storage_unavailable: bool = False,
) -> dict[str, Any]:
    if current_storage_unavailable or (row is None and prior_storage_unavailable):
        return {
            'status': LIQUIDITY_TURNOVER_STATUS_STORAGE_UNAVAILABLE,
            'reason': 'low_risk_factor_storage_unavailable',
            'provenance': _compact_provenance_summary(freshness_provenance),
        }

    source_row = row if row is not None else latest_prior_row
    if source_row is None:
        return {
            'status': 'missing',
            'reason': 'no_low_risk_liquidity_row_for_or_before_request_date',
            'provenance': _compact_provenance_summary(freshness_provenance),
        }

    avg_trading_value_20d = _to_float(source_row.get('avg_trading_value_20d'))
    liquidity_rank = _to_int(source_row.get('liquidity_rank'))
    liquidity_percentile = _to_float(source_row.get('liquidity_percentile'))
    passes_liquidity_floor = source_row.get('passes_liquidity_floor')
    if all(value is None for value in (avg_trading_value_20d, liquidity_rank, liquidity_percentile, passes_liquidity_floor)):
        return {
            'status': 'missing',
            'reason': 'low_risk_liquidity_fields_missing',
            'as_of_market_date': source_row.get('market_date'),
            'freshness_status': freshness_provenance.get('freshness_status'),
            'source': source_row.get('source'),
            'captured_at': source_row.get('captured_at'),
            'provenance': _compact_provenance_summary(freshness_provenance),
        }

    status = 'available' if row is not None else LIQUIDITY_TURNOVER_STATUS_STALE
    reason = None if row is not None else 'latest_prior_low_risk_liquidity_row_available'
    return {
        'status': status,
        'reason': reason,
        'as_of_market_date': source_row.get('market_date'),
        'freshness_status': freshness_provenance.get('freshness_status'),
        'source': source_row.get('source'),
        'captured_at': source_row.get('captured_at'),
        'avg_trading_value_20d': avg_trading_value_20d,
        'liquidity_rank': liquidity_rank,
        'liquidity_percentile': liquidity_percentile,
        'passes_liquidity_floor': passes_liquidity_floor,
        'ranking_basis': 'avg_trading_value_20d',
        'provenance': _compact_provenance_summary(freshness_provenance),
    }


def _build_intraday_liquidity_context(
    *,
    quote_context: dict[str, Any] | None,
    quote_freshness_provenance: dict[str, Any],
    intraday_execution_context: dict[str, Any] | None,
    intraday_execution_provenance: dict[str, Any],
) -> dict[str, Any]:
    if intraday_execution_context is None and quote_context is None:
        return {
            'status': 'unavailable',
            'reason': 'no_intraday_or_quote_context_available',
            'provenance': {
                'quote': _compact_provenance_summary(quote_freshness_provenance),
                'intraday_execution_context': _compact_provenance_summary(intraday_execution_provenance),
            },
        }

    intraday_status = (
        str(intraday_execution_context.get('status') or 'available')
        if isinstance(intraday_execution_context, dict)
        else 'unavailable'
    )
    spread = _maybe_dict((quote_context or {}).get('spread'))
    intraday_quote = _maybe_dict((intraday_execution_context or {}).get('quote'))
    intraday_orderbook = _maybe_dict((intraday_execution_context or {}).get('orderbook'))
    intraday_liquidity = _maybe_dict((intraday_execution_context or {}).get('liquidity'))
    quote_spread_points = _to_int(spread.get('spread_points'))
    if quote_spread_points is None:
        quote_spread_points = _to_int(intraday_quote.get('spread_points'))
    return {
        'status': intraday_status if intraday_execution_context is not None else ('available' if quote_context is not None else 'unavailable'),
        'reason': intraday_execution_provenance.get('reason') if intraday_execution_context is not None else quote_freshness_provenance.get('reason'),
        'freshness_status': (
            intraday_execution_provenance.get('freshness_status')
            if intraday_execution_context is not None
            else quote_freshness_provenance.get('freshness_status')
        ),
        'quote_freshness_status': quote_freshness_provenance.get('freshness_status'),
        'quote_trade_date': (quote_context or {}).get('trade_date'),
        'as_of_market_date': (intraday_execution_context or {}).get('as_of_market_date'),
        'as_of_timestamp': (intraday_execution_context or {}).get('as_of_timestamp'),
        'quote_spread_points': quote_spread_points,
        'quote_spread_bps_of_price': _to_float(spread.get('spread_bps_of_price')),
        'orderbook_imbalance': _to_float(intraday_orderbook.get('imbalance')),
        'volume_power_rank': _to_int(intraday_liquidity.get('volume_power_rank')),
        'volume_rank': _to_int(intraday_liquidity.get('volume_rank')),
        'quote_balance_rank': _to_int(intraday_liquidity.get('quote_balance_rank')),
        'provenance': {
            'quote': _compact_provenance_summary(quote_freshness_provenance),
            'intraday_execution_context': _compact_provenance_summary(intraday_execution_provenance),
        },
    }


def _build_compact_liquidity_context(
    *,
    requested_market_date: date,
    quote_context: dict[str, Any] | None,
    quote_freshness_provenance: dict[str, Any],
    price_context: dict[str, Any] | None,
    price_context_history: Sequence[dict[str, Any]],
    price_context_freshness_provenance: dict[str, Any],
    price_context_storage_unavailable: bool,
    low_risk_row: dict[str, Any] | None,
    latest_prior_low_risk_row: dict[str, Any] | None,
    low_risk_freshness_provenance: dict[str, Any],
    low_risk_storage_unavailable: bool,
    low_risk_prior_storage_unavailable: bool,
    intraday_execution_context: dict[str, Any] | None,
    intraday_execution_freshness_provenance: dict[str, Any],
) -> dict[str, Any]:
    turnover_context = _build_turnover_context(
        requested_market_date=requested_market_date,
        price_context=price_context,
        price_context_history=price_context_history,
        freshness_provenance=price_context_freshness_provenance,
        storage_unavailable=price_context_storage_unavailable,
    )
    ranking_context = _build_liquidity_ranking_context(
        row=low_risk_row,
        latest_prior_row=latest_prior_low_risk_row,
        freshness_provenance=low_risk_freshness_provenance,
        current_storage_unavailable=low_risk_storage_unavailable,
        prior_storage_unavailable=low_risk_prior_storage_unavailable,
    )
    intraday_context = _build_intraday_liquidity_context(
        quote_context=quote_context,
        quote_freshness_provenance=quote_freshness_provenance,
        intraday_execution_context=intraday_execution_context,
        intraday_execution_provenance=intraday_execution_freshness_provenance,
    )
    return {
        'status': turnover_context.get('status'),
        'reason': turnover_context.get('reason'),
        'turnover': turnover_context,
        'ranking': ranking_context,
        'intraday': intraday_context,
    }


def _issue_types_for_metadata(*, freshness_status: str, provenance_status: str) -> list[str]:
    issue_types: list[str] = []
    if freshness_status == FRESHNESS_STATUS_MISSING:
        issue_types.append('missing')
    elif freshness_status == FRESHNESS_STATUS_STALE:
        issue_types.append('stale')
    elif freshness_status == FRESHNESS_STATUS_AVAILABLE_BUT_OLD:
        issue_types.append('available_but_old')
    if provenance_status in {PROVENANCE_STATUS_FALLBACK, PROVENANCE_STATUS_PROVIDER_PLUS_FALLBACK}:
        issue_types.append('derived_or_fallback')
    return issue_types


def _build_freshness_provenance(
    *,
    requested_market_date: date,
    freshness_status: str,
    provenance_status: str,
    source_tables: Sequence[str],
    source_provider: str | None,
    as_of_market_date: str | None = None,
    as_of_timestamp: str | None = None,
    latest_available_market_date: str | None = None,
    latest_available_timestamp: str | None = None,
    row_count: int | None = None,
    history_row_count: int | None = None,
    trade_date_relation_to_request: str | None = None,
    reason: str | None = None,
    requested_symbol_coverage: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metadata = {
        'freshness_status': freshness_status,
        'provenance_status': provenance_status,
        'issue_types': _issue_types_for_metadata(
            freshness_status=freshness_status,
            provenance_status=provenance_status,
        ),
        'requested_market_date': requested_market_date.isoformat(),
        'as_of_market_date': as_of_market_date,
        'as_of_timestamp': as_of_timestamp,
        'latest_available_market_date': latest_available_market_date,
        'latest_available_timestamp': latest_available_timestamp,
        'trade_date_relation_to_request': trade_date_relation_to_request,
        'source_tables': list(source_tables),
        'source_provider': source_provider,
        'row_count': row_count,
        'history_row_count': history_row_count,
        'reason': reason,
        'is_missing': freshness_status == FRESHNESS_STATUS_MISSING,
        'is_stale': freshness_status == FRESHNESS_STATUS_STALE,
        'is_available_but_old': freshness_status == FRESHNESS_STATUS_AVAILABLE_BUT_OLD,
        'is_provider_sourced': provenance_status == PROVENANCE_STATUS_PROVIDER_SOURCED,
        'is_derived': provenance_status in {
            PROVENANCE_STATUS_DERIVED,
            PROVENANCE_STATUS_FALLBACK,
            PROVENANCE_STATUS_PROVIDER_PLUS_FALLBACK,
        },
        'is_fallback': provenance_status in {
            PROVENANCE_STATUS_FALLBACK,
            PROVENANCE_STATUS_PROVIDER_PLUS_FALLBACK,
        },
    }
    if requested_symbol_coverage is not None:
        metadata['requested_symbol_coverage'] = requested_symbol_coverage
    if extra:
        metadata.update(extra)
    return metadata


def _load_latest_prior_rows_for_model(
    *,
    model: type[Any],
    market_date: date,
    symbols: Sequence[str],
) -> dict[str, dict[str, Any]]:
    normalized_symbols = list(dict.fromkeys(symbols))
    if not normalized_symbols:
        return {}

    with session_scope() as session:
        ranked = (
            select(
                Symbol.symbol.label('symbol'),
                model.market_date.label('market_date'),
                model.captured_at.label('captured_at'),
                model.source.label('source'),
                func.row_number()
                .over(
                    partition_by=model.symbol_id,
                    order_by=(model.market_date.desc(), model.captured_at.desc(), model.id.desc()),
                )
                .label('row_rank'),
            )
            .join(Symbol, Symbol.id == model.symbol_id)
            .where(
                Symbol.symbol.in_(normalized_symbols),
                model.market_date < market_date,
            )
            .subquery()
        )
        rows = session.execute(select(ranked).where(ranked.c.row_rank == 1)).all()

    return {
        row.symbol: {
            'market_date': row.market_date.isoformat() if row.market_date else None,
            'captured_at': row.captured_at.isoformat() if row.captured_at else None,
            'source': row.source,
        }
        for row in rows
    }


def _load_latest_symbol_day_block_history(
    *,
    market_date: date,
    symbols: Sequence[str],
) -> dict[str, Any]:
    factor_rows: dict[str, dict[str, dict[str, Any]]] = {}
    factor_storage_unavailable: dict[str, bool] = {}
    for family, model in FACTOR_ROW_MODELS.items():
        try:
            factor_rows[family] = _load_latest_prior_rows_for_model(
                model=model,
                market_date=market_date,
                symbols=symbols,
            )
            factor_storage_unavailable[family] = False
        except SQLAlchemyError:
            factor_rows[family] = {}
            factor_storage_unavailable[family] = True
    try:
        signal_features = _load_latest_prior_rows_for_model(
            model=DailySignalFeature,
            market_date=market_date,
            symbols=symbols,
        )
        signal_features_storage_unavailable = False
    except SQLAlchemyError:
        signal_features = {}
        signal_features_storage_unavailable = True
    return {
        'factor_rows': factor_rows,
        'factor_storage_unavailable': factor_storage_unavailable,
        'signal_features': signal_features,
        'signal_features_storage_unavailable': signal_features_storage_unavailable,
    }


def _build_factor_row_freshness_provenance(
    *,
    family: str,
    row: dict[str, Any] | None,
    latest_prior_row: dict[str, Any] | None,
    requested_market_date: date,
    current_storage_unavailable: bool = False,
    prior_storage_unavailable: bool = False,
) -> dict[str, Any]:
    if current_storage_unavailable or (row is None and prior_storage_unavailable):
        return _build_freshness_provenance(
            requested_market_date=requested_market_date,
            freshness_status=FRESHNESS_STATUS_MISSING,
            provenance_status=FACTOR_SUMMARY_STATUS_STORAGE_UNAVAILABLE,
            source_tables=(FACTOR_ROW_PRIMARY_TABLES[family],),
            source_provider=FACTOR_ROW_SOURCE_PROVIDERS[family],
            row_count=0,
            reason=f'{family}_factor_storage_unavailable',
        )

    requested_market_date_iso = requested_market_date.isoformat()
    current_market_date = row.get('market_date') if row is not None else None
    current_captured_at = row.get('captured_at') if row is not None else None
    if row is not None and current_market_date == requested_market_date_iso:
        freshness_status = FRESHNESS_STATUS_FRESH
        as_of_market_date = current_market_date
        as_of_timestamp = current_captured_at
        latest_available_market_date = current_market_date
        latest_available_timestamp = current_captured_at
        reason = None
    elif latest_prior_row is not None:
        freshness_status = FRESHNESS_STATUS_AVAILABLE_BUT_OLD
        as_of_market_date = None
        as_of_timestamp = None
        latest_available_market_date = latest_prior_row.get('market_date')
        latest_available_timestamp = latest_prior_row.get('captured_at')
        reason = 'latest_prior_factor_row_available'
    else:
        freshness_status = FRESHNESS_STATUS_MISSING
        as_of_market_date = None
        as_of_timestamp = None
        latest_available_market_date = None
        latest_available_timestamp = None
        reason = 'no_stored_factor_row_for_or_before_request_date'

    source_provider = (
        row.get('source')
        if row is not None
        else latest_prior_row.get('source')
        if latest_prior_row is not None
        else FACTOR_ROW_SOURCE_PROVIDERS[family]
    )
    trade_date_relation = _trade_date_relation_to_request(
        as_of_market_date=current_market_date if row is not None else latest_prior_row.get('market_date') if latest_prior_row else None,
        requested_market_date=requested_market_date,
    )
    return _build_freshness_provenance(
        requested_market_date=requested_market_date,
        freshness_status=freshness_status,
        provenance_status=PROVENANCE_STATUS_DERIVED,
        source_tables=(FACTOR_ROW_PRIMARY_TABLES[family],),
        source_provider=source_provider,
        as_of_market_date=as_of_market_date,
        as_of_timestamp=as_of_timestamp,
        latest_available_market_date=latest_available_market_date,
        latest_available_timestamp=latest_available_timestamp,
        row_count=1 if row is not None else 0,
        trade_date_relation_to_request=trade_date_relation,
        reason=reason,
    )


def _build_quote_freshness_provenance(
    *,
    row: dict[str, Any] | None,
    requested_market_date: date,
) -> dict[str, Any]:
    if row is None:
        return _build_freshness_provenance(
            requested_market_date=requested_market_date,
            freshness_status=FRESHNESS_STATUS_MISSING,
            provenance_status=PROVENANCE_STATUS_PROVIDER_SOURCED,
            source_tables=('current_quote_cache', 'quote_snapshots'),
            source_provider='kis',
            row_count=0,
            reason='no_current_quote_cache_row',
        )

    quote_trade_date = row.get('trade_date')
    trade_date_relation = _trade_date_relation_to_request(
        as_of_market_date=quote_trade_date,
        requested_market_date=requested_market_date,
    )
    freshness_status = (
        FRESHNESS_STATUS_FRESH
        if trade_date_relation in {None, 'same_market_date'}
        else FRESHNESS_STATUS_STALE
    )
    provenance_status = (
        PROVENANCE_STATUS_PROVIDER_PLUS_FALLBACK
        if bool(row.get('fallback'))
        else PROVENANCE_STATUS_PROVIDER_SOURCED
    )
    return _build_freshness_provenance(
        requested_market_date=requested_market_date,
        freshness_status=freshness_status,
        provenance_status=provenance_status,
        source_tables=('current_quote_cache', 'quote_snapshots'),
        source_provider=str(row.get('source') or 'kis'),
        as_of_market_date=quote_trade_date,
        as_of_timestamp=row.get('captured_at') or row.get('cached_at'),
        latest_available_market_date=quote_trade_date,
        latest_available_timestamp=row.get('captured_at') or row.get('cached_at'),
        row_count=1,
        trade_date_relation_to_request=trade_date_relation,
        reason='quote_fallback_used' if bool(row.get('fallback')) else None,
    )


def _build_price_context_freshness_provenance(
    *,
    price_context: dict[str, Any] | None,
    price_context_history: list[dict[str, Any]],
    requested_market_date: date,
    storage_unavailable: bool = False,
) -> dict[str, Any]:
    if storage_unavailable:
        return _build_freshness_provenance(
            requested_market_date=requested_market_date,
            freshness_status=FRESHNESS_STATUS_MISSING,
            provenance_status=PROVENANCE_STATUS_DERIVED,
            source_tables=('daily_price_contexts', 'daily_price_bars'),
            source_provider='pykrx',
            row_count=0,
            history_row_count=0,
            reason='daily_price_context_storage_unavailable',
        )

    latest_history = price_context_history[-1] if price_context_history else None
    liquidity = price_context.get('liquidity') if isinstance(price_context, dict) else None
    price_bar_quality = price_context.get('price_bar_quality') if isinstance(price_context, dict) else None
    turnover_is_estimated = bool((liquidity or {}).get('turnover_is_estimated'))
    issue_codes = list((price_bar_quality or {}).get('issue_codes') or [])
    if price_context is not None:
        trade_date = price_context.get('trade_date')
        trade_date_relation = _trade_date_relation_to_request(
            as_of_market_date=trade_date,
            requested_market_date=requested_market_date,
        )
        freshness_status = (
            FRESHNESS_STATUS_FRESH
            if trade_date_relation in {None, 'same_market_date'}
            else FRESHNESS_STATUS_STALE
        )
        provenance_status = (
            PROVENANCE_STATUS_PROVIDER_PLUS_FALLBACK
            if turnover_is_estimated
            else PROVENANCE_STATUS_DERIVED
        )
        return _build_freshness_provenance(
            requested_market_date=requested_market_date,
            freshness_status=freshness_status,
            provenance_status=provenance_status,
            source_tables=('daily_price_contexts', 'daily_price_bars'),
            source_provider=str(price_context.get('source') or 'pykrx'),
            as_of_market_date=trade_date,
            as_of_timestamp=price_context.get('captured_at'),
            latest_available_market_date=trade_date,
            latest_available_timestamp=price_context.get('captured_at'),
            row_count=1,
            history_row_count=len(price_context_history),
            trade_date_relation_to_request=trade_date_relation,
            reason='estimated_turnover_from_owned_bars' if turnover_is_estimated else None,
            extra={
                'turnover_source': (liquidity or {}).get('turnover_source'),
                'price_bar_issue_codes': issue_codes,
            },
        )

    latest_trade_date = latest_history.get('market_date') if latest_history is not None else None
    latest_source = latest_history.get('source') if latest_history is not None else 'pykrx'
    return _build_freshness_provenance(
        requested_market_date=requested_market_date,
        freshness_status=FRESHNESS_STATUS_STALE if latest_history is not None else FRESHNESS_STATUS_MISSING,
        provenance_status=PROVENANCE_STATUS_DERIVED,
        source_tables=('daily_price_contexts', 'daily_price_bars'),
        source_provider=str(latest_source or 'pykrx'),
        latest_available_market_date=latest_trade_date,
        latest_available_timestamp=latest_history.get('captured_at') if latest_history is not None else None,
        row_count=0,
        history_row_count=len(price_context_history),
        trade_date_relation_to_request=_trade_date_relation_to_request(
            as_of_market_date=latest_trade_date,
            requested_market_date=requested_market_date,
        ),
        reason='latest_prior_price_context_available' if latest_history is not None else 'no_stored_price_context_for_or_before_request_date',
    )


def _build_signal_features_freshness_provenance(
    *,
    signal_features: dict[str, Any] | None,
    latest_prior_row: dict[str, Any] | None,
    requested_market_date: date,
    current_storage_unavailable: bool = False,
    prior_storage_unavailable: bool = False,
) -> dict[str, Any]:
    if current_storage_unavailable or (signal_features is None and prior_storage_unavailable):
        return _build_freshness_provenance(
            requested_market_date=requested_market_date,
            freshness_status=FRESHNESS_STATUS_MISSING,
            provenance_status=FACTOR_SUMMARY_STATUS_STORAGE_UNAVAILABLE,
            source_tables=('daily_signal_features',),
            source_provider='kis_ranking',
            row_count=0,
            reason='signal_features_storage_unavailable',
        )

    if signal_features is not None:
        row_freshness = signal_features.get('freshness') if isinstance(signal_features.get('freshness'), dict) else {}
        as_of_market_date = str(row_freshness.get('market_date') or signal_features.get('market_date') or '')
        if not as_of_market_date:
            as_of_market_date = requested_market_date.isoformat()
        row_kind = str(signal_features.get('row_kind') or (signal_features.get('provenance') or {}).get('row_kind') or '')
        if row_kind == 'fallback_only':
            provenance_status = PROVENANCE_STATUS_FALLBACK
        elif row_kind == 'direct_plus_fallback':
            provenance_status = PROVENANCE_STATUS_PROVIDER_PLUS_FALLBACK
        else:
            provenance_status = PROVENANCE_STATUS_PROVIDER_SOURCED
        trade_date_relation = _trade_date_relation_to_request(
            as_of_market_date=as_of_market_date,
            requested_market_date=requested_market_date,
        )
        freshness_status = (
            FRESHNESS_STATUS_FRESH
            if trade_date_relation in {None, 'same_market_date'}
            else FRESHNESS_STATUS_STALE
        )
        return _build_freshness_provenance(
            requested_market_date=requested_market_date,
            freshness_status=freshness_status,
            provenance_status=provenance_status,
            source_tables=('daily_signal_features',),
            source_provider=str(signal_features.get('source') or 'kis_ranking'),
            as_of_market_date=as_of_market_date,
            as_of_timestamp=signal_features.get('captured_at'),
            latest_available_market_date=as_of_market_date,
            latest_available_timestamp=signal_features.get('captured_at'),
            row_count=1,
            trade_date_relation_to_request=trade_date_relation,
            reason='owned_fallback_features_present' if provenance_status != PROVENANCE_STATUS_PROVIDER_SOURCED else None,
            extra={
                'row_kind': row_kind or None,
                'direct_signal_source_count': (
                    (signal_features.get('provenance') or {}).get('direct_signal_source_count')
                ),
                'fallback_groups_present': list(
                    (signal_features.get('provenance') or {}).get('fallback_groups_present') or []
                ),
            },
        )

    latest_available_market_date = latest_prior_row.get('market_date') if latest_prior_row is not None else None
    return _build_freshness_provenance(
        requested_market_date=requested_market_date,
        freshness_status=FRESHNESS_STATUS_STALE if latest_prior_row is not None else FRESHNESS_STATUS_MISSING,
        provenance_status=PROVENANCE_STATUS_PROVIDER_SOURCED,
        source_tables=('daily_signal_features',),
        source_provider=str(latest_prior_row.get('source') or 'kis_ranking') if latest_prior_row is not None else 'kis_ranking',
        latest_available_market_date=latest_available_market_date,
        latest_available_timestamp=latest_prior_row.get('captured_at') if latest_prior_row is not None else None,
        row_count=0,
        trade_date_relation_to_request=_trade_date_relation_to_request(
            as_of_market_date=latest_available_market_date,
            requested_market_date=requested_market_date,
        ),
        reason='latest_prior_signal_feature_row_available' if latest_prior_row is not None else 'no_stored_signal_feature_row_for_or_before_request_date',
    )


def _build_intraday_execution_context(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None

    return {
        'status': str(row.get('availability_status') or 'available'),
        'market_state': row.get('market_state'),
        'market_code': row.get('market_code'),
        'source': row.get('source'),
        'feature_version': row.get('feature_version'),
        'as_of_market_date': row.get('as_of_market_date'),
        'as_of_timestamp': row.get('as_of_timestamp'),
        'coverage': row.get('coverage'),
        'availability': row.get('availability'),
        'freshness': row.get('freshness'),
        'quote': row.get('quote'),
        'orderbook': row.get('orderbook'),
        'liquidity': row.get('liquidity'),
        'investor_program_activity': row.get('investor_program_activity'),
        'news_hints': row.get('news_hints'),
    }


def _build_intraday_execution_freshness_provenance(
    *,
    row: dict[str, Any] | None,
    requested_market_date: date,
) -> dict[str, Any]:
    if row is None:
        return _build_freshness_provenance(
            requested_market_date=requested_market_date,
            freshness_status=FRESHNESS_STATUS_MISSING,
            provenance_status=PROVENANCE_STATUS_PROVIDER_SOURCED,
            source_tables=('current_kis_intraday_snapshot_cache', 'kis_intraday_snapshots'),
            source_provider='kis',
            row_count=0,
            reason='no_current_kis_intraday_snapshot_cache_row',
        )

    as_of_market_date = str(
        row.get('as_of_market_date')
        or ((row.get('freshness') or {}).get('market_date') if isinstance(row.get('freshness'), dict) else '')
        or ''
    ) or None
    trade_date_relation = _trade_date_relation_to_request(
        as_of_market_date=as_of_market_date,
        requested_market_date=requested_market_date,
    )
    freshness_status = (
        FRESHNESS_STATUS_FRESH
        if trade_date_relation in {None, 'same_market_date'}
        else FRESHNESS_STATUS_STALE
    )
    return _build_freshness_provenance(
        requested_market_date=requested_market_date,
        freshness_status=freshness_status,
        provenance_status=PROVENANCE_STATUS_PROVIDER_SOURCED,
        source_tables=('current_kis_intraday_snapshot_cache', 'kis_intraday_snapshots'),
        source_provider=str(row.get('source') or 'kis'),
        as_of_market_date=as_of_market_date,
        as_of_timestamp=row.get('as_of_timestamp') or row.get('captured_at'),
        latest_available_market_date=as_of_market_date,
        latest_available_timestamp=row.get('captured_at'),
        row_count=1,
        trade_date_relation_to_request=trade_date_relation,
        reason='intraday_snapshot_partial' if str(row.get('availability_status') or '') == 'partial' else None,
        extra={
            'availability_status': row.get('availability_status'),
            'coverage_status': ((row.get('coverage') or {}).get('status') if isinstance(row.get('coverage'), dict) else None),
            'available_intraday_blocks': (
                ((row.get('coverage') or {}).get('available_blocks'))
                if isinstance(row.get('coverage'), dict)
                else None
            ),
        },
    )


def _build_fundamentals_freshness_provenance(
    *,
    fundamentals: dict[str, Any],
    requested_market_date: date,
) -> dict[str, Any]:
    latest_snapshot = fundamentals.get('latest_snapshot') if isinstance(fundamentals, dict) else None
    history_summary = fundamentals.get('history_summary') if isinstance(fundamentals, dict) else {}
    latest_receipt_date = history_summary.get('latest_receipt_date') if isinstance(history_summary, dict) else None
    latest_captured_at = (
        latest_snapshot.get('filing', {}).get('captured_at')
        if isinstance(latest_snapshot, dict)
        else None
    )
    trade_date_relation = _trade_date_relation_to_request(
        as_of_market_date=latest_receipt_date,
        requested_market_date=requested_market_date,
    )
    if latest_snapshot is None:
        freshness_status = FRESHNESS_STATUS_MISSING
        reason = 'no_stored_fundamentals_snapshot'
    elif trade_date_relation == 'same_market_date':
        freshness_status = FRESHNESS_STATUS_FRESH
        reason = None
    else:
        freshness_status = FRESHNESS_STATUS_AVAILABLE_BUT_OLD
        reason = 'latest_fundamentals_snapshot_precedes_request_date'
    return _build_freshness_provenance(
        requested_market_date=requested_market_date,
        freshness_status=freshness_status,
        provenance_status=PROVENANCE_STATUS_PROVIDER_SOURCED,
        source_tables=('dart_fundamentals_snapshots', 'dart_fundamentals_rows', 'dart_disclosure_events'),
        source_provider='dart',
        as_of_market_date=latest_receipt_date if latest_snapshot is not None else None,
        as_of_timestamp=latest_captured_at if latest_snapshot is not None else None,
        latest_available_market_date=latest_receipt_date,
        latest_available_timestamp=latest_captured_at,
        row_count=1 if latest_snapshot is not None else 0,
        history_row_count=int(history_summary.get('entries') or 0) if isinstance(history_summary, dict) else 0,
        trade_date_relation_to_request=trade_date_relation,
        reason=reason,
        extra={
            'statement_row_count': (
                _maybe_dict(latest_snapshot.get('statement')).get('row_count')
                if isinstance(latest_snapshot, dict)
                else None
            ),
            'latest_period_end_date': history_summary.get('latest_period_end_date') if isinstance(history_summary, dict) else None,
            'available_business_years': history_summary.get('available_business_years') if isinstance(history_summary, dict) else [],
        },
    )


def _build_recent_disclosures_freshness_provenance(
    *,
    disclosures: list[dict[str, Any]],
    requested_market_date: date,
    disclosures_limit: int,
    storage_unavailable: bool = False,
) -> dict[str, Any]:
    latest_receipt_date = disclosures[0].get('receipt_date') if disclosures else None
    trade_date_relation = _trade_date_relation_to_request(
        as_of_market_date=latest_receipt_date,
        requested_market_date=requested_market_date,
    )
    if storage_unavailable:
        freshness_status = FRESHNESS_STATUS_MISSING
        provenance_status = 'storage_unavailable'
        reason = 'dart_disclosure_storage_unavailable'
    elif not disclosures:
        freshness_status = FRESHNESS_STATUS_MISSING
        provenance_status = PROVENANCE_STATUS_PROVIDER_SOURCED
        reason = 'no_recent_disclosures_up_to_request_date'
    elif trade_date_relation == 'same_market_date':
        freshness_status = FRESHNESS_STATUS_FRESH
        provenance_status = PROVENANCE_STATUS_PROVIDER_SOURCED
        reason = None
    else:
        freshness_status = FRESHNESS_STATUS_AVAILABLE_BUT_OLD
        provenance_status = PROVENANCE_STATUS_PROVIDER_SOURCED
        reason = 'latest_disclosure_precedes_request_date'
    return _build_freshness_provenance(
        requested_market_date=requested_market_date,
        freshness_status=freshness_status,
        provenance_status=provenance_status,
        source_tables=('dart_disclosure_events',),
        source_provider='dart',
        as_of_market_date=latest_receipt_date,
        latest_available_market_date=latest_receipt_date,
        row_count=len(disclosures),
        trade_date_relation_to_request=trade_date_relation,
        reason=reason,
        extra={
            'limit_per_symbol': disclosures_limit,
        },
    )


def _build_recent_dart_events_context(
    *,
    disclosures: list[dict[str, Any]],
    fundamentals: dict[str, Any],
    storage_unavailable: bool,
) -> dict[str, Any]:
    latest_snapshot = fundamentals.get('latest_snapshot') if isinstance(fundamentals, dict) else None
    disclosure_context = (
        fundamentals.get('disclosure_context')
        if isinstance(fundamentals.get('disclosure_context'), dict)
        else _build_disclosure_context(disclosures=disclosures, latest_snapshot=latest_snapshot)
    )
    if storage_unavailable:
        status = 'unavailable'
        reason = 'dart_disclosure_storage_unavailable'
    elif disclosures:
        status = 'available'
        reason = None
    else:
        status = 'empty'
        reason = 'no_recent_disclosures'
    summary = dict(disclosure_context.get('summary') or {})
    return {
        'status': status,
        'reason': reason,
        'source': 'stored_dart_disclosure_events',
        'item_count': len(disclosures),
        'summary': summary,
        'latest_snapshot_relation': disclosure_context.get('latest_snapshot_relation') or {},
        'items': [
            {
                'rcept_no': item.get('rcept_no'),
                'receipt_date': item.get('receipt_date'),
                'report_nm': item.get('report_nm'),
                'filing_category': item.get('filing_category'),
                'is_financial_report': item.get('is_financial_report'),
                'is_correction': item.get('is_correction'),
                'is_ownership_related': item.get('is_ownership_related'),
                'is_large_holder_related': item.get('is_large_holder_related'),
                'is_audit_related': item.get('is_audit_related'),
                'disclosure_tags': list(item.get('disclosure_tags') or []),
                'fundamentals_capture_status': item.get('fundamentals_capture_status'),
            }
            for item in disclosures[:3]
        ],
    }


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


def _factor_summary_view_row(*, family: str, row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    if family == 'momentum':
        return enrich_momentum_audit_fields(dict(row))
    return row


def _factor_summary_source_status(*, family: str, row: dict[str, Any] | None) -> str | None:
    view_row = _factor_summary_view_row(family=family, row=row)
    if view_row is None:
        return None
    raw_value = view_row.get(FACTOR_SUMMARY_RAW_STATUS_FIELDS[family])
    text = str(raw_value or '').strip()
    return text or None


def _resolve_factor_summary_status(
    *,
    family: str,
    row: dict[str, Any] | None,
    freshness_provenance: dict[str, Any],
    current_storage_unavailable: bool,
    prior_storage_unavailable: bool,
) -> str:
    if current_storage_unavailable or (row is None and prior_storage_unavailable):
        return FACTOR_SUMMARY_STATUS_STORAGE_UNAVAILABLE

    freshness_status = str(freshness_provenance.get('freshness_status') or '').strip()
    if row is None:
        if freshness_status in {FRESHNESS_STATUS_STALE, FRESHNESS_STATUS_AVAILABLE_BUT_OLD}:
            return FACTOR_SUMMARY_STATUS_NOT_COMPUTED_FOR_MARKET_DATE
        return FACTOR_SUMMARY_STATUS_MISSING_SOURCE_ROW

    source_status = _factor_summary_source_status(family=family, row=row)
    score_field = FACTOR_SUMMARY_SCORE_FIELDS[family]
    score_available = _to_float(row.get(score_field)) is not None if score_field else False
    if source_status in FACTOR_SUMMARY_NOT_COMPUTED_RAW_STATUSES:
        return FACTOR_SUMMARY_STATUS_NOT_COMPUTED_FOR_MARKET_DATE
    if source_status in FACTOR_SUMMARY_INSUFFICIENT_HISTORY_RAW_STATUSES:
        return FACTOR_SUMMARY_STATUS_INSUFFICIENT_HISTORY
    if source_status in FACTOR_SUMMARY_NOT_ELIGIBLE_RAW_STATUSES:
        return FACTOR_SUMMARY_STATUS_NOT_ELIGIBLE
    if family in FACTOR_SUMMARY_SCORE_REQUIRED_FAMILIES and not score_available:
        return FACTOR_SUMMARY_STATUS_NOT_ELIGIBLE
    return FACTOR_SUMMARY_STATUS_AVAILABLE


def _build_factor_summary_entry(
    *,
    family: str,
    row: dict[str, Any] | None,
    score_context: dict[str, Any] | None,
    freshness_provenance: dict[str, Any],
    current_storage_unavailable: bool,
    prior_storage_unavailable: bool,
) -> dict[str, Any]:
    view_row = _factor_summary_view_row(family=family, row=row)
    score_field = FACTOR_SUMMARY_SCORE_FIELDS[family]
    rank_field = FACTOR_SUMMARY_RANK_FIELDS[family]
    percentile_field = FACTOR_SUMMARY_PERCENTILE_FIELDS[family]
    eligibility_field = FACTOR_SUMMARY_ELIGIBILITY_FIELDS[family]
    score = _to_float(view_row.get(score_field)) if view_row is not None else None
    rank = _to_int(view_row.get(rank_field)) if view_row is not None and rank_field else None
    percentile = _to_float(view_row.get(percentile_field)) if view_row is not None and percentile_field else None
    source_status = _factor_summary_source_status(family=family, row=view_row)
    entry = {
        'status': _resolve_factor_summary_status(
            family=family,
            row=view_row,
            freshness_provenance=freshness_provenance,
            current_storage_unavailable=current_storage_unavailable,
            prior_storage_unavailable=prior_storage_unavailable,
        ),
        'source_status': source_status,
        'score': score,
        'score_available': score is not None,
        'rank': rank,
        'percentile': percentile,
        'eligible': (
            bool(view_row.get(eligibility_field))
            if view_row is not None and eligibility_field and view_row.get(eligibility_field) is not None
            else None
        ),
        'source': (view_row or {}).get('source'),
        'feature_version': (view_row or {}).get('feature_version') or (view_row or {}).get('source'),
        'as_of_market_date': (view_row or {}).get('market_date'),
        'captured_at': (view_row or {}).get('captured_at'),
        'coverage_notes': list((score_context or {}).get('coverage_notes') or []),
        'components_used': _to_int((score_context or {}).get('components_used')),
        'components_possible': _to_int((score_context or {}).get('components_possible')),
        'provenance': _compact_provenance_summary(freshness_provenance),
    }
    return {
        key: value
        for key, value in entry.items()
        if value not in (None, [], {})
    }


def _build_signal_features_summary_entry(
    *,
    row: dict[str, Any] | None,
    freshness_provenance: dict[str, Any],
    current_storage_unavailable: bool,
    prior_storage_unavailable: bool,
) -> dict[str, Any]:
    if current_storage_unavailable or (row is None and prior_storage_unavailable):
        status = FACTOR_SUMMARY_STATUS_STORAGE_UNAVAILABLE
    elif row is None:
        freshness_status = str(freshness_provenance.get('freshness_status') or '').strip()
        status = (
            FACTOR_SUMMARY_STATUS_NOT_COMPUTED_FOR_MARKET_DATE
            if freshness_status == FRESHNESS_STATUS_STALE
            else FACTOR_SUMMARY_STATUS_MISSING_SOURCE_ROW
        )
    else:
        status = FACTOR_SUMMARY_STATUS_AVAILABLE
    provenance = row.get('provenance') if isinstance((row or {}).get('provenance'), dict) else {}
    entry = {
        'status': status,
        'signal_count': _to_int((row or {}).get('signal_count')),
        'row_kind': (row or {}).get('row_kind') or provenance.get('row_kind'),
        'direct_signal_source_count': _to_int(provenance.get('direct_signal_source_count')),
        'fallback_groups_present': list(provenance.get('fallback_groups_present') or []),
        'source': (row or {}).get('source'),
        'feature_version': (row or {}).get('source'),
        'as_of_market_date': ((row or {}).get('freshness') or {}).get('market_date') if isinstance((row or {}).get('freshness'), dict) else (row or {}).get('market_date'),
        'captured_at': (row or {}).get('captured_at'),
        'provenance': _compact_provenance_summary(freshness_provenance),
    }
    return {
        key: value
        for key, value in entry.items()
        if value not in (None, [], {})
    }


def _build_factor_summary(
    *,
    requested_market_date: date,
    factor_rows: dict[str, dict[str, Any] | None],
    factor_provenance: dict[str, dict[str, Any]],
    factor_storage_unavailable: dict[str, bool] | None,
    factor_prior_storage_unavailable: dict[str, bool] | None,
    scores: dict[str, dict[str, Any]],
    signal_features: dict[str, Any] | None,
    signal_features_provenance: dict[str, Any],
    signal_features_storage_unavailable: bool = False,
    signal_features_prior_storage_unavailable: bool = False,
) -> dict[str, Any]:
    factor_scores = {
        family: _build_factor_summary_entry(
            family=family,
            row=factor_rows.get(family),
            score_context=scores.get(family),
            freshness_provenance=factor_provenance.get(family, {}),
            current_storage_unavailable=bool((factor_storage_unavailable or {}).get(family)),
            prior_storage_unavailable=bool((factor_prior_storage_unavailable or {}).get(family)),
        )
        for family in PERSISTED_FACTOR_FAMILIES
    }
    return {
        'requested_market_date': requested_market_date.isoformat(),
        'factor_scores': factor_scores,
        'signal_features': _build_signal_features_summary_entry(
            row=signal_features,
            freshness_provenance=signal_features_provenance,
            current_storage_unavailable=signal_features_storage_unavailable,
            prior_storage_unavailable=signal_features_prior_storage_unavailable,
        ),
    }


def _build_factor_row_completeness(
    *,
    family: str,
    row: dict[str, Any] | None,
    storage_unavailable: bool = False,
) -> dict[str, Any]:
    if storage_unavailable:
        return {
            'row_status': FACTOR_SUMMARY_STATUS_STORAGE_UNAVAILABLE,
            'score_status': None,
            'score_field': FACTOR_ROW_PRIMARY_SCORE_FIELDS[family],
            'score_value_present': False,
            'null_submetric_fields': [],
            'null_submetric_count': 0,
            'present_submetric_fields': [],
            'present_submetric_count': 0,
            'status_fields': {},
        }
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


def _build_signal_feature_completeness(
    row: dict[str, Any] | None,
    *,
    storage_unavailable: bool = False,
) -> dict[str, Any]:
    if storage_unavailable:
        return {
            'row_status': FACTOR_SUMMARY_STATUS_STORAGE_UNAVAILABLE,
            'row_kind': FACTOR_SUMMARY_STATUS_STORAGE_UNAVAILABLE,
            'direct_signal_source_count': 0,
            'fallback_groups_present': [],
            'null_submetric_fields': [],
            'null_submetric_count': 0,
            'present_submetric_fields': [],
            'present_submetric_count': 0,
        }
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


def _build_intraday_execution_completeness(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        return {
            'status': 'unavailable',
            'reason': 'No persisted KIS intraday execution-context snapshot is available.',
            'available_blocks': 0,
            'partial_blocks': 0,
            'total_blocks': 5,
        }

    coverage = row.get('coverage') if isinstance(row.get('coverage'), dict) else {}
    block_statuses = dict(coverage.get('block_statuses') or {})
    return {
        'status': str(row.get('availability_status') or coverage.get('status') or 'available'),
        'available_blocks': int(coverage.get('available_blocks') or 0),
        'partial_blocks': int(coverage.get('partial_blocks') or 0),
        'total_blocks': int(coverage.get('total_blocks') or 5),
        'block_statuses': block_statuses,
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


def _latest_captured_at(rows: Sequence[dict[str, Any] | None]) -> str | None:
    latest_value = None
    latest_parsed = None
    for row in rows:
        if not isinstance(row, dict):
            continue
        captured_at = row.get('captured_at')
        parsed = _parse_iso_datetime(captured_at)
        if parsed is None:
            continue
        if latest_parsed is None or parsed > latest_parsed:
            latest_parsed = parsed
            latest_value = captured_at
    return latest_value


def _build_pykrx_block_freshness_provenance(
    *,
    requested_market_date: date,
    rows: Sequence[dict[str, Any] | None],
    source_provider: str,
    provenance_status: str,
    requested_symbol_coverage: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    present_rows = [row for row in rows if isinstance(row, dict)]
    latest_captured_at = _latest_captured_at(present_rows)
    if present_rows:
        freshness_status = FRESHNESS_STATUS_FRESH
        as_of_market_date = requested_market_date.isoformat()
        latest_available_market_date = requested_market_date.isoformat()
        trade_date_relation = 'same_market_date'
        reason = None
    else:
        freshness_status = FRESHNESS_STATUS_MISSING
        as_of_market_date = None
        latest_available_market_date = None
        trade_date_relation = None
        reason = 'no_stored_pykrx_enrichment_row_for_request_date'

    return _build_freshness_provenance(
        requested_market_date=requested_market_date,
        freshness_status=freshness_status,
        provenance_status=provenance_status,
        source_tables=('daily_pykrx_enrichment_rows',),
        source_provider=source_provider,
        as_of_market_date=as_of_market_date,
        as_of_timestamp=latest_captured_at if present_rows else None,
        latest_available_market_date=latest_available_market_date,
        latest_available_timestamp=latest_captured_at,
        row_count=len(present_rows),
        history_row_count=len(present_rows),
        trade_date_relation_to_request=trade_date_relation,
        reason=reason,
        requested_symbol_coverage=requested_symbol_coverage,
        extra=extra,
    )


def _build_context_block_completeness(
    context: dict[str, Any] | None,
    *,
    unavailable_reason: str,
) -> dict[str, Any]:
    if context is None:
        return {
            'status': 'unavailable',
            'reason': unavailable_reason,
        }

    return {
        'status': str(context.get('status') or 'available'),
        'missing_fields': list(context.get('missing_fields') or []),
    }


def _build_sector_peer_context(
    *,
    symbol: str,
    sector_row: dict[str, Any] | None,
    sector_rows_for_market: Sequence[dict[str, Any]],
) -> dict[str, Any] | None:
    if sector_row is None:
        return None

    payload = _maybe_dict(sector_row.get('payload'))
    sector_name = payload.get('sector_name')
    missing_fields = [] if sector_name else ['sector_name']
    self_entry = {
        'symbol': symbol,
        'name_ko': sector_row.get('name_ko'),
        'market_cap': payload.get('market_cap'),
        'change_pct': payload.get('change_pct'),
    }
    peer_entries: list[dict[str, Any]] = []
    if sector_name:
        for row in sector_rows_for_market:
            row_payload = _maybe_dict(row.get('payload'))
            if row_payload.get('sector_name') != sector_name:
                continue
            row_symbol = row.get('symbol') or row.get('scope_key')
            entry = {
                'symbol': row_symbol,
                'name_ko': row.get('name_ko'),
                'market_cap': row_payload.get('market_cap'),
                'change_pct': row_payload.get('change_pct'),
            }
            if row_symbol == symbol:
                self_entry = entry
                continue
            peer_entries.append(entry)
        peer_entries.sort(
            key=lambda item: (
                item.get('market_cap') is None,
                -(int(item.get('market_cap') or 0)),
                str(item.get('symbol') or ''),
            )
        )

    return {
        'status': 'available' if not missing_fields else 'partial',
        'market': sector_row.get('market'),
        'sector_name': sector_name,
        'close_price': payload.get('close_price'),
        'market_cap': payload.get('market_cap'),
        'change_pct': payload.get('change_pct'),
        'same_sector_symbol_count': len(peer_entries) + 1,
        'peer_count': len(peer_entries),
        'peers': peer_entries[:5],
        'self': self_entry,
        'missing_fields': missing_fields,
    }


def _build_market_breadth_context(
    market_row: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if market_row is None:
        return None

    payload = _maybe_dict(market_row.get('payload'))
    required_fields = ('market_symbol_count', 'up_count', 'down_count', 'flat_count')
    missing_fields = [field for field in required_fields if payload.get(field) is None]
    return {
        'status': 'available' if not missing_fields else 'partial',
        **payload,
        'missing_fields': missing_fields,
    }


def _build_investor_flow_context(
    *,
    symbol_row: dict[str, Any] | None,
    market_row: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if symbol_row is None and market_row is None:
        return None

    symbol_payload = _maybe_dict(symbol_row.get('payload')) if symbol_row is not None else None
    market_payload = _maybe_dict(market_row.get('payload')) if market_row is not None else None
    missing_fields: list[str] = []
    if not symbol_payload:
        missing_fields.append('symbol_flow')
    if not market_payload:
        missing_fields.append('market_flow')
    return {
        'status': 'available' if not missing_fields else 'partial',
        'market': (
            symbol_row.get('market')
            if symbol_row is not None
            else market_row.get('market')
            if market_row is not None
            else None
        ),
        'symbol_flow': symbol_payload,
        'market_flow': market_payload,
        'missing_fields': missing_fields,
    }


def _build_short_selling_context(
    short_row: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if short_row is None:
        return None

    payload = _maybe_dict(short_row.get('payload'))
    missing_fields: list[str] = []
    if not _maybe_dict(payload.get('short_volume')):
        missing_fields.append('short_volume')
    if not _maybe_dict(payload.get('short_balance')):
        missing_fields.append('short_balance')
    return {
        'status': 'available' if not missing_fields else 'partial',
        **payload,
        'missing_fields': missing_fields,
    }


def _build_valuation_context(
    *,
    valuation_row: dict[str, Any] | None,
    foreign_ownership_row: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if valuation_row is None and foreign_ownership_row is None:
        return None

    valuation_payload = _maybe_dict(valuation_row.get('payload')) if valuation_row is not None else None
    foreign_payload = _maybe_dict(foreign_ownership_row.get('payload')) if foreign_ownership_row is not None else None
    missing_fields: list[str] = []
    if not valuation_payload:
        missing_fields.append('valuation_snapshot')
    if not foreign_payload:
        missing_fields.append('foreign_ownership')
    return {
        'status': 'available' if not missing_fields else 'partial',
        'market': (
            valuation_row.get('market')
            if valuation_row is not None
            else foreign_ownership_row.get('market')
            if foreign_ownership_row is not None
            else None
        ),
        'valuation_snapshot': valuation_payload,
        'foreign_ownership': foreign_payload,
        'missing_fields': missing_fields,
    }


def _external_news_provider_status() -> dict[str, Any]:
    return NaverNewsSearchProvider().status().as_dict()


def _build_recent_external_news_context(items: list[dict[str, Any]], *, provider_status: dict[str, Any]) -> dict[str, Any]:
    provider_enabled = bool(provider_status.get('enabled'))
    if not provider_enabled:
        status = 'unavailable'
        reason = provider_status.get('reason') or 'external_news_provider_disabled'
    elif items:
        status = 'available'
        reason = None
    else:
        status = 'empty'
        reason = 'no_recent_external_news_found'
    limited_items = items[: settings.external_news_context_limit]
    event_tag_counts = Counter(
        tag
        for item in limited_items
        for tag in list(item.get('event_tags') or [])
    )
    sentiment_counts = Counter(
        str(item.get('sentiment'))
        for item in limited_items
        if item.get('sentiment')
    )
    max_materiality = max(
        (float(item.get('materiality_score') or 0.0) for item in limited_items),
        default=None,
    )
    return {
        'status': status,
        'reason': reason,
        'provider': provider_status,
        'lookback_days': settings.external_news_context_lookback_days,
        'item_count': len(items),
        'classification_summary': {
            'event_tag_counts': dict(event_tag_counts),
            'sentiment_counts': dict(sentiment_counts),
            'max_materiality_score': max_materiality,
            'classifier': 'rule_based_external_news_v1' if limited_items else None,
        },
        'items': [
            {
                'provider': item.get('provider'),
                'title': item.get('title'),
                'description': item.get('description'),
                'url': item.get('url'),
                'published_at': item.get('published_at'),
                'collected_at': item.get('collected_at'),
                'event_tags': list(item.get('event_tags') or []),
                'sentiment': item.get('sentiment'),
                'materiality_score': item.get('materiality_score'),
                'materiality_bucket': item.get('materiality_bucket'),
            }
            for item in limited_items
        ],
    }


def _build_external_news_freshness_provenance(
    *,
    context: dict[str, Any],
    requested_market_date: date,
) -> dict[str, Any]:
    status = str(context.get('status') or 'unavailable')
    if status == 'available':
        freshness_status = FRESHNESS_STATUS_FRESH
        provenance_status = PROVENANCE_STATUS_PROVIDER_SOURCED
    elif status == 'empty':
        freshness_status = FRESHNESS_STATUS_MISSING
        provenance_status = PROVENANCE_STATUS_PROVIDER_SOURCED
    else:
        freshness_status = FRESHNESS_STATUS_MISSING
        provenance_status = 'external_provider_unavailable'
    items = context.get('items') if isinstance(context.get('items'), list) else []
    return _build_freshness_provenance(
        requested_market_date=requested_market_date,
        freshness_status=freshness_status,
        provenance_status=provenance_status,
        source_tables=('external_news_items',),
        source_provider=str((context.get('provider') or {}).get('provider') or NAVER_NEWS_PROVIDER),
        reason=context.get('reason'),
        latest_available_timestamp=items[0].get('published_at') if items else None,
        history_row_count=int(context.get('item_count') or 0),
        extra={
            'provider_status': context.get('provider'),
            'lookback_days': context.get('lookback_days'),
        },
    )


def _build_symbol_block_coverage(
    *,
    available_count: int,
    partial_count: int,
    requested_count: int,
) -> dict[str, Any]:
    return {
        'available_rows': available_count,
        'partial_rows': partial_count,
        'unavailable_rows': requested_count - available_count - partial_count,
        'coverage_ratio': _ratio(available_count + partial_count, requested_count),
        'coverage_status': (
            'complete'
            if available_count == requested_count and requested_count > 0
            else 'partial'
            if (available_count + partial_count) > 0
            else 'missing'
        ),
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
        factor_storage_unavailable = {family: False for family in PERSISTED_FACTOR_FAMILIES}

        def _load_factor_rows(family: str, loader) -> list[dict[str, Any]]:
            try:
                return loader()
            except SQLAlchemyError:
                factor_storage_unavailable[family] = True
                return []

        signal_features_storage_unavailable = False
        try:
            signal_feature_rows = load_daily_signal_features(
                market_date=normalized_market_date,
                symbols=normalized_symbols,
                limit=max(len(normalized_symbols), 1000),
            )
        except SQLAlchemyError:
            signal_features_storage_unavailable = True
            signal_feature_rows = []
        value_rows = _load_factor_rows(
            'value',
            lambda: load_daily_value_factor_results(
                market_date=normalized_market_date,
                symbols=normalized_symbols,
                limit=max(len(normalized_symbols), 1000),
            ),
        )
        unified_value_rows = _load_factor_rows(
            'unified_value',
            lambda: load_daily_unified_value_factor_results(
                market_date=normalized_market_date,
                symbols=normalized_symbols,
                limit=max(len(normalized_symbols), 1000),
            ),
        )
        quality_rows = _load_factor_rows(
            'quality',
            lambda: load_daily_quality_factor_results(
                market_date=normalized_market_date,
                symbols=normalized_symbols,
                limit=max(len(normalized_symbols), 1000),
            ),
        )
        momentum_rows = _load_factor_rows(
            'momentum',
            lambda: load_daily_momentum_factor_results(
                market_date=normalized_market_date,
                symbols=normalized_symbols,
                limit=max(len(normalized_symbols), 1000),
            ),
        )
        advanced_value_rows = _load_factor_rows(
            'advanced_value',
            lambda: load_daily_advanced_value_factor_results(
                market_date=normalized_market_date,
                symbols=normalized_symbols,
                limit=max(len(normalized_symbols), 1000),
            ),
        )
        low_risk_rows = _load_factor_rows(
            'low_risk',
            lambda: load_daily_low_risk_factor_results(
                market_date=normalized_market_date,
                symbols=normalized_symbols,
                limit=max(len(normalized_symbols), 1000),
            ),
        )
        beta_rows = _load_factor_rows(
            'beta',
            lambda: load_daily_beta_factor_results(
                market_date=normalized_market_date,
                symbols=normalized_symbols,
                limit=max(len(normalized_symbols), 1000),
            ),
        )
        composite_rows = _load_factor_rows(
            'composite',
            lambda: load_daily_composite_factor_results(
                market_date=normalized_market_date,
                symbols=normalized_symbols,
                limit=max(len(normalized_symbols), 1000),
            ),
        )
        quote_rows = load_current_quotes(
            symbols=normalized_symbols,
            limit=max(len(normalized_symbols), 1000),
        )
        intraday_execution_rows = load_current_kis_intraday_snapshots(
            symbols=normalized_symbols,
            limit=max(len(normalized_symbols), 1000),
        )
        price_context_storage_unavailable = False
        try:
            price_context_rows = load_daily_price_contexts(
                market_date=normalized_market_date,
                symbols=normalized_symbols,
                limit=max(len(normalized_symbols), 1000),
            )
        except SQLAlchemyError:
            price_context_storage_unavailable = True
            price_context_rows = []
        try:
            price_context_history_rows = load_daily_price_context_history(
                market_date=normalized_market_date,
                symbols=normalized_symbols,
                history_days=PRICE_CONTEXT_HISTORY_DAYS,
            )
        except SQLAlchemyError:
            price_context_storage_unavailable = True
            price_context_history_rows = []
        market_relative_rows = load_daily_market_relative_contexts(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
        )
        pykrx_enrichment_inputs = pykrx_enrichment_service.load_symbol_day_context_inputs(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
        )
        dart_storage_unavailable = False
        try:
            fundamentals_rows = self.fundamentals_repository.list_stored_fundamentals_summary(
                stock_codes=normalized_symbols,
                fs_div=normalized_fs_div,
            )
        except SQLAlchemyError:
            dart_storage_unavailable = True
            fundamentals_rows = []
        latest_prior_rows = _load_latest_symbol_day_block_history(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
        )
        try:
            disclosures_by_symbol = self.disclosure_repository.list_recent_events_by_stock_codes(
                stock_codes=normalized_symbols,
                receipt_date_to=normalized_market_date,
                limit_per_symbol=disclosures_limit,
            )
        except SQLAlchemyError:
            dart_storage_unavailable = True
            disclosures_by_symbol = {symbol: [] for symbol in normalized_symbols}
        external_news_provider_status = _external_news_provider_status()
        external_news_by_symbol: dict[str, list[dict[str, Any]]] = {}
        if external_news_provider_status.get('enabled'):
            try:
                external_news_by_symbol = list_recent_external_news_for_symbols(
                    symbols=normalized_symbols,
                    limit_per_symbol=settings.external_news_context_limit,
                    lookback_days=settings.external_news_context_lookback_days,
                )
            except SQLAlchemyError:
                external_news_provider_status = {
                    **external_news_provider_status,
                    'enabled': False,
                    'status': 'storage_unavailable',
                    'reason': 'external_news_storage_unavailable',
                }

        symbol_map = {item['symbol']: item for item in symbol_rows}
        technical_map = {item['symbol']: item for item in technical_rows}
        action_map = {item['symbol']: item for item in action_rows}
        signal_feature_map = {item['symbol']: item for item in signal_feature_rows}
        value_map = {item['symbol']: item for item in value_rows}
        unified_value_map = {item['symbol']: item for item in unified_value_rows}
        quality_map = {item['symbol']: item for item in quality_rows}
        momentum_map = {item['symbol']: item for item in momentum_rows}
        advanced_value_map = {item['symbol']: item for item in advanced_value_rows}
        low_risk_map = {item['symbol']: item for item in low_risk_rows}
        beta_map = {item['symbol']: item for item in beta_rows}
        composite_map = {item['symbol']: item for item in composite_rows}
        factor_prior_storage_unavailable = dict(latest_prior_rows.get('factor_storage_unavailable') or {})
        signal_features_prior_storage_unavailable = bool(
            latest_prior_rows.get('signal_features_storage_unavailable') or False
        )
        quote_map = {item['symbol']: item for item in quote_rows}
        intraday_execution_map = {item['symbol']: item for item in intraday_execution_rows}
        price_context_map = {item['symbol']: item['context_payload'] for item in price_context_rows}
        price_context_history_map = _group_price_context_history(
            price_context_history_rows,
            symbols=normalized_symbols,
        )
        market_relative_map = {item['symbol']: item for item in market_relative_rows}
        pykrx_enrichment_by_symbol = dict(pykrx_enrichment_inputs.get('by_symbol') or {})
        pykrx_enrichment_by_market = dict(pykrx_enrichment_inputs.get('by_market') or {})
        sector_rows_by_market = {
            market: list(rows)
            for market, rows in dict(pykrx_enrichment_inputs.get('sector_rows_by_market') or {}).items()
        }
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
            raw_quote_row = quote_map.get(symbol)
            quote_context = _build_quote_context(raw_quote_row, market_date=normalized_market_date)
            intraday_execution_row = intraday_execution_map.get(symbol)
            intraday_execution_context = _build_intraday_execution_context(intraday_execution_row)
            price_context = price_context_map.get(symbol)
            price_context_history = price_context_history_map.get(symbol, [])
            technical_snapshot = technical_map.get(symbol)
            action_summary = action_map.get(symbol)
            signal_features = signal_feature_map.get(symbol)
            market_relative_context = market_relative_map.get(symbol)
            latest_prior_low_risk_row = ((latest_prior_rows.get('factor_rows') or {}).get('low_risk') or {}).get(symbol)
            company_row = symbol_map.get(symbol)
            company_market = str(company_row.get('market') or '').strip().upper() if company_row else ''
            symbol_enrichment_rows = dict(pykrx_enrichment_by_symbol.get(symbol) or {})
            market_enrichment_rows = dict(pykrx_enrichment_by_market.get(company_market) or {})
            sector_peer_context = _build_sector_peer_context(
                symbol=symbol,
                sector_row=symbol_enrichment_rows.get('sector_classification'),
                sector_rows_for_market=sector_rows_by_market.get(company_market, []),
            )
            market_breadth_context = _build_market_breadth_context(
                market_enrichment_rows.get('market_breadth'),
            )
            investor_flow_context = _build_investor_flow_context(
                symbol_row=symbol_enrichment_rows.get('investor_flow_symbol'),
                market_row=market_enrichment_rows.get('investor_flow_market'),
            )
            short_selling_context = _build_short_selling_context(
                symbol_enrichment_rows.get('short_selling'),
            )
            valuation_context = _build_valuation_context(
                valuation_row=symbol_enrichment_rows.get('valuation_snapshot'),
                foreign_ownership_row=symbol_enrichment_rows.get('foreign_ownership'),
            )
            current_factor_rows = {
                'value': value_map.get(symbol),
                'unified_value': unified_value_map.get(symbol),
                'quality': quality_map.get(symbol),
                'momentum': momentum_map.get(symbol),
                'advanced_value': advanced_value_map.get(symbol),
                'low_risk': low_risk_map.get(symbol),
                'beta': beta_map.get(symbol),
                'composite': composite_map.get(symbol),
            }
            scores = build_signal_decider_scores(
                market_date=normalized_market_date,
                unified_value_row=current_factor_rows['unified_value'],
                quality_row=current_factor_rows['quality'],
                momentum_row=current_factor_rows['momentum'],
            )
            persisted_factor_rows = {
                'value': _copy_selected_fields(
                    current_factor_rows['value'],
                    fields=FACTOR_ROW_FIELDS['value'],
                ),
                'unified_value': _copy_selected_fields(
                    current_factor_rows['unified_value'],
                    fields=FACTOR_ROW_FIELDS['unified_value'],
                ),
                'quality': _copy_selected_fields(
                    current_factor_rows['quality'],
                    fields=FACTOR_ROW_FIELDS['quality'],
                ),
                'momentum': _copy_selected_fields(
                    _factor_summary_view_row(family='momentum', row=current_factor_rows['momentum']),
                    fields=FACTOR_ROW_FIELDS['momentum'],
                ),
                'advanced_value': _copy_selected_fields(
                    current_factor_rows['advanced_value'],
                    fields=FACTOR_ROW_FIELDS['advanced_value'],
                ),
                'low_risk': _copy_selected_fields(
                    current_factor_rows['low_risk'],
                    fields=FACTOR_ROW_FIELDS['low_risk'],
                ),
                'beta': _copy_selected_fields(
                    current_factor_rows['beta'],
                    fields=FACTOR_ROW_FIELDS['beta'],
                ),
                'composite': _copy_selected_fields(
                    current_factor_rows['composite'],
                    fields=FACTOR_ROW_FIELDS['composite'],
                ),
            }
            factor_row_provenance = {
                family: _build_factor_row_freshness_provenance(
                    family=family,
                    row=current_factor_rows.get(family),
                    latest_prior_row=((latest_prior_rows.get('factor_rows') or {}).get(family) or {}).get(symbol),
                    requested_market_date=normalized_market_date,
                    current_storage_unavailable=bool(factor_storage_unavailable.get(family)),
                    prior_storage_unavailable=bool(factor_prior_storage_unavailable.get(family)),
                )
                for family, row in persisted_factor_rows.items()
            }
            for family, row in list(persisted_factor_rows.items()):
                if row is not None:
                    persisted_factor_rows[family] = {
                        **row,
                        'freshness_provenance': factor_row_provenance[family],
                    }
            for family, row in persisted_factor_rows.items():
                if row is not None:
                    factor_row_counts[family] += 1
            fundamentals = _build_fundamentals_summary(
                fundamentals_map.get(symbol, []),
                market_date=normalized_market_date,
            )
            recent_disclosures = [
                _serialize_disclosure(item) for item in disclosures_by_symbol.get(symbol, [])
            ]
            recent_dart_events = _build_recent_dart_events_context(
                disclosures=recent_disclosures,
                fundamentals=fundamentals,
                storage_unavailable=dart_storage_unavailable,
            )
            recent_external_news = _build_recent_external_news_context(
                external_news_by_symbol.get(symbol, []),
                provider_status=external_news_provider_status,
            )
            context_completeness = {
                'factor_rows': {
                    family: _build_factor_row_completeness(
                        family=family,
                        row=row,
                        storage_unavailable=bool(factor_storage_unavailable.get(family)),
                    )
                    for family, row in persisted_factor_rows.items()
                },
                'signal_features': _build_signal_feature_completeness(
                    signal_features,
                    storage_unavailable=signal_features_storage_unavailable,
                ),
                'intraday_execution_context': _build_intraday_execution_completeness(intraday_execution_row),
                'market_relative_context': _build_market_relative_completeness(market_relative_context),
                'sector_peer_mapping': _build_context_block_completeness(
                    sector_peer_context,
                    unavailable_reason='No persisted PyKRX sector classification is available for the requested date.',
                ),
                'market_breadth_context': _build_context_block_completeness(
                    market_breadth_context,
                    unavailable_reason='No persisted PyKRX market breadth row is available for the requested date.',
                ),
                'investor_flow_context': _build_context_block_completeness(
                    investor_flow_context,
                    unavailable_reason='No persisted PyKRX investor flow row is available for the requested date.',
                ),
                'short_selling_context': _build_context_block_completeness(
                    short_selling_context,
                    unavailable_reason='No persisted PyKRX short-selling row is available for the requested date.',
                ),
                'valuation_context': _build_context_block_completeness(
                    valuation_context,
                    unavailable_reason='No persisted PyKRX valuation or foreign ownership row is available for the requested date.',
                ),
                'recent_dart_events': {
                    'status': recent_dart_events.get('status'),
                    'missing_fields': [] if recent_dart_events.get('status') == 'available' else ['items'],
                    'unavailable_reason': recent_dart_events.get('reason'),
                },
                'recent_external_news': {
                    'status': recent_external_news.get('status'),
                    'missing_fields': [] if recent_external_news.get('status') == 'available' else ['items'],
                    'unavailable_reason': recent_external_news.get('reason'),
                },
            }
            disclosure_context = _build_disclosure_context(
                disclosures=recent_disclosures,
                latest_snapshot=fundamentals.get('latest_snapshot'),
            )
            quote_context_provenance = _build_quote_freshness_provenance(
                row=raw_quote_row,
                requested_market_date=normalized_market_date,
            )
            price_context_provenance = _build_price_context_freshness_provenance(
                price_context=price_context,
                price_context_history=price_context_history,
                requested_market_date=normalized_market_date,
                storage_unavailable=price_context_storage_unavailable,
            )
            signal_features_provenance = _build_signal_features_freshness_provenance(
                signal_features=signal_features,
                latest_prior_row=(latest_prior_rows.get('signal_features') or {}).get(symbol),
                requested_market_date=normalized_market_date,
                current_storage_unavailable=signal_features_storage_unavailable,
                prior_storage_unavailable=signal_features_prior_storage_unavailable,
            )
            intraday_execution_provenance = _build_intraday_execution_freshness_provenance(
                row=intraday_execution_row,
                requested_market_date=normalized_market_date,
            )
            fundamentals_provenance = _build_fundamentals_freshness_provenance(
                fundamentals=fundamentals,
                requested_market_date=normalized_market_date,
            )
            recent_disclosures_provenance = _build_recent_disclosures_freshness_provenance(
                disclosures=recent_disclosures,
                requested_market_date=normalized_market_date,
                disclosures_limit=disclosures_limit,
                storage_unavailable=dart_storage_unavailable,
            )
            recent_external_news_provenance = _build_external_news_freshness_provenance(
                context=recent_external_news,
                requested_market_date=normalized_market_date,
            )
            sector_peer_context_provenance = _build_pykrx_block_freshness_provenance(
                requested_market_date=normalized_market_date,
                rows=(symbol_enrichment_rows.get('sector_classification'),),
                source_provider='pykrx',
                provenance_status=PROVENANCE_STATUS_PROVIDER_SOURCED,
                extra={'category': 'sector_classification'},
            )
            market_breadth_context_provenance = _build_pykrx_block_freshness_provenance(
                requested_market_date=normalized_market_date,
                rows=(market_enrichment_rows.get('market_breadth'),),
                source_provider='pykrx_derived',
                provenance_status=PROVENANCE_STATUS_DERIVED,
                extra={'category': 'market_breadth'},
            )
            investor_flow_context_provenance = _build_pykrx_block_freshness_provenance(
                requested_market_date=normalized_market_date,
                rows=(
                    symbol_enrichment_rows.get('investor_flow_symbol'),
                    market_enrichment_rows.get('investor_flow_market'),
                ),
                source_provider='pykrx',
                provenance_status=PROVENANCE_STATUS_PROVIDER_SOURCED,
                extra={'categories': ['investor_flow_symbol', 'investor_flow_market']},
            )
            short_selling_context_provenance = _build_pykrx_block_freshness_provenance(
                requested_market_date=normalized_market_date,
                rows=(symbol_enrichment_rows.get('short_selling'),),
                source_provider='pykrx',
                provenance_status=PROVENANCE_STATUS_PROVIDER_SOURCED,
                extra={'category': 'short_selling'},
            )
            valuation_context_provenance = _build_pykrx_block_freshness_provenance(
                requested_market_date=normalized_market_date,
                rows=(
                    symbol_enrichment_rows.get('valuation_snapshot'),
                    symbol_enrichment_rows.get('foreign_ownership'),
                ),
                source_provider='pykrx',
                provenance_status=PROVENANCE_STATUS_PROVIDER_SOURCED,
                extra={'categories': ['valuation_snapshot', 'foreign_ownership']},
            )
            if quote_context is not None:
                quote_context = {
                    **quote_context,
                    'freshness_provenance': quote_context_provenance,
                }
            if price_context is not None:
                price_context = {
                    **price_context,
                    'freshness_provenance': price_context_provenance,
                }
            if signal_features is not None:
                signal_features = {
                    **signal_features,
                    'freshness_provenance': signal_features_provenance,
                }
            if intraday_execution_context is not None:
                intraday_execution_context = {
                    **intraday_execution_context,
                    'freshness_provenance': intraday_execution_provenance,
                }
            if sector_peer_context is not None:
                sector_peer_context = {
                    **sector_peer_context,
                    'freshness_provenance': sector_peer_context_provenance,
                }
            if market_breadth_context is not None:
                market_breadth_context = {
                    **market_breadth_context,
                    'freshness_provenance': market_breadth_context_provenance,
                }
            if investor_flow_context is not None:
                investor_flow_context = {
                    **investor_flow_context,
                    'freshness_provenance': investor_flow_context_provenance,
                }
            if short_selling_context is not None:
                short_selling_context = {
                    **short_selling_context,
                    'freshness_provenance': short_selling_context_provenance,
                }
            if valuation_context is not None:
                valuation_context = {
                    **valuation_context,
                    'freshness_provenance': valuation_context_provenance,
                }
            fundamentals = {
                **fundamentals,
                'disclosure_context': disclosure_context,
                'freshness_provenance': fundamentals_provenance,
            }
            if isinstance(fundamentals.get('latest_snapshot'), dict):
                if isinstance(fundamentals['latest_snapshot'].get('fundamental_context'), dict):
                    fundamentals['latest_snapshot']['fundamental_context'] = {
                        **fundamentals['latest_snapshot']['fundamental_context'],
                        'source_provenance': {
                            **(fundamentals['latest_snapshot']['fundamental_context'].get('source_provenance') or {}),
                            'freshness_provenance': fundamentals_provenance,
                        },
                    }
                fundamentals['latest_snapshot'] = {
                    **fundamentals['latest_snapshot'],
                    'freshness_provenance': fundamentals_provenance,
                }
            scores = {
                family: (
                    {
                        **score_payload,
                        'freshness_provenance': factor_row_provenance[family],
                    }
                    if isinstance(score_payload, dict)
                    else score_payload
                )
                for family, score_payload in scores.items()
            }
            factor_summary = _build_factor_summary(
                requested_market_date=normalized_market_date,
                factor_rows=current_factor_rows,
                factor_provenance=factor_row_provenance,
                factor_storage_unavailable=factor_storage_unavailable,
                factor_prior_storage_unavailable=factor_prior_storage_unavailable,
                scores=scores,
                signal_features=signal_features,
                signal_features_provenance=signal_features_provenance,
                signal_features_storage_unavailable=signal_features_storage_unavailable,
                signal_features_prior_storage_unavailable=signal_features_prior_storage_unavailable,
            )
            liquidity_context = _build_compact_liquidity_context(
                requested_market_date=normalized_market_date,
                quote_context=quote_context,
                quote_freshness_provenance=quote_context_provenance,
                price_context=price_context,
                price_context_history=price_context_history,
                price_context_freshness_provenance=price_context_provenance,
                price_context_storage_unavailable=price_context_storage_unavailable,
                low_risk_row=current_factor_rows['low_risk'],
                latest_prior_low_risk_row=latest_prior_low_risk_row,
                low_risk_freshness_provenance=factor_row_provenance['low_risk'],
                low_risk_storage_unavailable=bool(factor_storage_unavailable.get('low_risk')),
                low_risk_prior_storage_unavailable=bool(factor_prior_storage_unavailable.get('low_risk')),
                intraday_execution_context=intraday_execution_context,
                intraday_execution_freshness_provenance=intraday_execution_provenance,
            )

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
                'intraday_execution_context': intraday_execution_context is not None,
                'technical_snapshot': technical_snapshot is not None,
                'action_summary': action_summary is not None,
                'signal_features': signal_features is not None,
                'market_relative_context': (
                    isinstance(market_relative_context, dict)
                    and str(market_relative_context.get('status') or 'unavailable') != 'unavailable'
                ),
                'sector_peer_context': sector_peer_context is not None,
                'market_breadth_context': market_breadth_context is not None,
                'investor_flow_context': investor_flow_context is not None,
                'short_selling_context': short_selling_context is not None,
                'valuation_context': valuation_context is not None,
                'persisted_factor_rows': any(row is not None for row in persisted_factor_rows.values()),
                'fundamentals': fundamentals['latest_snapshot'] is not None,
                'recent_disclosures': bool(recent_disclosures),
                'recent_dart_events': recent_dart_events.get('status') == 'available',
                'recent_external_news': recent_external_news.get('status') == 'available',
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
                    'liquidity_context': liquidity_context,
                    'intraday_execution_context': intraday_execution_context,
                    'price_context_history': price_context_history,
                    'technical_snapshot': technical_snapshot,
                    'action_summary': action_summary,
                    'signal_features': signal_features,
                    'market_relative_context': market_relative_context,
                    'sector_peer_context': sector_peer_context,
                    'market_breadth_context': market_breadth_context,
                    'investor_flow_context': investor_flow_context,
                    'short_selling_context': short_selling_context,
                    'valuation_context': valuation_context,
                    'scores': scores,
                    'factor_summary': factor_summary,
                    'persisted_factor_rows': persisted_factor_rows,
                    'fundamentals': fundamentals,
                    'recent_disclosures': {
                        'count': len(recent_disclosures),
                        'items': recent_disclosures,
                        'freshness_provenance': recent_disclosures_provenance,
                    },
                    'recent_dart_events': {
                        **recent_dart_events,
                        'freshness_provenance': recent_disclosures_provenance,
                    },
                    'recent_external_news': {
                        **recent_external_news,
                        'freshness_provenance': recent_external_news_provenance,
                    },
                    'context_completeness': context_completeness,
                    'context_provenance': {
                        'quote_context': quote_context_provenance,
                        'price_context': price_context_provenance,
                        'intraday_execution_context': intraday_execution_provenance,
                        'signal_features': signal_features_provenance,
                        'sector_peer_context': sector_peer_context_provenance,
                        'market_breadth_context': market_breadth_context_provenance,
                        'investor_flow_context': investor_flow_context_provenance,
                        'short_selling_context': short_selling_context_provenance,
                        'valuation_context': valuation_context_provenance,
                        'fundamentals': fundamentals_provenance,
                        'recent_disclosures': recent_disclosures_provenance,
                        'recent_dart_events': recent_disclosures_provenance,
                        'recent_external_news': recent_external_news_provenance,
                        'factor_rows': factor_row_provenance,
                    },
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
        intraday_execution_available_count = sum(
            1
            for item in items
            if str((item.get('intraday_execution_context') or {}).get('status') or 'unavailable') == 'available'
        )
        intraday_execution_partial_count = sum(
            1
            for item in items
            if str((item.get('intraday_execution_context') or {}).get('status') or 'unavailable') == 'partial'
        )
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
        sector_peer_available_count = sum(
            1
            for item in items
            if str((item.get('sector_peer_context') or {}).get('status') or 'unavailable') == 'available'
        )
        sector_peer_partial_count = sum(
            1
            for item in items
            if str((item.get('sector_peer_context') or {}).get('status') or 'unavailable') == 'partial'
        )
        market_breadth_available_count = sum(
            1
            for item in items
            if str((item.get('market_breadth_context') or {}).get('status') or 'unavailable') == 'available'
        )
        market_breadth_partial_count = sum(
            1
            for item in items
            if str((item.get('market_breadth_context') or {}).get('status') or 'unavailable') == 'partial'
        )
        investor_flow_available_count = sum(
            1
            for item in items
            if str((item.get('investor_flow_context') or {}).get('status') or 'unavailable') == 'available'
        )
        investor_flow_partial_count = sum(
            1
            for item in items
            if str((item.get('investor_flow_context') or {}).get('status') or 'unavailable') == 'partial'
        )
        short_selling_available_count = sum(
            1
            for item in items
            if str((item.get('short_selling_context') or {}).get('status') or 'unavailable') == 'available'
        )
        short_selling_partial_count = sum(
            1
            for item in items
            if str((item.get('short_selling_context') or {}).get('status') or 'unavailable') == 'partial'
        )
        valuation_available_count = sum(
            1
            for item in items
            if str((item.get('valuation_context') or {}).get('status') or 'unavailable') == 'available'
        )
        valuation_partial_count = sum(
            1
            for item in items
            if str((item.get('valuation_context') or {}).get('status') or 'unavailable') == 'partial'
        )
        recent_dart_events_available_count = sum(
            1
            for item in items
            if str((item.get('recent_dart_events') or {}).get('status') or 'unavailable') == 'available'
        )
        recent_dart_events_unavailable_count = sum(
            1
            for item in items
            if str((item.get('recent_dart_events') or {}).get('status') or 'unavailable') == 'unavailable'
        )
        external_news_available_count = sum(
            1
            for item in items
            if str((item.get('recent_external_news') or {}).get('status') or 'unavailable') == 'available'
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
            'intraday_execution_context': _build_symbol_block_coverage(
                available_count=intraday_execution_available_count,
                partial_count=intraday_execution_partial_count,
                requested_count=requested_symbol_count,
            ),
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
            'sector_peer_mapping': _build_symbol_block_coverage(
                available_count=sector_peer_available_count,
                partial_count=sector_peer_partial_count,
                requested_count=requested_symbol_count,
            ),
            'market_breadth_context': _build_symbol_block_coverage(
                available_count=market_breadth_available_count,
                partial_count=market_breadth_partial_count,
                requested_count=requested_symbol_count,
            ),
            'investor_flow_context': _build_symbol_block_coverage(
                available_count=investor_flow_available_count,
                partial_count=investor_flow_partial_count,
                requested_count=requested_symbol_count,
            ),
            'short_selling_context': _build_symbol_block_coverage(
                available_count=short_selling_available_count,
                partial_count=short_selling_partial_count,
                requested_count=requested_symbol_count,
            ),
            'valuation_context': _build_symbol_block_coverage(
                available_count=valuation_available_count,
                partial_count=valuation_partial_count,
                requested_count=requested_symbol_count,
            ),
            'recent_dart_events': {
                'available_rows': recent_dart_events_available_count,
                'partial_rows': 0,
                'unavailable_rows': recent_dart_events_unavailable_count,
                'empty_rows': requested_symbol_count - recent_dart_events_available_count - recent_dart_events_unavailable_count,
                'coverage_ratio': _ratio(recent_dart_events_available_count, requested_symbol_count),
                'coverage_status': (
                    'complete'
                    if recent_dart_events_available_count == requested_symbol_count and requested_symbol_count > 0
                    else 'partial'
                    if recent_dart_events_available_count > 0
                    else 'unavailable'
                    if recent_dart_events_unavailable_count == requested_symbol_count and requested_symbol_count > 0
                    else 'missing'
                ),
            },
            'recent_external_news': {
                'available_rows': external_news_available_count,
                'partial_rows': 0,
                'unavailable_rows': requested_symbol_count - external_news_available_count,
                'coverage_ratio': _ratio(external_news_available_count, requested_symbol_count),
                'coverage_status': (
                    'complete'
                    if external_news_available_count == requested_symbol_count and requested_symbol_count > 0
                    else 'partial'
                    if external_news_available_count > 0
                    else 'missing'
                ),
                'provider_status': external_news_provider_status,
            },
        }
        for item in items:
            item['context_completeness']['requested_symbol_coverage'] = requested_symbol_coverage
            item['context_provenance']['signal_features']['requested_symbol_coverage'] = requested_symbol_coverage['signal_features']
            item['context_provenance']['intraday_execution_context']['requested_symbol_coverage'] = requested_symbol_coverage['intraday_execution_context']
            item['context_provenance']['sector_peer_context']['requested_symbol_coverage'] = requested_symbol_coverage['sector_peer_mapping']
            item['context_provenance']['market_breadth_context']['requested_symbol_coverage'] = requested_symbol_coverage['market_breadth_context']
            item['context_provenance']['investor_flow_context']['requested_symbol_coverage'] = requested_symbol_coverage['investor_flow_context']
            item['context_provenance']['short_selling_context']['requested_symbol_coverage'] = requested_symbol_coverage['short_selling_context']
            item['context_provenance']['valuation_context']['requested_symbol_coverage'] = requested_symbol_coverage['valuation_context']
            item['context_provenance']['recent_dart_events']['requested_symbol_coverage'] = requested_symbol_coverage['recent_dart_events']
            item['context_provenance']['recent_external_news']['requested_symbol_coverage'] = requested_symbol_coverage['recent_external_news']
            for family in PERSISTED_FACTOR_FAMILIES:
                item['context_provenance']['factor_rows'][family]['requested_symbol_coverage'] = factor_row_coverage[family]

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
                'intraday_execution_context_count': sum(1 for item in items if item['intraday_execution_context'] is not None),
                'technical_snapshot_count': sum(1 for item in items if item['technical_snapshot'] is not None),
                'action_summary_count': sum(1 for item in items if item['action_summary'] is not None),
                'signal_feature_count': sum(1 for item in items if item['signal_features'] is not None),
                'market_relative_context_count': sum(1 for item in items if item['market_relative_context'] is not None),
                'sector_peer_context_count': sum(1 for item in items if item['sector_peer_context'] is not None),
                'market_breadth_context_count': sum(1 for item in items if item['market_breadth_context'] is not None),
                'investor_flow_context_count': sum(1 for item in items if item['investor_flow_context'] is not None),
                'short_selling_context_count': sum(1 for item in items if item['short_selling_context'] is not None),
                'valuation_context_count': sum(1 for item in items if item['valuation_context'] is not None),
                'fundamentals_count': sum(
                    1 for item in items if item['fundamentals']['latest_snapshot'] is not None
                ),
                'recent_disclosure_symbol_count': sum(
                    1 for item in items if item['recent_disclosures']['count'] > 0
                ),
                'recent_dart_event_symbol_count': sum(
                    1 for item in items if item['recent_dart_events']['status'] == 'available'
                ),
                'recent_external_news_symbol_count': sum(
                    1 for item in items if item['recent_external_news']['status'] == 'available'
                ),
                'external_news_provider': external_news_provider_status,
                'factor_row_counts': factor_row_counts,
                'context_completeness': requested_symbol_coverage,
            },
            'items': items,
        }


agent_context_read_service = AgentContextReadService()
