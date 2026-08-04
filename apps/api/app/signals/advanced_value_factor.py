from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
import re
from typing import Any, Sequence

from sqlalchemy import func, or_, select, text

from app.db import session_scope
from app.db_models import (
    DartDisclosureEvent,
    DartFundamentalsHighlight,
    DartFundamentalsRow,
    DartFundamentalsSnapshot,
)
from app.market_data_store import (
    list_symbols_with_daily_bar_on_date,
    load_daily_advanced_value_factor_results,
    load_recent_price_bars_by_symbol,
    load_symbols,
    upsert_daily_advanced_value_factor_result,
)

ADVANCED_VALUE_FACTOR_VERSION = 'advanced_value_v1'
ADVANCED_VALUE_FACTOR_SOURCE = ADVANCED_VALUE_FACTOR_VERSION
ADVANCED_VALUE_FUNDAMENTALS_SNAPSHOT_CHUNK_SIZE = 100
ADVANCED_VALUE_FUNDAMENTALS_ROW_CHUNK_SIZE = 100
ADVANCED_VALUE_FUNDAMENTALS_ROW_FALLBACK_MAX_SNAPSHOTS = 500
ADVANCED_VALUE_FUNDAMENTALS_FS_DIV = 'CFS'
ADVANCED_VALUE_FUNDAMENTALS_REPORT_CODE = '11011'
ADVANCED_VALUE_FUNDAMENTALS_PERIOD_TYPE = 'annual'
ADVANCED_VALUE_MIN_COMPONENTS = 2
ADVANCED_VALUE_ELIGIBLE_SCORE = 0.70
ADVANCED_VALUE_WATCHLIST_SCORE = 0.50
ADVANCED_VALUE_SCORE_WEIGHTS = {
    'fcf_yield_percentile': 0.50,
    'ev_to_ebitda_percentile': 0.50,
}
EMPTY_RANKING_UNIVERSE_COUNTS = {
    'fcf_yield': 0,
    'ev_to_ebitda': 0,
    'advanced_value_score': 0,
}

METRIC_STATUS_AVAILABLE = 'available'
METRIC_STATUS_MISSING_MARKET_DATE_BAR = 'missing_market_date_bar'
METRIC_STATUS_MISSING_FUNDAMENTALS_SNAPSHOT = 'missing_fundamentals_snapshot'
METRIC_STATUS_MISSING_OPERATING_CASH_FLOW = 'missing_operating_cash_flow'
METRIC_STATUS_MISSING_CAPITAL_EXPENDITURE = 'missing_capital_expenditure'
METRIC_STATUS_MISSING_TOTAL_LIABILITIES = 'missing_total_liabilities'
METRIC_STATUS_MISSING_CASH_AND_CASH_EQUIVALENTS = 'missing_cash_and_cash_equivalents'
METRIC_STATUS_MISSING_OPERATING_INCOME = 'missing_operating_income'
METRIC_STATUS_MISSING_DEPRECIATION_AND_AMORTIZATION = 'missing_depreciation_and_amortization'
METRIC_STATUS_INVALID_MARKET_CAP_PROXY = 'invalid_market_cap_proxy'
METRIC_STATUS_INVALID_ENTERPRISE_VALUE = 'invalid_enterprise_value'
METRIC_STATUS_INVALID_NON_POSITIVE_EBITDA = 'invalid_non_positive_ebitda'

CLASSIFICATION_ELIGIBLE = 'eligible'
CLASSIFICATION_WATCHLIST = 'watchlist'
CLASSIFICATION_INELIGIBLE = 'ineligible'
CLASSIFICATION_INSUFFICIENT_COMPONENTS = 'insufficient_components'
CLASSIFICATION_MISSING_MARKET_DATE_BAR = 'missing_market_date_bar'
CLASSIFICATION_INSUFFICIENT_FUNDAMENTALS = 'insufficient_fundamentals'

MATCH_NORMALIZATION_PATTERN = re.compile(r'[\s\-\_\(\)\[\]\{\}/\\,.:;]+')

TOTAL_LIABILITIES_MATCHER = {
    'statement_types': {'BS'},
    'account_ids': {'ifrs-full_Liabilities', 'ifrs_Liabilities'},
    'account_names': {'부채총계', 'Liabilities'},
}
TOTAL_EQUITY_MATCHER = {
    'statement_types': {'BS'},
    'account_ids': {'ifrs-full_Equity', 'ifrs_Equity'},
    'account_names': {'자본총계', 'Equity'},
}
OPERATING_INCOME_MATCHER = {
    'statement_types': {'IS', 'CIS'},
    'account_ids': {
        'dart_OperatingIncomeLoss',
        'ifrs-full_ProfitLossFromOperatingActivities',
        'ifrs_ProfitLossFromOperatingActivities',
    },
    'account_names': {'영업이익', '영업손실', '영업이익(손실)', '영업손익', 'Operating income'},
}
OPERATING_CASH_FLOW_MATCHER = {
    'statement_types': {'CF'},
    'account_ids': {
        'ifrs-full_CashFlowsFromUsedInOperatingActivities',
        'ifrs-full_NetCashFlowsFromUsedInOperatingActivities',
    },
    'account_names': {
        '영업활동으로 인한 현금흐름',
        '영업활동현금흐름',
        '영업활동순현금흐름',
        'Cash flows from operating activities',
    },
}
CASH_AND_CASH_EQUIVALENTS_MATCHER = {
    'statement_types': {'BS'},
    'account_ids': {
        'ifrs-full_CashAndCashEquivalents',
        'ifrs_CashAndCashEquivalents',
    },
    'account_names': {
        '현금및현금성자산',
        'Cash and cash equivalents',
    },
    'account_name_contains': {
        '현금및현금성자산',
        'cashandcashequivalents',
    },
}
CAPEX_PPE_MATCHER = {
    'statement_types': {'CF'},
    'account_ids': {
        'ifrs-full_PurchaseOfPropertyPlantAndEquipment',
        'ifrs-full_AcquisitionOfPropertyPlantAndEquipment',
    },
    'account_names': {
        '유형자산의 취득',
        '유형자산 취득',
        'Acquisition of property, plant and equipment',
        'Purchase of property, plant and equipment',
    },
    'account_name_contains': {
        '유형자산의취득',
        '유형자산취득',
        'propertyplantandequipment',
    },
    'account_name_excludes': {
        '유형자산의처분',
        'disposalofpropertyplantandequipment',
    },
    'reject_excluded_name_matches': True,
}
CAPEX_INTANGIBLE_MATCHER = {
    'statement_types': {'CF'},
    'account_ids': {
        'ifrs-full_PurchaseOfIntangibleAssets',
        'ifrs-full_AcquisitionOfIntangibleAssetsOtherThanGoodwill',
    },
    'account_names': {
        '무형자산의 취득',
        '무형자산 취득',
        'Acquisition of intangible assets',
        'Purchase of intangible assets',
    },
    'account_name_contains': {
        '무형자산의취득',
        '무형자산취득',
        'intangibleassets',
    },
    'account_name_excludes': {
        '무형자산의처분',
        'disposalofintangibleassets',
    },
    'reject_excluded_name_matches': True,
}
DEPRECIATION_AND_AMORTIZATION_MATCHER = {
    'statement_types': {'CF', 'IS', 'CIS'},
    'account_ids': set(),
    'account_names': {
        '감가상각비및상각비',
        '감가상각비와상각비',
        'Depreciation and amortisation',
        'Depreciation and amortization',
    },
    'account_name_contains': {
        '감가상각비및상각비',
        '감가상각비와상각비',
        'depreciationandamortisation',
        'depreciationandamortization',
    },
}
DEPRECIATION_MATCHER = {
    'statement_types': {'CF', 'IS', 'CIS'},
    'account_ids': set(),
    'account_names': {
        '감가상각비',
        'Depreciation',
    },
    'account_name_contains': {
        '감가상각비',
        'depreciation',
    },
    'account_name_excludes': {
        '상각비및',
        'depreciationandamortisation',
        'depreciationandamortization',
    },
}
AMORTIZATION_MATCHER = {
    'statement_types': {'CF', 'IS', 'CIS'},
    'account_ids': set(),
    'account_names': {
        '상각비',
        '무형자산상각비',
        'Amortisation',
        'Amortization',
    },
    'account_name_contains': {
        '무형자산상각비',
        'amortisation',
        'amortization',
    },
    'account_name_excludes': {
        '감가상각비및',
        'depreciationandamortisation',
        'depreciationandamortization',
    },
}

FUNDAMENTALS_FALLBACK_MATCHERS = (
    TOTAL_LIABILITIES_MATCHER,
    TOTAL_EQUITY_MATCHER,
    OPERATING_INCOME_MATCHER,
    OPERATING_CASH_FLOW_MATCHER,
    CASH_AND_CASH_EQUIVALENTS_MATCHER,
    CAPEX_PPE_MATCHER,
    CAPEX_INTANGIBLE_MATCHER,
    DEPRECIATION_AND_AMORTIZATION_MATCHER,
    DEPRECIATION_MATCHER,
    AMORTIZATION_MATCHER,
)


def _round(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def _normalize_market_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _normalize_symbol(symbol: str) -> str:
    normalized = str(symbol or '').strip().upper()
    if not normalized:
        raise ValueError('symbol must not be blank')
    if normalized.isdigit():
        normalized = normalized.zfill(6)
    return normalized


def _normalize_symbols(symbols: Sequence[str] | None) -> list[str] | None:
    if symbols is None:
        return None
    return list(dict.fromkeys(_normalize_symbol(symbol) for symbol in symbols))


def _matcher_values(key: str) -> set[str]:
    values: set[str] = set()
    for matcher in FUNDAMENTALS_FALLBACK_MATCHERS:
        values.update(str(value).strip() for value in matcher.get(key, set()) if str(value or '').strip())
    return values


def _fallback_statement_row_filter():
    account_ids = _matcher_values('account_ids')
    account_names = _matcher_values('account_names')
    contains_tokens = _matcher_values('account_name_contains')
    conditions = []
    if account_ids:
        conditions.append(DartFundamentalsRow.account_id.in_(account_ids))
    if account_names:
        conditions.append(DartFundamentalsRow.account_name.in_(account_names))
    for token in contains_tokens:
        conditions.append(DartFundamentalsRow.account_name.ilike(f'%{token}%'))
    return or_(*conditions)


def _percentile_from_rank(rank: int, total: int) -> float:
    if total <= 1:
        return 1.0
    return _round((total - rank) / (total - 1), 6) or 0.0


def _sort_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        items,
        key=lambda item: (
            not bool(item.get('is_advanced_value_eligible')),
            item.get('advanced_value_score') is None,
            -(float(item['advanced_value_score'])) if item.get('advanced_value_score') is not None else 999999.0,
            float(item['ev_to_ebitda']) if item.get('ev_to_ebitda') is not None else 999999.0,
            item['symbol'],
        ),
    )


def _normalize_match_text(value: str | None) -> str:
    if not value:
        return ''
    return MATCH_NORMALIZATION_PATTERN.sub('', str(value).strip().lower())


def _serialize_highlight_row(row: DartFundamentalsHighlight) -> dict[str, Any]:
    return {
        'metric_key': row.metric_key,
        'statement_type': row.statement_type,
        'statement_name': row.statement_name,
        'account_id': row.account_id,
        'account_name': row.account_name,
        'current_amount': row.current_amount,
        'current_cumulative_amount': row.current_cumulative_amount,
        'ratio_value': _round(float(row.ratio_value), 6) if row.ratio_value is not None else None,
        'currency': row.currency,
    }


def _serialize_statement_row(row: DartFundamentalsRow) -> dict[str, Any]:
    return {
        'row_key': row.row_key,
        'statement_type': row.statement_type,
        'statement_name': row.statement_name,
        'account_id': row.account_id,
        'account_name': row.account_name,
        'account_detail': row.account_detail,
        'current_amount': row.current_amount,
        'current_cumulative_amount': row.current_cumulative_amount,
        'row_order': row.row_order,
        'currency': row.currency,
    }


def _highlight_amount(
    highlight_row: dict[str, Any] | None,
    *,
    flow: bool,
    absolute: bool = False,
) -> tuple[float | None, str | None]:
    if highlight_row is None:
        return None, None
    if flow and highlight_row.get('current_amount') is None and highlight_row.get('current_cumulative_amount') is not None:
        amount_field = 'current_cumulative_amount'
    else:
        amount_field = 'current_amount'
    amount = highlight_row.get(amount_field)
    if amount is None:
        return None, amount_field
    value = float(amount)
    if absolute:
        value = abs(value)
    return _round(value, 4), amount_field


def _statement_amount(
    row: dict[str, Any] | None,
    *,
    flow: bool,
    absolute: bool = False,
) -> tuple[float | None, str | None]:
    if row is None:
        return None, None
    if flow and row.get('current_amount') is None and row.get('current_cumulative_amount') is not None:
        amount_field = 'current_cumulative_amount'
    else:
        amount_field = 'current_amount'
    amount = row.get(amount_field)
    if amount is None:
        return None, amount_field
    value = float(amount)
    if absolute:
        value = abs(value)
    return _round(value, 4), amount_field


def _build_source_metadata(
    *,
    source_type: str,
    row: dict[str, Any] | None,
    amount_field: str | None,
) -> dict[str, Any] | None:
    if row is None:
        return None
    amount_value = row.get(amount_field) if amount_field else None
    return {
        'source_type': source_type,
        'statement_type': row.get('statement_type'),
        'statement_name': row.get('statement_name'),
        'account_id': row.get('account_id'),
        'account_name': row.get('account_name'),
        'account_detail': row.get('account_detail'),
        'row_key': row.get('row_key'),
        'amount_field': amount_field,
        'amount_value': _round(float(amount_value), 4) if amount_value is not None else None,
        'currency': row.get('currency'),
    }


def _match_score(
    row: dict[str, Any],
    *,
    matcher: dict[str, Any],
) -> tuple[int, int, int, int, int] | None:
    normalized_statement_types = {
        (value or '').strip().upper() for value in matcher.get('statement_types', set()) if value
    }
    normalized_account_ids = {_normalize_match_text(value) for value in matcher.get('account_ids', set()) if value}
    normalized_account_names = {_normalize_match_text(value) for value in matcher.get('account_names', set()) if value}
    normalized_account_name_contains = {
        _normalize_match_text(value) for value in matcher.get('account_name_contains', set()) if value
    }
    normalized_account_name_excludes = {
        _normalize_match_text(value) for value in matcher.get('account_name_excludes', set()) if value
    }

    account_id = _normalize_match_text(row.get('account_id'))
    account_name = _normalize_match_text(row.get('account_name'))
    statement_type = (row.get('statement_type') or '').strip().upper()

    id_match = account_id in normalized_account_ids if account_id else False
    exact_name_match = account_name in normalized_account_names if account_name else False
    contains_name_match = bool(account_name) and any(token in account_name for token in normalized_account_name_contains)
    excluded_name_match = bool(account_name) and any(token in account_name for token in normalized_account_name_excludes)
    if excluded_name_match and matcher.get('reject_excluded_name_matches'):
        return None

    if not id_match and not exact_name_match and not contains_name_match:
        return None

    score = 0
    if normalized_statement_types:
        if statement_type in normalized_statement_types:
            score += 40
        else:
            score -= 40
    if id_match:
        score += 120
    if exact_name_match:
        score += 100
    if contains_name_match:
        score += 80
    if excluded_name_match:
        score -= 100

    row_order = row.get('row_order')
    row_bias = -(int(row_order) if row_order is not None else 999999)
    current_amount_present = 1 if row.get('current_amount') is not None else 0
    cumulative_amount_present = 1 if row.get('current_cumulative_amount') is not None else 0
    return (score, row_bias, current_amount_present, cumulative_amount_present, 0)


def _find_statement_row(rows: Sequence[dict[str, Any]], matcher: dict[str, Any]) -> dict[str, Any] | None:
    best_row: dict[str, Any] | None = None
    best_score: tuple[int, int, int, int, int] | None = None
    for row in rows:
        score = _match_score(row, matcher=matcher)
        if score is None:
            continue
        if best_score is None or score > best_score:
            best_row = row
            best_score = score
    return best_row


def _extract_metric_with_fallback(
    *,
    highlight_row: dict[str, Any] | None,
    rows: Sequence[dict[str, Any]],
    matcher: dict[str, Any],
    flow: bool,
    absolute: bool = False,
) -> tuple[float | None, dict[str, Any] | None]:
    value, amount_field = _highlight_amount(highlight_row, flow=flow, absolute=absolute)
    if value is not None:
        return value, _build_source_metadata(source_type='highlight', row=highlight_row, amount_field=amount_field)
    matched_row = _find_statement_row(rows, matcher)
    value, amount_field = _statement_amount(matched_row, flow=flow, absolute=absolute)
    return value, _build_source_metadata(source_type='statement_row', row=matched_row, amount_field=amount_field)


@dataclass(slots=True)
class AnnualFundamentalsSnapshot:
    symbol: str
    snapshot_id: int
    corp_name: str
    business_year: int
    report_code: str
    fs_div: str
    period_type: str
    receipt_date: str | None
    rcept_no: str
    captured_at: str | None
    currency: str | None
    highlights_by_key: dict[str, dict[str, Any]]
    statement_rows: list[dict[str, Any]]


@dataclass(slots=True)
class AdvancedValueFactorInput:
    symbol: str
    name_ko: str | None
    market: str | None
    bars: list[dict[str, Any]]
    annual_fundamentals: AnnualFundamentalsSnapshot | None


def derive_annual_financial_context(snapshot: AnnualFundamentalsSnapshot) -> dict[str, Any]:
    highlights = snapshot.highlights_by_key
    rows = snapshot.statement_rows

    total_equity, total_equity_source = _extract_metric_with_fallback(
        highlight_row=highlights.get('total_equity'),
        rows=rows,
        matcher=TOTAL_EQUITY_MATCHER,
        flow=False,
    )
    total_liabilities, total_liabilities_source = _extract_metric_with_fallback(
        highlight_row=highlights.get('total_liabilities'),
        rows=rows,
        matcher=TOTAL_LIABILITIES_MATCHER,
        flow=False,
    )
    operating_income, operating_income_source = _extract_metric_with_fallback(
        highlight_row=highlights.get('operating_income'),
        rows=rows,
        matcher=OPERATING_INCOME_MATCHER,
        flow=True,
    )
    operating_cash_flow, operating_cash_flow_source = _extract_metric_with_fallback(
        highlight_row=highlights.get('operating_cash_flow'),
        rows=rows,
        matcher=OPERATING_CASH_FLOW_MATCHER,
        flow=True,
    )

    cash_row = _find_statement_row(rows, CASH_AND_CASH_EQUIVALENTS_MATCHER)
    cash_and_cash_equivalents, cash_amount_field = _statement_amount(cash_row, flow=False)

    capex_ppe_row = _find_statement_row(rows, CAPEX_PPE_MATCHER)
    capex_ppe, capex_ppe_amount_field = _statement_amount(capex_ppe_row, flow=True, absolute=True)
    capex_intangible_row = _find_statement_row(rows, CAPEX_INTANGIBLE_MATCHER)
    capex_intangible, capex_intangible_amount_field = _statement_amount(
        capex_intangible_row,
        flow=True,
        absolute=True,
    )
    capital_expenditure_components = [value for value in (capex_ppe, capex_intangible) if value is not None]
    capital_expenditure = (
        _round(sum(capital_expenditure_components), 4) if capital_expenditure_components else None
    )

    depreciation_and_amortization_row = _find_statement_row(rows, DEPRECIATION_AND_AMORTIZATION_MATCHER)
    depreciation_row = None
    amortization_row = None
    depreciation = None
    amortization = None
    depreciation_amount_field = None
    amortization_amount_field = None
    depreciation_and_amortization_amount_field = None
    if depreciation_and_amortization_row is not None:
        depreciation_and_amortization, depreciation_and_amortization_amount_field = _statement_amount(
            depreciation_and_amortization_row,
            flow=True,
            absolute=True,
        )
    else:
        depreciation_row = _find_statement_row(rows, DEPRECIATION_MATCHER)
        depreciation, depreciation_amount_field = _statement_amount(depreciation_row, flow=True, absolute=True)
        amortization_row = _find_statement_row(rows, AMORTIZATION_MATCHER)
        amortization, amortization_amount_field = _statement_amount(amortization_row, flow=True, absolute=True)
        if depreciation is None and amortization is None:
            depreciation_and_amortization = None
        else:
            depreciation_and_amortization = _round((depreciation or 0.0) + (amortization or 0.0), 4)

    return {
        'total_equity': total_equity,
        'total_liabilities': total_liabilities,
        'operating_income': operating_income,
        'operating_cash_flow': operating_cash_flow,
        'cash_and_cash_equivalents': cash_and_cash_equivalents,
        'capital_expenditure': capital_expenditure,
        'depreciation_and_amortization': depreciation_and_amortization,
        'audit': {
            'annual_snapshot': {
                'snapshot_id': snapshot.snapshot_id,
                'corp_name': snapshot.corp_name,
                'business_year': snapshot.business_year,
                'report_code': snapshot.report_code,
                'fs_div': snapshot.fs_div,
                'period_type': snapshot.period_type,
                'receipt_date': snapshot.receipt_date,
                'rcept_no': snapshot.rcept_no,
                'captured_at': snapshot.captured_at,
                'currency': snapshot.currency,
            },
            'matched_inputs': {
                'total_equity': total_equity_source,
                'total_liabilities': total_liabilities_source,
                'operating_income': operating_income_source,
                'operating_cash_flow': operating_cash_flow_source,
                'cash_and_cash_equivalents': _build_source_metadata(
                    source_type='statement_row',
                    row=cash_row,
                    amount_field=cash_amount_field,
                ),
                'capex_ppe': _build_source_metadata(
                    source_type='statement_row',
                    row=capex_ppe_row,
                    amount_field=capex_ppe_amount_field,
                ),
                'capex_intangible': _build_source_metadata(
                    source_type='statement_row',
                    row=capex_intangible_row,
                    amount_field=capex_intangible_amount_field,
                ),
                'depreciation_and_amortization': _build_source_metadata(
                    source_type='statement_row',
                    row=depreciation_and_amortization_row,
                    amount_field=depreciation_and_amortization_amount_field,
                ),
                'depreciation': _build_source_metadata(
                    source_type='statement_row',
                    row=depreciation_row,
                    amount_field=depreciation_amount_field,
                ),
                'amortization': _build_source_metadata(
                    source_type='statement_row',
                    row=amortization_row,
                    amount_field=amortization_amount_field,
                ),
            },
        },
    }


def compute_market_cap_proxy(*, pbr: float | None, total_equity: float | None) -> float | None:
    if pbr is None or total_equity is None:
        return None
    return _round(pbr * total_equity, 4)


def compute_free_cash_flow(*, operating_cash_flow: float | None, capital_expenditure: float | None) -> float | None:
    if operating_cash_flow is None or capital_expenditure is None:
        return None
    return _round(operating_cash_flow - capital_expenditure, 4)


def compute_fcf_yield(*, free_cash_flow: float | None, market_cap_proxy: float | None) -> float | None:
    if free_cash_flow is None or market_cap_proxy in (None, 0):
        return None
    return _round(free_cash_flow / market_cap_proxy, 6)


def compute_ebitda(*, operating_income: float | None, depreciation_and_amortization: float | None) -> float | None:
    if operating_income is None or depreciation_and_amortization is None:
        return None
    return _round(operating_income + depreciation_and_amortization, 4)


def compute_enterprise_value(
    *,
    market_cap_proxy: float | None,
    total_liabilities: float | None,
    cash_and_cash_equivalents: float | None,
) -> float | None:
    if market_cap_proxy is None or total_liabilities is None or cash_and_cash_equivalents is None:
        return None
    return _round(market_cap_proxy + total_liabilities - cash_and_cash_equivalents, 4)


def compute_ev_to_ebitda(*, enterprise_value: float | None, ebitda: float | None) -> float | None:
    if enterprise_value is None or ebitda in (None, 0):
        return None
    return _round(enterprise_value / ebitda, 6)


class DailyAdvancedValueFactorService:
    def rebuild_for_market_date(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
        source: str = ADVANCED_VALUE_FACTOR_SOURCE,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbols = _normalize_symbols(symbols)
        requested_symbols = list(normalized_symbols or [])
        if normalized_symbols == []:
            return {
                'market_date': normalized_market_date.isoformat(),
                'source': source,
                'feature_version': source,
                'requested_symbols': requested_symbols,
                'matched_symbols': [],
                'missing_symbols': [],
                'summary': self._build_summary(
                    [],
                    requested_symbols=requested_symbols,
                    matched_symbols=[],
                    missing_symbols=[],
                    universe_counts=EMPTY_RANKING_UNIVERSE_COUNTS,
                ),
                'results_persisted': 0,
                'results_failed': 0,
                'failed_symbols': [],
                'failed_symbol_errors': [],
                'items': [],
            }

        inputs, matched_symbols, missing_symbols = self._load_inputs(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
        )
        if normalized_symbols is not None and not matched_symbols:
            return {
                'market_date': normalized_market_date.isoformat(),
                'source': source,
                'feature_version': source,
                'requested_symbols': requested_symbols,
                'matched_symbols': [],
                'missing_symbols': missing_symbols,
                'summary': self._build_summary(
                    [],
                    requested_symbols=requested_symbols,
                    matched_symbols=[],
                    missing_symbols=missing_symbols,
                    universe_counts=EMPTY_RANKING_UNIVERSE_COUNTS,
                ),
                'results_persisted': 0,
                'results_failed': 0,
                'failed_symbols': [],
                'failed_symbol_errors': [],
                'items': [],
            }
        ranking_inputs = inputs
        if normalized_symbols is not None:
            ranking_inputs, _, _ = self._load_inputs(
                market_date=normalized_market_date,
                symbols=None,
            )

        ranking_items = [
            self._build_symbol_result(
                input_row=item,
                market_date=normalized_market_date,
                source=source,
            )
            for item in ranking_inputs
        ]
        universe_counts = self._assign_metric_rankings(ranking_items)
        self._assign_scores_and_classifications(ranking_items)
        universe_counts['advanced_value_score'] = sum(
            1 for item in ranking_items if item.get('advanced_value_score') is not None
        )

        if normalized_symbols is None:
            items = _sort_items(ranking_items)
        else:
            ranked_items_by_symbol = {item['symbol']: item for item in ranking_items}
            items = []
            for input_row in inputs:
                item = ranked_items_by_symbol.get(input_row.symbol)
                if item is None:
                    item = self._build_symbol_result(
                        input_row=input_row,
                        market_date=normalized_market_date,
                        source=source,
                    )
                    self._assign_scores_and_classifications([item])
                items.append(item)

        for item in items:
            item['factor_payload'] = self._build_factor_payload(
                item=item,
                source=source,
                universe_counts=universe_counts,
            )
        summary = self._build_summary(
            items,
            requested_symbols=requested_symbols,
            matched_symbols=matched_symbols,
            missing_symbols=missing_symbols,
            universe_counts=universe_counts,
        )
        for item in items:
            self._strip_internal_fields(item)

        persisted_count = 0
        failed_symbols: list[str] = []
        failed_symbol_errors: list[dict[str, str]] = []
        for item in items:
            try:
                upsert_daily_advanced_value_factor_result(item)
                persisted_count += 1
            except Exception as exc:
                failed_symbols.append(item['symbol'])
                failed_symbol_errors.append({'symbol': item['symbol'], 'error': str(exc)})

        return {
            'market_date': normalized_market_date.isoformat(),
            'source': source,
            'feature_version': source,
            'requested_symbols': requested_symbols,
            'matched_symbols': matched_symbols,
            'missing_symbols': missing_symbols,
            'summary': summary,
            'results_persisted': persisted_count,
            'results_failed': len(failed_symbols),
            'failed_symbols': failed_symbols,
            'failed_symbol_errors': failed_symbol_errors,
            'items': _sort_items(items),
        }

    def inspect(
        self,
        *,
        market_date: str | date,
        symbol: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbol = _normalize_symbol(symbol) if symbol is not None else None
        requested_symbols = [normalized_symbol] if normalized_symbol else []
        items = load_daily_advanced_value_factor_results(
            market_date=normalized_market_date,
            symbols=requested_symbols or None,
            limit=(1 if normalized_symbol else limit),
        )
        matched_symbols = [item['symbol'] for item in items]
        matched_symbol_set = set(matched_symbols)
        summary = {
            'entries': len(items),
            'requested_symbol': normalized_symbol,
            'matched_symbols': matched_symbols,
            'missing_symbols': [candidate for candidate in requested_symbols if candidate not in matched_symbol_set],
            'eligible_count': sum(1 for item in items if item.get('is_advanced_value_eligible')),
            'watchlist_count': sum(
                1 for item in items if item.get('advanced_value_classification') == CLASSIFICATION_WATCHLIST
            ),
            'ineligible_count': sum(
                1 for item in items if item.get('advanced_value_classification') == CLASSIFICATION_INELIGIBLE
            ),
            'insufficient_components_count': sum(
                1
                for item in items
                if item.get('advanced_value_classification') == CLASSIFICATION_INSUFFICIENT_COMPONENTS
            ),
        }
        payload: dict[str, Any] = {
            'market_date': normalized_market_date.isoformat(),
            'feature_version': ADVANCED_VALUE_FACTOR_VERSION,
            'summary': summary,
        }
        if normalized_symbol:
            payload['item'] = items[0] if items else None
        else:
            payload['items'] = items
        return payload

    def evaluate_symbol(
        self,
        *,
        symbol: str,
        market_date: str | date,
    ) -> dict[str, Any]:
        inspection = self.inspect(market_date=market_date, symbol=symbol)
        return {
            'market_date': inspection['market_date'],
            'feature_version': inspection['feature_version'],
            'symbol': _normalize_symbol(symbol),
            'summary': inspection['summary'],
            'item': inspection.get('item'),
        }

    def _load_inputs(
        self,
        *,
        market_date: date,
        symbols: list[str] | None,
    ) -> tuple[list[AdvancedValueFactorInput], list[str], list[str]]:
        if symbols == []:
            return [], [], []
        if symbols is None:
            matched_symbols = list_symbols_with_daily_bar_on_date(market_date.isoformat())
            if not matched_symbols:
                return [], [], []
            missing_symbols: list[str] = []
        else:
            symbol_rows = load_symbols(symbols=symbols)
            matched_symbols = [item['symbol'] for item in symbol_rows]
            matched_symbol_set = set(matched_symbols)
            missing_symbols = [symbol for symbol in symbols if symbol not in matched_symbol_set]
            if not matched_symbols:
                return [], [], missing_symbols

        symbol_rows = load_symbols(symbols=matched_symbols)
        bars_by_symbol = load_recent_price_bars_by_symbol(
            market_date=market_date,
            symbols=matched_symbols,
            history_days=1,
        )
        fundamentals_by_symbol = self._load_latest_annual_fundamentals(
            market_date=market_date,
            symbols=matched_symbols,
        )
        inputs = [
            AdvancedValueFactorInput(
                symbol=row['symbol'],
                name_ko=row.get('name_ko'),
                market=row.get('market'),
                bars=bars_by_symbol.get(row['symbol'], []),
                annual_fundamentals=fundamentals_by_symbol.get(row['symbol']),
            )
            for row in symbol_rows
        ]
        return inputs, matched_symbols, missing_symbols

    def _load_latest_annual_fundamentals(
        self,
        *,
        market_date: date,
        symbols: Sequence[str],
    ) -> dict[str, AnnualFundamentalsSnapshot]:
        if not symbols:
            return {}

        normalized_symbols = list(dict.fromkeys(symbols))
        with session_scope() as session:
            session.execute(text("SET LOCAL statement_timeout = '60s'"))
            snapshot_rows = []
            for start in range(0, len(normalized_symbols), ADVANCED_VALUE_FUNDAMENTALS_SNAPSHOT_CHUNK_SIZE):
                symbol_chunk = normalized_symbols[start:start + ADVANCED_VALUE_FUNDAMENTALS_SNAPSHOT_CHUNK_SIZE]
                latest_rank = func.row_number().over(
                    partition_by=DartFundamentalsSnapshot.stock_code,
                    order_by=(
                        DartFundamentalsSnapshot.business_year.desc(),
                        DartDisclosureEvent.receipt_date.desc().nullslast(),
                        DartFundamentalsSnapshot.id.desc(),
                    ),
                ).label('row_rank')
                latest_snapshot_subquery = (
                    select(
                        DartFundamentalsSnapshot.id.label('snapshot_id'),
                        DartFundamentalsSnapshot.stock_code.label('stock_code'),
                        DartFundamentalsSnapshot.corp_name.label('corp_name'),
                        DartFundamentalsSnapshot.business_year.label('business_year'),
                        DartFundamentalsSnapshot.report_code.label('report_code'),
                        DartFundamentalsSnapshot.fs_div.label('fs_div'),
                        DartFundamentalsSnapshot.period_type.label('period_type'),
                        DartFundamentalsSnapshot.currency.label('currency'),
                        DartFundamentalsSnapshot.captured_at.label('captured_at'),
                        DartFundamentalsSnapshot.rcept_no.label('rcept_no'),
                        DartDisclosureEvent.receipt_date.label('receipt_date'),
                        latest_rank,
                    )
                    .join(DartDisclosureEvent, DartDisclosureEvent.id == DartFundamentalsSnapshot.disclosure_event_id)
                    .where(
                        DartFundamentalsSnapshot.stock_code.in_(symbol_chunk),
                        DartFundamentalsSnapshot.fs_div == ADVANCED_VALUE_FUNDAMENTALS_FS_DIV,
                        DartFundamentalsSnapshot.report_code == ADVANCED_VALUE_FUNDAMENTALS_REPORT_CODE,
                        DartFundamentalsSnapshot.period_type == ADVANCED_VALUE_FUNDAMENTALS_PERIOD_TYPE,
                        DartFundamentalsSnapshot.business_year <= market_date.year,
                        DartDisclosureEvent.receipt_date <= market_date,
                    )
                    .subquery()
                )
                snapshot_rows.extend(
                    session.execute(
                        select(latest_snapshot_subquery).where(latest_snapshot_subquery.c.row_rank == 1)
                    ).mappings().all()
                )
            if not snapshot_rows:
                return {}

            snapshot_metadata_by_id: dict[int, dict[str, Any]] = {}
            snapshot_id_by_symbol: dict[str, int] = {}
            for row in snapshot_rows:
                mapping = row._mapping if hasattr(row, '_mapping') else row
                snapshot_id = int(mapping['snapshot_id'])
                stock_code = str(mapping['stock_code'])
                snapshot_metadata_by_id[snapshot_id] = {
                    'symbol': stock_code,
                    'snapshot_id': snapshot_id,
                    'corp_name': mapping['corp_name'],
                    'business_year': int(mapping['business_year']),
                    'report_code': mapping['report_code'],
                    'fs_div': mapping['fs_div'],
                    'period_type': mapping['period_type'],
                    'receipt_date': mapping['receipt_date'].isoformat() if mapping['receipt_date'] else None,
                    'rcept_no': mapping['rcept_no'],
                    'captured_at': mapping['captured_at'].isoformat() if mapping['captured_at'] else None,
                    'currency': mapping['currency'],
                }
                snapshot_id_by_symbol[stock_code] = snapshot_id

            snapshot_ids = list(snapshot_metadata_by_id.keys())
            highlights_by_snapshot: dict[int, dict[str, dict[str, Any]]] = defaultdict(dict)
            rows_by_snapshot: dict[int, list[dict[str, Any]]] = defaultdict(list)
            for start in range(0, len(snapshot_ids), ADVANCED_VALUE_FUNDAMENTALS_ROW_CHUNK_SIZE):
                snapshot_id_chunk = snapshot_ids[start:start + ADVANCED_VALUE_FUNDAMENTALS_ROW_CHUNK_SIZE]
                highlight_rows = session.execute(
                    select(DartFundamentalsHighlight)
                    .where(DartFundamentalsHighlight.snapshot_id.in_(snapshot_id_chunk))
                ).scalars()
                for row in highlight_rows:
                    highlights_by_snapshot[row.snapshot_id][row.metric_key] = _serialize_highlight_row(row)
            if len(snapshot_ids) <= ADVANCED_VALUE_FUNDAMENTALS_ROW_FALLBACK_MAX_SNAPSHOTS:
                for start in range(0, len(snapshot_ids), ADVANCED_VALUE_FUNDAMENTALS_ROW_CHUNK_SIZE):
                    snapshot_id_chunk = snapshot_ids[start:start + ADVANCED_VALUE_FUNDAMENTALS_ROW_CHUNK_SIZE]
                    statement_rows = session.execute(
                        select(DartFundamentalsRow)
                        .where(
                            DartFundamentalsRow.snapshot_id.in_(snapshot_id_chunk),
                            _fallback_statement_row_filter(),
                        )
                        .order_by(
                            DartFundamentalsRow.snapshot_id.asc(),
                            DartFundamentalsRow.row_order.asc(),
                            DartFundamentalsRow.id.asc(),
                        )
                    ).scalars()
                    for row in statement_rows:
                        rows_by_snapshot[row.snapshot_id].append(_serialize_statement_row(row))

            result: dict[str, AnnualFundamentalsSnapshot] = {}
            for symbol, snapshot_id in snapshot_id_by_symbol.items():
                metadata = snapshot_metadata_by_id[snapshot_id]
                result[symbol] = AnnualFundamentalsSnapshot(
                    symbol=symbol,
                    snapshot_id=snapshot_id,
                    corp_name=str(metadata['corp_name']),
                    business_year=int(metadata['business_year']),
                    report_code=str(metadata['report_code']),
                    fs_div=str(metadata['fs_div']),
                    period_type=str(metadata['period_type']),
                    receipt_date=metadata['receipt_date'],
                    rcept_no=str(metadata['rcept_no']),
                    captured_at=metadata['captured_at'],
                    currency=metadata['currency'],
                    highlights_by_key=highlights_by_snapshot.get(snapshot_id, {}),
                    statement_rows=rows_by_snapshot.get(snapshot_id, []),
                )
            return result

    def _build_symbol_result(
        self,
        *,
        input_row: AdvancedValueFactorInput,
        market_date: date,
        source: str,
    ) -> dict[str, Any]:
        market_date_iso = market_date.isoformat()
        current_bar = input_row.bars[0] if input_row.bars and input_row.bars[0].get('trade_date') == market_date_iso else None
        annual_fundamentals = input_row.annual_fundamentals
        financial_context = derive_annual_financial_context(annual_fundamentals) if annual_fundamentals is not None else None

        pbr = _round(float(current_bar['pbr']), 6) if current_bar and current_bar.get('pbr') is not None else None
        total_equity = financial_context.get('total_equity') if financial_context else None
        total_liabilities = financial_context.get('total_liabilities') if financial_context else None
        operating_income = financial_context.get('operating_income') if financial_context else None
        operating_cash_flow = financial_context.get('operating_cash_flow') if financial_context else None
        cash_and_cash_equivalents = financial_context.get('cash_and_cash_equivalents') if financial_context else None
        capital_expenditure = financial_context.get('capital_expenditure') if financial_context else None
        depreciation_and_amortization = (
            financial_context.get('depreciation_and_amortization') if financial_context else None
        )

        market_cap_proxy = compute_market_cap_proxy(pbr=pbr, total_equity=total_equity)
        free_cash_flow = compute_free_cash_flow(
            operating_cash_flow=operating_cash_flow,
            capital_expenditure=capital_expenditure,
        )
        fcf_yield = compute_fcf_yield(
            free_cash_flow=free_cash_flow,
            market_cap_proxy=market_cap_proxy,
        )
        ebitda = compute_ebitda(
            operating_income=operating_income,
            depreciation_and_amortization=depreciation_and_amortization,
        )
        enterprise_value = compute_enterprise_value(
            market_cap_proxy=market_cap_proxy,
            total_liabilities=total_liabilities,
            cash_and_cash_equivalents=cash_and_cash_equivalents,
        )
        ev_to_ebitda = compute_ev_to_ebitda(
            enterprise_value=enterprise_value,
            ebitda=ebitda,
        )

        if current_bar is None:
            fcf_yield_status = METRIC_STATUS_MISSING_MARKET_DATE_BAR
            ev_to_ebitda_status = METRIC_STATUS_MISSING_MARKET_DATE_BAR
        elif annual_fundamentals is None:
            fcf_yield_status = METRIC_STATUS_MISSING_FUNDAMENTALS_SNAPSHOT
            ev_to_ebitda_status = METRIC_STATUS_MISSING_FUNDAMENTALS_SNAPSHOT
        else:
            if operating_cash_flow is None:
                fcf_yield_status = METRIC_STATUS_MISSING_OPERATING_CASH_FLOW
            elif capital_expenditure is None:
                fcf_yield_status = METRIC_STATUS_MISSING_CAPITAL_EXPENDITURE
            elif market_cap_proxy is None or market_cap_proxy <= 0:
                fcf_yield_status = METRIC_STATUS_INVALID_MARKET_CAP_PROXY
            else:
                fcf_yield_status = METRIC_STATUS_AVAILABLE

            if total_liabilities is None:
                ev_to_ebitda_status = METRIC_STATUS_MISSING_TOTAL_LIABILITIES
            elif cash_and_cash_equivalents is None:
                ev_to_ebitda_status = METRIC_STATUS_MISSING_CASH_AND_CASH_EQUIVALENTS
            elif operating_income is None:
                ev_to_ebitda_status = METRIC_STATUS_MISSING_OPERATING_INCOME
            elif depreciation_and_amortization is None:
                ev_to_ebitda_status = METRIC_STATUS_MISSING_DEPRECIATION_AND_AMORTIZATION
            elif market_cap_proxy is None or market_cap_proxy <= 0:
                ev_to_ebitda_status = METRIC_STATUS_INVALID_MARKET_CAP_PROXY
            elif enterprise_value is None or enterprise_value <= 0:
                ev_to_ebitda_status = METRIC_STATUS_INVALID_ENTERPRISE_VALUE
            elif ebitda is None or ebitda <= 0:
                ev_to_ebitda_status = METRIC_STATUS_INVALID_NON_POSITIVE_EBITDA
            else:
                ev_to_ebitda_status = METRIC_STATUS_AVAILABLE

        return {
            'symbol': input_row.symbol,
            'name_ko': input_row.name_ko,
            'market': input_row.market,
            'market_date': market_date_iso,
            'source': source,
            'feature_version': source,
            'market_cap_proxy': market_cap_proxy,
            'enterprise_value': enterprise_value,
            'ebitda': ebitda,
            'free_cash_flow': free_cash_flow,
            'fcf_yield': fcf_yield,
            'ev_to_ebitda': ev_to_ebitda,
            'fcf_yield_status': fcf_yield_status,
            'ev_to_ebitda_status': ev_to_ebitda_status,
            'fcf_yield_valid_for_scoring': fcf_yield_status == METRIC_STATUS_AVAILABLE and fcf_yield is not None,
            'ev_to_ebitda_valid_for_scoring': (
                ev_to_ebitda_status == METRIC_STATUS_AVAILABLE and ev_to_ebitda is not None
            ),
            'fcf_yield_rank': None,
            'ev_to_ebitda_rank': None,
            'fcf_yield_percentile': None,
            'ev_to_ebitda_percentile': None,
            'advanced_value_score': None,
            'advanced_value_rank': None,
            'advanced_value_percentile': None,
            'advanced_value_components_used': 0,
            'advanced_value_classification': CLASSIFICATION_INSUFFICIENT_COMPONENTS,
            'is_advanced_value_eligible': False,
            'factor_payload': {},
            '_current_bar_available': current_bar is not None,
            '_annual_snapshot_available': annual_fundamentals is not None,
            '_price_bar_source': current_bar.get('source') if current_bar is not None else None,
            '_price_bar_captured_at': current_bar.get('captured_at') if current_bar is not None else None,
            '_audit_payload': financial_context.get('audit') if financial_context else None,
            '_pbr': pbr,
            '_total_equity': total_equity,
            '_total_liabilities': total_liabilities,
            '_operating_income': operating_income,
            '_operating_cash_flow': operating_cash_flow,
            '_cash_and_cash_equivalents': cash_and_cash_equivalents,
            '_capital_expenditure': capital_expenditure,
            '_depreciation_and_amortization': depreciation_and_amortization,
        }

    def _assign_metric_rankings(self, items: list[dict[str, Any]]) -> dict[str, int]:
        return {
            'fcf_yield': self._assign_single_metric_rankings(
                items,
                value_key='fcf_yield',
                valid_key='fcf_yield_valid_for_scoring',
                rank_key='fcf_yield_rank',
                percentile_key='fcf_yield_percentile',
                best='higher',
            ),
            'ev_to_ebitda': self._assign_single_metric_rankings(
                items,
                value_key='ev_to_ebitda',
                valid_key='ev_to_ebitda_valid_for_scoring',
                rank_key='ev_to_ebitda_rank',
                percentile_key='ev_to_ebitda_percentile',
                best='lower',
            ),
            'advanced_value_score': 0,
        }

    def _assign_single_metric_rankings(
        self,
        items: list[dict[str, Any]],
        *,
        value_key: str,
        valid_key: str,
        rank_key: str,
        percentile_key: str,
        best: str,
    ) -> int:
        candidates = [
            item
            for item in items
            if item.get(valid_key) is True
            and item.get(value_key) is not None
        ]
        if best == 'lower':
            ordered = sorted(candidates, key=lambda item: (float(item[value_key]), item['symbol']))
        elif best == 'higher':
            ordered = sorted(candidates, key=lambda item: (-float(item[value_key]), item['symbol']))
        else:
            raise ValueError(f'Unsupported ranking direction: {best}')

        total = len(ordered)
        for rank, item in enumerate(ordered, start=1):
            item[rank_key] = rank
            item[percentile_key] = _percentile_from_rank(rank, total)
        return total

    def _assign_scores_and_classifications(self, items: list[dict[str, Any]]) -> None:
        for item in items:
            components_used = 0
            weighted_sum = 0.0
            for key, weight in ADVANCED_VALUE_SCORE_WEIGHTS.items():
                value = item.get(key)
                if value is None:
                    continue
                components_used += 1
                weighted_sum += float(value) * float(weight)

            item['advanced_value_components_used'] = components_used
            if components_used < ADVANCED_VALUE_MIN_COMPONENTS:
                if not item.get('_current_bar_available'):
                    item['advanced_value_classification'] = CLASSIFICATION_MISSING_MARKET_DATE_BAR
                elif not item.get('_annual_snapshot_available'):
                    item['advanced_value_classification'] = CLASSIFICATION_INSUFFICIENT_FUNDAMENTALS
                else:
                    item['advanced_value_classification'] = CLASSIFICATION_INSUFFICIENT_COMPONENTS
                item['advanced_value_score'] = None
                item['is_advanced_value_eligible'] = False
                continue

            item['advanced_value_score'] = _round(weighted_sum, 6)
            if float(item['advanced_value_score']) >= ADVANCED_VALUE_ELIGIBLE_SCORE:
                item['advanced_value_classification'] = CLASSIFICATION_ELIGIBLE
                item['is_advanced_value_eligible'] = True
            elif float(item['advanced_value_score']) >= ADVANCED_VALUE_WATCHLIST_SCORE:
                item['advanced_value_classification'] = CLASSIFICATION_WATCHLIST
                item['is_advanced_value_eligible'] = False
            else:
                item['advanced_value_classification'] = CLASSIFICATION_INELIGIBLE
                item['is_advanced_value_eligible'] = False

        scored_items = sorted(
            [item for item in items if item.get('advanced_value_score') is not None],
            key=lambda item: (-float(item['advanced_value_score']), item['symbol']),
        )
        total = len(scored_items)
        for rank, item in enumerate(scored_items, start=1):
            item['advanced_value_rank'] = rank
            item['advanced_value_percentile'] = _percentile_from_rank(rank, total)

    def _build_factor_payload(
        self,
        *,
        item: dict[str, Any],
        source: str,
        universe_counts: dict[str, int],
    ) -> dict[str, Any]:
        return {
            'feature_version': source,
            'methodology': {
                'financial_snapshot_policy': {
                    'report_code': ADVANCED_VALUE_FUNDAMENTALS_REPORT_CODE,
                    'period_type': ADVANCED_VALUE_FUNDAMENTALS_PERIOD_TYPE,
                    'fs_div': ADVANCED_VALUE_FUNDAMENTALS_FS_DIV,
                    'selection_rule': 'latest_receipt_on_or_before_market_date',
                },
                'field_policies': {
                    'flow_amount_preference': 'current_amount_then_current_cumulative_amount',
                    'stock_amount_preference': 'current_amount',
                    'price_bar_policy': 'same_day_daily_price_bar_required',
                    'sector_normalization': 'not_applied',
                    'winsorization': 'not_applied',
                },
                'free_cash_flow_policy': {
                    'formula': 'operating_cash_flow - capital_expenditure',
                    'capital_expenditure_definition': (
                        'abs(property_plant_equipment_acquisition_cash_outflow) + '
                        'abs(intangible_asset_acquisition_cash_outflow)'
                    ),
                    'yield_denominator': 'market_cap_proxy',
                },
                'enterprise_value_policy': {
                    'market_cap_proxy_formula': 'pbr * total_equity',
                    'debt_proxy_formula': 'total_liabilities',
                    'cash_formula': 'cash_and_cash_equivalents',
                    'enterprise_value_formula': 'market_cap_proxy + total_liabilities - cash_and_cash_equivalents',
                },
                'ebitda_policy': {
                    'formula': 'operating_income + depreciation_and_amortization',
                    'depreciation_and_amortization_rule': (
                        'use combined matched row when available, otherwise sum matched depreciation and amortization'
                    ),
                },
                'scoring': {
                    'score_method': 'equal_weight_mean_of_component_percentiles',
                    'weights': ADVANCED_VALUE_SCORE_WEIGHTS,
                    'minimum_valid_components': ADVANCED_VALUE_MIN_COMPONENTS,
                    'ranking_directions': {
                        'fcf_yield': 'higher_better',
                        'ev_to_ebitda': 'lower_better',
                        'advanced_value_score': 'higher_better',
                    },
                    'eligible_threshold': ADVANCED_VALUE_ELIGIBLE_SCORE,
                    'watchlist_threshold': ADVANCED_VALUE_WATCHLIST_SCORE,
                    'tie_breaker': 'symbol_asc',
                },
            },
            'availability': {
                'fcf_yield_status': item['fcf_yield_status'],
                'ev_to_ebitda_status': item['ev_to_ebitda_status'],
                'fcf_yield_valid_for_scoring': item['fcf_yield_valid_for_scoring'],
                'ev_to_ebitda_valid_for_scoring': item['ev_to_ebitda_valid_for_scoring'],
                'advanced_value_components_used': item['advanced_value_components_used'],
            },
            'inputs': {
                'pbr': item.get('_pbr'),
                'total_equity': item.get('_total_equity'),
                'total_liabilities': item.get('_total_liabilities'),
                'operating_income': item.get('_operating_income'),
                'operating_cash_flow': item.get('_operating_cash_flow'),
                'cash_and_cash_equivalents': item.get('_cash_and_cash_equivalents'),
                'capital_expenditure': item.get('_capital_expenditure'),
                'depreciation_and_amortization': item.get('_depreciation_and_amortization'),
                'market_cap_proxy': item.get('market_cap_proxy'),
                'enterprise_value': item.get('enterprise_value'),
                'ebitda': item.get('ebitda'),
                'free_cash_flow': item.get('free_cash_flow'),
            },
            'ranking': {
                'fcf_yield_rank': item['fcf_yield_rank'],
                'ev_to_ebitda_rank': item['ev_to_ebitda_rank'],
                'advanced_value_rank': item['advanced_value_rank'],
                'fcf_yield_percentile': item['fcf_yield_percentile'],
                'ev_to_ebitda_percentile': item['ev_to_ebitda_percentile'],
                'advanced_value_percentile': item['advanced_value_percentile'],
                'ranking_universe_counts': universe_counts,
            },
            'audit': {
                'trade_date': item['market_date'],
                'name_ko': item.get('name_ko'),
                'market': item.get('market'),
                'price_bar': {
                    'source': item.get('_price_bar_source'),
                    'captured_at': item.get('_price_bar_captured_at'),
                },
                'fundamentals': item.get('_audit_payload'),
            },
        }

    def _build_summary(
        self,
        items: list[dict[str, Any]],
        *,
        requested_symbols: list[str],
        matched_symbols: list[str],
        missing_symbols: list[str],
        universe_counts: dict[str, int],
    ) -> dict[str, Any]:
        return {
            'requested_symbol_count': len(requested_symbols) if requested_symbols else len(matched_symbols),
            'matched_symbol_count': len(matched_symbols),
            'missing_symbol_count': len(missing_symbols),
            'symbols_with_market_date_bar': sum(1 for item in items if item.get('_current_bar_available')),
            'symbols_with_annual_fundamentals': sum(1 for item in items if item.get('_annual_snapshot_available')),
            'fcf_yield_valid_count': sum(1 for item in items if item.get('fcf_yield_valid_for_scoring')),
            'ev_to_ebitda_valid_count': sum(1 for item in items if item.get('ev_to_ebitda_valid_for_scoring')),
            'eligible_count': sum(
                1 for item in items if item.get('advanced_value_classification') == CLASSIFICATION_ELIGIBLE
            ),
            'watchlist_count': sum(
                1 for item in items if item.get('advanced_value_classification') == CLASSIFICATION_WATCHLIST
            ),
            'ineligible_count': sum(
                1 for item in items if item.get('advanced_value_classification') == CLASSIFICATION_INELIGIBLE
            ),
            'insufficient_components_count': sum(
                1
                for item in items
                if item.get('advanced_value_classification') == CLASSIFICATION_INSUFFICIENT_COMPONENTS
            ),
            'insufficient_fundamentals_count': sum(
                1
                for item in items
                if item.get('advanced_value_classification') == CLASSIFICATION_INSUFFICIENT_FUNDAMENTALS
            ),
            'missing_market_date_bar_count': sum(
                1
                for item in items
                if item.get('advanced_value_classification') == CLASSIFICATION_MISSING_MARKET_DATE_BAR
            ),
            'ranking_universe_counts': universe_counts,
        }

    @staticmethod
    def _strip_internal_fields(item: dict[str, Any]) -> None:
        for key in (
            '_current_bar_available',
            '_annual_snapshot_available',
            '_price_bar_source',
            '_price_bar_captured_at',
            '_audit_payload',
            '_pbr',
            '_total_equity',
            '_total_liabilities',
            '_operating_income',
            '_operating_cash_flow',
            '_cash_and_cash_equivalents',
            '_capital_expenditure',
            '_depreciation_and_amortization',
        ):
            item.pop(key, None)


daily_advanced_value_factor_service = DailyAdvancedValueFactorService()
