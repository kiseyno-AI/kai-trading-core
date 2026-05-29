from __future__ import annotations

from datetime import date
from typing import Any, Sequence

from app.eod.pykrx_enrichment import pykrx_enrichment_service

REQUIRED_EXEC_DECIDER_PYKRX_ENRICHMENT_CATEGORIES: tuple[str, ...] = (
    'sector_classification',
    'investor_flow_symbol',
    'short_selling',
)


def _normalize_iso_date(value: str | date) -> str:
    if isinstance(value, date):
        return value.isoformat()
    return date.fromisoformat(str(value)).isoformat()


def _coerce_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _category_gate_item(
    *,
    category: str,
    payload: dict[str, Any],
    missing_category_payload: bool,
) -> dict[str, Any]:
    expected_rows = _coerce_optional_int(payload.get('expected_rows'))
    present_rows = _coerce_optional_int(payload.get('present_rows')) or 0
    coverage_ratio = _coerce_optional_float(payload.get('coverage_ratio'))
    if coverage_ratio is None and expected_rows and expected_rows > 0:
        coverage_ratio = round(min(present_rows, expected_rows) / expected_rows, 6)
    coverage_status = str(payload.get('coverage_status') or 'missing').strip().lower()
    missing_rows = max(expected_rows - present_rows, 0) if expected_rows is not None else None

    if missing_category_payload:
        gate_status = 'waiting'
        reason = 'coverage_expectation_unavailable'
    elif expected_rows is None or expected_rows <= 0:
        gate_status = 'not_applicable'
        reason = 'no_expected_rows_for_category'
    elif present_rows <= 0 or coverage_status == 'missing':
        gate_status = 'blocked'
        reason = 'missing_required_pykrx_enrichment_category'
    else:
        gate_status = 'ready'
        reason = None if coverage_status == 'complete' else 'required_pykrx_enrichment_category_near_complete'

    return {
        'category': category,
        'scope_type': payload.get('scope_type'),
        'gate_status': gate_status,
        'coverage_status': coverage_status,
        'expected_rows': expected_rows,
        'present_rows': present_rows,
        'missing_rows': missing_rows,
        'coverage_ratio': coverage_ratio,
        'reason': reason,
    }


def build_pykrx_enrichment_preflight(
    *,
    market_date: str | date,
    coverage: dict[str, Any],
    categories: Sequence[str] = REQUIRED_EXEC_DECIDER_PYKRX_ENRICHMENT_CATEGORIES,
) -> dict[str, Any]:
    market_date_iso = _normalize_iso_date(market_date)
    category_payloads = dict(coverage.get('categories') if isinstance(coverage.get('categories'), dict) else {})
    gate_items = [
        _category_gate_item(
            category=category,
            payload=(
                dict(category_payloads.get(category))
                if isinstance(category_payloads.get(category), dict)
                else {}
            ),
            missing_category_payload=not isinstance(category_payloads.get(category), dict),
        )
        for category in categories
    ]
    blocked_categories = [item for item in gate_items if item['gate_status'] == 'blocked']
    waiting_categories = [item for item in gate_items if item['gate_status'] == 'waiting']
    not_applicable_categories = [item for item in gate_items if item['gate_status'] == 'not_applicable']
    warning_categories = [
        item
        for item in gate_items
        if item['gate_status'] == 'ready' and item.get('coverage_status') != 'complete'
    ]
    if blocked_categories:
        status = 'blocked'
    elif waiting_categories:
        status = 'waiting'
    elif len(not_applicable_categories) == len(gate_items):
        status = 'not_applicable'
    else:
        status = 'ready'

    allowed = status in {'ready', 'not_applicable'}
    failed_categories = [
        item['category']
        for item in [*blocked_categories, *waiting_categories]
    ]
    category_row_counts = {
        item['category']: item['present_rows']
        for item in gate_items
    }
    zero_coverage_categories = [
        item['category']
        for item in gate_items
        if (item.get('expected_rows') or 0) > 0 and int(item.get('present_rows') or 0) <= 0
    ]
    return {
        'market_date': market_date_iso,
        'status': status,
        'allowed': allowed,
        'required_categories': list(categories),
        'category_gates': gate_items,
        'blocked_categories': blocked_categories,
        'waiting_categories': waiting_categories,
        'warning_categories': warning_categories,
        'not_applicable_categories': not_applicable_categories,
        'failed_categories': failed_categories,
        'zero_coverage_categories': zero_coverage_categories,
        'category_row_counts': category_row_counts,
        'requested_symbol_count': coverage.get('requested_symbol_count'),
        'requested_market_count': coverage.get('requested_market_count'),
    }


def inspect_pykrx_enrichment_preflight(
    *,
    market_date: str | date,
    markets: Sequence[str] | None = None,
    symbols: Sequence[str] | None = None,
    categories: Sequence[str] = REQUIRED_EXEC_DECIDER_PYKRX_ENRICHMENT_CATEGORIES,
) -> dict[str, Any]:
    market_date_iso = _normalize_iso_date(market_date)
    inspection = pykrx_enrichment_service.inspect(
        market_date=market_date_iso,
        markets=markets,
        symbols=symbols,
        categories=categories,
        limit=1,
    )
    preflight = build_pykrx_enrichment_preflight(
        market_date=market_date_iso,
        coverage=dict(inspection.get('coverage') or {}),
        categories=categories,
    )
    return {
        **preflight,
        'inspection_summary': inspection.get('summary') or {},
    }
