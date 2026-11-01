from __future__ import annotations

from datetime import date
from typing import Any, Sequence

from app.eod.pykrx_enrichment import pykrx_enrichment_service
from app.eod.pykrx_enrichment_coverage_policy import (
    required_category_minimum_coverage_ratio,
)

REQUIRED_EXEC_DECIDER_PYKRX_ENRICHMENT_CATEGORIES: tuple[str, ...] = (
    'sector_classification',
    'investor_flow_symbol',
    'short_selling',
    'valuation_snapshot',
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


def build_pykrx_enrichment_category_gate_item(
    *,
    category: str,
    payload: dict[str, Any],
    missing_category_payload: bool,
) -> dict[str, Any]:
    expected_rows = _coerce_optional_int(payload.get('expected_rows'))
    present_rows = _coerce_optional_int(payload.get('present_rows')) or 0
    coverage_ratio = _coerce_optional_float(payload.get('coverage_ratio'))
    expectation_status = str(payload.get('expectation_status') or 'available').strip().lower()
    if expected_rows and expected_rows > 0:
        coverage_ratio = round(min(present_rows, expected_rows) / expected_rows, 6)
    coverage_status = str(payload.get('coverage_status') or 'missing').strip().lower()
    missing_rows = max(expected_rows - present_rows, 0) if expected_rows is not None else None
    decision_usable_scope_keys = payload.get('decision_usable_scope_keys')
    decision_usable_evidence_available = any(
        key in payload
        for key in (
            'decision_usable_present_rows',
            'decision_usable_ratio',
            'decision_usable_status',
        )
    ) or isinstance(decision_usable_scope_keys, list)
    decision_usable_rows = _coerce_optional_int(
        payload.get('decision_usable_present_rows')
    )
    if (
        decision_usable_rows is None
        and isinstance(decision_usable_scope_keys, list)
    ):
        decision_usable_rows = len(decision_usable_scope_keys)
    decision_usable_ratio = _coerce_optional_float(
        payload.get('decision_usable_ratio')
    )
    if (
        expected_rows
        and expected_rows > 0
        and decision_usable_rows is not None
    ):
        decision_usable_ratio = round(
            min(decision_usable_rows, expected_rows) / expected_rows,
            6,
        )
    raw_decision_usable_status = payload.get('decision_usable_status')
    decision_usable_status = (
        str(raw_decision_usable_status).strip().lower()
        if raw_decision_usable_status is not None
        and str(raw_decision_usable_status).strip()
        else None
    )
    if decision_usable_evidence_available:
        if decision_usable_status is None:
            if decision_usable_ratio is None:
                decision_usable_status = 'missing'
            elif decision_usable_ratio >= 1.0:
                decision_usable_status = 'complete'
            elif decision_usable_ratio > 0.0:
                decision_usable_status = 'partial'
            else:
                decision_usable_status = 'missing'
    else:
        # Compatibility for authoritative coverage snapshots written before
        # decision-usable component evidence existed.  Full component
        # completeness must not replace this fallback: short balance is an
        # operator-quality signal, not a required decision input.
        decision_usable_rows = present_rows
        decision_usable_ratio = coverage_ratio
        decision_usable_status = coverage_status

    completeness_evidence_available = any(
        key in payload
        for key in (
            'complete_present_rows',
            'completeness_ratio',
            'completeness_status',
        )
    )
    complete_present_rows = _coerce_optional_int(
        payload.get('complete_present_rows')
    )
    completeness_ratio = _coerce_optional_float(
        payload.get('completeness_ratio')
    )
    raw_completeness_status = payload.get('completeness_status')
    completeness_status = (
        str(raw_completeness_status).strip().lower()
        if raw_completeness_status is not None
        and str(raw_completeness_status).strip()
        else None
    )
    if (
        expected_rows
        and expected_rows > 0
        and complete_present_rows is not None
    ):
        completeness_ratio = round(
            min(complete_present_rows, expected_rows) / expected_rows,
            6,
        )
    if completeness_evidence_available and completeness_status is None:
        if completeness_ratio is None:
            completeness_status = 'missing'
        elif completeness_ratio >= 1.0:
            completeness_status = 'complete'
        elif completeness_ratio > 0.0:
            completeness_status = 'partial'
        else:
            completeness_status = 'missing'

    missing_decision_usable_rows = (
        max(expected_rows - decision_usable_rows, 0)
        if expected_rows is not None and decision_usable_rows is not None
        else None
    )
    missing_complete_rows = (
        max(expected_rows - complete_present_rows, 0)
        if expected_rows is not None and complete_present_rows is not None
        else None
    )
    minimum_coverage_ratio = required_category_minimum_coverage_ratio(category)

    if (
        not missing_category_payload
        and expectation_status == 'not_applicable'
        and coverage_status == 'not_applicable'
        and expected_rows is not None
        and expected_rows <= 0
    ):
        gate_status = 'not_applicable'
        reason = 'no_expected_rows_for_category'
    elif (
        missing_category_payload
        or expectation_status == 'unavailable'
        or expected_rows is None
        or expected_rows <= 0
    ):
        gate_status = 'waiting'
        reason = 'coverage_expectation_unavailable'
    elif (
        decision_usable_status == 'missing'
        or (
            decision_usable_rows is not None
            and decision_usable_rows <= 0
        )
        or (
            decision_usable_ratio is not None
            and decision_usable_ratio <= 0.0
        )
    ):
        gate_status = 'blocked'
        reason = (
            'missing_required_pykrx_enrichment_decision_evidence'
            if decision_usable_evidence_available
            else 'missing_required_pykrx_enrichment_category'
        )
    elif (
        decision_usable_ratio is None
        or decision_usable_ratio < minimum_coverage_ratio
    ):
        gate_status = 'blocked'
        reason = 'required_pykrx_enrichment_category_below_min_coverage'
    else:
        gate_status = 'ready'
        reason = (
            'required_pykrx_enrichment_category_near_complete'
            if decision_usable_status != 'complete'
            else 'required_pykrx_enrichment_optional_components_incomplete'
            if completeness_evidence_available
            and completeness_status != 'complete'
            else None
        )

    operator_warning = bool(
        gate_status == 'ready'
        and (
            decision_usable_status != 'complete'
            or (
                completeness_evidence_available
                and completeness_status != 'complete'
            )
        )
    )

    return {
        'category': category,
        'scope_type': payload.get('scope_type'),
        'gate_status': gate_status,
        'coverage_status': coverage_status,
        'expectation_status': expectation_status,
        'expected_rows': expected_rows,
        'present_rows': present_rows,
        'missing_rows': missing_rows,
        'coverage_ratio': coverage_ratio,
        'minimum_coverage_ratio': minimum_coverage_ratio,
        'complete_present_rows': complete_present_rows,
        'completeness_ratio': completeness_ratio,
        'completeness_status': (
            completeness_status
            if completeness_status is not None
            else coverage_status
            if not completeness_evidence_available
            else decision_usable_status
        ),
        'completeness_evidence_available': completeness_evidence_available,
        'decision_usable_evidence_available': (
            decision_usable_evidence_available
        ),
        'decision_usable_rows': decision_usable_rows,
        'decision_usable_ratio': decision_usable_ratio,
        'decision_usable_status': decision_usable_status,
        'missing_decision_usable_rows': missing_decision_usable_rows,
        'missing_complete_rows': missing_complete_rows,
        'operator_warning': operator_warning,
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
        build_pykrx_enrichment_category_gate_item(
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
        if item.get('operator_warning') is True
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
    category_decision_usable_row_counts = {
        item['category']: item['decision_usable_rows']
        for item in gate_items
    }
    zero_coverage_categories = [
        item['category']
        for item in gate_items
        if (item.get('expected_rows') or 0) > 0 and int(item.get('present_rows') or 0) <= 0
    ]
    zero_decision_usable_categories = [
        item['category']
        for item in gate_items
        if (item.get('expected_rows') or 0) > 0
        and (
            item.get('decision_usable_status') == 'missing'
            or (
                item.get('decision_usable_rows') is not None
                and int(item.get('decision_usable_rows') or 0) <= 0
            )
            or (
                item.get('decision_usable_ratio') is not None
                and float(item.get('decision_usable_ratio') or 0.0) <= 0.0
            )
        )
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
        'zero_decision_usable_categories': zero_decision_usable_categories,
        'category_row_counts': category_row_counts,
        'category_decision_usable_row_counts': (
            category_decision_usable_row_counts
        ),
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
