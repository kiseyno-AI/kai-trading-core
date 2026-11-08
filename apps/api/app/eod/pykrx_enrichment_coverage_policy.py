from __future__ import annotations

REQUIRED_CATEGORY_MIN_COVERAGE_RATIO = 0.99
REQUIRED_CATEGORY_MIN_COVERAGE_RATIOS: dict[str, float] = {
    # PyKRX valuation excludes a small, stable subset of the broader KRX
    # universe. Direct same-day rows remain mandatory; 95% of the supported
    # stock universe is the explicit readiness floor.
    'valuation_snapshot': 0.95,
    # Per-symbol investor flows are not published for a stable ~4% of the
    # universe (95.8-96.1% on 2026-10-02/10-12/10-20/11-02), so the 99% default
    # blocked every downstream stage on ordinary days. Same floor as valuation.
    'investor_flow_symbol': 0.95,
}


def required_category_minimum_coverage_ratio(category: str) -> float:
    return REQUIRED_CATEGORY_MIN_COVERAGE_RATIOS.get(
        category,
        REQUIRED_CATEGORY_MIN_COVERAGE_RATIO,
    )
