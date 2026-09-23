from __future__ import annotations

REQUIRED_CATEGORY_MIN_COVERAGE_RATIO = 0.99
REQUIRED_CATEGORY_MIN_COVERAGE_RATIOS: dict[str, float] = {
    # PyKRX valuation excludes a small, stable subset of the broader KRX
    # universe. Direct same-day rows remain mandatory; 95% of the supported
    # stock universe is the explicit readiness floor.
    'valuation_snapshot': 0.95,
}


def required_category_minimum_coverage_ratio(category: str) -> float:
    return REQUIRED_CATEGORY_MIN_COVERAGE_RATIOS.get(
        category,
        REQUIRED_CATEGORY_MIN_COVERAGE_RATIO,
    )
