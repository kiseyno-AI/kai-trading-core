"""Read-only audit of daily derived-table coverage against the price-bar universe.

The daily pipeline can mark a stage ``completed`` or ``partial`` while the table it
writes covers only a fraction of the day's universe (for example a factor stage whose
input cohort narrowed to the symbols that finished an upstream stage). Each stage
reports its coverage against its *own* requested scope, so such a hole is invisible in
stage statuses. This audit compares every daily table's row count with the number of
stored price bars for the same market date.

Nothing here writes to the database or changes pipeline behavior.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

BASELINE_KEY = 'bars'

# Factor and filter tables legitimately omit some symbols (preferred shares, SPACs and
# very recent listings have no standalone fundamentals or history), so the default
# threshold leaves room for that without hiding a real hole.
DEFAULT_MIN_RATIO = 0.90


@dataclass(frozen=True)
class CoverageTable:
    key: str
    table: str
    date_column: str


COVERAGE_TABLES: tuple[CoverageTable, ...] = (
    CoverageTable(BASELINE_KEY, 'daily_price_bars', 'trade_date'),
    CoverageTable('technical', 'daily_technical_snapshots', 'market_date'),
    CoverageTable('price_context', 'daily_price_contexts', 'market_date'),
    CoverageTable('action_summary', 'daily_action_summaries', 'market_date'),
    CoverageTable('market_risk_snapshot', 'daily_market_risk_snapshots', 'market_date'),
    CoverageTable('market_risk_filter', 'daily_market_risk_filter_results', 'market_date'),
    CoverageTable('piotroski', 'daily_piotroski_filter_results', 'market_date'),
    CoverageTable('momentum', 'daily_momentum_factor_results', 'market_date'),
    CoverageTable('value', 'daily_value_factor_results', 'market_date'),
    CoverageTable('low_risk', 'daily_low_risk_factor_results', 'market_date'),
    CoverageTable('beta', 'daily_beta_factor_results', 'market_date'),
    CoverageTable('advanced_value', 'daily_advanced_value_factor_results', 'market_date'),
    CoverageTable('unified_value', 'daily_unified_value_factor_results', 'market_date'),
    CoverageTable('quality', 'daily_quality_factor_results', 'market_date'),
    CoverageTable('composite', 'daily_composite_factor_results', 'market_date'),
    CoverageTable('signal_features', 'daily_signal_features', 'market_date'),
)

STATUS_OK = 'ok'
STATUS_LOW = 'low'
STATUS_EMPTY = 'empty'
STATUS_NO_BASELINE = 'no_baseline'


def evaluate_daily_coverage(
    counts: Mapping[str, int],
    *,
    market_date: str,
    min_ratio: float = DEFAULT_MIN_RATIO,
    tables: Sequence[CoverageTable] = COVERAGE_TABLES,
) -> dict[str, Any]:
    """Classify each table's row count relative to the day's price-bar count.

    ``counts`` maps :class:`CoverageTable` keys to row counts; a missing key counts as 0.
    A table is ``empty`` at zero rows, ``low`` below ``min_ratio`` of the baseline and
    ``ok`` otherwise. Without any price bars no ratio is meaningful, so the day is
    ``degraded`` with status ``no_baseline`` rather than silently passing.
    """

    if not 0.0 <= float(min_ratio) <= 1.0:
        raise ValueError('min_ratio must be between 0 and 1')
    baseline = int(counts.get(BASELINE_KEY, 0) or 0)
    rows: list[dict[str, Any]] = []
    problem_keys: list[str] = []
    for spec in tables:
        if spec.key == BASELINE_KEY:
            continue
        count = int(counts.get(spec.key, 0) or 0)
        if baseline <= 0:
            ratio = None
            status = STATUS_NO_BASELINE
        else:
            ratio = count / baseline
            if count == 0:
                status = STATUS_EMPTY
            elif ratio < float(min_ratio):
                status = STATUS_LOW
            else:
                status = STATUS_OK
        if status != STATUS_OK:
            problem_keys.append(spec.key)
        rows.append(
            {
                'key': spec.key,
                'table': spec.table,
                'count': count,
                'ratio': None if ratio is None else round(ratio, 4),
                'status': status,
            }
        )
    return {
        'market_date': market_date,
        'baseline_count': baseline,
        'min_ratio': float(min_ratio),
        'status': 'degraded' if problem_keys or baseline <= 0 else 'ok',
        'problem_tables': problem_keys,
        'tables': rows,
    }
