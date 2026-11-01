from __future__ import annotations

from collections.abc import Sequence
from typing import Any


EXACT_SCOPE_OUTCOME_MANIFEST_VERSIONS = frozenset(
    {
        'action_summaries_scope_outcomes_v1',
        'auto_eod_followup_admission_scope_outcomes_v1',
        'dart_daily_data_scope_outcomes_v1',
        'dart_fundamentals_scope_outcomes_v1',
        'factor_followup_scope_outcomes_v1',
        'factor_scope_outcomes_v1',
        'market_risk_filter_scope_outcomes_v1',
        'piotroski_scope_outcomes_v1',
        'pykrx_enrichment_scope_outcomes_v1',
        'signal_features_scope_outcomes_v1',
        'technical_snapshots_scope_outcomes_v1',
    }
)


def build_exact_symbol_scope_outcome(
    *,
    manifest_version: str,
    selected_symbols: Sequence[str],
    successful_symbols: Sequence[str],
    retry_debt_symbols: Sequence[str],
) -> dict[str, Any]:
    """Build an exhaustive, identity-bearing symbol outcome partition."""

    selected = list(
        dict.fromkeys(
            str(value).strip()
            for value in selected_symbols
            if str(value).strip()
        )
    )
    successful_set = {
        str(value).strip()
        for value in successful_symbols
        if str(value).strip()
    }
    retry_debt_set = {
        str(value).strip()
        for value in retry_debt_symbols
        if str(value).strip()
    }
    selected_set = set(selected)
    if successful_set & retry_debt_set:
        raise ValueError('exact symbol scope outcomes overlap')
    if selected_set != successful_set | retry_debt_set:
        raise ValueError('exact symbol scope outcomes are not exhaustive')
    return {
        'scope_outcome_manifest_version': manifest_version,
        'selected_stock_codes': selected,
        'successful_stock_codes': [
            symbol for symbol in selected if symbol in successful_set
        ],
        'retry_debt_stock_codes': [
            symbol for symbol in selected if symbol in retry_debt_set
        ],
        'terminal_not_applicable_stock_codes': [],
        'scope_outcome_manifest': [
            {
                'stock_code': symbol,
                'outcome': (
                    'success'
                    if symbol in successful_set
                    else 'retry_debt'
                ),
            }
            for symbol in selected
        ],
    }
