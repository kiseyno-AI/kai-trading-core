from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from app.eod.market_membership import market_membership_sha256
from app.market.symbol_codes import normalize_krx_short_code


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
        'price_contexts_scope_outcomes_v1',
        'pykrx_enrichment_scope_outcomes_v1',
        'signal_features_scope_outcomes_v1',
        'technical_snapshots_scope_outcomes_v1',
        'trending_ranks_scope_outcomes_v1',
    }
)

STAGE_EXACT_SCOPE_OUTCOME_MANIFEST_VERSIONS: dict[
    str, frozenset[str]
] = {
    'price_collection': frozenset(
        {'price_contexts_scope_outcomes_v1'}
    ),
    'price_contexts': frozenset({'price_contexts_scope_outcomes_v1'}),
    'pykrx_enrichment': frozenset(
        {'pykrx_enrichment_scope_outcomes_v1'}
    ),
    'technical_snapshots': frozenset(
        {'technical_snapshots_scope_outcomes_v1'}
    ),
    'action_summaries': frozenset(
        {'action_summaries_scope_outcomes_v1'}
    ),
    'market_risk_filter': frozenset(
        {'market_risk_filter_scope_outcomes_v1'}
    ),
    'fundamentals_daily_refresh': frozenset(
        {
            'dart_daily_data_scope_outcomes_v1',
            'dart_fundamentals_scope_outcomes_v1',
        }
    ),
    'piotroski_filter': frozenset({'piotroski_scope_outcomes_v1'}),
    **{
        stage_id: frozenset({'factor_scope_outcomes_v1'})
        for stage_id in (
            'momentum_factors',
            'value_factors',
            'low_risk_factors',
            'beta_factors',
            'advanced_value_factors',
            'unified_value_factors',
            'quality_factors',
            'composite_factors',
        )
    },
    'signal_features': frozenset(
        {'signal_features_scope_outcomes_v1'}
    ),
    'trending_ranks': frozenset(
        {'trending_ranks_scope_outcomes_v1'}
    ),
}

STAGE_ATOMIC_FINALIZATION_STAGE_IDS: dict[str, str] = {
    'price_collection': 'price_contexts',
    'price_contexts': 'price_contexts',
    'technical_snapshots': 'technical_snapshots',
    'action_summaries': 'action_summaries',
    'trending_ranks': 'trending_ranks',
    'market_risk_filter': 'market_risk_filter',
    'piotroski_filter': 'piotroski_filter',
    **{
        stage_id: stage_id
        for stage_id in (
            'momentum_factors',
            'value_factors',
            'low_risk_factors',
            'beta_factors',
            'advanced_value_factors',
            'unified_value_factors',
            'quality_factors',
            'composite_factors',
        )
    },
    'signal_features': 'signal_features',
}

FOLLOWUP_STAGE_RECOVERY_SCOPE_SCHEMA_VERSION = (
    'followup_stage_recovery_scope_v1'
)


def _normalize_krx_symbol_sequence(
    values: Sequence[Any],
) -> list[str] | None:
    normalized: list[str] = []
    for value in values:
        stock_code = normalize_krx_short_code(value)
        if stock_code is None:
            return None
        normalized.append(stock_code)
    return normalized


def build_exact_scope_partition_identity_proof(
    *,
    retry_debt_symbols: Sequence[str],
    terminal_not_applicable_symbols: Sequence[str],
) -> dict[str, Any]:
    """Bind compact retry and terminal identities to their classifications."""

    retry_debt = list(retry_debt_symbols)
    terminal_not_applicable = list(terminal_not_applicable_symbols)
    return {
        'retry_debt_symbol_count': len(retry_debt),
        'retry_debt_symbol_sha256': market_membership_sha256(retry_debt),
        'terminal_not_applicable_symbol_count': len(
            terminal_not_applicable
        ),
        'terminal_not_applicable_symbol_sha256': (
            market_membership_sha256(terminal_not_applicable)
        ),
    }


def exact_scope_partition_identity_proof_matches(
    *,
    proof: Mapping[str, Any],
    retry_debt_symbols: Sequence[str],
    terminal_not_applicable_symbols: Sequence[str],
) -> bool:
    """Reject compact proofs whose retry/terminal identities were swapped."""

    expected = build_exact_scope_partition_identity_proof(
        retry_debt_symbols=retry_debt_symbols,
        terminal_not_applicable_symbols=terminal_not_applicable_symbols,
    )
    if any(
        isinstance(proof.get(field), bool)
        for field in (
            'retry_debt_symbol_count',
            'terminal_not_applicable_symbol_count',
        )
    ):
        return False
    return all(
        proof.get(field) == expected_value
        for field, expected_value in expected.items()
    )


def validate_exact_symbol_scope_outcome(
    *,
    result: Mapping[str, Any],
    invocation_symbols: Sequence[str],
    status: str | None = None,
    allow_failed_all: bool = False,
) -> dict[str, Any] | None:
    """Validate an exhaustive per-symbol outcome against its invocation."""

    normalized_status = str(
        status if status is not None else result.get('status') or ''
    ).strip().lower()
    failed_all_candidate = bool(
        allow_failed_all and normalized_status == 'failed'
    )
    if normalized_status not in {'completed', 'partial'} and not (
        failed_all_candidate
    ):
        return None
    structural_status = (
        'partial' if failed_all_candidate else normalized_status
    )
    version = str(
        result.get('scope_outcome_manifest_version') or ''
    ).strip()
    if version not in EXACT_SCOPE_OUTCOME_MANIFEST_VERSIONS:
        return None

    def normalize_list(
        key: str,
        *,
        optional: bool = False,
    ) -> list[str] | None:
        raw = result.get(key)
        if optional and raw is None:
            return []
        if not isinstance(raw, list):
            return None
        normalized = _normalize_krx_symbol_sequence(raw)
        if (
            normalized is None
            or len(normalized) != len(set(normalized))
        ):
            return None
        return normalized

    selected = normalize_list('selected_stock_codes')
    successful = normalize_list('successful_stock_codes')
    retry_debt = normalize_list('retry_debt_stock_codes')
    terminal_not_applicable = normalize_list(
        'terminal_not_applicable_stock_codes',
        optional=True,
    )
    if (
        selected is None
        or successful is None
        or retry_debt is None
        or terminal_not_applicable is None
    ):
        return None

    normalized_invocation = _normalize_krx_symbol_sequence(
        invocation_symbols
    )
    if (
        normalized_invocation is None
        or len(normalized_invocation) != len(set(normalized_invocation))
        or set(selected) != set(normalized_invocation)
    ):
        return None

    selected_set = set(selected)
    successful_set = set(successful)
    retry_debt_set = set(retry_debt)
    terminal_set = set(terminal_not_applicable)
    if (
        successful_set & retry_debt_set
        or successful_set & terminal_set
        or retry_debt_set & terminal_set
        or selected_set
        != successful_set | retry_debt_set | terminal_set
        or (structural_status == 'completed' and retry_debt_set)
    ):
        return None
    if failed_all_candidate and (
        successful_set
        or terminal_set
        or retry_debt_set != selected_set
    ):
        return None

    raw_manifest = result.get('scope_outcome_manifest')
    if not isinstance(raw_manifest, list):
        return None
    manifest_outcomes: dict[str, str] = {}
    for item in raw_manifest:
        if not isinstance(item, Mapping):
            return None
        stock_code = normalize_krx_short_code(item.get('stock_code'))
        outcome = str(item.get('outcome') or '').strip()
        if (
            not stock_code
            or stock_code in manifest_outcomes
            or outcome
            not in {'success', 'retry_debt', 'terminal_not_applicable'}
        ):
            return None
        manifest_outcomes[stock_code] = outcome
    expected_manifest_outcomes = {
        **{stock_code: 'success' for stock_code in successful_set},
        **{stock_code: 'retry_debt' for stock_code in retry_debt_set},
        **{
            stock_code: 'terminal_not_applicable'
            for stock_code in terminal_set
        },
    }
    if manifest_outcomes != expected_manifest_outcomes:
        return None
    return {
        'manifest_version': version,
        'selected_stock_codes': selected,
        'successful_stock_codes': successful,
        'retry_debt_stock_codes': retry_debt,
        'terminal_not_applicable_stock_codes': terminal_not_applicable,
    }


def validate_atomic_scope_finalization(
    *,
    result: Mapping[str, Any],
    stage_id: str,
    market_date: str,
    exact_symbols: Sequence[str],
    successful_symbols: Sequence[str],
) -> dict[str, Any] | None:
    """Bind a persisted exact-scope proof to its stage and symbol sets."""

    def normalize_symbols(values: Sequence[str]) -> list[str] | None:
        normalized = _normalize_krx_symbol_sequence(values)
        if (
            normalized is None
            or len(normalized) != len(set(normalized))
        ):
            return None
        return normalized

    normalized_exact = normalize_symbols(exact_symbols)
    normalized_successful = normalize_symbols(successful_symbols)
    if (
        normalized_exact is None
        or not normalized_exact
        or normalized_successful is None
        or not set(normalized_successful) <= set(normalized_exact)
    ):
        return None
    finalization = result.get('scope_finalization')
    if not isinstance(finalization, Mapping):
        return None
    expected = {
        'stage': stage_id,
        'market_date': str(market_date),
        'exact_symbol_count': len(normalized_exact),
        'exact_membership_sha256': market_membership_sha256(
            normalized_exact
        ),
        'successful_symbol_count': len(normalized_successful),
        'successful_membership_sha256': market_membership_sha256(
            normalized_successful
        ),
        'ready': True,
    }
    if any(
        finalization.get(field) != expected_value
        for field, expected_value in expected.items()
    ):
        return None
    deleted_count = finalization.get('deleted_stale_row_count')
    if (
        isinstance(deleted_count, bool)
        or not isinstance(deleted_count, int)
        or deleted_count < 0
    ):
        return None
    return dict(finalization)


def validate_stage_bound_scope_outcome(
    *,
    result: Mapping[str, Any],
    expected_stage_id: str,
    invocation_symbols: Sequence[str],
    status: str | None = None,
    allow_failed_all: bool = False,
    expected_market_date: str | None = None,
    publication_symbols: Sequence[str] | None = None,
) -> dict[str, Any] | None:
    """Validate a partition as proof for one specific pipeline stage.

    Shared factor manifest names are intentionally insufficient on their own:
    atomic stages must also prove the canonical finalization stage, market
    date, publication membership, and successful cohort.
    """

    expected_versions = STAGE_EXACT_SCOPE_OUTCOME_MANIFEST_VERSIONS.get(
        expected_stage_id
    )
    if expected_versions is None:
        return None
    partition = validate_exact_symbol_scope_outcome(
        result=result,
        invocation_symbols=invocation_symbols,
        status=status,
        allow_failed_all=allow_failed_all,
    )
    if (
        partition is None
        or partition['manifest_version'] not in expected_versions
    ):
        return None

    finalization_stage_id = STAGE_ATOMIC_FINALIZATION_STAGE_IDS.get(
        expected_stage_id
    )
    if finalization_stage_id is not None:
        if expected_market_date is None or publication_symbols is None:
            return None
        normalized_publication_list = _normalize_krx_symbol_sequence(
            publication_symbols
        )
        if normalized_publication_list is None:
            return None
        normalized_publication = set(normalized_publication_list)
        if not set(partition['selected_stock_codes']).issubset(
            normalized_publication
        ):
            return None
        finalization = validate_atomic_scope_finalization(
            result=result,
            stage_id=finalization_stage_id,
            market_date=str(expected_market_date),
            exact_symbols=publication_symbols,
            successful_symbols=partition['successful_stock_codes'],
        )
        if finalization is None:
            return None
        partition = {
            **partition,
            'scope_finalization': finalization,
        }
    elif expected_market_date is not None:
        result_market_date = str(result.get('market_date') or '').strip()
        if not result_market_date or result_market_date != str(
            expected_market_date
        ).strip():
            return None
    return partition


def build_exact_symbol_scope_outcome(
    *,
    manifest_version: str,
    selected_symbols: Sequence[str],
    successful_symbols: Sequence[str],
    retry_debt_symbols: Sequence[str],
    terminal_not_applicable_symbols: Sequence[str] = (),
) -> dict[str, Any]:
    """Build an exhaustive, identity-bearing symbol outcome partition."""

    normalized_selected = _normalize_krx_symbol_sequence(selected_symbols)
    normalized_successful = _normalize_krx_symbol_sequence(
        successful_symbols
    )
    normalized_retry_debt = _normalize_krx_symbol_sequence(
        retry_debt_symbols
    )
    normalized_terminal = _normalize_krx_symbol_sequence(
        terminal_not_applicable_symbols
    )
    if any(
        normalized is None
        for normalized in (
            normalized_selected,
            normalized_successful,
            normalized_retry_debt,
            normalized_terminal,
        )
    ):
        raise ValueError(
            'exact symbol scope outcomes require canonical KRX short codes'
        )
    selected = list(dict.fromkeys(normalized_selected or []))
    successful_set = set(normalized_successful or [])
    retry_debt_set = set(normalized_retry_debt or [])
    terminal_not_applicable_set = set(normalized_terminal or [])
    selected_set = set(selected)
    if (
        successful_set & retry_debt_set
        or successful_set & terminal_not_applicable_set
        or retry_debt_set & terminal_not_applicable_set
    ):
        raise ValueError('exact symbol scope outcomes overlap')
    if selected_set != (
        successful_set | retry_debt_set | terminal_not_applicable_set
    ):
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
        'terminal_not_applicable_stock_codes': [
            symbol
            for symbol in selected
            if symbol in terminal_not_applicable_set
        ],
        'scope_outcome_manifest': [
            {
                'stock_code': symbol,
                'outcome': (
                    'success'
                    if symbol in successful_set
                    else (
                        'retry_debt'
                        if symbol in retry_debt_set
                        else 'terminal_not_applicable'
                    )
                ),
            }
            for symbol in selected
        ],
    }
