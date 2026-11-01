from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from typing import Any, TypeVar

from app.eod.scope_outcomes import build_exact_symbol_scope_outcome


FACTOR_COVERAGE_CONTRACT_VERSION = 'factor_usable_coverage_v1'
FACTOR_SCOPE_OUTCOME_MANIFEST_VERSION = 'factor_scope_outcomes_v1'

ItemPredicate = Callable[[Mapping[str, Any]], bool]
ReasonResolver = Callable[[Mapping[str, Any]], str | None]
InputFamilySpec = tuple[ItemPredicate, ReasonResolver]
SymbolInput = TypeVar('SymbolInput')


SYMBOL_LOCAL_DATA_ERRORS = (
    ArithmeticError,
    KeyError,
    TypeError,
    ValueError,
)
# Compatibility alias for callers that still perform prove-by-singleton
# attribution before converting a data-shaped failure to symbol debt.
SYMBOL_LOCAL_COMPUTATION_ERRORS = SYMBOL_LOCAL_DATA_ERRORS


class SymbolComputationError(Exception):
    """Marks a data-shaped failure proven to belong to one symbol only."""

    def __init__(self, cause: Exception) -> None:
        super().__init__(str(cause))
        self.cause = cause


def run_symbol_computation(
    build: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    """Explicitly convert one symbol's local data error into retry debt."""

    try:
        return build()
    except SYMBOL_LOCAL_DATA_ERRORS as exc:
        raise SymbolComputationError(exc) from exc


def build_symbol_results(
    inputs: Sequence[SymbolInput],
    *,
    symbol_of: Callable[[SymbolInput], str],
    build: Callable[[SymbolInput], dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[str], list[dict[str, str]]]:
    """Compute independent symbol rows without hiding global stage failures.

    Only data-shaped exceptions raised while computing one symbol are converted
    to retry debt. Database/session/ownership failures and cohort-wide invariant
    failures happen outside this boundary (or use a different exception type),
    so they continue to abort the stage.
    """

    items: list[dict[str, Any]] = []
    failed_symbols: list[str] = []
    failed_symbol_errors: list[dict[str, str]] = []
    for input_row in inputs:
        symbol = _normalize_symbol(symbol_of(input_row))
        if not symbol:
            raise ValueError('symbol identity is required for isolated computation')
        try:
            items.append(build(input_row))
        except SymbolComputationError as exc:
            cause = exc.cause
            failed_symbols.append(symbol)
            failed_symbol_errors.append(
                {
                    'symbol': symbol,
                    'error': (
                        'symbol_processing_failed: '
                        f'{type(cause).__name__}: {cause}'
                    ),
                }
            )
    return items, failed_symbols, failed_symbol_errors


def attach_symbol_payloads(
    items: Sequence[dict[str, Any]],
    *,
    payload_builder: Callable[[dict[str, Any]], dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[str], list[dict[str, str]]]:
    """Attach one symbol's audit payload inside the same isolation boundary."""

    def attach(item: dict[str, Any]) -> dict[str, Any]:
        return run_symbol_computation(
            lambda: _attach_symbol_payload(
                item=item,
                payload=payload_builder(item),
            )
        )

    return build_symbol_results(
        items,
        symbol_of=lambda item: str(item.get('symbol') or ''),
        build=attach,
    )


def _attach_symbol_payload(
    *,
    item: dict[str, Any],
    payload: dict[str, Any],
) -> dict[str, Any]:
    item['factor_payload'] = payload
    return item


def merge_symbol_failures(
    *failure_groups: tuple[Sequence[str], Sequence[Mapping[str, Any]]],
) -> tuple[list[str], list[dict[str, str]]]:
    """Merge failure cohorts while preserving the most specific first error."""

    symbols: list[str] = []
    error_by_symbol: dict[str, str] = {}
    for failed_symbols, failed_symbol_errors in failure_groups:
        for value in failed_symbols:
            symbol = _normalize_symbol(value)
            if symbol and symbol not in symbols:
                symbols.append(symbol)
        for failure in failed_symbol_errors:
            symbol = _normalize_symbol(failure.get('symbol'))
            if symbol and symbol not in error_by_symbol:
                error_by_symbol[symbol] = str(failure.get('error') or '')
    return symbols, [
        {'symbol': symbol, 'error': error_by_symbol.get(symbol, '')}
        for symbol in symbols
    ]


def persist_symbol_items_with_isolation(
    items: Sequence[dict[str, Any]],
    *,
    bulk_write: Callable[[Sequence[dict[str, Any]]], Any],
    item_write: Callable[[dict[str, Any]], Any] | None,
    failure_prefix: str = 'persistence_failed',
    write_empty_batch: bool = False,
) -> tuple[list[dict[str, Any]], list[str], list[dict[str, str]]]:
    """Persist symbol rows while keeping infrastructure failures fatal.

    A data-shaped bulk failure is retried one row at a time so attribution is
    proven before it becomes retry debt. SQLAlchemy/database exceptions,
    transaction failures, and unknown runtime failures are intentionally not
    caught at either boundary and therefore abort the stage.
    """

    normalized_items: list[tuple[dict[str, Any], str]] = []
    for item in items:
        symbol = _normalize_symbol(item.get('symbol'))
        if not symbol:
            raise ValueError('symbol identity is required for isolated persistence')
        normalized_items.append((item, symbol))

    if not normalized_items:
        if write_empty_batch:
            bulk_write(items)
        return [], [], []

    try:
        bulk_write(items)
    except SYMBOL_LOCAL_DATA_ERRORS as bulk_exc:
        if item_write is None:
            failed_symbols = [symbol for _, symbol in normalized_items]
            return (
                [],
                failed_symbols,
                [
                    {
                        'symbol': symbol,
                        'error': (
                            f'{failure_prefix}: '
                            f'{type(bulk_exc).__name__}: {bulk_exc}'
                        ),
                    }
                    for symbol in failed_symbols
                ],
            )
        persisted_items: list[dict[str, Any]] = []
        failed_symbols: list[str] = []
        failed_symbol_errors: list[dict[str, str]] = []
        for item, symbol in normalized_items:
            try:
                item_write(item)
            except SYMBOL_LOCAL_DATA_ERRORS as exc:
                failed_symbols.append(symbol)
                failed_symbol_errors.append(
                    {
                        'symbol': symbol,
                        'error': (
                            f'{failure_prefix}: '
                            f'{type(exc).__name__}: {exc}'
                        ),
                    }
                )
            else:
                persisted_items.append(item)
        return persisted_items, failed_symbols, failed_symbol_errors

    return list(items), [], []


def partition_symbol_failures(
    *,
    failed_symbols: Sequence[str],
    failed_symbol_errors: Sequence[Mapping[str, Any]],
    target_symbols: Sequence[str],
) -> tuple[
    list[str],
    list[dict[str, str]],
    list[str],
    list[dict[str, str]],
]:
    """Separate persistence-target failures from ranking-only dependency debt."""

    target_set = {_normalize_symbol(symbol) for symbol in target_symbols}
    target_failures = [
        symbol
        for symbol in failed_symbols
        if _normalize_symbol(symbol) in target_set
    ]
    dependency_failures = [
        symbol
        for symbol in failed_symbols
        if _normalize_symbol(symbol) not in target_set
    ]
    target_failure_set = {_normalize_symbol(symbol) for symbol in target_failures}
    dependency_failure_set = {
        _normalize_symbol(symbol) for symbol in dependency_failures
    }
    target_errors = [
        {'symbol': _normalize_symbol(item.get('symbol')), 'error': str(item.get('error') or '')}
        for item in failed_symbol_errors
        if _normalize_symbol(item.get('symbol')) in target_failure_set
    ]
    dependency_errors = [
        {'symbol': _normalize_symbol(item.get('symbol')), 'error': str(item.get('error') or '')}
        for item in failed_symbol_errors
        if _normalize_symbol(item.get('symbol')) in dependency_failure_set
    ]
    return (
        list(dict.fromkeys(_normalize_symbol(symbol) for symbol in target_failures)),
        target_errors,
        list(dict.fromkeys(_normalize_symbol(symbol) for symbol in dependency_failures)),
        dependency_errors,
    )


def attach_ranking_dependency_contract(
    summary: dict[str, Any],
    *,
    failed_symbols: Sequence[str],
    failed_symbol_errors: Sequence[Mapping[str, Any]],
) -> None:
    """Expose ranking-population debt without polluting target persistence scope."""

    symbols, errors = merge_symbol_failures(
        (failed_symbols, failed_symbol_errors),
    )
    summary.update(
        {
            'ranking_dependency_status': (
                'degraded' if symbols else 'complete'
            ),
            'ranking_dependency_failed_symbol_count': len(symbols),
            'ranking_dependency_failed_symbols': symbols,
            'ranking_dependency_failed_symbol_errors': errors,
            'ranking_dependency_retry_debt_symbols': symbols,
        }
    )
    if not symbols:
        return
    summary['coverage_status'] = 'partial'
    summary['coverage_reason'] = 'ranking_dependency_incomplete'
    summary['quality_status'] = 'degraded'
    summary['quality_warning'] = 'ranking_dependency_incomplete'


def _normalized_reason(value: str | None, *, fallback: str) -> str:
    normalized = str(value or '').strip().lower()
    return normalized or fallback


def _coverage_ratio(*, available_count: int, universe_count: int) -> float:
    if universe_count <= 0:
        return 0.0
    return round(available_count / universe_count, 6)


def _normalize_symbol(value: Any) -> str:
    symbol = str(value or '').strip().upper()
    if symbol.isdigit():
        symbol = symbol.zfill(6)
    return symbol


def _build_persistence_scope_outcome(
    *,
    items: Sequence[Mapping[str, Any]],
    persisted_items: Sequence[Mapping[str, Any]],
    missing_symbols: Sequence[str],
    failed_symbols: Sequence[str],
) -> dict[str, Any]:
    """Bind partial persistence to an exhaustive per-symbol partition.

    A factor row can be decision-degraded yet still be a successfully
    persisted, explicitly classified observation.  Retry debt is therefore
    limited to selected symbols for which no row was durably persisted.  If
    row identity is malformed or ambiguous, omit the proof so downstream
    orchestration remains fail-closed instead of guessing a success cohort.
    """

    item_symbols = [_normalize_symbol(item.get('symbol')) for item in items]
    persisted_symbols = [
        _normalize_symbol(item.get('symbol')) for item in persisted_items
    ]
    missing = [_normalize_symbol(symbol) for symbol in missing_symbols]
    failed = [_normalize_symbol(symbol) for symbol in failed_symbols]
    identity_groups = (item_symbols, persisted_symbols, missing, failed)
    if any(not symbol for group in identity_groups for symbol in group):
        return {}
    if len(item_symbols) != len(set(item_symbols)):
        return {}
    if len(persisted_symbols) != len(set(persisted_symbols)):
        return {}

    selected_symbols = list(
        dict.fromkeys([*item_symbols, *missing, *failed])
    )
    selected_set = set(selected_symbols)
    successful_set = set(persisted_symbols)
    if (
        not successful_set <= selected_set
        or successful_set.intersection({*missing, *failed})
    ):
        return {}
    retry_debt_symbols = [
        symbol for symbol in selected_symbols if symbol not in successful_set
    ]
    return build_exact_symbol_scope_outcome(
        manifest_version=FACTOR_SCOPE_OUTCOME_MANIFEST_VERSION,
        selected_symbols=selected_symbols,
        successful_symbols=persisted_symbols,
        retry_debt_symbols=retry_debt_symbols,
    )


def build_factor_coverage_contract(
    *,
    items: Sequence[Mapping[str, Any]],
    persisted_items: Sequence[Mapping[str, Any]],
    missing_symbols: Sequence[str],
    failed_symbols: Sequence[str],
    is_ready: ItemPredicate,
    is_complete: ItemPredicate,
    unusable_reason: ReasonResolver,
    input_families: Mapping[str, InputFamilySpec],
    failed_symbol_errors: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Build the common decision-usable coverage contract for factor stages.

    Output usability is deliberately based on rows that were actually
    persisted. Input-family coverage uses the same persisted cohort so a
    computed-but-unpersisted row cannot make downstream strategy evidence look
    healthier than it is.
    """

    persisted = list(persisted_items)
    processing_failed_symbol_set = {
        _normalize_symbol(item.get('symbol'))
        for item in failed_symbol_errors
        if str(item.get('error') or '').startswith(
            'symbol_processing_failed:'
        )
    }
    processing_failed_symbol_set.discard('')
    processing_failed_symbols = [
        _normalize_symbol(symbol)
        for symbol in failed_symbols
        if _normalize_symbol(symbol) in processing_failed_symbol_set
    ]
    persistence_failed_symbols = [
        _normalize_symbol(symbol)
        for symbol in failed_symbols
        if _normalize_symbol(symbol) not in processing_failed_symbol_set
    ]
    scope_outcome = _build_persistence_scope_outcome(
        items=items,
        persisted_items=persisted,
        missing_symbols=missing_symbols,
        failed_symbols=failed_symbols,
    )
    universe_count = len(
        scope_outcome.get('selected_stock_codes')
        or [*items, *missing_symbols]
    )
    ready_items = [item for item in persisted if is_ready(item)]
    complete_items = [item for item in ready_items if is_complete(item)]
    ready_count = len(ready_items)
    complete_count = len(complete_items)
    partial_count = ready_count - complete_count
    unusable_count = max(universe_count - ready_count, 0)

    missing_reason_counts: Counter[str] = Counter()
    if missing_symbols:
        missing_reason_counts['missing_symbol'] += len(missing_symbols)
    if processing_failed_symbols:
        missing_reason_counts['symbol_processing_failed'] += len(
            set(processing_failed_symbols)
        )
    if persistence_failed_symbols:
        missing_reason_counts['persistence_failed'] += len(
            set(persistence_failed_symbols)
        )
    retryable_symbols = sorted(
        scope_outcome.get('retry_debt_stock_codes')
        or {
            _normalize_symbol(symbol)
            for symbol in (
                [*missing_symbols, *failed_symbols]
                if scope_outcome
                else [
                    *(item.get('symbol') for item in items),
                    *missing_symbols,
                    *failed_symbols,
                ]
            )
            if _normalize_symbol(symbol)
        }
    )
    for item in persisted:
        if is_ready(item):
            continue
        missing_reason_counts[
            _normalized_reason(
                unusable_reason(item),
                fallback='unusable_factor_row',
            )
        ] += 1

    input_family_cohort_coverage: dict[str, dict[str, Any]] = {}
    for family, (is_available, missing_reason) in input_families.items():
        available_count = sum(1 for item in persisted if is_available(item))
        family_missing_reasons: Counter[str] = Counter()
        if missing_symbols:
            family_missing_reasons['missing_symbol'] += len(missing_symbols)
        if processing_failed_symbols:
            family_missing_reasons['symbol_processing_failed'] += len(
                set(processing_failed_symbols)
            )
        if persistence_failed_symbols:
            family_missing_reasons['persistence_failed'] += len(
                set(persistence_failed_symbols)
            )
        for item in persisted:
            if is_available(item):
                continue
            family_missing_reasons[
                _normalized_reason(
                    missing_reason(item),
                    fallback='unavailable_input',
                )
            ] += 1
        input_family_cohort_coverage[family] = {
            'cohort_count': universe_count,
            'available_count': available_count,
            'unavailable_count': max(universe_count - available_count, 0),
            'coverage_ratio': _coverage_ratio(
                available_count=available_count,
                universe_count=universe_count,
            ),
            'missing_reason_counts': dict(sorted(family_missing_reasons.items())),
        }

    if universe_count > 0 and ready_count == universe_count:
        coverage_status = 'completed'
        coverage_reason = 'all_factor_rows_usable'
    elif ready_count > 0:
        coverage_status = 'partial'
        coverage_reason = 'partial_usable_coverage'
    else:
        coverage_status = 'partial'
        coverage_reason = 'no_usable_factor_rows'

    if ready_count == 0:
        quality_status = 'unavailable'
        quality_warning = 'no_usable_factor_rows'
    elif unusable_count > 0 and partial_count > 0:
        quality_status = 'degraded'
        quality_warning = 'partial_and_unusable_factor_cohorts'
    elif unusable_count > 0:
        quality_status = 'degraded'
        quality_warning = 'unusable_factor_cohort'
    elif partial_count > 0:
        quality_status = 'degraded'
        quality_warning = 'partial_input_factor_cohort'
    else:
        quality_status = 'complete'
        quality_warning = None

    return {
        'coverage_contract_version': FACTOR_COVERAGE_CONTRACT_VERSION,
        'coverage_status': coverage_status,
        'coverage_reason': coverage_reason,
        'quality_status': quality_status,
        'quality_warning': quality_warning,
        'universe_count': universe_count,
        'computed_count': len(items),
        'persisted_count': len(persisted),
        'scored_count': ready_count,
        'ready_count': ready_count,
        'complete_count': complete_count,
        'partial_count': partial_count,
        'unusable_count': unusable_count,
        'unready_count': unusable_count,
        'usable_coverage_ratio': _coverage_ratio(
            available_count=ready_count,
            universe_count=universe_count,
        ),
        'missing_reason_counts': dict(sorted(missing_reason_counts.items())),
        'retryable_symbol_count': len(retryable_symbols),
        'retryable_symbols': retryable_symbols,
        'symbol_processing_failed_count': len(
            set(processing_failed_symbols)
        ),
        'persistence_failed_count': len(
            set(persistence_failed_symbols)
        ),
        'input_family_cohort_coverage': input_family_cohort_coverage,
        **scope_outcome,
    }


def factor_stage_result_contract(
    summary: Mapping[str, Any],
    *,
    explicit_empty_scope: bool = False,
) -> dict[str, Any]:
    """Expose coverage as a nonblocking partial only when usable rows exist."""

    ready_count = int(summary.get('ready_count') or 0)
    retryable_symbols = list(summary.get('retryable_symbols') or [])
    retryable_count = int(
        summary.get('retryable_symbol_count')
        or len(retryable_symbols)
    )
    has_exact_scope_outcome = (
        summary.get('scope_outcome_manifest_version')
        == FACTOR_SCOPE_OUTCOME_MANIFEST_VERSION
        and isinstance(summary.get('selected_stock_codes'), list)
        and isinstance(summary.get('successful_stock_codes'), list)
        and isinstance(summary.get('retry_debt_stock_codes'), list)
        and isinstance(summary.get('scope_outcome_manifest'), list)
    )
    ranking_dependency_failed_count = int(
        summary.get('ranking_dependency_failed_symbol_count') or 0
    )
    ranking_dependency_incomplete = (
        not explicit_empty_scope and ranking_dependency_failed_count > 0
    )
    gate_ready = explicit_empty_scope or (
        ready_count > 0
        and has_exact_scope_outcome
        and not ranking_dependency_incomplete
    )
    result = {
        'status': (
            'completed'
            if explicit_empty_scope
            else (
                (
                    'partial'
                    if ranking_dependency_incomplete
                    else str(summary.get('coverage_status') or 'partial')
                )
                if has_exact_scope_outcome
                else 'partial'
            )
        ),
        'reason': (
            'explicit_empty_scope_noop'
            if explicit_empty_scope
            else (
                (
                    'ranking_dependency_incomplete'
                    if ranking_dependency_incomplete
                    else str(
                        summary.get('coverage_reason')
                        or 'no_usable_factor_rows'
                    )
                )
                if has_exact_scope_outcome
                else 'scope_outcome_manifest_invalid'
            )
        ),
        'quality_status': (
            'degraded'
            if ranking_dependency_incomplete
            else str(summary.get('quality_status') or 'unavailable')
        ),
        'quality_warning': (
            'ranking_dependency_incomplete'
            if ranking_dependency_incomplete
            else summary.get('quality_warning')
        ),
        'gate_status': (
            'ready' if gate_ready else 'blocked'
        ),
        'blocking_status': (
            'non_blocking' if gate_ready else 'blocking'
        ),
        'remaining_retryable_events': (
            0 if explicit_empty_scope else retryable_count
        ),
        'retryable_symbols': (
            [] if explicit_empty_scope else retryable_symbols
        ),
    }
    for key in (
        'scope_outcome_manifest_version',
        'selected_stock_codes',
        'successful_stock_codes',
        'retry_debt_stock_codes',
        'terminal_not_applicable_stock_codes',
        'scope_outcome_manifest',
    ):
        if key in summary:
            result[key] = summary[key]
    for key in (
        'ranking_dependency_status',
        'ranking_dependency_failed_symbol_count',
        'ranking_dependency_failed_symbols',
        'ranking_dependency_failed_symbol_errors',
        'ranking_dependency_retry_debt_symbols',
    ):
        if key in summary:
            result[key] = summary[key]
    return result
