from __future__ import annotations

from typing import Any, Mapping


GENERIC_PARTIAL_PREREQUISITE_MIN_COVERAGE_RATIO = 0.99
HIGH_COVERAGE_PARTIAL_STAGE_IDS: frozenset[str] = frozenset(
    {
        'technical_snapshots',
        'action_summaries',
        'trending_ranks',
        'market_risk_snapshots',
        'market_risk_filter',
        'piotroski_filter',
        'momentum_factors',
        'value_factors',
        'low_risk_factors',
        'beta_factors',
        'advanced_value_factors',
        'unified_value_factors',
        'quality_factors',
        'composite_factors',
    }
)
ORDERING_ONLY_OPTIONAL_CONTEXT_STAGE_IDS: frozenset[str] = frozenset(
    {
        'external_news_collect',
        'external_news_enrich',
        'macro_context_refresh',
    }
)
SATISFIED_DEPENDENCY_SKIP_REASONS: frozenset[str] = frozenset(
    {
        'already_completed',
        'macro_context_refresh_not_due',
        'no_agent_feedback_eligible_symbols',
        'no_stale_or_missing_events',
        'next_open_window_expired',
    }
)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _payloads(outcome: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    details = _mapping(outcome.get('details'))
    return (
        outcome,
        _mapping(outcome.get('result')),
        _mapping(outcome.get('result_summary')),
        details,
        _mapping(details.get('result_summary')),
    )


def _skip_reasons(outcome: Mapping[str, Any]) -> set[str]:
    return {
        reason
        for payload in _payloads(outcome)
        if (reason := str(payload.get('reason') or '').strip().lower())
    }


def _optional_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _first_nonnegative_int(
    payload: Mapping[str, Any],
    keys: tuple[str, ...],
) -> int | None:
    for key in keys:
        parsed = _optional_int(payload.get(key))
        if parsed is not None and parsed >= 0:
            return parsed
    return None


def _partial_has_blocking_evidence(
    payloads: tuple[Mapping[str, Any], ...],
) -> bool:
    gate_statuses = {
        str(payload.get('gate_status') or '').strip().lower()
        for payload in payloads
        if str(payload.get('gate_status') or '').strip()
    }
    if gate_statuses & {'blocked', 'waiting'}:
        return True
    blocking_statuses = {
        str(payload.get('blocking_status') or '').strip().lower()
        for payload in payloads
        if str(payload.get('blocking_status') or '').strip()
    }
    if blocking_statuses & {'blocked', 'blocking'}:
        return True
    for payload in payloads:
        for key in ('blocking_stages', 'failed_blocking_stages'):
            value = payload.get(key)
            if isinstance(value, (list, tuple, set, dict)) and value:
                return True
    return False


def _partial_retries_are_terminal(
    payloads: tuple[Mapping[str, Any], ...],
) -> bool:
    remaining_values = [
        parsed
        for payload in payloads
        if (
            parsed := _optional_int(
                payload.get('remaining_retryable_events')
            )
        )
        is not None
    ]
    if any(value > 0 for value in remaining_values):
        return False

    for payload in payloads:
        requested = _optional_int(payload.get('retry_cycles_requested'))
        completed = _optional_int(payload.get('retry_cycles_completed'))
        if requested is None or requested <= 0:
            continue
        if completed is None or completed < requested:
            return bool(remaining_values) and max(remaining_values) == 0
    return True


def _has_positive_failure_evidence(
    payloads: tuple[Mapping[str, Any], ...],
) -> bool:
    failure_count_keys = (
        'failed',
        'failed_count',
        'symbols_failed',
        'events_failed',
        'results_failed',
        'snapshots_failed',
        'contexts_failed',
        'features_failed',
        'action_summaries_failed',
        'errors_count',
    )
    failure_collection_keys = (
        'errors',
        'failed_symbols',
        'unattempted_symbols',
    )
    for payload in payloads:
        if any(
            (_optional_int(payload.get(key)) or 0) > 0
            for key in failure_count_keys
        ):
            return True
        if any(
            isinstance(payload.get(key), (list, tuple, set, dict))
            and bool(payload.get(key))
            for key in failure_collection_keys
        ):
            return True
        if str(payload.get('error') or '').strip():
            return True
        if str(payload.get('error_message') or '').strip():
            return True
    return False


def _stage_ids(
    payloads: tuple[Mapping[str, Any], ...],
) -> set[str]:
    return {
        str(payload.get(key) or '').strip().lower()
        for payload in payloads
        for key in ('stage', 'stage_id', 'task')
        if str(payload.get(key) or '').strip()
    }


def _optional_context_degradation_is_terminal(
    *,
    status: str,
    payloads: tuple[Mapping[str, Any], ...],
) -> bool:
    """Treat completed optional-context attempts as ordering evidence.

    External news and macro context can reduce decision confidence, but their
    terminal degradation must not become a readiness gate for agent feedback.
    Active or explicitly blocking outcomes still fail closed.
    """

    if not (
        _stage_ids(payloads) & ORDERING_ONLY_OPTIONAL_CONTEXT_STAGE_IDS
    ):
        return False
    if _partial_has_blocking_evidence(payloads):
        return False
    if not _partial_retries_are_terminal(payloads):
        return False
    if status == 'partial':
        return True
    if status != 'skipped':
        return False
    payload_statuses = {
        str(payload.get('status') or '').strip().lower()
        for payload in payloads
        if str(payload.get('status') or '').strip()
    }
    payload_reasons = {
        str(payload.get('reason') or '').strip().lower()
        for payload in payloads
        if str(payload.get('reason') or '').strip()
    }
    return bool(payload_statuses & {'disabled', 'not_configured'}) or (
        'macro_context_disabled' in payload_reasons
    )


def _partial_has_high_coverage(
    payloads: tuple[Mapping[str, Any], ...],
) -> bool:
    metric_families = (
        ('rows_persisted', ('failed', 'failed_count'), ('expected_rows', 'total')),
        ('succeeded', ('failed',), ('total',)),
        ('symbols_succeeded', ('symbols_failed',), ('symbols_total', 'total')),
        (
            'results_persisted',
            ('results_failed', 'failed_count'),
            ('total', 'requested_count', 'requested_symbol_count', 'symbols_selected'),
        ),
        (
            'snapshots_persisted',
            ('snapshots_failed',),
            ('total', 'requested_count', 'requested_symbol_count'),
        ),
        (
            'contexts_persisted',
            ('contexts_failed',),
            ('total', 'requested_count', 'requested_symbol_count'),
        ),
        (
            'features_persisted',
            ('features_failed',),
            ('total', 'features_total', 'requested_count'),
        ),
        (
            'action_summaries_persisted',
            ('action_summaries_failed',),
            ('total', 'requested_count', 'requested_symbol_count'),
        ),
        (
            'symbols_refreshed',
            ('symbols_failed',),
            ('total', 'symbols_total', 'requested_count'),
        ),
    )
    coverage_ratios: list[float] = []
    for payload in payloads:
        for success_key, failed_keys, total_keys in metric_families:
            succeeded = _optional_int(payload.get(success_key))
            if succeeded is None or succeeded < 0:
                continue
            failed = _first_nonnegative_int(payload, failed_keys)
            total = _first_nonnegative_int(payload, total_keys)
            if total is None and failed is not None:
                total = succeeded + failed
            if total is None or total <= 0:
                continue
            coverage_ratios.append(min(succeeded, total) / total)
    return bool(coverage_ratios) and all(
        ratio >= GENERIC_PARTIAL_PREREQUISITE_MIN_COVERAGE_RATIO
        for ratio in coverage_ratios
    )


def _partial_is_piotroski_only_fundamentals_gap(
    outcome: Mapping[str, Any],
    payloads: tuple[Mapping[str, Any], ...],
) -> bool:
    if 'fundamentals_daily_refresh' not in _stage_ids(payloads):
        return False
    partial_reasons = {
        str(reason or '').strip().lower()
        for payload in payloads
        for reason in (
            payload.get('partial_reasons')
            if isinstance(payload.get('partial_reasons'), list)
            else []
        )
        if str(reason or '').strip()
    }
    if partial_reasons != {'piotroski_filter_followup_failed'}:
        return False
    failure_keys = ('symbols_failed', 'events_failed')
    return not any(
        (_optional_int(payload.get(key)) or 0) > 0
        for payload in payloads
        for key in failure_keys
    )


def _nested_stages_are_satisfied(outcome: Mapping[str, Any]) -> bool | None:
    nested_stages = outcome.get('stages')
    if not isinstance(nested_stages, Mapping) or not nested_stages:
        return None
    return all(
        daily_pipeline_outcome_is_satisfied(
            {
                'stage': str(stage_id),
                **dict(_mapping(stage_outcome)),
            }
        )
        for stage_id, stage_outcome in nested_stages.items()
    )


def _partial_outcome_is_satisfied(outcome: Mapping[str, Any]) -> bool:
    payloads = _payloads(outcome)
    if _partial_has_blocking_evidence(payloads):
        return False
    if not _partial_retries_are_terminal(payloads):
        return False

    gate_statuses = {
        str(payload.get('gate_status') or '').strip().lower()
        for payload in payloads
        if str(payload.get('gate_status') or '').strip()
    }
    nested_stages_satisfied = _nested_stages_are_satisfied(outcome)
    if nested_stages_satisfied is not None:
        return nested_stages_satisfied
    if _optional_context_degradation_is_terminal(
        status='partial',
        payloads=payloads,
    ):
        return True
    if gate_statuses == {'ready'}:
        return True
    return (
        bool(_stage_ids(payloads) & HIGH_COVERAGE_PARTIAL_STAGE_IDS)
        and _partial_has_high_coverage(payloads)
    ) or _partial_is_piotroski_only_fundamentals_gap(outcome, payloads)


def daily_pipeline_outcome_is_satisfied(
    outcome: Mapping[str, Any] | None,
) -> bool:
    """Return whether an outcome is safe evidence for downstream progress.

    Persisted stage rows and in-memory outcomes use slightly different
    wrappers, so the reason may live under ``result``, ``result_summary``, or
    ``details.result_summary``. Unknown and conflicting skip reasons fail
    closed; only the explicit idempotent/no-work allowlist and terminal
    optional-context degradation markers are safe downstream proof.
    """

    if not isinstance(outcome, Mapping):
        return False
    status = str(outcome.get('status') or '').strip().lower()
    if status == 'completed':
        payloads = _payloads(outcome)
        if (
            _partial_has_blocking_evidence(payloads)
            or not _partial_retries_are_terminal(payloads)
            or _has_positive_failure_evidence(payloads)
        ):
            return False
        nested_stages_satisfied = _nested_stages_are_satisfied(outcome)
        return (
            True
            if nested_stages_satisfied is None
            else nested_stages_satisfied
        )
    if status == 'partial':
        return _partial_outcome_is_satisfied(outcome)
    if status != 'skipped':
        return False
    payloads = _payloads(outcome)
    if _optional_context_degradation_is_terminal(
        status='skipped',
        payloads=payloads,
    ):
        return True
    reasons = _skip_reasons(outcome)
    return bool(reasons) and reasons <= SATISFIED_DEPENDENCY_SKIP_REASONS
