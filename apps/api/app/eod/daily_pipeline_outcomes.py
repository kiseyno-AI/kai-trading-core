from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Mapping

from app.eod.market_membership import market_membership_sha256
from app.eod.scope_outcomes import EXACT_SCOPE_OUTCOME_MANIFEST_VERSIONS


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
PYKRX_ENRICHMENT_REQUIRED_CATEGORIES: frozenset[str] = frozenset(
    {
        'sector_classification',
        'investor_flow_symbol',
        'short_selling',
        'valuation_snapshot',
    }
)
_TERMINAL_FEEDBACK_LEASE_LOSS_ERROR = re.compile(
    r'\Afeedback retry lease ownership lost for '
    r'(?P<run_id>feedback-cycle-(?P<market_date>\d{4}-\d{2}-\d{2})-'
    r'[0-9a-f]{10}) at before_parent_finalization: '
    r'heartbeat_refresh_grace_exceeded\Z'
)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def pykrx_enrichment_preflight_is_ready(
    preflight: Mapping[str, Any] | None,
) -> bool:
    """Return whether persisted enrichment proves all required gates ready."""

    if not isinstance(preflight, Mapping):
        return False
    required_categories = preflight.get('required_categories')
    category_row_counts = preflight.get('category_row_counts')
    category_decision_usable_row_counts = preflight.get(
        'category_decision_usable_row_counts'
    )
    decision_usable_counts_are_ready = bool(
        not isinstance(category_decision_usable_row_counts, Mapping)
        or all(
            (
                _optional_int(
                    category_decision_usable_row_counts.get(category)
                )
                or 0
            )
            > 0
            for category in PYKRX_ENRICHMENT_REQUIRED_CATEGORIES
        )
    )
    zero_decision_usable_categories = preflight.get(
        'zero_decision_usable_categories'
    )
    return bool(
        preflight.get('allowed') is True
        and str(preflight.get('status') or '').strip().lower() == 'ready'
        and isinstance(required_categories, list)
        and all(
            isinstance(category, str) and category.strip()
            for category in required_categories
        )
        and set(required_categories)
        == PYKRX_ENRICHMENT_REQUIRED_CATEGORIES
        and isinstance(category_row_counts, Mapping)
        and all(
            (_optional_int(category_row_counts.get(category)) or 0) > 0
            for category in PYKRX_ENRICHMENT_REQUIRED_CATEGORIES
        )
        and decision_usable_counts_are_ready
        and preflight.get('failed_categories') == []
        and preflight.get('blocked_categories') == []
        and preflight.get('waiting_categories') == []
        and preflight.get('zero_coverage_categories') == []
        and (
            zero_decision_usable_categories is None
            or zero_decision_usable_categories == []
        )
    )


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


def _semantic_result_payloads(
    result: Mapping[str, Any],
) -> tuple[Mapping[str, Any], ...]:
    payloads: list[Mapping[str, Any]] = []
    pending: list[Mapping[str, Any]] = [result]
    seen_ids: set[int] = set()
    while pending:
        payload = pending.pop(0)
        if id(payload) in seen_ids:
            continue
        seen_ids.add(id(payload))
        payloads.append(payload)
        for key in ('result', 'result_summary', 'summary', 'details'):
            nested = payload.get(key)
            if isinstance(nested, Mapping):
                pending.append(nested)
    return tuple(payloads)


def daily_pipeline_result_has_no_ready_content(
    result: Mapping[str, Any] | None,
) -> bool:
    """Return whether persisted factor rows contain no decision-usable result."""

    if not isinstance(result, Mapping):
        return False
    payloads = _semantic_result_payloads(result)
    persisted_count = next(
        (
            value
            for key in ('results_persisted', 'rows_persisted')
            for payload in payloads
            if (value := _optional_int(payload.get(key))) is not None
            and value > 0
        ),
        None,
    )
    if persisted_count is not None:
        for payload in payloads:
            persisted_ready_count = _optional_int(
                payload.get('persisted_ready_count')
            )
            persisted_unready_count = _optional_int(
                payload.get('persisted_unready_count')
            )
            if (
                persisted_ready_count == 0
                and (persisted_unready_count or 0) >= persisted_count
            ):
                return True
    expected_count = next(
        (
            value
            for key in (
                'results_persisted',
                'rows_persisted',
                'matched_symbol_count',
                'requested_symbol_count',
                'total',
            )
            for payload in payloads
            if (value := _optional_int(payload.get(key))) is not None
            and value > 0
        ),
        None,
    )
    if expected_count is None:
        return False

    for payload in payloads:
        ready_count = _optional_int(payload.get('ready_count'))
        insufficient_count = _optional_int(
            payload.get('insufficient_history_count')
        )
        unready_count = _optional_int(payload.get('unready_count'))
        if ready_count != 0:
            continue
        if (unready_count or 0) >= expected_count:
            return True
        if (insufficient_count or 0) >= expected_count:
            return True
    return False


def daily_pipeline_zero_scope_gate_status(
    result: Mapping[str, Any] | None,
) -> str | None:
    """Return a blocking semantic status for zero-work incomplete context."""

    if not isinstance(result, Mapping):
        return None
    summary = _mapping(result.get('summary'))
    scope_counts = [
        value
        for payload in (result, summary)
        for key in ('items_total', 'symbol_count')
        if (value := _optional_int(payload.get(key))) is not None
    ]
    if not scope_counts or any(value > 0 for value in scope_counts):
        return None

    context = _mapping(summary.get('agent_context_completeness'))
    context_statuses = {
        str(value or '').strip().lower()
        for value in (
            result.get('agent_context_completeness_status'),
            summary.get('agent_context_completeness_status'),
            context.get('status'),
        )
        if str(value or '').strip()
    }
    if context_statuses & {'waiting', 'pending', 'running'}:
        return 'waiting'
    if context_statuses & {
        'abandoned',
        'blocked',
        'failed',
        'incomplete',
        'interrupted',
        'partial',
        'unverified',
    }:
        return 'blocked'
    pending_scope_count = _optional_int(context.get('pending_scope_count')) or 0
    if context.get('candidate_set_definitive') is False and pending_scope_count > 0:
        return 'blocked'
    return None


def terminal_continuation_expired_decision_skip_evidence_is_valid(
    *,
    stage_row: object,
    stage_id: str,
    market_date: date,
) -> bool:
    """Validate the exact no-child result emitted after an action window closes."""

    def stage_int(value: Any) -> int:
        try:
            return int(value if value is not None else 0)
        except (TypeError, ValueError):
            return 0

    details = getattr(stage_row, 'details', {})
    result_summary = (
        details.get('result_summary')
        if isinstance(details, Mapping)
        and isinstance(details.get('result_summary'), Mapping)
        else {}
    )
    required_result = {
        'triggered': False,
        'status': 'skipped',
        'reason': 'next_open_window_expired',
        'market_date': market_date.isoformat(),
        'stage': stage_id,
    }
    allowed_result_keys = {
        *required_result,
        'target_trade_date',
        'current_market_date',
        'market_state',
        'completed_after_action_window',
    }
    completed_at = getattr(stage_row, 'completed_at', None)
    if isinstance(completed_at, datetime):
        if completed_at.tzinfo is None or completed_at.utcoffset() is None:
            completed_at = completed_at.replace(tzinfo=timezone.utc)
        else:
            completed_at = completed_at.astimezone(timezone.utc)
    else:
        completed_at = None
    if not (
        str(getattr(stage_row, 'status', '') or '').strip().lower()
        == 'skipped'
        and completed_at is not None
        and not str(getattr(stage_row, 'error_message', '') or '').strip()
        and str(getattr(stage_row, 'current_step', '') or '').strip()
        == f'{stage_id}_skipped'
        and all(
            stage_int(getattr(stage_row, field, 0)) == 0
            for field in (
                'processed',
                'total',
                'succeeded',
                'failed',
                'remaining',
            )
        )
        and isinstance(getattr(stage_row, 'source_run_ids', {}), Mapping)
        and not getattr(stage_row, 'source_run_ids', {})
        and set(result_summary).issubset(allowed_result_keys)
        and all(
            result_summary.get(key) == value
            for key, value in required_result.items()
        )
        and (
            result_summary.get('completed_after_action_window') is None
            or result_summary.get('completed_after_action_window') is True
        )
    ):
        return False

    try:
        target_trade_date = date.fromisoformat(
            str(result_summary.get('target_trade_date') or '')
        )
        current_market_date = date.fromisoformat(
            str(result_summary.get('current_market_date') or '')
        )
    except ValueError:
        return False
    market_state = str(
        result_summary.get('market_state') or ''
    ).strip().upper()
    return bool(
        target_trade_date > market_date
        and market_state
        in {'PREOPEN', 'OPEN', 'CLOSED', 'WEEKEND', 'HOLIDAY'}
        and current_market_date
        == completed_at.astimezone(timezone(timedelta(hours=9))).date()
        and (
            target_trade_date < current_market_date
            if market_state == 'PREOPEN'
            else target_trade_date <= current_market_date
        )
    )


def terminal_continuation_marker_is_valid(
    metadata: Mapping[str, Any] | None,
) -> bool:
    """Recognize only the one-shot terminal continuation claim shape."""

    if not isinstance(metadata, Mapping):
        return False
    claimed_at = metadata.get('terminal_continuation_claimed_at')
    try:
        parsed_claimed_at = datetime.fromisoformat(
            str(claimed_at or '').strip().replace('Z', '+00:00')
        )
    except ValueError:
        return False
    if parsed_claimed_at.tzinfo is None:
        parsed_claimed_at = parsed_claimed_at.replace(tzinfo=timezone.utc)
    return bool(
        _optional_int(metadata.get('terminal_continuation_count')) == 1
        and _optional_int(metadata.get('terminal_continuation_version')) == 1
        and str(metadata.get('terminal_continuation_reason') or '').strip()
        == 'summary_knowledge_cutoff_repair'
        and str(metadata.get('resume_from_stage') or '').strip()
        == 'agent_feedback_summaries'
        and parsed_claimed_at.tzinfo is not None
        and isinstance(
            metadata.get('terminal_continuation_evidence'),
            Mapping,
        )
    )


def terminal_continuation_resume_is_unconsumed(
    metadata: Mapping[str, Any] | None,
) -> bool:
    """Accept only a missing/null or exact integer-zero reclaim counter."""

    if not isinstance(metadata, Mapping):
        return False
    value = metadata.get('terminal_continuation_resume_count')
    return value is None or (type(value) is int and value == 0)


def terminal_continuation_lease_loss_resume_lineage_is_valid(
    metadata: Mapping[str, Any] | None,
) -> bool:
    """Recognize repeated restart recovery of one claimed lease-loss suffix."""

    if not isinstance(metadata, Mapping):
        return False
    resume_count = metadata.get('terminal_continuation_resume_count')
    if type(resume_count) is not int or resume_count < 1:
        return False
    resumed_from_status = str(
        metadata.get('terminal_continuation_resumed_from_status') or ''
    ).strip().lower()
    return bool(
        (resume_count == 1 and resumed_from_status == 'failed')
        or (resume_count >= 2 and resumed_from_status == 'interrupted')
    )


def terminal_continuation_feedback_lease_loss_run_id(
    *,
    pipeline_run: Mapping[str, Any] | None,
    stage: Mapping[str, Any] | None,
) -> str | None:
    """Return the child id only for the exact reclaimable lease-loss shape."""

    if not isinstance(pipeline_run, Mapping) or not isinstance(stage, Mapping):
        return None
    metadata = pipeline_run.get('run_metadata')
    if not terminal_continuation_marker_is_valid(
        metadata if isinstance(metadata, Mapping) else None
    ):
        return None
    if not terminal_continuation_resume_is_unconsumed(metadata):
        return None
    if not (
        str(pipeline_run.get('status') or '').strip().lower() == 'failed'
        and str(pipeline_run.get('current_stage') or '').strip()
        == 'signal_decider'
        and pipeline_run.get('completed_at') is not None
        and str(stage.get('stage_id') or stage.get('stage') or '').strip()
        == 'signal_decider'
        and str(stage.get('status') or '').strip().lower() == 'failed'
        and str(stage.get('current_step') or '').strip()
        == 'signal_decider_failed'
        and stage.get('completed_at') is not None
    ):
        return None

    parent_error = str(pipeline_run.get('error_message') or '').strip()
    stage_error = str(stage.get('error_message') or '').strip()
    if not parent_error or parent_error != stage_error:
        return None
    error_match = _TERMINAL_FEEDBACK_LEASE_LOSS_ERROR.fullmatch(parent_error)
    if error_match is None:
        return None
    if str(pipeline_run.get('market_date') or '')[:10] != error_match.group(
        'market_date'
    ):
        return None

    child_run_id = error_match.group('run_id')
    source_run_ids = stage.get('source_run_ids')
    if not (
        isinstance(source_run_ids, Mapping)
        and source_run_ids == {'agent_feedback_run_id': child_run_id}
    ):
        return None

    counts = {
        key: _optional_int(stage.get(key))
        for key in ('processed', 'total', 'succeeded', 'failed', 'remaining')
    }
    if any(value is None or value < 0 for value in counts.values()):
        return None
    processed = counts['processed']
    total = counts['total']
    succeeded = counts['succeeded']
    failed = counts['failed']
    remaining = counts['remaining']
    if not (
        processed > 0
        and total > 0
        and failed > 0
        and processed == succeeded + failed
        and total == processed + remaining
    ):
        return None
    return child_run_id


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


def normalize_daily_pipeline_stage_status(
    *,
    stage_id: str | None,
    status: Any,
    outcome: Mapping[str, Any] | None = None,
) -> str:
    """Canonicalize terminal optional-context unavailability.

    The default daily pipeline treats news and macro context as ordering-only
    inputs. Once an attempted optional stage has terminally reported that the
    context is unavailable, keep that degradation visible as ``partial``
    rather than leaving a non-canonical status that poisons the parent run.
    Required stages and any active, blocked, or retry-pending shape remain
    unchanged and therefore fail closed.
    """

    normalized_status = str(status or '').strip().lower()
    normalized_stage_id = str(stage_id or '').strip().lower()
    if (
        normalized_status != 'unavailable'
        or normalized_stage_id
        not in ORDERING_ONLY_OPTIONAL_CONTEXT_STAGE_IDS
    ):
        return normalized_status

    candidate = dict(outcome or {})
    candidate['stage'] = normalized_stage_id
    candidate['status'] = normalized_status
    payloads = _semantic_result_payloads(candidate)
    if _partial_has_blocking_evidence(payloads):
        return normalized_status
    if not _partial_retries_are_terminal(payloads):
        return normalized_status

    unresolved_statuses = {
        'abandoned',
        'blocked',
        'in_progress',
        'interrupted',
        'pending',
        'queued',
        'retry_pending',
        'retrying',
        'running',
        'waiting',
    }
    observed_statuses = {
        str(payload.get(key) or '').strip().lower()
        for payload in payloads
        for key in ('status', 'stage_status', 'run_status', 'retry_status')
        if str(payload.get(key) or '').strip()
    }
    if observed_statuses & unresolved_statuses:
        return normalized_status

    retry_pending_flags = (
        'has_pending_retry',
        'retry_pending',
        'retry_required',
        'retry_scheduled',
    )
    retry_pending_counts = (
        'pending_retry_count',
        'remaining_retry_count',
        'retry_pending_count',
    )
    if any(
        payload.get(key) is True
        for payload in payloads
        for key in retry_pending_flags
    ) or any(
        (_optional_int(payload.get(key)) or 0) > 0
        for payload in payloads
        for key in retry_pending_counts
    ):
        return normalized_status
    if any(
        payload.get('triggered') is False
        and payload.get('safe_skip') is not True
        for payload in payloads
    ):
        return normalized_status
    if any(
        payload.get('terminal') is False
        or payload.get('completed') is False
        for payload in payloads
    ):
        return normalized_status
    return 'partial'


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


def _has_exact_scope_outcome_manifest(
    payloads: tuple[Mapping[str, Any], ...],
) -> bool:
    """Return whether a payload contains an exhaustive identity partition."""

    for payload in payloads:
        proof = payload.get('exact_scope_outcome_proof')
        if not isinstance(proof, Mapping):
            continue
        selected = proof.get('selected_stock_codes')
        retry_debt = proof.get('retry_debt_stock_codes')
        terminal = proof.get('terminal_not_applicable_stock_codes')
        if not all(
            isinstance(value, list)
            for value in (selected, retry_debt, terminal)
        ):
            continue

        def normalize_proof_codes(
            values: list[Any],
        ) -> list[str] | None:
            normalized = [
                (
                    str(value or '').strip().zfill(6)
                    if str(value or '').strip().isdigit()
                    else str(value or '').strip().upper()
                )
                for value in values
            ]
            if (
                any(not value for value in normalized)
                or len(normalized) != len(set(normalized))
                or normalized != values
            ):
                return None
            return normalized

        selected_codes = normalize_proof_codes(selected)
        retry_codes = normalize_proof_codes(retry_debt)
        terminal_codes = normalize_proof_codes(terminal)
        if any(
            value is None
            for value in (selected_codes, retry_codes, terminal_codes)
        ):
            continue
        selected_codes = selected_codes or []
        retry_codes = retry_codes or []
        terminal_codes = terminal_codes or []
        selected_set = set(selected_codes)
        retry_set = set(retry_codes)
        terminal_set = set(terminal_codes)
        successful_codes = [
            code
            for code in selected_codes
            if code not in retry_set | terminal_set
        ]
        if (
            proof.get('schema_version')
            != 'exact_scope_outcome_proof_v1'
            or str(proof.get('manifest_version') or '').strip()
            not in EXACT_SCOPE_OUTCOME_MANIFEST_VERSIONS
            or not selected_codes
            or selected_codes != sorted(selected_codes)
            or retry_set & terminal_set
            or not (retry_set | terminal_set) <= selected_set
            or isinstance(proof.get('selected_symbol_count'), bool)
            or proof.get('selected_symbol_count') != len(selected_codes)
            or proof.get('selected_symbol_sha256')
            != market_membership_sha256(selected_codes)
            or isinstance(proof.get('successful_symbol_count'), bool)
            or proof.get('successful_symbol_count')
            != len(successful_codes)
            or proof.get('successful_symbol_sha256')
            != market_membership_sha256(successful_codes)
        ):
            continue
        return True

    for payload in payloads:
        version = str(
            payload.get('scope_outcome_manifest_version') or ''
        ).strip()
        if version not in EXACT_SCOPE_OUTCOME_MANIFEST_VERSIONS:
            continue
        selected = payload.get('selected_stock_codes')
        successful = payload.get('successful_stock_codes')
        retry_debt = payload.get('retry_debt_stock_codes')
        terminal = payload.get('terminal_not_applicable_stock_codes', [])
        manifest = payload.get('scope_outcome_manifest')
        if not all(
            isinstance(value, list)
            for value in (
                selected,
                successful,
                retry_debt,
                terminal,
                manifest,
            )
        ):
            continue

        def normalize(values: list[Any]) -> list[str] | None:
            normalized = [
                (
                    str(value or '').strip().zfill(6)
                    if str(value or '').strip().isdigit()
                    else str(value or '').strip().upper()
                )
                for value in values
            ]
            if (
                any(not value for value in normalized)
                or len(normalized) != len(set(normalized))
            ):
                return None
            return normalized

        selected_codes = normalize(selected)
        successful_codes = normalize(successful)
        retry_codes = normalize(retry_debt)
        terminal_codes = normalize(terminal)
        if any(
            value is None
            for value in (
                selected_codes,
                successful_codes,
                retry_codes,
                terminal_codes,
            )
        ):
            continue
        selected_set = set(selected_codes or [])
        successful_set = set(successful_codes or [])
        retry_set = set(retry_codes or [])
        terminal_set = set(terminal_codes or [])
        if (
            not selected_set
            or successful_set & retry_set
            or successful_set & terminal_set
            or retry_set & terminal_set
            or selected_set
            != successful_set | retry_set | terminal_set
        ):
            continue
        authoritative_scope_counts = [
            parsed
            for scope_payload in payloads
            for key in (
                'selected_symbol_count',
                'requested_symbol_count',
                'symbols_selected',
                'symbols_total',
                'features_total',
                'universe_count',
                'cohort_count',
            )
            if (parsed := _optional_int(scope_payload.get(key)))
            is not None
            and parsed >= 0
        ]
        if not authoritative_scope_counts:
            for scope_payload in payloads:
                for succeeded_key, failed_key in (
                    ('results_persisted', 'results_failed'),
                    ('features_persisted', 'features_failed'),
                    ('snapshots_persisted', 'snapshots_failed'),
                    ('contexts_persisted', 'contexts_failed'),
                    (
                        'action_summaries_persisted',
                        'action_summaries_failed',
                    ),
                    ('symbols_succeeded', 'symbols_failed'),
                    ('succeeded', 'failed'),
                ):
                    succeeded_count = _optional_int(
                        scope_payload.get(succeeded_key)
                    )
                    failed_count = _optional_int(
                        scope_payload.get(failed_key)
                    )
                    if (
                        succeeded_count is not None
                        and succeeded_count >= 0
                        and failed_count is not None
                        and failed_count >= 0
                    ):
                        authoritative_scope_counts.append(
                            succeeded_count + failed_count
                        )
        if not authoritative_scope_counts:
            authoritative_scope_counts = [
                parsed
                for scope_payload in payloads
                if (parsed := _optional_int(scope_payload.get('total')))
                is not None
                and parsed >= 0
            ]
        if (
            not authoritative_scope_counts
            or any(
                count != len(selected_set)
                for count in authoritative_scope_counts
            )
        ):
            continue
        manifest_outcomes: dict[str, str] = {}
        malformed = False
        for item in manifest:
            if not isinstance(item, Mapping):
                malformed = True
                break
            raw_stock_code = str(item.get('stock_code') or '').strip()
            stock_code = (
                raw_stock_code.zfill(6)
                if raw_stock_code.isdigit()
                else raw_stock_code.upper()
            )
            outcome = str(item.get('outcome') or '').strip()
            if (
                not stock_code
                or stock_code in manifest_outcomes
                or outcome
                not in {
                    'success',
                    'retry_debt',
                    'terminal_not_applicable',
                }
            ):
                malformed = True
                break
            manifest_outcomes[stock_code] = outcome
        expected = {
            **{code: 'success' for code in successful_set},
            **{code: 'retry_debt' for code in retry_set},
            **{code: 'terminal_not_applicable' for code in terminal_set},
        }
        if not malformed and manifest_outcomes == expected:
            return True
    return False


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
    if daily_pipeline_result_has_no_ready_content(outcome):
        return False
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
    high_coverage_stage = bool(
        _stage_ids(payloads) & HIGH_COVERAGE_PARTIAL_STAGE_IDS
    )
    exact_scope_proven = _has_exact_scope_outcome_manifest(payloads)
    if gate_statuses == {'ready'}:
        return not high_coverage_stage or exact_scope_proven
    return (
        high_coverage_stage
        and exact_scope_proven
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
    normalized_outcome = dict(outcome)
    status = normalize_daily_pipeline_stage_status(
        stage_id=str(
            outcome.get('stage')
            or outcome.get('stage_id')
            or outcome.get('task')
            or ''
        ),
        status=outcome.get('status'),
        outcome=outcome,
    )
    normalized_outcome['status'] = status
    if status == 'completed':
        payloads = _payloads(normalized_outcome)
        if (
            daily_pipeline_result_has_no_ready_content(normalized_outcome)
            or _partial_has_blocking_evidence(payloads)
            or not _partial_retries_are_terminal(payloads)
            or _has_positive_failure_evidence(payloads)
        ):
            return False
        nested_stages_satisfied = _nested_stages_are_satisfied(
            normalized_outcome
        )
        return (
            True
            if nested_stages_satisfied is None
            else nested_stages_satisfied
        )
    if status == 'partial':
        return _partial_outcome_is_satisfied(normalized_outcome)
    if status != 'skipped':
        return False
    payloads = _payloads(normalized_outcome)
    if _optional_context_degradation_is_terminal(
        status='skipped',
        payloads=payloads,
    ):
        return True
    reasons = _skip_reasons(normalized_outcome)
    return bool(reasons) and reasons <= SATISFIED_DEPENDENCY_SKIP_REASONS


def daily_pipeline_nonblocking_warning_stage_ids(
    stage_outcomes: Mapping[str, Mapping[str, Any] | str],
) -> tuple[str, ...]:
    """Return optional terminal partials that must not fail the parent run.

    The stage remains visibly ``partial`` for diagnostics. Only ordering-only
    context stages with satisfied terminal evidence are downgraded to parent
    run warnings; active, blocked, failed, or retryable work remains fail
    closed.
    """

    warning_stage_ids: list[str] = []
    for raw_stage_id, raw_outcome in stage_outcomes.items():
        stage_id = str(raw_stage_id or '').strip().lower()
        if stage_id not in ORDERING_ONLY_OPTIONAL_CONTEXT_STAGE_IDS:
            continue
        outcome = (
            dict(raw_outcome)
            if isinstance(raw_outcome, Mapping)
            else {'status': raw_outcome}
        )
        outcome['stage'] = stage_id
        outcome['status'] = normalize_daily_pipeline_stage_status(
            stage_id=stage_id,
            status=outcome.get('status'),
            outcome=outcome,
        )
        if (
            str(outcome.get('status') or '').strip().lower() == 'partial'
            and daily_pipeline_outcome_is_satisfied(outcome)
        ):
            warning_stage_ids.append(stage_id)
    return tuple(warning_stage_ids)


def daily_pipeline_warning_metadata(
    stage_outcomes: Mapping[str, Mapping[str, Any] | str],
) -> dict[str, Any]:
    """Build stable parent/API quality metadata for optional degradation."""

    warning_stage_ids = list(
        daily_pipeline_nonblocking_warning_stage_ids(stage_outcomes)
    )
    return {
        'quality_status': 'degraded' if warning_stage_ids else 'healthy',
        'warning_stage_ids': warning_stage_ids,
        'warning_count': len(warning_stage_ids),
    }


def daily_pipeline_prefix_outcome_is_satisfied(
    outcome: Mapping[str, Any] | None,
) -> bool:
    """Accept a ready enrichment warning only as terminal-prefix evidence.

    Older enrichment rows counted a provider warning as ``failed=1`` even
    when every required downstream category had positive, ready coverage.
    Keep the generic outcome contract strict and admit only that fully
    evidenced legacy shape while resuming a later terminal suffix.
    """

    if daily_pipeline_outcome_is_satisfied(outcome):
        return True
    if not isinstance(outcome, Mapping):
        return False
    payloads = _payloads(outcome)
    if 'pykrx_enrichment' not in _stage_ids(payloads):
        return False
    if (
        str(outcome.get('status') or '').strip().lower() != 'completed'
        or str(outcome.get('error_message') or '').strip()
        or not outcome.get('completed_at')
    ):
        return False

    processed = _optional_int(outcome.get('processed'))
    total = _optional_int(outcome.get('total'))
    succeeded = _optional_int(outcome.get('succeeded'))
    failed = _optional_int(outcome.get('failed'))
    remaining = _optional_int(outcome.get('remaining'))
    if not (
        total is not None
        and total > 0
        and processed == total
        and succeeded is not None
        and failed is not None
        and failed > 0
        and succeeded + failed == total
        and remaining == 0
    ):
        return False

    result_summary = _mapping(
        _mapping(outcome.get('details')).get('result_summary')
    )
    if (
        str(result_summary.get('status') or '').strip().lower()
        != 'completed'
        or result_summary.get('point_in_time_recovery') is not True
    ):
        return False
    requested = result_summary.get('categories_requested')
    categories = result_summary.get('categories')
    if (
        not isinstance(requested, list)
        or not requested
        or not all(
            isinstance(category, str) and category.strip()
            for category in requested
        )
        or len(requested) != len(set(requested))
        or not isinstance(categories, Mapping)
        or set(requested) != set(categories)
        or total != len(requested)
    ):
        return False

    warning_categories: set[str] = set()
    for category in requested:
        details = categories.get(category)
        if not isinstance(details, Mapping):
            return False
        category_status = str(details.get('status') or '').strip().lower()
        rows_persisted = _optional_int(details.get('rows_persisted'))
        if (
            category_status not in {'completed', 'partial'}
            or rows_persisted is None
            or rows_persisted <= 0
        ):
            return False
        errors = details.get('errors')
        if isinstance(errors, (list, tuple)) and errors:
            warning_categories.add(category)
            if category_status != 'partial':
                return False
    if len(warning_categories) != failed:
        return False

    if not pykrx_enrichment_preflight_is_ready(
        _mapping(result_summary.get('downstream_preflight'))
    ):
        return False
    return True
