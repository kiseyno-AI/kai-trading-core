from __future__ import annotations

from typing import Any, Mapping, Sequence

from app.agent_context.run_modes import (
    FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
)


LEGACY_CUTOFF_REPAIR_STAGE_IDS = (
    'signal_decider',
    'next_open_execution_plan',
    'signal_decider_evaluations',
)
SUMMARY_CUTOFF_CONTRACT_REPAIR_VERSION = 1
COMPLETED_SUMMARIZER_INTERSECTION_PROOF_VERSION = (
    'completed_summarizer_intersection_v1'
)


def normalize_symbol_codes(symbols: Sequence[object]) -> list[str]:
    deduped: dict[str, None] = {}
    for symbol in symbols:
        normalized = str(symbol).strip()
        if not normalized:
            continue
        if normalized.isdigit():
            normalized = normalized.zfill(6)
        deduped.setdefault(normalized, None)
    return list(deduped)


def stage_result_summary(stage: object) -> Mapping[str, Any]:
    if not isinstance(stage, Mapping):
        return {}
    details = stage.get('details')
    if isinstance(details, Mapping):
        result_summary = details.get('result_summary')
        if isinstance(result_summary, Mapping):
            return result_summary
    result_summary = stage.get('result_summary')
    return result_summary if isinstance(result_summary, Mapping) else {}


def summary_cutoff_proof_from_stage(
    stage: object,
) -> dict[str, Any] | None:
    if not isinstance(stage, Mapping):
        return None
    if str(stage.get('status') or '').strip().lower() != 'completed':
        return None

    result_summary = stage_result_summary(stage)
    if str(result_summary.get('status') or '').strip().lower() != 'completed':
        return None
    source_run_ids = (
        stage.get('source_run_ids')
        if isinstance(stage.get('source_run_ids'), Mapping)
        else {}
    )
    source_ids = {
        str(value or '').strip()
        for value in (
            source_run_ids.get('agent_feedback_run_id'),
            source_run_ids.get('run_id'),
            result_summary.get('run_id'),
        )
        if str(value or '').strip()
    }
    if len(source_ids) != 1:
        return None

    feedback_scope_mode = str(
        result_summary.get('feedback_scope_mode') or ''
    ).strip()
    if feedback_scope_mode not in {
        FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
        FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
    }:
        return None
    requested_symbols = result_summary.get('requested_symbols')
    if requested_symbols is None:
        normalized_symbols = None
    else:
        if not isinstance(requested_symbols, list):
            return None
        normalized_symbols = normalize_symbol_codes(requested_symbols)
        if not normalized_symbols or requested_symbols != normalized_symbols:
            return None
    return {
        'source_run_id': next(iter(source_ids)),
        'feedback_scope_mode': feedback_scope_mode,
        'requested_symbols': normalized_symbols,
    }


def summary_scope_matches_parent(
    *,
    feedback_scope_mode: object,
    summary_symbols: object,
    parent_symbols: object,
) -> bool:
    if not isinstance(summary_symbols, (list, tuple)) or not isinstance(
        parent_symbols,
        (list, tuple),
    ):
        return False
    normalized_summary = normalize_symbol_codes(summary_symbols)
    normalized_parent = normalize_symbol_codes(parent_symbols)
    if (
        not normalized_summary
        or not normalized_parent
        or list(summary_symbols) != normalized_summary
        or list(parent_symbols) != normalized_parent
    ):
        return False
    normalized_mode = str(feedback_scope_mode or '').strip()
    if normalized_mode == FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE:
        return (
            len(normalized_summary) == len(normalized_parent)
            and set(normalized_summary) == set(normalized_parent)
        )
    if normalized_mode == FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH:
        return set(normalized_summary).issubset(normalized_parent)
    return False


def completed_summary_scope_matches_parent(
    *,
    feedback_scope_mode: object,
    completed_symbols: object,
    completed_scope_proof: object,
    parent_symbols: object,
) -> bool:
    """Validate an exact completed summarizer intersection.

    This is deliberately separate from ``summary_scope_matches_parent``.
    Raw BROAD scopes must still equal their parent. Only a versioned proof
    produced while resolving one exact completed summary run may narrow that
    admitted BROAD scope to the symbols completed by every summarizer.
    """

    normalized_mode = str(feedback_scope_mode or '').strip()
    if normalized_mode not in {
        FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
        FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
    } or not isinstance(completed_scope_proof, Mapping):
        return False
    if (
        str(completed_scope_proof.get('schema_version') or '').strip()
        != COMPLETED_SUMMARIZER_INTERSECTION_PROOF_VERSION
    ):
        return False
    proof_completed_symbols = completed_scope_proof.get(
        'completed_symbols'
    )
    admitted_symbols = completed_scope_proof.get('admitted_symbols')
    if not all(
        isinstance(symbols, (list, tuple))
        for symbols in (
            completed_symbols,
            proof_completed_symbols,
            admitted_symbols,
        )
    ):
        return False
    normalized_completed = normalize_symbol_codes(completed_symbols)
    normalized_proof_completed = normalize_symbol_codes(
        proof_completed_symbols
    )
    normalized_admitted = normalize_symbol_codes(admitted_symbols)
    if not (
        normalized_completed
        and normalized_admitted
        and list(completed_symbols) == normalized_completed
        and list(proof_completed_symbols) == normalized_proof_completed
        and list(admitted_symbols) == normalized_admitted
        and normalized_completed == normalized_proof_completed
        and set(normalized_completed).issubset(normalized_admitted)
    ):
        return False
    if parent_symbols is None:
        return True
    return summary_scope_matches_parent(
        feedback_scope_mode=normalized_mode,
        summary_symbols=normalized_admitted,
        parent_symbols=parent_symbols,
    )


def is_summary_cutoff_contract_repair_candidate(
    *,
    pipeline_run: Mapping[str, Any],
    summary_cutoff_proof: Mapping[str, Any] | None,
    price_retry_symbols: Sequence[str] | set[str],
) -> bool:
    """Recognize only zero-work rows produced by the retired cutoff bug."""

    if (
        price_retry_symbols
        or not isinstance(summary_cutoff_proof, Mapping)
        or not str(summary_cutoff_proof.get('source_run_id') or '').strip()
        or str(
            summary_cutoff_proof.get('feedback_scope_mode') or ''
        ).strip()
        != FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
        or str(pipeline_run.get('trigger') or '').strip()
        != 'auto_eod_followup_resume'
        or str(pipeline_run.get('status') or '').strip().lower()
        != 'blocked'
    ):
        return False

    proof_symbols = summary_cutoff_proof.get('requested_symbols')
    if proof_symbols is not None:
        if not isinstance(proof_symbols, (list, tuple)):
            return False
        if list(proof_symbols) != normalize_symbol_codes(proof_symbols):
            return False

    metadata = pipeline_run.get('run_metadata')
    stages = pipeline_run.get('stages')
    if not isinstance(metadata, Mapping) or not isinstance(stages, Mapping):
        return False
    persisted_cutoff = metadata.get('feedback_knowledge_cutoff')
    if not (
        metadata.get('synthetic_recovery_parent') is not True
        and str(metadata.get('resume_from_stage') or '').strip()
        == 'signal_decider'
        and str(pipeline_run.get('current_stage') or '').strip()
        == 'signal_decider_evaluations'
        and isinstance(persisted_cutoff, Mapping)
        and persisted_cutoff.get('ready') is False
        and str(persisted_cutoff.get('reason') or '').strip()
        == 'summary_run_missing'
        and not str(persisted_cutoff.get('source_run_id') or '').strip()
        and str(
            persisted_cutoff.get('feedback_scope_mode') or ''
        ).strip()
        == FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE
    ):
        return False

    expected_stage_ids = LEGACY_CUTOFF_REPAIR_STAGE_IDS
    resolved_stages = metadata.get('resolved_stages')
    stage_statuses = metadata.get('stage_statuses')
    if not (
        isinstance(resolved_stages, (list, tuple))
        and tuple(resolved_stages) == expected_stage_ids
        and set(stages) == set(expected_stage_ids)
        and isinstance(stage_statuses, Mapping)
        and set(stage_statuses) == set(expected_stage_ids)
    ):
        return False

    def zero_work_block(stage_id: str) -> bool:
        stage = stages.get(stage_id)
        if not isinstance(stage, Mapping):
            return False
        source_run_ids = stage.get('source_run_ids')
        if not isinstance(source_run_ids, Mapping) or source_run_ids:
            return False
        stage_status = str(stage.get('status') or '').strip().lower()
        return bool(
            stage_status == 'blocked'
            and str(stage_statuses.get(stage_id) or '').strip().lower()
            == stage_status
            and all(
                field in stage
                and type(stage[field]) is int
                and stage[field] == 0
                for field in (
                    'processed',
                    'total',
                    'succeeded',
                    'failed',
                    'remaining',
                )
            )
        )

    if not all(zero_work_block(stage_id) for stage_id in expected_stage_ids):
        return False

    decider_summary = stage_result_summary(stages['signal_decider'])
    cutoff_resolution = decider_summary.get('knowledge_cutoff_resolution')
    if not (
        decider_summary.get('triggered') is False
        and str(decider_summary.get('status') or '').strip().lower()
        == 'blocked'
        and str(decider_summary.get('reason') or '').strip()
        == 'summary_knowledge_cutoff_unavailable'
        and isinstance(cutoff_resolution, Mapping)
        and cutoff_resolution.get('ready') is False
        and str(cutoff_resolution.get('reason') or '').strip()
        == 'summary_run_missing'
        and not str(cutoff_resolution.get('source_run_id') or '').strip()
        and str(
            cutoff_resolution.get('feedback_scope_mode') or ''
        ).strip()
        == FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE
    ):
        return False

    plan_summary = stage_result_summary(
        stages['next_open_execution_plan']
    )
    evaluation_summary = stage_result_summary(
        stages['signal_decider_evaluations']
    )
    return bool(
        plan_summary.get('triggered') is False
        and str(plan_summary.get('status') or '').strip().lower()
        == 'blocked'
        and str(plan_summary.get('reason') or '').strip()
        == 'signal_decider_stage_incomplete'
        and str(plan_summary.get('upstream_status') or '').strip().lower()
        == 'blocked'
        and evaluation_summary.get('triggered') is False
        and str(evaluation_summary.get('status') or '').strip().lower()
        == 'blocked'
        and str(evaluation_summary.get('reason') or '').strip()
        == 'declared_dependencies_incomplete'
        and evaluation_summary.get('incomplete_dependencies')
        == ['signal_decider']
    )


def is_resumable_summary_cutoff_contract_repair(
    pipeline_run: Mapping[str, Any],
) -> bool:
    """Recognize an interrupted sixth repair before any durable work."""

    if (
        str(pipeline_run.get('trigger') or '').strip()
        != 'auto_eod_followup_resume'
        or str(pipeline_run.get('status') or '').strip().lower()
        not in {'running', 'interrupted', 'failed'}
    ):
        return False
    metadata = pipeline_run.get('run_metadata')
    stages = pipeline_run.get('stages')
    if not isinstance(metadata, Mapping) or not isinstance(stages, Mapping):
        return False
    resume_count = metadata.get(
        'summary_cutoff_contract_repair_resume_count'
    )
    if not (
        summary_cutoff_contract_repair_marker_is_valid(metadata)
        and (
            resume_count is None
            or type(resume_count) is int
            and resume_count == 0
        )
    ):
        return False
    repair_proof = metadata['summary_cutoff_contract_repair_proof']
    proof_symbols = repair_proof.get('requested_symbols')
    if proof_symbols is not None and (
        not isinstance(proof_symbols, list)
        or not proof_symbols
        or proof_symbols != normalize_symbol_codes(proof_symbols)
    ):
        return False
    resolved_stages = metadata.get('resolved_stages')
    persisted_stage_ids = set(stages)
    valid_stage_prefixes = {
        frozenset(),
        frozenset(LEGACY_CUTOFF_REPAIR_STAGE_IDS[:1]),
    }
    if not (
        isinstance(resolved_stages, (list, tuple))
        and tuple(resolved_stages) == LEGACY_CUTOFF_REPAIR_STAGE_IDS
        and frozenset(persisted_stage_ids) in valid_stage_prefixes
    ):
        return False
    current_stage = str(pipeline_run.get('current_stage') or '').strip()
    expected_current_stage = (
        LEGACY_CUTOFF_REPAIR_STAGE_IDS[len(persisted_stage_ids) - 1]
        if persisted_stage_ids
        else ''
    )
    if current_stage != expected_current_stage:
        return False

    for stage_id, stage in stages.items():
        if not isinstance(stage, Mapping):
            return False
        if str(stage.get('status') or '').strip().lower() not in {
            'pending',
            'running',
            'interrupted',
            'failed',
        }:
            return False
        if not all(
            field in stage and type(stage[field]) is int
            for field in (
                'processed',
                'total',
                'succeeded',
                'failed',
                'remaining',
            )
        ):
            return False
        if not (
            stage['processed'] == 0
            and stage['succeeded'] == 0
            and stage['failed'] == 0
            and stage['total'] >= 0
            and stage['remaining'] == stage['total']
        ):
            return False
        source_run_ids = stage.get('source_run_ids')
        if not isinstance(source_run_ids, Mapping) or source_run_ids:
            return False
        if stage_result_summary(stage):
            return False
    return True


def summary_cutoff_contract_repair_marker_is_valid(
    metadata: object,
) -> bool:
    if not isinstance(metadata, Mapping):
        return False
    repair_version = metadata.get(
        'summary_cutoff_contract_repair_version'
    )
    repair_proof = metadata.get('summary_cutoff_contract_repair_proof')
    resume_count = metadata.get(
        'summary_cutoff_contract_repair_resume_count'
    )
    if not (
        type(repair_version) is int
        and repair_version == SUMMARY_CUTOFF_CONTRACT_REPAIR_VERSION
        and (
            resume_count is None
            or type(resume_count) is int
            and resume_count in {0, 1}
        )
        and isinstance(repair_proof, Mapping)
        and str(repair_proof.get('source_run_id') or '').strip()
        and str(repair_proof.get('feedback_scope_mode') or '').strip()
        == FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
    ):
        return False
    proof_symbols = repair_proof.get('requested_symbols')
    return bool(
        proof_symbols is None
        or isinstance(proof_symbols, list)
        and proof_symbols
        and proof_symbols == normalize_symbol_codes(proof_symbols)
    )
