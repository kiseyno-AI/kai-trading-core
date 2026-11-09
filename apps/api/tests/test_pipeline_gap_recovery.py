from __future__ import annotations

from copy import deepcopy
from contextlib import ExitStack
from unittest.mock import AsyncMock, Mock, patch

import pytest
from sqlalchemy.exc import IntegrityError

from app.core.settings import settings
from app.eod.pykrx_backfill import (
    AUTO_FOLLOWUP_RECOVERY_TRIGGER,
    FOLLOWUP_STAGE_DEBT_SCHEMA_VERSION,
    FOLLOWUP_STAGE_PROOF_PUBLICATION_SCOPE_KEY,
    FOLLOWUP_STAGE_PROOF_RECOVERY_LINEAGE_KEY,
    PykrxBackfillService,
    _advance_followup_stage_debt_state,
    _build_followup_stage_recovery_scope_outcome,
    _count_all_nonconsuming_provider_admission_runs,
    _count_nonconsuming_provider_admission_runs,
    _count_nonconsuming_stage_debt_progress_runs,
    _followup_exact_result_authority,
    _followup_recovery_stage_attempt_finished,
    _followup_stage_debt_terminal_cohort,
    _next_followup_stage_debt_request,
    _recovered_followup_admission_scope_outcome,
    _record_followup_stage_debt_attempt,
    _required_provider_credentials_are_ready,
    _resolve_dart_followup_recovery_scope,
    _resolve_followup_stage_debt_state,
    _validated_followup_exact_partition,
    _validated_followup_stage_debt_state,
    _validated_required_provider_admission,
)
from app.eod.daily_pipeline_stages import (
    DAILY_STAGE_ORDER,
    DEFAULT_DAILY_STAGE_ORDER,
    STAGE_DEFINITIONS,
    DailyPipelineStageService,
)
from app.eod.daily_pipeline_outcomes import (
    daily_pipeline_outcome_is_satisfied,
    daily_pipeline_warning_metadata,
)
from app.eod.pykrx_pipeline_runner import (
    AUTO_DAILY_FOLLOWUP_STAGE_ORDER,
    AUTO_DAILY_TRACKED_STAGE_ORDER,
    FACTOR_FOLLOWUP_STAGE_ORDER,
    _build_daily_fundamentals_refresh_kwargs,
    _extract_result_counts,
    _resolve_auto_daily_tracked_stages,
    _required_provider_blocked_result,
    _run_daily_factor_followup,
    _run_daily_intraday_snapshot_followup,
    _run_daily_piotroski_filter_followup,
    _run_terminalized_daily_intraday_snapshot_followup,
    _terminalize_optional_intraday_snapshot_result,
    _validated_atomic_scope_partition,
    _validated_atomic_successful_cohort,
    _validated_successful_cohort,
)
from app.eod.scope_outcomes import (
    FOLLOWUP_STAGE_RECOVERY_SCOPE_SCHEMA_VERSION,
    build_exact_symbol_scope_outcome,
)
from app.eod.market_membership import market_membership_sha256


SYMBOLS = ['000001', '000002', '000003']


def test_intraday_snapshots_is_an_official_pre_feedback_stage() -> None:
    resolved = _resolve_auto_daily_tracked_stages(
        selected_stages=AUTO_DAILY_TRACKED_STAGE_ORDER,
        chain_fundamentals_followup=True,
    )
    feedback_resolution = DailyPipelineStageService()._resolve_stage_ids(
        ['agent_feedback_summaries']
    )

    assert resolved == list(AUTO_DAILY_TRACKED_STAGE_ORDER)
    assert 'intraday_snapshots' in AUTO_DAILY_FOLLOWUP_STAGE_ORDER
    assert 'intraday_snapshots' in resolved
    assert set(STAGE_DEFINITIONS) == set(DAILY_STAGE_ORDER)
    assert 'intraday_snapshots' in DEFAULT_DAILY_STAGE_ORDER
    assert 'signal_decider_evaluations' in DAILY_STAGE_ORDER
    assert 'signal_decider_evaluations' in STAGE_DEFINITIONS
    assert 'signal_decider_evaluations' not in DEFAULT_DAILY_STAGE_ORDER
    assert 'signal_decider_evaluations' not in AUTO_DAILY_FOLLOWUP_STAGE_ORDER
    assert 'signal_decider_evaluations' not in AUTO_DAILY_TRACKED_STAGE_ORDER
    assert (
        AUTO_DAILY_FOLLOWUP_STAGE_ORDER.index('macro_context_refresh')
        < AUTO_DAILY_FOLLOWUP_STAGE_ORDER.index('intraday_snapshots')
        < AUTO_DAILY_FOLLOWUP_STAGE_ORDER.index(
            'agent_feedback_summaries'
        )
    )
    assert STAGE_DEFINITIONS['intraday_snapshots'].depends_on == ()
    assert 'intraday_snapshots' in STAGE_DEFINITIONS[
        'agent_feedback_summaries'
    ].depends_on
    assert (
        feedback_resolution.index('intraday_snapshots')
        < feedback_resolution.index('agent_feedback_summaries')
    )


def test_intraday_snapshot_stage_routes_as_terminal_optional_context() -> None:
    terminal_result = {
        'triggered': False,
        'status': 'skipped',
        'safe_skip': True,
        'optional_context': True,
        'reason': 'followup_disabled',
        'market_date': '2026-10-30',
    }
    service = DailyPipelineStageService()
    with (
        patch.object(
            settings,
            'kis_intraday_snapshot_enabled',
            True,
        ),
        patch(
            'app.eod.daily_pipeline_stages.'
            '_run_terminalized_daily_intraday_snapshot_followup',
            return_value=terminal_result,
        ) as collect_intraday,
    ):
        result = service._execute_stage(
            'intraday_snapshots',
            market_date='2026-10-30',
            symbols=None,
            markets=['KOSPI', 'KOSDAQ', 'KONEX'],
            force=False,
            bypass_prerequisite_gate=False,
            fs_div='CFS',
            symbol_limit=None,
            symbol_offset=0,
            capture_limit=500,
            retry_cycles=None,
            feedback_max_cycles=1,
            feedback_outer_batch_size=1,
            reuse_existing_feedback=True,
        )

    assert result is terminal_result
    collect_intraday.assert_called_once_with(
        market_date='2026-10-30',
        enabled=True,
    )
    outcome = service._build_stage_outcome(
        'intraday_snapshots',
        result,
    )
    assert outcome['status'] == 'completed'
    assert daily_pipeline_outcome_is_satisfied(outcome)


def _exact_result(
    *,
    selected: list[str],
    successful: list[str],
    retry_debt: list[str],
    version: str,
    status: str | None = None,
    stage_id: str | None = None,
    market_date: str = '2026-10-30',
    publication_symbols: list[str] | None = None,
    terminal_not_applicable: list[str] | None = None,
) -> dict:
    resolved_stage_id = stage_id or {
        'piotroski_scope_outcomes_v1': 'piotroski_filter',
        'signal_features_scope_outcomes_v1': 'signal_features',
        'factor_scope_outcomes_v1': 'momentum_factors',
    }.get(version, 'fundamentals_daily_refresh')
    exact_symbols = list(publication_symbols or selected)
    return {
        'status': status or ('partial' if retry_debt else 'completed'),
        'market_date': market_date,
        'scope_finalization': {
            'stage': resolved_stage_id,
            'market_date': market_date,
            'exact_symbol_count': len(exact_symbols),
            'exact_membership_sha256': market_membership_sha256(
                exact_symbols
            ),
            'successful_symbol_count': len(successful),
            'successful_membership_sha256': market_membership_sha256(
                successful
            ),
            'deleted_stale_row_count': 0,
            'ready': True,
        },
        **build_exact_symbol_scope_outcome(
            manifest_version=version,
            selected_symbols=selected,
            successful_symbols=successful,
            retry_debt_symbols=retry_debt,
            terminal_not_applicable_symbols=(
                terminal_not_applicable or []
            ),
        ),
    }


def _stored_stage_proof(
    result: dict,
    *,
    publication_symbols: list[str],
) -> dict:
    return {
        **result,
        FOLLOWUP_STAGE_PROOF_PUBLICATION_SCOPE_KEY: list(
            publication_symbols
        ),
    }


def _compact_quality_only_piotroski_stage(
    symbols: list[str],
) -> dict:
    result = _exact_result(
        selected=symbols,
        successful=symbols,
        retry_debt=[],
        version='piotroski_scope_outcomes_v1',
        status='partial',
        stage_id='piotroski_filter',
        publication_symbols=symbols,
    )
    result.update(
        {
            'gate_status': 'ready',
            'blocking_status': 'non_blocking',
            'ranking_dependency_status': 'complete',
            'reason': 'piotroski_evidence_incomplete',
            'quality_warning': 'piotroski_evidence_incomplete',
            'full_signal_coverage_status': 'incomplete',
            'full_signal_usable_count': len(symbols) - 2,
            'full_signal_incomplete_count': 1,
            'full_signal_unusable_count': 1,
            'results_persisted': len(symbols),
            'results_failed': 0,
        }
    )
    details = DailyPipelineStageService._build_stage_run_details(
        stage_id='piotroski_filter',
        result=result,
        invocation_symbols=symbols,
        market_date='2026-10-30',
        publication_symbols=symbols,
    )
    return {'status': 'partial', 'details': details}


def _quality_only_signal_features_result(symbols: list[str]) -> dict:
    result = _exact_result(
        selected=symbols,
        successful=symbols,
        retry_debt=[],
        version='signal_features_scope_outcomes_v1',
        status='partial',
        stage_id='signal_features',
        publication_symbols=symbols,
    )
    result.update(
        {
            'reason': 'signal_feature_sources_incomplete',
            'source_completeness_status': 'partial',
            'incomplete_sources': [
                'overtime-fluctuation',
                'overtime-volume',
                'volume-power',
            ],
            'quality_status': 'degraded',
            'complete_stock_codes': [],
            'degraded_stock_codes': list(symbols),
            'complete_stock_code_count': 0,
            'degraded_stock_code_count': len(symbols),
            'semantic_ready_count': len(symbols),
            'semantic_retry_count': 0,
            'features_persisted': len(symbols),
            'features_failed': 0,
            'failed_symbols': [],
        }
    )
    return result


def _compact_quality_only_signal_features_stage(
    symbols: list[str],
) -> dict:
    result = _quality_only_signal_features_result(symbols)
    details = DailyPipelineStageService._build_stage_run_details(
        stage_id='signal_features',
        result=result,
        invocation_symbols=symbols,
        market_date='2026-10-30',
        publication_symbols=symbols,
    )
    return {'status': 'partial', 'details': details}


def test_compact_nonblocking_piotroski_warning_is_not_new_debt() -> None:
    symbols = [f'{index:06d}' for index in range(1, 118)]
    stage = _compact_quality_only_piotroski_stage(symbols)

    authority = _followup_exact_result_authority(
        stage_id='piotroski_filter',
        stage=stage,
    )

    assert authority is not None
    assert authority['status'] == 'partial'
    assert authority['selected_stock_codes'] == symbols
    assert authority['successful_stock_codes'] == symbols
    assert authority['retry_debt_stock_codes'] == []
    common_kwargs = {
        'stage_id': 'piotroski_filter',
        'stage': stage,
        'expected_market_date': '2026-10-30',
        'expected_publication_symbols': symbols,
        'allowed_partition_symbols': symbols,
    }
    assert _validated_followup_exact_partition(**common_kwargs) is None
    validated = _validated_followup_exact_partition(
        **common_kwargs,
        allow_quality_only_partial=True,
    )
    assert validated is not None
    assert validated[0]['successful_stock_codes'] == symbols


def test_signal_feature_quality_only_partial_advances_exact_success_scope() -> None:
    symbols = [f'{index:06d}' for index in range(1, 4)]
    result = _quality_only_signal_features_result(symbols)
    stage = _compact_quality_only_signal_features_stage(symbols)

    assert _validated_successful_cohort(
        stage_result=result,
        invocation_symbols=symbols,
        stage_id='signal_features',
    ) == symbols
    assert _validated_atomic_successful_cohort(
        stage_result=result,
        invocation_symbols=symbols,
        stage_id='signal_features',
        finalization_stage_id='signal_features',
        market_date='2026-10-30',
        exact_publication_symbols=symbols,
    ) == symbols
    stage_scope = DailyPipelineStageService._exact_partial_success_scope(
        outcome={'status': 'partial', 'result': result},
        invocation_symbols=symbols,
        expected_stage_id='signal_features',
        expected_market_date='2026-10-30',
        publication_symbols=symbols,
    )
    assert stage_scope is not None
    validated = _validated_followup_exact_partition(
        stage_id='signal_features',
        stage=stage,
        expected_market_date='2026-10-30',
        expected_publication_symbols=symbols,
        allowed_partition_symbols=symbols,
        allow_quality_only_partial=True,
    )
    assert validated is not None
    assert validated[0]['successful_stock_codes'] == symbols


def test_failed_signal_feature_wrapper_stays_fail_closed() -> None:
    symbols = [f'{index:06d}' for index in range(1, 4)]
    result = _quality_only_signal_features_result(symbols)
    compact_stage = _compact_quality_only_signal_features_stage(symbols)
    stage = {
        'status': 'failed',
        'result': result,
        'details': compact_stage['details'],
    }

    assert _validated_followup_exact_partition(
        stage_id='signal_features',
        stage=stage,
        expected_market_date='2026-10-30',
        expected_publication_symbols=symbols,
        allowed_partition_symbols=symbols,
        allow_quality_only_partial=True,
    ) is None


def test_compact_signal_feature_quality_partial_accepts_omitted_large_quality_list() -> None:
    symbols = [f'{index:06d}' for index in range(1, 118)]
    stage = _compact_quality_only_signal_features_stage(symbols)
    result_summary = stage['details']['result_summary']

    assert result_summary['complete_stock_codes'] == []
    assert 'degraded_stock_codes' not in result_summary
    validated = _validated_followup_exact_partition(
        stage_id='signal_features',
        stage=stage,
        expected_market_date='2026-10-30',
        expected_publication_symbols=symbols,
        allowed_partition_symbols=symbols,
        allow_quality_only_partial=True,
    )
    assert validated is not None
    assert validated[0]['successful_stock_codes'] == symbols


@pytest.mark.parametrize(
    ('field', 'value'),
    (
        ('reason', 'unknown_partial'),
        ('source_completeness_status', 'complete'),
        ('incomplete_sources', []),
        ('quality_status', 'unavailable'),
        ('semantic_ready_count', 2),
        ('semantic_retry_count', 1),
        ('features_persisted', 2),
        ('features_failed', 1),
        ('degraded_stock_codes', ['000001', '000002']),
        ('degraded_stock_code_count', 2),
        ('failed_symbols', ['000003']),
        ('gate_status', 'blocked'),
        ('blocking_status', 'blocking'),
        ('ranking_dependency_status', 'incomplete'),
    ),
)
def test_signal_feature_quality_partial_tampering_stays_fail_closed(
    field: str,
    value: object,
) -> None:
    symbols = [f'{index:06d}' for index in range(1, 4)]
    result = _quality_only_signal_features_result(symbols)
    result[field] = value

    assert _validated_successful_cohort(
        stage_result=result,
        invocation_symbols=symbols,
        stage_id='signal_features',
    ) is None
    assert DailyPipelineStageService._exact_partial_success_scope(
        outcome={'status': 'partial', 'result': result},
        invocation_symbols=symbols,
        expected_stage_id='signal_features',
        expected_market_date='2026-10-30',
        publication_symbols=symbols,
    ) is None


def test_quality_partial_uses_top_level_result_summary_market_date() -> None:
    symbols = [f'{index:06d}' for index in range(1, 4)]
    result_summary = _exact_result(
        selected=symbols,
        successful=symbols,
        retry_debt=[],
        version='piotroski_scope_outcomes_v1',
        status='partial',
        stage_id='piotroski_filter',
        publication_symbols=symbols,
    )
    result_summary.update(
        {
            'gate_status': 'ready',
            'blocking_status': 'non_blocking',
            'ranking_dependency_status': 'complete',
            'reason': 'piotroski_evidence_incomplete',
            'quality_warning': 'piotroski_evidence_incomplete',
            'results_persisted': len(symbols),
            'results_failed': 0,
        }
    )

    assert daily_pipeline_outcome_is_satisfied(
        {
            'stage': 'piotroski_filter',
            'status': 'partial',
            'result_summary': result_summary,
        }
    )


@pytest.mark.parametrize(
    ('field', 'value'),
    (
        ('blocking_status', 'blocking'),
        ('quality_warning', 'different_warning'),
        ('gate_status', 'blocked'),
        ('reason', 'unknown_quality_warning'),
        ('full_signal_usable_count', 0),
        ('results_persisted', 117.0),
    ),
)
def test_compact_piotroski_warning_tampering_stays_fail_closed(
    field: str,
    value: object,
) -> None:
    symbols = [f'{index:06d}' for index in range(1, 118)]
    stage = _compact_quality_only_piotroski_stage(symbols)
    stage['details']['result_summary'][field] = value

    assert _followup_exact_result_authority(
        stage_id='piotroski_filter',
        stage=stage,
    ) is None


def test_latest_exact_piotroski_proof_wins_over_stale_eod_proof() -> None:
    fundamentals = _exact_result(
        selected=SYMBOLS,
        successful=SYMBOLS,
        retry_debt=[],
        version='dart_fundamentals_scope_outcomes_v1',
        status='completed',
        stage_id='fundamentals_daily_refresh',
        publication_symbols=SYMBOLS,
    )
    current_piotroski = _compact_quality_only_piotroski_stage(SYMBOLS)
    stale_piotroski = _exact_result(
        selected=SYMBOLS[:2],
        successful=SYMBOLS[:2],
        retry_debt=[],
        version='piotroski_scope_outcomes_v1',
        status='completed',
        stage_id='piotroski_filter',
        publication_symbols=SYMBOLS[:2],
    )

    resolution = _resolve_followup_stage_debt_state(
        market_date='2026-10-30',
        eod_collection_run_id=77,
        latest_eod_metadata={
            'followup_admission_scope_outcome': _parent_admission_outcome(),
            'stage_progress': {
                'piotroski_filter': {
                    'status': 'completed',
                    'result': stale_piotroski,
                }
            },
        },
        stage_rows={
            'fundamentals_daily_refresh': {
                'status': 'completed',
                'result': fundamentals,
            },
            'piotroski_filter': current_piotroski,
        },
        incomplete_stages=['trending_ranks'],
    )

    assert resolution == {'required': False, 'ready': False}

    legacy_dependency_resolution = _resolve_followup_stage_debt_state(
        market_date='2026-10-30',
        eod_collection_run_id=77,
        latest_eod_metadata={
            'followup_admission_scope_outcome': _parent_admission_outcome(),
        },
        stage_rows={'piotroski_filter': current_piotroski},
        incomplete_stages=['trending_ranks'],
    )
    assert legacy_dependency_resolution == {
        'required': False,
        'ready': False,
    }


def test_compact_factor_quality_counts_can_remain_nested() -> None:
    symbols = [f'{index:06d}' for index in range(1, 118)]
    result = _exact_result(
        selected=symbols,
        successful=symbols,
        retry_debt=[],
        version='factor_scope_outcomes_v1',
        status='partial',
        stage_id='value_factors',
        publication_symbols=symbols,
    )
    result.update(
        {
            'gate_status': 'ready',
            'blocking_status': 'non_blocking',
            'ranking_dependency_status': 'complete',
            'reason': 'partial_usable_coverage',
            'quality_warning': 'unusable_factor_cohort',
            'quality_status': 'degraded',
            'remaining_retryable_events': 0,
            'results_persisted': len(symbols),
            'results_failed': 0,
            'summary': {
                'persisted_ready_count': len(symbols) - 2,
                'persisted_unready_count': 2,
            },
        }
    )
    details = DailyPipelineStageService._build_stage_run_details(
        stage_id='value_factors',
        result=result,
        invocation_symbols=symbols,
        market_date='2026-10-30',
        publication_symbols=symbols,
    )

    authority = _followup_exact_result_authority(
        stage_id='value_factors',
        stage={'status': 'partial', 'details': details},
    )

    assert authority is not None
    assert authority['status'] == 'partial'
    assert authority['successful_stock_codes'] == symbols


def _linked_full_admission_recovery_run() -> dict:
    manifests = {
        'pykrx_enrichment': 'pykrx_enrichment_scope_outcomes_v1',
        'technical_snapshots': 'technical_snapshots_scope_outcomes_v1',
        'action_summaries': 'action_summaries_scope_outcomes_v1',
    }
    stages = {
        stage_id: {
            'status': 'completed',
            'result': _exact_result(
                selected=SYMBOLS,
                successful=SYMBOLS,
                retry_debt=[],
                version=manifest_version,
                status='completed',
                stage_id=stage_id,
                publication_symbols=SYMBOLS,
            ),
        }
        for stage_id, manifest_version in manifests.items()
    }
    return {
        'id': 603,
        'market_date': '2026-10-30',
        'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
        'status': 'partial',
        'run_metadata': {
            'entrypoint': 'daily_pipeline_stage_service.run',
            'reused_eod_collection_run_id': 77,
            'point_in_time_recovery': True,
            'resume_from_stage': 'pykrx_enrichment',
            'resolved_stages': list(manifests),
            'symbols': list(SYMBOLS),
            'exact_membership_symbol_count': len(SYMBOLS),
            'exact_membership_sha256': market_membership_sha256(SYMBOLS),
        },
        'stages': stages,
    }


def test_linked_full_recovery_reconciles_stale_admission_only() -> None:
    prior_admission = build_exact_symbol_scope_outcome(
        manifest_version='auto_eod_followup_admission_scope_outcomes_v1',
        selected_symbols=SYMBOLS,
        successful_symbols=SYMBOLS[:2],
        retry_debt_symbols=SYMBOLS[2:],
    )
    latest_eod_run = {
        'id': 77,
        'symbols_total': len(SYMBOLS),
        'symbols_succeeded': len(SYMBOLS),
        'symbols_failed': 0,
        'run_metadata': {
            'requested_symbols': list(SYMBOLS),
            'failed_symbols': [],
            'unattempted_symbols': [],
            'followup_admission_scope_outcome': prior_admission,
        },
    }
    pipeline_run = _linked_full_admission_recovery_run()

    with patch(
        'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
        return_value=SYMBOLS,
    ):
        recovered = _recovered_followup_admission_scope_outcome(
            latest_eod_run=latest_eod_run,
            market_date='2026-10-30',
            pipeline_run=pipeline_run,
        )

    assert recovered is not None
    assert recovered['selected_stock_codes'] == SYMBOLS
    assert recovered['successful_stock_codes'] == SYMBOLS
    assert recovered['retry_debt_stock_codes'] == []

    wrong_hash = deepcopy(pipeline_run)
    wrong_hash['run_metadata']['exact_membership_sha256'] = '0' * 64
    with patch(
        'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
        return_value=SYMBOLS,
    ):
        assert _recovered_followup_admission_scope_outcome(
            latest_eod_run=latest_eod_run,
            market_date='2026-10-30',
            pipeline_run=wrong_hash,
        ) is None

    retrying_stage = deepcopy(pipeline_run)
    retrying_stage['stages']['action_summaries']['result'] = _exact_result(
        selected=SYMBOLS,
        successful=SYMBOLS[:2],
        retry_debt=SYMBOLS[2:],
        version='action_summaries_scope_outcomes_v1',
        status='partial',
        stage_id='action_summaries',
        publication_symbols=SYMBOLS,
    )
    retrying_stage['stages']['action_summaries']['status'] = 'partial'
    with patch(
        'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
        return_value=SYMBOLS,
    ):
        assert _recovered_followup_admission_scope_outcome(
            latest_eod_run=latest_eod_run,
            market_date='2026-10-30',
            pipeline_run=retrying_stage,
        ) is None

    wrong_manifest = deepcopy(pipeline_run)
    wrong_manifest['stages']['technical_snapshots']['result'][
        'scope_outcome_manifest_version'
    ] = 'action_summaries_scope_outcomes_v1'
    wrong_finalization = deepcopy(pipeline_run)
    wrong_finalization['stages']['action_summaries']['result'][
        'scope_finalization'
    ]['stage'] = 'technical_snapshots'
    bool_lineage = deepcopy(pipeline_run)
    bool_lineage['run_metadata']['reused_eod_collection_run_id'] = True
    for invalid_run in (
        wrong_manifest,
        wrong_finalization,
        bool_lineage,
    ):
        with patch(
            'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
            return_value=SYMBOLS,
        ):
            assert _recovered_followup_admission_scope_outcome(
                latest_eod_run=latest_eod_run,
                market_date='2026-10-30',
                pipeline_run=invalid_run,
            ) is None


def test_price_collection_requires_its_dedicated_scope_manifest() -> None:
    proof = {
        'status': 'completed',
        **build_exact_symbol_scope_outcome(
            manifest_version='price_contexts_scope_outcomes_v1',
            selected_symbols=SYMBOLS,
            successful_symbols=SYMBOLS,
            retry_debt_symbols=[],
        ),
    }

    assert _validated_successful_cohort(
        stage_result=proof,
        invocation_symbols=SYMBOLS,
        stage_id='price_collection',
    ) == SYMBOLS
    wrong_manifest = dict(proof)
    wrong_manifest['scope_outcome_manifest_version'] = (
        'pykrx_enrichment_scope_outcomes_v1'
    )
    assert _validated_successful_cohort(
        stage_result=wrong_manifest,
        invocation_symbols=SYMBOLS,
        stage_id='price_collection',
    ) is None


def _provider_admission(
    *,
    provider: str = 'dart',
    missing_credentials: list[str] | None = None,
) -> dict:
    return {
        'schema_version': 'required_provider_admission_v1',
        'provider': provider,
        'required': True,
        'required_stage_ids': (
            ['fundamentals_daily_refresh']
            if provider == 'dart'
            else ['market_risk_filter']
        ),
        'ready': False,
        'status': 'blocked_missing_credential',
        'classification': 'blocked_missing_credential',
        'attempt_started': False,
        'consumes_retry_budget': False,
        'recoverable': True,
        'missing_credentials': missing_credentials or ['DART_API_KEY'],
        'recovery': {
            'action': 'configure_credentials',
            'recheck_required': True,
        },
    }


def _provider_blocked_stage() -> dict:
    return {
        'status': 'blocked',
        'details': {
            'result_summary': {
                'triggered': False,
                'status': 'blocked',
                'classification': 'blocked_missing_credential',
                'stage': 'fundamentals_daily_refresh',
                'provider': 'dart',
                'attempt_started': False,
                'consumes_retry_budget': False,
                'provider_admission': _provider_admission(),
            }
        },
    }


def _parent_admission_outcome() -> dict:
    return build_exact_symbol_scope_outcome(
        manifest_version='auto_eod_followup_admission_scope_outcomes_v1',
        selected_symbols=SYMBOLS,
        successful_symbols=SYMBOLS,
        retry_debt_symbols=[],
    )


def _completed_stage_debt_prerequisites(
    target_stage_id: str,
) -> tuple[dict[str, list[str]], dict[str, list[dict]]]:
    stage_order = [
        'fundamentals_daily_refresh',
        'piotroski_filter',
        *FACTOR_FOLLOWUP_STAGE_ORDER,
        'signal_features',
    ]
    versions = {
        'fundamentals_daily_refresh': (
            'dart_fundamentals_scope_outcomes_v1'
        ),
        'piotroski_filter': 'piotroski_scope_outcomes_v1',
        **{
            stage_id: 'factor_scope_outcomes_v1'
            for stage_id in FACTOR_FOLLOWUP_STAGE_ORDER
        },
        'signal_features': 'signal_features_scope_outcomes_v1',
    }
    successful: dict[str, list[str]] = {}
    proofs: dict[str, list[dict]] = {}
    for stage_id in stage_order:
        if stage_id == target_stage_id:
            break
        result = _exact_result(
            selected=SYMBOLS,
            successful=SYMBOLS,
            retry_debt=[],
            version=versions[stage_id],
            status='completed',
            stage_id=stage_id,
            publication_symbols=SYMBOLS,
        )
        successful[stage_id] = list(SYMBOLS)
        proofs[stage_id] = [
            _stored_stage_proof(
                result,
                publication_symbols=SYMBOLS,
            )
        ]
    return successful, proofs


def _piotroski_attempt_only_stage_debt_state(
    *,
    pipeline_run_id: int = 607,
) -> dict:
    successful, proofs = _completed_stage_debt_prerequisites(
        'piotroski_filter'
    )
    state = {
        'schema_version': FOLLOWUP_STAGE_DEBT_SCHEMA_VERSION,
        'market_date': '2026-10-30',
        'eod_collection_run_id': 77,
        'pending_by_stage': {'piotroski_filter': list(SYMBOLS)},
        'successful_by_stage': successful,
        'terminal_by_stage': {},
        'proofs_by_stage': proofs,
        'recovery_progress_count': 0,
        'recovery_attempts_by_stage': {},
        'applied_recovery_pipeline_run_ids': [],
    }
    recorded = _record_followup_stage_debt_attempt(
        state,
        stage_id='piotroski_filter',
        invocation_symbols=SYMBOLS,
        recovery_pipeline_run_id=pipeline_run_id,
    )
    assert recorded is not None
    return recorded


def _resolve_piotroski_recovery(
    state: dict,
    *,
    stage: dict | None = None,
    pipeline_run_id: int = 607,
    eod_collection_run_id: int = 77,
) -> dict:
    return _resolve_followup_stage_debt_state(
        market_date='2026-10-30',
        eod_collection_run_id=77,
        latest_eod_metadata={
            'followup_admission_scope_outcome': (
                _parent_admission_outcome()
            ),
            'followup_stage_debt': state,
        },
        stage_rows={
            'piotroski_filter': (
                stage or _compact_quality_only_piotroski_stage(SYMBOLS)
            )
        },
        incomplete_stages=['piotroski_filter'],
        latest_pipeline_run={
            'id': pipeline_run_id,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'run_metadata': {
                'reused_eod_collection_run_id': eod_collection_run_id,
                'resolved_stages': ['piotroski_filter'],
            },
        },
    )


def test_quality_only_piotroski_recovery_advances_stage_debt() -> None:
    successful, proofs = _completed_stage_debt_prerequisites(
        'piotroski_filter'
    )
    state = {
        'schema_version': FOLLOWUP_STAGE_DEBT_SCHEMA_VERSION,
        'market_date': '2026-10-30',
        'eod_collection_run_id': 77,
        'pending_by_stage': {'piotroski_filter': list(SYMBOLS)},
        'successful_by_stage': successful,
        'terminal_by_stage': {},
        'proofs_by_stage': proofs,
        'recovery_progress_count': 0,
        'recovery_attempts_by_stage': {},
        'applied_recovery_pipeline_run_ids': [],
    }

    advanced = _advance_followup_stage_debt_state(
        state,
        stage_id='piotroski_filter',
        invocation_symbols=SYMBOLS,
        stage_result=_compact_quality_only_piotroski_stage(SYMBOLS),
        recovery_pipeline_run_id=607,
    )

    assert advanced is not None
    assert advanced['successful_by_stage']['piotroski_filter'] == SYMBOLS
    assert 'piotroski_filter' not in advanced['pending_by_stage']
    assert advanced['recovery_attempts_by_stage'] == {
        'piotroski_filter': 1
    }
    assert advanced['applied_recovery_pipeline_run_ids'] == [607]
    assert advanced['recovery_progress_count'] == 1
    assert _validated_followup_stage_debt_state(
        advanced,
        market_date='2026-10-30',
        eod_collection_run_id=77,
        parent_symbols=SYMBOLS,
        publication_symbols=SYMBOLS,
    ) is not None


def test_attempt_only_quality_partial_is_promoted_without_double_count() -> None:
    attempt_only = _piotroski_attempt_only_stage_debt_state()

    first = _resolve_piotroski_recovery(attempt_only)

    assert first['ready'] is True
    assert first['source'] == 'persisted_stage_debt_reconciled'
    promoted = first['state']
    assert promoted['successful_by_stage']['piotroski_filter'] == SYMBOLS
    assert promoted['recovery_attempts_by_stage'] == {
        'piotroski_filter': 1
    }
    assert promoted['applied_recovery_pipeline_run_ids'] == [607]
    assert promoted['recovery_progress_count'] == 1
    proof = promoted['proofs_by_stage']['piotroski_filter'][0]
    assert proof[FOLLOWUP_STAGE_PROOF_RECOVERY_LINEAGE_KEY][
        'pipeline_run_id'
    ] == 607

    second = _resolve_piotroski_recovery(promoted)

    assert second['ready'] is True
    assert second['source'] == 'persisted_stage_debt'
    assert second['state'] == promoted


def test_attempt_only_quality_partial_reconciles_behind_earlier_debt() -> None:
    residual_symbol = SYMBOLS[0]
    piotroski_symbols = SYMBOLS[1:]
    fundamentals_result = _exact_result(
        selected=SYMBOLS,
        successful=piotroski_symbols,
        retry_debt=[residual_symbol],
        version='dart_fundamentals_scope_outcomes_v1',
        status='partial',
        stage_id='fundamentals_daily_refresh',
        publication_symbols=SYMBOLS,
    )
    state = {
        'schema_version': FOLLOWUP_STAGE_DEBT_SCHEMA_VERSION,
        'market_date': '2026-10-30',
        'eod_collection_run_id': 77,
        'pending_by_stage': {
            'fundamentals_daily_refresh': [residual_symbol],
            'piotroski_filter': list(piotroski_symbols),
        },
        'successful_by_stage': {
            'fundamentals_daily_refresh': list(piotroski_symbols),
        },
        'terminal_by_stage': {},
        'proofs_by_stage': {
            'fundamentals_daily_refresh': [
                _stored_stage_proof(
                    fundamentals_result,
                    publication_symbols=SYMBOLS,
                )
            ]
        },
        'recovery_progress_count': 0,
        'recovery_attempts_by_stage': {},
        'applied_recovery_pipeline_run_ids': [],
    }
    attempt_only = _record_followup_stage_debt_attempt(
        state,
        stage_id='piotroski_filter',
        invocation_symbols=piotroski_symbols,
        recovery_pipeline_run_id=607,
    )
    assert attempt_only is not None

    piotroski_stage = _compact_quality_only_piotroski_stage(
        piotroski_symbols
    )
    piotroski_summary = piotroski_stage['details']['result_summary']
    piotroski_summary['full_signal_usable_count'] = 1
    piotroski_summary['full_signal_incomplete_count'] = 1
    piotroski_summary['full_signal_unusable_count'] = 0

    resolved = _resolve_followup_stage_debt_state(
        market_date='2026-10-30',
        eod_collection_run_id=77,
        latest_eod_metadata={
            'followup_admission_scope_outcome': (
                _parent_admission_outcome()
            ),
            'followup_stage_debt': attempt_only,
        },
        stage_rows={'piotroski_filter': piotroski_stage},
        incomplete_stages=[
            'fundamentals_daily_refresh',
            'piotroski_filter',
        ],
        latest_pipeline_run={
            'id': 607,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'run_metadata': {
                'reused_eod_collection_run_id': 77,
                'resolved_stages': ['piotroski_filter'],
            },
        },
    )

    assert resolved['ready'] is True
    assert resolved['source'] == 'persisted_stage_debt_reconciled'
    promoted = resolved['state']
    assert promoted['pending_by_stage'][
        'fundamentals_daily_refresh'
    ] == [residual_symbol]
    assert promoted['successful_by_stage']['piotroski_filter'] == (
        piotroski_symbols
    )
    assert promoted['recovery_attempts_by_stage'] == {
        'piotroski_filter': 1
    }
    assert promoted['applied_recovery_pipeline_run_ids'] == [607]
    assert promoted['recovery_progress_count'] == 1
    assert _next_followup_stage_debt_request(promoted) == (
        'fundamentals_daily_refresh',
        [residual_symbol],
    )


def test_run607_shape_advances_existing_factor_success_cohort() -> None:
    parent_symbols = ['000001', '000002', '000003', '000004']
    residual_symbol = parent_symbols[0]
    proven_symbol = parent_symbols[1]
    piotroski_retry_symbols = parent_symbols[2:]
    fundamentals_success = parent_symbols[1:]

    fundamentals_result = _exact_result(
        selected=parent_symbols,
        successful=fundamentals_success,
        retry_debt=[residual_symbol],
        version='dart_fundamentals_scope_outcomes_v1',
        status='partial',
        stage_id='fundamentals_daily_refresh',
        publication_symbols=parent_symbols,
    )
    fundamentals_proof = _stored_stage_proof(
        fundamentals_result,
        publication_symbols=parent_symbols,
    )
    fundamentals_proof[
        FOLLOWUP_STAGE_PROOF_RECOVERY_LINEAGE_KEY
    ] = {
        'schema_version': 'followup_recovery_lineage_v1',
        'pipeline_run_id': 606,
        'eod_collection_run_id': 77,
        'stage_id': 'fundamentals_daily_refresh',
    }
    piotroski_result = _exact_result(
        selected=fundamentals_success,
        successful=[proven_symbol],
        retry_debt=piotroski_retry_symbols,
        version='piotroski_scope_outcomes_v1',
        status='partial',
        stage_id='piotroski_filter',
        publication_symbols=parent_symbols,
    )
    signal_result = _exact_result(
        selected=[proven_symbol],
        successful=[proven_symbol],
        retry_debt=[],
        version='signal_features_scope_outcomes_v1',
        status='completed',
        publication_symbols=parent_symbols,
    )
    state = {
        'schema_version': FOLLOWUP_STAGE_DEBT_SCHEMA_VERSION,
        'market_date': '2026-10-30',
        'eod_collection_run_id': 77,
        'pending_by_stage': {
            'fundamentals_daily_refresh': [residual_symbol],
            'piotroski_filter': list(piotroski_retry_symbols),
        },
        'successful_by_stage': {
            'fundamentals_daily_refresh': fundamentals_success,
            'piotroski_filter': [proven_symbol],
            **{
                stage_id: [proven_symbol]
                for stage_id in FACTOR_FOLLOWUP_STAGE_ORDER
            },
            'signal_features': [proven_symbol],
        },
        'terminal_by_stage': {},
        'proofs_by_stage': {
            'fundamentals_daily_refresh': [fundamentals_proof],
            'piotroski_filter': [
                _stored_stage_proof(
                    piotroski_result,
                    publication_symbols=parent_symbols,
                )
            ],
            **{
                stage_id: [
                    _stored_stage_proof(
                        _exact_result(
                            selected=[proven_symbol],
                            successful=[proven_symbol],
                            retry_debt=[],
                            version='factor_scope_outcomes_v1',
                            status='completed',
                            stage_id=stage_id,
                            publication_symbols=parent_symbols,
                        ),
                        publication_symbols=parent_symbols,
                    )
                ]
                for stage_id in FACTOR_FOLLOWUP_STAGE_ORDER
            },
            'signal_features': [
                _stored_stage_proof(
                    signal_result,
                    publication_symbols=parent_symbols,
                )
            ],
        },
        'recovery_progress_count': 1,
        'recovery_attempts_by_stage': {
            'fundamentals_daily_refresh': 1,
            'piotroski_filter': 1,
        },
        'applied_recovery_pipeline_run_ids': [606, 607],
    }
    piotroski_stage = _compact_quality_only_piotroski_stage(
        piotroski_retry_symbols
    )
    piotroski_summary = piotroski_stage['details']['result_summary']
    piotroski_summary['full_signal_usable_count'] = 1
    piotroski_summary['full_signal_incomplete_count'] = 1
    piotroski_summary['full_signal_unusable_count'] = 0

    resolved = _resolve_followup_stage_debt_state(
        market_date='2026-10-30',
        eod_collection_run_id=77,
        latest_eod_metadata={
            'followup_admission_scope_outcome': (
                build_exact_symbol_scope_outcome(
                    manifest_version=(
                        'auto_eod_followup_admission_scope_outcomes_v1'
                    ),
                    selected_symbols=parent_symbols,
                    successful_symbols=parent_symbols,
                    retry_debt_symbols=[],
                )
            ),
            'followup_stage_debt': state,
        },
        stage_rows={'piotroski_filter': piotroski_stage},
        incomplete_stages=[
            'fundamentals_daily_refresh',
            'piotroski_filter',
        ],
        latest_pipeline_run={
            'id': 607,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'run_metadata': {
                'reused_eod_collection_run_id': 77,
                'resolved_stages': ['piotroski_filter'],
            },
        },
    )

    assert resolved['ready'] is True, resolved
    assert resolved['source'] == 'persisted_stage_debt_reconciled'
    promoted = resolved['state']
    assert promoted['pending_by_stage']['momentum_factors'] == (
        piotroski_retry_symbols
    )
    assert _next_followup_stage_debt_request(promoted) == (
        'momentum_factors',
        piotroski_retry_symbols,
    )
    assert _followup_stage_debt_terminal_cohort(
        promoted,
        point_in_time_recovery=True,
        require_prior_attempts=False,
    ) == ('factor_intersection', [proven_symbol])


@pytest.mark.parametrize(
    ('field', 'value'),
    (
        ('reason', 'unknown_partial'),
        ('gate_status', 'blocked'),
        ('ranking_dependency_status', 'incomplete'),
        ('results_persisted', len(SYMBOLS) - 1),
    ),
)
def test_attempt_only_quality_partial_tampering_stays_pending(
    field: str,
    value: object,
) -> None:
    attempt_only = _piotroski_attempt_only_stage_debt_state()
    stage = _compact_quality_only_piotroski_stage(SYMBOLS)
    stage['details']['result_summary'][field] = value

    resolved = _resolve_piotroski_recovery(attempt_only, stage=stage)

    assert resolved['ready'] is True
    assert resolved['state']['pending_by_stage']['piotroski_filter'] == (
        SYMBOLS
    )
    assert resolved['state']['successful_by_stage'].get(
        'piotroski_filter'
    ) is None
    assert resolved['state']['recovery_attempts_by_stage'] == {
        'piotroski_filter': 1
    }
    assert resolved['state']['applied_recovery_pipeline_run_ids'] == [607]
    assert resolved['state']['recovery_progress_count'] == 0


def test_attempt_only_quality_partial_requires_exact_recovery_lineage() -> None:
    attempt_only = _piotroski_attempt_only_stage_debt_state()

    mismatched = _resolve_piotroski_recovery(
        attempt_only,
        eod_collection_run_id=78,
    )

    assert mismatched['ready'] is True
    assert mismatched['source'] == 'persisted_stage_debt'
    assert mismatched['state'] == attempt_only


def test_stage_debt_ledger_accepts_strict_signal_quality_partial_proof() -> None:
    successful, proofs = _completed_stage_debt_prerequisites(
        'signal_features'
    )
    signal_result = _quality_only_signal_features_result(SYMBOLS)
    successful['signal_features'] = list(SYMBOLS)
    proofs['signal_features'] = [
        _stored_stage_proof(
            signal_result,
            publication_symbols=SYMBOLS,
        )
    ]
    state = {
        'schema_version': FOLLOWUP_STAGE_DEBT_SCHEMA_VERSION,
        'market_date': '2026-10-30',
        'eod_collection_run_id': 77,
        'pending_by_stage': {},
        'successful_by_stage': successful,
        'terminal_by_stage': {},
        'proofs_by_stage': proofs,
        'recovery_progress_count': 0,
        'recovery_attempts_by_stage': {},
        'applied_recovery_pipeline_run_ids': [],
    }

    assert _validated_followup_stage_debt_state(
        state,
        market_date='2026-10-30',
        eod_collection_run_id=77,
        parent_symbols=SYMBOLS,
        publication_symbols=SYMBOLS,
        require_pending=False,
    ) is not None

    tampered_reason = deepcopy(state)
    tampered_reason['proofs_by_stage']['signal_features'][0][
        'reason'
    ] = 'unknown_partial'
    assert _validated_followup_stage_debt_state(
        tampered_reason,
        market_date='2026-10-30',
        eod_collection_run_id=77,
        parent_symbols=SYMBOLS,
        publication_symbols=SYMBOLS,
        require_pending=False,
    ) is None

    failed_wrapper = deepcopy(state)
    failed_wrapper['proofs_by_stage']['signal_features'][0][
        'status'
    ] = 'failed'
    assert _validated_followup_stage_debt_state(
        failed_wrapper,
        market_date='2026-10-30',
        eod_collection_run_id=77,
        parent_symbols=SYMBOLS,
        publication_symbols=SYMBOLS,
        require_pending=False,
    ) is None


def test_full_cohort_dart_refresh_is_explicitly_bound_to_frozen_symbols() -> None:
    callback = Mock()

    kwargs = _build_daily_fundamentals_refresh_kwargs(
        market_date='2026-10-30',
        admitted_symbols=['1', '000002', '000002'],
        progress_callback=callback,
    )

    assert kwargs['symbols'] == ['000001', '000002']
    assert kwargs['market_date'] == '2026-10-30'
    assert kwargs['progress_callback'] is callback


def test_piotroski_exact_partial_advances_only_proven_success_subset() -> None:
    partial = {
        **_exact_result(
            selected=['000001', '000002'],
            successful=['000001'],
            retry_debt=['000002'],
            version='piotroski_scope_outcomes_v1',
        ),
        'reason': 'piotroski_evidence_incomplete',
        'gate_status': 'ready',
    }

    assert _validated_successful_cohort(
        stage_result=partial,
        invocation_symbols=['000001', '000002'],
        stage_id='piotroski_filter',
    ) == ['000001']


def test_piotroski_quality_only_partial_advances_on_initial_execution() -> None:
    symbols = [f'{index:06d}' for index in range(1, 118)]
    quality_only = _exact_result(
        selected=symbols,
        successful=symbols,
        retry_debt=[],
        version='piotroski_scope_outcomes_v1',
        status='partial',
        stage_id='piotroski_filter',
        publication_symbols=symbols,
    )
    quality_only.update(
        {
            'gate_status': 'ready',
            'blocking_status': 'non_blocking',
            'ranking_dependency_status': 'complete',
            'reason': 'piotroski_evidence_incomplete',
            'quality_warning': 'piotroski_evidence_incomplete',
            'full_signal_coverage_status': 'incomplete',
            'full_signal_usable_count': len(symbols) - 2,
            'full_signal_incomplete_count': 1,
            'full_signal_unusable_count': 1,
            'results_persisted': len(symbols),
            'results_failed': 0,
        }
    )

    assert _validated_successful_cohort(
        stage_result=quality_only,
        invocation_symbols=symbols,
        stage_id='piotroski_filter',
    ) == symbols
    assert _validated_atomic_successful_cohort(
        stage_result=quality_only,
        invocation_symbols=symbols,
        stage_id='piotroski_filter',
        finalization_stage_id='piotroski_filter',
        market_date='2026-10-30',
        exact_publication_symbols=symbols,
    ) == symbols
    stage_scope = DailyPipelineStageService._exact_partial_success_scope(
        outcome={'status': 'partial', 'result': quality_only},
        invocation_symbols=symbols,
        expected_stage_id='piotroski_filter',
        expected_market_date='2026-10-30',
        publication_symbols=symbols,
    )
    assert stage_scope is not None
    assert stage_scope['manifest_version'] == 'piotroski_scope_outcomes_v1'
    assert stage_scope['selected_stock_codes'] == symbols
    assert stage_scope['successful_stock_codes'] == symbols
    assert stage_scope['retry_debt_stock_codes'] == []
    assert stage_scope['terminal_not_applicable_stock_codes'] == []


def test_partial_piotroski_followup_keeps_full_membership_authority() -> None:
    authority = {'scope': 'frozen-market-membership'}
    result = _exact_result(
        selected=[SYMBOLS[0]],
        successful=[SYMBOLS[0]],
        retry_debt=[],
        version='piotroski_scope_outcomes_v1',
        status='completed',
        stage_id='piotroski_filter',
        publication_symbols=SYMBOLS,
    )

    with patch(
        'app.signals.piotroski_filter.'
        'daily_piotroski_filter_service.rebuild_for_market_date',
        return_value=result,
    ) as rebuild:
        returned = _run_daily_piotroski_filter_followup(
            market_date='2026-10-30',
            exact_membership_symbols=SYMBOLS,
            admitted_symbols=[SYMBOLS[0]],
            authoritative_market_scope=authority,
        )

    assert returned is result
    rebuild.assert_called_once_with(
        market_date='2026-10-30',
        symbols=[SYMBOLS[0]],
        ranking_symbols=SYMBOLS,
        exact_scope_symbols=SYMBOLS,
        authoritative_market_scope=authority,
    )


def test_piotroski_global_or_zero_success_failure_remains_blocked() -> None:
    zero_success = {
        **_exact_result(
            selected=['000001'],
            successful=[],
            retry_debt=['000001'],
            version='piotroski_scope_outcomes_v1',
        ),
        'reason': 'piotroski_evidence_incomplete',
        'gate_status': 'ready',
    }
    global_failure = {
        **_exact_result(
            selected=['000001'],
            successful=['000001'],
            retry_debt=[],
            version='piotroski_scope_outcomes_v1',
        ),
        'reason': 'piotroski_evidence_unavailable',
        'gate_status': 'ready',
    }

    assert _validated_successful_cohort(
        stage_result=zero_success,
        invocation_symbols=['000001'],
        stage_id='piotroski_filter',
    ) is None
    assert _validated_successful_cohort(
        stage_result=global_failure,
        invocation_symbols=['000001'],
        stage_id='piotroski_filter',
    ) is None
    assert _validated_successful_cohort(
        stage_result={
            'status': 'partial',
            'reason': 'piotroski_evidence_unavailable',
        },
        invocation_symbols=['000001'],
        stage_id='piotroski_filter',
    ) is None
    assert _validated_successful_cohort(
        stage_result={'status': 'completed'},
        invocation_symbols=['000001'],
        stage_id='piotroski_filter',
    ) is None
    for stage_id in (*FACTOR_FOLLOWUP_STAGE_ORDER, 'signal_features'):
        assert _validated_successful_cohort(
            stage_result={'status': 'completed'},
            invocation_symbols=['000001'],
            stage_id=stage_id,
        ) is None


def test_failed_all_atomic_proof_is_preserved_but_cannot_advance_child() -> None:
    for stage_id, manifest_version in (
        ('piotroski_filter', 'piotroski_scope_outcomes_v1'),
        ('momentum_factors', 'factor_scope_outcomes_v1'),
    ):
        failed_all = _exact_result(
            selected=SYMBOLS,
            successful=[],
            retry_debt=SYMBOLS,
            version=manifest_version,
            status='failed',
            stage_id=stage_id,
            publication_symbols=SYMBOLS,
        )

        partition = _validated_atomic_scope_partition(
            stage_result=failed_all,
            invocation_symbols=SYMBOLS,
            stage_id=stage_id,
            finalization_stage_id=stage_id,
            market_date='2026-10-30',
            exact_publication_symbols=SYMBOLS,
        )

        assert partition is not None
        assert partition['successful_stock_codes'] == []
        assert partition['retry_debt_stock_codes'] == SYMBOLS
        assert _validated_atomic_successful_cohort(
            stage_result=failed_all,
            invocation_symbols=SYMBOLS,
            stage_id=stage_id,
            finalization_stage_id=stage_id,
            market_date='2026-10-30',
            exact_publication_symbols=SYMBOLS,
        ) is None


def test_successful_cohort_rejects_a_manifest_from_the_wrong_stage() -> None:
    wrong_stage_manifest = _exact_result(
        selected=['000001'],
        successful=['000001'],
        retry_debt=[],
        version='factor_scope_outcomes_v1',
        status='completed',
    )

    assert _validated_successful_cohort(
        stage_result=wrong_stage_manifest,
        invocation_symbols=['000001'],
        stage_id='piotroski_filter',
    ) is None


def test_cross_sectional_retry_separates_ranking_and_publication_scopes() -> None:
    service = DailyPipelineStageService()
    stage_result = _exact_result(
        selected=['000002'],
        successful=['000002'],
        retry_debt=[],
        version='piotroski_scope_outcomes_v1',
        status='completed',
    )
    with patch(
        'app.eod.daily_pipeline_stages.'
        'daily_piotroski_filter_service.rebuild_for_market_date',
        return_value=stage_result,
    ) as rebuild:
        result = service._execute_stage(
            'piotroski_filter',
            market_date='2026-10-30',
            symbols=['000002'],
            exact_scope_symbols=SYMBOLS,
            ranking_scope_symbols=SYMBOLS,
            publication_scope_symbols=['000002'],
            markets=['KOSPI', 'KOSDAQ', 'KONEX'],
            force=False,
            bypass_prerequisite_gate=False,
            fs_div='CFS',
            symbol_limit=None,
            symbol_offset=0,
            capture_limit=500,
            retry_cycles=None,
            feedback_max_cycles=1,
            feedback_outer_batch_size=1,
            reuse_existing_feedback=True,
        )

    assert result is stage_result
    rebuild.assert_called_once_with(
        market_date='2026-10-30',
        symbols=['000002'],
        ranking_symbols=SYMBOLS,
        exact_scope_symbols=['000002'],
    )


def test_stage_debt_scope_proof_preserves_success_debt_and_terminal_sets() -> None:
    proof = _build_followup_stage_recovery_scope_outcome(
        {
            'pending_by_stage': {'piotroski_filter': ['000002']},
            'successful_by_stage': {'piotroski_filter': ['000001']},
            'terminal_by_stage': {'piotroski_filter': ['000003']},
        },
        stage_id='piotroski_filter',
    )

    assert proof is not None
    assert proof['schema_version'] == (
        FOLLOWUP_STAGE_RECOVERY_SCOPE_SCHEMA_VERSION
    )
    assert proof['stage_id'] == 'piotroski_filter'
    assert proof['selected_stock_codes'] == SYMBOLS
    assert proof['successful_stock_codes'] == ['000001']
    assert proof['retry_debt_stock_codes'] == ['000002']
    assert proof['terminal_not_applicable_stock_codes'] == ['000003']


def test_stage_debt_progress_does_not_consume_failure_budget() -> None:
    baseline = _exact_result(
        selected=SYMBOLS,
        successful=['000001'],
        retry_debt=['000002', '000003'],
        version='dart_fundamentals_scope_outcomes_v1',
    )
    progressed = _exact_result(
        selected=['000002', '000003'],
        successful=['000002'],
        retry_debt=['000003'],
        version='dart_fundamentals_scope_outcomes_v1',
    )
    failed_all = _exact_result(
        selected=['000003'],
        successful=[],
        retry_debt=['000003'],
        version='dart_fundamentals_scope_outcomes_v1',
    )

    assert _count_nonconsuming_stage_debt_progress_runs(
        {
            'recovery_progress_count': 1,
            'proofs_by_stage': {
                'fundamentals_daily_refresh': [
                    baseline,
                    progressed,
                    failed_all,
                ]
            }
        }
    ) == 1
    assert _count_nonconsuming_stage_debt_progress_runs(
        {'proofs_by_stage': {'fundamentals_daily_refresh': [progressed]}}
    ) == 0


def test_persisted_stage_debt_progress_requires_applied_run_lineage() -> None:
    baseline = _exact_result(
        selected=SYMBOLS,
        successful=['000001'],
        retry_debt=['000002', '000003'],
        version='dart_fundamentals_scope_outcomes_v1',
    )
    state = {
        'schema_version': FOLLOWUP_STAGE_DEBT_SCHEMA_VERSION,
        'market_date': '2026-10-30',
        'eod_collection_run_id': 77,
        'pending_by_stage': {
            'fundamentals_daily_refresh': ['000002', '000003']
        },
        'successful_by_stage': {
            'fundamentals_daily_refresh': ['000001']
        },
        'terminal_by_stage': {},
        'proofs_by_stage': {
            'fundamentals_daily_refresh': [
                _stored_stage_proof(
                    baseline,
                    publication_symbols=SYMBOLS,
                )
            ]
        },
        'recovery_progress_count': 1,
        'recovery_attempts_by_stage': {},
        'applied_recovery_pipeline_run_ids': [],
    }

    assert _validated_followup_stage_debt_state(
        state,
        market_date='2026-10-30',
        eod_collection_run_id=77,
        parent_symbols=SYMBOLS,
        publication_symbols=SYMBOLS,
    ) is None

    state['recovery_attempts_by_stage'] = {
        'fundamentals_daily_refresh': 1
    }
    state['applied_recovery_pipeline_run_ids'] = [901]
    assert _validated_followup_stage_debt_state(
        state,
        market_date='2026-10-30',
        eod_collection_run_id=77,
        parent_symbols=SYMBOLS,
        publication_symbols=SYMBOLS,
    ) is None


def test_dependency_admitted_stage_first_recovery_counts_as_progress() -> None:
    state = {
        'schema_version': FOLLOWUP_STAGE_DEBT_SCHEMA_VERSION,
        'market_date': '2026-10-30',
        'eod_collection_run_id': 77,
        'pending_by_stage': {'piotroski_filter': ['000002']},
        'successful_by_stage': {'piotroski_filter': ['000001']},
        'terminal_by_stage': {},
        'proofs_by_stage': {},
        'recovery_progress_count': 0,
    }
    recovered = _exact_result(
        selected=['000002'],
        successful=['000002'],
        retry_debt=[],
        version='piotroski_scope_outcomes_v1',
        status='completed',
    )

    advanced = _advance_followup_stage_debt_state(
        state,
        stage_id='piotroski_filter',
        invocation_symbols=['000002'],
        stage_result=recovered,
        recovery_pipeline_run_id=901,
    )

    assert advanced is not None
    assert advanced['recovery_progress_count'] == 1
    assert advanced['proofs_by_stage']['piotroski_filter'][0][
        FOLLOWUP_STAGE_PROOF_RECOVERY_LINEAGE_KEY
    ] == {
        'schema_version': 'followup_recovery_lineage_v1',
        'pipeline_run_id': 901,
        'eod_collection_run_id': 77,
        'stage_id': 'piotroski_filter',
    }
    assert _count_nonconsuming_stage_debt_progress_runs(advanced) == 1


def test_persisted_stage_debt_maps_must_match_latest_exact_proofs() -> None:
    baseline = _exact_result(
        selected=SYMBOLS,
        successful=[SYMBOLS[0]],
        retry_debt=SYMBOLS[1:],
        version='dart_fundamentals_scope_outcomes_v1',
    )
    state = {
        'schema_version': FOLLOWUP_STAGE_DEBT_SCHEMA_VERSION,
        'market_date': '2026-10-30',
        'eod_collection_run_id': 77,
        'pending_by_stage': {
            'fundamentals_daily_refresh': SYMBOLS[1:],
        },
        'successful_by_stage': {
            'fundamentals_daily_refresh': [SYMBOLS[0]],
        },
        'terminal_by_stage': {},
        'proofs_by_stage': {
            'fundamentals_daily_refresh': [
                _stored_stage_proof(
                    baseline,
                    publication_symbols=SYMBOLS,
                )
            ]
        },
        'recovery_progress_count': 0,
        'recovery_attempts_by_stage': {},
        'applied_recovery_pipeline_run_ids': [],
    }

    assert _validated_followup_stage_debt_state(
        state,
        market_date='2026-10-30',
        eod_collection_run_id=77,
        parent_symbols=SYMBOLS,
        publication_symbols=SYMBOLS,
    ) is not None

    proofless_success = deepcopy(state)
    proofless_success['pending_by_stage'] = {}
    proofless_success['successful_by_stage'] = {
        'fundamentals_daily_refresh': SYMBOLS,
    }
    assert _validated_followup_stage_debt_state(
        proofless_success,
        market_date='2026-10-30',
        eod_collection_run_id=77,
        parent_symbols=SYMBOLS,
        publication_symbols=SYMBOLS,
        require_pending=False,
    ) is None

    overlapping = deepcopy(state)
    overlapping['successful_by_stage'][
        'fundamentals_daily_refresh'
    ].append(SYMBOLS[1])
    assert _validated_followup_stage_debt_state(
        overlapping,
        market_date='2026-10-30',
        eod_collection_run_id=77,
        parent_symbols=SYMBOLS,
        publication_symbols=SYMBOLS,
    ) is None


def test_attempt_only_stage_debt_record_rotates_without_accepting_output() -> None:
    state = {
        'pending_by_stage': {
            'momentum_factors': SYMBOLS,
            'value_factors': SYMBOLS,
        },
        'successful_by_stage': {},
        'terminal_by_stage': {},
        'proofs_by_stage': {},
        'recovery_progress_count': 0,
        'recovery_attempts_by_stage': {},
        'applied_recovery_pipeline_run_ids': [],
    }

    recorded = _record_followup_stage_debt_attempt(
        state,
        stage_id='momentum_factors',
        invocation_symbols=SYMBOLS,
        recovery_pipeline_run_id=901,
    )

    assert recorded is not None
    assert recorded['pending_by_stage'] == state['pending_by_stage']
    assert recorded['proofs_by_stage'] == {}
    assert recorded['recovery_progress_count'] == 0
    assert recorded['recovery_attempts_by_stage'] == {
        'momentum_factors': 1,
    }
    assert recorded['applied_recovery_pipeline_run_ids'] == [901]
    assert _next_followup_stage_debt_request(recorded) == (
        'value_factors',
        SYMBOLS,
    )


def test_zero_attempt_provider_block_does_not_rotate_stage_debt() -> None:
    provider_block = _provider_blocked_stage()
    provider_block.update(
        {
            'started_at': '2026-10-30T12:00:00+00:00',
            'completed_at': '2026-10-30T12:00:01+00:00',
        }
    )

    assert not _followup_recovery_stage_attempt_finished(
        provider_block,
        expected_stage_id='fundamentals_daily_refresh',
    )


def test_initial_stage_debt_propagates_upstream_terminal_closure() -> None:
    dart = _exact_result(
        selected=SYMBOLS[:2],
        successful=[SYMBOLS[0]],
        retry_debt=[],
        terminal_not_applicable=[SYMBOLS[1]],
        version='dart_fundamentals_scope_outcomes_v1',
        status='completed',
        publication_symbols=SYMBOLS[:2],
    )
    piotroski = _exact_result(
        selected=[SYMBOLS[0]],
        successful=[SYMBOLS[0]],
        retry_debt=[],
        version='piotroski_scope_outcomes_v1',
        status='completed',
        publication_symbols=SYMBOLS[:2],
    )
    momentum = _exact_result(
        selected=[SYMBOLS[0]],
        successful=[],
        retry_debt=[SYMBOLS[0]],
        version='factor_scope_outcomes_v1',
        status='failed',
        stage_id='momentum_factors',
        publication_symbols=SYMBOLS[:2],
    )
    stage_progress = {
        'fundamentals_daily_refresh': {
            'status': 'completed',
            'details': {'result_summary': dart},
        },
        'piotroski_filter': {
            'status': 'completed',
            'details': {'result_summary': piotroski},
        },
        'momentum_factors': {
            'status': 'failed',
            'details': {'result_summary': momentum},
        },
    }

    resolution = _resolve_followup_stage_debt_state(
        market_date='2026-10-30',
        eod_collection_run_id=77,
        latest_eod_metadata={
            'followup_admission_scope_outcome': (
                build_exact_symbol_scope_outcome(
                    manifest_version=(
                        'auto_eod_followup_admission_scope_outcomes_v1'
                    ),
                    selected_symbols=SYMBOLS[:2],
                    successful_symbols=SYMBOLS[:2],
                    retry_debt_symbols=[],
                )
            ),
            'stage_progress': stage_progress,
        },
        stage_rows={},
        incomplete_stages=['momentum_factors'],
    )

    assert resolution['ready'] is True
    state = resolution['state']
    assert state['terminal_by_stage']['piotroski_filter'] == [
        SYMBOLS[1]
    ]
    assert state['terminal_by_stage']['momentum_factors'] == [
        SYMBOLS[1]
    ]
    assert _validated_followup_stage_debt_state(
        state,
        market_date='2026-10-30',
        eod_collection_run_id=77,
        parent_symbols=SYMBOLS[:2],
        publication_symbols=SYMBOLS[:2],
    ) is not None


def test_upstream_recovery_propagates_existing_terminal_to_new_child() -> None:
    baseline = _exact_result(
        selected=SYMBOLS[:2],
        successful=[],
        retry_debt=[SYMBOLS[0]],
        terminal_not_applicable=[SYMBOLS[1]],
        version='dart_fundamentals_scope_outcomes_v1',
        publication_symbols=SYMBOLS[:2],
    )
    state = {
        'schema_version': FOLLOWUP_STAGE_DEBT_SCHEMA_VERSION,
        'market_date': '2026-10-30',
        'eod_collection_run_id': 77,
        'pending_by_stage': {
            'fundamentals_daily_refresh': [SYMBOLS[0]],
        },
        'successful_by_stage': {},
        'terminal_by_stage': {
            'fundamentals_daily_refresh': [SYMBOLS[1]],
        },
        'proofs_by_stage': {
            'fundamentals_daily_refresh': [
                _stored_stage_proof(
                    baseline,
                    publication_symbols=SYMBOLS[:2],
                )
            ]
        },
        'recovery_progress_count': 0,
        'recovery_attempts_by_stage': {},
        'applied_recovery_pipeline_run_ids': [],
    }
    recovered = _exact_result(
        selected=[SYMBOLS[0]],
        successful=[SYMBOLS[0]],
        retry_debt=[],
        version='dart_fundamentals_scope_outcomes_v1',
        status='completed',
        publication_symbols=[SYMBOLS[0]],
    )

    advanced = _advance_followup_stage_debt_state(
        state,
        stage_id='fundamentals_daily_refresh',
        invocation_symbols=[SYMBOLS[0]],
        stage_result=recovered,
        recovery_pipeline_run_id=901,
    )

    assert advanced is not None
    assert advanced['pending_by_stage']['piotroski_filter'] == [
        SYMBOLS[0]
    ]
    assert advanced['terminal_by_stage']['piotroski_filter'] == [
        SYMBOLS[1]
    ]
    assert _validated_followup_stage_debt_state(
        advanced,
        market_date='2026-10-30',
        eod_collection_run_id=77,
        parent_symbols=SYMBOLS[:2],
        publication_symbols=SYMBOLS[:2],
        require_pending=False,
    ) is not None

    wrong_eod_lineage = deepcopy(advanced)
    wrong_eod_lineage['proofs_by_stage']['fundamentals_daily_refresh'][1][
        FOLLOWUP_STAGE_PROOF_RECOVERY_LINEAGE_KEY
    ]['eod_collection_run_id'] = 78
    assert _validated_followup_stage_debt_state(
        wrong_eod_lineage,
        market_date='2026-10-30',
        eod_collection_run_id=77,
        parent_symbols=SYMBOLS[:2],
        publication_symbols=SYMBOLS[:2],
        require_pending=False,
    ) is None

    wrong_pipeline_run = deepcopy(advanced)
    wrong_pipeline_run['proofs_by_stage']['fundamentals_daily_refresh'][1][
        FOLLOWUP_STAGE_PROOF_RECOVERY_LINEAGE_KEY
    ]['pipeline_run_id'] = 902
    assert _validated_followup_stage_debt_state(
        wrong_pipeline_run,
        market_date='2026-10-30',
        eod_collection_run_id=77,
        parent_symbols=SYMBOLS[:2],
        publication_symbols=SYMBOLS[:2],
        require_pending=False,
    ) is None


def test_failed_all_factor_retry_rotates_to_unattempted_sibling_debt() -> None:
    state = {
        'schema_version': FOLLOWUP_STAGE_DEBT_SCHEMA_VERSION,
        'market_date': '2026-10-30',
        'eod_collection_run_id': 77,
        'pending_by_stage': {
            'momentum_factors': list(SYMBOLS),
            'value_factors': list(SYMBOLS),
        },
        'successful_by_stage': {},
        'terminal_by_stage': {},
        'proofs_by_stage': {},
        'recovery_progress_count': 0,
        'recovery_attempts_by_stage': {},
        'applied_recovery_pipeline_run_ids': [],
    }
    assert _next_followup_stage_debt_request(state) == (
        'momentum_factors',
        SYMBOLS,
    )
    failed_all_momentum = _exact_result(
        selected=SYMBOLS,
        successful=[],
        retry_debt=SYMBOLS,
        version='factor_scope_outcomes_v1',
        status='failed',
        stage_id='momentum_factors',
        publication_symbols=SYMBOLS,
    )

    advanced = _advance_followup_stage_debt_state(
        state,
        stage_id='momentum_factors',
        invocation_symbols=SYMBOLS,
        stage_result=failed_all_momentum,
        recovery_pipeline_run_id=901,
    )

    assert advanced is not None
    assert advanced['pending_by_stage']['momentum_factors'] == SYMBOLS
    assert advanced['recovery_attempts_by_stage'] == {
        'momentum_factors': 1
    }
    assert advanced['applied_recovery_pipeline_run_ids'] == [901]
    assert advanced['recovery_progress_count'] == 0
    assert _next_followup_stage_debt_request(advanced) == (
        'value_factors',
        SYMBOLS,
    )


def test_large_compact_factor_recovery_advances_and_rejects_tampering() -> None:
    symbols = [f'{index:06d}' for index in range(1, 118)]
    retry_debt = symbols[-1:]
    recovered = _exact_result(
        selected=symbols,
        successful=symbols[:-1],
        retry_debt=retry_debt,
        version='factor_scope_outcomes_v1',
        stage_id='momentum_factors',
        publication_symbols=symbols,
    )
    details = DailyPipelineStageService._build_stage_run_details(
        stage_id='momentum_factors',
        result=recovered,
        invocation_symbols=symbols,
        market_date='2026-10-30',
        publication_symbols=symbols,
    )
    assert 'selected_stock_codes' not in details['result_summary']
    state = {
        'schema_version': FOLLOWUP_STAGE_DEBT_SCHEMA_VERSION,
        'market_date': '2026-10-30',
        'eod_collection_run_id': 77,
        'pending_by_stage': {'momentum_factors': symbols},
        'successful_by_stage': {},
        'terminal_by_stage': {},
        'proofs_by_stage': {},
        'recovery_progress_count': 0,
        'recovery_attempts_by_stage': {},
        'applied_recovery_pipeline_run_ids': [],
    }
    stage_row = {'status': 'partial', 'details': details}

    advanced = _advance_followup_stage_debt_state(
        state,
        stage_id='momentum_factors',
        invocation_symbols=symbols,
        stage_result=stage_row,
        recovery_pipeline_run_id=902,
    )

    assert advanced is not None
    assert advanced['pending_by_stage']['momentum_factors'] == retry_debt
    assert advanced['recovery_attempts_by_stage'] == {
        'momentum_factors': 1
    }
    tampered = deepcopy(stage_row)
    tampered['details']['exact_scope_outcome_proof'][
        'selected_symbol_sha256'
    ] = '0' * 64
    assert _advance_followup_stage_debt_state(
        state,
        stage_id='momentum_factors',
        invocation_symbols=symbols,
        stage_result=tampered,
        recovery_pipeline_run_id=903,
    ) is None


def test_piotroski_global_gate_blocks_raw_and_compact_progression() -> None:
    symbols = [f'{index:06d}' for index in range(1, 118)]
    blocked = _exact_result(
        selected=symbols,
        successful=symbols,
        retry_debt=[],
        version='piotroski_scope_outcomes_v1',
        status='partial',
        stage_id='piotroski_filter',
        publication_symbols=symbols,
    )
    blocked.update(
        {
            'gate_status': 'blocked',
            'ranking_dependency_status': 'incomplete',
            'reason': 'ranking_dependency_incomplete',
        }
    )
    raw_outcome = {'status': 'partial', 'result': blocked}

    assert DailyPipelineStageService._exact_partial_success_scope(
        outcome=raw_outcome,
        invocation_symbols=symbols,
        expected_stage_id='piotroski_filter',
        expected_market_date='2026-10-30',
        publication_symbols=symbols,
    ) is None
    assert _validated_atomic_successful_cohort(
        stage_result=blocked,
        invocation_symbols=symbols,
        stage_id='piotroski_filter',
        finalization_stage_id='piotroski_filter',
        market_date='2026-10-30',
        exact_publication_symbols=symbols,
    ) is None

    details = DailyPipelineStageService._build_stage_run_details(
        stage_id='piotroski_filter',
        result=blocked,
        invocation_symbols=symbols,
        market_date='2026-10-30',
        publication_symbols=symbols,
    )
    assert 'selected_stock_codes' not in details['result_summary']
    compact_outcome = {
        'status': 'partial',
        'result': {'status': 'partial'},
        'details': details,
    }
    assert DailyPipelineStageService._exact_partial_success_scope(
        outcome=compact_outcome,
        invocation_symbols=None,
        expected_stage_id='piotroski_filter',
        expected_market_date='2026-10-30',
        publication_symbols=symbols,
    ) is None

    reason_blocked = deepcopy(blocked)
    reason_blocked.update(
        {
            'gate_status': 'ready',
            'ranking_dependency_status': 'completed',
            'reason': 'piotroski_evidence_incomplete',
        }
    )
    reason_details = DailyPipelineStageService._build_stage_run_details(
        stage_id='piotroski_filter',
        result=reason_blocked,
        invocation_symbols=symbols,
        market_date='2026-10-30',
        publication_symbols=symbols,
    )
    debt_state = {
        'schema_version': FOLLOWUP_STAGE_DEBT_SCHEMA_VERSION,
        'market_date': '2026-10-30',
        'eod_collection_run_id': 77,
        'pending_by_stage': {'piotroski_filter': symbols},
        'successful_by_stage': {},
        'terminal_by_stage': {},
        'proofs_by_stage': {},
        'recovery_progress_count': 0,
        'recovery_attempts_by_stage': {},
        'applied_recovery_pipeline_run_ids': [],
    }
    assert _advance_followup_stage_debt_state(
        debt_state,
        stage_id='piotroski_filter',
        invocation_symbols=symbols,
        stage_result={'status': 'partial', 'result': reason_blocked},
        recovery_pipeline_run_id=904,
    ) is None
    assert _advance_followup_stage_debt_state(
        debt_state,
        stage_id='piotroski_filter',
        invocation_symbols=symbols,
        stage_result={'status': 'partial', 'details': reason_details},
        recovery_pipeline_run_id=905,
    ) is None


def test_large_compact_failed_all_factor_recovery_is_durable() -> None:
    symbols = [f'{index:06d}' for index in range(1, 118)]
    failed_all = _exact_result(
        selected=symbols,
        successful=[],
        retry_debt=symbols,
        version='factor_scope_outcomes_v1',
        status='failed',
        stage_id='momentum_factors',
        publication_symbols=symbols,
    )
    details = DailyPipelineStageService._build_stage_run_details(
        stage_id='momentum_factors',
        result=failed_all,
        invocation_symbols=symbols,
        market_date='2026-10-30',
        publication_symbols=symbols,
    )
    assert details['exact_scope_outcome_proof']['outcome_status'] == 'failed'
    state = {
        'schema_version': FOLLOWUP_STAGE_DEBT_SCHEMA_VERSION,
        'market_date': '2026-10-30',
        'eod_collection_run_id': 77,
        'pending_by_stage': {'momentum_factors': symbols},
        'successful_by_stage': {},
        'terminal_by_stage': {},
        'proofs_by_stage': {},
        'recovery_progress_count': 0,
        'recovery_attempts_by_stage': {},
        'applied_recovery_pipeline_run_ids': [],
    }

    advanced = _advance_followup_stage_debt_state(
        state,
        stage_id='momentum_factors',
        invocation_symbols=symbols,
        stage_result={'status': 'failed', 'details': details},
        recovery_pipeline_run_id=904,
    )

    assert advanced is not None
    assert advanced['pending_by_stage']['momentum_factors'] == symbols
    assert advanced['recovery_attempts_by_stage'] == {
        'momentum_factors': 1
    }
    assert advanced['applied_recovery_pipeline_run_ids'] == [904]
    assert advanced['recovery_progress_count'] == 0


def test_failed_all_recovery_is_applied_once_per_pipeline_run_id() -> None:
    prerequisite_successful, prerequisite_proofs = (
        _completed_stage_debt_prerequisites('momentum_factors')
    )
    failed_all_momentum = _exact_result(
        selected=SYMBOLS,
        successful=[],
        retry_debt=SYMBOLS,
        version='factor_scope_outcomes_v1',
        status='failed',
        stage_id='momentum_factors',
        publication_symbols=SYMBOLS,
    )
    initial_state = {
        'schema_version': FOLLOWUP_STAGE_DEBT_SCHEMA_VERSION,
        'market_date': '2026-10-30',
        'eod_collection_run_id': 77,
        'pending_by_stage': {'momentum_factors': list(SYMBOLS)},
        'successful_by_stage': prerequisite_successful,
        'terminal_by_stage': {},
        'proofs_by_stage': prerequisite_proofs,
        'recovery_progress_count': 0,
        'recovery_attempts_by_stage': {},
        'applied_recovery_pipeline_run_ids': [],
    }
    stage_rows = {
        'momentum_factors': {
            'status': 'failed',
            'details': {'result_summary': failed_all_momentum},
        }
    }

    def resolve(state: dict, pipeline_run_id: int) -> dict:
        return _resolve_followup_stage_debt_state(
            market_date='2026-10-30',
            eod_collection_run_id=77,
            latest_eod_metadata={
                'followup_admission_scope_outcome': (
                    _parent_admission_outcome()
                ),
                'followup_stage_debt': state,
            },
            stage_rows=stage_rows,
            incomplete_stages=['momentum_factors'],
            latest_pipeline_run={
                'id': pipeline_run_id,
                'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
                'run_metadata': {
                    'reused_eod_collection_run_id': 77,
                    'resolved_stages': ['momentum_factors'],
                },
            },
        )

    first = resolve(initial_state, 901)
    assert first['ready'] is True
    assert first['source'] == 'persisted_stage_debt_reconciled'
    after_first = first['state']
    assert after_first['recovery_attempts_by_stage'] == {
        'momentum_factors': 1
    }
    assert after_first['applied_recovery_pipeline_run_ids'] == [901]
    assert after_first['recovery_progress_count'] == 0
    assert len(after_first['proofs_by_stage']['momentum_factors']) == 1
    assert after_first['proofs_by_stage']['momentum_factors'][0][
        FOLLOWUP_STAGE_PROOF_RECOVERY_LINEAGE_KEY
    ] == {
        'schema_version': 'followup_recovery_lineage_v1',
        'pipeline_run_id': 901,
        'eod_collection_run_id': 77,
        'stage_id': 'momentum_factors',
    }

    same_run_poll = resolve(after_first, 901)
    assert same_run_poll['ready'] is True
    assert same_run_poll['source'] == 'persisted_stage_debt'
    assert same_run_poll['state'] == after_first

    new_run = resolve(same_run_poll['state'], 902)
    assert new_run['ready'] is True
    assert new_run['source'] == 'persisted_stage_debt_reconciled'
    assert new_run['state']['recovery_attempts_by_stage'] == {
        'momentum_factors': 2
    }
    assert new_run['state']['applied_recovery_pipeline_run_ids'] == [
        901,
        902,
    ]
    assert new_run['state']['recovery_progress_count'] == 0
    assert len(
        new_run['state']['proofs_by_stage']['momentum_factors']
    ) == 2
    assert _next_followup_stage_debt_request(new_run['state']) == (
        'value_factors',
        SYMBOLS,
    )


def test_exact_stage_recovery_authorizes_historical_dart_debt_only() -> None:
    service = DailyPipelineStageService()
    recovered = _exact_result(
        selected=['000002'],
        successful=['000002'],
        retry_debt=[],
        version='dart_fundamentals_scope_outcomes_v1',
        status='completed',
    )
    with (
        patch(
            'app.eod.daily_pipeline_stages.'
            'should_skip_point_in_time_current_only_stage',
            return_value=True,
        ),
        patch(
            'app.eod.daily_pipeline_stages.'
            'dart_fundamentals_capture_service.run_daily_incremental_refresh',
            new=AsyncMock(return_value=recovered),
        ) as refresh,
    ):
        result = service._execute_stage(
            'fundamentals_daily_refresh',
            market_date='2026-10-30',
            symbols=['000002'],
            exact_scope_symbols=SYMBOLS,
            markets=['KOSPI', 'KOSDAQ', 'KONEX'],
            force=False,
            bypass_prerequisite_gate=False,
            fs_div='CFS',
            symbol_limit=None,
            symbol_offset=0,
            capture_limit=500,
            retry_cycles=None,
            feedback_max_cycles=1,
            feedback_outer_batch_size=1,
            reuse_existing_feedback=True,
            point_in_time_recovery=True,
            exact_stage_recovery_authorized=True,
        )

    assert result is recovered
    refresh.assert_awaited_once()


def test_factor_siblings_keep_independent_scopes_and_composite_intersects() -> None:
    invocations: dict[str, list[str]] = {}
    publication_scopes: dict[str, list[str]] = {}
    ranking_scopes: dict[str, list[str]] = {}

    def stage_result(stage_id: str, **kwargs) -> dict:
        selected = list(kwargs['symbols'])
        invocations[stage_id] = selected
        publication_scopes[stage_id] = list(kwargs['exact_scope_symbols'])
        ranking_scopes[stage_id] = list(kwargs['ranking_symbols'])
        if stage_id == 'momentum_factors':
            selected, successful, debt = SYMBOLS, SYMBOLS[:2], SYMBOLS[2:]
        elif stage_id == 'value_factors':
            selected, successful, debt = (
                SYMBOLS,
                [SYMBOLS[0], SYMBOLS[2]],
                [SYMBOLS[1]],
            )
        elif stage_id == 'composite_factors':
            selected, successful, debt = [SYMBOLS[0]], [SYMBOLS[0]], []
        else:
            selected, successful, debt = SYMBOLS, SYMBOLS, []
        return _exact_result(
            selected=list(selected),
            successful=list(successful),
            retry_debt=list(debt),
            version='factor_scope_outcomes_v1',
            stage_id=stage_id,
            publication_symbols=list(kwargs['exact_scope_symbols']),
        )

    patch_targets = {
        'momentum_factors': (
            'app.signals.momentum_factor.'
            'daily_momentum_factor_service.rebuild_for_market_date'
        ),
        'value_factors': (
            'app.signals.value_factor.'
            'daily_value_factor_service.rebuild_for_market_date'
        ),
        'low_risk_factors': (
            'app.signals.low_risk_factor.'
            'daily_low_risk_factor_service.rebuild_for_market_date'
        ),
        'beta_factors': (
            'app.signals.beta_factor.'
            'daily_beta_factor_service.rebuild_for_market_date'
        ),
        'advanced_value_factors': (
            'app.signals.advanced_value_factor.'
            'daily_advanced_value_factor_service.rebuild_for_market_date'
        ),
        'unified_value_factors': (
            'app.signals.unified_value_factor.'
            'daily_unified_value_factor_service.rebuild_for_market_date'
        ),
        'quality_factors': (
            'app.signals.quality_factor.'
            'daily_quality_factor_service.rebuild_for_market_date'
        ),
        'composite_factors': (
            'app.signals.composite_factor.'
            'daily_composite_factor_service.rebuild_for_market_date'
        ),
    }
    with ExitStack() as stack:
        for stage_id, target in patch_targets.items():
            stack.enter_context(
                patch(
                    target,
                    side_effect=(
                        lambda stage_id=stage_id, **kwargs: stage_result(
                            stage_id,
                            **kwargs,
                        )
                    ),
                )
            )
        result = _run_daily_factor_followup(
            market_date='2026-10-30',
            exact_membership_symbols=SYMBOLS,
            stage_executor=lambda _stage_id, call: (call(), True),
        )

    assert all(
        invocations[stage_id] == SYMBOLS
        for stage_id in FACTOR_FOLLOWUP_STAGE_ORDER
        if stage_id != 'composite_factors'
    )
    assert all(
        publication_scopes[stage_id] == SYMBOLS
        for stage_id in FACTOR_FOLLOWUP_STAGE_ORDER
    )
    assert all(
        ranking_scopes[stage_id] == SYMBOLS
        for stage_id in FACTOR_FOLLOWUP_STAGE_ORDER
    )
    assert invocations['composite_factors'] == [SYMBOLS[0]]
    assert result['status'] == 'partial'
    assert result['successful_stock_codes'] == [SYMBOLS[0]]
    assert result['retry_debt_stock_codes'] == SYMBOLS[1:]


def test_factor_global_gate_failure_blocks_only_dependent_progression() -> None:
    invoked: list[str] = []

    def execute(stage_id: str, _call) -> tuple[dict, bool]:
        invoked.append(stage_id)
        result = _exact_result(
            selected=SYMBOLS,
            successful=SYMBOLS,
            retry_debt=[],
            version='factor_scope_outcomes_v1',
            status=(
                'partial' if stage_id == 'momentum_factors' else 'completed'
            ),
            stage_id=stage_id,
            publication_symbols=SYMBOLS,
        )
        if stage_id == 'momentum_factors':
            result.update(
                {
                    'gate_status': 'blocked',
                    'ranking_dependency_status': 'degraded',
                    'reason': 'ranking_dependency_incomplete',
                }
            )
        return result, True

    result = _run_daily_factor_followup(
        market_date='2026-10-30',
        exact_membership_symbols=SYMBOLS,
        stage_executor=execute,
    )

    assert 'momentum_factors' in invoked
    assert 'value_factors' in invoked
    assert 'low_risk_factors' in invoked
    assert 'composite_factors' not in invoked
    assert result['stages']['composite_factors']['status'] == 'blocked'
    assert result['successful_stock_codes'] == []
    assert result['retry_debt_stock_codes'] == SYMBOLS
    assert result['gate_status'] == 'blocked'


def test_factor_followup_rejects_manifestless_completed_stage_results() -> None:
    bare_completed = {
        'status': 'completed',
        'scope_finalization': {'ready': True},
        'successful_stock_codes': SYMBOLS,
        'retry_debt_stock_codes': [],
    }

    result = _run_daily_factor_followup(
        market_date='2026-10-30',
        exact_membership_symbols=SYMBOLS,
        stage_executor=lambda _stage_id, _call: (
            dict(bare_completed),
            True,
        ),
    )

    assert result['status'] == 'partial'
    assert result['successful_stock_codes'] == []
    assert result['retry_debt_stock_codes'] == SYMBOLS
    assert result['stages']['momentum_factors']['status'] == 'failed'
    assert 'stage-bound atomic exact current-attempt outcome manifest' in (
        result['stages']['momentum_factors']['error_message']
    )


def test_partial_factor_followup_keeps_full_membership_authority() -> None:
    authority = {'scope': 'frozen-market-membership'}
    momentum_result = _exact_result(
        selected=[SYMBOLS[0]],
        successful=[SYMBOLS[0]],
        retry_debt=[],
        version='factor_scope_outcomes_v1',
        status='completed',
        stage_id='momentum_factors',
        publication_symbols=SYMBOLS,
    )

    def execute(stage_id: str, call) -> tuple[dict, bool]:
        if stage_id == 'momentum_factors':
            return call(), True
        return (
            _exact_result(
                selected=[SYMBOLS[0]],
                successful=[SYMBOLS[0]],
                retry_debt=[],
                version='factor_scope_outcomes_v1',
                status='completed',
                stage_id=stage_id,
                publication_symbols=SYMBOLS,
            ),
            True,
        )

    with patch(
        'app.signals.momentum_factor.'
        'daily_momentum_factor_service.rebuild_for_market_date',
        return_value=momentum_result,
    ) as rebuild:
        _run_daily_factor_followup(
            market_date='2026-10-30',
            exact_membership_symbols=SYMBOLS,
            admitted_symbols=[SYMBOLS[0]],
            authoritative_market_scope=authority,
            stage_executor=execute,
        )

    rebuild.assert_called_once_with(
        market_date='2026-10-30',
        symbols=[SYMBOLS[0]],
        ranking_symbols=SYMBOLS,
        exact_scope_symbols=SYMBOLS,
        authoritative_market_scope=authority,
    )


def test_swapped_compact_factor_proof_rejects_raw_manifest_fallback() -> None:
    result = _exact_result(
        selected=SYMBOLS,
        successful=SYMBOLS[:2],
        retry_debt=SYMBOLS[2:],
        version='factor_scope_outcomes_v1',
        stage_id='momentum_factors',
    )
    details = DailyPipelineStageService._build_stage_run_details(
        stage_id='momentum_factors',
        result=result,
        invocation_symbols=SYMBOLS,
        market_date='2026-10-30',
        publication_symbols=SYMBOLS,
    )
    assert details['result_summary'][
        'scope_outcome_manifest_version'
    ] == 'factor_scope_outcomes_v1'
    assert details['exact_scope_outcome_proof'][
        'schema_version'
    ] == 'exact_scope_outcome_proof_v2'

    tampered_details = deepcopy(details)
    tampered_details['exact_scope_outcome_proof']['stage_id'] = (
        'value_factors'
    )
    persisted_outcome = {
        'stage': 'momentum_factors',
        'status': 'partial',
        'details': tampered_details,
    }

    assert daily_pipeline_outcome_is_satisfied(persisted_outcome) is False
    assert DailyPipelineStageService._exact_partial_success_scope(
        outcome={
            **persisted_outcome,
            'result': dict(tampered_details['result_summary']),
        },
        invocation_symbols=None,
        expected_stage_id='momentum_factors',
        expected_market_date='2026-10-30',
        publication_symbols=SYMBOLS,
    ) is None


def test_intraday_followup_preserves_parent_market_date() -> None:
    with (
        patch(
            'app.agent_context.eligibility.'
            'load_agent_feedback_eligible_symbols',
            return_value=['000001'],
        ),
        patch(
            'app.market.kis_intraday_snapshots.'
            'kis_intraday_snapshot_service.collect_snapshots',
            new=AsyncMock(
                return_value={
                    'status': 'completed',
                    'market_date': '2026-11-01',
                }
            ),
        ),
    ):
        result = _run_daily_intraday_snapshot_followup(
            market_date='2026-10-30',
            enabled=True,
        )

    assert result['market_date'] == '2026-10-30'
    assert result['triggered'] is True


def test_optional_intraday_partial_is_terminal_degraded_without_hiding_evidence() -> None:
    result = _terminalize_optional_intraday_snapshot_result(
        market_date='2026-10-30',
        result={
            'triggered': True,
            'status': 'partial',
            'reason': 'per_symbol_failures',
            'requested_symbols': ['000001', '000002'],
            'snapshots_persisted': 1,
            'snapshots_failed': 1,
            'failed_symbols': ['000002'],
        },
    )

    assert result['status'] == 'partial'
    assert result['safe_skip'] is False
    assert result['optional_context'] is True
    assert result['terminal'] is True
    assert result['retry_required'] is False
    assert result['retry_pending'] is False
    assert result['market_date'] == '2026-10-30'
    assert result['collection_status'] == 'partial'
    assert result['collection']['failed_symbols'] == ['000002']
    assert _extract_result_counts(
        result, default_total=2769, stage_id='intraday_snapshots',
    ) == {'processed': 2, 'total': 2, 'succeeded': 1, 'failed': 1}
    outcome = DailyPipelineStageService()._build_stage_outcome(
        'intraday_snapshots',
        result,
    )
    assert outcome['status'] == 'partial'
    assert daily_pipeline_outcome_is_satisfied(outcome)
    assert daily_pipeline_warning_metadata(
        {'intraday_snapshots': outcome}
    ) == {
        'quality_status': 'degraded',
        'warning_stage_ids': ['intraday_snapshots'],
        'warning_count': 1,
    }


@pytest.mark.parametrize(
    'error',
    [
        RuntimeError('unexpected intraday invariant'),
        IntegrityError(
            'insert into kis_intraday_snapshots',
            {},
            RuntimeError('constraint violation'),
        ),
    ],
)
def test_optional_intraday_unknown_or_integrity_errors_remain_fatal(
    error: Exception,
) -> None:
    with (
        patch(
            'app.eod.pykrx_pipeline_runner.'
            '_run_daily_intraday_snapshot_followup',
            side_effect=error,
        ),
        pytest.raises(type(error)),
    ):
        _run_terminalized_daily_intraday_snapshot_followup(
            market_date='2026-10-30',
            enabled=True,
        )


def test_dart_recovery_uses_only_first_incomplete_exact_manifest_debt() -> None:
    stage_result = _exact_result(
        selected=SYMBOLS,
        successful=[SYMBOLS[0], SYMBOLS[2]],
        retry_debt=[SYMBOLS[1]],
        version='dart_fundamentals_scope_outcomes_v1',
    )

    scope = _resolve_dart_followup_recovery_scope(
        incomplete_stages=[
            'fundamentals_daily_refresh',
            'piotroski_filter',
        ],
        stage_rows={
            'fundamentals_daily_refresh': {
                'status': 'partial',
                'details': {'result_summary': stage_result},
            }
        },
        run_metadata={},
        latest_eod_metadata={
            'followup_admission_scope_outcome': (
                _parent_admission_outcome()
            )
        },
    )

    assert scope == {
        'required': True,
        'ready': True,
        'symbols': [SYMBOLS[1]],
        'source': 'first_incomplete_exact_manifest',
        'manifest_version': 'dart_fundamentals_scope_outcomes_v1',
    }


def test_dart_recovery_restores_large_compact_exact_manifest() -> None:
    symbols = [f'{index:06d}' for index in range(1, 118)]
    retry_debt = symbols[-3:]
    stage_result = _exact_result(
        selected=symbols,
        successful=symbols[:-3],
        retry_debt=retry_debt,
        version='dart_fundamentals_scope_outcomes_v1',
    )
    details = DailyPipelineStageService._build_stage_run_details(
        stage_id='fundamentals_daily_refresh',
        result=stage_result,
        invocation_symbols=symbols,
        market_date='2026-10-30',
        publication_symbols=symbols,
    )
    assert 'selected_stock_codes' not in details['result_summary']
    assert details['exact_scope_outcome_proof'][
        'selected_stock_codes'
    ] == symbols

    scope = _resolve_dart_followup_recovery_scope(
        incomplete_stages=['fundamentals_daily_refresh'],
        stage_rows={
            'fundamentals_daily_refresh': {
                'status': 'partial',
                'details': details,
            }
        },
        run_metadata={
            'market_date': '2026-10-30',
            'symbols': symbols,
        },
        latest_eod_metadata={
            'followup_admission_scope_outcome': (
                build_exact_symbol_scope_outcome(
                    manifest_version=(
                        'auto_eod_followup_admission_scope_outcomes_v1'
                    ),
                    selected_symbols=symbols,
                    successful_symbols=symbols,
                    retry_debt_symbols=[],
                )
            )
        },
    )

    assert scope['ready'] is True
    assert scope['symbols'] == retry_debt


def test_dart_recovery_fails_closed_for_invalid_manifest() -> None:
    invalid = _exact_result(
        selected=SYMBOLS,
        successful=SYMBOLS[:2],
        retry_debt=SYMBOLS[2:],
        version='dart_fundamentals_scope_outcomes_v1',
    )
    invalid['scope_outcome_manifest'][0]['outcome'] = 'retry_debt'

    scope = _resolve_dart_followup_recovery_scope(
        incomplete_stages=['fundamentals_daily_refresh'],
        stage_rows={
            'fundamentals_daily_refresh': {
                'status': 'partial',
                'details': {'result_summary': invalid},
            }
        },
        run_metadata={},
        latest_eod_metadata={
            'followup_admission_scope_outcome': (
                _parent_admission_outcome()
            )
        },
    )

    assert scope['ready'] is False
    assert scope['reason'] == 'dart_recovery_exact_manifest_invalid'

    wrong_stage_manifest = {
        **invalid,
        **build_exact_symbol_scope_outcome(
            manifest_version='factor_scope_outcomes_v1',
            selected_symbols=SYMBOLS,
            successful_symbols=SYMBOLS[:2],
            retry_debt_symbols=SYMBOLS[2:],
        ),
    }
    wrong_stage_scope = _resolve_dart_followup_recovery_scope(
        incomplete_stages=['fundamentals_daily_refresh'],
        stage_rows={
            'fundamentals_daily_refresh': {
                'status': 'partial',
                'details': {'result_summary': wrong_stage_manifest},
            }
        },
        run_metadata={},
        latest_eod_metadata={
            'followup_admission_scope_outcome': (
                _parent_admission_outcome()
            )
        },
    )
    assert wrong_stage_scope['ready'] is False
    assert wrong_stage_scope['reason'] == (
        'dart_recovery_manifest_version_invalid'
    )


def test_dart_recovery_requires_exact_parent_or_run_scope_authority() -> None:
    stage_result = _exact_result(
        selected=SYMBOLS,
        successful=SYMBOLS[:2],
        retry_debt=SYMBOLS[2:],
        version='dart_fundamentals_scope_outcomes_v1',
    )

    scope = _resolve_dart_followup_recovery_scope(
        incomplete_stages=['fundamentals_daily_refresh'],
        stage_rows={
            'fundamentals_daily_refresh': {
                'status': 'partial',
                'details': {'result_summary': stage_result},
            }
        },
        run_metadata={},
        latest_eod_metadata={},
    )

    assert scope['ready'] is False
    assert scope['reason'] == 'dart_recovery_scope_authority_unavailable'


def test_dart_recovery_accepts_exact_failed_all_debt_partition() -> None:
    failed_all_debt = _exact_result(
        selected=SYMBOLS,
        successful=[],
        retry_debt=SYMBOLS,
        version='dart_fundamentals_scope_outcomes_v1',
        status='failed',
    )

    scope = _resolve_dart_followup_recovery_scope(
        incomplete_stages=['fundamentals_daily_refresh'],
        stage_rows={
            'fundamentals_daily_refresh': {
                'status': 'failed',
                'details': {'result_summary': failed_all_debt},
            }
        },
        run_metadata={},
        latest_eod_metadata={
            'followup_admission_scope_outcome': (
                _parent_admission_outcome()
            )
        },
    )

    assert scope['ready'] is True
    assert scope['symbols'] == SYMBOLS

    invalid_failed = {
        **failed_all_debt,
        **build_exact_symbol_scope_outcome(
            manifest_version='dart_fundamentals_scope_outcomes_v1',
            selected_symbols=SYMBOLS,
            successful_symbols=SYMBOLS[:1],
            retry_debt_symbols=SYMBOLS[1:],
        ),
        'status': 'failed',
    }
    invalid_scope = _resolve_dart_followup_recovery_scope(
        incomplete_stages=['fundamentals_daily_refresh'],
        stage_rows={
            'fundamentals_daily_refresh': {
                'status': 'failed',
                'details': {'result_summary': invalid_failed},
            }
        },
        run_metadata={},
        latest_eod_metadata={
            'followup_admission_scope_outcome': (
                _parent_admission_outcome()
            )
        },
    )
    assert invalid_scope['ready'] is False
    assert invalid_scope['reason'] == 'dart_recovery_exact_manifest_invalid'


def test_heterogeneous_stage_debt_advances_dart_without_clearing_later_debt() -> None:
    dart_result = _exact_result(
        selected=SYMBOLS,
        successful=[SYMBOLS[0], SYMBOLS[2]],
        retry_debt=[SYMBOLS[1]],
        version='dart_fundamentals_scope_outcomes_v1',
    )
    piotroski_result = _exact_result(
        selected=[SYMBOLS[0], SYMBOLS[2]],
        successful=[SYMBOLS[0]],
        retry_debt=[SYMBOLS[2]],
        version='piotroski_scope_outcomes_v1',
        publication_symbols=SYMBOLS,
    )
    eod_metadata = {
        'followup_admission_scope_outcome': _parent_admission_outcome(),
        'downstream_retry_symbols': ['009999'],
        'inherited_downstream_retry_symbols': ['008888'],
        'stage_progress': {
            'fundamentals_daily_refresh': {
                'status': 'partial',
                'details': {'result_summary': dart_result},
            },
            'piotroski_filter': {
                'status': 'partial',
                'details': {'result_summary': piotroski_result},
            },
        },
    }
    resolution = _resolve_followup_stage_debt_state(
        market_date='2026-10-30',
        eod_collection_run_id=77,
        latest_eod_metadata=eod_metadata,
        stage_rows=eod_metadata['stage_progress'],
        incomplete_stages=[
            'fundamentals_daily_refresh',
            'piotroski_filter',
        ],
    )

    assert resolution['ready'] is True
    state = resolution['state']
    assert state['pending_by_stage'] == {
        'fundamentals_daily_refresh': [SYMBOLS[1]],
        'piotroski_filter': [SYMBOLS[2]],
    }
    assert _next_followup_stage_debt_request(state) == (
        'fundamentals_daily_refresh',
        [SYMBOLS[1]],
    )

    recovered_dart = _exact_result(
        selected=[SYMBOLS[1]],
        successful=[SYMBOLS[1]],
        retry_debt=[],
        version='dart_fundamentals_scope_outcomes_v1',
        status='completed',
    )
    advanced = _advance_followup_stage_debt_state(
        state,
        stage_id='fundamentals_daily_refresh',
        invocation_symbols=[SYMBOLS[1]],
        stage_result=recovered_dart,
        recovery_pipeline_run_id=901,
    )

    assert advanced is not None
    assert advanced['recovery_progress_count'] == 1
    assert _next_followup_stage_debt_request(advanced) == (
        'piotroski_filter',
        [SYMBOLS[1], SYMBOLS[2]],
    )
    assert SYMBOLS[2] in advanced['pending_by_stage'][
        'piotroski_filter'
    ]

    latest_eod_run = {
        'id': 77,
        'trade_date': '2026-10-30',
        'symbols_succeeded': 3,
        'symbols_failed': 0,
        'run_metadata': eod_metadata,
    }
    with patch(
        'app.eod.pykrx_backfill.update_eod_collection_run_progress'
    ) as update_progress:
        persisted = PykrxBackfillService()._persist_followup_stage_debt_state(
            latest_eod_run=latest_eod_run,
            state=advanced,
        )

    assert persisted['updated'] is True
    persisted_metadata = update_progress.call_args.kwargs['run_metadata']
    assert persisted_metadata['downstream_retry_symbols'] == ['009999']
    assert persisted_metadata['inherited_downstream_retry_symbols'] == [
        '008888'
    ]
    assert persisted_metadata['followup_stage_debt'][
        'pending_by_stage'
    ]['piotroski_filter'] == [SYMBOLS[1], SYMBOLS[2]]


def test_persisted_stage_debt_reconciles_a_durable_recovery_after_crash() -> None:
    parent = _parent_admission_outcome()
    original_dart = _exact_result(
        selected=SYMBOLS,
        successful=[SYMBOLS[0], SYMBOLS[2]],
        retry_debt=[SYMBOLS[1]],
        version='dart_fundamentals_scope_outcomes_v1',
    )
    original_piotroski = _exact_result(
        selected=[SYMBOLS[0], SYMBOLS[2]],
        successful=[SYMBOLS[0]],
        retry_debt=[SYMBOLS[2]],
        version='piotroski_scope_outcomes_v1',
        publication_symbols=SYMBOLS,
    )
    initial = _resolve_followup_stage_debt_state(
        market_date='2026-10-30',
        eod_collection_run_id=77,
        latest_eod_metadata={
            'followup_admission_scope_outcome': parent,
            'stage_progress': {
                'fundamentals_daily_refresh': {
                    'status': 'partial',
                    'details': {'result_summary': original_dart},
                },
                'piotroski_filter': {
                    'status': 'partial',
                    'details': {'result_summary': original_piotroski},
                },
            },
        },
        stage_rows={},
        incomplete_stages=[
            'fundamentals_daily_refresh',
            'piotroski_filter',
        ],
    )
    persisted_state = initial['state']
    recovered_dart = _exact_result(
        selected=[SYMBOLS[1]],
        successful=[SYMBOLS[1]],
        retry_debt=[],
        version='dart_fundamentals_scope_outcomes_v1',
        status='completed',
    )

    reconciled = _resolve_followup_stage_debt_state(
        market_date='2026-10-30',
        eod_collection_run_id=77,
        latest_eod_metadata={
            'followup_admission_scope_outcome': parent,
            'followup_stage_debt': persisted_state,
        },
        stage_rows={
            'fundamentals_daily_refresh': {
                'status': 'completed',
                'details': {'result_summary': recovered_dart},
            }
        },
        incomplete_stages=['fundamentals_daily_refresh'],
        latest_pipeline_run={
            'id': 91,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'run_metadata': {
                'reused_eod_collection_run_id': 77,
                'resolved_stages': ['fundamentals_daily_refresh'],
            },
        },
    )

    assert reconciled['ready'] is True
    assert reconciled['source'] == 'persisted_stage_debt_reconciled'
    assert _next_followup_stage_debt_request(reconciled['state']) == (
        'piotroski_filter',
        [SYMBOLS[1], SYMBOLS[2]],
    )


def test_final_reconciled_stage_debt_is_persisted_before_convergence() -> None:
    parent = _parent_admission_outcome()
    prerequisite_successful, prerequisite_proofs = (
        _completed_stage_debt_prerequisites('signal_features')
    )
    original = _exact_result(
        selected=SYMBOLS,
        successful=SYMBOLS[:2],
        retry_debt=[SYMBOLS[2]],
        version='signal_features_scope_outcomes_v1',
    )
    persisted_state = {
        'schema_version': FOLLOWUP_STAGE_DEBT_SCHEMA_VERSION,
        'market_date': '2026-10-30',
        'eod_collection_run_id': 77,
        'pending_by_stage': {'signal_features': [SYMBOLS[2]]},
        'successful_by_stage': {
            **prerequisite_successful,
            'signal_features': SYMBOLS[:2],
        },
        'terminal_by_stage': {},
        'proofs_by_stage': {
            **prerequisite_proofs,
            'signal_features': [
                _stored_stage_proof(
                    original,
                    publication_symbols=SYMBOLS,
                )
            ]
        },
        'recovery_progress_count': 0,
    }
    recovered = _exact_result(
        selected=[SYMBOLS[2]],
        successful=[SYMBOLS[2]],
        retry_debt=[],
        version='signal_features_scope_outcomes_v1',
        status='completed',
    )
    completed_recovery = {
        'id': 93,
        'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
        'status': 'completed',
        'updated_at': '2026-10-30T12:10:00+00:00',
        'market_date': '2026-10-30',
        'run_metadata': {
            'resolved_stages': ['signal_features'],
            'reused_eod_collection_run_id': 77,
            'symbols': [SYMBOLS[2]],
        },
        'stages': {
            'signal_features': {
                'status': 'completed',
                'details': {'result_summary': recovered},
            }
        },
    }
    suffix_stages = {
        stage_id: {'status': 'completed'}
        for stage_id in (
            'agent_feedback_summaries',
            'signal_decider',
            'next_open_execution_plan',
        )
    }
    suffix_result = {
        'triggered': True,
        'status': 'completed',
        'market_date': '2026-10-30',
        'pipeline_run_id': 94,
        'pipeline_run': {
            'id': 94,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'completed',
            'market_date': '2026-10-30',
            'run_metadata': {
                'resolved_stages': list(suffix_stages),
                'symbols': list(SYMBOLS),
                'reused_eod_collection_run_id': 77,
                'point_in_time_recovery': True,
            },
            'stages': suffix_stages,
        },
        'resolved_stages': list(suffix_stages),
        'stages': suffix_stages,
    }
    latest_eod_run = {
        'id': 77,
        'trade_date': '2026-10-30',
        'status': 'completed_with_errors',
        'symbols_total': len(SYMBOLS),
        'symbols_succeeded': len(SYMBOLS),
        'symbols_failed': 0,
        'run_metadata': {
            'requested_symbols': SYMBOLS,
            'failed_symbols': [],
            'unattempted_symbols': [],
            'downstream_retry_symbols': [],
            'inherited_downstream_retry_symbols': [],
            'followup_admission_scope_outcome': parent,
            'followup_stage_debt': persisted_state,
        },
    }
    service = PykrxBackfillService()
    with (
        patch(
            'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
            'get_latest_run_for_context',
            return_value=completed_recovery,
        ),
        patch(
            'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
            'converge_terminal_stages_run',
            return_value={'updated': True, 'status': 'completed'},
        ),
        patch(
            'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
            'count_runs',
            return_value=0,
        ),
        patch(
            'app.eod.pykrx_backfill.update_eod_collection_run_progress'
        ) as update_eod,
        patch(
            'app.eod.pykrx_backfill.count_eod_run_persisted_symbols',
            return_value=len(SYMBOLS),
        ),
        patch(
            'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
            return_value=SYMBOLS,
        ),
        patch('app.eod.pykrx_backfill.complete_eod_collection_run'),
        patch(
            'app.eod.daily_pipeline_stages.'
            'daily_pipeline_stage_service.run',
            return_value=suffix_result,
        ) as run_stage,
    ):
        result = service._resume_auto_daily_followups_if_due(
            market_date='2026-10-30',
            latest_available_trade_date='2026-10-30',
            latest_eod_run=latest_eod_run,
            markets=('KOSPI', 'KOSDAQ', 'KONEX'),
        )

    run_stage.assert_called_once()
    assert run_stage.call_args.kwargs['stages'] == list(suffix_stages)
    assert run_stage.call_args.kwargs['symbols'] == SYMBOLS
    assert result['reason'] == 'stage_debt_success_cohort_resumed'
    tombstone = latest_eod_run['run_metadata']['followup_stage_debt']
    assert tombstone['pending_by_stage'] == {}
    assert latest_eod_run['run_metadata']['followup_recovery_required'] is False
    assert latest_eod_run['run_metadata'][
        'followup_stage_debt_terminal_progression'
    ]['processed_stock_codes'] == SYMBOLS
    assert any(
        call.kwargs['run_metadata']['followup_stage_debt'][
            'pending_by_stage'
        ]
        == {}
        and call.kwargs['run_metadata']['followup_recovery_required'] is False
        for call in update_eod.call_args_list
    )


def test_persisted_partial_recovery_proof_is_not_reapplied() -> None:
    parent = _parent_admission_outcome()
    original = _exact_result(
        selected=SYMBOLS,
        successful=[SYMBOLS[0]],
        retry_debt=[SYMBOLS[1], SYMBOLS[2]],
        version='dart_fundamentals_scope_outcomes_v1',
    )
    initial = _resolve_followup_stage_debt_state(
        market_date='2026-10-30',
        eod_collection_run_id=77,
        latest_eod_metadata={
            'followup_admission_scope_outcome': parent,
            'stage_progress': {
                'fundamentals_daily_refresh': {
                    'status': 'partial',
                    'details': {'result_summary': original},
                }
            },
        },
        stage_rows={},
        incomplete_stages=['fundamentals_daily_refresh'],
    )
    partial = _exact_result(
        selected=[SYMBOLS[1], SYMBOLS[2]],
        successful=[SYMBOLS[1]],
        retry_debt=[SYMBOLS[2]],
        version='dart_fundamentals_scope_outcomes_v1',
    )
    persisted = _advance_followup_stage_debt_state(
        initial['state'],
        stage_id='fundamentals_daily_refresh',
        invocation_symbols=[SYMBOLS[1], SYMBOLS[2]],
        stage_result=partial,
        recovery_pipeline_run_id=902,
    )
    assert persisted is not None

    resolved = _resolve_followup_stage_debt_state(
        market_date='2026-10-30',
        eod_collection_run_id=77,
        latest_eod_metadata={
            'followup_admission_scope_outcome': parent,
            'followup_stage_debt': persisted,
        },
        stage_rows={
            'fundamentals_daily_refresh': {
                'status': 'partial',
                'details': {'result_summary': partial},
            }
        },
        incomplete_stages=['fundamentals_daily_refresh'],
        latest_pipeline_run={
            'id': 902,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'run_metadata': {
                'reused_eod_collection_run_id': 77,
                'resolved_stages': ['fundamentals_daily_refresh'],
            },
        },
    )

    assert resolved['ready'] is True
    assert resolved['source'] == 'persisted_stage_debt'
    assert resolved['state']['recovery_attempts_by_stage'] == {
        'fundamentals_daily_refresh': 1
    }
    assert resolved['state']['applied_recovery_pipeline_run_ids'] == [902]
    assert _next_followup_stage_debt_request(resolved['state']) == (
        'piotroski_filter',
        [SYMBOLS[0], SYMBOLS[1]],
    )


def test_resolved_stage_debt_persists_a_proof_tombstone() -> None:
    latest_eod_run = {
        'id': 77,
        'trade_date': '2026-10-30',
        'symbols_succeeded': len(SYMBOLS),
        'symbols_failed': 0,
        'run_metadata': {
            'followup_admission_scope_outcome': (
                _parent_admission_outcome()
            ),
            'downstream_retry_symbols': [],
            'inherited_downstream_retry_symbols': [],
        },
    }
    resolved_state = {
        'schema_version': FOLLOWUP_STAGE_DEBT_SCHEMA_VERSION,
        'market_date': '2026-10-30',
        'eod_collection_run_id': 77,
        'pending_by_stage': {},
        'successful_by_stage': {
            'fundamentals_daily_refresh': SYMBOLS,
        },
        'terminal_by_stage': {},
        'proofs_by_stage': {
            'fundamentals_daily_refresh': [
                _stored_stage_proof(
                    _exact_result(
                        selected=SYMBOLS,
                        successful=SYMBOLS,
                        retry_debt=[],
                        version=(
                            'dart_fundamentals_scope_outcomes_v1'
                        ),
                        status='completed',
                    ),
                    publication_symbols=SYMBOLS,
                )
            ]
        },
    }

    with patch(
        'app.eod.pykrx_backfill.update_eod_collection_run_progress'
    ) as update_progress:
        persisted = PykrxBackfillService()._persist_followup_stage_debt_state(
            latest_eod_run=latest_eod_run,
            state=resolved_state,
        )

    assert persisted['updated'] is True
    metadata = update_progress.call_args.kwargs['run_metadata']
    assert metadata['followup_stage_debt']['pending_by_stage'] == {}
    assert metadata['followup_recovery_required'] is False


def test_successful_recovery_clears_only_proven_downstream_scope() -> None:
    latest_eod_run = {
        'id': 77,
        'trade_date': '2026-10-30',
        'status': 'completed_with_errors',
        'symbols_total': 0,
        'symbols_succeeded': 0,
        'symbols_failed': 0,
        'run_metadata': {
            'failed_symbols': [],
            'downstream_retry_symbols': [SYMBOLS[0], SYMBOLS[1]],
            'inherited_downstream_retry_symbols': [
                SYMBOLS[1],
                SYMBOLS[2],
            ],
        },
    }
    with patch(
        'app.eod.pykrx_backfill.update_eod_collection_run_progress'
    ) as update_progress:
        result = PykrxBackfillService()._persist_successful_followup_recovery(
            latest_eod_run=latest_eod_run,
            recovery_result={
                'status': 'completed',
                'pipeline_run_id': 91,
                '_proven_resolved_downstream_symbols': [SYMBOLS[1]],
            },
        )

    assert result['updated'] is True
    metadata = update_progress.call_args.kwargs['run_metadata']
    assert metadata['downstream_retry_symbols'] == [SYMBOLS[0]]
    assert metadata['inherited_downstream_retry_symbols'] == [SYMBOLS[2]]
    assert metadata['followup_recovery_required'] is True


def test_stage_debt_rejects_globally_blocked_derived_successes() -> None:
    blocked_piotroski = {
        **_exact_result(
            selected=SYMBOLS,
            successful=SYMBOLS[:1],
            retry_debt=SYMBOLS[1:],
            version='piotroski_scope_outcomes_v1',
        ),
        'gate_status': 'blocked',
    }
    resolution = _resolve_followup_stage_debt_state(
        market_date='2026-10-30',
        eod_collection_run_id=77,
        latest_eod_metadata={
            'followup_admission_scope_outcome': (
                _parent_admission_outcome()
            ),
            'stage_progress': {
                'piotroski_filter': {
                    'status': 'partial',
                    'details': {'result_summary': blocked_piotroski},
                }
            },
        },
        stage_rows={
            'piotroski_filter': {
                'status': 'partial',
                'details': {'result_summary': blocked_piotroski},
            }
        },
        incomplete_stages=['piotroski_filter'],
    )

    assert resolution['ready'] is False
    assert resolution['reason'] == (
        'piotroski_filter_stage_debt_manifest_invalid'
    )


def test_recovery_runs_dart_debt_then_exact_derived_debt() -> None:
    dart_result = _exact_result(
        selected=SYMBOLS,
        successful=[SYMBOLS[0], SYMBOLS[2]],
        retry_debt=[SYMBOLS[1]],
        version='dart_fundamentals_scope_outcomes_v1',
    )
    piotroski_result = _exact_result(
        selected=[SYMBOLS[0], SYMBOLS[2]],
        successful=[SYMBOLS[0]],
        retry_debt=[SYMBOLS[2]],
        version='piotroski_scope_outcomes_v1',
        publication_symbols=SYMBOLS,
    )
    original_stages = {
        'fundamentals_daily_refresh': {
            'status': 'partial',
            'details': {'result_summary': dart_result},
        },
        'piotroski_filter': {
            'status': 'partial',
            'details': {'result_summary': piotroski_result},
        },
    }
    original_pipeline_run = {
        'id': 91,
        'trigger': 'auto_close_scheduler',
        'status': 'partial',
        'updated_at': '2026-10-30T12:00:00+00:00',
        'market_date': '2026-10-30',
        'run_metadata': {
            'resolved_stages': [
                'fundamentals_daily_refresh',
                'piotroski_filter',
            ],
            'eod_collection_run_id': 77,
            'symbols': SYMBOLS,
        },
        'stages': original_stages,
    }
    latest_eod_run = {
        'id': 77,
        'trade_date': '2026-10-30',
        'status': 'completed_with_errors',
        'symbols_total': 3,
        'symbols_succeeded': 3,
        'symbols_failed': 0,
        'run_metadata': {
            'failed_symbols': [],
            'unattempted_symbols': [],
            'followup_admission_scope_outcome': (
                _parent_admission_outcome()
            ),
            'stage_progress': original_stages,
            'downstream_retry_symbols': ['009999'],
        },
    }
    recovered_dart = _exact_result(
        selected=[SYMBOLS[1]],
        successful=[SYMBOLS[1]],
        retry_debt=[],
        version='dart_fundamentals_scope_outcomes_v1',
        status='completed',
    )
    recovered_pipeline_result = {
        'status': 'completed',
        'pipeline_run_id': 92,
            'pipeline_run': {
                'id': 92,
                'run_metadata': {
                    'reused_eod_collection_run_id': 77,
                    'resolved_stages': [
                        'fundamentals_daily_refresh'
                    ],
                },
            },
        'market_date': '2026-10-30',
        'resolved_stages': ['fundamentals_daily_refresh'],
        'stages': {
            'fundamentals_daily_refresh': {
                'status': 'completed',
                'details': {'result_summary': recovered_dart},
            }
        },
    }
    service = PykrxBackfillService()
    with (
        patch(
            'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
            'get_latest_run_for_context',
            return_value=original_pipeline_run,
        ),
        patch(
            'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
            'count_runs',
            return_value=0,
        ),
        patch(
            'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
            'converge_terminal_stages_run',
            return_value={'updated': True, 'status': 'completed'},
        ),
        patch(
            'app.eod.pykrx_backfill.update_eod_collection_run_progress'
        ),
        patch(
            'app.eod.daily_pipeline_stages.'
            'daily_pipeline_stage_service.run',
            return_value=recovered_pipeline_result,
        ) as run_stage,
    ):
        result = service._resume_auto_daily_followups_if_due(
            market_date='2026-10-30',
            latest_available_trade_date='2026-10-30',
            latest_eod_run=latest_eod_run,
            markets=('KOSPI', 'KOSDAQ', 'KONEX'),
        )

    assert run_stage.call_args.kwargs['stages'] == [
        'fundamentals_daily_refresh'
    ]
    assert run_stage.call_args.kwargs['symbols'] == [SYMBOLS[1]]
    assert result['followup_recovery']['stage_specific_recovery'] is True
    assert latest_eod_run['run_metadata']['downstream_retry_symbols'] == [
        '009999'
    ]
    persisted_debt = latest_eod_run['run_metadata'][
        'followup_stage_debt'
    ]
    assert _next_followup_stage_debt_request(persisted_debt) == (
        'piotroski_filter',
        [SYMBOLS[1], SYMBOLS[2]],
    )

    completed_dart_pipeline_run = {
        'id': 92,
        'trigger': 'auto_eod_followup_resume',
        'status': 'completed',
        'updated_at': '2026-10-30T12:05:00+00:00',
        'market_date': '2026-10-30',
        'run_metadata': {
            'resolved_stages': ['fundamentals_daily_refresh'],
            'reused_eod_collection_run_id': 77,
            'symbols': [SYMBOLS[1]],
        },
        'stages': recovered_pipeline_result['stages'],
    }
    with (
        patch(
            'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
            'get_latest_run_for_context',
            return_value=completed_dart_pipeline_run,
        ),
        patch(
            'app.eod.pykrx_backfill.update_eod_collection_run_progress'
        ),
        patch(
            'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
            'count_runs',
            return_value=0,
        ),
        patch(
            'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
            'converge_terminal_stages_run',
            return_value={'updated': True, 'status': 'completed'},
        ),
        patch(
            'app.eod.daily_pipeline_stages.'
            'daily_pipeline_stage_service.run',
            return_value={
                'status': 'completed',
                'pipeline_run_id': 93,
                'stages': {
                    'piotroski_filter': {
                        'status': 'completed',
                        'details': {
                            'result_summary': _exact_result(
                                selected=[SYMBOLS[1], SYMBOLS[2]],
                                successful=[SYMBOLS[1], SYMBOLS[2]],
                                retry_debt=[],
                                version='piotroski_scope_outcomes_v1',
                                status='completed',
                            )
                        },
                    }
                },
            },
        ) as run_derived,
    ):
        recovered = service._resume_auto_daily_followups_if_due(
            market_date='2026-10-30',
            latest_available_trade_date='2026-10-30',
            latest_eod_run=latest_eod_run,
            markets=('KOSPI', 'KOSDAQ', 'KONEX'),
        )

    run_derived.assert_called_once()
    derived_kwargs = run_derived.call_args.kwargs
    assert derived_kwargs['stages'] == ['piotroski_filter']
    assert derived_kwargs['symbols'] == [
        SYMBOLS[1],
        SYMBOLS[2],
    ]
    assert derived_kwargs['point_in_time_recovery'] is True
    assert derived_kwargs['exact_membership_symbols'] == SYMBOLS
    stage_scope = derived_kwargs['recovery_stage_scope_outcome']
    assert stage_scope['selected_stock_codes'] == SYMBOLS
    assert stage_scope['successful_stock_codes'] == [SYMBOLS[0]]
    assert stage_scope['retry_debt_stock_codes'] == [
        SYMBOLS[1],
        SYMBOLS[2],
    ]
    assert recovered['followup_recovery']['stage_specific_recovery'] is True


def test_provider_admission_is_nonconsuming_and_rearms_after_configuration() -> None:
    stage = _provider_blocked_stage()
    admission = _validated_required_provider_admission(
        stage,
        expected_stage_id='fundamentals_daily_refresh',
    )

    assert admission is not None
    assert admission['recoverable'] is True
    assert _count_nonconsuming_provider_admission_runs(
        [
            {
                'id': 10,
                'trigger': 'auto_eod_followup_resume',
                'run_metadata': {
                    'resolved_stages': ['fundamentals_daily_refresh']
                },
                'stages': {'fundamentals_daily_refresh': stage},
            }
        ]
    ) == 1
    assert _count_nonconsuming_provider_admission_runs(
        [
            {
                'id': 11,
                'trigger': 'auto_eod_followup_resume',
                'run_metadata': {
                    'resolved_stages': [
                        'market_risk_filter',
                        'fundamentals_daily_refresh',
                    ]
                },
                'stages': {
                    'market_risk_filter': {'status': 'failed'},
                    'fundamentals_daily_refresh': stage,
                },
            }
        ]
    ) == 0
    with patch.object(settings, 'dart_api_key', ''):
        assert _required_provider_credentials_are_ready(admission) is False
    with patch.object(settings, 'dart_api_key', 'configured'):
        assert _required_provider_credentials_are_ready(admission) is True

    malformed_stage = _provider_blocked_stage()
    del malformed_stage['details']['result_summary'][
        'provider_admission'
    ]['recoverable']
    assert _validated_required_provider_admission(
        malformed_stage,
        expected_stage_id='fundamentals_daily_refresh',
    ) is None

    malformed_stage = _provider_blocked_stage()
    malformed_stage['details']['result_summary'][
        'attempt_started'
    ] = True
    assert _validated_required_provider_admission(
        malformed_stage,
        expected_stage_id='fundamentals_daily_refresh',
    ) is None

    malformed_stage = _provider_blocked_stage()
    malformed_stage['details']['result_summary']['provider_admission'][
        'required_stage_ids'
    ] = ['piotroski_filter']
    assert _validated_required_provider_admission(
        malformed_stage,
        expected_stage_id='fundamentals_daily_refresh',
    ) is None


def test_provider_admission_budget_exemption_scans_full_lineage() -> None:
    def run(run_id: int) -> dict:
        return {
            'id': run_id,
            'trigger': 'auto_eod_followup_resume',
            'run_metadata': {
                'resolved_stages': ['fundamentals_daily_refresh']
            },
            'stages': {
                'fundamentals_daily_refresh': _provider_blocked_stage()
            },
        }

    current = run(10)
    first_page = [run(run_id) for run_id in range(9, 1, -1)]
    second_page = [run(1)]
    with patch(
        'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
        'list_prior_followup_runs_for_lineage',
        side_effect=[first_page, second_page],
    ) as list_prior:
        count = _count_all_nonconsuming_provider_admission_runs(
            current_run=current,
            raw_recovery_attempts=10,
            market_date='2026-10-30',
            reused_eod_collection_run_id=99,
        )

    assert count == 10
    assert list_prior.call_count == 2
    assert list_prior.call_args_list[1].kwargs[
        'before_pipeline_run_id'
    ] == 2
    assert all(
        item.kwargs['order_by_id_desc'] is True
        for item in list_prior.call_args_list
    )


def test_old_provider_blocks_remain_nonconsuming_after_real_failure() -> None:
    def provider_block(run_id: int) -> dict:
        return {
            'id': run_id,
            'trigger': 'auto_eod_followup_resume',
            'run_metadata': {
                'resolved_stages': ['fundamentals_daily_refresh']
            },
            'stages': {
                'fundamentals_daily_refresh': _provider_blocked_stage()
            },
        }

    real_failure = {
        'id': 10,
        'trigger': 'auto_eod_followup_resume',
        'run_metadata': {
            'resolved_stages': ['fundamentals_daily_refresh']
        },
        'stages': {
            'fundamentals_daily_refresh': {'status': 'failed'}
        },
    }
    prior_blocks = [provider_block(run_id) for run_id in range(9, 0, -1)]
    with patch(
        'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
        'list_prior_followup_runs_for_lineage',
        side_effect=[prior_blocks[:8], prior_blocks[8:]],
    ):
        count = _count_all_nonconsuming_provider_admission_runs(
            current_run=real_failure,
            raw_recovery_attempts=10,
            market_date='2026-10-30',
            reused_eod_collection_run_id=99,
        )

    assert count == 9


def test_runner_required_provider_preflight_returns_exact_no_attempt_contract() -> None:
    with (
        patch.object(settings, 'kis_app_key', ''),
        patch.object(settings, 'kis_app_secret', ''),
    ):
        result = _required_provider_blocked_result(
            stage_id='market_risk_filter',
            market_date='2026-10-30',
            symbols=SYMBOLS,
        )

    assert result is not None
    assert result['triggered'] is False
    assert result['status'] == 'blocked'
    assert result['classification'] == 'blocked_missing_credential'
    assert result['attempt_started'] is False
    assert result['consumes_retry_budget'] is False
    assert result['deferred_symbols'] == SYMBOLS
    assert result['provider_admission'] == {
        **_provider_admission(
            provider='kis',
            missing_credentials=['KIS_APP_KEY', 'KIS_APP_SECRET'],
        ),
    }


def test_runner_required_provider_preflight_clears_after_configuration() -> None:
    with (
        patch.object(settings, 'kis_app_key', 'configured-key'),
        patch.object(settings, 'kis_app_secret', 'configured-secret'),
    ):
        result = _required_provider_blocked_result(
            stage_id='market_risk_filter',
            market_date='2026-10-30',
            symbols=SYMBOLS,
        )

    assert result is None


def test_price_only_factor_on_price_cohort_is_not_a_stage_debt_scope_mismatch() -> None:
    # 2026-11-09: DART/Piotroski partial, momentum ran on the whole price cohort
    # (e8b19f29). Recovery must not read that as a scope mismatch and stop.
    dart_result = _exact_result(
        selected=SYMBOLS,
        successful=SYMBOLS[:2],
        retry_debt=[SYMBOLS[2]],
        version='dart_fundamentals_scope_outcomes_v1',
    )
    piotroski_result = _exact_result(
        selected=SYMBOLS[:2],
        successful=SYMBOLS[:2],
        retry_debt=[],
        version='piotroski_scope_outcomes_v1',
        status='completed',
        publication_symbols=SYMBOLS,
    )
    momentum_result = _exact_result(
        selected=SYMBOLS,
        successful=SYMBOLS,
        retry_debt=[],
        version='factor_scope_outcomes_v1',
        status='completed',
        stage_id='momentum_factors',
        publication_symbols=SYMBOLS,
    )
    eod_metadata = {
        'followup_admission_scope_outcome': _parent_admission_outcome(),
        'stage_progress': {
            'fundamentals_daily_refresh': {
                'status': 'partial',
                'details': {'result_summary': dart_result},
            },
            'piotroski_filter': {
                'status': 'completed',
                'details': {'result_summary': piotroski_result},
            },
            'momentum_factors': {
                'status': 'completed',
                'details': {'result_summary': momentum_result},
            },
        },
    }
    resolution = _resolve_followup_stage_debt_state(
        market_date='2026-10-30',
        eod_collection_run_id=77,
        latest_eod_metadata=eod_metadata,
        stage_rows=eod_metadata['stage_progress'],
        incomplete_stages=['fundamentals_daily_refresh'],
    )

    assert resolution.get('reason') != 'momentum_factors_stage_debt_scope_mismatch'
    assert resolution['ready'] is True
    state = resolution['state']
    assert 'momentum_factors' not in state['pending_by_stage']
    # Recovery resumes the real debt (the DART symbol), not the price-only factor.
    assert _next_followup_stage_debt_request(state) == (
        'fundamentals_daily_refresh',
        [SYMBOLS[2]],
    )
