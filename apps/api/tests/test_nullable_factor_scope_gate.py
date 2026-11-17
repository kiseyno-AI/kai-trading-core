from types import SimpleNamespace
from unittest.mock import patch

import pytest

from app.agent_context.prerequisite_service import AgentFeedbackPrerequisiteService
from app.eod.market_membership import build_market_membership_snapshot
from app.eod.pipeline_status import EODPipelineStatusService
from app.eod.scope_outcomes import build_exact_symbol_scope_outcome
from tests.test_eod_pipeline_status import (
    _FakeMarketState, _full_feedback_prerequisite_stage_counts,
    _full_pykrx_enrichment_coverage, _scope_gap_evidence,
)


DAY = '2026-04-21'
NULL_SCOPE = ['065150', '196450', '236200']
STAGE = 'unified_value_factors'


def _status_service(universe, evidence_overrides=None):
    count = len(universe)
    timestamp = DAY + 'T09:00:00+00:00'
    factor_counts = _full_feedback_prerequisite_stage_counts(total=count, updated_at=timestamp)
    factor_counts[STAGE].update(
        decision_ready_count=int('005930' in universe),
        decision_degraded_count=len(set(universe).intersection(NULL_SCOPE)),
    )
    membership = build_market_membership_snapshot(
        symbols=[SimpleNamespace(symbol=symbol, market='KOSPI') for symbol in universe], markets=['KOSPI'],
    )

    def coverage(*, stage_id, market_date, symbols):
        assert market_date == DAY
        evidence = _scope_gap_evidence(requested_count=len(symbols), present_count=len(symbols))
        if stage_id == STAGE:
            evidence.update(
                publication_readiness_required=False, decision_ready_count=int('005930' in symbols),
                decision_degraded_count=len(set(symbols).intersection(NULL_SCOPE)), quality_status='degraded',
            )
            evidence.update(evidence_overrides or {})
        return evidence

    return EODPipelineStatusService(
        market_state_loader=_FakeMarketState,
        pykrx_run_loader=lambda **_: {
            'id': 593, 'status': 'completed', 'symbols_total': count, 'symbols_succeeded': count,
            'run_metadata': {
                'markets': ['KOSPI'], 'requested_symbols': universe,
                'universe_snapshot': {'markets': ['KOSPI'], 'market_membership': membership},
            },
        },
        pykrx_stage_counts_loader=lambda *_: {}, live_symbol_count_loader=lambda: count,
        fundamentals_refresh_loader=lambda **_: {
            'id': 538, 'status': 'completed', 'market_date': DAY, 'fs_div': 'CFS',
            'symbols_total': count, 'symbols_checked': count, 'symbols_fresh': count,
            'run_metadata': build_exact_symbol_scope_outcome(
                manifest_version='dart_daily_data_scope_outcomes_v1',
                selected_symbols=universe, successful_symbols=universe, retry_debt_symbols=[],
            ),
        },
        official_run_loader=lambda **_: None, official_stage_history_loader=lambda **_: {},
        price_bar_count_loader=lambda **_: count, technical_snapshot_count_loader=lambda *_: count,
        action_summary_count_loader=lambda *_: count,
        signal_feature_counts_loader=lambda **_: {
            'persisted': count, 'semantic_ready_count': count, 'semantic_unready_count': 0,
            'quality_complete_count': count, 'quality_degraded_count': 0, 'quality_retry_count': 0,
        },
        row_count_stage_counts_loader=lambda **_: factor_counts, row_count_scope_gap_loader=coverage,
        pykrx_enrichment_loader=lambda **kwargs: _full_pykrx_enrichment_coverage(
            symbol_count=len(kwargs.get('symbols') or universe), market_count=1, updated_at=timestamp,
        ),
    )


@pytest.mark.parametrize('universe', [NULL_SCOPE, [*NULL_SCOPE, '005930']])
@pytest.mark.parametrize('reader', ['status', 'prerequisites'])
@pytest.mark.parametrize('requested', [NULL_SCOPE, None])
def test_published_nullable_factor_scope_is_ready_in_status_and_feedback_prerequisites(universe, reader, requested):
    service = _status_service(universe)
    if reader == 'status':
        result = service.read(market_date=DAY, scope_symbols=requested)
    else:
        with patch('app.eod.pipeline_status.eod_pipeline_status_service', service):
            result = AgentFeedbackPrerequisiteService().inspect_feedback_prerequisites(
                market_date=DAY, symbols=requested, required_stage_ids=(STAGE,),
            )
    stage = result['stages'][STAGE]
    assert stage['gate_status'] == 'ready', stage['details']['gate_reason']
    assert stage['severity'] == 'degraded'
    assert stage['blocking_status'] == 'non_blocking'
    prefix = 'requested_scope' if requested else 'exact_scope'
    assert stage['details'][prefix + '_decision_ready_count'] == int(requested is None and '005930' in universe)
    assert stage['details'][prefix + '_decision_degraded_count'] == 3
    if reader == 'prerequisites':
        assert result['ready'] is True


@pytest.mark.parametrize('evidence', [
    {'publication_readiness_required': None}, {'publication_readiness_required': True},
    {'decision_degraded_count': 2}, {'decision_degraded_count': None},
    {'decision_degraded_count': True}, {'authoritative': False},
    {'evidence_version': 'unverified'}, {'requested_count': 2},
    {'present_count': 2, 'missing_count': 1, 'decision_degraded_count': 2},
])
def test_nullable_factor_exception_rejects_missing_required_or_unverified_scope(evidence):
    service = _status_service(NULL_SCOPE, evidence)
    result = service.read(market_date=DAY, scope_symbols=NULL_SCOPE)
    assert result['stages'][STAGE]['gate_status'] == 'blocked'
    with patch('app.eod.pipeline_status.eod_pipeline_status_service', service):
        result = AgentFeedbackPrerequisiteService().inspect_feedback_prerequisites(
            market_date=DAY, symbols=NULL_SCOPE, required_stage_ids=(STAGE,),
        )
    assert result['ready'] is False


@pytest.mark.parametrize('stage_id,status,gate,guards', [
    ('signal_features', 'completed', 'ready', {}), ('market_risk_filter', 'completed', 'ready', {}),
    (STAGE, 'failed', 'ready', {}), (STAGE, 'blocked', 'ready', {}), (STAGE, 'completed', 'blocked', {}),
    (STAGE, 'completed', 'ready', {'blocking_stages': ['pykrx_enrichment']}),
    (STAGE, 'completed', 'ready', {'failed_blocking_stages': ['price_collection']}),
    (STAGE, 'completed', 'ready', {'scope_verification_required': True}),
])
def test_nullable_factor_exception_preserves_mandatory_and_failed_stage_guards(stage_id, status, gate, guards):
    evidence = _scope_gap_evidence(
        requested_count=3, present_count=3, publication_readiness_required=False,
        decision_ready_count=0, decision_degraded_count=3,
    )
    stage = {
        'task': stage_id, 'status': status, 'gate_status': gate, 'succeeded': 3,
        'details': {'decision_ready_count': 0, 'decision_degraded_count': 3, 'requested_scope_coverage': evidence, **guards},
    }
    assert AgentFeedbackPrerequisiteService._classify_stage_gate_status(stage) == 'blocked'


@pytest.mark.parametrize('caller', ['scheduled', 'explicit'])
def test_daily_feedback_callers_share_nullable_factor_prerequisite_gate(caller):
    from unittest.mock import Mock

    from app.eod.daily_pipeline_stages import DailyPipelineStageService
    from app.eod.pykrx_pipeline_runner import _run_daily_feedback_followup

    service = _status_service(NULL_SCOPE)
    retry = Mock(return_value={'status': 'completed'})
    with (
        patch('app.eod.pipeline_status.eod_pipeline_status_service', service),
        patch('app.agent_context.retry_service.agent_feedback_retry_service.run_retry_cycles', retry),
        patch('app.eod.pykrx_pipeline_runner.build_expired_decision_replay_result', return_value=None),
        patch('app.eod.daily_pipeline_stages.build_expired_decision_replay_result', return_value=None),
    ):
        if caller == 'scheduled':
            result = _run_daily_feedback_followup(market_date=DAY, enabled=True, symbols=NULL_SCOPE)
        else:
            stages = DailyPipelineStageService(feedback_retry_service=SimpleNamespace(run_retry_cycles=retry))
            result = stages._execute_stage(
                'agent_feedback_summaries', market_date=DAY, symbols=NULL_SCOPE, markets=['KOSPI'],
                force=False, bypass_prerequisite_gate=False, fs_div='CFS', symbol_limit=None,
                symbol_offset=0, capture_limit=500, retry_cycles=None, feedback_max_cycles=1,
                feedback_outer_batch_size=20, reuse_existing_feedback=True,
            )
    assert result['status'] == 'completed', result
    retry.assert_called_once()
    assert retry.call_args.kwargs['config'].reuse_existing is True


def test_null_factor_producer_publishes_optional_readiness_and_exact_quality_counts():
    from datetime import date

    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from app.db_models import DailyUnifiedValueFactorResult, Symbol
    from app.eod.pipeline_status import _load_row_count_stage_scope_gap

    engine = create_engine('sqlite://')
    for model in (Symbol, DailyUnifiedValueFactorResult):
        model.__table__.create(engine)
    with Session(engine) as session:
        session.add(Symbol(id=1, symbol='065150', name_ko='test', market='KOSPI'))
        session.add(DailyUnifiedValueFactorResult(
            id=1, symbol_id=1, market_date=date.fromisoformat(DAY), factor_payload={}, unified_value_score=None,
        ))
        session.commit()
        with patch('app.eod.pipeline_status.session_scope') as scope:
            scope.return_value.__enter__.return_value = session
            evidence = _load_row_count_stage_scope_gap(stage_id=STAGE, market_date=DAY, symbols=['065150'])
    assert evidence['publication_readiness_required'] is False
    assert (evidence['present_count'], evidence['missing_count']) == (1, 0)
    assert (evidence['decision_ready_count'], evidence['decision_degraded_count']) == (0, 1)
    assert evidence['quality_status'] == 'degraded'


@pytest.mark.parametrize('count', [0, 3])
def test_empty_or_all_not_applicable_factor_scope_keeps_prior_gate_semantics(count):
    evidence = _scope_gap_evidence(
        requested_count=count, present_count=0, missing_count=0, not_applicable_count=count,
        publication_readiness_required=False, decision_ready_count=0, decision_degraded_count=0,
    )
    service = EODPipelineStatusService(row_count_scope_gap_loader=lambda **kwargs: evidence)
    stage = service._apply_scope_coverage_gate(
        stage_id=STAGE, market_date=DAY, scope_symbols=NULL_SCOPE[:count],
        stage={'task': STAGE, 'status': 'completed', 'gate_status': 'ready', 'details': {}},
    )
    assert stage['gate_status'] == 'ready'
