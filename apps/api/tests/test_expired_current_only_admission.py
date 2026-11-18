"""Read-only admission policy against the captured EOD596/598 contract projection."""

import asyncio
import hashlib
import json
from contextlib import contextmanager
from copy import deepcopy
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import create_engine, delete, event, insert, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db_models import (
    AgentContextFeedbackRun, DailyMarketRiskFilterResult, DailyMarketRiskSnapshot,
    DailyPipelineRun, DailyPipelineStageRun, DailyPriceBar, EODCollectionRun, Symbol,
)
from app.eod.scope_outcomes import build_exact_symbol_scope_outcome
from app.eod import auto_pykrx
from app.core.settings import Settings
from app.repositories import eod_run_repository as repository


def _timestamp(value):
    return datetime.fromisoformat(value).astimezone(timezone.utc).replace(tzinfo=None) if value else None


@pytest.fixture
def admission_db():
    fixture = json.loads((Path(__file__).parent / 'fixtures/expired_current_only_debt_596.json').read_text())
    engine = create_engine('sqlite+pysqlite:///:memory:',
                           connect_args={'check_same_thread': False}, poolclass=StaticPool)
    for model in [Symbol, EODCollectionRun, DailyPipelineRun, DailyPipelineStageRun,
                  AgentContextFeedbackRun, DailyPriceBar, DailyMarketRiskSnapshot,
                  DailyMarketRiskFilterResult]:
        model.__table__.create(engine)
    factory = sessionmaker(bind=engine)
    symbols = sorted(set(fixture['historical']['run_metadata']['requested_symbols'])
                     | set(fixture['current']['run_metadata']['requested_symbols']))
    symbol_ids = {symbol: index for index, symbol in enumerate(symbols, 1)}
    with factory.begin() as session:
        session.execute(insert(Symbol), [
            {'id': symbol_ids[s], 'symbol': s, 'name_ko': s, 'market': 'KOSDAQ'} for s in symbols
        ])
        for key in ['historical', 'current']:
            record = deepcopy(fixture[key])
            retry = record.pop('admission_retry_symbols')
            metadata = record['run_metadata']
            selected = metadata['requested_symbols']
            metadata['followup_admission_scope_outcome'] = build_exact_symbol_scope_outcome(
                manifest_version='auto_eod_followup_admission_scope_outcomes_v1',
                selected_symbols=selected, successful_symbols=[s for s in selected if s not in retry],
                retry_debt_symbols=retry,
            )
            record['trade_date'] = date.fromisoformat(record['trade_date'])
            for field in ['started_at', 'completed_at']:
                record[field] = _timestamp(record[field])
            session.add(EODCollectionRun(**record))
            session.execute(insert(DailyPriceBar), [
                {'symbol_id': symbol_ids[s], 'eod_run_id': record['id'],
                 'trade_date': record['trade_date'], 'source': 'pykrx',
                 'open_price': 1000, 'high_price': 1100, 'low_price': 900,
                 'close_price': 1000, 'volume': 100, 'turnover': 100000}
                for s in selected
            ])
        for key in ['historical_pipeline', 'current_pipeline']:
            record = deepcopy(fixture[key])
            record.setdefault('market_date', '2026-11-17')
            record.setdefault('trigger', 'auto_close_scheduler')
            record['market_date'] = date.fromisoformat(record['market_date'])
            for field in ['started_at', 'completed_at']:
                if field in record:
                    record[field] = _timestamp(record[field])
            session.add(DailyPipelineRun(**record))
        for raw in fixture['historical_stages']:
            row = deepcopy(raw)
            row.update(pipeline_run_id=652, market_date=date.fromisoformat(row['market_date']))
            for field in ['started_at', 'completed_at']:
                row[field] = _timestamp(row.get(field))
            session.add(DailyPipelineStageRun(**row))
        for raw in fixture['historical_feedback_runs']:
            row = deepcopy(raw)
            row['market_date'] = date.fromisoformat(row['market_date'])
            for field in ['started_at', 'completed_at']:
                row[field] = _timestamp(row[field])
            session.add(AgentContextFeedbackRun(**row))
        for row in fixture['current_risk']:
            common = {'symbol_id': symbol_ids[row['symbol']], 'market_date': date(2026, 11, 17), 'eod_run_id': 598}
            session.add(DailyMarketRiskSnapshot(
                **common, id=row['snapshot_id'], source=row['snapshot_source'],
                captured_at=_timestamp(row['snapshot_captured_at']),
                vi_class_code='N', is_management_item=row['is_management_item'],
                is_trade_halted=row['is_trade_halted'],
                snapshot_payload={'raw': row['raw_risk_flags'], 'vi_evidence_status': row['vi_evidence_status']},
            ))
            session.add(DailyMarketRiskFilterResult(
                **common, id=row['filter_id'], source=row['filter_source'],
                captured_at=_timestamp(row['filter_captured_at']), risk_state=row['risk_state'],
                passed_filter=row['passed_filter'], watchlist_flagged=row['watchlist_flagged'],
                is_management_item=row['is_management_item'], is_trade_halted=row['is_trade_halted'],
                filter_payload={
                    'snapshot_source': row['snapshot_source'], 'snapshot_market_date': row['filter_snapshot_market_date'],
                    'snapshot_source_endpoint': row['source_endpoint'],
                    'snapshot_captured_at': row['snapshot_captured_at'],
                    'vi_evidence_status': row['vi_evidence_status'],
                    'blocking_reasons': row['blocking_reasons'], 'watchlist_reasons': row['watchlist_reasons'],
                },
            ))

    @contextmanager
    def session_scope():
        with factory.begin() as session:
            yield session

    market = SimpleNamespace(state='OPEN', is_trading_day=True, server_time='2026-11-18T10:00:00+09:00')
    calendar = {date(2026, 11, 13): date(2026, 11, 16), date(2026, 11, 17): date(2026, 11, 18)}
    with (
        patch.object(repository, 'session_scope', session_scope),
        patch.object(repository, 'get_market_state', return_value=market),
        patch.object(repository, 'next_krx_trading_date', side_effect=lambda d, **_: calendar.get(d)) as lookup,
    ):
        yield SimpleNamespace(factory=factory, engine=engine, fixture=fixture, market=market, calendar=lookup)
    engine.dispose()


def _read(**kwargs):
    return repository.load_active_auto_eod_recovery_admission(
        on_or_after='2026-11-12', on_or_before='2026-11-18',
        **({'allow_expired_current_only_debt': True, 'target_trade_date': '2026-11-18',
            'source_market_date': '2026-11-17', 'source_eod_run_id': 598} | kwargs),
    )


def test_actual_596_classifies_history_but_keeps_current_models_pending(admission_db):
    raw = repository.load_oldest_outstanding_auto_eod_recovery_target(
        on_or_after='2026-11-12', on_or_before='2026-11-18',
    )
    assert raw == repository.AutoEODRecoveryTarget('2026-11-13', 596)
    with admission_db.factory() as session:
        before = deepcopy(session.get(EODCollectionRun, 596).run_metadata)
    statements = []
    event.listen(admission_db.engine, 'before_cursor_execute', lambda _c, _u, statement, *_: statements.append(statement))
    result = _read()
    assert result.target == repository.AutoEODRecoveryTarget('2026-11-17', 598)
    witness, = result.historical_classifications
    assert witness['historical_eod_run_id'] == 596
    assert witness['historical_pipeline_run_id'] == 652
    assert witness['historical_target_trade_date'] == '2026-11-16'
    assert witness['source_eod_run_id'] == 598
    assert witness['risk_failure_symbols'] == ['016600', '096240', '355390', '377460']
    assert witness['current_risk_rejected_symbols'] == ['377460']
    assert witness['historical_model_opportunities']['agent_feedback_summaries']['failed'] == 2
    assert witness['historical_model_opportunities']['signal_decider']['failed'] == 21
    assert all(not s.lstrip().upper().startswith(('UPDATE ', 'DELETE ', 'INSERT ')) for s in statements)
    with admission_db.factory() as session:
        assert session.get(EODCollectionRun, 596).run_metadata == before
        risk = session.get(DailyMarketRiskFilterResult, 488829)
        assert risk.passed_filter is False and risk.is_trade_halted is True
    admission_db.calendar.assert_any_call(date(2026, 11, 17), allow_fallback=False)


def test_policy_defaults_off():
    assert Settings(_env_file=None).eod_auto_pykrx_expired_current_only_admission_enabled is False


def _install_actual_exhausted_current_lineage(admission_db, *, with_running_publication=False):
    fixture = json.loads((Path(__file__).parent / 'fixtures/current598_exhausted_admission.json').read_text())
    with admission_db.factory.begin() as session:
        for raw in fixture['runs']:
            row = deepcopy(raw)
            row['market_date'] = date.fromisoformat(row['market_date'])
            for field in ['started_at', 'completed_at']:
                row[field] = _timestamp(row[field])
            session.merge(DailyPipelineRun(**row))
        for raw in fixture['stages']:
            row = deepcopy(raw)
            # The frozen SQL projection omits stage dates; its exact parent
            # is the current17 lineage. Supply that required adapter column.
            row['market_date'] = date(2026, 11, 17)
            for field in ['started_at', 'completed_at']:
                row[field] = _timestamp(row[field])
            session.add(DailyPipelineStageRun(**row))
        row = deepcopy(fixture['active_feedback'])
        row['market_date'] = date.fromisoformat(row['market_date'])
        for field in ['started_at', 'completed_at']:
            row[field] = _timestamp(row[field])
        session.add(AgentContextFeedbackRun(**row))
        if with_running_publication:
            row = deepcopy(fixture['running_summary_publication'])
            row['market_date'] = date.fromisoformat(row['market_date'])
            for field in ['started_at', 'completed_at']:
                row[field] = _timestamp(row[field])
            session.add(DailyPipelineRun(**row))
            for raw in fixture['running_summary_stages']:
                row = deepcopy(raw)
                row.update(pipeline_run_id=665, market_date=date(2026, 11, 17))
                for field in ['started_at', 'completed_at']:
                    row[field] = _timestamp(row[field])
                session.add(DailyPipelineStageRun(**row))


@pytest.mark.parametrize('with_running_publication', [False, True])
def test_actual_retry_cap_cannot_hide_running_current_summary(admission_db, with_running_publication):
    _install_actual_exhausted_current_lineage(admission_db, with_running_publication=with_running_publication)
    raw_current = repository.load_oldest_outstanding_auto_eod_recovery_target(
        on_or_after='2026-11-14', on_or_before='2026-11-18',
    )
    if not with_running_publication:
        assert raw_current is None
    result = _read()
    assert len(result.historical_classifications) == 1
    assert result.target == (raw_current or repository.AutoEODRecoveryTarget('2026-11-13', 596))
    assert result.current_source_terminal_ready is False


def _install_synthetic_future_completion(admission_db, *, zero_plan_kind=None):
    """Only a future unit adapter; never a claim about current production."""
    from tests.test_daily_pipeline_stages import _completed_feedback_result
    from tests.test_eod_run_stage_support import EODRunStageSupportTests
    from app.eod.daily_pipeline_stages import DailyPipelineStageService

    cutoff = datetime(2026, 11, 17, 12, 39, 40, 591119, tzinfo=timezone.utc)
    completed_at = datetime(2026, 11, 18, 0, 30)
    original_request = json.loads((Path(__file__).parent / 'fixtures/current598_exhausted_admission.json').read_text())['active_feedback']['request_payload']['requested_symbols']
    summary = _completed_feedback_result(symbols=['000660'], cutoff_at=cutoff)
    summary['requested_symbols'] = original_request
    summary_proof = DailyPipelineStageService._build_feedback_completion_scope_proof(
        summary, invocation_symbols=original_request,
    )
    future = EODRunStageSupportTests._nonblocking_partial_recovery_result()
    signal_proof = deepcopy(future['stages']['signal_decider']['details']['completion_scope_proof'])
    signal_proof['knowledge_cutoff_at'] = cutoff.isoformat()
    signal_proof['proof_digest'] = DailyPipelineStageService._stage_completion_scope_proof_digest(signal_proof)
    plan_proof = future['stages']['next_open_execution_plan']['details']['completion_scope_proof']
    if zero_plan_kind is not None:
        plan_result = deepcopy(future['stages']['next_open_execution_plan']['result'])
        manifest = plan_result['completion_manifest']
        if zero_plan_kind == 'no_applicable_snapshots':
            plan_result['items'] = []
            for entry in manifest['decision_partition']:
                entry.update(canonical_action='reject', disposition='non_actionable', included_items=[],
                             exclusion={'kind': 'non_actionable', 'reason_code': 'latest_snapshot_not_actionable'})
            manifest.update(included_decision_count=0, excluded_decision_count=2, included_item_count=0)
        else:
            for item in plan_result['items']:
                item.update(eligibility_status='blocked', handoff_active=False, exec_monitoring_active=False)
            for entry in manifest['decision_partition']:
                for item in entry['included_items']:
                    item.update(eligibility_status='blocked', handoff_active=False, exec_monitoring_active=False)
        manifest.pop('manifest_digest')
        manifest['manifest_digest'] = hashlib.sha256(json.dumps(
            manifest, sort_keys=True, separators=(',', ':'), ensure_ascii=True,
        ).encode()).hexdigest()
        plan_proof = DailyPipelineStageService._build_next_open_completion_scope_proof(
            plan_result, parent_scope_proof=signal_proof,
        )
        assert plan_proof is not None and plan_proof['active_item_count'] == 0
    assert summary_proof is not None
    with admission_db.factory.begin() as session:
        for run_id, stages in [(1900, ['agent_feedback_summaries']),
                               (1901, ['signal_decider', 'next_open_execution_plan'])]:
            session.add(DailyPipelineRun(
                id=run_id, market_date=date(2026, 11, 17), trigger='auto_eod_followup_resume',
                status='completed', started_at=datetime(2026, 11, 18, 0, 5), completed_at=completed_at,
                run_metadata={'reused_eod_collection_run_id': 598, 'entrypoint': 'daily_pipeline_stage_service.run',
                              'requested_stages': stages, 'resolved_stages': stages},
            ))
        for stage, run_id, proof, result in [
            ('agent_feedback_summaries', 1900, summary_proof, summary),
            ('signal_decider', 1901, signal_proof, {'status': 'completed', 'run_id': 'decider-run'}),
            ('next_open_execution_plan', 1901, plan_proof, {'summary': {
                'source_market_date': '2026-11-17', 'target_trade_date': '2026-11-18',
                'items_total': plan_proof['item_count'], 'eligible_count': 0 if zero_plan_kind else 1,
                'agent_context_completeness': {'status': 'complete', 'candidate_set_definitive': True,
                                               'pending_scope_count': 0},
            }}),
        ]:
            session.add(DailyPipelineStageRun(
                pipeline_run_id=run_id, market_date=date(2026, 11, 17), stage_id=stage,
                status='completed', completed_at=completed_at,
                source_run_ids={'run_id': result['run_id']} if 'run_id' in result else {},
                details={'completion_scope_proof': proof, 'result_summary': result},
            ))
        for run_id, count in [('summary-run', 3), ('decider-run', 2)]:
            session.add(AgentContextFeedbackRun(
                run_id=run_id, market_date=date(2026, 11, 17), status='completed',
                symbols_total=count, completed_count=count, failed_count=0,
                started_at=datetime(2026, 11, 18, 0, 0), completed_at=completed_at,
                request_payload={'knowledge_cutoff_at': cutoff.isoformat(), 'requested_symbols': original_request,
                                 'role_symbol_scopes': summary_proof['role_symbol_scopes']},
            ))
        current = session.get(EODCollectionRun, 598)
        current.run_metadata = {**current.run_metadata, 'followup_recovery_required': False,
            'downstream_recovery_pipeline_run_id': 1901,
            'downstream_recovery_completed_at': '2026-11-18T00:31:00+00:00'}


def test_split_native_completion_proofs_can_prove_future_current_readiness(admission_db):
    _install_synthetic_future_completion(admission_db)
    result = _read()
    assert result.target is None
    assert result.current_source_terminal_ready is True
    assert len(result.historical_classifications) == 1
    with admission_db.factory() as session:
        assert session.get(DailyPipelineRun, 655).status != 'completed'


@pytest.mark.parametrize('kind', ['no_applicable_snapshots', 'all_plans_blocked_by_existing_gates'])
def test_zero_accepted_plans_with_completed_deciders_is_normal_success(admission_db, kind):
    _install_synthetic_future_completion(admission_db, zero_plan_kind=kind)
    result = _read()
    assert result.target is None
    assert result.current_source_terminal_ready is True
    with admission_db.factory() as session:
        plan = session.execute(select(DailyPipelineStageRun).where(
            DailyPipelineStageRun.pipeline_run_id == 1901,
            DailyPipelineStageRun.stage_id == 'next_open_execution_plan',
        )).scalar_one()
        assert plan.details['completion_scope_proof']['active_item_count'] == 0


@pytest.mark.parametrize('optional_warning', [False, True])
@pytest.mark.parametrize('recovery_reference', [False, True])
def test_normal_full_publication_can_prove_completion(admission_db, optional_warning, recovery_reference):
    _install_synthetic_future_completion(admission_db)
    with admission_db.factory.begin() as session:
        current = session.get(EODCollectionRun, 598)
        metadata = deepcopy(current.run_metadata)
        if not recovery_reference:
            metadata.pop('downstream_recovery_pipeline_run_id')
            metadata.pop('downstream_recovery_completed_at')
            metadata['daily_pipeline_run_id'] = 1901
        current.run_metadata = metadata
        expected = metadata['expected_daily_pipeline_stages']
        pipeline = session.get(DailyPipelineRun, 1901)
        pipeline.trigger = 'auto_close_scheduler'
        pipeline.status = 'partial' if optional_warning else 'completed'
        pipeline.run_metadata = {'eod_collection_run_id': 598,
            'entrypoint': 'PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due',
            'requested_stages': expected, 'resolved_stages': expected}
        summary_row = session.execute(select(DailyPipelineStageRun).where(
            DailyPipelineStageRun.pipeline_run_id == 1900,
        )).scalar_one()
        summary_row.pipeline_run_id = 1901
        for stage in set(expected) - {'agent_feedback_summaries', 'signal_decider', 'next_open_execution_plan'}:
            warning = optional_warning and stage == 'macro_context_refresh'
            session.add(DailyPipelineStageRun(
                pipeline_run_id=1901, market_date=date(2026, 11, 17), stage_id=stage,
                status='partial' if warning else 'completed',
                completed_at=datetime(2026, 11, 18, 0, 30),
                details={'result_summary': {'status': 'partial', 'terminal': True,
                    'optional_context': True, 'retry_required': False, 'retry_pending': False,
                    'remaining_retry_count': 0}} if warning else {},
            ))
    result = _read()
    assert result.target is None
    assert result.current_source_terminal_ready is True


@pytest.mark.parametrize('stage', ['price_collection', 'quality_factors', 'fundamentals_daily_refresh'])
def test_raw_none_does_not_hide_current_mandatory_data_failure(admission_db, stage):
    _install_synthetic_future_completion(admission_db)
    with admission_db.factory.begin() as session:
        current = session.get(EODCollectionRun, 598)
        metadata = deepcopy(current.run_metadata)
        metadata['stage_progress'][stage]['status'] = 'failed'
        current.run_metadata = metadata
    with patch.object(repository, 'load_oldest_outstanding_auto_eod_recovery_target', return_value=None):
        result = _read()
    assert result.target is None
    assert result.current_source_terminal_ready is False


@pytest.mark.parametrize('case', [
    'missing_reconciliation', 'wrong_recovery_eod', 'missing_plan_proof',
    'wrong_summary_eod', 'wrong_summary_cutoff', 'wrong_plan_target',
    'current_feedback_running', 'current_feedback_failed', 'wrong_parent_pair',
])
def test_current_terminal_readiness_needs_all_native_receipts(admission_db, case):
    from app.eod.daily_pipeline_stages import DailyPipelineStageService
    _install_synthetic_future_completion(admission_db)
    with admission_db.factory.begin() as session:
        if case == 'missing_reconciliation':
            source = session.get(EODCollectionRun, 598)
            source.run_metadata = {**source.run_metadata, 'followup_recovery_required': True}
        elif case in {'wrong_recovery_eod', 'wrong_summary_eod'}:
            run = session.get(DailyPipelineRun, 1901 if case == 'wrong_recovery_eod' else 1900)
            run.run_metadata = {**run.run_metadata, 'reused_eod_collection_run_id': 596}
        elif case in {'current_feedback_running', 'current_feedback_failed'}:
            child = session.execute(select(AgentContextFeedbackRun).where(
                AgentContextFeedbackRun.run_id == 'summary-run',
            )).scalar_one()
            if case.endswith('running'):
                child.status = 'running'
            else:
                child.failed_count = 1
        else:
            stage_id = 'agent_feedback_summaries' if case == 'wrong_summary_cutoff' else 'next_open_execution_plan'
            row = session.execute(select(DailyPipelineStageRun).where(
                DailyPipelineStageRun.pipeline_run_id == (1900 if stage_id == 'agent_feedback_summaries' else 1901),
                DailyPipelineStageRun.stage_id == stage_id,
            )).scalar_one()
            details = deepcopy(row.details)
            if case == 'missing_plan_proof':
                details.pop('completion_scope_proof')
            elif case == 'wrong_plan_target':
                details['result_summary']['summary']['target_trade_date'] = '2026-11-19'
            else:
                proof = details['completion_scope_proof']
                proof['knowledge_cutoff_at' if case == 'wrong_summary_cutoff' else 'parent_summary_run_id'] = (
                    '2026-11-17T12:39:41.591119+00:00' if case == 'wrong_summary_cutoff' else 'another-summary'
                )
                proof['proof_digest'] = DailyPipelineStageService._stage_completion_scope_proof_digest(proof)
            row.details = details
    result = _read()
    assert result.target is not None
    assert result.current_source_terminal_ready is False


@pytest.mark.parametrize('market_state', ['PREOPEN', 'OPEN'])
def test_raw_none_exhausted_current_stays_blocked_without_rearming(admission_db, market_state):
    _install_actual_exhausted_current_lineage(admission_db)
    admission_db.market.state = market_state
    scheduler = auto_pykrx.AutoPykrxEODScheduler()
    with (
        patch.object(auto_pykrx.settings, 'eod_auto_pykrx_expired_current_only_admission_enabled', True),
        patch.object(auto_pykrx, 'async_get_market_state', AsyncMock(return_value=admission_db.market)),
        patch.object(auto_pykrx, '_oldest_recovery_window_date', AsyncMock(return_value='2026-11-14')),
        patch.object(auto_pykrx, 'async_previous_krx_trading_date', AsyncMock(return_value=date(2026, 11, 17))),
        patch.object(scheduler, '_run_scheduled_daily_pipeline', AsyncMock()) as dispatch,
    ):
        result = asyncio.run(scheduler.schedule_recovery_if_due(expected_market_state=market_state))
    assert result == auto_pykrx.RecoveryOnlyScheduleDisposition.DUE_BLOCKED
    assert scheduler.state.last_reason == 'current_source_terminal_authority_unavailable'
    assert scheduler._task is None
    dispatch.assert_not_called()


@pytest.mark.parametrize('uncollected', [False, True])
def test_opt_in_preserves_eligible_or_uncollected_previous_session_preopen(admission_db, uncollected):
    if uncollected:
        with admission_db.factory.begin() as session:
            session.delete(session.get(EODCollectionRun, 598))
    admission_db.market.state = 'PREOPEN'
    scheduler = auto_pykrx.AutoPykrxEODScheduler()
    async def schedule():
        result = await scheduler.schedule_recovery_if_due(expected_market_state='PREOPEN')
        if scheduler._task is not None:
            await scheduler._task
        return result
    with (
        patch.object(auto_pykrx.settings, 'eod_auto_pykrx_expired_current_only_admission_enabled', True),
        patch.object(auto_pykrx, 'async_get_market_state', AsyncMock(return_value=admission_db.market)),
        patch.object(auto_pykrx, '_oldest_recovery_window_date', AsyncMock(return_value='2026-11-12')),
        patch.object(auto_pykrx, 'async_previous_krx_trading_date', AsyncMock(return_value=date(2026, 11, 17))),
        patch.object(scheduler, '_run_scheduled_daily_pipeline', AsyncMock()) as dispatch,
    ):
        result = asyncio.run(schedule())
    assert result == auto_pykrx.RecoveryOnlyScheduleDisposition.DUE_STARTED
    assert dispatch.call_args.args == ('2026-11-17',)
    assert dispatch.call_args.kwargs['point_in_time_recovery'] is True
    if uncollected:
        assert 'recovery_eod_run_id' not in dispatch.call_args.kwargs
    else:
        assert dispatch.call_args.kwargs['recovery_eod_run_id'] == 598


@pytest.mark.parametrize('override', [
    {'allow_expired_current_only_debt': False}, {'source_eod_run_id': None},
    {'source_eod_run_id': 596}, {'source_market_date': '2026-11-16'},
    {'target_trade_date': '2026-11-19'},
])
def test_opt_in_and_exact_current_context_are_required(admission_db, override):
    result = _read(**override)
    assert result.target == repository.AutoEODRecoveryTarget('2026-11-13', 596)
    assert result.historical_classifications == ()


@pytest.mark.parametrize('case', [
    'historical_price_failure', 'historical_price_identity', 'historical_factor_failure',
    'historical_dart_failure', 'historical_unknown_stage', 'historical_rearm',
    'historical_stage_debt', 'historical_missing_admission', 'historical_wrong_pipeline',
    'historical_wrong_stage_date', 'historical_active_model', 'historical_unknown_risk_error',
    'current_price_failure', 'current_factor_failure', 'missing_current_risk',
    'wrong_current_risk_date', 'wrong_current_risk_lineage', 'wrong_snapshot_lineage',
    'unknown_current_risk', 'missing_snapshot', 'malformed_current_provider',
    'wrong_model_eod_lineage', 'wrong_plan_target', 'model_expired_analysis_replay',
    'malformed_raw_flag', 'inconsistent_negative_risk',
    'missing_feedback_run', 'wrong_feedback_date', 'active_feedback_run',
])
def test_mixed_unknown_or_unverified_authority_remains_blocking(admission_db, case):
    with admission_db.factory.begin() as session:
        historical = session.get(EODCollectionRun, 596)
        current = session.get(EODCollectionRun, 598)
        old_meta, new_meta = deepcopy(historical.run_metadata), deepcopy(current.run_metadata)
        if case == 'historical_price_failure':
            old_meta['failed_symbols'] = ['016600']
        elif case == 'historical_price_identity':
            session.execute(delete(DailyPriceBar).where(DailyPriceBar.eod_run_id == 596, DailyPriceBar.symbol_id == 1))
        elif case in {'historical_factor_failure', 'historical_dart_failure', 'current_factor_failure'}:
            meta = new_meta if case.startswith('current') else old_meta
            stage = 'fundamentals_daily_refresh' if 'dart' in case else 'quality_factors'
            meta['stage_progress'][stage]['status'] = 'failed'
        elif case == 'historical_unknown_stage':
            old_meta['expected_daily_pipeline_stages'].append('unknown_provider')
        elif case == 'historical_rearm':
            old_meta['same_day_intraday_watch_rearm'] = {'state': 'pending'}
        elif case == 'historical_stage_debt':
            old_meta['followup_stage_debt'] = {'pending_by_stage': {'quality_factors': ['016600']}}
        elif case == 'historical_missing_admission':
            old_meta.pop('followup_admission_scope_outcome')
        elif case == 'historical_wrong_pipeline':
            pipeline = session.get(DailyPipelineRun, 652)
            pipeline.run_metadata = {**pipeline.run_metadata, 'eod_collection_run_id': 595}
        elif case in {'historical_wrong_stage_date', 'historical_active_model'}:
            stage = session.execute(select(DailyPipelineStageRun).where(
                DailyPipelineStageRun.pipeline_run_id == 652,
                DailyPipelineStageRun.stage_id == 'signal_decider',
            )).scalar_one()
            if case.endswith('date'):
                stage.market_date = date(2026, 11, 12)
            else:
                stage.status = 'running'
        elif case == 'historical_unknown_risk_error':
            old_meta['stage_progress']['market_risk_filter']['failed_symbol_errors'][0]['error'] = 'malformed payload'
        elif case == 'wrong_model_eod_lineage':
            stage = session.execute(select(DailyPipelineStageRun).where(
                DailyPipelineStageRun.pipeline_run_id == 652,
                DailyPipelineStageRun.stage_id == 'signal_decider',
            )).scalar_one()
            stage.source_run_ids = {**stage.source_run_ids, 'eod_run_id': 598}
        elif case == 'wrong_plan_target':
            stage = session.execute(select(DailyPipelineStageRun).where(
                DailyPipelineStageRun.pipeline_run_id == 652,
                DailyPipelineStageRun.stage_id == 'next_open_execution_plan',
            )).scalar_one()
            stage.details = {'result_summary': {'summary': {
                'source_market_date': '2026-11-13', 'target_trade_date': '2026-11-18',
            }}}
        elif case == 'model_expired_analysis_replay':
            pipeline = session.get(DailyPipelineRun, 652)
            pipeline.run_metadata = {**pipeline.run_metadata, 'allow_expired_analysis_replay': True}
        elif case == 'current_price_failure':
            new_meta['failed_symbols'] = ['016600']
        elif case == 'missing_current_risk':
            session.execute(delete(DailyMarketRiskFilterResult).where(DailyMarketRiskFilterResult.id == 489465))
        elif case == 'wrong_current_risk_date':
            session.get(DailyMarketRiskFilterResult, 489465).market_date = date(2026, 11, 16)
        elif case == 'wrong_current_risk_lineage':
            session.get(DailyMarketRiskFilterResult, 489465).eod_run_id = 596
        elif case == 'wrong_snapshot_lineage':
            session.get(DailyMarketRiskSnapshot, 493860).eod_run_id = 596
        elif case == 'unknown_current_risk':
            item = session.get(DailyMarketRiskFilterResult, 489465)
            item.filter_payload = {**item.filter_payload, 'vi_evidence_status': 'unknown'}
        elif case == 'missing_snapshot':
            session.execute(delete(DailyMarketRiskSnapshot).where(DailyMarketRiskSnapshot.id == 493860))
        elif case == 'malformed_current_provider':
            item = session.get(DailyMarketRiskSnapshot, 493860)
            item.snapshot_payload = {'raw': {}, 'vi_evidence_status': 'inactive'}
        elif case == 'malformed_raw_flag':
            item = session.get(DailyMarketRiskSnapshot, 493860)
            payload = deepcopy(item.snapshot_payload)
            payload['raw']['trht_yn'] = 'UNKNOWN'
            item.snapshot_payload = payload
        elif case == 'inconsistent_negative_risk':
            session.get(DailyMarketRiskFilterResult, 488829).passed_filter = True
        elif case in {'missing_feedback_run', 'wrong_feedback_date', 'active_feedback_run'}:
            feedback = session.execute(select(AgentContextFeedbackRun).where(
                AgentContextFeedbackRun.run_id == 'feedback-cycle-2026-11-13-12e2823498',
            )).scalar_one()
            if case == 'missing_feedback_run':
                session.delete(feedback)
            elif case == 'wrong_feedback_date':
                feedback.market_date = date(2026, 11, 17)
            else:
                feedback.status = 'running'
        historical.run_metadata, current.run_metadata = old_meta, new_meta
    result = _read()
    assert result.target == repository.AutoEODRecoveryTarget('2026-11-13', 596)
    assert result.historical_classifications == ()


def test_current_target_and_superseded_current_source_still_block(admission_db):
    admission_db.market.server_time = '2026-11-16T10:00:00+09:00'
    result = _read(target_trade_date='2026-11-16', source_market_date='2026-11-13', source_eod_run_id=596)
    assert result.target.eod_run_id == 596 and result.historical_classifications == ()
    admission_db.market.server_time = '2026-11-18T10:00:00+09:00'
    with admission_db.factory.begin() as session:
        session.add(EODCollectionRun(
            id=599, market='KRX', source='pykrx', trade_date=date(2026, 11, 17),
            trigger='auto_close_scheduler', status='failed', run_metadata={'mode': 'daily_auto'},
            started_at=datetime(2026, 11, 18),
        ))
    result = _read()
    assert result.target.eod_run_id == 596 and result.historical_classifications == ()


@pytest.mark.parametrize('enabled', [False, True])
@pytest.mark.parametrize('current_pending', [False, True])
def test_live_scheduler_uses_classification_only_with_opt_in(admission_db, enabled, current_pending):
    if not current_pending:
        _install_synthetic_future_completion(admission_db)
    scheduler = auto_pykrx.AutoPykrxEODScheduler()
    with (
        patch.object(auto_pykrx.settings, 'eod_auto_pykrx_expired_current_only_admission_enabled', enabled),
        patch.object(auto_pykrx, 'async_get_market_state', AsyncMock(return_value=admission_db.market)),
        patch.object(auto_pykrx, '_oldest_recovery_window_date', AsyncMock(return_value='2026-11-12')),
        patch.object(auto_pykrx, 'async_previous_krx_trading_date', AsyncMock(return_value=date(2026, 11, 17))) as calendar,
        patch.object(auto_pykrx, 'load_oldest_outstanding_auto_eod_recovery_target', wraps=repository.load_oldest_outstanding_auto_eod_recovery_target),
        patch.object(auto_pykrx, 'load_latest_eod_collection_run', wraps=repository.load_latest_eod_collection_run) as source,
        patch.object(auto_pykrx, 'load_active_auto_eod_recovery_admission', wraps=repository.load_active_auto_eod_recovery_admission) as classifier,
        patch.object(auto_pykrx.PYKRX_BACKFILL_SERVICE, 'run_daily_pipeline_if_due') as dispatch,
    ):
        result = asyncio.run(scheduler.schedule_recovery_if_due(expected_market_state='OPEN'))
    assert result == (auto_pykrx.RecoveryOnlyScheduleDisposition.NO_DEBT if enabled and not current_pending
                      else auto_pykrx.RecoveryOnlyScheduleDisposition.DUE_BLOCKED)
    assert scheduler.state.active_task is False
    dispatch.assert_not_called()
    if enabled:
        source.assert_called_once_with(market='KRX', trade_date='2026-11-17', source='pykrx')
        calendar.assert_called_once_with(date(2026, 11, 18), allow_fallback=False)
        assert classifier.call_args.kwargs['source_eod_run_id'] == 598
        assert classifier.call_args.kwargs['target_trade_date'] == '2026-11-18'
        assert scheduler.snapshot()['last_result']['historical_debt_admission']['classification_count'] == 1
        if not current_pending:
            assert scheduler.state.last_reason == 'no_active_recovery_debt'
    else:
        classifier.assert_not_called()
        source.assert_not_called()
        calendar.assert_not_called()
