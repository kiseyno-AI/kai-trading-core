from copy import deepcopy
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError, TimeoutError as SQLAlchemyTimeoutError

from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES, SUMMARIZER_AGENT_NAMES
from app.agent_context.retry_service import resolve_terminal_role_success_intersection
from app.agent_context.model_identity import build_observed_model_identity, expected_decider_model
from app.agent_context.response_contract import normalize_agent_response
from app.agent_context.run_modes import DAILY_RETRY_CYCLE_MODE
from app.core.settings import settings
from app.db_models import AgentContextFeedback, AgentContextFeedbackRun, SignalDeciderDecisionSnapshot, SignalDeciderExecutionPlanItem
from app.eod.daily_pipeline_stages import DailyPipelineStageService
from app.eod.pipeline_status import EODPipelineStatusService
from app.eod.terminal_completion import load_plan_publication_items, validate_feedback_publications
from app.eod.pykrx_backfill import PYKRX_STAGE_SEQUENCE, PykrxBackfillService, PykrxSymbol
from tests.test_auto_pykrx import _AnySymbolTurnoverFrame, _FakeDailyPipelineRunStateService, _completed_factor_followup_result
from tests.test_agent_context_response_contract import _complete_signal_decider_feedback, _complete_short_term_decider_feedback
from tests.test_daily_pipeline_stages import _completed_decider_result, _completed_feedback_result
from tests.test_eod_pipeline_status import _completed_next_open_history_stage, _completed_official_stage


def _scheduled_chain(symbol_count=2):
    market_date = '2026-11-20'
    cutoff = datetime(2026, 11, 20, 12, tzinfo=timezone.utc)
    plan_stage = _completed_next_open_history_stage(
        run_id=681, market_date=market_date, total=symbol_count,
    )['stage']
    proof = plan_stage['details']['completion_scope_proof']
    plan = deepcopy(plan_stage['details']['result_summary'])
    manifest = plan['completion_manifest']
    manifest['role_symbol_scopes'] = deepcopy(proof['role_symbol_scopes'])
    for entry in manifest['decision_partition']:
        for item in entry['included_items']:
            item.update(eligibility_status='blocked', handoff_active=False, exec_monitoring_active=False)
    manifest.pop('manifest_digest')
    manifest['manifest_digest'] = hashlib.sha256(json.dumps(
        manifest, ensure_ascii=True, sort_keys=True, separators=(',', ':'),
    ).encode()).hexdigest()
    items = [{**item, **{k: entry[k] for k in (
        'symbol', 'decision_agent_name', 'decision_snapshot_id', 'decision_run_id',
    )}} for entry in manifest['decision_partition'] for item in entry['included_items']]
    plan['summary'] = {
        'target_trade_date': '2026-11-23', 'source_market_date': market_date,
        'items_total': len(items), 'eligible_count': 0, 'monitor_candidate_count': 0,
        'account_targets_configured': True, 'account_targets_valid': True,
        'enrichment_preflight_allowed': True, 'agent_context_completeness_status': 'complete',
    }
    symbols = [f'{index:06d}' for index in range(1, symbol_count + 1)]
    summary = _completed_feedback_result(symbols=symbols, cutoff_at=cutoff,
                                         run_id=proof['parent_summary_run_id'])
    decider = _completed_decider_result(
        symbols=symbols, cutoff_at=cutoff,
        source_summary_run_id=summary['run_id'], run_id=proof['parent_run_id'],
    )
    decider.update(scope_authority=None, knowledge_cutoff_at=cutoff.isoformat())
    decider.pop('decision_identities')
    for result in (summary, decider):
        result['market_date'] = market_date
    stages = {}
    for stage_id, result, count in (
        ('agent_feedback_summaries', summary, symbol_count * 3), ('signal_decider', decider, symbol_count * 2),
        ('next_open_execution_plan', plan, symbol_count),
    ):
        stage = _completed_official_stage(run_id=681, stage_id=stage_id,
                                          market_date=market_date, processed=count, total=count)['stage']
        stage['source_run_ids'] = {'eod_run_id': 601}
        if stage_id != 'next_open_execution_plan':
            stage['source_run_ids']['agent_feedback_run_id'] = result['run_id']
        stage['details'] = {'mode': 'auto_eod_pipeline_tracking', 'result_summary': result}
        stages[stage_id] = stage
    return {'id': 681, 'market_date': market_date, 'trigger': 'auto_close_scheduler',
            'status': 'completed', 'run_metadata': {
                'entrypoint': 'PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due',
                'eod_collection_run_id': 601,
            }, 'stages': stages}, items, [{k: e[k] for k in (
                'symbol', 'decision_agent_name', 'decision_snapshot_id', 'decision_run_id',
            )} for e in manifest['decision_partition']]


def _partial_summary_result(*, run_id='partial-summary'):
    result = _completed_feedback_result(
        symbols=['000001', '000002'],
        cutoff_at=datetime(2026, 11, 20, 12, tzinfo=timezone.utc), run_id=run_id,
    )
    result.update(status='partial', completed=False, market_date='2026-11-20', final_pending=0)
    summary = result['progress']['summary']
    summary.update(run_status='partial', run_table_status='partial', completed_feedback_count=5,
                   failed_items=1, completed_symbols=1, pending_symbols=0, in_progress_symbols=0)
    failed_role = SUMMARIZER_AGENT_NAMES[0]
    summary['failed_symbols_by_role'] = {role: ['000002'] if role == failed_role else []
                                         for role in SUMMARIZER_AGENT_NAMES}
    summary['role_progress'][failed_role].update(
        status='completed_with_failures', boundary_status='completed_with_failures',
        completed_symbols=1, failed_items=1, retry_pending_symbols=1,
    )
    return result


@pytest.mark.parametrize('mutation', [None, 'outside_intersection', 'wrong_cutoff', 'invalid_parent_proof'])
def test_partial_summary_can_bind_only_its_published_terminal_intersection_to_deciders(mutation):
    run, _, identities = _scheduled_chain(symbol_count=1)
    summary_result = _partial_summary_result(
        run_id=run['stages']['agent_feedback_summaries']['details']['result_summary']['run_id'],
    )
    resolution = {
        'ready': True, 'source_run_id': summary_result['run_id'],
        'knowledge_cutoff_at': summary_result['knowledge_cutoff_at'],
        'feedback_scope_mode': summary_result['feedback_scope_mode'],
        'requested_symbols': ['000001'],
        'completed_scope_proof': {'schema_version': 'completed_summarizer_intersection_v1',
                                  'completed_symbols': ['000001'], 'admitted_symbols': ['000001', '000002']},
    }
    decider = run['stages']['signal_decider']['details']['result_summary']
    if mutation == 'outside_intersection':
        decider['requested_symbols'] = ['000001', '000002']
    elif mutation == 'wrong_cutoff':
        decider['knowledge_cutoff_at'] = '2026-11-20T13:00:00+00:00'
    with patch.object(DailyPipelineStageService, '_validate_terminal_feedback_publications',
                      return_value={'valid': True, 'decision_identities': identities,
                                    'summary_cutoff_resolution': resolution}):
        details = DailyPipelineStageService.build_scheduled_terminal_details(
            stage_id='signal_decider', market_date='2026-11-20',
            result=decider,
            summary_result=summary_result,
            summary_scope_proof={'proof_digest': 'invalid'} if mutation == 'invalid_parent_proof' else None,
        )
    proof = details.get('completion_scope_proof')
    if mutation is not None:
        assert proof is None
        return
    assert proof is not None
    assert proof['admitted_symbols'] == proof['completed_symbols'] == ['000001']
    assert proof['source_summary_run_id'] == summary_result['run_id']
    assert summary_result['status'] == 'partial'
    assert summary_result['progress']['summary']['failed_symbols_by_role'][SUMMARIZER_AGENT_NAMES[0]] == ['000002']


@pytest.mark.parametrize('mutation', [None, 'automatic_scope', 'broader_admission', 'missing_covered_row', 'malformed_covered_row',
                                    'foreign_cutoff', 'unfinished_parent', 'foreign_run',
                                    'forged_failure_scope', 'missing_role_scope', 'wrong_dispatch_scope', 'missing_admission'])
def test_partial_summary_publication_requires_its_persisted_success_intersection(mutation):
    result = _partial_summary_result()
    cutoff = datetime.fromisoformat(result['knowledge_cutoff_at'])
    parent = AgentContextFeedbackRun(
        run_id=result['run_id'], market_date=cutoff.date(), status='partial', completed_at=cutoff,
        symbols_total=6, completed_count=5, failed_count=1,
        request_payload={'mode': DAILY_RETRY_CYCLE_MODE, 'market_date': '2026-11-20',
                         'roles': list(SUMMARIZER_AGENT_NAMES), 'fs_div': 'CFS',
                         'feedback_scope_mode': result['feedback_scope_mode'],
                         'role_symbol_scopes': deepcopy(result['progress']['summary']['role_symbol_scopes']),
                         'symbols': ['000001', '000002'], 'requested_symbols': ['000001', '000002'],
                         'knowledge_cutoff_at': cutoff.isoformat()},
        summary_payload={'progress_summary': deepcopy(result['progress']['summary'])},
    )
    rows, children = [], []
    # The uncompleted symbol has no publication for the failed role. Only the exact
    # intersection is consumable; the parent's full 5/6 progress remains unchanged.
    for index, role in enumerate(SUMMARIZER_AGENT_NAMES):
        payload = normalize_agent_response(
            role=role, output_text=json.dumps({
                'role': role, 'summary': 'Verified terminal summary.', 'confidence': 0.7,
                'risk_flags': [], 'details': {'coverage': 'complete'}, 'improvement_notes': [],
            }), raw_response={}, market_date=cutoff.date(), symbol='000001', agent_name=role,
        )
        assert payload['response_contract_valid']
        child_id = f'summary-child-{index}'
        rows.append(AgentContextFeedback(
            id=index + 1, market_date=cutoff.date(), symbol='000001', agent_name=role,
            run_id=child_id, received_at=cutoff, normalized_feedback_payload=payload,
        ))
        children.append(AgentContextFeedbackRun(
            run_id=child_id, market_date=cutoff.date(), status='partial', completed_at=cutoff,
            request_payload={'parent_run_id': parent.run_id, 'knowledge_cutoff_at': cutoff.isoformat()},
        ))
    if mutation == 'automatic_scope':
        result['requested_symbols'] = None
    elif mutation == 'broader_admission':
        result['requested_symbols'] = parent.request_payload['requested_symbols'] = ['000001', '000002', '000003']
    elif mutation == 'missing_covered_row':
        rows.pop()
    elif mutation == 'malformed_covered_row':
        rows[0].normalized_feedback_payload.pop('summary')
    elif mutation == 'foreign_cutoff':
        children[0].request_payload['knowledge_cutoff_at'] = '2026-11-20T13:00:00+00:00'
    elif mutation == 'unfinished_parent':
        parent.completed_at = None
    elif mutation == 'foreign_run':
        result['progress']['summary']['run_id'] = 'foreign'
    elif mutation == 'forged_failure_scope':
        result['progress']['summary']['failed_symbols_by_role'][SUMMARIZER_AGENT_NAMES[0]] = ['000001']
    elif mutation == 'missing_role_scope':
        result['progress']['summary']['role_symbol_scopes'].pop(SUMMARIZER_AGENT_NAMES[-1])
    elif mutation == 'wrong_dispatch_scope':
        parent.request_payload['symbols'] = ['000001', '000003']
    elif mutation == 'missing_admission':
        parent.request_payload.pop('requested_symbols')
    session = MagicMock()
    session.execute.side_effect = [
        MagicMock(), MagicMock(scalar_one_or_none=lambda: parent),
        MagicMock(scalars=lambda: SimpleNamespace(all=lambda: rows)),
        MagicMock(scalars=lambda: SimpleNamespace(all=lambda: children)),
    ]
    @contextmanager
    def readonly_session(**_):
        yield session
    with patch('app.eod.terminal_completion.session_scope', readonly_session):
        publication = validate_feedback_publications(market_date='2026-11-20', result=result)
    assert publication['valid'] is (mutation in {None, 'automatic_scope', 'broader_admission'})
    if publication['valid']:
        resolution = publication['summary_cutoff_resolution']
        assert resolution['requested_symbols'] == ['000001']
        assert resolution['completed_scope_proof']['admitted_symbols'] == (
            ['000001', '000002', '000003'] if mutation == 'broader_admission' else ['000001', '000002']
        )
        assert resolution['terminal_role_scope_proof']['failed_items'] == 1
        assert parent.status == result['status'] == 'partial'
        assert parent.completed_count == 5
    assert all(str(c.args[0]).lstrip().startswith(('SELECT', 'SET TRANSACTION')) for c in session.execute.call_args_list)


def test_completed_scheduled_summary_keeps_exact_completion_after_status_read():
    """The scheduled writer stores full results without the explicit writer's proof."""
    market_date = '2026-11-20'
    result = _completed_feedback_result(
        symbols=['000001', '000002'],
        cutoff_at=datetime(2026, 11, 20, 12, tzinfo=timezone.utc),
    )
    result['market_date'] = market_date
    item = _completed_official_stage(
        run_id=681, stage_id='agent_feedback_summaries',
        market_date=market_date, processed=6, total=6,
    )
    item['stage']['details'] = {
        'mode': 'auto_eod_pipeline_tracking',
        'result_summary': deepcopy(result),
    }
    item['stage']['source_run_ids'] = {'eod_run_id': 601, 'agent_feedback_run_id': 'summary-run'}
    item['run'].update(trigger='auto_close_scheduler', stages={'agent_feedback_summaries': item['stage']})
    item['run']['run_metadata']['entrypoint'] = 'PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due'
    item['run']['run_metadata']['eod_collection_run_id'] = 601
    service = EODPipelineStatusService()
    with patch.object(
        DailyPipelineStageService, '_validate_terminal_feedback_publications',
        return_value={'valid': True, 'decision_identities': []}, create=True,
    ):
        stages = service._overlay_official_run_stages(
            stages={}, official_run=item['run'], market_date=market_date,
        )
    assert stages['agent_feedback_summaries']['status'] == 'completed'
    assert DailyPipelineStageService.validated_stage_completion_scope_proof(
        'agent_feedback_summaries',
        stages['agent_feedback_summaries']['details'].get('completion_scope_proof'),
    ) is not None


def test_legacy_scheduled_chain_reconstructs_bound_proofs_without_rerunning_models():
    run, items, identities = _scheduled_chain()
    def validate(**kwargs):
        return {'valid': True, 'decision_identities': identities if kwargs['result']['roles'] == list(DECISION_SUPPORT_AGENT_NAMES) else []}
    service = EODPipelineStatusService()
    with (
        patch.object(DailyPipelineStageService, '_validate_terminal_feedback_publications', side_effect=validate),
        patch('app.eod.terminal_completion.load_plan_publication_items', return_value=items),
        patch('app.agent_context.retry_service.agent_feedback_retry_service.run_retry_cycles') as dispatch,
    ):
        stages = service._overlay_official_run_stages(stages={}, official_run=run, market_date='2026-11-20')
        stages = service._normalize_stage_contract(stages)
    assert [stages[k]['status'] for k in run['stages']] == ['completed'] * 3
    assert stages['next_open_execution_plan']['details']['completion_scope_proof']['active_item_count'] == 0
    dispatch.assert_not_called()
    assert all('completion_scope_proof' not in s['details'] for s in run['stages'].values())


@pytest.mark.parametrize('mutation', ['invalid_proof', 'invalid_summary_proof', 'foreign_source', 'foreign_eod', 'incomplete', 'missing_role', 'wrong_cutoff', 'bad_digest'])
def test_legacy_scheduled_chain_does_not_replace_invalid_or_incomplete_evidence(mutation):
    run, items, identities = _scheduled_chain()
    stages = run['stages']
    decider = stages['signal_decider']
    result = decider['details']['result_summary']
    if mutation == 'invalid_proof':
        decider['details']['completion_scope_proof'] = {'proof_digest': 'invalid'}
    elif mutation == 'invalid_summary_proof':
        stages['agent_feedback_summaries']['details']['completion_scope_proof'] = {'proof_digest': 'invalid'}
    elif mutation == 'foreign_source':
        decider['source_run_ids']['agent_feedback_run_id'] = 'foreign-run'
    elif mutation == 'foreign_eod':
        decider['source_run_ids']['eod_run_id'] = 602
    elif mutation == 'incomplete':
        result['progress']['summary']['role_progress']['signal-decider']['pending_symbols'] = 1
    elif mutation == 'missing_role':
        result['roles'] = ['signal-decider']
    elif mutation == 'wrong_cutoff':
        result['knowledge_cutoff_at'] = '2026-11-20T13:00:00+00:00'
    else:
        stages['next_open_execution_plan']['details']['result_summary']['completion_manifest']['manifest_digest'] = 'tampered'
    with (
        patch.object(DailyPipelineStageService, '_validate_terminal_feedback_publications', return_value={'valid': True, 'decision_identities': identities}),
        patch('app.eod.terminal_completion.load_plan_publication_items', return_value=items),
    ):
        actual = EODPipelineStatusService()._overlay_official_run_stages(stages={}, official_run=run, market_date='2026-11-20')
        actual = EODPipelineStatusService._normalize_stage_contract(actual)
    assert actual['next_open_execution_plan']['status'] == 'blocked'


def _publication_rows():
    cutoff = datetime(2026, 11, 20, 12, tzinfo=timezone.utc)
    symbol = '0017J0'
    result = _completed_decider_result(symbols=[symbol], cutoff_at=cutoff,
                                       source_summary_run_id='summary-run', run_id='decider-run')
    result.update(market_date='2026-11-20', knowledge_cutoff_at=cutoff.isoformat())
    scopes = result['progress']['summary']['role_symbol_scopes']
    parent = AgentContextFeedbackRun(
        run_id='decider-run', market_date=cutoff.date(), status='completed',
        symbols_total=2, completed_count=2, failed_count=0, completed_at=cutoff,
        request_payload={'mode': DAILY_RETRY_CYCLE_MODE, 'market_date': '2026-11-20',
                         'roles': list(DECISION_SUPPORT_AGENT_NAMES), 'fs_div': 'CFS',
                         'feedback_scope_mode': result['feedback_scope_mode'],
                         'role_symbol_scopes': scopes, 'knowledge_cutoff_at': cutoff.isoformat(),
                         'source_summary_run_id': 'summary-run'}, summary_payload={},
    )
    feedback, children, snapshots = [], [], []
    for index, role in enumerate(DECISION_SUPPORT_AGENT_NAMES):
        builder = _complete_signal_decider_feedback if role == 'signal-decider' else _complete_short_term_decider_feedback
        raw = builder(symbol=symbol, market_date='2026-11-20', traceability={'symbol': symbol})
        normalized = normalize_agent_response(role=role, output_text=json.dumps(raw), raw_response={},
                                              market_date=cutoff.date(), symbol=symbol, agent_name=role)
        expected_model = expected_decider_model(role, settings_obj=settings)
        provider, model = expected_model.split('/', 1)
        normalized.update(model_identity=build_observed_model_identity(
            role=role, expected_model=expected_model, actual_provider=provider, actual_model=model,
        ), runtime_contract={'verified': True})
        assert normalized['response_contract_valid']
        child_id = f'child-{index}'
        feedback.append(AgentContextFeedback(
            id=index + 1, market_date=cutoff.date(), symbol=symbol, agent_name=role,
            run_id=child_id, received_at=cutoff, summary_payload={},
            normalized_feedback_payload=normalized, feedback_payload=normalized,
        ))
        children.append(AgentContextFeedbackRun(
            run_id=child_id, market_date=cutoff.date(), status='completed',
            completed_at=cutoff,
            request_payload={'parent_run_id': parent.run_id, 'knowledge_cutoff_at': cutoff.isoformat()},
        ))
        snapshots.append(SignalDeciderDecisionSnapshot(
            id=index + 1, feedback_id=index + 1, market_date=cutoff.date(), symbol=symbol,
            agent_name=role, decision_run_id=child_id, response_payload=deepcopy(normalized),
            next_day_open_action='buy',
            target_price=normalized.get('target_price'), stop_loss_price=normalized.get('stop_loss_price'),
            decision_action='buy', preferred_action='buy',
            entry_price_basis='trigger_limit' if role == 'short-term-decider' else 'next_open',
            confidence_score_10=normalized.get('confidence_score_10'),
            conviction_score_10=normalized.get('conviction_score_10'),
            expected_return_pct=None if role == 'short-term-decider' else normalized.get('expected_return_pct'),
            suggested_holding_period=normalized.get('expected_holding_period') if role == 'short-term-decider' else normalized.get('suggested_holding_period'),
            data_quality_warnings_payload=deepcopy(normalized.get('data_quality_warnings') or []),
        ))
    return result, parent, feedback, children, snapshots


@pytest.mark.parametrize('mutation', [None, 'foreign_parent', 'wrong_date', 'missing_feedback',
                                    'missing_role', 'malformed_feedback', 'typed_mismatch',
                                    'unverified_model', 'unverified_runtime', 'running_child', 'failed_child',
                                    'partial_child', 'partial_malformed_feedback', 'partial_unfinished_child',
                                    'reused_source', 'wrong_reuse_identity', 'reused_running_child',
                                    'reused_unfinished_child', 'reused_wrong_date', 'reused_wrong_cutoff',
                                    'typed_confidence', 'typed_conviction', 'typed_decision_action',
                                    'typed_preferred_action', 'typed_entry_basis', 'typed_return', 'typed_holding_period',
                                    'typed_numeric_rounding', 'typed_wait_projection',
                                    'snapshot_invalid_false', 'snapshot_invalid_none', 'snapshot_invalid_zero',
                                    'snapshot_legacy_no_flag', 'warning_extra', 'warning_hidden',
                                    'warning_match', 'warning_empty_none'])
def test_publication_validator_checks_actual_orm_feedback_and_typed_contracts(mutation):
    result, parent, rows, children, snapshots = _publication_rows()
    if mutation == 'foreign_parent':
        children[0].request_payload['parent_run_id'] = 'foreign'
    elif mutation == 'wrong_date':
        parent.request_payload['market_date'] = '2026-11-19'
    elif mutation == 'missing_feedback':
        rows.pop()
    elif mutation == 'missing_role':
        parent.request_payload['role_symbol_scopes'] = {children[0].run_id: []}
    elif mutation == 'malformed_feedback':
        rows[0].normalized_feedback_payload.pop('thesis_summary')
    elif mutation == 'typed_mismatch':
        snapshots[0].response_payload['target_price'] += 100
    elif mutation in {'snapshot_invalid_false', 'snapshot_invalid_none', 'snapshot_invalid_zero'}:
        snapshots[0].response_payload['response_contract_valid'] = {
            'snapshot_invalid_false': False, 'snapshot_invalid_none': None, 'snapshot_invalid_zero': 0,
        }[mutation]
    elif mutation == 'snapshot_legacy_no_flag':
        snapshots[0].response_payload.pop('response_contract_valid')
    elif mutation == 'warning_extra':
        snapshots[0].data_quality_warnings_payload = ['unmatched_warning']
    elif mutation in {'warning_hidden', 'warning_match'}:
        for payload in (rows[0].normalized_feedback_payload, snapshots[0].response_payload):
            payload['data_quality_warnings'] = ['published_warning']
        if mutation == 'warning_match':
            snapshots[0].data_quality_warnings_payload = ['published_warning']
    elif mutation == 'warning_empty_none':
        snapshots[0].data_quality_warnings_payload = None
    elif mutation == 'typed_numeric_rounding':
        index = next(i for i, row in enumerate(rows) if row.agent_name == 'signal-decider')
        rows[index].normalized_feedback_payload['expected_return_pct'] = 6.1234565
        snapshots[index].response_payload['expected_return_pct'] = 6.1234565
        snapshots[index].expected_return_pct = Decimal('6.123457')
    elif mutation == 'typed_wait_projection':
        index = next(i for i, row in enumerate(rows) if row.agent_name == 'short-term-decider')
        for payload in (rows[index].normalized_feedback_payload, snapshots[index].response_payload):
            payload.update(tactical_action='wait', preferred_action='wait', wait_reason={
                'code': 'extended_entry_price', 'summary': 'Wait for the existing trigger.',
            })
        snapshots[index].decision_action = snapshots[index].preferred_action = snapshots[index].next_day_open_action = 'wait'
    elif isinstance(mutation, str) and mutation.startswith('typed_'):
        field, value = {
            'typed_confidence': ('confidence_score_10', 0),
            'typed_conviction': ('conviction_score_10', 0),
            'typed_decision_action': ('decision_action', 'reject'),
            'typed_preferred_action': ('preferred_action', 'reject'),
            'typed_entry_basis': ('entry_price_basis', 'next_close'),
            'typed_return': ('expected_return_pct', 999),
            'typed_holding_period': ('suggested_holding_period', 'foreign-period'),
        }[mutation]
        setattr(snapshots[0], field, value)
    elif mutation == 'unverified_model':
        rows[0].normalized_feedback_payload['model_identity']['verified'] = False
    elif mutation == 'unverified_runtime':
        rows[0].normalized_feedback_payload['runtime_contract']['verified'] = False
    elif mutation == 'running_child':
        children[0].status = 'running'
    elif mutation == 'failed_child':
        children[0].status = 'failed'
    elif mutation in {'partial_child', 'partial_malformed_feedback', 'partial_unfinished_child'}:
        children[0].status = 'partial'
        if mutation == 'partial_malformed_feedback':
            rows[0].normalized_feedback_payload.pop('thesis_summary')
        elif mutation == 'partial_unfinished_child':
            children[0].completed_at = None
    elif mutation in {'reused_source', 'wrong_reuse_identity', 'reused_running_child',
                      'reused_unfinished_child', 'reused_wrong_date', 'reused_wrong_cutoff'}:
        row = rows[0]
        children[0].request_payload['parent_run_id'] = 'earlier-valid-parent'
        parent.summary_payload = {
            'reused_symbols_by_role': {row.agent_name: [row.symbol]},
            'reused_feedback_evidence_by_role': {row.agent_name: {row.symbol: {
                'schema_version': 'agent_feedback_reuse_evidence_v1',
                'feedback_id': 999 if mutation == 'wrong_reuse_identity' else row.id,
                'feedback_run_id': row.run_id,
                'feedback_received_at': row.received_at.isoformat(),
            }}},
        }
        if mutation == 'reused_running_child':
            children[0].status = 'running'
        elif mutation == 'reused_unfinished_child':
            children[0].completed_at = None
        elif mutation == 'reused_wrong_date':
            children[0].market_date = datetime(2026, 11, 19).date()
        elif mutation == 'reused_wrong_cutoff':
            children[0].request_payload['knowledge_cutoff_at'] = '2026-11-20T13:00:00+00:00'
    session = MagicMock()
    session.execute.side_effect = [
        MagicMock(), MagicMock(scalar_one_or_none=lambda: parent),
        MagicMock(scalars=lambda: SimpleNamespace(all=lambda: rows)),
        MagicMock(scalars=lambda: SimpleNamespace(all=lambda: children)),
        MagicMock(scalars=lambda: SimpleNamespace(all=lambda: snapshots)),
    ]
    @contextmanager
    def readonly_session(**_):
        yield session
    with patch('app.eod.terminal_completion.session_scope', readonly_session):
        actual = validate_feedback_publications(market_date='2026-11-20', result=result,
                                                source_summary_run_id='summary-run')
    assert actual['valid'] is (mutation in {None, 'reused_source', 'partial_child', 'typed_numeric_rounding',
                                          'typed_wait_projection', 'snapshot_legacy_no_flag', 'warning_match', 'warning_empty_none'})
    assert str(session.execute.call_args_list[0].args[0]) == 'SET TRANSACTION READ ONLY'
    assert all(str(c.args[0]).lstrip().startswith(('SELECT', 'SET TRANSACTION')) for c in session.execute.call_args_list)


def test_legacy_plan_loader_uses_real_plan_columns_and_snapshot_lineage():
    run, items, _ = _scheduled_chain()
    result = run['stages']['next_open_execution_plan']['details']['result_summary']
    rows = [(SignalDeciderExecutionPlanItem(
        id=item['plan_item_id'], account_reference=item['account_reference'], symbol=item['symbol'],
        decision_agent_name=item['decision_agent_name'], decision_snapshot_id=item['decision_snapshot_id'],
    ), item['decision_run_id']) for item in items]
    session = MagicMock()
    session.execute.side_effect = [MagicMock(), SimpleNamespace(all=lambda: rows)]
    @contextmanager
    def readonly_session(**_):
        yield session
    with patch('app.eod.terminal_completion.session_scope', readonly_session):
        actual = load_plan_publication_items(market_date='2026-11-20', result=result)
    assert actual == [{k: v for k, v in item.items() if k in actual[0]} for item in items]


@pytest.mark.parametrize(('proof_error', 'partial_parent'), [
    (None, False), (None, True), (SQLAlchemyError('read failed'), False),
    (SQLAlchemyTimeoutError('read timed out'), False),
    (KeyError('manifest'), False), (TypeError('manifest'), False), (ValueError('manifest'), False),
])
def test_scheduled_pipeline_persists_the_same_terminal_proofs_as_explicit_writer(proof_error, partial_parent):
    run, items, identities = _scheduled_chain(symbol_count=1 if partial_parent else 2)
    results = {key: deepcopy(value['details']['result_summary']) for key, value in run['stages'].items()}
    if partial_parent:
        results['agent_feedback_summaries'] = _partial_summary_result(
            run_id=results['agent_feedback_summaries']['run_id'],
        )
    results['next_open_execution_plan']['items'] = items
    symbols = ['000001', '000002']
    tracker = _FakeDailyPipelineRunStateService()
    service = PykrxBackfillService()
    completed = {'status': 'completed', 'successful_stock_codes': symbols}
    scalar_services = {
        'daily_price_context_service.rebuild_for_market_date': {'contexts_persisted': 2, 'contexts_failed': 0},
        'daily_technical_snapshot_service.rebuild_for_market_date': {'snapshots_persisted': 2, 'snapshots_failed': 0},
        'daily_action_summary_service.rebuild_for_market_date': {'action_summaries_persisted': 2, 'action_summaries_failed': 0},
        'daily_signal_feature_service.refresh_for_market_date': {'features_persisted': 2, 'features_failed': 0},
        'pykrx_enrichment_service.collect_for_market_date': completed,
        'compute_and_persist_daily_trending_ranks': {'trending_ranks_persisted': 2},
        'create_eod_collection_run': 601,
        '_get_market_ohlcv_by_date': SimpleNamespace(empty=False),
        '_get_market_ohlcv_by_ticker': _AnySymbolTurnoverFrame(),
        'persist_daily_price_bars': 2,
        'update_eod_collection_run_progress': None,
        'complete_eod_collection_run': None,
    }
    followups = {
        '_run_daily_piotroski_filter_followup': completed,
        '_run_daily_factor_followup': _completed_factor_followup_result(),
        '_run_daily_external_news_followup': completed,
        '_run_daily_external_news_enrichment_followup': completed,
        '_run_daily_macro_context_followup': completed,
        '_run_terminalized_daily_intraday_snapshot_followup': {'status': 'skipped', 'safe_skip': True, 'reason': 'non_trading_day'},
        '_run_daily_feedback_followup': results['agent_feedback_summaries'],
        '_run_daily_signal_decider_followup': results['signal_decider'],
        '_run_daily_next_open_execution_plan_followup': results['next_open_execution_plan'],
    }
    def validate(**kwargs):
        checked_result = kwargs['result']
        publication = {'valid': True, 'decision_identities': identities if checked_result['roles'] == list(DECISION_SUPPORT_AGENT_NAMES) else []}
        if checked_result.get('status') == 'partial':
            scope = resolve_terminal_role_success_intersection(
                progress_summary=checked_result['progress']['summary'],
                roles=SUMMARIZER_AGENT_NAMES,
            )
            assert scope['ready']
            publication['summary_cutoff_resolution'] = {
                'ready': True, 'source_run_id': checked_result['run_id'],
                'knowledge_cutoff_at': checked_result['knowledge_cutoff_at'],
                'feedback_scope_mode': checked_result['feedback_scope_mode'],
                'requested_symbols': scope['completed_symbols'],
                'completed_scope_proof': {'schema_version': 'completed_summarizer_intersection_v1',
                                          'completed_symbols': scope['completed_symbols'],
                                          'admitted_symbols': scope['admitted_symbols']},
            }
        return publication
    build = DailyPipelineStageService.build_scheduled_terminal_details
    def build_details(**kwargs):
        if proof_error is not None:
            raise proof_error
        return build(**kwargs)
    with ExitStack() as stack:
        for key, value in scalar_services.items():
            stack.enter_context(patch('app.eod.pykrx_backfill.' + key, return_value=value))
        dispatches = {key: stack.enter_context(patch('app.eod.pykrx_pipeline_runner.' + key, return_value=value))
                      for key, value in followups.items()}
        for key in ('_validated_atomic_successful_cohort', '_validated_successful_cohort', '_validated_exact_piotroski_membership_symbols'):
            stack.enter_context(patch('app.eod.pykrx_pipeline_runner.' + key, return_value=symbols))
        stack.enter_context(patch('app.eod.pykrx_pipeline_runner._validated_atomic_scope_partition', return_value={}))
        stack.enter_context(patch('app.eod.pykrx_pipeline_runner._finalize_daily_stage_scope', return_value={'ready': True}))
        stack.enter_context(patch('app.eod.pykrx_backfill.daily_pipeline_run_state_service', tracker))
        stack.enter_context(patch('app.eod.pykrx_backfill.settings.dart_api_key', 'test-key'))
        stack.enter_context(patch('app.eod.pykrx_backfill.settings.kis_app_key', 'test-key'))
        stack.enter_context(patch('app.eod.pykrx_backfill.settings.kis_app_secret', 'test-secret'))
        stack.enter_context(patch('app.eod.pykrx_backfill.settings.openclaw_feedback_trigger_after_fundamentals_refresh', True))
        stack.enter_context(patch('app.eod.pykrx_pipeline_runner.get_market_state', return_value=SimpleNamespace(state='CLOSED', server_time='2026-11-20T21:00:00+09:00')))
        stack.enter_context(patch('app.eod.pykrx_backfill.daily_market_risk_filter_service.rebuild_for_market_date', new=AsyncMock(return_value=completed)))
        stack.enter_context(patch('app.eod.pykrx_backfill.dart_fundamentals_capture_service.run_daily_incremental_refresh', new=AsyncMock(return_value=completed)))
        stack.enter_context(patch.object(service, 'load_active_universe', return_value=[PykrxSymbol(symbol=s, market='KOSPI', name_ko=s) for s in symbols]))
        stack.enter_context(patch.object(service, 'resolve_latest_trade_date', return_value='2026-11-20'))
        stack.enter_context(patch.object(service, '_load_symbol_fundamentals', return_value=None))
        stack.enter_context(patch.object(service, '_map_symbol_bars', return_value=[{'trade_date': '2026-11-20', 'open_price': 1, 'high_price': 2, 'low_price': 1, 'close_price': 2, 'volume': 3, 'turnover': 4}]))
        stack.enter_context(patch.object(DailyPipelineStageService, '_validate_terminal_feedback_publications', side_effect=validate))
        stack.enter_context(patch.object(DailyPipelineStageService, 'build_scheduled_terminal_details', side_effect=build_details))
        pipeline_result = service._run_market_data_pipeline(
            start_date='20261120', end_date='20261120', markets=('KOSPI',),
            trigger='auto_close_scheduler', refresh_universe=False, mode='daily_auto',
            selected_stages=PYKRX_STAGE_SEQUENCE, track_daily_pipeline=True,
        )
    if proof_error is not None:
        summary = next(event for event in reversed(tracker.stage_updates)
                       if event['stage_id'] == 'agent_feedback_summaries')
        assert summary['status'] == 'partial'
        assert 'completion_scope_proof' not in summary['details']
        assert summary['details']['completion_authority_error_type'] == type(proof_error).__name__
        assert pipeline_result['feedback_followup']['status'] == 'partial'
        assert results['agent_feedback_summaries']['status'] == 'partial'
        dispatches['_run_daily_signal_decider_followup'].assert_not_called()
        dispatches['_run_daily_next_open_execution_plan_followup'].assert_not_called()
        return
    stored = {event['stage_id']: event for event in tracker.stage_updates if event['stage_id'] in results and event['status'] == 'completed'}
    assert set(stored) == set(results) - ({'agent_feedback_summaries'} if partial_parent else set())
    if partial_parent:
        summary = next(event for event in reversed(tracker.stage_updates)
                       if event['stage_id'] == 'agent_feedback_summaries')
        assert summary['status'] == pipeline_result['feedback_followup']['status'] == 'partial'
        assert 'completion_scope_proof' not in summary['details']
        assert summary['details']['summary_cutoff_resolution']['requested_symbols'] == ['000001']
        assert results['agent_feedback_summaries']['progress']['summary']['failed_items'] == 1
        assert dispatches['_run_daily_signal_decider_followup'].call_args.kwargs['symbols'] == ['000001']
        assert stored['next_open_execution_plan']['details']['completion_scope_proof']['active_item_count'] == 0
    explicit = MagicMock()
    explicit_service = DailyPipelineStageService(run_state_service=explicit)
    for stage_id, event in stored.items():
        proof = event['details'].get('completion_scope_proof')
        assert DailyPipelineStageService.validated_stage_completion_scope_proof(stage_id, proof) is not None
        explicit_service._finish_tracked_stage(
            pipeline_run_id=682, stage_id=stage_id, market_date='2026-11-20',
            outcome={'status': 'completed'}, result=results[stage_id],
            completion_parent_proof=proof if stage_id == 'signal_decider' else stored['signal_decider']['details']['completion_scope_proof'],
        )
        assert explicit.finish_stage.call_args.kwargs['details']['completion_scope_proof'] == proof
