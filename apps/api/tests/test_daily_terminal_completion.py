from copy import deepcopy
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES
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


def _scheduled_chain():
    market_date = '2026-11-20'
    cutoff = datetime(2026, 11, 20, 12, tzinfo=timezone.utc)
    plan_stage = _completed_next_open_history_stage(
        run_id=681, market_date=market_date, total=2,
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
    summary = _completed_feedback_result(symbols=['000001', '000002'], cutoff_at=cutoff,
                                         run_id=proof['parent_summary_run_id'])
    decider = _completed_decider_result(
        symbols=['000001', '000002'], cutoff_at=cutoff,
        source_summary_run_id=summary['run_id'], run_id=proof['parent_run_id'],
    )
    decider.update(scope_authority=None, knowledge_cutoff_at=cutoff.isoformat())
    decider.pop('decision_identities')
    for result in (summary, decider):
        result['market_date'] = market_date
    stages = {}
    for stage_id, result, count in (
        ('agent_feedback_summaries', summary, 6), ('signal_decider', decider, 4),
        ('next_open_execution_plan', plan, 2),
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
        ))
    return result, parent, feedback, children, snapshots


@pytest.mark.parametrize('mutation', [None, 'foreign_parent', 'wrong_date', 'missing_feedback',
                                    'missing_role', 'malformed_feedback', 'typed_mismatch',
                                    'unverified_model', 'unverified_runtime', 'running_child', 'failed_child',
                                    'partial_child', 'partial_malformed_feedback', 'partial_unfinished_child',
                                    'reused_source', 'wrong_reuse_identity'])
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
    elif mutation in {'reused_source', 'wrong_reuse_identity'}:
        row = rows[0]
        children[0].request_payload['parent_run_id'] = 'earlier-valid-parent'
        parent.summary_payload = {
            'reused_symbols_by_role': {row.agent_name: [row.symbol]},
            'reused_feedback_evidence_by_role': {row.agent_name: {row.symbol: {
                'schema_version': 'agent_feedback_reuse_evidence_v1',
                'feedback_id': row.id if mutation == 'reused_source' else 999,
                'feedback_run_id': row.run_id,
                'feedback_received_at': row.received_at.isoformat(),
            }}},
        }
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
    assert actual['valid'] is (mutation in {None, 'reused_source', 'partial_child'})
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


def test_scheduled_pipeline_persists_the_same_terminal_proofs_as_explicit_writer():
    run, items, identities = _scheduled_chain()
    results = {key: deepcopy(value['details']['result_summary']) for key, value in run['stages'].items()}
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
        return {'valid': True, 'decision_identities': identities if kwargs['result']['roles'] == list(DECISION_SUPPORT_AGENT_NAMES) else []}
    with ExitStack() as stack:
        for key, value in scalar_services.items():
            stack.enter_context(patch('app.eod.pykrx_backfill.' + key, return_value=value))
        for key, value in followups.items():
            stack.enter_context(patch('app.eod.pykrx_pipeline_runner.' + key, return_value=value))
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
        service._run_market_data_pipeline(
            start_date='20261120', end_date='20261120', markets=('KOSPI',),
            trigger='auto_close_scheduler', refresh_universe=False, mode='daily_auto',
            selected_stages=PYKRX_STAGE_SEQUENCE, track_daily_pipeline=True,
        )
    stored = {event['stage_id']: event for event in tracker.stage_updates if event['stage_id'] in results and event['status'] == 'completed'}
    assert set(stored) == set(results)
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
