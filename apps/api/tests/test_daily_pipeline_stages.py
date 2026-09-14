import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES, SUMMARIZER_AGENT_NAMES
from app.eod.daily_pipeline_state import _build_stage_duration_details
from app.eod.daily_pipeline_stages import FACTOR_STAGE_ORDER, DailyPipelineStageService


def _feedback_prerequisites(*, status: str = 'ready', missing_stage_ids: list[str] | None = None) -> dict:
    missing_stage_ids = list(missing_stage_ids or [])
    ready = status == 'ready'
    return {
        'market_date': '2026-04-14',
        'fs_div': 'CFS',
        'status': status,
        'ready': ready,
        'required_stage_ids': ['fundamentals_daily_refresh', 'market_risk_filter', 'piotroski_filter', *FACTOR_STAGE_ORDER],
        'ready_stage_ids': [] if not ready else ['fundamentals_daily_refresh', 'market_risk_filter', 'piotroski_filter', *FACTOR_STAGE_ORDER],
        'waiting_stage_ids': missing_stage_ids if status == 'waiting' else [],
        'blocked_stage_ids': missing_stage_ids if status == 'blocked' else [],
        'missing_prerequisites': [
            {
                'stage': stage_id,
                'label': stage_id.replace('_', ' '),
                'status': 'running' if status == 'waiting' else 'partial',
                'gate_status': status,
                'current_activity': f'{status} {stage_id}',
                'details': {},
            }
            for stage_id in missing_stage_ids
        ],
        'stages': {},
    }


class _FakePykrxStageRunner:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def run_daily_pipeline_stages(self, **kwargs):
        self.calls.append(kwargs)
        return {
            'run_id': 901,
            'status': 'completed',
            'latest_available_trade_date': kwargs['market_date'],
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'bars_upserted': 2,
            'technical_snapshots': {
                'market_date': kwargs['market_date'],
                'snapshots_persisted': 2,
                'snapshots_failed': 0,
            },
        }


class _FakeFeedbackRetryService:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def run_retry_cycles(
        self,
        *,
        config,
        force: bool = False,
        bypass_prerequisite_gate: bool = False,
    ):
        self.calls.append(
            {
                'config': config,
                'force': force,
                'bypass_prerequisite_gate': bypass_prerequisite_gate,
            }
        )
        if config.progress_callback is not None:
            config.progress_callback(
                status='running',
                current_step='role_started',
                current_activity='Agent feedback role started',
                processed=1,
                total=4,
                succeeded=1,
                failed=0,
                current_period=config.roles[0],
                source_run_ids={'agent_feedback_run_id': 'fake-feedback-run'},
            )
        return {
            'completed': True,
            'final_pending': 0,
            'roles': list(config.roles),
            'requested_symbols': list(config.symbols) if config.symbols is not None else None,
        }


class _FakeSignalDeciderEvaluationService:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def compute_recent_evaluations(self, **kwargs):
        self.calls.append(kwargs)
        return {
            'request': {
                'as_of_market_date': kwargs['as_of_market_date'],
                'lookback_days': kwargs['lookback_days'],
            },
            'summary': {
                'outcomes_upserted': 4,
                'evaluations_upserted': 4,
            },
            'items': [],
        }


class _FakePipelineRunStateService:
    def __init__(self) -> None:
        self.next_run_id = 9000
        self.runs: list[dict] = []
        self.stage_events: list[dict] = []

    def start_run(self, **kwargs):
        run_id = self.next_run_id
        self.next_run_id += 1
        payload = {
            'id': run_id,
            **kwargs,
            'status': 'running',
            'stages': {},
        }
        self.runs.append(payload)
        return payload

    def start_stage(self, **kwargs):
        self.stage_events.append({'event': 'start_stage', **kwargs})
        return {'id': len(self.stage_events), **kwargs, 'status': 'running'}

    def update_stage(self, **kwargs):
        self.stage_events.append({'event': 'update_stage', **kwargs})
        return {'id': len(self.stage_events), **kwargs}

    def finish_stage(self, **kwargs):
        self.stage_events.append({'event': 'finish_stage', **kwargs})
        return {'id': len(self.stage_events), **kwargs}

    def finish_run(self, **kwargs):
        self.runs[-1]['status'] = kwargs.get('status')
        self.runs[-1]['finish'] = kwargs
        return {**self.runs[-1], **kwargs}


class _FakeDeduplicatedRecoveryRunStateService(_FakePipelineRunStateService):
    def start_run(self, **kwargs):
        return {
            'id': 315,
            'market_date': kwargs['market_date'],
            'trigger': 'monitor_stale_continuation',
            'status': 'completed',
            'stages': {'signal_decider_evaluations': {'status': 'completed'}},
            'deduplicated': True,
            'deduplication_reason': 'recovery_already_completed',
        }


class _FakeSkippedPykrxStageRunner:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def run_daily_pipeline_stages(self, **kwargs):
        self.calls.append(kwargs)
        return {
            'triggered': False,
            'reason': 'already_running',
            'market_date': kwargs['market_date'],
            'latest_available_trade_date': kwargs['market_date'],
            'source': 'pykrx',
        }


class _FakePartiallySuccessfulPykrxStageRunner:
    def run_daily_pipeline_stages(self, **kwargs):
        return {
            'run_id': 902,
            'status': 'completed_with_errors',
            'latest_available_trade_date': kwargs['market_date'],
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'bars_upserted': 2,
            'technical_snapshots': {
                'market_date': kwargs['market_date'],
                'status': 'partial',
                'snapshots_persisted': 1,
                'snapshots_failed': 1,
            },
        }


class DailyPipelineStageServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.run_state_service = _FakePipelineRunStateService()
        self.run_state_patcher = patch(
            'app.eod.daily_pipeline_stages.daily_pipeline_run_state_service',
            self.run_state_service,
        )
        self.run_state_patcher.start()
        self.addCleanup(self.run_state_patcher.stop)
        self.prerequisite_patcher = patch(
            'app.eod.daily_pipeline_stages.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
            side_effect=lambda **_: _feedback_prerequisites(),
        )
        self.prerequisite_patcher.start()
        self.addCleanup(self.prerequisite_patcher.stop)

    def test_describe_returns_canonical_stage_order_and_default_sequence(self) -> None:
        service = DailyPipelineStageService()

        payload = service.describe()

        self.assertEqual(
            payload['stage_order'][:7],
            [
                'master_refresh',
                'pykrx_universe_refresh',
                'price_collection',
                'pykrx_enrichment',
                'technical_snapshots',
                'action_summaries',
                'trending_ranks',
            ],
        )
        self.assertIn('fundamentals_daily_refresh', payload['default_daily_stage_order'])
        self.assertIn('momentum_factors', payload['default_daily_stage_order'])
        self.assertIn('composite_factors', payload['default_daily_stage_order'])
        self.assertIn('external_news_collect', payload['default_daily_stage_order'])
        self.assertIn('external_news_enrich', payload['default_daily_stage_order'])
        self.assertIn('macro_context_refresh', payload['default_daily_stage_order'])
        self.assertLess(
            payload['default_daily_stage_order'].index('external_news_collect'),
            payload['default_daily_stage_order'].index('external_news_enrich'),
        )
        self.assertLess(
            payload['default_daily_stage_order'].index('external_news_enrich'),
            payload['default_daily_stage_order'].index('macro_context_refresh'),
        )
        self.assertLess(
            payload['default_daily_stage_order'].index('macro_context_refresh'),
            payload['default_daily_stage_order'].index('signal_decider'),
        )
        self.assertIn('signal_decider', payload['default_daily_stage_order'])
        self.assertLess(
            payload['default_daily_stage_order'].index('signal_decider'),
            payload['default_daily_stage_order'].index('next_open_execution_plan'),
        )
        self.assertLess(
            payload['default_daily_stage_order'].index('next_open_execution_plan'),
            payload['default_daily_stage_order'].index('signal_decider_evaluations'),
        )
        self.assertIn('unified_value_factors', payload['stage_order'])
        self.assertIn('quality_factors', payload['stage_order'])

        unified_value_stage = next(
            stage
            for stage in payload['stages']
            if stage['id'] == 'unified_value_factors'
        )
        self.assertEqual(
            unified_value_stage['depends_on'],
            ['price_collection', 'fundamentals_daily_refresh'],
        )
        quality_stage = next(
            stage
            for stage in payload['stages']
            if stage['id'] == 'quality_factors'
        )
        self.assertEqual(
            quality_stage['depends_on'],
            ['price_collection', 'fundamentals_daily_refresh'],
        )

        advanced_value_stage = next(
            stage
            for stage in payload['stages']
            if stage['id'] == 'advanced_value_factors'
        )
        self.assertEqual(
            advanced_value_stage['depends_on'],
            ['price_collection', 'fundamentals_daily_refresh'],
        )
        composite_stage = next(
            stage
            for stage in payload['stages']
            if stage['id'] == 'composite_factors'
        )
        self.assertEqual(
            composite_stage['depends_on'],
            [
                'momentum_factors',
                'value_factors',
                'low_risk_factors',
                'beta_factors',
                'advanced_value_factors',
            ],
        )
        self.assertTrue(composite_stage['default_selected'])
        external_news_stage = next(
            stage
            for stage in payload['stages']
            if stage['id'] == 'external_news_collect'
        )
        self.assertEqual(
            external_news_stage['depends_on'],
            ['market_risk_filter', 'piotroski_filter', *FACTOR_STAGE_ORDER],
        )
        external_news_enrich_stage = next(
            stage
            for stage in payload['stages']
            if stage['id'] == 'external_news_enrich'
        )
        self.assertEqual(external_news_enrich_stage['depends_on'], ['external_news_collect'])
        macro_context_stage = next(
            stage
            for stage in payload['stages']
            if stage['id'] == 'macro_context_refresh'
        )
        self.assertEqual(macro_context_stage['depends_on'], ['external_news_enrich'])
        agent_feedback_stage = next(
            stage
            for stage in payload['stages']
            if stage['id'] == 'agent_feedback_summaries'
        )
        self.assertEqual(
            agent_feedback_stage['depends_on'],
            [
                'market_risk_filter',
                'piotroski_filter',
                *FACTOR_STAGE_ORDER,
                'external_news_collect',
                'external_news_enrich',
                'macro_context_refresh',
            ],
        )
        self.assertTrue(external_news_stage['default_selected'])
        self.assertTrue(external_news_enrich_stage['default_selected'])
        self.assertTrue(macro_context_stage['default_selected'])

    def test_stage_duration_details_marks_slow_stage(self) -> None:
        started_at = datetime(2026, 7, 21, 9, 0, 0, tzinfo=timezone.utc)
        completed_at = datetime(2026, 7, 21, 9, 6, 1, 123000, tzinfo=timezone.utc)

        details = _build_stage_duration_details(started_at=started_at, completed_at=completed_at)

        self.assertEqual(details['duration_ms'], 361123)
        self.assertEqual(details['duration_seconds'], 361.123)
        self.assertTrue(details['slow_stage'])
        self.assertEqual(details['slow_stage_threshold_ms'], 300000)

    def test_monitor_recovery_returns_existing_run_without_executing_stages(self) -> None:
        pykrx_runner = _FakePykrxStageRunner()
        service = DailyPipelineStageService(
            pykrx_service=pykrx_runner,
            run_state_service=_FakeDeduplicatedRecoveryRunStateService(),
        )

        result = service.run(
            market_date='2026-08-21',
            stages=['price_collection'],
            trigger='monitor_stale_continuation',
            force=True,
        )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['reason'], 'recovery_already_completed')
        self.assertEqual(result['pipeline_run_id'], 315)
        self.assertEqual(pykrx_runner.calls, [])

    def test_run_reorders_requested_subset_and_splits_feedback_stages(self) -> None:
        pykrx_runner = _FakePykrxStageRunner()
        feedback_retry_service = _FakeFeedbackRetryService()
        evaluation_service = _FakeSignalDeciderEvaluationService()
        service = DailyPipelineStageService(
            pykrx_service=pykrx_runner,
            feedback_retry_service=feedback_retry_service,
            decision_loop_service=evaluation_service,
        )

        with (
            patch.object(service, '_execute_stage', side_effect=service._execute_stage) as execute_stage,
            patch.object(service, '_normalize_market_date', return_value='2026-04-14'),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'signal_decider_evaluations',
                    'signal_decider',
                    'technical_snapshots',
                    'agent_feedback_summaries',
                    'price_collection',
                ],
                symbols=['005930', '000660'],
            )

        self.assertEqual(
            result['resolved_stages'],
            [
                'price_collection',
                'technical_snapshots',
                'agent_feedback_summaries',
                'signal_decider',
                'signal_decider_evaluations',
            ],
        )
        self.assertEqual(pykrx_runner.calls[0]['stages'], ['price_collection', 'technical_snapshots'])
        self.assertEqual(pykrx_runner.calls[0]['symbols'], ['005930', '000660'])
        self.assertEqual(feedback_retry_service.calls[0]['config'].roles, SUMMARIZER_AGENT_NAMES)
        self.assertEqual(feedback_retry_service.calls[1]['config'].roles, DECISION_SUPPORT_AGENT_NAMES)
        for feedback_call in feedback_retry_service.calls:
            cutoff = feedback_call['config'].knowledge_cutoff_at
            self.assertIsInstance(cutoff, datetime)
            self.assertIsNotNone(cutoff.utcoffset())
            self.assertLessEqual(cutoff, datetime.now(timezone.utc))
        self.assertEqual(evaluation_service.calls[0]['as_of_market_date'], '2026-04-14')
        self.assertEqual(evaluation_service.calls[0]['symbols'], ['005930', '000660'])
        self.assertGreaterEqual(execute_stage.call_count, 3)

    def test_run_uses_default_daily_stage_order_including_factor_stages(self) -> None:
        pykrx_runner = _FakePykrxStageRunner()
        service = DailyPipelineStageService(pykrx_service=pykrx_runner)

        with (
            patch.object(service, '_normalize_market_date', return_value='2026-04-14'),
            patch.object(service, '_execute_stage', side_effect=lambda stage_id, **_: {'stage_id': stage_id}) as execute_stage,
        ):
            result = service.run(market_date='2026-04-14')

        self.assertEqual(
            result['resolved_stages'],
            service.describe()['default_daily_stage_order'],
        )
        self.assertEqual(
            pykrx_runner.calls[0]['stages'],
            [
                'price_collection',
                'technical_snapshots',
                'action_summaries',
                'trending_ranks',
                'signal_features',
                'market_risk_filter',
            ],
        )
        executed_stage_ids = [call.args[0] for call in execute_stage.call_args_list]
        self.assertEqual(
            executed_stage_ids[:4],
            [
                'master_refresh',
                'pykrx_universe_refresh',
                'pykrx_enrichment',
                'fundamentals_daily_refresh',
            ],
        )
        self.assertEqual(
            executed_stage_ids[4:13],
            [
                'piotroski_filter',
                'momentum_factors',
                'value_factors',
                'low_risk_factors',
                'beta_factors',
                'advanced_value_factors',
                'unified_value_factors',
                'quality_factors',
                'composite_factors',
            ],
        )
        self.assertEqual(executed_stage_ids[13], 'external_news_collect')
        self.assertEqual(executed_stage_ids[14], 'external_news_enrich')
        self.assertEqual(executed_stage_ids[15], 'macro_context_refresh')

    def test_run_resume_from_market_risk_filter_preserves_upstream_outputs_by_not_rerunning_them(self) -> None:
        service = DailyPipelineStageService()

        with (
            patch.object(service, '_normalize_market_date', return_value='2026-04-14'),
            patch.object(service, '_execute_stage', side_effect=lambda stage_id, **_: {'stage_id': stage_id}) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                resume_from_stage='market_risk_filter',
            )

        self.assertEqual(result['resolved_stages'][0], 'market_risk_filter')
        self.assertNotIn('price_collection', result['resolved_stages'])
        self.assertNotIn('technical_snapshots', result['resolved_stages'])
        self.assertEqual(execute_stage.call_args_list[0].args[0], 'market_risk_filter')
        self.assertEqual(self.run_state_service.runs[0]['run_metadata']['resume_from_stage'], 'market_risk_filter')

    def test_run_resume_from_market_risk_snapshots_includes_requested_resume_stage(self) -> None:
        service = DailyPipelineStageService()

        with (
            patch.object(service, '_normalize_market_date', return_value='2026-04-14'),
            patch.object(service, '_execute_stage', side_effect=lambda stage_id, **_: {'stage_id': stage_id}) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                resume_from_stage='market_risk_snapshots',
            )

        self.assertEqual(result['resolved_stages'][0], 'market_risk_snapshots')
        self.assertIn('market_risk_snapshots', result['requested_stages'])
        self.assertEqual(execute_stage.call_args_list[0].args[0], 'market_risk_snapshots')
        self.assertEqual(self.run_state_service.runs[0]['run_metadata']['resume_from_stage'], 'market_risk_snapshots')

    def test_run_starting_at_market_risk_filter_checkpoints_progress_before_rows_exist(self) -> None:
        service = DailyPipelineStageService()

        def _execute(stage_id, **kwargs):
            kwargs['progress_callback'](
                status='running',
                current_step='fetching_kis_market_risk_snapshots',
                current_activity='Fetching KIS market-risk snapshot for 005930',
                processed=1,
                total=2,
                succeeded=1,
                failed=0,
                current_symbol='005930',
            )
            return {
                'market_date': kwargs['market_date'],
                'matched_symbols': ['005930', '000660'],
                'results_persisted': 0,
                'results_failed': 0,
                'items': [],
            }

        with (
            patch.object(service, '_normalize_market_date', return_value='2026-04-14'),
            patch.object(service, '_execute_stage', side_effect=_execute),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['market_risk_filter'],
                symbols=['005930', '000660'],
            )

        self.assertEqual(result['status'], 'completed')
        progress_events = [
            event
            for event in self.run_state_service.stage_events
            if event['event'] == 'update_stage' and event['stage_id'] == 'market_risk_filter'
        ]
        self.assertEqual(progress_events[0]['status'], 'running')
        self.assertEqual(progress_events[0]['processed'], 1)
        self.assertEqual(progress_events[0]['total'], 2)
        self.assertEqual(progress_events[0]['current_symbol'], '005930')

    def test_run_gates_feedback_stage_until_prerequisites_are_ready(self) -> None:
        pykrx_runner = _FakePykrxStageRunner()
        feedback_retry_service = _FakeFeedbackRetryService()
        service = DailyPipelineStageService(
            pykrx_service=pykrx_runner,
            feedback_retry_service=feedback_retry_service,
        )

        with patch(
            'app.eod.daily_pipeline_stages.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
            side_effect=lambda **_: _feedback_prerequisites(
                status='waiting',
                missing_stage_ids=['fundamentals_daily_refresh', 'quality_factors'],
            ),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['agent_feedback_summaries'],
            )

        self.assertEqual(result['status'], 'waiting')
        self.assertEqual(result['stages']['agent_feedback_summaries']['status'], 'waiting')
        self.assertEqual(
            result['stages']['agent_feedback_summaries']['result']['reason'],
            'feedback_prerequisites_not_ready',
        )
        self.assertEqual(feedback_retry_service.calls, [])

    def test_fundamentals_stage_checkpoints_dart_progress(self) -> None:
        service = DailyPipelineStageService()

        async def _fake_refresh(**kwargs):
            kwargs['progress_callback'](
                status='running',
                current_step='syncing_disclosures',
                current_activity='DART fundamentals daily refresh: syncing disclosures',
                processed=1,
                total=3,
                succeeded=1,
                failed=0,
                current_symbol='005930',
                current_period='CFS',
                source_run_ids={'dart_refresh_run_id': 77},
            )
            return {
                'triggered': True,
                'run_id': 77,
                'status': 'completed',
                'symbols_refreshed': 3,
                'symbols_failed': 0,
                'events_captured': 3,
                'events_failed': 0,
            }

        with (
            patch.object(service, '_normalize_market_date', return_value='2026-04-14'),
            patch(
                'app.eod.daily_pipeline_stages.dart_fundamentals_capture_service.run_daily_incremental_refresh',
                side_effect=_fake_refresh,
            ),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['fundamentals_daily_refresh'],
            )

        self.assertEqual(result['status'], 'completed')
        progress_events = [
            event
            for event in self.run_state_service.stage_events
            if event['event'] == 'update_stage' and event['stage_id'] == 'fundamentals_daily_refresh'
        ]
        self.assertEqual(progress_events[0]['current_step'], 'syncing_disclosures')
        self.assertEqual(progress_events[0]['processed'], 1)
        self.assertEqual(progress_events[0]['source_run_ids']['dart_refresh_run_id'], 77)

    def test_agent_feedback_stage_checkpoints_retry_progress(self) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        service = DailyPipelineStageService(feedback_retry_service=feedback_retry_service)

        result = service.run(
            market_date='2026-04-14',
            stages=['agent_feedback_summaries'],
            symbols=['005930', '000660'],
        )

        self.assertEqual(result['status'], 'completed')
        self.assertIsNotNone(feedback_retry_service.calls[0]['config'].progress_callback)
        progress_events = [
            event
            for event in self.run_state_service.stage_events
            if event['event'] == 'update_stage' and event['stage_id'] == 'agent_feedback_summaries'
        ]
        self.assertEqual(progress_events[0]['current_step'], 'role_started')
        self.assertEqual(progress_events[0]['processed'], 1)
        self.assertEqual(progress_events[0]['source_run_ids']['agent_feedback_run_id'], 'fake-feedback-run')

    def test_forced_feedback_recovery_reuses_existing_rows(self) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        service = DailyPipelineStageService(feedback_retry_service=feedback_retry_service)

        result = service.run(
            market_date='2026-04-14',
            stages=['agent_feedback_summaries'],
            force=True,
            bypass_prerequisite_gate=True,
            reuse_existing_feedback=True,
        )

        self.assertEqual(result['status'], 'completed')
        call = feedback_retry_service.calls[0]
        self.assertFalse(call['force'])
        self.assertTrue(call['bypass_prerequisite_gate'])
        self.assertTrue(call['config'].reuse_existing)

    def test_run_rejects_unknown_stage(self) -> None:
        service = DailyPipelineStageService()

        with self.assertRaisesRegex(ValueError, 'Unknown daily stage'):
            service.run(market_date='2026-04-14', stages=['not-a-stage'])

    def test_run_marks_grouped_pykrx_skip_payloads_as_skipped(self) -> None:
        pykrx_runner = _FakeSkippedPykrxStageRunner()
        service = DailyPipelineStageService(pykrx_service=pykrx_runner)

        result = service.run(
            market_date='2026-04-14',
            stages=['price_collection', 'technical_snapshots'],
        )

        self.assertEqual(result['status'], 'skipped')
        self.assertEqual(result['stages']['price_collection']['status'], 'skipped')
        self.assertEqual(result['stages']['technical_snapshots']['status'], 'skipped')
        self.assertFalse(result['stages']['price_collection']['result']['triggered'])
        self.assertEqual(result['stages']['technical_snapshots']['result']['reason'], 'already_running')

    def test_resolve_result_status_marks_error_only_payload_failed(self) -> None:
        status = DailyPipelineStageService._resolve_result_status({'error': 'boom'})

        self.assertEqual(status, 'failed')

    def test_resolve_result_status_marks_error_payload_with_progress_partial(self) -> None:
        status = DailyPipelineStageService._resolve_result_status(
            {
                'error': 'boom',
                'trending_ranks_persisted': 2,
            }
        )

        self.assertEqual(status, 'partial')

    def test_resolve_result_status_downgrades_completed_feedback_with_failures(self) -> None:
        status = DailyPipelineStageService._resolve_result_status(
            {
                'status': 'completed',
                'completed': True,
                'final_pending': 0,
                'progress': {
                    'summary': {
                        'expected_feedback_count': 160,
                        'completed_feedback_count': 60,
                        'failed_items': 100,
                    }
                },
            }
        )

        self.assertEqual(status, 'partial')

    def test_resolve_result_status_normalizes_ok_to_completed(self) -> None:
        status = DailyPipelineStageService._resolve_result_status({'status': 'ok'})

        self.assertEqual(status, 'completed')

    def test_resolve_result_status_downgrades_ok_with_failures(self) -> None:
        status = DailyPipelineStageService._resolve_result_status(
            {'status': 'ok', 'summary': {'failed_count': 1}}
        )

        self.assertEqual(status, 'partial')

    def test_resolve_result_status_accepts_failed_item_collections(self) -> None:
        status = DailyPipelineStageService._resolve_result_status(
            {'error': 'some items failed', 'failed_items': ['005930']}
        )

        self.assertEqual(status, 'partial')

    def test_extract_stage_counts_uses_external_news_collection_summary(self) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='external_news_collect',
            result={
                'status': 'ok',
                'summary': {
                    'eligible_symbol_count': 201,
                    'collected_symbol_count': 201,
                    'items_inserted': 252,
                },
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 201,
                'total': 201,
                'succeeded': 201,
                'failed': 0,
            },
        )

    def test_extract_stage_counts_preserves_explicit_zero_eligible_news_scope(self) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='external_news_collect',
            result={
                'status': 'disabled',
                'summary': {
                    'requested_symbol_count': 201,
                    'eligible_symbol_count': 0,
                    'collected_symbol_count': 0,
                },
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 0,
                'total': 0,
                'succeeded': 0,
                'failed': 0,
            },
        )

    def test_extract_stage_counts_uses_external_news_enrichment_result(self) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='external_news_enrich',
            result={
                'status': 'partial',
                'items_inspected': 703,
                'items_updated': 701,
                'items_missing_classification': 3,
                'items_remaining_at_least': 1,
                'missing_item_ids': [11, 12],
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 703,
                'total': 704,
                'succeeded': 701,
                'failed': 2,
            },
        )

    def test_extract_stage_counts_keeps_news_failures_beyond_id_preview(self) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='external_news_enrich',
            result={
                'status': 'partial',
                'items_inspected': 120,
                'items_missing_classification': 101,
                'items_remaining_at_least': 1,
                'missing_item_ids': list(range(50)),
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 120,
                'total': 121,
                'succeeded': 20,
                'failed': 100,
            },
        )

    def test_build_stage_run_details_marks_external_news_total_as_lower_bound(self) -> None:
        details = DailyPipelineStageService._build_stage_run_details(
            stage_id='external_news_enrich',
            result={
                'status': 'partial',
                'items_inspected': 703,
                'items_remaining_at_least': 1,
            },
        )

        self.assertEqual(
            details['count_semantics'],
            {
                'total': 'lower_bound',
                'total_at_least': 704,
                'remaining': 'lower_bound',
                'remaining_at_least': 1,
            },
        )

    def test_explicit_zero_stage_scope_rejects_lossy_or_boolean_counts(self) -> None:
        self.assertFalse(
            DailyPipelineStageService._has_explicit_zero_stage_scope(
                stage_id='external_news_collect',
                result={'summary': {'eligible_symbol_count': 0.5}},
            )
        )
        self.assertFalse(
            DailyPipelineStageService._has_explicit_zero_stage_scope(
                stage_id='external_news_enrich',
                result={
                    'items_inspected': False,
                    'items_remaining_at_least': 0,
                },
            )
        )

    def test_execute_stage_routes_macro_context_refresh_to_service(self) -> None:
        service = DailyPipelineStageService()

        with patch(
            'app.eod.daily_pipeline_stages.macro_context_service.refresh_context',
            return_value={'status': 'completed', 'observations_upserted': 3, 'observations_failed': 0},
        ) as refresh_context:
            result = service._execute_stage(
                stage_id='macro_context_refresh',
                market_date='2026-04-24',
                symbols=['005930', '000660'],
                markets=None,
                force=True,
                bypass_prerequisite_gate=False,
                fs_div='CFS',
                symbol_limit=None,
                symbol_offset=0,
                capture_limit=500,
                retry_cycles=None,
                feedback_max_cycles=3,
                feedback_outer_batch_size=20,
                reuse_existing_feedback=True,
            )

        self.assertEqual(result['observations_upserted'], 3)
        self.assertEqual(
            refresh_context.call_args.kwargs,
            {
                'force': True,
                'trigger': 'daily_pipeline_stage',
            },
        )

    def test_execute_stage_routes_unified_value_factor_stage_to_service(self) -> None:
        service = DailyPipelineStageService()

        with patch(
            'app.eod.daily_pipeline_stages.daily_unified_value_factor_service.rebuild_for_market_date',
            return_value={'results_persisted': 2},
        ) as compute_unified_value:
            result = service._execute_stage(
                stage_id='unified_value_factors',
                market_date='2026-04-24',
                symbols=['005930', '000660'],
                markets=None,
                force=False,
                bypass_prerequisite_gate=False,
                fs_div='CFS',
                symbol_limit=None,
                symbol_offset=0,
                capture_limit=500,
                retry_cycles=None,
                feedback_max_cycles=3,
                feedback_outer_batch_size=20,
                reuse_existing_feedback=True,
            )

        self.assertEqual(result, {'results_persisted': 2})
        self.assertEqual(compute_unified_value.call_args.kwargs['market_date'], '2026-04-24')
        self.assertEqual(compute_unified_value.call_args.kwargs['symbols'], ['005930', '000660'])

    def test_execute_stage_routes_quality_factor_stage_to_service(self) -> None:
        service = DailyPipelineStageService()

        with patch(
            'app.eod.daily_pipeline_stages.daily_quality_factor_service.rebuild_for_market_date',
            return_value={'results_persisted': 2},
        ) as compute_quality:
            result = service._execute_stage(
                stage_id='quality_factors',
                market_date='2026-04-24',
                symbols=['005930', '000660'],
                markets=None,
                force=False,
                bypass_prerequisite_gate=False,
                fs_div='CFS',
                symbol_limit=None,
                symbol_offset=0,
                capture_limit=500,
                retry_cycles=None,
                feedback_max_cycles=3,
                feedback_outer_batch_size=20,
                reuse_existing_feedback=True,
            )

        self.assertEqual(result, {'results_persisted': 2})
        self.assertEqual(compute_quality.call_args.kwargs['market_date'], '2026-04-24')
        self.assertEqual(compute_quality.call_args.kwargs['symbols'], ['005930', '000660'])

    def test_execute_stage_refreshes_next_open_plan_after_decider(self) -> None:
        service = DailyPipelineStageService()

        with patch(
            'app.orders.next_open_execution.signal_decider_next_open_execution_service.refresh_plan_items',
            return_value={'summary': {'items_total': 2, 'eligible_count': 1}},
        ) as refresh_plan:
            result = service._execute_stage(
                stage_id='next_open_execution_plan',
                market_date='2026-04-14',
                symbols=None,
                markets=None,
                force=False,
                bypass_prerequisite_gate=False,
                fs_div='CFS',
                symbol_limit=None,
                symbol_offset=0,
                capture_limit=500,
                retry_cycles=None,
                feedback_max_cycles=3,
                feedback_outer_batch_size=20,
                reuse_existing_feedback=True,
            )

        self.assertEqual(result['summary']['items_total'], 2)
        refresh_plan.assert_called_once_with(
            target_trade_date='2026-04-15',
            trigger='daily_pipeline_post_decider',
            force=False,
        )

    def test_next_open_plan_stage_counts_eligible_and_blocked_rows(self) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='next_open_execution_plan',
            result={
                'summary': {
                    'items_total': 18,
                    'eligible_count': 4,
                    'monitor_candidate_count': 10,
                    'blocked_count': 4,
                }
            },
        )

        self.assertEqual(
            counts,
            {'processed': 18, 'total': 18, 'succeeded': 18, 'failed': 0},
        )

    def test_price_stage_ignores_unrelated_grouped_stage_failure(self) -> None:
        service = DailyPipelineStageService()

        result = service._extract_pykrx_stage_result(
            'price_collection',
            {
                'run_id': 901,
                'status': 'completed_with_errors',
                'latest_available_trade_date': '2026-04-14',
                'symbols_total': 2,
                'symbols_succeeded': 2,
                'symbols_failed': 0,
                'technical_snapshots': {
                    'status': 'partial',
                    'snapshots_persisted': 1,
                    'snapshots_failed': 1,
                },
            },
        )

        self.assertEqual(result['status'], 'completed')

    def test_grouped_technical_failure_does_not_block_enrichment_after_price_success(self) -> None:
        enrichment_service = MagicMock()
        enrichment_service.collect_for_market_date.return_value = {
            'status': 'completed',
            'categories_requested': [],
            'categories': {},
            'coverage': {},
        }
        service = DailyPipelineStageService(
            pykrx_service=_FakePartiallySuccessfulPykrxStageRunner(),
            pykrx_enrichment_service_ref=enrichment_service,
        )

        with patch(
            'app.eod.daily_pipeline_stages.build_pykrx_enrichment_preflight',
            return_value={'allowed': True, 'failed_categories': []},
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'price_collection',
                    'pykrx_enrichment',
                    'technical_snapshots',
                ],
            )

        self.assertEqual(result['stages']['price_collection']['status'], 'completed')
        self.assertEqual(result['stages']['technical_snapshots']['status'], 'partial')
        self.assertEqual(result['stages']['pykrx_enrichment']['status'], 'completed')
        enrichment_service.collect_for_market_date.assert_called_once()

    def test_execute_pykrx_enrichment_blocks_zero_required_coverage(self) -> None:
        required_categories = (
            'sector_classification',
            'investor_flow_symbol',
            'short_selling',
        )
        collection_result = {
            'status': 'completed',
            'categories_requested': list(required_categories),
            'categories': {
                category: {'status': 'completed', 'rows_persisted': 0, 'errors': []}
                for category in required_categories
            },
            'coverage': {
                'categories': {
                    category: {
                        'scope_type': 'symbol',
                        'expected_rows': 1,
                        'present_rows': 0,
                        'coverage_ratio': 0.0,
                        'coverage_status': 'missing',
                    }
                    for category in required_categories
                }
            },
        }
        enrichment_service = MagicMock()
        enrichment_service.collect_for_market_date.return_value = collection_result
        service = DailyPipelineStageService(
            pykrx_enrichment_service_ref=enrichment_service,
        )

        result = service._execute_stage(
            stage_id='pykrx_enrichment',
            market_date='2026-04-14',
            symbols=['005930'],
            markets=['KOSPI'],
            force=False,
            bypass_prerequisite_gate=False,
            fs_div='CFS',
            symbol_limit=None,
            symbol_offset=0,
            capture_limit=500,
            retry_cycles=None,
            feedback_max_cycles=3,
            feedback_outer_batch_size=20,
            reuse_existing_feedback=True,
        )

        self.assertEqual(result['status'], 'partial')
        self.assertFalse(result['downstream_preflight']['allowed'])
        self.assertEqual(
            set(result['downstream_preflight']['failed_categories']),
            set(required_categories),
        )

    def test_recovered_stage_status_is_terminal_completed(self) -> None:
        self.assertEqual(
            DailyPipelineStageService._resolve_result_status(
                {'status': 'recovered'}
            ),
            'completed',
        )

    def test_run_blocks_next_open_plan_when_signal_decider_is_incomplete(self) -> None:
        service = DailyPipelineStageService()

        def execute(stage_id, **_):
            if stage_id == 'signal_decider':
                return {'status': 'blocked', 'reason': 'feedback_prerequisites_not_ready'}
            raise AssertionError('next-open plan refresh must not run after a blocked decider')

        with (
            patch.object(service, '_normalize_market_date', return_value='2026-04-14'),
            patch.object(service, '_execute_stage', side_effect=execute) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['signal_decider', 'next_open_execution_plan'],
            )

        self.assertEqual(result['stages']['signal_decider']['status'], 'blocked')
        self.assertEqual(result['stages']['next_open_execution_plan']['status'], 'blocked')
        self.assertEqual(
            result['stages']['next_open_execution_plan']['result']['reason'],
            'signal_decider_stage_incomplete',
        )
        self.assertEqual([call.args[0] for call in execute_stage.call_args_list], ['signal_decider'])


if __name__ == '__main__':
    unittest.main()
