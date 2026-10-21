import threading
import unittest
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES, SUMMARIZER_AGENT_NAMES
from app.agent_context.run_modes import (
    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
)
from app.core.settings import settings
from app.eod.daily_pipeline_outcomes import daily_pipeline_outcome_is_satisfied
from app.eod.daily_pipeline_state import _build_stage_duration_details
from app.eod.daily_pipeline_stages import (
    CURRENT_FEEDBACK_PREREQUISITE_STAGE_ORDER,
    FACTOR_STAGE_ORDER,
    DailyPipelineStageService,
)


def _feedback_prerequisites(*, status: str = 'ready', missing_stage_ids: list[str] | None = None) -> dict:
    missing_stage_ids = list(missing_stage_ids or [])
    ready = status == 'ready'
    return {
        'market_date': '2026-04-14',
        'fs_div': 'CFS',
        'status': status,
        'ready': ready,
        'required_stage_ids': ['fundamentals_daily_refresh', 'market_risk_filter', 'piotroski_filter', *FACTOR_STAGE_ORDER, 'signal_features'],
        'ready_stage_ids': [] if not ready else ['fundamentals_daily_refresh', 'market_risk_filter', 'piotroski_filter', *FACTOR_STAGE_ORDER, 'signal_features'],
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
        self.heartbeat_results: list[bool] = []
        self.heartbeat_event = threading.Event()
        self.ownership_lost_event = threading.Event()

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

    def heartbeat_stage(self, **kwargs):
        self.stage_events.append({'event': 'heartbeat_stage', **kwargs})
        self.heartbeat_event.set()
        if self.heartbeat_results:
            result = self.heartbeat_results.pop(0)
            if result is False:
                self.ownership_lost_event.set()
            return result
        return True


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


class _FakeTerminalContinuationRunStateService(_FakePipelineRunStateService):
    def __init__(
        self,
        *,
        claimed: bool = True,
        claim_error: Exception | None = None,
        persisted_stage_outcomes: dict | None = None,
    ) -> None:
        super().__init__()
        self.claimed = claimed
        self.claim_error = claim_error
        self.persisted_stage_outcomes = dict(
            persisted_stage_outcomes or {}
        )
        self.claim_calls: list[dict] = []
        self.start_run_calls: list[dict] = []
        self.finish_calls: list[dict] = []

    def start_run(self, **kwargs):
        self.start_run_calls.append(kwargs)
        raise AssertionError('terminal continuation must not create a new run')

    def claim_terminal_continuation(self, **kwargs):
        self.claim_calls.append(kwargs)
        if self.claim_error is not None:
            raise self.claim_error
        if not self.claimed:
            return {
                'claimed': False,
                'reason': 'terminal_continuation_already_claimed',
                'pipeline_run_id': kwargs['pipeline_run_id'],
            }
        return {
            'claimed': True,
            'reason': kwargs['reason'],
            'pipeline_run_id': kwargs['pipeline_run_id'],
            'persisted_stage_outcomes': self.persisted_stage_outcomes,
            'knowledge_cutoff_resolution': {
                'ready': True,
                'reason': 'latest_completed_summary_cutoff',
                'knowledge_cutoff_at': datetime(
                    2026,
                    4,
                    14,
                    8,
                    30,
                    tzinfo=timezone.utc,
                ),
                'source_run_id': 'feedback-cycle-summary',
                'feedback_scope_mode': 'broad_feedback_universe',
                'requested_symbols': ['005930'],
                'completed_scope_proof': {
                    'schema_version': (
                        'completed_summarizer_intersection_v1'
                    ),
                    'completed_symbols': ['005930'],
                    'admitted_symbols': ['005930'],
                },
            },
            'pipeline_run': {
                'id': kwargs['pipeline_run_id'],
                'status': 'running',
                'stages': {},
                'run_metadata': {
                    'symbols': ['005930'],
                    'markets': ['KOSPI'],
                    'force': False,
                    'fs_div': 'CFS',
                    'symbol_limit': None,
                    'symbol_offset': 0,
                    'capture_limit': 500,
                    'retry_cycles': None,
                    'feedback_max_cycles': 3,
                    'feedback_outer_batch_size': 20,
                    'reuse_existing_feedback': True,
                    'bypass_prerequisite_gate': False,
                    'point_in_time_recovery': True,
                    'stage_statuses': {
                        'price_collection': 'partial',
                        'macro_context_refresh': 'completed',
                        'agent_feedback_summaries': 'pending',
                        'signal_decider': 'pending',
                        'next_open_execution_plan': 'pending',
                        'signal_decider_evaluations': 'pending',
                    },
                    'terminal_continuation_evidence': {
                        'parent': {
                            'stage_statuses': {
                                'price_collection': 'partial',
                                'macro_context_refresh': 'completed',
                                'agent_feedback_summaries': 'completed',
                                'signal_decider': 'failed',
                                'next_open_execution_plan': 'blocked',
                                'signal_decider_evaluations': 'blocked',
                            },
                        },
                        'suffix_stages': {
                            'agent_feedback_summaries': {
                                'source_run_ids': {
                                    'run_id': 'feedback-cycle-summary'
                                },
                            },
                        },
                    },
                },
            },
        }

    def finish_run(self, **kwargs):
        self.finish_calls.append(kwargs)
        return {
            'id': kwargs['pipeline_run_id'],
            'status': kwargs['status'],
            'run_metadata': kwargs.get('run_metadata') or {},
        }


class _FakeCutoffContractRepairRunStateService(
    _FakePipelineRunStateService
):
    def __init__(self) -> None:
        super().__init__()
        self.claim_calls: list[dict] = []
        self.start_run_calls: list[dict] = []
        self.finish_calls: list[dict] = []

    def start_run(self, **kwargs):
        self.start_run_calls.append(kwargs)
        raise AssertionError('cutoff repair resume must not create a new run')

    def claim_summary_cutoff_contract_repair_resume(self, **kwargs):
        self.claim_calls.append(kwargs)
        proof = {
            'source_run_id': 'feedback-cycle-summary',
            'feedback_scope_mode': 'decider_scope_fast_path',
            'requested_symbols': None,
        }
        return {
            'claimed': True,
            'resumed': True,
            'pipeline_run_id': kwargs['pipeline_run_id'],
            'summary_cutoff_proof': proof,
            'knowledge_cutoff_resolution': {
                'ready': True,
                'reason': 'latest_completed_summary_cutoff',
                'knowledge_cutoff_at': datetime(
                    2026,
                    4,
                    14,
                    8,
                    30,
                    tzinfo=timezone.utc,
                ),
                'source_run_id': 'feedback-cycle-summary',
                'feedback_scope_mode': 'decider_scope_fast_path',
                'requested_symbols': ['005930'],
                'completed_scope_proof': {
                    'schema_version': (
                        'completed_summarizer_intersection_v1'
                    ),
                    'completed_symbols': ['005930'],
                    'admitted_symbols': ['005930'],
                },
            },
            'pipeline_run': {
                'id': kwargs['pipeline_run_id'],
                'status': 'running',
                'stages': {},
                'run_metadata': {
                    'symbols': None,
                    'markets': ['KOSPI'],
                    'force': False,
                    'fs_div': 'CFS',
                    'symbol_limit': None,
                    'symbol_offset': 0,
                    'capture_limit': 500,
                    'retry_cycles': None,
                    'feedback_max_cycles': 3,
                    'feedback_outer_batch_size': 20,
                    'reuse_existing_feedback': True,
                    'bypass_prerequisite_gate': False,
                    'point_in_time_recovery': False,
                    'stage_statuses': {
                        'signal_decider': 'pending',
                        'next_open_execution_plan': 'pending',
                        'signal_decider_evaluations': 'pending',
                    },
                    'summary_cutoff_contract_repair_version': 1,
                    'summary_cutoff_contract_repair_proof': proof,
                },
            },
        }

    def finish_run(self, **kwargs):
        self.finish_calls.append(kwargs)
        return {
            'id': kwargs['pipeline_run_id'],
            'status': kwargs['status'],
            'run_metadata': kwargs.get('run_metadata') or {},
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
        self.market_state_patcher = patch(
            'app.eod.daily_pipeline_stages.get_market_state',
            return_value=MagicMock(
                state='CLOSED',
                server_time='2026-04-14T16:00:00+09:00',
            ),
        )
        self.market_state_patcher.start()
        self.addCleanup(self.market_state_patcher.stop)

    def _execute_pykrx_enrichment(
        self,
        *,
        collection_result: dict,
        preflight: dict,
    ) -> tuple[DailyPipelineStageService, dict]:
        enrichment_service = MagicMock()
        enrichment_service.collect_for_market_date.return_value = (
            collection_result
        )
        service = DailyPipelineStageService(
            pykrx_enrichment_service_ref=enrichment_service,
        )
        with patch(
            'app.eod.daily_pipeline_stages.build_pykrx_enrichment_preflight',
            return_value=preflight,
        ):
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
        return service, result

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
            payload['default_daily_stage_order'].index('composite_factors'),
            payload['default_daily_stage_order'].index('signal_features'),
        )
        self.assertLess(
            payload['default_daily_stage_order'].index('signal_features'),
            payload['default_daily_stage_order'].index('external_news_collect'),
        )
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
            ['price_collection', 'pykrx_enrichment', 'fundamentals_daily_refresh'],
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
            ['price_collection', 'pykrx_enrichment', 'fundamentals_daily_refresh'],
        )
        piotroski_stage = next(
            stage for stage in payload['stages'] if stage['id'] == 'piotroski_filter'
        )
        self.assertEqual(
            piotroski_stage['depends_on'],
            ['fundamentals_daily_refresh', 'pykrx_enrichment'],
        )
        value_stage = next(
            stage for stage in payload['stages'] if stage['id'] == 'value_factors'
        )
        self.assertEqual(
            value_stage['depends_on'],
            ['price_collection', 'pykrx_enrichment'],
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
        signal_feature_stage = next(
            stage
            for stage in payload['stages']
            if stage['id'] == 'signal_features'
        )
        self.assertEqual(
            signal_feature_stage['depends_on'],
            ['market_risk_filter', 'piotroski_filter', *FACTOR_STAGE_ORDER],
        )
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
                'signal_features',
                'external_news_collect',
                'external_news_enrich',
                'macro_context_refresh',
            ],
        )
        signal_decider_stage = next(
            stage
            for stage in payload['stages']
            if stage['id'] == 'signal_decider'
        )
        self.assertIn(
            'signal_decider_decision_snapshots',
            signal_decider_stage['output_tables'],
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

    def test_recovery_threads_reused_eod_run_id_to_all_derived_writers(self) -> None:
        service = DailyPipelineStageService()

        with (
            patch(
                'app.eod.daily_pipeline_stages.daily_price_context_service.'
                'rebuild_for_market_date',
                return_value={
                    'contexts_persisted': 2,
                    'contexts_failed': 0,
                    'items': [],
                },
            ) as rebuild_contexts,
            patch(
                'app.eod.daily_pipeline_stages.daily_technical_snapshot_service.'
                'rebuild_for_market_date',
                return_value={
                    'snapshots_persisted': 2,
                    'snapshots_failed': 0,
                },
            ) as rebuild_snapshots,
            patch(
                'app.eod.daily_pipeline_stages.daily_action_summary_service.'
                'rebuild_for_market_date',
                return_value={
                    'action_summaries_persisted': 2,
                    'action_summaries_failed': 0,
                },
            ) as rebuild_action_summaries,
            patch(
                'app.eod.daily_pipeline_stages.'
                'compute_and_persist_daily_trending_ranks',
                return_value={
                    'trending_ranks_persisted': 6,
                    'periods_persisted': 3,
                },
            ) as rebuild_trending_ranks,
        ):
            result = service.run(
                market_date='2026-04-24',
                stages=[
                    'technical_snapshots',
                    'action_summaries',
                    'trending_ranks',
                ],
                symbols=['005930', '000660'],
                reused_eod_collection_run_id=7,
                trigger='auto_followup_recovery',
            )

        self.assertEqual(result['status'], 'completed')
        expected_symbol_scope = {
            'market_date': '2026-04-24',
            'symbols': ['005930', '000660'],
            'source': 'stage_orchestrator',
            'run_id': 7,
        }
        rebuild_contexts.assert_called_once_with(**expected_symbol_scope)
        rebuild_snapshots.assert_called_once_with(**expected_symbol_scope)
        rebuild_action_summaries.assert_called_once_with(**expected_symbol_scope)
        rebuild_trending_ranks.assert_called_once_with(
            market_date='2026-04-24',
            source='stage_orchestrator',
            run_id=7,
        )

    def test_prior_date_recovery_skips_current_only_stages_but_runs_as_of_stages(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        call_kwargs = {
            'market_date': '2026-09-11',
            'symbols': ['099999'],
            'markets': ['KOSPI'],
            'force': False,
            'bypass_prerequisite_gate': False,
            'fs_div': 'CFS',
            'symbol_limit': None,
            'symbol_offset': 0,
            'capture_limit': 500,
            'retry_cycles': None,
            'feedback_max_cycles': 3,
            'feedback_outer_batch_size': 20,
            'reuse_existing_feedback': True,
            'point_in_time_recovery': True,
        }
        current_only_stage_ids = (
            'master_refresh',
            'pykrx_universe_refresh',
            'signal_features',
            'market_risk_snapshots',
            'market_risk_filter',
            'fundamentals_daily_refresh',
            'external_news_collect',
            'external_news_enrich',
            'macro_context_refresh',
        )

        with (
            patch(
                'app.eod.daily_pipeline_stages.daily_piotroski_filter_service.'
                'rebuild_for_market_date',
                return_value={'results_persisted': 1, 'results_failed': 0},
            ) as rebuild_piotroski,
            patch(
                'app.eod.daily_pipeline_stages.daily_momentum_factor_service.'
                'rebuild_for_market_date',
                return_value={'results_persisted': 1, 'results_failed': 0},
            ) as rebuild_momentum,
            patch(
                'app.eod.daily_pipeline_stages.daily_price_context_service.'
                'rebuild_for_market_date',
                return_value={'contexts_persisted': 1, 'contexts_failed': 0},
            ) as rebuild_context,
            patch(
                'app.eod.daily_pipeline_stages.daily_technical_snapshot_service.'
                'rebuild_for_market_date',
                return_value={'snapshots_persisted': 1, 'snapshots_failed': 0},
            ) as rebuild_technical,
            patch.object(
                service.pykrx_enrichment_service,
                'collect_for_market_date',
                return_value={'status': 'completed', 'coverage': {}},
            ) as collect_enrichment,
        ):
            skipped_results = {
                stage_id: service._execute_stage(
                    stage_id=stage_id,
                    **call_kwargs,
                )
                for stage_id in current_only_stage_ids
            }
            piotroski_result = service._execute_stage(
                stage_id='piotroski_filter',
                **call_kwargs,
            )
            service._execute_stage(
                stage_id='pykrx_enrichment',
                **call_kwargs,
            )
            price_derived_result = service._execute_stage(
                stage_id='technical_snapshots',
                **call_kwargs,
            )

        for stage_id, result in skipped_results.items():
            with self.subTest(stage_id=stage_id):
                self.assertFalse(result['triggered'])
                self.assertTrue(result['safe_skip'])
                self.assertEqual(result['status'], 'skipped')
                self.assertEqual(
                    result['reason'],
                    'point_in_time_recovery_current_only_stage',
                )
        self.assertEqual(piotroski_result['results_persisted'], 1)
        rebuild_piotroski.assert_called_once_with(
            market_date='2026-09-11',
        )
        collect_enrichment.assert_called_once_with(
            market_date='2026-09-11',
            markets=['KOSPI'],
            symbols=['099999'],
            use_full_market_coverage=True,
            point_in_time_recovery=True,
        )
        self.assertEqual(price_derived_result['snapshots_persisted'], 1)
        rebuild_context.assert_called_once_with(
            market_date='2026-09-11',
            symbols=['099999'],
            source='stage_orchestrator',
            run_id=None,
        )
        rebuild_technical.assert_called_once_with(
            market_date='2026-09-11',
            symbols=['099999'],
            source='stage_orchestrator',
            run_id=None,
        )

    def test_prior_date_recovery_runs_as_of_piotroski_before_factor_chain(
        self,
    ) -> None:
        service = DailyPipelineStageService()

        with (
            patch(
                'app.eod.daily_pipeline_stages.daily_piotroski_filter_service.'
                'rebuild_for_market_date',
                return_value={'results_persisted': 1, 'results_failed': 0},
            ) as rebuild_piotroski,
            patch(
                'app.eod.daily_pipeline_stages.daily_momentum_factor_service.'
                'rebuild_for_market_date',
                return_value={'results_persisted': 1, 'results_failed': 0},
            ) as rebuild_momentum,
        ):
            result = service.run(
                market_date='2026-09-11',
                stages=['piotroski_filter', 'momentum_factors'],
                symbols=['099999'],
                trigger='manual_api',
                point_in_time_recovery=True,
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(
            result['stages']['piotroski_filter']['status'],
            'completed',
        )
        self.assertEqual(
            result['stages']['momentum_factors']['status'],
            'completed',
        )
        rebuild_piotroski.assert_called_once_with(market_date='2026-09-11')
        rebuild_momentum.assert_called_once_with(
            market_date='2026-09-11',
            symbols=['099999'],
        )

    def test_preopen_recovery_runs_previous_session_market_risk_stages(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        market = MagicMock(
            state='PREOPEN',
            is_trading_day=True,
            is_open=False,
            current_calendar_date=None,
            server_time='2026-09-29T08:30:00+09:00',
        )
        call_kwargs = {
            'market_date': '2026-09-28',
            'symbols': ['005930'],
            'markets': ['KOSPI'],
            'force': False,
            'bypass_prerequisite_gate': False,
            'fs_div': 'CFS',
            'symbol_limit': None,
            'symbol_offset': 0,
            'capture_limit': 500,
            'retry_cycles': None,
            'feedback_max_cycles': 3,
            'feedback_outer_batch_size': 20,
            'reuse_existing_feedback': True,
            'point_in_time_recovery': True,
        }

        with (
            patch(
                'app.eod.daily_pipeline_stages.get_market_state',
                return_value=market,
            ),
            patch(
                'app.eod.pykrx_pipeline_runner.previous_krx_trading_date',
                return_value=datetime(2026, 9, 28).date(),
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_market_risk_snapshot_service.collect_for_market_date',
                new=AsyncMock(
                    return_value={
                        'snapshots_persisted': 1,
                        'snapshots_failed': 0,
                    }
                ),
            ) as collect_snapshots,
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_market_risk_filter_service.rebuild_for_market_date',
                new=AsyncMock(
                    return_value={
                        'results_persisted': 1,
                        'results_failed': 0,
                    }
                ),
            ) as rebuild_filters,
        ):
            snapshot_result = service._execute_stage(
                stage_id='market_risk_snapshots',
                **call_kwargs,
            )
            filter_result = service._execute_stage(
                stage_id='market_risk_filter',
                **call_kwargs,
            )

        self.assertEqual(snapshot_result['snapshots_persisted'], 1)
        self.assertEqual(filter_result['results_persisted'], 1)
        collect_snapshots.assert_awaited_once()
        rebuild_filters.assert_awaited_once()

    def test_production_boundaries_auto_promote_prior_date_current_only_skip(
        self,
    ) -> None:
        for trigger in (
            'manual_api',
            'manual_api_stage',
            'internal_ops_cli',
            'internal_ops_cli_stage',
        ):
            with self.subTest(trigger=trigger):
                service = DailyPipelineStageService()
                with (
                    patch(
                        'app.eod.daily_pipeline_stages.get_market_state',
                        return_value=MagicMock(
                            state='OPEN',
                            server_time='2026-09-14T10:00:00+09:00',
                        ),
                    ),
                    patch(
                        'app.eod.daily_pipeline_stages.'
                        'daily_market_risk_snapshot_service.'
                        'collect_for_market_date',
                    ) as collect_current_risk,
                ):
                    result = service.run(
                        market_date='2026-09-11',
                        stages=['market_risk_snapshots'],
                        symbols=['099999'],
                        trigger=trigger,
                    )

                self.assertEqual(result['status'], 'completed')
                stage = result['stages']['market_risk_snapshots']
                self.assertEqual(stage['status'], 'completed')
                self.assertTrue(stage['result']['safe_skip'])
                collect_current_risk.assert_not_called()

    def test_run_reorders_requested_subset_and_splits_feedback_stages(self) -> None:
        pykrx_runner = _FakePykrxStageRunner()
        feedback_retry_service = _FakeFeedbackRetryService()
        evaluation_service = _FakeSignalDeciderEvaluationService()
        feedback_cutoff = datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc)
        later_cutoff = datetime(2026, 4, 14, 9, 45, tzinfo=timezone.utc)
        service = DailyPipelineStageService(
            pykrx_service=pykrx_runner,
            feedback_retry_service=feedback_retry_service,
            decision_loop_service=evaluation_service,
        )

        with (
            patch.object(service, '_execute_stage', side_effect=service._execute_stage) as execute_stage,
            patch.object(service, '_normalize_market_date', return_value='2026-04-14'),
            patch('app.eod.daily_pipeline_stages.datetime') as clock,
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': feedback_cutoff,
                    'source_run_id': 'fake-feedback-run',
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'requested_symbols': ['000660', '005930'],
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': ['000660', '005930'],
                        'admitted_symbols': ['000660', '005930'],
                    },
                },
            ),
        ):
            clock.now.side_effect = [feedback_cutoff, later_cutoff]
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
                fs_div='OFS',
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
        self.assertTrue(
            all(
                feedback_call['config'].fs_div == 'OFS'
                for feedback_call in feedback_retry_service.calls
            )
        )
        self.assertEqual(
            [
                feedback_call['config'].knowledge_cutoff_at
                for feedback_call in feedback_retry_service.calls
            ],
            [feedback_cutoff, feedback_cutoff],
        )
        clock.now.assert_called_once_with(timezone.utc)
        self.assertEqual(evaluation_service.calls[0]['as_of_market_date'], '2026-04-14')
        self.assertEqual(evaluation_service.calls[0]['symbols'], ['005930', '000660'])
        self.assertGreaterEqual(execute_stage.call_count, 3)

    def test_same_run_decider_uses_completed_summary_intersection(self) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        cutoff = datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc)
        parent_symbols = ['000660', '005930', '035420']
        completed_intersection = ['000660', '005930']
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )

        def feedback_result(*, config, **_kwargs):
            feedback_retry_service.calls.append({'config': config})
            if config.roles == SUMMARIZER_AGENT_NAMES:
                return {
                    'completed': True,
                    'status': 'completed',
                    'final_pending': 0,
                    'roles': list(config.roles),
                    'requested_symbols': parent_symbols,
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'run_id': 'feedback-cycle-summary-same-run',
                }
            return {
                'completed': True,
                'status': 'completed',
                'final_pending': 0,
                'roles': list(config.roles),
                'requested_symbols': list(config.symbols or ()),
            }

        with (
            patch.object(
                feedback_retry_service,
                'run_retry_cycles',
                side_effect=feedback_result,
            ),
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch(
                'app.eod.daily_pipeline_stages.datetime'
            ) as clock,
            patch(
                'app.eod.daily_pipeline_stages.'
                'agent_feedback_prerequisite_service.'
                'inspect_feedback_prerequisites',
                return_value=_feedback_prerequisites(),
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': cutoff,
                    'source_run_id': 'feedback-cycle-summary-same-run',
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'requested_symbols': completed_intersection,
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': completed_intersection,
                        'admitted_symbols': parent_symbols,
                    },
                },
            ) as resolve_cutoff,
            patch(
                'app.eod.daily_pipeline_stages.'
                'build_expired_decision_replay_result',
                return_value=None,
            ),
        ):
            clock.now.return_value = cutoff
            result = service.run(
                market_date='2026-04-14',
                stages=['agent_feedback_summaries', 'signal_decider'],
                symbols=parent_symbols,
                fs_div='CFS',
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(len(feedback_retry_service.calls), 2)
        self.assertEqual(
            feedback_retry_service.calls[1]['config'].symbols,
            tuple(completed_intersection),
        )
        self.assertEqual(
            feedback_retry_service.calls[1]['config'].scope_authority[
                'kind'
            ],
            'completed_summary_intersection_authoritative_cohort',
        )
        self.assertEqual(
            feedback_retry_service.calls[1]['config'].scope_authority[
                'source_summary_run_id'
            ],
            'feedback-cycle-summary-same-run',
        )
        resolve_cutoff.assert_called_once_with(
            market_date=datetime(2026, 4, 14).date(),
            feedback_scope_mode='broad_feedback_universe',
            fs_div='CFS',
            source_run_id='feedback-cycle-summary-same-run',
            requested_symbols=parent_symbols,
        )

    def test_decider_only_recovery_inherits_completed_summary_cutoff(self) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        cutoff = datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc)
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': cutoff,
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': (
                        FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                    ),
                    'requested_symbols': ['000660', '005930'],
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': ['000660', '005930'],
                        'admitted_symbols': ['000660', '005930'],
                    },
                },
            ) as resolve_cutoff,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['signal_decider'],
                trigger='auto_eod_followup_resume',
                summary_cutoff_proof={
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': (
                        FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                    ),
                    'requested_symbols': None,
                },
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(
            feedback_retry_service.calls[0]['config'].knowledge_cutoff_at,
            cutoff,
        )
        self.assertEqual(
            feedback_retry_service.calls[0]['config'].symbols,
            ('000660', '005930'),
        )
        self.assertEqual(
            result['pipeline_run']['run_metadata'][
                'feedback_knowledge_cutoff'
            ]['source_run_id'],
            'feedback-cycle-summary',
        )
        self.assertEqual(
            result['pipeline_run']['run_metadata'][
                'feedback_knowledge_cutoff'
            ]['requested_symbols'],
            ['000660', '005930'],
        )
        resolve_cutoff.assert_called_once_with(
            market_date=datetime(2026, 4, 14).date(),
            feedback_scope_mode=(
                FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
            ),
            fs_div='CFS',
            source_run_id='feedback-cycle-summary',
            requested_symbols=None,
        )

    def test_decider_only_latest_summary_is_pinned_to_exact_completed_scope(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        cutoff = datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc)
        parent_symbols = ['000660', '005930']
        completed_intersection = ['005930']
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )
        latest_resolution = {
            'ready': True,
            'reason': 'latest_completed_summary_cutoff',
            'knowledge_cutoff_at': cutoff,
            'source_run_id': 'feedback-cycle-summary-latest',
            'feedback_scope_mode': 'broad_feedback_universe',
            'requested_symbols': parent_symbols,
        }
        exact_resolution = {
            **latest_resolution,
            'requested_symbols': completed_intersection,
            'completed_scope_proof': {
                'schema_version': 'completed_summarizer_intersection_v1',
                'completed_symbols': completed_intersection,
                'admitted_symbols': parent_symbols,
            },
        }

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                side_effect=[latest_resolution, exact_resolution],
            ) as resolve_cutoff,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['signal_decider'],
                symbols=parent_symbols,
                trigger='manual_api',
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(
            feedback_retry_service.calls[0]['config'].symbols,
            tuple(completed_intersection),
        )
        self.assertEqual(
            feedback_retry_service.calls[0]['config'].scope_authority[
                'source_summary_run_id'
            ],
            'feedback-cycle-summary-latest',
        )
        self.assertEqual(resolve_cutoff.call_count, 2)
        self.assertEqual(
            resolve_cutoff.call_args_list[1].kwargs,
            {
                'market_date': datetime(2026, 4, 14).date(),
                'feedback_scope_mode': 'broad_feedback_universe',
                'fs_div': 'CFS',
                'source_run_id': 'feedback-cycle-summary-latest',
                'requested_symbols': parent_symbols,
            },
        )

    def test_decider_only_latest_summary_blocks_when_exact_pin_fails(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        cutoff = datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc)
        parent_symbols = ['000660', '005930']
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                side_effect=[
                    {
                        'ready': True,
                        'reason': 'latest_completed_summary_cutoff',
                        'knowledge_cutoff_at': cutoff,
                        'source_run_id': 'feedback-cycle-summary-latest',
                        'feedback_scope_mode': 'broad_feedback_universe',
                        'requested_symbols': parent_symbols,
                    },
                    {
                        'ready': False,
                        'reason': 'summary_run_lineage_mismatch',
                        'knowledge_cutoff_at': None,
                        'source_run_id': 'feedback-cycle-summary-latest',
                        'feedback_scope_mode': 'broad_feedback_universe',
                        'requested_symbols': parent_symbols,
                    },
                ],
            ) as resolve_cutoff,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['signal_decider'],
                symbols=parent_symbols,
                trigger='internal_ops_cli',
            )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(
            result['stages']['signal_decider']['result']['reason'],
            'summary_knowledge_cutoff_unavailable',
        )
        self.assertEqual(resolve_cutoff.call_count, 2)
        self.assertEqual(feedback_retry_service.calls, [])

    def test_decider_only_recovery_rejects_proof_outside_explicit_scope(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        cutoff = datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc)
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': cutoff,
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': (
                        FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                    ),
                    'requested_symbols': ['000660', '005930'],
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': ['000660', '005930'],
                        'admitted_symbols': ['000660', '005930'],
                    },
                },
            ),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['signal_decider'],
                symbols=['005930'],
                trigger='auto_eod_followup_resume',
                summary_cutoff_proof={
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': (
                        FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                    ),
                    'requested_symbols': None,
                },
            )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(
            result['stages']['signal_decider']['result'][
                'knowledge_cutoff_resolution'
            ]['reason'],
            'summary_run_symbol_scope_mismatch',
        )
        self.assertEqual(feedback_retry_service.calls, [])

    def test_decider_only_force_bypass_blocks_without_proven_summary_cutoff(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': False,
                    'reason': 'summary_run_incomplete',
                    'knowledge_cutoff_at': None,
                    'source_run_id': 'feedback-cycle-partial',
                    'feedback_scope_mode': 'broad_feedback_universe',
                },
            ),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['signal_decider'],
                symbols=['005930'],
                trigger='auto_eod_followup_resume',
                force=True,
                bypass_prerequisite_gate=True,
            )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(
            result['stages']['signal_decider']['result']['reason'],
            'summary_knowledge_cutoff_unavailable',
        )
        self.assertEqual(feedback_retry_service.calls, [])

    def test_terminal_continuation_reuses_same_run_and_original_summary_cutoff(self) -> None:
        run_state_service = _FakeTerminalContinuationRunStateService()
        cutoff = datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc)
        service = DailyPipelineStageService(
            run_state_service=run_state_service,
        )
        executed: list[tuple[str, datetime | None]] = []

        def execute(stage_id, **kwargs):
            executed.append((stage_id, kwargs['feedback_knowledge_cutoff_at']))
            if stage_id == 'agent_feedback_summaries':
                return {
                    'status': 'completed',
                    'run_id': 'feedback-cycle-summary-rerun',
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'requested_symbols': ['005930'],
                }
            return {'status': 'completed', 'stage_id': stage_id}

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                side_effect=execute,
            ) as execute_stage,
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': cutoff,
                    'source_run_id': 'feedback-cycle-summary-rerun',
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'requested_symbols': ['005930'],
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': ['005930'],
                        'admitted_symbols': ['005930'],
                    },
                },
            ),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                trigger='auto_eod_followup_resume',
                symbols=['999999'],
                markets=['KOSDAQ'],
                force=True,
                bypass_prerequisite_gate=True,
                fs_div='OFS',
                feedback_max_cycles=9,
                feedback_outer_batch_size=99,
                reuse_existing_feedback=False,
                point_in_time_recovery=True,
                reused_eod_collection_run_id=546,
                existing_pipeline_run_id=456,
            )

        self.assertEqual(result['pipeline_run_id'], 456)
        self.assertEqual(run_state_service.start_run_calls, [])
        self.assertEqual(
            run_state_service.claim_calls[0]['reused_eod_collection_run_id'],
            546,
        )
        self.assertEqual(
            [stage_id for stage_id, _ in executed],
            [
                'agent_feedback_summaries',
                'signal_decider',
                'next_open_execution_plan',
                'signal_decider_evaluations',
            ],
        )
        self.assertTrue(all(stage_cutoff == cutoff for _, stage_cutoff in executed))
        for call in execute_stage.call_args_list:
            self.assertEqual(call.kwargs['symbols'], ['005930'])
            self.assertEqual(call.kwargs['markets'], ['KOSPI'])
            self.assertFalse(call.kwargs['force'])
            self.assertFalse(call.kwargs['bypass_prerequisite_gate'])
            self.assertEqual(call.kwargs['fs_div'], 'CFS')
            self.assertEqual(call.kwargs['feedback_max_cycles'], 3)
            self.assertEqual(call.kwargs['feedback_outer_batch_size'], 20)
            self.assertTrue(call.kwargs['reuse_existing_feedback'])
            self.assertTrue(call.kwargs['point_in_time_recovery'])
        final_statuses = run_state_service.finish_calls[0]['run_metadata'][
            'stage_statuses'
        ]
        self.assertEqual(final_statuses['price_collection'], 'partial')
        self.assertEqual(final_statuses['macro_context_refresh'], 'completed')
        self.assertEqual(final_statuses['signal_decider'], 'completed')
        self.assertEqual(result['status'], 'partial')

    def test_terminal_continuation_limits_decider_to_fast_summary_scope(
        self,
    ) -> None:
        run_state_service = _FakeTerminalContinuationRunStateService()
        original_claim = run_state_service.claim_terminal_continuation

        def claim_with_fast_subset(**kwargs):
            claim = original_claim(**kwargs)
            claim['knowledge_cutoff_resolution'].update(
                {
                    'feedback_scope_mode': 'decider_scope_fast_path',
                    'requested_symbols': ['005930'],
                }
            )
            claim['pipeline_run']['run_metadata']['symbols'] = [
                '005930',
                '000660',
            ]
            return claim

        run_state_service.claim_terminal_continuation = (
            claim_with_fast_subset
        )
        service = DailyPipelineStageService(
            run_state_service=run_state_service,
        )
        executed_symbols: dict[str, list[str] | None] = {}

        def execute(stage_id, **kwargs):
            executed_symbols[stage_id] = kwargs['symbols']
            if stage_id == 'agent_feedback_summaries':
                return {
                    'status': 'completed',
                    'run_id': 'feedback-cycle-summary-rerun',
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'requested_symbols': ['005930', '000660'],
                }
            return {'status': 'completed', 'stage_id': stage_id}

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(service, '_execute_stage', side_effect=execute),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': datetime(
                        2026,
                        4,
                        14,
                        8,
                        30,
                        tzinfo=timezone.utc,
                    ),
                    'source_run_id': 'feedback-cycle-summary-rerun',
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'requested_symbols': ['005930'],
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': ['005930'],
                        'admitted_symbols': ['005930', '000660'],
                    },
                },
            ),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                trigger='auto_eod_followup_resume',
                reused_eod_collection_run_id=546,
                existing_pipeline_run_id=456,
            )

        self.assertEqual(result['pipeline_run_id'], 456)
        self.assertEqual(
            executed_symbols['signal_decider'],
            ['005930'],
        )

    def test_terminal_continuation_refreshes_decider_from_rerun_summary(
        self,
    ) -> None:
        run_state_service = _FakeTerminalContinuationRunStateService()
        original_claim = run_state_service.claim_terminal_continuation
        parent_symbols = ['005930', '000660']
        original_intersection = ['005930']
        rerun_intersection = ['000660']
        cutoff = datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc)

        def claim_with_prior_intersection(**kwargs):
            claim = original_claim(**kwargs)
            claim['pipeline_run']['run_metadata']['symbols'] = (
                parent_symbols
            )
            claim['knowledge_cutoff_resolution'].update(
                {
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'requested_symbols': original_intersection,
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': original_intersection,
                        'admitted_symbols': parent_symbols,
                    },
                }
            )
            return claim

        run_state_service.claim_terminal_continuation = (
            claim_with_prior_intersection
        )
        service = DailyPipelineStageService(
            run_state_service=run_state_service,
        )
        executed_symbols: dict[str, list[str] | None] = {}
        decider_cutoff_resolution: dict | None = None

        def execute(stage_id, **kwargs):
            nonlocal decider_cutoff_resolution
            executed_symbols[stage_id] = kwargs['symbols']
            if stage_id == 'agent_feedback_summaries':
                return {
                    'status': 'completed',
                    'run_id': 'feedback-cycle-summary-rerun',
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'requested_symbols': parent_symbols,
                }
            if stage_id == 'signal_decider':
                decider_cutoff_resolution = kwargs[
                    'feedback_cutoff_resolution'
                ]
            return {'status': 'completed', 'stage_id': stage_id}

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(service, '_execute_stage', side_effect=execute),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': cutoff,
                    'source_run_id': 'feedback-cycle-summary-rerun',
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'requested_symbols': rerun_intersection,
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': rerun_intersection,
                        'admitted_symbols': parent_symbols,
                    },
                },
            ) as resolve_cutoff,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                trigger='auto_eod_followup_resume',
                reused_eod_collection_run_id=546,
                existing_pipeline_run_id=456,
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(
            executed_symbols['agent_feedback_summaries'],
            parent_symbols,
        )
        self.assertEqual(
            executed_symbols['signal_decider'],
            rerun_intersection,
        )
        self.assertEqual(
            decider_cutoff_resolution['source_run_id'],
            'feedback-cycle-summary-rerun',
        )
        resolve_cutoff.assert_called_once_with(
            market_date=datetime(2026, 4, 14).date(),
            feedback_scope_mode='broad_feedback_universe',
            fs_div='CFS',
            source_run_id='feedback-cycle-summary-rerun',
            requested_symbols=parent_symbols,
        )

    def test_cutoff_contract_repair_resume_reuses_same_run(self) -> None:
        run_state_service = _FakeCutoffContractRepairRunStateService()
        cutoff = datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc)
        service = DailyPipelineStageService(
            run_state_service=run_state_service,
        )
        executed: list[tuple[str, list[str] | None]] = []

        def execute(stage_id, **kwargs):
            executed.append((stage_id, kwargs['symbols']))
            return {'status': 'completed', 'stage_id': stage_id}

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': cutoff,
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': 'decider_scope_fast_path',
                    'requested_symbols': ['005930'],
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': ['005930'],
                        'admitted_symbols': ['005930'],
                    },
                },
            ) as resolve_cutoff,
            patch.object(
                service,
                '_execute_stage',
                side_effect=execute,
            ),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                trigger='auto_eod_followup_resume',
                resume_from_stage='signal_decider',
                reused_eod_collection_run_id=546,
                existing_pipeline_run_id=456,
                summary_cutoff_contract_repair=True,
                summary_cutoff_proof={
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': 'decider_scope_fast_path',
                    'requested_symbols': None,
                },
            )

        self.assertEqual(result['pipeline_run_id'], 456)
        self.assertEqual(run_state_service.start_run_calls, [])
        self.assertEqual(
            run_state_service.claim_calls,
            [
                {
                    'pipeline_run_id': 456,
                    'market_date': '2026-04-14',
                    'reused_eod_collection_run_id': 546,
                }
            ],
        )
        self.assertEqual(
            executed,
            [
                ('signal_decider', ['005930']),
                ('next_open_execution_plan', None),
                ('signal_decider_evaluations', None),
            ],
        )
        resolve_cutoff.assert_not_called()
        self.assertEqual(result['status'], 'completed')

    def test_cutoff_contract_repair_resume_rejects_invalid_persisted_fs_div(
        self,
    ) -> None:
        class InvalidContextRunStateService(
            _FakeCutoffContractRepairRunStateService
        ):
            def claim_summary_cutoff_contract_repair_resume(self, **kwargs):
                result = super().claim_summary_cutoff_contract_repair_resume(
                    **kwargs
                )
                result['pipeline_run']['run_metadata']['fs_div'] = 'INVALID'
                return result

        run_state_service = InvalidContextRunStateService()
        service = DailyPipelineStageService(
            run_state_service=run_state_service,
        )

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                return_value={'status': 'completed'},
            ),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                'cutoff contract repair fs_div is invalid',
            ):
                service.run(
                    market_date='2026-04-14',
                    stages=[
                        'signal_decider',
                        'next_open_execution_plan',
                        'signal_decider_evaluations',
                    ],
                    trigger='auto_eod_followup_resume',
                    resume_from_stage='signal_decider',
                    reused_eod_collection_run_id=546,
                    existing_pipeline_run_id=456,
                    summary_cutoff_contract_repair=True,
                    summary_cutoff_proof={
                        'source_run_id': 'feedback-cycle-summary',
                        'feedback_scope_mode': 'decider_scope_fast_path',
                        'requested_symbols': None,
                    },
                )

        self.assertEqual(len(run_state_service.finish_calls), 1)
        self.assertEqual(
            run_state_service.finish_calls[0]['pipeline_run_id'],
            456,
        )
        self.assertEqual(run_state_service.finish_calls[0]['status'], 'failed')

    def test_terminal_continuation_context_recovers_summary_result_lineage(
        self,
    ) -> None:
        run_state_service = _FakeTerminalContinuationRunStateService()
        pipeline_run = run_state_service.claim_terminal_continuation(
            pipeline_run_id=456,
            reason='summary_knowledge_cutoff_repair',
        )['pipeline_run']
        summary_evidence = pipeline_run['run_metadata'][
            'terminal_continuation_evidence'
        ]['suffix_stages']['agent_feedback_summaries']
        summary_evidence['source_run_ids'] = {'eod_run_id': 546}
        summary_evidence['details'] = {
            'result_summary': {'run_id': 'feedback-cycle-summary'}
        }

        context = DailyPipelineStageService(
            run_state_service=run_state_service,
        )._terminal_continuation_execution_context(pipeline_run)

        self.assertEqual(
            context['summary_source_run_id'],
            'feedback-cycle-summary',
        )

        summary_evidence['source_run_ids'] = {
            'run_id': 'another-feedback-cycle'
        }
        with self.assertRaisesRegex(
            RuntimeError,
            'terminal continuation summary source run is unavailable',
        ):
            DailyPipelineStageService(
                run_state_service=run_state_service,
            )._terminal_continuation_execution_context(pipeline_run)

    def test_terminal_continuation_skips_persisted_completed_prefix(self) -> None:
        suffix_ids = [
            'agent_feedback_summaries',
            'signal_decider',
            'next_open_execution_plan',
            'signal_decider_evaluations',
        ]
        for completed_prefix_size in range(1, len(suffix_ids)):
            with self.subTest(completed_prefix_size=completed_prefix_size):
                persisted_stage_outcomes = {
                    stage_id: {
                        'stage': stage_id,
                        'stage_id': stage_id,
                        'status': 'completed',
                        'processed': 1,
                        'total': 1,
                        'succeeded': 1,
                        'failed': 0,
                        'error_message': None,
                        'details': {
                            'result_summary': {
                                'status': 'completed',
                                'run_id': f'{stage_id}-continued',
                            }
                        },
                    }
                    for stage_id in suffix_ids[:completed_prefix_size]
                }
                persisted_stage_outcomes['agent_feedback_summaries'][
                    'source_run_ids'
                ] = {'run_id': 'feedback-cycle-summary'}
                persisted_stage_outcomes['agent_feedback_summaries'][
                    'details'
                ]['result_summary'].update(
                    {
                        'run_id': 'feedback-cycle-summary',
                        'feedback_scope_mode': (
                            'broad_feedback_universe'
                        ),
                    }
                )
                run_state_service = (
                    _FakeTerminalContinuationRunStateService(
                        persisted_stage_outcomes=(
                            persisted_stage_outcomes
                        ),
                    )
                )
                service = DailyPipelineStageService(
                    run_state_service=run_state_service,
                )
                executed: list[str] = []

                def execute(stage_id, **_kwargs):
                    executed.append(stage_id)
                    return {
                        'status': 'completed',
                        'stage_id': stage_id,
                    }

                with (
                    patch.object(
                        service,
                        '_normalize_market_date',
                        return_value='2026-04-14',
                    ),
                    patch.object(
                        service,
                        '_execute_stage',
                        side_effect=execute,
                    ),
                ):
                    result = service.run(
                        market_date='2026-04-14',
                        stages=suffix_ids,
                        trigger='auto_eod_followup_resume',
                        reused_eod_collection_run_id=546,
                        existing_pipeline_run_id=456,
                    )

                self.assertEqual(
                    executed,
                    suffix_ids[completed_prefix_size:],
                )
                started_stage_ids = [
                    event['stage_id']
                    for event in run_state_service.stage_events
                    if event['event'] == 'start_stage'
                ]
                for stage_id in suffix_ids[:completed_prefix_size]:
                    self.assertNotIn(stage_id, started_stage_ids)
                    self.assertEqual(
                        result['stages'][stage_id]['status'],
                        'completed',
                    )
                self.assertEqual(result['status'], 'partial')

    def test_terminal_continuation_preserves_original_scope_for_skipped_summary(
        self,
    ) -> None:
        skipped_summary = {
            'stage': 'agent_feedback_summaries',
            'stage_id': 'agent_feedback_summaries',
            'status': 'skipped',
            'processed': 0,
            'total': 0,
            'succeeded': 0,
            'failed': 0,
            'remaining': 0,
            'error_message': None,
            'source_run_ids': {},
            'details': {
                'result_summary': {
                    'triggered': False,
                    'status': 'skipped',
                    'reason': 'next_open_window_expired',
                    'market_date': '2026-04-14',
                    'target_trade_date': '2026-04-15',
                    'current_market_date': '2026-04-15',
                    'market_state': 'OPEN',
                    'stage': 'agent_feedback_summaries',
                }
            },
        }
        run_state_service = _FakeTerminalContinuationRunStateService(
            persisted_stage_outcomes={
                'agent_feedback_summaries': skipped_summary,
            },
        )
        service = DailyPipelineStageService(
            run_state_service=run_state_service,
        )
        executed: list[str] = []

        def execute(stage_id, **_kwargs):
            executed.append(stage_id)
            return {'status': 'completed', 'stage_id': stage_id}

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(service, '_execute_stage', side_effect=execute),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                trigger='auto_eod_followup_resume',
                reused_eod_collection_run_id=546,
                existing_pipeline_run_id=456,
            )

        self.assertEqual(
            result['stages']['agent_feedback_summaries']['status'],
            'skipped',
        )
        self.assertEqual(
            executed,
            [
                'signal_decider',
                'next_open_execution_plan',
                'signal_decider_evaluations',
            ],
        )

    def test_terminal_continuation_uses_persisted_rerun_summary_scope(
        self,
    ) -> None:
        parent_symbols = ['005930', '000660']
        rerun_symbols = ['000660']
        rerun_source_run_id = 'agent_feedback_summaries-rerun'
        persisted_summary_outcome = {
            'stage': 'agent_feedback_summaries',
            'stage_id': 'agent_feedback_summaries',
            'status': 'completed',
            'processed': 1,
            'total': 1,
            'succeeded': 1,
            'failed': 0,
            'error_message': None,
            'details': {
                'result_summary': {
                    'status': 'completed',
                    'run_id': rerun_source_run_id,
                    'feedback_scope_mode': 'broad_feedback_universe',
                }
            },
        }
        run_state_service = _FakeTerminalContinuationRunStateService(
            persisted_stage_outcomes={
                'agent_feedback_summaries': persisted_summary_outcome,
            },
        )
        original_claim = run_state_service.claim_terminal_continuation

        def claim_with_rerun_summary(**kwargs):
            claim = original_claim(**kwargs)
            claim['pipeline_run']['run_metadata']['symbols'] = (
                parent_symbols
            )
            claim['knowledge_cutoff_resolution'].update(
                {
                    'source_run_id': rerun_source_run_id,
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'requested_symbols': rerun_symbols,
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': rerun_symbols,
                        'admitted_symbols': parent_symbols,
                    },
                }
            )
            return claim

        run_state_service.claim_terminal_continuation = (
            claim_with_rerun_summary
        )
        service = DailyPipelineStageService(
            run_state_service=run_state_service,
        )
        executed_symbols: dict[str, list[str] | None] = {}
        decider_source_run_id: str | None = None

        def execute(stage_id, **kwargs):
            nonlocal decider_source_run_id
            executed_symbols[stage_id] = kwargs['symbols']
            if stage_id == 'signal_decider':
                decider_source_run_id = kwargs[
                    'feedback_cutoff_resolution'
                ]['source_run_id']
            return {'status': 'completed', 'stage_id': stage_id}

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(service, '_execute_stage', side_effect=execute),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                trigger='auto_eod_followup_resume',
                reused_eod_collection_run_id=546,
                existing_pipeline_run_id=456,
            )

        self.assertEqual(result['status'], 'partial')
        self.assertNotIn(
            'agent_feedback_summaries',
            executed_symbols,
        )
        self.assertEqual(
            executed_symbols['signal_decider'],
            rerun_symbols,
        )
        self.assertEqual(decider_source_run_id, rerun_source_run_id)

    def test_terminal_continuation_does_not_finalize_when_claim_proof_raises(
        self,
    ) -> None:
        run_state_service = _FakeTerminalContinuationRunStateService(
            claim_error=RuntimeError('cutoff lookup failed'),
        )
        service = DailyPipelineStageService(
            run_state_service=run_state_service,
        )

        with patch.object(
            service,
            '_normalize_market_date',
            return_value='2026-04-14',
        ):
            with self.assertRaisesRegex(RuntimeError, 'cutoff lookup failed'):
                service.run(
                    market_date='2026-04-14',
                    stages=[
                        'agent_feedback_summaries',
                        'signal_decider',
                        'next_open_execution_plan',
                        'signal_decider_evaluations',
                    ],
                    trigger='auto_eod_followup_resume',
                    point_in_time_recovery=True,
                    reused_eod_collection_run_id=546,
                    existing_pipeline_run_id=456,
                )

        self.assertEqual(len(run_state_service.claim_calls), 1)
        self.assertEqual(run_state_service.finish_calls, [])
        self.assertEqual(run_state_service.stage_events, [])

    def test_terminal_continuation_stops_when_claim_is_rejected(self) -> None:
        run_state_service = _FakeTerminalContinuationRunStateService(
            claimed=False,
        )
        service = DailyPipelineStageService(
            run_state_service=run_state_service,
        )

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(service, '_execute_stage') as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                trigger='auto_eod_followup_resume',
                reused_eod_collection_run_id=546,
                existing_pipeline_run_id=456,
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(
            result['reason'],
            'terminal_continuation_already_claimed',
        )
        execute_stage.assert_not_called()

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
        self.assertEqual(executed_stage_ids[13], 'signal_features')
        self.assertEqual(executed_stage_ids[14], 'external_news_collect')
        self.assertEqual(executed_stage_ids[15], 'external_news_enrich')
        self.assertEqual(executed_stage_ids[16], 'macro_context_refresh')

    def test_run_treats_already_completed_skip_as_satisfied_dependency(self) -> None:
        service = DailyPipelineStageService()

        def execute(stage_id, **_):
            if stage_id == 'fundamentals_daily_refresh':
                return {
                    'triggered': False,
                    'reason': 'already_completed',
                    'market_date': '2026-04-14',
                }
            return {'status': 'completed', 'stage_id': stage_id}

        with (
            patch.object(service, '_normalize_market_date', return_value='2026-04-14'),
            patch.object(service, '_execute_stage', side_effect=execute) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['fundamentals_daily_refresh', 'advanced_value_factors'],
            )

        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'skipped')
        self.assertEqual(result['stages']['advanced_value_factors']['status'], 'completed')
        self.assertEqual(
            [call.args[0] for call in execute_stage.call_args_list],
            ['fundamentals_daily_refresh', 'advanced_value_factors'],
        )

    def test_run_keeps_non_satisfied_skip_dependency_blocked(self) -> None:
        service = DailyPipelineStageService()

        def execute(stage_id, **_):
            if stage_id == 'fundamentals_daily_refresh':
                return {
                    'triggered': False,
                    'reason': 'already_running',
                    'market_date': '2026-04-14',
                }
            raise AssertionError('dependent stage must not run after an unsafe skip')

        with (
            patch.object(service, '_normalize_market_date', return_value='2026-04-14'),
            patch.object(service, '_execute_stage', side_effect=execute) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['fundamentals_daily_refresh', 'advanced_value_factors'],
            )

        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'skipped')
        self.assertEqual(result['stages']['advanced_value_factors']['status'], 'blocked')
        self.assertEqual(
            result['stages']['advanced_value_factors']['result']['incomplete_dependencies'],
            ['fundamentals_daily_refresh'],
        )
        self.assertEqual(
            [call.args[0] for call in execute_stage.call_args_list],
            ['fundamentals_daily_refresh'],
        )

    def test_recovery_treats_terminal_optional_context_gaps_as_ordering_only(
        self,
    ) -> None:
        optional_context_results = (
            (
                'external_news_collect',
                {
                    'status': 'disabled',
                    'reason': 'provider_disabled',
                    'summary': {
                        'requested_symbol_count': 2,
                        'eligible_symbol_count': 0,
                        'collected_symbol_count': 0,
                    },
                },
                'skipped',
            ),
            (
                'external_news_enrich',
                {
                    'status': 'partial',
                    'items_inspected': 2,
                    'items_missing_classification': 1,
                    'items_remaining_at_least': 1,
                    'missing_item_ids': [101],
                },
                'partial',
            ),
            (
                'macro_context_refresh',
                {
                    'status': 'unavailable',
                    'observations_upserted': 0,
                    'observations_failed': 3,
                },
                'partial',
            ),
            (
                'macro_context_refresh',
                {
                    'status': 'partial',
                    'observations_upserted': 2,
                    'observations_failed': 1,
                },
                'partial',
            ),
        )

        for stage_id, optional_result, expected_status in optional_context_results:
            with self.subTest(stage_id=stage_id, optional_result=optional_result):
                run_state_service = _FakePipelineRunStateService()
                service = DailyPipelineStageService(
                    run_state_service=run_state_service,
                )

                def execute(current_stage_id, **_):
                    if current_stage_id == stage_id:
                        return dict(optional_result)
                    return {'status': 'completed', 'stage_id': current_stage_id}

                with (
                    patch.object(
                        service,
                        '_normalize_market_date',
                        return_value='2026-04-14',
                    ),
                    patch.object(
                        service,
                        '_execute_stage',
                        side_effect=execute,
                    ) as execute_stage,
                ):
                    result = service.run(
                        market_date='2026-04-14',
                        stages=[stage_id, 'agent_feedback_summaries'],
                        trigger='auto_eod_followup_resume',
                    )

                self.assertEqual(
                    result['stages'][stage_id]['status'],
                    expected_status,
                )
                self.assertEqual(
                    result['stages']['agent_feedback_summaries']['status'],
                    'completed',
                )
                self.assertEqual(
                    [call.args[0] for call in execute_stage.call_args_list],
                    [stage_id, 'agent_feedback_summaries'],
                )
                self.assertTrue(daily_pipeline_outcome_is_satisfied(result))

    def test_run_allows_terminal_high_coverage_factor_partial_dependency(
        self,
    ) -> None:
        service = DailyPipelineStageService()

        def execute(stage_id, **_):
            if stage_id == 'momentum_factors':
                return {
                    'status': 'partial',
                    'results_persisted': 99,
                    'results_failed': 1,
                    'remaining_retryable_events': 0,
                }
            return {'status': 'completed', 'stage_id': stage_id}

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                side_effect=execute,
            ) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['momentum_factors', 'composite_factors'],
                trigger='auto_eod_followup_resume',
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['stages']['momentum_factors']['status'], 'partial')
        self.assertEqual(result['stages']['composite_factors']['status'], 'completed')
        self.assertFalse(result['pipeline_run']['run_metadata']['resumable'])
        self.assertEqual(
            [call.args[0] for call in execute_stage.call_args_list],
            ['momentum_factors', 'composite_factors'],
        )

    def test_run_blocks_unproven_factor_partial_dependency(self) -> None:
        service = DailyPipelineStageService()

        for partial_result in (
            {
                'status': 'partial',
                'results_persisted': 98,
                'results_failed': 2,
                'remaining_retryable_events': 0,
            },
            {
                'status': 'partial',
                'results_persisted': 99,
                'results_failed': 1,
                'retry_cycles_requested': 3,
                'retry_cycles_completed': 1,
                'remaining_retryable_events': 1,
            },
        ):
            with self.subTest(partial_result=partial_result):
                with (
                    patch.object(
                        service,
                        '_normalize_market_date',
                        return_value='2026-04-14',
                    ),
                    patch.object(
                        service,
                        '_execute_stage',
                        side_effect=[partial_result],
                    ) as execute_stage,
                ):
                    result = service.run(
                        market_date='2026-04-14',
                        stages=['momentum_factors', 'composite_factors'],
                    )

                self.assertEqual(
                    result['stages']['composite_factors']['status'],
                    'blocked',
                )
                self.assertEqual(execute_stage.call_count, 1)

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

    def test_suffix_recovery_cannot_bypass_missing_persisted_prerequisites(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )

        with patch(
            'app.eod.daily_pipeline_stages.'
            'agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
            return_value=_feedback_prerequisites(
                status='waiting',
                missing_stage_ids=[
                    'momentum_factors',
                    'beta_factors',
                    'signal_features',
                ],
            ),
        ) as inspect_prerequisites:
            for force in (False, True):
                with self.subTest(force=force):
                    result = service.run(
                        market_date='2026-04-14',
                        stages=[
                            'agent_feedback_summaries',
                            'signal_decider',
                            'next_open_execution_plan',
                        ],
                        force=force,
                        bypass_prerequisite_gate=True,
                        trigger='stage_orchestrator',
                    )

                    self.assertEqual(result['status'], 'waiting')
                    self.assertEqual(
                        result['reason'],
                        'feedback_prerequisites_not_ready',
                    )
                    self.assertIsNone(result['pipeline_run_id'])
        self.assertEqual(inspect_prerequisites.call_count, 2)
        self.assertTrue(
            all(
                call.kwargs['required_stage_ids']
                == CURRENT_FEEDBACK_PREREQUISITE_STAGE_ORDER
                for call in inspect_prerequisites.call_args_list
            )
        )
        self.assertEqual(self.run_state_service.runs, [])
        self.assertEqual(feedback_retry_service.calls, [])

    def test_point_in_time_suffix_keeps_date_bounded_numeric_prerequisites(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )

        with patch(
            'app.eod.daily_pipeline_stages.'
            'agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
            return_value=_feedback_prerequisites(),
        ) as inspect_prerequisites:
            result = service.run(
                market_date='2026-04-14',
                stages=['agent_feedback_summaries'],
                point_in_time_recovery=True,
            )

        self.assertEqual(result['status'], 'completed')
        self.assertNotIn(
            'signal_features',
            inspect_prerequisites.call_args.kwargs['required_stage_ids'],
        )
        self.assertEqual(len(feedback_retry_service.calls), 1)

    def test_suffix_recovery_with_ready_persisted_prerequisites_remains_allowed(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )

        result = service.run(
            market_date='2026-04-14',
            stages=['agent_feedback_summaries'],
            force=True,
            bypass_prerequisite_gate=True,
            reuse_existing_feedback=True,
        )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(len(self.run_state_service.runs), 1)
        self.assertEqual(len(feedback_retry_service.calls), 1)

    def test_factor_suffix_preflight_excludes_prerequisites_planned_in_run(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        planned_stages = [
            *FACTOR_STAGE_ORDER,
            'signal_features',
            'agent_feedback_summaries',
        ]

        with (
            patch(
                'app.eod.daily_pipeline_stages.'
                'agent_feedback_prerequisite_service.'
                'inspect_feedback_prerequisites',
                return_value=_feedback_prerequisites(),
            ) as inspect_prerequisites,
            patch.object(
                service,
                '_execute_stage',
                return_value={
                    'triggered': True,
                    'status': 'completed',
                    'results_persisted': 1,
                    'results_failed': 0,
                },
            ) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=planned_stages,
                resume_from_stage='momentum_factors',
            )

        self.assertEqual(result['status'], 'completed')
        required_stage_ids = (
            inspect_prerequisites.call_args.kwargs['required_stage_ids']
        )
        self.assertNotIn('momentum_factors', required_stage_ids)
        self.assertNotIn('signal_features', required_stage_ids)
        self.assertIn('fundamentals_daily_refresh', required_stage_ids)
        self.assertEqual(
            [
                call.kwargs.get('stage_id')
                or (call.args[0] if call.args else None)
                for call in execute_stage.call_args_list
            ],
            planned_stages,
        )
        self.assertEqual(len(self.run_state_service.runs), 1)

    def test_factor_stage_heartbeats_while_synchronous_work_is_running(
        self,
    ) -> None:
        service = DailyPipelineStageService()

        def execute(stage_id, **_):
            self.assertEqual(stage_id, 'momentum_factors')
            self.assertTrue(
                self.run_state_service.heartbeat_event.wait(timeout=0.5),
                'factor stage did not emit a heartbeat while work was active',
            )
            return {
                'status': 'completed',
                'results_persisted': 2,
                'results_failed': 0,
            }

        with (
            patch(
                'app.eod.daily_pipeline_stages.'
                'FACTOR_STAGE_HEARTBEAT_INTERVAL_SECONDS',
                0.01,
                create=True,
            ),
            patch.object(service, '_execute_stage', side_effect=execute),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['momentum_factors'],
            )

        self.assertEqual(result['status'], 'completed')
        heartbeat_events = [
            event
            for event in self.run_state_service.stage_events
            if event['event'] == 'heartbeat_stage'
        ]
        self.assertTrue(heartbeat_events)
        self.assertEqual(heartbeat_events[0]['stage_id'], 'momentum_factors')

    def test_factor_pipeline_stops_before_next_stage_after_ownership_loss(
        self,
    ) -> None:
        self.run_state_service.heartbeat_results = [True, False]
        service = DailyPipelineStageService()
        executed_stages: list[str] = []

        def execute(stage_id, **_):
            executed_stages.append(stage_id)
            if stage_id != 'momentum_factors':
                raise AssertionError(
                    'pipeline must stop before the next factor after ownership loss'
                )
            self.assertTrue(
                self.run_state_service.ownership_lost_event.wait(timeout=0.5),
                'ownership loss was not observed during the active factor',
            )
            return {
                'status': 'completed',
                'results_persisted': 2,
                'results_failed': 0,
            }

        with (
            patch(
                'app.eod.daily_pipeline_stages.'
                'FACTOR_STAGE_HEARTBEAT_INTERVAL_SECONDS',
                0.01,
                create=True,
            ),
            patch.object(service, '_execute_stage', side_effect=execute),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['momentum_factors', 'value_factors'],
            )

        self.assertEqual(executed_stages, ['momentum_factors'])
        self.assertEqual(result['status'], 'interrupted')
        self.assertEqual(result['reason'], 'pipeline_run_ownership_lost')

    def test_factor_stage_is_not_started_when_initial_ownership_check_fails(
        self,
    ) -> None:
        self.run_state_service.heartbeat_results = [False]
        service = DailyPipelineStageService()

        with patch.object(service, '_execute_stage') as execute_stage:
            result = service.run(
                market_date='2026-04-14',
                stages=['momentum_factors'],
            )

        execute_stage.assert_not_called()
        self.assertEqual(result['status'], 'interrupted')
        self.assertEqual(result['reason'], 'pipeline_run_ownership_lost')
        self.assertNotIn('finish', self.run_state_service.runs[-1])

    def test_stage_start_failure_retries_run_terminalization_and_preserves_error(
        self,
    ) -> None:
        self.run_state_service.start_stage = MagicMock(
            side_effect=RuntimeError('stage checkpoint unavailable')
        )
        self.run_state_service.finish_run = MagicMock(
            side_effect=[
                RuntimeError('finish timeout 1'),
                RuntimeError('finish timeout 2'),
                {'id': 9000, 'status': 'failed'},
            ]
        )
        service = DailyPipelineStageService()

        with self.assertRaisesRegex(
            RuntimeError,
            'stage checkpoint unavailable',
        ):
            service.run(
                market_date='2026-04-14',
                stages=['momentum_factors'],
            )

        self.assertEqual(self.run_state_service.finish_run.call_count, 3)
        for finish_call in self.run_state_service.finish_run.call_args_list:
            self.assertEqual(finish_call.kwargs['status'], 'failed')
            self.assertEqual(
                finish_call.kwargs['error_message'],
                'stage checkpoint unavailable',
            )

    def test_final_run_checkpoint_retries_transient_failures(self) -> None:
        self.run_state_service.finish_run = MagicMock(
            side_effect=[
                RuntimeError('finish timeout 1'),
                RuntimeError('finish timeout 2'),
                {'id': 9000, 'status': 'completed'},
            ]
        )
        service = DailyPipelineStageService()

        with patch.object(
            service,
            '_execute_stage',
            return_value={
                'triggered': True,
                'status': 'completed',
                'symbols_refreshed': 1,
                'symbols_failed': 0,
            },
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['master_refresh'],
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(self.run_state_service.finish_run.call_count, 3)

    def test_stage_failure_preserves_error_when_stage_cleanup_fails(self) -> None:
        self.run_state_service.finish_stage = MagicMock(
            side_effect=RuntimeError('stage cleanup unavailable')
        )
        self.run_state_service.finish_run = MagicMock(
            return_value={'id': 9000, 'status': 'failed'}
        )
        service = DailyPipelineStageService()

        with (
            patch.object(
                service,
                '_execute_stage',
                side_effect=ValueError('stage body failed'),
            ),
            self.assertRaisesRegex(ValueError, 'stage body failed'),
        ):
            service.run(
                market_date='2026-04-14',
                stages=['master_refresh'],
            )

        self.run_state_service.finish_run.assert_called_once()
        self.assertEqual(
            self.run_state_service.finish_run.call_args.kwargs['status'],
            'failed',
        )

    def test_stage_terminal_checkpoint_failure_closes_parent_run(self) -> None:
        self.run_state_service.finish_stage = MagicMock(
            side_effect=RuntimeError('stage terminal checkpoint unavailable')
        )
        self.run_state_service.finish_run = MagicMock(
            return_value={'id': 9000, 'status': 'failed'}
        )
        service = DailyPipelineStageService()

        with self.assertRaisesRegex(
            RuntimeError,
            'stage terminal checkpoint unavailable',
        ):
            service._finish_tracked_stage(
                pipeline_run_id=9000,
                stage_id='master_refresh',
                market_date='2026-04-14',
                outcome={'status': 'completed'},
                result={
                    'status': 'completed',
                    'symbols_refreshed': 1,
                    'symbols_failed': 0,
                },
            )

        self.assertEqual(self.run_state_service.finish_stage.call_count, 3)
        self.run_state_service.finish_run.assert_called_once()
        self.assertEqual(
            self.run_state_service.finish_run.call_args.kwargs['status'],
            'failed',
        )

    def test_factor_exception_after_ownership_loss_avoids_late_state_writes(
        self,
    ) -> None:
        self.run_state_service.heartbeat_results = [True, False]
        service = DailyPipelineStageService()
        executed_stages: list[str] = []

        def execute(stage_id, **_):
            executed_stages.append(stage_id)
            self.assertEqual(stage_id, 'momentum_factors')
            self.assertTrue(
                self.run_state_service.ownership_lost_event.wait(timeout=0.5),
            )
            raise RuntimeError('factor failed after ownership loss')

        with (
            patch(
                'app.eod.daily_pipeline_stages.'
                'FACTOR_STAGE_HEARTBEAT_INTERVAL_SECONDS',
                0.01,
            ),
            patch.object(service, '_execute_stage', side_effect=execute),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['momentum_factors', 'value_factors'],
            )

        self.assertEqual(executed_stages, ['momentum_factors'])
        self.assertEqual(result['status'], 'interrupted')
        self.assertEqual(result['reason'], 'pipeline_run_ownership_lost')
        self.assertNotIn('finish', self.run_state_service.runs[-1])
        self.assertFalse(
            any(
                event['event'] == 'finish_stage'
                for event in self.run_state_service.stage_events
            )
        )

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

    def test_feedback_rearm_keeps_parent_prerequisite_admission(self) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )
        cutoff = datetime(
            2026,
            4,
            14,
            8,
            30,
            tzinfo=timezone.utc,
        )
        symbols = ['005930', '000660']
        role_symbol_scopes = {
            role: list(symbols)
            for role in SUMMARIZER_AGENT_NAMES
        }
        inherited_retry_config = {
            'source_run_id': 'feedback-cycle-parent',
            'roles': list(SUMMARIZER_AGENT_NAMES),
            'requested_symbols': list(symbols),
            'role_symbol_scopes': role_symbol_scopes,
            'knowledge_cutoff_at': cutoff,
            'feedback_scope_mode': (
                FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
            ),
            'fs_div': 'OFS',
            'policy_context_as_of_date': date(2026, 4, 15),
            'scope_authority': None,
            'idempotency_key': None,
            'outer_batch_size': 17,
        }

        with patch(
            'app.eod.daily_pipeline_stages.'
            'agent_feedback_prerequisite_service.'
            'inspect_feedback_prerequisites',
            return_value=_feedback_prerequisites(
                status='blocked',
                missing_stage_ids=['fundamentals_daily_refresh'],
            ),
        ) as inspect_prerequisites:
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                resume_from_stage='agent_feedback_summaries',
                trigger='auto_eod_followup_resume',
                point_in_time_recovery=True,
                symbols=symbols,
                fs_div='OFS',
                inherit_feedback_prerequisite_admission=True,
                inherited_feedback_knowledge_cutoff_at=cutoff,
                inherited_feedback_source_run_id=(
                    'feedback-cycle-parent'
                ),
                inherited_feedback_retry_config=(
                    inherited_retry_config
                ),
                feedback_max_cycles=4,
                feedback_outer_batch_size=17,
                reuse_existing_feedback=True,
                reused_eod_collection_run_id=7,
            )

        self.assertEqual(
            result['stages']['agent_feedback_summaries']['status'],
            'completed',
        )
        self.assertGreaterEqual(inspect_prerequisites.call_count, 2)
        self.assertGreaterEqual(len(feedback_retry_service.calls), 1)
        call = feedback_retry_service.calls[0]
        self.assertFalse(call['force'])
        self.assertTrue(call['bypass_prerequisite_gate'])
        self.assertTrue(call['config'].reuse_existing)
        self.assertEqual(
            call['config'].reuse_parent_run_id,
            'feedback-cycle-parent',
        )
        self.assertEqual(
            call['config'].knowledge_cutoff_at,
            cutoff,
        )
        self.assertEqual(call['config'].symbols, tuple(symbols))
        self.assertEqual(
            call['config'].role_symbol_scopes,
            role_symbol_scopes,
        )
        self.assertEqual(
            call['config'].feedback_scope_mode,
            FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
        )
        self.assertEqual(call['config'].fs_div, 'OFS')
        self.assertEqual(
            call['config'].policy_context_as_of_date,
            date(2026, 4, 15),
        )
        self.assertIsNone(call['config'].scope_authority)
        self.assertEqual(call['config'].outer_batch_size, 17)
        self.assertEqual(call['config'].max_cycles, 4)
        historical_metadata = self.run_state_service.runs[-1][
            'run_metadata'
        ]
        self.assertEqual(
            historical_metadata['inherited_feedback_source_run_id'],
            'feedback-cycle-parent',
        )
        self.assertEqual(
            historical_metadata[
                'inherited_feedback_knowledge_cutoff_at'
            ],
            cutoff.isoformat(),
        )
        self.assertEqual(
            historical_metadata['feedback_retry_parent_contract'],
            {
                'schema_version': 'feedback_retry_parent_v1',
                'source_run_id': 'feedback-cycle-parent',
                'knowledge_cutoff_at': cutoff.isoformat(),
                'feedback_scope_mode': (
                    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                ),
            },
        )

        same_day_retry_service = _FakeFeedbackRetryService()
        same_day_service = DailyPipelineStageService(
            feedback_retry_service=same_day_retry_service,
        )
        with patch(
            'app.eod.daily_pipeline_stages.'
            'agent_feedback_prerequisite_service.'
            'inspect_feedback_prerequisites',
            return_value=_feedback_prerequisites(),
        ) as inspect_same_day_prerequisites:
            same_day_result = same_day_service.run(
                market_date='2026-04-14',
                stages=[
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                resume_from_stage='agent_feedback_summaries',
                trigger='auto_eod_followup_resume',
                symbols=symbols,
                fs_div='OFS',
                inherited_feedback_retry_config=inherited_retry_config,
                feedback_max_cycles=4,
                feedback_outer_batch_size=17,
                reuse_existing_feedback=True,
                reused_eod_collection_run_id=7,
            )

        self.assertEqual(
            same_day_result['stages']['agent_feedback_summaries'][
                'status'
            ],
            'completed',
        )
        self.assertGreaterEqual(
            inspect_same_day_prerequisites.call_count,
            1,
        )
        same_day_call = same_day_retry_service.calls[0]
        self.assertFalse(same_day_call['bypass_prerequisite_gate'])
        self.assertEqual(
            same_day_call['config'].reuse_parent_run_id,
            'feedback-cycle-parent',
        )
        self.assertEqual(same_day_call['config'].knowledge_cutoff_at, cutoff)
        self.assertEqual(same_day_call['config'].symbols, tuple(symbols))
        self.assertEqual(
            same_day_call['config'].feedback_scope_mode,
            FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
        )
        same_day_metadata = self.run_state_service.runs[-1][
            'run_metadata'
        ]
        self.assertEqual(
            same_day_metadata['inherited_feedback_source_run_id'],
            'feedback-cycle-parent',
        )
        self.assertEqual(
            same_day_metadata['feedback_retry_parent_contract'][
                'feedback_scope_mode'
            ],
            FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
        )

    def test_signal_decider_does_not_reuse_summary_parent_lineage(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )
        cutoff = datetime(
            2026,
            4,
            14,
            8,
            30,
            tzinfo=timezone.utc,
        )

        with patch(
            'app.eod.daily_pipeline_stages.'
            'build_expired_decision_replay_result',
            return_value=None,
        ):
            result = service._execute_stage(
                stage_id='signal_decider',
                market_date='2026-04-14',
                symbols=['005930'],
                markets=['KOSPI'],
                force=False,
                bypass_prerequisite_gate=True,
                fs_div='CFS',
                symbol_limit=None,
                symbol_offset=0,
                capture_limit=500,
                retry_cycles=None,
                feedback_max_cycles=3,
                feedback_outer_batch_size=20,
                reuse_existing_feedback=True,
                feedback_knowledge_cutoff_at=cutoff,
                feedback_reuse_parent_run_id=(
                    'feedback-cycle-summary-parent'
                ),
                feedback_cutoff_resolution={
                    'ready': True,
                    'knowledge_cutoff_at': cutoff,
                    'source_run_id': 'feedback-cycle-summary-parent',
                    'feedback_scope_mode': (
                        'broad_feedback_universe'
                    ),
                    'requested_symbols': ['005930'],
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': ['005930'],
                        'admitted_symbols': ['005930'],
                    },
                },
            )

        self.assertTrue(result['completed'])
        self.assertEqual(len(feedback_retry_service.calls), 1)
        config = feedback_retry_service.calls[0]['config']
        self.assertEqual(
            config.feedback_scope_mode,
            'decider_scope_fast_path',
        )
        self.assertIsNone(config.reuse_parent_run_id)

    def test_run_rejects_unknown_stage(self) -> None:
        service = DailyPipelineStageService()

        with self.assertRaisesRegex(ValueError, 'Unknown daily stage'):
            service.run(market_date='2026-04-14', stages=['not-a-stage'])

    def test_next_open_only_request_injects_signal_decider_dependency(self) -> None:
        service = DailyPipelineStageService()

        validated = service.validate_run_request(
            market_date='2026-04-14',
            stages=['next_open_execution_plan'],
            resume_from_stage=None,
        )

        self.assertEqual(
            validated['resolved_stages'],
            ['signal_decider', 'next_open_execution_plan'],
        )

    def test_next_open_resume_keeps_persisted_dependency_boundary(self) -> None:
        service = DailyPipelineStageService()

        validated = service.validate_run_request(
            market_date='2026-04-14',
            stages=['next_open_execution_plan'],
            resume_from_stage='next_open_execution_plan',
        )

        self.assertEqual(
            validated['resolved_stages'],
            ['next_open_execution_plan'],
        )

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

    def test_resolve_result_status_downgrades_completed_feedback_with_pending_role(self) -> None:
        status = DailyPipelineStageService._resolve_result_status(
            {
                'status': 'completed',
                'completed': True,
                'final_pending': 0,
                'progress': {
                    'summary': {
                        'expected_feedback_count': 239,
                        'completed_feedback_count': 239,
                        'failed_items': 0,
                        'role_progress': {
                            'fundamental-summarizer': {
                                'status': 'partial',
                                'total_symbols': 80,
                                'processed_symbols': 79,
                                'completed_symbols': 79,
                                'pending_symbols': 1,
                                'failed_items': 0,
                            }
                        },
                    }
                },
            }
        )

        self.assertEqual(status, 'partial')

    def test_stage_result_details_omit_high_volume_nested_evidence(self) -> None:
        details = DailyPipelineStageService._build_stage_run_details(
            stage_id='signal_decider_evaluations',
            result={
                'status': 'completed',
                'summary': {'evaluations_upserted': 200},
                'evaluation_refresh': {
                    'summary': {'evaluations_upserted': 200},
                    'items': [{'symbol': f'{index:06d}'} for index in range(200)],
                },
                'cycles': [{'items': list(range(200))}],
                'matched_symbols': [f'{index:06d}' for index in range(200)],
                'progress': {
                    'summary': {
                        'completed_feedback_count': 200,
                        'funnel_diagnostics': {'symbols': list(range(200))},
                    }
                },
            },
        )

        result_summary = details['result_summary']
        self.assertEqual(
            result_summary['evaluation_refresh'],
            {'summary': {'evaluations_upserted': 200}},
        )
        self.assertNotIn('cycles', result_summary)
        self.assertNotIn('matched_symbols', result_summary)
        self.assertNotIn(
            'funnel_diagnostics',
            result_summary['progress']['summary'],
        )
        self.assertEqual(
            details['result_summary_omissions']['evaluation_refresh.items'],
            {'type': 'list', 'item_count': 200},
        )
        self.assertEqual(
            details['result_summary_omissions']['cycles'],
            {'type': 'list', 'item_count': 1},
        )
        self.assertLess(len(str(details)), 5_000)

    def test_stage_result_details_preserve_scalar_aggregates_with_bulk_field_names(self) -> None:
        details = DailyPipelineStageService._build_stage_run_details(
            stage_id='signal_decider_evaluations',
            result={
                'status': 'completed',
                'summary': {
                    'items': 201,
                    'rows': 200,
                    'stages': 3,
                    'request_payload': None,
                },
            },
        )

        self.assertEqual(
            details['result_summary']['summary'],
            {
                'items': 201,
                'rows': 200,
                'stages': 3,
                'request_payload': None,
            },
        )
        self.assertNotIn('result_summary_omissions', details)

    def test_resolve_result_status_uses_role_boundary_and_completed_counts(self) -> None:
        status = DailyPipelineStageService._resolve_result_status(
            {
                'status': 'completed',
                'progress': {
                    'summary': {
                        'role_progress': {
                            'fundamental-summarizer': {
                                'status': 'completed',
                                'boundary_status': 'incomplete',
                                'total_symbols': 80,
                                'processed_symbols': 80,
                                'completed_symbols': 79,
                                'pending_symbols': 0,
                                'retry_pending_symbols': 1,
                                'failed_items': 0,
                            }
                        }
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

    def test_resolve_result_status_rejects_persisted_factor_rows_without_ready_content(
        self,
    ) -> None:
        status = DailyPipelineStageService._resolve_result_status(
            {
                'results_persisted': 2_678,
                'results_failed': 0,
                'summary': {
                    'requested_symbol_count': 2_678,
                    'matched_symbol_count': 2_678,
                    'insufficient_history_count': 2_678,
                    'ready_count': 0,
                },
            }
        )

        self.assertEqual(status, 'partial')

    def test_resolve_result_status_blocks_zero_scope_with_incomplete_context(
        self,
    ) -> None:
        status = DailyPipelineStageService._resolve_result_status(
            {
                'summary': {
                    'items_total': 0,
                    'symbol_count': 0,
                    'agent_context_completeness_status': 'incomplete',
                    'agent_context_completeness': {
                        'status': 'incomplete',
                        'candidate_set_definitive': False,
                        'pending_scope_count': 157,
                    },
                },
                'items': [],
            }
        )

        self.assertEqual(status, 'blocked')

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

    def test_extract_stage_counts_keeps_trending_progress_in_period_units(self) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='trending_ranks',
            result={
                'trending_ranks_persisted': 6,
                'periods_persisted': 3,
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 3,
                'total': 3,
                'succeeded': 3,
                'failed': 0,
            },
        )

    def test_extract_stage_counts_uses_signal_decider_terminal_work(self) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='signal_decider',
            result={
                'status': 'completed',
                'completed': True,
                'final_pending': 0,
                'progress': {
                    'result': {
                        'roles': {
                            'short-term-decider': {
                                'requested_count': 80,
                                'completed_count': 80,
                                'error_count': 0,
                            },
                            'signal-decider': {
                                'requested_count': 80,
                                'completed_count': 80,
                                'error_count': 0,
                            },
                        },
                    },
                    'summary': {
                        'run_status': 'completed',
                        'run_table_status': 'completed',
                        'expected_feedback_count': 160,
                        'completed_feedback_count': 160,
                        'failed_items': 0,
                    },
                },
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 160,
                'total': 160,
                'succeeded': 160,
                'failed': 0,
            },
        )

    def test_extract_stage_counts_uses_signal_decider_evaluation_writes(
        self,
    ) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='signal_decider_evaluations',
            result={
                'request': {
                    'horizons': [1, 3, 5, 10],
                },
                'summary': {
                    'status': 'completed',
                    'decisions_considered': 960,
                    'outcomes_upserted': 3_840,
                    'evaluations_upserted': 3_840,
                    'not_computed_count': 0,
                    'warning_count': 0,
                    'coverage': {
                        'count': 3_840,
                        'computed_count': 2_284,
                        'pending_maturity_count': 1_550,
                        'insufficient_price_data_count': 6,
                        'not_computed_count': 0,
                    },
                },
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 3_840,
                'total': 3_840,
                'succeeded': 3_840,
                'failed': 0,
            },
        )

    def test_extract_stage_counts_keeps_decider_request_denominator_when_summary_is_truncated(
        self,
    ) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='signal_decider',
            result={
                'progress': {
                    'result': {
                        'roles': {
                            'short-term-decider': {
                                'requested_count': 80,
                                'completed_count': 80,
                                'error_count': 0,
                            },
                            'signal-decider': {
                                'requested_count': 80,
                                'completed_count': 80,
                                'error_count': 0,
                            },
                        },
                    },
                    'summary': {
                        'expected_feedback_count': 65,
                        'completed_feedback_count': 65,
                        'failed_items': 0,
                    },
                },
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 65,
                'total': 160,
                'succeeded': 65,
                'failed': 0,
            },
        )

    def test_extract_stage_counts_keeps_evaluation_request_denominator_when_writes_are_truncated(
        self,
    ) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='signal_decider_evaluations',
            result={
                'request': {
                    'horizons': [1, 3, 5, 10],
                },
                'summary': {
                    'decisions_considered': 960,
                    'outcomes_upserted': 65,
                    'evaluations_upserted': 65,
                    'failed_count': 0,
                    'coverage': {
                        'count': 3_840,
                        'computed_count': 2_284,
                        'pending_maturity_count': 1_550,
                        'insufficient_price_data_count': 6,
                        'not_computed_count': 0,
                    },
                },
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 65,
                'total': 3_840,
                'succeeded': 65,
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

    def test_extract_stage_counts_treats_ready_enrichment_warning_as_success(
        self,
    ) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='pykrx_enrichment',
            result={
                'categories_requested': [
                    'sector_classification',
                    'short_selling',
                ],
                'categories': {
                    'sector_classification': {
                        'status': 'completed',
                        'rows_persisted': 10,
                        'errors': [],
                    },
                    'short_selling': {
                        'status': 'partial',
                        'rows_persisted': 10,
                        'errors': [
                            {
                                'operation': 'short_selling_balance',
                                'error': 'empty provider frame',
                            }
                        ],
                    },
                },
                'downstream_preflight': {
                    'allowed': True,
                    'status': 'ready',
                    'required_categories': [
                        'sector_classification',
                        'investor_flow_symbol',
                        'short_selling',
                        'valuation_snapshot',
                    ],
                    'category_row_counts': {
                        'sector_classification': 10,
                        'investor_flow_symbol': 10,
                        'short_selling': 10,
                        'valuation_snapshot': 10,
                    },
                    'failed_categories': [],
                    'blocked_categories': [],
                    'waiting_categories': [],
                    'zero_coverage_categories': [],
                },
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 2,
                'total': 2,
                'succeeded': 2,
                'failed': 0,
            },
        )

    def test_extract_stage_counts_keeps_preflight_failed_enrichment_failed(
        self,
    ) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='pykrx_enrichment',
            result={
                'categories_requested': [
                    'sector_classification',
                    'short_selling',
                ],
                'categories': {
                    'sector_classification': {
                        'status': 'completed',
                        'rows_persisted': 10,
                        'errors': [],
                    },
                    'short_selling': {
                        'status': 'partial',
                        'rows_persisted': 10,
                        'errors': ['coverage incomplete'],
                    },
                },
                'downstream_preflight': {
                    'allowed': False,
                    'status': 'blocked',
                    'failed_categories': ['short_selling'],
                },
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 2,
                'total': 2,
                'succeeded': 1,
                'failed': 1,
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

    def test_execute_stage_forwards_market_date_to_external_news_enrichment(self) -> None:
        service = DailyPipelineStageService()

        with patch(
            'app.eod.daily_pipeline_stages.external_news_service.enrich_recent_classification',
            return_value={'status': 'completed', 'items_inspected': 2},
        ) as enrich:
            result = service._execute_stage(
                stage_id='external_news_enrich',
                market_date='2026-10-07',
                symbols=['005930', '000660'],
                markets=[],
                force=False,
                bypass_prerequisite_gate=False,
                fs_div='CFS',
                symbol_limit=100,
                symbol_offset=0,
                capture_limit=500,
                retry_cycles=None,
                feedback_max_cycles=3,
                feedback_outer_batch_size=20,
                reuse_existing_feedback=True,
            )

        self.assertEqual(result['status'], 'completed')
        enrich.assert_called_once_with(
            symbols=['005930', '000660'],
            market_date='2026-10-07',
            lookback_days=settings.external_news_context_lookback_days,
            limit=100,
        )

    def test_technical_stage_repairs_price_contexts_for_the_same_scope(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        call_kwargs = {
            'stage_id': 'technical_snapshots',
            'market_date': '2026-04-24',
            'symbols': ['005930', '000660'],
            'markets': ['KOSPI'],
            'force': False,
            'bypass_prerequisite_gate': False,
            'fs_div': 'CFS',
            'symbol_limit': None,
            'symbol_offset': 0,
            'capture_limit': 500,
            'retry_cycles': None,
            'feedback_max_cycles': 3,
            'feedback_outer_batch_size': 20,
            'reuse_existing_feedback': True,
        }

        with (
            patch(
                'app.eod.daily_pipeline_stages.daily_price_context_service.'
                'rebuild_for_market_date',
                return_value={
                    'contexts_persisted': 2,
                    'contexts_failed': 0,
                    'items': [{'symbol': '005930'}],
                },
            ) as rebuild_contexts,
            patch(
                'app.eod.daily_pipeline_stages.daily_technical_snapshot_service.'
                'rebuild_for_market_date',
                return_value={
                    'snapshots_persisted': 2,
                    'snapshots_failed': 0,
                },
            ) as rebuild_snapshots,
        ):
            result = service._execute_stage(**call_kwargs)

        expected_scope = {
            'market_date': '2026-04-24',
            'symbols': ['005930', '000660'],
            'source': 'stage_orchestrator',
            'run_id': None,
        }
        rebuild_contexts.assert_called_once_with(**expected_scope)
        rebuild_snapshots.assert_called_once_with(**expected_scope)
        self.assertEqual(result['contexts_persisted'], 2)
        self.assertEqual(result['contexts_failed'], 0)
        self.assertNotIn('items', result['price_contexts'])

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

        with (
            patch(
                'app.eod.daily_pipeline_stages.get_market_state',
                return_value=MagicMock(state='PREOPEN'),
            ),
            patch(
                'app.orders.next_open_execution.signal_decider_next_open_execution_service.refresh_plan_items',
                return_value={'summary': {'items_total': 2, 'eligible_count': 1}},
            ) as refresh_plan,
        ):
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
                point_in_time_recovery=True,
            )

        self.assertEqual(result['summary']['items_total'], 2)
        refresh_plan.assert_called_once_with(
            target_trade_date='2026-04-15',
            trigger='daily_pipeline_post_decider',
            force=False,
        )

    def test_execute_stage_skips_expired_prior_date_open_plan(self) -> None:
        service = DailyPipelineStageService()
        market = MagicMock(
            state='OPEN',
            server_time='2026-09-14T10:00:00+09:00',
        )

        with (
            patch(
                'app.eod.daily_pipeline_stages.get_market_state',
                return_value=market,
            ),
            patch(
                'app.orders.next_open_execution.normalize_next_open_target_trade_date',
                return_value=datetime(2026, 9, 14).date(),
            ),
            patch(
                'app.orders.next_open_execution.signal_decider_next_open_execution_service.refresh_plan_items',
            ) as refresh_plan,
        ):
            result = service._execute_stage(
                stage_id='next_open_execution_plan',
                market_date='2026-09-11',
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

        self.assertFalse(result['triggered'])
        self.assertEqual(result['status'], 'skipped')
        self.assertEqual(result['reason'], 'next_open_window_expired')
        self.assertEqual(result['stage'], 'next_open_execution_plan')
        refresh_plan.assert_not_called()

    def test_execute_stage_hands_preopen_admitted_recovery_to_open_plan(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        market = MagicMock(
            state='OPEN',
            server_time='2026-09-14T09:01:00+09:00',
        )

        with (
            patch(
                'app.eod.daily_pipeline_stages.get_market_state',
                return_value=market,
            ),
            patch(
                'app.orders.next_open_execution.'
                'normalize_next_open_target_trade_date',
                return_value=date(2026, 9, 14),
            ),
            patch(
                'app.orders.next_open_execution.'
                'signal_decider_next_open_execution_service.'
                'refresh_plan_items',
                return_value={'status': 'completed'},
            ) as refresh_plan,
        ):
            result = service._execute_stage(
                stage_id='next_open_execution_plan',
                market_date='2026-09-11',
                symbols=None,
                markets=[],
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
                point_in_time_recovery=True,
                allow_same_day_intraday_watch_continuation=True,
            )

        self.assertEqual(result['status'], 'completed')
        refresh_plan.assert_called_once_with(
            target_trade_date='2026-09-12',
            trigger='daily_pipeline_post_decider',
            force=False,
        )

    def test_execute_stage_skips_expired_prior_date_decision_replay(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        evaluation_service = _FakeSignalDeciderEvaluationService()
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
            decision_loop_service=evaluation_service,
        )
        market = MagicMock(
            state='OPEN',
            server_time='2026-09-14T10:00:00+09:00',
        )
        call_kwargs = {
            'market_date': '2026-09-11',
            'symbols': ['005930'],
            'markets': [],
            'force': False,
            'bypass_prerequisite_gate': False,
            'fs_div': 'CFS',
            'symbol_limit': None,
            'symbol_offset': 0,
            'capture_limit': 500,
            'retry_cycles': None,
            'feedback_max_cycles': 3,
            'feedback_outer_batch_size': 20,
            'reuse_existing_feedback': True,
        }

        with (
            patch(
                'app.eod.daily_pipeline_stages.get_market_state',
                return_value=market,
            ),
            patch(
                'app.orders.next_open_execution.'
                'normalize_next_open_target_trade_date',
                return_value=datetime(2026, 9, 14).date(),
            ),
        ):
            results = [
                service._execute_stage(stage_id=stage_id, **call_kwargs)
                for stage_id in (
                    'agent_feedback_summaries',
                    'signal_decider',
                    'signal_decider_evaluations',
                )
            ]

        self.assertTrue(
            all(result['status'] == 'skipped' for result in results)
        )
        self.assertTrue(
            all(
                result['reason'] == 'next_open_window_expired'
                for result in results
            )
        )
        self.assertEqual(feedback_retry_service.calls, [])
        self.assertEqual(evaluation_service.calls, [])

    def test_execute_decider_crossing_market_open_finishes_as_expired(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )
        call_kwargs = {
            'stage_id': 'signal_decider',
            'market_date': '2026-09-11',
            'symbols': ['005930'],
            'markets': [],
            'force': False,
            'bypass_prerequisite_gate': False,
            'fs_div': 'CFS',
            'symbol_limit': None,
            'symbol_offset': 0,
            'capture_limit': 500,
            'retry_cycles': None,
            'feedback_max_cycles': 3,
            'feedback_outer_batch_size': 20,
            'reuse_existing_feedback': True,
        }

        with (
            patch(
                'app.eod.daily_pipeline_stages.get_market_state',
                side_effect=[
                    MagicMock(
                        state='PREOPEN',
                        server_time='2026-09-14T08:59:00+09:00',
                    ),
                    MagicMock(
                        state='OPEN',
                        server_time='2026-09-14T09:01:00+09:00',
                    ),
                ],
            ),
            patch(
                'app.orders.next_open_execution.'
                'normalize_next_open_target_trade_date',
                return_value=datetime(2026, 9, 14).date(),
            ),
        ):
            result = service._execute_stage(**call_kwargs)

        self.assertEqual(result['status'], 'skipped')
        self.assertEqual(result['reason'], 'next_open_window_expired')
        self.assertEqual(len(feedback_retry_service.calls), 1)

    def test_execute_decider_keeps_preopen_admission_after_market_open(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )
        call_kwargs = {
            'stage_id': 'signal_decider',
            'market_date': '2026-09-11',
            'symbols': ['005930'],
            'markets': [],
            'force': False,
            'bypass_prerequisite_gate': False,
            'fs_div': 'CFS',
            'symbol_limit': None,
            'symbol_offset': 0,
            'capture_limit': 500,
            'retry_cycles': None,
            'feedback_max_cycles': 3,
            'feedback_outer_batch_size': 20,
            'reuse_existing_feedback': True,
            'allow_same_day_intraday_watch_continuation': True,
        }

        with (
            patch(
                'app.eod.daily_pipeline_stages.get_market_state',
                side_effect=[
                    MagicMock(
                        state='PREOPEN',
                        server_time='2026-09-14T08:59:00+09:00',
                    ),
                    MagicMock(
                        state='OPEN',
                        server_time='2026-09-14T09:01:00+09:00',
                    ),
                ],
            ),
            patch(
                'app.orders.next_open_execution.'
                'normalize_next_open_target_trade_date',
                return_value=date(2026, 9, 14),
            ),
        ):
            result = service._execute_stage(**call_kwargs)

        self.assertTrue(result['completed'])
        self.assertEqual(len(feedback_retry_service.calls), 1)

    def test_exact_auto_recovery_persists_preopen_plan_admission(self) -> None:
        service = DailyPipelineStageService()
        preopen = MagicMock(
            state='PREOPEN',
            server_time='2026-09-14T08:59:00+09:00',
        )

        with (
            patch(
                'app.eod.daily_pipeline_stages.get_market_state',
                return_value=preopen,
            ),
            patch(
                'app.orders.next_open_execution.'
                'normalize_next_open_target_trade_date',
                return_value=date(2026, 9, 14),
            ),
            patch.object(
                service,
                '_execute_stage',
                return_value={'status': 'completed'},
            ) as execute_stage,
        ):
            result = service.run(
                market_date='2026-09-11',
                stages=['next_open_execution_plan'],
                resume_from_stage='next_open_execution_plan',
                trigger='auto_eod_followup_resume',
                point_in_time_recovery=True,
            )

        self.assertTrue(
            execute_stage.call_args.kwargs[
                'allow_same_day_intraday_watch_continuation'
            ]
        )
        self.assertTrue(
            self.run_state_service.runs[-1]['run_metadata'][
                'same_day_intraday_watch_continuation_admitted'
            ]
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
                provider_admission_fence_owned=True,
            )

        self.assertEqual(result['stages']['price_collection']['status'], 'completed')
        self.assertEqual(result['stages']['technical_snapshots']['status'], 'partial')
        self.assertEqual(result['stages']['pykrx_enrichment']['status'], 'completed')
        enrichment_service.collect_for_market_date.assert_called_once()
        self.assertTrue(
            enrichment_service.collect_for_market_date.call_args.kwargs[
                'use_full_market_coverage'
            ]
        )
        self.assertTrue(
            enrichment_service.collect_for_market_date.call_args.kwargs[
                'daily_price_bar_lock_owned'
            ]
        )

    def test_grouped_price_context_failure_marks_technical_stage_partial(
        self,
    ) -> None:
        pykrx_runner = MagicMock()
        pykrx_runner.run_daily_pipeline_stages.return_value = {
            'run_id': 903,
            'status': 'completed_with_errors',
            'latest_available_trade_date': '2026-04-14',
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'bars_upserted': 2,
            'price_contexts': {
                'contexts_persisted': 1,
                'contexts_failed': 1,
                '_retry_metadata': {'failed_symbols': ['000660']},
            },
            'technical_snapshots': {
                'market_date': '2026-04-14',
                'snapshots_persisted': 2,
                'snapshots_failed': 0,
            },
        }
        service = DailyPipelineStageService(pykrx_service=pykrx_runner)

        result = service.run(
            market_date='2026-04-14',
            stages=['price_collection', 'technical_snapshots'],
        )

        technical = result['stages']['technical_snapshots']
        self.assertEqual(technical['status'], 'partial')
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(technical['result']['contexts_persisted'], 1)
        self.assertEqual(technical['result']['contexts_failed'], 1)
        self.assertEqual(
            technical['result']['price_contexts']['_retry_metadata'][
                'failed_symbols'
            ],
            ['000660'],
        )
        technical_finish = next(
            event
            for event in self.run_state_service.stage_events
            if event['event'] == 'finish_stage'
            and event['stage_id'] == 'technical_snapshots'
        )
        self.assertEqual(technical_finish['status'], 'partial')
        self.assertEqual(technical_finish['failed'], 1)

    def test_execute_pykrx_enrichment_blocks_zero_required_coverage(self) -> None:
        required_categories = (
            'sector_classification',
            'investor_flow_symbol',
            'short_selling',
            'valuation_snapshot',
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

    def test_execute_pykrx_enrichment_completes_ready_provider_warning(
        self,
    ) -> None:
        required_categories = (
            'sector_classification',
            'investor_flow_symbol',
            'short_selling',
            'valuation_snapshot',
        )
        collection_result = {
            'status': 'partial',
            'categories_requested': list(required_categories),
            'categories': {
                'sector_classification': {
                    'status': 'completed',
                    'rows_persisted': 10,
                    'errors': [],
                },
                'investor_flow_symbol': {
                    'status': 'completed',
                    'rows_persisted': 10,
                    'errors': [],
                },
                'short_selling': {
                    'status': 'partial',
                    'rows_persisted': 10,
                    'errors': ['empty provider frame'],
                },
                'valuation_snapshot': {
                    'status': 'completed',
                    'rows_persisted': 10,
                    'errors': [],
                },
            },
            'coverage': {},
        }
        ready_preflight = {
            'allowed': True,
            'status': 'ready',
            'required_categories': list(required_categories),
            'category_row_counts': {
                category: 10 for category in required_categories
            },
            'failed_categories': [],
            'blocked_categories': [],
            'waiting_categories': [],
            'zero_coverage_categories': [],
        }
        service, result = self._execute_pykrx_enrichment(
            collection_result=collection_result,
            preflight=ready_preflight,
        )

        self.assertEqual(result['status'], 'completed')
        outcome = service._build_stage_outcome(
            'pykrx_enrichment',
            result,
        )
        self.assertEqual(outcome['status'], 'completed')
        self.assertTrue(service._dependency_outcome_is_satisfied(outcome))
        self.assertEqual(
            service._extract_stage_counts(
                stage_id='pykrx_enrichment',
                result=result,
            ),
            {
                'processed': 4,
                'total': 4,
                'succeeded': 4,
                'failed': 0,
            },
        )

    def test_execute_pykrx_enrichment_keeps_incomplete_optional_work_partial(
        self,
    ) -> None:
        required_categories = (
            'sector_classification',
            'investor_flow_symbol',
            'short_selling',
        )
        ready_preflight = {
            'allowed': True,
            'status': 'ready',
            'required_categories': list(required_categories),
            'category_row_counts': {
                category: 10 for category in required_categories
            },
            'failed_categories': [],
            'blocked_categories': [],
            'waiting_categories': [],
            'zero_coverage_categories': [],
        }
        required_results = {
            category: {
                'status': 'completed',
                'rows_persisted': 10,
                'errors': [],
            }
            for category in required_categories
        }
        cases = {
            'failed_optional_category': (
                {
                    **required_results,
                    'foreign_ownership': {
                        'status': 'partial',
                        'rows_persisted': 0,
                        'errors': ['provider unavailable'],
                    },
                },
                {'processed': 4, 'total': 4, 'succeeded': 3, 'failed': 1},
            ),
            'missing_optional_category': (
                required_results,
                {'processed': 3, 'total': 4, 'succeeded': 3, 'failed': 0},
            ),
        }

        for case_name, (category_results, expected_counts) in cases.items():
            with self.subTest(case=case_name):
                collection_result = {
                    'status': 'partial',
                    'categories_requested': [
                        *required_categories,
                        'foreign_ownership',
                    ],
                    'categories': category_results,
                    'coverage': {},
                }
                service, result = self._execute_pykrx_enrichment(
                    collection_result=collection_result,
                    preflight=ready_preflight,
                )

                self.assertEqual(result['status'], 'partial')
                outcome = service._build_stage_outcome(
                    'pykrx_enrichment',
                    result,
                )
                self.assertEqual(outcome['status'], 'partial')
                self.assertFalse(
                    service._dependency_outcome_is_satisfied(outcome)
                )
                self.assertEqual(
                    service._extract_stage_counts(
                        stage_id='pykrx_enrichment',
                        result=result,
                    ),
                    expected_counts,
                )

    def test_execute_pykrx_enrichment_keeps_unproven_warning_partial(
        self,
    ) -> None:
        collection_result = {
            'status': 'partial',
            'categories_requested': ['short_selling'],
            'categories': {
                'short_selling': {
                    'status': 'partial',
                    'rows_persisted': 10,
                    'errors': ['empty provider frame'],
                },
            },
            'coverage': {},
        }
        service, result = self._execute_pykrx_enrichment(
            collection_result=collection_result,
            preflight={
                'allowed': True,
                'status': 'ready',
                'required_categories': ['short_selling'],
                'category_row_counts': {'short_selling': 10},
                'failed_categories': [],
                'blocked_categories': [],
                'waiting_categories': [],
                'zero_coverage_categories': [],
            },
        )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(
            service._build_stage_outcome(
                'pykrx_enrichment',
                result,
            )['status'],
            'partial',
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
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': datetime(
                        2026,
                        4,
                        14,
                        8,
                        30,
                        tzinfo=timezone.utc,
                    ),
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': 'broad_feedback_universe',
                },
            ),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['next_open_execution_plan'],
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
