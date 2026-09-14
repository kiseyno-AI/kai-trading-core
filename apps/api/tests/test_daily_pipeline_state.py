import unittest
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db_models import DailyPipelineRun, DailyPipelineStageRun, EODCollectionRun
from app.eod.daily_pipeline_outcomes import daily_pipeline_outcome_is_satisfied
from app.eod.daily_pipeline_state import (
    DailyPipelineRunStateService,
    _stage_row_outcome,
)


class DailyPipelineRunStateServiceTests(unittest.TestCase):
    def test_startup_stage_adapter_preserves_safe_partial_evidence(self) -> None:
        stage = SimpleNamespace(
            stage_id='momentum_factors',
            status='partial',
            processed=100,
            total=100,
            succeeded=99,
            failed=1,
            error_message=None,
            details={'result_summary': {'remaining_retryable_events': 0}},
        )

        outcome = _stage_row_outcome(stage)

        self.assertEqual(outcome['stage'], 'momentum_factors')
        self.assertEqual(outcome['succeeded'], 99)
        self.assertEqual(outcome['total'], 100)
        self.assertTrue(daily_pipeline_outcome_is_satisfied(outcome))

    def test_converge_terminal_stages_run_derives_completed_and_preserves_audit(
        self,
    ) -> None:
        interrupted_at = datetime(2026, 9, 14, 7, 34, 11, tzinfo=timezone.utc)
        converged_at = interrupted_at + timedelta(minutes=5)
        run = SimpleNamespace(
            id=387,
            status='interrupted',
            error_message='Core restarted.',
            completed_at=interrupted_at,
            updated_at=interrupted_at,
            run_metadata={
                'resolved_stages': ['technical_snapshots', 'action_summaries'],
                'interrupted_reason': 'Core restarted.',
                'interrupted_at': interrupted_at.isoformat(),
                'resumable': True,
                'eod_collection_run_id': 522,
            },
        )
        stages = [
            SimpleNamespace(stage_id='technical_snapshots', status='completed'),
            SimpleNamespace(
                stage_id='action_summaries',
                status='skipped',
                details={
                    'result_summary': {'reason': 'already_completed'},
                },
            ),
        ]
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        stages_result = MagicMock()
        stages_result.scalars.return_value = stages
        session = MagicMock()
        session.execute.side_effect = [run_result, stages_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch(
                'app.eod.daily_pipeline_state._utcnow',
                return_value=converged_at,
            ),
        ):
            result = DailyPipelineRunStateService().converge_terminal_stages_run(
                pipeline_run_id=387,
            )

        self.assertEqual(
            result,
            {
                'updated': True,
                'pipeline_run_id': 387,
                'status': 'completed',
                'resolved_stages': [
                    'technical_snapshots',
                    'action_summaries',
                ],
                'stage_statuses': {
                    'technical_snapshots': 'completed',
                    'action_summaries': 'skipped',
                },
            },
        )
        self.assertEqual(run.status, 'completed')
        self.assertIsNone(run.error_message)
        self.assertEqual(run.completed_at, converged_at)
        self.assertEqual(run.updated_at, converged_at)
        self.assertEqual(run.run_metadata['interrupted_reason'], 'Core restarted.')
        self.assertEqual(
            run.run_metadata['interrupted_at'],
            interrupted_at.isoformat(),
        )
        self.assertEqual(run.run_metadata['eod_collection_run_id'], 522)
        self.assertFalse(run.run_metadata['resumable'])
        self.assertEqual(
            run.run_metadata['converged_at'],
            converged_at.isoformat(),
        )
        self.assertEqual(
            run.run_metadata['converged_from_status'],
            'interrupted',
        )

    def test_converge_terminal_stages_run_closes_running_parent_checkpoint_gap(
        self,
    ) -> None:
        now = datetime(2026, 9, 14, 7, 39, 11, tzinfo=timezone.utc)
        run = SimpleNamespace(
            id=388,
            status='running',
            error_message=None,
            completed_at=None,
            updated_at=now - timedelta(seconds=1),
            run_metadata={
                'resolved_stages': ['technical_snapshots'],
                'resumable': False,
            },
        )
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        stages_result = MagicMock()
        stages_result.scalars.return_value = [
            SimpleNamespace(
                stage_id='technical_snapshots',
                status='completed',
            )
        ]
        session = MagicMock()
        session.execute.side_effect = [run_result, stages_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
        ):
            result = (
                DailyPipelineRunStateService()
                .converge_terminal_stages_run(pipeline_run_id=388)
            )

        self.assertTrue(result['updated'])
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(run.status, 'completed')
        self.assertIsNone(run.error_message)
        self.assertEqual(run.completed_at, now)
        self.assertEqual(run.run_metadata['converged_from_status'], 'running')
        self.assertFalse(run.run_metadata['resumable'])

    def test_converge_safe_partial_stage_preserves_partial_parent_status(
        self,
    ) -> None:
        now = datetime(2026, 9, 14, 7, 39, 11, tzinfo=timezone.utc)
        run = SimpleNamespace(
            id=388,
            status='interrupted',
            error_message='Core restarted.',
            completed_at=now - timedelta(minutes=1),
            updated_at=now - timedelta(minutes=1),
            run_metadata={
                'resolved_stages': ['momentum_factors'],
                'resumable': True,
            },
        )
        stage = SimpleNamespace(
            stage_id='momentum_factors',
            status='partial',
            processed=100,
            total=100,
            succeeded=99,
            failed=1,
            details={'result_summary': {'remaining_retryable_events': 0}},
        )
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        stages_result = MagicMock()
        stages_result.scalars.return_value = [stage]
        session = MagicMock()
        session.execute.side_effect = [run_result, stages_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
        ):
            result = (
                DailyPipelineRunStateService()
                .converge_terminal_stages_run(pipeline_run_id=388)
            )

        self.assertTrue(result['updated'])
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(run.status, 'partial')
        self.assertFalse(run.run_metadata['resumable'])

    def test_converge_satisfied_terminal_partial_marks_non_resumable(
        self,
    ) -> None:
        completed_at = datetime(2026, 9, 14, 7, 39, 11, tzinfo=timezone.utc)
        finalized_at = completed_at + timedelta(seconds=1)
        run = SimpleNamespace(
            id=393,
            status='partial',
            error_message='One factor row was unavailable.',
            completed_at=completed_at,
            updated_at=completed_at,
            run_metadata={
                'resolved_stages': ['momentum_factors'],
                'resumable': True,
                'reused_eod_collection_run_id': 526,
            },
        )
        stage = SimpleNamespace(
            stage_id='momentum_factors',
            status='partial',
            processed=100,
            total=100,
            succeeded=99,
            failed=1,
            error_message=None,
            details={'result_summary': {'remaining_retryable_events': 0}},
        )
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        stages_result = MagicMock()
        stages_result.scalars.return_value = [stage]
        session = MagicMock()
        session.execute.side_effect = [run_result, stages_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch(
                'app.eod.daily_pipeline_state._utcnow',
                return_value=finalized_at,
            ),
        ):
            result = (
                DailyPipelineRunStateService()
                .converge_terminal_stages_run(
                    pipeline_run_id=393,
                    allow_satisfied_terminal=True,
                )
            )

        self.assertEqual(
            result,
            {
                'updated': True,
                'pipeline_run_id': 393,
                'status': 'partial',
                'resolved_stages': ['momentum_factors'],
                'stage_statuses': {'momentum_factors': 'partial'},
            },
        )
        self.assertEqual(run.status, 'partial')
        self.assertEqual(run.error_message, 'One factor row was unavailable.')
        self.assertEqual(run.completed_at, completed_at)
        self.assertEqual(run.updated_at, finalized_at)
        self.assertEqual(run.run_metadata['reused_eod_collection_run_id'], 526)
        self.assertFalse(run.run_metadata['resumable'])
        self.assertEqual(
            run.run_metadata['terminal_satisfaction_confirmed_at'],
            finalized_at.isoformat(),
        )
        self.assertEqual(
            run.run_metadata['stage_statuses'],
            {'momentum_factors': 'partial'},
        )

    def test_converge_satisfied_terminal_partial_rejects_retryable_stage(
        self,
    ) -> None:
        completed_at = datetime(2026, 9, 14, 7, 39, 11, tzinfo=timezone.utc)
        metadata = {
            'resolved_stages': ['momentum_factors'],
            'resumable': True,
        }
        run = SimpleNamespace(
            id=394,
            status='partial',
            error_message='Retryable factor work remains.',
            completed_at=completed_at,
            updated_at=completed_at,
            run_metadata=dict(metadata),
        )
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        stages_result = MagicMock()
        stages_result.scalars.return_value = [
            SimpleNamespace(
                stage_id='momentum_factors',
                status='partial',
                processed=99,
                total=100,
                succeeded=99,
                failed=0,
                error_message=None,
                details={
                    'result_summary': {'remaining_retryable_events': 1},
                },
            )
        ]
        session = MagicMock()
        session.execute.side_effect = [run_result, stages_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ):
            result = (
                DailyPipelineRunStateService()
                .converge_terminal_stages_run(
                    pipeline_run_id=394,
                    allow_satisfied_terminal=True,
                )
            )

        self.assertEqual(
            result,
            {
                'updated': False,
                'pipeline_run_id': 394,
                'status': 'partial',
                'reason': 'resolved_stages_incomplete',
                'incomplete_stages': {'momentum_factors': 'partial'},
            },
        )
        self.assertEqual(run.status, 'partial')
        self.assertEqual(run.error_message, 'Retryable factor work remains.')
        self.assertEqual(run.completed_at, completed_at)
        self.assertEqual(run.updated_at, completed_at)
        self.assertEqual(run.run_metadata, metadata)

    def test_converge_terminal_stages_run_derives_skipped_when_all_stages_skipped(
        self,
    ) -> None:
        now = datetime(2026, 9, 14, 7, 39, 11, tzinfo=timezone.utc)
        run = SimpleNamespace(
            id=388,
            status='interrupted',
            error_message='Core restarted.',
            completed_at=now - timedelta(minutes=1),
            updated_at=now - timedelta(minutes=1),
            run_metadata={
                'resolved_stages': ['external_news_collect'],
                'interrupted_reason': 'Core restarted.',
                'resumable': True,
            },
        )
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        stages_result = MagicMock()
        stages_result.scalars.return_value = [
            SimpleNamespace(
                stage_id='external_news_collect',
                status='skipped',
                details={
                    'result_summary': {
                        'reason': 'no_agent_feedback_eligible_symbols',
                    },
                },
            )
        ]
        session = MagicMock()
        session.execute.side_effect = [run_result, stages_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
        ):
            result = DailyPipelineRunStateService().converge_terminal_stages_run(
                pipeline_run_id=388,
            )

        self.assertTrue(result['updated'])
        self.assertEqual(result['status'], 'skipped')
        self.assertEqual(run.status, 'skipped')
        self.assertIsNone(run.error_message)
        self.assertFalse(run.run_metadata['resumable'])

    def test_converge_terminal_stages_run_rejects_unsafe_skipped_stage(
        self,
    ) -> None:
        interrupted_at = datetime(2026, 9, 14, 7, 39, 11, tzinfo=timezone.utc)
        metadata = {
            'resolved_stages': ['fundamentals_daily_refresh'],
            'interrupted_reason': 'Core restarted.',
            'resumable': True,
        }
        run = SimpleNamespace(
            id=392,
            status='interrupted',
            error_message='Core restarted.',
            completed_at=interrupted_at,
            updated_at=interrupted_at,
            run_metadata=dict(metadata),
        )
        stages_result = MagicMock()
        stages_result.scalars.return_value = [
            SimpleNamespace(
                stage_id='fundamentals_daily_refresh',
                status='skipped',
                details={
                    'result_summary': {
                        'reason': 'dart_api_key_not_configured',
                    },
                },
            )
        ]
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        session = MagicMock()
        session.execute.side_effect = [run_result, stages_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ):
            result = DailyPipelineRunStateService().converge_terminal_stages_run(
                pipeline_run_id=392,
            )

        self.assertEqual(result['reason'], 'resolved_stages_incomplete')
        self.assertEqual(
            result['incomplete_stages'],
            {'fundamentals_daily_refresh': 'skipped'},
        )
        self.assertFalse(result['updated'])
        self.assertEqual(run.status, 'interrupted')
        self.assertEqual(run.run_metadata, metadata)

    def test_converge_terminal_stages_run_fails_closed_when_stage_row_is_missing(
        self,
    ) -> None:
        interrupted_at = datetime(2026, 9, 14, 7, 34, 11, tzinfo=timezone.utc)
        metadata = {
            'resolved_stages': ['technical_snapshots', 'action_summaries'],
            'interrupted_reason': 'Core restarted.',
            'resumable': True,
        }
        run = SimpleNamespace(
            id=389,
            status='interrupted',
            error_message='Core restarted.',
            completed_at=interrupted_at,
            updated_at=interrupted_at,
            run_metadata=dict(metadata),
        )
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        stages_result = MagicMock()
        stages_result.scalars.return_value = [
            SimpleNamespace(stage_id='technical_snapshots', status='completed')
        ]
        session = MagicMock()
        session.execute.side_effect = [run_result, stages_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ):
            result = DailyPipelineRunStateService().converge_terminal_stages_run(
                pipeline_run_id=389,
            )

        self.assertEqual(
            result,
            {
                'updated': False,
                'pipeline_run_id': 389,
                'status': 'interrupted',
                'reason': 'resolved_stage_rows_missing',
                'missing_stages': ['action_summaries'],
            },
        )
        self.assertEqual(run.status, 'interrupted')
        self.assertEqual(run.error_message, 'Core restarted.')
        self.assertEqual(run.completed_at, interrupted_at)
        self.assertEqual(run.updated_at, interrupted_at)
        self.assertEqual(run.run_metadata, metadata)

    def test_converge_terminal_stages_run_fails_closed_on_nonterminal_stage(
        self,
    ) -> None:
        interrupted_at = datetime(2026, 9, 14, 7, 34, 11, tzinfo=timezone.utc)
        metadata = {
            'resolved_stages': ['technical_snapshots'],
            'interrupted_reason': 'Core restarted.',
            'resumable': True,
        }
        run = SimpleNamespace(
            id=390,
            status='interrupted',
            error_message='Core restarted.',
            completed_at=interrupted_at,
            updated_at=interrupted_at,
            run_metadata=dict(metadata),
        )
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        stages_result = MagicMock()
        stages_result.scalars.return_value = [
            SimpleNamespace(stage_id='technical_snapshots', status='interrupted')
        ]
        session = MagicMock()
        session.execute.side_effect = [run_result, stages_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ):
            result = DailyPipelineRunStateService().converge_terminal_stages_run(
                pipeline_run_id=390,
            )

        self.assertEqual(
            result,
            {
                'updated': False,
                'pipeline_run_id': 390,
                'status': 'interrupted',
                'reason': 'resolved_stages_incomplete',
                'incomplete_stages': {'technical_snapshots': 'interrupted'},
            },
        )
        self.assertEqual(run.status, 'interrupted')
        self.assertEqual(run.error_message, 'Core restarted.')
        self.assertEqual(run.completed_at, interrupted_at)
        self.assertEqual(run.updated_at, interrupted_at)
        self.assertEqual(run.run_metadata, metadata)

    def test_converge_terminal_stages_run_fails_closed_without_resolved_stages(
        self,
    ) -> None:
        interrupted_at = datetime(2026, 9, 14, 7, 34, 11, tzinfo=timezone.utc)
        metadata = {
            'interrupted_reason': 'Core restarted.',
            'resumable': True,
        }
        run = SimpleNamespace(
            id=391,
            status='interrupted',
            error_message='Core restarted.',
            completed_at=interrupted_at,
            updated_at=interrupted_at,
            run_metadata=dict(metadata),
        )
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        session = MagicMock()
        session.execute.return_value = run_result
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ):
            result = DailyPipelineRunStateService().converge_terminal_stages_run(
                pipeline_run_id=391,
            )

        self.assertEqual(
            result,
            {
                'updated': False,
                'pipeline_run_id': 391,
                'status': 'interrupted',
                'reason': 'resolved_stages_unavailable',
            },
        )
        self.assertEqual(session.execute.call_count, 1)
        self.assertEqual(run.status, 'interrupted')
        self.assertEqual(run.error_message, 'Core restarted.')
        self.assertEqual(run.completed_at, interrupted_at)
        self.assertEqual(run.updated_at, interrupted_at)
        self.assertEqual(run.run_metadata, metadata)

    def test_link_eod_run_persists_parent_reference_immediately(self) -> None:
        now = datetime(2026, 9, 14, 7, 34, 11, tzinfo=timezone.utc)
        run = SimpleNamespace(
            id=387,
            market_date=date(2026, 9, 14),
            trigger='auto_close_scheduler',
            status='running',
            updated_at=now - timedelta(seconds=1),
            run_metadata={'mode': 'daily_auto'},
        )
        eod_run = SimpleNamespace(
            id=522,
            trade_date=date(2026, 9, 14),
            market='KRX',
            source='pykrx',
            trigger='auto_close_scheduler',
        )
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        session = MagicMock()
        session.execute.return_value = run_result
        session.get.return_value = eod_run
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
        ):
            result = DailyPipelineRunStateService().link_eod_run(
                pipeline_run_id=387,
                eod_run_id=522,
            )

        self.assertEqual(
            result,
            {'pipeline_run_id': 387, 'eod_run_id': 522},
        )
        self.assertEqual(run.run_metadata['eod_collection_run_id'], 522)
        self.assertEqual(run.updated_at, now)

    def test_startup_interrupt_marks_linked_eod_run_retryable_immediately(
        self,
    ) -> None:
        now = datetime(2026, 9, 14, 7, 34, 11, tzinfo=timezone.utc)
        run = SimpleNamespace(
            id=387,
            market_date=date(2026, 9, 14),
            trigger='auto_close_scheduler',
            status='running',
            error_message=None,
            completed_at=None,
            updated_at=now - timedelta(seconds=6),
            run_metadata={
                'mode': 'daily_auto',
                'resolved_stages': ['price_collection', 'technical_snapshots'],
            },
        )
        stage = SimpleNamespace(
            stage_id='price_collection',
            market_date=date(2026, 9, 14),
            status='running',
            processed=3_148,
            total=3_549,
            remaining=3_549,
            error_message=None,
            current_step='fetch_symbol_ohlcv',
            current_activity='Collecting prices',
            completed_at=None,
            updated_at=now - timedelta(seconds=6),
            source_run_ids={'eod_run_id': 522},
            details={},
        )
        eod_run = SimpleNamespace(
            id=522,
            market='KRX',
            trade_date=date(2026, 9, 14),
            source='pykrx',
            trigger='auto_close_scheduler',
            status='running',
            symbols_total=3_549,
            symbols_succeeded=3_147,
            symbols_failed=1,
            error_message=None,
            completed_at=None,
            run_metadata={
                'requested_symbols': [
                    '005930',
                    '000660',
                    '121850',
                    '319660',
                ],
                'failed_symbols': ['121850'],
                'unattempted_symbols': ['319660'],
                'stage_progress': {
                    'price_collection': {
                        'status': 'running',
                        'current_step': 'fetch_symbol_ohlcv',
                    }
                },
            },
        )
        runs_result = MagicMock()
        runs_result.scalars.return_value = [run]
        stages_result = MagicMock()
        stages_result.scalars.return_value = [stage]
        persisted_symbols_result = MagicMock()
        persisted_symbols_result.scalars.return_value = [
            '000660',
            '005930',
            '319660',
        ]
        lock_result = MagicMock()
        lock_result.scalar.return_value = True
        session = MagicMock()
        session.execute.side_effect = [
            runs_result,
            stages_result,
            lock_result,
            persisted_symbols_result,
        ]
        session.get.return_value = eod_run
        session_context = MagicMock()
        session_context.__enter__.return_value = session
        reason = 'Core process started while a daily pipeline was running.'

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
        ):
            result = DailyPipelineRunStateService().mark_active_runs_interrupted(
                reason=reason
            )

        self.assertEqual(result['run_ids'], [387])
        self.assertEqual(result['interrupted_eod_run_ids'], [522])
        self.assertEqual(stage.remaining, 401)
        session.get.assert_called_once_with(EODCollectionRun, 522)
        self.assertEqual(eod_run.status, 'failed')
        self.assertEqual(eod_run.symbols_succeeded, 3_147)
        self.assertEqual(eod_run.symbols_failed, 402)
        self.assertEqual(eod_run.completed_at, now)
        self.assertEqual(eod_run.error_message, reason)
        self.assertTrue(eod_run.run_metadata['resumable'])
        self.assertEqual(
            eod_run.run_metadata['downstream_retry_symbols'],
            ['005930', '000660', '319660'],
        )
        self.assertEqual(eod_run.run_metadata['downstream_retry_symbols_count'], 3)
        self.assertEqual(eod_run.run_metadata['unattempted_symbols'], [])
        self.assertEqual(eod_run.run_metadata['unattempted_symbols_count'], 0)
        self.assertEqual(
            eod_run.symbols_succeeded + eod_run.symbols_failed,
            eod_run.symbols_total,
        )
        self.assertEqual(
            eod_run.run_metadata['stage_progress']['price_collection']['status'],
            'failed',
        )
        self.assertEqual(
            eod_run.run_metadata['stage_progress']['price_collection'][
                'current_step'
            ],
            'core_restart_interrupted',
        )

    def test_startup_interrupt_reconciles_running_equal_count_symbol_substitution(
        self,
    ) -> None:
        now = datetime(2026, 9, 14, 7, 34, 11, tzinfo=timezone.utc)
        run = SimpleNamespace(
            id=392,
            market_date=date(2026, 9, 14),
            trigger='auto_close_scheduler',
            status='running',
            error_message=None,
            completed_at=None,
            updated_at=now,
            run_metadata={
                'mode': 'daily_auto',
                'resolved_stages': [
                    'price_collection',
                    'technical_snapshots',
                ],
            },
        )
        completed_stage = SimpleNamespace(
            stage_id='price_collection',
            market_date=date(2026, 9, 14),
            status='completed',
            source_run_ids={'eod_run_id': 525},
        )
        running_stage = SimpleNamespace(
            stage_id='technical_snapshots',
            market_date=date(2026, 9, 14),
            status='running',
            processed=1,
            total=2,
            remaining=1,
            error_message=None,
            current_step='technical_snapshots_running',
            current_activity='Computing snapshots',
            completed_at=None,
            updated_at=now,
            source_run_ids={},
            details={},
        )
        eod_run = SimpleNamespace(
            id=525,
            market='KRX',
            trade_date=date(2026, 9, 14),
            source='pykrx',
            trigger='auto_close_scheduler',
            status='running',
            symbols_total=2,
            symbols_succeeded=2,
            symbols_failed=0,
            error_message=None,
            completed_at=None,
            run_metadata={
                'requested_symbols': ['005930', '000660'],
                'failed_symbols': [],
                'unattempted_symbols': [],
            },
        )
        runs_result = MagicMock()
        runs_result.scalars.return_value = [run]
        stages_result = MagicMock()
        stages_result.scalars.return_value = [completed_stage, running_stage]
        lock_result = MagicMock()
        lock_result.scalar.return_value = True
        persisted_symbols_result = MagicMock()
        persisted_symbols_result.scalars.return_value = ['005930', '035420']
        session = MagicMock()
        session.execute.side_effect = [
            runs_result,
            stages_result,
            lock_result,
            persisted_symbols_result,
        ]
        session.get.return_value = eod_run
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
        ):
            result = DailyPipelineRunStateService().mark_active_runs_interrupted(
                reason='Core restarted.'
            )

        self.assertEqual(result['interrupted_eod_run_ids'], [525])
        self.assertEqual(eod_run.status, 'failed')
        self.assertEqual(eod_run.symbols_succeeded, 1)
        self.assertEqual(eod_run.symbols_failed, 1)
        self.assertEqual(eod_run.run_metadata['failed_symbols'], [])
        self.assertEqual(eod_run.run_metadata['unattempted_symbols'], ['000660'])
        self.assertEqual(
            eod_run.run_metadata['downstream_retry_symbols'],
            ['005930'],
        )
        self.assertEqual(
            eod_run.run_metadata['unexpected_persisted_symbols'],
            ['035420'],
        )
        self.assertEqual(
            eod_run.symbols_succeeded + eod_run.symbols_failed,
            eod_run.symbols_total,
        )

    def test_startup_interrupt_downgrades_terminal_eod_with_missing_requested_symbol(
        self,
    ) -> None:
        now = datetime(2026, 9, 14, 7, 34, 11, tzinfo=timezone.utc)
        completed_at = now - timedelta(minutes=5)

        for terminal_status in ('completed', 'completed_with_errors'):
            with self.subTest(terminal_status=terminal_status):
                run = SimpleNamespace(
                    id=393,
                    market_date=date(2026, 9, 14),
                    trigger='auto_close_scheduler',
                    status='running',
                    error_message=None,
                    completed_at=None,
                    updated_at=now,
                    run_metadata={
                        'mode': 'daily_auto',
                        'resolved_stages': [
                            'price_collection',
                            'technical_snapshots',
                        ],
                    },
                )
                stages = [
                    SimpleNamespace(
                        stage_id='price_collection',
                        market_date=date(2026, 9, 14),
                        status='completed',
                        source_run_ids={'eod_run_id': 526},
                    ),
                    SimpleNamespace(
                        stage_id='technical_snapshots',
                        market_date=date(2026, 9, 14),
                        status='completed',
                        source_run_ids={},
                    ),
                ]
                eod_run = SimpleNamespace(
                    id=526,
                    market='KRX',
                    trade_date=date(2026, 9, 14),
                    source='pykrx',
                    trigger='auto_close_scheduler',
                    status=terminal_status,
                    symbols_total=2,
                    symbols_succeeded=2,
                    symbols_failed=0,
                    error_message=(
                        None
                        if terminal_status == 'completed'
                        else 'existing downstream warning'
                    ),
                    completed_at=completed_at,
                    run_metadata={
                        'requested_symbols': ['005930', '000660'],
                        'failed_symbols': [],
                        'unattempted_symbols': [],
                        'downstream_retry_symbols': [],
                    },
                )
                runs_result = MagicMock()
                runs_result.scalars.return_value = [run]
                stages_result = MagicMock()
                stages_result.scalars.return_value = stages
                lock_result = MagicMock()
                lock_result.scalar.return_value = True
                persisted_symbols_result = MagicMock()
                persisted_symbols_result.scalars.return_value = ['005930']
                session = MagicMock()
                session.execute.side_effect = [
                    runs_result,
                    stages_result,
                    lock_result,
                    persisted_symbols_result,
                ]
                session.get.return_value = eod_run
                session_context = MagicMock()
                session_context.__enter__.return_value = session

                with (
                    patch(
                        'app.eod.daily_pipeline_state.session_scope',
                        return_value=session_context,
                    ),
                    patch(
                        'app.eod.daily_pipeline_state._utcnow',
                        return_value=now,
                    ),
                ):
                    result = (
                        DailyPipelineRunStateService()
                        .mark_active_runs_interrupted(
                            reason='Core restarted.'
                        )
                    )

                self.assertEqual(result['augmented_eod_run_ids'], [526])
                self.assertEqual(eod_run.status, 'completed_with_errors')
                self.assertEqual(eod_run.symbols_succeeded, 1)
                self.assertEqual(eod_run.symbols_failed, 1)
                self.assertEqual(
                    eod_run.run_metadata['unattempted_symbols'],
                    ['000660'],
                )
                self.assertEqual(
                    eod_run.run_metadata['unattempted_symbols_count'],
                    1,
                )
                self.assertTrue(eod_run.run_metadata['resumable'])
                self.assertEqual(eod_run.completed_at, completed_at)

    def test_startup_interrupt_does_not_close_mismatched_linked_eod_run(
        self,
    ) -> None:
        now = datetime(2026, 9, 14, 7, 34, 11, tzinfo=timezone.utc)
        mismatches = (
            {'market': 'US'},
            {'source': 'other'},
            {'trade_date': date(2026, 9, 12)},
        )

        for mismatch in mismatches:
            with self.subTest(mismatch=mismatch):
                run = SimpleNamespace(
                    id=387,
                    market_date=date(2026, 9, 14),
                    trigger='auto_close_scheduler',
                    status='running',
                    error_message=None,
                    completed_at=None,
                    updated_at=now - timedelta(seconds=6),
                    run_metadata={
                        'mode': 'daily_auto',
                        'resolved_stages': [
                            'price_collection',
                            'technical_snapshots',
                        ],
                    },
                )
                stage = SimpleNamespace(
                    stage_id='price_collection',
                    market_date=date(2026, 9, 14),
                    status='running',
                    processed=3_148,
                    total=3_549,
                    remaining=401,
                    error_message=None,
                    current_step='fetch_symbol_ohlcv',
                    current_activity='Collecting prices',
                    completed_at=None,
                    updated_at=now - timedelta(seconds=6),
                    source_run_ids={'eod_run_id': 522},
                    details={},
                )
                eod_attributes = {
                    'id': 522,
                    'market': 'KRX',
                    'trade_date': date(2026, 9, 14),
                    'source': 'pykrx',
                    'trigger': 'auto_close_scheduler',
                    'status': 'running',
                    'run_metadata': {},
                    **mismatch,
                }
                eod_run = SimpleNamespace(**eod_attributes)
                runs_result = MagicMock()
                runs_result.scalars.return_value = [run]
                stages_result = MagicMock()
                stages_result.scalars.return_value = [stage]
                lock_result = MagicMock()
                lock_result.scalar.return_value = True
                session = MagicMock()
                session.execute.side_effect = [
                    runs_result,
                    stages_result,
                    lock_result,
                ]
                session.get.return_value = eod_run
                session_context = MagicMock()
                session_context.__enter__.return_value = session

                with (
                    patch(
                        'app.eod.daily_pipeline_state.session_scope',
                        return_value=session_context,
                    ),
                    patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
                ):
                    result = (
                        DailyPipelineRunStateService().mark_active_runs_interrupted(
                            reason='Core restarted.'
                        )
                    )

                self.assertEqual(result['interrupted_eod_run_ids'], [])
                self.assertEqual(eod_run.status, 'running')
                self.assertEqual(eod_run.run_metadata, {})
                self.assertEqual(session.execute.call_count, 3)

    def test_startup_interrupt_skips_live_provider_lock_owner(self) -> None:
        now = datetime(2026, 9, 14, 7, 34, 11, tzinfo=timezone.utc)
        run = SimpleNamespace(
            id=387,
            market_date=date(2026, 9, 14),
            trigger='auto_close_scheduler',
            status='running',
            error_message=None,
            completed_at=None,
            updated_at=now,
            run_metadata={
                'mode': 'daily_auto',
                'resolved_stages': ['price_collection', 'technical_snapshots'],
            },
        )
        stage = SimpleNamespace(
            stage_id='price_collection',
            market_date=date(2026, 9, 14),
            status='running',
            processed=100,
            total=200,
            remaining=100,
            error_message=None,
            current_step='fetch_symbol_ohlcv',
            current_activity='Collecting prices',
            completed_at=None,
            updated_at=now,
            source_run_ids={'eod_run_id': 522},
            details={},
        )
        eod_run = SimpleNamespace(
            id=522,
            market='KRX',
            trade_date=date(2026, 9, 14),
            source='pykrx',
            trigger='auto_close_scheduler',
            status='running',
            run_metadata={},
        )
        runs_result = MagicMock()
        runs_result.scalars.return_value = [run]
        stages_result = MagicMock()
        stages_result.scalars.return_value = [stage]
        lock_result = MagicMock()
        lock_result.scalar.return_value = False
        session = MagicMock()
        session.execute.side_effect = [runs_result, stages_result, lock_result]
        session.get.return_value = eod_run
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
        ):
            result = DailyPipelineRunStateService().mark_active_runs_interrupted(
                reason='Core restarted.'
            )

        self.assertEqual(result['run_ids'], [])
        self.assertEqual(result['provider_lock_owned_run_ids'], [387])
        self.assertEqual(run.status, 'running')
        self.assertEqual(stage.status, 'running')
        self.assertEqual(eod_run.status, 'running')

    def test_startup_interrupt_fences_auto_parent_before_eod_link_exists(
        self,
    ) -> None:
        now = datetime(2026, 9, 14, 7, 34, 11, tzinfo=timezone.utc)
        run = SimpleNamespace(
            id=387,
            market_date=date(2026, 9, 14),
            trigger='auto_close_scheduler',
            status='running',
            error_message=None,
            completed_at=None,
            updated_at=now,
            run_metadata={
                'mode': 'daily_auto',
                'resolved_stages': ['price_collection'],
            },
        )
        stage = SimpleNamespace(
            stage_id='price_collection',
            market_date=date(2026, 9, 14),
            status='running',
            source_run_ids={},
        )
        runs_result = MagicMock()
        runs_result.scalars.return_value = [run]
        stages_result = MagicMock()
        stages_result.scalars.return_value = [stage]
        lock_result = MagicMock()
        lock_result.scalar.return_value = False
        session = MagicMock()
        session.execute.side_effect = [runs_result, stages_result, lock_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ):
            result = DailyPipelineRunStateService().mark_active_runs_interrupted(
                reason='Core restarted.'
            )

        self.assertEqual(result['run_ids'], [])
        self.assertEqual(result['provider_lock_owned_run_ids'], [387])
        self.assertEqual(run.status, 'running')
        self.assertEqual(stage.status, 'running')
        lock_call = session.execute.call_args_list[2]
        self.assertIn('pg_try_advisory_xact_lock', str(lock_call.args[0]))
        self.assertEqual(
            lock_call.args[1],
            {'source_key': 10_001, 'trade_key': 20_260_914},
        )

    def test_startup_interrupt_links_unique_running_eod_before_first_checkpoint(
        self,
    ) -> None:
        now = datetime(2026, 9, 14, 7, 34, 11, tzinfo=timezone.utc)
        run = SimpleNamespace(
            id=387,
            market_date=date(2026, 9, 14),
            trigger='auto_close_scheduler',
            status='running',
            error_message=None,
            completed_at=None,
            updated_at=now,
            run_metadata={
                'mode': 'daily_auto',
                'resolved_stages': ['price_collection', 'technical_snapshots'],
            },
        )
        stage = SimpleNamespace(
            stage_id='price_collection',
            market_date=date(2026, 9, 14),
            status='running',
            processed=0,
            total=3_549,
            remaining=3_549,
            error_message=None,
            current_step='price_collection_started',
            current_activity='Starting prices',
            completed_at=None,
            updated_at=now,
            source_run_ids={},
            details={},
        )
        eod_run = SimpleNamespace(
            id=522,
            market='KRX',
            trade_date=date(2026, 9, 14),
            source='pykrx',
            trigger='auto_close_scheduler',
            status='running',
            symbols_total=3_549,
            symbols_succeeded=0,
            symbols_failed=0,
            error_message=None,
            completed_at=None,
            run_metadata={'requested_symbols': ['005930']},
        )
        runs_result = MagicMock()
        runs_result.scalars.return_value = [run]
        stages_result = MagicMock()
        stages_result.scalars.return_value = [stage]
        metadata_candidates_result = MagicMock()
        metadata_candidates_result.scalars.return_value = []
        legacy_candidates_result = MagicMock()
        legacy_candidates_result.scalars.return_value = [eod_run]
        lock_result = MagicMock()
        lock_result.scalar.return_value = True
        persisted_symbols_result = MagicMock()
        persisted_symbols_result.scalars.return_value = []
        session = MagicMock()
        session.execute.side_effect = [
            runs_result,
            stages_result,
            lock_result,
            metadata_candidates_result,
            legacy_candidates_result,
            persisted_symbols_result,
        ]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
        ):
            result = DailyPipelineRunStateService().mark_active_runs_interrupted(
                reason='Core restarted.'
            )

        self.assertEqual(result['run_ids'], [387])
        self.assertEqual(result['interrupted_eod_run_ids'], [522])
        self.assertEqual(eod_run.status, 'failed')

    def test_startup_interrupt_augments_terminal_eod_from_prior_stage_link(
        self,
    ) -> None:
        now = datetime(2026, 9, 14, 9, 0, tzinfo=timezone.utc)
        run = SimpleNamespace(
            id=390,
            market_date=date(2026, 9, 14),
            trigger='auto_close_scheduler',
            status='running',
            error_message=None,
            completed_at=None,
            updated_at=now,
            run_metadata={
                'mode': 'daily_auto',
                'resolved_stages': ['price_collection', 'momentum_factors'],
            },
        )
        completed_stage = SimpleNamespace(
            stage_id='price_collection',
            market_date=date(2026, 9, 14),
            status='completed',
            source_run_ids={'eod_run_id': 522},
        )
        running_stage = SimpleNamespace(
            stage_id='momentum_factors',
            market_date=date(2026, 9, 14),
            status='running',
            processed=1,
            total=4,
            remaining=3,
            error_message=None,
            current_step='factor_running',
            current_activity='Computing factors',
            completed_at=None,
            updated_at=now,
            source_run_ids={},
            details={},
        )
        eod_run = SimpleNamespace(
            id=522,
            market='KRX',
            trade_date=date(2026, 9, 14),
            source='pykrx',
            trigger='auto_close_scheduler',
            status='completed_with_errors',
            symbols_total=4,
            symbols_succeeded=3,
            symbols_failed=1,
            error_message='one price failed',
            completed_at=now - timedelta(minutes=5),
            run_metadata={
                'requested_symbols': ['005930', '000660', '319660', '121850'],
                'failed_symbols': ['121850'],
                'downstream_retry_symbols': ['035420'],
                'inherited_downstream_retry_symbols': ['035420'],
                'stage_progress': {
                    'momentum_factors': {
                        'status': 'running',
                        'current_step': 'factor_running',
                    }
                },
            },
        )
        runs_result = MagicMock()
        runs_result.scalars.return_value = [run]
        stages_result = MagicMock()
        stages_result.scalars.return_value = [completed_stage, running_stage]
        lock_result = MagicMock()
        lock_result.scalar.return_value = True
        persisted_symbols_result = MagicMock()
        persisted_symbols_result.scalars.return_value = [
            '000660',
            '005930',
            '319660',
        ]
        inherited_symbols_result = MagicMock()
        inherited_symbols_result.scalars.return_value = ['035420']
        session = MagicMock()
        session.execute.side_effect = [
            runs_result,
            stages_result,
            lock_result,
            persisted_symbols_result,
            inherited_symbols_result,
        ]
        session.get.return_value = eod_run
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
        ):
            result = DailyPipelineRunStateService().mark_active_runs_interrupted(
                reason='Core restarted.'
            )

        self.assertEqual(result['run_ids'], [390])
        self.assertEqual(result['interrupted_eod_run_ids'], [])
        self.assertEqual(result['augmented_eod_run_ids'], [522])
        self.assertEqual(eod_run.status, 'completed_with_errors')
        self.assertEqual(eod_run.symbols_succeeded, 3)
        self.assertEqual(eod_run.symbols_failed, 1)
        self.assertEqual(eod_run.error_message, 'one price failed')
        self.assertEqual(eod_run.completed_at, now - timedelta(minutes=5))
        self.assertEqual(
            eod_run.run_metadata['downstream_retry_symbols'],
            ['005930', '000660', '319660', '035420'],
        )
        self.assertEqual(eod_run.run_metadata['downstream_retry_symbols_count'], 4)
        self.assertEqual(
            eod_run.run_metadata['inherited_downstream_retry_symbols'],
            ['035420'],
        )
        self.assertTrue(eod_run.run_metadata['followup_recovery_required'])
        self.assertEqual(
            eod_run.run_metadata['stage_progress']['momentum_factors']['status'],
            'failed',
        )
        self.assertEqual(completed_stage.status, 'completed')
        self.assertEqual(running_stage.status, 'interrupted')

    def test_startup_interrupt_retries_unsafe_skipped_downstream_stage(
        self,
    ) -> None:
        now = datetime(2026, 9, 14, 9, 0, tzinfo=timezone.utc)
        run = SimpleNamespace(
            id=394,
            market_date=date(2026, 9, 14),
            trigger='auto_close_scheduler',
            status='running',
            error_message=None,
            completed_at=None,
            updated_at=now,
            run_metadata={
                'mode': 'daily_auto',
                'resolved_stages': [
                    'price_collection',
                    'fundamentals_daily_refresh',
                ],
            },
        )
        stages = [
            SimpleNamespace(
                stage_id='price_collection',
                status='completed',
                source_run_ids={'eod_run_id': 527},
                details={},
            ),
            SimpleNamespace(
                stage_id='fundamentals_daily_refresh',
                status='skipped',
                source_run_ids={},
                details={
                    'result_summary': {
                        'reason': 'dart_api_key_not_configured',
                    },
                },
            ),
        ]
        eod_run = SimpleNamespace(
            id=527,
            market='KRX',
            trade_date=date(2026, 9, 14),
            source='pykrx',
            trigger='auto_close_scheduler',
            status='completed',
            symbols_total=1,
            symbols_succeeded=1,
            symbols_failed=0,
            error_message=None,
            completed_at=now - timedelta(minutes=5),
            run_metadata={
                'requested_symbols': ['005930'],
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': [],
            },
        )
        runs_result = MagicMock()
        runs_result.scalars.return_value = [run]
        stages_result = MagicMock()
        stages_result.scalars.return_value = stages
        lock_result = MagicMock()
        lock_result.scalar.return_value = True
        persisted_symbols_result = MagicMock()
        persisted_symbols_result.scalars.return_value = ['005930']
        session = MagicMock()
        session.execute.side_effect = [
            runs_result,
            stages_result,
            lock_result,
            persisted_symbols_result,
        ]
        session.get.return_value = eod_run
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
        ):
            result = DailyPipelineRunStateService().mark_active_runs_interrupted(
                reason='Core restarted.'
            )

        self.assertEqual(result['augmented_eod_run_ids'], [527])
        self.assertEqual(
            eod_run.run_metadata['downstream_retry_symbols'],
            ['005930'],
        )
        self.assertTrue(eod_run.run_metadata['followup_recovery_required'])

    def test_startup_interrupt_preserves_complete_price_collection_as_terminal(
        self,
    ) -> None:
        now = datetime(2026, 9, 14, 8, 0, tzinfo=timezone.utc)
        run = SimpleNamespace(
            id=389,
            market_date=date(2026, 9, 14),
            trigger='auto_close_scheduler',
            status='running',
            error_message=None,
            completed_at=None,
            updated_at=now,
            run_metadata={
                'mode': 'daily_auto',
                'resolved_stages': ['price_collection', 'technical_snapshots'],
            },
        )
        stage = SimpleNamespace(
            stage_id='technical_snapshots',
            market_date=date(2026, 9, 14),
            status='running',
            processed=2,
            total=2,
            remaining=0,
            error_message=None,
            current_step='technical_snapshots_running',
            current_activity='Computing snapshots',
            completed_at=None,
            updated_at=now,
            source_run_ids={'eod_run_id': 523},
            details={},
        )
        eod_run = SimpleNamespace(
            id=523,
            market='KRX',
            trade_date=date(2026, 9, 14),
            source='pykrx',
            trigger='auto_close_scheduler',
            status='running',
            symbols_total=2,
            symbols_succeeded=2,
            symbols_failed=0,
            error_message=None,
            completed_at=None,
            run_metadata={
                'requested_symbols': ['005930', '000660'],
                'failed_symbols': [],
                'unattempted_symbols': [],
            },
        )
        runs_result = MagicMock()
        runs_result.scalars.return_value = [run]
        stages_result = MagicMock()
        stages_result.scalars.return_value = [stage]
        lock_result = MagicMock()
        lock_result.scalar.return_value = True
        persisted_symbols_result = MagicMock()
        persisted_symbols_result.scalars.return_value = ['000660', '005930']
        session = MagicMock()
        session.execute.side_effect = [
            runs_result,
            stages_result,
            lock_result,
            persisted_symbols_result,
        ]
        session.get.return_value = eod_run
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
        ):
            result = DailyPipelineRunStateService().mark_active_runs_interrupted(
                reason='Core restarted.'
            )

        self.assertEqual(result['interrupted_eod_run_ids'], [523])
        self.assertEqual(eod_run.status, 'completed_with_errors')
        self.assertEqual(eod_run.symbols_succeeded, 2)
        self.assertEqual(eod_run.symbols_failed, 0)
        self.assertEqual(
            eod_run.run_metadata['downstream_retry_symbols'],
            ['005930', '000660'],
        )

    def test_startup_preserves_existing_context_gap_after_tracked_stages_finish(
        self,
    ) -> None:
        now = datetime(2026, 9, 14, 8, 0, tzinfo=timezone.utc)
        run = SimpleNamespace(
            id=391,
            market_date=date(2026, 9, 14),
            trigger='auto_close_scheduler',
            status='running',
            error_message=None,
            completed_at=None,
            updated_at=now,
            run_metadata={
                'mode': 'daily_auto',
                'resolved_stages': [
                    'price_collection',
                    'technical_snapshots',
                ],
            },
        )
        stages = [
            SimpleNamespace(
                stage_id='price_collection',
                status='completed',
                source_run_ids={'eod_run_id': 524},
            ),
            SimpleNamespace(
                stage_id='technical_snapshots',
                status='completed',
                source_run_ids={},
            ),
        ]
        eod_run = SimpleNamespace(
            id=524,
            market='KRX',
            trade_date=date(2026, 9, 14),
            source='pykrx',
            trigger='auto_close_scheduler',
            status='completed_with_errors',
            symbols_total=1,
            symbols_succeeded=1,
            symbols_failed=0,
            error_message='price context persistence incomplete',
            completed_at=now - timedelta(seconds=1),
            run_metadata={
                'requested_symbols': ['005930'],
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': ['005930'],
            },
        )
        runs_result = MagicMock()
        runs_result.scalars.return_value = [run]
        stages_result = MagicMock()
        stages_result.scalars.return_value = stages
        lock_result = MagicMock()
        lock_result.scalar.return_value = True
        persisted_symbols_result = MagicMock()
        persisted_symbols_result.scalars.return_value = ['005930']
        session = MagicMock()
        session.execute.side_effect = [
            runs_result,
            stages_result,
            lock_result,
            persisted_symbols_result,
        ]
        session.get.return_value = eod_run
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
        ):
            result = DailyPipelineRunStateService().mark_active_runs_interrupted(
                reason='Core restarted.'
            )

        self.assertEqual(result['run_ids'], [391])
        self.assertEqual(result['augmented_eod_run_ids'], [524])
        self.assertEqual(
            eod_run.run_metadata['downstream_retry_symbols'],
            ['005930'],
        )
        self.assertTrue(eod_run.run_metadata['followup_recovery_required'])

    def test_latest_run_without_market_date_uses_actual_newest_run(self) -> None:
        engine = create_engine('sqlite+pysqlite:///:memory:')
        DailyPipelineRun.__table__.create(engine)
        DailyPipelineStageRun.__table__.create(engine)
        session_factory = sessionmaker(bind=engine, future=True)
        started_at = datetime(2026, 9, 12, tzinfo=timezone.utc)
        with session_factory.begin() as session:
            session.add_all(
                [
                    DailyPipelineRun(
                        market_date=date(2026, 9, 8),
                        trigger='auto_close_scheduler',
                        status='completed',
                        started_at=started_at,
                        run_metadata={},
                    ),
                    DailyPipelineRun(
                        market_date=date(2026, 9, 11),
                        trigger='manual_daily_api',
                        status='partial',
                        started_at=started_at + timedelta(hours=1),
                        run_metadata={},
                    ),
                ]
            )

        @contextmanager
        def _session_scope():
            with session_factory.begin() as session:
                yield session

        try:
            with patch('app.eod.daily_pipeline_state.session_scope', _session_scope):
                result = DailyPipelineRunStateService().get_latest_run(
                    market_date=None
                )
        finally:
            engine.dispose()

        self.assertIsNotNone(result)
        self.assertEqual(result['market_date'], '2026-09-11')
        self.assertEqual(result['trigger'], 'manual_daily_api')
        self.assertEqual(result['status'], 'partial')

    def test_context_lookup_filters_before_limit_with_more_than_100_unrelated_runs(self) -> None:
        engine = create_engine('sqlite+pysqlite:///:memory:')
        DailyPipelineRun.__table__.create(engine)
        DailyPipelineStageRun.__table__.create(engine)
        session_factory = sessionmaker(bind=engine, future=True)
        market_date = date(2026, 9, 5)
        started_at = datetime(2026, 9, 5, tzinfo=timezone.utc)
        with session_factory.begin() as session:
            session.add(
                DailyPipelineRun(
                    market_date=market_date,
                    trigger='auto_close_scheduler',
                    status='partial',
                    started_at=started_at,
                    run_metadata={
                        'entrypoint': 'PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due'
                    },
                )
            )
            session.add_all(
                [
                    DailyPipelineRun(
                        market_date=market_date,
                        trigger='manual_api',
                        status='completed',
                        started_at=started_at + timedelta(minutes=index + 1),
                        run_metadata={'entrypoint': 'daily_pipeline_stage_service.run'},
                    )
                    for index in range(101)
                ]
            )

        @contextmanager
        def _session_scope():
            with session_factory.begin() as session:
                yield session

        try:
            with patch('app.eod.daily_pipeline_state.session_scope', _session_scope):
                result = DailyPipelineRunStateService().get_latest_run_for_context(
                    market_date=market_date,
                    triggers=('auto_eod_followup_resume',),
                    entrypoints=(
                        'PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due',
                    ),
                    search_limit=100,
                )
        finally:
            engine.dispose()

        self.assertIsNotNone(result)
        self.assertEqual(result['trigger'], 'auto_close_scheduler')
        self.assertEqual(result['status'], 'partial')

    def test_context_lookup_ignores_newer_unrelated_manual_run(self) -> None:
        now = datetime(2026, 9, 6, tzinfo=timezone.utc)
        manual_run = SimpleNamespace(
            id=402,
            market_date=date(2026, 9, 5),
            trigger='manual_api',
            status='completed',
            current_stage=None,
            started_at=now,
            updated_at=now,
            completed_at=now,
            error_message=None,
            run_metadata={'entrypoint': 'daily_pipeline_stage_service.run'},
        )
        automatic_run = SimpleNamespace(
            id=401,
            market_date=date(2026, 9, 5),
            trigger='auto_close_scheduler',
            status='partial',
            current_stage='signal_decider',
            started_at=now - timedelta(minutes=5),
            updated_at=now - timedelta(minutes=4),
            completed_at=now - timedelta(minutes=4),
            error_message=None,
            run_metadata={
                'entrypoint': 'PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due',
                'resolved_stages': ['signal_decider'],
            },
        )
        runs_result = MagicMock()
        runs_result.scalars.return_value = [manual_run, automatic_run]
        stages_result = MagicMock()
        stages_result.scalars.return_value = []
        session = MagicMock()
        session.execute.side_effect = [runs_result, stages_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch('app.eod.daily_pipeline_state.session_scope', return_value=session_context):
            result = DailyPipelineRunStateService().get_latest_run_for_context(
                market_date='2026-09-05',
                triggers=('auto_eod_followup_resume',),
                entrypoints=('PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due',),
            )

        self.assertIsNotNone(result)
        self.assertEqual(result['id'], 401)
        self.assertEqual(result['status'], 'partial')

    def test_count_runs_uses_exact_trigger(self) -> None:
        run_ids_result = MagicMock()
        run_ids_result.scalars.return_value = [501, 502, 503]
        session = MagicMock()
        session.execute.return_value = run_ids_result
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch('app.eod.daily_pipeline_state.session_scope', return_value=session_context):
            result = DailyPipelineRunStateService().count_runs(
                market_date='2026-09-05',
                trigger='auto_eod_followup_resume',
            )

        self.assertEqual(result, 3)

    def test_count_runs_scopes_recovery_attempts_to_reused_eod_lineage(
        self,
    ) -> None:
        run_ids_result = MagicMock()
        run_ids_result.scalars.return_value = [503]
        session = MagicMock()
        session.execute.return_value = run_ids_result
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ):
            result = DailyPipelineRunStateService().count_runs(
                market_date='2026-09-05',
                trigger='auto_eod_followup_resume',
                reused_eod_collection_run_id=7,
            )

        self.assertEqual(result, 1)
        statement = session.execute.call_args.args[0]
        compiled = statement.compile()
        self.assertIn('reused_eod_collection_run_id', compiled.params.values())
        self.assertIn(7, compiled.params.values())

    def test_monitor_recovery_acquires_advisory_lock_before_deduplicating(self) -> None:
        now = datetime(2026, 8, 24, tzinfo=timezone.utc)
        existing_run = SimpleNamespace(
            id=315,
            market_date=date(2026, 8, 21),
            trigger='monitor_stale_continuation',
            status='completed',
            current_stage=None,
            started_at=now,
            updated_at=now,
            completed_at=now,
            error_message=None,
            run_metadata={},
        )
        existing_result = MagicMock()
        existing_result.scalar_one_or_none.return_value = existing_run
        stages_result = MagicMock()
        stages_result.scalars.return_value = []
        session = MagicMock()
        active_result = MagicMock()
        active_result.scalars.return_value = []
        session.execute.side_effect = [MagicMock(), active_result, existing_result, stages_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch('app.eod.daily_pipeline_state.session_scope', return_value=session_context),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
        ):
            result = DailyPipelineRunStateService().start_run(
                market_date='2026-08-21',
                trigger='monitor_stale_continuation',
                requested_stages=['price_collection'],
                resolved_stages=['price_collection'],
            )

        advisory_lock_call = session.execute.call_args_list[0]
        self.assertEqual(
            str(advisory_lock_call.args[0]),
            'SELECT pg_advisory_xact_lock(:source_key, :trade_key)',
        )
        self.assertEqual(
            advisory_lock_call.args[1],
            {'source_key': 20_001, 'trade_key': 20260821},
        )
        self.assertTrue(result['deduplicated'])
        self.assertEqual(result['deduplication_reason'], 'recovery_already_completed')

    def test_manual_api_deduplicates_overlapping_active_run(self) -> None:
        now = datetime(2026, 8, 29, tzinfo=timezone.utc)
        existing_run = SimpleNamespace(
            id=341,
            market_date=date(2026, 8, 28),
            trigger='manual_api',
            status='running',
            current_stage='agent_feedback_summaries',
            started_at=now,
            updated_at=now,
            completed_at=None,
            error_message=None,
            run_metadata={
                'resolved_stages': ['agent_feedback_summaries', 'signal_decider'],
            },
        )
        active_result = MagicMock()
        active_result.scalars.return_value = [existing_run]
        stages_result = MagicMock()
        stages_result.scalars.return_value = []
        session = MagicMock()
        session.execute.side_effect = [MagicMock(), active_result, stages_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch('app.eod.daily_pipeline_state.session_scope', return_value=session_context),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
        ):
            result = DailyPipelineRunStateService().start_run(
                market_date='2026-08-28',
                trigger='manual_api',
                requested_stages=['signal_decider'],
                resolved_stages=['signal_decider'],
            )

        self.assertTrue(result['deduplicated'])
        self.assertEqual(result['id'], 341)
        self.assertEqual(result['deduplication_reason'], 'overlapping_run_already_active')

    def test_start_run_abandons_stale_activity_before_admitting_recovery(self) -> None:
        now = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
        last_activity_at = now - timedelta(minutes=31)
        stale_run = SimpleNamespace(
            id=341,
            market_date=date(2026, 8, 28),
            trigger='manual_api',
            status='running',
            current_stage='signal_decider',
            started_at=last_activity_at - timedelta(minutes=5),
            updated_at=last_activity_at,
            completed_at=None,
            error_message=None,
            run_metadata={'resolved_stages': ['signal_decider'], 'owner': 'stale-worker'},
        )
        stale_stage = SimpleNamespace(
            id=812,
            pipeline_run_id=341,
            market_date=date(2026, 8, 28),
            stage_id='signal_decider',
            status='running',
            processed=2,
            total=10,
            succeeded=2,
            failed=0,
            remaining=8,
            current_step='dispatching',
            current_activity='Dispatching signal decider',
            current_symbol='005930',
            current_period=None,
            started_at=last_activity_at - timedelta(minutes=5),
            updated_at=last_activity_at,
            completed_at=None,
            error_message=None,
            source_run_ids={},
            details={},
        )
        active_result = MagicMock()
        active_result.scalars.return_value = [stale_run]
        stages_result = MagicMock()
        stages_result.scalars.return_value = [stale_stage]
        existing_recovery_result = MagicMock()
        existing_recovery_result.scalar_one_or_none.return_value = None
        session = MagicMock()
        session.execute.side_effect = [
            MagicMock(),
            active_result,
            stages_result,
            existing_recovery_result,
        ]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch('app.eod.daily_pipeline_state.session_scope', return_value=session_context),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
            patch('app.eod.daily_pipeline_state.settings.eod_auto_pykrx_stale_run_minutes', 30),
        ):
            result = DailyPipelineRunStateService().start_run(
                market_date='2026-08-28',
                trigger='monitor_stale_continuation',
                requested_stages=['signal_decider'],
                resolved_stages=['signal_decider'],
            )

        self.assertNotIn('deduplicated', result)
        self.assertEqual(stale_run.status, 'abandoned')
        self.assertEqual(stale_run.completed_at, now)
        self.assertEqual(stale_run.run_metadata['abandoned_reason'], 'stale_activity_timeout')
        self.assertEqual(stale_run.run_metadata['last_activity_at'], last_activity_at.isoformat())
        self.assertTrue(stale_run.run_metadata['resumable'])
        self.assertEqual(stale_stage.status, 'abandoned')
        self.assertEqual(stale_stage.current_step, 'stale_run_abandoned')
        self.assertEqual(stale_stage.details['stale_after_minutes'], 30)
        admitted_run = session.add.call_args.args[0]
        self.assertEqual(admitted_run.status, 'running')
        self.assertEqual(admitted_run.trigger, 'monitor_stale_continuation')
        self.assertEqual(admitted_run.run_metadata['resolved_stages'], ['signal_decider'])
        self.assertNotIn('owner', admitted_run.run_metadata)

    def test_finish_run_rejects_terminal_run_owned_by_reconciled_worker(self) -> None:
        abandoned_run = SimpleNamespace(id=341, status='abandoned')
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = abandoned_run
        session = MagicMock()
        session.execute.return_value = run_result
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch('app.eod.daily_pipeline_state.session_scope', return_value=session_context):
            with self.assertRaisesRegex(
                ValueError,
                r'daily pipeline run is not active: 341 \(abandoned\)',
            ):
                DailyPipelineRunStateService().finish_run(
                    pipeline_run_id=341,
                    status='completed',
                )

    def test_finish_stage_recomputes_stale_remaining_from_terminal_counts(self) -> None:
        now = datetime(2026, 9, 12, 16, 5, tzinfo=timezone.utc)
        run = SimpleNamespace(
            id=386,
            status='running',
            current_stage='price_collection',
            updated_at=now,
        )
        stage = SimpleNamespace(
            id=901,
            pipeline_run_id=386,
            market_date=date(2026, 9, 11),
            stage_id='price_collection',
            status='running',
            processed=3_548,
            total=3_549,
            succeeded=3_548,
            failed=0,
            remaining=3_549,
            current_step='collecting',
            current_activity='Collecting prices',
            current_symbol='472220',
            current_period=None,
            started_at=now - timedelta(minutes=10),
            updated_at=now - timedelta(minutes=1),
            completed_at=None,
            error_message=None,
            source_run_ids={},
            details={},
        )
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        stage_result = MagicMock()
        stage_result.scalar_one_or_none.return_value = stage
        session = MagicMock()
        session.execute.side_effect = [run_result, stage_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
        ):
            result = DailyPipelineRunStateService().finish_stage(
                pipeline_run_id=386,
                stage_id='price_collection',
                market_date='2026-09-11',
                status='partial',
                processed=3_549,
                total=3_549,
                succeeded=3_548,
                failed=1,
            )

        self.assertEqual(result['remaining'], 0)
        self.assertEqual(stage.remaining, 0)

    def test_direct_terminal_update_recomputes_stale_remaining(self) -> None:
        now = datetime(2026, 9, 12, 16, 5, tzinfo=timezone.utc)
        run = SimpleNamespace(
            id=386,
            status='running',
            current_stage='price_collection',
            updated_at=now,
        )
        stage = SimpleNamespace(
            id=901,
            pipeline_run_id=386,
            market_date=date(2026, 9, 11),
            stage_id='price_collection',
            status='running',
            processed=3_548,
            total=3_549,
            succeeded=3_548,
            failed=0,
            remaining=3_549,
            current_step='collecting',
            current_activity='Collecting prices',
            current_symbol='472220',
            current_period=None,
            started_at=now - timedelta(minutes=10),
            updated_at=now - timedelta(minutes=1),
            completed_at=None,
            error_message=None,
            source_run_ids={},
            details={},
        )
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        stage_result = MagicMock()
        stage_result.scalar_one_or_none.return_value = stage
        session = MagicMock()
        session.execute.side_effect = [run_result, stage_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
        ):
            result = DailyPipelineRunStateService().update_stage(
                pipeline_run_id=386,
                stage_id='price_collection',
                market_date='2026-09-11',
                status='partial',
                processed=3_549,
                total=3_549,
                succeeded=3_548,
                failed=1,
            )

        self.assertEqual(result['remaining'], 0)
        self.assertEqual(stage.remaining, 0)


if __name__ == '__main__':
    unittest.main()
