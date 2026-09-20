import unittest
from copy import deepcopy
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
    _expired_decision_skip_evidence_is_valid,
    _stage_row_outcome,
)


class DailyPipelineRunStateServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.terminal_cutoff = datetime(
            2026,
            9,
            20,
            0,
            55,
            tzinfo=timezone.utc,
        )
        self.terminal_cutoff_resolver = patch(
            'app.eod.daily_pipeline_state.'
            '_resolve_terminal_summary_knowledge_cutoff',
            return_value={
                'ready': True,
                'reason': 'latest_completed_summary_cutoff',
                'knowledge_cutoff_at': self.terminal_cutoff,
                'source_run_id': (
                    'agent_feedback_summaries-prior'
                ),
                'feedback_scope_mode': 'broad_feedback_universe',
            },
        )
        self.resolve_terminal_cutoff = (
            self.terminal_cutoff_resolver.start()
        )
        self.addCleanup(self.terminal_cutoff_resolver.stop)

    @staticmethod
    def _terminal_continuation_stage(
        *,
        stage_id: str,
        status: str,
        details: dict | None = None,
        stage_row_id: int = 1,
    ) -> SimpleNamespace:
        now = datetime(2026, 9, 20, 8, 45, tzinfo=timezone.utc)
        completed = status in {'completed', 'failed', 'blocked'}
        return SimpleNamespace(
            id=stage_row_id,
            pipeline_run_id=456,
            market_date=date(2026, 9, 18),
            stage_id=stage_id,
            status=status,
            processed=1 if completed else 0,
            total=1 if completed else 0,
            succeeded=1 if status == 'completed' else 0,
            failed=0,
            remaining=0,
            current_step=f'{stage_id}_{status}',
            current_activity=f'{stage_id} {status}',
            current_symbol=None,
            current_period=None,
            started_at=now - timedelta(minutes=5),
            updated_at=now,
            completed_at=now if completed else None,
            error_message=None,
            source_run_ids={'run_id': f'{stage_id}-prior'},
            details=dict(details or {}),
        )

    @classmethod
    def _terminal_continuation_fixture(
        cls,
    ) -> tuple[SimpleNamespace, list[SimpleNamespace], SimpleNamespace]:
        now = datetime(2026, 9, 20, 8, 45, tzinfo=timezone.utc)
        resolved_stages = [
            'pykrx_enrichment',
            'agent_feedback_summaries',
            'signal_decider',
            'next_open_execution_plan',
            'signal_decider_evaluations',
        ]
        run = SimpleNamespace(
            id=456,
            market_date=date(2026, 9, 18),
            trigger='auto_eod_followup_resume',
            status='blocked',
            current_stage='signal_decider_evaluations',
            started_at=now - timedelta(hours=8),
            updated_at=now,
            completed_at=now,
            error_message='Signal decider dependency was unavailable.',
            run_metadata={
                'entrypoint': 'daily_pipeline_stage_service.run',
                'requested_stages': list(resolved_stages),
                'resolved_stages': list(resolved_stages),
                'resume_from_stage': 'pykrx_enrichment',
                'reused_eod_collection_run_id': 546,
                'symbols': ['005930'],
                'markets': ['KOSPI'],
                'force': False,
                'fs_div': 'CFS',
                'symbol_limit': None,
                'symbol_offset': 0,
                'capture_limit': 500,
                'reuse_existing_feedback': True,
                'bypass_prerequisite_gate': False,
                'point_in_time_recovery': True,
                'stage_statuses': {
                    'pykrx_enrichment': 'completed',
                    'agent_feedback_summaries': 'completed',
                    'signal_decider': 'failed',
                    'next_open_execution_plan': 'blocked',
                    'signal_decider_evaluations': 'blocked',
                },
            },
        )
        stages = [
            cls._terminal_continuation_stage(
                stage_id='pykrx_enrichment',
                status='completed',
                stage_row_id=1,
            ),
            cls._terminal_continuation_stage(
                stage_id='agent_feedback_summaries',
                status='completed',
                details={'result_summary': {'status': 'completed'}},
                stage_row_id=2,
            ),
            cls._terminal_continuation_stage(
                stage_id='signal_decider',
                status='failed',
                details={
                    'result_summary': {
                        'status': 'failed',
                        'progress': {
                            'summary': {
                                'failed_items': 160,
                                'failure_groups': {
                                    'dependency/missing-upstream': {
                                        'failure_status': 'dependency/missing-upstream',
                                        'failure_category': 'dependency',
                                        'count': 160,
                                        'retryable_count': 160,
                                        'failure_kinds': [
                                            'dependency_missing_upstream'
                                        ],
                                    }
                                },
                            }
                        },
                    }
                },
                stage_row_id=3,
            ),
            cls._terminal_continuation_stage(
                stage_id='next_open_execution_plan',
                status='blocked',
                details={
                    'result_summary': {
                        'triggered': False,
                        'status': 'blocked',
                        'reason': 'signal_decider_stage_incomplete',
                        'upstream_status': 'failed',
                    }
                },
                stage_row_id=4,
            ),
            cls._terminal_continuation_stage(
                stage_id='signal_decider_evaluations',
                status='blocked',
                details={
                    'result_summary': {
                        'triggered': False,
                        'status': 'blocked',
                        'reason': 'declared_dependencies_incomplete',
                        'incomplete_dependencies': ['signal_decider'],
                    }
                },
                stage_row_id=5,
            ),
        ]
        eod_run = SimpleNamespace(
            id=546,
            market='KRX',
            source='pykrx',
            trade_date=date(2026, 9, 18),
        )
        return run, stages, eod_run

    @staticmethod
    def _terminal_continuation_lineage(
        run_ids: tuple[int, ...] = (441, 442, 443, 444, 445, 456),
    ) -> list[SimpleNamespace]:
        started_at = datetime(2026, 9, 19, tzinfo=timezone.utc)
        return [
            SimpleNamespace(
                id=run_id,
                started_at=started_at + timedelta(minutes=index),
            )
            for index, run_id in enumerate(run_ids)
        ]

    @staticmethod
    def _terminal_feedback_child_run(
        *,
        stage_id: str,
        run_id: str,
    ) -> SimpleNamespace:
        roles = (
            ['eod-summarizer', 'technical-summarizer', 'fundamental-summarizer']
            if stage_id == 'agent_feedback_summaries'
            else ['short-term-decider', 'signal-decider']
        )
        scope_mode = (
            'broad_feedback_universe'
            if stage_id == 'agent_feedback_summaries'
            else 'decider_scope_fast_path'
        )
        cutoff = '2026-09-20T00:55:00+00:00'
        return SimpleNamespace(
            run_id=run_id,
            market_date=date(2026, 9, 18),
            status='completed',
            completed_at=datetime(
                2026,
                9,
                20,
                1,
                0,
                tzinfo=timezone.utc,
            ),
            failed_count=0,
            symbols_total=len(roles),
            completed_count=len(roles),
            request_payload={
                'market_date': '2026-09-18',
                'request_run_id': run_id,
                'mode': 'daily_retry_cycle_role_sequenced',
                'roles': roles,
                'requested_symbols': ['005930'],
                'feedback_scope_mode': scope_mode,
                'admission_scope_by_role': {
                    role: ['005930'] for role in roles
                },
                'role_symbol_scopes': {
                    role: ['005930'] for role in roles
                },
                'role_symbol_counts': {role: 1 for role in roles},
                'fs_div': 'CFS',
                'reuse_existing': True,
                'force_recompute': False,
                'knowledge_cutoff_at': cutoff,
                'admission_knowledge_cutoff_at': cutoff,
                'requested_knowledge_cutoff_at': cutoff,
            },
        )

    @classmethod
    def _terminal_continuation_session(
        cls,
        *,
        run: SimpleNamespace,
        stages: list[SimpleNamespace],
        eod_run: SimpleNamespace,
    ) -> MagicMock:
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        lineage_result = MagicMock()
        lineage_result.scalars.return_value = (
            cls._terminal_continuation_lineage()
        )
        active_result = MagicMock()
        active_result.scalars.return_value = []
        stages_result = MagicMock()
        stages_result.scalars.return_value = stages
        feedback_child_rows: list[SimpleNamespace] = []
        for stage in stages:
            if (
                stage.stage_id
                not in {'agent_feedback_summaries', 'signal_decider'}
                or stage.status != 'completed'
            ):
                continue
            source_run_ids = stage.source_run_ids or {}
            run_id = str(
                source_run_ids.get('agent_feedback_run_id')
                or source_run_ids.get('run_id')
                or ''
            ).strip()
            if run_id:
                feedback_child_rows.append(
                    cls._terminal_feedback_child_run(
                        stage_id=stage.stage_id,
                        run_id=run_id,
                    )
                )
        feedback_child_results = []
        for feedback_child_row in feedback_child_rows:
            feedback_child_result = MagicMock()
            feedback_child_result.scalar_one_or_none.return_value = (
                feedback_child_row
            )
            feedback_child_results.append(feedback_child_result)
        session = MagicMock()
        session.execute.side_effect = [
            MagicMock(),
            run_result,
            lineage_result,
            active_result,
            stages_result,
            *feedback_child_results,
        ]
        session.get.return_value = eod_run
        session.feedback_child_rows = feedback_child_rows
        return session

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

    def test_converge_terminal_continuation_accepts_ready_enrichment_warning(
        self,
    ) -> None:
        completed_at = datetime(2026, 9, 20, 9, 0, tzinfo=timezone.utc)
        finalized_at = completed_at + timedelta(seconds=1)
        run = SimpleNamespace(
            id=456,
            status='completed',
            error_message=None,
            completed_at=completed_at,
            updated_at=completed_at,
            run_metadata={
                'resolved_stages': [
                    'pykrx_enrichment',
                    'signal_decider',
                ],
                'resumable': True,
                'terminal_continuation_count': 1,
                'terminal_continuation_version': 1,
                'terminal_continuation_reason': (
                    'summary_knowledge_cutoff_repair'
                ),
                'terminal_continuation_claimed_at': (
                    '2026-09-20T08:55:00+00:00'
                ),
                'terminal_continuation_evidence': {},
                'resume_from_stage': 'agent_feedback_summaries',
            },
        )
        enrichment = SimpleNamespace(
            stage_id='pykrx_enrichment',
            status='completed',
            processed=3,
            total=3,
            succeeded=2,
            failed=1,
            remaining=0,
            completed_at=completed_at - timedelta(minutes=1),
            error_message=None,
            details={
                'result_summary': {
                    'status': 'completed',
                    'point_in_time_recovery': True,
                    'categories_requested': [
                        'sector_classification',
                        'investor_flow_symbol',
                        'short_selling',
                    ],
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
                    },
                    'downstream_preflight': {
                        'allowed': True,
                        'status': 'ready',
                        'required_categories': [
                            'sector_classification',
                            'investor_flow_symbol',
                            'short_selling',
                        ],
                        'category_row_counts': {
                            'sector_classification': 10,
                            'investor_flow_symbol': 10,
                            'short_selling': 10,
                        },
                        'failed_categories': [],
                        'blocked_categories': [],
                        'waiting_categories': [],
                        'zero_coverage_categories': [],
                    },
                }
            },
        )
        stages = [
            enrichment,
            SimpleNamespace(
                stage_id='signal_decider',
                status='completed',
                processed=1,
                total=1,
                succeeded=1,
                failed=0,
                remaining=0,
                completed_at=completed_at,
                error_message=None,
                details={'result_summary': {'status': 'completed'}},
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
                return_value=finalized_at,
            ),
        ):
            result = (
                DailyPipelineRunStateService()
                .converge_terminal_stages_run(
                    pipeline_run_id=456,
                    allow_satisfied_terminal=True,
                )
            )

        self.assertTrue(result['updated'])
        self.assertEqual(result['status'], 'completed')
        self.assertFalse(run.run_metadata['resumable'])
        self.assertEqual(run.updated_at, finalized_at)

        unmarked_run = SimpleNamespace(
            id=457,
            status='completed',
            error_message=None,
            completed_at=completed_at,
            updated_at=completed_at,
            run_metadata={
                'resolved_stages': [
                    'pykrx_enrichment',
                    'signal_decider',
                ],
                'resumable': True,
            },
        )
        unmarked_run_result = MagicMock()
        unmarked_run_result.scalar_one_or_none.return_value = unmarked_run
        unmarked_stages_result = MagicMock()
        unmarked_stages_result.scalars.return_value = stages
        unmarked_session = MagicMock()
        unmarked_session.execute.side_effect = [
            unmarked_run_result,
            unmarked_stages_result,
        ]
        unmarked_context = MagicMock()
        unmarked_context.__enter__.return_value = unmarked_session

        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=unmarked_context,
        ):
            unmarked_result = (
                DailyPipelineRunStateService()
                .converge_terminal_stages_run(
                    pipeline_run_id=457,
                    allow_satisfied_terminal=True,
                )
            )

        self.assertFalse(unmarked_result['updated'])
        self.assertEqual(
            unmarked_result['reason'],
            'resolved_stages_incomplete',
        )
        self.assertEqual(
            unmarked_result['incomplete_stages'],
            {'pykrx_enrichment': 'completed'},
        )

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

    def test_claim_terminal_continuation_accepts_ready_enrichment_warning(
        self,
    ) -> None:
        run, stages, eod_run = self._terminal_continuation_fixture()
        enrichment = stages[0]
        enrichment.processed = 3
        enrichment.total = 3
        enrichment.succeeded = 2
        enrichment.failed = 1
        enrichment.details = {
            'result_summary': {
                'status': 'completed',
                'point_in_time_recovery': True,
                'categories_requested': [
                    'sector_classification',
                    'investor_flow_symbol',
                    'short_selling',
                ],
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
                },
                'downstream_preflight': {
                    'allowed': True,
                    'status': 'ready',
                    'required_categories': [
                        'sector_classification',
                        'investor_flow_symbol',
                        'short_selling',
                    ],
                    'category_row_counts': {
                        'sector_classification': 10,
                        'investor_flow_symbol': 10,
                        'short_selling': 10,
                    },
                    'failed_categories': [],
                    'blocked_categories': [],
                    'waiting_categories': [],
                    'zero_coverage_categories': [],
                },
            }
        }
        run.run_metadata['repair_checkpoint_pipeline_run_id'] = 440

        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        lineage_result = MagicMock()
        lineage_result.scalars.return_value = (
            self._terminal_continuation_lineage()
        )
        active_result = MagicMock()
        active_result.scalars.return_value = []
        stages_result = MagicMock()
        stages_result.scalars.return_value = stages
        session = MagicMock()
        session.execute.side_effect = [
            MagicMock(),
            run_result,
            lineage_result,
            active_result,
            stages_result,
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
                return_value=datetime(
                    2026,
                    9,
                    20,
                    9,
                    0,
                    tzinfo=timezone.utc,
                ),
            ),
        ):
            result = DailyPipelineRunStateService().claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )

        self.assertTrue(result['claimed'])
        self.assertEqual(result['pipeline_run_id'], 456)
        self.assertEqual(run.status, 'running')
        self.assertEqual(
            run.run_metadata['terminal_continuation_count'],
            1,
        )

    def test_claim_terminal_continuation_reopens_same_run_and_archives_suffix(
        self,
    ) -> None:
        claimed_at = datetime(2026, 9, 20, 9, 0, tzinfo=timezone.utc)
        run, stages, eod_run = self._terminal_continuation_fixture()
        run.run_metadata['repair_checkpoint_pipeline_run_id'] = 440
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        lineage_result = MagicMock()
        lineage_result.scalars.return_value = (
            self._terminal_continuation_lineage()
        )
        active_result = MagicMock()
        active_result.scalars.return_value = []
        stages_result = MagicMock()
        stages_result.scalars.return_value = stages
        session = MagicMock()
        session.execute.side_effect = [
            MagicMock(),
            run_result,
            lineage_result,
            active_result,
            stages_result,
        ]
        session.get.return_value = eod_run
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ) as session_scope_mock,
            patch(
                'app.eod.daily_pipeline_state._utcnow',
                return_value=claimed_at,
            ),
        ):
            result = DailyPipelineRunStateService().claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )

        session_scope_mock.assert_called_once_with(
            statement_timeout_ms=15_000,
        )
        lock_call = session.execute.call_args_list[0]
        self.assertEqual(
            str(lock_call.args[0]),
            'SELECT pg_advisory_xact_lock(:source_key, :trade_key)',
        )
        self.assertEqual(
            lock_call.args[1],
            {'source_key': 20_001, 'trade_key': 20260918},
        )
        lineage_statement = session.execute.call_args_list[2].args[0]
        lineage_compiled = lineage_statement.compile()
        self.assertIn(
            'reused_eod_collection_run_id',
            lineage_compiled.params.values(),
        )
        self.assertIn(546, lineage_compiled.params.values())
        self.assertIn(
            'auto_eod_followup_resume',
            lineage_compiled.params.values(),
        )
        self.assertIn('FOR UPDATE', str(lineage_statement))
        self.assertTrue(result['claimed'])
        self.resolve_terminal_cutoff.assert_called_once_with(
            market_date=date(2026, 9, 18),
            source_run_id='agent_feedback_summaries-prior',
            requested_symbols=['005930'],
            source_run_session=session,
        )
        self.assertEqual(
            result['knowledge_cutoff_resolution'][
                'knowledge_cutoff_at'
            ],
            self.terminal_cutoff,
        )
        self.assertEqual(result['pipeline_run_id'], 456)
        self.assertEqual(result['reason'], 'summary_knowledge_cutoff_repair')
        self.assertEqual(result['pipeline_run']['id'], 456)
        self.assertEqual(result['pipeline_run']['status'], 'running')
        self.assertEqual(
            result['pipeline_run']['current_stage'],
            'agent_feedback_summaries',
        )
        self.assertEqual(run.status, 'running')
        self.assertIsNone(run.completed_at)
        self.assertIsNone(run.error_message)
        self.assertEqual(run.started_at, datetime(2026, 9, 20, 0, 45, tzinfo=timezone.utc))

        metadata = run.run_metadata
        self.assertEqual(metadata['terminal_continuation_count'], 1)
        self.assertEqual(metadata['terminal_continuation_version'], 1)
        self.assertEqual(
            metadata['terminal_continuation_claimed_at'],
            claimed_at.isoformat(),
        )
        self.assertEqual(
            metadata['terminal_continuation_reason'],
            'summary_knowledge_cutoff_repair',
        )
        self.assertEqual(
            metadata['resume_from_stage'],
            'agent_feedback_summaries',
        )
        evidence = metadata['terminal_continuation_evidence']
        self.assertEqual(evidence['parent']['status'], 'blocked')
        self.assertEqual(
            evidence['parent']['current_stage'],
            'signal_decider_evaluations',
        )
        self.assertEqual(
            evidence['parent']['resume_from_stage'],
            'pykrx_enrichment',
        )
        self.assertEqual(
            list(evidence['suffix_stages']),
            [
                'agent_feedback_summaries',
                'signal_decider',
                'next_open_execution_plan',
                'signal_decider_evaluations',
            ],
        )
        self.assertEqual(
            evidence['suffix_stages']['signal_decider']['status'],
            'failed',
        )
        self.assertEqual(
            evidence['lineage'],
            {
                'reused_eod_collection_run_id': 546,
                'expected_run_count': 6,
                'observed_run_count': 6,
                'ordered_pipeline_run_ids': [441, 442, 443, 444, 445, 456],
                'target_ordinal': 6,
                'latest_pipeline_run_id': 456,
                'repair_checkpoint_pipeline_run_id': 440,
            },
        )
        self.assertEqual(
            evidence['summary_proof'],
            {
                'ready': True,
                'reason': 'latest_completed_summary_cutoff',
                'knowledge_cutoff_at': (
                    self.terminal_cutoff.isoformat()
                ),
                'source_run_id': (
                    'agent_feedback_summaries-prior'
                ),
                'feedback_scope_mode': 'broad_feedback_universe',
                'requested_symbols': ['005930'],
                'requested_symbol_count': 1,
            },
        )
        for stage in stages[1:]:
            self.assertEqual(stage.status, 'pending')
            self.assertEqual(stage.processed, 0)
            self.assertEqual(stage.total, 0)
            self.assertEqual(stage.succeeded, 0)
            self.assertEqual(stage.failed, 0)
            self.assertEqual(stage.remaining, 0)
            self.assertIsNone(stage.completed_at)
            self.assertIsNone(stage.error_message)
            self.assertEqual(stage.source_run_ids, {})
            self.assertEqual(stage.details, {})
        self.assertEqual(stages[0].status, 'completed')
        self.assertEqual(
            metadata['stage_statuses'],
            {
                'pykrx_enrichment': 'completed',
                'agent_feedback_summaries': 'pending',
                'signal_decider': 'pending',
                'next_open_execution_plan': 'pending',
                'signal_decider_evaluations': 'pending',
            },
        )

    def test_claim_terminal_continuation_is_one_shot(self) -> None:
        run, _, _ = self._terminal_continuation_fixture()
        run.run_metadata['terminal_continuation_count'] = 1
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        session = MagicMock()
        session.execute.side_effect = [MagicMock(), run_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ):
            result = DailyPipelineRunStateService().claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )

        self.assertFalse(result['claimed'])
        self.assertEqual(result['reason'], 'terminal_continuation_already_claimed')
        self.assertEqual(run.status, 'blocked')
        self.assertEqual(session.execute.call_count, 2)

    def test_claim_terminal_continuation_resumes_stale_claim_in_place(
        self,
    ) -> None:
        claimed_at = datetime(2026, 9, 20, 9, 0, tzinfo=timezone.utc)
        resumed_at = claimed_at + timedelta(minutes=31)
        run, stages, eod_run = self._terminal_continuation_fixture()
        lineage = self._terminal_continuation_lineage()

        def build_session() -> MagicMock:
            run_result = MagicMock()
            run_result.scalar_one_or_none.return_value = run
            lineage_result = MagicMock()
            lineage_result.scalars.return_value = lineage
            active_result = MagicMock()
            active_result.scalars.return_value = []
            stages_result = MagicMock()
            stages_result.scalars.return_value = stages
            session = MagicMock()
            session.execute.side_effect = [
                MagicMock(),
                run_result,
                lineage_result,
                active_result,
                stages_result,
            ]
            session.get.return_value = eod_run
            return session

        service = DailyPipelineRunStateService()
        first_session = build_session()
        first_context = MagicMock()
        first_context.__enter__.return_value = first_session
        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=first_context,
            ),
            patch(
                'app.eod.daily_pipeline_state._utcnow',
                return_value=claimed_at,
            ),
        ):
            first = service.claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )

        self.assertTrue(first['claimed'])
        original_evidence = run.run_metadata[
            'terminal_continuation_evidence'
        ]
        self.resolve_terminal_cutoff.reset_mock()

        second_session = build_session()
        second_context = MagicMock()
        second_context.__enter__.return_value = second_session
        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=second_context,
            ),
            patch(
                'app.eod.daily_pipeline_state._utcnow',
                return_value=resumed_at,
            ),
            patch(
                'app.eod.daily_pipeline_state.settings.'
                'eod_auto_pykrx_stale_run_minutes',
                30,
            ),
        ):
            resumed = service.claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )

        self.assertTrue(resumed['claimed'])
        self.assertTrue(resumed['resumed'])
        self.assertEqual(resumed['pipeline_run_id'], 456)
        self.assertEqual(run.status, 'running')
        self.assertEqual(run.current_stage, 'agent_feedback_summaries')
        self.assertEqual(
            run.run_metadata['terminal_continuation_count'],
            1,
        )
        self.assertEqual(
            run.run_metadata['terminal_continuation_resume_count'],
            1,
        )
        self.assertEqual(
            run.run_metadata['terminal_continuation_claimed_at'],
            claimed_at.isoformat(),
        )
        self.assertEqual(
            run.run_metadata['terminal_continuation_resumed_at'],
            resumed_at.isoformat(),
        )
        self.assertIs(
            run.run_metadata['terminal_continuation_evidence'],
            original_evidence,
        )
        self.resolve_terminal_cutoff.assert_called_once_with(
            market_date=date(2026, 9, 18),
            source_run_id='agent_feedback_summaries-prior',
            requested_symbols=['005930'],
            source_run_session=second_session,
        )
        for stage in stages[1:]:
            self.assertEqual(stage.status, 'pending')
            self.assertEqual(
                stage.current_step,
                'terminal_continuation_pending',
            )

    def test_claim_terminal_continuation_rejects_fresh_claim_owner(
        self,
    ) -> None:
        claimed_at = datetime(2026, 9, 20, 9, 0, tzinfo=timezone.utc)
        run, stages, eod_run = self._terminal_continuation_fixture()
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        lineage_result = MagicMock()
        lineage_result.scalars.return_value = (
            self._terminal_continuation_lineage()
        )
        active_result = MagicMock()
        active_result.scalars.return_value = []
        stages_result = MagicMock()
        stages_result.scalars.return_value = stages
        session = MagicMock()
        session.execute.side_effect = [
            MagicMock(),
            run_result,
            lineage_result,
            active_result,
            stages_result,
        ]
        session.get.return_value = eod_run
        session_context = MagicMock()
        session_context.__enter__.return_value = session
        service = DailyPipelineRunStateService()

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch(
                'app.eod.daily_pipeline_state._utcnow',
                return_value=claimed_at,
            ),
        ):
            first = service.claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )
        self.assertTrue(first['claimed'])

        fresh_result = MagicMock()
        fresh_result.scalar_one_or_none.return_value = run
        fresh_session = MagicMock()
        fresh_session.execute.side_effect = [MagicMock(), fresh_result]
        fresh_context = MagicMock()
        fresh_context.__enter__.return_value = fresh_session
        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=fresh_context,
            ),
            patch(
                'app.eod.daily_pipeline_state._utcnow',
                return_value=claimed_at + timedelta(minutes=1),
            ),
            patch(
                'app.eod.daily_pipeline_state.settings.'
                'eod_auto_pykrx_stale_run_minutes',
                30,
            ),
        ):
            second = service.claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )

        self.assertFalse(second['claimed'])
        self.assertEqual(
            second['reason'],
            'terminal_continuation_still_active',
        )
        self.assertEqual(
            run.run_metadata['terminal_continuation_count'],
            1,
        )
        self.assertNotIn(
            'terminal_continuation_resume_count',
            run.run_metadata,
        )
        self.assertEqual(fresh_session.execute.call_count, 2)

    def test_claim_terminal_continuation_resumes_interrupted_claim_in_place(
        self,
    ) -> None:
        claimed_at = datetime(2026, 9, 20, 9, 0, tzinfo=timezone.utc)
        interrupted_at = claimed_at + timedelta(minutes=1)
        run, stages, eod_run = self._terminal_continuation_fixture()
        service = DailyPipelineRunStateService()

        first_session = self._terminal_continuation_session(
            run=run,
            stages=stages,
            eod_run=eod_run,
        )
        first_context = MagicMock()
        first_context.__enter__.return_value = first_session
        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=first_context,
            ),
            patch(
                'app.eod.daily_pipeline_state._utcnow',
                return_value=claimed_at,
            ),
        ):
            first = service.claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )
        self.assertTrue(first['claimed'])

        run.status = 'interrupted'
        run.updated_at = interrupted_at
        run.completed_at = interrupted_at
        run.run_metadata.update(
            {
                'resumable': True,
                'interrupted_reason': 'Core restarted.',
                'interrupted_at': interrupted_at.isoformat(),
            }
        )
        second_session = self._terminal_continuation_session(
            run=run,
            stages=stages,
            eod_run=eod_run,
        )
        second_context = MagicMock()
        second_context.__enter__.return_value = second_session
        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=second_context,
            ),
            patch(
                'app.eod.daily_pipeline_state._utcnow',
                return_value=interrupted_at + timedelta(seconds=1),
            ),
        ):
            resumed = service.claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )

        self.assertTrue(resumed['claimed'])
        self.assertTrue(resumed['resumed'])
        self.assertEqual(run.status, 'running')
        self.assertEqual(
            run.run_metadata['terminal_continuation_count'],
            1,
        )
        self.assertEqual(
            run.run_metadata['terminal_continuation_resumed_from_status'],
            'interrupted',
        )

    def test_claim_terminal_continuation_reclaims_exact_feedback_lease_loss(
        self,
    ) -> None:
        claimed_at = datetime(2026, 9, 20, 9, 0, tzinfo=timezone.utc)
        failed_at = claimed_at + timedelta(minutes=1)
        run, stages, eod_run = self._terminal_continuation_fixture()
        service = DailyPipelineRunStateService()

        first_session = self._terminal_continuation_session(
            run=run,
            stages=stages,
            eod_run=eod_run,
        )
        first_context = MagicMock()
        first_context.__enter__.return_value = first_session
        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=first_context,
            ),
            patch(
                'app.eod.daily_pipeline_state._utcnow',
                return_value=claimed_at,
            ),
        ):
            first = service.claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )
        self.assertTrue(first['claimed'])

        summary_stage = stages[1]
        summary_stage.status = 'completed'
        summary_stage.processed = 1
        summary_stage.total = 1
        summary_stage.succeeded = 1
        summary_stage.failed = 0
        summary_stage.remaining = 0
        summary_stage.current_step = 'agent_feedback_summaries_completed'
        summary_stage.completed_at = failed_at
        summary_stage.updated_at = failed_at
        summary_stage.error_message = None
        summary_stage.source_run_ids = {
            'run_id': 'agent_feedback_summaries-continued'
        }
        summary_stage.details = {
            'result_summary': {
                'status': 'completed',
                'run_id': 'agent_feedback_summaries-continued',
            }
        }

        lease_run_id = 'feedback-cycle-2026-09-18-deadbeef01'
        lease_loss_error = (
            f'feedback retry lease ownership lost for {lease_run_id} at '
            'before_parent_finalization: '
            'heartbeat_refresh_grace_exceeded'
        )
        decider_stage = stages[2]
        decider_stage.status = 'failed'
        decider_stage.processed = 2
        decider_stage.total = 2
        decider_stage.succeeded = 0
        decider_stage.failed = 2
        decider_stage.remaining = 0
        decider_stage.current_step = 'signal_decider_failed'
        decider_stage.completed_at = failed_at
        decider_stage.updated_at = failed_at
        decider_stage.error_message = lease_loss_error
        decider_stage.source_run_ids = {
            'agent_feedback_run_id': lease_run_id
        }
        decider_stage.details = {}
        run.status = 'failed'
        run.current_stage = 'signal_decider'
        run.updated_at = failed_at
        run.completed_at = failed_at
        run.error_message = lease_loss_error

        pending_run = deepcopy(run)
        pending_stages = deepcopy(stages)
        pending_decider = pending_stages[2]
        pending_decider.status = 'pending'
        pending_decider.processed = 0
        pending_decider.total = 0
        pending_decider.succeeded = 0
        pending_decider.failed = 0
        pending_decider.remaining = 0
        pending_decider.current_step = 'terminal_continuation_pending'
        pending_decider.completed_at = None
        pending_decider.error_message = None
        pending_decider.source_run_ids = {}
        pending_decider.details = {}
        pending_session = self._terminal_continuation_session(
            run=pending_run,
            stages=pending_stages,
            eod_run=eod_run,
        )
        pending_context = MagicMock()
        pending_context.__enter__.return_value = pending_session
        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=pending_context,
            ),
            patch(
                'app.eod.daily_pipeline_state._utcnow',
                return_value=failed_at + timedelta(seconds=1),
            ),
        ):
            pending_result = service.claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )
        self.assertFalse(pending_result['claimed'])
        self.assertEqual(
            pending_result['reason'],
            'terminal_continuation_stage_progress_invalid',
        )

        generic_run = deepcopy(run)
        generic_stages = deepcopy(stages)
        generic_run.error_message = 'upstream request failed'
        generic_stages[2].error_message = 'upstream request failed'
        generic_session = self._terminal_continuation_session(
            run=generic_run,
            stages=generic_stages,
            eod_run=eod_run,
        )
        generic_context = MagicMock()
        generic_context.__enter__.return_value = generic_session
        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=generic_context,
            ),
            patch(
                'app.eod.daily_pipeline_state._utcnow',
                return_value=failed_at + timedelta(seconds=1),
            ),
        ):
            generic_result = service.claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )
        self.assertFalse(generic_result['claimed'])
        self.assertEqual(
            generic_result['reason'],
            'terminal_continuation_stage_progress_invalid',
        )

        for malformed_resume_count in ('0', 'corrupt', True):
            with self.subTest(
                malformed_resume_count=malformed_resume_count,
            ):
                malformed_run = deepcopy(run)
                malformed_run.run_metadata[
                    'terminal_continuation_resume_count'
                ] = malformed_resume_count
                malformed_run_result = MagicMock()
                malformed_run_result.scalar_one_or_none.return_value = (
                    malformed_run
                )
                malformed_session = MagicMock()
                malformed_session.execute.side_effect = [
                    MagicMock(),
                    malformed_run_result,
                ]
                malformed_context = MagicMock()
                malformed_context.__enter__.return_value = (
                    malformed_session
                )
                with patch(
                    'app.eod.daily_pipeline_state.session_scope',
                    return_value=malformed_context,
                ):
                    malformed_result = (
                        service.claim_terminal_continuation(
                            pipeline_run_id=456,
                            market_date='2026-09-18',
                            reused_eod_collection_run_id=546,
                        )
                    )
                self.assertFalse(malformed_result['claimed'])
                self.assertEqual(
                    malformed_result['reason'],
                    'terminal_continuation_failed_lease_loss_ineligible',
                )
                self.assertEqual(malformed_session.execute.call_count, 2)

        lease_child = self._terminal_feedback_child_run(
            stage_id='signal_decider',
            run_id=lease_run_id,
        )
        lease_child.status = 'abandoned'
        lease_child.completed_at = failed_at
        lease_child.completed_count = 0
        lease_child.failed_count = lease_child.symbols_total
        lease_child.request_payload['retry_admission'] = {
            'schema_version': 'agent_feedback_retry_admission_v1',
            'claimed_at': claimed_at.isoformat(),
            'owner_token_hash': 'a' * 64,
        }
        lease_child.summary_payload = {
            'admission_lease_loss': {
                'schema_version': (
                    'agent_feedback_retry_admission_lease_loss_v1'
                ),
                'detected_at': failed_at.isoformat(),
                'stage': 'before_checkpoint:role_batch_completed',
                'reason': 'heartbeat_refresh_grace_exceeded',
                'owner_token_fingerprint': 'a' * 16,
            }
        }
        summary_child = self._terminal_feedback_child_run(
            stage_id='agent_feedback_summaries',
            run_id='agent_feedback_summaries-continued',
        )

        def build_resume_session(child: SimpleNamespace) -> MagicMock:
            run_result = MagicMock()
            run_result.scalar_one_or_none.return_value = run
            lineage_result = MagicMock()
            lineage_result.scalars.return_value = (
                self._terminal_continuation_lineage()
            )
            active_result = MagicMock()
            active_result.scalars.return_value = []
            stages_result = MagicMock()
            stages_result.scalars.return_value = stages
            lease_child_result = MagicMock()
            lease_child_result.scalar_one_or_none.return_value = child
            summary_child_result = MagicMock()
            summary_child_result.scalar_one_or_none.return_value = (
                summary_child
            )
            session = MagicMock()
            session.execute.side_effect = [
                MagicMock(),
                run_result,
                lineage_result,
                active_result,
                stages_result,
                lease_child_result,
                summary_child_result,
            ]
            session.get.return_value = eod_run
            return session

        tampered_child = deepcopy(lease_child)
        tampered_child.summary_payload['admission_lease_loss'][
            'owner_token_fingerprint'
        ] = 'b' * 16
        tampered_context = MagicMock()
        tampered_context.__enter__.return_value = build_resume_session(
            tampered_child
        )
        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=tampered_context,
            ),
            patch(
                'app.eod.daily_pipeline_state._utcnow',
                return_value=failed_at + timedelta(seconds=1),
            ),
        ):
            tampered_result = service.claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )
        self.assertFalse(tampered_result['claimed'])
        self.assertEqual(
            tampered_result['reason'],
            'terminal_continuation_failed_lease_loss_evidence_invalid',
        )
        self.assertEqual(tampered_result['field'], 'admission_lease_loss')
        self.assertEqual(run.status, 'failed')
        self.assertEqual(run.completed_at, failed_at)
        self.assertEqual(run.error_message, lease_loss_error)
        self.assertEqual(decider_stage.status, 'failed')
        self.assertEqual(decider_stage.processed, 2)
        self.assertEqual(
            decider_stage.source_run_ids,
            {'agent_feedback_run_id': lease_run_id},
        )
        self.assertNotIn(
            'terminal_continuation_resume_count',
            run.run_metadata,
        )
        tampered_context.__enter__.return_value.flush.assert_not_called()

        resume_session = build_resume_session(lease_child)
        resume_context = MagicMock()
        resume_context.__enter__.return_value = resume_session
        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=resume_context,
            ),
            patch(
                'app.eod.daily_pipeline_state._utcnow',
                return_value=failed_at + timedelta(seconds=1),
            ),
        ):
            resumed = service.claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )

        self.assertTrue(resumed['claimed'])
        self.assertTrue(resumed['resumed'])
        self.assertEqual(resumed['resume_from_stage'], 'signal_decider')
        self.assertEqual(
            list(resumed['persisted_stage_outcomes']),
            ['agent_feedback_summaries'],
        )
        self.assertEqual(run.status, 'running')
        self.assertEqual(run.current_stage, 'signal_decider')
        self.assertEqual(
            run.run_metadata['terminal_continuation_resumed_from_status'],
            'failed',
        )
        self.assertEqual(
            run.run_metadata['terminal_continuation_resume_count'],
            1,
        )
        self.assertEqual(
            run.run_metadata[
                'terminal_continuation_feedback_lease_loss_run_id'
            ],
            lease_run_id,
        )
        failed_child_statement = (
            resume_session.execute.call_args_list[5].args[0]
        )
        failed_child_compiled = failed_child_statement.compile()
        self.assertIn(
            lease_run_id,
            failed_child_compiled.params.values(),
        )
        self.assertIn(
            'agent_context_feedback_runs',
            str(failed_child_statement),
        )
        self.assertIn('FOR UPDATE', str(failed_child_statement))
        self.assertEqual(summary_stage.status, 'completed')
        for stage in stages[2:]:
            self.assertEqual(stage.status, 'pending')
            self.assertEqual(stage.processed, 0)
            self.assertEqual(stage.details, {})

        interrupted_at = failed_at + timedelta(minutes=1)
        run.status = 'interrupted'
        run.current_stage = 'signal_decider'
        run.updated_at = interrupted_at
        run.completed_at = interrupted_at
        run.error_message = 'Core restarted.'
        run.run_metadata.update(
            {
                'resumable': True,
                'interrupted_reason': 'Core restarted.',
                'interrupted_at': interrupted_at.isoformat(),
            }
        )
        for invalid_resume_count, invalid_resume_origin in (
            (2, 'failed'),
            (1, 'running'),
        ):
            with self.subTest(
                invalid_resume_count=invalid_resume_count,
                invalid_resume_origin=invalid_resume_origin,
            ):
                run.run_metadata[
                    'terminal_continuation_resume_count'
                ] = invalid_resume_count
                run.run_metadata[
                    'terminal_continuation_resumed_from_status'
                ] = invalid_resume_origin
                invalid_restart_session = build_resume_session(
                    lease_child
                )
                invalid_restart_context = MagicMock()
                invalid_restart_context.__enter__.return_value = (
                    invalid_restart_session
                )
                with (
                    patch(
                        'app.eod.daily_pipeline_state.session_scope',
                        return_value=invalid_restart_context,
                    ),
                    patch(
                        'app.eod.daily_pipeline_state._utcnow',
                        return_value=(
                            interrupted_at + timedelta(minutes=31)
                        ),
                    ),
                ):
                    invalid_restart_result = (
                        service.claim_terminal_continuation(
                            pipeline_run_id=456,
                            market_date='2026-09-18',
                            reused_eod_collection_run_id=546,
                        )
                    )
                self.assertFalse(invalid_restart_result['claimed'])
                self.assertEqual(
                    invalid_restart_result['reason'],
                    'terminal_continuation_failed_lease_loss_ineligible',
                )
                invalid_restart_session.flush.assert_not_called()
        run.run_metadata['terminal_continuation_resume_count'] = 1
        run.run_metadata[
            'terminal_continuation_resumed_from_status'
        ] = 'failed'
        tampered_restart_child = deepcopy(lease_child)
        tampered_restart_child.summary_payload['admission_lease_loss'][
            'owner_token_fingerprint'
        ] = 'b' * 16
        tampered_restart_session = build_resume_session(
            tampered_restart_child
        )
        tampered_restart_context = MagicMock()
        tampered_restart_context.__enter__.return_value = (
            tampered_restart_session
        )
        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=tampered_restart_context,
            ),
            patch(
                'app.eod.daily_pipeline_state._utcnow',
                return_value=interrupted_at + timedelta(minutes=31),
            ),
        ):
            tampered_restart_result = (
                service.claim_terminal_continuation(
                    pipeline_run_id=456,
                    market_date='2026-09-18',
                    reused_eod_collection_run_id=546,
                )
            )
        self.assertFalse(tampered_restart_result['claimed'])
        self.assertEqual(
            tampered_restart_result['reason'],
            'terminal_continuation_failed_lease_loss_evidence_invalid',
        )
        self.assertEqual(
            tampered_restart_result['field'],
            'admission_lease_loss',
        )
        self.assertEqual(
            run.run_metadata['terminal_continuation_resume_count'],
            1,
        )

        valid_restart_session = build_resume_session(lease_child)
        valid_restart_context = MagicMock()
        valid_restart_context.__enter__.return_value = (
            valid_restart_session
        )
        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=valid_restart_context,
            ),
            patch(
                'app.eod.daily_pipeline_state._utcnow',
                return_value=interrupted_at + timedelta(minutes=31),
            ),
        ):
            valid_restart_result = service.claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )
        self.assertTrue(valid_restart_result['claimed'])
        self.assertEqual(
            run.run_metadata['terminal_continuation_resume_count'],
            2,
        )

        decider_stage.status = 'failed'
        decider_stage.processed = 2
        decider_stage.total = 2
        decider_stage.succeeded = 0
        decider_stage.failed = 2
        decider_stage.remaining = 0
        decider_stage.current_step = 'signal_decider_failed'
        decider_stage.completed_at = failed_at + timedelta(minutes=1)
        decider_stage.error_message = lease_loss_error
        decider_stage.source_run_ids = {
            'agent_feedback_run_id': lease_run_id
        }
        run.status = 'failed'
        run.current_stage = 'signal_decider'
        run.completed_at = decider_stage.completed_at
        run.error_message = lease_loss_error
        second_run_result = MagicMock()
        second_run_result.scalar_one_or_none.return_value = run
        second_session = MagicMock()
        second_session.execute.side_effect = [
            MagicMock(),
            second_run_result,
        ]
        second_context = MagicMock()
        second_context.__enter__.return_value = second_session
        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=second_context,
        ):
            second_result = service.claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )
        self.assertFalse(second_result['claimed'])
        self.assertEqual(
            second_result['reason'],
            'terminal_continuation_failed_lease_loss_ineligible',
        )
        self.assertEqual(decider_stage.status, 'failed')

    def test_claim_terminal_continuation_resumes_from_first_unfinished_stage(
        self,
    ) -> None:
        claimed_at = datetime(2026, 9, 20, 9, 0, tzinfo=timezone.utc)
        interrupted_at = claimed_at + timedelta(minutes=1)
        suffix_ids = (
            'agent_feedback_summaries',
            'signal_decider',
            'next_open_execution_plan',
            'signal_decider_evaluations',
        )

        for completed_prefix_size in range(len(suffix_ids)):
            with self.subTest(completed_prefix_size=completed_prefix_size):
                run, stages, eod_run = self._terminal_continuation_fixture()
                service = DailyPipelineRunStateService()
                first_session = self._terminal_continuation_session(
                    run=run,
                    stages=stages,
                    eod_run=eod_run,
                )
                first_context = MagicMock()
                first_context.__enter__.return_value = first_session
                with (
                    patch(
                        'app.eod.daily_pipeline_state.session_scope',
                        return_value=first_context,
                    ),
                    patch(
                        'app.eod.daily_pipeline_state._utcnow',
                        return_value=claimed_at,
                    ),
                ):
                    first = service.claim_terminal_continuation(
                        pipeline_run_id=456,
                        market_date='2026-09-18',
                        reused_eod_collection_run_id=546,
                    )
                self.assertTrue(first['claimed'])

                suffix_stages = stages[1:]
                for stage in suffix_stages[:completed_prefix_size]:
                    stage.status = 'completed'
                    stage.processed = 1
                    stage.total = 1
                    stage.succeeded = 1
                    stage.failed = 0
                    stage.remaining = 0
                    stage.current_step = f'{stage.stage_id}_completed'
                    stage.current_activity = f'Completed {stage.stage_id}'
                    stage.completed_at = interrupted_at
                    stage.updated_at = interrupted_at
                    stage.error_message = None
                    stage.source_run_ids = {
                        'run_id': f'{stage.stage_id}-continued'
                    }
                    stage.details = {
                        'result_summary': {
                            'status': 'completed',
                            'run_id': f'{stage.stage_id}-continued',
                        }
                    }

                interrupted_stage = suffix_stages[completed_prefix_size]
                if completed_prefix_size == 1:
                    between_run = deepcopy(run)
                    between_stages = deepcopy(stages)
                    between_run.status = 'interrupted'
                    satisfied_stage_id = suffix_ids[0]
                    between_run.current_stage = satisfied_stage_id
                    between_run.updated_at = interrupted_at
                    between_run.completed_at = interrupted_at
                    between_run.run_metadata.update(
                        {
                            'resumable': True,
                            'interrupted_reason': 'Core restarted.',
                            'interrupted_at': interrupted_at.isoformat(),
                        }
                    )
                    tampered_session = self._terminal_continuation_session(
                        run=between_run,
                        stages=between_stages,
                        eod_run=eod_run,
                    )
                    tampered_session.feedback_child_rows[
                        0
                    ].request_payload['requested_symbols'] = ['000660']
                    tampered_context = MagicMock()
                    tampered_context.__enter__.return_value = (
                        tampered_session
                    )
                    with (
                        patch(
                            'app.eod.daily_pipeline_state.session_scope',
                            return_value=tampered_context,
                        ),
                        patch(
                            'app.eod.daily_pipeline_state._utcnow',
                            return_value=(
                                interrupted_at + timedelta(seconds=1)
                            ),
                        ),
                    ):
                        tampered_result = (
                            service.claim_terminal_continuation(
                                pipeline_run_id=456,
                                market_date='2026-09-18',
                                reused_eod_collection_run_id=546,
                            )
                        )
                    self.assertFalse(tampered_result['claimed'])
                    self.assertEqual(
                        tampered_result['reason'],
                        'terminal_continuation_feedback_prefix_evidence_invalid',
                    )
                    incomplete_session = (
                        self._terminal_continuation_session(
                            run=between_run,
                            stages=between_stages,
                            eod_run=eod_run,
                        )
                    )
                    incomplete_session.feedback_child_rows[
                        0
                    ].completed_count = 0
                    incomplete_context = MagicMock()
                    incomplete_context.__enter__.return_value = (
                        incomplete_session
                    )
                    with (
                        patch(
                            'app.eod.daily_pipeline_state.session_scope',
                            return_value=incomplete_context,
                        ),
                        patch(
                            'app.eod.daily_pipeline_state._utcnow',
                            return_value=(
                                interrupted_at + timedelta(seconds=1)
                            ),
                        ),
                    ):
                        incomplete_result = (
                            service.claim_terminal_continuation(
                                pipeline_run_id=456,
                                market_date='2026-09-18',
                                reused_eod_collection_run_id=546,
                            )
                        )
                    self.assertFalse(incomplete_result['claimed'])
                    self.assertEqual(
                        incomplete_result['reason'],
                        'terminal_continuation_feedback_prefix_evidence_invalid',
                    )
                    between_session = self._terminal_continuation_session(
                        run=between_run,
                        stages=between_stages,
                        eod_run=eod_run,
                    )
                    between_context = MagicMock()
                    between_context.__enter__.return_value = between_session
                    with (
                        patch(
                            'app.eod.daily_pipeline_state.session_scope',
                            return_value=between_context,
                        ),
                        patch(
                            'app.eod.daily_pipeline_state._utcnow',
                            return_value=(
                                interrupted_at + timedelta(seconds=1)
                            ),
                        ),
                    ):
                        between_result = (
                            service.claim_terminal_continuation(
                                pipeline_run_id=456,
                                market_date='2026-09-18',
                                reused_eod_collection_run_id=546,
                            )
                        )
                    self.assertTrue(between_result['claimed'])
                    self.assertEqual(
                        between_result['resume_from_stage'],
                        suffix_ids[1],
                    )
                    self.assertEqual(
                        between_stages[1].stage_id,
                        satisfied_stage_id,
                    )
                    self.assertEqual(between_stages[1].status, 'completed')

                if interrupted_stage.stage_id in {
                    'agent_feedback_summaries',
                    'signal_decider',
                }:
                    interrupted_stage.status = 'running'
                    interrupted_stage.total = 1
                    interrupted_stage.remaining = 1
                    interrupted_stage.current_step = (
                        f'{interrupted_stage.stage_id}_started'
                    )
                    interrupted_stage.completed_at = None
                    interrupted_stage.updated_at = claimed_at
                    interrupted_stage.error_message = None
                    interrupted_stage.details = {'resumable': True}
                    run.status = 'running'
                    run.current_stage = interrupted_stage.stage_id
                    run.updated_at = claimed_at
                    run.completed_at = None
                    running_session = self._terminal_continuation_session(
                        run=run,
                        stages=stages,
                        eod_run=eod_run,
                    )
                    running_context = MagicMock()
                    running_context.__enter__.return_value = running_session
                    with (
                        patch(
                            'app.eod.daily_pipeline_state.session_scope',
                            return_value=running_context,
                        ),
                        patch(
                            'app.eod.daily_pipeline_state._utcnow',
                            return_value=(
                                claimed_at + timedelta(minutes=31)
                            ),
                        ),
                    ):
                        running_result = (
                            service.claim_terminal_continuation(
                                pipeline_run_id=456,
                                market_date='2026-09-18',
                                reused_eod_collection_run_id=546,
                            )
                        )
                    self.assertFalse(running_result['claimed'])
                    self.assertEqual(
                        running_result['reason'],
                        'terminal_continuation_feedback_stage_still_active',
                    )

                interrupted_stage.status = 'interrupted'
                interrupted_stage.total = 1
                interrupted_stage.remaining = 1
                interrupted_stage.current_step = 'core_restart_interrupted'
                interrupted_stage.current_activity = 'Core restarted.'
                interrupted_stage.completed_at = interrupted_at
                interrupted_stage.updated_at = interrupted_at
                interrupted_stage.error_message = 'Core restarted.'
                interrupted_stage.details = {
                    'interrupted_reason': 'Core restarted.',
                    'resumable': True,
                }
                run.status = 'interrupted'
                run.current_stage = interrupted_stage.stage_id
                run.updated_at = interrupted_at
                run.completed_at = interrupted_at
                run.run_metadata.update(
                    {
                        'resumable': True,
                        'interrupted_reason': 'Core restarted.',
                        'interrupted_at': interrupted_at.isoformat(),
                    }
                )

                if interrupted_stage.stage_id in {
                    'agent_feedback_summaries',
                    'signal_decider',
                }:
                    active_session = self._terminal_continuation_session(
                        run=run,
                        stages=stages,
                        eod_run=eod_run,
                    )
                    active_context = MagicMock()
                    active_context.__enter__.return_value = active_session
                    with (
                        patch(
                            'app.eod.daily_pipeline_state.session_scope',
                            return_value=active_context,
                        ),
                        patch(
                            'app.eod.daily_pipeline_state._utcnow',
                            return_value=(
                                interrupted_at + timedelta(seconds=1)
                            ),
                        ),
                    ):
                        still_active = service.claim_terminal_continuation(
                            pipeline_run_id=456,
                            market_date='2026-09-18',
                            reused_eod_collection_run_id=546,
                        )
                    self.assertFalse(still_active['claimed'])
                    self.assertEqual(
                        still_active['reason'],
                        'terminal_continuation_feedback_stage_still_active',
                    )
                    self.assertEqual(interrupted_stage.status, 'interrupted')

                second_session = self._terminal_continuation_session(
                    run=run,
                    stages=stages,
                    eod_run=eod_run,
                )
                second_context = MagicMock()
                second_context.__enter__.return_value = second_session
                with (
                    patch(
                        'app.eod.daily_pipeline_state.session_scope',
                        return_value=second_context,
                    ),
                    patch(
                        'app.eod.daily_pipeline_state._utcnow',
                        return_value=interrupted_at + timedelta(minutes=31),
                    ),
                ):
                    resumed = service.claim_terminal_continuation(
                        pipeline_run_id=456,
                        market_date='2026-09-18',
                        reused_eod_collection_run_id=546,
                    )

                self.assertTrue(resumed['claimed'])
                self.assertTrue(resumed['resumed'])
                self.assertEqual(
                    resumed['resume_from_stage'],
                    interrupted_stage.stage_id,
                )
                self.assertEqual(
                    list(resumed['persisted_stage_outcomes']),
                    list(suffix_ids[:completed_prefix_size]),
                )
                self.assertEqual(run.current_stage, interrupted_stage.stage_id)
                for stage in suffix_stages[:completed_prefix_size]:
                    self.assertEqual(stage.status, 'completed')
                    self.assertEqual(stage.succeeded, 1)
                for stage in suffix_stages[completed_prefix_size:]:
                    self.assertEqual(stage.status, 'pending')
                    self.assertEqual(stage.processed, 0)
                    self.assertEqual(stage.details, {})
                self.assertEqual(
                    {
                        stage_id: run.run_metadata['stage_statuses'][stage_id]
                        for stage_id in suffix_ids
                    },
                    {
                        **{
                            stage_id: 'completed'
                            for stage_id in suffix_ids[:completed_prefix_size]
                        },
                        **{
                            stage_id: 'pending'
                            for stage_id in suffix_ids[completed_prefix_size:]
                        },
                    },
                )
                if completed_prefix_size == 1:
                    second_completed_stage = suffix_stages[1]
                    second_completed_stage.status = 'completed'
                    second_completed_stage.processed = 1
                    second_completed_stage.total = 1
                    second_completed_stage.succeeded = 1
                    second_completed_stage.remaining = 0
                    second_completed_stage.current_step = (
                        'signal_decider_completed'
                    )
                    second_completed_stage.completed_at = (
                        interrupted_at + timedelta(minutes=32)
                    )
                    second_completed_stage.updated_at = (
                        second_completed_stage.completed_at
                    )
                    second_completed_stage.source_run_ids = {
                        'run_id': 'signal_decider-continued'
                    }
                    second_completed_stage.details = {
                        'result_summary': {
                            'status': 'completed',
                            'run_id': 'signal_decider-continued',
                        }
                    }
                    next_interrupted_stage = suffix_stages[2]
                    next_interrupted_stage.status = 'interrupted'
                    next_interrupted_stage.current_step = (
                        'core_restart_interrupted'
                    )
                    next_interrupted_stage.completed_at = (
                        interrupted_at + timedelta(minutes=32)
                    )
                    next_interrupted_stage.updated_at = (
                        next_interrupted_stage.completed_at
                    )
                    next_interrupted_stage.error_message = 'Core restarted.'
                    next_interrupted_stage.details = {
                        'interrupted_reason': 'Core restarted.',
                        'resumable': True,
                    }
                    run.status = 'interrupted'
                    run.current_stage = next_interrupted_stage.stage_id
                    run.updated_at = next_interrupted_stage.updated_at
                    run.completed_at = next_interrupted_stage.completed_at
                    run.run_metadata.update(
                        {
                            'resumable': True,
                            'interrupted_reason': 'Core restarted.',
                            'interrupted_at': (
                                next_interrupted_stage.completed_at.isoformat()
                            ),
                        }
                    )
                    third_session = self._terminal_continuation_session(
                        run=run,
                        stages=stages,
                        eod_run=eod_run,
                    )
                    third_context = MagicMock()
                    third_context.__enter__.return_value = third_session
                    with (
                        patch(
                            'app.eod.daily_pipeline_state.session_scope',
                            return_value=third_context,
                        ),
                        patch(
                            'app.eod.daily_pipeline_state._utcnow',
                            return_value=(
                                interrupted_at + timedelta(minutes=33)
                            ),
                        ),
                    ):
                        resumed_again = (
                            service.claim_terminal_continuation(
                                pipeline_run_id=456,
                                market_date='2026-09-18',
                                reused_eod_collection_run_id=546,
                            )
                        )
                    self.assertTrue(resumed_again['claimed'])
                    self.assertEqual(
                        list(resumed_again['persisted_stage_outcomes']),
                        list(suffix_ids[:2]),
                    )
                    self.assertEqual(
                        resumed_again['resume_from_stage'],
                        'next_open_execution_plan',
                    )
                    self.assertEqual(
                        run.run_metadata['terminal_continuation_count'],
                        1,
                    )
                    self.assertEqual(
                        run.run_metadata['terminal_continuation_resume_count'],
                        2,
                    )

    def test_claim_terminal_continuation_preserves_expired_feedback_skips(
        self,
    ) -> None:
        claimed_at = datetime(2026, 9, 20, 9, 0, tzinfo=timezone.utc)
        interrupted_at = datetime(
            2026,
            9,
            21,
            0,
            5,
            tzinfo=timezone.utc,
        )
        suffix_ids = (
            'agent_feedback_summaries',
            'signal_decider',
            'next_open_execution_plan',
            'signal_decider_evaluations',
        )

        for skipped_prefix_size in (1, 2, 3):
            with self.subTest(skipped_prefix_size=skipped_prefix_size):
                run, stages, eod_run = self._terminal_continuation_fixture()
                service = DailyPipelineRunStateService()
                first_session = self._terminal_continuation_session(
                    run=run,
                    stages=stages,
                    eod_run=eod_run,
                )
                first_context = MagicMock()
                first_context.__enter__.return_value = first_session
                with (
                    patch(
                        'app.eod.daily_pipeline_state.session_scope',
                        return_value=first_context,
                    ),
                    patch(
                        'app.eod.daily_pipeline_state._utcnow',
                        return_value=claimed_at,
                    ),
                ):
                    claimed = service.claim_terminal_continuation(
                        pipeline_run_id=456,
                        market_date='2026-09-18',
                        reused_eod_collection_run_id=546,
                    )
                self.assertTrue(claimed['claimed'])

                suffix_stages = stages[1:]
                for stage in suffix_stages[:skipped_prefix_size]:
                    stage.status = 'skipped'
                    stage.processed = 0
                    stage.total = 0
                    stage.succeeded = 0
                    stage.failed = 0
                    stage.remaining = 0
                    stage.current_step = f'{stage.stage_id}_skipped'
                    stage.current_activity = f'Skipped {stage.stage_id}'
                    stage.updated_at = interrupted_at
                    stage.completed_at = interrupted_at
                    stage.error_message = None
                    stage.source_run_ids = {}
                    stage.details = {
                        'result_summary': {
                            'triggered': False,
                            'status': 'skipped',
                            'reason': 'next_open_window_expired',
                            'market_date': '2026-09-18',
                            'target_trade_date': '2026-09-21',
                            'current_market_date': '2026-09-21',
                            'market_state': 'OPEN',
                            'stage': stage.stage_id,
                        }
                    }

                run.status = 'interrupted'
                run.current_stage = suffix_ids[skipped_prefix_size - 1]
                run.updated_at = interrupted_at
                run.completed_at = interrupted_at
                run.run_metadata.update(
                    {
                        'resumable': True,
                        'interrupted_reason': 'Core restarted.',
                        'interrupted_at': interrupted_at.isoformat(),
                    }
                )
                resume_session = self._terminal_continuation_session(
                    run=run,
                    stages=stages,
                    eod_run=eod_run,
                )
                resume_context = MagicMock()
                resume_context.__enter__.return_value = resume_session
                with (
                    patch(
                        'app.eod.daily_pipeline_state.session_scope',
                        return_value=resume_context,
                    ),
                    patch(
                        'app.eod.daily_pipeline_state._utcnow',
                        return_value=interrupted_at + timedelta(seconds=1),
                    ),
                ):
                    resumed = service.claim_terminal_continuation(
                        pipeline_run_id=456,
                        market_date='2026-09-18',
                        reused_eod_collection_run_id=546,
                    )

                self.assertTrue(resumed['claimed'])
                self.assertEqual(
                    resumed['resume_from_stage'],
                    suffix_ids[skipped_prefix_size],
                )
                self.assertEqual(
                    list(resumed['persisted_stage_outcomes']),
                    list(suffix_ids[:skipped_prefix_size]),
                )
                for stage in suffix_stages[:skipped_prefix_size]:
                    self.assertEqual(stage.status, 'skipped')
                    self.assertEqual(stage.source_run_ids, {})

    def test_expired_decision_skip_rejects_forged_market_context(
        self,
    ) -> None:
        completed_at = datetime(
            2026,
            9,
            21,
            0,
            5,
            tzinfo=timezone.utc,
        )
        stage = self._terminal_continuation_stage(
            stage_id='agent_feedback_summaries',
            status='skipped',
        )
        stage.processed = 0
        stage.total = 0
        stage.succeeded = 0
        stage.failed = 0
        stage.remaining = 0
        stage.current_step = 'agent_feedback_summaries_skipped'
        stage.completed_at = completed_at
        stage.error_message = None
        stage.source_run_ids = {}
        stage.details = {
            'result_summary': {
                'triggered': False,
                'status': 'skipped',
                'reason': 'next_open_window_expired',
                'market_date': '2026-09-18',
                'target_trade_date': '2026-09-21',
                'current_market_date': '2026-09-21',
                'market_state': 'OPEN',
                'stage': 'agent_feedback_summaries',
            }
        }

        self.assertTrue(
            _expired_decision_skip_evidence_is_valid(
                stage_row=stage,
                stage_id='agent_feedback_summaries',
                market_date=date(2026, 9, 18),
            )
        )

        stage.details['result_summary']['market_state'] = 'BOGUS'
        self.assertFalse(
            _expired_decision_skip_evidence_is_valid(
                stage_row=stage,
                stage_id='agent_feedback_summaries',
                market_date=date(2026, 9, 18),
            )
        )

        stage.details['result_summary'].update(
            {
                'market_state': 'OPEN',
                'target_trade_date': '2099-09-20',
                'current_market_date': '2099-09-21',
            }
        )
        self.assertFalse(
            _expired_decision_skip_evidence_is_valid(
                stage_row=stage,
                stage_id='agent_feedback_summaries',
                market_date=date(2026, 9, 18),
            )
        )

    def test_claim_terminal_continuation_rejects_noncontiguous_progress(
        self,
    ) -> None:
        claimed_at = datetime(2026, 9, 20, 9, 0, tzinfo=timezone.utc)
        interrupted_at = claimed_at + timedelta(minutes=1)
        run, stages, eod_run = self._terminal_continuation_fixture()
        service = DailyPipelineRunStateService()
        first_session = self._terminal_continuation_session(
            run=run,
            stages=stages,
            eod_run=eod_run,
        )
        first_context = MagicMock()
        first_context.__enter__.return_value = first_session
        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=first_context,
            ),
            patch(
                'app.eod.daily_pipeline_state._utcnow',
                return_value=claimed_at,
            ),
        ):
            first = service.claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )
        self.assertTrue(first['claimed'])

        summary_stage = stages[1]
        summary_stage.status = 'interrupted'
        summary_stage.current_step = 'core_restart_interrupted'
        summary_stage.completed_at = interrupted_at
        summary_stage.updated_at = interrupted_at
        summary_stage.error_message = 'Core restarted.'
        summary_stage.details = {
            'interrupted_reason': 'Core restarted.',
            'resumable': True,
        }
        later_stage = stages[2]
        later_stage.status = 'completed'
        later_stage.processed = 1
        later_stage.total = 1
        later_stage.succeeded = 1
        later_stage.completed_at = interrupted_at
        later_stage.updated_at = interrupted_at
        later_stage.details = {
            'result_summary': {'status': 'completed'}
        }
        run.status = 'interrupted'
        run.current_stage = summary_stage.stage_id
        run.updated_at = interrupted_at
        run.completed_at = interrupted_at
        run.run_metadata.update(
            {
                'resumable': True,
                'interrupted_reason': 'Core restarted.',
                'interrupted_at': interrupted_at.isoformat(),
            }
        )

        second_session = self._terminal_continuation_session(
            run=run,
            stages=stages,
            eod_run=eod_run,
        )
        second_context = MagicMock()
        second_context.__enter__.return_value = second_session
        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=second_context,
            ),
            patch(
                'app.eod.daily_pipeline_state._utcnow',
                return_value=interrupted_at + timedelta(seconds=1),
            ),
        ):
            resumed = service.claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )

        self.assertFalse(resumed['claimed'])
        self.assertEqual(
            resumed['reason'],
            'terminal_continuation_stage_progress_invalid',
        )

    def test_claim_terminal_continuation_rejects_tampered_claim_evidence(
        self,
    ) -> None:
        claimed_at = datetime(2026, 9, 20, 9, 0, tzinfo=timezone.utc)

        def touched_suffix(run, stages):
            stages[1].current_step = 'agent_feedback_summaries_started'

        def changed_scope(run, stages):
            run.run_metadata['terminal_continuation_evidence'][
                'summary_proof'
            ]['feedback_scope_mode'] = 'requested_symbols'

        def changed_archived_failure(run, stages):
            evidence = run.run_metadata[
                'terminal_continuation_evidence'
            ]['suffix_stages']['signal_decider']
            evidence['details']['result_summary']['progress']['summary'][
                'failure_groups'
            ]['dependency/missing-upstream']['failure_category'] = 'transport'

        def changed_archived_summary_status(run, stages):
            run.run_metadata['terminal_continuation_evidence'][
                'suffix_stages'
            ]['agent_feedback_summaries']['status'] = 'partial'

        def changed_execution_context(run, stages):
            run.run_metadata['fs_div'] = 'INVALID'

        cases = (
            (
                'touched suffix',
                touched_suffix,
                'terminal_continuation_suffix_already_started',
            ),
            (
                'changed feedback scope',
                changed_scope,
                'summary_knowledge_cutoff_proof_mismatch',
            ),
            (
                'changed archived failure',
                changed_archived_failure,
                'terminal_continuation_archived_suffix_invalid',
            ),
            (
                'changed archived summary status',
                changed_archived_summary_status,
                'terminal_continuation_archived_suffix_invalid',
            ),
            (
                'changed execution context',
                changed_execution_context,
                'terminal_continuation_execution_context_invalid',
            ),
        )
        for label, mutate, expected_reason in cases:
            with self.subTest(label=label):
                run, stages, eod_run = self._terminal_continuation_fixture()
                service = DailyPipelineRunStateService()
                first_session = self._terminal_continuation_session(
                    run=run,
                    stages=stages,
                    eod_run=eod_run,
                )
                first_context = MagicMock()
                first_context.__enter__.return_value = first_session
                with (
                    patch(
                        'app.eod.daily_pipeline_state.session_scope',
                        return_value=first_context,
                    ),
                    patch(
                        'app.eod.daily_pipeline_state._utcnow',
                        return_value=claimed_at,
                    ),
                ):
                    first = service.claim_terminal_continuation(
                        pipeline_run_id=456,
                        market_date='2026-09-18',
                        reused_eod_collection_run_id=546,
                    )
                self.assertTrue(first['claimed'])
                mutate(run, stages)

                second_session = self._terminal_continuation_session(
                    run=run,
                    stages=stages,
                    eod_run=eod_run,
                )
                second_context = MagicMock()
                second_context.__enter__.return_value = second_session
                with (
                    patch(
                        'app.eod.daily_pipeline_state.session_scope',
                        return_value=second_context,
                    ),
                    patch(
                        'app.eod.daily_pipeline_state._utcnow',
                        return_value=claimed_at + timedelta(minutes=31),
                    ),
                    patch(
                        'app.eod.daily_pipeline_state.settings.'
                        'eod_auto_pykrx_stale_run_minutes',
                        30,
                    ),
                ):
                    result = service.claim_terminal_continuation(
                        pipeline_run_id=456,
                        market_date='2026-09-18',
                        reused_eod_collection_run_id=546,
                    )

                self.assertFalse(result['claimed'])
                self.assertEqual(result['reason'], expected_reason)
                self.assertEqual(
                    run.run_metadata['terminal_continuation_count'],
                    1,
                )

    def test_claim_terminal_continuation_rejects_missing_execution_scope(
        self,
    ) -> None:
        run, _, _ = self._terminal_continuation_fixture()
        run.run_metadata['symbols'] = []
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        session = MagicMock()
        session.execute.side_effect = [MagicMock(), run_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ):
            result = DailyPipelineRunStateService().claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )

        self.assertFalse(result['claimed'])
        self.assertEqual(
            result['reason'],
            'terminal_continuation_execution_context_invalid',
        )
        self.assertEqual(result['field'], 'symbols')
        self.assertEqual(run.status, 'blocked')

    def test_claim_terminal_continuation_requires_exact_summary_source_run(
        self,
    ) -> None:
        run, stages, eod_run = self._terminal_continuation_fixture()
        stages[1].source_run_ids = {}
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        lineage_result = MagicMock()
        lineage_result.scalars.return_value = (
            self._terminal_continuation_lineage()
        )
        active_result = MagicMock()
        active_result.scalars.return_value = []
        stages_result = MagicMock()
        stages_result.scalars.return_value = stages
        session = MagicMock()
        session.execute.side_effect = [
            MagicMock(),
            run_result,
            lineage_result,
            active_result,
            stages_result,
        ]
        session.get.return_value = eod_run
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ):
            result = DailyPipelineRunStateService().claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )

        self.assertFalse(result['claimed'])
        self.assertEqual(
            result['reason'],
            'summary_source_run_id_unavailable',
        )
        self.assertEqual(run.status, 'blocked')

    def test_cutoff_proof_failure_does_not_spend_one_shot_claim(self) -> None:
        run, stages, eod_run = self._terminal_continuation_fixture()

        def build_session_context():
            run_result = MagicMock()
            run_result.scalar_one_or_none.return_value = run
            lineage_result = MagicMock()
            lineage_result.scalars.return_value = (
                self._terminal_continuation_lineage()
            )
            active_result = MagicMock()
            active_result.scalars.return_value = []
            stages_result = MagicMock()
            stages_result.scalars.return_value = stages
            session = MagicMock()
            session.execute.side_effect = [
                MagicMock(),
                run_result,
                lineage_result,
                active_result,
                stages_result,
            ]
            session.get.return_value = eod_run
            context = MagicMock()
            context.__enter__.return_value = session
            return context

        unavailable_context = build_session_context()
        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=unavailable_context,
            ),
            patch(
                'app.eod.daily_pipeline_state.'
                '_resolve_terminal_summary_knowledge_cutoff',
                return_value={
                    'ready': False,
                    'reason': 'summary_run_symbol_scope_mismatch',
                    'knowledge_cutoff_at': None,
                    'source_run_id': (
                        'agent_feedback_summaries-prior'
                    ),
                    'feedback_scope_mode': (
                        'broad_feedback_universe'
                    ),
                },
            ),
        ):
            first = DailyPipelineRunStateService().claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )

        self.assertFalse(first['claimed'])
        self.assertEqual(
            first['reason'],
            'summary_knowledge_cutoff_unavailable',
        )
        self.assertEqual(run.status, 'blocked')
        self.assertNotIn(
            'terminal_continuation_count',
            run.run_metadata,
        )
        self.assertEqual(stages[1].status, 'completed')

        retry_context = build_session_context()
        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=retry_context,
        ):
            second = DailyPipelineRunStateService().claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )

        self.assertTrue(second['claimed'])
        self.assertEqual(run.status, 'running')
        self.assertEqual(
            run.run_metadata['terminal_continuation_count'],
            1,
        )

    def test_cutoff_lookup_exception_leaves_terminal_claim_unchanged(
        self,
    ) -> None:
        run, stages, eod_run = self._terminal_continuation_fixture()
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        lineage_result = MagicMock()
        lineage_result.scalars.return_value = (
            self._terminal_continuation_lineage()
        )
        active_result = MagicMock()
        active_result.scalars.return_value = []
        stages_result = MagicMock()
        stages_result.scalars.return_value = stages
        session = MagicMock()
        session.execute.side_effect = [
            MagicMock(),
            run_result,
            lineage_result,
            active_result,
            stages_result,
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
                'app.eod.daily_pipeline_state.'
                '_resolve_terminal_summary_knowledge_cutoff',
                side_effect=RuntimeError('transient cutoff timeout'),
            ),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                'transient cutoff timeout',
            ):
                DailyPipelineRunStateService().claim_terminal_continuation(
                    pipeline_run_id=456,
                    market_date='2026-09-18',
                    reused_eod_collection_run_id=546,
                )

        self.assertEqual(run.status, 'blocked')
        self.assertNotIn(
            'terminal_continuation_count',
            run.run_metadata,
        )
        self.assertEqual(stages[1].status, 'completed')

    def test_claim_terminal_continuation_rejects_overlapping_active_run(
        self,
    ) -> None:
        run, _, eod_run = self._terminal_continuation_fixture()
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        lineage_result = MagicMock()
        lineage_result.scalars.return_value = (
            self._terminal_continuation_lineage()
        )
        active_result = MagicMock()
        active_result.scalars.return_value = [SimpleNamespace(id=457)]
        session = MagicMock()
        session.execute.side_effect = [
            MagicMock(),
            run_result,
            lineage_result,
            active_result,
        ]
        session.get.return_value = eod_run
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ):
            result = DailyPipelineRunStateService().claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )

        self.assertFalse(result['claimed'])
        self.assertEqual(result['reason'], 'overlapping_run_already_active')
        self.assertEqual(result['active_pipeline_run_ids'], [457])
        self.assertEqual(run.status, 'blocked')

    def test_claim_terminal_continuation_rejects_wrong_eod_lineage(self) -> None:
        run, _, _ = self._terminal_continuation_fixture()
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        session = MagicMock()
        session.execute.side_effect = [MagicMock(), run_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ):
            result = DailyPipelineRunStateService().claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=999,
            )

        self.assertFalse(result['claimed'])
        self.assertEqual(result['reason'], 'eod_lineage_mismatch')
        self.assertEqual(run.status, 'blocked')
        session.get.assert_not_called()

    def test_claim_terminal_continuation_requires_exactly_six_lineage_runs(
        self,
    ) -> None:
        for run_ids in (
            (442, 443, 444, 445, 456),
            (440, 441, 442, 443, 444, 445, 456),
        ):
            with self.subTest(run_count=len(run_ids)):
                run, _, eod_run = self._terminal_continuation_fixture()
                run_result = MagicMock()
                run_result.scalar_one_or_none.return_value = run
                lineage_result = MagicMock()
                lineage_result.scalars.return_value = (
                    self._terminal_continuation_lineage(run_ids)
                )
                session = MagicMock()
                session.execute.side_effect = [
                    MagicMock(),
                    run_result,
                    lineage_result,
                ]
                session.get.return_value = eod_run
                session_context = MagicMock()
                session_context.__enter__.return_value = session

                with patch(
                    'app.eod.daily_pipeline_state.session_scope',
                    return_value=session_context,
                ):
                    result = (
                        DailyPipelineRunStateService()
                        .claim_terminal_continuation(
                            pipeline_run_id=456,
                            market_date='2026-09-18',
                            reused_eod_collection_run_id=546,
                        )
                    )

                self.assertFalse(result['claimed'])
                self.assertEqual(
                    result['reason'],
                    'terminal_continuation_lineage_count_mismatch',
                )
                self.assertEqual(result['expected_run_count'], 6)
                self.assertEqual(result['observed_run_count'], len(run_ids))
                self.assertEqual(run.status, 'blocked')
                self.assertEqual(session.execute.call_count, 3)

    def test_claim_terminal_continuation_requires_target_to_be_sixth_latest(
        self,
    ) -> None:
        run, _, eod_run = self._terminal_continuation_fixture()
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        lineage_result = MagicMock()
        lineage_result.scalars.return_value = (
            self._terminal_continuation_lineage(
                (441, 442, 443, 444, 456, 457)
            )
        )
        session = MagicMock()
        session.execute.side_effect = [
            MagicMock(),
            run_result,
            lineage_result,
        ]
        session.get.return_value = eod_run
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ):
            result = DailyPipelineRunStateService().claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )

        self.assertFalse(result['claimed'])
        self.assertEqual(
            result['reason'],
            'terminal_continuation_target_not_sixth_latest',
        )
        self.assertEqual(result['target_ordinal'], 5)
        self.assertEqual(result['latest_pipeline_run_id'], 457)
        self.assertEqual(run.status, 'blocked')
        self.assertEqual(session.execute.call_count, 3)

    def test_claim_terminal_continuation_rejects_noncanonical_decider_failure(
        self,
    ) -> None:
        run, stages, eod_run = self._terminal_continuation_fixture()
        failure_groups = stages[2].details['result_summary']['progress'][
            'summary'
        ]['failure_groups']
        failure_groups['provider/overloaded'] = {
            'failure_category': 'provider',
            'count': 1,
            'retryable_count': 1,
            'failure_kinds': ['provider_overloaded'],
        }
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        lineage_result = MagicMock()
        lineage_result.scalars.return_value = (
            self._terminal_continuation_lineage()
        )
        active_result = MagicMock()
        active_result.scalars.return_value = []
        stages_result = MagicMock()
        stages_result.scalars.return_value = stages
        session = MagicMock()
        session.execute.side_effect = [
            MagicMock(),
            run_result,
            lineage_result,
            active_result,
            stages_result,
        ]
        session.get.return_value = eod_run
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ):
            result = DailyPipelineRunStateService().claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )

        self.assertFalse(result['claimed'])
        self.assertEqual(
            result['reason'],
            'signal_decider_failure_not_retryable_missing_upstream_only',
        )
        self.assertEqual(run.status, 'blocked')
        self.assertEqual(stages[2].status, 'failed')

    def test_claim_terminal_continuation_rejects_unexpected_blocked_suffix(
        self,
    ) -> None:
        run, stages, eod_run = self._terminal_continuation_fixture()
        stages[3].details['result_summary']['reason'] = 'market_closed'
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        lineage_result = MagicMock()
        lineage_result.scalars.return_value = (
            self._terminal_continuation_lineage()
        )
        active_result = MagicMock()
        active_result.scalars.return_value = []
        stages_result = MagicMock()
        stages_result.scalars.return_value = stages
        session = MagicMock()
        session.execute.side_effect = [
            MagicMock(),
            run_result,
            lineage_result,
            active_result,
            stages_result,
        ]
        session.get.return_value = eod_run
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ):
            result = DailyPipelineRunStateService().claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )

        self.assertFalse(result['claimed'])
        self.assertEqual(result['reason'], 'next_open_block_reason_mismatch')
        self.assertEqual(run.status, 'blocked')
        self.assertEqual(stages[3].status, 'blocked')

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

        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ) as session_scope_mock:
            with self.assertRaisesRegex(
                ValueError,
                r'daily pipeline run is not active: 341 \(abandoned\)',
            ):
                DailyPipelineRunStateService().finish_run(
                    pipeline_run_id=341,
                    status='completed',
                )
        session_scope_mock.assert_called_once_with(statement_timeout_ms=15_000)

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
            ) as session_scope_mock,
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
        session_scope_mock.assert_called_once_with(statement_timeout_ms=15_000)

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
