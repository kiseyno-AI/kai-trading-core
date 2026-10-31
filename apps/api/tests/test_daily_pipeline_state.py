import hashlib
import unittest
from copy import deepcopy
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.agent_context.lease_loss_evidence import (
    terminal_feedback_completed_run_evidence_error,
)
from app.db_models import DailyPipelineRun, DailyPipelineStageRun, EODCollectionRun
from app.eod.daily_pipeline_outcomes import daily_pipeline_outcome_is_satisfied
from app.eod.daily_pipeline_state import (
    DailyPipelineRunStateService,
    _expired_decision_skip_evidence_is_valid,
    _summary_cutoff_contract_repair_execution_context_error,
    _stage_row_outcome,
)


def _completed_feedback_summary_stage(proof: dict) -> dict:
    return {
        'status': 'completed',
        'source_run_ids': {'run_id': proof['source_run_id']},
        'details': {
            'result_summary': {
                'status': 'completed',
                **proof,
                'run_id': proof['source_run_id'],
            }
        },
    }


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
        self.completed_scope_proof = {
            'schema_version': 'completed_summarizer_intersection_v1',
            'completed_symbols': ['005930'],
            'admitted_symbols': ['005930'],
        }
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
                'feedback_scope_mode': 'decider_scope_fast_path',
                'requested_symbols': ['005930'],
                'completed_scope_proof': self.completed_scope_proof,
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
                details={
                    'result_summary': {
                        'status': 'completed',
                        'run_id': 'agent_feedback_summaries-prior',
                        'feedback_scope_mode': (
                            'decider_scope_fast_path'
                        ),
                        'requested_symbols': None,
                    }
                },
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

    @staticmethod
    def _same_day_rearm_claim_fixture(
        *,
        lineage_count: int = 6,
        conflicting_cutoff: bool = False,
        current_checkpoint: str = 'after_signal',
    ) -> tuple[SimpleNamespace, list[SimpleNamespace], SimpleNamespace, MagicMock, dict]:
        source_date = date(2026, 10, 27)
        cutoff = datetime(2026, 10, 27, 7, 55, tzinfo=timezone.utc)
        source_completed_at = datetime(
            2026, 10, 28, 0, 35, tzinfo=timezone.utc
        )
        marker_started_at = source_completed_at + timedelta(seconds=1)
        current_started_at = source_completed_at + timedelta(minutes=2)
        interrupted_at = current_started_at + timedelta(minutes=2)
        proof = {
            'source_run_id': 'feedback-cycle-summary',
            'feedback_scope_mode': 'decider_scope_fast_path',
            'requested_symbols': None,
            'expected_completed_symbols': ['005930'],
            'knowledge_cutoff_at': cutoff.isoformat(),
            'fs_div': 'CFS',
        }

        def run_metadata(*, cutoff_source: str = 'feedback-cycle-summary') -> dict:
            return {
                'entrypoint': 'daily_pipeline_stage_service.run',
                'resolved_stages': [
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                'resume_from_stage': 'signal_decider',
                'reused_eod_collection_run_id': 581,
                'symbols': None,
                'markets': ['KOSPI', 'KOSDAQ', 'KONEX'],
                'fs_div': 'CFS',
                'point_in_time_recovery': True,
                'force': False,
                'reuse_existing_feedback': True,
                'allow_expired_analysis_replay': False,
                'same_day_intraday_watch_continuation_admitted': True,
                'feedback_knowledge_cutoff': {
                    'ready': True,
                    'knowledge_cutoff_at': cutoff.isoformat(),
                    'source_run_id': cutoff_source,
                    'feedback_scope_mode': 'decider_scope_fast_path',
                    'requested_symbols': ['005930'],
                },
            }

        interruption_reason = 'Core restarted after signal completion.'
        current = SimpleNamespace(
            id=593,
            market_date=source_date,
            trigger='auto_eod_followup_resume',
            status='interrupted',
            current_stage='signal_decider',
            started_at=current_started_at,
            updated_at=interrupted_at,
            completed_at=interrupted_at,
            error_message=interruption_reason,
            run_metadata={
                **run_metadata(),
                'resumable': True,
                'interrupted_at': interrupted_at.isoformat(),
                'interrupted_reason': interruption_reason,
            },
        )
        signal = SimpleNamespace(
            id=1,
            pipeline_run_id=593,
            market_date=source_date,
            stage_id='signal_decider',
            status='completed',
            processed=160,
            total=160,
            succeeded=160,
            failed=0,
            remaining=0,
            current_step='signal_decider_completed',
            current_activity='Signal decider completed',
            current_symbol=None,
            current_period=None,
            started_at=current_started_at,
            updated_at=current_started_at + timedelta(minutes=1),
            completed_at=current_started_at + timedelta(minutes=1),
            error_message=None,
            source_run_ids={
                'agent_feedback_run_id': 'decider-cycle-completed'
            },
            details={'result_summary': {'status': 'completed'}},
        )
        current_stage_rows = [signal]
        if current_checkpoint in {'during_plan', 'during_evaluation'}:
            plan_completed = current_checkpoint == 'during_evaluation'
            current_stage_rows.append(
                SimpleNamespace(
                    id=2,
                    pipeline_run_id=593,
                    market_date=source_date,
                    stage_id='next_open_execution_plan',
                    status=(
                        'completed' if plan_completed else 'interrupted'
                    ),
                    processed=1 if plan_completed else 0,
                    total=1 if plan_completed else 0,
                    succeeded=1 if plan_completed else 0,
                    failed=0,
                    remaining=0,
                    current_step=(
                        'next_open_execution_plan_completed'
                        if plan_completed
                        else 'core_restart_interrupted'
                    ),
                    current_activity='Plan stage',
                    current_symbol=None,
                    current_period=None,
                    started_at=current_started_at + timedelta(minutes=1),
                    updated_at=interrupted_at,
                    completed_at=interrupted_at,
                    error_message=(
                        None if plan_completed else interruption_reason
                    ),
                    source_run_ids=(
                        {'plan_id': 77} if plan_completed else {}
                    ),
                    details=(
                        {'result_summary': {'status': 'completed'}}
                        if plan_completed
                        else {
                            'resumable': True,
                            'interrupted_reason': interruption_reason,
                        }
                    ),
                )
            )
        if current_checkpoint == 'during_evaluation':
            current_stage_rows.append(
                SimpleNamespace(
                    id=3,
                    pipeline_run_id=593,
                    market_date=source_date,
                    stage_id='signal_decider_evaluations',
                    status='interrupted',
                    processed=0,
                    total=0,
                    succeeded=0,
                    failed=0,
                    remaining=0,
                    current_step='core_restart_interrupted',
                    current_activity='Evaluation stage',
                    current_symbol=None,
                    current_period=None,
                    started_at=current_started_at + timedelta(minutes=2),
                    updated_at=interrupted_at,
                    completed_at=interrupted_at,
                    error_message=interruption_reason,
                    source_run_ids={},
                    details={
                        'resumable': True,
                        'interrupted_reason': interruption_reason,
                    },
                )
            )
        if current_checkpoint == 'during_plan':
            current.current_stage = 'next_open_execution_plan'
        elif current_checkpoint == 'during_evaluation':
            current.current_stage = 'signal_decider_evaluations'
        elif current_checkpoint != 'after_signal':
            raise AssertionError(
                f'unknown current checkpoint: {current_checkpoint}'
            )
        proof_run = SimpleNamespace(
            id=590,
            market_date=source_date,
            trigger='auto_eod_followup_resume',
            status='blocked',
            current_stage='signal_decider_evaluations',
            started_at=source_completed_at - timedelta(minutes=10),
            completed_at=source_completed_at - timedelta(minutes=5),
            error_message=None,
            run_metadata=run_metadata(
                cutoff_source=(
                    'conflicting-feedback-cycle'
                    if conflicting_cutoff
                    else 'feedback-cycle-summary'
                )
            ),
        )
        source_run = SimpleNamespace(
            id=592,
            market_date=source_date,
            trigger='auto_eod_followup_resume',
            status='skipped',
            current_stage='signal_decider_evaluations',
            started_at=source_completed_at - timedelta(minutes=1),
            completed_at=source_completed_at,
            error_message=None,
            run_metadata={
                **run_metadata(),
                'same_day_intraday_watch_continuation_admitted': False,
            },
        )
        filler_count = max(0, lineage_count - 3)
        filler_ids = list(range(560, 560 + filler_count))
        lineage = [
            *[
                SimpleNamespace(
                    id=run_id,
                    started_at=source_completed_at
                    - timedelta(minutes=30 - index),
                    run_metadata=run_metadata(),
                )
                for index, run_id in enumerate(filler_ids)
            ],
            proof_run,
            source_run,
            current,
        ]
        eod_run = SimpleNamespace(
            id=581,
            market='KRX',
            source='pykrx',
            trade_date=source_date,
            run_metadata={
                'same_day_intraday_watch_rearm': {
                    'version': 1,
                    'status': 'pending',
                    'source_false_skip_run_id': 592,
                    'proof_pipeline_run_id': 590,
                    'target_trade_date': '2026-10-28',
                    'attempt_limit': 8,
                    'started_at': marker_started_at.isoformat(),
                }
            },
        )
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = current
        lineage_result = MagicMock()
        lineage_result.scalars.return_value = lineage
        active_result = MagicMock()
        active_result.scalars.return_value = []
        stages_result = MagicMock()
        stages_result.scalars.return_value = current_stage_rows
        marker_evidence_rows: list[SimpleNamespace] = []
        for stage_id in (
            'signal_decider',
            'next_open_execution_plan',
            'signal_decider_evaluations',
        ):
            marker_evidence_rows.append(
                SimpleNamespace(
                    id=100 + len(marker_evidence_rows),
                    pipeline_run_id=592,
                    market_date=source_date,
                    stage_id=stage_id,
                    status='skipped',
                    processed=0,
                    total=0,
                    succeeded=0,
                    failed=0,
                    remaining=0,
                    completed_at=source_completed_at,
                    error_message=None,
                    source_run_ids={},
                    details={
                        'result_summary': {
                            'triggered': False,
                            'status': 'skipped',
                            'reason': 'next_open_window_expired',
                            'market_date': '2026-10-27',
                            'target_trade_date': '2026-10-28',
                            'current_market_date': '2026-10-28',
                            'market_state': 'OPEN',
                            'stage': stage_id,
                        }
                    },
                )
            )
        marker_evidence_rows.extend(
            [
                SimpleNamespace(
                    id=200,
                    pipeline_run_id=590,
                    market_date=source_date,
                    stage_id='signal_decider',
                    status='partial',
                    processed=160,
                    total=160,
                    succeeded=147,
                    failed=13,
                    remaining=0,
                    completed_at=source_completed_at - timedelta(minutes=5),
                    error_message=None,
                    source_run_ids={
                        'agent_feedback_run_id': 'decider-cycle-prior'
                    },
                    details={
                        'result_summary': {
                            'run_id': 'decider-cycle-prior',
                            'prerequisites': {'ready': True},
                            'progress': {
                                'summary': {
                                    'run_status': 'partial',
                                    'failed_items': 13,
                                    'failure_groups': {
                                        'openclaw_transport_error': {
                                            'count': 13,
                                            'retryable_count': 13,
                                        }
                                    },
                                    'role_progress': {
                                        'short-term-decider': {
                                            'retry_pending_symbols': 13,
                                            'in_progress_symbols': 0,
                                            'pending_symbols': 0,
                                        },
                                        'signal-decider': {
                                            'retry_pending_symbols': 0,
                                            'in_progress_symbols': 0,
                                            'pending_symbols': 0,
                                        },
                                    },
                                }
                            },
                        }
                    },
                ),
                SimpleNamespace(
                    id=201,
                    pipeline_run_id=590,
                    market_date=source_date,
                    stage_id='next_open_execution_plan',
                    status='blocked',
                    processed=0,
                    total=0,
                    succeeded=0,
                    failed=0,
                    remaining=0,
                    completed_at=source_completed_at - timedelta(minutes=5),
                    error_message=None,
                    source_run_ids={},
                    details={
                        'result_summary': {
                            'reason': 'signal_decider_stage_incomplete'
                        }
                    },
                ),
                SimpleNamespace(
                    id=202,
                    pipeline_run_id=590,
                    market_date=source_date,
                    stage_id='signal_decider_evaluations',
                    status='blocked',
                    processed=0,
                    total=0,
                    succeeded=0,
                    failed=0,
                    remaining=0,
                    completed_at=source_completed_at - timedelta(minutes=5),
                    error_message=None,
                    source_run_ids={},
                    details={
                        'result_summary': {
                            'reason': 'declared_dependencies_incomplete',
                            'incomplete_dependencies': ['signal_decider'],
                        }
                    },
                ),
            ]
        )
        marker_evidence_result = MagicMock()
        marker_evidence_result.scalars.return_value = marker_evidence_rows
        session = MagicMock()
        session.execute.side_effect = [
            MagicMock(),
            run_result,
            lineage_result,
            marker_evidence_result,
            active_result,
            stages_result,
        ]
        session.lineage_rows = lineage
        session.marker_evidence_rows = marker_evidence_rows
        session.get.return_value = eod_run
        return current, current_stage_rows, eod_run, session, proof

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

    def test_same_day_rearm_claim_preserves_completed_signal_lazy_tail(
        self,
    ) -> None:
        current, signal_rows, _, session, proof = (
            self._same_day_rearm_claim_fixture()
        )
        context = MagicMock()
        context.__enter__.return_value = session

        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=context,
        ):
            result = (
                DailyPipelineRunStateService()
                .claim_same_day_intraday_watch_rearm_resume(
                    pipeline_run_id=593,
                    market_date='2026-10-27',
                    reused_eod_collection_run_id=581,
                    target_trade_date='2026-10-28',
                    expected_summary_cutoff_proof=proof,
                )
            )

        self.assertTrue(result['claimed'], result)
        self.assertEqual(result['resume_from_stage'], 'next_open_execution_plan')
        self.assertEqual(
            list(result['persisted_stage_outcomes']), ['signal_decider']
        )
        self.assertEqual(signal_rows[0].status, 'completed')
        self.assertEqual(current.status, 'running')
        self.assertEqual(current.current_stage, 'next_open_execution_plan')
        self.assertEqual(
            current.run_metadata['stage_statuses'],
            {
                'signal_decider': 'completed',
                'next_open_execution_plan': 'pending',
                'signal_decider_evaluations': 'pending',
            },
        )
        session.flush.assert_called_once()

    def test_same_day_rearm_claim_accepts_lazy_plan_and_evaluation_shapes(
        self,
    ) -> None:
        for checkpoint, expected_resume, expected_persisted in (
            (
                'during_plan',
                'next_open_execution_plan',
                ['signal_decider'],
            ),
            (
                'during_evaluation',
                'signal_decider_evaluations',
                ['signal_decider', 'next_open_execution_plan'],
            ),
        ):
            with self.subTest(checkpoint=checkpoint):
                current, stage_rows, _, session, proof = (
                    self._same_day_rearm_claim_fixture(
                        current_checkpoint=checkpoint
                    )
                )
                context = MagicMock()
                context.__enter__.return_value = session
                with patch(
                    'app.eod.daily_pipeline_state.session_scope',
                    return_value=context,
                ):
                    result = (
                        DailyPipelineRunStateService()
                        .claim_same_day_intraday_watch_rearm_resume(
                            pipeline_run_id=593,
                            market_date='2026-10-27',
                            reused_eod_collection_run_id=581,
                            target_trade_date='2026-10-28',
                            expected_summary_cutoff_proof=proof,
                        )
                    )
                self.assertTrue(result['claimed'], result)
                self.assertEqual(result['resume_from_stage'], expected_resume)
                self.assertEqual(
                    list(result['persisted_stage_outcomes']),
                    expected_persisted,
                )
                self.assertEqual(current.current_stage, expected_resume)
                self.assertEqual(
                    {
                        row.stage_id: row.status
                        for row in stage_rows
                    },
                    {
                        stage_id: (
                            'completed'
                            if stage_id in expected_persisted
                            else 'pending'
                        )
                        for stage_id in {
                            row.stage_id for row in stage_rows
                        }
                    },
                )
                session.flush.assert_called_once()

    def test_same_day_rearm_claim_rejects_attempt_nine_and_cutoff_mismatch(
        self,
    ) -> None:
        for lineage_count, conflicting_cutoff, expected_reason in (
            (9, False, 'same_day_rearm_lineage_mismatch'),
            (8, True, 'proof_pipeline_cutoff_mismatch'),
        ):
            with self.subTest(
                lineage_count=lineage_count,
                conflicting_cutoff=conflicting_cutoff,
            ):
                _, _, _, session, proof = (
                    self._same_day_rearm_claim_fixture(
                        lineage_count=lineage_count,
                        conflicting_cutoff=conflicting_cutoff,
                    )
                )
                context = MagicMock()
                context.__enter__.return_value = session
                with patch(
                    'app.eod.daily_pipeline_state.session_scope',
                    return_value=context,
                ):
                    result = (
                        DailyPipelineRunStateService()
                        .claim_same_day_intraday_watch_rearm_resume(
                            pipeline_run_id=593,
                            market_date='2026-10-27',
                            reused_eod_collection_run_id=581,
                            target_trade_date='2026-10-28',
                            expected_summary_cutoff_proof=proof,
                        )
                    )
                self.assertFalse(result['claimed'], result)
                self.assertEqual(result['reason'], expected_reason)
                session.flush.assert_not_called()

    def test_same_day_rearm_claim_revalidates_marker_source_and_proof(
        self,
    ) -> None:
        for mutation, expected_reason in (
            ('proof_source_id', 'proof_pipeline_evidence_invalid'),
            ('source_false_skip', 'source_false_skip_evidence_invalid'),
            (
                'proof_chronology',
                'same_day_rearm_proof_chronology_mismatch',
            ),
        ):
            with self.subTest(mutation=mutation):
                _, _, _, session, proof = (
                    self._same_day_rearm_claim_fixture()
                )
                marker_rows = session.marker_evidence_rows
                if mutation == 'proof_chronology':
                    proof_run = next(
                        row for row in session.lineage_rows if row.id == 590
                    )
                    source_run = next(
                        row for row in session.lineage_rows if row.id == 592
                    )
                    proof_run.started_at = source_run.started_at + timedelta(
                        seconds=1
                    )
                elif mutation == 'proof_source_id':
                    proof_signal = next(
                        row
                        for row in marker_rows
                        if row.pipeline_run_id == 590
                        and row.stage_id == 'signal_decider'
                    )
                    proof_signal.source_run_ids = {
                        'agent_feedback_run_id': 'mutated-cycle'
                    }
                else:
                    source_signal = next(
                        row
                        for row in marker_rows
                        if row.pipeline_run_id == 592
                        and row.stage_id == 'signal_decider'
                    )
                    source_signal.details['result_summary'][
                        'target_trade_date'
                    ] = '2026-10-29'
                context = MagicMock()
                context.__enter__.return_value = session
                with patch(
                    'app.eod.daily_pipeline_state.session_scope',
                    return_value=context,
                ):
                    result = (
                        DailyPipelineRunStateService()
                        .claim_same_day_intraday_watch_rearm_resume(
                            pipeline_run_id=593,
                            market_date='2026-10-27',
                            reused_eod_collection_run_id=581,
                            target_trade_date='2026-10-28',
                            expected_summary_cutoff_proof=proof,
                        )
                    )
                self.assertFalse(result['claimed'], result)
                self.assertEqual(result['reason'], expected_reason)
                session.flush.assert_not_called()

    def test_authoritative_decider_completed_evidence_allows_capped_role_subsets(
        self,
    ) -> None:
        parent_symbols = [f'{100000 + index:06d}' for index in range(120)]
        completed_symbols = parent_symbols[:115]
        roles = ['short-term-decider', 'signal-decider']
        role_symbol_scopes = {
            roles[0]: completed_symbols[:80],
            roles[1]: completed_symbols[20:100],
        }
        cutoff = datetime(2026, 9, 20, 0, 55, tzinfo=timezone.utc)
        source_run_id = 'agent_feedback_summaries-rerun'
        decider_run_id = 'signal_decider-rerun'
        child_run = SimpleNamespace(
            run_id=decider_run_id,
            market_date=date(2026, 9, 18),
            status='completed',
            completed_at=cutoff + timedelta(minutes=5),
            failed_count=0,
            symbols_total=160,
            completed_count=160,
            request_payload={
                'market_date': '2026-09-18',
                'request_run_id': decider_run_id,
                'mode': 'daily_retry_cycle_role_sequenced',
                'roles': roles,
                'requested_symbols': completed_symbols,
                'feedback_scope_mode': 'decider_scope_fast_path',
                'admission_scope_by_role': {
                    role: completed_symbols for role in roles
                },
                'role_symbol_scopes': role_symbol_scopes,
                'role_symbol_counts': {
                    role: len(role_symbol_scopes[role]) for role in roles
                },
                'fs_div': 'CFS',
                'reuse_existing': True,
                'force_recompute': False,
                'knowledge_cutoff_at': cutoff.isoformat(),
                'admission_knowledge_cutoff_at': cutoff.isoformat(),
                'requested_knowledge_cutoff_at': cutoff.isoformat(),
                'scope_authority': {
                    'schema_version': 'agent_feedback_scope_authority_v1',
                    'kind': (
                        'completed_summary_intersection_authoritative_cohort'
                    ),
                    'source_summary_run_id': source_run_id,
                    'source_feedback_scope_mode': (
                        'broad_feedback_universe'
                    ),
                    'decision_feedback_scope_mode': (
                        'decider_scope_fast_path'
                    ),
                    'knowledge_cutoff_at': cutoff.isoformat(),
                    'fs_div': 'CFS',
                    'completed_symbols': completed_symbols,
                    'admitted_symbol_count': len(parent_symbols),
                    'admitted_symbols_sha256': hashlib.sha256(
                        '\n'.join(sorted(parent_symbols)).encode('utf-8')
                    ).hexdigest(),
                },
            },
        )
        completed_summary_proof = {
            'ready': True,
            'reason': 'latest_completed_summary_cutoff',
            'knowledge_cutoff_at': cutoff.isoformat(),
            'source_run_id': source_run_id,
            'feedback_scope_mode': 'broad_feedback_universe',
            'requested_symbols': completed_symbols,
            'requested_symbol_count': len(completed_symbols),
            'completed_scope_proof': {
                'schema_version': 'completed_summarizer_intersection_v1',
                'completed_symbols': completed_symbols,
                'admitted_symbols': parent_symbols,
            },
        }

        error = terminal_feedback_completed_run_evidence_error(
            child_run=child_run,
            child_run_id=decider_run_id,
            stage_id='signal_decider',
            market_date=date(2026, 9, 18),
            metadata={
                'symbols': parent_symbols,
                'fs_div': 'CFS',
                'reuse_existing_feedback': True,
                'force': False,
            },
            knowledge_cutoff_at=cutoff,
            completed_summary_proof=completed_summary_proof,
            expected_summary_source_run_id=source_run_id,
        )

        self.assertIsNone(error)

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

    def test_converge_optional_context_partial_completes_with_warning_metadata(
        self,
    ) -> None:
        now = datetime(2026, 9, 14, 7, 39, 11, tzinfo=timezone.utc)
        run = SimpleNamespace(
            id=398,
            status='interrupted',
            error_message='Core restarted.',
            completed_at=now - timedelta(minutes=1),
            updated_at=now - timedelta(minutes=1),
            run_metadata={
                'resolved_stages': [
                    'technical_snapshots',
                    'macro_context_refresh',
                ],
                'resumable': True,
            },
        )
        stages = [
            SimpleNamespace(
                stage_id='technical_snapshots',
                status='completed',
                processed=100,
                total=100,
                succeeded=100,
                failed=0,
                remaining=0,
            ),
            SimpleNamespace(
                stage_id='macro_context_refresh',
                status='unavailable',
                processed=3,
                total=3,
                succeeded=2,
                failed=1,
                remaining=0,
                details={
                    'result_summary': {
                        'triggered': True,
                        'status': 'unavailable',
                        'remaining_retryable_events': 0,
                    },
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
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
        ):
            result = (
                DailyPipelineRunStateService()
                .converge_terminal_stages_run(pipeline_run_id=398)
            )

        self.assertTrue(result['updated'])
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(run.status, 'completed')
        self.assertEqual(run.run_metadata['quality_status'], 'degraded')
        self.assertEqual(
            run.run_metadata['warning_stage_ids'],
            ['macro_context_refresh'],
        )
        self.assertEqual(run.run_metadata['warning_count'], 1)
        self.assertEqual(
            run.run_metadata['stage_statuses']['macro_context_refresh'],
            'partial',
        )

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
            processed=4,
            total=4,
            succeeded=3,
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
                        'valuation_snapshot',
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
                        'valuation_snapshot': {
                            'status': 'completed',
                            'rows_persisted': 10,
                            'errors': [],
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
            run_metadata={'daily_pipeline_run_id': 387},
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
        self.assertEqual(
            eod_run.run_metadata['daily_pipeline_run_id'],
            387,
        )
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
        persisted_symbols_statement = session.execute.call_args_list[3].args[0]
        persisted_symbols_sql = str(persisted_symbols_statement).lower()
        self.assertIn(
            'daily_price_bars.repair_origin_eod_run_id',
            persisted_symbols_sql,
        )
        self.assertIn(
            'daily_price_bars.source',
            persisted_symbols_sql,
        )
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
            {'source_key': 10_003, 'trade_key': 20_260_914},
        )

    def test_startup_interrupt_uses_exact_reverse_eod_link_before_first_checkpoint(
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
                'entrypoint': 'daily_pipeline_stage_service.run',
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
            run_metadata={
                'daily_pipeline_run_id': 387,
                'requested_symbols': ['005930'],
            },
        )
        runs_result = MagicMock()
        runs_result.scalars.return_value = [run]
        stages_result = MagicMock()
        stages_result.scalars.return_value = [stage]
        metadata_candidates_result = MagicMock()
        metadata_candidates_result.scalars.return_value = [eod_run]
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
        reverse_link_statement = session.execute.call_args_list[3].args[0]
        self.assertIn(
            'daily_pipeline_run_id',
            reverse_link_statement.compile().params.values(),
        )
        self.assertEqual(session.execute.call_count, 5)

    def test_startup_interrupt_never_infers_eod_owner_from_date_and_trigger(
        self,
    ) -> None:
        now = datetime(2026, 9, 14, 7, 34, 11, tzinfo=timezone.utc)
        run = SimpleNamespace(
            id=388,
            market_date=date(2026, 9, 14),
            trigger='internal_ops_cli_stage',
            status='running',
            error_message=None,
            completed_at=None,
            updated_at=now,
            run_metadata={
                'entrypoint': 'daily_pipeline_stage_service.run',
                'resolved_stages': ['price_collection'],
            },
        )
        stage = SimpleNamespace(
            stage_id='price_collection',
            market_date=date(2026, 9, 14),
            status='running',
            processed=0,
            total=1,
            remaining=1,
            error_message=None,
            current_step='price_collection_started',
            current_activity='Starting prices',
            completed_at=None,
            updated_at=now,
            source_run_ids={},
            details={},
        )
        runs_result = MagicMock()
        runs_result.scalars.return_value = [run]
        stages_result = MagicMock()
        stages_result.scalars.return_value = [stage]
        lock_result = MagicMock()
        lock_result.scalar.return_value = True
        reverse_candidates_result = MagicMock()
        reverse_candidates_result.scalars.return_value = []
        session = MagicMock()
        session.execute.side_effect = [
            runs_result,
            stages_result,
            lock_result,
            reverse_candidates_result,
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

        self.assertEqual(result['run_ids'], [388])
        self.assertEqual(result['interrupted_eod_run_ids'], [])
        self.assertEqual(result['augmented_eod_run_ids'], [])
        self.assertEqual(session.execute.call_count, 4)
        reverse_link_statement = session.execute.call_args_list[3].args[0]
        self.assertIn(
            'daily_pipeline_run_id',
            reverse_link_statement.compile().params.values(),
        )
        session.get.assert_not_called()

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

    def test_list_prior_followups_uses_exact_lineage_and_bounded_order(
        self,
    ) -> None:
        prior_runs = [SimpleNamespace(id=579), SimpleNamespace(id=578)]
        runs_result = MagicMock()
        runs_result.scalars.return_value = prior_runs
        first_stages_result = MagicMock()
        first_stages_result.scalars.return_value = []
        second_stages_result = MagicMock()
        second_stages_result.scalars.return_value = []
        session = MagicMock()
        session.execute.side_effect = [
            runs_result,
            first_stages_result,
            second_stages_result,
        ]
        session_context = MagicMock()
        session_context.__enter__.return_value = session
        service = DailyPipelineRunStateService()

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch.object(
                service,
                '_serialize_run',
                side_effect=lambda run, _stages: {'id': run.id},
            ),
        ):
            result = service.list_prior_followup_runs_for_lineage(
                market_date='2026-10-23',
                reused_eod_collection_run_id=579,
                before_pipeline_run_id=580,
                limit=99,
            )

        self.assertEqual(result, [{'id': 579}, {'id': 578}])
        statement = session.execute.call_args_list[0].args[0]
        compiled = statement.compile()
        compiled_sql = str(compiled)
        self.assertIn('daily_pipeline_runs.market_date =', compiled_sql)
        self.assertIn('daily_pipeline_runs.trigger =', compiled_sql)
        self.assertIn('daily_pipeline_runs.id <', compiled_sql)
        self.assertIn('reused_eod_collection_run_id', compiled.params.values())
        self.assertIn(date(2026, 10, 23), compiled.params.values())
        self.assertIn('auto_eod_followup_resume', compiled.params.values())
        self.assertIn(579, compiled.params.values())
        self.assertIn(580, compiled.params.values())
        self.assertIn(8, compiled.params.values())
        self.assertIn(
            'ORDER BY daily_pipeline_runs.started_at DESC, '
            'daily_pipeline_runs.id DESC',
            compiled_sql,
        )

    def test_claim_terminal_continuation_accepts_ready_enrichment_warning(
        self,
    ) -> None:
        run, stages, eod_run = self._terminal_continuation_fixture()
        enrichment = stages[0]
        enrichment.processed = 4
        enrichment.total = 4
        enrichment.succeeded = 3
        enrichment.failed = 1
        enrichment.details = {
            'result_summary': {
                'status': 'completed',
                'point_in_time_recovery': True,
                'categories_requested': [
                    'sector_classification',
                    'investor_flow_symbol',
                    'short_selling',
                    'valuation_snapshot',
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
                    'valuation_snapshot': {
                        'status': 'completed',
                        'rows_persisted': 10,
                        'errors': [],
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
            feedback_scope_mode='decider_scope_fast_path',
            requested_symbols=None,
            fs_div='CFS',
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
                'feedback_scope_mode': 'decider_scope_fast_path',
                'requested_symbols': ['005930'],
                'requested_symbol_count': 1,
                'completed_scope_proof': self.completed_scope_proof,
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
        self.assertEqual(
            resumed['knowledge_cutoff_resolution'][
                'completed_scope_proof'
            ],
            self.completed_scope_proof,
        )
        self.resolve_terminal_cutoff.assert_called_once_with(
            market_date=date(2026, 9, 18),
            source_run_id='agent_feedback_summaries-prior',
            feedback_scope_mode='decider_scope_fast_path',
            requested_symbols=['005930'],
            fs_div='CFS',
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

        run.run_metadata['terminal_continuation_evidence'][
            'summary_proof'
        ].pop('completed_scope_proof')

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

        self.assertTrue(resumed['claimed'], resumed)
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
        self.assertEqual(
            run.run_metadata['terminal_continuation_evidence'][
                'summary_proof'
            ]['completed_scope_proof'],
            self.completed_scope_proof,
        )
        self.assertEqual(
            resumed['knowledge_cutoff_resolution'][
                'completed_scope_proof'
            ],
            self.completed_scope_proof,
        )

    def test_terminal_resume_uses_completed_rerun_summary_scope(self) -> None:
        claimed_at = datetime(2026, 9, 20, 9, 0, tzinfo=timezone.utc)
        interrupted_at = claimed_at + timedelta(minutes=1)
        old_source_run_id = 'agent_feedback_summaries-prior'
        rerun_source_run_id = 'agent_feedback_summaries-rerun'
        parent_symbols = ['005930', '000660']
        old_intersection = ['005930']
        rerun_intersection = ['000660']
        run, stages, eod_run = self._terminal_continuation_fixture()
        run.run_metadata['symbols'] = parent_symbols

        def resolve_cutoff(**kwargs):
            source_run_id = kwargs['source_run_id']
            if source_run_id == old_source_run_id:
                return {
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': self.terminal_cutoff,
                    'source_run_id': old_source_run_id,
                    'feedback_scope_mode': 'decider_scope_fast_path',
                    'requested_symbols': old_intersection,
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': old_intersection,
                        'admitted_symbols': parent_symbols,
                    },
                }
            self.assertEqual(source_run_id, rerun_source_run_id)
            self.assertEqual(
                kwargs['feedback_scope_mode'],
                'broad_feedback_universe',
            )
            self.assertEqual(kwargs['requested_symbols'], parent_symbols)
            return {
                'ready': True,
                'reason': 'latest_completed_summary_cutoff',
                'knowledge_cutoff_at': self.terminal_cutoff,
                'source_run_id': rerun_source_run_id,
                'feedback_scope_mode': 'broad_feedback_universe',
                'requested_symbols': rerun_intersection,
                'completed_scope_proof': {
                    'schema_version': (
                        'completed_summarizer_intersection_v1'
                    ),
                    'completed_symbols': rerun_intersection,
                    'admitted_symbols': parent_symbols,
                },
            }

        self.resolve_terminal_cutoff.side_effect = resolve_cutoff
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
        self.assertTrue(first['claimed'], first)
        run.run_metadata['terminal_continuation_evidence'][
            'summary_proof'
        ].pop('completed_scope_proof')

        summary_stage = stages[1]
        summary_stage.status = 'completed'
        summary_stage.processed = 1
        summary_stage.total = 1
        summary_stage.succeeded = 1
        summary_stage.failed = 0
        summary_stage.remaining = 0
        summary_stage.current_step = 'agent_feedback_summaries_completed'
        summary_stage.current_activity = 'Completed rerun summaries'
        summary_stage.updated_at = interrupted_at
        summary_stage.completed_at = interrupted_at
        summary_stage.error_message = None
        summary_stage.source_run_ids = {'run_id': rerun_source_run_id}
        summary_stage.details = {
            'result_summary': {
                'status': 'completed',
                'run_id': rerun_source_run_id,
                'feedback_scope_mode': 'broad_feedback_universe',
            }
        }
        run.status = 'interrupted'
        run.current_stage = 'agent_feedback_summaries'
        run.updated_at = interrupted_at
        run.completed_at = interrupted_at
        run.run_metadata.update(
            {
                'resumable': True,
                'interrupted_reason': 'Core restarted after summaries.',
                'interrupted_at': interrupted_at.isoformat(),
            }
        )
        resume_session = self._terminal_continuation_session(
            run=run,
            stages=stages,
            eod_run=eod_run,
        )
        summary_child = resume_session.feedback_child_rows[0]
        summary_roles = summary_child.request_payload['roles']
        summary_child.request_payload.update(
            {
                'requested_symbols': parent_symbols,
                'admission_scope_by_role': {
                    role: parent_symbols for role in summary_roles
                },
                'role_symbol_scopes': {
                    role: parent_symbols for role in summary_roles
                },
                'role_symbol_counts': {
                    role: len(parent_symbols) for role in summary_roles
                },
            }
        )
        summary_child.symbols_total = len(summary_roles) * len(
            parent_symbols
        )
        summary_child.completed_count = summary_child.symbols_total
        resume_context = MagicMock()
        resume_context.__enter__.return_value = resume_session
        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=resume_context,
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

        self.assertTrue(resumed['claimed'], resumed)
        self.assertEqual(resumed['resume_from_stage'], 'signal_decider')
        self.assertEqual(
            resumed['knowledge_cutoff_resolution']['source_run_id'],
            rerun_source_run_id,
        )
        self.assertEqual(
            resumed['knowledge_cutoff_resolution']['requested_symbols'],
            rerun_intersection,
        )
        self.assertEqual(
            run.run_metadata['terminal_continuation_evidence'][
                'summary_proof'
            ]['source_run_id'],
            old_source_run_id,
        )

        self.assertEqual(
            run.run_metadata['terminal_continuation_evidence'][
                'summary_proof'
            ]['completed_scope_proof']['completed_symbols'],
            old_intersection,
        )
        self.assertEqual(
            run.run_metadata['terminal_continuation_evidence'][
                'completed_prefix_summary_proof'
            ]['completed_scope_proof']['completed_symbols'],
            rerun_intersection,
        )

        signal_stage = stages[2]
        second_interrupted_at = interrupted_at + timedelta(minutes=32)
        signal_stage.status = 'interrupted'
        signal_stage.total = 1
        signal_stage.remaining = 1
        signal_stage.current_step = 'core_restart_interrupted'
        signal_stage.current_activity = 'Core restarted again.'
        signal_stage.updated_at = second_interrupted_at
        signal_stage.completed_at = second_interrupted_at
        signal_stage.error_message = 'Core restarted again.'
        signal_stage.details = {
            'resumable': True,
            'interrupted_reason': 'Core restarted again.',
        }
        run.status = 'interrupted'
        run.current_stage = 'signal_decider'
        run.updated_at = second_interrupted_at
        run.completed_at = second_interrupted_at
        run.run_metadata.update(
            {
                'resumable': True,
                'interrupted_reason': 'Core restarted again.',
                'interrupted_at': second_interrupted_at.isoformat(),
            }
        )
        second_resume_session = self._terminal_continuation_session(
            run=run,
            stages=stages,
            eod_run=eod_run,
        )
        second_summary_child = second_resume_session.feedback_child_rows[0]
        second_summary_child.request_payload = deepcopy(
            summary_child.request_payload
        )
        second_summary_child.symbols_total = summary_child.symbols_total
        second_summary_child.completed_count = (
            summary_child.completed_count
        )
        second_resume_context = MagicMock()
        second_resume_context.__enter__.return_value = (
            second_resume_session
        )
        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=second_resume_context,
            ),
            patch(
                'app.eod.daily_pipeline_state._utcnow',
                return_value=second_interrupted_at + timedelta(minutes=31),
            ),
        ):
            resumed_again = service.claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )

        self.assertTrue(resumed_again['claimed'], resumed_again)
        self.assertEqual(
            resumed_again['knowledge_cutoff_resolution']['source_run_id'],
            rerun_source_run_id,
        )
        self.assertEqual(
            resumed_again['knowledge_cutoff_resolution'][
                'requested_symbols'
            ],
            rerun_intersection,
        )
        self.assertEqual(
            run.run_metadata['terminal_continuation_evidence'][
                'summary_proof'
            ]['source_run_id'],
            old_source_run_id,
        )

        decider_run_id = 'signal_decider-rerun'
        third_interrupted_at = second_interrupted_at + timedelta(minutes=32)
        signal_stage.status = 'completed'
        signal_stage.processed = 2
        signal_stage.total = 2
        signal_stage.succeeded = 2
        signal_stage.failed = 0
        signal_stage.remaining = 0
        signal_stage.current_step = 'signal_decider_completed'
        signal_stage.current_activity = 'Completed rerun decider'
        signal_stage.updated_at = third_interrupted_at
        signal_stage.completed_at = third_interrupted_at
        signal_stage.error_message = None
        signal_stage.source_run_ids = {'run_id': decider_run_id}
        signal_stage.details = {
            'result_summary': {
                'status': 'completed',
                'run_id': decider_run_id,
            }
        }
        plan_stage = stages[3]
        plan_stage.status = 'interrupted'
        plan_stage.current_step = 'core_restart_interrupted'
        plan_stage.current_activity = 'Core restarted after decider.'
        plan_stage.updated_at = third_interrupted_at
        plan_stage.completed_at = third_interrupted_at
        plan_stage.error_message = 'Core restarted after decider.'
        plan_stage.details = {
            'resumable': True,
            'interrupted_reason': 'Core restarted after decider.',
        }
        run.status = 'interrupted'
        run.current_stage = 'next_open_execution_plan'
        run.updated_at = third_interrupted_at
        run.completed_at = third_interrupted_at
        run.run_metadata.update(
            {
                'resumable': True,
                'interrupted_reason': 'Core restarted after decider.',
                'interrupted_at': third_interrupted_at.isoformat(),
            }
        )
        third_resume_session = self._terminal_continuation_session(
            run=run,
            stages=stages,
            eod_run=eod_run,
        )
        third_summary_child, third_decider_child = (
            third_resume_session.feedback_child_rows
        )
        third_summary_child.request_payload = deepcopy(
            summary_child.request_payload
        )
        third_summary_child.symbols_total = summary_child.symbols_total
        third_summary_child.completed_count = summary_child.completed_count
        decider_roles = third_decider_child.request_payload['roles']
        admitted_symbols = sorted(parent_symbols)
        third_decider_child.request_payload.update(
            {
                'requested_symbols': rerun_intersection,
                'admission_scope_by_role': {
                    role: rerun_intersection for role in decider_roles
                },
                'role_symbol_scopes': {
                    role: rerun_intersection for role in decider_roles
                },
                'role_symbol_counts': {
                    role: len(rerun_intersection) for role in decider_roles
                },
                'scope_authority': {
                    'schema_version': 'agent_feedback_scope_authority_v1',
                    'kind': (
                        'completed_summary_intersection_authoritative_cohort'
                    ),
                    'source_summary_run_id': rerun_source_run_id,
                    'source_feedback_scope_mode': (
                        'broad_feedback_universe'
                    ),
                    'decision_feedback_scope_mode': (
                        'decider_scope_fast_path'
                    ),
                    'knowledge_cutoff_at': self.terminal_cutoff.isoformat(),
                    'fs_div': 'CFS',
                    'completed_symbols': rerun_intersection,
                    'admitted_symbol_count': len(admitted_symbols),
                    'admitted_symbols_sha256': hashlib.sha256(
                        '\n'.join(admitted_symbols).encode('utf-8')
                    ).hexdigest(),
                },
            }
        )
        third_decider_child.symbols_total = len(decider_roles) * len(
            rerun_intersection
        )
        third_decider_child.completed_count = (
            third_decider_child.symbols_total
        )

        def tampered_decider_session(
            *,
            authority_field: str,
            authority_value: str,
        ) -> MagicMock:
            tampered_session = self._terminal_continuation_session(
                run=run,
                stages=stages,
                eod_run=eod_run,
            )
            tampered_summary_child, tampered_decider_child = (
                tampered_session.feedback_child_rows
            )
            tampered_summary_child.request_payload = deepcopy(
                third_summary_child.request_payload
            )
            tampered_summary_child.symbols_total = (
                third_summary_child.symbols_total
            )
            tampered_summary_child.completed_count = (
                third_summary_child.completed_count
            )
            tampered_decider_child.request_payload = deepcopy(
                third_decider_child.request_payload
            )
            tampered_decider_child.request_payload['scope_authority'][
                authority_field
            ] = authority_value
            tampered_decider_child.symbols_total = (
                third_decider_child.symbols_total
            )
            tampered_decider_child.completed_count = (
                third_decider_child.completed_count
            )
            return tampered_session

        for authority_field, authority_value in (
            ('source_summary_run_id', old_source_run_id),
            (
                'knowledge_cutoff_at',
                (self.terminal_cutoff + timedelta(seconds=1)).isoformat(),
            ),
        ):
            with self.subTest(authority_field=authority_field):
                tampered_session = tampered_decider_session(
                    authority_field=authority_field,
                    authority_value=authority_value,
                )
                tampered_context = MagicMock()
                tampered_context.__enter__.return_value = tampered_session
                with (
                    patch(
                        'app.eod.daily_pipeline_state.session_scope',
                        return_value=tampered_context,
                    ),
                    patch(
                        'app.eod.daily_pipeline_state._utcnow',
                        return_value=(
                            third_interrupted_at + timedelta(minutes=31)
                        ),
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
                    'terminal_continuation_feedback_prefix_evidence_invalid',
                )
                self.assertEqual(
                    tampered_result['field'],
                    'scope_authority',
                )
                tampered_session.flush.assert_not_called()

        third_resume_context = MagicMock()
        third_resume_context.__enter__.return_value = third_resume_session
        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=third_resume_context,
            ),
            patch(
                'app.eod.daily_pipeline_state._utcnow',
                return_value=third_interrupted_at + timedelta(minutes=31),
            ),
        ):
            resumed_after_decider = service.claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )

        self.assertTrue(resumed_after_decider['claimed'], resumed_after_decider)
        self.assertEqual(
            resumed_after_decider['resume_from_stage'],
            'next_open_execution_plan',
        )
        self.assertEqual(
            resumed_after_decider['knowledge_cutoff_resolution'][
                'source_run_id'
            ],
            rerun_source_run_id,
        )
        self.assertEqual(
            run.run_metadata['terminal_continuation_evidence'][
                'summary_proof'
            ]['source_run_id'],
            old_source_run_id,
        )

    def test_claim_terminal_continuation_reclaims_exact_feedback_lease_loss(
        self,
    ) -> None:
        claimed_at = datetime(2026, 9, 20, 9, 0, tzinfo=timezone.utc)
        failed_at = claimed_at + timedelta(minutes=1)
        run, stages, eod_run = self._terminal_continuation_fixture()
        parent_symbols = ['005930', '000660']
        run.run_metadata['symbols'] = parent_symbols
        service = DailyPipelineRunStateService()

        def resolve_cutoff(**kwargs):
            admitted_symbols = kwargs['requested_symbols'] or ['005930']
            return {
                'ready': True,
                'reason': 'latest_completed_summary_cutoff',
                'knowledge_cutoff_at': self.terminal_cutoff,
                'source_run_id': kwargs['source_run_id'],
                'feedback_scope_mode': kwargs['feedback_scope_mode'],
                'requested_symbols': ['005930'],
                'completed_scope_proof': {
                    'schema_version': (
                        'completed_summarizer_intersection_v1'
                    ),
                    'completed_symbols': ['005930'],
                    'admitted_symbols': admitted_symbols,
                },
            }

        self.resolve_terminal_cutoff.side_effect = resolve_cutoff

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
                'feedback_scope_mode': 'broad_feedback_universe',
                'requested_symbols': ['005930'],
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
        lease_child.request_payload['scope_authority'] = {
            'schema_version': 'agent_feedback_scope_authority_v1',
            'kind': 'completed_summary_intersection_authoritative_cohort',
            'source_summary_run_id': (
                'agent_feedback_summaries-continued'
            ),
            'source_feedback_scope_mode': 'broad_feedback_universe',
            'decision_feedback_scope_mode': 'decider_scope_fast_path',
            'knowledge_cutoff_at': self.terminal_cutoff.isoformat(),
            'fs_div': 'CFS',
            'completed_symbols': ['005930'],
            'admitted_symbol_count': len(parent_symbols),
            'admitted_symbols_sha256': hashlib.sha256(
                '\n'.join(sorted(parent_symbols)).encode('utf-8')
            ).hexdigest(),
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
        summary_roles = summary_child.request_payload['roles']
        summary_child.request_payload.update(
            {
                'requested_symbols': parent_symbols,
                'admission_scope_by_role': {
                    role: parent_symbols for role in summary_roles
                },
                'role_symbol_scopes': {
                    role: parent_symbols for role in summary_roles
                },
                'role_symbol_counts': {
                    role: len(parent_symbols) for role in summary_roles
                },
            }
        )
        summary_child.symbols_total = len(summary_roles) * len(parent_symbols)
        summary_child.completed_count = summary_child.symbols_total

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
                summary_child_result,
                lease_child_result,
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
            resume_session.execute.call_args_list[6].args[0]
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

        second_interrupted_at = interrupted_at + timedelta(minutes=32)
        run.status = 'interrupted'
        run.current_stage = 'signal_decider'
        run.updated_at = second_interrupted_at
        run.completed_at = second_interrupted_at
        run.error_message = 'Core restarted again.'
        run.run_metadata.update(
            {
                'resumable': True,
                'interrupted_reason': 'Core restarted again.',
                'interrupted_at': second_interrupted_at.isoformat(),
            }
        )
        decider_stage.status = 'interrupted'
        decider_stage.current_step = 'core_restart_interrupted'
        decider_stage.updated_at = second_interrupted_at
        decider_stage.completed_at = second_interrupted_at
        decider_stage.error_message = 'Core restarted again.'
        decider_stage.details = {
            'resumable': True,
            'interrupted_reason': 'Core restarted again.',
        }
        second_restart_session = build_resume_session(lease_child)
        second_restart_context = MagicMock()
        second_restart_context.__enter__.return_value = (
            second_restart_session
        )
        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=second_restart_context,
            ),
            patch(
                'app.eod.daily_pipeline_state._utcnow',
                return_value=(
                    second_interrupted_at + timedelta(minutes=31)
                ),
            ),
        ):
            second_restart_result = service.claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )
        self.assertTrue(second_restart_result['claimed'])
        self.assertEqual(
            run.run_metadata['terminal_continuation_resume_count'],
            3,
        )
        self.assertEqual(
            run.run_metadata['terminal_continuation_resumed_from_status'],
            'interrupted',
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

        def resolve_cutoff(**kwargs):
            admitted_symbols = kwargs['requested_symbols'] or ['005930']
            return {
                'ready': True,
                'reason': 'latest_completed_summary_cutoff',
                'knowledge_cutoff_at': self.terminal_cutoff,
                'source_run_id': kwargs['source_run_id'],
                'feedback_scope_mode': kwargs['feedback_scope_mode'],
                'requested_symbols': ['005930'],
                'completed_scope_proof': {
                    'schema_version': (
                        'completed_summarizer_intersection_v1'
                    ),
                    'completed_symbols': ['005930'],
                    'admitted_symbols': admitted_symbols,
                },
            }

        self.resolve_terminal_cutoff.side_effect = resolve_cutoff

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
                    if stage.stage_id == 'agent_feedback_summaries':
                        stage.details['result_summary'].update(
                            {
                                'feedback_scope_mode': (
                                    'broad_feedback_universe'
                                ),
                                'requested_symbols': ['005930'],
                            }
                        )

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
                    self.assertTrue(between_result['claimed'], between_result)
                    self.assertEqual(
                        between_result['resume_from_stage'],
                        suffix_ids[1],
                    )
                    self.assertEqual(
                        between_result['knowledge_cutoff_resolution'][
                            'completed_scope_proof'
                        ],
                        self.completed_scope_proof,
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

                self.assertTrue(resumed['claimed'], resumed)
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
                    self.assertTrue(resumed_again['claimed'], resumed_again)
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

        def changed_completed_scope_proof(run, stages):
            run.run_metadata['terminal_continuation_evidence'][
                'summary_proof'
            ]['completed_scope_proof'] = {
                'schema_version': 'completed_summarizer_intersection_v1',
                'completed_symbols': ['000660'],
                'admitted_symbols': ['005930'],
            }

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
                'changed completed scope proof',
                changed_completed_scope_proof,
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

    def test_claim_cutoff_contract_repair_resumes_zero_work_in_place(
        self,
    ) -> None:
        interrupted_at = datetime(
            2026,
            9,
            20,
            9,
            0,
            tzinfo=timezone.utc,
        )
        stage_ids = [
            'signal_decider',
            'next_open_execution_plan',
            'signal_decider_evaluations',
        ]
        proof = {
            'source_run_id': 'feedback-cycle-summary',
            'feedback_scope_mode': 'decider_scope_fast_path',
            'requested_symbols': None,
        }
        run = SimpleNamespace(
            id=456,
            market_date=date(2026, 9, 18),
            trigger='auto_eod_followup_resume',
            status='interrupted',
            current_stage='signal_decider',
            started_at=interrupted_at - timedelta(minutes=1),
            updated_at=interrupted_at,
            completed_at=interrupted_at,
            error_message='Core restarted.',
            run_metadata={
                'entrypoint': 'daily_pipeline_stage_service.run',
                'reused_eod_collection_run_id': 546,
                'resolved_stages': stage_ids,
                'resume_from_stage': 'signal_decider',
                'markets': ['KOSPI'],
                'symbols': ['005930', '000660'],
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
                'summary_cutoff_contract_repair_version': 1,
                'summary_cutoff_contract_repair_proof': proof,
                'resumable': True,
                'interrupted_reason': 'Core restarted.',
                'interrupted_at': interrupted_at.isoformat(),
            },
        )
        stage = SimpleNamespace(
            id=1,
            pipeline_run_id=456,
            market_date=date(2026, 9, 18),
            stage_id='signal_decider',
            status='interrupted',
            processed=0,
            total=2,
            succeeded=0,
            failed=0,
            remaining=2,
            current_step='core_restart_interrupted',
            current_activity='Core restarted.',
            current_symbol=None,
            current_period=None,
            started_at=interrupted_at - timedelta(seconds=10),
            updated_at=interrupted_at,
            completed_at=interrupted_at,
            error_message='Core restarted.',
            source_run_ids={},
            details={
                'resumable': True,
                'interrupted_reason': 'Core restarted.',
            },
        )
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        lineage_result = MagicMock()
        lineage_result.scalars.return_value = [
            SimpleNamespace(id=run_id)
            for run_id in (451, 452, 453, 454, 455, 456)
        ]
        active_result = MagicMock()
        active_result.scalars.return_value = []
        stages_result = MagicMock()
        stages_result.scalars.return_value = [stage]
        session = MagicMock()
        session.execute.side_effect = [
            MagicMock(),
            run_result,
            lineage_result,
            active_result,
            stages_result,
        ]
        session.get.return_value = SimpleNamespace(
            id=546,
            market='KRX',
            source='pykrx',
            trade_date=date(2026, 9, 18),
            run_metadata={
                'stage_progress': {
                    'agent_feedback_summaries': (
                        _completed_feedback_summary_stage(proof)
                    )
                }
            },
        )
        session_context = MagicMock()
        session_context.__enter__.return_value = session
        self.resolve_terminal_cutoff.return_value = {
            'ready': True,
            'reason': 'latest_completed_summary_cutoff',
            'knowledge_cutoff_at': self.terminal_cutoff,
            'source_run_id': 'feedback-cycle-summary',
            'feedback_scope_mode': 'decider_scope_fast_path',
            'requested_symbols': ['005930'],
            'completed_scope_proof': self.completed_scope_proof,
        }
        resumed_at = interrupted_at + timedelta(hours=1)

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch(
                'app.eod.daily_pipeline_state._utcnow',
                return_value=resumed_at,
            ),
        ):
            result = (
                DailyPipelineRunStateService()
                .claim_summary_cutoff_contract_repair_resume(
                    pipeline_run_id=456,
                    market_date='2026-09-18',
                    reused_eod_collection_run_id=546,
                )
            )

        self.assertTrue(result['claimed'])
        self.assertTrue(result['resumed'])
        self.assertEqual(result['summary_cutoff_proof'], proof)
        self.assertEqual(
            result['knowledge_cutoff_resolution']['requested_symbols'],
            ['005930'],
        )
        self.resolve_terminal_cutoff.assert_called_once_with(
            market_date=date(2026, 9, 18),
            source_run_id='feedback-cycle-summary',
            feedback_scope_mode='decider_scope_fast_path',
            requested_symbols=None,
            fs_div='CFS',
            source_run_session=session,
        )
        self.assertEqual(run.status, 'running')
        self.assertEqual(run.current_stage, 'signal_decider')
        self.assertIsNone(run.completed_at)
        self.assertIsNone(run.error_message)
        self.assertEqual(
            run.run_metadata[
                'summary_cutoff_contract_repair_resume_count'
            ],
            1,
        )
        self.assertEqual(stage.status, 'pending')
        self.assertEqual(stage.current_step, 'cutoff_contract_repair_pending')
        self.assertEqual(stage.started_at, resumed_at)
        self.assertEqual(stage.source_run_ids, {})
        self.assertEqual(stage.details, {})

    def test_claim_cutoff_contract_repair_rejects_invalid_context_before_claim(
        self,
    ) -> None:
        interrupted_at = datetime(
            2026,
            9,
            20,
            9,
            0,
            tzinfo=timezone.utc,
        )
        stage_ids = [
            'signal_decider',
            'next_open_execution_plan',
            'signal_decider_evaluations',
        ]
        proof = {
            'source_run_id': 'feedback-cycle-summary',
            'feedback_scope_mode': 'decider_scope_fast_path',
            'requested_symbols': None,
        }
        run = SimpleNamespace(
            id=456,
            market_date=date(2026, 9, 18),
            trigger='auto_eod_followup_resume',
            status='interrupted',
            current_stage='signal_decider',
            started_at=interrupted_at - timedelta(minutes=1),
            updated_at=interrupted_at,
            completed_at=interrupted_at,
            error_message='Core restarted.',
            run_metadata={
                'entrypoint': 'daily_pipeline_stage_service.run',
                'reused_eod_collection_run_id': 546,
                'resolved_stages': stage_ids,
                'resume_from_stage': 'signal_decider',
                'markets': ['KOSPI'],
                'symbols': ['005930'],
                'force': False,
                'fs_div': 'INVALID',
                'symbol_limit': None,
                'symbol_offset': 0,
                'capture_limit': 500,
                'retry_cycles': None,
                'feedback_max_cycles': 3,
                'feedback_outer_batch_size': 20,
                'reuse_existing_feedback': True,
                'bypass_prerequisite_gate': False,
                'point_in_time_recovery': False,
                'summary_cutoff_contract_repair_version': 1,
                'summary_cutoff_contract_repair_proof': proof,
                'resumable': True,
                'interrupted_reason': 'Core restarted.',
                'interrupted_at': interrupted_at.isoformat(),
            },
        )
        stage = SimpleNamespace(
            id=1,
            pipeline_run_id=456,
            market_date=date(2026, 9, 18),
            stage_id='signal_decider',
            status='interrupted',
            processed=0,
            total=1,
            succeeded=0,
            failed=0,
            remaining=1,
            current_step='core_restart_interrupted',
            current_activity='Core restarted.',
            current_symbol=None,
            current_period=None,
            started_at=interrupted_at - timedelta(seconds=10),
            updated_at=interrupted_at,
            completed_at=interrupted_at,
            error_message='Core restarted.',
            source_run_ids={},
            details={'resumable': True},
        )
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        lineage_result = MagicMock()
        lineage_result.scalars.return_value = [
            SimpleNamespace(id=run_id)
            for run_id in (451, 452, 453, 454, 455, 456)
        ]
        active_result = MagicMock()
        active_result.scalars.return_value = []
        stages_result = MagicMock()
        stages_result.scalars.return_value = [stage]
        session = MagicMock()
        session.execute.side_effect = [
            MagicMock(),
            run_result,
            lineage_result,
            active_result,
            stages_result,
        ]
        eod_run = SimpleNamespace(
            id=546,
            market='KRX',
            source='pykrx',
            trade_date=date(2026, 9, 18),
            run_metadata={
                'stage_progress': {
                    'agent_feedback_summaries': (
                        _completed_feedback_summary_stage(proof)
                    )
                }
            },
        )
        session.get.return_value = eod_run
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ):
            result = (
                DailyPipelineRunStateService()
                .claim_summary_cutoff_contract_repair_resume(
                    pipeline_run_id=456,
                    market_date='2026-09-18',
                    reused_eod_collection_run_id=546,
                )
            )

        self.assertFalse(result['claimed'])
        self.assertEqual(
            result['reason'],
            'cutoff_contract_repair_execution_context_invalid',
        )
        self.assertEqual(result['field'], 'fs_div')
        self.assertEqual(run.status, 'interrupted')
        self.assertNotIn(
            'summary_cutoff_contract_repair_resume_count',
            run.run_metadata,
        )

        run.run_metadata['fs_div'] = 'CFS'
        mismatched_proof = {
            **proof,
            'source_run_id': 'different-feedback-summary',
        }
        eod_run.run_metadata['stage_progress'][
            'agent_feedback_summaries'
        ] = _completed_feedback_summary_stage(mismatched_proof)
        session.execute.side_effect = [
            MagicMock(),
            run_result,
            lineage_result,
            active_result,
            stages_result,
        ]
        self.resolve_terminal_cutoff.reset_mock()
        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ):
            proof_rejected = (
                DailyPipelineRunStateService()
                .claim_summary_cutoff_contract_repair_resume(
                    pipeline_run_id=456,
                    market_date='2026-09-18',
                    reused_eod_collection_run_id=546,
                )
            )

        self.assertFalse(proof_rejected['claimed'])
        self.assertEqual(
            proof_rejected['reason'],
            'eod_summary_cutoff_proof_mismatch',
        )
        self.assertEqual(run.status, 'interrupted')
        self.assertNotIn(
            'summary_cutoff_contract_repair_resume_count',
            run.run_metadata,
        )
        self.resolve_terminal_cutoff.assert_not_called()

        eod_run.run_metadata['stage_progress'][
            'agent_feedback_summaries'
        ] = _completed_feedback_summary_stage(proof)
        session.execute.side_effect = [
            MagicMock(),
            run_result,
            lineage_result,
            active_result,
            stages_result,
        ]
        self.resolve_terminal_cutoff.return_value = {
            'ready': False,
            'reason': 'summary_run_missing',
            'knowledge_cutoff_at': None,
            'source_run_id': 'feedback-cycle-summary',
            'feedback_scope_mode': 'decider_scope_fast_path',
            'requested_symbols': None,
        }
        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ):
            source_rejected = (
                DailyPipelineRunStateService()
                .claim_summary_cutoff_contract_repair_resume(
                    pipeline_run_id=456,
                    market_date='2026-09-18',
                    reused_eod_collection_run_id=546,
                )
            )

        self.assertFalse(source_rejected['claimed'])
        self.assertEqual(
            source_rejected['reason'],
            'summary_knowledge_cutoff_unavailable',
        )
        self.assertEqual(run.status, 'interrupted')
        self.assertNotIn(
            'summary_cutoff_contract_repair_resume_count',
            run.run_metadata,
        )
        self.resolve_terminal_cutoff.assert_called_once_with(
            market_date=date(2026, 9, 18),
            source_run_id='feedback-cycle-summary',
            feedback_scope_mode='decider_scope_fast_path',
            requested_symbols=None,
            fs_div='CFS',
            source_run_session=session,
        )

        session.execute.side_effect = [
            MagicMock(),
            run_result,
            lineage_result,
            active_result,
            stages_result,
        ]
        self.resolve_terminal_cutoff.reset_mock()
        self.resolve_terminal_cutoff.return_value = {
            'ready': True,
            'reason': 'latest_completed_summary_cutoff',
            'knowledge_cutoff_at': self.terminal_cutoff.isoformat(),
            'source_run_id': 'feedback-cycle-summary',
            'feedback_scope_mode': 'decider_scope_fast_path',
            'requested_symbols': ['005930'],
        }
        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ):
            malformed_cutoff_rejected = (
                DailyPipelineRunStateService()
                .claim_summary_cutoff_contract_repair_resume(
                    pipeline_run_id=456,
                    market_date='2026-09-18',
                    reused_eod_collection_run_id=546,
                )
            )

        self.assertFalse(malformed_cutoff_rejected['claimed'])
        self.assertEqual(
            malformed_cutoff_rejected['reason'],
            'summary_knowledge_cutoff_proof_mismatch',
        )
        self.assertEqual(run.status, 'interrupted')
        self.assertNotIn(
            'summary_cutoff_contract_repair_resume_count',
            run.run_metadata,
        )

    def test_cutoff_contract_repair_execution_context_is_fail_closed(
        self,
    ) -> None:
        valid = {
            'entrypoint': 'daily_pipeline_stage_service.run',
            'reused_eod_collection_run_id': 546,
            'resume_from_stage': 'signal_decider',
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
            'point_in_time_recovery': False,
        }
        cases = {
            'entrypoint': {'entrypoint': 'other'},
            'reused_eod_collection_run_id': {
                'reused_eod_collection_run_id': '546'
            },
            'resume_from_stage': {'resume_from_stage': 'next_open_execution_plan'},
            'symbols': {'symbols': ['5930']},
            'markets': {'markets': []},
            'force': {'force': 'false'},
            'bypass_prerequisite_gate': {
                'bypass_prerequisite_gate': 0
            },
            'fs_div': {'fs_div': 'cfs'},
            'symbol_limit': {'symbol_limit': 0},
            'symbol_offset': {'symbol_offset': -1},
            'capture_limit': {'capture_limit': 0},
            'retry_cycles': {'retry_cycles': 0},
            'feedback_max_cycles': {'feedback_max_cycles': 0},
            'feedback_outer_batch_size': {
                'feedback_outer_batch_size': 0
            },
            'reuse_existing_feedback': {
                'reuse_existing_feedback': 'true'
            },
            'point_in_time_recovery': {'point_in_time_recovery': None},
        }

        self.assertIsNone(
            _summary_cutoff_contract_repair_execution_context_error(
                valid,
                reused_eod_collection_run_id=546,
            )
        )
        for expected_field, replacement in cases.items():
            with self.subTest(field=expected_field):
                metadata = {**valid, **replacement}
                self.assertEqual(
                    _summary_cutoff_contract_repair_execution_context_error(
                        metadata,
                        reused_eod_collection_run_id=546,
                    ),
                    expected_field,
                )

        for missing_field in (
            'symbol_limit',
            'symbol_offset',
            'capture_limit',
            'retry_cycles',
            'feedback_max_cycles',
            'feedback_outer_batch_size',
        ):
            with self.subTest(missing_field=missing_field):
                metadata = dict(valid)
                metadata.pop(missing_field)
                self.assertEqual(
                    _summary_cutoff_contract_repair_execution_context_error(
                        metadata,
                        reused_eod_collection_run_id=546,
                    ),
                    missing_field,
                )

    def test_claim_terminal_continuation_recovers_summary_source_from_result(
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

        self.assertTrue(result['claimed'])
        self.assertEqual(run.status, 'running')
        self.resolve_terminal_cutoff.assert_called_once_with(
            market_date=date(2026, 9, 18),
            source_run_id='agent_feedback_summaries-prior',
            feedback_scope_mode='decider_scope_fast_path',
            requested_symbols=None,
            fs_div='CFS',
            source_run_session=session,
        )

    def test_claim_terminal_continuation_rejects_disjoint_fast_summary_scope(
        self,
    ) -> None:
        run, stages, eod_run = self._terminal_continuation_fixture()
        session = self._terminal_continuation_session(
            run=run,
            stages=stages,
            eod_run=eod_run,
        )
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
                return_value={
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': self.terminal_cutoff,
                    'source_run_id': 'agent_feedback_summaries-prior',
                    'feedback_scope_mode': 'decider_scope_fast_path',
                    'requested_symbols': ['000660'],
                },
            ),
        ):
            result = DailyPipelineRunStateService().claim_terminal_continuation(
                pipeline_run_id=456,
                market_date='2026-09-18',
                reused_eod_collection_run_id=546,
            )

        self.assertFalse(result['claimed'])
        self.assertEqual(result['reason'], 'summary_symbol_scope_mismatch')
        self.assertEqual(run.status, 'blocked')
        self.assertNotIn(
            'terminal_continuation_count',
            run.run_metadata,
        )

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

    def test_same_date_active_factor_run_blocks_non_overlapping_feedback_suffix(
        self,
    ) -> None:
        now = datetime(2026, 8, 29, tzinfo=timezone.utc)
        existing_run = SimpleNamespace(
            id=342,
            market_date=date(2026, 8, 28),
            trigger='auto_close_scheduler',
            status='running',
            current_stage='momentum_factors',
            started_at=now,
            updated_at=now,
            completed_at=None,
            error_message=None,
            run_metadata={'resolved_stages': ['momentum_factors']},
        )
        active_result = MagicMock()
        active_result.scalars.return_value = [existing_run]
        stages_result = MagicMock()
        stages_result.scalars.return_value = []
        session = MagicMock()
        session.execute.side_effect = [
            MagicMock(),
            active_result,
            stages_result,
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
            result = DailyPipelineRunStateService().start_run(
                market_date='2026-08-28',
                trigger='manual_api',
                requested_stages=['agent_feedback_summaries', 'signal_decider'],
                resolved_stages=['agent_feedback_summaries', 'signal_decider'],
            )

        self.assertTrue(result['deduplicated'])
        self.assertEqual(result['id'], 342)
        self.assertEqual(
            result['deduplication_reason'],
            'overlapping_run_already_active',
        )
        session.add.assert_not_called()

    def test_start_run_deduplicates_stale_activity_without_stealing_ownership(
        self,
    ) -> None:
        now = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
        last_activity_at = now - timedelta(minutes=31)
        stale_run = SimpleNamespace(
            id=341,
            market_date=date(2026, 8, 28),
            trigger='manual_api',
            status='running',
            current_stage='momentum_factors',
            started_at=last_activity_at - timedelta(minutes=5),
            updated_at=last_activity_at,
            completed_at=None,
            error_message=None,
            run_metadata={'resolved_stages': ['momentum_factors'], 'owner': 'slow-worker'},
        )
        stale_stage = SimpleNamespace(
            id=812,
            pipeline_run_id=341,
            market_date=date(2026, 8, 28),
            stage_id='momentum_factors',
            status='running',
            processed=2,
            total=10,
            succeeded=2,
            failed=0,
            remaining=8,
            current_step='dispatching',
            current_activity='Computing momentum factors',
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
        session = MagicMock()
        session.execute.side_effect = [
            MagicMock(),
            active_result,
            stages_result,
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

        self.assertTrue(result['deduplicated'])
        self.assertEqual(
            result['deduplication_reason'],
            'stale_run_requires_explicit_reconciliation',
        )
        self.assertEqual(result['id'], 341)
        self.assertEqual(stale_run.status, 'running')
        self.assertIsNone(stale_run.completed_at)
        self.assertEqual(stale_run.run_metadata['owner'], 'slow-worker')
        self.assertEqual(stale_stage.status, 'running')
        self.assertEqual(stale_stage.current_step, 'dispatching')
        session.add.assert_not_called()

    def test_provider_fence_proof_only_reconciles_stale_auto_lineage(
        self,
    ) -> None:
        now = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
        stale_at = now - timedelta(minutes=31)
        cases = (
            {
                'name': 'stale_auto_without_proof',
                'provider_fence_owned': False,
                'trigger': 'auto_close_scheduler',
                'updated_at': stale_at,
                'run_metadata': {
                    'entrypoint': (
                        'PYKRX_BACKFILL_SERVICE.'
                        'run_daily_pipeline_if_due'
                    ),
                    'mode': 'daily_auto',
                    'resolved_stages': ['momentum_factors'],
                },
            },
            {
                'name': 'stale_manual_with_proof',
                'provider_fence_owned': True,
                'trigger': 'manual_api',
                'updated_at': stale_at,
                'run_metadata': {
                    'entrypoint': 'daily_pipeline_stage_service.run',
                    'mode': 'manual',
                    'resolved_stages': ['momentum_factors'],
                },
            },
            {
                'name': 'fresh_auto_with_proof',
                'provider_fence_owned': True,
                'trigger': 'auto_close_scheduler',
                'updated_at': now,
                'run_metadata': {
                    'entrypoint': (
                        'PYKRX_BACKFILL_SERVICE.'
                        'run_daily_pipeline_if_due'
                    ),
                    'mode': 'daily_auto',
                    'resolved_stages': ['momentum_factors'],
                },
            },
        )

        for case in cases:
            with self.subTest(case['name']):
                active_run = SimpleNamespace(
                    id=341,
                    market_date=date(2026, 8, 28),
                    trigger=case['trigger'],
                    status='running',
                    current_stage='momentum_factors',
                    started_at=stale_at - timedelta(minutes=5),
                    updated_at=case['updated_at'],
                    completed_at=None,
                    error_message=None,
                    run_metadata=case['run_metadata'],
                )
                active_stage = SimpleNamespace(
                    id=812,
                    pipeline_run_id=341,
                    market_date=date(2026, 8, 28),
                    stage_id='momentum_factors',
                    status='running',
                    processed=2,
                    total=10,
                    succeeded=2,
                    failed=0,
                    remaining=8,
                    current_step='computing',
                    current_activity='Computing momentum factors',
                    current_symbol=None,
                    current_period=None,
                    started_at=stale_at - timedelta(minutes=5),
                    updated_at=case['updated_at'],
                    completed_at=None,
                    error_message=None,
                    source_run_ids={},
                    details={},
                )
                active_result = MagicMock()
                active_result.scalars.return_value = [active_run]
                stages_result = MagicMock()
                stages_result.scalars.return_value = [active_stage]
                session = MagicMock()
                session.execute.side_effect = [
                    MagicMock(),
                    active_result,
                    stages_result,
                ]
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
                    patch(
                        'app.eod.daily_pipeline_state.settings.'
                        'eod_auto_pykrx_stale_run_minutes',
                        30,
                    ),
                ):
                    result = DailyPipelineRunStateService().start_run(
                        market_date='2026-08-28',
                        trigger='auto_close_scheduler',
                        requested_stages=['momentum_factors'],
                        resolved_stages=['momentum_factors'],
                        provider_admission_fence_owned=case[
                            'provider_fence_owned'
                        ],
                    )

                self.assertTrue(result['deduplicated'])
                self.assertEqual(active_run.status, 'running')
                self.assertEqual(active_stage.status, 'running')
                session.add.assert_not_called()

    def test_provider_fence_reconciles_stale_auto_lineage_before_admission(
        self,
    ) -> None:
        now = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
        stale_at = now - timedelta(minutes=31)
        stale_run = SimpleNamespace(
            id=341,
            market_date=date(2026, 8, 28),
            trigger='auto_close_scheduler',
            status='running',
            current_stage='momentum_factors',
            started_at=stale_at - timedelta(minutes=5),
            updated_at=stale_at,
            completed_at=None,
            error_message=None,
            run_metadata={
                'entrypoint': (
                    'PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due'
                ),
                'mode': 'daily_auto',
                'resolved_stages': ['momentum_factors'],
            },
        )
        stale_stage = SimpleNamespace(
            id=812,
            pipeline_run_id=341,
            market_date=date(2026, 8, 28),
            stage_id='momentum_factors',
            status='running',
            processed=2,
            total=10,
            succeeded=2,
            failed=0,
            remaining=8,
            current_step='computing',
            current_activity='Computing momentum factors',
            current_symbol=None,
            current_period=None,
            started_at=stale_at - timedelta(minutes=5),
            updated_at=stale_at,
            completed_at=None,
            error_message=None,
            source_run_ids={},
            details={},
        )
        active_result = MagicMock()
        active_result.scalars.return_value = [stale_run]
        stages_result = MagicMock()
        stages_result.scalars.return_value = [stale_stage]
        session = MagicMock()
        session.execute.side_effect = [
            MagicMock(),
            active_result,
            stages_result,
        ]

        def assign_run_id(row) -> None:
            row.id = 999

        session.add.side_effect = assign_run_id
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
            patch(
                'app.eod.daily_pipeline_state.settings.'
                'eod_auto_pykrx_stale_run_minutes',
                30,
            ),
        ):
            result = DailyPipelineRunStateService().start_run(
                market_date='2026-08-28',
                trigger='auto_close_scheduler',
                requested_stages=['momentum_factors'],
                resolved_stages=['momentum_factors'],
                provider_admission_fence_owned=True,
            )

        self.assertNotIn('deduplicated', result)
        self.assertEqual(result['id'], 999)
        self.assertEqual(stale_run.status, 'interrupted')
        self.assertEqual(stale_run.completed_at, now)
        self.assertEqual(
            stale_run.run_metadata['interrupted_reason'],
            'stale_auto_run_reconciled_under_provider_fence',
        )
        self.assertTrue(stale_run.run_metadata['resumable'])
        self.assertEqual(stale_stage.status, 'interrupted')
        self.assertEqual(
            stale_stage.current_step,
            'provider_fence_reconciled',
        )

    def test_provider_fence_accepts_stale_auto_followup_recovery_lineage(
        self,
    ) -> None:
        stale_cutoff = datetime(
            2026,
            8,
            29,
            11,
            30,
            tzinfo=timezone.utc,
        )
        recovery_run = SimpleNamespace(
            trigger='auto_eod_followup_resume',
            started_at=stale_cutoff - timedelta(minutes=5),
            updated_at=stale_cutoff - timedelta(seconds=1),
            run_metadata={
                'entrypoint': 'daily_pipeline_stage_service.run',
                'reused_eod_collection_run_id': 77,
            },
        )

        self.assertTrue(
            DailyPipelineRunStateService.
            _is_stale_auto_run_owned_by_provider_fence(
                recovery_run,
                stale_cutoff=stale_cutoff,
            )
        )

        for trigger in ('manual_daily_api', 'internal_ops_cli'):
            with self.subTest(trigger=trigger):
                provider_run = SimpleNamespace(
                    trigger=trigger,
                    started_at=stale_cutoff - timedelta(minutes=5),
                    updated_at=stale_cutoff - timedelta(seconds=1),
                    run_metadata={
                        'entrypoint': (
                            'PYKRX_BACKFILL_SERVICE.'
                            'run_daily_pipeline_if_due'
                        ),
                        'mode': 'daily_auto',
                    },
                )
                self.assertTrue(
                    DailyPipelineRunStateService.
                    _is_stale_auto_run_owned_by_provider_fence(
                        provider_run,
                        stale_cutoff=stale_cutoff,
                    )
                )

    def test_provider_fence_blocks_all_when_any_same_date_owner_is_ineligible(
        self,
    ) -> None:
        now = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
        stale_at = now - timedelta(minutes=31)

        def active_run(
            run_id: int,
            *,
            updated_at: datetime,
            trigger: str = 'auto_close_scheduler',
        ) -> SimpleNamespace:
            return SimpleNamespace(
                id=run_id,
                market_date=date(2026, 8, 28),
                trigger=trigger,
                status='running',
                current_stage='momentum_factors',
                started_at=stale_at - timedelta(minutes=5),
                updated_at=updated_at,
                completed_at=None,
                error_message=None,
                run_metadata={
                    'entrypoint': (
                        'PYKRX_BACKFILL_SERVICE.'
                        'run_daily_pipeline_if_due'
                    ),
                    'mode': 'daily_auto',
                },
            )

        stale_auto = active_run(341, updated_at=stale_at)
        fresh_auto = active_run(342, updated_at=now)
        active_result = MagicMock()
        active_result.scalars.return_value = [stale_auto, fresh_auto]
        stages_result = MagicMock()
        stages_result.scalars.return_value = []
        session = MagicMock()
        session.execute.side_effect = [
            MagicMock(),
            active_result,
            stages_result,
        ]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
            patch(
                'app.eod.daily_pipeline_state.settings.'
                'eod_auto_pykrx_stale_run_minutes',
                30,
            ),
        ):
            result = DailyPipelineRunStateService().start_run(
                market_date='2026-08-28',
                trigger='auto_close_scheduler',
                requested_stages=['momentum_factors'],
                resolved_stages=['momentum_factors'],
                provider_admission_fence_owned=True,
            )

        self.assertTrue(result['deduplicated'])
        self.assertEqual(stale_auto.status, 'running')
        self.assertEqual(fresh_auto.status, 'running')
        session.add.assert_not_called()

    def test_heartbeat_stage_refreshes_owned_run_with_short_isolated_write(
        self,
    ) -> None:
        previous = datetime(2026, 8, 29, 11, 0, tzinfo=timezone.utc)
        now = datetime(2026, 8, 29, 11, 1, tzinfo=timezone.utc)
        run = SimpleNamespace(
            id=341,
            status='running',
            current_stage='momentum_factors',
            updated_at=previous,
        )
        stage = SimpleNamespace(
            pipeline_run_id=341,
            stage_id='momentum_factors',
            status='running',
            updated_at=previous,
            processed=17,
            total=2_678,
            current_activity='Computing momentum factors',
            details={'source': 'daily_factor_followup'},
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
            owned = DailyPipelineRunStateService().heartbeat_stage(
                pipeline_run_id=341,
                stage_id='momentum_factors',
                market_date='2026-08-28',
            )

        self.assertTrue(owned)
        session_scope_mock.assert_called_once_with(
            statement_timeout_ms=2_000,
            lock_timeout_ms=1_000,
            isolated_connection=True,
        )
        self.assertEqual(run.updated_at, now)
        self.assertEqual(stage.updated_at, now)
        self.assertEqual(run.current_stage, 'momentum_factors')
        self.assertEqual(stage.status, 'running')
        self.assertEqual(stage.processed, 17)
        self.assertEqual(stage.total, 2_678)
        self.assertEqual(
            stage.current_activity,
            'Computing momentum factors',
        )
        self.assertEqual(
            stage.details,
            {'source': 'daily_factor_followup'},
        )

    def test_heartbeat_stage_rejects_stage_or_market_date_mismatch(self) -> None:
        run = SimpleNamespace(
            id=341,
            status='running',
            current_stage='momentum_factors',
            updated_at=datetime(2026, 8, 29, 11, 0, tzinfo=timezone.utc),
            market_date=date(2026, 8, 28),
        )
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        stage_result = MagicMock()
        stage_result.scalar_one_or_none.return_value = None
        session = MagicMock()
        session.execute.side_effect = [run_result, stage_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ):
            owned = DailyPipelineRunStateService().heartbeat_stage(
                pipeline_run_id=341,
                stage_id='value_factors',
                market_date='2026-08-28',
            )

        self.assertFalse(owned)
        self.assertEqual(
            run.updated_at,
            datetime(2026, 8, 29, 11, 0, tzinfo=timezone.utc),
        )

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

    def test_finish_run_is_idempotent_for_same_terminal_status(self) -> None:
        completed_at = datetime(
            2026,
            8,
            29,
            11,
            0,
            tzinfo=timezone.utc,
        )
        completed_run = SimpleNamespace(
            id=341,
            market_date=date(2026, 8, 28),
            trigger='auto_close_scheduler',
            status='completed',
            current_stage='signal_decider',
            started_at=completed_at - timedelta(hours=1),
            updated_at=completed_at,
            completed_at=completed_at,
            error_message=None,
            run_metadata={'terminal': True},
        )
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = completed_run
        stage_result = MagicMock()
        stage_result.scalars.return_value = []
        session = MagicMock()
        session.execute.side_effect = [run_result, stage_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ):
            result = DailyPipelineRunStateService().finish_run(
                pipeline_run_id=341,
                status='completed',
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(completed_run.completed_at, completed_at)
        self.assertEqual(completed_run.run_metadata, {'terminal': True})
        session.flush.assert_not_called()

    def test_finish_run_interrupts_only_running_children_for_terminal_outcomes(self) -> None:
        now = datetime(2026, 10, 31, 2, 30, tzinfo=timezone.utc)
        started_at = now - timedelta(minutes=15)

        for terminal_status in ('failed', 'blocked', 'interrupted'):
            with self.subTest(terminal_status=terminal_status):
                run = SimpleNamespace(
                    id=447,
                    market_date=date(2026, 9, 18),
                    trigger='auto_close_scheduler',
                    status='running',
                    current_stage='signal_decider',
                    started_at=started_at,
                    updated_at=started_at,
                    completed_at=None,
                    error_message=None,
                    run_metadata={'resolved_stages': ['signal_decider']},
                )
                running_stage = SimpleNamespace(
                    id=2431,
                    pipeline_run_id=447,
                    market_date=date(2026, 9, 18),
                    stage_id='signal_decider',
                    status='running',
                    processed=3,
                    total=10,
                    succeeded=2,
                    failed=1,
                    remaining=99,
                    current_step='dispatching',
                    current_activity='Dispatching signal decisions',
                    current_symbol='005930',
                    current_period=None,
                    started_at=started_at,
                    updated_at=started_at,
                    completed_at=None,
                    error_message=None,
                    source_run_ids={'eod_run_id': 991},
                    details={'durable_progress': True},
                )
                terminal_stage = SimpleNamespace(
                    id=2430,
                    pipeline_run_id=447,
                    market_date=date(2026, 9, 18),
                    stage_id='agent_feedback_summaries',
                    status='completed',
                    processed=357,
                    total=360,
                    succeeded=357,
                    failed=3,
                    remaining=3,
                    current_step='completed',
                    current_activity='Completed with retry debt',
                    current_symbol=None,
                    current_period=None,
                    started_at=started_at,
                    updated_at=started_at,
                    completed_at=started_at,
                    error_message=None,
                    source_run_ids={'feedback_run_id': 88},
                    details={'retry_debt_symbols': ['105740', '028050', '241690']},
                )
                terminal_stage_snapshot = deepcopy(vars(terminal_stage))
                run_result = MagicMock()
                run_result.scalar_one_or_none.return_value = run
                stage_result = MagicMock()
                stage_result.scalars.return_value = [terminal_stage, running_stage]
                session = MagicMock()
                session.execute.side_effect = [run_result, stage_result]
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
                    result = DailyPipelineRunStateService().finish_run(
                        pipeline_run_id=447,
                        status=terminal_status,
                        error_message='parent terminalized',
                    )

                self.assertEqual(result['status'], terminal_status)
                self.assertEqual(running_stage.status, 'interrupted')
                self.assertEqual(running_stage.remaining, 7)
                self.assertEqual(running_stage.processed, 3)
                self.assertEqual(running_stage.total, 10)
                self.assertEqual(running_stage.succeeded, 2)
                self.assertEqual(running_stage.failed, 1)
                self.assertEqual(
                    running_stage.source_run_ids,
                    {'eod_run_id': 991},
                )
                self.assertEqual(running_stage.completed_at, now)
                self.assertEqual(running_stage.updated_at, now)
                self.assertEqual(
                    running_stage.details['terminal_parent_reconciliation'][
                        'parent_status'
                    ],
                    terminal_status,
                )
                self.assertEqual(
                    running_stage.details['terminal_parent_reconciliation'][
                        'previous_stage_status'
                    ],
                    'running',
                )
                self.assertEqual(vars(terminal_stage), terminal_stage_snapshot)
                session.flush.assert_called_once_with()

    def test_finish_run_same_terminal_replay_repairs_running_child_without_rewriting_parent(self) -> None:
        completed_at = datetime(2026, 8, 29, 11, 0, tzinfo=timezone.utc)
        replayed_at = completed_at + timedelta(days=60)
        failed_run = SimpleNamespace(
            id=217,
            market_date=date(2026, 7, 30),
            trigger='auto_close_scheduler',
            status='failed',
            current_stage='signal_decider',
            started_at=completed_at - timedelta(hours=1),
            updated_at=completed_at,
            completed_at=completed_at,
            error_message='original parent failure',
            run_metadata={'terminal': True},
        )
        running_stage = SimpleNamespace(
            id=1136,
            pipeline_run_id=217,
            market_date=date(2026, 7, 30),
            stage_id='signal_decider',
            status='running',
            processed=100,
            total=120,
            succeeded=100,
            failed=0,
            remaining=20,
            current_step='dispatching',
            current_activity='Dispatching signal decisions',
            current_symbol=None,
            current_period=None,
            started_at=completed_at - timedelta(minutes=10),
            updated_at=completed_at - timedelta(minutes=1),
            completed_at=None,
            error_message=None,
            source_run_ids={'feedback_run_id': 91},
            details={},
        )
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = failed_run
        stage_result = MagicMock()
        stage_result.scalars.return_value = [running_stage]
        session = MagicMock()
        session.execute.side_effect = [run_result, stage_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch(
                'app.eod.daily_pipeline_state._utcnow',
                return_value=replayed_at,
            ),
        ):
            result = DailyPipelineRunStateService().finish_run(
                pipeline_run_id=217,
                status='failed',
                error_message='new failure must not replace history',
                run_metadata={'new': True},
            )

        self.assertEqual(result['status'], 'failed')
        self.assertEqual(failed_run.completed_at, completed_at)
        self.assertEqual(failed_run.updated_at, completed_at)
        self.assertEqual(failed_run.error_message, 'original parent failure')
        self.assertEqual(failed_run.run_metadata, {'terminal': True})
        self.assertEqual(running_stage.status, 'interrupted')
        self.assertEqual(running_stage.completed_at, completed_at)
        self.assertEqual(
            running_stage.updated_at,
            completed_at - timedelta(minutes=1),
        )
        session.flush.assert_called_once_with()

    def test_exact_terminal_child_repair_preserves_historical_updated_at(self) -> None:
        engine = create_engine('sqlite+pysqlite:///:memory:')
        DailyPipelineRun.__table__.create(engine)
        DailyPipelineStageRun.__table__.create(engine)
        session_factory = sessionmaker(bind=engine, future=True)
        historical_updated_at = datetime(
            2026,
            9,
            18,
            13,
            45,
            tzinfo=timezone.utc,
        )
        parent_completed_at = historical_updated_at + timedelta(minutes=1)
        with session_factory.begin() as session:
            session.add(
                DailyPipelineRun(
                    id=447,
                    market_date=date(2026, 9, 18),
                    trigger='auto_close_scheduler',
                    status='blocked',
                    current_stage='signal_decider',
                    started_at=historical_updated_at - timedelta(hours=1),
                    updated_at=parent_completed_at,
                    completed_at=parent_completed_at,
                    error_message='parent blocked',
                    run_metadata={},
                )
            )
            session.add_all(
                [
                    DailyPipelineStageRun(
                        id=2431,
                        pipeline_run_id=447,
                        market_date=date(2026, 9, 18),
                        stage_id='signal_decider',
                        status='running',
                        processed=3,
                        total=10,
                        succeeded=2,
                        failed=1,
                        remaining=99,
                        current_step='dispatching',
                        current_activity='Dispatching signal decisions',
                        started_at=historical_updated_at - timedelta(minutes=5),
                        updated_at=historical_updated_at,
                        source_run_ids={'eod_run_id': 991},
                        details={'durable_progress': True},
                    ),
                    DailyPipelineStageRun(
                        id=2430,
                        pipeline_run_id=447,
                        market_date=date(2026, 9, 18),
                        stage_id='agent_feedback_summaries',
                        status='completed',
                        processed=357,
                        total=360,
                        succeeded=357,
                        failed=3,
                        remaining=3,
                        current_step='completed',
                        current_activity='Completed with retry debt',
                        started_at=historical_updated_at - timedelta(minutes=10),
                        updated_at=historical_updated_at,
                        completed_at=historical_updated_at,
                        source_run_ids={'feedback_run_id': 88},
                        details={'retry_debt_symbols': ['105740']},
                    ),
                ]
            )

        @contextmanager
        def _session_scope(**_kwargs):
            with session_factory.begin() as session:
                yield session

        try:
            with (
                patch(
                    'app.eod.daily_pipeline_state.session_scope',
                    _session_scope,
                ),
                patch(
                    'app.eod.daily_pipeline_state._utcnow',
                    return_value=datetime(
                        2026,
                        10,
                        31,
                        2,
                        30,
                        tzinfo=timezone.utc,
                    ),
                ),
            ):
                dry_run_result = (
                    DailyPipelineRunStateService()
                    .reconcile_terminal_parent_running_stages(
                        expected_orphans={
                            447: ('blocked', 'signal_decider'),
                        },
                        expected_orphan_count=1,
                        apply=False,
                        preserve_historical_updated_at=True,
                    )
                )
                with session_factory() as session:
                    dry_run_stage = session.get(DailyPipelineStageRun, 2431)
                    assert dry_run_stage is not None
                    self.assertEqual(dry_run_stage.status, 'running')
                    self.assertEqual(
                        dry_run_stage.updated_at.replace(
                            tzinfo=timezone.utc
                        ),
                        historical_updated_at,
                    )
                result = (
                    DailyPipelineRunStateService()
                    .reconcile_terminal_parent_running_stages(
                        expected_orphans={
                            447: ('blocked', 'signal_decider'),
                        },
                        expected_orphan_count=1,
                        apply=True,
                        preserve_historical_updated_at=True,
                    )
                )
                replay_result = (
                    DailyPipelineRunStateService()
                    .reconcile_terminal_parent_running_stages(
                        expected_orphans={
                            447: ('blocked', 'signal_decider'),
                        },
                        expected_orphan_count=1,
                        apply=True,
                        preserve_historical_updated_at=True,
                    )
                )

            with session_factory() as session:
                repaired = session.get(DailyPipelineStageRun, 2431)
                terminal = session.get(DailyPipelineStageRun, 2430)
                assert repaired is not None
                assert terminal is not None
                self.assertEqual(repaired.status, 'interrupted')
                self.assertEqual(repaired.remaining, 7)
                self.assertEqual(
                    repaired.updated_at.replace(tzinfo=timezone.utc),
                    historical_updated_at,
                )
                self.assertEqual(
                    repaired.completed_at.replace(tzinfo=timezone.utc),
                    parent_completed_at,
                )
                self.assertEqual(repaired.processed, 3)
                self.assertEqual(repaired.total, 10)
                self.assertEqual(repaired.succeeded, 2)
                self.assertEqual(repaired.failed, 1)
                self.assertEqual(repaired.source_run_ids, {'eod_run_id': 991})
                self.assertTrue(repaired.details['durable_progress'])
                self.assertEqual(
                    repaired.details['terminal_parent_reconciliation'][
                        'schema_version'
                    ],
                    'terminal_parent_running_stage_reconciliation_v1',
                )
                self.assertEqual(terminal.status, 'completed')
                self.assertEqual(terminal.details, {'retry_debt_symbols': ['105740']})
                self.assertEqual(
                    terminal.updated_at.replace(tzinfo=timezone.utc),
                    historical_updated_at,
                )
        finally:
            engine.dispose()

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['repaired_stage_run_ids'], [2431])
        self.assertEqual(dry_run_result['status'], 'ready')
        self.assertEqual(replay_result['status'], 'already_applied')

    def test_exact_terminal_child_repair_blocks_without_exact_scope_match(self) -> None:
        run = SimpleNamespace(
            id=447,
            status='blocked',
            completed_at=datetime(
                2026,
                9,
                18,
                13,
                46,
                tzinfo=timezone.utc,
            ),
        )
        stage = SimpleNamespace(
            id=2431,
            pipeline_run_id=447,
            stage_id='signal_decider',
            status='running',
            details={},
        )
        parent_result = MagicMock()
        parent_result.scalars.return_value = [run]
        orphan_result = MagicMock()
        orphan_result.all.return_value = [(run, stage)]
        stage_result = MagicMock()
        stage_result.scalars.return_value = [stage]
        session = MagicMock()
        session.execute.side_effect = [parent_result, orphan_result, stage_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ):
            result = (
                DailyPipelineRunStateService()
                .reconcile_terminal_parent_running_stages(
                    expected_orphans={
                        447: ('blocked', 'signal_decider'),
                        231: ('interrupted', 'signal_decider'),
                    },
                    expected_orphan_count=2,
                    apply=True,
                    preserve_historical_updated_at=True,
                )
            )

        self.assertEqual(result['status'], 'blocked')
        self.assertFalse(result['invariants']['parent_ids_match'])
        self.assertEqual(stage.status, 'running')
        session.flush.assert_not_called()

    def test_exact_terminal_child_repair_blocks_on_status_or_stage_drift(self) -> None:
        completed_at = datetime(2026, 9, 18, 13, 46, tzinfo=timezone.utc)
        for parent_status, stage_id in (
            ('failed', 'agent_feedback_summaries'),
            ('blocked', 'signal_decider'),
        ):
            with self.subTest(
                parent_status=parent_status,
                stage_id=stage_id,
            ):
                run = SimpleNamespace(
                    id=447,
                    status=parent_status,
                    completed_at=completed_at,
                )
                stage = SimpleNamespace(
                    id=2431,
                    pipeline_run_id=447,
                    stage_id=stage_id,
                    status='running',
                    details={},
                )
                parent_result = MagicMock()
                parent_result.scalars.return_value = [run]
                orphan_result = MagicMock()
                orphan_result.all.return_value = [(run, stage)]
                stage_result = MagicMock()
                stage_result.scalars.return_value = [stage]
                session = MagicMock()
                session.execute.side_effect = [
                    parent_result,
                    orphan_result,
                    stage_result,
                ]
                session_context = MagicMock()
                session_context.__enter__.return_value = session

                with patch(
                    'app.eod.daily_pipeline_state.session_scope',
                    return_value=session_context,
                ):
                    result = (
                        DailyPipelineRunStateService()
                        .reconcile_terminal_parent_running_stages(
                            expected_orphans={
                                447: (
                                    'blocked',
                                    'agent_feedback_summaries',
                                ),
                            },
                            expected_orphan_count=1,
                            apply=True,
                        )
                    )

                self.assertEqual(result['status'], 'blocked')
                self.assertFalse(
                    result['invariants']['orphan_manifest_matches']
                )
                self.assertEqual(stage.status, 'running')
                session.flush.assert_not_called()

    def test_exact_terminal_child_repair_rolls_back_if_postcondition_fails(self) -> None:
        now = datetime(2026, 9, 18, 13, 46, tzinfo=timezone.utc)
        run = SimpleNamespace(
            id=447,
            status='blocked',
            completed_at=now,
            error_message='parent blocked',
        )
        stage = SimpleNamespace(
            id=2431,
            pipeline_run_id=447,
            stage_id='agent_feedback_summaries',
            status='running',
            processed=3,
            total=10,
            succeeded=2,
            failed=1,
            remaining=99,
            current_step='dispatching',
            current_activity='Dispatching summaries',
            current_symbol=None,
            current_period=None,
            updated_at=now - timedelta(minutes=1),
            completed_at=None,
            error_message=None,
            source_run_ids={'feedback_run_id': 88},
            details={},
        )
        parent_result = MagicMock()
        parent_result.scalars.return_value = [run]
        orphan_result = MagicMock()
        orphan_result.all.return_value = [(run, stage)]
        stage_result = MagicMock()
        stage_result.scalars.return_value = [stage]
        postcondition_result = MagicMock()
        postcondition_result.all.return_value = [(run, stage)]
        session = MagicMock()
        session.execute.side_effect = [
            parent_result,
            orphan_result,
            stage_result,
            postcondition_result,
        ]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch(
            'app.eod.daily_pipeline_state.session_scope',
            return_value=session_context,
        ):
            result = (
                DailyPipelineRunStateService()
                .reconcile_terminal_parent_running_stages(
                    expected_orphans={
                        447: ('blocked', 'agent_feedback_summaries'),
                    },
                    expected_orphan_count=1,
                    apply=True,
                )
            )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(
            result['reason'],
            'terminal_child_repair_postcondition_failed',
        )
        session.flush.assert_called_once_with()
        session.rollback.assert_called_once_with()

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
