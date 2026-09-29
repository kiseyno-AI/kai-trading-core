import importlib.util
import os
import sys
import threading
import types
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import ANY, MagicMock, patch
from uuid import uuid4

from sqlalchemy import delete, select

from app.db import session_scope
from app.db_models import (
    AgentContextFeedback,
    AgentContextFeedbackFailure,
    AgentContextFeedbackRun,
)
from app.agent_context.retry_service import (
    AgentFeedbackRetryService,
    EXPECTED_ROLES,
    FeedbackRetryLeaseLostError,
    RetryCyclePlan,
    RoleSequenceRoleState,
    RoleSequenceSnapshot,
    _SameDayTerminalSymbols,
    _FeedbackRetryClaimLease,
    _feedback_retry_admission_lock_key,
    _feedback_retry_owner_token_hash,
    _claim_feedback_retry_run,
    _previous_run_matches_fs_div,
    _previous_run_matches_knowledge_cutoff,
    _refresh_feedback_retry_claim,
    resolve_latest_completed_summary_knowledge_cutoff,
)
from app.agent_context.roles import (
    DECISION_SUPPORT_AGENT_NAMES,
    SUMMARIZER_AGENT_NAMES,
)
from app.agent_context.orchestrator_helpers import build_upstream_feedback_bundle
from app.agent_context.prerequisite_service import FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER
from app.agent_context.reuse_service import (
    AgentContextReuseService,
    FeedbackReuseStaticInputs,
)
from app.agent_context.run_modes import DAILY_RETRY_CYCLE_MODE
from app.agent_context.runtime_state import RuntimeFeedbackRunState
from scripts import run_feedback_retry_cycles


def _decision(
    *,
    should_dispatch,
    latest_feedback=None,
    latest_feedback_id=None,
    latest_feedback_received_at=None,
    latest_decision_snapshot_id=None,
    latest_decision_snapshot_fingerprint=None,
    latest_decision_snapshot_observed_at=None,
    reason=None,
):
    return SimpleNamespace(
        should_dispatch=should_dispatch,
        latest_feedback=latest_feedback,
        latest_feedback_id=latest_feedback_id,
        latest_feedback_received_at=latest_feedback_received_at,
        latest_decision_snapshot_id=latest_decision_snapshot_id,
        latest_decision_snapshot_fingerprint=(
            latest_decision_snapshot_fingerprint
        ),
        latest_decision_snapshot_observed_at=(
            latest_decision_snapshot_observed_at
        ),
        reason=reason,
    )


def _feedback_prerequisites(*, status: str = 'ready', missing_stage_ids: list[str] | None = None) -> dict:
    missing_stage_ids = list(missing_stage_ids or [])
    ready = status == 'ready'
    return {
        'market_date': '2026-04-16',
        'fs_div': 'CFS',
        'status': status,
        'ready': ready,
        'required_stage_ids': list(FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER),
        'ready_stage_ids': list(FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER) if ready else [],
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


def _role_state(
    *,
    role: str,
    symbols: tuple[str, ...],
    completed: tuple[str, ...] = (),
    failed: tuple[str, ...] = (),
    reused: tuple[str, ...] = (),
    boundary_pending: tuple[str, ...] = (),
    retry_pending: tuple[str, ...] = (),
    boundary_status: str | None = None,
) -> RoleSequenceRoleState:
    resolved_boundary_status = boundary_status
    if resolved_boundary_status is None:
        if not symbols:
            resolved_boundary_status = 'completed'
        elif boundary_pending:
            resolved_boundary_status = 'incomplete' if (completed or failed) else 'pending'
        elif failed and completed:
            resolved_boundary_status = 'completed_with_failures'
        elif failed:
            resolved_boundary_status = 'failed'
        else:
            resolved_boundary_status = 'completed'
    return RoleSequenceRoleState(
        role=role,
        total_symbols=len(symbols),
        completed_symbols=completed,
        failed_symbols=failed,
        reused_symbols=reused,
        terminal_symbols=tuple(symbol for symbol in symbols if symbol in set(completed) | set(failed)),
        boundary_pending_symbols=boundary_pending,
        retry_pending_symbols=retry_pending,
        boundary_status=resolved_boundary_status,
    )


def _snapshot(
    *,
    market_date: str = '2026-04-16',
    symbols: tuple[str, ...],
    role_states: dict[str, RoleSequenceRoleState],
) -> RoleSequenceSnapshot:
    roles = tuple(role_states.keys())
    return RoleSequenceSnapshot(
        market_date=date.fromisoformat(market_date),
        symbols=symbols,
        roles=roles,
        role_states=role_states,
    )


class SnapshotSequence:
    def __init__(self, snapshots):
        self.snapshots = list(snapshots)
        self.calls = 0

    def __call__(self, **kwargs):
        self.calls += 1
        if self.calls <= len(self.snapshots):
            return self.snapshots[self.calls - 1]
        return self.snapshots[-1]


class RecordingRunStateService:
    def __init__(self):
        self.started = []
        self.checkpoints = []
        self.finished = []

    def find_latest_run(self, **kwargs):
        return None

    def start_run(self, **kwargs):
        self.started.append(kwargs)
        return {'ok': True, 'run_id': kwargs['run_id'], 'status': 'running'}

    def checkpoint_run(self, **kwargs):
        self.checkpoints.append(kwargs)
        return {'ok': True, 'run_id': kwargs['run_id'], 'status': kwargs['status']}

    def finish_run(self, **kwargs):
        self.finished.append(kwargs)
        return {'ok': True, 'run_id': kwargs['run_id'], 'status': kwargs['status']}


class FeedbackRetryCycleTests(unittest.TestCase):
    def test_resolves_completed_summary_cutoff_for_exact_source_and_symbol_scope(
        self,
    ):
        cutoff = datetime(2026, 4, 16, 6, 45, tzinfo=timezone.utc)
        source_run_id = 'feedback-cycle-2026-04-16-summary'
        source_run_session = object()
        exact_run = {
            'run_id': source_run_id,
            'market_date': date(2026, 4, 16),
            'status': 'completed',
            'symbols_total': 6,
            'completed_count': 6,
            'failed_count': 0,
            'completed_at': cutoff + timedelta(minutes=1),
            'request_payload': {
                'mode': DAILY_RETRY_CYCLE_MODE,
                'roles': [
                    'eod-summarizer',
                    'technical-summarizer',
                    'fundamental-summarizer',
                ],
                'requested_symbols': ['000660', '005930'],
                'feedback_scope_mode': 'broad_feedback_universe',
                'request_run_id': source_run_id,
                'child_run_lineage': {
                    'schema_version': 'agent_feedback_child_run_lineage_v1',
                    'parent_run_id': source_run_id,
                    'child_link_field': 'parent_run_id',
                },
                'admission_knowledge_cutoff_at': cutoff.isoformat(),
            },
        }

        with (
            patch(
                'app.agent_context.retry_service.'
                'agent_context_feedback_run_state_service.get_run',
                return_value=exact_run,
            ) as get_run,
            patch(
                'app.agent_context.retry_service.'
                'agent_context_feedback_run_state_service.find_latest_run',
            ) as find_latest_run,
        ):
            result = resolve_latest_completed_summary_knowledge_cutoff(
                market_date=date(2026, 4, 16),
                feedback_scope_mode='broad_feedback_universe',
                source_run_id=source_run_id,
                requested_symbols=['005930', '000660'],
                now=cutoff + timedelta(minutes=5),
                source_run_session=source_run_session,
            )

        self.assertTrue(result['ready'])
        self.assertEqual(result['knowledge_cutoff_at'], cutoff)
        self.assertEqual(result['source_run_id'], source_run_id)
        self.assertEqual(result['requested_symbols'], ['000660', '005930'])
        get_run.assert_called_once_with(
            run_id=source_run_id,
            session=source_run_session,
            for_update=True,
        )
        find_latest_run.assert_not_called()

    def test_exact_summary_cutoff_rejects_symbol_scope_mismatch(self):
        cutoff = datetime(2026, 4, 16, 6, 45, tzinfo=timezone.utc)
        source_run_id = 'feedback-cycle-2026-04-16-summary'
        exact_run = {
            'run_id': source_run_id,
            'market_date': date(2026, 4, 16),
            'status': 'completed',
            'symbols_total': 3,
            'completed_count': 3,
            'failed_count': 0,
            'completed_at': cutoff + timedelta(minutes=1),
            'request_payload': {
                'mode': DAILY_RETRY_CYCLE_MODE,
                'roles': list(EXPECTED_ROLES[:3]),
                'requested_symbols': ['005930'],
                'feedback_scope_mode': 'broad_feedback_universe',
                'request_run_id': source_run_id,
                'child_run_lineage': {
                    'schema_version': 'agent_feedback_child_run_lineage_v1',
                    'parent_run_id': source_run_id,
                    'child_link_field': 'parent_run_id',
                },
                'admission_knowledge_cutoff_at': cutoff.isoformat(),
            },
        }

        with patch(
            'app.agent_context.retry_service.'
            'agent_context_feedback_run_state_service.get_run',
            return_value=exact_run,
        ):
            result = resolve_latest_completed_summary_knowledge_cutoff(
                market_date=date(2026, 4, 16),
                feedback_scope_mode='broad_feedback_universe',
                source_run_id=source_run_id,
                requested_symbols=['000660'],
                now=cutoff + timedelta(minutes=5),
            )

        self.assertFalse(result['ready'])
        self.assertEqual(result['reason'], 'summary_run_symbol_scope_mismatch')
        self.assertEqual(result['source_run_id'], source_run_id)

    def test_exact_summary_cutoff_rejects_conflicting_self_lineage(self):
        cutoff = datetime(2026, 4, 16, 6, 45, tzinfo=timezone.utc)
        source_run_id = 'feedback-cycle-2026-04-16-summary'
        exact_run = {
            'run_id': source_run_id,
            'market_date': date(2026, 4, 16),
            'status': 'completed',
            'symbols_total': 3,
            'completed_count': 3,
            'failed_count': 0,
            'completed_at': cutoff + timedelta(minutes=1),
            'request_payload': {
                'mode': DAILY_RETRY_CYCLE_MODE,
                'roles': list(EXPECTED_ROLES[:3]),
                'requested_symbols': ['005930'],
                'feedback_scope_mode': 'broad_feedback_universe',
                'request_run_id': source_run_id,
                'child_run_lineage': {
                    'schema_version': 'agent_feedback_child_run_lineage_v1',
                    'parent_run_id': 'different-parent',
                    'child_link_field': 'parent_run_id',
                },
                'admission_knowledge_cutoff_at': cutoff.isoformat(),
            },
        }

        with patch(
            'app.agent_context.retry_service.'
            'agent_context_feedback_run_state_service.get_run',
            return_value=exact_run,
        ):
            result = resolve_latest_completed_summary_knowledge_cutoff(
                market_date=date(2026, 4, 16),
                feedback_scope_mode='broad_feedback_universe',
                source_run_id=source_run_id,
                requested_symbols=['005930'],
                now=cutoff + timedelta(minutes=5),
            )

        self.assertFalse(result['ready'])
        self.assertEqual(result['reason'], 'summary_run_lineage_mismatch')
        self.assertEqual(result['source_run_id'], source_run_id)

    def test_resolves_latest_completed_summary_cutoff_for_decider_recovery(self):
        cutoff = datetime(2026, 4, 16, 6, 45, tzinfo=timezone.utc)
        latest_run = {
            'run_id': 'feedback-cycle-2026-04-16-summary',
            'status': 'completed',
            'symbols_total': 30,
            'completed_count': 30,
            'failed_count': 0,
            'completed_at': cutoff + timedelta(minutes=1),
            'request_payload': {
                'roles': [
                    'eod-summarizer',
                    'technical-summarizer',
                    'fundamental-summarizer',
                ],
                'admission_knowledge_cutoff_at': cutoff.isoformat(),
            },
        }

        with patch(
            'app.agent_context.retry_service.'
            'agent_context_feedback_run_state_service.find_latest_run',
            return_value=latest_run,
        ) as find_latest_run:
            result = resolve_latest_completed_summary_knowledge_cutoff(
                market_date=date(2026, 4, 16),
                feedback_scope_mode='broad_feedback_universe',
                now=cutoff + timedelta(minutes=5),
            )

        self.assertTrue(result['ready'])
        self.assertEqual(result['knowledge_cutoff_at'], cutoff)
        self.assertEqual(result['source_run_id'], latest_run['run_id'])
        find_latest_run.assert_called_once_with(
            market_date=date(2026, 4, 16),
            requested_roles=[
                'eod-summarizer',
                'technical-summarizer',
                'fundamental-summarizer',
            ],
            request_modes=[DAILY_RETRY_CYCLE_MODE],
            feedback_scope_mode='broad_feedback_universe',
        )

    def test_rejects_unproven_summary_cutoff_for_decider_recovery(self):
        now = datetime(2026, 4, 16, 7, 0, tzinfo=timezone.utc)
        unsafe_runs = [
            None,
            {
                'run_id': 'failed-summary',
                'status': 'failed',
                'symbols_total': 30,
                'completed_count': 0,
                'failed_count': 30,
                'completed_at': now,
                'request_payload': {
                    'roles': list(EXPECTED_ROLES[:3]),
                    'admission_knowledge_cutoff_at': now.isoformat(),
                },
            },
            {
                'run_id': 'partial-summary',
                'status': 'completed',
                'symbols_total': 30,
                'completed_count': 29,
                'failed_count': 0,
                'completed_at': now,
                'request_payload': {
                    'roles': list(EXPECTED_ROLES[:3]),
                    'admission_knowledge_cutoff_at': now.isoformat(),
                },
            },
            {
                'run_id': 'naive-cutoff',
                'status': 'completed',
                'symbols_total': 30,
                'completed_count': 30,
                'failed_count': 0,
                'completed_at': now,
                'request_payload': {
                    'roles': list(EXPECTED_ROLES[:3]),
                    'admission_knowledge_cutoff_at': '2026-04-16T06:45:00',
                },
            },
            {
                'run_id': 'future-cutoff',
                'status': 'completed',
                'symbols_total': 30,
                'completed_count': 30,
                'failed_count': 0,
                'completed_at': now,
                'request_payload': {
                    'roles': list(EXPECTED_ROLES[:3]),
                    'admission_knowledge_cutoff_at': (
                        now + timedelta(seconds=1)
                    ).isoformat(),
                },
            },
        ]

        for latest_run in unsafe_runs:
            with self.subTest(run=latest_run and latest_run['run_id']):
                with patch(
                    'app.agent_context.retry_service.'
                    'agent_context_feedback_run_state_service.find_latest_run',
                    return_value=latest_run,
                ):
                    result = resolve_latest_completed_summary_knowledge_cutoff(
                        market_date=date(2026, 4, 16),
                        feedback_scope_mode='broad_feedback_universe',
                        now=now,
                    )

                self.assertFalse(result['ready'])
                self.assertIsNone(result['knowledge_cutoff_at'])

    def test_prior_run_resume_requires_exact_normalized_cutoff(self):
        previous_run = {
            'request_payload': {
                'admission_knowledge_cutoff_at': (
                    '2026-04-16T15:30:00+09:00'
                ),
            },
        }

        self.assertTrue(
            _previous_run_matches_knowledge_cutoff(
                previous_run,
                datetime(2026, 4, 16, 6, 30, tzinfo=timezone.utc),
            )
        )
        self.assertFalse(
            _previous_run_matches_knowledge_cutoff(
                previous_run,
                datetime(2026, 4, 16, 6, 29, tzinfo=timezone.utc),
            )
        )
        self.assertFalse(
            _previous_run_matches_knowledge_cutoff(
                {'request_payload': {}},
                datetime(2026, 4, 16, 6, 30, tzinfo=timezone.utc),
            )
        )

    def test_prior_run_resume_requires_exact_fs_div(self):
        previous_run = {'request_payload': {'fs_div': 'cfs'}}

        self.assertTrue(_previous_run_matches_fs_div(previous_run, 'CFS'))
        self.assertFalse(_previous_run_matches_fs_div(previous_run, 'OFS'))
        self.assertFalse(
            _previous_run_matches_fs_div({'request_payload': {}}, 'CFS')
        )

    def test_run_retry_cycles_discards_prior_resume_when_cutoff_differs(self):
        service = AgentFeedbackRetryService()
        run_state_service = RecordingRunStateService()
        run_state_service.find_latest_run = lambda **_: {
            'run_id': 'prior-retry-run',
            'request_payload': {
                'admission_knowledge_cutoff_at': '2026-04-16T06:29:00+00:00',
            },
        }

        class LeaseContext:
            def __enter__(self):
                return SimpleNamespace()

            def __exit__(self, exc_type, exc, traceback):
                return False

        effective_cutoff = datetime(2026, 4, 16, 6, 30, tzinfo=timezone.utc)
        with patch(
            'app.agent_context.retry_service.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
            return_value=_feedback_prerequisites(),
        ), patch.object(
            service, '_load_eligible_symbols', return_value=['005930'],
        ), patch(
            'app.agent_context.retry_service.resolve_external_news_knowledge_cutoff_for_symbols',
            return_value=(effective_cutoff, 'requested'),
        ), patch(
            'app.agent_context.retry_service.agent_context_feedback_run_state_service',
            run_state_service,
        ), patch(
            'app.agent_context.retry_service._claim_feedback_retry_run',
            return_value={'acquired': True, 'owner_token': 'owner-token'},
        ), patch(
            'app.agent_context.retry_service._feedback_retry_claim_lease',
            return_value=LeaseContext(),
        ), patch(
            'app.agent_context.retry_service._finish_feedback_retry_claim_if_running',
        ), patch.object(
            service,
            '_run_retry_cycles_admitted',
            return_value={'status': 'completed'},
        ) as run_admitted:
            service.run_retry_cycles(
                config=SimpleNamespace(
                    market_date=date.fromisoformat('2026-04-16'),
                    roles=('signal-decider',),
                    symbols=('005930',),
                    knowledge_cutoff_at='2026-04-16T15:30:00+09:00',
                    force_recompute=False,
                )
            )

        self.assertIsNone(run_admitted.call_args.kwargs['previous_run'])
        self.assertEqual(
            run_admitted.call_args.kwargs['effective_knowledge_cutoff_at'],
            effective_cutoff,
        )

    def test_feedback_retry_admission_claim_atomically_rejects_overlapping_scope(self):
        suffix = uuid4().hex[:10].upper()
        run_ids = [f'claim-one-{suffix}', f'claim-two-{suffix}']
        market_date = date.fromisoformat('2040-01-02')
        started_at = datetime.now(timezone.utc)
        barrier = threading.Barrier(2)

        def claim(run_id):
            barrier.wait(timeout=5)
            return _claim_feedback_retry_run(
                market_date=market_date,
                roles=('signal-decider',),
                requested_symbols=(f'A{suffix}', f'B{suffix}'),
                feedback_scope_mode='decider_scope_fast_path',
                run_id=run_id,
                started_at=started_at,
            )

        def cleanup():
            with session_scope() as session:
                session.execute(
                    delete(AgentContextFeedbackRun).where(
                        AgentContextFeedbackRun.run_id.in_(run_ids)
                    )
                )

        self.addCleanup(cleanup)

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(claim, run_ids))

        acquired = [result for result in results if result['acquired']]
        blocked = [result for result in results if not result['acquired']]
        self.assertEqual(len(acquired), 1)
        self.assertEqual(len(blocked), 1)
        self.assertEqual(
            blocked[0]['conflict_run']['run_id'],
            acquired[0]['run_id'],
        )
        self.assertTrue(blocked[0]['exact_scope'])

    def test_feedback_retry_admission_claim_blocks_partially_overlapping_scope(self):
        suffix = uuid4().hex[:10].upper()
        first_run_id = f'claim-multi-{suffix}'
        second_run_id = f'claim-overlap-{suffix}'
        disjoint_run_id = f'claim-disjoint-{suffix}'
        run_ids = [first_run_id, second_run_id, disjoint_run_id]
        market_date = date.fromisoformat('2040-01-03')
        started_at = datetime.now(timezone.utc)

        def cleanup():
            with session_scope() as session:
                session.execute(
                    delete(AgentContextFeedbackRun).where(
                        AgentContextFeedbackRun.run_id.in_(run_ids)
                    )
                )

        self.addCleanup(cleanup)

        first = _claim_feedback_retry_run(
            market_date=market_date,
            roles=('signal-decider',),
            requested_symbols=(f'A{suffix}', f'B{suffix}'),
            feedback_scope_mode='decider_scope_fast_path',
            run_id=first_run_id,
            started_at=started_at,
        )
        second = _claim_feedback_retry_run(
            market_date=market_date,
            roles=('signal-decider',),
            requested_symbols=(f'A{suffix}',),
            feedback_scope_mode='decider_scope_fast_path',
            run_id=second_run_id,
            started_at=started_at,
        )
        disjoint = _claim_feedback_retry_run(
            market_date=market_date,
            roles=('signal-decider',),
            requested_symbols=(f'C{suffix}',),
            feedback_scope_mode='decider_scope_fast_path',
            run_id=disjoint_run_id,
            started_at=started_at,
        )

        self.assertTrue(first['acquired'])
        self.assertFalse(second['acquired'])
        self.assertFalse(second['exact_scope'])
        self.assertEqual(second['conflict_run']['run_id'], first_run_id)
        self.assertTrue(disjoint['acquired'])

    def test_feedback_retry_admission_does_not_join_different_context(self):
        suffix = uuid4().hex[:10].upper()
        market_date = date.fromisoformat('2040-01-05')
        symbol = f'A{suffix}'
        first_run_id = f'claim-identity-{suffix}'
        equivalent_run_id = f'claim-equivalent-{suffix}'
        cutoff_run_id = f'claim-cutoff-{suffix}'
        mode_run_id = f'claim-mode-{suffix}'
        fs_div_run_id = f'claim-fs-div-{suffix}'
        run_ids = [
            first_run_id,
            equivalent_run_id,
            cutoff_run_id,
            mode_run_id,
            fs_div_run_id,
        ]

        def cleanup():
            with session_scope() as session:
                session.execute(
                    delete(AgentContextFeedbackRun).where(
                        AgentContextFeedbackRun.run_id.in_(run_ids)
                    )
                )

        self.addCleanup(cleanup)
        common = {
            'market_date': market_date,
            'roles': ('signal-decider',),
            'requested_symbols': (symbol,),
            'started_at': datetime.now(timezone.utc),
        }
        first = _claim_feedback_retry_run(
            **common,
            feedback_scope_mode='decider_scope_fast_path',
            admission_knowledge_cutoff_at='2026-01-02T09:00:00+09:00',
            fs_div='CFS',
            run_id=first_run_id,
        )
        equivalent = _claim_feedback_retry_run(
            **common,
            feedback_scope_mode='decider_scope_fast_path',
            admission_knowledge_cutoff_at='2026-01-02T00:00:00+00:00',
            fs_div='CFS',
            run_id=equivalent_run_id,
        )
        different_cutoff = _claim_feedback_retry_run(
            **common,
            feedback_scope_mode='decider_scope_fast_path',
            admission_knowledge_cutoff_at='2026-01-02T01:00:00+00:00',
            fs_div='CFS',
            run_id=cutoff_run_id,
        )
        different_mode = _claim_feedback_retry_run(
            **common,
            feedback_scope_mode='broad_feedback_universe',
            admission_knowledge_cutoff_at='2026-01-02T00:00:00+00:00',
            fs_div='CFS',
            run_id=mode_run_id,
        )
        different_fs_div = _claim_feedback_retry_run(
            **common,
            feedback_scope_mode='decider_scope_fast_path',
            admission_knowledge_cutoff_at='2026-01-02T00:00:00+00:00',
            fs_div='OFS',
            run_id=fs_div_run_id,
        )

        self.assertTrue(first['acquired'])
        self.assertFalse(equivalent['acquired'])
        self.assertTrue(equivalent['exact_scope'])
        self.assertFalse(different_cutoff['acquired'])
        self.assertFalse(different_cutoff['exact_scope'])
        self.assertFalse(different_mode['acquired'])
        self.assertFalse(different_mode['exact_scope'])
        self.assertFalse(different_fs_div['acquired'])
        self.assertFalse(different_fs_div['exact_scope'])

    def test_feedback_retry_admission_claim_recovers_stale_owner(self):
        suffix = uuid4().hex[:10].upper()
        stale_run_id = f'claim-stale-{suffix}'
        replacement_run_id = f'claim-replacement-{suffix}'
        run_ids = [stale_run_id, replacement_run_id]
        market_date = date.fromisoformat('2040-01-04')
        stale_at = datetime.now(timezone.utc) - timedelta(hours=2)
        symbol = f'A{suffix}'
        request_payload = {
            'mode': 'daily_retry_cycle_role_sequenced',
            'roles': ['signal-decider'],
            'requested_symbols': [symbol],
            'admission_scope_by_role': {'signal-decider': [symbol]},
        }

        with session_scope() as session:
            session.add(
                AgentContextFeedbackRun(
                    run_id=stale_run_id,
                    market_date=market_date,
                    status='running',
                    symbols_total=1,
                    completed_count=0,
                    failed_count=0,
                    request_payload=request_payload,
                    summary_payload={},
                    started_at=stale_at,
                    updated_at=stale_at,
                )
            )

        def cleanup():
            with session_scope() as session:
                session.execute(
                    delete(AgentContextFeedbackRun).where(
                        AgentContextFeedbackRun.run_id.in_(run_ids)
                    )
                )

        self.addCleanup(cleanup)

        replacement = _claim_feedback_retry_run(
            market_date=market_date,
            roles=('signal-decider',),
            requested_symbols=(symbol,),
            feedback_scope_mode='decider_scope_fast_path',
            run_id=replacement_run_id,
            started_at=datetime.now(timezone.utc),
        )

        with session_scope() as session:
            stale_status = session.execute(
                select(AgentContextFeedbackRun.status).where(
                    AgentContextFeedbackRun.run_id == stale_run_id
                )
            ).scalar_one()
        self.assertTrue(replacement['acquired'])
        self.assertEqual(stale_status, 'abandoned')

    def test_retry_owner_losing_lease_before_child_dispatch_fails_closed(self):
        suffix = uuid4().hex[:10].upper()
        market_date = date.fromisoformat('2040-01-06')
        symbol = f'A{suffix}'
        replacement_run_id = f'claim-takeover-{suffix}'
        created_run_ids = [replacement_run_id]
        roles = ('eod-summarizer',)
        service = AgentFeedbackRetryService()
        run_state_service = RecordingRunStateService()
        snapshots = SnapshotSequence(
            [
                _snapshot(
                    market_date=market_date.isoformat(),
                    symbols=(symbol,),
                    role_states={
                        roles[0]: _role_state(
                            role=roles[0],
                            symbols=(symbol,),
                            boundary_pending=(symbol,),
                            retry_pending=(symbol,),
                        )
                    },
                ),
                _snapshot(
                    market_date=market_date.isoformat(),
                    symbols=(symbol,),
                    role_states={
                        roles[0]: _role_state(
                            role=roles[0],
                            symbols=(symbol,),
                            boundary_pending=(symbol,),
                            retry_pending=(symbol,),
                        )
                    },
                ),
                _snapshot(
                    market_date=market_date.isoformat(),
                    symbols=(symbol,),
                    role_states={
                        roles[0]: _role_state(
                            role=roles[0],
                            symbols=(symbol,),
                            completed=(symbol,),
                        )
                    },
                ),
            ]
        )
        original_checkpoint = service._checkpoint_run_state
        takeover_result: dict[str, object] = {}

        def checkpoint_with_takeover(**kwargs):
            result = original_checkpoint(**kwargs)
            if (
                kwargs['lifecycle_stage'] == 'role_started'
                and not takeover_result
            ):
                owner_run_id = kwargs['request_run_id']
                created_run_ids.append(owner_run_id)
                stale_at = datetime.now(timezone.utc) - timedelta(hours=2)
                with session_scope() as session:
                    owner = session.execute(
                        select(AgentContextFeedbackRun).where(
                            AgentContextFeedbackRun.run_id == owner_run_id
                        )
                    ).scalar_one()
                    owner.updated_at = stale_at
                takeover_result.update(
                    _claim_feedback_retry_run(
                        market_date=market_date,
                        roles=roles,
                        requested_symbols=(symbol,),
                        feedback_scope_mode='decider_scope_fast_path',
                        run_id=replacement_run_id,
                        started_at=datetime.now(timezone.utc),
                    )
                )
            return result

        def cleanup():
            with session_scope() as session:
                session.execute(
                    delete(AgentContextFeedbackRun).where(
                        AgentContextFeedbackRun.run_id.in_(created_run_ids)
                    )
                )

        self.addCleanup(cleanup)

        with (
            patch.object(service, '_load_eligible_symbols', return_value=[symbol]),
            patch.object(
                service,
                '_build_role_sequence_snapshot',
                side_effect=snapshots,
            ),
            patch.object(
                service,
                '_checkpoint_run_state',
                side_effect=checkpoint_with_takeover,
            ),
            patch(
                'app.agent_context.retry_service.'
                'agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
                return_value=_feedback_prerequisites(),
            ),
            patch(
                'app.agent_context.retry_service.build_agent_feedback_role_funnel',
                return_value={
                    'role_symbols': {roles[0]: [symbol]},
                    'role_symbol_counts': {roles[0]: 1},
                    'role_diagnostics': {roles[0]: {}},
                },
            ),
            patch(
                'app.agent_context.retry_service.'
                'agent_context_feedback_run_state_service',
                run_state_service,
            ),
            patch(
                'app.agent_context.retry_service.'
                'agent_feedback_orchestrator.dispatch_for_symbols'
            ) as dispatch,
            patch(
                'app.agent_context.retry_service.'
                'agent_context_progress_service.get_feedback_progress',
                return_value={'summary': {'pending_symbols': 1}},
            ),
            self.assertRaisesRegex(RuntimeError, 'lease ownership lost'),
        ):
            service.run_retry_cycles(
                config=SimpleNamespace(
                    market_date=market_date,
                    roles=roles,
                    symbols=(symbol,),
                    feedback_scope_mode='decider_scope_fast_path',
                    max_cycles=1,
                    outer_batch_size=1,
                    reuse_existing=True,
                    force_recompute=False,
                    progress_callback=None,
                )
            )

        self.assertTrue(takeover_result['acquired'])
        dispatch.assert_not_called()
        with session_scope() as session:
            owner = session.execute(
                select(AgentContextFeedbackRun).where(
                    AgentContextFeedbackRun.run_id == created_run_ids[-1]
                )
            ).scalar_one()
            self.assertEqual(owner.status, 'abandoned')
            retry_admission = owner.request_payload['retry_admission']
            self.assertNotIn('owner_token', retry_admission)
            self.assertRegex(
                retry_admission['owner_token_hash'],
                r'^[0-9a-f]{64}$',
            )
            self.assertEqual(
                owner.summary_payload['admission_lease_loss']['stage'],
                'before_child_dispatch',
            )

    def test_retry_heartbeat_errors_exhaust_bounded_grace(self):
        lease = _FeedbackRetryClaimLease(
            run_id='heartbeat-grace-owner',
            owner_token='heartbeat-grace-token',
            heartbeat_grace_seconds=10,
        )
        lease._last_confirmed_monotonic = 100.0

        with (
            patch(
                'app.agent_context.retry_service.time.monotonic',
                return_value=109.9,
            ),
            patch(
                'app.agent_context.retry_service.'
                '_record_feedback_retry_lease_loss',
                return_value=True,
            ) as record_loss,
        ):
            lease.note_heartbeat_exception(TimeoutError('temporary'))

        self.assertFalse(lease.is_lost)
        record_loss.assert_not_called()

        with (
            patch(
                'app.agent_context.retry_service.time.monotonic',
                return_value=110.0,
            ),
            patch(
                'app.agent_context.retry_service.'
                '_record_feedback_retry_lease_loss',
                return_value=True,
            ) as record_loss,
        ):
            lease.note_heartbeat_exception(TimeoutError('expired'))

        self.assertTrue(lease.is_lost)
        record_loss.assert_called_once_with(
            run_id='heartbeat-grace-owner',
            owner_token='heartbeat-grace-token',
            stage='heartbeat',
            reason='heartbeat_refresh_grace_exceeded',
            error_type='TimeoutError',
        )
        with self.assertRaisesRegex(
            FeedbackRetryLeaseLostError,
            'lease ownership lost',
        ):
            lease.assert_owned(stage='before_child_dispatch')

    def test_retry_heartbeat_refresh_does_not_use_exhausted_shared_pool(self):
        owner_token = 'isolated-heartbeat-token'
        row = SimpleNamespace(
            status='running',
            request_payload={
                'retry_admission': {
                    'owner_token_hash': _feedback_retry_owner_token_hash(
                        owner_token
                    ),
                },
            },
            updated_at=None,
        )
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = row
        isolated_scope = MagicMock()
        isolated_scope.__enter__.return_value = session
        isolated_scope.__exit__.return_value = False
        scope_modes: list[bool] = []

        def starving_shared_pool(*, isolated_connection=False, **_kwargs):
            scope_modes.append(isolated_connection)
            if not isolated_connection:
                raise AssertionError('heartbeat used exhausted shared pool')
            return isolated_scope

        with patch(
            'app.agent_context.retry_service.session_scope',
            side_effect=starving_shared_pool,
        ):
            refreshed = _refresh_feedback_retry_claim(
                run_id='isolated-heartbeat-owner',
                owner_token=owner_token,
            )

        self.assertTrue(refreshed)
        self.assertEqual(scope_modes, [True])
        self.assertIsNotNone(row.updated_at)

    def test_retry_lease_ignores_late_heartbeat_after_finalization_begins(self):
        lease = _FeedbackRetryClaimLease(
            run_id='finalizing-owner',
            owner_token='finalizing-token',
            heartbeat_grace_seconds=10,
        )
        lease.begin_finalization()

        with patch(
            'app.agent_context.retry_service.'
            '_record_feedback_retry_lease_loss',
            return_value=True,
        ) as record_loss:
            lease.mark_lost(
                reason='durable_claim_not_owned',
                stage='heartbeat',
            )

        self.assertTrue(lease.is_finalizing)
        self.assertFalse(lease.is_lost)
        record_loss.assert_not_called()

    def test_run_retry_cycles_finalizes_durable_claim_when_owner_raises(self):
        suffix = uuid4().hex[:10].upper()
        market_date = date.fromisoformat('2040-01-05')
        symbol = f'A{suffix}'
        created_run_ids: list[str] = []
        service = AgentFeedbackRetryService()
        config = SimpleNamespace(
            market_date=market_date,
            roles=('signal-decider',),
            symbols=(symbol,),
            feedback_scope_mode='decider_scope_fast_path',
            max_cycles=3,
            outer_batch_size=20,
            reuse_existing=True,
            force_recompute=False,
            progress_callback=None,
        )

        def fail_after_claim(**kwargs):
            created_run_ids.append(kwargs['request_run_id'])
            raise RuntimeError('simulated owner failure')

        def cleanup():
            if not created_run_ids:
                return
            with session_scope() as session:
                session.execute(
                    delete(AgentContextFeedbackRun).where(
                        AgentContextFeedbackRun.run_id.in_(created_run_ids)
                    )
                )

        self.addCleanup(cleanup)

        with (
            patch.object(
                service,
                '_run_retry_cycles_admitted',
                side_effect=fail_after_claim,
            ),
            self.assertRaisesRegex(RuntimeError, 'simulated owner failure'),
        ):
            service.run_retry_cycles(config=config)

        self.assertEqual(len(created_run_ids), 1)
        with session_scope() as session:
            persisted = session.execute(
                select(AgentContextFeedbackRun).where(
                    AgentContextFeedbackRun.run_id == created_run_ids[0]
                )
            ).scalar_one()
            self.assertEqual(persisted.status, 'failed')
            self.assertIsNotNone(persisted.completed_at)
            self.assertEqual(
                persisted.summary_payload['admission_owner_error']['type'],
                'RuntimeError',
            )

    def test_feedback_retry_admission_lock_key_is_market_date_scoped(self):
        lock_key = _feedback_retry_admission_lock_key(
            market_date=date.fromisoformat('2026-04-16'),
            roles=('signal-decider', 'short-term-overlay'),
            symbols=('005930', '000660'),
        )
        reordered_lock_key = _feedback_retry_admission_lock_key(
            market_date=date.fromisoformat('2026-04-16'),
            roles=('short-term-overlay', 'signal-decider'),
            symbols=('000660', '005930'),
        )
        self.assertEqual(lock_key, reordered_lock_key)
        self.assertEqual(
            lock_key,
            _feedback_retry_admission_lock_key(
                market_date=date.fromisoformat('2026-04-16'),
                roles=('eod-summarizer',),
                symbols=None,
            ),
        )

    def test_equal_time_failure_prevents_feedback_reuse(self):
        service = AgentContextReuseService()
        suffix = uuid4().hex[:10]
        symbol = f'E{suffix.upper()}'
        role = 'eod-summarizer'
        market_date = date.fromisoformat('2026-04-16')
        observed_at = datetime(2026, 4, 16, 6, 0, tzinfo=timezone.utc)

        with session_scope() as session:
            session.add_all(
                [
                    AgentContextFeedback(
                        market_date=market_date,
                        symbol=symbol,
                        agent_name=role,
                        run_id=f'equal-success-{suffix}',
                        summary_payload={},
                        feedback_payload={'summary': 'ambiguous success'},
                        context_payload={},
                        received_at=observed_at,
                    ),
                    AgentContextFeedbackFailure(
                        market_date=market_date,
                        symbol=symbol,
                        agent_name=role,
                        run_id=f'equal-failure-{suffix}',
                        failure_kind='dispatch_failed',
                        retryable=True,
                        summary_payload={'error': 'ambiguous failure'},
                        context_payload={},
                        received_at=observed_at,
                    ),
                ]
            )

        def cleanup():
            with session_scope() as session:
                session.execute(
                    delete(AgentContextFeedback).where(
                        AgentContextFeedback.market_date == market_date,
                        AgentContextFeedback.symbol == symbol,
                        AgentContextFeedback.agent_name == role,
                    )
                )
                session.execute(
                    delete(AgentContextFeedbackFailure).where(
                        AgentContextFeedbackFailure.market_date == market_date,
                        AgentContextFeedbackFailure.symbol == symbol,
                        AgentContextFeedbackFailure.agent_name == role,
                    )
                )

        self.addCleanup(cleanup)

        decision = service.build_latest_decisions(
            market_date=market_date,
            symbols=[symbol],
            roles=[role],
        )[symbol][role]

        self.assertTrue(decision.should_dispatch)
        self.assertEqual(decision.reason, 'latest_failure_pending_retry')

    def test_newer_parent_success_outweighs_late_old_lineage_failure(self):
        service = AgentContextReuseService()
        suffix = uuid4().hex[:10].upper()
        symbol = f'L{suffix}'
        role = 'signal-decider'
        market_date = date.fromisoformat('2026-04-16')
        old_parent = f'old-parent-{suffix}'
        old_child = f'old-child-{suffix}'
        new_parent = f'new-parent-{suffix}'
        new_child = f'new-child-{suffix}'
        run_ids = [old_parent, old_child, new_parent, new_child]

        def run_payload(*, parent_run_id=None, mode=None):
            payload = {
                'mode': mode or 'role_sequential_runtime_buffered',
                'roles': [role],
                'symbols': [symbol],
                'requested_symbols': [symbol],
                'role_symbol_scopes': {role: [symbol]},
            }
            if parent_run_id is not None:
                payload['parent_run_id'] = parent_run_id
            return payload

        with session_scope() as session:
            session.add_all(
                [
                    AgentContextFeedbackRun(
                        run_id=old_parent,
                        market_date=market_date,
                        status='failed',
                        symbols_total=1,
                        completed_count=0,
                        failed_count=1,
                        request_payload=run_payload(
                            mode='daily_retry_cycle_role_sequenced'
                        ),
                        summary_payload={},
                        started_at=datetime(
                            2026, 4, 16, 5, 0, tzinfo=timezone.utc
                        ),
                    ),
                    AgentContextFeedbackRun(
                        run_id=old_child,
                        market_date=market_date,
                        status='failed',
                        symbols_total=1,
                        completed_count=0,
                        failed_count=1,
                        request_payload=run_payload(
                            parent_run_id=old_parent
                        ),
                        summary_payload={},
                        started_at=datetime(
                            2026, 4, 16, 5, 1, tzinfo=timezone.utc
                        ),
                    ),
                    AgentContextFeedbackRun(
                        run_id=new_parent,
                        market_date=market_date,
                        status='completed',
                        symbols_total=1,
                        completed_count=1,
                        failed_count=0,
                        request_payload=run_payload(
                            mode='daily_retry_cycle_role_sequenced'
                        ),
                        summary_payload={},
                        started_at=datetime(
                            2026, 4, 16, 6, 0, tzinfo=timezone.utc
                        ),
                    ),
                    AgentContextFeedbackRun(
                        run_id=new_child,
                        market_date=market_date,
                        status='completed',
                        symbols_total=1,
                        completed_count=1,
                        failed_count=0,
                        request_payload=run_payload(
                            parent_run_id=new_parent
                        ),
                        summary_payload={},
                        started_at=datetime(
                            2026, 4, 16, 6, 1, tzinfo=timezone.utc
                        ),
                    ),
                    AgentContextFeedback(
                        market_date=market_date,
                        symbol=symbol,
                        agent_name=role,
                        run_id=new_child,
                        summary_payload={},
                        feedback_payload={'decision': 'WAIT'},
                        context_payload={},
                        received_at=datetime(
                            2026, 4, 16, 6, 2, tzinfo=timezone.utc
                        ),
                    ),
                    AgentContextFeedbackFailure(
                        market_date=market_date,
                        symbol=symbol,
                        agent_name=role,
                        run_id=old_child,
                        failure_kind='agent_error',
                        retryable=True,
                        summary_payload={'error': 'late persisted old failure'},
                        context_payload={},
                        received_at=datetime(
                            2026, 4, 16, 6, 3, tzinfo=timezone.utc
                        ),
                    ),
                ]
            )

        def cleanup():
            with session_scope() as session:
                session.execute(
                    delete(AgentContextFeedback).where(
                        AgentContextFeedback.market_date == market_date,
                        AgentContextFeedback.symbol == symbol,
                        AgentContextFeedback.agent_name == role,
                    )
                )
                session.execute(
                    delete(AgentContextFeedbackFailure).where(
                        AgentContextFeedbackFailure.market_date == market_date,
                        AgentContextFeedbackFailure.symbol == symbol,
                        AgentContextFeedbackFailure.agent_name == role,
                    )
                )
                session.execute(
                    delete(AgentContextFeedbackRun).where(
                        AgentContextFeedbackRun.run_id.in_(run_ids)
                    )
                )

        self.addCleanup(cleanup)

        decision = service.build_latest_decisions(
            market_date=market_date,
            symbols=[symbol],
            roles=[role],
        )[symbol][role]

        self.assertFalse(decision.should_dispatch)
        self.assertEqual(decision.reason, 'reuse_latest_feedback')
        self.assertEqual(decision.latest_feedback['run_id'], new_child)

    def test_newer_parent_failure_outweighs_later_old_lineage_child_success(self):
        service = AgentContextReuseService()
        suffix = uuid4().hex[:10].upper()
        symbol = f'I{suffix}'
        role = 'eod-summarizer'
        market_date = date.fromisoformat('2026-04-16')
        old_parent = f'inverse-old-parent-{suffix}'
        old_success_child = f'inverse-old-success-{suffix}'
        new_parent = f'inverse-new-parent-{suffix}'
        new_failure_child = f'inverse-new-failure-{suffix}'
        run_ids = [
            old_parent,
            old_success_child,
            new_parent,
            new_failure_child,
        ]

        def run_payload(*, parent_run_id=None, mode=None):
            payload = {
                'mode': mode or 'role_sequential_runtime_buffered',
                'roles': [role],
                'symbols': [symbol],
                'requested_symbols': [symbol],
                'role_symbol_scopes': {role: [symbol]},
            }
            if parent_run_id is not None:
                payload['parent_run_id'] = parent_run_id
            return payload

        with session_scope() as session:
            session.add_all(
                [
                    AgentContextFeedbackRun(
                        run_id=old_parent,
                        market_date=market_date,
                        status='completed',
                        symbols_total=1,
                        completed_count=1,
                        failed_count=0,
                        request_payload=run_payload(
                            mode='daily_retry_cycle_role_sequenced'
                        ),
                        summary_payload={},
                        started_at=datetime(
                            2026, 4, 16, 5, 0, tzinfo=timezone.utc
                        ),
                    ),
                    AgentContextFeedbackRun(
                        run_id=new_parent,
                        market_date=market_date,
                        status='failed',
                        symbols_total=1,
                        completed_count=0,
                        failed_count=1,
                        request_payload=run_payload(
                            mode='daily_retry_cycle_role_sequenced'
                        ),
                        summary_payload={},
                        started_at=datetime(
                            2026, 4, 16, 6, 0, tzinfo=timezone.utc
                        ),
                    ),
                    AgentContextFeedbackRun(
                        run_id=new_failure_child,
                        market_date=market_date,
                        status='failed',
                        symbols_total=1,
                        completed_count=0,
                        failed_count=1,
                        request_payload=run_payload(
                            parent_run_id=new_parent
                        ),
                        summary_payload={},
                        started_at=datetime(
                            2026, 4, 16, 6, 10, tzinfo=timezone.utc
                        ),
                    ),
                    AgentContextFeedbackRun(
                        run_id=old_success_child,
                        market_date=market_date,
                        status='completed',
                        symbols_total=1,
                        completed_count=1,
                        failed_count=0,
                        request_payload=run_payload(
                            parent_run_id=old_parent
                        ),
                        summary_payload={},
                        started_at=datetime(
                            2026, 4, 16, 7, 0, tzinfo=timezone.utc
                        ),
                    ),
                    AgentContextFeedbackFailure(
                        market_date=market_date,
                        symbol=symbol,
                        agent_name=role,
                        run_id=new_failure_child,
                        failure_kind='agent_error',
                        retryable=True,
                        summary_payload={'error': 'newer parent failed'},
                        context_payload={},
                        received_at=datetime(
                            2026, 4, 16, 6, 11, tzinfo=timezone.utc
                        ),
                    ),
                    AgentContextFeedback(
                        market_date=market_date,
                        symbol=symbol,
                        agent_name=role,
                        run_id=old_success_child,
                        summary_payload={},
                        feedback_payload={'summary': 'late old child success'},
                        context_payload={},
                        received_at=datetime(
                            2026, 4, 16, 7, 1, tzinfo=timezone.utc
                        ),
                    ),
                ]
            )

        def cleanup():
            with session_scope() as session:
                session.execute(
                    delete(AgentContextFeedback).where(
                        AgentContextFeedback.market_date == market_date,
                        AgentContextFeedback.symbol == symbol,
                        AgentContextFeedback.agent_name == role,
                    )
                )
                session.execute(
                    delete(AgentContextFeedbackFailure).where(
                        AgentContextFeedbackFailure.market_date == market_date,
                        AgentContextFeedbackFailure.symbol == symbol,
                        AgentContextFeedbackFailure.agent_name == role,
                    )
                )
                session.execute(
                    delete(AgentContextFeedbackRun).where(
                        AgentContextFeedbackRun.run_id.in_(run_ids)
                    )
                )

        self.addCleanup(cleanup)

        decisions = service.build_latest_decisions(
            market_date=market_date,
            symbols=[symbol],
            roles=[role],
        )
        decision = decisions[symbol][role]

        self.assertTrue(decision.should_dispatch)
        self.assertEqual(decision.reason, 'latest_failure_pending_retry')
        upstream, traceability, missing = build_upstream_feedback_bundle(
            symbol=symbol,
            market_date=market_date,
            buffered_lookup={},
            upstream_reuse_decisions=decisions,
            upstream_summary_roles=(role,),
        )
        self.assertEqual(upstream, {})
        self.assertEqual(traceability, {})
        self.assertEqual(missing, [role])

    def test_same_run_failure_remains_authoritative_over_feedback(self):
        service = AgentContextReuseService()
        suffix = uuid4().hex[:10].upper()
        symbol = f'S{suffix}'
        role = 'signal-decider'
        run_id = f'same-child-{suffix}'
        market_date = date.fromisoformat('2026-04-16')
        feedback_at = datetime(2026, 4, 16, 6, 0, tzinfo=timezone.utc)

        with session_scope() as session:
            session.add_all(
                [
                    AgentContextFeedback(
                        market_date=market_date,
                        symbol=symbol,
                        agent_name=role,
                        run_id=run_id,
                        summary_payload={},
                        feedback_payload={'decision': 'WAIT'},
                        context_payload={},
                        received_at=feedback_at,
                    ),
                    AgentContextFeedbackFailure(
                        market_date=market_date,
                        symbol=symbol,
                        agent_name=role,
                        run_id=run_id,
                        failure_kind='agent_error',
                        retryable=True,
                        summary_payload={'error': 'same dispatch failed'},
                        context_payload={},
                        received_at=feedback_at + timedelta(seconds=1),
                    ),
                ]
            )

        def cleanup():
            with session_scope() as session:
                session.execute(
                    delete(AgentContextFeedback).where(
                        AgentContextFeedback.market_date == market_date,
                        AgentContextFeedback.symbol == symbol,
                        AgentContextFeedback.agent_name == role,
                    )
                )
                session.execute(
                    delete(AgentContextFeedbackFailure).where(
                        AgentContextFeedbackFailure.market_date == market_date,
                        AgentContextFeedbackFailure.symbol == symbol,
                        AgentContextFeedbackFailure.agent_name == role,
                    )
                )

        self.addCleanup(cleanup)

        decision = service.build_latest_decisions(
            market_date=market_date,
            symbols=[symbol],
            roles=[role],
        )[symbol][role]

        self.assertTrue(decision.should_dispatch)
        self.assertEqual(decision.reason, 'latest_failure_pending_retry')

    def test_reuse_lineage_detaches_rows_and_breaks_equal_time_ties_by_id(self):
        service = AgentFeedbackRetryService()
        suffix = uuid4().hex[:10]
        symbol = f'R{suffix.upper()}'
        role = 'signal-decider'
        prior_parent = f'prior-parent-{suffix}'
        current_parent = f'current-parent-{suffix}'
        older_child = f'older-child-{suffix}'
        newer_child = f'newer-child-{suffix}'
        run_ids = [prior_parent, current_parent, older_child, newer_child]
        market_date = date.fromisoformat('2026-04-16')
        parent_started_at = datetime(
            2026, 4, 16, 6, 0, tzinfo=timezone.utc
        )
        child_started_at = datetime(
            2026, 4, 16, 6, 1, tzinfo=timezone.utc
        )

        def request_payload(*, parent_run_id=None, mode=None):
            payload = {
                'mode': mode or 'role_sequential_runtime_buffered',
                'roles': [role],
                'symbols': [symbol],
                'requested_symbols': [symbol],
                'role_symbol_scopes': {role: [symbol]},
            }
            if parent_run_id is not None:
                payload['parent_run_id'] = parent_run_id
            return payload

        with session_scope() as session:
            session.add_all(
                [
                    AgentContextFeedbackRun(
                        run_id=prior_parent,
                        market_date=market_date,
                        status='failed',
                        symbols_total=1,
                        completed_count=0,
                        failed_count=1,
                        request_payload=request_payload(
                            mode='daily_retry_cycle_role_sequenced'
                        ),
                        summary_payload={},
                        started_at=parent_started_at,
                    ),
                    AgentContextFeedbackRun(
                        run_id=current_parent,
                        market_date=market_date,
                        status='running',
                        symbols_total=1,
                        completed_count=0,
                        failed_count=0,
                        request_payload=request_payload(
                            mode='daily_retry_cycle_role_sequenced'
                        ),
                        summary_payload={},
                        started_at=parent_started_at,
                    ),
                    AgentContextFeedbackRun(
                        run_id=older_child,
                        market_date=market_date,
                        status='completed',
                        symbols_total=1,
                        completed_count=1,
                        failed_count=0,
                        request_payload=request_payload(
                            parent_run_id=current_parent
                        ),
                        summary_payload={},
                        started_at=child_started_at,
                    ),
                    AgentContextFeedbackRun(
                        run_id=newer_child,
                        market_date=market_date,
                        status='abandoned',
                        symbols_total=1,
                        completed_count=0,
                        failed_count=1,
                        request_payload=request_payload(
                            parent_run_id=current_parent
                        ),
                        summary_payload={},
                        started_at=child_started_at,
                    ),
                ]
            )

        def cleanup():
            with session_scope() as session:
                session.execute(
                    delete(AgentContextFeedbackRun).where(
                        AgentContextFeedbackRun.run_id.in_(run_ids)
                    )
                )

        self.addCleanup(cleanup)

        (
            latest_parent_by_key,
            parent_by_child_run_id,
            latest_child_by_parent_key,
        ) = service._load_reuse_lineage_context(
            market_date=market_date,
            roles=(role,),
            symbols=(symbol,),
            current_parent_run_id=current_parent,
            current_parent_started_at=parent_started_at,
        )

        self.assertEqual(
            latest_parent_by_key[(symbol, role)],
            (prior_parent, 'failed'),
        )
        self.assertEqual(
            parent_by_child_run_id[newer_child],
            current_parent,
        )
        self.assertEqual(
            latest_child_by_parent_key[(current_parent, symbol, role)],
            (newer_child, 'abandoned'),
        )

    def test_reuse_lineage_marks_only_failed_scope_of_partial_child_unusable(self):
        service = AgentFeedbackRetryService()
        suffix = uuid4().hex[:10].upper()
        success_symbol = f'S{suffix}'
        failed_symbol = f'F{suffix}'
        role = 'signal-decider'
        prior_parent = f'prior-partial-parent-{suffix}'
        prior_child = f'prior-partial-child-{suffix}'
        current_parent = f'current-parent-{suffix}'
        run_ids = [prior_parent, prior_child, current_parent]
        market_date = date.fromisoformat('2026-04-16')
        parent_started_at = datetime(
            2026, 4, 16, 6, 0, tzinfo=timezone.utc
        )
        symbols = [success_symbol, failed_symbol]

        def request_payload(*, parent_run_id=None, mode=None):
            payload = {
                'mode': mode or 'role_sequential_runtime_buffered',
                'roles': [role],
                'symbols': symbols,
                'requested_symbols': symbols,
                'role_symbol_scopes': {role: symbols},
            }
            if parent_run_id is not None:
                payload['parent_run_id'] = parent_run_id
            return payload

        with session_scope() as session:
            session.add_all(
                [
                    AgentContextFeedbackRun(
                        run_id=prior_parent,
                        market_date=market_date,
                        status='partial',
                        symbols_total=2,
                        completed_count=1,
                        failed_count=1,
                        request_payload=request_payload(
                            mode='daily_retry_cycle_role_sequenced'
                        ),
                        summary_payload={
                            'failed_symbols_by_role': {
                                role: [failed_symbol]
                            }
                        },
                        started_at=datetime(
                            2026, 4, 16, 5, 0, tzinfo=timezone.utc
                        ),
                    ),
                    AgentContextFeedbackRun(
                        run_id=prior_child,
                        market_date=market_date,
                        status='completed_with_failures',
                        symbols_total=2,
                        completed_count=1,
                        failed_count=1,
                        request_payload=request_payload(
                            parent_run_id=prior_parent
                        ),
                        summary_payload={
                            'failed_symbols_by_role': {
                                role: [failed_symbol]
                            }
                        },
                        started_at=datetime(
                            2026, 4, 16, 5, 1, tzinfo=timezone.utc
                        ),
                    ),
                    AgentContextFeedbackRun(
                        run_id=current_parent,
                        market_date=market_date,
                        status='running',
                        symbols_total=2,
                        completed_count=0,
                        failed_count=0,
                        request_payload=request_payload(
                            mode='daily_retry_cycle_role_sequenced'
                        ),
                        summary_payload={},
                        started_at=parent_started_at,
                    ),
                ]
            )

        def cleanup():
            with session_scope() as session:
                session.execute(
                    delete(AgentContextFeedbackRun).where(
                        AgentContextFeedbackRun.run_id.in_(run_ids)
                    )
                )

        self.addCleanup(cleanup)

        (
            latest_parent_by_key,
            _,
            latest_child_by_parent_key,
        ) = service._load_reuse_lineage_context(
            market_date=market_date,
            roles=(role,),
            symbols=symbols,
            current_parent_run_id=current_parent,
            current_parent_started_at=parent_started_at,
        )

        self.assertEqual(
            latest_parent_by_key[(success_symbol, role)],
            (prior_parent, 'partial'),
        )
        self.assertEqual(
            latest_parent_by_key[(failed_symbol, role)],
            (prior_parent, 'failed'),
        )
        self.assertEqual(
            latest_child_by_parent_key[
                (prior_parent, success_symbol, role)
            ],
            (prior_child, 'completed_with_failures'),
        )
        self.assertEqual(
            latest_child_by_parent_key[
                (prior_parent, failed_symbol, role)
            ],
            (prior_child, 'failed'),
        )

    def test_exact_lease_loss_parent_reuses_only_persisted_child_success(
        self,
    ):
        service = AgentFeedbackRetryService()
        suffix = uuid4().hex[:10].upper()
        success_symbol = f'S{suffix}'
        failed_symbol = f'F{suffix}'
        symbols = [success_symbol, failed_symbol]
        role = 'short-term-decider'
        market_date = date.fromisoformat('2026-04-16')
        lease_parent = f'feedback-cycle-2026-04-16-{suffix.lower()}'
        partial_child = f'symbol-run-2026-04-16-{suffix.lower()}'
        current_parent = f'current-parent-{suffix}'
        run_ids = [lease_parent, partial_child, current_parent]
        cutoff = '2026-04-16T04:55:00+00:00'
        lease_completed_at = datetime(
            2026,
            4,
            16,
            5,
            2,
            tzinfo=timezone.utc,
        )
        roles = list(DECISION_SUPPORT_AGENT_NAMES)
        role_scopes = {
            decision_role: (
                list(symbols) if decision_role == role else []
            )
            for decision_role in roles
        }
        role_counts = {
            decision_role: len(role_scopes[decision_role])
            for decision_role in roles
        }
        lease_parent_payload = {
            'market_date': market_date.isoformat(),
            'request_run_id': lease_parent,
            'mode': 'daily_retry_cycle_role_sequenced',
            'roles': roles,
            'requested_symbols': symbols,
            'feedback_scope_mode': 'decider_scope_fast_path',
            'admission_scope_by_role': {
                decision_role: list(symbols)
                for decision_role in roles
            },
            'role_symbol_scopes': role_scopes,
            'role_symbol_counts': role_counts,
            'fs_div': 'CFS',
            'reuse_existing': True,
            'force_recompute': False,
            'knowledge_cutoff_at': cutoff,
            'admission_knowledge_cutoff_at': cutoff,
            'requested_knowledge_cutoff_at': cutoff,
            'retry_admission': {
                'schema_version': 'agent_feedback_retry_admission_v1',
                'claimed_at': '2026-04-16T05:00:00+00:00',
                'owner_token_hash': 'a' * 64,
            },
        }
        with session_scope() as session:
            session.add_all(
                [
                    AgentContextFeedbackRun(
                        run_id=lease_parent,
                        market_date=market_date,
                        status='abandoned',
                        symbols_total=2,
                        completed_count=0,
                        failed_count=2,
                        request_payload=lease_parent_payload,
                        summary_payload={
                            'admission_lease_loss': {
                                'schema_version': (
                                    'agent_feedback_retry_admission_lease_loss_v1'
                                ),
                                'detected_at': (
                                    lease_completed_at.isoformat()
                                ),
                                'stage': (
                                    'before_checkpoint:'
                                    'role_batch_completed'
                                ),
                                'reason': (
                                    'heartbeat_refresh_grace_exceeded'
                                ),
                                'owner_token_fingerprint': 'a' * 16,
                            }
                        },
                        started_at=datetime(
                            2026,
                            4,
                            16,
                            5,
                            0,
                            tzinfo=timezone.utc,
                        ),
                        completed_at=lease_completed_at,
                    ),
                    AgentContextFeedbackRun(
                        run_id=partial_child,
                        market_date=market_date,
                        status='partial',
                        symbols_total=2,
                        completed_count=1,
                        failed_count=1,
                        request_payload={
                            'parent_run_id': lease_parent,
                            'roles': [role],
                            'requested_symbols': symbols,
                            'role_symbol_scopes': {role: symbols},
                        },
                        summary_payload={
                            'failed_symbols_by_role': {
                                role: [failed_symbol]
                            }
                        },
                        started_at=datetime(
                            2026,
                            4,
                            16,
                            5,
                            1,
                            tzinfo=timezone.utc,
                        ),
                        completed_at=lease_completed_at,
                    ),
                    AgentContextFeedbackRun(
                        run_id=current_parent,
                        market_date=market_date,
                        status='running',
                        symbols_total=2,
                        completed_count=0,
                        failed_count=0,
                        request_payload={
                            'mode': 'daily_retry_cycle_role_sequenced',
                            'roles': [role],
                            'requested_symbols': symbols,
                            'role_symbol_scopes': {role: symbols},
                        },
                        summary_payload={},
                        started_at=datetime(
                            2026,
                            4,
                            16,
                            6,
                            0,
                            tzinfo=timezone.utc,
                        ),
                    ),
                ]
            )

        def cleanup():
            with session_scope() as session:
                session.execute(
                    delete(AgentContextFeedbackRun).where(
                        AgentContextFeedbackRun.run_id.in_(run_ids)
                    )
                )

        self.addCleanup(cleanup)

        reuse_decisions = {
            success_symbol: {
                role: _decision(
                    should_dispatch=False,
                    latest_feedback_id=81,
                    latest_feedback={
                        'id': 81,
                        'run_id': partial_child,
                        'received_at': (
                            '2026-04-16T05:01:30+00:00'
                        ),
                    },
                    latest_feedback_received_at=(
                        '2026-04-16T05:01:30+00:00'
                    ),
                    latest_decision_snapshot_id=181,
                    latest_decision_snapshot_fingerprint='snapshot-181',
                    latest_decision_snapshot_observed_at=(
                        '2026-04-16T05:01:20+00:00'
                    ),
                    reason='reuse_latest_feedback',
                )
            },
            failed_symbol: {
                role: _decision(
                    should_dispatch=True,
                    latest_feedback_received_at=(
                        '2026-04-16T05:01:30+00:00'
                    ),
                    reason='latest_failure_pending_retry',
                )
            },
        }

        def build_snapshot():
            with (
                patch.object(
                    service,
                    '_load_same_day_terminal_symbols',
                    return_value=({}, {}),
                ),
                patch(
                    'app.agent_context.retry_service.'
                    'agent_context_reuse_service.build_latest_decisions',
                    return_value=reuse_decisions,
                ),
            ):
                return service._build_role_sequence_snapshot(
                    market_date=market_date,
                    roles=(role,),
                    eligible_symbols=tuple(symbols),
                    role_symbol_scopes={role: tuple(symbols)},
                    reuse_existing=True,
                    parent_run_id=current_parent,
                    parent_started_at=datetime(
                        2026,
                        4,
                        16,
                        6,
                        0,
                        tzinfo=timezone.utc,
                    ),
                    knowledge_cutoff_at=cutoff,
                    fs_div='CFS',
                )

        snapshot = build_snapshot()
        role_state = snapshot.role_states[role]
        self.assertEqual(role_state.reused_symbols, (success_symbol,))
        self.assertEqual(role_state.completed_symbols, (success_symbol,))
        self.assertEqual(
            role_state.boundary_pending_symbols,
            (failed_symbol,),
        )

        with session_scope() as session:
            parent = session.execute(
                select(AgentContextFeedbackRun).where(
                    AgentContextFeedbackRun.run_id == lease_parent
                )
            ).scalar_one()
            parent.status = 'failed'

        failed_parent_snapshot = build_snapshot()
        failed_parent_role_state = failed_parent_snapshot.role_states[role]
        self.assertEqual(failed_parent_role_state.reused_symbols, ())
        self.assertEqual(failed_parent_role_state.completed_symbols, ())
        self.assertEqual(
            failed_parent_role_state.boundary_pending_symbols,
            tuple(symbols),
        )

        with session_scope() as session:
            parent = session.execute(
                select(AgentContextFeedbackRun).where(
                    AgentContextFeedbackRun.run_id == lease_parent
                )
            ).scalar_one()
            parent.status = 'abandoned'
            parent.summary_payload = {
                'admission_lease_loss': {
                    **parent.summary_payload['admission_lease_loss'],
                    'owner_token_fingerprint': 'b' * 16,
                }
            }

        malformed_snapshot = build_snapshot()
        malformed_role_state = malformed_snapshot.role_states[role]
        self.assertEqual(malformed_role_state.reused_symbols, ())
        self.assertEqual(malformed_role_state.completed_symbols, ())
        self.assertEqual(
            malformed_role_state.boundary_pending_symbols,
            tuple(symbols),
        )

    def test_newer_failed_child_overrides_older_sibling_success(self):
        service = AgentFeedbackRetryService()
        suffix = uuid4().hex[:10]
        symbol = f'F{suffix.upper()}'
        role = 'eod-summarizer'
        parent_run = f'failure-parent-{suffix}'
        successful_child = f'success-child-{suffix}'
        failed_child = f'failed-child-{suffix}'
        run_ids = [parent_run, successful_child, failed_child]
        market_date = date.fromisoformat('2026-04-16')
        parent_started_at = datetime(
            2026, 4, 16, 6, 0, tzinfo=timezone.utc
        )

        def request_payload(*, parent_run_id=None):
            payload = {
                'mode': 'role_sequential_runtime_buffered',
                'roles': [role],
                'symbols': [symbol],
                'requested_symbols': [symbol],
                'role_symbol_scopes': {role: [symbol]},
            }
            if parent_run_id is not None:
                payload['parent_run_id'] = parent_run_id
            return payload

        with session_scope() as session:
            session.add_all(
                [
                    AgentContextFeedbackRun(
                        run_id=parent_run,
                        market_date=market_date,
                        status='running',
                        symbols_total=1,
                        completed_count=0,
                        failed_count=0,
                        request_payload={
                            **request_payload(),
                            'mode': 'daily_retry_cycle_role_sequenced',
                        },
                        summary_payload={},
                        started_at=parent_started_at,
                    ),
                    AgentContextFeedbackRun(
                        run_id=successful_child,
                        market_date=market_date,
                        status='completed',
                        symbols_total=1,
                        completed_count=1,
                        failed_count=0,
                        request_payload=request_payload(
                            parent_run_id=parent_run
                        ),
                        summary_payload={},
                        started_at=datetime(
                            2026, 4, 16, 6, 1, tzinfo=timezone.utc
                        ),
                    ),
                    AgentContextFeedbackRun(
                        run_id=failed_child,
                        market_date=market_date,
                        status='failed',
                        symbols_total=1,
                        completed_count=0,
                        failed_count=1,
                        request_payload=request_payload(
                            parent_run_id=parent_run
                        ),
                        summary_payload={},
                        started_at=datetime(
                            2026, 4, 16, 6, 2, tzinfo=timezone.utc
                        ),
                    ),
                    AgentContextFeedback(
                        market_date=market_date,
                        symbol=symbol,
                        agent_name=role,
                        run_id=successful_child,
                        summary_payload={},
                        feedback_payload={'summary': 'older success'},
                        received_at=datetime(
                            2026, 4, 16, 6, 1, 30, tzinfo=timezone.utc
                        ),
                    ),
                    AgentContextFeedbackFailure(
                        market_date=market_date,
                        symbol=symbol,
                        agent_name=role,
                        run_id=failed_child,
                        failure_kind='agent_error',
                        retryable=True,
                        summary_payload={'error': 'newer failure'},
                        received_at=datetime(
                            2026, 4, 16, 6, 2, 30, tzinfo=timezone.utc
                        ),
                    ),
                ]
            )

        def cleanup():
            with session_scope() as session:
                session.execute(
                    delete(AgentContextFeedback).where(
                        AgentContextFeedback.market_date == market_date,
                        AgentContextFeedback.symbol == symbol,
                        AgentContextFeedback.agent_name == role,
                    )
                )
                session.execute(
                    delete(AgentContextFeedbackFailure).where(
                        AgentContextFeedbackFailure.market_date == market_date,
                        AgentContextFeedbackFailure.symbol == symbol,
                        AgentContextFeedbackFailure.agent_name == role,
                    )
                )
                session.execute(
                    delete(AgentContextFeedbackRun).where(
                        AgentContextFeedbackRun.run_id.in_(run_ids)
                    )
                )

        self.addCleanup(cleanup)

        with patch(
            'app.agent_context.retry_service.agent_context_reuse_service.build_latest_decisions',
            return_value={
                symbol: {
                    role: _decision(
                        should_dispatch=True,
                        reason='latest_failure_pending_retry',
                    )
                }
            },
        ):
            snapshot = service._build_role_sequence_snapshot(
                market_date=market_date,
                roles=(role,),
                eligible_symbols=(symbol,),
                parent_run_id=parent_run,
                parent_started_at=parent_started_at,
            )

        role_state = snapshot.role_states[role]
        self.assertEqual(role_state.completed_symbols, ())
        self.assertEqual(role_state.failed_symbols, (symbol,))
        self.assertEqual(role_state.retry_pending_symbols, (symbol,))
        self.assertEqual(role_state.boundary_status, 'failed')

    def test_partial_child_summary_keeps_failed_scope_after_failure_row_overwrite(
        self,
    ):
        service = AgentFeedbackRetryService()
        suffix = uuid4().hex[:10].upper()
        success_symbol = f'A{suffix}'
        failed_symbol = f'B{suffix}'
        role = 'signal-decider'
        parent_run = f'partial-parent-{suffix}'
        current_child = f'partial-child-{suffix}'
        old_child = f'old-child-{suffix}'
        run_ids = [parent_run, current_child, old_child]
        market_date = date.fromisoformat('2026-04-16')
        parent_started_at = datetime(
            2026, 4, 16, 6, 0, tzinfo=timezone.utc
        )
        symbols = [success_symbol, failed_symbol]

        def child_request(run_parent: str | None = None):
            payload = {
                'roles': [role],
                'symbols': symbols,
                'requested_symbols': symbols,
                'role_symbol_scopes': {role: symbols},
            }
            if run_parent is not None:
                payload['parent_run_id'] = run_parent
            return payload

        with session_scope() as session:
            session.add_all(
                [
                    AgentContextFeedbackRun(
                        run_id=parent_run,
                        market_date=market_date,
                        status='running',
                        symbols_total=2,
                        completed_count=0,
                        failed_count=0,
                        request_payload={
                            **child_request(),
                            'mode': 'daily_retry_cycle_role_sequenced',
                        },
                        summary_payload={},
                        started_at=parent_started_at,
                    ),
                    AgentContextFeedbackRun(
                        run_id=old_child,
                        market_date=market_date,
                        status='failed',
                        symbols_total=2,
                        completed_count=0,
                        failed_count=2,
                        request_payload=child_request('older-parent'),
                        summary_payload={},
                        started_at=datetime(
                            2026, 4, 16, 5, 0, tzinfo=timezone.utc
                        ),
                    ),
                    AgentContextFeedbackRun(
                        run_id=current_child,
                        market_date=market_date,
                        status='completed_with_failures',
                        symbols_total=2,
                        completed_count=1,
                        failed_count=1,
                        request_payload=child_request(parent_run),
                        summary_payload={
                            'failed_symbols_by_role': {
                                role: [failed_symbol]
                            }
                        },
                        started_at=datetime(
                            2026, 4, 16, 6, 1, tzinfo=timezone.utc
                        ),
                    ),
                    AgentContextFeedback(
                        market_date=market_date,
                        symbol=success_symbol,
                        agent_name=role,
                        run_id=current_child,
                        summary_payload={},
                        feedback_payload={'summary': 'current success'},
                        received_at=datetime(
                            2026, 4, 16, 6, 2, tzinfo=timezone.utc
                        ),
                    ),
                    AgentContextFeedback(
                        market_date=market_date,
                        symbol=failed_symbol,
                        agent_name=role,
                        run_id=current_child,
                        summary_payload={},
                        feedback_payload={'summary': 'stale response'},
                        received_at=datetime(
                            2026, 4, 16, 6, 2, tzinfo=timezone.utc
                        ),
                    ),
                    AgentContextFeedbackFailure(
                        market_date=market_date,
                        symbol=failed_symbol,
                        agent_name=role,
                        run_id=old_child,
                        failure_kind='agent_error',
                        retryable=True,
                        summary_payload={'error': 'late old failure'},
                        received_at=datetime(
                            2026, 4, 16, 6, 3, tzinfo=timezone.utc
                        ),
                    ),
                ]
            )

        def cleanup():
            with session_scope() as session:
                session.execute(
                    delete(AgentContextFeedback).where(
                        AgentContextFeedback.market_date == market_date,
                        AgentContextFeedback.symbol.in_(symbols),
                        AgentContextFeedback.agent_name == role,
                    )
                )
                session.execute(
                    delete(AgentContextFeedbackFailure).where(
                        AgentContextFeedbackFailure.market_date == market_date,
                        AgentContextFeedbackFailure.symbol.in_(symbols),
                        AgentContextFeedbackFailure.agent_name == role,
                    )
                )
                session.execute(
                    delete(AgentContextFeedbackRun).where(
                        AgentContextFeedbackRun.run_id.in_(run_ids)
                    )
                )

        self.addCleanup(cleanup)

        completed, failed = service._load_same_day_terminal_symbols(
            market_date=market_date,
            roles=(role,),
            symbols=symbols,
            required_parent_run_id=parent_run,
        )

        self.assertEqual(completed[role], {success_symbol})
        self.assertEqual(failed[role], {failed_symbol})

    def test_retry_parent_summary_preserves_exact_reused_symbols_by_role(self):
        market_date = date.fromisoformat('2026-04-16')
        state = RuntimeFeedbackRunState(
            run_id='feedback-parent-reuse-summary',
            market_date=market_date,
            requested_symbols=['005930', '000660'],
            requested_roles=['eod-summarizer'],
            role_sequence=['eod-summarizer'],
            parallel_per_role=1,
            batch_size=20,
            started_at=datetime.now(timezone.utc),
            reused_symbols_by_role={'eod-summarizer': {'000660', '005930'}},
            reused_feedback_evidence_by_role={
                'eod-summarizer': {
                    '000660': {
                        'schema_version': 'agent_feedback_reuse_evidence_v1',
                        'feedback_id': 10,
                        'feedback_run_id': 'reuse-source-000660',
                        'feedback_received_at': '2026-04-16T05:00:00+00:00',
                    },
                    '005930': {
                        'schema_version': 'agent_feedback_reuse_evidence_v1',
                        'feedback_id': 11,
                        'feedback_run_id': 'reuse-source-005930',
                        'feedback_received_at': '2026-04-16T05:01:00+00:00',
                    },
                }
            },
        )

        summary = AgentFeedbackRetryService()._build_run_summary_payload(
            market_date=market_date,
            request_payload={'request_run_id': state.run_id},
            state=state,
            lifecycle_stage='finished',
        )

        self.assertEqual(
            summary['reused_symbols_by_role'],
            {'eod-summarizer': ['000660', '005930']},
        )
        self.assertEqual(
            summary['reused_feedback_evidence_by_role']['eod-summarizer'][
                '005930'
            ]['feedback_run_id'],
            'reuse-source-005930',
        )

    def test_role_snapshot_attests_same_day_terminal_feedback_as_reused(self):
        service = AgentFeedbackRetryService()
        role = 'signal-decider'
        parent_started_at = datetime(2026, 4, 16, 6, 0, tzinfo=timezone.utc)

        def load_terminal_symbols(**kwargs):
            if kwargs.get('required_parent_run_id'):
                return ({}, {})
            return ({role: {'005930'}}, {})

        with (
            patch.object(
                service,
                '_load_same_day_terminal_symbols',
                side_effect=load_terminal_symbols,
            ),
            patch(
                'app.agent_context.retry_service.agent_context_reuse_service.build_latest_decisions',
                return_value={
                    '005930': {
                        role: _decision(
                            should_dispatch=False,
                            latest_feedback_id=17,
                            latest_feedback={
                                'id': 17,
                                'run_id': 'feedback-source-reuse',
                                'received_at': '2026-04-16T05:59:00+00:00',
                            },
                            latest_feedback_received_at='2026-04-16T05:59:00+00:00',
                            latest_decision_snapshot_id=117,
                            latest_decision_snapshot_fingerprint='snapshot-117',
                            latest_decision_snapshot_observed_at='2026-04-16T05:58:00+00:00',
                            reason='reuse_latest_feedback',
                        )
                    }
                },
            ),
        ):
            snapshot = service._build_role_sequence_snapshot(
                market_date=date.fromisoformat('2026-04-16'),
                roles=(role,),
                eligible_symbols=('005930',),
                reuse_existing=True,
                parent_run_id='feedback-parent-reuse',
                parent_started_at=parent_started_at,
            )

        role_state = snapshot.role_states[role]
        self.assertEqual(role_state.completed_symbols, ('005930',))
        self.assertEqual(role_state.reused_symbols, ('005930',))
        self.assertEqual(
            role_state.reused_feedback_evidence['005930']['feedback_run_id'],
            'feedback-source-reuse',
        )
        self.assertEqual(role_state.boundary_pending_symbols, ())

    def test_role_snapshot_does_not_reuse_stale_same_day_feedback(self):
        service = AgentFeedbackRetryService()
        role = 'signal-decider'

        def load_terminal_symbols(**kwargs):
            if kwargs.get('required_parent_run_id'):
                return ({}, {})
            return ({role: {'005930'}}, {})

        with (
            patch.object(
                service,
                '_load_same_day_terminal_symbols',
                side_effect=load_terminal_symbols,
            ),
            patch(
                'app.agent_context.retry_service.agent_context_reuse_service.build_latest_decisions',
                return_value={
                    '005930': {
                        role: _decision(
                            should_dispatch=True,
                            latest_feedback_received_at='2026-04-16T05:00:00+00:00',
                            reason='source_updated',
                        )
                    }
                },
            ),
        ):
            snapshot = service._build_role_sequence_snapshot(
                market_date=date.fromisoformat('2026-04-16'),
                roles=(role,),
                eligible_symbols=('005930',),
                reuse_existing=True,
                parent_run_id='feedback-parent-stale',
                parent_started_at=datetime(
                    2026,
                    4,
                    16,
                    6,
                    0,
                    tzinfo=timezone.utc,
                ),
            )

        role_state = snapshot.role_states[role]
        self.assertEqual(role_state.completed_symbols, ())
        self.assertEqual(role_state.reused_symbols, ())
        self.assertEqual(role_state.boundary_pending_symbols, ('005930',))

    def test_role_snapshot_rejects_reuse_from_parent_older_than_latest_authority(self):
        service = AgentFeedbackRetryService()
        role = 'signal-decider'
        key = ('005930', role)

        def load_terminal_symbols(**kwargs):
            if kwargs.get('required_parent_run_id'):
                return ({}, {})
            return ({role: {'005930'}}, {})

        with (
            patch.object(
                service,
                '_load_same_day_terminal_symbols',
                side_effect=load_terminal_symbols,
            ),
            patch.object(
                service,
                '_load_reuse_lineage_context',
                return_value=(
                    {key: ('parent-two', 'failed')},
                    {'late-child-one': 'parent-one'},
                    {
                        ('parent-two', '005930', role): (
                            'failed-child-two',
                            'failed',
                        )
                    },
                ),
            ),
            patch(
                'app.agent_context.retry_service.agent_context_reuse_service.build_latest_decisions',
                return_value={
                    '005930': {
                        role: _decision(
                            should_dispatch=False,
                            latest_feedback_id=19,
                            latest_feedback={
                                'id': 19,
                                'run_id': 'late-child-one',
                                'received_at': '2026-04-16T05:59:00+00:00',
                            },
                            latest_feedback_received_at='2026-04-16T05:59:00+00:00',
                            latest_decision_snapshot_id=119,
                            latest_decision_snapshot_fingerprint='snapshot-119',
                            latest_decision_snapshot_observed_at='2026-04-16T05:58:00+00:00',
                            reason='reuse_latest_feedback',
                        )
                    }
                },
            ),
        ):
            snapshot = service._build_role_sequence_snapshot(
                market_date=date.fromisoformat('2026-04-16'),
                roles=(role,),
                eligible_symbols=('005930',),
                reuse_existing=True,
                parent_run_id='parent-three',
                parent_started_at=datetime(
                    2026, 4, 16, 6, 0, tzinfo=timezone.utc
                ),
            )

        role_state = snapshot.role_states[role]
        self.assertEqual(role_state.completed_symbols, ())
        self.assertEqual(role_state.reused_symbols, ())
        self.assertEqual(role_state.boundary_pending_symbols, ('005930',))

    def test_role_snapshot_rejects_older_sibling_from_latest_parent(self):
        service = AgentFeedbackRetryService()
        role = 'signal-decider'
        key = ('005930', role)

        with (
            patch.object(
                service,
                '_load_same_day_terminal_symbols',
                side_effect=lambda **kwargs: (
                    ({}, {})
                    if kwargs.get('required_parent_run_id')
                    else ({role: {'005930'}}, {})
                ),
            ),
            patch.object(
                service,
                '_load_reuse_lineage_context',
                return_value=(
                    {key: ('parent-two', 'completed')},
                    {
                        'child-a': 'parent-two',
                        'child-b': 'parent-two',
                    },
                    {
                        ('parent-two', '005930', role): (
                            'child-b',
                            'completed',
                        )
                    },
                ),
            ),
            patch(
                'app.agent_context.retry_service.agent_context_reuse_service.build_latest_decisions',
                return_value={
                    '005930': {
                        role: _decision(
                            should_dispatch=False,
                            latest_feedback_id=20,
                            latest_feedback={
                                'id': 20,
                                'run_id': 'child-a',
                                'received_at': '2026-04-16T05:59:00+00:00',
                            },
                            latest_feedback_received_at='2026-04-16T05:59:00+00:00',
                            latest_decision_snapshot_id=120,
                            latest_decision_snapshot_fingerprint='snapshot-120',
                            latest_decision_snapshot_observed_at='2026-04-16T05:58:00+00:00',
                            reason='reuse_latest_feedback',
                        )
                    }
                },
            ),
        ):
            snapshot = service._build_role_sequence_snapshot(
                market_date=date.fromisoformat('2026-04-16'),
                roles=(role,),
                eligible_symbols=('005930',),
                reuse_existing=True,
                parent_run_id='parent-three',
                parent_started_at=datetime(
                    2026, 4, 16, 6, 0, tzinfo=timezone.utc
                ),
            )

        role_state = snapshot.role_states[role]
        self.assertEqual(role_state.completed_symbols, ())
        self.assertEqual(role_state.reused_symbols, ())
        self.assertEqual(role_state.boundary_pending_symbols, ('005930',))

    def test_role_snapshot_reuses_latest_lineage_success_over_older_global_failure(self):
        service = AgentFeedbackRetryService()
        role = 'signal-decider'
        key = ('005930', role)

        with (
            patch.object(
                service,
                '_load_same_day_terminal_symbols',
                side_effect=lambda **kwargs: (
                    ({}, {})
                    if kwargs.get('required_parent_run_id')
                    else ({}, {role: {'005930'}})
                ),
            ),
            patch.object(
                service,
                '_load_reuse_lineage_context',
                return_value=(
                    {key: ('latest-parent', 'completed')},
                    {'latest-child': 'latest-parent'},
                    {
                        ('latest-parent', '005930', role): (
                            'latest-child',
                            'completed',
                        )
                    },
                ),
            ),
            patch(
                'app.agent_context.retry_service.agent_context_reuse_service.build_latest_decisions',
                return_value={
                    '005930': {
                        role: _decision(
                            should_dispatch=True,
                            latest_feedback_id=22,
                            latest_feedback={
                                'id': 22,
                                'run_id': 'latest-child',
                                'received_at': '2026-04-16T05:59:00+00:00',
                            },
                            latest_feedback_received_at=(
                                '2026-04-16T05:59:00+00:00'
                            ),
                            latest_decision_snapshot_id=122,
                            latest_decision_snapshot_fingerprint=(
                                'snapshot-122'
                            ),
                            latest_decision_snapshot_observed_at=(
                                '2026-04-16T05:58:00+00:00'
                            ),
                            reason='latest_failure_pending_retry',
                        )
                    }
                },
            ),
        ):
            snapshot = service._build_role_sequence_snapshot(
                market_date=date.fromisoformat('2026-04-16'),
                roles=(role,),
                eligible_symbols=('005930',),
                reuse_existing=True,
                parent_run_id='current-parent',
                parent_started_at=datetime(
                    2026, 4, 16, 6, 0, tzinfo=timezone.utc
                ),
            )

        role_state = snapshot.role_states[role]
        self.assertEqual(role_state.completed_symbols, ('005930',))
        self.assertEqual(role_state.reused_symbols, ('005930',))
        self.assertEqual(role_state.failed_symbols, ())
        self.assertEqual(role_state.boundary_pending_symbols, ())

    def test_role_snapshot_never_reuses_feedback_from_failed_child(self):
        service = AgentFeedbackRetryService()
        role = 'signal-decider'
        key = ('005930', role)

        with (
            patch.object(
                service,
                '_load_same_day_terminal_symbols',
                side_effect=lambda **kwargs: (
                    ({}, {})
                    if kwargs.get('required_parent_run_id')
                    else ({role: {'005930'}}, {})
                ),
            ),
            patch.object(
                service,
                '_load_reuse_lineage_context',
                return_value=(
                    {key: ('prior-parent', 'failed')},
                    {'failed-child': 'prior-parent'},
                    {
                        ('prior-parent', '005930', role): (
                            'failed-child',
                            'failed',
                        )
                    },
                ),
            ),
            patch(
                'app.agent_context.retry_service.agent_context_reuse_service.build_latest_decisions',
                return_value={
                    '005930': {
                        role: _decision(
                            should_dispatch=False,
                            latest_feedback_id=21,
                            latest_feedback={
                                'id': 21,
                                'run_id': 'failed-child',
                                'received_at': (
                                    '2026-04-16T05:59:00+00:00'
                                ),
                            },
                            latest_feedback_received_at=(
                                '2026-04-16T05:59:00+00:00'
                            ),
                            latest_decision_snapshot_id=121,
                            latest_decision_snapshot_fingerprint=(
                                'snapshot-121'
                            ),
                            latest_decision_snapshot_observed_at=(
                                '2026-04-16T05:58:00+00:00'
                            ),
                            reason='reuse_latest_feedback',
                        )
                    }
                },
            ),
        ):
            snapshot = service._build_role_sequence_snapshot(
                market_date=date.fromisoformat('2026-04-16'),
                roles=(role,),
                eligible_symbols=('005930',),
                reuse_existing=True,
                parent_run_id='current-parent',
                parent_started_at=datetime(
                    2026, 4, 16, 6, 0, tzinfo=timezone.utc
                ),
            )

        role_state = snapshot.role_states[role]
        self.assertEqual(role_state.completed_symbols, ())
        self.assertEqual(role_state.reused_symbols, ())
        self.assertEqual(role_state.boundary_pending_symbols, ('005930',))

    def test_role_snapshot_does_not_reuse_feedback_received_after_parent_started(self):
        service = AgentFeedbackRetryService()
        role = 'signal-decider'

        def load_terminal_symbols(**kwargs):
            if kwargs.get('required_parent_run_id'):
                return ({}, {})
            return ({role: {'005930'}}, {})

        with (
            patch.object(
                service,
                '_load_same_day_terminal_symbols',
                side_effect=load_terminal_symbols,
            ),
            patch(
                'app.agent_context.retry_service.agent_context_reuse_service.build_latest_decisions',
                return_value={
                    '005930': {
                        role: _decision(
                            should_dispatch=False,
                            latest_feedback_id=18,
                            latest_feedback={
                                'id': 18,
                                'run_id': 'feedback-source-after-cutoff',
                                'received_at': '2026-04-16T06:01:00+00:00',
                            },
                            latest_feedback_received_at='2026-04-16T06:01:00+00:00',
                            latest_decision_snapshot_id=118,
                            latest_decision_snapshot_fingerprint='snapshot-118',
                            latest_decision_snapshot_observed_at='2026-04-16T05:58:00+00:00',
                            reason='reuse_latest_feedback',
                        )
                    }
                },
            ),
        ):
            snapshot = service._build_role_sequence_snapshot(
                market_date=date.fromisoformat('2026-04-16'),
                roles=(role,),
                eligible_symbols=('005930',),
                reuse_existing=True,
                parent_run_id='feedback-parent-cutoff',
                parent_started_at=datetime(
                    2026,
                    4,
                    16,
                    6,
                    0,
                    tzinfo=timezone.utc,
                ),
            )

        role_state = snapshot.role_states[role]
        self.assertEqual(role_state.completed_symbols, ())
        self.assertEqual(role_state.reused_symbols, ())
        self.assertEqual(role_state.boundary_pending_symbols, ('005930',))

    def test_short_term_decider_keeps_ready_current_parent_failure_retryable_over_late_older_success(self):
        service = AgentFeedbackRetryService()
        role = 'short-term-decider'
        parent_run_id = 'feedback-parent-current-failure'
        parent_started_at = datetime(2026, 4, 16, 6, 0, tzinfo=timezone.utc)

        def load_terminal_symbols(**kwargs):
            if kwargs.get('required_parent_run_id') == parent_run_id:
                return ({}, {role: {'005930'}})
            return ({role: {'005930'}}, {})

        with (
            patch.object(
                service,
                '_load_same_day_terminal_symbols',
                side_effect=load_terminal_symbols,
            ),
            patch(
                'app.agent_context.retry_service.agent_context_reuse_service.build_latest_decisions',
                return_value={
                    '005930': {
                        role: _decision(
                            should_dispatch=False,
                            latest_feedback_received_at='2026-04-16T06:01:00+00:00',
                            reason='reuse_latest_feedback',
                        )
                    }
                },
            ),
            patch.object(
                service,
                '_filter_decider_retry_ready_symbols',
                return_value=('005930',),
            ) as filter_ready,
            patch.object(
                service,
                '_load_reuse_lineage_context',
                return_value=({}, {}, {}),
            ),
        ):
            snapshot = service._build_role_sequence_snapshot(
                market_date=date.fromisoformat('2026-04-16'),
                roles=(role,),
                eligible_symbols=('005930',),
                reuse_existing=True,
                parent_run_id=parent_run_id,
                parent_started_at=parent_started_at,
            )

        role_state = snapshot.role_states[role]
        self.assertEqual(role_state.completed_symbols, ())
        self.assertEqual(role_state.reused_symbols, ())
        self.assertEqual(role_state.failed_symbols, ('005930',))
        self.assertEqual(role_state.terminal_symbols, ('005930',))
        self.assertEqual(role_state.boundary_pending_symbols, ())
        self.assertEqual(role_state.retry_pending_symbols, ('005930',))
        self.assertEqual(role_state.boundary_status, 'failed')
        filter_ready.assert_called_once()

    def test_retryable_dependency_failure_bypasses_decider_reuse_gate_but_terminal_failure_does_not(
        self,
    ):
        service = AgentFeedbackRetryService()
        role = 'short-term-decider'
        retryable_symbol = '005930'
        terminal_symbol = '000660'
        completed_symbol = '035420'
        symbols = (
            retryable_symbol,
            terminal_symbol,
            completed_symbol,
        )
        terminal_state = _SameDayTerminalSymbols(
            completed_by_role={role: {completed_symbol}},
            failed_by_role={role: {retryable_symbol, terminal_symbol}},
            retryable_failed_by_role={role: {retryable_symbol}},
            dependency_changed_retryable_failed_by_role={
                role: {retryable_symbol}
            },
        )

        with (
            patch.object(
                service,
                '_load_same_day_terminal_symbols',
                return_value=terminal_state,
            ),
            patch.object(
                service,
                '_filter_decider_retry_ready_symbols',
                return_value=(),
            ) as filter_ready,
        ):
            snapshot = service._build_role_sequence_snapshot(
                market_date=date.fromisoformat('2026-04-16'),
                roles=(role,),
                eligible_symbols=symbols,
                reuse_existing=False,
                parent_run_id='retry-parent',
            )

        role_state = snapshot.role_states[role]
        self.assertEqual(
            role_state.failed_symbols,
            (retryable_symbol, terminal_symbol),
        )
        self.assertEqual(
            role_state.retry_pending_symbols,
            (retryable_symbol,),
        )
        filter_ready.assert_not_called()

    def test_role_snapshot_force_recompute_counts_only_current_parent_feedback(self):
        service = AgentFeedbackRetryService()
        role = 'signal-decider'
        parent_run_id = 'feedback-parent-force-recompute'

        def load_terminal_symbols(**kwargs):
            if kwargs.get('required_parent_run_id') == parent_run_id:
                return ({role: {'005930'}}, {})
            return ({role: {'005930', '000660'}}, {})

        with (
            patch.object(
                service,
                '_load_same_day_terminal_symbols',
                side_effect=load_terminal_symbols,
            ),
            patch(
                'app.agent_context.retry_service.agent_context_reuse_service.build_latest_decisions',
                return_value={},
            ),
        ):
            snapshot = service._build_role_sequence_snapshot(
                market_date=date.fromisoformat('2026-04-16'),
                roles=(role,),
                eligible_symbols=('005930', '000660'),
                force_recompute=True,
                reuse_existing=True,
                parent_run_id=parent_run_id,
                parent_started_at=datetime(
                    2026,
                    4,
                    16,
                    6,
                    0,
                    tzinfo=timezone.utc,
                ),
            )

        role_state = snapshot.role_states[role]
        self.assertEqual(role_state.completed_symbols, ('005930',))
        self.assertEqual(role_state.reused_symbols, ())
        self.assertEqual(role_state.boundary_pending_symbols, ('000660',))

    def test_final_status_keeps_exhausted_terminal_failures_visible(self):
        symbols = ('005930', '000660')
        snapshot = _snapshot(
            symbols=symbols,
            role_states={
                'signal-decider': _role_state(
                    role='signal-decider',
                    symbols=symbols,
                    completed=('005930',),
                    failed=('000660',),
                )
            },
        )

        status = AgentFeedbackRetryService._resolve_final_status(
            eligible_symbols=symbols,
            snapshot=snapshot,
            retry_pending_symbols=(),
            boundary_pending_roles=(),
        )

        self.assertEqual(status, 'partial')

    def test_final_status_is_failed_when_every_terminal_item_failed(self):
        symbols = ('005930', '000660')
        snapshot = _snapshot(
            symbols=symbols,
            role_states={
                'signal-decider': _role_state(
                    role='signal-decider',
                    symbols=symbols,
                    failed=symbols,
                )
            },
        )

        status = AgentFeedbackRetryService._resolve_final_status(
            eligible_symbols=symbols,
            snapshot=snapshot,
            retry_pending_symbols=(),
            boundary_pending_roles=(),
        )

        self.assertEqual(status, 'failed')
    def setUp(self):
        self.prerequisite_patcher = patch(
            'app.agent_context.retry_service.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
            side_effect=lambda **_: _feedback_prerequisites(),
        )
        self.prerequisite_patcher.start()
        self.addCleanup(self.prerequisite_patcher.stop)

    def test_load_incomplete_symbols_keeps_stale_feedback_retryable(self):
        service = AgentFeedbackRetryService()

        decisions = {
            '005930': {
                'eod-summarizer': _decision(should_dispatch=False),
                'technical-summarizer': _decision(should_dispatch=True),
                'fundamental-summarizer': _decision(should_dispatch=False),
            }
        }

        with patch(
            'app.agent_context.retry_service.load_agent_feedback_eligible_symbols',
            return_value=['005930'],
        ):
            with patch(
                'app.agent_context.retry_service.agent_context_reuse_service.build_latest_decisions',
                return_value=decisions,
            ):
                result = service.load_incomplete_symbols(
                    market_date=date.fromisoformat('2026-04-16'),
                    roles=EXPECTED_ROLES,
                )

        self.assertEqual(result, ['005930'])

    def test_load_incomplete_symbols_drops_freshly_complete_symbols(self):
        service = AgentFeedbackRetryService()

        decisions = {
            '005930': {
                role: _decision(should_dispatch=False)
                for role in EXPECTED_ROLES
            }
        }

        with patch(
            'app.agent_context.retry_service.load_agent_feedback_eligible_symbols',
            return_value=['005930'],
        ):
            with patch(
                'app.agent_context.retry_service.agent_context_reuse_service.build_latest_decisions',
                return_value=decisions,
            ):
                result = service.load_incomplete_symbols(
                    market_date=date.fromisoformat('2026-04-16'),
                    roles=EXPECTED_ROLES,
                )

        self.assertEqual(result, [])

    def test_filter_decider_retry_ready_symbols_waits_for_upstream_summaries(self):
        service = AgentFeedbackRetryService()
        market_date = date.fromisoformat('2026-04-16')
        decisions = {
            '005930': {
                'eod-summarizer': _decision(should_dispatch=False, latest_feedback={'market_date': '2026-04-16'}),
                'technical-summarizer': _decision(should_dispatch=True, latest_feedback={'market_date': '2026-04-16'}),
                'fundamental-summarizer': _decision(should_dispatch=False, latest_feedback={'market_date': '2026-04-15'}),
            },
            '000660': {
                'eod-summarizer': _decision(should_dispatch=False, latest_feedback={'market_date': '2026-04-16'}),
                'technical-summarizer': _decision(should_dispatch=False, latest_feedback={'market_date': '2026-04-16'}),
                'fundamental-summarizer': _decision(should_dispatch=True, latest_feedback=None),
            },
            '035420': {
                'eod-summarizer': _decision(
                    should_dispatch=False,
                    latest_feedback={
                        'market_date': '2026-04-16',
                        'feedback_payload': {'summary': 'ready'},
                    },
                ),
                'technical-summarizer': _decision(
                    should_dispatch=False,
                    latest_feedback={
                        'market_date': '2026-04-16',
                        'feedback_payload': {'summary': 'ready'},
                    },
                ),
                'fundamental-summarizer': _decision(
                    should_dispatch=False,
                    latest_feedback={
                        'market_date': '2026-04-15',
                        'feedback_payload': {'summary': 'reusable'},
                    },
                    reason='reuse_unchanged_fundamental_inputs',
                ),
            },
        }

        with patch(
            'app.agent_context.retry_service.agent_context_reuse_service.build_latest_decisions',
            return_value=decisions,
        ) as build_latest_decisions:
            result = service._filter_decider_retry_ready_symbols(
                market_date=market_date,
                symbols=('005930', '000660', '035420'),
            )

        self.assertEqual(result, ('035420',))
        build_latest_decisions.assert_called_once_with(
            market_date=market_date,
            symbols=['005930', '000660', '035420'],
            roles=SUMMARIZER_AGENT_NAMES,
            force_recompute=False,
            fs_div='CFS',
        )

    def test_same_day_terminal_symbols_require_matching_knowledge_cutoff(self):
        service = AgentFeedbackRetryService()
        market_date = date.fromisoformat('2026-04-16')
        feedback_rows = [
            SimpleNamespace(
                id=1,
                symbol='005930',
                agent_name='eod-summarizer',
                run_id='matching-run',
                received_at=datetime(
                    2026,
                    4,
                    16,
                    6,
                    31,
                    tzinfo=timezone.utc,
                ),
            ),
            SimpleNamespace(
                id=2,
                symbol='000660',
                agent_name='eod-summarizer',
                run_id='stale-run',
                received_at=datetime(
                    2026,
                    4,
                    16,
                    6,
                    32,
                    tzinfo=timezone.utc,
                ),
            ),
        ]
        cutoff_run_rows = [
            SimpleNamespace(
                run_id='matching-run',
                request_payload={
                    'knowledge_cutoff_at': (
                        '2026-04-16T06:30:00+00:00'
                    )
                },
            ),
            SimpleNamespace(
                run_id='stale-run',
                request_payload={
                    'knowledge_cutoff_at': (
                        '2026-04-16T05:30:00+00:00'
                    )
                },
            ),
        ]
        session = MagicMock()
        session.execute.side_effect = [
            MagicMock(all=lambda: feedback_rows),
            MagicMock(all=lambda: []),
            MagicMock(all=lambda: cutoff_run_rows),
        ]

        with patch(
            'app.agent_context.retry_service.session_scope'
        ) as session_scope_mock:
            session_scope_mock.return_value.__enter__.return_value = session
            completed, failed = service._load_same_day_terminal_symbols(
                market_date=market_date,
                roles=('eod-summarizer',),
                symbols=('005930', '000660'),
                knowledge_cutoff_at='2026-04-16T06:30:00+00:00',
            )

        self.assertEqual(completed['eod-summarizer'], {'005930'})
        self.assertEqual(failed['eod-summarizer'], set())

    def test_same_day_terminal_symbols_preserve_failure_retry_semantics(self):
        service = AgentFeedbackRetryService()
        market_date = date.fromisoformat('2026-04-16')
        role = 'short-term-decider'
        failure_rows = [
            SimpleNamespace(
                id=1,
                symbol='005930',
                agent_name=role,
                run_id='retryable-child',
                failure_kind=(
                    'dependency_upstream_changed_during_dispatch'
                ),
                retryable=True,
                received_at=datetime(
                    2026,
                    4,
                    16,
                    6,
                    31,
                    tzinfo=timezone.utc,
                ),
            ),
            SimpleNamespace(
                id=2,
                symbol='000660',
                agent_name=role,
                run_id='terminal-child',
                failure_kind='response_contract_invalid',
                retryable=False,
                received_at=datetime(
                    2026,
                    4,
                    16,
                    6,
                    32,
                    tzinfo=timezone.utc,
                ),
            ),
        ]
        session = MagicMock()
        session.execute.side_effect = [
            MagicMock(all=lambda: []),
            MagicMock(all=lambda: failure_rows),
        ]

        with patch(
            'app.agent_context.retry_service.session_scope'
        ) as session_scope_mock:
            session_scope_mock.return_value.__enter__.return_value = session
            terminal_state = service._load_same_day_terminal_symbols(
                market_date=market_date,
                roles=(role,),
                symbols=('005930', '000660'),
            )

        self.assertEqual(
            terminal_state.failed_by_role[role],
            {'005930', '000660'},
        )
        self.assertEqual(
            terminal_state.retryable_failed_by_role[role],
            {'005930'},
        )
        self.assertEqual(
            terminal_state.dependency_changed_retryable_failed_by_role[role],
            {'005930'},
        )

    def test_automatic_decider_filter_revalidates_same_day_and_prior_day_fundamentals(self):
        service = AgentFeedbackRetryService()
        completed_by_role = {
            'eod-summarizer': {'005930', '000660'},
            'technical-summarizer': {'005930', '000660'},
            'fundamental-summarizer': {'005930'},
        }
        with (
            patch.object(
                service,
                '_load_same_day_terminal_symbols',
                return_value=(completed_by_role, {}),
            ) as load_terminal,
            patch.object(
                service,
                '_filter_decider_retry_ready_symbols',
                return_value=('000660',),
            ) as filter_upstream,
        ):
            result = (
                service._filter_automatic_decider_upstream_ready_symbols(
                    market_date=date.fromisoformat('2026-04-16'),
                    symbols=('005930', '000660', '035420'),
                    knowledge_cutoff_at='2026-04-16T06:30:00+00:00',
                    fs_div='CFS',
                )
            )

        self.assertEqual(result, ('000660',))
        load_terminal.assert_called_once_with(
            market_date=date.fromisoformat('2026-04-16'),
            roles=SUMMARIZER_AGENT_NAMES,
            symbols=['005930', '000660', '035420'],
            knowledge_cutoff_at='2026-04-16T06:30:00+00:00',
        )
        filter_upstream.assert_called_once_with(
            market_date=date.fromisoformat('2026-04-16'),
            symbols=('005930', '000660'),
            knowledge_cutoff_at='2026-04-16T06:30:00+00:00',
            fs_div='CFS',
            reuse_static_inputs=None,
        )

    def test_automatic_decider_filter_revalidates_same_day_fundamental_fingerprint(self):
        service = AgentFeedbackRetryService()
        market_date = date.fromisoformat('2026-04-16')
        symbol = '289040'
        completed_by_role = {
            role: {symbol}
            for role in SUMMARIZER_AGENT_NAMES
        }
        decisions = {
            symbol: {
                'eod-summarizer': _decision(
                    should_dispatch=False,
                    latest_feedback={
                        'market_date': market_date.isoformat(),
                        'feedback_payload': {'summary': 'ready'},
                    },
                ),
                'technical-summarizer': _decision(
                    should_dispatch=False,
                    latest_feedback={
                        'market_date': market_date.isoformat(),
                        'feedback_payload': {'summary': 'ready'},
                    },
                ),
                'fundamental-summarizer': _decision(
                    should_dispatch=True,
                    latest_feedback={
                        'market_date': market_date.isoformat(),
                        'feedback_payload': {'summary': 'legacy summary'},
                    },
                    reason='stored_fundamental_input_fingerprint_missing',
                ),
            },
        }
        reuse_static_inputs = FeedbackReuseStaticInputs()

        with (
            patch.object(
                service,
                '_load_same_day_terminal_symbols',
                return_value=(completed_by_role, {}),
            ),
            patch(
                'app.agent_context.retry_service.'
                'agent_context_reuse_service.build_latest_decisions',
                return_value=decisions,
            ) as build_latest_decisions,
        ):
            result = (
                service._filter_automatic_decider_upstream_ready_symbols(
                    market_date=market_date,
                    symbols=(symbol,),
                    knowledge_cutoff_at='2026-04-16T06:30:00+00:00',
                    fs_div='CFS',
                    reuse_static_inputs=reuse_static_inputs,
                )
            )

        self.assertEqual(result, ())
        build_latest_decisions.assert_called_once_with(
            market_date=market_date,
            symbols=[symbol],
            roles=SUMMARIZER_AGENT_NAMES,
            force_recompute=False,
            fs_div='CFS',
            knowledge_cutoff_at='2026-04-16T06:30:00+00:00',
            static_inputs=reuse_static_inputs,
        )

    def test_load_incomplete_symbols_uses_feedback_eligible_universe(self):
        service = AgentFeedbackRetryService()
        decisions = {
            '005930': {
                role: _decision(should_dispatch=True)
                for role in EXPECTED_ROLES
            }
        }

        with patch(
            'app.agent_context.retry_service.load_agent_feedback_eligible_symbols',
            return_value=['005930'],
        ) as load_eligible:
            with patch(
                'app.agent_context.retry_service.agent_context_reuse_service.build_latest_decisions',
                return_value=decisions,
            ) as build_decisions:
                result = service.load_incomplete_symbols(
                    market_date=date.fromisoformat('2026-04-16'),
                    roles=EXPECTED_ROLES,
                )

        load_eligible.assert_called_once_with(
            market_date=date.fromisoformat('2026-04-16'),
            requested_symbols=None,
        )
        build_decisions.assert_called_once_with(
            market_date=date.fromisoformat('2026-04-16'),
            symbols=['005930'],
            roles=EXPECTED_ROLES,
            force_recompute=False,
            fs_div='CFS',
        )
        self.assertEqual(result, ['005930'])

    def test_load_eligible_symbols_respects_requested_symbol_subset_and_order(self):
        service = AgentFeedbackRetryService()

        with patch(
            'app.agent_context.retry_service.load_agent_feedback_eligible_symbols',
            return_value=['035420', '005930'],
        ) as load_eligible:
            result = service._load_eligible_symbols(
                market_date=date.fromisoformat('2026-04-16'),
                requested_symbols=('035420', '005930', '114630'),
            )

        load_eligible.assert_called_once_with(
            market_date=date.fromisoformat('2026-04-16'),
            requested_symbols=('035420', '005930', '114630'),
        )
        self.assertEqual(result, ['035420', '005930'])

    def test_parse_args_uses_env_market_date_default(self):
        with patch.dict('os.environ', {'FEEDBACK_RETRY_MARKET_DATE': '2026-04-16'}, clear=True):
            with patch('sys.argv', ['run_feedback_retry_cycles.py']):
                args = run_feedback_retry_cycles._parse_args()

        self.assertEqual(args.market_date, date.fromisoformat('2026-04-16'))
        self.assertIsNone(args.symbols)

    def test_parse_args_cli_market_date_overrides_env_default(self):
        with patch.dict('os.environ', {'FEEDBACK_RETRY_MARKET_DATE': '2026-04-15'}, clear=True):
            with patch('sys.argv', ['run_feedback_retry_cycles.py', '--market-date', '2026-04-16']):
                args = run_feedback_retry_cycles._parse_args()

        self.assertEqual(args.market_date, date.fromisoformat('2026-04-16'))

    def test_parse_args_accepts_explicit_symbols(self):
        with patch(
            'sys.argv',
            ['run_feedback_retry_cycles.py', '--symbols', '005930,000660', '035420'],
        ):
            args = run_feedback_retry_cycles._parse_args()

        self.assertEqual(args.symbols, ['005930,000660', '035420'])
        self.assertFalse(args.force_recompute)

    def test_parse_args_accepts_force_recompute(self):
        with patch('sys.argv', ['run_feedback_retry_cycles.py', '--force-recompute']):
            args = run_feedback_retry_cycles._parse_args()

        self.assertTrue(args.force_recompute)

    def test_script_loads_dotenv_before_configured_app_imports(self):
        observed_env_values: list[str | None] = []
        script_path = Path(run_feedback_retry_cycles.__file__).resolve()
        module_name = 'run_feedback_retry_cycles_import_order_test'

        app_module = types.ModuleType('app')
        app_module.__path__ = []
        agent_context_module = types.ModuleType('app.agent_context')
        agent_context_module.__path__ = []
        retry_service_module = types.ModuleType('app.agent_context.retry_service')
        run_modes_module = types.ModuleType('app.agent_context.run_modes')

        class DummyConfig:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        def retry_service_getattr(name):
            if name.startswith('__'):
                raise AttributeError(name)
            observed_env_values.append(os.environ.get('KAI_IMPORT_ORDER_SENTINEL'))
            values = {
                'EXPECTED_ROLES': ('eod-summarizer',),
                'FeedbackRetryCycleConfig': DummyConfig,
                'agent_feedback_retry_service': SimpleNamespace(run_retry_cycles=lambda **_: {}),
            }
            if name in values:
                return values[name]
            raise AttributeError(name)

        def run_modes_getattr(name):
            if name.startswith('__'):
                raise AttributeError(name)
            observed_env_values.append(os.environ.get('KAI_IMPORT_ORDER_SENTINEL'))
            values = {
                'FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH': 'decider_scope_fast_path',
                'FEEDBACK_SCOPE_MODES': ('decider_scope_fast_path',),
            }
            if name in values:
                return values[name]
            raise AttributeError(name)

        retry_service_module.__getattr__ = retry_service_getattr
        run_modes_module.__getattr__ = run_modes_getattr
        fake_modules = {
            'app': app_module,
            'app.agent_context': agent_context_module,
            'app.agent_context.retry_service': retry_service_module,
            'app.agent_context.run_modes': run_modes_module,
        }

        spec = importlib.util.spec_from_file_location(module_name, script_path)
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        module = importlib.util.module_from_spec(spec)

        with (
            patch.dict('os.environ', {}, clear=True),
            patch.dict(sys.modules, fake_modules),
            patch.object(Path, 'exists', return_value=True),
            patch.object(Path, 'read_text', return_value='KAI_IMPORT_ORDER_SENTINEL=loaded\n'),
        ):
            sys.modules[module_name] = module
            try:
                spec.loader.exec_module(module)
            finally:
                sys.modules.pop(module_name, None)

        self.assertTrue(observed_env_values)
        self.assertTrue(all(value == 'loaded' for value in observed_env_values))

    def test_run_retry_cycles_includes_progress_response_fields(self):
        service = AgentFeedbackRetryService()
        run_state_service = RecordingRunStateService()
        progress = {
            'request': {
                'market_date': '2026-04-16',
                'symbols': None,
                'expected_agents': list(EXPECTED_ROLES),
            },
            'summary': {
                'first_feedback_at': '2026-04-16T01:00:00+00:00',
                'elapsed_seconds': 300,
                'completed_feedback_per_minute': 0.6,
                'completed_symbols_per_minute': 0.2,
                'estimated_remaining_seconds': 0,
                'estimated_completion_at': '2026-04-16T01:05:00+00:00',
                'eta_confidence': 'high',
            },
        }

        with patch.object(service, '_load_eligible_symbols', return_value=[]):
            with patch('app.agent_context.retry_service.agent_context_feedback_run_state_service', run_state_service):
                with patch(
                    'app.agent_context.retry_service.agent_context_progress_service.get_feedback_progress',
                    return_value=progress,
                ) as get_progress:
                    result = service.run_retry_cycles(
                        config=SimpleNamespace(
                            market_date=date.fromisoformat('2026-04-16'),
                            roles=EXPECTED_ROLES,
                            max_cycles=3,
                            outer_batch_size=20,
                            reuse_existing=True,
                        )
                    )

        get_progress.assert_called_once_with(
            market_date=date.fromisoformat('2026-04-16'),
            symbols=None,
            expected_agents=EXPECTED_ROLES,
            run_id=ANY,
        )
        self.assertTrue(result['completed'])
        self.assertEqual(result['progress']['summary']['first_feedback_at'], '2026-04-16T01:00:00+00:00')
        self.assertEqual(result['progress']['summary']['completed_feedback_per_minute'], 0.6)
        self.assertEqual(result['progress']['summary']['estimated_completion_at'], '2026-04-16T01:05:00+00:00')

    def test_run_retry_cycles_joins_identical_active_scope_when_admission_is_busy(self):
        service = AgentFeedbackRetryService()
        config = SimpleNamespace(
            market_date=date.fromisoformat('2026-04-16'),
            roles=('signal-decider',),
            symbols=('005930',),
            feedback_scope_mode='decider_scope_fast_path',
            max_cycles=3,
            outer_batch_size=20,
            reuse_existing=True,
            force_recompute=False,
            progress_callback=None,
        )
        active_run = {
            'run_id': 'feedback-cycle-active',
            'status': 'running',
            'request_payload': {
                'requested_symbols': ['005930'],
                'symbols': ['005930'],
                'roles': ['signal-decider'],
                'mode': 'daily_retry_cycle_role_sequenced',
                'feedback_scope_mode': 'decider_scope_fast_path',
            },
        }
        run_state_service = RecordingRunStateService()
        run_state_service.find_latest_run = lambda **_: active_run

        with (
            patch(
                'app.agent_context.retry_service._claim_feedback_retry_run',
                return_value={
                    'acquired': False,
                    'run_id': None,
                    'conflict_run': active_run,
                    'exact_scope': True,
                },
            ),
            patch(
                'app.agent_context.retry_service.agent_context_feedback_run_state_service',
                run_state_service,
            ),
            patch.object(
                service,
                '_run_retry_cycles_admitted',
                create=True,
            ) as admitted_run,
            patch.object(
                service,
                '_get_retry_progress',
                return_value={'summary': {'pending_symbols': 1}},
            ),
        ):
            result = service.run_retry_cycles(config=config)

        admitted_run.assert_not_called()
        self.assertEqual(result['run_id'], 'feedback-cycle-active')
        self.assertEqual(result['status'], 'running')
        self.assertTrue(result['joined_existing_run'])
        self.assertEqual(result['admission_status'], 'active_scope_joined')

    def test_run_retry_cycles_scopes_progress_to_requested_roles(self):
        service = AgentFeedbackRetryService()
        run_state_service = RecordingRunStateService()
        progress = {
            'request': {
                'market_date': '2026-04-16',
                'symbols': None,
                'expected_agents': ['technical-summarizer'],
            },
            'summary': {
                'completed_symbols': 10,
                'expected_feedback_count': 10,
                'estimated_remaining_seconds': 0,
            },
        }

        with patch.object(service, '_load_eligible_symbols', return_value=[]):
            with patch('app.agent_context.retry_service.agent_context_feedback_run_state_service', run_state_service):
                with patch(
                    'app.agent_context.retry_service.agent_context_progress_service.get_feedback_progress',
                    return_value=progress,
                ) as get_progress:
                    result = service.run_retry_cycles(
                        config=SimpleNamespace(
                            market_date=date.fromisoformat('2026-04-16'),
                            roles=('technical-summarizer',),
                            max_cycles=3,
                            outer_batch_size=20,
                            reuse_existing=True,
                        )
                    )

        get_progress.assert_called_once_with(
            market_date=date.fromisoformat('2026-04-16'),
            symbols=None,
            expected_agents=('technical-summarizer',),
            run_id=ANY,
        )
        self.assertEqual(result['progress']['request']['expected_agents'], ['technical-summarizer'])
        self.assertEqual(result['progress']['summary']['expected_feedback_count'], 10)

    def test_run_retry_cycles_preserves_requested_symbol_scope_in_progress_and_run_payload(self):
        service = AgentFeedbackRetryService()
        run_state_service = RecordingRunStateService()
        snapshot = _snapshot(
            symbols=('005930',),
            role_states={
                role: _role_state(
                    role=role,
                    symbols=('005930',),
                    completed=('005930',),
                    reused=('005930',) if role == 'eod-summarizer' else (),
                )
                for role in EXPECTED_ROLES
            },
        )
        progress = {
            'request': {
                'market_date': '2026-04-16',
                'symbols': ['005930'],
                'expected_agents': list(EXPECTED_ROLES),
            },
            'summary': {
                'completed_symbols': 1,
                'expected_feedback_count': 4,
            },
        }

        with patch.object(service, '_load_eligible_symbols', return_value=['005930']):
            with patch.object(service, '_build_role_sequence_snapshot', return_value=snapshot):
                with patch(
                    'app.agent_context.retry_service.agent_context_feedback_run_state_service',
                    run_state_service,
                ):
                    with patch(
                        'app.agent_context.retry_service.agent_context_progress_service.get_feedback_progress',
                        return_value=progress,
                    ) as get_progress:
                        result = service.run_retry_cycles(
                            config=SimpleNamespace(
                                market_date=date.fromisoformat('2026-04-16'),
                                roles=EXPECTED_ROLES,
                                symbols=('005930',),
                                max_cycles=3,
                                outer_batch_size=20,
                                reuse_existing=True,
                            )
                        )

        get_progress.assert_called_once_with(
            market_date=date.fromisoformat('2026-04-16'),
            symbols=['005930'],
            expected_agents=EXPECTED_ROLES,
            run_id=ANY,
        )
        self.assertEqual(run_state_service.started[0]['request_payload']['requested_symbols'], ['005930'])
        self.assertEqual(run_state_service.started[0]['request_payload']['symbols'], ['005930'])
        self.assertEqual(
            run_state_service.started[0]['request_payload']['scope'],
            'requested_symbols_scored_by_strategy_aware_prefilter',
        )
        self.assertEqual(run_state_service.started[0]['request_payload']['feedback_scope_mode'], 'decider_scope_fast_path')
        self.assertEqual(
            run_state_service.finished[0]['summary_payload']['feedback_scope_mode'],
            'decider_scope_fast_path',
        )
        self.assertEqual(
            run_state_service.finished[0]['summary_payload']['reused_symbols_by_role'],
            {'eod-summarizer': ['005930']},
        )
        self.assertEqual(result['feedback_scope_mode'], 'decider_scope_fast_path')
        self.assertEqual(result['requested_symbols'], ['005930'])
        self.assertEqual(result['eligible_symbols'], ['005930'])
        self.assertEqual(result['skipped_symbols'], [])

    def _run_decider_scope_case(
        self,
        *,
        upstream_ready_symbols: tuple[str, ...],
        requested_symbols: tuple[str, ...] | None = None,
    ):
        service = AgentFeedbackRetryService()
        run_state_service = RecordingRunStateService()
        roles = DECISION_SUPPORT_AGENT_NAMES
        eligible_symbols = requested_symbols or (
            '005930',
            '000660',
            '035420',
        )

        def build_completed_snapshot(**kwargs):
            role_scopes = kwargs['role_symbol_scopes']
            snapshot_roles = tuple(kwargs['roles'])
            snapshot_symbols = tuple(kwargs['eligible_symbols'])
            return RoleSequenceSnapshot(
                market_date=kwargs['market_date'],
                symbols=snapshot_symbols,
                roles=snapshot_roles,
                role_states={
                    role: _role_state(
                        role=role,
                        symbols=tuple(role_scopes.get(role, snapshot_symbols)),
                        completed=tuple(role_scopes.get(role, snapshot_symbols)),
                        reused=tuple(role_scopes.get(role, snapshot_symbols)),
                    )
                    for role in snapshot_roles
                },
                role_symbol_scopes={
                    role: tuple(role_scopes.get(role, snapshot_symbols))
                    for role in snapshot_roles
                },
            )
        eligibility_records = [
            {
                'symbol': symbol,
                'candidate_rank': rank,
                'candidate_score': 100 - rank,
                'hard_blocked': False,
                'cap_excluded': False,
                'forced_inclusion_reasons': [],
            }
            for rank, symbol in enumerate(eligible_symbols, start=1)
        ]
        progress = {
            'request': {'expected_agents': list(roles)},
            'summary': {'run_status': 'completed'},
        }

        with (
            patch(
                'app.agent_context.retry_service.load_agent_feedback_eligibility_records',
                return_value=eligibility_records,
            ),
            patch.object(
                service,
                '_filter_automatic_decider_upstream_ready_symbols',
                return_value=upstream_ready_symbols,
            ) as filter_ready,
            patch.object(
                service,
                '_build_role_sequence_snapshot',
                side_effect=build_completed_snapshot,
            ) as build_snapshot,
            patch(
                'app.agent_context.retry_service.agent_context_feedback_run_state_service',
                run_state_service,
            ),
            patch(
                'app.agent_context.retry_service.agent_context_progress_service.get_feedback_progress',
                return_value=progress,
            ),
            patch(
                'app.agent_context.eligibility.settings.openclaw_feedback_role_cap_short_term_decider',
                2,
            ),
            patch(
                'app.agent_context.eligibility.settings.openclaw_feedback_role_cap_signal_decider',
                2,
            ),
        ):
            result = service._run_retry_cycles_admitted(
                config=SimpleNamespace(
                    market_date=date.fromisoformat('2026-04-16'),
                    roles=roles,
                    symbols=requested_symbols,
                    feedback_scope_mode='decider_scope_fast_path',
                    max_cycles=3,
                    outer_batch_size=20,
                    reuse_existing=True,
                    force_recompute=False,
                    progress_callback=None,
                ),
                prerequisites=_feedback_prerequisites(),
                preloaded_eligible_symbols=eligible_symbols,
                effective_knowledge_cutoff_at=datetime(
                    2026,
                    4,
                    16,
                    6,
                    30,
                    tzinfo=timezone.utc,
                ),
                knowledge_cutoff_source='market_close',
            )

        return result, run_state_service, filter_ready, build_snapshot

    def test_auto_decider_scope_promotes_unready_symbol_to_full_role_repair(self):
        upstream_ready_symbols = ('005930',)
        result, run_state_service, filter_ready, build_snapshot = (
            self._run_decider_scope_case(
                upstream_ready_symbols=upstream_ready_symbols,
            )
        )

        filter_ready.assert_called_once_with(
            market_date=date.fromisoformat('2026-04-16'),
            symbols=('005930', '000660'),
            knowledge_cutoff_at=datetime(
                2026,
                4,
                16,
                6,
                30,
                tzinfo=timezone.utc,
            ),
            fs_div='CFS',
            reuse_static_inputs=ANY,
        )
        self.assertIs(
            filter_ready.call_args.kwargs['reuse_static_inputs'],
            build_snapshot.call_args_list[0].kwargs['reuse_static_inputs'],
        )
        request_payload = run_state_service.started[0]['request_payload']
        self.assertEqual(request_payload['roles'], list(EXPECTED_ROLES))
        self.assertEqual(
            request_payload['role_symbol_scopes'],
            {
                'eod-summarizer': ['000660'],
                'technical-summarizer': ['000660'],
                'fundamental-summarizer': ['000660'],
                'short-term-decider': ['005930', '000660'],
                'signal-decider': ['005930', '000660'],
            },
        )
        self.assertEqual(
            request_payload['decider_upstream_admission'],
            {
                'applied': True,
                'candidate_count': 2,
                'ready_count': 1,
                'excluded_count': 0,
                'excluded_symbols': [],
                'scope_preserved': True,
                'fallback_reason': 'full_role_repair',
                'repair_count': 1,
                'repair_symbols': ['000660'],
                'repair_roles': list(EXPECTED_ROLES[:3]),
            },
        )
        self.assertTrue(result['completed'])

    def test_auto_decider_scope_repairs_every_symbol_when_none_are_ready(self):
        result, run_state_service, filter_ready, _ = (
            self._run_decider_scope_case(
                upstream_ready_symbols=(),
            )
        )

        filter_ready.assert_called_once()
        request_payload = run_state_service.started[0]['request_payload']
        self.assertEqual(request_payload['roles'], list(EXPECTED_ROLES))
        self.assertEqual(
            request_payload['role_symbol_scopes'],
            {
                role: ['005930', '000660']
                for role in EXPECTED_ROLES
            },
        )
        self.assertEqual(
            request_payload['decider_upstream_admission'],
            {
                'applied': True,
                'candidate_count': 2,
                'ready_count': 0,
                'excluded_count': 0,
                'excluded_symbols': [],
                'scope_preserved': True,
                'fallback_reason': 'full_role_repair',
                'repair_count': 2,
                'repair_symbols': ['005930', '000660'],
                'repair_roles': list(EXPECTED_ROLES[:3]),
            },
        )
        self.assertTrue(result['completed'])

    def test_explicit_decider_scope_is_not_silently_filtered(self):
        symbol = '005930'
        result, run_state_service, filter_ready, _ = (
            self._run_decider_scope_case(
                upstream_ready_symbols=(symbol,),
                requested_symbols=(symbol,),
            )
        )

        filter_ready.assert_not_called()
        request_payload = run_state_service.started[0]['request_payload']
        self.assertEqual(
            request_payload['role_symbol_scopes'],
            {role: [symbol] for role in DECISION_SUPPORT_AGENT_NAMES},
        )
        self.assertEqual(
            request_payload['decider_upstream_admission'][
                'fallback_reason'
            ],
            'requested_scope',
        )
        self.assertTrue(
            request_payload['decider_upstream_admission'][
                'scope_preserved'
            ]
        )
        self.assertTrue(result['completed'])

    def test_run_retry_cycles_dispatches_role_symbols_in_outer_batches(self):
        service = AgentFeedbackRetryService()
        run_state_service = RecordingRunStateService()
        roles = ('eod-summarizer',)
        symbols = ('000020', '000040', '000050', '000070', '000080')
        snapshots = SnapshotSequence(
            [
                _snapshot(
                    symbols=symbols,
                    role_states={
                        'eod-summarizer': _role_state(
                            role='eod-summarizer',
                            symbols=symbols,
                            boundary_pending=symbols,
                            retry_pending=symbols,
                        )
                    },
                ),
                _snapshot(
                    symbols=symbols,
                    role_states={
                        'eod-summarizer': _role_state(
                            role='eod-summarizer',
                            symbols=symbols,
                            boundary_pending=symbols,
                            retry_pending=symbols,
                        )
                    },
                ),
                _snapshot(
                    symbols=symbols,
                    role_states={
                        'eod-summarizer': _role_state(
                            role='eod-summarizer',
                            symbols=symbols,
                            completed=symbols,
                        )
                    },
                ),
            ]
        )
        dispatch_calls = []

        def dispatch_for_symbols(**kwargs):
            dispatch_calls.append(kwargs)
            return {
                'request': {'roles': kwargs['roles'], 'symbols': kwargs['symbols']},
                'summary': {'status': 'completed', 'completed': len(kwargs['symbols']), 'failed': 0},
            }

        progress = {'request': {'expected_agents': list(roles)}, 'summary': {'cycle_status': 'completed'}}

        with patch.object(service, '_load_eligible_symbols', return_value=list(symbols)):
            with patch.object(service, '_build_role_sequence_snapshot', side_effect=snapshots):
                with patch('app.agent_context.retry_service.agent_context_feedback_run_state_service', run_state_service):
                    with patch(
                        'app.agent_context.retry_service.agent_feedback_orchestrator.dispatch_for_symbols',
                        side_effect=dispatch_for_symbols,
                    ):
                        with patch(
                            'app.agent_context.retry_service.agent_context_progress_service.get_feedback_progress',
                            return_value=progress,
                        ):
                            result = service.run_retry_cycles(
                                config=SimpleNamespace(
                                    market_date=date.fromisoformat('2040-04-16'),
                                    roles=roles,
                                    fs_div='OFS',
                                    max_cycles=1,
                                    outer_batch_size=2,
                                    reuse_existing=True,
                                    knowledge_cutoff_at=None,
                                )
                            )

        self.assertEqual(
            [call['symbols'] for call in dispatch_calls],
            [['000020', '000040'], ['000050', '000070'], ['000080']],
        )
        self.assertEqual(
            [call['role_symbol_scopes']['eod-summarizer'] for call in dispatch_calls],
            [['000020', '000040'], ['000050', '000070'], ['000080']],
        )
        self.assertEqual(
            [call['funnel_diagnostics']['dispatch_chunk'] for call in dispatch_calls],
            [
                {'index': 1, 'total': 3, 'outer_batch_size': 2},
                {'index': 2, 'total': 3, 'outer_batch_size': 2},
                {'index': 3, 'total': 3, 'outer_batch_size': 2},
            ],
        )
        parent_run_id = run_state_service.started[0]['run_id']
        self.assertEqual(
            run_state_service.started[0]['request_payload']['child_run_lineage'],
            {
                'schema_version': 'agent_feedback_child_run_lineage_v1',
                'parent_run_id': parent_run_id,
                'child_link_field': 'parent_run_id',
            },
        )
        self.assertEqual(
            run_state_service.finished[0]['summary_payload']['request'][
                'child_run_lineage'
            ],
            run_state_service.started[0]['request_payload']['child_run_lineage'],
        )
        self.assertTrue(
            all(call['parent_run_id'] == parent_run_id for call in dispatch_calls)
        )
        effective_knowledge_cutoff_at = run_state_service.started[0][
            'request_payload'
        ]['knowledge_cutoff_at']
        self.assertIsNotNone(effective_knowledge_cutoff_at)
        self.assertTrue(
            all(
                call['knowledge_cutoff_at']
                == effective_knowledge_cutoff_at
                for call in dispatch_calls
            )
        )
        self.assertEqual(
            run_state_service.started[0]['request_payload']['fs_div'],
            'OFS',
        )
        self.assertTrue(
            all(call['fs_div'] == 'OFS' for call in dispatch_calls)
        )
        self.assertTrue(result['completed'])
        self.assertEqual(result['cycles'][0]['role_batches'][0]['summary']['chunks'], 3)

    def test_force_recompute_batch_checkpoint_does_not_count_old_rows_as_completed(self):
        service = AgentFeedbackRetryService()
        run_state_service = RecordingRunStateService()
        roles = ('eod-summarizer',)
        symbols = ('000020', '000040', '000050', '000070')
        regenerated_symbols = set()
        dispatch_calls = []
        mid_chunk_snapshot_calls = []

        def build_snapshot(**kwargs):
            if 0 < len(dispatch_calls) < 2:
                mid_chunk_snapshot_calls.append(kwargs)
            if kwargs.get('force_recompute'):
                completed = tuple(symbol for symbol in symbols if symbol in regenerated_symbols)
                pending = tuple(symbol for symbol in symbols if symbol not in regenerated_symbols)
            else:
                completed = symbols
                pending = ()
            return _snapshot(
                symbols=symbols,
                role_states={
                    'eod-summarizer': _role_state(
                        role='eod-summarizer',
                        symbols=symbols,
                        completed=completed,
                        boundary_pending=pending,
                        retry_pending=pending,
                    )
                },
            )

        def dispatch_for_symbols(**kwargs):
            dispatch_calls.append(kwargs)
            regenerated_symbols.update(kwargs['symbols'])
            return {
                'request': {'roles': kwargs['roles'], 'symbols': kwargs['symbols']},
                'summary': {'status': 'completed', 'completed': len(kwargs['symbols']), 'failed': 0},
            }

        progress = {'request': {'expected_agents': list(roles)}, 'summary': {'cycle_status': 'completed'}}

        with patch.object(service, '_load_eligible_symbols', return_value=list(symbols)):
            with patch.object(service, '_build_role_sequence_snapshot', side_effect=build_snapshot):
                with patch('app.agent_context.retry_service.agent_context_feedback_run_state_service', run_state_service):
                    with patch(
                        'app.agent_context.retry_service.agent_feedback_orchestrator.dispatch_for_symbols',
                        side_effect=dispatch_for_symbols,
                    ):
                        with patch(
                            'app.agent_context.retry_service.agent_context_progress_service.get_feedback_progress',
                            return_value=progress,
                        ):
                            service.run_retry_cycles(
                                config=SimpleNamespace(
                                    market_date=date.fromisoformat('2026-04-16'),
                                    roles=roles,
                                    max_cycles=1,
                                    outer_batch_size=2,
                                    reuse_existing=True,
                                    force_recompute=True,
                                )
                            )

        self.assertEqual(
            [call['symbols'] for call in dispatch_calls],
            [['000020', '000040'], ['000050', '000070']],
        )
        self.assertTrue(all(call['reuse_existing'] is False for call in dispatch_calls))
        self.assertTrue(all(call['force_recompute'] is True for call in dispatch_calls))
        self.assertEqual(len(mid_chunk_snapshot_calls), 1)
        self.assertIs(mid_chunk_snapshot_calls[0]['force_recompute'], True)

        batch_checkpoints = [
            checkpoint
            for checkpoint in run_state_service.checkpoints
            if checkpoint['summary_payload']['lifecycle_stage'] == 'role_batch_completed'
        ]
        self.assertEqual(len(batch_checkpoints), 1)
        batch_summary = batch_checkpoints[0]['summary_payload']['progress_summary']
        self.assertEqual(batch_summary['completed_feedback_count'], 2)
        self.assertEqual(batch_summary['pending_symbols'], 2)
        self.assertEqual(
            batch_summary['role_progress']['eod-summarizer']['pending_symbols'],
            2,
        )

    def test_run_retry_cycles_returns_waiting_gate_when_prerequisites_are_not_ready(self):
        service = AgentFeedbackRetryService()
        run_state_service = RecordingRunStateService()

        with (
            patch(
                'app.agent_context.retry_service.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
                side_effect=lambda **_: _feedback_prerequisites(
                    status='waiting',
                    missing_stage_ids=['fundamentals_daily_refresh', 'composite_factors'],
                ),
            ),
            patch(
                'app.agent_context.retry_service.agent_context_feedback_run_state_service',
                run_state_service,
            ),
        ):
            result = service.run_retry_cycles(
                config=SimpleNamespace(
                    market_date=date.fromisoformat('2026-04-16'),
                    roles=EXPECTED_ROLES,
                    max_cycles=3,
                    outer_batch_size=20,
                    reuse_existing=True,
                )
            )

        self.assertTrue(result['gated'])
        self.assertEqual(result['status'], 'waiting')
        self.assertEqual(result['run_id'], None)
        self.assertEqual(run_state_service.started, [])

    def test_run_retry_cycles_bypasses_unready_gate_without_forcing_recompute(self):
        service = AgentFeedbackRetryService()
        run_state_service = RecordingRunStateService()
        progress = {'request': {'expected_agents': ['eod-summarizer']}, 'summary': {'run_status': 'completed'}}

        with (
            patch(
                'app.agent_context.retry_service.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
                side_effect=lambda **_: _feedback_prerequisites(
                    status='waiting',
                    missing_stage_ids=['fundamentals_daily_refresh'],
                ),
            ),
            patch.object(service, '_load_eligible_symbols', return_value=[]),
            patch('app.agent_context.retry_service.load_agent_feedback_eligibility_records', return_value=[]),
            patch(
                'app.agent_context.retry_service.agent_context_feedback_run_state_service',
                run_state_service,
            ),
            patch(
                'app.agent_context.retry_service.agent_context_progress_service.get_feedback_progress',
                return_value=progress,
            ),
        ):
            result = service.run_retry_cycles(
                config=SimpleNamespace(
                    market_date=date.fromisoformat('2026-04-16'),
                    roles=('eod-summarizer',),
                    max_cycles=3,
                    outer_batch_size=20,
                    reuse_existing=True,
                ),
                bypass_prerequisite_gate=True,
            )

        self.assertNotIn('gated', result)
        self.assertFalse(result['force_recompute'])
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(run_state_service.started[0]['request_payload']['force_recompute'], False)

    def test_run_retry_cycles_propagates_gate_bypass_to_child_dispatch(self):
        service = AgentFeedbackRetryService()
        run_state_service = RecordingRunStateService()
        roles = ('eod-summarizer',)
        symbols = ('005930',)
        pending_snapshot = _snapshot(
            symbols=symbols,
            role_states={
                'eod-summarizer': _role_state(
                    role='eod-summarizer',
                    symbols=symbols,
                    boundary_pending=symbols,
                    retry_pending=symbols,
                )
            },
        )
        completed_snapshot = _snapshot(
            symbols=symbols,
            role_states={
                'eod-summarizer': _role_state(
                    role='eod-summarizer',
                    symbols=symbols,
                    completed=symbols,
                )
            },
        )
        snapshots = SnapshotSequence(
            [pending_snapshot, pending_snapshot, completed_snapshot]
        )
        progress = {
            'request': {'expected_agents': list(roles)},
            'summary': {'run_status': 'completed'},
        }

        with (
            patch.object(
                service,
                '_build_role_sequence_snapshot',
                side_effect=snapshots,
            ),
            patch(
                'app.agent_context.retry_service.load_agent_feedback_eligibility_records',
                return_value=[],
            ),
            patch(
                'app.agent_context.retry_service.agent_context_feedback_run_state_service',
                run_state_service,
            ),
            patch(
                'app.agent_context.retry_service.agent_feedback_orchestrator.dispatch_for_symbols',
                return_value={
                    'request': {'roles': list(roles), 'symbols': list(symbols)},
                    'summary': {'status': 'completed', 'completed': 1, 'failed': 0},
                },
            ) as dispatch_child,
            patch(
                'app.agent_context.retry_service.agent_context_progress_service.get_feedback_progress',
                return_value=progress,
            ),
        ):
            result = service._run_retry_cycles_admitted(
                config=SimpleNamespace(
                    market_date=date.fromisoformat('2040-04-17'),
                    roles=roles,
                    max_cycles=1,
                    outer_batch_size=20,
                    reuse_existing=True,
                ),
                bypass_prerequisite_gate=True,
                prerequisites=_feedback_prerequisites(
                    status='waiting',
                    missing_stage_ids=['fundamentals_daily_refresh'],
                ),
                preloaded_eligible_symbols=symbols,
                effective_knowledge_cutoff_at=datetime(
                    2040,
                    4,
                    17,
                    6,
                    30,
                    tzinfo=timezone.utc,
                ),
                knowledge_cutoff_source='market_close',
            )

        dispatch_child.assert_called_once()
        dispatch_kwargs = dispatch_child.call_args.kwargs
        self.assertEqual(dispatch_kwargs['symbols'], ['005930'])
        self.assertIs(dispatch_kwargs['force'], True)
        self.assertIs(dispatch_kwargs['force_recompute'], False)
        self.assertIs(dispatch_kwargs['reuse_existing'], False)
        self.assertTrue(result['completed'])

    def test_run_retry_cycles_persists_role_boundaries_and_advances_past_terminal_failures(self):
        service = AgentFeedbackRetryService()
        run_state_service = RecordingRunStateService()
        roles = ('technical-summarizer', 'fundamental-summarizer')
        symbols = ('005930', '000660')
        snapshots = SnapshotSequence(
            [
                _snapshot(
                    symbols=symbols,
                    role_states={
                        'technical-summarizer': _role_state(
                            role='technical-summarizer',
                            symbols=symbols,
                            boundary_pending=symbols,
                            retry_pending=symbols,
                        ),
                        'fundamental-summarizer': _role_state(
                            role='fundamental-summarizer',
                            symbols=symbols,
                            boundary_pending=symbols,
                            retry_pending=symbols,
                        ),
                    },
                ),
                _snapshot(
                    symbols=symbols,
                    role_states={
                        'technical-summarizer': _role_state(
                            role='technical-summarizer',
                            symbols=symbols,
                            boundary_pending=symbols,
                            retry_pending=symbols,
                        ),
                        'fundamental-summarizer': _role_state(
                            role='fundamental-summarizer',
                            symbols=symbols,
                            boundary_pending=symbols,
                            retry_pending=symbols,
                        ),
                    },
                ),
                _snapshot(
                    symbols=symbols,
                    role_states={
                        'technical-summarizer': _role_state(
                            role='technical-summarizer',
                            symbols=symbols,
                            completed=('005930',),
                            failed=('000660',),
                            retry_pending=('000660',),
                        ),
                        'fundamental-summarizer': _role_state(
                            role='fundamental-summarizer',
                            symbols=symbols,
                            boundary_pending=symbols,
                            retry_pending=symbols,
                        ),
                    },
                ),
                _snapshot(
                    symbols=symbols,
                    role_states={
                        'technical-summarizer': _role_state(
                            role='technical-summarizer',
                            symbols=symbols,
                            completed=('005930',),
                            failed=('000660',),
                            retry_pending=('000660',),
                        ),
                        'fundamental-summarizer': _role_state(
                            role='fundamental-summarizer',
                            symbols=symbols,
                            boundary_pending=symbols,
                            retry_pending=symbols,
                        ),
                    },
                ),
                _snapshot(
                    symbols=symbols,
                    role_states={
                        'technical-summarizer': _role_state(
                            role='technical-summarizer',
                            symbols=symbols,
                            completed=('005930',),
                            failed=('000660',),
                            retry_pending=('000660',),
                        ),
                        'fundamental-summarizer': _role_state(
                            role='fundamental-summarizer',
                            symbols=symbols,
                            completed=symbols,
                        ),
                    },
                ),
            ]
        )
        dispatch_calls = []

        def dispatch_for_symbols(**kwargs):
            dispatch_calls.append(kwargs)
            role = kwargs['roles'][0]
            return {
                'request': {'roles': kwargs['roles'], 'symbols': kwargs['symbols']},
                'summary': {
                    'status': 'partial' if role == 'technical-summarizer' else 'completed',
                    'completed': len(kwargs['symbols']),
                    'failed': 1 if role == 'technical-summarizer' else 0,
                },
            }

        progress = {'request': {'expected_agents': list(roles)}, 'summary': {'cycle_status': 'partial'}}

        with patch.object(service, '_load_eligible_symbols', return_value=list(symbols)):
            with patch.object(service, '_build_role_sequence_snapshot', side_effect=snapshots):
                with patch('app.agent_context.retry_service.agent_context_feedback_run_state_service', run_state_service):
                    with patch(
                        'app.agent_context.retry_service.agent_feedback_orchestrator.dispatch_for_symbols',
                        side_effect=dispatch_for_symbols,
                    ):
                        with patch(
                            'app.agent_context.retry_service.agent_context_progress_service.get_feedback_progress',
                            return_value=progress,
                        ):
                            result = service.run_retry_cycles(
                                config=SimpleNamespace(
                                    market_date=date.fromisoformat('2026-04-16'),
                                    roles=roles,
                                    max_cycles=1,
                                    outer_batch_size=20,
                                    reuse_existing=True,
                                ),
                                force=True,
                            )

        self.assertEqual(
            [(call['roles'][0], call['symbols']) for call in dispatch_calls],
            [('technical-summarizer', ['005930', '000660']), ('fundamental-summarizer', ['005930', '000660'])],
        )
        self.assertTrue(all(call['force'] is True for call in dispatch_calls))
        self.assertTrue(all(call['reuse_existing'] is False for call in dispatch_calls))
        role_completed_checkpoints = [
            checkpoint['summary_payload']
            for checkpoint in run_state_service.checkpoints
            if checkpoint['summary_payload']['lifecycle_stage'] == 'role_completed'
        ]
        self.assertEqual(role_completed_checkpoints[0]['completed_roles'], ['technical-summarizer'])
        self.assertEqual(role_completed_checkpoints[0]['remaining_roles'], ['fundamental-summarizer'])
        self.assertEqual(role_completed_checkpoints[0]['role_statuses']['technical-summarizer'], 'completed_with_failures')
        self.assertEqual(role_completed_checkpoints[1]['completed_roles'], ['technical-summarizer', 'fundamental-summarizer'])
        self.assertEqual(run_state_service.finished[0]['status'], 'partial')
        self.assertFalse(result['completed'])
        self.assertEqual(result['final_pending'], 1)

    def test_signal_decider_finishes_first_pass_then_retries_only_failed_symbols(self):
        service = AgentFeedbackRetryService()
        run_state_service = RecordingRunStateService()
        roles = ('signal-decider',)
        symbols = ('005930', '000660', '035420')
        completed_symbols: set[str] = set()
        failed_symbols: set[str] = set()
        retry_gap_symbols = {'035420'}
        dispatch_calls = []

        def build_snapshot(**kwargs):
            completed = tuple(symbol for symbol in symbols if symbol in completed_symbols)
            failed = tuple(symbol for symbol in symbols if symbol in failed_symbols and symbol not in completed_symbols)
            terminal = set(completed) | set(failed)
            boundary_pending = tuple(symbol for symbol in symbols if symbol not in terminal)
            retry_pending = (
                tuple(symbol for symbol in symbols if symbol in failed_symbols or symbol in retry_gap_symbols)
                if terminal
                else symbols
            )
            return _snapshot(
                symbols=symbols,
                role_states={
                    'signal-decider': _role_state(
                        role='signal-decider',
                        symbols=symbols,
                        completed=completed,
                        failed=failed,
                        boundary_pending=boundary_pending,
                        retry_pending=retry_pending,
                    )
                },
            )

        def dispatch_for_symbols(**kwargs):
            dispatch_calls.append(kwargs)
            if len(dispatch_calls) == 1:
                completed_symbols.update({'005930', '035420'})
                failed_symbols.add('000660')
                status = 'partial'
                failed = 1
            else:
                completed_symbols.add('000660')
                failed_symbols.discard('000660')
                status = 'completed'
                failed = 0
            return {
                'request': {'roles': kwargs['roles'], 'symbols': kwargs['symbols']},
                'summary': {'status': status, 'completed': len(kwargs['symbols']) - failed, 'failed': failed},
            }

        progress = {'request': {'expected_agents': list(roles)}, 'summary': {'cycle_status': 'completed'}}

        with patch.object(service, '_load_eligible_symbols', return_value=list(symbols)):
            with (
                patch(
                    'app.agent_context.retry_service.resolve_external_news_knowledge_cutoff_for_symbols',
                    return_value=(
                        datetime(
                            2026,
                            4,
                            16,
                            6,
                            30,
                            tzinfo=timezone.utc,
                        ),
                        'test',
                    ),
                ),
                patch(
                    'app.agent_context.retry_service.load_agent_feedback_eligibility_records',
                    return_value=[
                        {
                            'symbol': symbol,
                            'candidate_rank': rank,
                            'candidate_score': 100 - rank,
                            'hard_blocked': False,
                            'cap_excluded': False,
                            'forced_inclusion_reasons': [],
                        }
                        for rank, symbol in enumerate(symbols, start=1)
                    ],
                ),
                patch.object(
                    service,
                    '_filter_automatic_decider_upstream_ready_symbols',
                    side_effect=lambda **kwargs: tuple(kwargs['symbols']),
                ),
                patch.object(
                    service,
                    '_filter_decider_retry_ready_symbols',
                    side_effect=lambda **kwargs: tuple(kwargs['symbols']),
                ),
            ):
                with patch.object(service, '_build_role_sequence_snapshot', side_effect=build_snapshot):
                    with patch('app.agent_context.retry_service.agent_context_feedback_run_state_service', run_state_service):
                        with patch(
                            'app.agent_context.retry_service.agent_feedback_orchestrator.dispatch_for_symbols',
                            side_effect=dispatch_for_symbols,
                        ):
                            with patch(
                                'app.agent_context.retry_service.agent_context_progress_service.get_feedback_progress',
                                return_value=progress,
                            ):
                                result = service.run_retry_cycles(
                                    config=SimpleNamespace(
                                        market_date=date.fromisoformat('2026-04-16'),
                                        roles=roles,
                                        max_cycles=3,
                                        outer_batch_size=20,
                                        reuse_existing=True,
                                    )
                                )

        self.assertEqual(
            [(call['roles'][0], call['symbols']) for call in dispatch_calls],
            [
                ('signal-decider', ['005930', '000660', '035420']),
                ('signal-decider', ['000660']),
            ],
        )
        self.assertTrue(all(call['reuse_existing'] is False for call in dispatch_calls))
        self.assertTrue(result['completed'])
        self.assertEqual(result['final_pending'], 0)
        self.assertEqual(result['cycles'][0]['mode'], 'boundary_resume')
        self.assertEqual(result['cycles'][1]['mode'], 'retry_followup')

        final_summary = run_state_service.finished[-1]['summary_payload']['progress_summary']
        self.assertEqual(
            set(final_summary['first_pass_completed_symbols_by_role']['signal-decider']),
            set(symbols),
        )
        self.assertEqual(final_summary['first_pass_failed_symbols_by_role']['signal-decider'], ['000660'])
        self.assertEqual(final_summary['retry_queued_symbols_by_role']['signal-decider'], ['000660'])
        self.assertEqual(final_summary['retry_attempted_symbols_by_role']['signal-decider'], ['000660'])
        role_progress = final_summary['role_progress']['signal-decider']
        self.assertEqual(role_progress['first_pass_completed_symbols'], 3)
        self.assertEqual(role_progress['first_pass_failed_symbols'], 1)
        self.assertEqual(role_progress['retry_queued_symbols'], 1)
        self.assertEqual(role_progress['retry_attempted_symbols'], 1)

    def test_retry_cycles_retry_dependency_failure_but_not_terminal_failure(
        self,
    ):
        service = AgentFeedbackRetryService()
        run_state_service = RecordingRunStateService()
        role = 'short-term-decider'
        symbols = ('005930', '000660', '035420')
        retryable_symbol = '005930'
        terminal_symbol = '000660'
        completed_symbol = '035420'
        dispatch_phase = {'value': 0}
        dispatch_calls = []

        def build_snapshot(**_kwargs):
            if dispatch_phase['value'] == 0:
                role_state = _role_state(
                    role=role,
                    symbols=symbols,
                    boundary_pending=symbols,
                    retry_pending=symbols,
                )
            elif dispatch_phase['value'] == 1:
                role_state = _role_state(
                    role=role,
                    symbols=symbols,
                    completed=(completed_symbol,),
                    failed=(retryable_symbol, terminal_symbol),
                    retry_pending=(retryable_symbol,),
                )
            else:
                role_state = _role_state(
                    role=role,
                    symbols=symbols,
                    completed=(retryable_symbol, completed_symbol),
                    failed=(terminal_symbol,),
                )
            return _snapshot(
                symbols=symbols,
                role_states={role: role_state},
            )

        def dispatch_for_symbols(**kwargs):
            dispatch_calls.append(kwargs)
            dispatch_phase['value'] += 1
            failed = 2 if dispatch_phase['value'] == 1 else 0
            return {
                'request': {
                    'roles': kwargs['roles'],
                    'symbols': kwargs['symbols'],
                },
                'summary': {
                    'status': 'partial' if failed else 'completed',
                    'completed': len(kwargs['symbols']) - failed,
                    'failed': failed,
                },
            }

        eligibility_records = [
            {
                'symbol': symbol,
                'candidate_rank': rank,
                'candidate_score': 100 - rank,
                'hard_blocked': False,
                'cap_excluded': False,
                'forced_inclusion_reasons': [],
            }
            for rank, symbol in enumerate(symbols, start=1)
        ]
        progress = {
            'request': {'expected_agents': [role]},
            'summary': {'cycle_status': 'partial'},
        }

        with (
            patch(
                'app.agent_context.retry_service.'
                'load_agent_feedback_eligibility_records',
                return_value=eligibility_records,
            ),
            patch.object(
                service,
                '_build_role_sequence_snapshot',
                side_effect=build_snapshot,
            ),
            patch(
                'app.agent_context.retry_service.'
                'agent_context_feedback_run_state_service',
                run_state_service,
            ),
            patch(
                'app.agent_context.retry_service.'
                'agent_feedback_orchestrator.dispatch_for_symbols',
                side_effect=dispatch_for_symbols,
            ),
            patch(
                'app.agent_context.retry_service.'
                'agent_context_progress_service.get_feedback_progress',
                return_value=progress,
            ),
        ):
            result = service._run_retry_cycles_admitted(
                config=SimpleNamespace(
                    market_date=date.fromisoformat('2026-04-16'),
                    roles=(role,),
                    symbols=symbols,
                    feedback_scope_mode='decider_scope_fast_path',
                    max_cycles=3,
                    outer_batch_size=20,
                    reuse_existing=False,
                    force_recompute=False,
                    progress_callback=None,
                ),
                prerequisites=_feedback_prerequisites(),
                preloaded_eligible_symbols=symbols,
                effective_knowledge_cutoff_at=datetime(
                    2026,
                    4,
                    16,
                    6,
                    30,
                    tzinfo=timezone.utc,
                ),
                knowledge_cutoff_source='market_close',
            )

        self.assertEqual(
            [call['symbols'] for call in dispatch_calls],
            [list(symbols), [retryable_symbol]],
        )
        self.assertEqual(
            [cycle['mode'] for cycle in result['cycles']],
            ['boundary_resume', 'retry_followup'],
        )
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['final_pending'], 0)

    def test_decider_fast_path_retry_does_not_resume_from_prior_broad_scope_run(self):
        service = AgentFeedbackRetryService()
        run_state_service = RecordingRunStateService()
        roles = ('signal-decider',)
        symbols = ('005930',)
        retry_completed = {'value': False}
        dispatch_calls = []
        find_latest_calls = []
        previous_broad_run = {
            'run_id': 'prior-broad-retry-run',
            'request_payload': {
                'symbols': list(symbols),
                'requested_symbols': list(symbols),
                'roles': list(roles),
                'mode': 'daily_retry_cycle_role_sequenced',
                'feedback_scope_mode': 'broad_feedback_universe',
            },
            'summary_payload': {
                'feedback_scope_mode': 'broad_feedback_universe',
                'progress_summary': {
                    'cycle_number': 1,
                    'completed_cycles': 1,
                },
            },
        }

        def find_latest_run(**kwargs):
            find_latest_calls.append(kwargs)
            if kwargs.get('feedback_scope_mode') in (None, 'broad_feedback_universe'):
                return previous_broad_run
            return None

        def build_snapshot(**kwargs):
            if retry_completed['value']:
                role_state = _role_state(
                    role='signal-decider',
                    symbols=symbols,
                    completed=symbols,
                )
            else:
                role_state = _role_state(
                    role='signal-decider',
                    symbols=symbols,
                    failed=symbols,
                    retry_pending=symbols,
                )
            return _snapshot(
                symbols=symbols,
                role_states={'signal-decider': role_state},
            )

        def dispatch_for_symbols(**kwargs):
            dispatch_calls.append(kwargs)
            retry_completed['value'] = True
            return {
                'request': {'roles': kwargs['roles'], 'symbols': kwargs['symbols']},
                'summary': {'status': 'completed', 'completed': len(kwargs['symbols']), 'failed': 0},
            }

        progress = {'request': {'expected_agents': list(roles)}, 'summary': {'cycle_status': 'completed'}}
        run_state_service.find_latest_run = find_latest_run

        with patch.object(service, '_load_eligible_symbols', return_value=list(symbols)):
            with patch(
                'app.agent_context.retry_service.load_agent_feedback_eligibility_records',
                return_value=[],
            ):
                with patch.object(service, '_build_role_sequence_snapshot', side_effect=build_snapshot):
                    with patch('app.agent_context.retry_service.agent_context_feedback_run_state_service', run_state_service):
                        with patch(
                            'app.agent_context.retry_service.agent_feedback_orchestrator.dispatch_for_symbols',
                            side_effect=dispatch_for_symbols,
                        ):
                            with patch(
                                'app.agent_context.retry_service.agent_context_progress_service.get_feedback_progress',
                                return_value=progress,
                            ):
                                result = service.run_retry_cycles(
                                    config=SimpleNamespace(
                                        market_date=date.fromisoformat('2026-04-16'),
                                        roles=roles,
                                        symbols=symbols,
                                        max_cycles=1,
                                        outer_batch_size=20,
                                        reuse_existing=True,
                                        feedback_scope_mode='decider_scope_fast_path',
                                    )
                                )

        self.assertEqual(find_latest_calls[0]['feedback_scope_mode'], 'decider_scope_fast_path')
        self.assertIsNone(run_state_service.started[0]['request_payload']['resumed_from_run_id'])
        self.assertEqual(
            [(call['roles'][0], call['symbols']) for call in dispatch_calls],
            [('signal-decider', ['005930'])],
        )
        self.assertEqual(result['cycles'][0]['mode'], 'retry_followup')
        self.assertTrue(result['completed'])

    def test_run_retry_cycles_resumes_from_durable_role_boundary_without_redoing_completed_roles(self):
        service = AgentFeedbackRetryService()
        run_state_service = RecordingRunStateService()
        previous_run = {
            'run_id': 'prior-retry-run',
            'request_payload': {
                'admission_knowledge_cutoff_at': (
                    '2026-04-16T06:30:00+00:00'
                ),
                'fs_div': 'CFS',
            },
            'summary_payload': {
                'progress_summary': {
                    'cycle_number': 1,
                    'completed_cycles': 0,
                }
            },
        }
        run_state_service.find_latest_run = lambda **kwargs: previous_run
        roles = ('eod-summarizer', 'technical-summarizer', 'fundamental-summarizer', 'signal-decider')
        symbols = ('005930', '000660')
        snapshots = SnapshotSequence(
            [
                _snapshot(
                    symbols=symbols,
                    role_states={
                        'eod-summarizer': _role_state(role='eod-summarizer', symbols=symbols, completed=symbols),
                        'technical-summarizer': _role_state(
                            role='technical-summarizer',
                            symbols=symbols,
                            completed=('005930',),
                            failed=('000660',),
                        ),
                        'fundamental-summarizer': _role_state(
                            role='fundamental-summarizer',
                            symbols=symbols,
                            completed=('005930',),
                            boundary_pending=('000660',),
                            retry_pending=('000660',),
                        ),
                        'signal-decider': _role_state(
                            role='signal-decider',
                            symbols=symbols,
                            boundary_pending=symbols,
                            retry_pending=symbols,
                        ),
                    },
                ),
                _snapshot(
                    symbols=symbols,
                    role_states={
                        'eod-summarizer': _role_state(role='eod-summarizer', symbols=symbols, completed=symbols),
                        'technical-summarizer': _role_state(
                            role='technical-summarizer',
                            symbols=symbols,
                            completed=('005930',),
                            failed=('000660',),
                        ),
                        'fundamental-summarizer': _role_state(
                            role='fundamental-summarizer',
                            symbols=symbols,
                            completed=('005930',),
                            boundary_pending=('000660',),
                            retry_pending=('000660',),
                        ),
                        'signal-decider': _role_state(
                            role='signal-decider',
                            symbols=symbols,
                            boundary_pending=symbols,
                            retry_pending=symbols,
                        ),
                    },
                ),
                _snapshot(
                    symbols=symbols,
                    role_states={
                        'eod-summarizer': _role_state(role='eod-summarizer', symbols=symbols, completed=symbols),
                        'technical-summarizer': _role_state(
                            role='technical-summarizer',
                            symbols=symbols,
                            completed=('005930',),
                            failed=('000660',),
                        ),
                        'fundamental-summarizer': _role_state(role='fundamental-summarizer', symbols=symbols, completed=symbols),
                        'signal-decider': _role_state(
                            role='signal-decider',
                            symbols=symbols,
                            boundary_pending=symbols,
                            retry_pending=symbols,
                        ),
                    },
                ),
                _snapshot(
                    symbols=symbols,
                    role_states={
                        'eod-summarizer': _role_state(role='eod-summarizer', symbols=symbols, completed=symbols),
                        'technical-summarizer': _role_state(
                            role='technical-summarizer',
                            symbols=symbols,
                            completed=('005930',),
                            failed=('000660',),
                        ),
                        'fundamental-summarizer': _role_state(role='fundamental-summarizer', symbols=symbols, completed=symbols),
                        'signal-decider': _role_state(
                            role='signal-decider',
                            symbols=symbols,
                            boundary_pending=symbols,
                            retry_pending=symbols,
                        ),
                    },
                ),
                _snapshot(
                    symbols=symbols,
                    role_states={
                        'eod-summarizer': _role_state(role='eod-summarizer', symbols=symbols, completed=symbols),
                        'technical-summarizer': _role_state(
                            role='technical-summarizer',
                            symbols=symbols,
                            completed=('005930',),
                            failed=('000660',),
                        ),
                        'fundamental-summarizer': _role_state(role='fundamental-summarizer', symbols=symbols, completed=symbols),
                        'signal-decider': _role_state(role='signal-decider', symbols=symbols, completed=symbols),
                    },
                ),
            ]
        )
        dispatch_calls = []

        def dispatch_for_symbols(**kwargs):
            dispatch_calls.append(kwargs)
            return {
                'request': {'roles': kwargs['roles'], 'symbols': kwargs['symbols']},
                'summary': {'status': 'completed', 'completed': len(kwargs['symbols']), 'failed': 0},
            }

        progress = {'request': {'expected_agents': list(roles)}, 'summary': {'cycle_status': 'completed'}}

        with patch.object(service, '_load_eligible_symbols', return_value=list(symbols)):
            with patch.object(service, '_build_role_sequence_snapshot', side_effect=snapshots):
                with patch('app.agent_context.retry_service.agent_context_feedback_run_state_service', run_state_service):
                    with patch(
                        'app.agent_context.retry_service.agent_feedback_orchestrator.dispatch_for_symbols',
                        side_effect=dispatch_for_symbols,
                    ):
                        with patch(
                            'app.agent_context.retry_service.agent_context_progress_service.get_feedback_progress',
                            return_value=progress,
                        ):
                            result = service.run_retry_cycles(
                                config=SimpleNamespace(
                                    market_date=date.fromisoformat('2026-04-16'),
                                    roles=roles,
                                    max_cycles=3,
                                    outer_batch_size=20,
                                    knowledge_cutoff_at=(
                                        '2026-04-16T15:30:00+09:00'
                                    ),
                                    reuse_existing=True,
                                )
                            )

        self.assertEqual(
            [(call['roles'][0], call['symbols']) for call in dispatch_calls],
            [('fundamental-summarizer', ['000660']), ('signal-decider', ['005930', '000660'])],
        )
        self.assertEqual(run_state_service.started[0]['request_payload']['resumed_from_run_id'], 'prior-retry-run')
        self.assertEqual(run_state_service.finished[-1]['status'], 'partial')
        self.assertEqual(result['status'], 'partial')
        role_started_checkpoints = [
            checkpoint['summary_payload']
            for checkpoint in run_state_service.checkpoints
            if checkpoint['summary_payload']['lifecycle_stage'] == 'role_started'
        ]
        self.assertEqual(
            role_started_checkpoints[0]['completed_roles'],
            ['eod-summarizer', 'technical-summarizer'],
        )
        self.assertEqual(
            role_started_checkpoints[0]['remaining_roles'],
            ['fundamental-summarizer', 'signal-decider'],
        )

    def test_run_retry_cycles_marks_failed_cycle_status_for_failed_run(self):
        roles = ('technical-summarizer',)
        symbols = ('005930',)
        service = AgentFeedbackRetryService()
        run_state_service = RecordingRunStateService()
        snapshots = SnapshotSequence(
            [
                _snapshot(
                    symbols=symbols,
                    role_states={
                        'technical-summarizer': _role_state(
                            role='technical-summarizer',
                            symbols=symbols,
                            boundary_pending=symbols,
                            retry_pending=symbols,
                        ),
                    },
                ),
                _snapshot(
                    symbols=symbols,
                    role_states={
                        'technical-summarizer': _role_state(
                            role='technical-summarizer',
                            symbols=symbols,
                            boundary_pending=symbols,
                            retry_pending=symbols,
                            boundary_status='failed',
                        ),
                    },
                ),
            ]
        )

        def dispatch_for_symbols(**kwargs):
            return {
                'request': {'roles': kwargs['roles'], 'symbols': kwargs['symbols']},
                'summary': {'status': 'failed', 'completed': 0, 'failed': len(kwargs['symbols'])},
            }

        progress = {'request': {'expected_agents': list(roles)}, 'summary': {'cycle_status': 'failed'}}
        plans = [
            RetryCyclePlan(
                cycle_number=1,
                completed_cycles=0,
                mode='boundary_resume',
                current_role='technical-summarizer',
                plan_roles=('technical-summarizer',),
                completed_roles=(),
                remaining_roles=('technical-summarizer',),
            ),
            None,
        ]

        def resolve_cycle_plan(**kwargs):
            return plans.pop(0) if plans else None

        with patch.object(service, '_load_eligible_symbols', return_value=list(symbols)):
            with patch.object(service, '_build_role_sequence_snapshot', side_effect=snapshots):
                with patch.object(service, '_resolve_cycle_plan', side_effect=resolve_cycle_plan):
                    with patch('app.agent_context.retry_service.agent_context_feedback_run_state_service', run_state_service):
                        with patch(
                            'app.agent_context.retry_service.agent_feedback_orchestrator.dispatch_for_symbols',
                            side_effect=dispatch_for_symbols,
                        ):
                            with patch(
                                'app.agent_context.retry_service.agent_context_progress_service.get_feedback_progress',
                                return_value=progress,
                            ):
                                service.run_retry_cycles(
                                    config=SimpleNamespace(
                                        market_date=date.fromisoformat('2026-04-16'),
                                        roles=roles,
                                        max_cycles=1,
                                        outer_batch_size=20,
                                        reuse_existing=True,
                                    )
                                )

        self.assertEqual(run_state_service.finished[-1]['status'], 'failed')
        self.assertEqual(run_state_service.finished[-1]['summary_payload']['cycle_status'], 'failed')


if __name__ == '__main__':
    unittest.main()
