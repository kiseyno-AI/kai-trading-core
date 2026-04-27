import unittest
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import ANY, patch

from app.agent_context.retry_service import (
    AgentFeedbackRetryService,
    EXPECTED_ROLES,
    RetryCyclePlan,
    RoleSequenceRoleState,
    RoleSequenceSnapshot,
)
from scripts import run_feedback_retry_cycles


def _decision(*, should_dispatch):
    return SimpleNamespace(should_dispatch=should_dispatch)


def _role_state(
    *,
    role: str,
    symbols: tuple[str, ...],
    completed: tuple[str, ...] = (),
    failed: tuple[str, ...] = (),
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
            'app.agent_context.retry_service.load_daily_piotroski_filter_results',
            return_value=[{'symbol': '005930', 'passed_filter': True}],
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
            'app.agent_context.retry_service.load_daily_piotroski_filter_results',
            return_value=[{'symbol': '005930', 'passed_filter': True}],
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

    def test_load_incomplete_symbols_uses_only_passed_piotroski_symbols(self):
        service = AgentFeedbackRetryService()
        decisions = {
            '005930': {
                role: _decision(should_dispatch=True)
                for role in EXPECTED_ROLES
            }
        }

        with patch(
            'app.agent_context.retry_service.load_daily_piotroski_filter_results',
            return_value=[{'symbol': '005930', 'passed_filter': True}],
        ) as load_results:
            with patch(
                'app.agent_context.retry_service.agent_context_reuse_service.build_latest_decisions',
                return_value=decisions,
            ) as build_decisions:
                result = service.load_incomplete_symbols(
                    market_date=date.fromisoformat('2026-04-16'),
                    roles=EXPECTED_ROLES,
                )

        load_results.assert_called_once_with(
            market_date=date.fromisoformat('2026-04-16'),
            passed_filter=True,
            limit=None,
        )
        build_decisions.assert_called_once_with(
            market_date=date.fromisoformat('2026-04-16'),
            symbols=['005930'],
            roles=EXPECTED_ROLES,
        )
        self.assertEqual(result, ['005930'])

    def test_load_eligible_symbols_respects_requested_symbol_subset_and_order(self):
        service = AgentFeedbackRetryService()

        with patch(
            'app.agent_context.retry_service.load_daily_piotroski_filter_results',
            return_value=[
                {'symbol': '000660', 'passed_filter': True},
                {'symbol': '005930', 'passed_filter': True},
                {'symbol': '035420', 'passed_filter': True},
            ],
        ):
            result = service._load_eligible_symbols(
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
            'requested_symbols_intersected_with_piotroski_passed_filter',
        )
        self.assertEqual(result['requested_symbols'], ['005930'])
        self.assertEqual(result['eligible_symbols'], ['005930'])
        self.assertEqual(result['skipped_symbols'], [])

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
                                )
                            )

        self.assertEqual(
            [(call['roles'][0], call['symbols']) for call in dispatch_calls],
            [('technical-summarizer', ['005930', '000660']), ('fundamental-summarizer', ['005930', '000660'])],
        )
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

    def test_run_retry_cycles_resumes_from_durable_role_boundary_without_redoing_completed_roles(self):
        service = AgentFeedbackRetryService()
        run_state_service = RecordingRunStateService()
        previous_run = {
            'run_id': 'prior-retry-run',
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
                                    reuse_existing=True,
                                )
                            )

        self.assertEqual(
            [(call['roles'][0], call['symbols']) for call in dispatch_calls],
            [('fundamental-summarizer', ['000660']), ('signal-decider', ['005930', '000660'])],
        )
        self.assertEqual(run_state_service.started[0]['request_payload']['resumed_from_run_id'], 'prior-retry-run')
        self.assertEqual(run_state_service.finished[-1]['status'], 'completed')
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
