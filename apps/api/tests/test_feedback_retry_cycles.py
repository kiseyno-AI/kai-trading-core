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
from app.agent_context.prerequisite_service import FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER
from scripts import run_feedback_retry_cycles


def _decision(*, should_dispatch):
    return SimpleNamespace(should_dispatch=should_dispatch)


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

    def test_load_incomplete_symbols_uses_active_filter_intersection(self):
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
            'requested_symbols_intersected_with_active_pre_filters',
        )
        self.assertEqual(result['requested_symbols'], ['005930'])
        self.assertEqual(result['eligible_symbols'], ['005930'])
        self.assertEqual(result['skipped_symbols'], [])

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
