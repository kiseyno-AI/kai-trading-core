import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from app.agent_context.progress_service import AgentContextProgressService
from app.agent_context.runtime_state import RuntimeFeedbackRunState


class FakeExecuteResult:
    def __init__(self, *, scalars=None, scalar=None, rows=None):
        self._scalars = scalars or []
        self._scalar = scalar
        self._rows = rows or []

    def scalars(self):
        return self

    def all(self):
        return self._scalars or self._rows

    def scalar_one(self):
        return self._scalar

    def __iter__(self):
        return iter(self._rows)


class FakeProgressSession:
    def __init__(self):
        self.statements = []
        self.market_universe_symbols = ['005930', '000660']
        self.filtered_first_feedback_at = datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc)
        self.filtered_latest_feedback_at = datetime(2026, 4, 14, 1, 5, tzinfo=timezone.utc)
        self.feedback_rows = [
            SimpleNamespace(
                symbol='005930',
                agent_name='eod-summarizer',
                run_id='run-1',
                received_at=self.filtered_first_feedback_at,
                id=1,
            ),
            SimpleNamespace(
                symbol='005930',
                agent_name='technical-summarizer',
                run_id='run-1',
                received_at=datetime(2026, 4, 14, 1, 2, tzinfo=timezone.utc),
                id=2,
            ),
            SimpleNamespace(
                symbol='005930',
                agent_name='fundamental-summarizer',
                run_id='run-1',
                received_at=self.filtered_latest_feedback_at,
                id=3,
            ),
            SimpleNamespace(
                symbol='005930',
                agent_name='signal-decider',
                run_id='run-1',
                received_at=datetime(2026, 4, 14, 1, 4, tzinfo=timezone.utc),
                id=30,
            ),
            SimpleNamespace(
                symbol='000660',
                agent_name='eod-summarizer',
                run_id='run-1',
                received_at=datetime(2026, 4, 14, 1, 6, tzinfo=timezone.utc),
                id=4,
            ),
            SimpleNamespace(
                symbol='035420',
                agent_name='technical-summarizer',
                run_id='other-run',
                received_at=datetime(2026, 4, 14, 1, 7, tzinfo=timezone.utc),
                id=5,
            ),
        ]
        self.failure_rows = [
            SimpleNamespace(
                symbol='000660',
                agent_name='technical-summarizer',
                run_id='run-1',
                received_at=datetime(2026, 4, 14, 1, 8, tzinfo=timezone.utc),
                id=10,
            ),
            SimpleNamespace(
                symbol='000660',
                agent_name='fundamental-summarizer',
                run_id='run-1',
                received_at=datetime(2026, 4, 14, 1, 9, tzinfo=timezone.utc),
                id=11,
            ),
            SimpleNamespace(
                symbol='005930',
                agent_name='technical-summarizer',
                run_id='stale-failure',
                received_at=datetime(2026, 4, 14, 1, 10, tzinfo=timezone.utc),
                id=12,
            ),
            SimpleNamespace(
                symbol='035420',
                agent_name='technical-summarizer',
                run_id='other-run',
                received_at=datetime(2026, 4, 14, 1, 11, tzinfo=timezone.utc),
                id=13,
            ),
        ]

    def execute(self, statement):
        self.statements.append(statement)
        sql = self._compile(statement)
        if 'daily_price_bars' in sql:
            return FakeExecuteResult(scalars=self.market_universe_symbols)
        if 'agent_context_feedback_failures' in sql:
            return FakeExecuteResult(rows=self.failure_rows)
        return FakeExecuteResult(rows=self.feedback_rows)

    @staticmethod
    def _compile(statement) -> str:
        return str(statement.compile(compile_kwargs={'literal_binds': True}))


class AgentContextProgressServiceTests(unittest.TestCase):
    def test_feedback_progress_uses_durable_run_snapshot_when_runtime_state_missing(self):
        fake_session = FakeProgressSession()
        run_snapshot = {
            'run_id': 'run-durable',
            'status': 'partial',
            'started_at': datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
            'completed_at': datetime(2026, 4, 14, 1, 12, tzinfo=timezone.utc),
            'summary_payload': {
                'current_role': 'fundamental-summarizer',
                'buffered_feedback_count': 3,
                'flushed_feedback_count': 3,
                'progress_summary': {
                    'run_id': 'run-durable',
                    'run_status': 'partial',
                    'run_table_status': 'partial',
                    'is_live': False,
                    'current_role': 'fundamental-summarizer',
                    'completed_roles': ['eod-summarizer'],
                    'remaining_roles': ['fundamental-summarizer'],
                    'cycle_number': 1,
                    'completed_cycles': 0,
                    'max_cycles': 3,
                    'cycle_mode': 'boundary_resume',
                    'cycle_status': 'partial',
                    'total_symbols': 2,
                    'processed_symbols': 2,
                    'completed_symbols': 1,
                    'in_progress_symbols': 0,
                    'pending_symbols': 0,
                    'failed_items': 1,
                    'completed_feedback_count': 3,
                    'expected_feedback_count': 4,
                    'role_progress': {
                        'eod-summarizer': {
                            'role': 'eod-summarizer',
                            'status': 'completed',
                            'total_symbols': 2,
                            'processed_symbols': 2,
                            'completed_symbols': 2,
                            'in_progress_symbols': 0,
                            'pending_symbols': 0,
                            'failed_items': 0,
                            'progress_ratio': 1.0,
                            'progress_percent': 100.0,
                        },
                        'technical-summarizer': {
                            'role': 'technical-summarizer',
                            'status': 'partial',
                            'total_symbols': 2,
                            'processed_symbols': 2,
                            'completed_symbols': 1,
                            'in_progress_symbols': 0,
                            'pending_symbols': 0,
                            'failed_items': 1,
                            'progress_ratio': 1.0,
                            'progress_percent': 100.0,
                        },
                    },
                },
            },
        }

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.agent_context.progress_service.session_scope', fake_session_scope):
            with patch('app.agent_context.progress_service.runtime_feedback_state_store.get_run', return_value=None):
                with patch(
                    'app.agent_context.progress_service.agent_context_feedback_run_state_service.find_latest_run',
                    return_value=run_snapshot,
                ):
                    result = AgentContextProgressService().get_feedback_progress(
                        market_date='2026-04-14',
                        symbols=['005930', '000660'],
                        expected_agents=['eod-summarizer', 'technical-summarizer'],
                    )

        self.assertEqual(result['summary']['run_id'], 'run-durable')
        self.assertEqual(result['summary']['run_table_status'], 'partial')
        self.assertEqual(result['summary']['current_role'], 'fundamental-summarizer')
        self.assertEqual(result['summary']['completed_roles'], ['eod-summarizer'])
        self.assertEqual(result['summary']['remaining_roles'], ['fundamental-summarizer'])
        self.assertEqual(result['summary']['cycle_number'], 1)
        self.assertEqual(result['summary']['buffered_feedback_count'], 3)
        self.assertEqual(result['summary']['terminal_feedback_count'], 4)
        self.assertEqual(result['summary']['progress_ratio'], 1.0)
        self.assertEqual(result['summary']['boundary_progress_ratio'], 1.0)
        self.assertEqual(
            result['result']['roles']['technical-summarizer'],
            {'requested_count': 2, 'completed_count': 1, 'error_count': 1},
        )

    def test_feedback_progress_runtime_summary_tracks_whole_cycle_completion_not_symbol_boundary(self):
        fake_session = FakeProgressSession()
        state = RuntimeFeedbackRunState(
            run_id='live-run',
            market_date=datetime(2026, 4, 14, tzinfo=timezone.utc).date(),
            requested_symbols=['005930', '000660'],
            requested_roles=[
                'eod-summarizer',
                'technical-summarizer',
                'fundamental-summarizer',
                'signal-decider',
            ],
            role_sequence=[
                'eod-summarizer',
                'technical-summarizer',
                'fundamental-summarizer',
                'signal-decider',
            ],
            parallel_per_role=2,
            batch_size=2,
            started_at=datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
            current_role='technical-summarizer',
        )
        state.completed_symbols_by_role = {
            'eod-summarizer': {'005930', '000660'},
            'technical-summarizer': {'005930'},
        }
        state.in_flight_slots_by_role = {
            'technical-summarizer': {
                1: {'symbols': ['000660']},
            }
        }
        state.last_activity_at = datetime(2026, 4, 14, 1, 5, tzinfo=timezone.utc)
        run_snapshot = {
            'run_id': 'live-run',
            'status': 'running',
            'started_at': datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
            'completed_at': None,
            'summary_payload': {'progress_summary': {'run_status': 'running'}},
        }

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.agent_context.progress_service.session_scope', fake_session_scope):
            with patch('app.agent_context.progress_service.runtime_feedback_state_store.get_run', return_value=state):
                with patch(
                    'app.agent_context.progress_service.agent_context_feedback_run_state_service.get_run',
                    return_value=run_snapshot,
                ):
                    result = AgentContextProgressService().get_feedback_progress(
                        market_date='2026-04-14',
                        symbols=['005930', '000660'],
                        run_id='live-run',
                    )

        summary = result['summary']
        self.assertEqual(summary['current_role'], 'technical-summarizer')
        self.assertEqual(summary['completed_roles'], ['eod-summarizer'])
        self.assertEqual(
            summary['remaining_roles'],
            ['technical-summarizer', 'fundamental-summarizer', 'signal-decider'],
        )
        self.assertEqual(summary['processed_symbols'], 2)
        self.assertEqual(summary['boundary_progress_ratio'], 1.0)
        self.assertEqual(summary['boundary_progress_percent'], 100.0)
        self.assertEqual(summary['terminal_feedback_count'], 3)
        self.assertEqual(summary['completed_feedback_count'], 3)
        self.assertEqual(summary['expected_feedback_count'], 8)
        self.assertEqual(summary['progress_ratio'], 0.375)
        self.assertEqual(summary['progress_percent'], 37.5)
        self.assertEqual(summary['role_progress']['technical-summarizer']['progress_percent'], 100.0)
        self.assertEqual(summary['role_progress']['fundamental-summarizer']['progress_percent'], 0.0)
        self.assertEqual(summary['role_progress']['signal-decider']['progress_percent'], 0.0)

    def test_feedback_progress_explicit_run_id_prefers_matching_runtime_state(self):
        fake_session = FakeProgressSession()
        runtime_summary = {
            'run_id': 'run-explicit',
            'run_status': 'running',
            'run_table_status': 'running',
            'is_live': True,
            'current_role': 'technical-summarizer',
            'total_symbols': 1,
            'processed_symbols': 1,
            'completed_symbols': 0,
            'in_progress_symbols': 1,
            'pending_symbols': 0,
            'failed_items': 0,
            'completed_feedback_count': 1,
            'expected_feedback_count': 4,
            'role_progress': {
                'technical-summarizer': {
                    'role': 'technical-summarizer',
                    'status': 'running',
                    'total_symbols': 1,
                    'processed_symbols': 1,
                    'completed_symbols': 0,
                    'in_progress_symbols': 1,
                    'pending_symbols': 0,
                    'failed_items': 0,
                    'progress_ratio': 1.0,
                    'progress_percent': 100.0,
                }
            },
        }
        runtime_state = SimpleNamespace(to_progress_summary=lambda: runtime_summary)
        run_snapshot = {
            'run_id': 'run-explicit',
            'status': 'running',
            'started_at': datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
            'completed_at': None,
            'summary_payload': {'progress_summary': {'run_status': 'partial'}},
        }

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.agent_context.progress_service.session_scope', fake_session_scope):
            with patch('app.agent_context.progress_service.runtime_feedback_state_store.get_run', return_value=runtime_state) as get_run:
                with patch(
                    'app.agent_context.progress_service.agent_context_feedback_run_state_service.get_run',
                    return_value=run_snapshot,
                ) as get_persisted_run:
                    with patch('app.agent_context.progress_service.agent_context_feedback_run_state_service.find_latest_run', return_value=None) as find_latest_run:
                        result = AgentContextProgressService().get_feedback_progress(
                            market_date='2026-04-14',
                            symbols=['005930'],
                            expected_agents=['technical-summarizer'],
                            run_id='run-explicit',
                        )

        get_persisted_run.assert_called_once_with(run_id='run-explicit')
        find_latest_run.assert_not_called()
        get_run.assert_called_once()
        self.assertEqual(result['summary']['run_id'], 'run-explicit')
        self.assertEqual(result['summary']['run_status'], 'running')
        self.assertEqual(result['summary']['run_table_status'], 'running')
        self.assertEqual(result['summary']['current_role'], 'technical-summarizer')
        self.assertEqual(result['result']['roles']['technical-summarizer']['requested_count'], 1)

    def test_feedback_progress_prefers_persisted_counts_for_live_daily_retry_cycle_runs(self):
        fake_session = FakeProgressSession()
        fake_session.market_universe_symbols = ['005930', '000660', '035420']
        runtime_summary = {
            'run_id': 'retry-run',
            'run_status': 'running',
            'run_table_status': 'running',
            'is_live': True,
            'current_role': 'technical-summarizer',
            'completed_roles': ['eod-summarizer'],
            'remaining_roles': ['technical-summarizer', 'fundamental-summarizer', 'signal-decider'],
            'cycle_number': 1,
            'completed_cycles': 0,
            'max_cycles': 3,
            'cycle_mode': 'daily_retry_cycle_role_sequenced',
            'cycle_status': 'running',
            'parallel_per_role': 2,
            'total_symbols': 1,
            'processed_symbols': 0,
            'completed_symbols': 0,
            'in_progress_symbols': 1,
            'pending_symbols': 0,
            'failed_items': 0,
            'completed_feedback_count': 0,
            'expected_feedback_count': 4,
            'buffered_feedback_count': 0,
            'buffered_failure_count': 0,
            'pending_feedback_count': 0,
            'pending_failure_count': 0,
            'flushed_feedback_count': 0,
            'flushed_failure_count': 0,
            'role_progress': {
                'technical-summarizer': {
                    'role': 'technical-summarizer',
                    'status': 'running',
                    'total_symbols': 1,
                    'processed_symbols': 1,
                    'completed_symbols': 0,
                    'in_progress_symbols': 1,
                    'pending_symbols': 0,
                    'failed_items': 0,
                    'progress_ratio': 1.0,
                    'progress_percent': 100.0,
                }
            },
        }
        runtime_state = SimpleNamespace(to_progress_summary=lambda: runtime_summary)
        run_snapshot = {
            'run_id': 'retry-run',
            'status': 'running',
            'symbols_total': 1,
            'started_at': datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
            'completed_at': None,
            'summary_payload': {
                'mode': 'daily_retry_cycle_role_sequenced',
                'progress_summary': {
                    'run_id': 'retry-run',
                    'run_status': 'running',
                    'run_table_status': 'running',
                    'current_role': 'technical-summarizer',
                    'completed_roles': ['eod-summarizer'],
                    'remaining_roles': ['technical-summarizer', 'fundamental-summarizer', 'signal-decider'],
                    'cycle_number': 1,
                    'completed_cycles': 0,
                    'max_cycles': 3,
                    'cycle_mode': 'daily_retry_cycle_role_sequenced',
                    'cycle_status': 'running',
                },
            },
            'request_payload': {
                'mode': 'daily_retry_cycle_role_sequenced',
                'symbols': ['005930'],
                'requested_symbols': ['005930', '000660'],
            },
        }

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.agent_context.progress_service.session_scope', fake_session_scope):
            with patch('app.agent_context.progress_service.runtime_feedback_state_store.get_run', return_value=runtime_state):
                with patch(
                    'app.agent_context.progress_service.agent_context_feedback_run_state_service.find_latest_run',
                    return_value=run_snapshot,
                ):
                    result = AgentContextProgressService().get_feedback_progress(
                        market_date='2026-04-14',
                        symbols=None,
                    )

        self.assertEqual(result['summary']['run_id'], 'retry-run')
        self.assertEqual(result['summary']['run_status'], 'running')
        self.assertEqual(result['summary']['current_role'], 'technical-summarizer')
        self.assertEqual(result['summary']['completed_roles'], ['eod-summarizer'])
        self.assertEqual(result['summary']['cycle_mode'], 'daily_retry_cycle_role_sequenced')
        self.assertEqual(result['summary']['total_symbols'], 1)
        self.assertEqual(result['summary']['expected_feedback_count'], 4)
        self.assertEqual(result['result']['roles']['eod-summarizer']['completed_count'], 1)
        self.assertEqual(result['result']['roles']['technical-summarizer']['completed_count'], 1)
        self.assertEqual(result['summary']['completed_feedback_count'], 4)
        self.assertEqual(result['summary']['failed_items'], 0)

    def _call_service(self, fake_session, *, symbols, expected_agents=None):
        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.agent_context.progress_service.session_scope', fake_session_scope):
            with patch('app.agent_context.progress_service.runtime_feedback_state_store.get_run', return_value=None):
                with patch('app.agent_context.progress_service.agent_context_feedback_run_state_service.find_latest_run', return_value=None):
                    with patch('app.agent_context.progress_service.agent_context_feedback_run_state_service.get_run', return_value=None):
                        return AgentContextProgressService().get_feedback_progress(
                            market_date='2026-04-14',
                            symbols=symbols,
                            expected_agents=expected_agents,
                        )

    def test_feedback_progress_explicit_symbols_ignore_active_runtime_when_no_durable_run_matches(self):
        fake_session = FakeProgressSession()
        runtime_summary = {
            'run_id': 'active-run',
            'run_status': 'running',
            'run_table_status': 'running',
            'is_live': True,
            'total_symbols': 2,
            'processed_symbols': 2,
            'completed_symbols': 0,
            'in_progress_symbols': 2,
            'pending_symbols': 0,
            'failed_items': 0,
            'completed_feedback_count': 0,
            'expected_feedback_count': 8,
            'role_progress': {
                'eod-summarizer': {
                    'role': 'eod-summarizer',
                    'status': 'running',
                    'total_symbols': 2,
                    'processed_symbols': 2,
                    'completed_symbols': 0,
                    'in_progress_symbols': 2,
                    'pending_symbols': 0,
                    'failed_items': 0,
                    'progress_ratio': 1.0,
                    'progress_percent': 100.0,
                }
            },
        }
        runtime_state = SimpleNamespace(
            requested_symbols=['005930', '000660'],
            to_progress_summary=lambda: runtime_summary,
        )

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.agent_context.progress_service.session_scope', fake_session_scope):
            with patch(
                'app.agent_context.progress_service.runtime_feedback_state_store.get_run',
                return_value=runtime_state,
            ) as get_run:
                with patch(
                    'app.agent_context.progress_service.agent_context_feedback_run_state_service.find_latest_run',
                    return_value=None,
                ):
                    result = AgentContextProgressService().get_feedback_progress(
                        market_date='2026-04-14',
                        symbols=['005930'],
                        expected_agents=['eod-summarizer'],
                    )

        get_run.assert_not_called()
        feedback_sql = fake_session._compile(fake_session.statements[0])
        self.assertIn("IN ('005930')", feedback_sql)
        self.assertEqual(result['summary']['run_id'], None)
        self.assertEqual(result['summary']['total_symbols'], 1)
        self.assertEqual(
            result['result']['roles']['eod-summarizer'],
            {'requested_count': 1, 'completed_count': 1, 'error_count': 0},
        )

    def test_feedback_progress_result_scopes_role_counts_to_requested_symbols(self):
        fake_session = FakeProgressSession()

        result = self._call_service(fake_session, symbols=['005930', '000660'])

        feedback_sql = fake_session._compile(fake_session.statements[0])
        failure_sql = fake_session._compile(fake_session.statements[1])

        self.assertIn('agent_context_feedback.symbol IN', feedback_sql)
        self.assertIn('agent_context_feedback_failures.symbol IN', failure_sql)
        self.assertEqual(result['result']['market_date'], '2026-04-14')
        self.assertEqual(
            result['result']['roles']['eod-summarizer'],
            {'requested_count': 2, 'completed_count': 2, 'error_count': 0},
        )
        self.assertEqual(
            result['result']['roles']['technical-summarizer'],
            {'requested_count': 2, 'completed_count': 1, 'error_count': 1},
        )
        self.assertEqual(
            result['result']['roles']['fundamental-summarizer'],
            {'requested_count': 2, 'completed_count': 1, 'error_count': 1},
        )
        self.assertEqual(
            result['result']['roles']['signal-decider'],
            {'requested_count': 2, 'completed_count': 1, 'error_count': 0},
        )
        self.assertEqual(
            set(result['result']['roles']['technical-summarizer'].keys()),
            {'requested_count', 'completed_count', 'error_count'},
        )

    def test_feedback_progress_uses_market_date_symbol_universe_when_symbols_omitted(self):
        fake_session = FakeProgressSession()

        result = self._call_service(fake_session, symbols=None)

        universe_sql = fake_session._compile(fake_session.statements[0])
        feedback_sql = fake_session._compile(fake_session.statements[1])

        self.assertIn('daily_price_bars', universe_sql)
        self.assertIn('agent_context_feedback.symbol IN', feedback_sql)
        self.assertEqual(result['summary']['total_symbols'], 2)
        self.assertEqual(result['result']['roles']['technical-summarizer']['requested_count'], 2)
        self.assertEqual(result['result']['roles']['technical-summarizer']['completed_count'], 1)

    def test_feedback_progress_without_symbols_uses_latest_durable_retry_cycle_run(self):
        fake_session = FakeProgressSession()
        run_snapshot = {
            'run_id': 'retry-run',
            'status': 'partial',
            'started_at': datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
            'completed_at': datetime(2026, 4, 14, 1, 10, tzinfo=timezone.utc),
            'summary_payload': {
                'progress_summary': {
                    'run_id': 'retry-run',
                    'run_status': 'partial',
                    'run_table_status': 'partial',
                    'is_live': False,
                    'current_role': 'signal-decider',
                    'completed_roles': ['eod-summarizer', 'technical-summarizer', 'fundamental-summarizer'],
                    'remaining_roles': ['signal-decider'],
                    'cycle_number': 2,
                    'completed_cycles': 1,
                    'max_cycles': 3,
                    'cycle_mode': 'retry_followup',
                    'cycle_status': 'partial',
                    'total_symbols': 2,
                    'processed_symbols': 2,
                    'completed_symbols': 1,
                    'in_progress_symbols': 1,
                    'pending_symbols': 0,
                    'failed_items': 1,
                    'completed_feedback_count': 7,
                    'expected_feedback_count': 8,
                    'role_progress': {
                        'signal-decider': {
                            'role': 'signal-decider',
                            'status': 'completed_with_failures',
                            'boundary_status': 'completed_with_failures',
                            'total_symbols': 2,
                            'processed_symbols': 2,
                            'completed_symbols': 1,
                            'in_progress_symbols': 0,
                            'pending_symbols': 0,
                            'terminal_symbols': 2,
                            'failed_items': 1,
                            'retry_pending_symbols': 1,
                            'progress_ratio': 1.0,
                            'progress_percent': 100.0,
                        }
                    },
                }
            },
        }

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.agent_context.progress_service.session_scope', fake_session_scope):
            with patch('app.agent_context.progress_service.runtime_feedback_state_store.get_run', return_value=None):
                with patch(
                    'app.agent_context.progress_service.agent_context_feedback_run_state_service.find_latest_run',
                    return_value=run_snapshot,
                ) as find_latest_run:
                    result = AgentContextProgressService().get_feedback_progress(
                        market_date='2026-04-14',
                        symbols=None,
                    )

        self.assertEqual(result['summary']['run_id'], 'retry-run')
        self.assertEqual(result['summary']['current_role'], 'signal-decider')
        self.assertEqual(result['summary']['completed_roles'], ['eod-summarizer', 'technical-summarizer', 'fundamental-summarizer'])
        self.assertEqual(result['summary']['remaining_roles'], ['signal-decider'])
        self.assertEqual(result['summary']['cycle_mode'], 'retry_followup')
        self.assertEqual(result['summary']['cycle_number'], 2)
        self.assertEqual(result['summary']['role_progress']['signal-decider']['retry_pending_symbols'], 1)
        self.assertEqual(result['summary']['terminal_feedback_count'], 8)
        self.assertEqual(result['summary']['progress_ratio'], 1.0)
        self.assertEqual(result['summary']['boundary_progress_ratio'], 1.0)
        self.assertEqual(result['result']['roles']['signal-decider']['error_count'], 1)
        self.assertEqual(find_latest_run.call_args.kwargs['request_modes'], ['daily_retry_cycle_role_sequenced'])

    def test_feedback_progress_prefers_run_scope_over_requested_symbols_when_run_was_filtered(self):
        fake_session = FakeProgressSession()
        run_snapshot = {
            'run_id': 'run-filtered',
            'status': 'partial',
            'symbols_total': 1,
            'started_at': datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
            'completed_at': datetime(2026, 4, 14, 1, 10, tzinfo=timezone.utc),
            'request_payload': {
                'symbols': ['005930'],
                'requested_symbols': ['005930', '000660'],
                'roles': ['technical-summarizer'],
            },
            'summary_payload': {},
        }

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.agent_context.progress_service.session_scope', fake_session_scope):
            with patch('app.agent_context.progress_service.runtime_feedback_state_store.get_run', return_value=None):
                with patch(
                    'app.agent_context.progress_service.agent_context_feedback_run_state_service.find_latest_run',
                    return_value=run_snapshot,
                ):
                    result = AgentContextProgressService().get_feedback_progress(
                        market_date='2026-04-14',
                        symbols=['005930', '000660'],
                        expected_agents=['technical-summarizer'],
                    )

        feedback_sql = fake_session._compile(fake_session.statements[0])
        self.assertIn("IN ('005930')", feedback_sql)
        self.assertEqual(result['summary']['total_symbols'], 1)
        self.assertEqual(result['result']['roles']['technical-summarizer']['requested_count'], 1)
        self.assertEqual(result['result']['roles']['technical-summarizer']['completed_count'], 1)

    def test_feedback_progress_without_symbols_uses_run_scope_before_market_universe_fallback(self):
        fake_session = FakeProgressSession()
        fake_session.market_universe_symbols = ['005930', '000660', '035420']
        run_snapshot = {
            'run_id': 'run-filtered',
            'status': 'partial',
            'symbols_total': 1,
            'started_at': datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
            'completed_at': datetime(2026, 4, 14, 1, 10, tzinfo=timezone.utc),
            'request_payload': {
                'symbols': ['005930'],
                'requested_symbols': ['005930', '000660'],
                'roles': ['technical-summarizer'],
                'mode': 'daily_retry_cycle_role_sequenced',
            },
            'summary_payload': {},
        }

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.agent_context.progress_service.session_scope', fake_session_scope):
            with patch('app.agent_context.progress_service.runtime_feedback_state_store.get_run', return_value=None):
                with patch(
                    'app.agent_context.progress_service.agent_context_feedback_run_state_service.find_latest_run',
                    return_value=run_snapshot,
                ):
                    result = AgentContextProgressService().get_feedback_progress(
                        market_date='2026-04-14',
                        symbols=None,
                        expected_agents=['technical-summarizer'],
                    )

        compiled_statements = [fake_session._compile(statement) for statement in fake_session.statements]
        self.assertFalse(any('daily_price_bars' in sql for sql in compiled_statements))
        self.assertEqual(result['summary']['total_symbols'], 1)
        self.assertEqual(result['result']['roles']['technical-summarizer']['requested_count'], 1)
        self.assertEqual(result['result']['roles']['technical-summarizer']['completed_count'], 1)

    def test_feedback_progress_returns_timing_rate_and_eta_fields(self):
        fake_session = FakeProgressSession()

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                value = cls(2026, 4, 14, 1, 10, tzinfo=timezone.utc)
                return value if tz is not None else value.replace(tzinfo=None)

        with patch('app.agent_context.progress_service.datetime', FixedDateTime):
            result = self._call_service(fake_session, symbols=['005930'])

        summary = result['summary']
        self.assertEqual(summary['run_status'], 'completed')
        self.assertEqual(summary['first_feedback_at'], '2026-04-14T01:00:00+00:00')
        self.assertEqual(summary['latest_feedback_at'], '2026-04-14T01:05:00+00:00')
        self.assertEqual(summary['elapsed_seconds'], 300)
        self.assertEqual(summary['completed_feedback_per_minute'], 0.8)
        self.assertEqual(summary['completed_symbols_per_minute'], 0.2)
        self.assertEqual(summary['estimated_remaining_seconds'], 0)
        self.assertEqual(summary['estimated_completion_at'], '2026-04-14T01:05:00+00:00')
        self.assertEqual(summary['eta_confidence'], 'high')

    def test_feedback_progress_scopes_counts_to_expected_agents(self):
        fake_session = FakeProgressSession()

        result = self._call_service(
            fake_session,
            symbols=['005930'],
            expected_agents=['technical-summarizer'],
        )

        feedback_sql = fake_session._compile(fake_session.statements[0])
        summary = result['summary']

        self.assertIn('agent_context_feedback.agent_name IN', feedback_sql)
        self.assertEqual(result['request']['expected_agents'], ['technical-summarizer'])
        self.assertEqual(
            result['result']['roles']['technical-summarizer'],
            {'requested_count': 1, 'completed_count': 1, 'error_count': 0},
        )
        self.assertEqual(summary['completed_symbols'], 1)
        self.assertEqual(summary['completed_feedback_count'], 1)
        self.assertEqual(summary['expected_feedback_count'], 1)

    def test_feedback_progress_counts_failure_only_symbols_as_processed(self):
        fake_session = FakeProgressSession()
        fake_session.feedback_rows = []
        fake_session.failure_rows = [
            SimpleNamespace(
                symbol='005930',
                agent_name='technical-summarizer',
                run_id='run-1',
                received_at=datetime(2026, 4, 14, 1, 8, tzinfo=timezone.utc),
                id=10,
            ),
        ]

        result = self._call_service(
            fake_session,
            symbols=['005930'],
            expected_agents=['technical-summarizer'],
        )

        summary = result['summary']
        self.assertEqual(summary['completed_symbols'], 0)
        self.assertEqual(summary['in_progress_symbols'], 1)
        self.assertEqual(summary['pending_symbols'], 0)
        self.assertEqual(summary['terminal_feedback_count'], 1)
        self.assertEqual(summary['boundary_progress_ratio'], 1.0)
        self.assertEqual(summary['boundary_progress_percent'], 100.0)
        self.assertEqual(summary['progress_ratio'], 1.0)
        self.assertEqual(summary['progress_percent'], 100.0)
        self.assertEqual(summary['failed_items'], 1)
        self.assertEqual(
            result['result']['roles']['technical-summarizer'],
            {'requested_count': 1, 'completed_count': 0, 'error_count': 1},
        )


if __name__ == '__main__':
    unittest.main()
