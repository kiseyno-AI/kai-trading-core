import unittest
from contextlib import contextmanager
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from app.agent_context.progress_service import AgentContextProgressService
from app.agent_context.runtime_state import RuntimeFeedbackRunState
from app.eod.daily_pipeline_stages import DailyPipelineStageService


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


def _reuse_decision(*, symbol: str, agent_name: str, should_dispatch: bool = False, reason: str = 'reuse_latest_feedback'):
    return SimpleNamespace(
        symbol=symbol,
        agent_name=agent_name,
        should_dispatch=should_dispatch,
        reason=reason,
        latest_feedback_id=77,
        latest_feedback_received_at='2026-04-13T01:00:00+00:00',
        latest_source_updated_at=None,
        latest_feedback={
            'id': 77,
            'market_date': '2026-04-13',
            'symbol': symbol,
            'agent_name': agent_name,
            'run_id': 'prior-run',
            'received_at': '2026-04-13T01:00:00+00:00',
        },
        input_fingerprint=None,
        stored_input_fingerprint=None,
        reuse_policy_version=None,
    )


class AgentContextProgressServiceTests(unittest.TestCase):
    def setUp(self):
        # Progress unit tests use a synthetic session and must never fall
        # through to the production prerequisite service/database.
        prerequisite_patcher = patch(
            'app.agent_context.progress_service.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
            return_value={'ready': True, 'status': 'ready', 'missing_prerequisites': []},
        )
        prerequisite_patcher.start()
        self.addCleanup(prerequisite_patcher.stop)

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
        self.assertIsNone(result['summary']['current_role'])
        self.assertFalse(result['summary']['is_live'])
        self.assertEqual(result['summary']['in_progress_symbols'], 0)
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

    def test_feedback_progress_self_heals_stale_running_summary_when_outputs_are_complete(self):
        fake_session = FakeProgressSession()
        fake_session.failure_rows = []
        fake_session.feedback_rows = [
            SimpleNamespace(
                symbol=symbol,
                agent_name=agent_name,
                run_id='stale-running-run',
                received_at=datetime(2026, 4, 14, 1, index, tzinfo=timezone.utc),
                id=index,
            )
            for index, (symbol, agent_name) in enumerate(
                [
                    ('005930', 'eod-summarizer'),
                    ('000660', 'eod-summarizer'),
                    ('005930', 'technical-summarizer'),
                    ('000660', 'technical-summarizer'),
                ],
                start=1,
            )
        ]
        stale_run_snapshot = {
            'run_id': 'stale-running-run',
            'status': 'running',
            'symbols_total': 2,
            'completed_count': 4,
            'failed_count': 0,
            'started_at': datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
            'completed_at': None,
            'request_payload': {
                'mode': 'daily_retry_cycle_role_sequenced',
                'symbols': ['005930', '000660'],
                'requested_symbols': ['005930', '000660'],
                'roles': ['eod-summarizer', 'technical-summarizer'],
            },
            'summary_payload': {
                'progress_summary': {
                    'run_id': 'stale-running-run',
                    'run_status': 'running',
                    'run_table_status': 'running',
                    'is_live': True,
                    'current_role': 'technical-summarizer',
                    'total_symbols': 2,
                    'processed_symbols': 2,
                    'completed_symbols': 2,
                    'in_progress_symbols': 0,
                    'pending_symbols': 0,
                    'failed_items': 0,
                    'completed_feedback_count': 4,
                    'expected_feedback_count': 4,
                    'progress_percent': 100.0,
                    'role_progress': {
                        'eod-summarizer': {
                            'role': 'eod-summarizer',
                            'status': 'completed',
                            'total_symbols': 2,
                            'completed_symbols': 2,
                            'failed_items': 0,
                        },
                        'technical-summarizer': {
                            'role': 'technical-summarizer',
                            'status': 'incomplete',
                            'total_symbols': 2,
                            'completed_symbols': 2,
                            'failed_items': 0,
                            'pending_symbols': 0,
                            'in_progress_symbols': 0,
                        },
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
                    return_value=stale_run_snapshot,
                ):
                    result = AgentContextProgressService().get_feedback_progress(
                        market_date='2026-04-14',
                        symbols=None,
                        expected_agents=['eod-summarizer', 'technical-summarizer'],
                    )

        summary = result['summary']
        self.assertEqual(summary['run_status'], 'completed')
        self.assertEqual(summary['run_table_status'], 'completed')
        self.assertFalse(summary['is_live'])
        self.assertIsNone(summary['current_role'])
        self.assertEqual(summary['completed_feedback_count'], 4)
        self.assertEqual(summary['expected_feedback_count'], 4)
        self.assertEqual(summary['progress_percent'], 100.0)
        self.assertEqual(summary['completed_roles'], ['eod-summarizer', 'technical-summarizer'])
        self.assertEqual(summary['remaining_roles'], [])
        self.assertEqual(summary['role_progress']['technical-summarizer']['status'], 'completed')

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

    def test_feedback_progress_merges_live_in_flight_state_into_current_role_counts(self):
        fake_session = FakeProgressSession()
        fake_session.market_universe_symbols = ['005930', '000660']
        fake_session.feedback_rows = [
            SimpleNamespace(
                symbol='005930',
                agent_name='eod-summarizer',
                run_id='retry-run',
                received_at=datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
                id=1,
            ),
            SimpleNamespace(
                symbol='000660',
                agent_name='eod-summarizer',
                run_id='retry-run',
                received_at=datetime(2026, 4, 14, 1, 1, tzinfo=timezone.utc),
                id=2,
            ),
            SimpleNamespace(
                symbol='005930',
                agent_name='technical-summarizer',
                run_id='retry-run',
                received_at=datetime(2026, 4, 14, 1, 2, tzinfo=timezone.utc),
                id=3,
            ),
        ]
        fake_session.failure_rows = []
        runtime_summary = {
            'run_id': 'retry-run',
            'run_status': 'running',
            'run_table_status': 'running',
            'is_live': True,
            'current_role': 'technical-summarizer',
            'completed_roles': ['eod-summarizer'],
            'remaining_roles': ['technical-summarizer'],
            'cycle_number': 1,
            'completed_cycles': 0,
            'max_cycles': 3,
            'cycle_mode': 'daily_retry_cycle_role_sequenced',
            'cycle_status': 'running',
            'parallel_per_role': 2,
            'total_symbols': 2,
            'processed_symbols': 2,
            'completed_symbols': 0,
            'in_progress_symbols': 2,
            'pending_symbols': 0,
            'failed_items': 0,
            'completed_feedback_count': 2,
            'expected_feedback_count': 4,
            'role_progress': {
                'technical-summarizer': {
                    'role': 'technical-summarizer',
                    'status': 'running',
                    'boundary_status': 'running',
                    'total_symbols': 2,
                    'processed_symbols': 1,
                    'completed_symbols': 0,
                    'in_progress_symbols': 1,
                    'pending_symbols': 1,
                    'failed_items': 0,
                    'progress_ratio': 0.5,
                    'progress_percent': 50.0,
                }
            },
        }
        runtime_state = SimpleNamespace(
            requested_symbols=['005930', '000660'],
            to_progress_summary=lambda: runtime_summary,
        )
        run_snapshot = {
            'run_id': 'retry-run',
            'status': 'running',
            'symbols_total': 2,
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
                    'remaining_roles': ['technical-summarizer'],
                    'cycle_number': 1,
                    'completed_cycles': 0,
                    'max_cycles': 3,
                    'cycle_mode': 'daily_retry_cycle_role_sequenced',
                    'cycle_status': 'running',
                },
            },
            'request_payload': {
                'mode': 'daily_retry_cycle_role_sequenced',
                'symbols': ['005930', '000660'],
                'requested_symbols': ['005930', '000660'],
                'roles': ['eod-summarizer', 'technical-summarizer'],
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
                        expected_agents=['eod-summarizer', 'technical-summarizer'],
                    )

        technical_progress = result['summary']['role_progress']['technical-summarizer']
        self.assertEqual(technical_progress['status'], 'running')
        self.assertEqual(technical_progress['boundary_status'], 'running')
        self.assertEqual(technical_progress['completed_symbols'], 1)
        self.assertEqual(technical_progress['in_progress_symbols'], 1)
        self.assertEqual(technical_progress['processed_symbols'], 2)
        self.assertEqual(technical_progress['pending_symbols'], 0)
        self.assertEqual(technical_progress['progress_percent'], 100.0)
        self.assertEqual(
            result['result']['roles']['technical-summarizer'],
            {'requested_count': 2, 'completed_count': 1, 'error_count': 0},
        )

    def test_feedback_progress_uses_terminal_runtime_role_scopes_for_heterogeneous_retry_cycle(self):
        roles = ['eod-summarizer', 'technical-summarizer', 'fundamental-summarizer']
        role_totals = {
            'eod-summarizer': 200,
            'technical-summarizer': 150,
            'fundamental-summarizer': 120,
        }

        def build_role_progress(*, total: int, completed: int) -> dict[str, object]:
            return {
                'status': 'completed' if completed == total else 'partial',
                'boundary_status': 'completed' if completed == total else 'partial',
                'total_symbols': total,
                'processed_symbols': completed,
                'completed_symbols': completed,
                'in_progress_symbols': 0,
                'pending_symbols': max(0, total - completed),
                'failed_items': 0,
                'progress_ratio': completed / total,
                'progress_percent': completed / total * 100,
            }

        runtime_summary = {
            'run_id': 'retry-run-heterogeneous',
            'run_status': 'completed',
            'run_table_status': 'completed',
            'is_live': False,
            'current_role': None,
            'completed_roles': roles,
            'remaining_roles': [],
            'cycle_mode': 'daily_retry_cycle_role_sequenced',
            'cycle_status': 'completed',
            'total_symbols': 200,
            'processed_symbols': 200,
            'completed_symbols': 200,
            'in_progress_symbols': 0,
            'pending_symbols': 0,
            'failed_items': 0,
            'completed_feedback_count': 470,
            'computed_feedback_count': 450,
            'reused_feedback_count': 20,
            'stale_feedback_count': 0,
            'expected_feedback_count': 470,
            'role_symbol_counts': role_totals,
            'role_progress': {
                role: build_role_progress(total=total, completed=total)
                for role, total in role_totals.items()
            },
        }
        runtime_state = SimpleNamespace(
            requested_symbols=[f'{index:06d}' for index in range(200)],
            to_progress_summary=lambda: runtime_summary,
        )
        run_snapshot = {
            'run_id': 'retry-run-heterogeneous',
            'status': 'completed',
            'symbols_total': 470,
            'completed_count': 470,
            'failed_count': 0,
            'started_at': datetime(2026, 9, 18, 6, 0, tzinfo=timezone.utc),
            'completed_at': datetime(2026, 9, 18, 8, 0, tzinfo=timezone.utc),
            'request_payload': {
                'mode': 'daily_retry_cycle_role_sequenced',
                'symbols': runtime_state.requested_symbols,
                'roles': roles,
            },
            'summary_payload': {'progress_summary': runtime_summary},
        }
        inflated_persisted_progress = {
            'result': {
                'market_date': '2026-09-18',
                'roles': {
                    role: {
                        'requested_count': 200,
                        'completed_count': role_totals[role],
                        'error_count': 0,
                    }
                    for role in roles
                },
            },
            'summary': {
                'run_id': None,
                'run_status': 'partial',
                'run_table_status': None,
                'is_live': False,
                'total_symbols': 200,
                'processed_symbols': 200,
                'completed_symbols': 120,
                'in_progress_symbols': 0,
                'partial_symbols': 80,
                'pending_symbols': 0,
                'failed_items': 1,
                'completed_feedback_count': 469,
                'computed_feedback_count': 469,
                'reused_feedback_count': 0,
                'stale_feedback_count': 2,
                'expected_feedback_count': 600,
                'role_progress': {
                    role: build_role_progress(total=200, completed=role_totals[role])
                    for role in roles
                },
            },
        }

        with patch(
            'app.agent_context.progress_service.agent_context_feedback_run_state_service.find_latest_run',
            return_value=run_snapshot,
        ):
            with patch(
                'app.agent_context.progress_service.runtime_feedback_state_store.get_run',
                return_value=runtime_state,
            ):
                with patch.object(
                    AgentContextProgressService,
                    '_build_persisted_progress',
                    return_value=inflated_persisted_progress,
                ):
                    result = AgentContextProgressService().get_feedback_progress(
                        market_date='2026-09-18',
                        symbols=None,
                    )

        self.assertEqual(result['summary']['expected_feedback_count'], 470)
        self.assertEqual(result['summary']['completed_feedback_count'], 470)
        self.assertEqual(result['summary']['computed_feedback_count'], 450)
        self.assertEqual(result['summary']['reused_feedback_count'], 20)
        self.assertEqual(result['summary']['stale_feedback_count'], 0)
        self.assertEqual(result['summary']['failed_items'], 0)
        self.assertEqual(
            {
                role: result['summary']['role_progress'][role]['total_symbols']
                for role in roles
            },
            role_totals,
        )
        self.assertEqual(
            {
                role: result['result']['roles'][role]['requested_count']
                for role in roles
            },
            role_totals,
        )
        stage_result = {'status': 'completed', 'progress': result}
        self.assertEqual(
            DailyPipelineStageService._extract_stage_counts(
                stage_id='agent_feedback_summaries',
                result=stage_result,
            ),
            {'processed': 470, 'total': 470, 'succeeded': 470, 'failed': 0},
        )
        self.assertEqual(
            DailyPipelineStageService._resolve_result_status(stage_result),
            'completed',
        )

    def test_terminal_retry_merge_keeps_partial_and_failed_aggregates_consistent(self):
        service = AgentContextProgressService()
        for run_status, role_status in (
            ('partial', 'completed_with_failures'),
            ('failed', 'failed'),
        ):
            with self.subTest(run_status=run_status):
                runtime_summary = {
                    'run_status': run_status,
                    'run_table_status': run_status,
                    'is_live': False,
                    'expected_feedback_count': 2,
                    'completed_feedback_count': 1,
                    'computed_feedback_count': 1,
                    'reused_feedback_count': 0,
                    'stale_feedback_count': 0,
                    'failed_items': 1,
                    'role_progress': {
                        'technical-summarizer': {
                            'status': role_status,
                            'boundary_status': role_status,
                            'total_symbols': 2,
                            'processed_symbols': 2,
                            'completed_symbols': 1,
                            'in_progress_symbols': 0,
                            'pending_symbols': 0,
                            'failed_items': 1,
                        }
                    },
                }
                merged = service._merge_live_retry_summary_with_persisted_progress(
                    persisted_summary={
                        'completed_feedback_count': 2,
                        'computed_feedback_count': 2,
                        'reused_feedback_count': 0,
                        'stale_feedback_count': 1,
                        'failed_items': 0,
                        'role_progress': {
                            'technical-summarizer': {
                                'total_symbols': 200,
                                'completed_symbols': 2,
                                'failed_items': 0,
                            }
                        },
                    },
                    runtime_summary=runtime_summary,
                    run_snapshot={
                        'run_id': f'retry-run-{run_status}',
                        'status': run_status,
                        'started_at': None,
                        'completed_at': datetime(2026, 9, 18, 8, 0, tzinfo=timezone.utc),
                        'request_payload': {},
                    },
                )

                self.assertEqual(merged['completed_feedback_count'], 1)
                self.assertEqual(merged['failed_items'], 1)
                self.assertEqual(merged['stale_feedback_count'], 0)
                self.assertEqual(
                    merged['role_progress']['technical-summarizer']['total_symbols'],
                    2,
                )
                self.assertEqual(
                    merged['role_progress']['technical-summarizer']['failed_items'],
                    1,
                )

    def test_feedback_progress_refreshes_live_run_snapshot_from_persisted_counts_without_runtime_state(self):
        fake_session = FakeProgressSession()
        fake_session.market_universe_symbols = ['005930', '000660', '035420']
        fake_session.feedback_rows = [
            SimpleNamespace(
                symbol='005930',
                agent_name='eod-summarizer',
                run_id='retry-run',
                received_at=datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
                id=1,
            ),
            SimpleNamespace(
                symbol='000660',
                agent_name='eod-summarizer',
                run_id='retry-run',
                received_at=datetime(2026, 4, 14, 1, 1, tzinfo=timezone.utc),
                id=2,
            ),
            SimpleNamespace(
                symbol='005930',
                agent_name='technical-summarizer',
                run_id='retry-run',
                received_at=datetime(2026, 4, 14, 1, 2, tzinfo=timezone.utc),
                id=3,
            ),
        ]
        fake_session.failure_rows = []
        run_snapshot = {
            'run_id': 'retry-run',
            'status': 'running',
            'symbols_total': 2,
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
                    'remaining_roles': ['technical-summarizer'],
                    'cycle_number': 1,
                    'completed_cycles': 0,
                    'max_cycles': 3,
                    'cycle_mode': 'daily_retry_cycle_role_sequenced',
                    'cycle_status': 'running',
                    'completed_feedback_count': 2,
                    'expected_feedback_count': 4,
                    'role_progress': {
                        'technical-summarizer': {
                            'role': 'technical-summarizer',
                            'status': 'pending',
                            'total_symbols': 2,
                            'processed_symbols': 0,
                            'completed_symbols': 0,
                            'in_progress_symbols': 0,
                            'pending_symbols': 2,
                            'failed_items': 0,
                            'progress_ratio': 0.0,
                            'progress_percent': 0.0,
                        }
                    },
                },
            },
            'request_payload': {
                'mode': 'daily_retry_cycle_role_sequenced',
                'symbols': ['005930', '000660'],
                'requested_symbols': ['005930', '000660'],
                'roles': ['eod-summarizer', 'technical-summarizer'],
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
                        symbols=None,
                        expected_agents=['eod-summarizer', 'technical-summarizer'],
                    )

        technical_progress = result['summary']['role_progress']['technical-summarizer']
        self.assertEqual(result['summary']['run_id'], 'retry-run')
        self.assertEqual(result['summary']['run_status'], 'running')
        self.assertEqual(result['summary']['current_role'], 'technical-summarizer')
        self.assertEqual(result['summary']['completed_roles'], ['eod-summarizer'])
        self.assertEqual(result['summary']['completed_feedback_count'], 3)
        self.assertEqual(result['summary']['expected_feedback_count'], 4)
        self.assertEqual(technical_progress['status'], 'partial')
        self.assertEqual(technical_progress['completed_symbols'], 1)
        self.assertEqual(technical_progress['pending_symbols'], 1)
        self.assertEqual(
            result['result']['roles']['technical-summarizer'],
            {'requested_count': 2, 'completed_count': 1, 'error_count': 0},
        )

    def test_feedback_progress_refreshes_live_symbol_run_from_persisted_child_rows(self):
        fake_session = FakeProgressSession()
        fake_session.market_universe_symbols = ['005930', '000660']
        fake_session.feedback_rows = [
            SimpleNamespace(
                symbol='005930',
                agent_name='eod-summarizer',
                run_id='symbol-run-2026-06-19-stale',
                received_at=datetime(2026, 6, 19, 9, 30, tzinfo=timezone.utc),
                id=1,
            ),
            SimpleNamespace(
                symbol='000660',
                agent_name='eod-summarizer',
                run_id='symbol-run-2026-06-19-stale',
                received_at=datetime(2026, 6, 19, 9, 39, tzinfo=timezone.utc),
                id=2,
            ),
        ]
        fake_session.failure_rows = [
            SimpleNamespace(
                symbol='000660',
                agent_name='technical-summarizer',
                run_id='symbol-run-2026-06-19-stale',
                failure_kind='openclaw_transport_timeout',
                retryable=True,
                summary_payload={'error': 'ReadTimeout'},
                context_payload={'dispatch_id': 'dispatch-timeout'},
                received_at=datetime(2026, 6, 19, 9, 38, tzinfo=timezone.utc),
                id=10,
            )
        ]
        run_snapshot = {
            'run_id': 'symbol-run-2026-06-19-stale',
            'status': 'running',
            'symbols_total': 2,
            'completed_count': 0,
            'failed_count': 0,
            'started_at': datetime(2026, 6, 19, 7, 53, tzinfo=timezone.utc),
            'completed_at': None,
            'request_payload': {
                'mode': 'role_sequential_runtime_buffered',
                'symbols': ['005930', '000660'],
                'requested_symbols': ['005930', '000660'],
                'roles': ['eod-summarizer', 'technical-summarizer'],
            },
            'summary_payload': {
                'progress_summary': {
                    'run_id': 'symbol-run-2026-06-19-stale',
                    'run_status': 'running',
                    'run_table_status': 'running',
                    'is_live': True,
                    'current_role': 'eod-summarizer',
                    'completed_feedback_count': 0,
                    'failed_items': 0,
                    'expected_feedback_count': 4,
                    'latest_feedback_at': '2026-06-19T07:53:00+00:00',
                    'role_progress': {
                        'eod-summarizer': {
                            'role': 'eod-summarizer',
                            'status': 'running',
                            'total_symbols': 2,
                            'processed_symbols': 0,
                            'completed_symbols': 0,
                            'in_progress_symbols': 0,
                            'pending_symbols': 2,
                            'failed_items': 0,
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
                    'app.agent_context.progress_service.agent_context_feedback_run_state_service.get_run',
                    return_value=run_snapshot,
                ):
                    result = AgentContextProgressService().get_feedback_progress(
                        market_date='2026-06-19',
                        run_id='symbol-run-2026-06-19-stale',
                    )

        summary = result['summary']
        self.assertEqual(summary['run_status'], 'running')
        self.assertEqual(summary['completed_feedback_count'], 2)
        self.assertEqual(summary['failed_items'], 1)
        self.assertEqual(summary['expected_feedback_count'], 4)
        self.assertEqual(summary['latest_feedback_at'], '2026-06-19T09:39:00+00:00')
        self.assertEqual(summary['latest_failed_feedback_at'], '2026-06-19T09:38:00+00:00')
        self.assertEqual(summary['role_progress']['eod-summarizer']['completed_symbols'], 2)
        self.assertEqual(summary['role_progress']['technical-summarizer']['failed_items'], 1)

    def test_feedback_progress_live_non_retry_run_ignores_prior_child_rows_from_other_run(self):
        fake_session = FakeProgressSession()
        fake_session.market_universe_symbols = ['005930', '000660']
        fake_session.feedback_rows = [
            SimpleNamespace(
                symbol='005930',
                agent_name='eod-summarizer',
                run_id='older-symbol-run',
                received_at=datetime(2026, 6, 19, 9, 30, tzinfo=timezone.utc),
                id=1,
            ),
            SimpleNamespace(
                symbol='000660',
                agent_name='eod-summarizer',
                run_id='older-symbol-run',
                received_at=datetime(2026, 6, 19, 9, 39, tzinfo=timezone.utc),
                id=2,
            ),
        ]
        fake_session.failure_rows = [
            SimpleNamespace(
                symbol='000660',
                agent_name='technical-summarizer',
                run_id='older-symbol-run',
                failure_kind='openclaw_transport_timeout',
                retryable=True,
                summary_payload={'error': 'ReadTimeout'},
                context_payload={'dispatch_id': 'older-timeout'},
                received_at=datetime(2026, 6, 19, 9, 38, tzinfo=timezone.utc),
                id=10,
            )
        ]
        run_snapshot = {
            'run_id': 'active-symbol-run',
            'status': 'running',
            'symbols_total': 2,
            'completed_count': 0,
            'failed_count': 0,
            'started_at': datetime(2026, 6, 19, 10, 0, tzinfo=timezone.utc),
            'completed_at': None,
            'request_payload': {
                'mode': 'role_sequential_runtime_buffered',
                'symbols': ['005930', '000660'],
                'requested_symbols': ['005930', '000660'],
                'roles': ['eod-summarizer', 'technical-summarizer'],
                'reuse_existing': False,
            },
            'summary_payload': {
                'progress_summary': {
                    'run_id': 'active-symbol-run',
                    'run_status': 'running',
                    'run_table_status': 'running',
                    'is_live': True,
                    'current_role': 'eod-summarizer',
                    'total_symbols': 2,
                    'processed_symbols': 0,
                    'completed_symbols': 0,
                    'in_progress_symbols': 0,
                    'pending_symbols': 2,
                    'completed_feedback_count': 0,
                    'failed_items': 0,
                    'expected_feedback_count': 4,
                    'latest_feedback_at': None,
                    'role_progress': {
                        'eod-summarizer': {
                            'role': 'eod-summarizer',
                            'status': 'running',
                            'total_symbols': 2,
                            'processed_symbols': 0,
                            'completed_symbols': 0,
                            'in_progress_symbols': 0,
                            'pending_symbols': 2,
                            'failed_items': 0,
                        },
                        'technical-summarizer': {
                            'role': 'technical-summarizer',
                            'status': 'pending',
                            'total_symbols': 2,
                            'processed_symbols': 0,
                            'completed_symbols': 0,
                            'in_progress_symbols': 0,
                            'pending_symbols': 2,
                            'failed_items': 0,
                        },
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
                    'app.agent_context.progress_service.agent_context_feedback_run_state_service.get_run',
                    return_value=run_snapshot,
                ):
                    with patch(
                        'app.agent_context.progress_service.agent_context_reuse_service.build_latest_decisions',
                        return_value={
                            '005930': {
                                'eod-summarizer': _reuse_decision(
                                    symbol='005930',
                                    agent_name='eod-summarizer',
                                )
                            }
                        },
                    ) as build_latest_decisions:
                        result = AgentContextProgressService().get_feedback_progress(
                            market_date='2026-06-19',
                            run_id='active-symbol-run',
                        )

        build_latest_decisions.assert_not_called()

        feedback_sql = fake_session._compile(fake_session.statements[0])
        failure_sql = fake_session._compile(fake_session.statements[1])
        self.assertIn("agent_context_feedback.run_id = 'active-symbol-run'", feedback_sql)
        self.assertIn("agent_context_feedback_failures.run_id = 'active-symbol-run'", failure_sql)

        summary = result['summary']
        self.assertEqual(summary['run_id'], 'active-symbol-run')
        self.assertEqual(summary['run_status'], 'running')
        self.assertEqual(summary['completed_feedback_count'], 0)
        self.assertEqual(summary['failed_items'], 0)
        self.assertEqual(summary['expected_feedback_count'], 4)
        self.assertEqual(summary['latest_feedback_at'], None)
        self.assertEqual(summary['latest_failed_feedback_at'], None)
        self.assertEqual(summary['role_progress']['eod-summarizer']['completed_symbols'], 0)
        self.assertEqual(summary['role_progress']['technical-summarizer']['failed_items'], 0)

    def test_feedback_progress_keeps_running_retry_cycle_checkpoint_live_when_cycle_completed(self):
        fake_session = FakeProgressSession()
        fake_session.feedback_rows = [
            SimpleNamespace(
                symbol='005930',
                agent_name='eod-summarizer',
                run_id='retry-run',
                received_at=datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
                id=1,
            ),
            SimpleNamespace(
                symbol='000660',
                agent_name='eod-summarizer',
                run_id='retry-run',
                received_at=datetime(2026, 4, 14, 1, 1, tzinfo=timezone.utc),
                id=2,
            ),
            SimpleNamespace(
                symbol='005930',
                agent_name='signal-decider',
                run_id='retry-run',
                received_at=datetime(2026, 4, 14, 1, 2, tzinfo=timezone.utc),
                id=3,
            ),
        ]
        fake_session.failure_rows = [
            SimpleNamespace(
                symbol='000660',
                agent_name='signal-decider',
                run_id='retry-run',
                failure_kind='openclaw_transport_timeout',
                retryable=True,
                summary_payload=None,
                context_payload=None,
                received_at=datetime(2026, 4, 14, 1, 3, tzinfo=timezone.utc),
                id=10,
            )
        ]
        runtime_state = RuntimeFeedbackRunState(
            run_id='retry-run',
            market_date=datetime(2026, 4, 14, tzinfo=timezone.utc).date(),
            requested_symbols=['005930', '000660'],
            requested_roles=['eod-summarizer', 'signal-decider'],
            role_sequence=['eod-summarizer', 'signal-decider'],
            parallel_per_role=2,
            batch_size=2,
            started_at=datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
            status='running',
            current_role=None,
            completed_roles=['eod-summarizer', 'signal-decider'],
            remaining_roles=[],
            cycle_number=1,
            completed_cycles=0,
            max_cycles=3,
            cycle_mode='daily_retry_cycle_role_sequenced',
            cycle_status='completed',
        )
        runtime_state.completed_symbols_by_role = {
            'eod-summarizer': {'005930', '000660'},
            'signal-decider': {'005930'},
        }
        runtime_state.failed_symbols_by_role = {'signal-decider': {'000660'}}
        runtime_state.role_boundary_statuses = {
            'eod-summarizer': 'completed',
            'signal-decider': 'completed_with_failures',
        }
        runtime_state.last_activity_at = datetime(2026, 4, 14, 1, 3, tzinfo=timezone.utc)
        run_snapshot = {
            'run_id': 'retry-run',
            'status': 'running',
            'symbols_total': 2,
            'started_at': datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
            'completed_at': None,
            'summary_payload': {
                'mode': 'daily_retry_cycle_role_sequenced',
                'progress_summary': {
                    'run_id': 'retry-run',
                    'run_status': 'running',
                    'run_table_status': 'running',
                    'is_live': True,
                    'current_role': None,
                    'completed_roles': ['eod-summarizer', 'signal-decider'],
                    'remaining_roles': [],
                    'cycle_number': 1,
                    'completed_cycles': 0,
                    'max_cycles': 3,
                    'cycle_mode': 'daily_retry_cycle_role_sequenced',
                    'cycle_status': 'completed',
                },
            },
            'request_payload': {
                'mode': 'daily_retry_cycle_role_sequenced',
                'symbols': ['005930', '000660'],
                'requested_symbols': ['005930', '000660'],
                'roles': ['eod-summarizer', 'signal-decider'],
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

        summary = result['summary']
        self.assertEqual(summary['run_id'], 'retry-run')
        self.assertEqual(summary['run_status'], 'running')
        self.assertEqual(summary['run_table_status'], 'running')
        self.assertTrue(summary['is_live'])
        self.assertEqual(summary['cycle_status'], 'completed')
        self.assertIsNone(summary['current_role'])
        self.assertEqual(summary['completed_symbols'], 1)
        self.assertEqual(summary['in_progress_symbols'], 1)
        self.assertEqual(summary['processed_symbols'], 2)
        self.assertEqual(summary['terminal_feedback_count'], 4)
        self.assertEqual(summary['progress_ratio'], 1.0)
        self.assertEqual(
            result['result']['roles']['signal-decider'],
            {'requested_count': 2, 'completed_count': 1, 'error_count': 1},
        )

    def _call_service(self, fake_session, *, symbols, expected_agents=None):
        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.agent_context.progress_service.session_scope', fake_session_scope):
            with patch('app.agent_context.progress_service.runtime_feedback_state_store.get_run', return_value=None):
                with patch('app.agent_context.progress_service.agent_context_feedback_run_state_service.find_latest_run', return_value=None):
                    with patch('app.agent_context.progress_service.agent_context_feedback_run_state_service.get_run', return_value=None):
                        with patch(
                            'app.agent_context.progress_service.agent_context_reuse_service.build_latest_decisions',
                            return_value={},
                        ):
                            return AgentContextProgressService().get_feedback_progress(
                                market_date='2026-04-14',
                                symbols=symbols,
                                expected_agents=expected_agents,
                            )

    def test_feedback_progress_explicit_symbol_subset_without_resolved_run_counts_prior_reuse(self):
        fake_session = FakeProgressSession()
        fake_session.feedback_rows = []
        fake_session.failure_rows = []
        reuse_decision = _reuse_decision(symbol='000660', agent_name='fundamental-summarizer')

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.agent_context.progress_service.session_scope', fake_session_scope):
            with patch('app.agent_context.progress_service.runtime_feedback_state_store.get_run', return_value=None) as get_runtime_run:
                with patch(
                    'app.agent_context.progress_service.agent_context_feedback_run_state_service.find_latest_run',
                    return_value=None,
                ):
                    with patch(
                        'app.agent_context.progress_service.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
                        return_value={'ready': True, 'status': 'ready', 'missing_prerequisites': []},
                    ):
                        with patch(
                            'app.agent_context.progress_service.agent_context_reuse_service.build_latest_decisions',
                            return_value={'000660': {'fundamental-summarizer': reuse_decision}},
                        ) as build_latest_decisions:
                            result = AgentContextProgressService().get_feedback_progress(
                                market_date='2026-04-14',
                                symbols=['000660'],
                                expected_agents=['fundamental-summarizer'],
                            )

        get_runtime_run.assert_not_called()
        build_latest_decisions.assert_called_once_with(
            market_date=date(2026, 4, 14),
            symbols=['000660'],
            roles=('fundamental-summarizer',),
        )
        summary = result['summary']
        role_summary = summary['role_progress']['fundamental-summarizer']
        self.assertIsNone(summary['run_id'])
        self.assertEqual(summary['run_status'], 'completed')
        self.assertEqual(summary['completed_feedback_count'], 1)
        self.assertEqual(summary['computed_feedback_count'], 0)
        self.assertEqual(summary['reused_feedback_count'], 1)
        self.assertEqual(summary['pending_symbols'], 0)
        self.assertEqual(role_summary['completed_symbols'], 1)
        self.assertEqual(role_summary['computed_symbols'], 0)
        self.assertEqual(role_summary['reused_symbols'], 1)
        self.assertEqual(role_summary['pending_symbols'], 0)
        self.assertEqual(
            result['result']['roles']['fundamental-summarizer'],
            {'requested_count': 1, 'completed_count': 1, 'error_count': 0},
        )

    def test_feedback_progress_resolved_run_reuse_opt_outs_do_not_count_prior_reuse(self):
        for case_name, opt_out_payload in (
            ('reuse_existing_false', {'reuse_existing': False}),
            ('force_recompute_true', {'force_recompute': True}),
        ):
            with self.subTest(case_name=case_name):
                fake_session = FakeProgressSession()
                fake_session.feedback_rows = []
                fake_session.failure_rows = []
                run_snapshot = {
                    'run_id': f'run-{case_name}',
                    'status': 'partial',
                    'symbols_total': 1,
                    'completed_count': 0,
                    'failed_count': 0,
                    'started_at': datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
                    'completed_at': datetime(2026, 4, 14, 1, 5, tzinfo=timezone.utc),
                    'request_payload': {
                        'symbols': ['000660'],
                        'requested_symbols': ['000660'],
                        'roles': ['fundamental-summarizer'],
                        **opt_out_payload,
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
                            with patch(
                                'app.agent_context.progress_service.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
                                return_value={'ready': True, 'status': 'ready', 'missing_prerequisites': []},
                            ):
                                with patch(
                                    'app.agent_context.progress_service.agent_context_reuse_service.build_latest_decisions',
                                    return_value={
                                        '000660': {
                                            'fundamental-summarizer': _reuse_decision(
                                                symbol='000660',
                                                agent_name='fundamental-summarizer',
                                            )
                                        }
                                    },
                                ) as build_latest_decisions:
                                    result = AgentContextProgressService().get_feedback_progress(
                                        market_date='2026-04-14',
                                        symbols=['000660'],
                                        expected_agents=['fundamental-summarizer'],
                                    )

                build_latest_decisions.assert_not_called()
                summary = result['summary']
                role_summary = summary['role_progress']['fundamental-summarizer']
                self.assertEqual(summary['run_id'], f'run-{case_name}')
                self.assertEqual(summary['completed_feedback_count'], 0)
                self.assertEqual(summary['reused_feedback_count'], 0)
                self.assertEqual(summary['pending_symbols'], 1)
                self.assertEqual(role_summary['completed_symbols'], 0)
                self.assertEqual(role_summary['reused_symbols'], 0)
                self.assertEqual(role_summary['pending_symbols'], 1)

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
                    with patch(
                        'app.agent_context.progress_service.agent_context_reuse_service.build_latest_decisions',
                        return_value={},
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

    def test_feedback_progress_without_symbols_run_or_state_avoids_market_universe_lookup(self):
        fake_session = FakeProgressSession()

        result = self._call_service(fake_session, symbols=None)

        self.assertEqual(fake_session.statements, [])
        self.assertEqual(result['summary']['total_symbols'], 0)
        self.assertEqual(result['result']['roles']['technical-summarizer']['requested_count'], 0)
        self.assertEqual(result['result']['roles']['technical-summarizer']['completed_count'], 0)

    def test_feedback_progress_without_symbols_run_or_state_skips_prefilter_when_prerequisites_ready(self):
        fake_session = FakeProgressSession()

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.agent_context.progress_service.session_scope', fake_session_scope):
            with patch('app.agent_context.progress_service.runtime_feedback_state_store.get_run', return_value=None):
                with patch(
                    'app.agent_context.progress_service.agent_context_feedback_run_state_service.find_latest_run',
                    return_value=None,
                ):
                    with patch(
                        'app.agent_context.progress_service.agent_context_feedback_run_state_service.get_run',
                        return_value=None,
                    ):
                        with patch(
                            'app.agent_context.progress_service.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
                            return_value={'ready': True, 'status': 'ready', 'missing_prerequisites': []},
                        ):
                            result = AgentContextProgressService().get_feedback_progress(
                                market_date='2026-04-14',
                                symbols=None,
                                expected_agents=['technical-summarizer'],
                            )

        self.assertEqual(fake_session.statements, [])
        self.assertEqual(result['summary']['total_symbols'], 0)
        self.assertEqual(result['result']['roles']['technical-summarizer']['requested_count'], 0)
        self.assertEqual(result['result']['roles']['technical-summarizer']['completed_count'], 0)

    def test_feedback_progress_without_symbols_uses_latest_durable_retry_cycle_run(self):
        fake_session = FakeProgressSession()
        run_snapshot = {
            'run_id': 'retry-run',
            'status': 'partial',
            'started_at': datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
            'completed_at': datetime(2026, 4, 14, 1, 10, tzinfo=timezone.utc),
            'request_payload': {
                'mode': 'daily_retry_cycle_role_sequenced',
                'feedback_scope_mode': 'decider_scope_fast_path',
                'roles': ['eod-summarizer', 'technical-summarizer', 'fundamental-summarizer', 'short-term-decider', 'signal-decider'],
                'symbols': ['005930', '000660'],
            },
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
                    'feedback_scope_mode': 'decider_scope_fast_path',
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
        self.assertIsNone(result['summary']['current_role'])
        self.assertFalse(result['summary']['is_live'])
        self.assertEqual(result['summary']['in_progress_symbols'], 0)
        self.assertEqual(result['summary']['partial_symbols'], 1)
        self.assertEqual(result['summary']['completed_roles'], ['eod-summarizer', 'technical-summarizer', 'fundamental-summarizer'])
        self.assertEqual(result['summary']['remaining_roles'], ['signal-decider'])
        self.assertEqual(result['summary']['cycle_mode'], 'retry_followup')
        self.assertEqual(result['request']['feedback_scope_mode'], 'decider_scope_fast_path')
        self.assertEqual(result['summary']['feedback_scope_mode'], 'decider_scope_fast_path')
        self.assertEqual(result['summary']['cycle_number'], 2)
        self.assertEqual(result['summary']['role_progress']['signal-decider']['retry_pending_symbols'], 1)
        self.assertEqual(result['summary']['terminal_feedback_count'], 8)
        self.assertEqual(result['summary']['progress_ratio'], 1.0)
        self.assertEqual(result['summary']['boundary_progress_ratio'], 1.0)
        self.assertEqual(result['result']['roles']['signal-decider']['error_count'], 1)
        self.assertEqual(find_latest_run.call_args.kwargs['request_modes'], ['daily_retry_cycle_role_sequenced'])

    def test_feedback_progress_prefers_complete_persisted_counts_over_stale_durable_run_snapshot(self):
        fake_session = FakeProgressSession()
        fake_session.market_universe_symbols = ['005930', '000660']
        fake_session.feedback_rows = [
            SimpleNamespace(
                symbol=symbol,
                agent_name=agent_name,
                run_id=f'{agent_name}-completed',
                received_at=datetime(2026, 4, 14, 1, index, tzinfo=timezone.utc),
                id=index,
            )
            for index, (symbol, agent_name) in enumerate(
                [
                    ('005930', 'eod-summarizer'),
                    ('005930', 'technical-summarizer'),
                    ('000660', 'eod-summarizer'),
                    ('000660', 'technical-summarizer'),
                ],
                start=1,
            )
        ]
        fake_session.failure_rows = [
            SimpleNamespace(
                symbol='005930',
                agent_name='eod-summarizer',
                run_id='stale-failed-run',
                received_at=datetime(2026, 4, 13, 1, 0, tzinfo=timezone.utc),
                id=100,
            )
        ]
        stale_run_snapshot = {
            'run_id': 'stale-failed-run',
            'status': 'partial',
            'symbols_total': 2,
            'completed_count': 0,
            'failed_count': 4,
            'started_at': datetime(2026, 4, 13, 1, 0, tzinfo=timezone.utc),
            'completed_at': datetime(2026, 4, 13, 1, 10, tzinfo=timezone.utc),
            'request_payload': {
                'mode': 'daily_retry_cycle_role_sequenced',
                'symbols': ['005930', '000660'],
                'requested_symbols': ['005930', '000660'],
                'roles': ['eod-summarizer', 'technical-summarizer'],
            },
            'summary_payload': {
                'progress_summary': {
                    'run_id': 'stale-failed-run',
                    'run_status': 'partial',
                    'failed_items': 4,
                    'completed_feedback_count': 0,
                    'expected_feedback_count': 4,
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
                    return_value=stale_run_snapshot,
                ):
                    result = AgentContextProgressService().get_feedback_progress(
                        market_date='2026-04-14',
                        symbols=None,
                        expected_agents=['eod-summarizer', 'technical-summarizer'],
                    )

        self.assertIsNone(result['summary']['run_id'])
        self.assertEqual(result['summary']['run_status'], 'completed')
        self.assertEqual(result['summary']['completed_feedback_count'], 4)
        self.assertEqual(result['summary']['expected_feedback_count'], 4)
        self.assertEqual(result['summary']['failed_items'], 0)
        self.assertEqual(result['summary']['completed_roles'], ['eod-summarizer', 'technical-summarizer'])
        self.assertEqual(result['summary']['remaining_roles'], [])
        self.assertEqual(result['result']['roles']['eod-summarizer']['completed_count'], 2)
        self.assertEqual(result['result']['roles']['technical-summarizer']['completed_count'], 2)

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
            result = self._call_service(
                fake_session,
                symbols=['005930'],
                expected_agents=[
                    'eod-summarizer',
                    'technical-summarizer',
                    'fundamental-summarizer',
                    'signal-decider',
                ],
            )

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
        self.assertEqual(summary['in_progress_symbols'], 0)
        self.assertEqual(summary['partial_symbols'], 1)
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

    def test_feedback_progress_groups_timeout_and_invalid_json_failures(self):
        fake_session = FakeProgressSession()
        fake_session.feedback_rows = []
        fake_session.failure_rows = [
            SimpleNamespace(
                symbol='005930',
                agent_name='technical-summarizer',
                run_id='timeout-run',
                failure_kind='openclaw_transport_timeout',
                retryable=True,
                summary_payload={
                    'exception_type': 'ReadTimeout',
                    'error': '',
                    'failure_classification': {
                        'failure_kind': 'openclaw_transport_timeout',
                        'failure_status': 'transport/timeout',
                        'failure_category': 'transport',
                        'reason': 'OpenClaw HTTP request timed out before a usable response was received',
                    },
                },
                context_payload={'dispatch_id': 'dispatch-timeout'},
                received_at=datetime(2026, 4, 14, 1, 8, tzinfo=timezone.utc),
                id=10,
            ),
            SimpleNamespace(
                symbol='000660',
                agent_name='fundamental-summarizer',
                run_id='invalid-json-run',
                failure_kind='openclaw_response_invalid_json',
                retryable=True,
                summary_payload={
                    'exception_type': 'JSONDecodeError',
                    'response_json_decode_error': True,
                },
                context_payload={'response_status_code': 200, 'response_text_excerpt': '<html>bad gateway</html>'},
                received_at=datetime(2026, 4, 14, 1, 9, tzinfo=timezone.utc),
                id=11,
            ),
        ]

        result = self._call_service(
            fake_session,
            symbols=['005930', '000660'],
            expected_agents=['technical-summarizer', 'fundamental-summarizer'],
        )

        groups = result['summary']['failure_groups']
        self.assertEqual(groups['transport/timeout']['count'], 1)
        self.assertEqual(groups['transport/timeout']['agents'], {'technical-summarizer': 1})
        self.assertEqual(groups['response/invalid-json']['count'], 1)
        self.assertEqual(groups['response/invalid-json']['agents'], {'fundamental-summarizer': 1})


if __name__ == '__main__':
    unittest.main()
