import unittest
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.agent_context.run_state_service import AgentContextFeedbackRunStateService


class FakeExecuteResult:
    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return self

    def all(self):
        return self._rows


class FakeRunStateSession:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, statement):
        return FakeExecuteResult(self.rows)


def _build_run_row(
    *,
    row_id: int,
    run_id: str,
    market_date: date,
    started_at: datetime,
    request_payload: dict,
    status: str = 'completed',
    updated_at: datetime | None = None,
):
    return SimpleNamespace(
        id=row_id,
        run_id=run_id,
        market_date=market_date,
        status=status,
        symbols_total=2,
        completed_count=4,
        failed_count=0,
        request_payload=request_payload,
        summary_payload={},
        started_at=started_at,
        completed_at=started_at,
        created_at=started_at,
        updated_at=updated_at or started_at,
    )


class AgentContextFeedbackRunStateServiceTests(unittest.TestCase):
    def test_get_run_can_lock_exact_row_in_caller_transaction(self):
        service = AgentContextFeedbackRunStateService()
        run_row = _build_run_row(
            row_id=1,
            run_id='locked-summary-run',
            market_date=date(2026, 4, 14),
            started_at=datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
            request_payload={'roles': ['fundamental-summarizer']},
        )
        result = MagicMock()
        result.scalar_one_or_none.return_value = run_row
        caller_session = MagicMock()
        caller_session.execute.return_value = result

        with patch(
            'app.agent_context.run_state_service.session_scope'
        ) as session_scope_mock:
            snapshot = service.get_run(
                run_id='locked-summary-run',
                session=caller_session,
                for_update=True,
            )

        session_scope_mock.assert_not_called()
        statement = caller_session.execute.call_args.args[0]
        self.assertIn('FOR UPDATE', str(statement))
        self.assertEqual(snapshot['run_id'], 'locked-summary-run')

    def test_find_latest_run_matches_same_scope_when_symbol_and_role_order_differs(self):
        service = AgentContextFeedbackRunStateService()
        market_date = date.fromisoformat('2026-04-14')
        fake_session = FakeRunStateSession(
            [
                _build_run_row(
                    row_id=3,
                    run_id='newer-different-scope',
                    market_date=market_date,
                    started_at=datetime(2026, 4, 14, 1, 5, tzinfo=timezone.utc),
                    request_payload={
                        'symbols': ['005930', '035420'],
                        'roles': ['eod-summarizer', 'technical-summarizer'],
                    },
                ),
                _build_run_row(
                    row_id=2,
                    run_id='matching-run',
                    market_date=market_date,
                    started_at=datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
                    request_payload={
                        'symbols': ['000660', '005930'],
                        'roles': ['technical-summarizer', 'eod-summarizer'],
                    },
                ),
            ]
        )

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.agent_context.run_state_service.session_scope', fake_session_scope):
            result = service.find_latest_run(
                market_date=market_date,
                requested_symbols=['005930', '000660'],
                requested_roles=['eod-summarizer', 'technical-summarizer'],
            )

        self.assertIsNotNone(result)
        self.assertEqual(result['run_id'], 'matching-run')

    def test_find_latest_run_can_filter_by_request_mode(self):
        service = AgentContextFeedbackRunStateService()
        market_date = date.fromisoformat('2026-04-14')
        fake_session = FakeRunStateSession(
            [
                _build_run_row(
                    row_id=2,
                    run_id='symbol-dispatch-run',
                    market_date=market_date,
                    started_at=datetime(2026, 4, 14, 1, 5, tzinfo=timezone.utc),
                    request_payload={
                        'roles': ['technical-summarizer'],
                        'mode': 'role_sequential_runtime_buffered',
                    },
                ),
                _build_run_row(
                    row_id=1,
                    run_id='retry-cycle-run',
                    market_date=market_date,
                    started_at=datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
                    request_payload={
                        'roles': ['technical-summarizer'],
                        'mode': 'daily_retry_cycle_role_sequenced',
                    },
                ),
            ]
        )

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.agent_context.run_state_service.session_scope', fake_session_scope):
            result = service.find_latest_run(
                market_date=market_date,
                requested_roles=['technical-summarizer'],
                request_modes=['daily_retry_cycle_role_sequenced'],
            )

        self.assertIsNotNone(result)
        self.assertEqual(result['run_id'], 'retry-cycle-run')

    def test_find_latest_run_filters_by_feedback_scope_mode(self):
        service = AgentContextFeedbackRunStateService()
        market_date = date.fromisoformat('2026-04-14')
        fake_session = FakeRunStateSession(
            [
                _build_run_row(
                    row_id=2,
                    run_id='newer-broad-run',
                    market_date=market_date,
                    started_at=datetime(2026, 4, 14, 1, 5, tzinfo=timezone.utc),
                    request_payload={
                        'symbols': ['005930'],
                        'requested_symbols': ['005930'],
                        'roles': ['signal-decider'],
                        'mode': 'daily_retry_cycle_role_sequenced',
                        'feedback_scope_mode': 'broad_feedback_universe',
                    },
                ),
                _build_run_row(
                    row_id=1,
                    run_id='matching-fast-path-run',
                    market_date=market_date,
                    started_at=datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
                    request_payload={
                        'symbols': ['005930'],
                        'requested_symbols': ['005930'],
                        'roles': ['signal-decider'],
                        'mode': 'daily_retry_cycle_role_sequenced',
                        'feedback_scope_mode': 'decider_scope_fast_path',
                    },
                ),
            ]
        )

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.agent_context.run_state_service.session_scope', fake_session_scope):
            result = service.find_latest_run(
                market_date=market_date,
                requested_symbols=['005930'],
                requested_roles=['signal-decider'],
                request_modes=['daily_retry_cycle_role_sequenced'],
                feedback_scope_mode='decider_scope_fast_path',
            )

        self.assertIsNotNone(result)
        self.assertEqual(result['run_id'], 'matching-fast-path-run')

    def test_find_latest_run_matches_requested_symbol_scope_when_run_tracks_eligible_subset(self):
        service = AgentContextFeedbackRunStateService()
        market_date = date.fromisoformat('2026-04-14')
        fake_session = FakeRunStateSession(
            [
                _build_run_row(
                    row_id=1,
                    run_id='matching-run',
                    market_date=market_date,
                    started_at=datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
                    request_payload={
                        'symbols': ['005930'],
                        'requested_symbols': ['005930', '000660'],
                        'roles': ['eod-summarizer'],
                    },
                ),
            ]
        )

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.agent_context.run_state_service.session_scope', fake_session_scope):
            result = service.find_latest_run(
                market_date=market_date,
                requested_symbols=['005930', '000660'],
                requested_roles=['eod-summarizer'],
            )

        self.assertIsNotNone(result)
        self.assertEqual(result['run_id'], 'matching-run')

    def test_find_latest_run_marks_stale_running_rows_abandoned(self):
        service = AgentContextFeedbackRunStateService()
        market_date = date.fromisoformat('2026-04-14')
        stale_row = _build_run_row(
            row_id=2,
            run_id='stale-running-run',
            market_date=market_date,
            started_at=datetime.now(timezone.utc) - timedelta(hours=2),
            updated_at=datetime.now(timezone.utc) - timedelta(hours=1),
            status='running',
            request_payload={
                'symbols': ['005930'],
                'roles': ['signal-decider'],
            },
        )
        fake_session = FakeRunStateSession(
            [
                stale_row,
                _build_run_row(
                    row_id=1,
                    run_id='completed-run',
                    market_date=market_date,
                    started_at=datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
                    request_payload={
                        'symbols': ['005930'],
                        'roles': ['signal-decider'],
                    },
                ),
            ]
        )

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.agent_context.run_state_service.session_scope', fake_session_scope):
            result = service.find_latest_run(
                market_date=market_date,
                requested_symbols=['005930'],
                requested_roles=['signal-decider'],
            )

        self.assertIsNotNone(result)
        self.assertEqual(result['run_id'], 'completed-run')
        self.assertEqual(stale_row.status, 'abandoned')
        self.assertIsNotNone(stale_row.completed_at)
        self.assertGreaterEqual(stale_row.failed_count, 0)
        self.assertEqual(
            stale_row.summary_payload['stale_running_abandoned_reason'],
            'Marked abandoned after 30 minutes without completion.',
        )

    def test_startup_abandons_active_rows_and_revokes_retry_ownership(self):
        service = AgentContextFeedbackRunStateService()
        market_date = date.fromisoformat('2026-04-14')
        started_at = datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc)
        running_row = _build_run_row(
            row_id=2,
            run_id='orphaned-retry-run',
            market_date=market_date,
            started_at=started_at,
            status='running',
            request_payload={
                'mode': 'daily_retry_cycle_role_sequenced',
                'retry_admission': {'owner_token': 'old-owner'},
            },
        )
        running_row.symbols_total = 10
        running_row.completed_count = 3
        running_row.failed_count = 1
        running_row.completed_at = None
        running_row.summary_payload = {
            'existing': 'preserved',
            'progress_summary': {'expected_feedback_count': 30},
        }
        completed_row = _build_run_row(
            row_id=1,
            run_id='completed-run',
            market_date=market_date,
            started_at=started_at,
            status='completed',
            request_payload={'mode': 'daily_retry_cycle_role_sequenced'},
        )
        queued_row = _build_run_row(
            row_id=3,
            run_id='orphaned-queued-run',
            market_date=market_date,
            started_at=started_at,
            status='queued',
            request_payload={'mode': 'role_sequential_runtime_buffered'},
        )
        queued_row.completed_at = None
        fake_session = FakeRunStateSession(
            [running_row, queued_row, completed_row]
        )

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch(
            'app.agent_context.run_state_service.session_scope',
            fake_session_scope,
        ):
            result = service.mark_active_runs_abandoned(
                reason='Core restarted during feedback work.',
            )

        self.assertEqual(
            result,
            {
                'abandoned_runs': 2,
                'run_ids': [
                    'orphaned-retry-run',
                    'orphaned-queued-run',
                ],
            },
        )
        self.assertEqual(running_row.status, 'abandoned')
        self.assertEqual(running_row.failed_count, 27)
        self.assertIsNotNone(running_row.completed_at)
        self.assertEqual(
            running_row.request_payload['retry_admission'][
                'ownership_revoked_reason'
            ],
            'core_restart',
        )
        self.assertEqual(
            running_row.request_payload['retry_admission']['owner_token'],
            'old-owner',
        )
        self.assertEqual(
            running_row.summary_payload['startup_active_abandoned_reason'],
            'Core restarted during feedback work.',
        )
        self.assertEqual(
            running_row.summary_payload['existing'],
            'preserved',
        )
        self.assertEqual(
            running_row.summary_payload['lifecycle_stage'],
            'startup_abandoned',
        )
        self.assertEqual(
            running_row.summary_payload['progress_summary']['run_status'],
            'abandoned',
        )
        self.assertEqual(
            running_row.summary_payload['progress_summary'][
                'run_table_status'
            ],
            'abandoned',
        )
        self.assertFalse(
            running_row.summary_payload['progress_summary']['is_live']
        )
        self.assertIsNone(
            running_row.summary_payload['progress_summary']['current_role']
        )
        self.assertEqual(
            running_row.summary_payload['progress_summary']['failed_items'],
            27,
        )
        self.assertEqual(queued_row.status, 'abandoned')
        self.assertIsNotNone(queued_row.completed_at)
        self.assertEqual(completed_row.status, 'completed')

    def test_resolve_run_requested_symbols_prefers_actual_dispatch_scope(self):
        service = AgentContextFeedbackRunStateService()

        result = service.resolve_run_requested_symbols(
            run_snapshot={
                'symbols_total': 1,
                'request_payload': {
                    'symbols': ['005930'],
                    'requested_symbols': ['005930', '000660'],
                },
                'summary_payload': {},
            }
        )

        self.assertEqual(result, ['005930'])


if __name__ == '__main__':
    unittest.main()
