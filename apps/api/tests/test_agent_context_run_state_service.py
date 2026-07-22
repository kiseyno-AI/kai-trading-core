import unittest
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

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

    def test_find_latest_run_skips_stale_running_rows(self):
        service = AgentContextFeedbackRunStateService()
        market_date = date.fromisoformat('2026-04-14')
        fake_session = FakeRunStateSession(
            [
                _build_run_row(
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
                ),
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
