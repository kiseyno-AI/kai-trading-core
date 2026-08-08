import unittest
from contextlib import contextmanager
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy.orm.exc import DetachedInstanceError

from app.agent_context.feedback_failure_service import AgentContextFeedbackFailureService


class DetachingRow(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.closed = False

    def __getitem__(self, key):
        if self.closed:
            raise DetachedInstanceError('row was accessed after session close')
        return super().__getitem__(key)


class FakeMappingResult:
    def __init__(self, row):
        self.row = row

    def mappings(self):
        return self

    def one(self):
        return self.row


class FakeSession:
    def __init__(self, row):
        self.row = row

    def execute(self, statement):
        return FakeMappingResult(self.row)


class FakeScalarResult:
    def __init__(self, rows):
        self.rows = rows

    def scalars(self):
        return self

    def all(self):
        return self.rows


class FakeRowsSession:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, statement):
        return FakeScalarResult(self.rows)


class AgentContextFeedbackFailureServiceTests(unittest.TestCase):
    def test_normalize_failure_kind_truncates_long_values(self):
        service = AgentContextFeedbackFailureService()

        result = service._normalize_failure_kind('x' * 80)

        self.assertEqual(len(result), 64)
        self.assertEqual(result, 'x' * 64)

    def test_record_failure_serializes_before_session_closes(self):
        service = AgentContextFeedbackFailureService()
        row = DetachingRow(
            {
                'id': 17,
                'market_date': date(2026, 4, 14),
                'symbol': '005930',
                'agent_name': 'signal-decider',
                'run_id': 'run-1',
                'failure_kind': 'gateway_failed',
                'retryable': True,
                'summary_payload': {
                    'error': 'gateway failed',
                    'failure_kind': 'dispatch_failed',
                    'raw_failure_kind': 'gateway_failed',
                    'failure_status': 'dispatch/error',
                    'failure_category': 'dispatch',
                    'failure_classification': {
                        'failure_kind': 'dispatch_failed',
                        'failure_status': 'dispatch/error',
                        'failure_category': 'dispatch',
                        'reason': 'agent feedback dispatch failed without a more specific classifier',
                    },
                },
                'context_payload': {
                    'dispatch_id': 'dispatch-1',
                    'failure_classification': {
                        'failure_kind': 'dispatch_failed',
                        'failure_status': 'dispatch/error',
                        'failure_category': 'dispatch',
                        'reason': 'agent feedback dispatch failed without a more specific classifier',
                    },
                },
                'received_at': datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
            }
        )
        fake_session = FakeSession(row)

        @contextmanager
        def fake_session_scope():
            try:
                yield fake_session
            finally:
                row.closed = True

        with patch('app.agent_context.feedback_failure_service.session_scope', fake_session_scope):
            result = service.record_failure(
                market_date='2026-04-14',
                symbol='005930',
                agent_name='signal-decider',
                run_id='run-1',
                failure_kind='gateway_failed',
                retryable=True,
                summary={'error': 'gateway failed'},
                context={'dispatch_id': 'dispatch-1'},
            )

        self.assertEqual(result['id'], 17)
        self.assertEqual(result['market_date'], '2026-04-14')
        self.assertEqual(result['agent_name'], 'signal-decider')
        self.assertEqual(result['summary']['error'], 'gateway failed')
        self.assertEqual(result['summary']['failure_status'], 'dispatch/error')
        self.assertTrue(row.closed)

    def test_record_failure_classifies_missing_upstream_dependency(self):
        service = AgentContextFeedbackFailureService()
        row = DetachingRow(
            {
                'id': 18,
                'market_date': date(2026, 4, 14),
                'symbol': '005930',
                'agent_name': 'signal-decider',
                'run_id': 'run-2',
                'failure_kind': 'dependency_missing_upstream',
                'retryable': True,
                'summary_payload': {
                    'error': 'missing_decider_upstream_summaries:fundamental-summarizer',
                    'failure_kind': 'dependency_missing_upstream',
                    'raw_failure_kind': 'missing_decider_upstream_summaries:fundamental-summarizer',
                    'failure_status': 'dependency/missing-upstream',
                    'failure_category': 'dependency',
                    'failure_classification': {
                        'failure_kind': 'dependency_missing_upstream',
                        'failure_status': 'dependency/missing-upstream',
                        'failure_category': 'dependency',
                        'reason': 'missing upstream summarizer feedback required by decider role',
                    },
                },
                'context_payload': {
                    'missing_roles': ['fundamental-summarizer'],
                    'failure_classification': {
                        'failure_kind': 'dependency_missing_upstream',
                        'failure_status': 'dependency/missing-upstream',
                        'failure_category': 'dependency',
                        'reason': 'missing upstream summarizer feedback required by decider role',
                    },
                },
                'received_at': datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
            }
        )
        fake_session = FakeSession(row)

        @contextmanager
        def fake_session_scope():
            try:
                yield fake_session
            finally:
                row.closed = True

        with patch('app.agent_context.feedback_failure_service.session_scope', fake_session_scope):
            result = service.record_failure(
                market_date='2026-04-14',
                symbol='005930',
                agent_name='signal-decider',
                run_id='run-2',
                failure_kind='missing_decider_upstream_summaries:fundamental-summarizer',
                retryable=True,
                summary={'error': 'missing_decider_upstream_summaries:fundamental-summarizer'},
                context={'missing_roles': ['fundamental-summarizer']},
            )

        self.assertEqual(result['failure_kind'], 'dependency_missing_upstream')
        self.assertEqual(result['summary']['raw_failure_kind'], 'missing_decider_upstream_summaries:fundamental-summarizer')
        self.assertEqual(result['summary']['failure_status'], 'dependency/missing-upstream')

    def test_get_grouped_summary_groups_by_date_run_agent_and_failure_kind(self):
        service = AgentContextFeedbackFailureService()
        rows = [
            SimpleNamespace(
                market_date=date(2026, 4, 14),
                run_id='run-1',
                agent_name='signal-decider',
                failure_kind='dependency_missing_upstream',
                retryable=True,
                symbol='005930',
                received_at=datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
                id=1,
            ),
            SimpleNamespace(
                market_date=date(2026, 4, 14),
                run_id='run-1',
                agent_name='signal-decider',
                failure_kind='dependency_missing_upstream',
                retryable=True,
                symbol='000660',
                received_at=datetime(2026, 4, 14, 1, 2, tzinfo=timezone.utc),
                id=2,
            ),
            SimpleNamespace(
                market_date=date(2026, 4, 14),
                run_id='run-2',
                agent_name='openclaw-gateway',
                failure_kind='openclaw_auth_rejected',
                retryable=False,
                symbol='__RUN__',
                received_at=datetime(2026, 4, 14, 2, 0, tzinfo=timezone.utc),
                id=3,
            ),
        ]
        fake_session = FakeRowsSession(rows)

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.agent_context.feedback_failure_service.session_scope', fake_session_scope):
            result = service.get_grouped_summary(market_date='2026-04-14', limit=10)

        self.assertEqual(result['summary']['group_count'], 2)
        self.assertEqual(result['summary']['failure_count'], 3)
        self.assertEqual(result['summary']['retryable_count'], 2)
        self.assertEqual(result['summary']['by_failure_kind']['dependency_missing_upstream'], 2)
        self.assertEqual(result['summary']['by_failure_kind']['openclaw_auth_rejected'], 1)
        groups_by_kind = {group['failure_kind']: group for group in result['groups']}
        self.assertEqual(groups_by_kind['dependency_missing_upstream']['failure_count'], 2)
        self.assertEqual(groups_by_kind['dependency_missing_upstream']['symbols_sample'], ['005930', '000660'])
        self.assertEqual(groups_by_kind['dependency_missing_upstream']['failure_status'], 'dependency/missing-upstream')
        self.assertEqual(groups_by_kind['openclaw_auth_rejected']['agent_name'], 'openclaw-gateway')
        self.assertEqual(groups_by_kind['openclaw_auth_rejected']['failure_status'], 'auth/rejected')


if __name__ == '__main__':
    unittest.main()
