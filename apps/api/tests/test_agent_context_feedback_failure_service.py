import unittest
from contextlib import contextmanager
from datetime import date, datetime, timezone
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


if __name__ == '__main__':
    unittest.main()
