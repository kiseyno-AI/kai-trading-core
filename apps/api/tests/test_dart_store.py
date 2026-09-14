import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import patch

from app.dart.store import DartSyncRepository


class _FakeExecuteResult:
    def scalars(self):
        return []


class _FakeSession:
    def __init__(self):
        self.statement = None

    def execute(self, statement):
        self.statement = statement
        return _FakeExecuteResult()


class DartSyncRepositoryTests(unittest.TestCase):
    def test_list_recent_events_by_stock_codes_is_available_on_repository(self):
        repository = DartSyncRepository()

        result = repository.list_recent_events_by_stock_codes(
            stock_codes=['005930', '000660', '005930'],
            limit_per_symbol=0,
        )

        self.assertEqual(result, {'005930': [], '000660': []})

    def test_list_recent_events_by_stock_codes_uses_sql_partition_limit(self):
        repository = DartSyncRepository()
        fake_session = _FakeSession()

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.dart.store.session_scope', fake_session_scope):
            result = repository.list_recent_events_by_stock_codes(
                stock_codes=['005930', '000660'],
                receipt_date_to='2026-04-08',
                observed_at_or_after=datetime(
                    2026,
                    4,
                    8,
                    5,
                    30,
                    tzinfo=timezone.utc,
                ),
                observed_before=datetime(
                    2026,
                    4,
                    8,
                    6,
                    30,
                    tzinfo=timezone.utc,
                ),
                limit_per_symbol=3,
            )

        self.assertEqual(result, {'005930': [], '000660': []})
        compiled = str(fake_session.statement)
        self.assertIn('row_number()', compiled)
        self.assertIn('PARTITION BY dart_disclosure_events.stock_code', compiled)
        self.assertIn('dart_disclosure_events.created_at >=', compiled)
        self.assertIn('dart_disclosure_events.created_at <', compiled)
        self.assertIn('symbol_rank <=', compiled)
