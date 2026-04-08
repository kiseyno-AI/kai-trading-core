import unittest

from app.dart.store import DartSyncRepository


class DartSyncRepositoryTests(unittest.TestCase):
    def test_repository_exposes_agent_context_and_inspect_helpers(self):
        repository = DartSyncRepository()

        self.assertTrue(callable(repository.list_recent_events_by_stock_codes))
        self.assertTrue(callable(repository.get_summary))

        result = repository.list_recent_events_by_stock_codes(
            stock_codes=['005930', '000660', '005930'],
            limit_per_symbol=0,
        )

        self.assertEqual(result, {'005930': [], '000660': []})
