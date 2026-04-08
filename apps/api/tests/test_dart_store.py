import unittest

from app.dart.store import DartSyncRepository


class DartSyncRepositoryTests(unittest.TestCase):
    def test_list_recent_events_by_stock_codes_is_available_on_repository(self):
        repository = DartSyncRepository()

        result = repository.list_recent_events_by_stock_codes(
            stock_codes=['005930', '000660', '005930'],
            limit_per_symbol=0,
        )

        self.assertEqual(result, {'005930': [], '000660': []})
