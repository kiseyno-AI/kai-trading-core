from datetime import date
import unittest
from unittest.mock import patch

from app.agent_context.derived_contexts import DailyPriceContextService, DerivedContextBackfillService


class DailyPriceContextServiceTests(unittest.TestCase):
    @patch('app.agent_context.derived_contexts.upsert_daily_price_context')
    @patch('app.agent_context.derived_contexts.load_recent_price_bars_by_symbol')
    def test_compute_and_persist_skips_symbols_without_bar_for_requested_market_date(
        self,
        load_recent_price_bars_by_symbol_mock,
        upsert_daily_price_context_mock,
    ):
        load_recent_price_bars_by_symbol_mock.return_value = {
            '005930': [
                {
                    'trade_date': '2026-04-07',
                    'open_price': 70000,
                    'high_price': 71000,
                    'low_price': 69000,
                    'close_price': 70500,
                    'volume': 100,
                    'turnover': 1000,
                    'source': 'pykrx',
                    'captured_at': '2026-04-07T15:30:00+09:00',
                },
                {
                    'trade_date': '2026-04-04',
                    'open_price': 68000,
                    'high_price': 70000,
                    'low_price': 67500,
                    'close_price': 69500,
                    'volume': 90,
                    'turnover': 900,
                    'source': 'pykrx',
                    'captured_at': '2026-04-04T15:30:00+09:00',
                },
            ]
        }

        result = DailyPriceContextService().compute_and_persist_for_market_date(
            market_date='2026-04-08',
            symbols=['005930'],
        )

        self.assertEqual(result['contexts_persisted'], 0)
        self.assertEqual(result['contexts_failed'], 1)
        self.assertEqual(result['failed_symbols'], ['005930'])
        self.assertEqual(result['items'], [])
        upsert_daily_price_context_mock.assert_not_called()

    @patch('app.agent_context.derived_contexts.upsert_daily_price_context')
    @patch('app.agent_context.derived_contexts.load_recent_price_bars_by_symbol')
    def test_compute_and_persist_persists_when_latest_bar_matches_requested_market_date(
        self,
        load_recent_price_bars_by_symbol_mock,
        upsert_daily_price_context_mock,
    ):
        load_recent_price_bars_by_symbol_mock.return_value = {
            '005930': [
                {
                    'trade_date': '2026-04-08',
                    'open_price': 70000,
                    'high_price': 72000,
                    'low_price': 69950,
                    'close_price': 71500,
                    'volume': 123456,
                    'turnover': 8800000000,
                    'source': 'pykrx',
                    'captured_at': '2026-04-08T15:30:00+09:00',
                },
                {
                    'trade_date': '2026-04-07',
                    'open_price': 69800,
                    'high_price': 70500,
                    'low_price': 69000,
                    'close_price': 69800,
                    'volume': 120000,
                    'turnover': 8200000000,
                    'source': 'pykrx',
                    'captured_at': '2026-04-07T15:30:00+09:00',
                },
            ]
        }

        result = DailyPriceContextService().compute_and_persist_for_market_date(
            market_date='2026-04-08',
            symbols=['005930'],
        )

        self.assertEqual(result['contexts_persisted'], 1)
        self.assertEqual(result['contexts_failed'], 0)
        self.assertEqual(result['failed_symbols'], [])
        upsert_daily_price_context_mock.assert_called_once()


class DerivedContextBackfillServiceTests(unittest.TestCase):
    @patch('app.agent_context.derived_contexts.daily_market_participation_context_service.compute_and_persist_from_rows')
    @patch('app.agent_context.derived_contexts.load_daily_signal_feature_context_rows')
    @patch('app.agent_context.derived_contexts.daily_price_context_service.compute_and_persist_for_market_date')
    @patch('app.agent_context.derived_contexts.load_recent_daily_price_trade_dates')
    def test_backfill_recent_history_processes_recent_trading_dates_and_skips_missing_participation_rows(
        self,
        load_recent_trade_dates_mock,
        compute_price_contexts_mock,
        load_signal_rows_mock,
        compute_participation_context_mock,
    ):
        load_recent_trade_dates_mock.return_value = ['2026-04-10', '2026-04-11']
        compute_price_contexts_mock.side_effect = [
            {'market_date': '2026-04-10', 'contexts_persisted': 2, 'contexts_failed': 0},
            {'market_date': '2026-04-11', 'contexts_persisted': 3, 'contexts_failed': 1},
        ]
        load_signal_rows_mock.side_effect = [
            [{'symbol': '005930', 'signal_count': 1}],
            [],
        ]
        compute_participation_context_mock.return_value = {
            'market': 'KRX',
            'market_date': '2026-04-10',
            'source': 'kis_ranking',
            'captured_symbol_count': 1,
            'context_payload': {'captured_symbol_count': 1},
        }

        service = DerivedContextBackfillService()
        result = service.backfill_recent_history(
            end_date='2026-04-11',
            trading_days=2,
            market='KRX',
        )

        load_recent_trade_dates_mock.assert_called_once_with(end_date=date.fromisoformat('2026-04-11'), limit=2)
        self.assertEqual(
            compute_price_contexts_mock.call_args_list[0].kwargs,
            {'market_date': '2026-04-10', 'source': 'pykrx'},
        )
        self.assertEqual(
            compute_price_contexts_mock.call_args_list[1].kwargs,
            {'market_date': '2026-04-11', 'source': 'pykrx'},
        )
        compute_participation_context_mock.assert_called_once_with(
            market_date='2026-04-10',
            rows=[{'symbol': '005930', 'signal_count': 1}],
            market='KRX',
            source='kis_ranking',
        )
        self.assertEqual(result['market_dates'], ['2026-04-10', '2026-04-11'])
        self.assertEqual(result['price_contexts']['dates_processed'], 2)
        self.assertEqual(result['price_contexts']['contexts_persisted'], 5)
        self.assertEqual(result['price_contexts']['contexts_failed'], 1)
        self.assertEqual(result['participation_contexts']['dates_processed'], 1)
        self.assertEqual(result['participation_contexts']['dates_skipped'], 1)
        self.assertEqual(
            result['participation_contexts']['skipped_dates'],
            [{'market_date': '2026-04-11', 'reason': 'no_signal_features_for_market_date'}],
        )

    def test_backfill_recent_history_requires_positive_trading_days(self):
        service = DerivedContextBackfillService()

        with self.assertRaisesRegex(ValueError, 'trading_days must be greater than 0'):
            service.backfill_recent_history(trading_days=0)


if __name__ == '__main__':
    unittest.main()
