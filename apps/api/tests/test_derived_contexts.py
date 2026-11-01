from datetime import date
import unittest
from unittest.mock import patch

from app.agent_context.derived_contexts import DailyPriceContextService, DerivedContextBackfillService, build_price_context


class DailyPriceContextServiceTests(unittest.TestCase):
    def setUp(self):
        def _publish(**kwargs):
            successful_symbols = [
                row['symbol'] for row in kwargs['rows']
            ]
            return {
                'successful_symbols': successful_symbols,
                'failed_symbols': [],
                'failed_symbol_errors': [],
                'scope_finalization': {
                    'ready': True,
                    'deleted_stale_row_count': 0,
                },
            }

        self.replace_scope_patcher = patch(
            'app.agent_context.derived_contexts.'
            'replace_daily_price_contexts_exact_scope',
            side_effect=_publish,
        )
        self.replace_scope_mock = self.replace_scope_patcher.start()
        self.addCleanup(self.replace_scope_patcher.stop)

    def test_build_price_context_uses_estimated_turnover_and_flags_zero_open_rows(self):
        context = build_price_context(
            [
                {
                    'trade_date': '2026-05-06',
                    'open_price': 0,
                    'high_price': 71500,
                    'low_price': 70500,
                    'close_price': 71000,
                    'volume': 120000,
                    'turnover': 0,
                    'source': 'pykrx',
                    'captured_at': '2026-05-06T15:30:00+09:00',
                },
                {
                    'trade_date': '2026-05-02',
                    'open_price': 70000,
                    'high_price': 70600,
                    'low_price': 69800,
                    'close_price': 70200,
                    'volume': 100000,
                    'turnover': 7020000000,
                    'source': 'pykrx',
                    'captured_at': '2026-05-02T15:30:00+09:00',
                },
            ]
        )

        self.assertEqual(context['liquidity']['turnover_source'], 'estimated')
        self.assertEqual(context['liquidity']['turnover_krw'], 8520000000)
        self.assertEqual(context['liquidity']['provider_turnover_krw'], None)
        self.assertEqual(context['liquidity']['estimated_turnover_krw'], 8520000000)
        self.assertTrue(context['price_bar_quality']['open_price_zero'])
        self.assertTrue(context['price_bar_quality']['turnover_missing_or_zero'])
        self.assertFalse(context['price_bar_quality']['volume_zero'])
        self.assertTrue(context['price_bar_quality']['gap_unreliable_due_to_open'])
        self.assertTrue(context['price_bar_quality']['possible_suspension_or_special_session'])
        self.assertEqual(
            context['price_bar_quality']['issue_codes'],
            [
                'open_price_zero',
                'turnover_missing_or_zero',
                'gap_unreliable_due_to_open',
                'possible_suspension_or_special_session',
            ],
        )
        self.assertEqual(context['candle']['direction'], 'unknown')
        self.assertEqual(context['candle']['body_points'], None)
        self.assertEqual(context['gap']['reliable'], False)
        self.assertEqual(context['gap']['gap_points'], None)
        self.assertEqual(context['gap']['direction'], None)

    @patch('app.agent_context.derived_contexts.load_recent_price_bars_by_symbol')
    def test_rebuild_skips_symbols_without_bar_for_requested_market_date(
        self,
        load_recent_price_bars_by_symbol_mock,
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

        result = DailyPriceContextService().rebuild_for_market_date(
            market_date='2026-04-08',
            symbols=['005930'],
        )

        self.assertEqual(result['contexts_persisted'], 0)
        self.assertEqual(result['contexts_failed'], 1)
        self.assertEqual(result['failed_symbols'], ['005930'])
        self.assertEqual(result['items'], [])
        self.replace_scope_mock.assert_called_once_with(
            market_date=date(2026, 4, 8),
            rows=[],
            exact_symbols=['005930'],
            run_id=None,
        )

    @patch('app.agent_context.derived_contexts.load_recent_price_bars_by_symbol')
    def test_rebuild_persists_when_latest_bar_matches_requested_market_date(
        self,
        load_recent_price_bars_by_symbol_mock,
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

        result = DailyPriceContextService().rebuild_for_market_date(
            market_date='2026-04-08',
            symbols=['005930'],
        )

        self.assertEqual(result['contexts_persisted'], 1)
        self.assertEqual(result['contexts_failed'], 0)
        self.assertEqual(result['failed_symbols'], [])
        self.replace_scope_mock.assert_called_once()

    @patch('app.agent_context.derived_contexts.load_recent_price_bars_by_symbol')
    def test_rebuild_publishes_processed_subset_against_separate_exact_scope(
        self,
        load_recent_price_bars_by_symbol_mock,
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
                }
            ]
        }

        result = DailyPriceContextService().rebuild_for_market_date(
            market_date='2026-04-08',
            symbols=['005930'],
            exact_scope_symbols=['005930', '000660'],
        )

        self.assertEqual(result['requested_symbols'], 2)
        self.assertEqual(result['successful_stock_codes'], ['005930'])
        self.assertEqual(result['retry_debt_stock_codes'], ['000660'])
        self.assertEqual(
            self.replace_scope_mock.call_args.kwargs['exact_symbols'],
            ['005930', '000660'],
        )

    @patch('app.agent_context.derived_contexts.load_recent_price_bars_by_symbol', return_value={})
    def test_rebuild_can_return_full_internal_retry_scope_without_expanding_public_preview(
        self,
        _load_recent_price_bars_by_symbol_mock,
    ):
        symbols = [f'{index:06d}' for index in range(60)]

        result = DailyPriceContextService().rebuild_for_market_date(
            market_date='2026-04-08',
            symbols=symbols,
            include_retry_metadata=True,
        )

        self.assertEqual(result['contexts_failed'], 60)
        self.assertEqual(result['failed_symbols'], symbols[:50])
        self.assertEqual(result['_retry_metadata']['failed_symbols'], symbols)
        self.replace_scope_mock.assert_called_once_with(
            market_date=date(2026, 4, 8),
            rows=[],
            exact_symbols=symbols,
            run_id=None,
        )

    @patch('app.agent_context.derived_contexts.build_price_context')
    @patch('app.agent_context.derived_contexts.load_recent_price_bars_by_symbol')
    def test_rebuild_isolates_build_and_upsert_failures_with_exact_outcomes(
        self,
        load_recent_price_bars_by_symbol_mock,
        build_price_context_mock,
    ):
        symbols = ['005930', '000660', '035420']
        load_recent_price_bars_by_symbol_mock.return_value = {
            symbol: [{'trade_date': '2026-04-08'}]
            for symbol in symbols
        }
        build_price_context_mock.side_effect = [
            RuntimeError('invalid bar payload'),
            {'close_price': 100},
            {'close_price': 200},
        ]
        self.replace_scope_mock.side_effect = None
        self.replace_scope_mock.return_value = {
            'successful_symbols': ['035420'],
            'failed_symbols': ['000660'],
            'failed_symbol_errors': [
                {'symbol': '000660', 'error': 'write failed'},
            ],
            'scope_finalization': {
                'ready': True,
                'deleted_stale_row_count': 2,
            },
        }

        result = DailyPriceContextService().rebuild_for_market_date(
            market_date='2026-04-08',
            symbols=symbols,
        )

        self.assertEqual(result['contexts_persisted'], 1)
        self.assertEqual(result['contexts_failed'], 2)
        self.assertEqual(result['failed_symbols'], ['005930', '000660'])
        self.assertEqual(result['selected_stock_codes'], symbols)
        self.assertEqual(result['successful_stock_codes'], ['035420'])
        self.assertEqual(
            result['retry_debt_stock_codes'],
            ['005930', '000660'],
        )
        self.assertEqual(
            result['scope_outcome_manifest'],
            [
                {'stock_code': '005930', 'outcome': 'retry_debt'},
                {'stock_code': '000660', 'outcome': 'retry_debt'},
                {'stock_code': '035420', 'outcome': 'success'},
            ],
        )
        self.replace_scope_mock.assert_called_once_with(
            market_date=date(2026, 4, 8),
            rows=[
                {
                    'symbol': '000660',
                    'market_date': '2026-04-08',
                    'source': 'pykrx',
                    'context_payload': {'close_price': 100},
                },
                {
                    'symbol': '035420',
                    'market_date': '2026-04-08',
                    'source': 'pykrx',
                    'context_payload': {'close_price': 200},
                },
            ],
            exact_symbols=symbols,
            run_id=None,
        )


class DerivedContextBackfillServiceTests(unittest.TestCase):
    @patch('app.agent_context.derived_contexts.daily_market_participation_context_service.rebuild_from_rows')
    @patch('app.agent_context.derived_contexts.load_daily_signal_feature_context_rows')
    @patch('app.agent_context.derived_contexts.daily_price_context_service.rebuild_for_market_date')
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
