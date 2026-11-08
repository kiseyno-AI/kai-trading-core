
import unittest
from unittest.mock import patch

from app.eod.action_summaries import DailyActionSummaryService


def _snapshot(**overrides) -> dict:
    item = {
        'technical_snapshot_id': 9,
        'symbol': '005930',
        'market_date': '2026-04-29',
        'source': 'pykrx',
        'close_price': 180,
        'volume': 3500,
        'trend_state': 'uptrend',
        'ma_alignment': 'bullish',
        'breakout_20d': True,
        'breakdown_20d': False,
        'return_5d': 8.2,
        'return_20d': 14.4,
        'volume_ratio_20d': 1.9,
        'atr_14': 4.0,
        'atr_pct_14': 2.2,
        'signal_payload': {
            'range': {
                'prior_20d_high': 171.0,
                'prior_20d_low': 149.0,
            },
            'volume': {
                'ratio_20d': 1.9,
            },
            'volatility': {
                'stop_context': {
                    'one_atr_below_close': 176.0,
                },
            },
        },
    }
    item.update(overrides)
    return item


class DailyActionSummaryServiceTests(unittest.TestCase):
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
            'app.eod.action_summaries.'
            'replace_daily_action_summaries_exact_scope',
            side_effect=_publish,
        )
        self.replace_scope_mock = self.replace_scope_patcher.start()
        self.addCleanup(self.replace_scope_patcher.stop)

    def test_compute_summary_returns_long_breakout_setup(self):
        service = DailyActionSummaryService()

        result = service.compute_summary(_snapshot())

        self.assertEqual(result.action_label, 'long_breakout')
        self.assertEqual(result.bias_direction, 'long')
        self.assertGreaterEqual(result.signal_score, 70)
        self.assertEqual(result.confidence_tier, 'high')
        self.assertEqual(result.risk_tier, 'low')
        self.assertEqual(result.trigger_price, 180.0)
        self.assertEqual(result.invalidation_price, 176.0)
        self.assertIn('fresh_20d_breakout', result.reasons)
        self.assertEqual(result.summary_payload['setup_state'], 'confirmed')

    def test_compute_summary_returns_risk_off_for_bearish_breakdown(self):
        service = DailyActionSummaryService()

        result = service.compute_summary(
            _snapshot(
                close_price=120,
                trend_state='downtrend',
                ma_alignment='bearish',
                breakout_20d=False,
                breakdown_20d=True,
                return_5d=-6.1,
                return_20d=-13.4,
                volume_ratio_20d=1.8,
                atr_14=6.0,
                atr_pct_14=5.0,
                signal_payload={
                    'range': {
                        'prior_20d_high': 154.0,
                        'prior_20d_low': 125.0,
                    },
                    'volatility': {
                        'stop_context': {
                            'one_atr_below_close': 114.0,
                        },
                    },
                },
            )
        )

        self.assertIn(result.action_label, {'short_breakdown', 'risk_off'})
        self.assertEqual(result.bias_direction, 'short')
        self.assertLessEqual(result.signal_score, -70)
        self.assertEqual(result.risk_tier, 'medium')
        self.assertEqual(result.invalidation_price, 126.0)
        self.assertIn('fresh_20d_breakdown', result.reasons)

    def test_compute_summary_returns_insufficient_data_when_snapshot_history_is_thin(self):
        service = DailyActionSummaryService()

        result = service.compute_summary(
            _snapshot(
                ma_alignment='insufficient_history',
                trend_state='insufficient_history',
                return_20d=None,
                volume_ratio_20d=None,
                atr_14=None,
                atr_pct_14=None,
                signal_payload={},
            )
        )

        self.assertEqual(result.action_label, 'insufficient_data')
        self.assertEqual(result.confidence_tier, 'low')
        self.assertEqual(result.risk_tier, 'unknown')

    def test_compute_summary_preserves_price_bar_quality_and_estimated_liquidity_context(self):
        service = DailyActionSummaryService()

        result = service.compute_summary(
            _snapshot(
                signal_payload={
                    'range': {
                        'prior_20d_high': 171.0,
                        'prior_20d_low': 149.0,
                    },
                    'volatility': {
                        'stop_context': {
                            'one_atr_below_close': 176.0,
                        },
                    },
                    'price_bar_context': {
                        'liquidity': {
                            'turnover_krw': 8520000000,
                            'turnover_source': 'estimated',
                            'turnover_is_estimated': True,
                            'provider_turnover_krw': None,
                            'estimated_turnover_krw': 8520000000,
                        },
                        'price_bar_quality': {
                            'open_price_zero': True,
                            'turnover_missing_or_zero': True,
                            'volume_zero': False,
                            'gap_unreliable_due_to_open': True,
                            'possible_suspension_or_special_session': True,
                            'issue_codes': [
                                'open_price_zero',
                                'turnover_missing_or_zero',
                                'gap_unreliable_due_to_open',
                                'possible_suspension_or_special_session',
                            ],
                        },
                        'gap': {
                            'direction': None,
                            'gap_percent': None,
                            'reliable': False,
                        },
                    },
                },
            )
        )

        self.assertEqual(
            result.summary_payload['data_quality']['liquidity']['turnover_source'],
            'estimated',
        )
        self.assertTrue(result.summary_payload['data_quality']['liquidity']['turnover_is_estimated'])
        self.assertTrue(result.summary_payload['data_quality']['price_bar_quality']['open_price_zero'])
        self.assertTrue(result.summary_payload['data_quality']['price_bar_quality']['gap_unreliable_due_to_open'])
        self.assertEqual(result.summary_payload['data_quality']['gap']['reliable'], False)

    def test_rebuild_propagates_progress_callback_failures(self):
        service = DailyActionSummaryService()
        progress_updates: list[dict] = []

        def _failing_progress_callback(progress: dict) -> None:
            progress_updates.append(progress)
            raise RuntimeError('progress store unavailable')

        with (
            patch(
                'app.eod.action_summaries.'
                'list_symbols_with_daily_bar_on_date',
                return_value=['005930'],
            ),
            patch(
                'app.eod.action_summaries.load_daily_technical_snapshot_inputs',
                return_value=[_snapshot()],
            ),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                'progress store unavailable',
            ):
                service.rebuild_for_market_date(
                    market_date='2026-04-29',
                    progress_callback=_failing_progress_callback,
                )

        self.assertEqual(
            [item['status'] for item in progress_updates],
            ['running'],
        )
        self.replace_scope_mock.assert_not_called()

    def test_rebuild_uses_exact_same_day_price_bar_universe_as_denominator(
        self,
    ):
        service = DailyActionSummaryService()

        with (
            patch(
                'app.eod.action_summaries.'
                'list_symbols_with_daily_bar_on_date',
                return_value=['005930', '000660'],
            ) as price_bar_symbols,
            patch(
                'app.eod.action_summaries.'
                'load_daily_technical_snapshot_inputs',
                return_value=[_snapshot()],
            ) as snapshot_inputs,
        ):
            result = service.rebuild_for_market_date(
                market_date='2026-04-29',
                include_retry_metadata=True,
            )

        price_bar_symbols.assert_called_once_with('2026-04-29')
        snapshot_inputs.assert_called_once_with(
            market_date='2026-04-29',
            symbols=['005930', '000660'],
            limit=100,
        )
        self.assertEqual(result['requested_symbols'], 2)
        self.assertEqual(result['action_summaries_persisted'], 1)
        self.assertEqual(result['action_summaries_failed'], 1)
        self.assertEqual(
            result['scope_outcome_manifest_version'],
            'action_summaries_scope_outcomes_v1',
        )
        self.assertEqual(result['successful_stock_codes'], ['005930'])
        self.assertEqual(result['retry_debt_stock_codes'], ['000660'])
        self.assertEqual(
            result['scope_outcome_manifest'],
            [
                {'stock_code': '005930', 'outcome': 'success'},
                {'stock_code': '000660', 'outcome': 'retry_debt'},
            ],
        )
        self.assertEqual(
            result['denominator_source'],
            'same_day_daily_price_bars',
        )
        self.replace_scope_mock.assert_called_once_with(
            market_date='2026-04-29',
            rows=[self.replace_scope_mock.call_args.kwargs['rows'][0]],
            exact_symbols=['005930', '000660'],
            run_id=None,
        )

    def test_rebuild_can_return_full_internal_retry_scope_without_expanding_public_preview(self):
        service = DailyActionSummaryService()
        symbols = [f'{index:06d}' for index in range(60)]

        with (
            patch('app.eod.action_summaries.load_daily_technical_snapshot_inputs', return_value=[]),
        ):
            result = service.rebuild_for_market_date(
                market_date='2026-04-29',
                symbols=symbols,
                include_retry_metadata=True,
            )

        self.assertEqual(result['action_summaries_failed'], 60)
        self.assertEqual(result['failed_symbols'], symbols[:50])
        self.assertEqual(result['_retry_metadata']['failed_symbols'], symbols)
        self.replace_scope_mock.assert_called_once_with(
            market_date='2026-04-29',
            rows=[],
            exact_symbols=symbols,
            run_id=None,
        )

    def test_rebuild_isolates_known_data_error_and_publishes_sibling(self):
        service = DailyActionSummaryService()
        snapshots = [_snapshot(symbol='005930'), _snapshot(symbol='000660')]

        with (
            patch(
                'app.eod.action_summaries.load_daily_technical_snapshot_inputs',
                return_value=snapshots,
            ),
            patch.object(
                service,
                'compute_summary',
                side_effect=[
                    ValueError('bad snapshot'),
                    service.compute_summary(snapshots[1]),
                ],
            ),
        ):
            result = service.rebuild_for_market_date(
                market_date='2026-04-29',
                symbols=['005930', '000660'],
            )

        self.assertEqual(result['successful_stock_codes'], ['000660'])
        self.assertEqual(result['retry_debt_stock_codes'], ['005930'])

    def test_rebuild_propagates_unknown_compute_failure(self):
        service = DailyActionSummaryService()

        with (
            patch(
                'app.eod.action_summaries.load_daily_technical_snapshot_inputs',
                return_value=[_snapshot()],
            ),
            patch.object(
                service,
                'compute_summary',
                side_effect=RuntimeError('unexpected action invariant'),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, 'unexpected action'):
                service.rebuild_for_market_date(
                    market_date='2026-04-29',
                    symbols=['005930'],
                )

        self.replace_scope_mock.assert_not_called()
