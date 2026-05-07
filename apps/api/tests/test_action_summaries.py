
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

    def test_rebuild_ignores_progress_callback_failures(self):
        service = DailyActionSummaryService()
        progress_updates: list[dict] = []

        def _failing_progress_callback(progress: dict) -> None:
            progress_updates.append(progress)
            raise RuntimeError('progress store unavailable')

        with (
            patch(
                'app.eod.action_summaries.load_daily_technical_snapshot_inputs',
                return_value=[_snapshot()],
            ),
            patch('app.eod.action_summaries.upsert_daily_action_summary') as upsert_mock,
        ):
            result = service.rebuild_for_market_date(
                market_date='2026-04-29',
                progress_callback=_failing_progress_callback,
            )

        self.assertEqual(result['action_summaries_persisted'], 1)
        self.assertEqual(result['action_summaries_failed'], 0)
        self.assertEqual(result['failed_symbols'], [])
        self.assertEqual([item['status'] for item in progress_updates], ['running', 'completed'])
        upsert_mock.assert_called_once()
