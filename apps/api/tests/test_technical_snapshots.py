import unittest
from datetime import date, timedelta
from unittest.mock import patch

from app.eod.technical_snapshots import DailyTechnicalSnapshotService


def _bars() -> list[dict]:
    items: list[dict] = []
    start = date(2026, 2, 28)
    for day in range(1, 61):
        close_price = 100 + day
        items.append(
            {
                'trade_date': (start + timedelta(days=day)).isoformat(),
                'open_price': close_price - 1,
                'high_price': close_price + 2,
                'low_price': close_price - 3,
                'close_price': close_price,
                'volume': 1_000 + (day * 10),
                'turnover': 10_000 + (day * 100),
            }
        )
    items[-1]['trade_date'] = '2026-04-29'
    items[-1]['close_price'] = 180
    items[-1]['high_price'] = 182
    items[-1]['low_price'] = 176
    items[-1]['volume'] = 3_500
    return items


class DailyTechnicalSnapshotServiceTests(unittest.TestCase):
    def test_compute_snapshot_returns_expected_signal_shape(self):
        service = DailyTechnicalSnapshotService()

        result = service.compute_snapshot(
            symbol='005930',
            market_date='2026-04-29',
            bars=_bars(),
            source='pykrx',
        )

        self.assertEqual(result.symbol, '005930')
        self.assertEqual(result.trend_state, 'uptrend')
        self.assertEqual(result.ma_alignment, 'bullish')
        self.assertTrue(result.breakout_20d)
        self.assertFalse(result.breakdown_20d)
        self.assertIsNotNone(result.return_5d)
        self.assertIsNotNone(result.return_20d)
        self.assertGreater(result.volume_ratio_20d, 1.5)
        self.assertIsNotNone(result.atr_14)
        self.assertEqual(result.signal_payload['volume']['unusual_volume'], True)
        self.assertEqual(result.signal_payload['range']['breakout_20d'], True)

    def test_compute_snapshot_requires_full_prior_20_bar_window_for_breakout_flags(self):
        service = DailyTechnicalSnapshotService()
        bars = _bars()[:20]
        bars[-1]['trade_date'] = '2026-03-20'
        bars[-1]['close_price'] = 500
        bars[-1]['high_price'] = 505
        bars[-1]['low_price'] = 495

        result = service.compute_snapshot(
            symbol='005930',
            market_date='2026-03-20',
            bars=bars,
            source='pykrx',
        )

        self.assertFalse(result.breakout_20d)
        self.assertFalse(result.breakdown_20d)
        self.assertIsNone(result.signal_payload['range']['prior_20d_high'])
        self.assertIsNone(result.signal_payload['range']['prior_20d_low'])

    def test_compute_snapshot_requires_latest_bar_to_match_market_date(self):
        service = DailyTechnicalSnapshotService()
        bars = _bars()
        bars[-1]['trade_date'] = '2026-04-28'

        with self.assertRaisesRegex(ValueError, 'Latest stored bar'):
            service.compute_snapshot(
                symbol='005930',
                market_date='2026-04-29',
                bars=bars,
            )

    def test_compute_and_persist_ignores_progress_callback_failures(self):
        service = DailyTechnicalSnapshotService()
        progress_updates: list[dict] = []

        def _failing_progress_callback(progress: dict) -> None:
            progress_updates.append(progress)
            raise RuntimeError('progress store unavailable')

        with (
            patch(
                'app.eod.technical_snapshots.list_symbols_with_daily_bar_on_date',
                return_value=['005930'],
            ),
            patch(
                'app.eod.technical_snapshots.load_daily_bars_for_symbol',
                return_value=_bars(),
            ),
            patch('app.eod.technical_snapshots.upsert_daily_technical_snapshot') as upsert_mock,
        ):
            result = service.compute_and_persist_for_market_date(
                market_date='2026-04-29',
                progress_callback=_failing_progress_callback,
            )

        self.assertEqual(result['snapshots_persisted'], 1)
        self.assertEqual(result['snapshots_failed'], 0)
        self.assertEqual(result['failed_symbols'], [])
        self.assertEqual([item['status'] for item in progress_updates], ['running', 'completed'])
        upsert_mock.assert_called_once()
