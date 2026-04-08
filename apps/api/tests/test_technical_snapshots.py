import unittest

from app.eod.technical_snapshots import DailyTechnicalSnapshotService


def _bars() -> list[dict]:
    items: list[dict] = []
    for day in range(1, 61):
        close_price = 100 + day
        items.append(
            {
                'trade_date': f'2026-03-{day:02d}' if day <= 31 else f'2026-04-{day - 31:02d}',
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
