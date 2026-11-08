import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

from app.signals import overtime_ranking_capture as capture
from app.signals.overtime_ranking_capture import (
    OVERTIME_FLUCTUATION_SOURCE,
    OVERTIME_VOLUME_SOURCE,
    OvertimeRankingCaptureStore,
    capture_overtime_rankings_if_due,
)

KST = ZoneInfo('Asia/Seoul')
CLOSED_TRADING_DAY = SimpleNamespace(state='CLOSED', is_trading_day=True)


def _kst(hour, minute, day=6):
    return datetime(2026, 11, day, hour, minute, tzinfo=KST)


def _client(volume_rows, fluct_rows):
    return SimpleNamespace(
        fetch_domestic_ranking_overtime_volume=AsyncMock(
            return_value={'output1': [{'ovtm_untp_exch_vol': '1'}], 'output2': volume_rows, 'page_count': 1}
        ),
        fetch_domestic_ranking_overtime_fluctuation=AsyncMock(
            return_value={'output1': [], 'output2': fluct_rows, 'page_count': 1}
        ),
    )


GOOD_VOLUME = [{'stck_shrn_iscd': '005930', 'ovtm_untp_vol': '1200'}, {'stck_shrn_iscd': '000660', 'ovtm_untp_vol': '0'}]
GOOD_FLUCT = [{'mksc_shrn_iscd': '005930', 'ovtm_untp_prdy_ctrt': '2.1'}]
ZERO_VOLUME = [{'stck_shrn_iscd': '005930', 'ovtm_untp_vol': '0'}]


class OvertimeRankingCaptureTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        capture._last_attempt_at.clear()
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.store = OvertimeRankingCaptureStore(Path(self._dir.name))

    async def test_captures_once_inside_the_after_hours_window(self):
        client = _client(GOOD_VOLUME, GOOD_FLUCT)

        result = await capture_overtime_rankings_if_due(
            market=CLOSED_TRADING_DAY, client=client, store=self.store, now=_kst(17, 50)
        )

        self.assertEqual(result['status'], 'captured')
        self.assertEqual(result['usable_counts'], {OVERTIME_VOLUME_SOURCE: 1, OVERTIME_FLUCTUATION_SOURCE: 1})
        rows, meta = self.store.usable_rows('2026-11-06', OVERTIME_VOLUME_SOURCE)
        self.assertEqual(len(rows), 2)
        self.assertEqual(meta['origin'], 'after_hours_capture')
        self.assertEqual(meta['captured_at'], '2026-11-06T17:50:00+09:00')

        again = await capture_overtime_rankings_if_due(
            market=CLOSED_TRADING_DAY, client=client, store=self.store, now=_kst(18, 10)
        )
        self.assertEqual(again['reason'], 'already_captured')
        self.assertEqual(client.fetch_domestic_ranking_overtime_volume.await_count, 1)

    async def test_never_calls_kis_outside_the_window_or_off_trading_days(self):
        client = _client(GOOD_VOLUME, GOOD_FLUCT)
        cases = [
            (CLOSED_TRADING_DAY, _kst(17, 44)),
            (CLOSED_TRADING_DAY, _kst(18, 30)),
            (CLOSED_TRADING_DAY, _kst(21, 15)),
            (SimpleNamespace(state='OPEN', is_trading_day=True), _kst(17, 50)),
            (SimpleNamespace(state='WEEKEND', is_trading_day=False), _kst(17, 50, day=7)),
            (SimpleNamespace(state='CLOSED', is_trading_day=False), _kst(17, 50)),
        ]
        for market, now in cases:
            with self.subTest(state=market.state, now=now.isoformat()):
                result = await capture_overtime_rankings_if_due(
                    market=market, client=client, store=self.store, now=now
                )
                self.assertEqual(result['status'], 'skipped')
        client.fetch_domestic_ranking_overtime_volume.assert_not_awaited()
        self.assertIsNone(self.store.load('2026-11-06'))

    async def test_empty_snapshot_is_retried_with_spacing_and_never_replaces_a_better_one(self):
        empty_client = _client(ZERO_VOLUME, [])
        first = await capture_overtime_rankings_if_due(
            market=CLOSED_TRADING_DAY, client=empty_client, store=self.store, now=_kst(17, 46)
        )
        self.assertEqual(first['status'], 'captured')
        self.assertIsNone(self.store.usable_rows('2026-11-06', OVERTIME_VOLUME_SOURCE))

        too_soon = await capture_overtime_rankings_if_due(
            market=CLOSED_TRADING_DAY, client=empty_client, store=self.store, now=_kst(17, 49)
        )
        self.assertEqual(too_soon['reason'], 'attempt_interval')

        good_client = _client(GOOD_VOLUME, GOOD_FLUCT)
        better = await capture_overtime_rankings_if_due(
            market=CLOSED_TRADING_DAY, client=good_client, store=self.store, now=_kst(17, 52)
        )
        self.assertEqual(better['status'], 'captured')
        self.assertIsNotNone(self.store.usable_rows('2026-11-06', OVERTIME_VOLUME_SOURCE))

    async def test_a_later_worse_snapshot_keeps_the_existing_capture(self):
        await capture_overtime_rankings_if_due(
            market=CLOSED_TRADING_DAY, client=_client(GOOD_VOLUME, []), store=self.store, now=_kst(17, 50)
        )
        worse = await capture_overtime_rankings_if_due(
            market=CLOSED_TRADING_DAY, client=_client(ZERO_VOLUME, []), store=self.store, now=_kst(17, 56)
        )
        self.assertEqual(worse['status'], 'kept_existing')
        self.assertIsNotNone(self.store.usable_rows('2026-11-06', OVERTIME_VOLUME_SOURCE))

    async def test_a_later_snapshot_with_only_the_other_source_keeps_both(self):
        # Review case: 20 volume rows stored, then a capture with only
        # fluctuation rows replaced the whole file and the volume was lost.
        volume = [{'stck_shrn_iscd': f'{i:06d}', 'ovtm_untp_vol': '100'} for i in range(20)]
        fluct = [{'mksc_shrn_iscd': f'{i:06d}', 'ovtm_untp_prdy_ctrt': '1.0'} for i in range(30)]
        await capture_overtime_rankings_if_due(
            market=CLOSED_TRADING_DAY, client=_client(volume, []), store=self.store, now=_kst(17, 50)
        )
        later = await capture_overtime_rankings_if_due(
            market=CLOSED_TRADING_DAY, client=_client(ZERO_VOLUME, fluct), store=self.store, now=_kst(17, 56)
        )

        self.assertEqual(later['improved_sources'], [OVERTIME_FLUCTUATION_SOURCE])
        self.assertEqual(len(self.store.usable_rows('2026-11-06', OVERTIME_VOLUME_SOURCE)[0]), 20)
        self.assertEqual(len(self.store.usable_rows('2026-11-06', OVERTIME_FLUCTUATION_SOURCE)[0]), 30)
        self.assertEqual(
            self.store.usable_rows('2026-11-06', OVERTIME_VOLUME_SOURCE)[1]['captured_at'],
            _kst(17, 50).isoformat(),
        )
        self.assertEqual(
            self.store.usable_rows('2026-11-06', OVERTIME_FLUCTUATION_SOURCE)[1]['captured_at'],
            _kst(17, 56).isoformat(),
        )

    async def test_one_failing_source_does_not_block_the_other(self):
        client = _client(GOOD_VOLUME, GOOD_FLUCT)
        client.fetch_domestic_ranking_overtime_fluctuation = AsyncMock(side_effect=RuntimeError('down'))

        result = await capture_overtime_rankings_if_due(
            market=CLOSED_TRADING_DAY, client=client, store=self.store, now=_kst(17, 50)
        )

        self.assertEqual(result['status'], 'captured')
        self.assertEqual(result['errors'], {OVERTIME_FLUCTUATION_SOURCE: 'down'})
        self.assertIsNotNone(self.store.usable_rows('2026-11-06', OVERTIME_VOLUME_SOURCE))
        self.assertIsNone(self.store.usable_rows('2026-11-06', OVERTIME_FLUCTUATION_SOURCE))

    def test_store_ignores_other_dates_and_foreign_files(self):
        self.store.root.mkdir(parents=True, exist_ok=True)
        (self.store.root / '2026-11-06.json').write_text('{"schema_version": "other"}', encoding='utf-8')
        self.assertIsNone(self.store.load('2026-11-06'))
        self.assertIsNone(self.store.usable_rows('2026-11-05', OVERTIME_VOLUME_SOURCE))


if __name__ == '__main__':
    unittest.main()
