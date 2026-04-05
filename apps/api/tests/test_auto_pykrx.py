from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, patch

from app.eod.auto_pykrx import AutoPykrxEODScheduler
from app.eod.pykrx_backfill import PykrxBackfillService
from app.market.clock import KST, MarketState


@contextmanager
def _lock(acquired: bool = True):
    yield acquired


class PykrxBackfillServiceTests(TestCase):
    def test_run_daily_collection_skips_completed_trade_date(self) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 7,
                    'status': 'completed',
                    'symbols_failed': 0,
                },
            ),
            patch.object(service, '_run_collection') as run_collection,
        ):
            result = service.run_daily_collection_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['reason'], 'already_completed')
        run_collection.assert_not_called()

    def test_run_daily_collection_force_bypasses_stale_running_row(self) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 8,
                    'status': 'running',
                    'symbols_failed': 0,
                },
            ),
            patch.object(
                service,
                '_run_collection',
                return_value={
                    'run_id': 9,
                    'source': 'pykrx',
                    'status': 'completed',
                },
            ) as run_collection,
        ):
            result = service.run_daily_collection_if_due(
                market_date='2026-04-03',
                trigger='manual_daily_api',
                force=True,
            )

        self.assertTrue(result['triggered'])
        self.assertEqual(result['run_id'], 9)
        run_collection.assert_called_once()


class AutoPykrxEODSchedulerTests(IsolatedAsyncioTestCase):
    async def test_maybe_schedule_waits_for_close_buffer(self) -> None:
        scheduler = AutoPykrxEODScheduler()
        now = datetime.now(KST)
        close_at = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        market = MarketState(
            market='KRX',
            state='CLOSED',
            is_trading_day=True,
            is_open=False,
            server_time=now.isoformat(),
            session_open_at=(close_at - timedelta(hours=6, minutes=30)).isoformat(),
            session_close_at=close_at.isoformat(),
        )

        with patch('app.eod.auto_pykrx.get_market_state', return_value=market):
            await scheduler.maybe_schedule()

        snapshot = scheduler.snapshot()
        self.assertFalse(snapshot['active_task'])
        self.assertEqual(snapshot['last_status'], 'idle')
        self.assertIn('waiting_until:', snapshot['last_reason'])

    async def test_maybe_schedule_starts_background_task_after_close_buffer(self) -> None:
        scheduler = AutoPykrxEODScheduler()
        now = datetime.now(KST)
        close_at = (now - timedelta(minutes=30)).replace(second=0, microsecond=0)
        market = MarketState(
            market='KRX',
            state='CLOSED',
            is_trading_day=True,
            is_open=False,
            server_time=now.isoformat(),
            session_open_at=(close_at - timedelta(hours=6, minutes=30)).isoformat(),
            session_close_at=close_at.isoformat(),
        )

        with patch('app.eod.auto_pykrx.get_market_state', return_value=market), patch.object(
            scheduler,
            '_run_for_market_date',
            new=AsyncMock(return_value=None),
        ):
            await scheduler.maybe_schedule()
            await scheduler.stop()

        snapshot = scheduler.snapshot()
        self.assertEqual(snapshot['last_status'], 'running')
