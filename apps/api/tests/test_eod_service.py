from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from app.eod.service import EODService
from app.market.clock import MarketState


class EODServiceLockTests(IsolatedAsyncioTestCase):
    async def test_capture_if_due_waits_for_nxt_settlement(self) -> None:
        service = EODService()
        market = MarketState(
            market='KRX',
            state='CLOSED',
            is_trading_day=True,
            is_open=False,
            server_time='2026-09-23T20:09:00+09:00',
            session_open_at='2026-09-23T09:00:00+09:00',
            session_close_at='2026-09-23T15:30:00+09:00',
            latest_valid_trading_date='2026-09-23',
        )

        with (
            patch('app.eod.service.async_get_market_state', return_value=market),
            patch.object(
                service,
                'capture_market_date',
                new=AsyncMock(),
            ) as capture_market_date,
        ):
            result = await service.capture_if_due()

        self.assertFalse(result['triggered'])
        self.assertEqual(
            result['reason'],
            'waiting_until:2026-09-23T20:10:00+09:00',
        )
        capture_market_date.assert_not_awaited()

    async def test_capture_if_due_runs_after_nxt_settlement(self) -> None:
        service = EODService()
        market = MarketState(
            market='KRX',
            state='CLOSED',
            is_trading_day=True,
            is_open=False,
            server_time='2026-09-23T20:11:00+09:00',
            session_open_at='2026-09-23T09:00:00+09:00',
            session_close_at='2026-09-23T15:30:00+09:00',
            latest_valid_trading_date='2026-09-23',
        )

        with (
            patch('app.eod.service.async_get_market_state', return_value=market),
            patch.object(
                service,
                'capture_market_date',
                new=AsyncMock(return_value={'triggered': True}),
            ) as capture_market_date,
        ):
            result = await service.capture_if_due(trigger='manual_test')

        self.assertTrue(result['triggered'])
        capture_market_date.assert_awaited_once_with(
            '2026-09-23',
            trigger='manual_test',
            now=datetime.fromisoformat('2026-09-23T20:11:00+09:00'),
        )

    async def test_direct_capture_blocks_same_day_before_nxt_settlement(self) -> None:
        service = EODService()

        with self.assertRaisesRegex(ValueError, 'blocked until'):
            await service.capture_market_date(
                '2026-09-23',
                now=datetime.fromisoformat('2026-09-23T20:09:00+09:00'),
            )

    async def test_direct_capture_rejects_future_date(self) -> None:
        service = EODService()

        with self.assertRaisesRegex(ValueError, 'future date'):
            await service.capture_market_date(
                '2026-09-24',
                now=datetime.fromisoformat('2026-09-23T21:00:00+09:00'),
            )

    async def test_capture_market_date_holds_shared_price_bar_writer_lock(
        self,
    ) -> None:
        service = EODService()

        @contextmanager
        def available_lock(**kwargs):
            self.assertEqual(
                kwargs,
                {
                    'market': 'KRX',
                    'trade_date': '2026-09-23',
                    'source': 'daily_price_bars',
                },
            )
            yield True

        with (
            patch(
                'app.eod.service.advisory_eod_collection_lock',
                available_lock,
            ),
            patch.object(
                service,
                '_capture_market_date_locked',
                new=AsyncMock(return_value={'status': 'completed'}),
            ) as capture_locked,
        ):
            result = await service.capture_market_date(
                '2026-09-23',
                trigger='manual_test',
            )

        self.assertEqual(result, {'status': 'completed'})
        capture_locked.assert_awaited_once_with(
            '2026-09-23',
            trigger='manual_test',
        )

    async def test_capture_market_date_fails_when_price_bar_writer_lock_is_busy(
        self,
    ) -> None:
        service = EODService()

        @contextmanager
        def unavailable_lock(**_kwargs):
            yield False

        with (
            patch(
                'app.eod.service.advisory_eod_collection_lock',
                unavailable_lock,
            ),
            patch.object(
                service,
                '_capture_market_date_locked',
                new=AsyncMock(),
            ) as capture_locked,
        ):
            with self.assertRaisesRegex(RuntimeError, 'lock is already held'):
                await service.capture_market_date('2026-09-23')

        capture_locked.assert_not_awaited()
