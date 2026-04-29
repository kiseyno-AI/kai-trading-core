import unittest
from unittest.mock import AsyncMock, patch

from app.eod.runner import EODBackgroundRunner
from app.market.clock import MarketState


class EODBackgroundRunnerTests(unittest.IsolatedAsyncioTestCase):
    async def test_preopen_loop_refreshes_master_and_next_open_plan(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=0)
        market_state = MarketState(
            market='KRX',
            state='PREOPEN',
            is_trading_day=True,
            is_open=False,
            server_time='2026-04-30T08:45:00+09:00',
            session_open_at='2026-04-30T09:00:00+09:00',
            session_close_at='2026-04-30T15:30:00+09:00',
        )

        async def stop_after_tick(_seconds: int) -> None:
            runner._running = False

        with (
            patch('app.eod.runner.get_market_state', return_value=market_state),
            patch('app.eod.runner.refresh_master_if_due') as refresh_master,
            patch('app.eod.runner.signal_decider_next_open_execution_service.refresh_plan_if_due') as refresh_plan,
            patch(
                'app.eod.runner.trading_order_reconciliation_service.run_periodic_if_due',
                new=AsyncMock(),
            ) as reconcile,
            patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
        ):
            runner._running = True
            await runner._run_loop()

        refresh_master.assert_called_once_with()
        refresh_plan.assert_called_once_with()
        reconcile.assert_awaited_once_with()

    async def test_open_loop_runs_next_open_execution_submission(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=0)
        market_state = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-04-30T09:05:00+09:00',
            session_open_at='2026-04-30T09:00:00+09:00',
            session_close_at='2026-04-30T15:30:00+09:00',
        )

        async def stop_after_tick(_seconds: int) -> None:
            runner._running = False

        with (
            patch('app.eod.runner.get_market_state', return_value=market_state),
            patch(
                'app.eod.runner.signal_decider_next_open_execution_service.run_market_open_execution_if_due',
                new=AsyncMock(),
            ) as run_execution,
            patch(
                'app.eod.runner.trading_order_reconciliation_service.run_periodic_if_due',
                new=AsyncMock(),
            ) as reconcile,
            patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
        ):
            runner._running = True
            await runner._run_loop()

        run_execution.assert_awaited_once_with(trigger='market_open_scheduler')
        reconcile.assert_awaited_once_with()
