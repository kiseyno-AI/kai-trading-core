import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from app.eod.runner import (
    BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS,
    EODBackgroundRunner,
    is_preopen_exec_dispatch_due,
)
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

    async def test_preopen_sync_maintenance_uses_thread_offload(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=0)
        market_state = MarketState(
            market='KRX',
            state='PREOPEN',
            is_trading_day=True,
            is_open=False,
            server_time='2026-04-30T07:45:00+09:00',
            session_open_at='2026-04-30T09:00:00+09:00',
            session_close_at='2026-04-30T15:30:00+09:00',
        )

        async def stop_after_tick(_seconds: int) -> None:
            runner._running = False

        async def run_inline(func, *args, **kwargs):
            return func(*args, **kwargs)

        with (
            patch('app.eod.runner.asyncio.to_thread', side_effect=run_inline) as to_thread,
            patch('app.eod.runner.get_market_state', return_value=market_state),
            patch('app.eod.runner.refresh_master_if_due'),
            patch('app.eod.runner.signal_decider_next_open_execution_service.refresh_plan_if_due'),
            patch(
                'app.eod.runner.trading_order_reconciliation_service.run_periodic_if_due',
                new=AsyncMock(),
            ),
            patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
        ):
            runner._running = True
            await runner._run_loop()

        self.assertGreaterEqual(to_thread.await_count, 2)

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
                'app.eod.runner.signal_decider_intraday_auto_sell_service.run_intraday_auto_sell_if_due',
                new=AsyncMock(),
            ) as run_intraday_auto_sell,
            patch(
                'app.eod.runner.trading_order_reconciliation_service.run_periodic_if_due',
                new=AsyncMock(),
            ) as reconcile,
            patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
        ):
            runner._running = True
            await runner._run_loop()

        run_execution.assert_awaited_once_with(trigger='market_open_scheduler')
        run_intraday_auto_sell.assert_awaited_once_with(trigger='market_open_scheduler')
        reconcile.assert_awaited_once_with()

    async def test_preopen_loop_dispatches_exec_plan_when_delegation_enabled(self) -> None:
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
            patch('app.eod.runner.refresh_master_if_due'),
            patch('app.eod.runner.signal_decider_next_open_execution_service.refresh_plan_if_due'),
            patch('app.eod.runner.signal_decider_exec_integration_service.should_delegate_execution', return_value=True),
            patch(
                'app.eod.runner.signal_decider_exec_integration_service.dispatch_next_open_plan',
                new=AsyncMock(return_value={'summary': {'delegated_to_exec': True}}),
            ) as dispatch_exec,
            patch(
                'app.eod.runner.trading_order_reconciliation_service.run_periodic_if_due',
                new=AsyncMock(),
            ),
            patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
        ):
            runner._running = True
            await runner._run_loop()

        dispatch_exec.assert_awaited_once_with(
            target_trade_date='2026-04-30',
            refresh_plan=False,
            trigger='preopen_exec_dispatch',
        )

    async def test_preopen_loop_waits_until_0800_for_exec_dispatch(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=0)
        market_state = MarketState(
            market='KRX',
            state='PREOPEN',
            is_trading_day=True,
            is_open=False,
            server_time='2026-04-30T07:59:30+09:00',
            session_open_at='2026-04-30T09:00:00+09:00',
            session_close_at='2026-04-30T15:30:00+09:00',
        )

        async def stop_after_tick(_seconds: int) -> None:
            runner._running = False

        with (
            patch('app.eod.runner.get_market_state', return_value=market_state),
            patch('app.eod.runner.refresh_master_if_due'),
            patch('app.eod.runner.signal_decider_next_open_execution_service.refresh_plan_if_due'),
            patch('app.eod.runner.signal_decider_exec_integration_service.should_delegate_execution', return_value=True),
            patch(
                'app.eod.runner.signal_decider_exec_integration_service.dispatch_next_open_plan',
                new=AsyncMock(return_value={'summary': {'delegated_to_exec': True}}),
            ) as dispatch_exec,
            patch(
                'app.eod.runner.trading_order_reconciliation_service.run_periodic_if_due',
                new=AsyncMock(),
            ),
            patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
        ):
            runner._running = True
            await runner._run_loop()

        dispatch_exec.assert_not_awaited()

    def test_preopen_exec_dispatch_due_boundary_is_0800_kst(self) -> None:
        before = MarketState(
            market='KRX',
            state='PREOPEN',
            is_trading_day=True,
            is_open=False,
            server_time='2026-04-30T07:59:59+09:00',
            session_open_at='2026-04-30T09:00:00+09:00',
            session_close_at='2026-04-30T15:30:00+09:00',
        )
        at_boundary = MarketState(
            market='KRX',
            state='PREOPEN',
            is_trading_day=True,
            is_open=False,
            server_time='2026-04-30T08:00:00+09:00',
            session_open_at='2026-04-30T09:00:00+09:00',
            session_close_at='2026-04-30T15:30:00+09:00',
        )

        self.assertFalse(is_preopen_exec_dispatch_due(before))
        self.assertTrue(is_preopen_exec_dispatch_due(at_boundary))

    async def test_open_loop_refreshes_exec_readback_when_delegation_enabled(self) -> None:
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
            patch('app.eod.runner.signal_decider_exec_integration_service.should_delegate_execution', return_value=True),
            patch(
                'app.eod.runner.signal_decider_exec_integration_service.schedule_signal_decider_exec_readback_refresh',
                return_value={
                    'request': {'target_trade_date': '2026-04-30'},
                    'refresh': {'status': 'scheduled', 'in_flight': True},
                },
            ) as schedule_refresh,
            patch(
                'app.eod.runner.trading_order_reconciliation_service.run_periodic_if_due',
                new=AsyncMock(),
            ),
            patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
        ):
            runner._running = True
            await runner._run_loop()

        schedule_refresh.assert_called_once_with(
            target_trade_date='2026-04-30',
            trigger='market_open_scheduler',
            timeout_seconds=BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS,
        )

    async def test_open_loop_schedules_slow_exec_readback_without_blocking_cycle(self) -> None:
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

        class SlowExecReadbackService:
            def __init__(self) -> None:
                self.calls: list[dict] = []
                self.refresh_started = asyncio.Event()
                self.release_refresh = asyncio.Event()
                self.refresh_state = {
                    'status': 'not_requested',
                    'in_flight': False,
                }
                self.task: asyncio.Task | None = None

            def should_delegate_execution(self) -> bool:
                return True

            def schedule_signal_decider_exec_readback_refresh(self, **kwargs) -> dict:
                self.calls.append(kwargs)
                self.refresh_state = {
                    'status': 'scheduled',
                    'in_flight': True,
                    'last_trigger': kwargs.get('trigger'),
                }
                self.task = asyncio.create_task(self._slow_refresh())
                return {
                    'request': {
                        'target_trade_date': kwargs.get('target_trade_date'),
                        'trigger': kwargs.get('trigger'),
                    },
                    'refresh': self.refresh_state,
                }

            async def _slow_refresh(self) -> None:
                self.refresh_started.set()
                await self.release_refresh.wait()
                self.refresh_state.update(
                    {
                        'status': 'succeeded',
                        'in_flight': False,
                    }
                )

        fake_service = SlowExecReadbackService()

        async def stop_after_tick(_seconds: int) -> None:
            runner._running = False

        try:
            with (
                patch('app.eod.runner.get_market_state', return_value=market_state),
                patch('app.eod.runner.signal_decider_exec_integration_service', fake_service),
                patch(
                    'app.eod.runner.trading_order_reconciliation_service.run_periodic_if_due',
                    new=AsyncMock(),
                ) as reconcile,
                patch(
                    'app.eod.runner.market_snapshot_service.refresh_market_snapshot_if_due',
                    new=AsyncMock(),
                ),
                patch(
                    'app.eod.runner.portfolio_summary_service.refresh_portfolio_summary_if_due',
                    new=AsyncMock(),
                ),
                patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
            ):
                runner._running = True
                await asyncio.wait_for(runner._run_loop(), timeout=0.25)

            await asyncio.wait_for(fake_service.refresh_started.wait(), timeout=0.25)
            self.assertEqual(
                fake_service.calls,
                [
                    {
                        'target_trade_date': '2026-04-30',
                        'trigger': 'market_open_scheduler',
                        'timeout_seconds': BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS,
                    }
                ],
            )
            self.assertEqual(fake_service.refresh_state['status'], 'scheduled')
            self.assertTrue(fake_service.refresh_state['in_flight'])
            self.assertIsNotNone(fake_service.task)
            self.assertFalse(fake_service.task.done())
            reconcile.assert_awaited_once_with()
        finally:
            fake_service.release_refresh.set()
            if fake_service.task is not None:
                await asyncio.gather(fake_service.task, return_exceptions=True)
