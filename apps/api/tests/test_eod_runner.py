import asyncio
from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import AsyncMock, patch

from app.eod.runner import (
    BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS,
    EODBackgroundRunner,
    NEXT_OPEN_PLAN_REFRESH_SINGLE_FLIGHT_KEY,
    OPEN_EXEC_DISPATCH_SINGLE_FLIGHT_KEY,
    is_preopen_exec_dispatch_due,
)
from app.market.clock import MarketState


class EODBackgroundRunnerTests(unittest.IsolatedAsyncioTestCase):
    def test_snapshot_exposes_fresh_runner_heartbeat(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=60)
        now = datetime(2026, 10, 7, 5, 0, tzinfo=timezone.utc)
        runner._running = True
        runner._started_at = now - timedelta(minutes=10)
        runner._last_tick_started_at = now - timedelta(seconds=35)
        runner._last_tick_completed_at = now - timedelta(seconds=30)
        runner._last_tick_succeeded_at = now - timedelta(seconds=30)
        runner._tick_count = 12

        snapshot = runner.snapshot(now=now)

        self.assertEqual(snapshot['status'], 'running')
        self.assertEqual(snapshot['heartbeat_at'], '2026-10-07T04:59:30+00:00')
        self.assertEqual(snapshot['last_tick']['count'], 12)
        self.assertEqual(
            snapshot['last_tick']['started_at'],
            '2026-10-07T04:59:25+00:00',
        )
        self.assertEqual(
            snapshot['last_tick']['completed_at'],
            '2026-10-07T04:59:30+00:00',
        )
        self.assertEqual(snapshot['staleness']['age_seconds'], 30)
        self.assertEqual(snapshot['staleness']['stale_after_seconds'], 180)
        self.assertFalse(snapshot['staleness']['is_stale'])

    def test_snapshot_marks_running_runner_stale_after_three_intervals(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=60)
        now = datetime(2026, 10, 7, 5, 0, tzinfo=timezone.utc)
        runner._running = True
        runner._started_at = now - timedelta(minutes=10)
        runner._last_tick_completed_at = now - timedelta(seconds=181)
        runner._last_tick_succeeded_at = now - timedelta(seconds=181)
        runner._tick_count = 4

        snapshot = runner.snapshot(now=now)

        self.assertEqual(snapshot['status'], 'stale')
        self.assertTrue(snapshot['staleness']['is_stale'])
        self.assertEqual(snapshot['staleness']['age_seconds'], 181)

    async def test_transient_critical_failure_does_not_revoke_delegated_buys(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=60)
        market = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-04-30T11:05:00+09:00',
            session_open_at='2026-04-30T09:00:00+09:00',
            session_close_at='2026-04-30T15:30:00+09:00',
        )

        async def fail_once() -> None:
            raise RuntimeError('temporary provider failure')

        with patch(
            'app.eod.runner.signal_decider_exec_integration_service.'
            'revoke_delegated_buys_for_unavailable_market_evidence',
            new=AsyncMock(),
        ) as revoke_delegated_buys:
            with self.assertRaisesRegex(RuntimeError, 'temporary provider failure'):
                await runner._run_buy_authority_critical_step(
                    name='market_snapshot_refresh_if_due',
                    market=market,
                    awaitable=fail_once(),
                )

        revoke_delegated_buys.assert_not_awaited()
        self.assertEqual(
            runner._buy_authority_critical_failures['market_snapshot_refresh_if_due'],
            1,
        )

    async def test_third_consecutive_critical_failure_revokes_delegated_buys(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=60)
        market = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-04-30T11:05:00+09:00',
            session_open_at='2026-04-30T09:00:00+09:00',
            session_close_at='2026-04-30T15:30:00+09:00',
        )

        async def fail() -> None:
            raise RuntimeError('persistent provider failure')

        with patch(
            'app.eod.runner.signal_decider_exec_integration_service.'
            'revoke_delegated_buys_for_unavailable_market_evidence',
            new=AsyncMock(),
        ) as revoke_delegated_buys:
            for _ in range(3):
                with self.assertRaisesRegex(RuntimeError, 'persistent provider failure'):
                    await runner._run_buy_authority_critical_step(
                        name='market_snapshot_refresh_if_due',
                        market=market,
                        awaitable=fail(),
                    )

        revoke_delegated_buys.assert_awaited_once_with(
            target_trade_date='2026-04-30',
            trigger='background_runner:market_snapshot_refresh_if_due_failed',
        )

    async def test_successful_critical_step_resets_failure_streak(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=60)
        market = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-04-30T11:05:00+09:00',
            session_open_at='2026-04-30T09:00:00+09:00',
            session_close_at='2026-04-30T15:30:00+09:00',
        )

        async def fail() -> None:
            raise RuntimeError('temporary provider failure')

        async def succeed() -> None:
            return None

        with patch(
            'app.eod.runner.signal_decider_exec_integration_service.'
            'revoke_delegated_buys_for_unavailable_market_evidence',
            new=AsyncMock(),
        ) as revoke_delegated_buys:
            with self.assertRaises(RuntimeError):
                await runner._run_buy_authority_critical_step(
                    name='market_snapshot_refresh_if_due',
                    market=market,
                    awaitable=fail(),
                )
            await runner._run_buy_authority_critical_step(
                name='market_snapshot_refresh_if_due',
                market=market,
                awaitable=succeed(),
            )

        revoke_delegated_buys.assert_not_awaited()
        self.assertNotIn(
            'market_snapshot_refresh_if_due',
            runner._buy_authority_critical_failures,
        )

    def test_open_drive_poll_interval_cannot_undersample_refresh_buckets(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=900)
        open_market = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-04-30T10:00:00+09:00',
            session_open_at='2026-04-30T09:00:00+09:00',
            session_close_at='2026-04-30T15:30:00+09:00',
        )
        after_window = MarketState(
            **{
                **open_market.__dict__,
                'server_time': '2026-04-30T14:31:00+09:00',
            }
        )

        with patch(
            'app.eod.runner.settings.market_snapshot_open_drive_refresh_interval_seconds',
            300,
        ):
            self.assertEqual(runner._next_poll_interval_seconds(open_market), 300)
            self.assertEqual(runner._next_poll_interval_seconds(after_window), 900)

    def test_open_drive_snapshot_force_due_obeys_window_and_five_minute_cadence(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=60)

        def market_at(server_time: str) -> MarketState:
            return MarketState(
                market='KRX',
                state='OPEN',
                is_trading_day=True,
                is_open=True,
                server_time=server_time,
                session_open_at='2026-04-30T09:00:00+09:00',
                session_close_at='2026-04-30T15:30:00+09:00',
            )

        with patch(
            'app.eod.runner.settings.market_snapshot_open_drive_refresh_interval_seconds',
            300,
        ):
            self.assertTrue(
                runner._claim_market_snapshot_open_drive_refresh(
                    market_at('2026-04-30T09:00:00+09:00')
                )
            )
            self.assertFalse(
                runner._claim_market_snapshot_open_drive_refresh(
                    market_at('2026-04-30T09:04:59+09:00')
                )
            )
            self.assertTrue(
                runner._claim_market_snapshot_open_drive_refresh(
                    market_at('2026-04-30T09:05:00+09:00')
                )
            )
            self.assertTrue(
                runner._claim_market_snapshot_open_drive_refresh(
                    market_at('2026-04-30T14:30:59+09:00')
                )
            )
            self.assertFalse(
                runner._claim_market_snapshot_open_drive_refresh(
                    market_at('2026-04-30T14:31:00+09:00')
                )
            )

            offset_runner = EODBackgroundRunner(interval_seconds=60)
            self.assertTrue(
                offset_runner._claim_market_snapshot_open_drive_refresh(
                    market_at('2026-04-30T09:01:00+09:00')
                )
            )
            self.assertTrue(
                offset_runner._claim_market_snapshot_open_drive_refresh(
                    market_at('2026-04-30T09:05:00+09:00')
                )
            )

    async def test_market_snapshot_refresh_is_forced_only_during_open_drive_window(self) -> None:
        cases = (
            ('2026-04-30T09:10:00+09:00', True),
            ('2026-04-30T09:21:00+09:00', True),
            ('2026-04-30T14:30:00+09:00', True),
            ('2026-04-30T14:31:00+09:00', False),
        )

        for server_time, expected_force in cases:
            with self.subTest(server_time=server_time):
                runner = EODBackgroundRunner(interval_seconds=0)
                market_state = MarketState(
                    market='KRX',
                    state='OPEN',
                    is_trading_day=True,
                    is_open=True,
                    server_time=server_time,
                    session_open_at='2026-04-30T09:00:00+09:00',
                    session_close_at='2026-04-30T15:30:00+09:00',
                )

                async def stop_after_tick(_seconds: int) -> None:
                    runner._running = False

                with (
                    patch('app.eod.runner.settings.background_runner_enabled', True),
                    patch('app.eod.runner.async_get_market_state', return_value=market_state),
                    patch(
                        'app.eod.runner.signal_decider_exec_integration_service.should_delegate_execution',
                        return_value=False,
                    ),
                    patch(
                        'app.eod.runner.signal_decider_exec_integration_service.should_manage_delegated_authority',
                        return_value=False,
                    ),
                    patch(
                        'app.eod.runner.signal_decider_next_open_execution_service.run_market_open_execution_if_due',
                        new=AsyncMock(),
                    ),
                    patch(
                        'app.eod.runner.signal_decider_intraday_auto_sell_service.run_intraday_auto_sell_if_due',
                        new=AsyncMock(),
                    ),
                    patch(
                        'app.eod.runner.trading_order_reconciliation_service.run_periodic_if_due',
                        new=AsyncMock(),
                    ),
                    patch(
                        'app.eod.runner.market_snapshot_service.refresh_market_snapshot_if_due',
                        new=AsyncMock(),
                    ) as refresh_snapshot,
                    patch(
                        'app.eod.runner.portfolio_summary_service.refresh_portfolio_summary_if_due',
                        new=AsyncMock(),
                    ),
                    patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
                ):
                    runner._running = True
                    await runner._run_loop()

                refresh_snapshot.assert_awaited_once_with(
                    trigger='background_runner:open',
                    force_index=expected_force,
                )

    async def test_open_master_refresh_precedes_snapshot_plan_and_dispatch(self) -> None:
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
        events: list[str] = []
        plan_loops: list[asyncio.AbstractEventLoop] = []

        async def stop_after_tick(_seconds: int) -> None:
            runner._running = False

        async def run_inline(func, *args, **kwargs):
            return func(*args, **kwargs)

        async def refresh_snapshot(**_kwargs):
            events.append('snapshot')

        def refresh_master():
            events.append('master')

        def refresh_plan(**kwargs):
            events.append('plan')
            plan_loops.append(kwargs['_live_market_snapshot_loop'])

        async def dispatch_plan(**_kwargs):
            events.append('dispatch')

        with (
            patch('app.eod.runner.settings.background_runner_enabled', True),
            patch('app.eod.runner.async_get_market_state', return_value=market_state),
            patch('app.eod.runner.asyncio.to_thread', side_effect=run_inline),
            patch(
                'app.eod.runner.market_snapshot_service.refresh_market_snapshot_if_due',
                side_effect=refresh_snapshot,
            ),
            patch(
                'app.eod.runner.refresh_master_if_due',
                side_effect=refresh_master,
            ),
            patch(
                'app.eod.runner.signal_decider_next_open_execution_service.refresh_plan_if_due',
                side_effect=refresh_plan,
            ),
            patch(
                'app.eod.runner.signal_decider_exec_integration_service.should_delegate_execution',
                return_value=True,
            ),
            patch(
                'app.eod.runner.signal_decider_exec_integration_service.dispatch_next_open_plan',
                side_effect=dispatch_plan,
            ),
            patch(
                'app.eod.runner.signal_decider_exec_integration_service.schedule_signal_decider_exec_readback_refresh',
                return_value={'request': {}, 'refresh': {}},
            ),
            patch(
                'app.eod.runner.trading_order_reconciliation_service.run_periodic_if_due',
                new=AsyncMock(),
            ),
            patch(
                'app.eod.runner.portfolio_summary_service.refresh_portfolio_summary_if_due',
                new=AsyncMock(),
            ),
            patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
        ):
            runner._running = True
            await runner._run_loop()

        self.assertEqual(events[:4], ['master', 'snapshot', 'plan', 'dispatch'])
        self.assertEqual(plan_loops, [asyncio.get_running_loop()])

    async def test_open_master_refresh_timeout_fails_closed_before_snapshot_and_orders(self) -> None:
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
        release_master = asyncio.Event()

        async def stalled_to_thread(_func) -> None:
            await release_master.wait()

        async def stop_after_tick(_seconds: int) -> None:
            runner._running = False

        try:
            with (
                patch('app.eod.runner.settings.background_runner_enabled', True),
                patch('app.eod.runner.BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS', 0.01),
                patch('app.eod.runner.async_get_market_state', return_value=market_state),
                patch('app.eod.runner.asyncio.to_thread', side_effect=stalled_to_thread),
                patch(
                    'app.eod.runner.market_snapshot_service.refresh_market_snapshot_if_due',
                    new=AsyncMock(),
                ) as refresh_snapshot,
                patch(
                    'app.eod.runner.signal_decider_next_open_execution_service.run_market_open_execution_if_due',
                    new=AsyncMock(),
                ) as run_execution,
                patch(
                    'app.eod.runner.signal_decider_intraday_auto_sell_service.run_intraday_auto_sell_if_due',
                    new=AsyncMock(),
                ) as run_intraday_auto_sell,
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.should_delegate_execution',
                    return_value=False,
                ),
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.should_manage_delegated_authority',
                    return_value=False,
                ),
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.'
                    'revoke_delegated_buys_for_unavailable_market_evidence',
                    new=AsyncMock(),
                ) as revoke_delegated_buys,
                patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
            ):
                runner._running = True
                await runner._run_loop()

            refresh_snapshot.assert_not_awaited()
            run_execution.assert_not_awaited()
            run_intraday_auto_sell.assert_not_awaited()
            revoke_delegated_buys.assert_not_awaited()
        finally:
            release_master.set()
            if runner._blocking_step_tasks:
                await asyncio.gather(
                    *runner._blocking_step_tasks.values(),
                    return_exceptions=True,
                )

    async def test_open_snapshot_timeout_fails_closed_before_plan_and_dispatch(self) -> None:
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
        release_snapshot = asyncio.Event()

        async def stalled_snapshot(**_kwargs) -> None:
            await release_snapshot.wait()

        async def stop_after_tick(_seconds: int) -> None:
            runner._running = False

        try:
            with (
                patch('app.eod.runner.settings.background_runner_enabled', True),
                patch('app.eod.runner.BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS', 0.01),
                patch('app.eod.runner.async_get_market_state', return_value=market_state),
                patch('app.eod.runner.refresh_master_if_due'),
                patch(
                    'app.eod.runner.market_snapshot_service.refresh_market_snapshot_if_due',
                    side_effect=stalled_snapshot,
                ),
                patch(
                    'app.eod.runner.signal_decider_next_open_execution_service.refresh_plan_if_due'
                ) as refresh_plan,
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.should_delegate_execution',
                    return_value=True,
                ),
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.dispatch_next_open_plan',
                    new=AsyncMock(),
                ) as dispatch_plan,
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.'
                    'revoke_delegated_buys_for_unavailable_market_evidence',
                    new=AsyncMock(),
                ) as revoke_delegated_buys,
                patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
            ):
                runner._running = True
                await runner._run_loop()

            refresh_plan.assert_not_called()
            dispatch_plan.assert_not_awaited()
            revoke_delegated_buys.assert_not_awaited()
        finally:
            release_snapshot.set()

    async def test_preopen_plan_timeout_fails_closed_before_dispatch(self) -> None:
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
        release_plan = asyncio.Event()
        offload_count = 0

        async def stalled_second_offload(func) -> None:
            nonlocal offload_count
            offload_count += 1
            if offload_count == 2:
                await release_plan.wait()
                return
            func()

        async def stop_after_tick(_seconds: int) -> None:
            runner._running = False

        try:
            with (
                patch('app.eod.runner.settings.background_runner_enabled', True),
                patch('app.eod.runner.BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS', 0.01),
                patch('app.eod.runner.async_get_market_state', return_value=market_state),
                patch('app.eod.runner.asyncio.to_thread', side_effect=stalled_second_offload),
                patch('app.eod.runner.refresh_master_if_due'),
                patch('app.eod.runner.signal_decider_next_open_execution_service.refresh_plan_if_due'),
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.should_delegate_execution',
                    return_value=True,
                ),
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.dispatch_next_open_plan',
                    new=AsyncMock(),
                ) as dispatch_plan,
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.'
                    'revoke_delegated_buys_for_unavailable_market_evidence',
                    new=AsyncMock(),
                ) as revoke_delegated_buys,
                patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
            ):
                runner._running = True
                await runner._run_loop()

            dispatch_plan.assert_not_awaited()
            revoke_delegated_buys.assert_not_awaited()
        finally:
            release_plan.set()
            if runner._blocking_step_tasks:
                await asyncio.gather(
                    *runner._blocking_step_tasks.values(),
                    return_exceptions=True,
                )

    async def test_open_plan_refresh_reuses_preopen_single_flight(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=0)
        open_market = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-04-30T09:00:10+09:00',
            session_open_at='2026-04-30T09:00:00+09:00',
            session_close_at='2026-04-30T15:30:00+09:00',
        )
        release_plan = asyncio.Event()
        preopen_plan_task = asyncio.create_task(release_plan.wait())
        runner._blocking_step_tasks[NEXT_OPEN_PLAN_REFRESH_SINGLE_FLIGHT_KEY] = (
            preopen_plan_task
        )

        async def stop_after_tick(_seconds: int) -> None:
            runner._running = False

        try:
            with (
                patch('app.eod.runner.settings.background_runner_enabled', True),
                patch('app.eod.runner.async_get_market_state', return_value=open_market),
                patch('app.eod.runner.refresh_master_if_due'),
                patch(
                    'app.eod.runner.market_snapshot_service.refresh_market_snapshot_if_due',
                    new=AsyncMock(),
                ),
                patch(
                    'app.eod.runner.signal_decider_next_open_execution_service.refresh_plan_if_due'
                ) as refresh_plan,
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.should_delegate_execution',
                    return_value=True,
                ),
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.dispatch_next_open_plan',
                    new=AsyncMock(),
                ) as dispatch_plan,
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.'
                    'revoke_delegated_buys_for_unavailable_market_evidence',
                    new=AsyncMock(),
                ) as revoke_delegated_buys,
                patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
            ):
                runner._running = True
                await runner._run_loop()

            refresh_plan.assert_not_called()
            dispatch_plan.assert_not_awaited()
            revoke_delegated_buys.assert_not_awaited()
        finally:
            release_plan.set()
            await preopen_plan_task
            runner._blocking_step_tasks.pop(
                NEXT_OPEN_PLAN_REFRESH_SINGLE_FLIGHT_KEY,
                None,
            )

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
            patch('app.eod.runner.async_get_market_state', return_value=market_state),
            patch('app.eod.runner.refresh_master_if_due') as refresh_master,
            patch('app.eod.runner.signal_decider_next_open_execution_service.refresh_plan_if_due') as refresh_plan,
            patch(
                'app.eod.runner.signal_decider_exec_integration_service.should_delegate_execution',
                return_value=False,
            ),
            patch(
                'app.eod.runner.signal_decider_exec_integration_service.should_manage_delegated_authority',
                return_value=False,
            ),
            patch(
                'app.eod.runner.trading_order_reconciliation_service.run_periodic_if_due',
                new=AsyncMock(),
            ) as reconcile,
            patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
        ):
            runner._running = True
            await runner._run_loop()

        refresh_master.assert_called_once_with()
        refresh_plan.assert_called_once_with(
            _live_market_snapshot_loop=asyncio.get_running_loop(),
        )
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
            patch('app.eod.runner.async_get_market_state', return_value=market_state),
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

    async def test_blocking_step_timeout_keeps_single_flight_until_worker_finishes(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=0)
        calls = 0
        release = asyncio.Event()

        async def slow_worker() -> None:
            nonlocal calls
            calls += 1
            await release.wait()

        async def fake_to_thread(_func) -> None:
            await slow_worker()

        with (
            patch('app.eod.runner.asyncio.to_thread', side_effect=fake_to_thread),
            patch('app.eod.runner.BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS', 0.01),
        ):
            await runner._run_blocking_step('slow_refresh', lambda: None)
            await runner._run_blocking_step('slow_refresh', lambda: None)
            self.assertEqual(calls, 1)
            self.assertIn('slow_refresh', runner._blocking_step_tasks)
            release.set()
            await asyncio.wait_for(runner._blocking_step_tasks['slow_refresh'], timeout=0.25)

        self.assertNotIn('slow_refresh', runner._blocking_step_tasks)

    async def test_async_step_timeout_recovers_when_shielded_refresh_succeeds(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=0)
        calls = 0
        release_refresh = asyncio.Event()
        refresh_task: asyncio.Task | None = None

        async def provider_refresh() -> dict[str, str]:
            await release_refresh.wait()
            return {'status': 'fresh'}

        async def shielded_refresh() -> dict[str, str]:
            nonlocal calls, refresh_task
            calls += 1
            if calls == 1:
                refresh_task = asyncio.create_task(provider_refresh())
                return await asyncio.shield(refresh_task)
            return {'status': 'fresh'}

        with patch('app.eod.runner.BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS', 0.01):
            with self.assertRaisesRegex(RuntimeError, 'timed out'):
                await runner._run_async_step(
                    'market_snapshot_refresh_if_due',
                    shielded_refresh(),
                    fail_closed=True,
                )

            with self.assertRaises(RuntimeError):
                await runner._run_async_step(
                    'market_snapshot_refresh_if_due',
                    shielded_refresh(),
                    fail_closed=True,
                )
            self.assertEqual(calls, 1)

            self.assertIsNotNone(refresh_task)
            release_refresh.set()
            await asyncio.wait_for(refresh_task, timeout=0.25)
            await asyncio.sleep(0)
            await asyncio.sleep(0)

            await runner._run_async_step(
                'market_snapshot_refresh_if_due',
                shielded_refresh(),
                fail_closed=True,
            )

        self.assertEqual(calls, 2)
        self.assertNotIn('market_snapshot_refresh_if_due', runner._step_timeout_cooldowns)

    async def test_stop_preserves_timed_out_worker_single_flight_until_thread_finishes(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=0)
        calls = 0
        release = asyncio.Event()

        async def fake_to_thread(_func) -> None:
            nonlocal calls
            calls += 1
            await release.wait()

        try:
            with (
                patch('app.eod.runner.asyncio.to_thread', side_effect=fake_to_thread),
                patch('app.eod.runner.BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS', 0.01),
                patch(
                    'app.eod.runner.auto_pykrx_eod_scheduler.stop',
                    new=AsyncMock(),
                ),
            ):
                await runner._run_blocking_step('restart_safe_refresh', lambda: None)
                await runner.stop()

                self.assertIn('restart_safe_refresh', runner._blocking_step_tasks)
                with self.assertRaisesRegex(RuntimeError, 'still in flight'):
                    await runner._run_blocking_step(
                        'restart_safe_refresh',
                        lambda: None,
                        fail_closed=True,
                    )
                self.assertEqual(calls, 1)
        finally:
            release.set()
            if runner._blocking_step_tasks:
                await asyncio.gather(
                    *runner._blocking_step_tasks.values(),
                    return_exceptions=True,
                )

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
            patch('app.eod.runner.async_get_market_state', return_value=market_state),
            patch(
                'app.eod.runner.signal_decider_exec_integration_service.should_delegate_execution',
                return_value=False,
            ),
            patch(
                'app.eod.runner.signal_decider_exec_integration_service.should_manage_delegated_authority',
                return_value=False,
            ),
            patch(
                'app.eod.runner.signal_decider_next_open_execution_service.run_market_open_execution_if_due',
                new=AsyncMock(),
            ) as run_execution,
            patch(
                'app.eod.runner.signal_decider_intraday_auto_sell_service.run_intraday_auto_sell_if_due',
                new=AsyncMock(),
            ) as run_intraday_auto_sell,
            patch(
                'app.eod.runner.market_snapshot_service.refresh_market_snapshot_if_due',
                new=AsyncMock(),
            ),
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

    async def test_open_loop_reconciles_managed_exec_authority_without_local_buy(self) -> None:
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
            patch('app.eod.runner.async_get_market_state', return_value=market_state),
            patch('app.eod.runner.refresh_master_if_due'),
            patch(
                'app.eod.runner.signal_decider_exec_integration_service.should_delegate_execution',
                return_value=False,
            ),
            patch(
                'app.eod.runner.signal_decider_exec_integration_service.should_manage_delegated_authority',
                return_value=True,
            ),
            patch(
                'app.eod.runner.signal_decider_exec_integration_service.dispatch_next_open_plan',
                new=AsyncMock(return_value={'summary': {'delegated_to_exec': True}}),
            ) as dispatch_exec,
            patch(
                'app.eod.runner.signal_decider_next_open_execution_service.run_market_open_execution_if_due',
                new=AsyncMock(),
            ) as run_local_execution,
            patch(
                'app.eod.runner.market_snapshot_service.refresh_market_snapshot_if_due',
                new=AsyncMock(),
            ),
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
            trigger='market_open_exec_control_reconciliation',
        )
        run_local_execution.assert_not_awaited()

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
            patch('app.eod.runner.async_get_market_state', return_value=market_state),
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
            patch('app.eod.runner.async_get_market_state', return_value=market_state),
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
            patch('app.eod.runner.async_get_market_state', return_value=market_state),
            patch('app.eod.runner.signal_decider_exec_integration_service.should_delegate_execution', return_value=True),
            patch(
                'app.eod.runner.signal_decider_next_open_execution_service.refresh_plan_if_due'
            ) as refresh_plan,
            patch(
                'app.eod.runner.signal_decider_exec_integration_service.dispatch_next_open_plan',
                new=AsyncMock(return_value={'summary': {'delegated_to_exec': True}}),
            ) as dispatch_exec,
            patch(
                'app.eod.runner.signal_decider_exec_integration_service.schedule_signal_decider_exec_readback_refresh',
                return_value={
                    'request': {'target_trade_date': '2026-04-30'},
                    'refresh': {'status': 'scheduled', 'in_flight': True},
                },
            ) as schedule_refresh,
            patch(
                'app.eod.runner.market_snapshot_service.refresh_market_snapshot_if_due',
                new=AsyncMock(),
            ),
            patch(
                'app.eod.runner.trading_order_reconciliation_service.run_periodic_if_due',
                new=AsyncMock(),
            ),
            patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
        ):
            runner._running = True
            await runner._run_loop()

        refresh_plan.assert_called_once_with(
            target_trade_date='2026-04-30',
            _live_market_snapshot_loop=asyncio.get_running_loop(),
        )
        dispatch_exec.assert_awaited_once_with(
            target_trade_date='2026-04-30',
            refresh_plan=False,
            trigger='market_open_exec_dispatch',
        )
        schedule_refresh.assert_called_once_with(
            target_trade_date='2026-04-30',
            trigger='market_open_scheduler',
            timeout_seconds=BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS,
        )

    async def test_open_loop_defers_readback_until_shielded_dispatch_completes(
        self,
    ) -> None:
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
        release_dispatch = asyncio.Event()
        dispatch_started = asyncio.Event()

        async def dispatch_plan(**_kwargs: object) -> dict[str, dict[str, bool]]:
            dispatch_started.set()
            await release_dispatch.wait()
            return {'summary': {'delegated_to_exec': True}}

        async def stop_after_tick(_seconds: int) -> None:
            runner._running = False

        try:
            with (
                patch('app.eod.runner.settings.background_runner_enabled', True),
                patch('app.eod.runner.BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS', 0.01),
                patch('app.eod.runner.async_get_market_state', return_value=market_state),
                patch('app.eod.runner.refresh_master_if_due'),
                patch(
                    'app.eod.runner.market_snapshot_service.refresh_market_snapshot_if_due',
                    new=AsyncMock(),
                ),
                patch(
                    'app.eod.runner.signal_decider_next_open_execution_service.refresh_plan_if_due'
                ) as refresh_plan,
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.should_delegate_execution',
                    return_value=True,
                ),
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.dispatch_next_open_plan',
                    new=AsyncMock(side_effect=dispatch_plan),
                ) as dispatch_exec,
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.'
                    'schedule_signal_decider_exec_readback_refresh',
                    return_value={'request': {}, 'refresh': {}},
                ) as schedule_refresh,
                patch(
                    'app.eod.runner.trading_order_reconciliation_service.run_periodic_if_due',
                    new=AsyncMock(),
                ),
                patch(
                    'app.eod.runner.portfolio_summary_service.refresh_portfolio_summary_if_due',
                    new=AsyncMock(),
                ),
            ):
                with patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick):
                    runner._running = True
                    await runner._run_loop()

                await asyncio.wait_for(dispatch_started.wait(), timeout=0.25)
                in_flight_dispatch = runner._async_step_tasks[
                    OPEN_EXEC_DISPATCH_SINGLE_FLIGHT_KEY
                ]
                refresh_plan.assert_called_once_with(
                    target_trade_date='2026-04-30',
                    _live_market_snapshot_loop=asyncio.get_running_loop(),
                )
                self.assertEqual(dispatch_exec.await_count, 1)
                schedule_refresh.assert_not_called()
                self.assertFalse(in_flight_dispatch.done())
                self.assertFalse(in_flight_dispatch.cancelled())

                with patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick):
                    runner._running = True
                    await runner._run_loop()

                refresh_plan.assert_called_once_with(
                    target_trade_date='2026-04-30',
                    _live_market_snapshot_loop=asyncio.get_running_loop(),
                )
                self.assertEqual(dispatch_exec.await_count, 1)
                schedule_refresh.assert_not_called()
                self.assertFalse(in_flight_dispatch.done())
                self.assertFalse(in_flight_dispatch.cancelled())

                release_dispatch.set()
                await asyncio.wait_for(in_flight_dispatch, timeout=0.25)
                await asyncio.sleep(0)

                with patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick):
                    runner._running = True
                    await runner._run_loop()

                self.assertEqual(refresh_plan.call_count, 2)
                self.assertEqual(dispatch_exec.await_count, 2)
                schedule_refresh.assert_called_once_with(
                    target_trade_date='2026-04-30',
                    trigger='market_open_scheduler',
                    timeout_seconds=0.01,
                )
        finally:
            release_dispatch.set()
            if runner._async_step_tasks:
                await asyncio.gather(
                    *runner._async_step_tasks.values(),
                    return_exceptions=True,
                )

    async def test_open_loop_skips_readback_when_dispatch_retry_is_in_cooldown(
        self,
    ) -> None:
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
        release_dispatch = asyncio.Event()

        async def failed_dispatch(**_kwargs: object) -> None:
            await release_dispatch.wait()
            raise RuntimeError('dispatch failed after timeout')

        async def stop_after_tick(_seconds: int) -> None:
            runner._running = False

        try:
            with (
                patch('app.eod.runner.settings.background_runner_enabled', True),
                patch('app.eod.runner.BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS', 0.01),
                patch('app.eod.runner.async_get_market_state', return_value=market_state),
                patch('app.eod.runner.refresh_master_if_due'),
                patch(
                    'app.eod.runner.market_snapshot_service.refresh_market_snapshot_if_due',
                    new=AsyncMock(),
                ),
                patch(
                    'app.eod.runner.signal_decider_next_open_execution_service.refresh_plan_if_due'
                ) as refresh_plan,
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.should_delegate_execution',
                    return_value=True,
                ),
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.dispatch_next_open_plan',
                    new=AsyncMock(side_effect=failed_dispatch),
                ) as dispatch_exec,
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.'
                    'schedule_signal_decider_exec_readback_refresh',
                    return_value={'request': {}, 'refresh': {}},
                ) as schedule_refresh,
                patch(
                    'app.eod.runner.trading_order_reconciliation_service.run_periodic_if_due',
                    new=AsyncMock(),
                ),
                patch(
                    'app.eod.runner.portfolio_summary_service.refresh_portfolio_summary_if_due',
                    new=AsyncMock(),
                ),
            ):
                await runner._run_async_step(
                    OPEN_EXEC_DISPATCH_SINGLE_FLIGHT_KEY,
                    dispatch_exec(
                        target_trade_date='2026-04-30',
                        refresh_plan=False,
                        trigger='market_open_exec_dispatch',
                    ),
                )
                in_flight_dispatch = runner._async_step_tasks[
                    OPEN_EXEC_DISPATCH_SINGLE_FLIGHT_KEY
                ]
                self.assertFalse(in_flight_dispatch.cancelled())

                release_dispatch.set()
                with self.assertRaisesRegex(RuntimeError, 'dispatch failed after timeout'):
                    await in_flight_dispatch
                await asyncio.sleep(0)
                await asyncio.sleep(0)

                self.assertNotIn(
                    OPEN_EXEC_DISPATCH_SINGLE_FLIGHT_KEY,
                    runner._async_step_tasks,
                )
                self.assertIn(
                    OPEN_EXEC_DISPATCH_SINGLE_FLIGHT_KEY,
                    runner._step_timeout_cooldowns,
                )

                with patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick):
                    runner._running = True
                    await runner._run_loop()

                refresh_plan.assert_called_once_with(
                    target_trade_date='2026-04-30',
                    _live_market_snapshot_loop=asyncio.get_running_loop(),
                )
                self.assertEqual(dispatch_exec.await_count, 1)
                schedule_refresh.assert_not_called()
        finally:
            release_dispatch.set()
            if runner._async_step_tasks:
                await asyncio.gather(
                    *runner._async_step_tasks.values(),
                    return_exceptions=True,
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
                self.dispatch_calls: list[dict] = []
                self.refresh_started = asyncio.Event()
                self.release_refresh = asyncio.Event()
                self.refresh_state = {
                    'status': 'not_requested',
                    'in_flight': False,
                }
                self.task: asyncio.Task | None = None

            def should_delegate_execution(self) -> bool:
                return True

            async def dispatch_next_open_plan(self, **kwargs) -> dict:
                self.dispatch_calls.append(kwargs)
                return {'summary': {'delegated_to_exec': True}}

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
                patch('app.eod.runner.async_get_market_state', return_value=market_state),
                patch('app.eod.runner.signal_decider_exec_integration_service', fake_service),
                patch('app.eod.runner.signal_decider_next_open_execution_service.refresh_plan_if_due'),
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
            self.assertEqual(
                fake_service.dispatch_calls,
                [
                    {
                        'target_trade_date': '2026-04-30',
                        'refresh_plan': False,
                        'trigger': 'market_open_exec_dispatch',
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

    async def test_closed_loop_schedules_eod_when_runtime_background_disabled(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=0)
        market_state = MarketState(
            market='KRX',
            state='CLOSED',
            is_trading_day=True,
            is_open=False,
            server_time='2026-04-30T15:45:00+09:00',
            session_open_at='2026-04-30T09:00:00+09:00',
            session_close_at='2026-04-30T15:30:00+09:00',
        )

        async def stop_after_tick(_seconds: int) -> None:
            runner._running = False

        with (
            patch('app.eod.runner.settings.background_runner_enabled', False),
            patch('app.eod.runner.async_get_market_state', return_value=market_state),
            patch(
                'app.eod.runner.auto_pykrx_eod_scheduler.schedule_daily_pipeline_if_due',
                new=AsyncMock(),
            ) as schedule_daily,
            patch(
                'app.eod.runner.trading_order_reconciliation_service.run_periodic_if_due',
                new=AsyncMock(),
            ) as reconcile,
            patch(
                'app.eod.runner.market_snapshot_service.refresh_market_snapshot_if_due',
                new=AsyncMock(),
            ) as refresh_snapshot,
            patch(
                'app.eod.runner.portfolio_summary_service.refresh_portfolio_summary_if_due',
                new=AsyncMock(),
            ) as refresh_portfolio,
            patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
        ):
            runner._running = True
            await runner._run_loop()

        schedule_daily.assert_awaited_once_with()
        reconcile.assert_not_awaited()
        refresh_snapshot.assert_not_awaited()
        refresh_portfolio.assert_not_awaited()

    async def test_open_loop_defers_heavy_prior_eod_recovery_until_market_close(
        self,
    ) -> None:
        runner = EODBackgroundRunner(interval_seconds=0)
        market_state = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-09-14T10:00:00+09:00',
            session_open_at='2026-09-14T09:00:00+09:00',
            session_close_at='2026-09-14T15:30:00+09:00',
            latest_valid_trading_date='2026-09-11',
        )

        async def stop_after_tick(_seconds: int) -> None:
            runner._running = False

        with (
            patch('app.eod.runner.settings.background_runner_enabled', False),
            patch('app.eod.runner.async_get_market_state', return_value=market_state),
            patch(
                'app.eod.runner.auto_pykrx_eod_scheduler.schedule_daily_pipeline_if_due',
                new=AsyncMock(),
            ) as schedule_daily,
            patch(
                'app.eod.runner.trading_order_reconciliation_service.run_periodic_if_due',
                new=AsyncMock(),
            ) as reconcile,
            patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
        ):
            runner._running = True
            await runner._run_loop()

        schedule_daily.assert_not_awaited()
        reconcile.assert_not_awaited()

    async def test_preopen_loop_prioritizes_outstanding_eod_recovery_before_plan_refresh(
        self,
    ) -> None:
        runner = EODBackgroundRunner(interval_seconds=0)
        market_state = MarketState(
            market='KRX',
            state='PREOPEN',
            is_trading_day=True,
            is_open=False,
            server_time='2026-09-14T04:00:00+09:00',
            session_open_at='2026-09-14T09:00:00+09:00',
            session_close_at='2026-09-14T15:30:00+09:00',
            latest_valid_trading_date='2026-09-11',
        )

        async def stop_after_tick(_seconds: int) -> None:
            runner._running = False

        async def mark_pipeline_active(**_kwargs: object) -> None:
            from app.eod.runner import auto_pykrx_eod_scheduler

            auto_pykrx_eod_scheduler.state.active_task = True

        with (
            patch('app.eod.runner.settings.background_runner_enabled', True),
            patch('app.eod.runner.async_get_market_state', return_value=market_state),
            patch(
                'app.eod.runner.auto_pykrx_eod_scheduler.schedule_daily_pipeline_if_due',
                new=AsyncMock(side_effect=mark_pipeline_active),
            ) as schedule_daily,
            patch('app.eod.runner.refresh_master_if_due') as refresh_master,
            patch(
                'app.eod.runner.signal_decider_next_open_execution_service.refresh_plan_if_due'
            ) as refresh_plan,
            patch(
                'app.eod.runner.signal_decider_exec_integration_service.should_delegate_execution',
                return_value=False,
            ),
            patch(
                'app.eod.runner.signal_decider_exec_integration_service.should_manage_delegated_authority',
                return_value=False,
            ),
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
            try:
                runner._running = True
                await runner._run_loop()
            finally:
                from app.eod.runner import auto_pykrx_eod_scheduler

                auto_pykrx_eod_scheduler.state.active_task = False

        schedule_daily.assert_awaited_once_with(expected_market_state='PREOPEN')
        refresh_master.assert_not_called()
        refresh_plan.assert_not_called()
        reconcile.assert_not_awaited()

    async def test_open_loop_stays_fail_closed_while_preopen_recovery_is_active(
        self,
    ) -> None:
        runner = EODBackgroundRunner(interval_seconds=0)
        market_state = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-09-14T09:00:01+09:00',
            session_open_at='2026-09-14T09:00:00+09:00',
            session_close_at='2026-09-14T15:30:00+09:00',
            latest_valid_trading_date='2026-09-14',
        )

        async def stop_after_tick(_seconds: int) -> None:
            runner._running = False

        from app.eod.runner import auto_pykrx_eod_scheduler

        auto_pykrx_eod_scheduler.state.active_task = True
        try:
            with (
                patch('app.eod.runner.settings.background_runner_enabled', True),
                patch('app.eod.runner.async_get_market_state', return_value=market_state),
                patch('app.eod.runner.refresh_master_if_due') as refresh_master,
                patch(
                    'app.eod.runner.market_snapshot_service.refresh_market_snapshot_if_due',
                    new=AsyncMock(),
                ) as refresh_snapshot,
                patch(
                    'app.eod.runner.signal_decider_next_open_execution_service.refresh_plan_if_due'
                ) as refresh_plan,
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.should_delegate_execution',
                    return_value=True,
                ),
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.dispatch_next_open_plan',
                    new=AsyncMock(),
                ) as dispatch_plan,
                patch(
                    'app.eod.runner.trading_order_reconciliation_service.run_periodic_if_due',
                    new=AsyncMock(),
                ) as reconcile,
                patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
            ):
                runner._running = True
                await runner._run_loop()
        finally:
            auto_pykrx_eod_scheduler.state.active_task = False

        refresh_master.assert_not_called()
        refresh_snapshot.assert_not_awaited()
        refresh_plan.assert_not_called()
        dispatch_plan.assert_not_awaited()
        reconcile.assert_not_awaited()

    async def test_preopen_recovery_inspection_timeout_blocks_plan_refresh(
        self,
    ) -> None:
        runner = EODBackgroundRunner(interval_seconds=0)
        market_state = MarketState(
            market='KRX',
            state='PREOPEN',
            is_trading_day=True,
            is_open=False,
            server_time='2026-09-14T04:00:00+09:00',
            session_open_at='2026-09-14T09:00:00+09:00',
            session_close_at='2026-09-14T15:30:00+09:00',
            latest_valid_trading_date='2026-09-11',
        )
        release_inspection = asyncio.Event()

        async def stalled_inspection(**_kwargs: object) -> None:
            await release_inspection.wait()

        async def stop_after_tick(_seconds: int) -> None:
            runner._running = False

        try:
            with (
                patch('app.eod.runner.settings.background_runner_enabled', True),
                patch('app.eod.runner.BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS', 0.01),
                patch('app.eod.runner.async_get_market_state', return_value=market_state),
                patch(
                    'app.eod.runner.auto_pykrx_eod_scheduler.schedule_daily_pipeline_if_due',
                    new=AsyncMock(side_effect=stalled_inspection),
                ),
                patch('app.eod.runner.refresh_master_if_due') as refresh_master,
                patch(
                    'app.eod.runner.signal_decider_next_open_execution_service.refresh_plan_if_due'
                ) as refresh_plan,
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.should_delegate_execution',
                    return_value=False,
                ),
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.should_manage_delegated_authority',
                    return_value=False,
                ),
                patch(
                    'app.eod.runner.trading_order_reconciliation_service.run_periodic_if_due',
                    new=AsyncMock(),
                ),
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
                await runner._run_loop()

            refresh_master.assert_not_called()
            refresh_plan.assert_not_called()
        finally:
            release_inspection.set()
            if runner._async_step_tasks:
                await asyncio.gather(
                    *runner._async_step_tasks.values(),
                    return_exceptions=True,
                )

    async def test_weekend_loop_schedules_latest_session_recovery(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=0)
        market_state = MarketState(
            market='KRX',
            state='WEEKEND',
            is_trading_day=False,
            is_open=False,
            server_time='2026-09-13T14:00:00+09:00',
            session_open_at=None,
            session_close_at=None,
            latest_valid_trading_date='2026-09-11',
        )

        async def stop_after_tick(_seconds: int) -> None:
            runner._running = False

        with (
            patch('app.eod.runner.settings.background_runner_enabled', False),
            patch(
                'app.eod.runner.async_get_market_state',
                return_value=market_state,
            ),
            patch(
                'app.eod.runner.auto_pykrx_eod_scheduler.schedule_daily_pipeline_if_due',
                new=AsyncMock(),
            ) as schedule_daily,
            patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
        ):
            runner._running = True
            await runner._run_loop()

        schedule_daily.assert_awaited_once_with()

    async def test_closed_loop_skips_noncritical_background_work_when_daily_pipeline_active(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=0)
        market_state = MarketState(
            market='KRX',
            state='CLOSED',
            is_trading_day=True,
            is_open=False,
            server_time='2026-04-30T16:00:00+09:00',
            session_open_at='2026-04-30T09:00:00+09:00',
            session_close_at='2026-04-30T15:30:00+09:00',
        )

        async def stop_after_tick(_seconds: int) -> None:
            runner._running = False

        async def mark_pipeline_active() -> None:
            from app.eod.runner import auto_pykrx_eod_scheduler

            auto_pykrx_eod_scheduler.state.active_task = True

        with (
            patch('app.eod.runner.async_get_market_state', return_value=market_state),
            patch(
                'app.eod.runner.auto_pykrx_eod_scheduler.schedule_daily_pipeline_if_due',
                new=AsyncMock(side_effect=mark_pipeline_active),
            ) as schedule_daily,
            patch(
                'app.eod.runner.trading_order_reconciliation_service.run_periodic_if_due',
                new=AsyncMock(),
            ) as reconcile,
            patch(
                'app.eod.runner.market_snapshot_service.refresh_market_snapshot_if_due',
                new=AsyncMock(),
            ) as refresh_snapshot,
            patch(
                'app.eod.runner.portfolio_summary_service.refresh_portfolio_summary_if_due',
                new=AsyncMock(),
            ) as refresh_portfolio,
            patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
        ):
            try:
                runner._running = True
                await runner._run_loop()
            finally:
                from app.eod.runner import auto_pykrx_eod_scheduler

                auto_pykrx_eod_scheduler.state.active_task = False

        schedule_daily.assert_awaited_once_with()
        reconcile.assert_not_awaited()
        refresh_snapshot.assert_not_awaited()
        refresh_portfolio.assert_not_awaited()
