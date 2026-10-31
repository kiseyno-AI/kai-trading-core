import asyncio
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
import threading
import unittest
from unittest.mock import ANY, AsyncMock, patch

from app.eod.auto_pykrx import RecoveryOnlyScheduleDisposition
from app.eod.runner import (
    BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS,
    EODBackgroundRunner,
    NEXT_OPEN_PLAN_REFRESH_TIMEOUT_SECONDS,
    NEXT_OPEN_PLAN_REFRESH_SINGLE_FLIGHT_KEY,
    OPEN_EXEC_DISPATCH_SINGLE_FLIGHT_KEY,
    WAIT_RECHECK_SINGLE_FLIGHT_KEY,
    is_preopen_exec_dispatch_due,
)
from app.market.clock import MarketState


class EODBackgroundRunnerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        recovery_scheduler_patcher = patch(
            'app.eod.runner.auto_pykrx_eod_scheduler.schedule_recovery_if_due',
            new=AsyncMock(
                return_value=RecoveryOnlyScheduleDisposition.NO_DEBT,
            ),
        )
        self.schedule_recovery_if_due = recovery_scheduler_patcher.start()
        self.addCleanup(recovery_scheduler_patcher.stop)

    async def _run_open_recovery_disposition_tick(
        self,
        disposition: RecoveryOnlyScheduleDisposition,
        *,
        refreshed_market_state: MarketState | None = None,
    ) -> dict[str, object]:
        runner = EODBackgroundRunner(interval_seconds=0)
        market_state = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-09-14T10:00:00+09:00',
            session_open_at='2026-09-14T09:00:00+09:00',
            session_close_at='2026-09-14T15:30:00+09:00',
            latest_valid_trading_date='2026-09-14',
        )

        async def stop_after_tick(_seconds: int) -> None:
            runner._running = False

        self.schedule_recovery_if_due.reset_mock()
        self.schedule_recovery_if_due.return_value = disposition
        with ExitStack() as stack:
            stack.enter_context(
                patch('app.eod.runner.settings.background_runner_enabled', True)
            )
            stack.enter_context(
                patch(
                    'app.eod.runner.settings.'
                    'signal_decider_wait_recheck_monitoring_enabled',
                    False,
                )
            )
            market_clock = stack.enter_context(
                patch(
                    'app.eod.runner.async_get_market_state',
                    new=AsyncMock(
                        **(
                            {
                                'side_effect': [
                                    market_state,
                                    refreshed_market_state,
                                ]
                            }
                            if refreshed_market_state is not None
                            else {'return_value': market_state}
                        )
                    ),
                )
            )
            refresh_master = stack.enter_context(
                patch('app.eod.runner.refresh_master_if_due')
            )
            refresh_snapshot = stack.enter_context(
                patch(
                    'app.eod.runner.market_snapshot_service.'
                    'refresh_market_snapshot_if_due',
                    new=AsyncMock(),
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.'
                    'should_delegate_execution',
                    return_value=False,
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.'
                    'should_manage_delegated_authority',
                    return_value=False,
                )
            )
            dispatch_plan = stack.enter_context(
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.'
                    'dispatch_next_open_plan',
                    new=AsyncMock(),
                )
            )
            run_execution = stack.enter_context(
                patch(
                    'app.eod.runner.signal_decider_next_open_execution_service.'
                    'run_market_open_execution_if_due',
                    new=AsyncMock(),
                )
            )
            run_auto_sell = stack.enter_context(
                patch(
                    'app.eod.runner.signal_decider_intraday_auto_sell_service.'
                    'run_intraday_auto_sell_if_due',
                    new=AsyncMock(),
                )
            )
            reconcile = stack.enter_context(
                patch(
                    'app.eod.runner.trading_order_reconciliation_service.'
                    'run_periodic_if_due',
                    new=AsyncMock(),
                )
            )
            refresh_portfolio = stack.enter_context(
                patch(
                    'app.eod.runner.portfolio_summary_service.'
                    'refresh_portfolio_summary_if_due',
                    new=AsyncMock(),
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.runner.asyncio.sleep',
                    new=AsyncMock(side_effect=stop_after_tick),
                )
            )

            runner._running = True
            await runner._run_loop()

        return {
            'market_clock': market_clock,
            'refresh_master': refresh_master,
            'refresh_snapshot': refresh_snapshot,
            'dispatch_plan': dispatch_plan,
            'run_execution': run_execution,
            'run_auto_sell': run_auto_sell,
            'reconcile': reconcile,
            'refresh_portfolio': refresh_portfolio,
        }

    async def test_start_registers_threadsafe_wait_recheck_wake_and_stop_clears_it(
        self,
    ) -> None:
        runner = EODBackgroundRunner(interval_seconds=60)
        parked = asyncio.Event()

        async def parked_loop() -> None:
            await parked.wait()

        with (
            patch.object(runner, '_run_loop', new=parked_loop),
            patch(
                'app.eod.runner.signal_decider_wait_recheck_service.'
                'set_wake_callback'
            ) as set_wake_callback,
            patch(
                'app.eod.runner.auto_pykrx_eod_scheduler.stop',
                new=AsyncMock(),
            ),
        ):
            await runner.start()
            callback = set_wake_callback.call_args_list[0].args[0]

            await asyncio.to_thread(callback)
            await asyncio.sleep(0)

            self.assertIsNotNone(runner._wait_recheck_wake_event)
            self.assertTrue(runner._wait_recheck_wake_event.is_set())
            self.assertEqual(runner._wait_recheck_wake_generation, 1)

            await runner.stop()

            self.assertEqual(set_wake_callback.call_count, 2)
            self.assertIsNone(set_wake_callback.call_args_list[-1].args[0])
            self.assertIsNone(runner._wait_recheck_wake_event)
            self.assertIsNone(runner._wait_recheck_wake_callback)

            # A provider thread may retain a stale callback briefly. It must
            # not resurrect or mutate the stopped runner.
            await asyncio.to_thread(callback)
            await asyncio.sleep(0)
            self.assertEqual(runner._wait_recheck_wake_generation, 1)

    async def test_observation_and_completion_wakes_break_poll_once_without_idle_spin(
        self,
    ) -> None:
        runner = EODBackgroundRunner(interval_seconds=60)
        runner._running = True
        wake_event = asyncio.Event()
        runner._wait_recheck_wake_event = wake_event

        for source in ('new_observation', 'dispatch_completion'):
            with self.subTest(source=source):
                wait_task = asyncio.create_task(
                    runner._wait_for_wake_or_timeout(60)
                )
                await asyncio.sleep(0)

                runner._signal_wait_recheck_wake(wake_event)

                self.assertTrue(
                    await asyncio.wait_for(wait_task, timeout=0.25)
                )
                self.assertEqual(
                    runner._wait_recheck_consumed_generation,
                    runner._wait_recheck_wake_generation,
                )

                idle_wait = asyncio.create_task(
                    runner._wait_for_wake_or_timeout(60)
                )
                await asyncio.sleep(0)
                await asyncio.sleep(0)
                self.assertFalse(idle_wait.done())
                idle_wait.cancel()
                await asyncio.gather(idle_wait, return_exceptions=True)

    async def test_scheduled_wait_recheck_wakes_only_for_explicit_follow_up(
        self,
    ) -> None:
        cases = (
            ({'status': 'completed', 'follow_up_required': True}, True),
            ({'status': 'idle', 'follow_up_required': False}, False),
            ({'status': 'completed', 'events_processed': 1}, False),
        )

        for result, expected_wake in cases:
            with self.subTest(result=result):
                runner = EODBackgroundRunner(interval_seconds=60)
                runner._running = True
                wake_event = asyncio.Event()
                runner._wait_recheck_wake_event = wake_event

                self.assertTrue(
                    runner._schedule_blocking_step(
                        WAIT_RECHECK_SINGLE_FLIGHT_KEY,
                        lambda cycle_result=result: cycle_result,
                        wake_on_follow_up=True,
                    )
                )
                task = runner._blocking_step_tasks[
                    WAIT_RECHECK_SINGLE_FLIGHT_KEY
                ]
                await asyncio.wait_for(task, timeout=0.25)
                await asyncio.sleep(0)

                self.assertNotIn(
                    WAIT_RECHECK_SINGLE_FLIGHT_KEY,
                    runner._blocking_step_tasks,
                )
                self.assertEqual(
                    runner._wait_recheck_wake_generation,
                    1 if expected_wake else 0,
                )
                self.assertEqual(wake_event.is_set(), expected_wake)

                if expected_wake:
                    self.assertTrue(
                        await runner._wait_for_wake_or_timeout(60)
                    )
                    idle_wait = asyncio.create_task(
                        runner._wait_for_wake_or_timeout(60)
                    )
                    await asyncio.sleep(0)
                    await asyncio.sleep(0)
                    self.assertFalse(idle_wait.done())
                    idle_wait.cancel()
                    await asyncio.gather(
                        idle_wait,
                        return_exceptions=True,
                    )

    async def test_wait_recheck_schedule_captures_epoch_before_thread_start(
        self,
    ) -> None:
        runner = EODBackgroundRunner(interval_seconds=60)
        runner._running = True
        market = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-10-30T10:00:00+09:00',
            session_open_at='2026-10-30T09:00:00+09:00',
            session_close_at='2026-10-30T15:30:00+09:00',
            calendar_source='kis_chk_holiday',
            calendar_source_status='ok',
        )
        with (
            patch(
                'app.eod.runner.async_get_market_state',
                new=AsyncMock(return_value=market),
            ),
            patch(
                'app.eod.runner.signal_decider_wait_recheck_service.'
                'candidate_cache_epoch',
                return_value=7,
            ),
            patch.object(
                runner,
                '_schedule_blocking_step',
                return_value=True,
            ) as schedule_blocking_step,
            patch(
                'app.eod.runner.signal_decider_wait_recheck_service.'
                'run_open_cycle'
            ) as run_open_cycle,
        ):
            self.assertTrue(
                await runner._schedule_wait_recheck_open_cycle()
            )
            scheduled_call = schedule_blocking_step.call_args
            scheduled_call.args[1]()

        self.assertEqual(
            scheduled_call.args[0],
            WAIT_RECHECK_SINGLE_FLIGHT_KEY,
        )
        self.assertTrue(scheduled_call.kwargs['wake_on_follow_up'])
        run_open_cycle.assert_called_once_with(
            market=market,
            expected_cache_epoch=7,
        )

    async def test_wait_recheck_schedule_rejects_revoked_or_unverified_market(
        self,
    ) -> None:
        authoritative = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-10-30T10:00:00+09:00',
            session_open_at='2026-10-30T09:00:00+09:00',
            session_close_at='2026-10-30T15:30:00+09:00',
            calendar_source='kis_chk_holiday',
            calendar_source_status='ok',
        )
        fallback = MarketState(
            **{
                **authoritative.__dict__,
                'calendar_source': 'exchange_calendars',
                'calendar_source_status': 'fallback',
            }
        )
        cases = (
            ('epoch_changed', authoritative, (7, 8), True),
            ('fallback', fallback, (7, 7), True),
            ('runner_stopped', authoritative, (7, 7), False),
        )
        for name, market, epochs, running in cases:
            with self.subTest(name=name):
                runner = EODBackgroundRunner(interval_seconds=60)
                runner._running = running
                with (
                    patch(
                        'app.eod.runner.async_get_market_state',
                        new=AsyncMock(return_value=market),
                    ),
                    patch(
                        'app.eod.runner.signal_decider_wait_recheck_service.'
                        'candidate_cache_epoch',
                        side_effect=epochs,
                    ),
                    patch.object(
                        runner,
                        '_schedule_blocking_step',
                    ) as schedule_blocking_step,
                ):
                    self.assertFalse(
                        await runner._schedule_wait_recheck_open_cycle()
                    )
                schedule_blocking_step.assert_not_called()

    async def test_wake_during_inflight_idle_cycle_is_preserved_until_completion(
        self,
    ) -> None:
        runner = EODBackgroundRunner(interval_seconds=60)
        runner._running = True
        wake_event = asyncio.Event()
        runner._wait_recheck_wake_event = wake_event
        cycle_started = threading.Event()
        release_cycle = threading.Event()

        def blocked_idle_cycle() -> dict[str, object]:
            cycle_started.set()
            release_cycle.wait(timeout=1)
            return {'status': 'idle', 'follow_up_required': False}

        self.assertTrue(
            runner._schedule_blocking_step(
                WAIT_RECHECK_SINGLE_FLIGHT_KEY,
                blocked_idle_cycle,
                wake_on_follow_up=True,
            )
        )
        first_task = runner._blocking_step_tasks[
            WAIT_RECHECK_SINGLE_FLIGHT_KEY
        ]
        self.assertTrue(await asyncio.to_thread(cycle_started.wait, 1))

        # A new observation wakes the runner, but the prior WAIT task still
        # owns the slot. Consuming the sleep edge must not consume the request.
        runner._signal_wait_recheck_wake(wake_event)
        self.assertTrue(await runner._wait_for_wake_or_timeout(60))
        self.assertTrue(runner._wait_recheck_dispatch_requested)
        self.assertFalse(
            runner._schedule_blocking_step(
                WAIT_RECHECK_SINGLE_FLIGHT_KEY,
                lambda: {'status': 'idle', 'follow_up_required': False},
                wake_on_follow_up=True,
            )
        )
        self.assertFalse(wake_event.is_set())

        # Even though the old task computed an idle result, its completion
        # re-signals the preserved observation wake after releasing the slot.
        release_cycle.set()
        await asyncio.wait_for(first_task, timeout=0.25)
        await asyncio.sleep(0)
        self.assertTrue(wake_event.is_set())
        self.assertTrue(runner._wait_recheck_dispatch_requested)
        self.assertTrue(await runner._wait_for_wake_or_timeout(60))

        # The follow-up cycle now owns the slot and consumes the request. Its
        # idle result must not manufacture another wake or cause idle spin.
        self.assertTrue(
            runner._schedule_blocking_step(
                WAIT_RECHECK_SINGLE_FLIGHT_KEY,
                lambda: {'status': 'idle', 'follow_up_required': False},
                wake_on_follow_up=True,
            )
        )
        follow_up_task = runner._blocking_step_tasks[
            WAIT_RECHECK_SINGLE_FLIGHT_KEY
        ]
        await asyncio.wait_for(follow_up_task, timeout=0.25)
        await asyncio.sleep(0)
        self.assertFalse(runner._wait_recheck_dispatch_requested)
        self.assertFalse(wake_event.is_set())

    async def test_wake_during_failed_inflight_cycle_is_preserved(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=60)
        runner._running = True
        wake_event = asyncio.Event()
        runner._wait_recheck_wake_event = wake_event
        cycle_started = threading.Event()
        release_cycle = threading.Event()

        def blocked_failed_cycle() -> dict[str, object]:
            cycle_started.set()
            release_cycle.wait(timeout=1)
            raise RuntimeError('synthetic wait-recheck failure')

        self.assertTrue(
            runner._schedule_blocking_step(
                WAIT_RECHECK_SINGLE_FLIGHT_KEY,
                blocked_failed_cycle,
                wake_on_follow_up=True,
            )
        )
        task = runner._blocking_step_tasks[
            WAIT_RECHECK_SINGLE_FLIGHT_KEY
        ]
        self.assertTrue(await asyncio.to_thread(cycle_started.wait, 1))

        runner._signal_wait_recheck_wake(wake_event)
        self.assertTrue(await runner._wait_for_wake_or_timeout(60))
        release_cycle.set()
        with self.assertRaises(RuntimeError):
            await task
        await asyncio.sleep(0)

        self.assertTrue(runner._wait_recheck_dispatch_requested)
        self.assertTrue(wake_event.is_set())

    async def test_active_daily_pipeline_wait_can_be_woken(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=60)
        runner._running = True
        wake_event = asyncio.Event()
        runner._wait_recheck_wake_event = wake_event
        market = MarketState(
            market='KRX',
            state='CLOSED',
            is_trading_day=True,
            is_open=False,
            server_time='2026-04-30T16:00:00+09:00',
            session_open_at='2026-04-30T09:00:00+09:00',
            session_close_at='2026-04-30T15:30:00+09:00',
        )
        cycle_started = [asyncio.Event(), asyncio.Event()]
        market_call_count = 0

        async def load_market() -> MarketState:
            nonlocal market_call_count
            market_call_count += 1
            cycle_started[market_call_count - 1].set()
            return market

        from app.eod.runner import auto_pykrx_eod_scheduler

        auto_pykrx_eod_scheduler.state.active_task = True
        try:
            with (
                patch(
                    'app.eod.runner.async_get_market_state',
                    side_effect=load_market,
                ),
                patch.object(
                    runner,
                    '_run_pending_session_cleanup',
                    new=AsyncMock(),
                ),
            ):
                run_task = asyncio.create_task(runner._run_loop())
                await asyncio.wait_for(
                    cycle_started[0].wait(),
                    timeout=0.25,
                )
                await asyncio.sleep(0)

                runner._signal_wait_recheck_wake(wake_event)
                await asyncio.wait_for(
                    cycle_started[1].wait(),
                    timeout=0.25,
                )
                await asyncio.sleep(0)
                self.assertTrue(runner._wait_recheck_dispatch_requested)

                runner._signal_wait_recheck_wake(wake_event)
                runner._running = False
                await asyncio.wait_for(run_task, timeout=0.25)
        finally:
            auto_pykrx_eod_scheduler.state.active_task = False

        self.assertEqual(market_call_count, 2)

    async def test_run_loop_wakes_for_observation_and_completion_without_idle_spin(
        self,
    ) -> None:
        runner = EODBackgroundRunner(interval_seconds=60)
        runner._running = True
        wake_event = asyncio.Event()
        runner._wait_recheck_wake_event = wake_event
        market = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-04-30T10:00:00+09:00',
            session_open_at='2026-04-30T09:00:00+09:00',
            session_close_at='2026-04-30T15:30:00+09:00',
        )
        cycle_started = [asyncio.Event() for _ in range(4)]
        market_call_count = 0

        async def load_market() -> MarketState:
            nonlocal market_call_count
            market_call_count += 1
            # OPEN ticks read once at admission and once again after the
            # recovery inspector certifies NO_DEBT.
            if market_call_count % 2 == 1:
                cycle_started[(market_call_count - 1) // 2].set()
            return market

        with (
            patch('app.eod.runner.settings.background_runner_enabled', False),
            patch(
                'app.eod.runner.async_get_market_state',
                side_effect=load_market,
            ),
            patch.object(
                runner,
                '_run_pending_session_cleanup',
                new=AsyncMock(),
            ),
        ):
            run_task = asyncio.create_task(runner._run_loop())
            try:
                await asyncio.wait_for(cycle_started[0].wait(), timeout=0.25)

                # The same loop-safe callback is used by the service for a new
                # observation and for dispatch completion. Each signal should
                # cut short one 60-second poll wait.
                runner._signal_wait_recheck_wake(wake_event)
                await asyncio.wait_for(cycle_started[1].wait(), timeout=0.25)

                runner._signal_wait_recheck_wake(wake_event)
                await asyncio.wait_for(cycle_started[2].wait(), timeout=0.25)

                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(
                        cycle_started[3].wait(),
                        timeout=0.02,
                    )
                self.assertEqual(market_call_count, 6)
            finally:
                runner._running = False
                wake_event.set()
                await asyncio.wait_for(run_task, timeout=0.25)

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
                patch('app.eod.runner.NEXT_OPEN_PLAN_REFRESH_TIMEOUT_SECONDS', 0.01),
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
            patch(
                'app.eod.runner.market_snapshot_service.refresh_market_snapshot_if_due',
                new=AsyncMock(),
            ),
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
            patch(
                'app.eod.runner.market_snapshot_service.refresh_market_snapshot_if_due',
                new=AsyncMock(),
            ),
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

    async def test_blocking_step_uses_next_open_refresh_timeout_override(
        self,
    ) -> None:
        runner = EODBackgroundRunner(interval_seconds=0)
        observed_timeout: float | None = None

        async def capture_wait_for(awaitable, *, timeout):
            nonlocal observed_timeout
            observed_timeout = timeout
            return await awaitable

        with patch(
            'app.eod.runner.asyncio.wait_for',
            side_effect=capture_wait_for,
        ):
            await runner._run_blocking_step(
                NEXT_OPEN_PLAN_REFRESH_SINGLE_FLIGHT_KEY,
                lambda: None,
                timeout_seconds=NEXT_OPEN_PLAN_REFRESH_TIMEOUT_SECONDS,
            )

        self.assertEqual(
            observed_timeout,
            NEXT_OPEN_PLAN_REFRESH_TIMEOUT_SECONDS,
        )

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
            patch(
                'app.eod.runner.market_snapshot_service.refresh_market_snapshot_if_due',
                new=AsyncMock(),
            ),
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

        schedule_daily.assert_awaited_once_with(
            expected_market_state='CLOSED'
        )
        reconcile.assert_not_awaited()
        refresh_snapshot.assert_not_awaited()
        refresh_portfolio.assert_not_awaited()

    async def test_closed_loop_refreshes_snapshot_before_starting_daily_pipeline_when_startup_maintenance_disabled(
        self,
    ) -> None:
        runner = EODBackgroundRunner(interval_seconds=0)
        market_state = MarketState(
            market='KRX',
            state='CLOSED',
            is_trading_day=True,
            is_open=False,
            server_time='2026-10-23T16:00:00+09:00',
            session_open_at='2026-10-23T09:00:00+09:00',
            session_close_at='2026-10-23T15:30:00+09:00',
        )
        events: list[str] = []

        async def refresh_snapshot(**_kwargs: object) -> None:
            events.append('snapshot')

        async def schedule_daily(**_kwargs: object) -> None:
            events.append('schedule')
            from app.eod.runner import auto_pykrx_eod_scheduler

            auto_pykrx_eod_scheduler.state.active_task = True

        async def stop_after_tick(_seconds: int) -> None:
            runner._running = False

        with (
            patch('app.eod.runner.settings.background_runner_enabled', True),
            patch('app.eod.runner.settings.startup_maintenance_enabled', False),
            patch('app.eod.runner.async_get_market_state', return_value=market_state),
            patch(
                'app.eod.runner.auto_pykrx_eod_scheduler.schedule_daily_pipeline_if_due',
                new=AsyncMock(side_effect=schedule_daily),
            ) as schedule_daily_pipeline,
            patch(
                'app.eod.runner.market_snapshot_service.refresh_market_snapshot_if_due',
                new=AsyncMock(side_effect=refresh_snapshot),
            ) as refresh_market_snapshot,
            patch(
                'app.eod.runner.trading_order_reconciliation_service.run_periodic_if_due',
                new=AsyncMock(),
            ) as reconcile,
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

        self.assertEqual(events, ['snapshot', 'schedule'])
        refresh_market_snapshot.assert_awaited_once_with(
            trigger='background_runner:closed:daily_pipeline_preflight',
            force_index=False,
        )
        schedule_daily_pipeline.assert_awaited_once_with(
            expected_market_state='CLOSED'
        )
        reconcile.assert_not_awaited()
        refresh_portfolio.assert_not_awaited()

    async def test_open_loop_db_inspection_unknown_blocks_buy_but_keeps_monitoring(
        self,
    ) -> None:
        calls = await self._run_open_recovery_disposition_tick(
            RecoveryOnlyScheduleDisposition.UNKNOWN,
        )

        self.schedule_recovery_if_due.assert_awaited_once_with(
            expected_market_state='OPEN'
        )
        self.assertEqual(calls['market_clock'].await_count, 2)
        calls['refresh_master'].assert_called_once_with()
        calls['refresh_snapshot'].assert_awaited_once()
        calls['dispatch_plan'].assert_not_awaited()
        calls['run_execution'].assert_not_awaited()
        calls['run_auto_sell'].assert_awaited_once_with(
            trigger='market_open_scheduler'
        )
        calls['reconcile'].assert_awaited_once_with()
        calls['refresh_portfolio'].assert_awaited_once_with(
            trigger='background_runner:open'
        )

    async def test_open_loop_recovery_debt_blocks_buy_but_keeps_monitoring(
        self,
    ) -> None:
        calls = await self._run_open_recovery_disposition_tick(
            RecoveryOnlyScheduleDisposition.DUE_BLOCKED,
        )

        self.schedule_recovery_if_due.assert_awaited_once_with(
            expected_market_state='OPEN'
        )
        calls['refresh_master'].assert_called_once_with()
        calls['refresh_snapshot'].assert_awaited_once()
        calls['dispatch_plan'].assert_not_awaited()
        calls['run_execution'].assert_not_awaited()
        calls['run_auto_sell'].assert_awaited_once_with(
            trigger='market_open_scheduler'
        )
        calls['reconcile'].assert_awaited_once_with()
        calls['refresh_portfolio'].assert_awaited_once_with(
            trigger='background_runner:open'
        )

    async def test_open_loop_recovery_debt_keeps_delegated_exec_readback(
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
            latest_valid_trading_date='2026-09-14',
        )

        async def stop_after_tick(_seconds: int) -> None:
            runner._running = False

        self.schedule_recovery_if_due.return_value = (
            RecoveryOnlyScheduleDisposition.DUE_BLOCKED
        )
        with (
            patch('app.eod.runner.settings.background_runner_enabled', True),
            patch(
                'app.eod.runner.settings.signal_decider_wait_recheck_monitoring_enabled',
                False,
            ),
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
                new=AsyncMock(),
            ) as dispatch_plan,
            patch(
                'app.eod.runner.signal_decider_exec_integration_service.'
                'schedule_signal_decider_exec_readback_refresh',
                return_value={'request': {}, 'refresh': {}},
            ) as schedule_readback,
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

        refresh_plan.assert_not_called()
        dispatch_plan.assert_not_awaited()
        schedule_readback.assert_called_once_with(
            target_trade_date='2026-09-14',
            trigger='market_open_recovery_debt_monitor',
            timeout_seconds=BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS,
        )

    async def test_open_loop_legacy_due_started_still_blocks_buy_only(
        self,
    ) -> None:
        calls = await self._run_open_recovery_disposition_tick(
            RecoveryOnlyScheduleDisposition.DUE_STARTED,
        )

        # Retain fail-closed compatibility if an older scheduler reports that
        # it started recovery before the runner can observe active_task.
        from app.eod.runner import auto_pykrx_eod_scheduler

        self.assertFalse(auto_pykrx_eod_scheduler.state.active_task)
        calls['refresh_master'].assert_called_once_with()
        calls['refresh_snapshot'].assert_awaited_once()
        calls['dispatch_plan'].assert_not_awaited()
        calls['run_execution'].assert_not_awaited()
        calls['run_auto_sell'].assert_awaited_once_with(
            trigger='market_open_scheduler'
        )
        calls['reconcile'].assert_awaited_once_with()
        calls['refresh_portfolio'].assert_awaited_once_with(
            trigger='background_runner:open'
        )

    async def test_open_loop_no_debt_runs_normal_open_work(self) -> None:
        calls = await self._run_open_recovery_disposition_tick(
            RecoveryOnlyScheduleDisposition.NO_DEBT,
        )

        self.schedule_recovery_if_due.assert_awaited_once_with(
            expected_market_state='OPEN'
        )
        self.assertEqual(calls['market_clock'].await_count, 2)
        calls['refresh_master'].assert_called_once_with()
        calls['refresh_snapshot'].assert_awaited_once()
        calls['dispatch_plan'].assert_not_awaited()
        calls['run_execution'].assert_awaited_once_with(
            trigger='market_open_scheduler'
        )
        calls['run_auto_sell'].assert_awaited_once_with(
            trigger='market_open_scheduler'
        )
        calls['reconcile'].assert_awaited_once_with()
        calls['refresh_portfolio'].assert_awaited_once_with(
            trigger='background_runner:open'
        )

    async def test_open_loop_no_debt_rechecks_market_before_trading(self) -> None:
        closed_market = MarketState(
            market='KRX',
            state='CLOSED',
            is_trading_day=True,
            is_open=False,
            server_time='2026-09-14T15:31:00+09:00',
            session_open_at='2026-09-14T09:00:00+09:00',
            session_close_at='2026-09-14T15:30:00+09:00',
            latest_valid_trading_date='2026-09-14',
        )

        calls = await self._run_open_recovery_disposition_tick(
            RecoveryOnlyScheduleDisposition.NO_DEBT,
            refreshed_market_state=closed_market,
        )

        self.assertEqual(calls['market_clock'].await_count, 2)
        calls['refresh_master'].assert_not_called()
        calls['refresh_snapshot'].assert_not_awaited()
        calls['dispatch_plan'].assert_not_awaited()
        calls['run_execution'].assert_not_awaited()
        calls['run_auto_sell'].assert_not_awaited()
        calls['reconcile'].assert_not_awaited()
        calls['refresh_portfolio'].assert_not_awaited()

    async def test_preopen_loop_keeps_monitoring_but_blocks_new_plan_when_recovery_debt_is_pending(
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

        self.schedule_recovery_if_due.return_value = (
            RecoveryOnlyScheduleDisposition.DUE_BLOCKED
        )
        with (
            patch('app.eod.runner.settings.background_runner_enabled', True),
            patch('app.eod.runner.async_get_market_state', return_value=market_state),
            patch(
                'app.eod.runner.auto_pykrx_eod_scheduler.schedule_daily_pipeline_if_due',
                new=AsyncMock(),
            ) as schedule_daily,
            patch('app.eod.runner.refresh_master_if_due') as refresh_master,
            patch(
                'app.eod.runner.signal_decider_next_open_execution_service.refresh_plan_if_due'
            ) as refresh_plan,
            patch(
                'app.eod.runner.signal_decider_exec_integration_service.should_delegate_execution',
                return_value=True,
            ),
            patch(
                'app.eod.runner.signal_decider_exec_integration_service.should_manage_delegated_authority',
                return_value=False,
            ),
            patch(
                'app.eod.runner.signal_decider_exec_integration_service.'
                'dispatch_next_open_plan',
                new=AsyncMock(),
            ) as dispatch_plan,
            patch(
                'app.eod.runner.signal_decider_exec_integration_service.'
                'schedule_signal_decider_exec_readback_refresh',
                return_value={'request': {}, 'refresh': {}},
            ) as schedule_readback,
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

        self.schedule_recovery_if_due.assert_awaited_once_with(
            expected_market_state='PREOPEN'
        )
        schedule_daily.assert_not_awaited()
        refresh_snapshot.assert_awaited_once_with(
            trigger='background_runner:preopen:daily_pipeline_preflight',
            force_index=False,
        )
        refresh_master.assert_called_once_with()
        refresh_plan.assert_not_called()
        dispatch_plan.assert_not_awaited()
        schedule_readback.assert_called_once_with(
            target_trade_date='2026-09-14',
            trigger='preopen_recovery_debt_monitor',
            timeout_seconds=BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS,
        )
        reconcile.assert_awaited_once_with()
        refresh_portfolio.assert_awaited_once_with(
            trigger='background_runner:preopen'
        )

    async def test_preopen_loop_no_debt_runs_existing_preopen_work(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=0)
        market_state = MarketState(
            market='KRX',
            state='PREOPEN',
            is_trading_day=True,
            is_open=False,
            server_time='2026-09-14T08:00:00+09:00',
            session_open_at='2026-09-14T09:00:00+09:00',
            session_close_at='2026-09-14T15:30:00+09:00',
            latest_valid_trading_date='2026-09-11',
        )

        async def stop_after_tick(_seconds: int) -> None:
            runner._running = False

        with (
            patch('app.eod.runner.settings.background_runner_enabled', True),
            patch('app.eod.runner.async_get_market_state', return_value=market_state) as market_clock,
            patch(
                'app.eod.runner.auto_pykrx_eod_scheduler.schedule_daily_pipeline_if_due',
                new=AsyncMock(),
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
            ) as refresh_snapshot,
            patch(
                'app.eod.runner.portfolio_summary_service.refresh_portfolio_summary_if_due',
                new=AsyncMock(),
            ) as refresh_portfolio,
            patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
        ):
            runner._running = True
            await runner._run_loop()

        self.schedule_recovery_if_due.assert_awaited_once_with(
            expected_market_state='PREOPEN'
        )
        schedule_daily.assert_not_awaited()
        self.assertEqual(market_clock.await_count, 2)
        refresh_snapshot.assert_awaited_once_with(
            trigger='background_runner:preopen:daily_pipeline_preflight',
            force_index=False,
        )
        refresh_master.assert_called_once_with()
        refresh_plan.assert_called_once_with(
            _live_market_snapshot_loop=ANY,
        )
        reconcile.assert_awaited_once_with()
        refresh_portfolio.assert_awaited_once_with(
            trigger='background_runner:preopen'
        )

    async def test_preopen_loop_no_debt_rechecks_market_before_work(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=0)
        preopen = MarketState(
            market='KRX',
            state='PREOPEN',
            is_trading_day=True,
            is_open=False,
            server_time='2026-09-14T08:59:59+09:00',
            session_open_at='2026-09-14T09:00:00+09:00',
            session_close_at='2026-09-14T15:30:00+09:00',
            latest_valid_trading_date='2026-09-11',
        )
        opened = MarketState(
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

        with (
            patch('app.eod.runner.settings.background_runner_enabled', True),
            patch(
                'app.eod.runner.async_get_market_state',
                new=AsyncMock(side_effect=[preopen, opened]),
            ) as market_clock,
            patch('app.eod.runner.refresh_master_if_due') as refresh_master,
            patch(
                'app.eod.runner.signal_decider_next_open_execution_service.refresh_plan_if_due'
            ) as refresh_plan,
            patch(
                'app.eod.runner.market_snapshot_service.refresh_market_snapshot_if_due',
                new=AsyncMock(),
            ) as refresh_snapshot,
            patch(
                'app.eod.runner.trading_order_reconciliation_service.run_periodic_if_due',
                new=AsyncMock(),
            ) as reconcile,
            patch(
                'app.eod.runner.portfolio_summary_service.refresh_portfolio_summary_if_due',
                new=AsyncMock(),
            ) as refresh_portfolio,
            patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
        ):
            runner._running = True
            await runner._run_loop()

        self.schedule_recovery_if_due.assert_awaited_once_with(
            expected_market_state='PREOPEN'
        )
        self.assertEqual(market_clock.await_count, 2)
        refresh_snapshot.assert_not_awaited()
        refresh_master.assert_not_called()
        refresh_plan.assert_not_called()
        reconcile.assert_not_awaited()
        refresh_portfolio.assert_not_awaited()

    async def test_open_loop_active_recovery_blocks_buy_but_keeps_monitoring(
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
                patch('app.eod.runner.settings.startup_maintenance_enabled', False),
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
                    return_value=False,
                ),
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.should_manage_delegated_authority',
                    return_value=False,
                ),
                patch(
                    'app.eod.runner.signal_decider_exec_integration_service.dispatch_next_open_plan',
                    new=AsyncMock(),
                ) as dispatch_plan,
                patch(
                    'app.eod.runner.signal_decider_next_open_execution_service.'
                    'run_market_open_execution_if_due',
                    new=AsyncMock(),
                ) as run_execution,
                patch(
                    'app.eod.runner.signal_decider_intraday_auto_sell_service.'
                    'run_intraday_auto_sell_if_due',
                    new=AsyncMock(),
                ) as run_auto_sell,
                patch(
                    'app.eod.runner.trading_order_reconciliation_service.run_periodic_if_due',
                    new=AsyncMock(),
                ) as reconcile,
                patch(
                    'app.eod.runner.portfolio_summary_service.'
                    'refresh_portfolio_summary_if_due',
                    new=AsyncMock(),
                ) as refresh_portfolio,
                patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
            ):
                runner._running = True
                await runner._run_loop()
        finally:
            auto_pykrx_eod_scheduler.state.active_task = False

        self.schedule_recovery_if_due.assert_not_awaited()
        refresh_master.assert_called_once_with()
        refresh_snapshot.assert_awaited_once()
        refresh_plan.assert_not_called()
        dispatch_plan.assert_not_awaited()
        run_execution.assert_not_awaited()
        run_auto_sell.assert_awaited_once_with(
            trigger='market_open_scheduler'
        )
        reconcile.assert_awaited_once_with()
        refresh_portfolio.assert_awaited_once_with(
            trigger='background_runner:open'
        )

    async def test_preopen_recovery_inspection_timeout_blocks_plan_but_keeps_monitoring(
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
            self.schedule_recovery_if_due.side_effect = stalled_inspection
            with (
                patch('app.eod.runner.settings.background_runner_enabled', True),
                patch('app.eod.runner.BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS', 0.01),
                patch('app.eod.runner.async_get_market_state', return_value=market_state),
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
                ) as refresh_snapshot,
                patch(
                    'app.eod.runner.portfolio_summary_service.refresh_portfolio_summary_if_due',
                    new=AsyncMock(),
                ) as refresh_portfolio,
                patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
            ):
                runner._running = True
                await runner._run_loop()

            refresh_master.assert_called_once_with()
            refresh_plan.assert_not_called()
            refresh_snapshot.assert_awaited_once_with(
                trigger='background_runner:preopen:daily_pipeline_preflight',
                force_index=False,
            )
            reconcile.assert_awaited_once_with()
            refresh_portfolio.assert_awaited_once_with(
                trigger='background_runner:preopen'
            )
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

        schedule_daily.assert_awaited_once_with(
            expected_market_state='WEEKEND'
        )

    async def test_closed_loop_preflights_snapshot_but_skips_other_background_work_when_daily_pipeline_starts(
        self,
    ) -> None:
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

        async def mark_pipeline_active(**_kwargs: object) -> None:
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

        schedule_daily.assert_awaited_once_with(
            expected_market_state='CLOSED'
        )
        reconcile.assert_not_awaited()
        refresh_snapshot.assert_awaited_once_with(
            trigger='background_runner:closed:daily_pipeline_preflight',
            force_index=False,
        )
        refresh_portfolio.assert_not_awaited()

    async def test_session_cleanup_continues_while_daily_pipeline_is_active(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=0)
        market_state = MarketState(
            market='KRX',
            state='CLOSED',
            is_trading_day=True,
            is_open=False,
            server_time='2026-10-19T16:00:00+09:00',
            session_open_at='2026-10-19T09:00:00+09:00',
            session_close_at='2026-10-19T15:30:00+09:00',
        )

        async def stop_after_tick(_seconds: int) -> None:
            runner._running = False

        async def run_inline(func, *args, **kwargs):
            return func(*args, **kwargs)

        from app.eod.runner import auto_pykrx_eod_scheduler

        auto_pykrx_eod_scheduler.state.active_task = True
        try:
            with (
                patch('app.eod.runner.async_get_market_state', return_value=market_state),
                patch(
                    'app.eod.runner.settings.openclaw_session_cleanup_enabled',
                    True,
                ),
                patch(
                    'app.eod.runner.settings.openclaw_gateway_token',
                    'configured',
                ),
                patch(
                    'app.eod.runner.settings.openclaw_session_cleanup_batch_limit',
                    17,
                ),
                patch(
                    'app.eod.runner.settings.openclaw_session_cleanup_run_budget_seconds',
                    23,
                ),
                patch('app.eod.runner.asyncio.to_thread', side_effect=run_inline),
                patch(
                    'app.eod.runner.agent_feedback_session_cleanup_service.run_pending_cleanup',
                    return_value={
                        'ok': True,
                        'summary': {'selected': 0, 'deleted': 0, 'failed': 0},
                        'items': [],
                    },
                ) as run_cleanup,
                patch('app.eod.runner.asyncio.sleep', side_effect=stop_after_tick),
            ):
                runner._running = True
                await runner._run_loop()
        finally:
            auto_pykrx_eod_scheduler.state.active_task = False

        run_cleanup.assert_called_once_with(
            limit=17,
            max_runtime_seconds=23,
        )
