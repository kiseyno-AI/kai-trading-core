import asyncio
from datetime import date, datetime
from time import perf_counter
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

from app.api.routes_market import market_status
from app.brokers.kis.client import KISClient
from app.market.clock import async_get_market_state, get_market_state
from app.market.session import (
    CALENDAR_SOURCE_EXCHANGE_CALENDARS,
    CALENDAR_SOURCE_KIS_CHK_HOLIDAY,
    RECOMMENDATION_BASIS_LATEST_COMPLETED_EOD,
    RECOMMENDATION_BASIS_LIVE_INTRADAY,
    build_market_session_metadata,
    is_krx_trading_day,
    KisKrxTradingCalendarProvider,
    KrxCalendarUnavailableError,
    latest_krx_trading_date,
    next_krx_trading_date,
    previous_krx_trading_date,
    reset_krx_calendar_provider,
    set_krx_calendar_provider,
    market_session_status_for_date,
)

KST = ZoneInfo('Asia/Seoul')


class FakeCalendarClient:
    def __init__(
        self,
        payload: dict | None = None,
        error: Exception | None = None,
        delay_seconds: float = 0,
    ) -> None:
        self.payload = payload or {'output': []}
        self.error = error
        self.delay_seconds = delay_seconds
        self.calls: list[dict] = []

    async def fetch_krx_holiday_calendar(self, **kwargs):
        self.calls.append(kwargs)
        if self.delay_seconds:
            await asyncio.sleep(self.delay_seconds)
        if self.error is not None:
            raise self.error
        return self.payload


class FakePagedKISClient(KISClient):
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.pages = [
            (
                {
                    'output': [{'bass_dt': '20260603', 'opnd_yn': 'N'}],
                    'ctx_area_fk': 'FK-PAGE-2',
                    'ctx_area_nk': 'NK-PAGE-2',
                },
                'M',
            ),
            (
                {
                    'output': [{'bass_dt': '20260604', 'opnd_yn': 'Y'}],
                    'ctx_area_fk': '',
                    'ctx_area_nk': '',
                },
                '',
            ),
        ]

    async def _get_response(self, path, *, tr_id, params, tr_cont=''):
        self.calls.append(
            {
                'path': path,
                'tr_id': tr_id,
                'params': dict(params),
                'tr_cont': tr_cont,
            }
        )
        return self.pages.pop(0)


class MarketSessionTests(unittest.TestCase):
    def tearDown(self) -> None:
        reset_krx_calendar_provider()

    def test_kis_client_fetches_chk_holiday_pages_with_context_keys(self):
        client = FakePagedKISClient()

        result = self.loop_run(client.fetch_krx_holiday_calendar(base_date='20260603'))

        self.assertEqual(
            [row['bass_dt'] for row in result['output']],
            ['20260603', '20260604'],
        )
        self.assertEqual(client.calls[0]['path'], '/uapi/domestic-stock/v1/quotations/chk-holiday')
        self.assertEqual(client.calls[0]['tr_id'], 'CTCA0903R')
        self.assertEqual(client.calls[0]['params']['BASS_DT'], '20260603')
        self.assertEqual(client.calls[0]['params']['CTX_AREA_FK'], '')
        self.assertEqual(client.calls[0]['params']['CTX_AREA_NK'], '')
        self.assertEqual(client.calls[1]['tr_cont'], 'N')
        self.assertEqual(client.calls[1]['params']['CTX_AREA_FK'], 'FK-PAGE-2')
        self.assertEqual(client.calls[1]['params']['CTX_AREA_NK'], 'NK-PAGE-2')

    def test_kis_provider_uses_opnd_yn_and_caches_rows(self):
        client = FakeCalendarClient(
            {
                'output': [
                    {'bass_dt': '20260602', 'opnd_yn': 'Y'},
                    {'bass_dt': '20260603', 'opnd_yn': 'N'},
                    {'bass_dt': '20260604', 'opnd_yn': 'Y'},
                ]
            }
        )
        provider = KisKrxTradingCalendarProvider(client=client)
        set_krx_calendar_provider(provider)

        self.assertFalse(is_krx_trading_day(date(2026, 6, 3)))
        self.assertTrue(is_krx_trading_day(date(2026, 6, 4)))
        self.assertEqual(latest_krx_trading_date(date(2026, 6, 3)), date(2026, 6, 2))
        self.assertEqual(previous_krx_trading_date(date(2026, 6, 3)), date(2026, 6, 2))
        self.assertEqual(next_krx_trading_date(date(2026, 6, 3)), date(2026, 6, 4))
        self.assertEqual(len(client.calls), 1)

    def test_kis_provider_falls_back_with_metadata_on_upstream_failure(self):
        provider = KisKrxTradingCalendarProvider(
            client=FakeCalendarClient(error=RuntimeError('kis down'))
        )

        result = provider.lookup_day(date(2026, 4, 29), allow_fallback=True)

        self.assertTrue(result.is_open)
        self.assertEqual(result.calendar_source, CALENDAR_SOURCE_EXCHANGE_CALENDARS)
        self.assertEqual(result.calendar_source_status, 'fallback')
        self.assertIn('kis down', result.calendar_source_error or '')
        with self.assertRaises(KrxCalendarUnavailableError):
            provider.lookup_day(date(2026, 4, 29), allow_fallback=False)

    def test_sync_provider_still_resolves_async_client_outside_running_loop(self):
        client = FakeCalendarClient({'output': [{'bass_dt': '20260604', 'opnd_yn': 'Y'}]})
        provider = KisKrxTradingCalendarProvider(client=client)

        result = provider.lookup_day(date(2026, 6, 4), allow_fallback=False)

        self.assertTrue(result.is_open)
        self.assertEqual(result.calendar_source, CALENDAR_SOURCE_KIS_CHK_HOLIDAY)
        self.assertEqual(len(client.calls), 1)

    def test_get_market_state_uses_kis_holiday_truth(self):
        provider = KisKrxTradingCalendarProvider(
            client=FakeCalendarClient(
                {
                    'output': [
                        {'bass_dt': '20260505', 'opnd_yn': 'N'},
                        {'bass_dt': '20260504', 'opnd_yn': 'Y'},
                    ]
                }
            )
        )
        set_krx_calendar_provider(provider)

        result = get_market_state(datetime(2026, 5, 5, 12, 0, tzinfo=KST))

        self.assertEqual(result.state, 'HOLIDAY')
        self.assertFalse(result.is_trading_day)
        self.assertEqual(result.latest_valid_trading_date, '2026-05-04')
        self.assertEqual(result.calendar_source, CALENDAR_SOURCE_KIS_CHK_HOLIDAY)
        self.assertEqual(result.calendar_source_status, 'ok')

    def test_async_get_market_state_does_not_block_loop_on_slow_calendar_lookup(self):
        async def scenario():
            client = FakeCalendarClient(
                {
                    'output': [
                        {'bass_dt': '20260603', 'opnd_yn': 'N'},
                        {'bass_dt': '20260602', 'opnd_yn': 'Y'},
                    ]
                },
                delay_seconds=0.15,
            )
            set_krx_calendar_provider(KisKrxTradingCalendarProvider(client=client))

            started = perf_counter()
            task = asyncio.create_task(
                async_get_market_state(datetime(2026, 6, 3, 12, 0, tzinfo=KST))
            )
            await asyncio.sleep(0.02)

            self.assertLess(perf_counter() - started, 0.10)
            self.assertFalse(task.done())
            result = await task
            self.assertEqual(result.state, 'HOLIDAY')
            self.assertEqual(result.latest_valid_trading_date, '2026-06-02')
            self.assertEqual(len(client.calls), 1)

        self.loop_run(scenario())

    def test_market_status_route_does_not_block_loop_on_slow_calendar_lookup(self):
        async def scenario():
            client = FakeCalendarClient(
                {
                    'output': [
                        {'bass_dt': '20260605', 'opnd_yn': 'Y'},
                    ]
                },
                delay_seconds=0.15,
            )
            set_krx_calendar_provider(KisKrxTradingCalendarProvider(client=client))

            async def fixed_market_state():
                return await async_get_market_state(datetime(2026, 6, 5, 12, 0, tzinfo=KST))

            with patch('app.api.routes_market.persist_market_status_snapshot'), patch(
                'app.api.routes_market.async_get_market_state',
                side_effect=fixed_market_state,
            ):
                started = perf_counter()
                task = asyncio.create_task(market_status())
                await asyncio.sleep(0.02)

                self.assertLess(perf_counter() - started, 0.10)
                self.assertFalse(task.done())
                payload = await task

            self.assertEqual(payload['market'], 'KRX')
            self.assertEqual(payload['calendar_source'], CALENDAR_SOURCE_KIS_CHK_HOLIDAY)
            self.assertEqual(len(client.calls), 1)

        self.loop_run(scenario())

    def test_market_status_route_falls_back_when_state_lookup_times_out(self):
        async def scenario():
            async def slow_market_state():
                await asyncio.sleep(0.05)

            with patch('app.api.routes_market.MARKET_STATUS_TIMEOUT_SECONDS', 0.01), patch(
                'app.api.routes_market.persist_market_status_snapshot'
            ) as persist, patch(
                'app.api.routes_market.async_get_market_state',
                side_effect=slow_market_state,
            ):
                payload = await market_status()

            self.assertEqual(payload['market'], 'KRX')
            self.assertEqual(payload['calendar_source'], 'local_weekday_fallback')
            self.assertEqual(payload['calendar_source_status'], 'fallback')
            self.assertEqual(payload['reason'], 'market_status_fallback')
            persist.assert_not_called()

        self.loop_run(scenario())

    @staticmethod
    def loop_run(awaitable):
        return asyncio.run(awaitable)

    def test_build_market_session_metadata_marks_weekend_as_latest_completed_eod(self):
        current_state = SimpleNamespace(
            state='WEEKEND',
            is_trading_day=False,
            is_open=False,
            reason='weekend',
            server_time='2026-04-11T12:00:00+09:00',
            current_calendar_date='2026-04-11',
            latest_valid_trading_date='2026-04-10',
            market_session='weekend',
        )

        result = build_market_session_metadata(
            requested_date='2026-04-11',
            current_state=current_state,
        )

        self.assertEqual(result['status'], 'weekend')
        self.assertEqual(result['requested_date'], '2026-04-11')
        self.assertEqual(result['latest_valid_trading_date'], '2026-04-10')
        self.assertFalse(result['is_trading_day'])
        self.assertFalse(result['intraday_data_expected'])
        self.assertFalse(result['quote_data_expected'])
        self.assertEqual(result['recommendation_basis'], RECOMMENDATION_BASIS_LATEST_COMPLETED_EOD)

    def test_get_market_state_marks_real_holiday_as_holiday(self):
        result = get_market_state(datetime(2026, 5, 5, 12, 0, tzinfo=KST))

        self.assertEqual(result.state, 'HOLIDAY')
        self.assertEqual(result.reason, 'holiday')
        self.assertEqual(result.current_calendar_date, '2026-05-05')
        self.assertEqual(result.latest_valid_trading_date, '2026-05-04')
        self.assertEqual(result.market_session, 'holiday')

    def test_market_session_helpers_recognize_real_holiday_and_open_session(self):
        self.assertFalse(is_krx_trading_day(date(2026, 5, 5)))
        self.assertEqual(market_session_status_for_date('2026-05-05'), 'holiday')

        open_state = SimpleNamespace(
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            reason=None,
            server_time='2026-04-29T10:05:00+09:00',
            current_calendar_date='2026-04-29',
            latest_valid_trading_date='2026-04-29',
            market_session='open',
        )
        result = build_market_session_metadata(
            requested_date='2026-04-29',
            current_state=open_state,
        )

        self.assertTrue(result['is_trading_day'])
        self.assertEqual(result['status'], 'open')
        self.assertTrue(result['intraday_data_expected'])
        self.assertTrue(result['quote_data_expected'])
        self.assertEqual(result['recommendation_basis'], RECOMMENDATION_BASIS_LIVE_INTRADAY)

    def test_historical_trading_day_uses_completed_eod_not_live_intraday(self):
        current_state = SimpleNamespace(
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            reason=None,
            server_time='2026-04-30T10:05:00+09:00',
            current_calendar_date='2026-04-30',
            latest_valid_trading_date='2026-04-30',
            market_session='open',
        )

        result = build_market_session_metadata(
            requested_date='2026-04-29',
            current_state=current_state,
        )

        self.assertEqual(result['status'], 'closed')
        self.assertFalse(result['requested_date_is_current_calendar_date'])
        self.assertTrue(result['requested_date_is_trading_day'])
        self.assertFalse(result['intraday_data_expected'])
        self.assertFalse(result['quote_data_expected'])
        self.assertEqual(result['recommendation_basis'], RECOMMENDATION_BASIS_LATEST_COMPLETED_EOD)


if __name__ == '__main__':
    unittest.main()
