import asyncio
import inspect
import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_quotes
from app.core.settings import settings
from app.quotes import kis_realtime_prices
from app.quotes.kis_realtime_prices import (
    KIS_REALTIME_PRICE_COLUMNS,
    KISRealtimePriceSubscriptionError,
    KISRealtimePriceService,
    parse_realtime_price_message,
)


class FakeKISRealtimePriceClient:
    def _environment(self) -> str:
        return 'demo'

    def websocket_url(self) -> str:
        return 'ws://example.test:31000'


class FakeWebSocket:
    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def send(self, message: str) -> None:
        self.sent.append(json.loads(message))

    async def pong(self, message: str) -> None:
        self.sent.append({'pong': message})


def _price_fields(
    *,
    symbol: str = '005930',
    price: str = '71500',
    hhmmss: str = '091530',
    business_date: str = '20260430',
) -> dict[str, str]:
    fields = {column: '' for column in KIS_REALTIME_PRICE_COLUMNS}
    fields.update(
        {
            'MKSC_SHRN_ISCD': symbol,
            'STCK_CNTG_HOUR': hhmmss,
            'STCK_PRPR': price,
            'PRDY_VRSS': '500',
            'PRDY_CTRT': '0.70',
            'ASKP1': '71600',
            'BIDP1': '71400',
            'ACML_VOL': '123456',
            'ACML_TR_PBMN': '8800000000',
            'BSOP_DATE': business_date,
            'TRHT_YN': 'N',
        }
    )
    return fields


def _message_for_rows(rows: list[dict[str, str]]) -> str:
    payload_values: list[str] = []
    for row in rows:
        payload_values.extend(row.get(column, '') for column in KIS_REALTIME_PRICE_COLUMNS)
    return f"0|H0STCNT0|{len(rows):03d}|{'^'.join(payload_values)}"


class KISRealtimePriceServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.original_settings = {
            'kis_realtime_price_max_subscriptions': settings.kis_realtime_price_max_subscriptions,
            'kis_realtime_price_subscribe_pause_seconds': settings.kis_realtime_price_subscribe_pause_seconds,
            'kis_realtime_price_unsubscribe_tr_type': settings.kis_realtime_price_unsubscribe_tr_type,
            'kis_realtime_price_stale_after_seconds': settings.kis_realtime_price_stale_after_seconds,
        }
        settings.kis_realtime_price_max_subscriptions = 2
        settings.kis_realtime_price_subscribe_pause_seconds = 0
        settings.kis_realtime_price_unsubscribe_tr_type = '2'
        settings.kis_realtime_price_stale_after_seconds = 60

    def tearDown(self) -> None:
        for key, value in self.original_settings.items():
            setattr(settings, key, value)

    async def test_parse_realtime_price_message_handles_multiple_ticks(self) -> None:
        received_at = datetime(2026, 4, 30, 0, 16, tzinfo=timezone.utc)
        message = _message_for_rows(
            [
                _price_fields(symbol='005930', price='71500'),
                _price_fields(symbol='000660', price='182000', hhmmss='091531'),
            ]
        )

        ticks = parse_realtime_price_message(message, received_at=received_at)

        self.assertEqual([tick.symbol for tick in ticks], ['005930', '000660'])
        self.assertEqual(ticks[0].current_price, 71500)
        self.assertEqual(ticks[0].bid_price, 71400)
        self.assertEqual(ticks[0].ask_price, 71600)
        self.assertEqual(ticks[0].price_as_of.isoformat(), '2026-04-30T00:15:30+00:00')
        self.assertFalse(ticks[0].trade_halt)

    async def test_ingest_updates_process_memory_without_db_persistence_dependency(self) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        received_at = datetime.now(timezone.utc)

        result = service.ingest_price_fields(_price_fields(), received_at=received_at)
        service.observe_monitored_symbols(['005930'], trigger='test')
        price_fields = service.price_fields_for_symbol('005930')

        self.assertIsNotNone(result)
        self.assertEqual(price_fields['current_price'], 71500)
        self.assertEqual(price_fields['price_source'], 'kis_realtime_ws')
        self.assertEqual(price_fields['price_status'], 'available')
        self.assertIn('005930', [item['symbol'] for item in service.monitored_snapshot()['items']])

        source = inspect.getsource(kis_realtime_prices)
        self.assertNotIn('persist_quote_snapshot', source)
        self.assertNotIn('session_scope', source)

    async def test_price_status_marks_stale_when_snapshot_exceeds_threshold(self) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        stale_received_at = datetime.now(timezone.utc) - timedelta(seconds=90)

        service.ingest_price_fields(_price_fields(), received_at=stale_received_at)

        self.assertEqual(service.price_fields_for_symbol('005930')['price_status'], 'stale')

    async def test_subscription_limit_and_dedupe(self) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        websocket = FakeWebSocket()

        snapshot = service.observe_monitored_symbols(
            ['005930', '000660', '005930', '035420'],
            trigger='test',
        )
        await service._sync_subscriptions(websocket, approval_key='approval-test')
        await service._sync_subscriptions(websocket, approval_key='approval-test')

        self.assertEqual(snapshot['allowed_symbols'], ['000660', '005930'])
        self.assertEqual(snapshot['limited_symbols'], ['035420'])
        self.assertEqual([msg['body']['input']['tr_key'] for msg in websocket.sent], ['000660', '005930'])
        self.assertEqual({msg['header']['tr_type'] for msg in websocket.sent}, {'1'})

        service.observe_monitored_symbols(['035420'], trigger='test')
        await service._sync_subscriptions(websocket, approval_key='approval-test')

        sent_after_update = websocket.sent[2:]
        self.assertEqual(
            [(msg['header']['tr_type'], msg['body']['input']['tr_key']) for msg in sent_after_update],
            [('2', '000660'), ('2', '005930'), ('1', '035420')],
        )

    async def test_subscription_limit_preserves_priority_order_when_requested(self) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())

        snapshot = service.observe_monitored_symbols(
            ['035420', '000660', '005930'],
            trigger='priority-test',
            preserve_order=True,
        )

        self.assertEqual(snapshot['allowed_symbols'], ['035420', '000660'])
        self.assertEqual(snapshot['limited_symbols'], ['005930'])
        limited_fields = service.price_fields_for_symbol('005930')
        self.assertEqual(limited_fields['price_status'], 'limited')
        self.assertFalse(limited_fields['realtime_subscribed'])
        self.assertEqual(limited_fields['realtime_subscription_reason'], 'capacity_limited')

    async def test_subscription_source_timeout_preserves_symbols_and_reuses_inflight_refresh(
        self,
    ) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        service.observe_monitored_symbols(['005930'], trigger='dashboard')
        release_loader = asyncio.Event()

        async def _blocked_loader(**_: object) -> list[str]:
            await release_loader.wait()
            return ['027710']

        blocked_loader = AsyncMock(side_effect=_blocked_loader)

        with (
            patch.object(service, '_load_runtime_monitored_symbols', blocked_loader),
            patch.object(
                kis_realtime_prices,
                'KIS_REALTIME_PRICE_SOURCE_REFRESH_TIMEOUT_SECONDS',
                0.01,
            ),
            patch.object(settings, 'kis_realtime_price_subscription_refresh_seconds', 60.0),
        ):
            first = await service.refresh_subscription_source_if_due(
                trigger='background',
                force=True,
            )
            second = await service.refresh_subscription_source_if_due(
                trigger='background',
                force=False,
            )
            release_loader.set()
            await asyncio.wait_for(
                asyncio.shield(service._source_refresh_task),
                timeout=1,
            )
            completed = await service.refresh_subscription_source_if_due(
                trigger='background',
                force=False,
            )

        self.assertEqual(first['allowed_symbols'], ['005930'])
        self.assertEqual(second['allowed_symbols'], ['005930'])
        self.assertEqual(completed['allowed_symbols'], ['027710'])
        self.assertEqual(blocked_loader.await_count, 1)
        self.assertIsNone(service._source_refresh_task)
        self.assertEqual(service.health_snapshot()['last_error']['stage'], 'subscription_source')
        self.assertIn('timed out', service.health_snapshot()['last_error']['message'])
        await service.stop()

    async def test_inflight_source_refresh_is_not_reawaited_after_refresh_interval(
        self,
    ) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        service.observe_monitored_symbols(['005930'], trigger='dashboard')
        release_loader = asyncio.Event()

        async def _blocked_loader(**_: object) -> list[str]:
            await release_loader.wait()
            return ['027710']

        blocked_loader = AsyncMock(side_effect=_blocked_loader)
        source_task = asyncio.create_task(blocked_loader(trigger='background'))
        service._source_refresh_task = source_task
        service._last_source_refresh_monotonic = 0.0

        try:
            with (
                patch.object(
                    kis_realtime_prices,
                    'KIS_REALTIME_PRICE_SOURCE_REFRESH_TIMEOUT_SECONDS',
                    1.0,
                ),
                patch.object(
                    settings,
                    'kis_realtime_price_subscription_refresh_seconds',
                    1.0,
                ),
            ):
                snapshot = await asyncio.wait_for(
                    service.refresh_subscription_source_if_due(
                        trigger='background',
                        force=False,
                    ),
                    timeout=0.05,
                )
        finally:
            release_loader.set()
            await asyncio.wait_for(asyncio.shield(source_task), timeout=1)

        self.assertEqual(snapshot['allowed_symbols'], ['005930'])
        self.assertEqual(blocked_loader.await_count, 1)
        self.assertIs(service._source_refresh_task, source_task)

        completed = await service.refresh_subscription_source_if_due(
            trigger='background',
            force=False,
        )
        self.assertEqual(completed['allowed_symbols'], ['027710'])
        self.assertIsNone(service._source_refresh_task)
        await service.stop()

    async def test_background_loop_does_not_force_subscription_source_refresh(self) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())

        async def _stop_after_refresh(**_: object) -> dict:
            service._stop_requested = True
            return {}

        refresh = AsyncMock(side_effect=_stop_after_refresh)
        wait_for_change = AsyncMock()
        with (
            patch.object(service, 'refresh_subscription_source_if_due', refresh),
            patch.object(service, '_wait_for_change_or_timeout', wait_for_change),
        ):
            await service._run()

        self.assertFalse(refresh.await_args.kwargs['force'])
        wait_for_change.assert_awaited_once()

    async def test_failed_subscription_ack_clears_subscribed_symbol_for_retry(self) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        websocket = FakeWebSocket()

        service.observe_monitored_symbols(['005930'], trigger='test')
        await service._sync_subscriptions(websocket, approval_key='approval-test')

        self.assertEqual(service.subscription_snapshot()['subscribed_symbols'], ['005930'])

        failure_message = json.dumps(
            {
                'header': {'tr_id': 'H0STCNT0', 'tr_key': '005930'},
                'body': {'rt_cd': '1', 'msg1': 'subscription rejected'},
            }
        )
        with self.assertRaises(KISRealtimePriceSubscriptionError):
            await service._handle_system_message(
                websocket,
                failure_message,
                received_at=datetime.now(timezone.utc),
            )

        health = service.health_snapshot()
        self.assertEqual(health['status'], 'reconnecting')
        self.assertEqual(service.subscription_snapshot()['subscribed_symbols'], [])
        self.assertEqual(health['subscription']['status'], 'failed')
        self.assertEqual(health['subscription']['result_code'], '1')
        self.assertEqual(health['subscription']['failed_symbol'], '005930')

        await service._sync_subscriptions(websocket, approval_key='approval-test')

        self.assertEqual(
            [msg['body']['input']['tr_key'] for msg in websocket.sent],
            ['005930', '005930'],
        )

    async def test_subscription_ack_failure_is_retryable_for_reconnect(self) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        exc = KISRealtimePriceSubscriptionError('subscription rejected')

        self.assertFalse(service._is_fatal_error(exc))
        self.assertTrue(service._is_retryable_error(exc))


class FakeRealtimePriceRouteService:
    def __init__(self) -> None:
        self.refresh_calls: list[dict] = []

    async def refresh_subscription_source_if_due(self, **kwargs) -> dict:
        self.refresh_calls.append(kwargs)
        return {'allowed_symbols': ['005930']}

    def monitored_snapshot(self) -> dict:
        return {
            'status': 'subscribed',
            'summary': {'items_total': 1},
            'items': [
                {
                    'symbol': '005930',
                    'current_price': 71500,
                    'price_as_of': '2026-04-30T00:15:30+00:00',
                    'price_source': 'kis_realtime_ws',
                    'price_status': 'available',
                }
            ],
            'health': {'status': 'subscribed'},
        }


class KISRealtimePriceRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = FastAPI()
        self.app.include_router(routes_quotes.router)
        self.original_service = routes_quotes.kis_realtime_price_service
        self.service = FakeRealtimePriceRouteService()
        routes_quotes.kis_realtime_price_service = self.service

    def tearDown(self) -> None:
        routes_quotes.kis_realtime_price_service = self.original_service

    def test_realtime_monitored_route_returns_memory_snapshot(self) -> None:
        client = TestClient(self.app)

        response = client.get('/quotes/realtime/monitored', params={'refresh_symbols': 'true'})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['items'][0]['symbol'], '005930')
        self.assertEqual(payload['items'][0]['current_price'], 71500)
        self.assertEqual(payload['items'][0]['price_status'], 'available')
        self.assertEqual(self.service.refresh_calls[0]['trigger'], 'route_quotes_realtime_monitored')
        self.assertFalse(self.service.refresh_calls[0]['force'])


if __name__ == '__main__':
    unittest.main()
