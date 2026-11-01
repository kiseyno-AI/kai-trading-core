import asyncio
import inspect
import json
import threading
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_quotes
from app.agent_context import wait_recheck_service as wait_recheck_module
from app.core.settings import settings
from app.quotes import kis_realtime_prices
from app.quotes.kis_realtime_prices import (
    KIS_REALTIME_PRICE_COLUMNS,
    KISRealtimeMonitoredSymbolSource,
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


def _runtime_source_payload(
    symbols: list[str],
    *,
    generated_at: datetime,
    membership_complete: bool,
    protected_symbols: set[str] | None = None,
    critical_symbols: set[str] | None = None,
) -> dict:
    protected_symbols = protected_symbols or set()
    critical_symbols = critical_symbols or set()
    return {
        'market_date': generated_at.astimezone(kis_realtime_prices.KST).date().isoformat(),
        'items': [
            {
                'symbol': symbol,
                **(
                    {'held_position': True, 'position_quantity': 1}
                    if symbol in critical_symbols
                    else {}
                ),
                **(
                    {'monitoring_state': 'reconciling'}
                    if symbol in protected_symbols
                    else {}
                ),
            }
            for symbol in symbols
        ],
        'realtime_subscription': {
            'desired_symbols': symbols,
            'desired_symbol_count': len(symbols),
        },
        'source_coverage': {'membership_complete': membership_complete},
        'freshness': {
            'schema_version': 'execution_runtime_dashboard_read_model_v1',
            'endpoint': 'monitored_symbols',
            'market_date': generated_at.astimezone(
                kis_realtime_prices.KST
            ).date().isoformat(),
            'status': 'fresh',
            'generated_at': generated_at.isoformat(),
            'is_stale': False,
            'is_partial': False,
            'refreshing': False,
        },
    }


def _runtime_source(
    symbols: list[str],
    *,
    protected_symbols: tuple[str, ...] = (),
) -> KISRealtimeMonitoredSymbolSource:
    generated_at = datetime.now(timezone.utc)
    return KISRealtimeMonitoredSymbolSource(
        symbols=tuple(symbols),
        market_date=generated_at.astimezone(
            kis_realtime_prices.KST
        ).date().isoformat(),
        source_version=generated_at,
        membership_complete=True,
        protected_symbols=protected_symbols,
    )


def _market_state(
    state: str,
    *,
    calendar_source: str = 'kis_chk_holiday',
    calendar_source_status: str = 'ok',
    calendar_source_error: str | None = None,
) -> SimpleNamespace:
    now = datetime.now(timezone.utc)
    normalized = state.strip().upper()
    is_trading_day = normalized not in {'WEEKEND', 'HOLIDAY'}
    return SimpleNamespace(
        market='KRX',
        state=normalized,
        is_trading_day=is_trading_day,
        is_open=normalized == 'OPEN',
        server_time=now.isoformat(),
        session_open_at=(now - timedelta(hours=1)).isoformat(),
        session_close_at=(now + timedelta(hours=1)).isoformat(),
        calendar_source=calendar_source,
        calendar_source_status=calendar_source_status,
        calendar_source_error=calendar_source_error,
    )


def _record_open_market_session(service: KISRealtimePriceService) -> None:
    service._record_market_session(_market_state('OPEN'), active=True)


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

    async def _assert_idle_market_transition_connects(
        self,
        *,
        initial_market: SimpleNamespace,
        expected_idle_status: str,
    ) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        service.observe_monitored_symbols(['005930'], trigger='test')
        trace: list[str] = []
        idle_statuses: list[str] = []
        authoritative_open = _market_state('OPEN')
        market_state = AsyncMock(
            side_effect=(
                initial_market,
                authoritative_open,
                authoritative_open,
                authoritative_open,
                authoritative_open,
                authoritative_open,
            )
        )

        async def wait_once(_: float) -> None:
            trace.append('idle_wait')
            idle_statuses.append(service.health_snapshot()['status'])

        wait_for_change = AsyncMock(side_effect=wait_once)

        async def refresh_once(**_: object) -> dict:
            trace.append('refresh')
            return {}

        refresh = AsyncMock(side_effect=refresh_once)

        async def issue_approval() -> dict[str, str]:
            trace.append('approval')
            return {'approval_key': 'approval-test'}

        service.client.issue_approval_key = AsyncMock(
            side_effect=issue_approval
        )
        websocket = FakeWebSocket()

        async def send_subscription(message: str) -> None:
            trace.append('subscription')
            websocket.sent.append(json.loads(message))

        websocket.send = AsyncMock(side_effect=send_subscription)

        async def stop_after_subscription(*_: object, **__: object) -> None:
            service._stop_requested = True

        receive_or_refresh = AsyncMock(side_effect=stop_after_subscription)

        class FakeConnection:
            async def __aenter__(self) -> FakeWebSocket:
                return websocket

            async def __aexit__(self, *_: object) -> None:
                return None

        def open_connection(*_: object, **__: object) -> FakeConnection:
            trace.append('connect')
            return FakeConnection()

        connect = Mock(side_effect=open_connection)
        with (
            patch(
                'app.agent_context.wait_recheck_service.'
                'signal_decider_wait_recheck_service.'
                'clear_candidate_cache'
            ) as clear_candidate_cache,
            patch.object(
                kis_realtime_prices,
                'async_get_market_state',
                market_state,
            ),
            patch.object(kis_realtime_prices.websockets, 'connect', connect),
            patch.object(
                service,
                'refresh_subscription_source_if_due',
                refresh,
            ),
            patch.object(
                service,
                '_wait_for_change_or_timeout',
                wait_for_change,
            ),
            patch.object(
                service,
                '_receive_or_refresh',
                receive_or_refresh,
            ),
            patch.object(
                service,
                '_is_configured',
                return_value=(True, None),
            ),
        ):
            await service._run()

        self.assertEqual(
            trace,
            ['idle_wait', 'refresh', 'approval', 'connect', 'subscription'],
        )
        self.assertEqual(idle_statuses, [expected_idle_status])
        wait_for_change.assert_awaited_once_with(
            settings.kis_realtime_price_idle_poll_seconds
        )
        refresh.assert_awaited_once_with(trigger='background', force=False)
        service.client.issue_approval_key.assert_awaited_once_with()
        connect.assert_called_once_with(
            'ws://example.test:31000',
            ping_interval=30.0,
            ping_timeout=10.0,
        )
        websocket.send.assert_awaited_once()
        receive_or_refresh.assert_awaited_once_with(
            websocket,
            approval_key='approval-test',
        )
        self.assertEqual(
            [
                (
                    item['header']['tr_type'],
                    item['body']['input']['tr_key'],
                )
                for item in websocket.sent
            ],
            [('1', '005930')],
        )
        self.assertEqual(market_state.await_count, 6)
        clear_candidate_cache.assert_called_once_with()
        health = service.health_snapshot()
        self.assertEqual(health['market_state'], 'OPEN')
        self.assertTrue(health['market_session_active'])
        self.assertFalse(health['websocket_connected'])
        self.assertEqual(health['market_calendar_source'], 'kis_chk_holiday')
        self.assertEqual(health['market_calendar_source_status'], 'ok')
        self.assertEqual(health['reconnect_attempts'], 0)
        self.assertIsNone(health['last_error'])

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
        _record_open_market_session(service)

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

    async def test_background_connection_uses_official_transport_heartbeat(
        self,
    ) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        service.observe_monitored_symbols(['005930'], trigger='test')
        service.client.issue_approval_key = AsyncMock(
            return_value={'approval_key': 'approval-test'}
        )
        websocket = FakeWebSocket()
        ping_message = json.dumps(
            {'header': {'tr_id': 'PINGPONG'}, 'body': {}}
        )

        async def receive_ping_once() -> str:
            service._stop_requested = True
            return ping_message

        websocket.recv = AsyncMock(side_effect=receive_ping_once)

        class FakeConnection:
            async def __aenter__(self) -> FakeWebSocket:
                return websocket

            async def __aexit__(self, *_: object) -> None:
                return None

        connect = Mock(return_value=FakeConnection())
        refresh = AsyncMock(return_value={})
        with (
            patch.object(
                kis_realtime_prices,
                'async_get_market_state',
                AsyncMock(return_value=_market_state('OPEN')),
            ),
            patch.object(kis_realtime_prices.websockets, 'connect', connect),
            patch.object(
                service,
                'refresh_subscription_source_if_due',
                refresh,
            ),
        ):
            await service._run()

        connect.assert_called_once_with(
            'ws://example.test:31000',
            ping_interval=30.0,
            ping_timeout=10.0,
        )
        self.assertIn({'pong': ping_message}, websocket.sent)
        self.assertEqual(
            service.health_snapshot()['metrics']['system_messages'],
            1,
        )

    async def test_background_stream_does_not_connect_when_market_is_weekend(
        self,
    ) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        service.observe_monitored_symbols(['005930'], trigger='test')
        service.client.issue_approval_key = AsyncMock(
            return_value={'approval_key': 'approval-test'}
        )
        refresh = AsyncMock(return_value={})
        connect = Mock()

        async def stop_after_idle(_: float) -> None:
            service._stop_requested = True

        with (
            patch(
                'app.agent_context.wait_recheck_service.'
                'signal_decider_wait_recheck_service.'
                'clear_candidate_cache'
            ) as clear_candidate_cache,
            patch.object(
                kis_realtime_prices,
                'async_get_market_state',
                AsyncMock(return_value=_market_state('WEEKEND')),
            ),
            patch.object(kis_realtime_prices.websockets, 'connect', connect),
            patch.object(
                service,
                'refresh_subscription_source_if_due',
                refresh,
            ),
            patch.object(
                service,
                '_wait_for_change_or_timeout',
                AsyncMock(side_effect=stop_after_idle),
            ),
        ):
            await service._run()

        service.client.issue_approval_key.assert_not_awaited()
        refresh.assert_not_awaited()
        connect.assert_not_called()
        health = service.health_snapshot()
        self.assertEqual(health['status'], 'idle_market_weekend')
        self.assertEqual(health['market_state'], 'WEEKEND')
        self.assertFalse(health['market_session_active'])
        self.assertFalse(health['websocket_connected'])
        self.assertEqual(health['reconnect_attempts'], 0)
        clear_candidate_cache.assert_called_once_with()

    def test_closed_market_snapshot_preserves_desired_membership_without_pending_or_limited_items(
        self,
    ) -> None:
        with (
            patch.object(settings, 'kis_realtime_price_enabled', True),
            patch.object(settings, 'kis_app_key', 'test-key'),
            patch.object(settings, 'kis_app_secret', 'test-secret'),
        ):
            service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
            service.observe_monitored_symbols(
                ['005930', '000660', '035420'],
                trigger='test',
            )
            service._set_market_idle(_market_state('WEEKEND'))

            closed = service.monitored_snapshot()

            self.assertEqual(closed['summary']['desired_count'], 3)
            self.assertEqual(closed['summary']['pending_count'], 0)
            self.assertEqual(closed['summary']['limited_count'], 0)
            self.assertEqual(
                {item['price_status'] for item in closed['items']},
                {'market_closed'},
            )
            self.assertEqual(
                {item['realtime_subscription_reason'] for item in closed['items']},
                {'market_weekend'},
            )

            _record_open_market_session(service)
            service.subscription_snapshot()
            reopened = service.monitored_snapshot()

            self.assertEqual(reopened['summary']['desired_count'], 3)
            self.assertEqual(reopened['summary']['pending_count'], 2)
            self.assertEqual(reopened['summary']['limited_count'], 1)
            self.assertEqual(
                {item['price_status'] for item in reopened['items']},
                {'pending', 'limited'},
            )

    async def test_background_stream_fails_closed_when_calendar_is_unverified(
        self,
    ) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        service.observe_monitored_symbols(['005930'], trigger='test')
        service.client.issue_approval_key = AsyncMock(
            return_value={'approval_key': 'approval-test'}
        )
        refresh = AsyncMock(return_value={})
        connect = Mock()

        async def stop_after_idle(_: float) -> None:
            service._stop_requested = True

        with (
            patch.object(
                kis_realtime_prices,
                'async_get_market_state',
                AsyncMock(
                    return_value=_market_state(
                        'OPEN',
                        calendar_source='exchange_calendars',
                        calendar_source_status='fallback',
                        calendar_source_error='kis calendar unavailable',
                    )
                ),
            ),
            patch.object(kis_realtime_prices.websockets, 'connect', connect),
            patch.object(
                service,
                'refresh_subscription_source_if_due',
                refresh,
            ),
            patch.object(
                service,
                '_wait_for_change_or_timeout',
                AsyncMock(side_effect=stop_after_idle),
            ),
        ):
            await service._run()

        service.client.issue_approval_key.assert_not_awaited()
        refresh.assert_not_awaited()
        connect.assert_not_called()
        health = service.health_snapshot()
        self.assertEqual(health['status'], 'idle_market_unverified')
        self.assertEqual(health['market_state'], 'OPEN')
        self.assertFalse(health['market_session_active'])
        self.assertFalse(health['websocket_connected'])
        self.assertEqual(
            health['market_calendar_source_status'],
            'fallback',
        )

    async def test_preopen_transitions_to_authoritative_open_without_reconnect(
        self,
    ) -> None:
        await self._assert_idle_market_transition_connects(
            initial_market=_market_state('PREOPEN'),
            expected_idle_status='idle_market_preopen',
        )

    async def test_closed_transitions_to_next_open_without_reconnect(
        self,
    ) -> None:
        await self._assert_idle_market_transition_connects(
            initial_market=_market_state('CLOSED'),
            expected_idle_status='idle_market_closed',
        )

    async def test_fallback_calendar_transitions_to_authoritative_open_without_reconnect(
        self,
    ) -> None:
        await self._assert_idle_market_transition_connects(
            initial_market=_market_state(
                'OPEN',
                calendar_source='exchange_calendars',
                calendar_source_status='fallback',
                calendar_source_error='kis calendar unavailable',
            ),
            expected_idle_status='idle_market_unverified',
        )

    async def test_inactive_market_resets_active_session_reconnect_backoff(
        self,
    ) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        service.observe_monitored_symbols(['005930'], trigger='test')
        service.client.issue_approval_key = AsyncMock(
            side_effect=RuntimeError('transient approval failure')
        )
        market_state = AsyncMock(
            side_effect=(
                _market_state('OPEN'),
                _market_state('OPEN'),
                _market_state('OPEN'),
                _market_state('OPEN'),
                _market_state('CLOSED'),
                _market_state('OPEN'),
                _market_state('OPEN'),
            )
        )
        sleep_delays: list[float] = []

        async def record_reconnect_sleep(delay: float) -> None:
            sleep_delays.append(delay)
            if len(sleep_delays) == 3:
                service._stop_requested = True

        with (
            patch.object(
                settings,
                'kis_realtime_reconnect_base_delay_seconds',
                3.0,
            ),
            patch.object(
                settings,
                'kis_realtime_reconnect_max_delay_seconds',
                30.0,
            ),
            patch(
                'app.agent_context.wait_recheck_service.'
                'signal_decider_wait_recheck_service.'
                'clear_candidate_cache'
            ) as clear_candidate_cache,
            patch.object(
                kis_realtime_prices,
                'async_get_market_state',
                market_state,
            ),
            patch.object(
                service,
                'refresh_subscription_source_if_due',
                AsyncMock(return_value={}),
            ),
            patch.object(
                service,
                '_wait_for_change_or_timeout',
                AsyncMock(),
            ),
            patch.object(
                kis_realtime_prices.asyncio,
                'sleep',
                AsyncMock(side_effect=record_reconnect_sleep),
            ),
        ):
            await service._run()

        self.assertEqual(sleep_delays, [3.0, 6.0, 3.0])
        self.assertEqual(market_state.await_count, 7)
        self.assertEqual(
            service.client.issue_approval_key.await_count,
            3,
        )
        self.assertEqual(
            service.health_snapshot()['reconnect_attempts'],
            3,
        )
        clear_candidate_cache.assert_called_once_with()

    async def test_market_state_error_invalidates_open_session_and_wait_cache(
        self,
    ) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        service._record_market_session(_market_state('OPEN'), active=True)
        with service._state_lock:
            service._base_symbols = {'000660'}
            service._base_symbol_order = ['000660']
            service._wait_recheck_symbols = {'005930'}
            service._wait_recheck_symbol_order = ['005930']
            service._desired_symbols = {'000660', '005930'}
            service._desired_symbol_order = ['000660', '005930']
            service._sync_health_symbols_locked()
        service.client.issue_approval_key = AsyncMock(
            return_value={'approval_key': 'approval-test'}
        )
        refresh = AsyncMock(return_value={})
        connect = Mock()

        async def stop_after_idle(_: float) -> None:
            service._stop_requested = True

        with (
            patch(
                'app.agent_context.wait_recheck_service.'
                'signal_decider_wait_recheck_service.'
                'clear_candidate_cache'
            ) as clear_candidate_cache,
            patch.object(
                kis_realtime_prices,
                'async_get_market_state',
                AsyncMock(side_effect=RuntimeError('calendar lookup failed')),
            ),
            patch.object(kis_realtime_prices.websockets, 'connect', connect),
            patch.object(
                service,
                'refresh_subscription_source_if_due',
                refresh,
            ),
            patch.object(
                service,
                '_wait_for_change_or_timeout',
                AsyncMock(side_effect=stop_after_idle),
            ),
        ):
            await service._run()

        service.client.issue_approval_key.assert_not_awaited()
        refresh.assert_not_awaited()
        connect.assert_not_called()
        health = service.health_snapshot()
        self.assertEqual(health['status'], 'idle_market_unverified')
        self.assertEqual(health['market_state'], 'UNKNOWN')
        self.assertFalse(health['market_session_active'])
        self.assertFalse(health['websocket_connected'])
        self.assertEqual(health['market_calendar_source_status'], 'unavailable')
        self.assertEqual(health['wait_recheck_symbols'], [])
        self.assertEqual(health['desired_symbols'], ['000660'])
        self.assertEqual(health['reconnect_attempts'], 0)
        self.assertEqual(health['last_error']['stage'], 'market_state')
        clear_candidate_cache.assert_called_once_with()

    async def test_background_stream_disconnects_when_open_market_closes(
        self,
    ) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        with service._state_lock:
            service._base_symbols = {'000660'}
            service._base_symbol_order = ['000660']
            service._wait_recheck_symbols = {'005930'}
            service._wait_recheck_symbol_order = ['005930']
            service._desired_symbols = {'000660', '005930'}
            service._desired_symbol_order = ['000660', '005930']
            service._sync_health_symbols_locked()
        service.client.issue_approval_key = AsyncMock(
            return_value={'approval_key': 'approval-test'}
        )
        websocket = FakeWebSocket()
        websocket.recv = AsyncMock()

        class FakeConnection:
            async def __aenter__(self) -> FakeWebSocket:
                return websocket

            async def __aexit__(self, *_: object) -> None:
                return None

        connect = Mock(return_value=FakeConnection())
        refresh = AsyncMock(return_value={})
        market_states = iter(
            (
                _market_state('OPEN'),
                _market_state('OPEN'),
                _market_state('OPEN'),
                _market_state('OPEN'),
                _market_state('CLOSED'),
            )
        )

        async def next_market_state() -> SimpleNamespace:
            market = next(market_states)
            if market.state == 'CLOSED':
                service._stop_requested = True
            return market

        with (
            patch(
                'app.agent_context.wait_recheck_service.'
                'signal_decider_wait_recheck_service.'
                'clear_candidate_cache'
            ) as clear_candidate_cache,
            patch.object(
                kis_realtime_prices,
                'async_get_market_state',
                AsyncMock(side_effect=next_market_state),
            ),
            patch.object(kis_realtime_prices.websockets, 'connect', connect),
            patch.object(
                service,
                'refresh_subscription_source_if_due',
                refresh,
            ),
        ):
            await service._run()

        connect.assert_called_once()
        websocket.recv.assert_not_awaited()
        health = service.health_snapshot()
        self.assertEqual(health['status'], 'idle_market_closed')
        self.assertEqual(health['market_state'], 'CLOSED')
        self.assertFalse(health['market_session_active'])
        self.assertFalse(health['websocket_connected'])
        self.assertEqual(health['subscribed_symbols'], [])
        self.assertEqual(health['desired_symbols'], ['000660'])
        self.assertEqual(health['wait_recheck_symbols'], [])
        clear_candidate_cache.assert_called_once_with()

    async def test_market_close_after_source_refresh_prevents_approval_and_connect(
        self,
    ) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        service.observe_monitored_symbols(['005930'], trigger='test')
        service.client.issue_approval_key = AsyncMock(
            return_value={'approval_key': 'approval-test'}
        )
        refresh = AsyncMock(return_value={})
        connect = Mock()
        market_states = iter(
            (_market_state('OPEN'), _market_state('CLOSED'))
        )

        async def next_market_state() -> SimpleNamespace:
            market = next(market_states)
            if market.state == 'CLOSED':
                service._stop_requested = True
            return market

        with (
            patch.object(
                kis_realtime_prices,
                'async_get_market_state',
                AsyncMock(side_effect=next_market_state),
            ),
            patch.object(kis_realtime_prices.websockets, 'connect', connect),
            patch.object(
                service,
                'refresh_subscription_source_if_due',
                refresh,
            ),
        ):
            await service._run()

        refresh.assert_awaited_once()
        service.client.issue_approval_key.assert_not_awaited()
        connect.assert_not_called()
        health = service.health_snapshot()
        self.assertEqual(health['status'], 'idle_market_closed')
        self.assertFalse(health['websocket_connected'])

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
        _record_open_market_session(service)
        service.observe_monitored_symbols(['005930'], trigger='dashboard')
        release_loader = asyncio.Event()

        async def _blocked_loader(
            **_: object,
        ) -> KISRealtimeMonitoredSymbolSource:
            await release_loader.wait()
            return _runtime_source(['027710'])

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
        _record_open_market_session(service)
        service.observe_monitored_symbols(['005930'], trigger='dashboard')
        release_loader = asyncio.Event()

        async def _blocked_loader(
            **_: object,
        ) -> KISRealtimeMonitoredSymbolSource:
            await release_loader.wait()
            return _runtime_source(['027710'])

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

    async def test_completed_source_refresh_does_not_replace_newer_observation(
        self,
    ) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        _record_open_market_session(service)
        service.observe_monitored_symbols(['005930'], trigger='initial-dashboard')
        loader_started = asyncio.Event()
        release_loader = asyncio.Event()

        async def _slow_stale_loader(**_: object) -> list[str]:
            loader_started.set()
            await release_loader.wait()
            return []

        with (
            patch.object(
                service,
                '_load_runtime_monitored_symbols',
                AsyncMock(side_effect=_slow_stale_loader),
            ),
            patch.object(
                kis_realtime_prices,
                'KIS_REALTIME_PRICE_SOURCE_REFRESH_TIMEOUT_SECONDS',
                0.01,
            ),
            patch.object(
                settings,
                'kis_realtime_price_subscription_refresh_seconds',
                60.0,
            ),
        ):
            timed_out = await service.refresh_subscription_source_if_due(
                trigger='background',
                force=True,
            )
            await asyncio.wait_for(loader_started.wait(), timeout=1)
            service.observe_monitored_symbols(
                ['027710'],
                trigger='newer-dashboard',
                preserve_order=True,
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

        self.assertEqual(timed_out['allowed_symbols'], ['005930'])
        self.assertEqual(completed['allowed_symbols'], ['027710'])
        self.assertIsNone(service._source_refresh_task)
        await service.stop()

    async def test_unversioned_empty_source_cannot_clear_current_symbols(self) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        _record_open_market_session(service)
        service.observe_monitored_symbols(
            ['027710'],
            trigger='current-dashboard',
            preserve_order=True,
        )

        with patch.object(
            service,
            '_load_runtime_monitored_symbols',
            new=AsyncMock(return_value=[]),
        ):
            snapshot = await service.refresh_subscription_source_if_due(
                trigger='legacy-source',
                force=True,
            )

        self.assertEqual(snapshot['allowed_symbols'], ['027710'])
        await service.stop()

    async def test_closed_market_fences_inflight_wait_source_before_apply(
        self,
    ) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        now = datetime.now(timezone.utc)
        with service._state_lock:
            service._base_symbols = {'000660'}
            service._base_symbol_order = ['000660']
            service._wait_recheck_symbols = {'005930'}
            service._wait_recheck_symbol_order = ['005930']
            service._desired_symbols = {'000660', '005930'}
            service._desired_symbol_order = ['000660', '005930']
            service._sync_health_symbols_locked()
        service._record_market_session(_market_state('CLOSED'), active=False)
        completed_source = KISRealtimeMonitoredSymbolSource(
            symbols=('000660', '005930'),
            market_date=now.astimezone(
                kis_realtime_prices.KST
            ).date().isoformat(),
            source_version=now,
            membership_complete=True,
            base_symbols=('000660',),
            wait_recheck_symbols=('005930',),
            wait_recheck_membership_complete=True,
            wait_recheck_source_version=now,
        )

        with (
            patch.object(
                service,
                '_load_runtime_monitored_symbols',
                AsyncMock(return_value=completed_source),
            ),
            patch(
                'app.agent_context.wait_recheck_service.'
                'signal_decider_wait_recheck_service.'
                'clear_candidate_cache'
            ) as clear_candidate_cache,
        ):
            snapshot = await service.refresh_subscription_source_if_due(
                trigger='route-refresh-after-close',
                force=True,
            )

        self.assertEqual(snapshot['desired_symbols'], ['000660'])
        self.assertEqual(snapshot['wait_recheck_symbols'], [])
        clear_candidate_cache.assert_called_once_with()

    async def test_stop_revokes_inflight_wait_cache_commit(self) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        _record_open_market_session(service)
        wait_service = wait_recheck_module.SignalDeciderWaitRecheckService()
        candidate = SimpleNamespace(
            symbol='005930',
            agent_name='signal-decider',
            target_trade_date=datetime.now(
                kis_realtime_prices.KST
            ).date(),
            source_snapshot_id=17,
        )
        source_started = threading.Event()
        allow_source_finish = threading.Event()
        source_finished = threading.Event()
        base_source = _runtime_source(['000660'])

        def blocking_wait_source(
            *,
            target_trade_date: object,
            expected_cache_epoch: int | None = None,
        ) -> SimpleNamespace:
            del target_trade_date
            source_started.set()
            if not allow_source_finish.wait(timeout=2):
                raise TimeoutError('test did not release WAIT source')
            if expected_cache_epoch is None:
                wait_service.replace_candidate_cache([candidate])
            else:
                wait_service.replace_candidate_cache(
                    [candidate],
                    expected_cache_epoch=expected_cache_epoch,
                )
            source_finished.set()
            return SimpleNamespace(
                symbols=('005930',),
                source_version=datetime.now(timezone.utc),
                membership_complete=True,
            )

        with (
            patch.object(
                settings,
                'signal_decider_wait_recheck_monitoring_enabled',
                True,
            ),
            patch.object(
                wait_recheck_module,
                'signal_decider_wait_recheck_service',
                wait_service,
            ),
            patch(
                'app.orders.exec_integration.'
                'signal_decider_exec_integration_service.'
                'load_realtime_monitored_symbol_source',
                AsyncMock(return_value=base_source),
            ),
            patch.object(
                wait_service,
                'load_subscription_source',
                side_effect=blocking_wait_source,
            ),
        ):
            refresh_task = asyncio.create_task(
                service.refresh_subscription_source_if_due(
                    trigger='shutdown-race',
                    force=True,
                )
            )
            self.assertTrue(
                await asyncio.to_thread(source_started.wait, 1)
            )
            await service.stop()
            allow_source_finish.set()
            self.assertTrue(
                await asyncio.to_thread(source_finished.wait, 1)
            )
            with self.assertRaises(asyncio.CancelledError):
                await refresh_task

        self.assertEqual(
            wait_service.runtime_snapshot()['candidate_count'],
            0,
        )

    async def test_non_authoritative_empty_source_preserves_current_symbols(self) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        service.observe_monitored_symbols(
            ['027710'],
            trigger='current-dashboard',
            preserve_order=True,
        )

        snapshot = service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                [],
                generated_at=datetime.now(timezone.utc),
                membership_complete=False,
            ),
            trigger='incomplete-dashboard',
        )

        self.assertEqual(snapshot['allowed_symbols'], ['027710'])
        self.assertEqual(
            service.health_snapshot()['metrics']['source_results_ignored'],
            1,
        )

    async def test_non_authoritative_source_preserves_current_capacity_order(
        self,
    ) -> None:
        settings.kis_realtime_price_max_subscriptions = 1
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        service.observe_monitored_symbols(
            ['027710'],
            trigger='current-dashboard',
            preserve_order=True,
        )

        snapshot = service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                ['005930'],
                generated_at=datetime.now(timezone.utc),
                membership_complete=False,
            ),
            trigger='incomplete-dashboard',
        )

        self.assertEqual(snapshot['allowed_symbols'], ['027710'])
        self.assertEqual(snapshot['limited_symbols'], ['005930'])

    async def test_non_authoritative_protected_source_preempts_regular_symbol(
        self,
    ) -> None:
        settings.kis_realtime_price_max_subscriptions = 1
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        service.observe_monitored_symbols(
            ['005930'],
            trigger='current-regular',
            preserve_order=True,
        )

        snapshot = service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                ['027710'],
                generated_at=datetime.now(timezone.utc),
                membership_complete=False,
                protected_symbols={'027710'},
            ),
            trigger='incomplete-protected',
        )

        self.assertEqual(snapshot['allowed_symbols'], ['027710'])
        self.assertEqual(snapshot['limited_symbols'], ['005930'])
        self.assertEqual(snapshot['protected_symbols'], ['027710'])

    async def test_incomplete_source_cannot_downgrade_current_protection(
        self,
    ) -> None:
        settings.kis_realtime_price_max_subscriptions = 1
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        now = datetime.now(timezone.utc)
        service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                ['027710'],
                generated_at=now,
                membership_complete=True,
                protected_symbols={'027710'},
            ),
            trigger='complete-protected',
        )

        snapshot = service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                ['005930', '027710'],
                generated_at=now + timedelta(seconds=1),
                membership_complete=False,
            ),
            trigger='incomplete-regular',
        )

        self.assertEqual(snapshot['allowed_symbols'], ['027710'])
        self.assertEqual(snapshot['limited_symbols'], ['005930'])
        self.assertEqual(snapshot['protected_symbols'], ['027710'])

    async def test_new_incomplete_protected_symbol_preempts_retained_protection(
        self,
    ) -> None:
        settings.kis_realtime_price_max_subscriptions = 1
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        now = datetime.now(timezone.utc)
        service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                ['000001'],
                generated_at=now,
                membership_complete=True,
                protected_symbols={'000001'},
            ),
            trigger='older-complete-protected',
        )

        snapshot = service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                ['000002'],
                generated_at=now + timedelta(seconds=1),
                membership_complete=False,
                protected_symbols={'000002'},
            ),
            trigger='newer-incomplete-protected',
        )

        self.assertEqual(snapshot['allowed_symbols'], ['000002'])
        self.assertEqual(snapshot['limited_symbols'], ['000001'])
        self.assertEqual(
            snapshot['protected_symbols'],
            ['000001', '000002'],
        )

    async def test_new_reconciling_symbol_cannot_preempt_retained_live_holding(
        self,
    ) -> None:
        settings.kis_realtime_price_max_subscriptions = 1
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        now = datetime.now(timezone.utc)
        service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                ['027710'],
                generated_at=now,
                membership_complete=True,
                protected_symbols={'027710'},
                critical_symbols={'027710'},
            ),
            trigger='complete-live-holding',
        )

        snapshot = service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                ['030000'],
                generated_at=now + timedelta(seconds=1),
                membership_complete=False,
                protected_symbols={'030000'},
            ),
            trigger='incomplete-reconciling',
        )

        self.assertEqual(snapshot['allowed_symbols'], ['027710'])
        self.assertEqual(snapshot['limited_symbols'], ['030000'])
        self.assertEqual(snapshot['critical_protected_symbols'], ['027710'])

    async def test_complete_source_can_demote_stale_protection(self) -> None:
        settings.kis_realtime_price_max_subscriptions = 1
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        now = datetime.now(timezone.utc)
        service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                ['027710'],
                generated_at=now,
                membership_complete=True,
                protected_symbols={'027710'},
            ),
            trigger='complete-protected',
        )

        snapshot = service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                ['005930', '027710'],
                generated_at=now + timedelta(seconds=1),
                membership_complete=True,
            ),
            trigger='complete-demotion',
        )

        self.assertEqual(snapshot['allowed_symbols'], ['005930'])
        self.assertEqual(snapshot['limited_symbols'], ['027710'])
        self.assertEqual(snapshot['protected_symbols'], [])

    async def test_complete_source_prioritizes_protected_symbol(self) -> None:
        settings.kis_realtime_price_max_subscriptions = 1
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())

        snapshot = service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                ['005930', '030000'],
                generated_at=datetime.now(timezone.utc),
                membership_complete=True,
                protected_symbols={'030000'},
            ),
            trigger='complete-protected-order',
        )

        self.assertEqual(snapshot['allowed_symbols'], ['030000'])
        self.assertEqual(snapshot['limited_symbols'], ['005930'])
        self.assertEqual(snapshot['protected_symbols'], ['030000'])

    async def test_planned_exit_intents_alone_do_not_take_capacity(self) -> None:
        settings.kis_realtime_price_max_subscriptions = 1
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        payload = _runtime_source_payload(
            ['005930', '030000'],
            generated_at=datetime.now(timezone.utc),
            membership_complete=True,
        )
        payload['items'][0]['monitoring_state'] = 'watching_fill'
        payload['items'][1].update(
            {
                'monitoring_state': 'idle',
                'planned_intents': ['target_sell', 'stop_sell'],
            }
        )

        snapshot = service.observe_runtime_monitored_payload(
            payload,
            trigger='planned-intents-only',
        )

        self.assertEqual(snapshot['allowed_symbols'], ['005930'])
        self.assertEqual(snapshot['limited_symbols'], ['030000'])
        self.assertEqual(snapshot['protected_symbols'], [])

    async def test_other_market_date_cannot_replace_current_symbols(self) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        service.observe_monitored_symbols(
            ['027710'],
            trigger='current-dashboard',
            preserve_order=True,
        )
        payload = _runtime_source_payload(
            [],
            generated_at=datetime.now(timezone.utc),
            membership_complete=True,
        )
        payload['market_date'] = '2000-01-01'

        snapshot = service.observe_runtime_monitored_payload(
            payload,
            trigger='historical-dashboard',
        )

        self.assertEqual(snapshot['allowed_symbols'], ['027710'])
        self.assertEqual(
            service.health_snapshot()['metrics']['source_results_ignored'],
            1,
        )

    async def test_unversioned_or_unscoped_payload_cannot_clear_symbols(self) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        service.observe_monitored_symbols(
            ['027710'],
            trigger='current-dashboard',
            preserve_order=True,
        )
        generated_at = datetime.now(timezone.utc)
        missing_market_date = _runtime_source_payload(
            [],
            generated_at=generated_at,
            membership_complete=True,
        )
        missing_market_date.pop('market_date')
        missing_source_version = _runtime_source_payload(
            [],
            generated_at=generated_at,
            membership_complete=True,
        )
        missing_source_version['freshness'].pop('generated_at')

        for trigger, payload in (
            ('missing-market-date', missing_market_date),
            ('missing-source-version', missing_source_version),
        ):
            snapshot = service.observe_runtime_monitored_payload(
                payload,
                trigger=trigger,
            )
            self.assertEqual(snapshot['allowed_symbols'], ['027710'])

    async def test_non_krx_symbol_revokes_complete_membership_authority(self) -> None:
        payload = _runtime_source_payload(
            ['005930', 'AAPL'],
            generated_at=datetime.now(timezone.utc),
            membership_complete=True,
        )

        source = KISRealtimePriceService._runtime_monitored_source_from_payload(
            payload
        )

        self.assertEqual(source.symbols, ('005930',))
        self.assertFalse(source.membership_complete)

    async def test_unicode_digit_symbol_revokes_complete_membership_authority(
        self,
    ) -> None:
        payload = _runtime_source_payload(
            ['005930', '００５９３０'],
            generated_at=datetime.now(timezone.utc),
            membership_complete=True,
        )

        source = KISRealtimePriceService._runtime_monitored_source_from_payload(
            payload
        )

        self.assertEqual(source.symbols, ('005930',))
        self.assertFalse(source.membership_complete)

    async def test_subscription_summary_mismatch_revokes_complete_membership_authority(
        self,
    ) -> None:
        payload = _runtime_source_payload(
            ['005930'],
            generated_at=datetime.now(timezone.utc),
            membership_complete=True,
        )
        payload['realtime_subscription'] = {
            'desired_symbols': [],
            'desired_symbol_count': 0,
        }

        source = KISRealtimePriceService._runtime_monitored_source_from_payload(
            payload
        )

        self.assertEqual(source.symbols, ('005930',))
        self.assertFalse(source.membership_complete)

    async def test_missing_explicit_freshness_flag_revokes_complete_authority(
        self,
    ) -> None:
        payload = _runtime_source_payload(
            ['005930'],
            generated_at=datetime.now(timezone.utc),
            membership_complete=True,
        )
        payload['freshness'].pop('is_partial')

        source = KISRealtimePriceService._runtime_monitored_source_from_payload(
            payload
        )

        self.assertFalse(source.membership_complete)

    async def test_malformed_item_revokes_complete_membership_authority(self) -> None:
        payload = _runtime_source_payload(
            ['005930'],
            generated_at=datetime.now(timezone.utc),
            membership_complete=True,
        )
        payload['items'].append('not-an-item')

        source = KISRealtimePriceService._runtime_monitored_source_from_payload(
            payload
        )

        self.assertEqual(source.symbols, ('005930',))
        self.assertFalse(source.membership_complete)

    async def test_authoritative_fresh_empty_source_clears_current_symbols(self) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        service.observe_monitored_symbols(
            ['027710'],
            trigger='current-dashboard',
            preserve_order=True,
        )

        snapshot = service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                [],
                generated_at=datetime.now(timezone.utc),
                membership_complete=True,
            ),
            trigger='complete-dashboard',
        )

        self.assertEqual(snapshot['allowed_symbols'], [])

    async def test_older_source_version_cannot_replace_newer_membership(self) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        newer_version = datetime.now(timezone.utc)
        newer = service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                ['027710'],
                generated_at=newer_version,
                membership_complete=True,
            ),
            trigger='newer-dashboard',
        )
        observed_at = service.health_snapshot()['last_observed_symbols_at']

        older = service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                [],
                generated_at=newer_version - timedelta(minutes=1),
                membership_complete=True,
            ),
            trigger='older-cache',
        )

        self.assertEqual(newer['allowed_symbols'], ['027710'])
        self.assertEqual(older['allowed_symbols'], ['027710'])
        self.assertEqual(
            service.health_snapshot()['last_observed_symbols_at'],
            observed_at,
        )

    async def test_newer_incomplete_empty_blocks_older_complete_empty(self) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        service.observe_monitored_symbols(
            ['027710'],
            trigger='current-dashboard',
            preserve_order=True,
        )
        initial_revision = service.observation_revision()
        newer_version = datetime.now(timezone.utc)

        newer = service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                [],
                generated_at=newer_version,
                membership_complete=False,
            ),
            trigger='newer-incomplete',
            expected_observation_revision=initial_revision,
        )
        older = service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                [],
                generated_at=newer_version - timedelta(minutes=1),
                membership_complete=True,
            ),
            trigger='older-complete',
            expected_observation_revision=initial_revision,
        )

        self.assertEqual(newer['allowed_symbols'], ['027710'])
        self.assertEqual(older['allowed_symbols'], ['027710'])
        self.assertGreater(service.observation_revision(), initial_revision)
        self.assertEqual(
            service.health_snapshot()['last_source_version'],
            newer_version.isoformat(),
        )

    async def test_future_source_cannot_replace_or_poison_current_membership(
        self,
    ) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        now = datetime.now(timezone.utc)
        service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                ['027710'],
                generated_at=now,
                membership_complete=True,
            ),
            trigger='current',
        )

        future = service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                ['005930'],
                generated_at=now + timedelta(hours=2),
                membership_complete=True,
            ),
            trigger='future',
        )
        next_current = service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                ['000660'],
                generated_at=now + timedelta(seconds=1),
                membership_complete=True,
            ),
            trigger='next-current',
        )

        self.assertEqual(future['allowed_symbols'], ['027710'])
        self.assertEqual(next_current['allowed_symbols'], ['000660'])
        self.assertEqual(
            service.health_snapshot()['last_source_version'],
            (now + timedelta(seconds=1)).isoformat(),
        )

    async def test_equal_version_conflict_is_merge_only(self) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        now = datetime.now(timezone.utc)
        service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                ['005930'],
                generated_at=now,
                membership_complete=True,
            ),
            trigger='first',
        )

        conflict = service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                ['000660'],
                generated_at=now,
                membership_complete=True,
            ),
            trigger='conflict',
        )

        self.assertEqual(
            set(conflict['desired_symbols']),
            {'000660', '005930'},
        )

    async def test_identical_cached_observation_does_not_cancel_newer_refresh(
        self,
    ) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        now = datetime.now(timezone.utc)
        initial_payload = _runtime_source_payload(
            ['005930'],
            generated_at=now,
            membership_complete=True,
        )
        service.observe_runtime_monitored_payload(
            initial_payload,
            trigger='initial',
        )
        initial_revision = service.observation_revision()

        service.observe_runtime_monitored_payload(
            initial_payload,
            trigger='same-cached-payload',
            expected_observation_revision=initial_revision,
        )
        refreshed = service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                ['027710'],
                generated_at=now + timedelta(seconds=1),
                membership_complete=True,
            ),
            trigger='newer-background-result',
            expected_observation_revision=initial_revision,
        )

        self.assertEqual(service.observation_revision(), initial_revision + 1)
        self.assertEqual(refreshed['allowed_symbols'], ['027710'])

    async def test_newer_timestamp_cannot_bypass_stale_revision_fence(
        self,
    ) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        now = datetime.now(timezone.utc)
        service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                ['005930'],
                generated_at=now,
                membership_complete=True,
            ),
            trigger='base',
        )
        stale_revision = service.observation_revision()
        service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                ['027710'],
                generated_at=now + timedelta(seconds=1),
                membership_complete=True,
            ),
            trigger='direct-newer-observation',
        )

        stale_task = service.observe_runtime_monitored_payload(
            _runtime_source_payload(
                ['000660'],
                generated_at=now + timedelta(seconds=2),
                membership_complete=True,
            ),
            trigger='slow-stale-task',
            expected_observation_revision=stale_revision,
        )

        self.assertEqual(stale_task['allowed_symbols'], ['027710'])

    async def test_runtime_source_loader_uses_nonblocking_integration_source(
        self,
    ) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        _record_open_market_session(service)
        now = datetime.now(timezone.utc)
        source = KISRealtimeMonitoredSymbolSource(
            symbols=('027710',),
            market_date=now.astimezone(
                kis_realtime_prices.KST
            ).date().isoformat(),
            source_version=now,
            membership_complete=True,
            protected_symbols=('027710',),
        )
        load_source = AsyncMock(return_value=source)
        blocking_dashboard_source = AsyncMock(
            side_effect=AssertionError(
                'realtime source must not enter the dashboard builder'
            )
        )

        with (
            patch.object(
                settings,
                'signal_decider_wait_recheck_monitoring_enabled',
                False,
            ),
            patch(
                'app.orders.exec_integration.'
                'signal_decider_exec_integration_service.'
                'load_realtime_monitored_symbol_source',
                load_source,
            ),
            patch(
                'app.orders.exec_integration.'
                'signal_decider_exec_integration_service.'
                'list_runtime_monitored_symbols',
                blocking_dashboard_source,
            ),
        ):
            result = await service._load_runtime_monitored_symbols(
                trigger='background'
            )

        self.assertIs(result, source)
        load_source.assert_awaited_once_with(
            trigger='kis_realtime_price:background',
        )
        blocking_dashboard_source.assert_not_awaited()

    async def test_background_loop_does_not_force_subscription_source_refresh(self) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())

        async def _stop_after_refresh(**_: object) -> dict:
            service._stop_requested = True
            return {}

        refresh = AsyncMock(side_effect=_stop_after_refresh)
        wait_for_change = AsyncMock()
        with (
            patch.object(
                kis_realtime_prices,
                'async_get_market_state',
                AsyncMock(return_value=_market_state('OPEN')),
            ),
            patch.object(service, 'refresh_subscription_source_if_due', refresh),
            patch.object(service, '_wait_for_change_or_timeout', wait_for_change),
        ):
            await service._run()

        self.assertFalse(refresh.await_args.kwargs['force'])
        wait_for_change.assert_awaited_once()

    async def test_failed_subscription_ack_clears_subscribed_symbol_for_retry(self) -> None:
        service = KISRealtimePriceService(client=FakeKISRealtimePriceClient())
        websocket = FakeWebSocket()
        _record_open_market_session(service)

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
