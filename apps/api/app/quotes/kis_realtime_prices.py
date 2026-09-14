from __future__ import annotations

import asyncio
import copy
import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, time as datetime_time, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import websockets

from app.brokers.kis.client import (
    KISAPIError,
    KISClient,
    KISClientError,
    KISConfigurationError,
    KISHTTPError,
)
from app.brokers.kis.provider import kis_client
from app.core.settings import settings

logger = logging.getLogger(__name__)

KST = ZoneInfo('Asia/Seoul')
KIS_REALTIME_PRICE_TR_ID = 'H0STCNT0'
KIS_REALTIME_PRICE_SOURCE = 'kis_realtime_ws'
KIS_REALTIME_PRICE_SOURCE_REFRESH_TIMEOUT_SECONDS = 10.0
KIS_REALTIME_PRICE_COLUMNS = (
    'MKSC_SHRN_ISCD',
    'STCK_CNTG_HOUR',
    'STCK_PRPR',
    'PRDY_VRSS_SIGN',
    'PRDY_VRSS',
    'PRDY_CTRT',
    'WGHN_AVRG_STCK_PRC',
    'STCK_OPRC',
    'STCK_HGPR',
    'STCK_LWPR',
    'ASKP1',
    'BIDP1',
    'CNTG_VOL',
    'ACML_VOL',
    'ACML_TR_PBMN',
    'SELN_CNTG_CSNU',
    'SHNU_CNTG_CSNU',
    'NTBY_CNTG_CSNU',
    'CTTR',
    'SELN_CNTG_SMTN',
    'SHNU_CNTG_SMTN',
    'CCLD_DVSN',
    'SHNU_RATE',
    'PRDY_VOL_VRSS_ACML_VOL_RATE',
    'OPRC_HOUR',
    'OPRC_VRSS_PRPR_SIGN',
    'OPRC_VRSS_PRPR',
    'HGPR_HOUR',
    'HGPR_VRSS_PRPR_SIGN',
    'HGPR_VRSS_PRPR',
    'LWPR_HOUR',
    'LWPR_VRSS_PRPR_SIGN',
    'LWPR_VRSS_PRPR',
    'BSOP_DATE',
    'NEW_MKOP_CLS_CODE',
    'TRHT_YN',
    'ASKP_RSQN1',
    'BIDP_RSQN1',
    'TOTAL_ASKP_RSQN',
    'TOTAL_BIDP_RSQN',
    'VOL_TNRT',
    'PRDY_SMNS_HOUR_ACML_VOL',
    'PRDY_SMNS_HOUR_ACML_VOL_RATE',
    'HOUR_CLS_CODE',
    'MRKT_TRTM_CLS_CODE',
    'VI_STND_PRC',
)


class KISRealtimePriceSubscriptionError(RuntimeError):
    pass


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _normalize_symbol(value: Any) -> str | None:
    normalized = ''.join(ch for ch in str(value or '').strip().upper() if ch.isalnum())
    return normalized or None


def _normalize_text(value: Any) -> str:
    return str(value or '').strip()


def _parse_decimal(value: Any) -> Decimal | None:
    normalized = _normalize_text(value).replace(',', '')
    if not normalized:
        return None
    try:
        return Decimal(normalized)
    except (InvalidOperation, ValueError):
        return None


def _to_json_number(value: Decimal | None) -> int | float | None:
    if value is None:
        return None
    if value == value.to_integral():
        return int(value)
    return float(value)


def _parse_positive_int(value: Any, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(0, parsed)


def _parse_data_count(value: Any, *, default: int) -> int:
    try:
        parsed = int(str(value or '').strip())
    except ValueError:
        return default
    return max(0, parsed)


def _parse_kis_timestamp(*, business_date: Any, hhmmss: Any, received_at: datetime) -> datetime:
    date_digits = ''.join(ch for ch in _normalize_text(business_date) if ch.isdigit())
    time_digits = ''.join(ch for ch in _normalize_text(hhmmss) if ch.isdigit())
    if len(time_digits) < 6:
        return received_at
    try:
        local_date = (
            datetime.strptime(date_digits[:8], '%Y%m%d').date()
            if len(date_digits) >= 8
            else received_at.astimezone(KST).date()
        )
        local_time = datetime_time(
            hour=int(time_digits[:2]),
            minute=int(time_digits[2:4]),
            second=int(time_digits[4:6]),
        )
        return datetime.combine(local_date, local_time, tzinfo=KST).astimezone(timezone.utc)
    except ValueError:
        return received_at


@dataclass(frozen=True)
class KISRealtimePriceTick:
    symbol: str
    current_price: Decimal | None
    price_as_of: datetime
    received_at: datetime
    change: Decimal | None
    change_pct: Decimal | None
    volume: Decimal | None
    turnover: Decimal | None
    bid_price: Decimal | None
    ask_price: Decimal | None
    trade_halt: bool | None
    raw_fields: dict[str, str]

    def to_public_dict(self, *, price_status: str = 'available') -> dict[str, Any]:
        return {
            'symbol': self.symbol,
            'current_price': _to_json_number(self.current_price),
            'price_as_of': _isoformat(self.price_as_of),
            'price_received_at': _isoformat(self.received_at),
            'price_source': KIS_REALTIME_PRICE_SOURCE,
            'price_status': price_status,
            'change': _to_json_number(self.change),
            'change_pct': _to_json_number(self.change_pct),
            'volume': _to_json_number(self.volume),
            'turnover': _to_json_number(self.turnover),
            'bid_price': _to_json_number(self.bid_price),
            'ask_price': _to_json_number(self.ask_price),
            'trade_halt': self.trade_halt,
        }


def parse_realtime_price_fields(
    fields: dict[str, Any],
    *,
    received_at: datetime | None = None,
) -> KISRealtimePriceTick | None:
    received_at = received_at or _utc_now()
    symbol = _normalize_symbol(fields.get('MKSC_SHRN_ISCD'))
    if not symbol:
        return None
    price_as_of = _parse_kis_timestamp(
        business_date=fields.get('BSOP_DATE'),
        hhmmss=fields.get('STCK_CNTG_HOUR'),
        received_at=received_at,
    )
    trade_halt_text = _normalize_text(fields.get('TRHT_YN')).upper()
    trade_halt = None
    if trade_halt_text:
        trade_halt = trade_halt_text in {'Y', '1', 'TRUE'}
    return KISRealtimePriceTick(
        symbol=symbol,
        current_price=_parse_decimal(fields.get('STCK_PRPR')),
        price_as_of=price_as_of,
        received_at=received_at,
        change=_parse_decimal(fields.get('PRDY_VRSS')),
        change_pct=_parse_decimal(fields.get('PRDY_CTRT')),
        volume=_parse_decimal(fields.get('ACML_VOL')),
        turnover=_parse_decimal(fields.get('ACML_TR_PBMN')),
        bid_price=_parse_decimal(fields.get('BIDP1')),
        ask_price=_parse_decimal(fields.get('ASKP1')),
        trade_halt=trade_halt,
        raw_fields={column: _normalize_text(fields.get(column)) for column in KIS_REALTIME_PRICE_COLUMNS},
    )


def parse_realtime_price_message(
    message: str,
    *,
    expected_tr_id: str = KIS_REALTIME_PRICE_TR_ID,
    received_at: datetime | None = None,
) -> list[KISRealtimePriceTick]:
    received_at = received_at or _utc_now()
    chunks = message.split('|', 3)
    if len(chunks) < 4:
        return []
    encryption_flag, tr_id, data_count_text, payload = chunks
    if tr_id != expected_tr_id or encryption_flag != '0':
        return []

    values = payload.split('^')
    column_count = len(KIS_REALTIME_PRICE_COLUMNS)
    inferred_count = len(values) // column_count
    data_count = _parse_data_count(data_count_text, default=inferred_count)
    ticks: list[KISRealtimePriceTick] = []
    for index in range(min(data_count, inferred_count)):
        start = index * column_count
        row_values = values[start:start + column_count]
        fields = {
            column: row_values[column_index] if column_index < len(row_values) else ''
            for column_index, column in enumerate(KIS_REALTIME_PRICE_COLUMNS)
        }
        tick = parse_realtime_price_fields(fields, received_at=received_at)
        if tick is not None:
            ticks.append(tick)
    return ticks


class KISRealtimePriceService:
    def __init__(self, *, client: KISClient | None = None) -> None:
        self.client = client or kis_client
        self._task: asyncio.Task[None] | None = None
        self._websocket: Any = None
        self._stop_requested = False
        self._subscription_changed_event: asyncio.Event | None = None
        self._source_refresh_lock: asyncio.Lock | None = None
        self._source_refresh_task: asyncio.Task[list[str]] | None = None
        self._last_source_refresh_monotonic = 0.0
        self._state_lock = threading.RLock()
        self._prices: dict[str, KISRealtimePriceTick] = {}
        self._desired_symbols: set[str] = set()
        self._desired_symbol_order: list[str] = []
        self._subscribed_symbols: set[str] = set()
        self._limited_symbols: set[str] = set()
        self._last_observed_symbols_at: datetime | None = None
        self._health: dict[str, Any] = self._build_health_state()

    def _build_health_state(self) -> dict[str, Any]:
        return {
            'enabled': settings.kis_realtime_price_enabled,
            'configured': False,
            'status': 'idle',
            'environment': self.client._environment(),
            'tr_id': self._price_tr_id(),
            'ws_url': self.client.websocket_url(),
            'max_subscriptions': settings.kis_realtime_price_max_subscriptions,
            'desired_symbols': [],
            'subscribed_symbols': [],
            'limited_symbols': [],
            'last_observed_symbols_at': None,
            'last_subscription_refresh_at': None,
            'last_connected_at': None,
            'last_disconnected_at': None,
            'last_message_received_at': None,
            'last_price_received_at': None,
            'last_error': None,
            'reconnect_attempts': 0,
            'consecutive_failures': 0,
            'approval': {
                'status': 'idle',
                'updated_at': None,
                'message': None,
            },
            'subscription': {
                'status': 'idle',
                'updated_at': None,
                'message': None,
            },
            'metrics': {
                'messages_received': 0,
                'system_messages': 0,
                'price_messages_received': 0,
                'prices_ingested': 0,
                'parse_errors': 0,
                'subscription_refreshes': 0,
                'subscribe_requests': 0,
                'unsubscribe_requests': 0,
            },
        }

    def _price_tr_id(self) -> str:
        return _normalize_text(settings.kis_realtime_price_tr_id) or KIS_REALTIME_PRICE_TR_ID

    def _is_configured(self) -> tuple[bool, str | None]:
        if not settings.kis_realtime_price_enabled:
            return False, 'disabled_by_config'
        if not settings.kis_app_key or not settings.kis_app_secret:
            return False, 'missing_kis_credentials'
        return True, None

    def _set_status(self, status: str) -> None:
        configured, reason = self._is_configured()
        with self._state_lock:
            self._health['enabled'] = settings.kis_realtime_price_enabled
            self._health['configured'] = configured
            self._health['status'] = status
            self._health['environment'] = self.client._environment()
            self._health['tr_id'] = self._price_tr_id()
            self._health['ws_url'] = self.client.websocket_url()
            self._health['max_subscriptions'] = settings.kis_realtime_price_max_subscriptions
            self._sync_health_symbols_locked()
            if not configured and reason:
                self._health['last_error'] = {
                    'stage': 'configuration',
                    'fatal': True,
                    'retryable': False,
                    'message': reason,
                    'occurred_at': _isoformat(_utc_now()),
                }

    def _sync_health_symbols_locked(self) -> None:
        ordered_symbols = self._ordered_desired_symbols_locked()
        self._health['desired_symbols'] = ordered_symbols
        self._health['subscribed_symbols'] = sorted(self._subscribed_symbols)
        self._health['limited_symbols'] = sorted(self._limited_symbols)
        self._health['last_observed_symbols_at'] = _isoformat(self._last_observed_symbols_at)

    def _record_error(self, *, stage: str, message: str, fatal: bool, retryable: bool) -> None:
        now = _utc_now()
        with self._state_lock:
            self._health['last_error'] = {
                'stage': stage,
                'fatal': fatal,
                'retryable': retryable,
                'message': message,
                'occurred_at': _isoformat(now),
            }
            self._health['consecutive_failures'] = int(self._health['consecutive_failures']) + 1
            self._health['status'] = 'fatal' if fatal else 'reconnecting'

    def _mark_connected(self) -> None:
        with self._state_lock:
            self._health['status'] = 'connected'
            self._health['last_connected_at'] = _isoformat(_utc_now())
            self._health['consecutive_failures'] = 0
            self._subscribed_symbols = set()
            self._sync_health_symbols_locked()

    def _mark_disconnected(self) -> None:
        with self._state_lock:
            self._health['last_disconnected_at'] = _isoformat(_utc_now())
            self._subscribed_symbols = set()
            self._sync_health_symbols_locked()

    def _increment_reconnect_attempt(self) -> None:
        with self._state_lock:
            self._health['reconnect_attempts'] = int(self._health['reconnect_attempts']) + 1

    async def start(self) -> None:
        self._set_status('starting')
        configured, reason = self._is_configured()
        if not settings.kis_realtime_price_enabled:
            self._set_status('disabled')
            return
        if not configured:
            logger.info('KIS realtime prices not started: %s', reason)
            self._set_status('config_missing')
            return
        if self._task is not None and not self._task.done():
            return
        self._stop_requested = False
        self._subscription_changed_event = asyncio.Event()
        self._source_refresh_lock = asyncio.Lock()
        self._task = asyncio.create_task(
            self._run(),
            name='kis-realtime-prices',
        )

    async def stop(self) -> None:
        self._stop_requested = True
        task = self._task
        source_refresh_task = self._source_refresh_task
        websocket = self._websocket
        self._task = None
        self._source_refresh_task = None
        self._websocket = None

        if self._subscription_changed_event is not None:
            self._subscription_changed_event.set()

        if websocket is not None:
            try:
                await websocket.close()
            except Exception:
                logger.debug('Ignoring realtime price websocket close failure during shutdown', exc_info=True)

        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        if source_refresh_task is not None and not source_refresh_task.done():
            source_refresh_task.cancel()
            try:
                await source_refresh_task
            except asyncio.CancelledError:
                pass

        self._set_status('stopped')

    def observe_monitored_symbols(
        self,
        symbols: Iterable[Any],
        *,
        trigger: str,
        preserve_order: bool = False,
    ) -> dict[str, Any]:
        if preserve_order:
            seen: set[str] = set()
            normalized_symbols = []
            for value in symbols:
                symbol = _normalize_symbol(value)
                if symbol and symbol not in seen:
                    seen.add(symbol)
                    normalized_symbols.append(symbol)
        else:
            normalized_symbols = sorted(
                {
                    symbol
                    for value in symbols
                    if (symbol := _normalize_symbol(value))
                }
            )
        with self._state_lock:
            before = set(self._desired_symbols)
            before_order = list(self._desired_symbol_order)
            self._desired_symbols = set(normalized_symbols)
            self._desired_symbol_order = list(normalized_symbols)
            self._last_observed_symbols_at = _utc_now()
            self._target_symbols_snapshot_locked()
            changed = before != self._desired_symbols or before_order != self._desired_symbol_order
            if changed:
                self._health['subscription']['message'] = f'symbols_observed:{trigger}'
                self._health['subscription']['updated_at'] = _isoformat(self._last_observed_symbols_at)
        if changed and self._subscription_changed_event is not None:
            self._subscription_changed_event.set()
        return self.subscription_snapshot()

    async def refresh_subscription_source_if_due(self, *, trigger: str, force: bool = False) -> dict[str, Any]:
        interval = max(1.0, float(settings.kis_realtime_price_subscription_refresh_seconds))
        now_monotonic = time.monotonic()
        existing_source_task = self._source_refresh_task
        if (
            not force
            and (now_monotonic - self._last_source_refresh_monotonic) < interval
            and (
                existing_source_task is None
                or not existing_source_task.done()
            )
        ):
            return self.subscription_snapshot()
        if self._source_refresh_lock is None:
            self._source_refresh_lock = asyncio.Lock()
        async with self._source_refresh_lock:
            now_monotonic = time.monotonic()
            source_task = self._source_refresh_task
            if (
                source_task is None
                and not force
                and (now_monotonic - self._last_source_refresh_monotonic) < interval
            ):
                return self.subscription_snapshot()
            if source_task is None:
                self._last_source_refresh_monotonic = now_monotonic
                source_task = asyncio.create_task(
                    self._load_runtime_monitored_symbols(trigger=trigger),
                    name='kis-realtime-price-symbol-source',
                )
                self._source_refresh_task = source_task
            try:
                symbols = await asyncio.wait_for(
                    asyncio.shield(source_task),
                    timeout=KIS_REALTIME_PRICE_SOURCE_REFRESH_TIMEOUT_SECONDS,
                )
            except TimeoutError:
                message = (
                    'monitored-symbol refresh timed out after '
                    f'{KIS_REALTIME_PRICE_SOURCE_REFRESH_TIMEOUT_SECONDS:g}s'
                )
                self._record_error(
                    stage='subscription_source',
                    message=message,
                    fatal=False,
                    retryable=True,
                )
                logger.warning('KIS realtime price %s', message)
            except Exception as exc:
                if self._source_refresh_task is source_task:
                    self._source_refresh_task = None
                self._record_error(
                    stage='subscription_source',
                    message=str(exc),
                    fatal=False,
                    retryable=True,
                )
                logger.warning('KIS realtime price monitored-symbol refresh failed: %s', exc)
            else:
                if self._source_refresh_task is source_task:
                    self._source_refresh_task = None
                with self._state_lock:
                    self._health['metrics']['subscription_refreshes'] += 1
                    self._health['last_subscription_refresh_at'] = _isoformat(_utc_now())
                self.observe_monitored_symbols(symbols, trigger=trigger, preserve_order=True)
        return self.subscription_snapshot()

    async def _load_runtime_monitored_symbols(self, *, trigger: str) -> list[str]:
        from app.orders.exec_integration import signal_decider_exec_integration_service

        payload = await signal_decider_exec_integration_service.list_runtime_monitored_symbols(
            refresh=False,
            trigger=f'kis_realtime_price:{trigger}',
        )
        return [
            str(item.get('symbol'))
            for item in payload.get('items', [])
            if isinstance(item, dict) and item.get('symbol')
        ]

    async def _run(self) -> None:
        reconnect_delay = max(0.5, float(settings.kis_realtime_reconnect_base_delay_seconds))
        max_reconnect_delay = max(reconnect_delay, float(settings.kis_realtime_reconnect_max_delay_seconds))

        while not self._stop_requested:
            await self.refresh_subscription_source_if_due(trigger='background', force=False)
            allowed_symbols, _ = self._target_symbols_snapshot()
            if not allowed_symbols:
                self._set_status('idle_no_symbols')
                await self._wait_for_change_or_timeout(settings.kis_realtime_price_idle_poll_seconds)
                continue

            stage = 'approval'
            try:
                approval = await self.client.issue_approval_key()
                with self._state_lock:
                    self._health['approval'] = {
                        'status': 'succeeded',
                        'updated_at': _isoformat(_utc_now()),
                        'message': 'approval_key_issued',
                    }

                stage = 'connect'
                async with websockets.connect(
                    self.client.websocket_url(),
                    ping_interval=None,
                ) as websocket:
                    self._websocket = websocket
                    self._mark_connected()
                    await self._sync_subscriptions(websocket, approval_key=approval['approval_key'])

                    while not self._stop_requested:
                        stage = 'receive'
                        await self._receive_or_refresh(websocket, approval_key=approval['approval_key'])
                        allowed_symbols, _ = self._target_symbols_snapshot()
                        if not allowed_symbols:
                            break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                fatal = self._is_fatal_error(exc)
                retryable = self._is_retryable_error(exc)
                self._record_error(
                    stage='subscription' if isinstance(exc, KISRealtimePriceSubscriptionError) else stage,
                    message=str(exc),
                    fatal=fatal,
                    retryable=retryable,
                )
                self._mark_disconnected()
                self._websocket = None
                if fatal or self._stop_requested:
                    break
                self._increment_reconnect_attempt()
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
            else:
                reconnect_delay = max(0.5, float(settings.kis_realtime_reconnect_base_delay_seconds))
                self._mark_disconnected()
                self._websocket = None

    async def _receive_or_refresh(self, websocket: Any, *, approval_key: str) -> None:
        interval = max(1.0, float(settings.kis_realtime_price_subscription_refresh_seconds))
        event = self._subscription_changed_event
        wait_timeout = min(1.0, interval)
        try:
            message = await asyncio.wait_for(websocket.recv(), timeout=wait_timeout)
        except TimeoutError:
            message = None

        if message is not None:
            await self._handle_message(websocket, message)

        if event is not None and event.is_set():
            event.clear()
            await self._sync_subscriptions(websocket, approval_key=approval_key)
            return
        await self.refresh_subscription_source_if_due(trigger='background', force=False)
        await self._sync_subscriptions(websocket, approval_key=approval_key)

    async def _wait_for_change_or_timeout(self, timeout_seconds: float) -> None:
        event = self._subscription_changed_event
        timeout = max(1.0, float(timeout_seconds))
        if event is None:
            await asyncio.sleep(timeout)
            return
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except TimeoutError:
            return
        finally:
            event.clear()

    def _is_fatal_error(self, exc: Exception) -> bool:
        if isinstance(exc, (KISConfigurationError, KISAPIError)):
            return True
        if isinstance(exc, KISHTTPError) and exc.status_code is not None and 400 <= exc.status_code < 500:
            return True
        return False

    def _is_retryable_error(self, exc: Exception) -> bool:
        if isinstance(exc, KISClientError):
            return exc.retryable
        return not self._is_fatal_error(exc)

    def _ordered_desired_symbols_locked(self) -> list[str]:
        ordered = [
            symbol
            for symbol in self._desired_symbol_order
            if symbol in self._desired_symbols
        ]
        if len(ordered) < len(self._desired_symbols):
            ordered.extend(sorted(self._desired_symbols - set(ordered)))
        return ordered

    def _target_symbols_snapshot_locked(self) -> tuple[list[str], list[str]]:
        max_subscriptions = _parse_positive_int(
            settings.kis_realtime_price_max_subscriptions,
            default=40,
        )
        desired = self._ordered_desired_symbols_locked()
        allowed = desired[:max_subscriptions]
        limited = desired[max_subscriptions:]
        self._limited_symbols = set(limited)
        self._sync_health_symbols_locked()
        return allowed, limited

    def _target_symbols_snapshot(self) -> tuple[list[str], list[str]]:
        with self._state_lock:
            allowed, limited = self._target_symbols_snapshot_locked()
        return allowed, limited

    async def _sync_subscriptions(self, websocket: Any, *, approval_key: str) -> None:
        allowed_symbols, limited_symbols = self._target_symbols_snapshot()
        with self._state_lock:
            subscribed = set(self._subscribed_symbols)

        unsubscribe_symbols = sorted(subscribed - set(allowed_symbols))
        subscribe_symbols = sorted(set(allowed_symbols) - subscribed)

        for symbol in unsubscribe_symbols:
            await self._send_subscription(
                websocket,
                approval_key=approval_key,
                symbol=symbol,
                tr_type=settings.kis_realtime_price_unsubscribe_tr_type,
            )
            with self._state_lock:
                self._subscribed_symbols.discard(symbol)
                self._health['metrics']['unsubscribe_requests'] += 1

        for symbol in subscribe_symbols:
            await self._send_subscription(
                websocket,
                approval_key=approval_key,
                symbol=symbol,
                tr_type='1',
            )
            with self._state_lock:
                self._subscribed_symbols.add(symbol)
                self._health['metrics']['subscribe_requests'] += 1

        with self._state_lock:
            if allowed_symbols:
                self._health['status'] = 'limited' if limited_symbols else 'subscribed'
                self._health['subscription'] = {
                    'status': 'limited' if limited_symbols else 'succeeded',
                    'updated_at': _isoformat(_utc_now()),
                    'message': (
                        f'{len(limited_symbols)} symbol(s) limited by configured max'
                        if limited_symbols
                        else 'subscriptions_current'
                    ),
                }
            else:
                self._health['status'] = 'idle_no_symbols'
                self._health['subscription'] = {
                    'status': 'idle',
                    'updated_at': _isoformat(_utc_now()),
                    'message': 'no_monitored_symbols',
                }
            self._sync_health_symbols_locked()

    async def _send_subscription(
        self,
        websocket: Any,
        *,
        approval_key: str,
        symbol: str,
        tr_type: str,
    ) -> None:
        message = {
            'header': {
                'approval_key': approval_key,
                'custtype': 'P',
                'tr_type': str(tr_type),
                'content-type': 'utf-8',
            },
            'body': {
                'input': {
                    'tr_id': self._price_tr_id(),
                    'tr_key': symbol,
                }
            },
        }
        await websocket.send(json.dumps(message))
        pause_seconds = max(0.0, float(settings.kis_realtime_price_subscribe_pause_seconds))
        if pause_seconds:
            await asyncio.sleep(pause_seconds)

    async def _handle_message(self, websocket: Any, message: Any) -> None:
        received_at = _utc_now()
        text_message = message if isinstance(message, str) else message.decode('utf-8', errors='ignore')
        with self._state_lock:
            self._health['metrics']['messages_received'] += 1
            self._health['last_message_received_at'] = _isoformat(received_at)

        if text_message.startswith('{'):
            await self._handle_system_message(websocket, text_message, received_at=received_at)
            return
        self.ingest_price_message(text_message, received_at=received_at)

    async def _handle_system_message(self, websocket: Any, message: str, *, received_at: datetime) -> None:
        payload = json.loads(message)
        header = payload.get('header') or {}
        body = payload.get('body') or {}
        tr_id = _normalize_text(header.get('tr_id'))

        with self._state_lock:
            self._health['metrics']['system_messages'] += 1

        if tr_id == 'PINGPONG':
            await websocket.pong(message)
            return

        if tr_id != self._price_tr_id():
            return

        result_code = _normalize_text(body.get('rt_cd'))
        message_text = _normalize_text(body.get('msg1')) or 'system_message'
        if result_code and result_code != '0':
            failed_symbol = self._system_message_symbol(header=header, body=body)
            failure_message = (
                f'{message_text} (rt_cd={result_code}, symbol={failed_symbol})'
                if failed_symbol
                else f'{message_text} (rt_cd={result_code})'
            )
            with self._state_lock:
                if failed_symbol:
                    self._subscribed_symbols.discard(failed_symbol)
                else:
                    self._subscribed_symbols.clear()
                self._health['status'] = 'reconnecting'
                self._health['subscription'] = {
                    'status': 'failed',
                    'updated_at': _isoformat(received_at),
                    'message': failure_message,
                    'result_code': result_code,
                    'failed_symbol': failed_symbol,
                }
                self._sync_health_symbols_locked()
            raise KISRealtimePriceSubscriptionError(failure_message)

    @staticmethod
    def _system_message_symbol(*, header: dict[str, Any], body: dict[str, Any]) -> str | None:
        output = body.get('output') if isinstance(body.get('output'), dict) else {}
        input_payload = body.get('input') if isinstance(body.get('input'), dict) else {}
        for source in (header, output, input_payload, body):
            for key in ('tr_key', 'trkey', 'symbol', 'MKSC_SHRN_ISCD'):
                if symbol := _normalize_symbol(source.get(key)):
                    return symbol
        return None

    def ingest_price_message(
        self,
        message: str,
        *,
        received_at: datetime | None = None,
    ) -> list[dict[str, Any]]:
        received_at = received_at or _utc_now()
        ticks = parse_realtime_price_message(
            message,
            expected_tr_id=self._price_tr_id(),
            received_at=received_at,
        )
        if not ticks:
            with self._state_lock:
                self._health['metrics']['parse_errors'] += 1
            return []
        with self._state_lock:
            self._health['metrics']['price_messages_received'] += 1
            self._health['metrics']['prices_ingested'] += len(ticks)
            self._health['last_price_received_at'] = _isoformat(received_at)
            for tick in ticks:
                self._prices[tick.symbol] = tick
        return [tick.to_public_dict() for tick in ticks]

    def ingest_price_fields(
        self,
        fields: dict[str, Any],
        *,
        received_at: datetime | None = None,
    ) -> dict[str, Any] | None:
        tick = parse_realtime_price_fields(fields, received_at=received_at)
        if tick is None:
            with self._state_lock:
                self._health['metrics']['parse_errors'] += 1
            return None
        with self._state_lock:
            self._prices[tick.symbol] = tick
            self._health['metrics']['prices_ingested'] += 1
            self._health['last_price_received_at'] = _isoformat(tick.received_at)
        return tick.to_public_dict()

    def price_fields_for_symbol(self, symbol: Any) -> dict[str, Any]:
        normalized = _normalize_symbol(symbol)
        if not normalized:
            return self._empty_price_fields('invalid_symbol')
        now = _utc_now()
        with self._state_lock:
            self._target_symbols_snapshot_locked()
            tick = self._prices.get(normalized)
            desired = normalized in self._desired_symbols
            subscribed = normalized in self._subscribed_symbols
            limited = normalized in self._limited_symbols
            enabled = settings.kis_realtime_price_enabled
            configured, _ = self._is_configured()
            subscription_fields = self._subscription_fields_for_symbol_locked(
                desired=desired,
                subscribed=subscribed,
                limited=limited,
                enabled=enabled,
                configured=configured,
            )

        if tick is not None:
            status = 'limited' if limited else self._price_status_for_tick(tick, now=now)
            return {
                **tick.to_public_dict(price_status=status),
                **subscription_fields,
                'display_price_available': tick.current_price is not None,
            }
        if limited:
            return self._empty_price_fields('limited', subscription_fields=subscription_fields)
        if not enabled:
            return self._empty_price_fields('disabled', subscription_fields=subscription_fields)
        if not configured:
            return self._empty_price_fields('not_configured', subscription_fields=subscription_fields)
        if desired or subscribed:
            return self._empty_price_fields('pending', subscription_fields=subscription_fields)
        return self._empty_price_fields('not_monitored', subscription_fields=subscription_fields)

    @staticmethod
    def _subscription_fields_for_symbol_locked(
        *,
        desired: bool,
        subscribed: bool,
        limited: bool,
        enabled: bool,
        configured: bool,
    ) -> dict[str, Any]:
        if limited:
            reason = 'capacity_limited'
        elif subscribed:
            reason = 'subscribed'
        elif not enabled:
            reason = 'disabled'
        elif not configured:
            reason = 'not_configured'
        elif desired:
            reason = 'pending'
        else:
            reason = 'not_monitored'
        return {
            'realtime_subscribed': bool(subscribed),
            'realtime_subscription_reason': reason,
        }

    def _price_status_for_tick(self, tick: KISRealtimePriceTick, *, now: datetime) -> str:
        stale_after_seconds = max(1.0, float(settings.kis_realtime_price_stale_after_seconds))
        age_seconds = max(0.0, (now - tick.received_at).total_seconds())
        return 'stale' if age_seconds > stale_after_seconds else 'available'

    @staticmethod
    def _empty_price_fields(
        status: str,
        *,
        subscription_fields: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            'current_price': None,
            'price_as_of': None,
            'price_received_at': None,
            'price_source': None,
            'price_status': status,
            'display_price_available': False,
            **(subscription_fields or {}),
        }

    def monitored_snapshot(self) -> dict[str, Any]:
        now = _utc_now()
        with self._state_lock:
            symbols = sorted(
                self._desired_symbols
                | self._subscribed_symbols
                | self._limited_symbols
            )
            status = self._health.get('status')
            health = copy.deepcopy(self._health)

        items = [
            {
                'symbol': symbol,
                **self.price_fields_for_symbol(symbol),
            }
            for symbol in symbols
        ]
        price_status_counts: dict[str, int] = {}
        for item in items:
            price_status = str(item.get('price_status') or 'unknown')
            price_status_counts[price_status] = price_status_counts.get(price_status, 0) + 1
        return {
            'status': status,
            'as_of': _isoformat(now),
            'summary': {
                'items_total': len(items),
                'subscribed_count': len(health.get('subscribed_symbols') or []),
                'limited_count': len(health.get('limited_symbols') or []),
                'price_status_counts': dict(sorted(price_status_counts.items())),
            },
            'items': items,
            'health': health,
        }

    def subscription_snapshot(self) -> dict[str, Any]:
        allowed, limited = self._target_symbols_snapshot()
        with self._state_lock:
            return {
                'desired_symbols': sorted(self._desired_symbols),
                'allowed_symbols': allowed,
                'limited_symbols': limited,
                'subscribed_symbols': sorted(self._subscribed_symbols),
                'max_subscriptions': settings.kis_realtime_price_max_subscriptions,
            }

    def health_snapshot(self) -> dict[str, Any]:
        now = _utc_now()
        with self._state_lock:
            self._sync_health_symbols_locked()
            payload = copy.deepcopy(self._health)
        last_message_received_at = payload.get('last_message_received_at')
        if last_message_received_at:
            try:
                last_message_at = datetime.fromisoformat(last_message_received_at)
            except ValueError:
                last_message_at = None
            if last_message_at is not None:
                payload['last_message_age_seconds'] = round(
                    max(0.0, (now - last_message_at).total_seconds()),
                    3,
                )
        return payload


kis_realtime_price_service = KISRealtimePriceService()
