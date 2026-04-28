from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import subprocess
import threading
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
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
from app.orders.domain import BrokerOrderEventSource, BrokerOrderEventType, BrokerOrderStatus
from app.orders.store import append_broker_order_event, list_broker_order_events, load_broker_order

logger = logging.getLogger(__name__)

KST = ZoneInfo('Asia/Seoul')
KIS_REALTIME_ORDER_NOTICE_COLUMNS = (
    'CUST_ID',
    'ACNT_NO',
    'ODER_NO',
    'OODER_NO',
    'SELN_BYOV_CLS',
    'RCTF_CLS',
    'ODER_KIND',
    'ODER_COND',
    'STCK_SHRN_ISCD',
    'CNTG_QTY',
    'CNTG_UNPR',
    'STCK_CNTG_HOUR',
    'RFUS_YN',
    'CNTG_YN',
    'ACPT_YN',
    'BRNC_NO',
    'ODER_QTY',
    'ACNT_NAME',
    'ORD_COND_PRC',
    'ORD_EXG_GB',
    'POPUP_YN',
    'FILLER',
    'CRDT_CLS',
    'CRDT_LOAN_DATE',
    'CNTG_ISNM40',
    'ODER_PRC',
)
FILL_EVENT_TYPES = {
    BrokerOrderEventType.PARTIAL_FILL.value,
    BrokerOrderEventType.FILL.value,
}
REJECT_MARKERS = {'1', 'Y', 'y', 'true', 'TRUE'}


class KISRealtimeSubscriptionError(RuntimeError):
    pass


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _mask_text(value: str) -> str | None:
    normalized = str(value or '').strip()
    if not normalized:
        return None
    if len(normalized) <= 4:
        return '*' * len(normalized)
    return f'{"*" * (len(normalized) - 4)}{normalized[-4:]}'


def _normalize_text(value: Any) -> str:
    return str(value or '').strip()


def _normalize_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    try:
        return Decimal(normalized)
    except (InvalidOperation, ValueError):
        return None


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, 'f')


def _normalize_bool_text(value: Any) -> str:
    return _normalize_text(value).upper()


def _normalize_account_reference(account_number: str) -> str | None:
    digits = ''.join(ch for ch in _normalize_text(account_number) if ch.isdigit())
    if len(digits) >= 10:
        return f'KIS:{digits[:8]}-{digits[8:10]}'
    return None


def _order_status_rank(status: str | None) -> int:
    ranks = {
        BrokerOrderStatus.PENDING_SUBMIT.value: 0,
        BrokerOrderStatus.SUBMITTED.value: 1,
        BrokerOrderStatus.ACCEPTED.value: 2,
        BrokerOrderStatus.CANCEL_PENDING.value: 2,
        BrokerOrderStatus.REPLACE_PENDING.value: 2,
        BrokerOrderStatus.PARTIAL_FILL.value: 3,
        BrokerOrderStatus.REPLACED.value: 4,
        BrokerOrderStatus.CANCELED.value: 4,
        BrokerOrderStatus.REJECTED.value: 4,
        BrokerOrderStatus.FILLED.value: 5,
    }
    return ranks.get(str(status or '').strip().lower(), -1)


def _combine_notice_time(*, hhmmss: str, received_at: datetime) -> datetime:
    normalized = ''.join(ch for ch in _normalize_text(hhmmss) if ch.isdigit())
    if len(normalized) != 6:
        return received_at

    local_received_at = received_at.astimezone(KST)
    local_occurred_at = datetime.combine(
        local_received_at.date(),
        time(
            hour=int(normalized[:2]),
            minute=int(normalized[2:4]),
            second=int(normalized[4:6]),
        ),
        tzinfo=KST,
    )
    if local_occurred_at - local_received_at > timedelta(hours=12):
        local_occurred_at -= timedelta(days=1)
    elif local_received_at - local_occurred_at > timedelta(hours=12):
        local_occurred_at += timedelta(days=1)
    return local_occurred_at.astimezone(timezone.utc)


def _today_local(received_at: datetime) -> date:
    return received_at.astimezone(KST).date()


class KISRealtimeOrderNoticeService:
    def __init__(self, *, client: KISClient | None = None) -> None:
        self.client = client or kis_client
        self._task: asyncio.Task[None] | None = None
        self._websocket: Any = None
        self._stop_requested = False
        self._notice_key: str | None = None
        self._notice_iv: str | None = None
        self._health_lock = threading.Lock()
        self._health: dict[str, Any] = self._build_health_state()

    def _build_health_state(self) -> dict[str, Any]:
        return {
            'enabled': settings.kis_realtime_order_notice_enabled,
            'configured': False,
            'status': 'idle',
            'environment': self.client._environment(),
            'tr_id': self._notice_tr_id(),
            'ws_url': self.client.websocket_url(),
            'hts_id_masked': _mask_text(settings.kis_hts_id),
            'last_connected_at': None,
            'last_disconnected_at': None,
            'last_message_received_at': None,
            'last_notice_received_at': None,
            'last_notice_ingested_at': None,
            'last_notice_ignored_at': None,
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
                'aes_key_active': False,
                'aes_key_received_at': None,
            },
            'last_error': None,
            'metrics': {
                'messages_received': 0,
                'system_messages': 0,
                'notices_received': 0,
                'notices_ingested': 0,
                'duplicate_notices': 0,
                'unmatched_notices': 0,
                'parse_errors': 0,
            },
        }

    def _notice_tr_id(self) -> str:
        return 'H0STCNI9' if settings.kis_use_mock else 'H0STCNI0'

    def _is_configured(self) -> tuple[bool, str | None]:
        if not settings.kis_realtime_order_notice_enabled:
            return False, 'disabled_by_config'
        if not settings.kis_app_key or not settings.kis_app_secret:
            return False, 'missing_kis_credentials'
        if not settings.kis_hts_id:
            return False, 'missing_kis_hts_id'
        return True, None

    def _set_status(self, status: str) -> None:
        configured, reason = self._is_configured()
        with self._health_lock:
            self._health['enabled'] = settings.kis_realtime_order_notice_enabled
            self._health['configured'] = configured
            self._health['status'] = status
            self._health['environment'] = self.client._environment()
            self._health['tr_id'] = self._notice_tr_id()
            self._health['ws_url'] = self.client.websocket_url()
            self._health['hts_id_masked'] = _mask_text(settings.kis_hts_id)
            if not configured and reason:
                self._health['last_error'] = {
                    'stage': 'configuration',
                    'fatal': True,
                    'retryable': False,
                    'message': reason,
                    'occurred_at': _isoformat(_utc_now()),
                }

    def _record_error(
        self,
        *,
        stage: str,
        message: str,
        fatal: bool,
        retryable: bool,
    ) -> None:
        now = _utc_now()
        with self._health_lock:
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
        now = _utc_now()
        with self._health_lock:
            self._health['status'] = 'connected'
            self._health['last_connected_at'] = _isoformat(now)
            self._health['consecutive_failures'] = 0

    def _mark_disconnected(self) -> None:
        now = _utc_now()
        with self._health_lock:
            self._health['last_disconnected_at'] = _isoformat(now)

    def _increment_reconnect_attempt(self) -> None:
        with self._health_lock:
            self._health['reconnect_attempts'] = int(self._health['reconnect_attempts']) + 1

    async def start(self) -> None:
        self._set_status('starting')
        configured, reason = self._is_configured()
        if not settings.kis_realtime_order_notice_enabled:
            self._set_status('disabled')
            return
        if not configured:
            logger.info('KIS realtime order notices not started: %s', reason)
            self._set_status('config_missing')
            return
        if self._task is not None and not self._task.done():
            return
        self._stop_requested = False
        self._task = asyncio.create_task(
            self._run(),
            name='kis-realtime-order-notices',
        )

    async def stop(self) -> None:
        self._stop_requested = True
        task = self._task
        websocket = self._websocket
        self._task = None
        self._websocket = None
        self._notice_key = None
        self._notice_iv = None

        if websocket is not None:
            try:
                await websocket.close()
            except Exception:
                logger.debug('Ignoring websocket close failure during shutdown', exc_info=True)

        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self._set_status('stopped')
        with self._health_lock:
            self._health['subscription']['aes_key_active'] = False
            self._health['subscription']['message'] = 'stopped'
            self._health['subscription']['updated_at'] = _isoformat(_utc_now())

    async def _run(self) -> None:
        reconnect_delay = max(0.5, float(settings.kis_realtime_reconnect_base_delay_seconds))
        max_reconnect_delay = max(reconnect_delay, float(settings.kis_realtime_reconnect_max_delay_seconds))

        while not self._stop_requested:
            stage = 'approval'
            try:
                approval = await self.client.issue_approval_key()
                with self._health_lock:
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
                    self._notice_key = None
                    self._notice_iv = None

                    stage = 'subscribe'
                    await websocket.send(
                        json.dumps(
                            {
                                'header': {
                                    'approval_key': approval['approval_key'],
                                    'custtype': 'P',
                                    'tr_type': '1',
                                    'content-type': 'utf-8',
                                },
                                'body': {
                                    'input': {
                                        'tr_id': self._notice_tr_id(),
                                        'tr_key': settings.kis_hts_id,
                                    }
                                },
                            }
                        )
                    )
                    with self._health_lock:
                        self._health['subscription'] = {
                            'status': 'subscribing',
                            'updated_at': _isoformat(_utc_now()),
                            'message': 'subscription_requested',
                            'aes_key_active': False,
                            'aes_key_received_at': None,
                        }

                    while not self._stop_requested:
                        stage = 'receive'
                        message = await websocket.recv()
                        await self._handle_message(websocket, message)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                fatal = self._is_fatal_error(exc)
                retryable = self._is_retryable_error(exc)
                self._record_error(
                    stage=stage,
                    message=str(exc),
                    fatal=fatal,
                    retryable=retryable,
                )
                self._mark_disconnected()
                self._websocket = None
                self._notice_key = None
                self._notice_iv = None
                if fatal or self._stop_requested:
                    break
                self._increment_reconnect_attempt()
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
            else:
                reconnect_delay = max(0.5, float(settings.kis_realtime_reconnect_base_delay_seconds))
                self._mark_disconnected()
                self._websocket = None
                self._notice_key = None
                self._notice_iv = None

    def _is_fatal_error(self, exc: Exception) -> bool:
        if isinstance(exc, (KISConfigurationError, KISAPIError, KISRealtimeSubscriptionError)):
            return True
        if isinstance(exc, KISHTTPError) and exc.status_code is not None and 400 <= exc.status_code < 500:
            return True
        return False

    def _is_retryable_error(self, exc: Exception) -> bool:
        if isinstance(exc, KISClientError):
            return exc.retryable
        return not self._is_fatal_error(exc)

    async def _handle_message(self, websocket: Any, message: Any) -> None:
        received_at = _utc_now()
        text_message = message if isinstance(message, str) else message.decode('utf-8', errors='ignore')
        with self._health_lock:
            self._health['metrics']['messages_received'] += 1
            self._health['last_message_received_at'] = _isoformat(received_at)

        if text_message.startswith('{'):
            await self._handle_system_message(websocket, text_message, received_at=received_at)
            return
        await self._handle_notice_message(text_message, received_at=received_at)

    async def _handle_system_message(self, websocket: Any, message: str, *, received_at: datetime) -> None:
        payload = json.loads(message)
        header = payload.get('header') or {}
        body = payload.get('body') or {}
        tr_id = _normalize_text(header.get('tr_id'))

        with self._health_lock:
            self._health['metrics']['system_messages'] += 1

        if tr_id == 'PINGPONG':
            await websocket.pong(message)
            return

        if tr_id != self._notice_tr_id():
            return

        result_code = _normalize_text(body.get('rt_cd'))
        message_text = _normalize_text(body.get('msg1')) or 'system_message'
        if result_code != '0':
            with self._health_lock:
                self._health['subscription'] = {
                    'status': 'failed',
                    'updated_at': _isoformat(received_at),
                    'message': message_text,
                    'aes_key_active': False,
                    'aes_key_received_at': None,
                }
            raise KISRealtimeSubscriptionError(message_text)

        output = body.get('output') or {}
        notice_key = _normalize_text(output.get('key'))
        notice_iv = _normalize_text(output.get('iv'))
        if not notice_key or not notice_iv:
            raise KISRealtimeSubscriptionError('KIS realtime subscription succeeded without AES key/iv')

        self._notice_key = notice_key
        self._notice_iv = notice_iv
        with self._health_lock:
            self._health['status'] = 'subscribed'
            self._health['subscription'] = {
                'status': 'succeeded',
                'updated_at': _isoformat(received_at),
                'message': message_text,
                'aes_key_active': True,
                'aes_key_received_at': _isoformat(received_at),
            }

    async def _handle_notice_message(self, message: str, *, received_at: datetime) -> None:
        if not self._notice_key or not self._notice_iv:
            with self._health_lock:
                self._health['metrics']['parse_errors'] += 1
            logger.warning('Received KIS realtime notice before AES key/iv were initialized')
            return

        try:
            chunks = message.split('|')
            if len(chunks) < 4 or chunks[1] != self._notice_tr_id():
                return
            decrypted_message = self._decrypt_notice_payload(
                cipher_text=chunks[3],
                key=self._notice_key,
                iv=self._notice_iv,
            )
            await asyncio.to_thread(
                self.ingest_decrypted_notice,
                decrypted_message,
                received_at=received_at,
            )
        except Exception:
            with self._health_lock:
                self._health['metrics']['parse_errors'] += 1
                self._health['last_notice_received_at'] = _isoformat(received_at)
            logger.exception('Failed to process KIS realtime order notice')

    def _decrypt_notice_payload(self, *, cipher_text: str, key: str, iv: str) -> str:
        process = subprocess.run(
            [
                'openssl',
                'enc',
                '-aes-256-cbc',
                '-d',
                '-base64',
                '-A',
                '-K',
                key.encode('utf-8').hex(),
                '-iv',
                iv.encode('utf-8').hex(),
            ],
            input=cipher_text,
            text=True,
            capture_output=True,
            check=False,
        )
        if process.returncode != 0:
            error_text = process.stderr.strip() or 'openssl decrypt failed'
            raise RuntimeError(error_text)
        return process.stdout

    def ingest_decrypted_notice(
        self,
        decrypted_message: str,
        *,
        received_at: datetime | None = None,
    ) -> dict[str, Any]:
        received_at = received_at or _utc_now()
        fields = self._deserialize_notice_fields(decrypted_message)
        return self.ingest_notice_fields(fields, received_at=received_at, decrypted_message=decrypted_message)

    def _deserialize_notice_fields(self, decrypted_message: str) -> dict[str, str]:
        values = decrypted_message.split('^')
        return {
            column: values[index] if index < len(values) else ''
            for index, column in enumerate(KIS_REALTIME_ORDER_NOTICE_COLUMNS)
        }

    def ingest_notice_fields(
        self,
        fields: dict[str, Any],
        *,
        received_at: datetime | None = None,
        decrypted_message: str | None = None,
    ) -> dict[str, Any]:
        received_at = received_at or _utc_now()
        with self._health_lock:
            self._health['metrics']['notices_received'] += 1
            self._health['last_notice_received_at'] = _isoformat(received_at)

        account_reference = _normalize_account_reference(_normalize_text(fields.get('ACNT_NO')))
        order_row = None
        matched_by = None
        for broker_order_id in (
            _normalize_text(fields.get('ODER_NO')),
            _normalize_text(fields.get('OODER_NO')),
        ):
            if not account_reference or not broker_order_id:
                continue
            order_row = load_broker_order(
                account_reference=account_reference,
                broker_order_id=broker_order_id,
            )
            if order_row is not None:
                matched_by = broker_order_id
                break

        if order_row is None or account_reference is None:
            with self._health_lock:
                self._health['metrics']['unmatched_notices'] += 1
                self._health['last_notice_ignored_at'] = _isoformat(received_at)
            logger.info(
                'Ignoring unmatched KIS realtime notice for account=%s order=%s origin_order=%s',
                account_reference,
                _normalize_text(fields.get('ODER_NO')),
                _normalize_text(fields.get('OODER_NO')),
            )
            return {
                'matched': False,
                'account_reference': account_reference,
                'broker_order_id': _normalize_text(fields.get('ODER_NO')),
            }

        normalized = self._normalize_notice_event(
            fields=fields,
            order_row=order_row,
            account_reference=account_reference,
            received_at=received_at,
            decrypted_message=decrypted_message,
        )
        append_result = append_broker_order_event(normalized)

        with self._health_lock:
            if append_result.get('inserted'):
                self._health['metrics']['notices_ingested'] += 1
            else:
                self._health['metrics']['duplicate_notices'] += 1
            self._health['last_notice_ingested_at'] = _isoformat(received_at)

        return {
            'matched': True,
            'matched_by': matched_by,
            'inserted': bool(append_result.get('inserted')),
            'event_type': append_result.get('event_type'),
            'resulting_status': append_result.get('resulting_status'),
            'broker_order_id': append_result.get('broker_order_id'),
            'client_order_id': append_result.get('client_order_id'),
        }

    def _normalize_notice_event(
        self,
        *,
        fields: dict[str, Any],
        order_row: dict[str, Any],
        account_reference: str,
        received_at: datetime,
        decrypted_message: str | None,
    ) -> dict[str, Any]:
        order_number = _normalize_text(fields.get('ODER_NO'))
        origin_order_number = _normalize_text(fields.get('OODER_NO'))
        cntg_yn = _normalize_text(fields.get('CNTG_YN'))
        acpt_yn = _normalize_text(fields.get('ACPT_YN'))
        rfus_yn = _normalize_bool_text(fields.get('RFUS_YN'))
        rctf_cls = _normalize_text(fields.get('RCTF_CLS'))
        symbol = _normalize_text(fields.get('STCK_SHRN_ISCD')).upper() or str(order_row['symbol'])
        notice_quantity = _normalize_decimal(fields.get('CNTG_QTY')) or Decimal('0')
        requested_quantity = _normalize_decimal(order_row.get('requested_quantity')) or Decimal('0')
        fill_price = _normalize_decimal(fields.get('CNTG_UNPR'))
        order_price = _normalize_decimal(fields.get('ODER_PRC'))
        current_filled_quantity = _normalize_decimal(order_row.get('filled_quantity')) or Decimal('0')
        current_average_fill_price = _normalize_decimal(order_row.get('average_fill_price'))
        last_broker_update_at = order_row.get('last_broker_update_at')
        occurred_at = _combine_notice_time(
            hhmmss=_normalize_text(fields.get('STCK_CNTG_HOUR')),
            received_at=received_at,
        )
        preserve_snapshot_fill = not isinstance(last_broker_update_at, datetime) or occurred_at >= last_broker_update_at
        prior_filled_quantity, prior_filled_value = self._summarize_prior_fill_events(
            order_row=order_row,
            occurred_at=occurred_at,
        )

        event_type = BrokerOrderEventType.REALTIME_EXECUTION_NOTICE.value
        resulting_status = str(order_row.get('current_status') or BrokerOrderStatus.SUBMITTED.value)
        quantity_delta = None
        cumulative_filled_quantity = None
        remaining_quantity = None
        average_fill_price = None
        notice_kind = 'unknown'

        correction_kind = self._resolve_correction_kind(
            rctf_cls=rctf_cls,
            order_row=order_row,
        )
        is_rejected = rfus_yn in REJECT_MARKERS

        if is_rejected:
            notice_kind = 'reject'
            event_type = BrokerOrderEventType.REJECT.value
            resulting_status = BrokerOrderStatus.REJECTED.value
        elif cntg_yn == '2':
            notice_kind = 'fill'
            quantity_delta = notice_quantity
            cumulative_filled_quantity = prior_filled_quantity + notice_quantity
            if preserve_snapshot_fill:
                cumulative_filled_quantity = max(current_filled_quantity, cumulative_filled_quantity)
            if requested_quantity > Decimal('0'):
                cumulative_filled_quantity = min(requested_quantity, cumulative_filled_quantity)
            remaining_quantity = (
                max(requested_quantity - cumulative_filled_quantity, Decimal('0'))
                if requested_quantity > Decimal('0')
                else None
            )
            resulting_status = (
                BrokerOrderStatus.FILLED.value
                if requested_quantity > Decimal('0') and cumulative_filled_quantity >= requested_quantity
                else BrokerOrderStatus.PARTIAL_FILL.value
            )
            event_type = (
                BrokerOrderEventType.FILL.value
                if resulting_status == BrokerOrderStatus.FILLED.value
                else BrokerOrderEventType.PARTIAL_FILL.value
            )
            if fill_price is not None and quantity_delta > Decimal('0'):
                if (
                    preserve_snapshot_fill
                    and cumulative_filled_quantity <= current_filled_quantity
                    and current_average_fill_price is not None
                ):
                    average_fill_price = current_average_fill_price
                else:
                    total_value = prior_filled_value + (fill_price * quantity_delta)
                    total_quantity = prior_filled_quantity + quantity_delta
                    if total_quantity > Decimal('0'):
                        average_fill_price = total_value / total_quantity
        elif correction_kind == 'cancel':
            notice_kind = 'cancel'
            event_type = BrokerOrderEventType.CANCEL_CONFIRMED.value
            resulting_status = BrokerOrderStatus.CANCELED.value
            remaining_quantity = Decimal('0')
        elif correction_kind == 'replace':
            notice_kind = 'replace'
            event_type = BrokerOrderEventType.REPLACE_CONFIRMED.value
            resulting_status = BrokerOrderStatus.REPLACED.value
        else:
            notice_kind = 'accept'
            event_type = BrokerOrderEventType.ACCEPT.value
            resulting_status = BrokerOrderStatus.ACCEPTED.value

        event_sequence = ':'.join(
            item or '-'
            for item in (
                _normalize_text(fields.get('STCK_CNTG_HOUR')),
                cntg_yn,
                acpt_yn,
                rfus_yn,
                rctf_cls,
            )
        )
        broker_event_id = ':'.join(
            item or '-'
            for item in (
                order_number or str(order_row.get('broker_order_id') or ''),
                origin_order_number,
                _normalize_text(fields.get('STCK_CNTG_HOUR')),
                cntg_yn,
                _decimal_text(notice_quantity) or '',
                _decimal_text(fill_price) or '',
            )
        )
        event_dedup_key = self._build_event_dedup_key(
            account_reference=account_reference,
            broker_order_id=order_number or str(order_row.get('broker_order_id') or ''),
            origin_order_id=origin_order_number,
            fields=fields,
            occurred_at=occurred_at,
        )
        normalized_payload = {
            'status': resulting_status,
            'notice_kind': notice_kind,
            'cntg_yn': cntg_yn,
            'acpt_yn': acpt_yn,
            'rfus_yn': rfus_yn,
            'rctf_cls': rctf_cls,
            'order_number': order_number,
            'origin_order_number': origin_order_number,
            'order_quantity': _decimal_text(_normalize_decimal(fields.get('ODER_QTY'))),
            'notice_quantity': _decimal_text(notice_quantity),
            'fill_price': _decimal_text(fill_price),
            'order_price': _decimal_text(order_price),
            'symbol_name': _normalize_text(fields.get('CNTG_ISNM40')),
            'exchange_code': _normalize_text(fields.get('ORD_EXG_GB')),
            'side_code': _normalize_text(fields.get('SELN_BYOV_CLS')),
            'received_at': _isoformat(received_at),
            'occurred_at': _isoformat(occurred_at),
        }
        raw_payload = {
            'channel': 'kis_realtime_order_notice',
            'received_at': _isoformat(received_at),
            'occurred_at': _isoformat(occurred_at),
            'fields': {key: _normalize_text(value) for key, value in fields.items()},
        }
        if decrypted_message is not None:
            raw_payload['decrypted_message'] = decrypted_message

        return {
            'broker_name': 'kis',
            'account_reference': account_reference,
            'client_order_id': str(order_row['client_order_id']),
            'broker_order_id': order_number or str(order_row.get('broker_order_id') or ''),
            'symbol': symbol,
            'event_type': event_type,
            'event_sequence': event_sequence,
            'broker_event_id': broker_event_id,
            'resulting_status': resulting_status,
            'event_occurred_at': occurred_at,
            'quantity_delta': quantity_delta,
            'cumulative_filled_quantity': cumulative_filled_quantity,
            'remaining_quantity': remaining_quantity,
            'average_fill_price': average_fill_price,
            'fill_price': fill_price,
            'source': BrokerOrderEventSource.WS_NOTICE.value,
            'event_dedup_key': event_dedup_key,
            'raw_payload': raw_payload,
            'normalized_payload': normalized_payload,
        }

    def _resolve_correction_kind(
        self,
        *,
        rctf_cls: str,
        order_row: dict[str, Any],
    ) -> str | None:
        normalized = rctf_cls.strip()
        if normalized in {'02', '2'}:
            return 'cancel'
        if normalized in {'01', '1'}:
            return 'replace'
        current_status = str(order_row.get('current_status') or '').strip().lower()
        if current_status == BrokerOrderStatus.CANCEL_PENDING.value:
            return 'cancel'
        if current_status == BrokerOrderStatus.REPLACE_PENDING.value:
            return 'replace'
        return None

    def _summarize_prior_fill_events(
        self,
        *,
        order_row: dict[str, Any],
        occurred_at: datetime,
    ) -> tuple[Decimal, Decimal]:
        prior_quantity = Decimal('0')
        prior_value = Decimal('0')
        prior_events = list_broker_order_events(
            account_reference=str(order_row['account_reference']),
            client_order_id=str(order_row['client_order_id']),
            limit=1000,
        )
        for event in prior_events:
            if str(event.get('source') or '').strip().lower() != BrokerOrderEventSource.WS_NOTICE.value:
                continue
            if str(event.get('event_type') or '').strip().lower() not in FILL_EVENT_TYPES:
                continue
            event_occurred_at = event.get('event_occurred_at')
            if not isinstance(event_occurred_at, datetime) or event_occurred_at > occurred_at:
                continue
            event_quantity = _normalize_decimal(event.get('quantity_delta')) or Decimal('0')
            event_price = _normalize_decimal(event.get('fill_price'))
            prior_quantity += event_quantity
            if event_price is not None:
                prior_value += event_price * event_quantity
        return prior_quantity, prior_value

    def _build_event_dedup_key(
        self,
        *,
        account_reference: str,
        broker_order_id: str,
        origin_order_id: str,
        fields: dict[str, Any],
        occurred_at: datetime,
    ) -> str:
        fingerprint_payload = '|'.join(
            [
                account_reference,
                broker_order_id,
                origin_order_id,
                _normalize_text(fields.get('STCK_CNTG_HOUR')),
                _normalize_text(fields.get('CNTG_YN')),
                _normalize_text(fields.get('RFUS_YN')),
                _normalize_text(fields.get('ACPT_YN')),
                _normalize_text(fields.get('RCTF_CLS')),
                _normalize_text(fields.get('STCK_SHRN_ISCD')),
                _normalize_text(fields.get('CNTG_QTY')),
                _normalize_text(fields.get('CNTG_UNPR')),
                _normalize_text(fields.get('ODER_QTY')),
                _normalize_text(fields.get('ODER_PRC')),
                occurred_at.isoformat(),
            ]
        )
        return f'kis-ws:{hashlib.sha256(fingerprint_payload.encode("utf-8")).hexdigest()}'

    def health_snapshot(self) -> dict[str, Any]:
        now = _utc_now()
        with self._health_lock:
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


kis_realtime_order_notice_service = KISRealtimeOrderNoticeService()
