from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import uuid4

from app.brokers.kis.client import (
    KISAPIError,
    KISClient,
    KISClientError,
    KISConfigurationError,
    KISHTTPError,
    KISTimeoutError,
    KISTransportError,
)
from app.brokers.kis.provider import kis_client
from app.orders.domain import (
    BrokerOrderEventSource,
    BrokerOrderEventType,
    BrokerOrderStatus,
    OrderRequestSubmissionStatus,
    OrderRequestValidationStatus,
    OrderSide,
    OrderType,
    TimeInForce,
)
from app.orders.store import (
    append_broker_order_event,
    create_order_request_if_absent,
    list_broker_order_events,
    list_broker_orders,
    load_broker_order,
    load_order_request,
    upsert_broker_order,
    upsert_order_request,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_text(value: Any, *, field: str, uppercase: bool = False, lowercase: bool = False) -> str:
    normalized = str(value or '').strip()
    if not normalized:
        raise TradingOrderValidationError(f'{field} is required')
    if uppercase:
        return normalized.upper()
    if lowercase:
        return normalized.lower()
    return normalized


def _normalize_optional_text(value: Any, *, uppercase: bool = False, lowercase: bool = False) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    if uppercase:
        return normalized.upper()
    if lowercase:
        return normalized.lower()
    return normalized


def _to_decimal(
    value: Any,
    *,
    field: str,
    required: bool = False,
    positive: bool = False,
    non_negative: bool = False,
) -> Decimal | None:
    if value is None or value == '':
        if required:
            raise TradingOrderValidationError(f'{field} is required')
        return None
    try:
        normalized = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise TradingOrderValidationError(f'{field} must be numeric') from exc
    if positive and normalized <= Decimal('0'):
        raise TradingOrderValidationError(f'{field} must be greater than zero')
    if non_negative and normalized < Decimal('0'):
        raise TradingOrderValidationError(f'{field} must be zero or greater')
    return normalized


def _decimal_to_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, 'f')


def _date_to_yyyymmdd(value: date | datetime) -> str:
    if isinstance(value, datetime):
        return value.date().strftime('%Y%m%d')
    return value.strftime('%Y%m%d')


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None or value == '':
        return None
    try:
        return value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _pick_first(mapping: dict[str, Any] | None, *keys: str) -> Any:
    if not isinstance(mapping, dict):
        return None
    lowered = {str(key).lower(): value for key, value in mapping.items()}
    for key in keys:
        if key in mapping:
            return mapping[key]
        candidate = lowered.get(key.lower())
        if candidate is not None:
            return candidate
    return None


class TradingOrderError(RuntimeError):
    def __init__(self, message: str, *, payload: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.payload = payload or {}


class TradingOrderValidationError(TradingOrderError):
    pass


class TradingOrderNotFoundError(TradingOrderError):
    pass


class TradingOrderIdempotencyConflictError(TradingOrderError):
    pass


class TradingOrderRejectedError(TradingOrderError):
    pass


class TradingOrderPendingSubmissionError(TradingOrderError):
    pass


class TradingOrderUpstreamError(TradingOrderError):
    pass


@dataclass(slots=True)
class AccountScope:
    account_reference: str
    account_number: str
    account_product_code: str


@dataclass(slots=True)
class SubmitOrderInput:
    client_order_id: str
    source: str
    side: str
    symbol: str
    quantity: Decimal
    price: Decimal | None
    order_type: str
    time_in_force: str
    exchange_id: str
    account_scope: AccountScope
    strategy_context_payload: dict[str, Any] | None
    sell_type: str | None
    condition_price: Decimal | None
    broker_order_code: str
    broker_order_price: Decimal


class TradingOrderService:
    def __init__(self, *, client: KISClient | None = None) -> None:
        self.client = client or kis_client

    async def submit_buy_order(self, **kwargs: Any) -> dict[str, Any]:
        return await self._submit_cash_order(side=OrderSide.BUY.value, **kwargs)

    async def submit_sell_order(self, **kwargs: Any) -> dict[str, Any]:
        return await self._submit_cash_order(side=OrderSide.SELL.value, **kwargs)

    async def cancel_order(
        self,
        *,
        account_reference: str,
        client_order_id: str,
        quantity: Any = None,
        all_remaining: bool = True,
        exchange_id: str = 'KRX',
    ) -> dict[str, Any]:
        now = _utc_now()
        account_reference = _normalize_text(account_reference, field='account_reference')
        client_order_id = _normalize_text(client_order_id, field='client_order_id')
        exchange_id = _normalize_text(exchange_id, field='exchange_id', uppercase=True)

        order_row = load_broker_order(account_reference=account_reference, client_order_id=client_order_id)
        request_row = load_order_request(account_reference=account_reference, client_order_id=client_order_id)
        if order_row is None or request_row is None:
            raise TradingOrderNotFoundError(
                'order not found',
                payload={
                    'account_reference': account_reference,
                    'client_order_id': client_order_id,
                },
            )

        if order_row['current_status'] in {BrokerOrderStatus.CANCEL_PENDING.value, BrokerOrderStatus.CANCELED.value}:
            return self._build_order_response(
                account_reference=account_reference,
                client_order_id=client_order_id,
                request_row=request_row,
                order_row=order_row,
                events=list_broker_order_events(
                    account_reference=account_reference,
                    client_order_id=client_order_id,
                ),
                operation='cancel',
                idempotent_replay=True,
            )

        if not order_row.get('broker_order_id'):
            raise TradingOrderValidationError(
                'cannot cancel an order that does not have a broker order id yet',
                payload=self._build_order_response(
                    account_reference=account_reference,
                    client_order_id=client_order_id,
                    request_row=request_row,
                    order_row=order_row,
                    events=list_broker_order_events(
                        account_reference=account_reference,
                        client_order_id=client_order_id,
                    ),
                    operation='cancel',
                ),
            )

        cancelable = await self.client.fetch_domestic_cancelable_orders(
            account_number=self._account_number_from_reference(account_reference),
            account_product_code=self._account_product_code_from_reference(account_reference),
            inquiry_division_1='0',
            inquiry_division_2='1' if order_row['side'] == OrderSide.SELL.value else '2',
        )
        cancelable_row = self._find_cancelable_row(order_row=order_row, cancelable_rows=cancelable.get('output') or [])
        if cancelable_row is None:
            raise TradingOrderValidationError(
                'order is not currently cancelable according to KIS inquiry',
                payload=self._build_order_response(
                    account_reference=account_reference,
                    client_order_id=client_order_id,
                    request_row=request_row,
                    order_row=order_row,
                    events=list_broker_order_events(
                        account_reference=account_reference,
                        client_order_id=client_order_id,
                    ),
                    operation='cancel',
                    broker_inquiry={'cancelable_orders': cancelable},
                ),
            )

        possible_quantity = _safe_decimal(cancelable_row.get('psbl_qty')) or _safe_decimal(order_row.get('remaining_quantity'))
        if possible_quantity is None or possible_quantity <= Decimal('0'):
            raise TradingOrderValidationError('order has no cancelable quantity remaining')

        cancel_quantity = _to_decimal(
            quantity if quantity is not None else possible_quantity,
            field='quantity',
            required=True,
            positive=True,
        )
        if cancel_quantity > possible_quantity:
            raise TradingOrderValidationError('cancel quantity exceeds KIS cancelable quantity')

        local_request_payload = {
            'action': 'cancel',
            'account_reference': account_reference,
            'exchange_id': exchange_id,
            'quantity': _decimal_to_text(cancel_quantity),
            'all_remaining': bool(all_remaining),
            'possible_quantity': _decimal_to_text(possible_quantity),
            'broker_order_id': order_row['broker_order_id'],
        }
        append_broker_order_event(
            {
                'broker_name': 'kis',
                'account_reference': account_reference,
                'client_order_id': client_order_id,
                'broker_order_id': order_row['broker_order_id'],
                'event_type': BrokerOrderEventType.CANCEL_REQUEST.value,
                'event_occurred_at': now,
                'source': BrokerOrderEventSource.LOCAL_REQUEST.value,
                'event_dedup_key': f'cancel-request-local:{client_order_id}',
                'raw_payload': local_request_payload,
                'normalized_payload': {
                    'action': 'cancel',
                    'status': BrokerOrderStatus.CANCEL_PENDING.value,
                },
            }
        )

        broker_order_code = str(cancelable_row.get('ord_dvsn_cd') or self._resolve_kis_order_code(
            order_type=order_row['order_type'],
            time_in_force=order_row.get('time_in_force') or TimeInForce.DAY.value,
            price=_safe_decimal(order_row.get('requested_price')),
        )[0])
        order_price = _safe_decimal(order_row.get('requested_price')) or Decimal('0')
        all_quantity_order_yn = 'Y' if all_remaining or cancel_quantity == possible_quantity else 'N'

        try:
            response_payload = await self.client.submit_domestic_order_cancel_replace(
                account_number=self._account_number_from_reference(account_reference),
                account_product_code=self._account_product_code_from_reference(account_reference),
                broker_order_id=str(order_row['broker_order_id']),
                exchange_forward_order_orgno=str(self._extract_forward_orgno(order_row) or '00000'),
                order_division_code=broker_order_code,
                replace_cancel_division_code='02',
                order_quantity=_decimal_to_text(cancel_quantity) or '0',
                order_price=_decimal_to_text(order_price) or '0',
                all_quantity_order_yn=all_quantity_order_yn,
                exchange_id_division_code=exchange_id,
            )
            snapshot_payload = {
                'status': BrokerOrderStatus.CANCEL_PENDING.value,
                'action': 'cancel',
                'exchange_id': exchange_id,
                'possible_quantity': _decimal_to_text(possible_quantity),
                'cancel_quantity': _decimal_to_text(cancel_quantity),
                'rt_cd': _pick_first(response_payload, 'rt_cd'),
                'msg_cd': _pick_first(response_payload, 'msg_cd'),
                'msg1': _pick_first(response_payload, 'msg1'),
            }
            order_row = upsert_broker_order(
                {
                    'broker_name': 'kis',
                    'order_request_id': request_row['id'],
                    'client_order_id': client_order_id,
                    'broker_order_id': order_row['broker_order_id'],
                    'account_reference': account_reference,
                    'symbol': order_row['symbol'],
                    'side': order_row['side'],
                    'order_type': order_row['order_type'],
                    'requested_quantity': order_row['requested_quantity'],
                    'requested_price': order_row['requested_price'],
                    'accepted_quantity': order_row['accepted_quantity'],
                    'accepted_price': order_row['accepted_price'],
                    'filled_quantity': order_row['filled_quantity'],
                    'remaining_quantity': order_row['remaining_quantity'],
                    'average_fill_price': order_row['average_fill_price'],
                    'time_in_force': order_row.get('time_in_force'),
                    'current_status': BrokerOrderStatus.CANCEL_PENDING.value,
                    'status_reason': _pick_first(response_payload, 'msg1') or 'cancel submitted',
                    'last_broker_update_at': now,
                    'raw_latest_payload': response_payload,
                    'snapshot_payload': snapshot_payload,
                }
            )
            append_broker_order_event(
                {
                    'broker_name': 'kis',
                    'account_reference': account_reference,
                    'client_order_id': client_order_id,
                    'broker_order_id': order_row['broker_order_id'],
                    'event_type': BrokerOrderEventType.CANCEL_REQUEST.value,
                    'event_occurred_at': now,
                    'source': BrokerOrderEventSource.REST_SUBMIT.value,
                    'event_dedup_key': f'cancel-request-rest:{client_order_id}',
                    'raw_payload': response_payload,
                    'normalized_payload': snapshot_payload,
                    'resulting_status': BrokerOrderStatus.CANCEL_PENDING.value,
                    'broker_event_id': _normalize_optional_text(_pick_first(response_payload.get('output'), 'ODNO', 'odno')),
                }
            )
        except KISAPIError as exc:
            append_broker_order_event(
                {
                    'broker_name': 'kis',
                    'account_reference': account_reference,
                    'client_order_id': client_order_id,
                    'broker_order_id': order_row['broker_order_id'],
                    'event_type': BrokerOrderEventType.REJECT.value,
                    'event_occurred_at': now,
                    'source': BrokerOrderEventSource.REST_SUBMIT.value,
                    'event_dedup_key': f'cancel-reject:{client_order_id}',
                    'raw_payload': exc.payload or {'error': str(exc)},
                    'normalized_payload': {
                        'action': 'cancel',
                        'status': order_row['current_status'],
                        'msg_cd': exc.msg_code,
                        'rt_cd': exc.result_code,
                        'message': str(exc),
                    },
                    'resulting_status': order_row['current_status'],
                }
            )
            raise TradingOrderRejectedError(
                str(exc),
                payload=self._build_order_response(
                    account_reference=account_reference,
                    client_order_id=client_order_id,
                    request_row=request_row,
                    order_row=load_broker_order(account_reference=account_reference, client_order_id=client_order_id),
                    events=list_broker_order_events(
                        account_reference=account_reference,
                        client_order_id=client_order_id,
                    ),
                    operation='cancel',
                    broker_inquiry={'cancelable_orders': cancelable},
                ),
            ) from exc
        except KISClientError as exc:
            order_row = upsert_broker_order(
                {
                    'broker_name': 'kis',
                    'order_request_id': request_row['id'],
                    'client_order_id': client_order_id,
                    'broker_order_id': order_row['broker_order_id'],
                    'account_reference': account_reference,
                    'symbol': order_row['symbol'],
                    'side': order_row['side'],
                    'order_type': order_row['order_type'],
                    'requested_quantity': order_row['requested_quantity'],
                    'requested_price': order_row['requested_price'],
                    'accepted_quantity': order_row['accepted_quantity'],
                    'accepted_price': order_row['accepted_price'],
                    'filled_quantity': order_row['filled_quantity'],
                    'remaining_quantity': order_row['remaining_quantity'],
                    'average_fill_price': order_row['average_fill_price'],
                    'time_in_force': order_row.get('time_in_force'),
                    'current_status': BrokerOrderStatus.CANCEL_PENDING.value if exc.ambiguous_submission else order_row['current_status'],
                    'status_reason': str(exc),
                    'last_broker_update_at': now,
                    'raw_latest_payload': exc.payload or {'error': str(exc)},
                    'snapshot_payload': {
                        'action': 'cancel',
                        'status': BrokerOrderStatus.CANCEL_PENDING.value if exc.ambiguous_submission else order_row['current_status'],
                        'error_type': type(exc).__name__,
                        'message': str(exc),
                    },
                }
            )
            raise self._map_upstream_error(
                exc,
                payload=self._build_order_response(
                    account_reference=account_reference,
                    client_order_id=client_order_id,
                    request_row=request_row,
                    order_row=order_row,
                    events=list_broker_order_events(
                        account_reference=account_reference,
                        client_order_id=client_order_id,
                    ),
                    operation='cancel',
                    broker_inquiry={'cancelable_orders': cancelable},
                ),
            ) from exc

        return self._build_order_response(
            account_reference=account_reference,
            client_order_id=client_order_id,
            request_row=request_row,
            order_row=order_row,
            events=list_broker_order_events(
                account_reference=account_reference,
                client_order_id=client_order_id,
            ),
            operation='cancel',
            broker_inquiry={'cancelable_orders': cancelable},
        )

    async def amend_order(
        self,
        *,
        account_reference: str,
        client_order_id: str,
        quantity: Any = None,
        price: Any = None,
        order_type: str | None = None,
        exchange_id: str = 'KRX',
    ) -> dict[str, Any]:
        now = _utc_now()
        account_reference = _normalize_text(account_reference, field='account_reference')
        client_order_id = _normalize_text(client_order_id, field='client_order_id')
        exchange_id = _normalize_text(exchange_id, field='exchange_id', uppercase=True)

        order_row = load_broker_order(account_reference=account_reference, client_order_id=client_order_id)
        request_row = load_order_request(account_reference=account_reference, client_order_id=client_order_id)
        if order_row is None or request_row is None:
            raise TradingOrderNotFoundError(
                'order not found',
                payload={
                    'account_reference': account_reference,
                    'client_order_id': client_order_id,
                },
            )

        if order_row['current_status'] == BrokerOrderStatus.REPLACE_PENDING.value:
            return self._build_order_response(
                account_reference=account_reference,
                client_order_id=client_order_id,
                request_row=request_row,
                order_row=order_row,
                events=list_broker_order_events(
                    account_reference=account_reference,
                    client_order_id=client_order_id,
                ),
                operation='amend',
                idempotent_replay=True,
            )

        if not order_row.get('broker_order_id'):
            raise TradingOrderValidationError(
                'cannot amend an order that does not have a broker order id yet',
                payload=self._build_order_response(
                    account_reference=account_reference,
                    client_order_id=client_order_id,
                    request_row=request_row,
                    order_row=order_row,
                    events=list_broker_order_events(
                        account_reference=account_reference,
                        client_order_id=client_order_id,
                    ),
                    operation='amend',
                ),
            )

        cancelable = await self.client.fetch_domestic_cancelable_orders(
            account_number=self._account_number_from_reference(account_reference),
            account_product_code=self._account_product_code_from_reference(account_reference),
            inquiry_division_1='0',
            inquiry_division_2='1' if order_row['side'] == OrderSide.SELL.value else '2',
        )
        cancelable_row = self._find_cancelable_row(order_row=order_row, cancelable_rows=cancelable.get('output') or [])
        if cancelable_row is None:
            raise TradingOrderValidationError(
                'order is not currently amendable according to KIS inquiry',
                payload=self._build_order_response(
                    account_reference=account_reference,
                    client_order_id=client_order_id,
                    request_row=request_row,
                    order_row=order_row,
                    events=list_broker_order_events(
                        account_reference=account_reference,
                        client_order_id=client_order_id,
                    ),
                    operation='amend',
                    broker_inquiry={'cancelable_orders': cancelable},
                ),
            )

        base_quantity = _safe_decimal(order_row.get('remaining_quantity')) or _safe_decimal(order_row.get('requested_quantity'))
        amended_quantity = _to_decimal(
            quantity if quantity is not None else base_quantity,
            field='quantity',
            required=True,
            positive=True,
        )
        amended_order_type = _normalize_optional_text(order_type, lowercase=True) or str(order_row['order_type'])
        amended_price_input = price if price is not None else order_row.get('requested_price')
        broker_order_code, broker_order_price = self._resolve_kis_order_code(
            order_type=amended_order_type,
            time_in_force=order_row.get('time_in_force') or TimeInForce.DAY.value,
            price=_to_decimal(amended_price_input, field='price'),
        )
        if amended_quantity == _safe_decimal(order_row.get('requested_quantity')) and broker_order_price == (_safe_decimal(order_row.get('requested_price')) or Decimal('0')) and amended_order_type == str(order_row['order_type']):
            raise TradingOrderValidationError('amend request does not change the order quantity, price, or type')

        possible_quantity = _safe_decimal(cancelable_row.get('psbl_qty')) or base_quantity
        if possible_quantity is not None and amended_quantity > possible_quantity:
            raise TradingOrderValidationError('amended quantity exceeds KIS amendable quantity')

        local_request_payload = {
            'action': 'amend',
            'account_reference': account_reference,
            'exchange_id': exchange_id,
            'quantity': _decimal_to_text(amended_quantity),
            'price': _decimal_to_text(broker_order_price),
            'order_type': amended_order_type,
            'broker_order_code': broker_order_code,
            'possible_quantity': _decimal_to_text(possible_quantity),
            'broker_order_id': order_row['broker_order_id'],
        }
        append_broker_order_event(
            {
                'broker_name': 'kis',
                'account_reference': account_reference,
                'client_order_id': client_order_id,
                'broker_order_id': order_row['broker_order_id'],
                'event_type': BrokerOrderEventType.REPLACE_REQUEST.value,
                'event_occurred_at': now,
                'source': BrokerOrderEventSource.LOCAL_REQUEST.value,
                'event_dedup_key': f'replace-request-local:{client_order_id}',
                'raw_payload': local_request_payload,
                'normalized_payload': {
                    'action': 'amend',
                    'status': BrokerOrderStatus.REPLACE_PENDING.value,
                },
            }
        )

        try:
            response_payload = await self.client.submit_domestic_order_cancel_replace(
                account_number=self._account_number_from_reference(account_reference),
                account_product_code=self._account_product_code_from_reference(account_reference),
                broker_order_id=str(order_row['broker_order_id']),
                exchange_forward_order_orgno=str(self._extract_forward_orgno(order_row) or '00000'),
                order_division_code=broker_order_code,
                replace_cancel_division_code='01',
                order_quantity=_decimal_to_text(amended_quantity) or '0',
                order_price=_decimal_to_text(broker_order_price) or '0',
                all_quantity_order_yn='Y' if possible_quantity is not None and amended_quantity == possible_quantity else 'N',
                exchange_id_division_code=exchange_id,
            )
            new_broker_order_id = _normalize_optional_text(_pick_first(response_payload.get('output'), 'ODNO', 'odno'))
            snapshot_payload = {
                'status': BrokerOrderStatus.REPLACE_PENDING.value,
                'action': 'amend',
                'exchange_id': exchange_id,
                'requested_quantity': _decimal_to_text(amended_quantity),
                'requested_price': _decimal_to_text(broker_order_price),
                'order_type': amended_order_type,
                'broker_order_code': broker_order_code,
                'rt_cd': _pick_first(response_payload, 'rt_cd'),
                'msg_cd': _pick_first(response_payload, 'msg_cd'),
                'msg1': _pick_first(response_payload, 'msg1'),
            }
            order_row = upsert_broker_order(
                {
                    'broker_name': 'kis',
                    'order_request_id': request_row['id'],
                    'client_order_id': client_order_id,
                    'broker_order_id': new_broker_order_id or order_row['broker_order_id'],
                    'account_reference': account_reference,
                    'symbol': order_row['symbol'],
                    'side': order_row['side'],
                    'order_type': amended_order_type,
                    'requested_quantity': amended_quantity,
                    'requested_price': broker_order_price if amended_order_type != OrderType.MARKET.value else None,
                    'accepted_quantity': order_row['accepted_quantity'],
                    'accepted_price': order_row['accepted_price'],
                    'filled_quantity': order_row['filled_quantity'],
                    'remaining_quantity': order_row['remaining_quantity'],
                    'average_fill_price': order_row['average_fill_price'],
                    'time_in_force': order_row.get('time_in_force'),
                    'current_status': BrokerOrderStatus.REPLACE_PENDING.value,
                    'status_reason': _pick_first(response_payload, 'msg1') or 'amend submitted',
                    'last_broker_update_at': now,
                    'raw_latest_payload': response_payload,
                    'snapshot_payload': snapshot_payload,
                }
            )
            append_broker_order_event(
                {
                    'broker_name': 'kis',
                    'account_reference': account_reference,
                    'client_order_id': client_order_id,
                    'broker_order_id': new_broker_order_id or order_row['broker_order_id'],
                    'event_type': BrokerOrderEventType.REPLACE_REQUEST.value,
                    'event_occurred_at': now,
                    'source': BrokerOrderEventSource.REST_SUBMIT.value,
                    'event_dedup_key': f'replace-request-rest:{client_order_id}',
                    'raw_payload': response_payload,
                    'normalized_payload': snapshot_payload,
                    'resulting_status': BrokerOrderStatus.REPLACE_PENDING.value,
                    'broker_event_id': new_broker_order_id,
                }
            )
        except KISAPIError as exc:
            append_broker_order_event(
                {
                    'broker_name': 'kis',
                    'account_reference': account_reference,
                    'client_order_id': client_order_id,
                    'broker_order_id': order_row['broker_order_id'],
                    'event_type': BrokerOrderEventType.REJECT.value,
                    'event_occurred_at': now,
                    'source': BrokerOrderEventSource.REST_SUBMIT.value,
                    'event_dedup_key': f'replace-reject:{client_order_id}',
                    'raw_payload': exc.payload or {'error': str(exc)},
                    'normalized_payload': {
                        'action': 'amend',
                        'status': order_row['current_status'],
                        'msg_cd': exc.msg_code,
                        'rt_cd': exc.result_code,
                        'message': str(exc),
                    },
                    'resulting_status': order_row['current_status'],
                }
            )
            raise TradingOrderRejectedError(
                str(exc),
                payload=self._build_order_response(
                    account_reference=account_reference,
                    client_order_id=client_order_id,
                    request_row=request_row,
                    order_row=load_broker_order(account_reference=account_reference, client_order_id=client_order_id),
                    events=list_broker_order_events(
                        account_reference=account_reference,
                        client_order_id=client_order_id,
                    ),
                    operation='amend',
                    broker_inquiry={'cancelable_orders': cancelable},
                ),
            ) from exc
        except KISClientError as exc:
            order_row = upsert_broker_order(
                {
                    'broker_name': 'kis',
                    'order_request_id': request_row['id'],
                    'client_order_id': client_order_id,
                    'broker_order_id': order_row['broker_order_id'],
                    'account_reference': account_reference,
                    'symbol': order_row['symbol'],
                    'side': order_row['side'],
                    'order_type': amended_order_type,
                    'requested_quantity': amended_quantity,
                    'requested_price': broker_order_price if amended_order_type != OrderType.MARKET.value else None,
                    'accepted_quantity': order_row['accepted_quantity'],
                    'accepted_price': order_row['accepted_price'],
                    'filled_quantity': order_row['filled_quantity'],
                    'remaining_quantity': order_row['remaining_quantity'],
                    'average_fill_price': order_row['average_fill_price'],
                    'time_in_force': order_row.get('time_in_force'),
                    'current_status': BrokerOrderStatus.REPLACE_PENDING.value if exc.ambiguous_submission else order_row['current_status'],
                    'status_reason': str(exc),
                    'last_broker_update_at': now,
                    'raw_latest_payload': exc.payload or {'error': str(exc)},
                    'snapshot_payload': {
                        'action': 'amend',
                        'status': BrokerOrderStatus.REPLACE_PENDING.value if exc.ambiguous_submission else order_row['current_status'],
                        'error_type': type(exc).__name__,
                        'message': str(exc),
                    },
                }
            )
            raise self._map_upstream_error(
                exc,
                payload=self._build_order_response(
                    account_reference=account_reference,
                    client_order_id=client_order_id,
                    request_row=request_row,
                    order_row=order_row,
                    events=list_broker_order_events(
                        account_reference=account_reference,
                        client_order_id=client_order_id,
                    ),
                    operation='amend',
                    broker_inquiry={'cancelable_orders': cancelable},
                ),
            ) from exc

        return self._build_order_response(
            account_reference=account_reference,
            client_order_id=client_order_id,
            request_row=request_row,
            order_row=order_row,
            events=list_broker_order_events(
                account_reference=account_reference,
                client_order_id=client_order_id,
            ),
            operation='amend',
            broker_inquiry={'cancelable_orders': cancelable},
        )

    async def get_order(
        self,
        *,
        account_reference: str,
        client_order_id: str,
        refresh: bool = False,
    ) -> dict[str, Any]:
        account_reference = _normalize_text(account_reference, field='account_reference')
        client_order_id = _normalize_text(client_order_id, field='client_order_id')
        request_row = load_order_request(account_reference=account_reference, client_order_id=client_order_id)
        order_row = load_broker_order(account_reference=account_reference, client_order_id=client_order_id)
        if request_row is None and order_row is None:
            raise TradingOrderNotFoundError(
                'order not found',
                payload={
                    'account_reference': account_reference,
                    'client_order_id': client_order_id,
                },
            )

        broker_inquiry = None
        if refresh and order_row is not None and order_row.get('broker_order_id'):
            broker_inquiry = await self._refresh_from_broker(order_row=order_row)
            request_row = load_order_request(account_reference=account_reference, client_order_id=client_order_id)
            order_row = load_broker_order(account_reference=account_reference, client_order_id=client_order_id)

        return self._build_order_response(
            account_reference=account_reference,
            client_order_id=client_order_id,
            request_row=request_row,
            order_row=order_row,
            events=list_broker_order_events(
                account_reference=account_reference,
                client_order_id=client_order_id,
            ),
            operation='get',
            broker_inquiry=broker_inquiry,
        )

    def list_orders(
        self,
        *,
        account_reference: str | None = None,
        symbol: str | None = None,
        statuses: list[str] | tuple[str, ...] | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        items = list_broker_orders(
            account_reference=_normalize_optional_text(account_reference),
            symbol=_normalize_optional_text(symbol, uppercase=True),
            statuses=[
                _normalize_text(status, field='status', lowercase=True)
                for status in (statuses or [])
            ],
            limit=max(1, min(int(limit), 500)),
        )
        return {
            'count': len(items),
            'items': items,
        }

    async def _submit_cash_order(self, *, side: str, **kwargs: Any) -> dict[str, Any]:
        now = _utc_now()
        normalized = self._normalize_submit_order(side=side, **kwargs)

        request_payload = {
            'action': 'submit',
            'side': normalized.side,
            'account_reference': normalized.account_scope.account_reference,
            'account_number': normalized.account_scope.account_number,
            'account_product_code': normalized.account_scope.account_product_code,
            'symbol': normalized.symbol,
            'quantity': _decimal_to_text(normalized.quantity),
            'price': _decimal_to_text(normalized.price),
            'order_type': normalized.order_type,
            'time_in_force': normalized.time_in_force,
            'exchange_id': normalized.exchange_id,
            'broker_order_code': normalized.broker_order_code,
            'sell_type': normalized.sell_type,
            'condition_price': _decimal_to_text(normalized.condition_price),
        }
        request_row, created = create_order_request_if_absent(
            {
                'broker_name': 'kis',
                'client_order_id': normalized.client_order_id,
                'source': normalized.source,
                'account_reference': normalized.account_scope.account_reference,
                'symbol': normalized.symbol,
                'side': normalized.side,
                'order_type': normalized.order_type,
                'requested_quantity': normalized.quantity,
                'requested_price': normalized.price,
                'time_in_force': normalized.time_in_force,
                'strategy_context_payload': normalized.strategy_context_payload,
                'request_payload': request_payload,
                'validation_status': OrderRequestValidationStatus.VALID.value,
                'validation_payload': {
                    'broker_order_code': normalized.broker_order_code,
                    'exchange_id': normalized.exchange_id,
                    'environment': self.client._environment(),
                },
                'submission_status': OrderRequestSubmissionStatus.PENDING_SUBMIT.value,
                'submission_attempts': 0,
            }
        )
        if not created:
            self._assert_same_submit_payload(existing_request=request_row, normalized=normalized)
            order_row = load_broker_order(
                account_reference=normalized.account_scope.account_reference,
                client_order_id=normalized.client_order_id,
            )
            return self._build_order_response(
                account_reference=normalized.account_scope.account_reference,
                client_order_id=normalized.client_order_id,
                request_row=request_row,
                order_row=order_row,
                events=list_broker_order_events(
                    account_reference=normalized.account_scope.account_reference,
                    client_order_id=normalized.client_order_id,
                ),
                operation='submit',
                created=False,
                idempotent_replay=True,
            )

        order_row = upsert_broker_order(
            {
                'order_request_id': request_row['id'],
                'broker_name': 'kis',
                'client_order_id': normalized.client_order_id,
                'account_reference': normalized.account_scope.account_reference,
                'symbol': normalized.symbol,
                'side': normalized.side,
                'order_type': normalized.order_type,
                'requested_quantity': normalized.quantity,
                'requested_price': normalized.price,
                'accepted_quantity': normalized.quantity,
                'accepted_price': normalized.price,
                'filled_quantity': Decimal('0'),
                'remaining_quantity': normalized.quantity,
                'time_in_force': normalized.time_in_force,
                'current_status': BrokerOrderStatus.PENDING_SUBMIT.value,
                'status_reason': 'request_persisted',
                'raw_latest_payload': request_payload,
                'snapshot_payload': {
                    'status': BrokerOrderStatus.PENDING_SUBMIT.value,
                    'action': 'submit',
                    'broker_order_code': normalized.broker_order_code,
                },
            }
        )
        append_broker_order_event(
            {
                'broker_name': 'kis',
                'account_reference': normalized.account_scope.account_reference,
                'client_order_id': normalized.client_order_id,
                'event_type': BrokerOrderEventType.SUBMIT_REQUEST.value,
                'event_occurred_at': now,
                'source': BrokerOrderEventSource.LOCAL_REQUEST.value,
                'event_dedup_key': f'submit-request-local:{normalized.client_order_id}',
                'raw_payload': request_payload,
                'normalized_payload': {
                    'status': BrokerOrderStatus.PENDING_SUBMIT.value,
                    'action': 'submit',
                    'broker_order_code': normalized.broker_order_code,
                },
            }
        )

        request_row = self._patch_order_request(
            request_row,
            submission_status=OrderRequestSubmissionStatus.SUBMITTING.value,
            submission_payload={
                'attempt': 1,
                'outcome': 'submitting',
                'environment': self.client._environment(),
            },
            submission_attempts=1,
            last_submission_error=None,
        )

        try:
            response_payload = await self.client.submit_domestic_cash_order(
                order_side=normalized.side,
                account_number=normalized.account_scope.account_number,
                account_product_code=normalized.account_scope.account_product_code,
                symbol=normalized.symbol,
                order_division_code=normalized.broker_order_code,
                order_quantity=_decimal_to_text(normalized.quantity) or '0',
                order_price=_decimal_to_text(normalized.broker_order_price) or '0',
                exchange_id_division_code=normalized.exchange_id,
                sell_type=normalized.sell_type or '',
                condition_price=_decimal_to_text(normalized.condition_price) or '',
            )
            broker_order_id = _normalize_optional_text(_pick_first(response_payload.get('output'), 'ODNO', 'odno'))
            forward_orgno = _normalize_optional_text(
                _pick_first(response_payload.get('output'), 'KRX_FWDG_ORD_ORGNO', 'krx_fwdg_ord_orgno')
            )
            request_row = self._patch_order_request(
                request_row,
                submission_status=OrderRequestSubmissionStatus.SUBMITTED.value,
                submission_payload={
                    'attempt': 1,
                    'outcome': 'submitted',
                    'environment': self.client._environment(),
                    'broker_order_id': broker_order_id,
                    'krx_fwdg_ord_orgno': forward_orgno,
                    'msg_cd': _pick_first(response_payload, 'msg_cd'),
                    'rt_cd': _pick_first(response_payload, 'rt_cd'),
                    'message': _pick_first(response_payload, 'msg1'),
                },
                submission_attempts=1,
                last_submission_error=None,
                submitted_at=now,
            )
            snapshot_payload = {
                'status': BrokerOrderStatus.SUBMITTED.value,
                'action': 'submit',
                'environment': self.client._environment(),
                'exchange_id': normalized.exchange_id,
                'broker_order_code': normalized.broker_order_code,
                'krx_fwdg_ord_orgno': forward_orgno,
                'broker_order_id': broker_order_id,
                'requested_quantity': _decimal_to_text(normalized.quantity),
                'requested_price': _decimal_to_text(normalized.price),
                'msg_cd': _pick_first(response_payload, 'msg_cd'),
                'rt_cd': _pick_first(response_payload, 'rt_cd'),
                'msg1': _pick_first(response_payload, 'msg1'),
            }
            order_row = upsert_broker_order(
                {
                    'order_request_id': request_row['id'],
                    'broker_name': 'kis',
                    'client_order_id': normalized.client_order_id,
                    'broker_order_id': broker_order_id,
                    'account_reference': normalized.account_scope.account_reference,
                    'symbol': normalized.symbol,
                    'side': normalized.side,
                    'order_type': normalized.order_type,
                    'requested_quantity': normalized.quantity,
                    'requested_price': normalized.price,
                    'accepted_quantity': normalized.quantity,
                    'accepted_price': normalized.price,
                    'filled_quantity': Decimal('0'),
                    'remaining_quantity': normalized.quantity,
                    'time_in_force': normalized.time_in_force,
                    'current_status': BrokerOrderStatus.SUBMITTED.value,
                    'status_reason': _pick_first(response_payload, 'msg1') or 'submitted',
                    'first_submitted_at': now,
                    'last_broker_update_at': now,
                    'raw_latest_payload': response_payload,
                    'snapshot_payload': snapshot_payload,
                }
            )
            append_broker_order_event(
                {
                    'broker_name': 'kis',
                    'account_reference': normalized.account_scope.account_reference,
                    'client_order_id': normalized.client_order_id,
                    'broker_order_id': broker_order_id,
                    'event_type': BrokerOrderEventType.SUBMIT_RESPONSE.value,
                    'event_occurred_at': now,
                    'source': BrokerOrderEventSource.REST_SUBMIT.value,
                    'event_dedup_key': f'submit-response-rest:{normalized.client_order_id}',
                    'raw_payload': response_payload,
                    'normalized_payload': snapshot_payload,
                    'resulting_status': BrokerOrderStatus.SUBMITTED.value,
                    'broker_event_id': broker_order_id,
                }
            )
        except KISAPIError as exc:
            request_row = self._patch_order_request(
                request_row,
                submission_status=OrderRequestSubmissionStatus.REJECTED.value,
                submission_payload={
                    'attempt': 1,
                    'outcome': 'rejected',
                    'environment': self.client._environment(),
                    'msg_cd': exc.msg_code,
                    'rt_cd': exc.result_code,
                    'message': str(exc),
                },
                submission_attempts=1,
                last_submission_error=str(exc),
            )
            order_row = upsert_broker_order(
                {
                    'order_request_id': request_row['id'],
                    'broker_name': 'kis',
                    'client_order_id': normalized.client_order_id,
                    'account_reference': normalized.account_scope.account_reference,
                    'symbol': normalized.symbol,
                    'side': normalized.side,
                    'order_type': normalized.order_type,
                    'requested_quantity': normalized.quantity,
                    'requested_price': normalized.price,
                    'accepted_quantity': normalized.quantity,
                    'accepted_price': normalized.price,
                    'filled_quantity': Decimal('0'),
                    'remaining_quantity': normalized.quantity,
                    'time_in_force': normalized.time_in_force,
                    'current_status': BrokerOrderStatus.REJECTED.value,
                    'status_reason': str(exc),
                    'last_broker_update_at': now,
                    'raw_latest_payload': exc.payload or {'error': str(exc)},
                    'snapshot_payload': {
                        'status': BrokerOrderStatus.REJECTED.value,
                        'action': 'submit',
                        'msg_cd': exc.msg_code,
                        'rt_cd': exc.result_code,
                        'message': str(exc),
                    },
                }
            )
            append_broker_order_event(
                {
                    'broker_name': 'kis',
                    'account_reference': normalized.account_scope.account_reference,
                    'client_order_id': normalized.client_order_id,
                    'broker_order_id': order_row.get('broker_order_id'),
                    'event_type': BrokerOrderEventType.REJECT.value,
                    'event_occurred_at': now,
                    'source': BrokerOrderEventSource.REST_SUBMIT.value,
                    'event_dedup_key': f'submit-reject-rest:{normalized.client_order_id}',
                    'raw_payload': exc.payload or {'error': str(exc)},
                    'normalized_payload': {
                        'status': BrokerOrderStatus.REJECTED.value,
                        'action': 'submit',
                        'msg_cd': exc.msg_code,
                        'rt_cd': exc.result_code,
                        'message': str(exc),
                    },
                    'resulting_status': BrokerOrderStatus.REJECTED.value,
                }
            )
            raise TradingOrderRejectedError(
                str(exc),
                payload=self._build_order_response(
                    account_reference=normalized.account_scope.account_reference,
                    client_order_id=normalized.client_order_id,
                    request_row=request_row,
                    order_row=order_row,
                    events=list_broker_order_events(
                        account_reference=normalized.account_scope.account_reference,
                        client_order_id=normalized.client_order_id,
                    ),
                    operation='submit',
                ),
            ) from exc
        except KISClientError as exc:
            pending_status = (
                OrderRequestSubmissionStatus.SUBMITTING.value
                if exc.ambiguous_submission
                else OrderRequestSubmissionStatus.FAILED.value
            )
            request_row = self._patch_order_request(
                request_row,
                submission_status=pending_status,
                submission_payload={
                    'attempt': 1,
                    'outcome': 'pending_confirmation' if exc.ambiguous_submission else 'failed',
                    'environment': self.client._environment(),
                    'error_type': type(exc).__name__,
                    'message': str(exc),
                },
                submission_attempts=1,
                last_submission_error=str(exc),
            )
            snapshot_status = (
                BrokerOrderStatus.PENDING_SUBMIT.value
                if exc.ambiguous_submission
                else BrokerOrderStatus.REJECTED.value
            )
            order_row = upsert_broker_order(
                {
                    'order_request_id': request_row['id'],
                    'broker_name': 'kis',
                    'client_order_id': normalized.client_order_id,
                    'account_reference': normalized.account_scope.account_reference,
                    'symbol': normalized.symbol,
                    'side': normalized.side,
                    'order_type': normalized.order_type,
                    'requested_quantity': normalized.quantity,
                    'requested_price': normalized.price,
                    'accepted_quantity': normalized.quantity,
                    'accepted_price': normalized.price,
                    'filled_quantity': Decimal('0'),
                    'remaining_quantity': normalized.quantity,
                    'time_in_force': normalized.time_in_force,
                    'current_status': snapshot_status,
                    'status_reason': str(exc),
                    'last_broker_update_at': now,
                    'raw_latest_payload': exc.payload or {'error': str(exc)},
                    'snapshot_payload': {
                        'status': snapshot_status,
                        'action': 'submit',
                        'error_type': type(exc).__name__,
                        'message': str(exc),
                    },
                }
            )
            append_broker_order_event(
                {
                    'broker_name': 'kis',
                    'account_reference': normalized.account_scope.account_reference,
                    'client_order_id': normalized.client_order_id,
                    'broker_order_id': order_row.get('broker_order_id'),
                    'event_type': BrokerOrderEventType.SUBMIT_RESPONSE.value,
                    'event_occurred_at': now,
                    'source': BrokerOrderEventSource.REST_SUBMIT.value,
                    'event_dedup_key': f'submit-response-error:{normalized.client_order_id}',
                    'raw_payload': exc.payload or {'error': str(exc)},
                    'normalized_payload': {
                        'status': snapshot_status,
                        'action': 'submit',
                        'error_type': type(exc).__name__,
                        'message': str(exc),
                    },
                    'resulting_status': snapshot_status,
                }
            )
            raise self._map_upstream_error(
                exc,
                payload=self._build_order_response(
                    account_reference=normalized.account_scope.account_reference,
                    client_order_id=normalized.client_order_id,
                    request_row=request_row,
                    order_row=order_row,
                    events=list_broker_order_events(
                        account_reference=normalized.account_scope.account_reference,
                        client_order_id=normalized.client_order_id,
                    ),
                    operation='submit',
                ),
            ) from exc

        return self._build_order_response(
            account_reference=normalized.account_scope.account_reference,
            client_order_id=normalized.client_order_id,
            request_row=request_row,
            order_row=order_row,
            events=list_broker_order_events(
                account_reference=normalized.account_scope.account_reference,
                client_order_id=normalized.client_order_id,
            ),
            operation='submit',
            created=True,
        )

    def _normalize_submit_order(self, *, side: str, **kwargs: Any) -> SubmitOrderInput:
        account_number = _normalize_text(kwargs.get('account_number'), field='account_number')
        account_product_code = _normalize_text(
            kwargs.get('account_product_code'),
            field='account_product_code',
        )
        account_reference = _normalize_optional_text(kwargs.get('account_reference'))
        expected_account_reference = f'KIS:{account_number}-{account_product_code}'
        if account_reference is not None and account_reference != expected_account_reference:
            raise TradingOrderValidationError(
                'account_reference must match account_number and account_product_code for submit operations',
                payload={
                    'account_reference': account_reference,
                    'account_number': account_number,
                    'account_product_code': account_product_code,
                    'expected_account_reference': expected_account_reference,
                },
            )
        client_order_id = _normalize_optional_text(kwargs.get('client_order_id')) or uuid4().hex
        symbol = _normalize_text(kwargs.get('symbol'), field='symbol', uppercase=True)
        order_type = _normalize_optional_text(kwargs.get('order_type'), lowercase=True) or OrderType.LIMIT.value
        time_in_force = _normalize_optional_text(kwargs.get('time_in_force'), lowercase=True) or TimeInForce.DAY.value
        exchange_id = _normalize_optional_text(kwargs.get('exchange_id'), uppercase=True) or 'KRX'
        source = _normalize_optional_text(kwargs.get('source'), lowercase=True) or 'api'
        quantity = _to_decimal(kwargs.get('quantity'), field='quantity', required=True, positive=True)
        price = _to_decimal(kwargs.get('price'), field='price')
        sell_type = _normalize_optional_text(kwargs.get('sell_type'))
        condition_price = _to_decimal(kwargs.get('condition_price'), field='condition_price')
        strategy_context_payload = kwargs.get('strategy_context_payload')
        if strategy_context_payload is not None and not isinstance(strategy_context_payload, dict):
            raise TradingOrderValidationError('strategy_context_payload must be a JSON object')

        broker_order_code, broker_order_price = self._resolve_kis_order_code(
            order_type=order_type,
            time_in_force=time_in_force,
            price=price,
        )

        return SubmitOrderInput(
            client_order_id=client_order_id,
            source=source,
            side=_normalize_text(side, field='side', lowercase=True),
            symbol=symbol,
            quantity=quantity or Decimal('0'),
            price=price,
            order_type=order_type,
            time_in_force=time_in_force,
            exchange_id=exchange_id,
            account_scope=AccountScope(
                account_reference=account_reference or expected_account_reference,
                account_number=account_number,
                account_product_code=account_product_code,
            ),
            strategy_context_payload=strategy_context_payload,
            sell_type=sell_type,
            condition_price=condition_price,
            broker_order_code=broker_order_code,
            broker_order_price=broker_order_price,
        )

    def _resolve_kis_order_code(
        self,
        *,
        order_type: str,
        time_in_force: str,
        price: Decimal | None,
    ) -> tuple[str, Decimal]:
        normalized_order_type = _normalize_text(order_type, field='order_type', lowercase=True)
        normalized_tif = _normalize_text(time_in_force, field='time_in_force', lowercase=True)
        if normalized_tif != TimeInForce.DAY.value:
            raise TradingOrderValidationError('only day time_in_force is supported for the initial KIS cash adapter')

        if normalized_order_type == OrderType.LIMIT.value:
            normalized_price = _to_decimal(price, field='price', required=True, positive=True)
            return '00', normalized_price or Decimal('0')
        if normalized_order_type == OrderType.MARKET.value:
            return '01', Decimal('0')
        if normalized_order_type == OrderType.BEST.value:
            return '03', Decimal('0')

        raise TradingOrderValidationError(
            f'unsupported order_type for KIS cash adapter: {normalized_order_type}'
        )

    def _assert_same_submit_payload(
        self,
        *,
        existing_request: dict[str, Any],
        normalized: SubmitOrderInput,
    ) -> None:
        existing_payload = existing_request.get('request_payload') or {}
        current_payload = {
            'side': normalized.side,
            'account_reference': normalized.account_scope.account_reference,
            'account_number': normalized.account_scope.account_number,
            'account_product_code': normalized.account_scope.account_product_code,
            'symbol': normalized.symbol,
            'quantity': _decimal_to_text(normalized.quantity),
            'price': _decimal_to_text(normalized.price),
            'order_type': normalized.order_type,
            'time_in_force': normalized.time_in_force,
            'exchange_id': normalized.exchange_id,
            'broker_order_code': normalized.broker_order_code,
        }
        comparable_existing = {
            key: existing_payload.get(key)
            for key in current_payload
        }
        if comparable_existing != current_payload:
            raise TradingOrderIdempotencyConflictError(
                'client_order_id already exists with a different request payload',
                payload={
                    'account_reference': normalized.account_scope.account_reference,
                    'client_order_id': normalized.client_order_id,
                    'existing_request_payload': comparable_existing,
                    'new_request_payload': current_payload,
                },
            )

    def _patch_order_request(self, request_row: dict[str, Any], **updates: Any) -> dict[str, Any]:
        merged = {
            **request_row,
            **updates,
        }
        return upsert_order_request(merged)

    async def _refresh_from_broker(self, *, order_row: dict[str, Any]) -> dict[str, Any]:
        account_reference = _normalize_text(order_row['account_reference'], field='account_reference')
        broker_order_id = _normalize_text(order_row['broker_order_id'], field='broker_order_id')
        now = _utc_now()
        inquiry_anchor = order_row.get('first_submitted_at') or order_row.get('created_at') or now
        if isinstance(inquiry_anchor, str):
            inquiry_anchor = datetime.fromisoformat(inquiry_anchor.replace('Z', '+00:00'))
        inquiry_date = inquiry_anchor.date() if isinstance(inquiry_anchor, datetime) else now.date()
        period_division = 'inner' if (now.date() - inquiry_date) <= timedelta(days=92) else 'before'

        daily = await self.client.fetch_domestic_daily_order_fills(
            period_division=period_division,
            account_number=self._account_number_from_reference(account_reference),
            account_product_code=self._account_product_code_from_reference(account_reference),
            inquiry_start_date=_date_to_yyyymmdd(inquiry_date),
            inquiry_end_date=_date_to_yyyymmdd(now),
            sell_buy_division_code='02' if order_row['side'] == OrderSide.BUY.value else '01',
            conclusion_division='00',
            inquiry_division='00',
            inquiry_division_3='00',
            symbol=order_row['symbol'],
            broker_order_id=broker_order_id,
            exchange_id_division_code=str(order_row.get('snapshot_payload', {}).get('exchange_id') or 'KRX'),
        )
        cancelable = await self.client.fetch_domestic_cancelable_orders(
            account_number=self._account_number_from_reference(account_reference),
            account_product_code=self._account_product_code_from_reference(account_reference),
            inquiry_division_1='0',
            inquiry_division_2='1' if order_row['side'] == OrderSide.SELL.value else '2',
        )
        matched_daily = self._find_daily_fill_row(order_row=order_row, inquiry_rows=daily.get('output1') or [])
        matched_cancelable = self._find_cancelable_row(order_row=order_row, cancelable_rows=cancelable.get('output') or [])
        if matched_daily is None and matched_cancelable is None:
            return {
                'daily_ccld': daily,
                'cancelable_orders': cancelable,
                'matched_daily_order': None,
                'matched_cancelable_order': None,
            }

        status, event_type = self._map_inquiry_state(
            order_row=order_row,
            daily_row=matched_daily,
            cancelable_row=matched_cancelable,
        )
        filled_quantity = _safe_decimal(_pick_first(matched_daily, 'tot_ccld_qty')) or _safe_decimal(order_row.get('filled_quantity')) or Decimal('0')
        remaining_quantity = (
            _safe_decimal(_pick_first(matched_daily, 'rmn_qty'))
            or _safe_decimal(_pick_first(matched_cancelable, 'psbl_qty'))
            or _safe_decimal(order_row.get('remaining_quantity'))
        )
        requested_quantity = _safe_decimal(_pick_first(matched_daily, 'ord_qty')) or _safe_decimal(order_row.get('requested_quantity')) or Decimal('0')
        average_fill_price = _safe_decimal(_pick_first(matched_daily, 'avg_prvs')) or _safe_decimal(order_row.get('average_fill_price'))
        requested_price = _safe_decimal(_pick_first(matched_daily, 'ord_unpr')) or _safe_decimal(order_row.get('requested_price'))
        snapshot_payload = {
            'status': status,
            'action': 'reconcile',
            'matched_daily_order': matched_daily,
            'matched_cancelable_order': matched_cancelable,
        }
        refreshed_row = upsert_broker_order(
            {
                'id': order_row['id'],
                'order_request_id': order_row['order_request_id'],
                'broker_name': 'kis',
                'client_order_id': order_row['client_order_id'],
                'broker_order_id': order_row['broker_order_id'],
                'account_reference': account_reference,
                'symbol': order_row['symbol'],
                'side': order_row['side'],
                'order_type': order_row['order_type'],
                'requested_quantity': requested_quantity,
                'requested_price': requested_price,
                'accepted_quantity': requested_quantity,
                'accepted_price': requested_price,
                'filled_quantity': filled_quantity,
                'remaining_quantity': remaining_quantity,
                'average_fill_price': average_fill_price,
                'time_in_force': order_row.get('time_in_force'),
                'current_status': status,
                'status_reason': f'refreshed_from_{event_type}',
                'last_broker_update_at': now,
                'raw_latest_payload': {
                    'daily_ccld': matched_daily,
                    'cancelable_order': matched_cancelable,
                },
                'snapshot_payload': snapshot_payload,
            }
        )
        append_broker_order_event(
            {
                'broker_name': 'kis',
                'account_reference': account_reference,
                'client_order_id': order_row['client_order_id'],
                'broker_order_id': refreshed_row['broker_order_id'],
                'event_type': event_type,
                'event_occurred_at': now,
                'source': BrokerOrderEventSource.REST_QUERY.value,
                'event_dedup_key': (
                    f'reconcile:{order_row["client_order_id"]}:{status}:'
                    f'{_decimal_to_text(filled_quantity)}:{_decimal_to_text(remaining_quantity)}'
                ),
                'raw_payload': {
                    'daily_ccld': matched_daily,
                    'cancelable_order': matched_cancelable,
                },
                'normalized_payload': snapshot_payload,
                'resulting_status': status,
                'cumulative_filled_quantity': filled_quantity,
                'quantity_delta': None,
                'fill_price': average_fill_price,
            }
        )
        return {
            'daily_ccld': daily,
            'cancelable_orders': cancelable,
            'matched_daily_order': matched_daily,
            'matched_cancelable_order': matched_cancelable,
        }

    def _map_inquiry_state(
        self,
        *,
        order_row: dict[str, Any],
        daily_row: dict[str, Any] | None,
        cancelable_row: dict[str, Any] | None,
    ) -> tuple[str, str]:
        if daily_row:
            requested_quantity = _safe_decimal(_pick_first(daily_row, 'ord_qty')) or Decimal('0')
            filled_quantity = _safe_decimal(_pick_first(daily_row, 'tot_ccld_qty')) or Decimal('0')
            remaining_quantity = _safe_decimal(_pick_first(daily_row, 'rmn_qty')) or Decimal('0')
            rejected_quantity = _safe_decimal(_pick_first(daily_row, 'rjct_qty')) or Decimal('0')
            cancel_confirm_quantity = _safe_decimal(_pick_first(daily_row, 'cnc_cfrm_qty')) or Decimal('0')
            canceled = str(_pick_first(daily_row, 'cncl_yn') or '').strip().upper() == 'Y'
            if canceled or cancel_confirm_quantity > Decimal('0'):
                return BrokerOrderStatus.CANCELED.value, BrokerOrderEventType.CANCEL_CONFIRMED.value
            if rejected_quantity > Decimal('0') and filled_quantity == Decimal('0'):
                return BrokerOrderStatus.REJECTED.value, BrokerOrderEventType.REJECT.value
            if requested_quantity > Decimal('0') and filled_quantity >= requested_quantity:
                return BrokerOrderStatus.FILLED.value, BrokerOrderEventType.FILL.value
            if filled_quantity > Decimal('0') and remaining_quantity > Decimal('0'):
                return BrokerOrderStatus.PARTIAL_FILL.value, BrokerOrderEventType.PARTIAL_FILL.value

        if cancelable_row:
            if order_row['current_status'] == BrokerOrderStatus.REPLACE_PENDING.value:
                return BrokerOrderStatus.REPLACED.value, BrokerOrderEventType.REPLACE_CONFIRMED.value
            return BrokerOrderStatus.ACCEPTED.value, BrokerOrderEventType.RECONCILE_SNAPSHOT.value

        return BrokerOrderStatus.SUBMITTED.value, BrokerOrderEventType.RECONCILE_SNAPSHOT.value

    def _find_daily_fill_row(
        self,
        *,
        order_row: dict[str, Any],
        inquiry_rows: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        broker_order_id = str(order_row.get('broker_order_id') or '')
        client_symbol = str(order_row.get('symbol') or '')
        latest_match: dict[str, Any] | None = None
        for row in inquiry_rows:
            row_order_id = str(_pick_first(row, 'odno') or '')
            row_origin_order_id = str(_pick_first(row, 'orgn_odno') or '')
            if broker_order_id and row_order_id not in {broker_order_id, row_origin_order_id}:
                continue
            if client_symbol and str(_pick_first(row, 'pdno') or '') not in {'', client_symbol}:
                continue
            latest_match = row
        return latest_match

    def _find_cancelable_row(
        self,
        *,
        order_row: dict[str, Any],
        cancelable_rows: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        broker_order_id = str(order_row.get('broker_order_id') or '')
        client_symbol = str(order_row.get('symbol') or '')
        for row in cancelable_rows:
            row_order_id = str(_pick_first(row, 'odno') or '')
            row_origin_order_id = str(_pick_first(row, 'orgn_odno') or '')
            if broker_order_id and row_order_id not in {broker_order_id, row_origin_order_id}:
                continue
            if client_symbol and str(_pick_first(row, 'pdno') or '') not in {'', client_symbol}:
                continue
            return row
        return None

    def _extract_forward_orgno(self, order_row: dict[str, Any]) -> str | None:
        snapshot_payload = order_row.get('snapshot_payload') or {}
        return _normalize_optional_text(snapshot_payload.get('krx_fwdg_ord_orgno'))

    def _build_order_response(
        self,
        *,
        account_reference: str,
        client_order_id: str,
        request_row: dict[str, Any] | None,
        order_row: dict[str, Any] | None,
        events: list[dict[str, Any]],
        operation: str,
        broker_inquiry: dict[str, Any] | None = None,
        created: bool | None = None,
        idempotent_replay: bool = False,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            'operation': operation,
            'broker_name': 'kis',
            'account_reference': account_reference,
            'client_order_id': client_order_id,
            'request': request_row,
            'order': order_row,
            'events': events,
            'idempotent_replay': idempotent_replay,
        }
        if created is not None:
            payload['created'] = created
        if broker_inquiry is not None:
            payload['broker_inquiry'] = broker_inquiry
        return payload

    def _map_upstream_error(
        self,
        exc: KISClientError,
        *,
        payload: dict[str, Any],
    ) -> TradingOrderError:
        if isinstance(exc, (KISTimeoutError, KISTransportError)) and exc.ambiguous_submission:
            return TradingOrderPendingSubmissionError(str(exc), payload=payload)
        if isinstance(exc, KISConfigurationError):
            return TradingOrderUpstreamError(str(exc), payload=payload)
        if isinstance(exc, KISHTTPError) and exc.ambiguous_submission:
            return TradingOrderPendingSubmissionError(str(exc), payload=payload)
        return TradingOrderUpstreamError(str(exc), payload=payload)

    def _account_number_from_reference(self, account_reference: str) -> str:
        if account_reference.startswith('KIS:') and '-' in account_reference:
            return account_reference.split(':', 1)[1].rsplit('-', 1)[0]
        raise TradingOrderValidationError(
            'account_reference must use the KIS:<account_number>-<product_code> format for control and inquiry operations'
        )

    def _account_product_code_from_reference(self, account_reference: str) -> str:
        if account_reference.startswith('KIS:') and '-' in account_reference:
            return account_reference.rsplit('-', 1)[1]
        raise TradingOrderValidationError(
            'account_reference must use the KIS:<account_number>-<product_code> format for control and inquiry operations'
        )


trading_order_service = TradingOrderService()
