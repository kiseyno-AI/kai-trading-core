from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.db import session_scope
from app.db_models import BrokerOrder, BrokerOrderEvent, OrderRequest
from app.orders.domain import BrokerOrderStatus, OrderRequestSubmissionStatus, OrderRequestValidationStatus


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_required_text(value: Any, *, field: str, lowercase: bool = False) -> str:
    normalized = str(value or '').strip()
    if not normalized:
        raise ValueError(f'{field} is required')
    return normalized.lower() if lowercase else normalized


def _normalize_optional_text(value: Any, *, lowercase: bool = False) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    return normalized.lower() if lowercase else normalized


def _normalize_symbol(value: Any) -> str:
    return _normalize_required_text(value, field='symbol').upper()


def _normalize_json_payload(value: Any, *, field: str, required: bool = False) -> dict | None:
    if value is None:
        if required:
            return {}
        return None
    if not isinstance(value, dict):
        raise ValueError(f'{field} must be a dict payload')
    return value


def _normalize_decimal(value: Any, *, field: str, required: bool = False) -> Decimal | None:
    if value is None or value == '':
        if required:
            raise ValueError(f'{field} is required')
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _normalize_datetime(value: Any, *, field: str, required: bool = False) -> datetime | None:
    if value is None or value == '':
        if required:
            raise ValueError(f'{field} is required')
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        normalized = value.strip().replace('Z', '+00:00')
        return datetime.fromisoformat(normalized)
    raise ValueError(f'{field} must be a datetime or ISO-8601 string')


def _coalesce_remaining_quantity(
    *,
    requested_quantity: Decimal | None,
    filled_quantity: Decimal | None,
    remaining_quantity: Decimal | None,
) -> Decimal | None:
    if remaining_quantity is not None:
        return remaining_quantity
    if requested_quantity is None or filled_quantity is None:
        return None
    remainder = requested_quantity - filled_quantity
    return remainder if remainder >= Decimal('0') else Decimal('0')


_STATUS_RANKS: dict[str, int] = {
    BrokerOrderStatus.PENDING_SUBMIT.value: 0,
    BrokerOrderStatus.SUBMITTED.value: 1,
    BrokerOrderStatus.ACCEPTED.value: 2,
    BrokerOrderStatus.CANCEL_PENDING.value: 2,
    BrokerOrderStatus.REPLACE_PENDING.value: 2,
    BrokerOrderStatus.RECONCILE_ONLY.value: 2,
    BrokerOrderStatus.PARTIAL_FILL.value: 3,
    BrokerOrderStatus.REPLACED.value: 4,
    BrokerOrderStatus.CANCELED.value: 4,
    BrokerOrderStatus.REJECTED.value: 4,
    BrokerOrderStatus.FILLED.value: 5,
}


def _status_rank(value: str | None) -> int:
    if value is None:
        return -1
    return _STATUS_RANKS.get(value, -1)


def _should_apply_event_snapshot_patch(
    row: BrokerOrder,
    *,
    event: dict[str, Any],
    occurred_at: datetime,
) -> bool:
    last_broker_update_at = row.last_broker_update_at
    if last_broker_update_at is None:
        return True

    new_status = _normalize_optional_text(event.get('resulting_status'), lowercase=True)
    current_status = _normalize_optional_text(row.current_status, lowercase=True)
    current_filled_quantity = row.filled_quantity if row.filled_quantity is not None else Decimal('0')
    new_cumulative_filled_quantity = _normalize_decimal(
        event.get('cumulative_filled_quantity'),
        field='cumulative_filled_quantity',
    )

    if occurred_at > last_broker_update_at:
        return True

    if occurred_at < last_broker_update_at:
        if new_cumulative_filled_quantity is not None and new_cumulative_filled_quantity > current_filled_quantity:
            return True
        if _status_rank(new_status) > _status_rank(current_status) and (
            new_cumulative_filled_quantity is None
            or new_cumulative_filled_quantity >= current_filled_quantity
        ):
            return True
        return False

    if new_cumulative_filled_quantity is not None and new_cumulative_filled_quantity > current_filled_quantity:
        return True
    return _status_rank(new_status) >= _status_rank(current_status)


def _serialize_order_request(row: OrderRequest) -> dict[str, Any]:
    return {
        'id': row.id,
        'broker_name': row.broker_name,
        'client_order_id': row.client_order_id,
        'source': row.source,
        'account_reference': row.account_reference,
        'symbol': row.symbol,
        'side': row.side,
        'order_type': row.order_type,
        'requested_quantity': row.requested_quantity,
        'requested_price': row.requested_price,
        'time_in_force': row.time_in_force,
        'strategy_context_payload': row.strategy_context_payload,
        'request_payload': row.request_payload,
        'validation_status': row.validation_status,
        'validation_payload': row.validation_payload,
        'submission_status': row.submission_status,
        'submission_payload': row.submission_payload,
        'submission_attempts': row.submission_attempts,
        'last_submission_error': row.last_submission_error,
        'submitted_at': row.submitted_at,
        'created_at': row.created_at,
        'updated_at': row.updated_at,
    }


def _serialize_broker_order(row: BrokerOrder) -> dict[str, Any]:
    return {
        'id': row.id,
        'order_request_id': row.order_request_id,
        'broker_name': row.broker_name,
        'broker_order_id': row.broker_order_id,
        'client_order_id': row.client_order_id,
        'account_reference': row.account_reference,
        'symbol': row.symbol,
        'side': row.side,
        'order_type': row.order_type,
        'requested_quantity': row.requested_quantity,
        'requested_price': row.requested_price,
        'accepted_quantity': row.accepted_quantity,
        'accepted_price': row.accepted_price,
        'filled_quantity': row.filled_quantity,
        'remaining_quantity': row.remaining_quantity,
        'average_fill_price': row.average_fill_price,
        'time_in_force': row.time_in_force,
        'current_status': row.current_status,
        'status_reason': row.status_reason,
        'first_submitted_at': row.first_submitted_at,
        'last_broker_update_at': row.last_broker_update_at,
        'last_fill_at': row.last_fill_at,
        'raw_latest_payload': row.raw_latest_payload,
        'snapshot_payload': row.snapshot_payload,
        'created_at': row.created_at,
        'updated_at': row.updated_at,
    }


def _serialize_broker_order_event(row: BrokerOrderEvent) -> dict[str, Any]:
    return {
        'id': row.id,
        'broker_order_row_id': row.broker_order_row_id,
        'order_request_id': row.order_request_id,
        'broker_name': row.broker_name,
        'broker_order_id': row.broker_order_id,
        'client_order_id': row.client_order_id,
        'account_reference': row.account_reference,
        'symbol': row.symbol,
        'event_type': row.event_type,
        'event_sequence': row.event_sequence,
        'broker_event_id': row.broker_event_id,
        'resulting_status': row.resulting_status,
        'event_occurred_at': row.event_occurred_at,
        'quantity_delta': row.quantity_delta,
        'cumulative_filled_quantity': row.cumulative_filled_quantity,
        'fill_price': row.fill_price,
        'source': row.source,
        'event_dedup_key': row.event_dedup_key,
        'raw_payload': row.raw_payload,
        'normalized_payload': row.normalized_payload,
        'created_at': row.created_at,
    }


def _normalize_order_request_values(request: dict[str, Any]) -> tuple[str, str, str, dict[str, Any]]:
    broker_name = _normalize_required_text(request.get('broker_name') or 'kis', field='broker_name', lowercase=True)
    account_reference = _normalize_required_text(request.get('account_reference'), field='account_reference')
    client_order_id = _normalize_required_text(request.get('client_order_id'), field='client_order_id')
    values = {
        'broker_name': broker_name,
        'client_order_id': client_order_id,
        'source': _normalize_required_text(request.get('source') or 'manual', field='source', lowercase=True),
        'account_reference': account_reference,
        'symbol': _normalize_symbol(request.get('symbol')),
        'side': _normalize_required_text(request.get('side'), field='side', lowercase=True),
        'order_type': _normalize_required_text(request.get('order_type'), field='order_type', lowercase=True),
        'requested_quantity': _normalize_decimal(
            request.get('requested_quantity'),
            field='requested_quantity',
            required=True,
        ),
        'requested_price': _normalize_decimal(request.get('requested_price'), field='requested_price'),
        'time_in_force': _normalize_optional_text(request.get('time_in_force'), lowercase=True),
        'strategy_context_payload': _normalize_json_payload(
            request.get('strategy_context_payload'),
            field='strategy_context_payload',
        ),
        'request_payload': _normalize_json_payload(
            request.get('request_payload'),
            field='request_payload',
            required=True,
        ),
        'validation_status': _normalize_required_text(
            request.get('validation_status') or OrderRequestValidationStatus.PENDING.value,
            field='validation_status',
            lowercase=True,
        ),
        'validation_payload': _normalize_json_payload(
            request.get('validation_payload'),
            field='validation_payload',
        ),
        'submission_status': _normalize_required_text(
            request.get('submission_status') or OrderRequestSubmissionStatus.PENDING_SUBMIT.value,
            field='submission_status',
            lowercase=True,
        ),
        'submission_payload': _normalize_json_payload(
            request.get('submission_payload'),
            field='submission_payload',
        ),
        'submission_attempts': int(request.get('submission_attempts') or 0),
        'last_submission_error': _normalize_optional_text(request.get('last_submission_error')),
        'submitted_at': _normalize_datetime(request.get('submitted_at'), field='submitted_at'),
    }
    return broker_name, account_reference, client_order_id, values


def _load_order_request_row(
    session,
    *,
    broker_name: str,
    account_reference: str,
    client_order_id: str,
) -> OrderRequest | None:
    return session.execute(
        select(OrderRequest).where(
            OrderRequest.broker_name == broker_name,
            OrderRequest.account_reference == account_reference,
            OrderRequest.client_order_id == client_order_id,
        )
    ).scalar_one_or_none()


def _load_broker_order_row(
    session,
    *,
    broker_order_row_id: int | None = None,
    broker_name: str | None = None,
    account_reference: str | None = None,
    client_order_id: str | None = None,
    broker_order_id: str | None = None,
) -> BrokerOrder | None:
    if broker_order_row_id is not None:
        return session.execute(
            select(BrokerOrder).where(BrokerOrder.id == int(broker_order_row_id))
        ).scalar_one_or_none()

    if broker_name is None or account_reference is None:
        return None

    if broker_order_id:
        row = session.execute(
            select(BrokerOrder).where(
                BrokerOrder.broker_name == broker_name,
                BrokerOrder.account_reference == account_reference,
                BrokerOrder.broker_order_id == broker_order_id,
            )
        ).scalar_one_or_none()
        if row is not None:
            return row

    if client_order_id:
        return session.execute(
            select(BrokerOrder).where(
                BrokerOrder.broker_name == broker_name,
                BrokerOrder.account_reference == account_reference,
                BrokerOrder.client_order_id == client_order_id,
            )
        ).scalar_one_or_none()

    return None


def upsert_order_request(request: dict[str, Any]) -> dict[str, Any]:
    now = _now_utc()
    _, _, _, values = _normalize_order_request_values(request)

    stmt = pg_insert(OrderRequest).values(**values)
    stmt = stmt.on_conflict_do_update(
        constraint='uq_order_requests_broker_account_client_order_id',
        set_={
            'source': stmt.excluded.source,
            'symbol': stmt.excluded.symbol,
            'side': stmt.excluded.side,
            'order_type': stmt.excluded.order_type,
            'requested_quantity': stmt.excluded.requested_quantity,
            'requested_price': stmt.excluded.requested_price,
            'time_in_force': stmt.excluded.time_in_force,
            'strategy_context_payload': stmt.excluded.strategy_context_payload,
            'request_payload': stmt.excluded.request_payload,
            'validation_status': stmt.excluded.validation_status,
            'validation_payload': stmt.excluded.validation_payload,
            'submission_status': stmt.excluded.submission_status,
            'submission_payload': stmt.excluded.submission_payload,
            'submission_attempts': stmt.excluded.submission_attempts,
            'last_submission_error': stmt.excluded.last_submission_error,
            'submitted_at': stmt.excluded.submitted_at,
            'updated_at': now,
        },
    ).returning(OrderRequest.id)

    with session_scope() as session:
        order_request_id = session.execute(stmt).scalar_one()
        row = session.execute(select(OrderRequest).where(OrderRequest.id == order_request_id)).scalar_one()
        return _serialize_order_request(row)


def create_order_request_if_absent(request: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    broker_name, account_reference, client_order_id, values = _normalize_order_request_values(request)

    stmt = pg_insert(OrderRequest).values(**values)
    stmt = stmt.on_conflict_do_nothing(
        constraint='uq_order_requests_broker_account_client_order_id'
    ).returning(OrderRequest.id)

    with session_scope() as session:
        inserted_id = session.execute(stmt).scalar_one_or_none()
        if inserted_id is not None:
            row = session.execute(select(OrderRequest).where(OrderRequest.id == inserted_id)).scalar_one()
            return _serialize_order_request(row), True

        row = _load_order_request_row(
            session,
            broker_name=broker_name,
            account_reference=account_reference,
            client_order_id=client_order_id,
        )
        if row is None:
            raise RuntimeError('order request insert conflict returned no existing row')
        return _serialize_order_request(row), False


def load_order_request(
    *,
    account_reference: str,
    client_order_id: str,
    broker_name: str = 'kis',
) -> dict[str, Any] | None:
    with session_scope() as session:
        row = _load_order_request_row(
            session,
            broker_name=_normalize_required_text(broker_name, field='broker_name', lowercase=True),
            account_reference=_normalize_required_text(account_reference, field='account_reference'),
            client_order_id=_normalize_required_text(client_order_id, field='client_order_id'),
        )
        if row is None:
            return None
        return _serialize_order_request(row)


def upsert_broker_order(snapshot: dict[str, Any]) -> dict[str, Any]:
    now = _now_utc()
    broker_name = _normalize_required_text(snapshot.get('broker_name') or 'kis', field='broker_name', lowercase=True)
    account_reference = _normalize_required_text(snapshot.get('account_reference'), field='account_reference')
    client_order_id = _normalize_required_text(snapshot.get('client_order_id'), field='client_order_id')
    external_broker_order_id = _normalize_optional_text(snapshot.get('broker_order_id'))

    with session_scope() as session:
        row = _load_broker_order_row(
            session,
            broker_name=broker_name,
            account_reference=account_reference,
            client_order_id=client_order_id,
            broker_order_id=external_broker_order_id,
        )
        if row is None:
            row = BrokerOrder(
                order_request_id=snapshot.get('order_request_id'),
                broker_name=broker_name,
                broker_order_id=external_broker_order_id,
                client_order_id=client_order_id,
                account_reference=account_reference,
                symbol=_normalize_symbol(snapshot.get('symbol')),
                side=_normalize_required_text(snapshot.get('side'), field='side', lowercase=True),
                order_type=_normalize_required_text(snapshot.get('order_type'), field='order_type', lowercase=True),
                requested_quantity=_normalize_decimal(
                    snapshot.get('requested_quantity'),
                    field='requested_quantity',
                    required=True,
                ),
                requested_price=_normalize_decimal(snapshot.get('requested_price'), field='requested_price'),
                filled_quantity=Decimal('0'),
                current_status=BrokerOrderStatus.PENDING_SUBMIT.value,
            )
            session.add(row)
            session.flush()
        elif row.order_request_id is None and snapshot.get('order_request_id') is not None:
            row.order_request_id = int(snapshot['order_request_id'])

        if row.order_request_id is None:
            order_request = _load_order_request_row(
                session,
                broker_name=broker_name,
                account_reference=account_reference,
                client_order_id=client_order_id,
            )
            if order_request is not None:
                row.order_request_id = order_request.id

        if external_broker_order_id is not None:
            row.broker_order_id = external_broker_order_id
        if 'symbol' in snapshot:
            row.symbol = _normalize_symbol(snapshot.get('symbol'))
        if 'side' in snapshot:
            row.side = _normalize_required_text(snapshot.get('side'), field='side', lowercase=True)
        if 'order_type' in snapshot:
            row.order_type = _normalize_required_text(snapshot.get('order_type'), field='order_type', lowercase=True)
        if 'requested_quantity' in snapshot:
            row.requested_quantity = _normalize_decimal(
                snapshot.get('requested_quantity'),
                field='requested_quantity',
                required=True,
            )
        if 'requested_price' in snapshot:
            row.requested_price = _normalize_decimal(snapshot.get('requested_price'), field='requested_price')
        if 'accepted_quantity' in snapshot:
            row.accepted_quantity = _normalize_decimal(snapshot.get('accepted_quantity'), field='accepted_quantity')
        if 'accepted_price' in snapshot:
            row.accepted_price = _normalize_decimal(snapshot.get('accepted_price'), field='accepted_price')
        if 'filled_quantity' in snapshot:
            row.filled_quantity = _normalize_decimal(
                snapshot.get('filled_quantity'),
                field='filled_quantity',
                required=True,
            ) or Decimal('0')
        if 'remaining_quantity' in snapshot:
            row.remaining_quantity = _normalize_decimal(snapshot.get('remaining_quantity'), field='remaining_quantity')
        row.remaining_quantity = _coalesce_remaining_quantity(
            requested_quantity=row.requested_quantity,
            filled_quantity=row.filled_quantity,
            remaining_quantity=row.remaining_quantity,
        )
        if 'average_fill_price' in snapshot:
            row.average_fill_price = _normalize_decimal(snapshot.get('average_fill_price'), field='average_fill_price')
        if 'time_in_force' in snapshot:
            row.time_in_force = _normalize_optional_text(snapshot.get('time_in_force'), lowercase=True)
        if 'current_status' in snapshot:
            row.current_status = _normalize_required_text(
                snapshot.get('current_status'),
                field='current_status',
                lowercase=True,
            )
        if 'status_reason' in snapshot:
            row.status_reason = _normalize_optional_text(snapshot.get('status_reason'))
        if 'first_submitted_at' in snapshot:
            row.first_submitted_at = _normalize_datetime(snapshot.get('first_submitted_at'), field='first_submitted_at')
        if 'last_broker_update_at' in snapshot:
            row.last_broker_update_at = _normalize_datetime(
                snapshot.get('last_broker_update_at'),
                field='last_broker_update_at',
            )
        if 'last_fill_at' in snapshot:
            row.last_fill_at = _normalize_datetime(snapshot.get('last_fill_at'), field='last_fill_at')
        if 'raw_latest_payload' in snapshot:
            row.raw_latest_payload = _normalize_json_payload(
                snapshot.get('raw_latest_payload'),
                field='raw_latest_payload',
            )
        if 'snapshot_payload' in snapshot:
            row.snapshot_payload = _normalize_json_payload(
                snapshot.get('snapshot_payload'),
                field='snapshot_payload',
            )

        if (
            row.first_submitted_at is None
            and row.current_status != BrokerOrderStatus.PENDING_SUBMIT.value
            and row.last_broker_update_at is not None
        ):
            row.first_submitted_at = row.last_broker_update_at

        row.updated_at = now
        session.flush()
        return _serialize_broker_order(row)


def load_broker_order(
    *,
    account_reference: str,
    client_order_id: str | None = None,
    broker_order_id: str | None = None,
    broker_order_row_id: int | None = None,
    broker_name: str = 'kis',
) -> dict[str, Any] | None:
    with session_scope() as session:
        row = _load_broker_order_row(
            session,
            broker_order_row_id=broker_order_row_id,
            broker_name=_normalize_required_text(broker_name, field='broker_name', lowercase=True),
            account_reference=_normalize_required_text(account_reference, field='account_reference'),
            client_order_id=_normalize_optional_text(client_order_id),
            broker_order_id=_normalize_optional_text(broker_order_id),
        )
        if row is None:
            return None
        return _serialize_broker_order(row)


def list_broker_orders(
    *,
    broker_name: str = 'kis',
    account_reference: str | None = None,
    symbol: str | None = None,
    statuses: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    normalized_broker_name = _normalize_required_text(broker_name, field='broker_name', lowercase=True)
    normalized_account_reference = _normalize_optional_text(account_reference)
    normalized_symbol = _normalize_optional_text(symbol)
    normalized_statuses = [
        _normalize_required_text(status, field='status', lowercase=True)
        for status in (statuses or [])
    ]

    with session_scope() as session:
        stmt = (
            select(BrokerOrder)
            .where(BrokerOrder.broker_name == normalized_broker_name)
            .order_by(func.coalesce(BrokerOrder.last_broker_update_at, BrokerOrder.updated_at).desc(), BrokerOrder.id.desc())
            .limit(limit)
        )
        if normalized_account_reference:
            stmt = stmt.where(BrokerOrder.account_reference == normalized_account_reference)
        if normalized_symbol:
            stmt = stmt.where(BrokerOrder.symbol == normalized_symbol.upper())
        if normalized_statuses:
            stmt = stmt.where(BrokerOrder.current_status.in_(normalized_statuses))
        rows = session.execute(stmt).scalars().all()
        return [_serialize_broker_order(row) for row in rows]


def _apply_event_snapshot_patch(
    row: BrokerOrder,
    *,
    event: dict[str, Any],
    occurred_at: datetime,
    now: datetime,
) -> None:
    broker_order_id = _normalize_optional_text(event.get('broker_order_id'))
    if broker_order_id is not None:
        row.broker_order_id = broker_order_id

    resulting_status = _normalize_optional_text(event.get('resulting_status'), lowercase=True)
    if resulting_status is not None:
        row.current_status = resulting_status

    if 'cumulative_filled_quantity' in event:
        row.filled_quantity = _normalize_decimal(
            event.get('cumulative_filled_quantity'),
            field='cumulative_filled_quantity',
        ) or Decimal('0')

    if 'remaining_quantity' in event:
        row.remaining_quantity = _normalize_decimal(event.get('remaining_quantity'), field='remaining_quantity')

    if 'average_fill_price' in event:
        row.average_fill_price = _normalize_decimal(event.get('average_fill_price'), field='average_fill_price')

    if event.get('fill_price') is not None:
        row.last_fill_at = occurred_at

    row.remaining_quantity = _coalesce_remaining_quantity(
        requested_quantity=row.requested_quantity,
        filled_quantity=row.filled_quantity,
        remaining_quantity=row.remaining_quantity,
    )
    row.last_broker_update_at = occurred_at
    row.raw_latest_payload = _normalize_json_payload(event.get('raw_payload'), field='raw_payload', required=True)
    row.snapshot_payload = _normalize_json_payload(
        event.get('normalized_payload'),
        field='normalized_payload',
        required=True,
    )
    if row.first_submitted_at is None and row.current_status != BrokerOrderStatus.PENDING_SUBMIT.value:
        row.first_submitted_at = occurred_at
    row.updated_at = now


def append_broker_order_event(event: dict[str, Any]) -> dict[str, Any]:
    now = _now_utc()
    broker_name = _normalize_required_text(event.get('broker_name') or 'kis', field='broker_name', lowercase=True)
    account_reference = _normalize_required_text(event.get('account_reference'), field='account_reference')
    client_order_id = _normalize_required_text(event.get('client_order_id'), field='client_order_id')
    broker_order_id = _normalize_optional_text(event.get('broker_order_id'))
    occurred_at = _normalize_datetime(event.get('event_occurred_at'), field='event_occurred_at', required=True)

    with session_scope() as session:
        order_row = _load_broker_order_row(
            session,
            broker_order_row_id=event.get('broker_order_row_id'),
            broker_name=broker_name,
            account_reference=account_reference,
            client_order_id=client_order_id,
            broker_order_id=broker_order_id,
        )
        if order_row is None:
            raise ValueError('broker order not found for event append')

        if broker_order_id is not None and client_order_id != order_row.client_order_id:
            raise ValueError('client_order_id does not match resolved broker order')

        values = {
            'broker_order_row_id': order_row.id,
            'order_request_id': event.get('order_request_id') or order_row.order_request_id,
            'broker_name': broker_name,
            'broker_order_id': broker_order_id or order_row.broker_order_id,
            'client_order_id': client_order_id,
            'account_reference': account_reference,
            'symbol': _normalize_symbol(event.get('symbol')) if event.get('symbol') is not None else order_row.symbol,
            'event_type': _normalize_required_text(event.get('event_type'), field='event_type', lowercase=True),
            'event_sequence': _normalize_optional_text(event.get('event_sequence')),
            'broker_event_id': _normalize_optional_text(event.get('broker_event_id')),
            'resulting_status': _normalize_optional_text(event.get('resulting_status'), lowercase=True),
            'event_occurred_at': occurred_at,
            'quantity_delta': _normalize_decimal(event.get('quantity_delta'), field='quantity_delta'),
            'cumulative_filled_quantity': _normalize_decimal(
                event.get('cumulative_filled_quantity'),
                field='cumulative_filled_quantity',
            ),
            'fill_price': _normalize_decimal(event.get('fill_price'), field='fill_price'),
            'source': _normalize_required_text(event.get('source'), field='source', lowercase=True),
            'event_dedup_key': _normalize_required_text(event.get('event_dedup_key'), field='event_dedup_key'),
            'raw_payload': _normalize_json_payload(event.get('raw_payload'), field='raw_payload', required=True),
            'normalized_payload': _normalize_json_payload(
                event.get('normalized_payload'),
                field='normalized_payload',
                required=True,
            ),
        }

        stmt = pg_insert(BrokerOrderEvent).values(**values)
        stmt = stmt.on_conflict_do_nothing(
            constraint='uq_broker_order_events_broker_order_dedup_key'
        ).returning(BrokerOrderEvent.id)
        inserted_id = session.execute(stmt).scalar_one_or_none()

        if inserted_id is None:
            existing = session.execute(
                select(BrokerOrderEvent).where(
                    BrokerOrderEvent.broker_order_row_id == order_row.id,
                    BrokerOrderEvent.event_dedup_key == values['event_dedup_key'],
                )
            ).scalar_one()
            payload = _serialize_broker_order_event(existing)
            payload['inserted'] = False
            return payload

        if _should_apply_event_snapshot_patch(order_row, event=event, occurred_at=occurred_at):
            _apply_event_snapshot_patch(order_row, event=event, occurred_at=occurred_at, now=now)
        inserted_row = session.execute(
            select(BrokerOrderEvent).where(BrokerOrderEvent.id == inserted_id)
        ).scalar_one()
        payload = _serialize_broker_order_event(inserted_row)
        payload['inserted'] = True
        return payload


def list_broker_order_events(
    *,
    broker_order_row_id: int | None = None,
    order_request_id: int | None = None,
    broker_name: str | None = None,
    account_reference: str | None = None,
    client_order_id: str | None = None,
    broker_order_id: str | None = None,
    symbol: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    with session_scope() as session:
        stmt = select(BrokerOrderEvent).order_by(
            BrokerOrderEvent.event_occurred_at.asc(),
            BrokerOrderEvent.id.asc(),
        ).limit(limit)
        if broker_order_row_id is not None:
            stmt = stmt.where(BrokerOrderEvent.broker_order_row_id == int(broker_order_row_id))
        if order_request_id is not None:
            stmt = stmt.where(BrokerOrderEvent.order_request_id == int(order_request_id))
        if broker_name:
            stmt = stmt.where(
                BrokerOrderEvent.broker_name
                == _normalize_required_text(broker_name, field='broker_name', lowercase=True)
            )
        if account_reference:
            stmt = stmt.where(
                BrokerOrderEvent.account_reference
                == _normalize_required_text(account_reference, field='account_reference')
            )
        if client_order_id:
            stmt = stmt.where(
                BrokerOrderEvent.client_order_id
                == _normalize_required_text(client_order_id, field='client_order_id')
            )
        if broker_order_id:
            stmt = stmt.where(BrokerOrderEvent.broker_order_id == _normalize_required_text(broker_order_id, field='broker_order_id'))
        if symbol:
            stmt = stmt.where(BrokerOrderEvent.symbol == _normalize_symbol(symbol))
        rows = session.execute(stmt).scalars().all()
        return [_serialize_broker_order_event(row) for row in rows]
