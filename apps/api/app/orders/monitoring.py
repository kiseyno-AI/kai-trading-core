from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from app.market.clock import KST
from app.orders.domain import BrokerOrderStatus
from app.orders.kis_realtime_order_notices import kis_realtime_order_notice_service
from app.orders.reconciliation import trading_order_reconciliation_service
from app.orders.service import TradingOrderService, trading_order_service
from app.orders.store import list_broker_orders, load_order_request

_OPEN_STATUSES = {
    BrokerOrderStatus.PENDING_SUBMIT.value,
    BrokerOrderStatus.SUBMITTED.value,
    BrokerOrderStatus.ACCEPTED.value,
    BrokerOrderStatus.CANCEL_PENDING.value,
    BrokerOrderStatus.REPLACE_PENDING.value,
    BrokerOrderStatus.PARTIAL_FILL.value,
    BrokerOrderStatus.RECONCILE_ONLY.value,
}

_STALE_PENDING_STATUSES = {
    BrokerOrderStatus.PENDING_SUBMIT.value,
    BrokerOrderStatus.SUBMITTED.value,
    BrokerOrderStatus.ACCEPTED.value,
    BrokerOrderStatus.CANCEL_PENDING.value,
    BrokerOrderStatus.REPLACE_PENDING.value,
}

_CANCELED_OR_REJECTED_STATUSES = {
    BrokerOrderStatus.CANCELED.value,
    BrokerOrderStatus.REJECTED.value,
}

_EVENT_LABELS = {
    'submit_request': 'Submit requested',
    'submit_response': 'Submit acknowledged',
    'accept': 'Accepted',
    'partial_fill': 'Partially filled',
    'fill': 'Filled',
    'cancel_request': 'Cancel requested',
    'cancel_confirmed': 'Canceled',
    'replace_request': 'Amend requested',
    'replace_confirmed': 'Amended',
    'reject': 'Rejected',
    'reconcile_snapshot': 'Reconciled snapshot',
    'realtime_execution_notice': 'Realtime execution notice',
    'manual_patch': 'Manual patch',
}

_SOURCE_LABELS = {
    'local_request': 'Local request',
    'rest_submit': 'REST submit',
    'rest_query': 'REST query',
    'ws_notice': 'Realtime notice',
    'poll_reconcile': 'Poll reconcile',
    'manual_patch': 'Manual patch',
}

_STATUS_LABELS = {
    BrokerOrderStatus.PENDING_SUBMIT.value: 'Pending submit',
    BrokerOrderStatus.SUBMITTED.value: 'Submitted',
    BrokerOrderStatus.ACCEPTED.value: 'Accepted',
    BrokerOrderStatus.PARTIAL_FILL.value: 'Partially filled',
    BrokerOrderStatus.FILLED.value: 'Filled',
    BrokerOrderStatus.CANCEL_PENDING.value: 'Cancel pending',
    BrokerOrderStatus.CANCELED.value: 'Canceled',
    BrokerOrderStatus.REPLACE_PENDING.value: 'Amend pending',
    BrokerOrderStatus.REPLACED.value: 'Amended',
    BrokerOrderStatus.REJECTED.value: 'Rejected',
    BrokerOrderStatus.RECONCILE_ONLY.value: 'Reconcile only',
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_text(value: Any, *, lowercase: bool = False, uppercase: bool = False) -> str:
    normalized = str(value or '').strip()
    if lowercase:
        return normalized.lower()
    if uppercase:
        return normalized.upper()
    return normalized


def _normalize_datetime(value: Any) -> datetime | None:
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            return None
    return None


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None or value == '':
        return None
    try:
        return value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _decimal_to_text(value: Any) -> str | None:
    normalized = _safe_decimal(value)
    if normalized is None:
        return None
    return format(normalized, 'f')


def _isoformat(value: Any) -> str | None:
    normalized = _normalize_datetime(value)
    return normalized.isoformat() if normalized is not None else None


class TradingOrderMonitoringService:
    def __init__(
        self,
        *,
        order_service: TradingOrderService | None = None,
        reconciliation_service: Any | None = None,
        realtime_notice_service: Any | None = None,
    ) -> None:
        self.order_service = order_service or trading_order_service
        self.reconciliation_service = reconciliation_service or trading_order_reconciliation_service
        self.realtime_notice_service = realtime_notice_service or kis_realtime_order_notice_service

    def lifecycle_view(
        self,
        *,
        account_reference: str | None = None,
        symbol: str | None = None,
        limit: int = 25,
        stale_after_minutes: int | None = None,
    ) -> dict[str, Any]:
        now = _utc_now()
        normalized_limit = max(1, min(int(limit), 200))
        today_start_utc, tomorrow_start_utc, today_trade_date = self._today_window(now=now)
        inspect_payload = self.reconciliation_service.inspect_orders(
            account_reference=account_reference,
            symbol=symbol,
            limit=normalized_limit,
            stale_after_minutes=stale_after_minutes,
        )
        realtime_health = self.realtime_health()

        today_items = self._build_monitor_items(
            list_broker_orders(
                account_reference=account_reference,
                symbol=symbol,
                submitted_after=today_start_utc,
                submitted_before=tomorrow_start_utc,
                limit=normalized_limit,
            ),
            stale_after_minutes=stale_after_minutes,
        )
        open_items = self._build_monitor_items(
            list_broker_orders(
                account_reference=account_reference,
                symbol=symbol,
                statuses=sorted(_OPEN_STATUSES),
                limit=normalized_limit,
            ),
            stale_after_minutes=stale_after_minutes,
        )
        partial_fill_items = self._build_monitor_items(
            list_broker_orders(
                account_reference=account_reference,
                symbol=symbol,
                statuses=[BrokerOrderStatus.PARTIAL_FILL.value],
                limit=normalized_limit,
            ),
            stale_after_minutes=stale_after_minutes,
        )
        filled_items = self._build_monitor_items(
            list_broker_orders(
                account_reference=account_reference,
                symbol=symbol,
                statuses=[BrokerOrderStatus.FILLED.value],
                limit=normalized_limit,
            ),
            stale_after_minutes=stale_after_minutes,
        )
        canceled_or_rejected_items = self._build_monitor_items(
            list_broker_orders(
                account_reference=account_reference,
                symbol=symbol,
                statuses=sorted(_CANCELED_OR_REJECTED_STATUSES),
                limit=normalized_limit,
            ),
            stale_after_minutes=stale_after_minutes,
        )
        replaced_items = self._build_monitor_items(
            list_broker_orders(
                account_reference=account_reference,
                symbol=symbol,
                statuses=[BrokerOrderStatus.REPLACED.value],
                limit=normalized_limit,
            ),
            stale_after_minutes=stale_after_minutes,
        )

        return {
            'as_of': now.isoformat(),
            'trade_date': today_trade_date,
            'filters': {
                'account_reference': account_reference,
                'symbol': _normalize_text(symbol, uppercase=True) or None,
                'limit': normalized_limit,
                'stale_after_minutes': inspect_payload['filters']['stale_after_minutes'],
            },
            'summary': {
                'today_orders': len(today_items),
                'open_orders': len(open_items),
                'partially_filled_orders': len(partial_fill_items),
                'filled_orders': len(filled_items),
                'canceled_or_rejected_orders': len(canceled_or_rejected_items),
                'replaced_orders': len(replaced_items),
                'mismatch_count': inspect_payload['mismatches']['count'],
                'stale_pending_count': inspect_payload['stale_pending_orders']['count'],
                'missing_fill_detail_count': inspect_payload['missing_fill_detail_orders']['count'],
                'realtime_healthy': realtime_health['summary']['healthy'],
                'realtime_status': realtime_health['summary']['status'],
            },
            'buckets': {
                'today_orders': {
                    'label': "Today's orders",
                    'count': len(today_items),
                    'items': today_items,
                },
                'open_orders': {
                    'label': 'Open orders',
                    'count': len(open_items),
                    'items': open_items,
                },
                'partially_filled_orders': {
                    'label': 'Partially filled orders',
                    'count': len(partial_fill_items),
                    'items': partial_fill_items,
                },
                'filled_orders': {
                    'label': 'Filled orders',
                    'count': len(filled_items),
                    'items': filled_items,
                },
                'canceled_or_rejected_orders': {
                    'label': 'Canceled or rejected orders',
                    'count': len(canceled_or_rejected_items),
                    'items': canceled_or_rejected_items,
                },
                'replaced_orders': {
                    'label': 'Replaced orders',
                    'count': len(replaced_items),
                    'items': replaced_items,
                },
            },
            'attention': {
                'mismatch_count': inspect_payload['mismatches']['count'],
                'stale_pending_count': inspect_payload['stale_pending_orders']['count'],
                'missing_fill_detail_count': inspect_payload['missing_fill_detail_orders']['count'],
                'realtime_attention_flags': realtime_health['summary']['attention_flags'],
            },
        }

    def mismatches_view(
        self,
        *,
        account_reference: str | None = None,
        symbol: str | None = None,
        limit: int = 50,
        stale_after_minutes: int | None = None,
    ) -> dict[str, Any]:
        inspect_payload = self.reconciliation_service.inspect_orders(
            account_reference=account_reference,
            symbol=symbol,
            limit=limit,
            stale_after_minutes=stale_after_minutes,
        )
        return {
            'as_of': _utc_now().isoformat(),
            'filters': inspect_payload['filters'],
            'reconciliation': inspect_payload['reconciliation'],
            'count': inspect_payload['mismatches']['count'],
            'items': self._build_monitor_items(
                inspect_payload['mismatches']['items'],
                stale_after_minutes=inspect_payload['filters']['stale_after_minutes'],
            ),
        }

    def stale_pending_view(
        self,
        *,
        account_reference: str | None = None,
        symbol: str | None = None,
        limit: int = 50,
        stale_after_minutes: int | None = None,
    ) -> dict[str, Any]:
        inspect_payload = self.reconciliation_service.inspect_orders(
            account_reference=account_reference,
            symbol=symbol,
            limit=limit,
            stale_after_minutes=stale_after_minutes,
        )
        return {
            'as_of': _utc_now().isoformat(),
            'filters': inspect_payload['filters'],
            'reconciliation': inspect_payload['reconciliation'],
            'count': inspect_payload['stale_pending_orders']['count'],
            'items': self._build_monitor_items(
                inspect_payload['stale_pending_orders']['items'],
                stale_after_minutes=inspect_payload['filters']['stale_after_minutes'],
            ),
        }

    def realtime_health(self) -> dict[str, Any]:
        health = self.realtime_notice_service.health_snapshot()
        status = _normalize_text(health.get('status'), lowercase=True) or 'unknown'
        summary = {
            'status': status,
            'healthy': bool(health.get('enabled') and health.get('configured') and status == 'subscribed'),
            'attention_flags': self._realtime_attention_flags(health),
            'last_message_received_at': health.get('last_message_received_at'),
            'last_message_age_seconds': health.get('last_message_age_seconds'),
            'reconnect_attempts': int(health.get('reconnect_attempts') or 0),
            'consecutive_failures': int(health.get('consecutive_failures') or 0),
        }
        return {
            'as_of': _utc_now().isoformat(),
            'summary': summary,
            'realtime_order_notices': health,
        }

    async def order_timeline_view(
        self,
        *,
        account_reference: str,
        client_order_id: str,
        refresh: bool = False,
    ) -> dict[str, Any]:
        detail = await self.order_service.get_order(
            account_reference=account_reference,
            client_order_id=client_order_id,
            refresh=refresh,
        )
        summary = self._build_monitor_item(
            detail.get('order'),
            request_row=detail.get('request'),
        )
        timeline_items = [self._build_timeline_item(item) for item in (detail.get('events') or [])]
        return {
            'operation': 'timeline',
            'broker_name': detail.get('broker_name') or 'kis',
            'account_reference': account_reference,
            'client_order_id': client_order_id,
            'summary': summary,
            'request': detail.get('request'),
            'order': detail.get('order'),
            'timeline': {
                'count': len(timeline_items),
                'items': timeline_items,
                'latest_event': timeline_items[-1] if timeline_items else None,
            },
            'broker_inquiry': detail.get('broker_inquiry'),
            'reconcile': detail.get('reconcile'),
        }

    def _build_monitor_items(
        self,
        order_rows: list[dict[str, Any]],
        *,
        stale_after_minutes: int | None = None,
    ) -> list[dict[str, Any]]:
        return [
            self._build_monitor_item(row, stale_after_minutes=stale_after_minutes)
            for row in order_rows
        ]

    def _build_monitor_item(
        self,
        order_row: dict[str, Any] | None,
        *,
        request_row: dict[str, Any] | None = None,
        stale_after_minutes: int | None = None,
    ) -> dict[str, Any] | None:
        if not isinstance(order_row, dict):
            return None

        account_reference = _normalize_text(order_row.get('account_reference')) or None
        client_order_id = _normalize_text(order_row.get('client_order_id')) or None
        if request_row is None and account_reference and client_order_id:
            request_row = load_order_request(
                account_reference=account_reference,
                client_order_id=client_order_id,
            )

        current_status = _normalize_text(order_row.get('current_status'), lowercase=True)
        last_reconcile_payload = order_row.get('last_reconcile_payload') or {}
        mismatch_reasons = [
            _normalize_text(reason, lowercase=True)
            for reason in (last_reconcile_payload.get('mismatch_reasons') or [])
            if _normalize_text(reason)
        ]
        is_stale_pending = self._is_stale_pending(
            order_row,
            stale_after_minutes=stale_after_minutes,
        )
        missing_fill_detail = bool(last_reconcile_payload.get('missing_fill_detail')) or self._has_missing_fill_detail(order_row)
        latest_error = (
            _normalize_text(last_reconcile_payload.get('error'))
            or _normalize_text((request_row or {}).get('last_submission_error'))
            or (_normalize_text(order_row.get('status_reason')) if current_status == BrokerOrderStatus.REJECTED.value else '')
            or None
        )

        attention_flags: list[str] = []
        if mismatch_reasons or _normalize_text(order_row.get('last_reconcile_status'), lowercase=True) == 'error':
            attention_flags.append('reconcile_mismatch')
        if is_stale_pending:
            attention_flags.append('stale_pending')
        if missing_fill_detail:
            attention_flags.append('missing_fill_detail')
        if current_status == BrokerOrderStatus.REJECTED.value:
            attention_flags.append('rejected')
        if latest_error and current_status != BrokerOrderStatus.REJECTED.value:
            attention_flags.append('latest_error')

        return {
            'client_order_id': client_order_id,
            'broker_order_id': _normalize_text(order_row.get('broker_order_id')) or None,
            'account_reference': account_reference,
            'symbol': _normalize_text(order_row.get('symbol'), uppercase=True) or None,
            'side': _normalize_text(order_row.get('side'), lowercase=True) or None,
            'order_type': _normalize_text(order_row.get('order_type'), lowercase=True) or None,
            'source': _normalize_text((request_row or {}).get('source'), lowercase=True) or None,
            'requested_quantity': _decimal_to_text(order_row.get('requested_quantity')),
            'requested_price': _decimal_to_text(order_row.get('requested_price')),
            'filled_quantity': _decimal_to_text(order_row.get('filled_quantity')),
            'remaining_quantity': _decimal_to_text(order_row.get('remaining_quantity')),
            'average_fill_price': _decimal_to_text(order_row.get('average_fill_price')),
            'current_status': current_status or None,
            'status_label': _STATUS_LABELS.get(current_status, current_status.replace('_', ' ').title() if current_status else None),
            'lifecycle_bucket': self._resolve_lifecycle_bucket(current_status),
            'is_open': current_status in _OPEN_STATUSES,
            'is_terminal': current_status not in _OPEN_STATUSES,
            'needs_attention': bool(attention_flags),
            'attention_flags': attention_flags,
            'submitted_at': _isoformat((request_row or {}).get('submitted_at') or order_row.get('first_submitted_at') or order_row.get('created_at')),
            'last_broker_update_at': _isoformat(order_row.get('last_broker_update_at')),
            'updated_at': _isoformat(order_row.get('updated_at')),
            'last_fill_at': _isoformat(order_row.get('last_fill_at')),
            'last_reconciled_at': _isoformat(order_row.get('last_reconciled_at')),
            'last_reconcile_status': _normalize_text(order_row.get('last_reconcile_status'), lowercase=True) or None,
            'last_reconcile_trigger': _normalize_text(order_row.get('last_reconcile_trigger'), lowercase=True) or None,
            'mismatch_reasons': mismatch_reasons,
            'is_stale_pending': is_stale_pending,
            'has_missing_fill_detail': missing_fill_detail,
            'latest_error': latest_error,
            'request_submission_status': _normalize_text((request_row or {}).get('submission_status'), lowercase=True) or None,
            'request_validation_status': _normalize_text((request_row or {}).get('validation_status'), lowercase=True) or None,
        }

    def _build_timeline_item(self, event_row: dict[str, Any]) -> dict[str, Any]:
        event_type = _normalize_text(event_row.get('event_type'), lowercase=True)
        source = _normalize_text(event_row.get('source'), lowercase=True)
        return {
            'id': event_row.get('id'),
            'event_type': event_type or None,
            'event_label': _EVENT_LABELS.get(event_type, event_type.replace('_', ' ').title() if event_type else None),
            'source': source or None,
            'source_label': _SOURCE_LABELS.get(source, source.replace('_', ' ').title() if source else None),
            'resulting_status': _normalize_text(event_row.get('resulting_status'), lowercase=True) or None,
            'resulting_status_label': _STATUS_LABELS.get(
                _normalize_text(event_row.get('resulting_status'), lowercase=True),
                _normalize_text(event_row.get('resulting_status'), lowercase=True).replace('_', ' ').title()
                if _normalize_text(event_row.get('resulting_status'), lowercase=True)
                else None,
            ),
            'event_occurred_at': _isoformat(event_row.get('event_occurred_at')),
            'quantity_delta': _decimal_to_text(event_row.get('quantity_delta')),
            'cumulative_filled_quantity': _decimal_to_text(event_row.get('cumulative_filled_quantity')),
            'fill_price': _decimal_to_text(event_row.get('fill_price')),
            'broker_event_id': _normalize_text(event_row.get('broker_event_id')) or None,
            'event_sequence': _normalize_text(event_row.get('event_sequence')) or None,
            'is_execution_event': event_type in {'partial_fill', 'fill', 'realtime_execution_notice'},
            'is_reconciliation_event': event_type == 'reconcile_snapshot' or source in {'poll_reconcile', 'rest_query'},
            'is_realtime_event': source == 'ws_notice',
            'raw_payload': event_row.get('raw_payload'),
            'normalized_payload': event_row.get('normalized_payload'),
        }

    def _has_missing_fill_detail(self, order_row: dict[str, Any]) -> bool:
        current_status = _normalize_text(order_row.get('current_status'), lowercase=True)
        filled_quantity = _safe_decimal(order_row.get('filled_quantity')) or Decimal('0')
        average_fill_price = _safe_decimal(order_row.get('average_fill_price'))
        return (
            current_status in {BrokerOrderStatus.PARTIAL_FILL.value, BrokerOrderStatus.FILLED.value}
            and filled_quantity > Decimal('0')
            and average_fill_price is None
        )

    def _is_stale_pending(
        self,
        order_row: dict[str, Any],
        *,
        stale_after_minutes: int | None,
    ) -> bool:
        current_status = _normalize_text(order_row.get('current_status'), lowercase=True)
        if current_status not in _STALE_PENDING_STATUSES:
            return False
        effective_stale_after = max(
            1,
            int(
                stale_after_minutes
                or self.reconciliation_service.snapshot().get('stale_after_minutes')
                or 15
            ),
        )
        last_update = (
            _normalize_datetime(order_row.get('last_broker_update_at'))
            or _normalize_datetime(order_row.get('updated_at'))
            or _normalize_datetime(order_row.get('created_at'))
        )
        if last_update is None:
            return False
        return _utc_now() - last_update >= timedelta(minutes=effective_stale_after)

    def _resolve_lifecycle_bucket(self, current_status: str) -> str:
        if current_status == BrokerOrderStatus.PARTIAL_FILL.value:
            return 'partially_filled_orders'
        if current_status == BrokerOrderStatus.FILLED.value:
            return 'filled_orders'
        if current_status in _CANCELED_OR_REJECTED_STATUSES:
            return 'canceled_or_rejected_orders'
        if current_status == BrokerOrderStatus.REPLACED.value:
            return 'replaced_orders'
        if current_status in _OPEN_STATUSES:
            return 'open_orders'
        return 'other'

    def _realtime_attention_flags(self, health: dict[str, Any]) -> list[str]:
        flags: list[str] = []
        status = _normalize_text(health.get('status'), lowercase=True)
        if not health.get('enabled'):
            flags.append('disabled_by_config')
        if not health.get('configured'):
            flags.append('configuration_incomplete')
        if status == 'fatal':
            flags.append('fatal_connection_error')
        elif status == 'reconnecting':
            flags.append('reconnecting')
        elif status not in {'subscribed'}:
            flags.append('subscription_not_ready')
        metrics = health.get('metrics') or {}
        if int(metrics.get('parse_errors') or 0) > 0:
            flags.append('parse_errors_seen')
        return flags

    def _today_window(self, *, now: datetime) -> tuple[datetime, datetime, str]:
        local_now = now.astimezone(KST)
        local_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        local_end = local_start + timedelta(days=1)
        return local_start.astimezone(timezone.utc), local_end.astimezone(timezone.utc), local_start.date().isoformat()


trading_order_monitoring_service = TradingOrderMonitoringService()
