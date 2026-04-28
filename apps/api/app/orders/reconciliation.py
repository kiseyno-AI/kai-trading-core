from __future__ import annotations

import asyncio
import copy
import threading
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from app.core.settings import settings
from app.orders.domain import BrokerOrderEventSource, BrokerOrderStatus
from app.orders.service import TradingOrderService, trading_order_service
from app.orders.store import list_broker_orders, upsert_broker_order

_TERMINAL_STATUSES = {
    BrokerOrderStatus.CANCELED.value,
    BrokerOrderStatus.FILLED.value,
    BrokerOrderStatus.REJECTED.value,
    BrokerOrderStatus.REPLACED.value,
}

_ACTIVE_STATUSES = {
    BrokerOrderStatus.PENDING_SUBMIT.value,
    BrokerOrderStatus.SUBMITTED.value,
    BrokerOrderStatus.ACCEPTED.value,
    BrokerOrderStatus.CANCEL_PENDING.value,
    BrokerOrderStatus.REPLACE_PENDING.value,
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_text(value: Any) -> str:
    return str(value or '').strip()


def _normalize_decimal(value: Any) -> Decimal | None:
    if value is None or value == '':
        return None
    try:
        return value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _normalize_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            return None
    return None


class TradingOrderReconciliationService:
    def __init__(self, *, order_service: TradingOrderService | None = None) -> None:
        self.order_service = order_service or trading_order_service
        self._run_lock = asyncio.Lock()
        self._state_lock = threading.Lock()
        self._state = self._build_state()

    def _build_state(self) -> dict[str, Any]:
        return {
            'enabled': settings.kis_order_reconcile_enabled,
            'startup_enabled': settings.kis_order_reconcile_startup_enabled,
            'on_reconnect_enabled': settings.kis_order_reconcile_on_reconnect_enabled,
            'interval_seconds': settings.kis_order_reconcile_interval_seconds,
            'stale_after_minutes': settings.kis_order_reconcile_stale_after_minutes,
            'recent_days': settings.kis_order_reconcile_recent_days,
            'batch_limit': settings.kis_order_reconcile_batch_limit,
            'running': False,
            'last_started_at': None,
            'last_finished_at': None,
            'last_trigger': None,
            'last_periodic_started_at': None,
            'last_run': None,
        }

    def snapshot(self) -> dict[str, Any]:
        with self._state_lock:
            payload = copy.deepcopy(self._state)
        payload['enabled'] = settings.kis_order_reconcile_enabled
        payload['startup_enabled'] = settings.kis_order_reconcile_startup_enabled
        payload['on_reconnect_enabled'] = settings.kis_order_reconcile_on_reconnect_enabled
        payload['interval_seconds'] = settings.kis_order_reconcile_interval_seconds
        payload['stale_after_minutes'] = settings.kis_order_reconcile_stale_after_minutes
        payload['recent_days'] = settings.kis_order_reconcile_recent_days
        payload['batch_limit'] = settings.kis_order_reconcile_batch_limit
        return payload

    async def run_startup_reconcile(self) -> dict[str, Any]:
        if not settings.kis_order_reconcile_enabled or not settings.kis_order_reconcile_startup_enabled:
            return {
                'triggered': False,
                'trigger': 'startup',
                'reason': 'disabled_by_config',
            }
        return await self.reconcile_recent_orders(
            trigger='startup',
            reason='api_startup_catchup',
            include_orders=False,
        )

    async def run_periodic_if_due(self) -> dict[str, Any]:
        if not settings.kis_order_reconcile_enabled:
            return {
                'triggered': False,
                'trigger': 'periodic',
                'reason': 'disabled_by_config',
            }

        interval_seconds = max(30, int(settings.kis_order_reconcile_interval_seconds))
        snapshot = self.snapshot()
        last_periodic_started_at = _normalize_datetime(snapshot.get('last_periodic_started_at'))
        now = _utc_now()
        if last_periodic_started_at is not None and (now - last_periodic_started_at).total_seconds() < interval_seconds:
            return {
                'triggered': False,
                'trigger': 'periodic',
                'reason': 'interval_not_elapsed',
                'interval_seconds': interval_seconds,
                'last_periodic_started_at': last_periodic_started_at.isoformat(),
            }

        return await self.reconcile_recent_orders(
            trigger='periodic',
            reason='scheduled_intraday_safety_sweep',
            include_orders=False,
        )

    async def run_reconnect_reconcile(self) -> dict[str, Any]:
        if not settings.kis_order_reconcile_enabled or not settings.kis_order_reconcile_on_reconnect_enabled:
            return {
                'triggered': False,
                'trigger': 'reconnect',
                'reason': 'disabled_by_config',
            }
        return await self.reconcile_recent_orders(
            trigger='reconnect',
            reason='realtime_notice_resubscribed',
            include_orders=False,
        )

    def schedule_reconnect_reconcile(self) -> None:
        if not settings.kis_order_reconcile_enabled or not settings.kis_order_reconcile_on_reconnect_enabled:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(
            self.run_reconnect_reconcile(),
            name='trading-order-reconcile-reconnect',
        )

    async def reconcile_single_order(
        self,
        *,
        account_reference: str,
        client_order_id: str,
        trigger: str = 'manual',
        reason: str | None = None,
    ) -> dict[str, Any]:
        return await self.order_service.reconcile_order(
            account_reference=account_reference,
            client_order_id=client_order_id,
            trigger=trigger,
            source=BrokerOrderEventSource.POLL_RECONCILE.value,
            reason=reason,
        )

    async def reconcile_recent_orders(
        self,
        *,
        account_reference: str | None = None,
        symbol: str | None = None,
        limit: int | None = None,
        trigger: str = 'manual',
        reason: str | None = None,
        include_orders: bool = True,
    ) -> dict[str, Any]:
        requested_limit = max(1, min(int(limit or settings.kis_order_reconcile_batch_limit), 500))
        return await self._run_reconcile(
            account_reference=account_reference,
            symbol=symbol,
            limit=requested_limit,
            trigger=trigger,
            reason=reason or trigger,
            include_orders=include_orders,
        )

    def inspect_orders(
        self,
        *,
        account_reference: str | None = None,
        symbol: str | None = None,
        limit: int = 50,
        stale_after_minutes: int | None = None,
    ) -> dict[str, Any]:
        normalized_limit = max(1, min(int(limit), 200))
        effective_stale_after = max(
            1,
            int(stale_after_minutes or settings.kis_order_reconcile_stale_after_minutes),
        )
        sample_limit = max(
            normalized_limit * 4,
            normalized_limit,
            min(max(1, int(settings.kis_order_reconcile_batch_limit)), 500),
        )
        sample_limit = min(sample_limit, 500)

        mismatch_candidates = list_broker_orders(
            account_reference=account_reference,
            symbol=symbol,
            reconcile_statuses=['repaired', 'error', 'missing_fill_detail', 'stale_pending', 'not_found'],
            limit=sample_limit,
        )
        stale_candidates = list_broker_orders(
            account_reference=account_reference,
            symbol=symbol,
            statuses=sorted(_ACTIVE_STATUSES),
            limit=sample_limit,
        )
        fill_candidates = list_broker_orders(
            account_reference=account_reference,
            symbol=symbol,
            statuses=[BrokerOrderStatus.PARTIAL_FILL.value, BrokerOrderStatus.FILLED.value],
            limit=sample_limit,
        )

        mismatches = [row for row in mismatch_candidates if self._has_reconcile_mismatch(row)][:normalized_limit]
        stale_pending = [
            row
            for row in stale_candidates
            if self._is_stale_pending(row, stale_after_minutes=effective_stale_after)
        ][:normalized_limit]
        missing_fill_detail = [
            row
            for row in fill_candidates
            if self._has_missing_fill_detail(row)
        ][:normalized_limit]

        return {
            'filters': {
                'account_reference': account_reference,
                'symbol': symbol.upper() if symbol else None,
                'limit': normalized_limit,
                'stale_after_minutes': effective_stale_after,
            },
            'reconciliation': self.snapshot(),
            'mismatches': {
                'count': len(mismatches),
                'items': mismatches,
            },
            'stale_pending_orders': {
                'count': len(stale_pending),
                'items': stale_pending,
            },
            'missing_fill_detail_orders': {
                'count': len(missing_fill_detail),
                'items': missing_fill_detail,
            },
        }

    async def _run_reconcile(
        self,
        *,
        account_reference: str | None,
        symbol: str | None,
        limit: int,
        trigger: str,
        reason: str,
        include_orders: bool,
    ) -> dict[str, Any]:
        async with self._run_lock:
            started_at = _utc_now()
            self._mark_running(trigger=trigger, started_at=started_at)

            selected_orders = self._select_candidate_orders(
                account_reference=account_reference,
                symbol=symbol,
                limit=limit,
            )

            results: list[dict[str, Any]] = []
            skipped_missing_broker_id = 0
            repaired_count = 0
            mismatch_count = 0
            stale_pending_count = 0
            missing_fill_detail_count = 0
            error_count = 0

            for order_row in selected_orders:
                broker_order_id = _normalize_text(order_row.get('broker_order_id'))
                if not broker_order_id:
                    skipped_missing_broker_id += 1
                    continue
                try:
                    result = await self.order_service.reconcile_persisted_order(
                        order_row=order_row,
                        trigger=trigger,
                        source=BrokerOrderEventSource.POLL_RECONCILE.value,
                        reason=reason,
                    )
                except Exception as exc:
                    error_count += 1
                    error_summary = self._mark_order_error(
                        order_row=order_row,
                        trigger=trigger,
                        reason=reason,
                        error_message=str(exc),
                        reconciled_at=started_at,
                    )
                    if include_orders:
                        results.append(error_summary)
                    continue

                reconcile_summary = result.get('reconcile') or {}
                if reconcile_summary.get('repair_applied'):
                    repaired_count += 1
                if reconcile_summary.get('mismatch_reasons'):
                    mismatch_count += 1
                if reconcile_summary.get('stale_pending'):
                    stale_pending_count += 1
                if reconcile_summary.get('missing_fill_detail'):
                    missing_fill_detail_count += 1

                if include_orders:
                    results.append(
                        {
                            'account_reference': result.get('account_reference'),
                            'client_order_id': result.get('client_order_id'),
                            'order': result.get('order'),
                            'reconcile': reconcile_summary,
                        }
                    )

            finished_at = _utc_now()
            summary = {
                'triggered': True,
                'trigger': trigger,
                'reason': reason,
                'started_at': started_at.isoformat(),
                'finished_at': finished_at.isoformat(),
                'account_reference': account_reference,
                'symbol': symbol.upper() if symbol else None,
                'selected_count': len(selected_orders),
                'reconciled_count': len(selected_orders) - error_count - skipped_missing_broker_id,
                'repaired_count': repaired_count,
                'mismatch_count': mismatch_count,
                'stale_pending_count': stale_pending_count,
                'missing_fill_detail_count': missing_fill_detail_count,
                'error_count': error_count,
                'skipped_missing_broker_order_id_count': skipped_missing_broker_id,
                'orders': results if include_orders else [],
            }
            self._mark_finished(
                trigger=trigger,
                finished_at=finished_at,
                summary=summary,
            )
            return summary

    def _select_candidate_orders(
        self,
        *,
        account_reference: str | None,
        symbol: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        sample_limit = min(max(limit * 3, limit, settings.kis_order_reconcile_batch_limit), 500)
        candidates = list_broker_orders(
            account_reference=account_reference,
            symbol=symbol,
            limit=sample_limit,
        )
        selected: list[dict[str, Any]] = []
        recent_cutoff = _utc_now() - timedelta(days=max(1, int(settings.kis_order_reconcile_recent_days)))

        for row in candidates:
            if self._should_reconcile_order(row, recent_cutoff=recent_cutoff):
                selected.append(row)
            if len(selected) >= limit:
                break
        return selected

    def _should_reconcile_order(self, order_row: dict[str, Any], *, recent_cutoff: datetime) -> bool:
        if self._has_missing_fill_detail(order_row):
            return True

        status = _normalize_text(order_row.get('current_status')).lower()
        last_reconcile_status = _normalize_text(order_row.get('last_reconcile_status')).lower()
        last_reconciled_at = _normalize_datetime(order_row.get('last_reconciled_at'))
        last_broker_update_at = _normalize_datetime(order_row.get('last_broker_update_at'))

        if last_reconcile_status in {'', 'repaired', 'stale_pending', 'not_found', 'missing_fill_detail', 'error'}:
            return True
        if status in _ACTIVE_STATUSES:
            return True
        if status in _TERMINAL_STATUSES:
            if last_reconciled_at is None:
                return True
            if last_broker_update_at is not None and last_reconciled_at < last_broker_update_at:
                return True
            if last_broker_update_at is not None and last_broker_update_at >= recent_cutoff:
                return True
        return False

    def _mark_order_error(
        self,
        *,
        order_row: dict[str, Any],
        trigger: str,
        reason: str,
        error_message: str,
        reconciled_at: datetime,
    ) -> dict[str, Any]:
        error_payload = {
            'trigger': trigger,
            'reason': reason,
            'reconciled_at': reconciled_at.isoformat(),
            'broker_truth_found': False,
            'repair_applied': False,
            'mismatch_reasons': [],
            'stale_pending': self._is_stale_pending(
                order_row,
                stale_after_minutes=max(1, int(settings.kis_order_reconcile_stale_after_minutes)),
            ),
            'missing_fill_detail': self._has_missing_fill_detail(order_row),
            'error': error_message,
        }
        updated_row = upsert_broker_order(
            {
                'id': order_row['id'],
                'order_request_id': order_row.get('order_request_id'),
                'broker_name': order_row['broker_name'],
                'client_order_id': order_row['client_order_id'],
                'broker_order_id': order_row.get('broker_order_id'),
                'account_reference': order_row['account_reference'],
                'symbol': order_row['symbol'],
                'side': order_row['side'],
                'order_type': order_row['order_type'],
                'requested_quantity': order_row['requested_quantity'],
                'requested_price': order_row.get('requested_price'),
                'accepted_quantity': order_row.get('accepted_quantity'),
                'accepted_price': order_row.get('accepted_price'),
                'filled_quantity': order_row.get('filled_quantity'),
                'remaining_quantity': order_row.get('remaining_quantity'),
                'average_fill_price': order_row.get('average_fill_price'),
                'time_in_force': order_row.get('time_in_force'),
                'current_status': order_row['current_status'],
                'status_reason': order_row.get('status_reason'),
                'first_submitted_at': order_row.get('first_submitted_at'),
                'last_broker_update_at': order_row.get('last_broker_update_at'),
                'last_fill_at': order_row.get('last_fill_at'),
                'last_reconciled_at': reconciled_at,
                'last_reconcile_status': 'error',
                'last_reconcile_trigger': trigger,
                'last_reconcile_payload': error_payload,
                'raw_latest_payload': order_row.get('raw_latest_payload'),
                'snapshot_payload': order_row.get('snapshot_payload'),
            }
        )
        return {
            'account_reference': updated_row['account_reference'],
            'client_order_id': updated_row['client_order_id'],
            'order': updated_row,
            'reconcile': error_payload,
        }

    def _mark_running(self, *, trigger: str, started_at: datetime) -> None:
        with self._state_lock:
            self._state['running'] = True
            self._state['last_started_at'] = started_at.isoformat()
            self._state['last_trigger'] = trigger
            if trigger == 'periodic':
                self._state['last_periodic_started_at'] = started_at.isoformat()

    def _mark_finished(self, *, trigger: str, finished_at: datetime, summary: dict[str, Any]) -> None:
        with self._state_lock:
            self._state['running'] = False
            self._state['last_finished_at'] = finished_at.isoformat()
            self._state['last_trigger'] = trigger
            self._state['last_run'] = summary

    def _has_reconcile_mismatch(self, order_row: dict[str, Any]) -> bool:
        payload = order_row.get('last_reconcile_payload') or {}
        mismatch_reasons = payload.get('mismatch_reasons') or []
        return bool(mismatch_reasons) or _normalize_text(order_row.get('last_reconcile_status')).lower() == 'error'

    def _has_missing_fill_detail(self, order_row: dict[str, Any]) -> bool:
        status = _normalize_text(order_row.get('current_status')).lower()
        filled_quantity = _normalize_decimal(order_row.get('filled_quantity')) or Decimal('0')
        average_fill_price = _normalize_decimal(order_row.get('average_fill_price'))
        return (
            status in {BrokerOrderStatus.PARTIAL_FILL.value, BrokerOrderStatus.FILLED.value}
            and filled_quantity > Decimal('0')
            and average_fill_price is None
        )

    def _is_stale_pending(self, order_row: dict[str, Any], *, stale_after_minutes: int) -> bool:
        status = _normalize_text(order_row.get('current_status')).lower()
        if status not in _ACTIVE_STATUSES:
            return False
        last_update = (
            _normalize_datetime(order_row.get('last_broker_update_at'))
            or _normalize_datetime(order_row.get('updated_at'))
            or _normalize_datetime(order_row.get('created_at'))
        )
        if last_update is None:
            return False
        return _utc_now() - last_update >= timedelta(minutes=max(1, stale_after_minutes))


trading_order_reconciliation_service = TradingOrderReconciliationService()
