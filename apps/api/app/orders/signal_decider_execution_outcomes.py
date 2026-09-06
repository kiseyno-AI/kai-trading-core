from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Sequence

from sqlalchemy import inspect, select

from app.agent_context.roles import SHORT_TERM_DECIDER_AGENT_NAME, SIGNAL_DECIDER_AGENT_NAME
from app.db import session_scope
from app.db_models import (
    BrokerOrder,
    BrokerOrderEvent,
    OrderRequest,
    SignalDeciderActualExecutionOutcome,
    SignalDeciderDecisionSnapshot,
    SignalDeciderExecutionPlanItem,
)
from app.orders.domain import BrokerOrderStatus

EXECUTION_WINDOW_NEXT_OPEN = 'next_open'
EXECUTION_WINDOW_INTRADAY_AUTO_SELL = 'intraday_auto_sell'
ORDER_SOURCE_NEXT_OPEN = 'signal_decider_next_open'
ORDER_SOURCE_AUTO_SELL = 'signal_decider_auto_sell'
EXEC_INTENT_NEXT_OPEN_BUY = 'next_open_buy'
EXEC_INTENT_INTRADAY_WATCH_BUY = 'intraday_watch_buy'
EXEC_INTENT_TARGET_SELL = 'target_sell'
EXEC_INTENT_STOP_SELL = 'stop_sell'
LIFECYCLE_PLANNED = 'planned'
LIFECYCLE_DRY_RUN = 'dry_run'
LIFECYCLE_ENTRY_SUBMITTED = 'entry_submitted'
LIFECYCLE_ENTRY_SUBMIT_FAILED = 'entry_submit_failed'
LIFECYCLE_ENTRY_REJECTED = 'entry_rejected'
LIFECYCLE_ENTRY_CANCELED = 'entry_canceled'
LIFECYCLE_PARTIALLY_OPEN = 'partially_open'
LIFECYCLE_OPEN = 'open'
LIFECYCLE_EXIT_SUBMITTED = 'exit_submitted'
LIFECYCLE_PARTIALLY_CLOSED = 'partially_closed'
LIFECYCLE_CLOSED = 'closed'
LIFECYCLE_UNRESOLVED = 'unresolved'
EXIT_REASON_TARGET_HIT = 'target_hit'
EXIT_REASON_STOP_LOSS_HIT = 'stop_loss_hit'
EXIT_REASON_POSITION_OPEN = 'position_open'
EXIT_REASON_MANUAL_CANCEL = 'manual_cancel'
EXIT_REASON_ENTRY_REJECTED = 'entry_rejected'
EXIT_REASON_ENTRY_CANCELED = 'entry_canceled'
EXIT_REASON_ENTRY_SUBMIT_FAILED = 'entry_submit_failed'
EXIT_REASON_DRY_RUN = 'dry_run'
EXIT_REASON_UNRESOLVED = 'unresolved'

ACTIVE_ORDER_STATUSES = {
    BrokerOrderStatus.PENDING_SUBMIT.value,
    BrokerOrderStatus.SUBMITTED.value,
    BrokerOrderStatus.ACCEPTED.value,
    BrokerOrderStatus.CANCEL_PENDING.value,
    BrokerOrderStatus.REPLACE_PENDING.value,
    BrokerOrderStatus.RECONCILE_ONLY.value,
    BrokerOrderStatus.PARTIAL_FILL.value,
}
FILLED_ORDER_STATUSES = {
    BrokerOrderStatus.PARTIAL_FILL.value,
    BrokerOrderStatus.FILLED.value,
}
TERMINAL_ORDER_STATUSES = {
    BrokerOrderStatus.CANCELED.value,
    BrokerOrderStatus.FILLED.value,
    BrokerOrderStatus.REJECTED.value,
    BrokerOrderStatus.REPLACED.value,
}
REMOTE_ACTIVE_STATUS_ALIASES = {
    'accepted',
    'active',
    'created',
    'monitoring',
    'open',
    'pending',
    'queued',
    'running',
    'scheduled',
    'submitted',
    'working',
}
REMOTE_PARTIAL_STATUS_ALIASES = {'partial', 'partial_fill', 'partially_filled'}
REMOTE_FILLED_STATUS_ALIASES = {'complete', 'completed', 'done', 'filled'}
REMOTE_CANCELLED_STATUS_ALIASES = {'canceled', 'cancelled'}
REMOTE_REJECTED_STATUS_ALIASES = {'declined', 'denied', 'expired', 'failed', 'not_approved', 'reject', 'rejected'}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_optional_text(value: Any, *, lowercase: bool = False) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    return normalized.lower() if lowercase else normalized


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None or value == '':
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _quantize_money(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)


def _quantize_return_pct(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return value.quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP)


def _decimal_to_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, 'f')


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = _normalize_optional_text(value)
    if text is None:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _to_json_number(value: Decimal | None) -> int | float | None:
    if value is None:
        return None
    if value == value.to_integral():
        return int(value)
    return float(value)


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    text = _normalize_optional_text(value)
    if text is None:
        return None
    normalized = text[:-1] + '+00:00' if text.endswith('Z') else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return _as_utc(parsed)


def _min_datetime(first: datetime | None, second: datetime | None) -> datetime | None:
    if first is None:
        return second
    if second is None:
        return first
    return min(first, second)


def _max_datetime(first: datetime | None, second: datetime | None) -> datetime | None:
    if first is None:
        return second
    if second is None:
        return first
    return max(first, second)


def _serialize_candidate_order_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        'client_order_id': row.get('client_order_id'),
        'source': row.get('source'),
        'submission_status': row.get('submission_status'),
        'order_status': row.get('order_status'),
        'filled_quantity': _decimal_to_text(_safe_decimal(row.get('filled_quantity'))),
        'average_fill_price': _to_json_number(_safe_decimal(row.get('average_fill_price'))),
        'submitted_at': row.get('submitted_at'),
        'last_fill_at': row.get('last_fill_at'),
    }


def _serialize_outcome_row(row: SignalDeciderActualExecutionOutcome) -> dict[str, Any]:
    diagnostics = row.diagnostics_payload if isinstance(row.diagnostics_payload, dict) else {}
    return {
        'outcome_id': row.id,
        'decision_snapshot_id': row.decision_snapshot_id,
        'entry_plan_item_id': row.entry_plan_item_id,
        'exit_plan_item_id': row.exit_plan_item_id,
        'entry_order_request_id': row.entry_order_request_id,
        'entry_broker_order_row_id': row.entry_broker_order_row_id,
        'exit_order_request_id': row.exit_order_request_id,
        'exit_broker_order_row_id': row.exit_broker_order_row_id,
        'broker_name': row.broker_name,
        'market_date': row.market_date.isoformat(),
        'source_market_date': row.source_market_date.isoformat(),
        'target_trade_date': row.target_trade_date.isoformat(),
        'account_reference': row.account_reference,
        'symbol': row.symbol,
        'policy_version': row.policy_version,
        'entry_side': row.entry_side,
        'exit_side': row.exit_side,
        'lifecycle_status': row.lifecycle_status,
        'exit_reason': row.exit_reason,
        'entry_submitted_at': _as_utc(row.entry_submitted_at).isoformat() if row.entry_submitted_at else None,
        'entry_filled_at': _as_utc(row.entry_filled_at).isoformat() if row.entry_filled_at else None,
        'exit_submitted_at': _as_utc(row.exit_submitted_at).isoformat() if row.exit_submitted_at else None,
        'exit_filled_at': _as_utc(row.exit_filled_at).isoformat() if row.exit_filled_at else None,
        'entry_filled_quantity': _decimal_to_text(_safe_decimal(row.entry_filled_quantity)),
        'exit_filled_quantity': _decimal_to_text(_safe_decimal(row.exit_filled_quantity)),
        'closed_quantity': _decimal_to_text(_safe_decimal(row.closed_quantity)),
        'open_quantity': _decimal_to_text(_safe_decimal(row.open_quantity)),
        'entry_average_fill_price': _to_json_number(_safe_decimal(row.entry_average_fill_price)),
        'exit_average_fill_price': _to_json_number(_safe_decimal(row.exit_average_fill_price)),
        'entry_notional_krw': _decimal_to_text(_safe_decimal(row.entry_notional_krw)),
        'exit_notional_krw': _decimal_to_text(_safe_decimal(row.exit_notional_krw)),
        'realized_pnl_krw': _decimal_to_text(_safe_decimal(row.realized_pnl_krw)),
        'realized_return_pct': _to_float(row.realized_return_pct),
        'entry_client_order_id': diagnostics.get('entry', {}).get('client_order_id'),
        'exit_client_order_ids': diagnostics.get('exit', {}).get('client_order_ids') or [],
        'diagnostics': diagnostics,
        'computed_at': _as_utc(row.computed_at).isoformat() if row.computed_at else None,
        'created_at': _as_utc(row.created_at).isoformat() if row.created_at else None,
        'updated_at': _as_utc(row.updated_at).isoformat() if row.updated_at else None,
    }


class SignalDeciderExecutionOutcomeService:
    def sync_entry_plan_item(self, *, entry_plan_item_id: int) -> dict[str, Any] | None:
        items = self.sync_entry_plan_items(entry_plan_item_ids=[entry_plan_item_id])
        return items[0] if items else None

    def sync_entry_plan_items(self, *, entry_plan_item_ids: Sequence[int]) -> list[dict[str, Any]]:
        normalized_ids = sorted({int(value) for value in entry_plan_item_ids if value is not None})
        if not normalized_ids:
            return []
        with session_scope() as session:
            if not self._outcome_table_exists(session=session):
                return []
            rows = session.execute(
                select(SignalDeciderExecutionPlanItem)
                .where(
                    SignalDeciderExecutionPlanItem.id.in_(normalized_ids),
                    SignalDeciderExecutionPlanItem.execution_window == EXECUTION_WINDOW_NEXT_OPEN,
                    SignalDeciderExecutionPlanItem.decision_agent_name.in_(
                        [SIGNAL_DECIDER_AGENT_NAME, SHORT_TERM_DECIDER_AGENT_NAME]
                    ),
                )
                .order_by(
                    SignalDeciderExecutionPlanItem.target_trade_date.desc(),
                    SignalDeciderExecutionPlanItem.account_reference.asc(),
                    SignalDeciderExecutionPlanItem.symbol.asc(),
                    SignalDeciderExecutionPlanItem.id.asc(),
                )
            ).scalars().all()
            synced_rows = [self._sync_entry_plan_row(session=session, entry_row=row) for row in rows]
            return [_serialize_outcome_row(row) for row in synced_rows if row is not None]

    def sync_for_decision_snapshot_ids(self, *, decision_snapshot_ids: Sequence[int]) -> list[dict[str, Any]]:
        normalized_ids = sorted({int(value) for value in decision_snapshot_ids if value is not None})
        if not normalized_ids:
            return []
        return self._sync_matching_entry_plans(decision_snapshot_ids=normalized_ids)

    def sync_for_target_trade_date(
        self,
        *,
        target_trade_date: str | date,
        account_reference: str | None = None,
        symbol: str | None = None,
    ) -> list[dict[str, Any]]:
        resolved_target_trade_date = (
            target_trade_date if isinstance(target_trade_date, date) else date.fromisoformat(str(target_trade_date))
        )
        return self._sync_matching_entry_plans(
            target_trade_date=resolved_target_trade_date,
            account_reference=_normalize_optional_text(account_reference),
            symbol=_normalize_optional_text(symbol),
        )

    def backfill_outcomes(
        self,
        *,
        market_date_from: str | date,
        market_date_to: str | date,
        symbol: str | None = None,
        account_reference: str | None = None,
        policy_version: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        resolved_market_date_from = (
            market_date_from if isinstance(market_date_from, date) else date.fromisoformat(str(market_date_from))
        )
        resolved_market_date_to = (
            market_date_to if isinstance(market_date_to, date) else date.fromisoformat(str(market_date_to))
        )
        if resolved_market_date_to < resolved_market_date_from:
            raise ValueError('market_date_to must be on or after market_date_from')

        resolved_limit = None if limit is None else max(1, int(limit))
        items = self._sync_matching_entry_plans(
            market_date_from=resolved_market_date_from,
            market_date_to=resolved_market_date_to,
            symbol=_normalize_optional_text(symbol),
            account_reference=_normalize_optional_text(account_reference),
            policy_version=_normalize_optional_text(policy_version),
            limit=resolved_limit,
        )
        return {
            'request': {
                'market_date_from': resolved_market_date_from.isoformat(),
                'market_date_to': resolved_market_date_to.isoformat(),
                'symbol': (_normalize_optional_text(symbol) or '').upper() or None,
                'account_reference': _normalize_optional_text(account_reference),
                'policy_version': _normalize_optional_text(policy_version),
                'limit': resolved_limit,
            },
            'summary': self.build_summary(items=items),
            'items': items,
        }

    def list_snapshot_outcomes(
        self,
        *,
        decision_snapshot_ids: Sequence[int],
        refresh: bool = False,
    ) -> list[dict[str, Any]]:
        normalized_ids = sorted({int(value) for value in decision_snapshot_ids if value is not None})
        if not normalized_ids:
            return []
        if refresh:
            self.sync_for_decision_snapshot_ids(decision_snapshot_ids=normalized_ids)
        with session_scope() as session:
            if not self._outcome_table_exists(session=session):
                return []
            rows = session.execute(
                select(SignalDeciderActualExecutionOutcome)
                .where(SignalDeciderActualExecutionOutcome.decision_snapshot_id.in_(normalized_ids))
                .order_by(
                    SignalDeciderActualExecutionOutcome.market_date.desc(),
                    SignalDeciderActualExecutionOutcome.target_trade_date.desc(),
                    SignalDeciderActualExecutionOutcome.account_reference.asc(),
                    SignalDeciderActualExecutionOutcome.symbol.asc(),
                    SignalDeciderActualExecutionOutcome.id.asc(),
                )
            ).scalars().all()
            return [_serialize_outcome_row(row) for row in rows]

    def list_outcomes(
        self,
        *,
        snapshot_id: int | None = None,
        market_date: str | date | None = None,
        market_date_from: str | date | None = None,
        market_date_to: str | date | None = None,
        symbol: str | None = None,
        account_reference: str | None = None,
        policy_version: str | None = None,
        lifecycle_status: str | None = None,
        exit_reason: str | None = None,
        refresh: bool = False,
        limit: int = 50,
    ) -> dict[str, Any]:
        normalized_snapshot_id = int(snapshot_id) if snapshot_id is not None else None
        normalized_market_date = market_date if isinstance(market_date, date) or market_date is None else date.fromisoformat(str(market_date))
        normalized_market_date_from = (
            market_date_from
            if isinstance(market_date_from, date) or market_date_from is None
            else date.fromisoformat(str(market_date_from))
        )
        normalized_market_date_to = (
            market_date_to
            if isinstance(market_date_to, date) or market_date_to is None
            else date.fromisoformat(str(market_date_to))
        )
        normalized_symbol = _normalize_optional_text(symbol)
        normalized_account_reference = _normalize_optional_text(account_reference)
        normalized_policy_version = _normalize_optional_text(policy_version)
        normalized_lifecycle_status = _normalize_optional_text(lifecycle_status, lowercase=True)
        normalized_exit_reason = _normalize_optional_text(exit_reason, lowercase=True)
        resolved_limit = max(1, int(limit))

        if refresh:
            self._sync_matching_entry_plans(
                decision_snapshot_ids=[normalized_snapshot_id] if normalized_snapshot_id is not None else None,
                market_date=normalized_market_date,
                market_date_from=normalized_market_date_from,
                market_date_to=normalized_market_date_to,
                symbol=normalized_symbol,
                account_reference=normalized_account_reference,
                policy_version=normalized_policy_version,
                limit=resolved_limit,
            )

        request_payload = {
            'snapshot_id': normalized_snapshot_id,
            'market_date': normalized_market_date.isoformat() if normalized_market_date else None,
            'market_date_from': normalized_market_date_from.isoformat() if normalized_market_date_from else None,
            'market_date_to': normalized_market_date_to.isoformat() if normalized_market_date_to else None,
            'symbol': normalized_symbol.upper() if normalized_symbol else None,
            'account_reference': normalized_account_reference,
            'policy_version': normalized_policy_version,
            'lifecycle_status': normalized_lifecycle_status,
            'exit_reason': normalized_exit_reason,
            'refresh': refresh,
            'limit': resolved_limit,
        }
        with session_scope() as session:
            if not self._outcome_table_exists(session=session):
                items: list[dict[str, Any]] = []
                return {
                    'request': request_payload,
                    'summary': self.build_summary(items=items),
                    'items': items,
                }
            stmt = (
                select(SignalDeciderActualExecutionOutcome)
                .order_by(
                    SignalDeciderActualExecutionOutcome.market_date.desc(),
                    SignalDeciderActualExecutionOutcome.target_trade_date.desc(),
                    SignalDeciderActualExecutionOutcome.account_reference.asc(),
                    SignalDeciderActualExecutionOutcome.symbol.asc(),
                    SignalDeciderActualExecutionOutcome.id.desc(),
                )
            )
            if normalized_snapshot_id is not None:
                stmt = stmt.where(SignalDeciderActualExecutionOutcome.decision_snapshot_id == normalized_snapshot_id)
            if normalized_market_date is not None:
                stmt = stmt.where(SignalDeciderActualExecutionOutcome.market_date == normalized_market_date)
            if normalized_market_date_from is not None:
                stmt = stmt.where(SignalDeciderActualExecutionOutcome.market_date >= normalized_market_date_from)
            if normalized_market_date_to is not None:
                stmt = stmt.where(SignalDeciderActualExecutionOutcome.market_date <= normalized_market_date_to)
            if normalized_symbol:
                stmt = stmt.where(SignalDeciderActualExecutionOutcome.symbol == normalized_symbol.upper())
            if normalized_account_reference:
                stmt = stmt.where(SignalDeciderActualExecutionOutcome.account_reference == normalized_account_reference)
            if normalized_policy_version:
                stmt = stmt.where(SignalDeciderActualExecutionOutcome.policy_version == normalized_policy_version)
            if normalized_lifecycle_status:
                stmt = stmt.where(SignalDeciderActualExecutionOutcome.lifecycle_status == normalized_lifecycle_status)
            if normalized_exit_reason:
                stmt = stmt.where(SignalDeciderActualExecutionOutcome.exit_reason == normalized_exit_reason)
            stmt = stmt.limit(resolved_limit)
            rows = session.execute(stmt).scalars().all()
            items = [_serialize_outcome_row(row) for row in rows]

        return {
            'request': request_payload,
            'summary': self.build_summary(items=items),
            'items': items,
        }

    def build_summary(self, *, items: Sequence[dict[str, Any]]) -> dict[str, Any]:
        lifecycle_counts = Counter(
            str(item.get('lifecycle_status') or '')
            for item in items
            if item.get('lifecycle_status')
        )
        exit_reason_counts = Counter(
            str(item.get('exit_reason') or '')
            for item in items
            if item.get('exit_reason')
        )
        realized_pnl_total = sum(
            ((_safe_decimal(item.get('realized_pnl_krw')) or Decimal('0')) for item in items),
            start=Decimal('0'),
        )
        closed_quantity_total = sum(
            ((_safe_decimal(item.get('closed_quantity')) or Decimal('0')) for item in items),
            start=Decimal('0'),
        )
        open_quantity_total = sum(
            ((_safe_decimal(item.get('open_quantity')) or Decimal('0')) for item in items),
            start=Decimal('0'),
        )
        return {
            'count': len(items),
            'closed_count': sum(1 for item in items if item.get('lifecycle_status') == LIFECYCLE_CLOSED),
            'open_count': sum(
                1
                for item in items
                if item.get('lifecycle_status')
                in {
                    LIFECYCLE_PARTIALLY_OPEN,
                    LIFECYCLE_OPEN,
                    LIFECYCLE_EXIT_SUBMITTED,
                    LIFECYCLE_PARTIALLY_CLOSED,
                }
            ),
            'unresolved_count': sum(1 for item in items if item.get('lifecycle_status') == LIFECYCLE_UNRESOLVED),
            'rejected_count': sum(1 for item in items if item.get('lifecycle_status') == LIFECYCLE_ENTRY_REJECTED),
            'lifecycle_status_counts': dict(sorted(lifecycle_counts.items())),
            'exit_reason_counts': dict(sorted(exit_reason_counts.items())),
            'total_closed_quantity': _decimal_to_text(closed_quantity_total),
            'total_open_quantity': _decimal_to_text(open_quantity_total),
            'total_realized_pnl_krw': _decimal_to_text(_quantize_money(realized_pnl_total)),
        }

    def _sync_matching_entry_plans(
        self,
        *,
        decision_snapshot_ids: Sequence[int] | None = None,
        target_trade_date: date | None = None,
        market_date: date | None = None,
        market_date_from: date | None = None,
        market_date_to: date | None = None,
        symbol: str | None = None,
        account_reference: str | None = None,
        policy_version: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        with session_scope() as session:
            if not self._outcome_table_exists(session=session):
                return []
            stmt = (
                select(SignalDeciderExecutionPlanItem)
                .where(
                    SignalDeciderExecutionPlanItem.execution_window == EXECUTION_WINDOW_NEXT_OPEN,
                    SignalDeciderExecutionPlanItem.decision_agent_name.in_(
                        [SIGNAL_DECIDER_AGENT_NAME, SHORT_TERM_DECIDER_AGENT_NAME]
                    ),
                )
                .order_by(
                    SignalDeciderExecutionPlanItem.target_trade_date.desc(),
                    SignalDeciderExecutionPlanItem.account_reference.asc(),
                    SignalDeciderExecutionPlanItem.symbol.asc(),
                    SignalDeciderExecutionPlanItem.id.asc(),
                )
            )
            if decision_snapshot_ids:
                stmt = stmt.where(SignalDeciderExecutionPlanItem.decision_snapshot_id.in_(decision_snapshot_ids))
            if target_trade_date is not None:
                stmt = stmt.where(SignalDeciderExecutionPlanItem.target_trade_date == target_trade_date)
            if market_date is not None:
                stmt = stmt.where(SignalDeciderExecutionPlanItem.source_market_date == market_date)
            if market_date_from is not None:
                stmt = stmt.where(SignalDeciderExecutionPlanItem.source_market_date >= market_date_from)
            if market_date_to is not None:
                stmt = stmt.where(SignalDeciderExecutionPlanItem.source_market_date <= market_date_to)
            if symbol:
                stmt = stmt.where(SignalDeciderExecutionPlanItem.symbol == symbol.upper())
            if account_reference:
                stmt = stmt.where(SignalDeciderExecutionPlanItem.account_reference == account_reference)
            if policy_version:
                stmt = stmt.where(SignalDeciderExecutionPlanItem.policy_version == policy_version)
            if limit is not None:
                stmt = stmt.limit(max(1, int(limit)))
            rows = session.execute(stmt).scalars().all()
            synced_rows = [self._sync_entry_plan_row(session=session, entry_row=row) for row in rows]
            return [_serialize_outcome_row(row) for row in synced_rows if row is not None]

    def _outcome_table_exists(self, *, session) -> bool:
        bind = session.get_bind()
        return inspect(bind).has_table(SignalDeciderActualExecutionOutcome.__tablename__)

    def _sync_entry_plan_row(
        self,
        *,
        session,
        entry_row: SignalDeciderExecutionPlanItem,
    ) -> SignalDeciderActualExecutionOutcome | None:
        snapshot = session.get(SignalDeciderDecisionSnapshot, entry_row.decision_snapshot_id)
        if snapshot is None:
            return None

        exit_row = self._load_exit_plan_row(session=session, entry_row=entry_row)
        entry_request = self._load_entry_request(session=session, entry_row=entry_row)
        entry_order = self._load_entry_order(session=session, entry_row=entry_row)
        entry_events = self._load_order_events(session=session, broker_order_row=entry_order)
        entry_pairs = [
            {
                'request': entry_request,
                'order': entry_order,
                'events': entry_events,
                'client_order_id': _normalize_optional_text(entry_row.client_order_id),
                'source': ORDER_SOURCE_NEXT_OPEN,
                'trigger_rule': None,
            }
        ]
        entry_local_metrics = self._aggregate_order_pairs(pairs=entry_pairs)
        entry_readback_metrics = self._aggregate_exec_readback_side(entry_row=entry_row, side='buy')
        entry_metrics = self._select_execution_metrics(
            local_metrics=entry_local_metrics,
            readback_metrics=entry_readback_metrics,
        )
        exit_pairs = self._load_linked_exit_pairs(
            session=session,
            entry_row=entry_row,
            exit_row=exit_row,
        )
        exit_local_metrics = self._aggregate_order_pairs(pairs=exit_pairs)
        exit_readback_metrics = self._aggregate_exec_readback_side(entry_row=entry_row, side='sell')
        exit_metrics = self._select_execution_metrics(
            local_metrics=exit_local_metrics,
            readback_metrics=exit_readback_metrics,
        )
        manual_exit_candidates = self._load_manual_exit_candidates(
            session=session,
            entry_row=entry_row,
            excluded_client_order_ids=set(exit_metrics['client_order_ids']),
            after_timestamp=entry_metrics['submitted_at'],
        )
        manual_candidate_fill_quantity = sum(
            (_safe_decimal(item.get('filled_quantity')) or Decimal('0'))
            for item in manual_exit_candidates
        )
        exit_trigger_rule = self._resolve_exit_trigger_rule(
            exit_row=exit_row,
            exit_pairs=exit_pairs,
        ) or self._resolve_exec_readback_exit_trigger_rule(entry_row=entry_row)

        entry_filled_quantity = entry_metrics['filled_quantity']
        exit_filled_quantity = exit_metrics['filled_quantity']
        closed_quantity = min(entry_filled_quantity, exit_filled_quantity)
        open_quantity = max(entry_filled_quantity - closed_quantity, Decimal('0'))
        entry_average_fill_price = entry_metrics['average_fill_price']
        exit_average_fill_price = exit_metrics['average_fill_price']
        entry_notional = _quantize_money(
            entry_filled_quantity * entry_average_fill_price
            if entry_filled_quantity > Decimal('0') and entry_average_fill_price is not None
            else None
        )
        exit_notional = _quantize_money(
            closed_quantity * exit_average_fill_price
            if closed_quantity > Decimal('0') and exit_average_fill_price is not None
            else None
        )
        realized_pnl = _quantize_money(
            (exit_average_fill_price - entry_average_fill_price) * closed_quantity
            if closed_quantity > Decimal('0')
            and entry_average_fill_price is not None
            and exit_average_fill_price is not None
            else None
        )
        realized_return_pct = self._return_pct(
            exit_average_fill_price,
            entry_average_fill_price,
        ) if closed_quantity > Decimal('0') else None

        lifecycle_status, exit_reason = self._resolve_lifecycle_and_exit_reason(
            entry_row=entry_row,
            entry_request=entry_request,
            entry_order=entry_order,
            entry_metrics=entry_metrics,
            entry_filled_quantity=entry_filled_quantity,
            exit_row=exit_row,
            exit_trigger_rule=exit_trigger_rule,
            exit_metrics=exit_metrics,
            closed_quantity=closed_quantity,
            open_quantity=open_quantity,
            manual_candidate_fill_quantity=manual_candidate_fill_quantity,
        )

        diagnostics = {
            'entry': {
                'client_order_id': entry_metrics['latest_client_order_id'] or _normalize_optional_text(entry_row.client_order_id),
                'order_request_id': entry_metrics['latest_order_request_id'] or entry_row.order_request_id,
                'broker_order_row_id': entry_metrics['latest_broker_order_row_id'] or entry_row.broker_order_row_id,
                'broker_order_id': entry_metrics['latest_broker_order_id'],
                'request_status': entry_metrics['latest_request_status'],
                'order_status': entry_metrics['latest_order_status'],
                'fill_event_ids': entry_metrics['fill_event_ids'],
                'pair_count': entry_metrics['pair_count'],
                'metric_source': entry_metrics.get('metric_source'),
                'exec_readback': self._serialize_exec_readback_metrics(entry_readback_metrics),
            },
            'exit': {
                'plan_item_id': exit_row.id if exit_row is not None else None,
                'client_order_ids': exit_metrics['client_order_ids'],
                'order_request_ids': exit_metrics['order_request_ids'],
                'broker_order_row_ids': exit_metrics['broker_order_row_ids'],
                'broker_order_ids': exit_metrics['broker_order_ids'],
                'request_status': exit_metrics['latest_request_status'],
                'order_status': exit_metrics['latest_order_status'],
                'fill_event_ids': exit_metrics['fill_event_ids'],
                'trigger_rule': exit_trigger_rule,
                'pair_count': exit_metrics['pair_count'],
                'metric_source': exit_metrics.get('metric_source'),
                'exec_readback': self._serialize_exec_readback_metrics(exit_readback_metrics),
            },
            'snapshot': {
                'decision_snapshot_id': snapshot.id,
                'target_price': snapshot.target_price,
                'stop_loss_price': snapshot.stop_loss_price,
                'expected_return_pct': _to_float(snapshot.expected_return_pct),
                'suggested_holding_period': snapshot.suggested_holding_period,
            },
            'plan': {
                'entry_plan_item_id': entry_row.id,
                'entry_execution_status': entry_row.execution_status,
                'entry_eligibility_status': entry_row.eligibility_status,
                'exit_plan_item_id': exit_row.id if exit_row is not None else None,
                'exit_execution_status': exit_row.execution_status if exit_row is not None else None,
                'exit_eligibility_status': exit_row.eligibility_status if exit_row is not None else None,
            },
            'attribution': {
                'manual_exit_candidate_count': len(manual_exit_candidates),
                'manual_exit_candidate_filled_quantity': _decimal_to_text(manual_candidate_fill_quantity),
                'manual_exit_candidates': [
                    _serialize_candidate_order_row(item)
                    for item in manual_exit_candidates[:10]
                ],
                'linked_exit_orders_only': True,
            },
        }

        item = {
            'decision_snapshot_id': snapshot.id,
            'entry_plan_item_id': entry_row.id,
            'exit_plan_item_id': exit_row.id if exit_row is not None else None,
            'entry_order_request_id': entry_metrics['latest_order_request_id'] or entry_row.order_request_id,
            'entry_broker_order_row_id': entry_metrics['latest_broker_order_row_id'] or entry_row.broker_order_row_id,
            'exit_order_request_id': exit_metrics['latest_order_request_id'] or (exit_row.order_request_id if exit_row is not None else None),
            'exit_broker_order_row_id': exit_metrics['latest_broker_order_row_id'] or (exit_row.broker_order_row_id if exit_row is not None else None),
            'broker_name': entry_row.broker_name,
            'market_date': snapshot.market_date,
            'source_market_date': entry_row.source_market_date,
            'target_trade_date': entry_row.target_trade_date,
            'account_reference': entry_row.account_reference,
            'symbol': entry_row.symbol,
            'policy_version': entry_row.policy_version,
            'entry_side': entry_row.planned_side,
            'exit_side': exit_row.planned_side if exit_row is not None else 'sell',
            'lifecycle_status': lifecycle_status,
            'exit_reason': exit_reason,
            'entry_submitted_at': entry_metrics['submitted_at'] or _as_utc(entry_row.submitted_at),
            'entry_filled_at': entry_metrics['first_fill_at'],
            'exit_submitted_at': exit_metrics['submitted_at'] or (_as_utc(exit_row.submitted_at) if exit_row is not None else None),
            'exit_filled_at': exit_metrics['last_fill_at'],
            'entry_filled_quantity': entry_filled_quantity,
            'exit_filled_quantity': exit_filled_quantity,
            'closed_quantity': closed_quantity,
            'open_quantity': open_quantity,
            'entry_average_fill_price': entry_average_fill_price,
            'exit_average_fill_price': exit_average_fill_price,
            'entry_notional_krw': entry_notional,
            'exit_notional_krw': exit_notional,
            'realized_pnl_krw': realized_pnl,
            'realized_return_pct': realized_return_pct,
            'diagnostics_payload': diagnostics,
            'computed_at': _utcnow(),
        }
        return self._upsert_outcome_row(session=session, item=item)

    def _load_exit_plan_row(self, *, session, entry_row: SignalDeciderExecutionPlanItem) -> SignalDeciderExecutionPlanItem | None:
        return session.execute(
            select(SignalDeciderExecutionPlanItem).where(
                SignalDeciderExecutionPlanItem.execution_window == EXECUTION_WINDOW_INTRADAY_AUTO_SELL,
                SignalDeciderExecutionPlanItem.decision_snapshot_id == entry_row.decision_snapshot_id,
                SignalDeciderExecutionPlanItem.target_trade_date == entry_row.target_trade_date,
                SignalDeciderExecutionPlanItem.account_reference == entry_row.account_reference,
                SignalDeciderExecutionPlanItem.symbol == entry_row.symbol,
            )
        ).scalar_one_or_none()

    def _load_entry_request(self, *, session, entry_row: SignalDeciderExecutionPlanItem) -> OrderRequest | None:
        if entry_row.order_request_id is not None:
            row = session.get(OrderRequest, int(entry_row.order_request_id))
            if row is not None:
                return row
        client_order_id = _normalize_optional_text(entry_row.client_order_id)
        if client_order_id is None:
            return None
        return session.execute(
            select(OrderRequest).where(
                OrderRequest.account_reference == entry_row.account_reference,
                OrderRequest.client_order_id == client_order_id,
            )
        ).scalar_one_or_none()

    def _load_entry_order(self, *, session, entry_row: SignalDeciderExecutionPlanItem) -> BrokerOrder | None:
        if entry_row.broker_order_row_id is not None:
            row = session.get(BrokerOrder, int(entry_row.broker_order_row_id))
            if row is not None:
                return row
        client_order_id = _normalize_optional_text(entry_row.client_order_id)
        if client_order_id is None:
            return None
        return session.execute(
            select(BrokerOrder).where(
                BrokerOrder.account_reference == entry_row.account_reference,
                BrokerOrder.client_order_id == client_order_id,
            )
        ).scalar_one_or_none()

    def _load_order_events(self, *, session, broker_order_row: BrokerOrder | None) -> list[BrokerOrderEvent]:
        if broker_order_row is None:
            return []
        return session.execute(
            select(BrokerOrderEvent)
            .where(BrokerOrderEvent.broker_order_row_id == broker_order_row.id)
            .order_by(BrokerOrderEvent.event_occurred_at.asc(), BrokerOrderEvent.id.asc())
        ).scalars().all()

    def _load_linked_exit_pairs(
        self,
        *,
        session,
        entry_row: SignalDeciderExecutionPlanItem,
        exit_row: SignalDeciderExecutionPlanItem | None,
    ) -> list[dict[str, Any]]:
        pairs: list[dict[str, Any]] = []
        seen_client_order_ids: set[str] = set()
        request_rows = session.execute(
            select(OrderRequest)
            .where(
                OrderRequest.account_reference == entry_row.account_reference,
                OrderRequest.symbol == entry_row.symbol,
                OrderRequest.side == 'sell',
            )
            .order_by(OrderRequest.submitted_at.asc().nullslast(), OrderRequest.created_at.asc(), OrderRequest.id.asc())
        ).scalars().all()
        for request_row in request_rows:
            strategy_payload = request_row.strategy_context_payload if isinstance(request_row.strategy_context_payload, dict) else {}
            auto_sell_payload = strategy_payload.get('signal_decider_intraday_auto_sell') or {}
            if int(auto_sell_payload.get('decision_snapshot_id') or 0) != int(entry_row.decision_snapshot_id):
                continue
            client_order_id = _normalize_optional_text(request_row.client_order_id)
            if client_order_id is None or client_order_id in seen_client_order_ids:
                continue
            seen_client_order_ids.add(client_order_id)
            order_row = session.execute(
                select(BrokerOrder).where(
                    BrokerOrder.account_reference == entry_row.account_reference,
                    BrokerOrder.client_order_id == client_order_id,
                )
            ).scalar_one_or_none()
            pairs.append(
                {
                    'request': request_row,
                    'order': order_row,
                    'events': self._load_order_events(session=session, broker_order_row=order_row),
                    'client_order_id': client_order_id,
                    'source': request_row.source,
                    'trigger_rule': _normalize_optional_text(auto_sell_payload.get('trigger_rule'), lowercase=True),
                }
            )

        if exit_row is not None:
            client_order_id = _normalize_optional_text(exit_row.client_order_id)
            if client_order_id and client_order_id not in seen_client_order_ids:
                request_row = self._load_entry_request(session=session, entry_row=exit_row)
                order_row = self._load_entry_order(session=session, entry_row=exit_row)
                pairs.append(
                    {
                        'request': request_row,
                        'order': order_row,
                        'events': self._load_order_events(session=session, broker_order_row=order_row),
                        'client_order_id': client_order_id,
                        'source': request_row.source if request_row is not None else ORDER_SOURCE_AUTO_SELL,
                        'trigger_rule': self._resolve_exit_trigger_rule(exit_row=exit_row, exit_pairs=[]),
                    }
                )

        pairs.sort(
            key=self._pair_sort_timestamp,
        )
        return pairs

    def _load_manual_exit_candidates(
        self,
        *,
        session,
        entry_row: SignalDeciderExecutionPlanItem,
        excluded_client_order_ids: set[str],
        after_timestamp: datetime | None,
    ) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        request_rows = session.execute(
            select(OrderRequest)
            .where(
                OrderRequest.account_reference == entry_row.account_reference,
                OrderRequest.symbol == entry_row.symbol,
                OrderRequest.side == 'sell',
            )
            .order_by(OrderRequest.submitted_at.asc().nullslast(), OrderRequest.created_at.asc(), OrderRequest.id.asc())
        ).scalars().all()
        for request_row in request_rows:
            client_order_id = _normalize_optional_text(request_row.client_order_id)
            if client_order_id is None or client_order_id in excluded_client_order_ids:
                continue
            strategy_payload = request_row.strategy_context_payload if isinstance(request_row.strategy_context_payload, dict) else {}
            if strategy_payload.get('signal_decider_intraday_auto_sell'):
                continue
            order_row = session.execute(
                select(BrokerOrder).where(
                    BrokerOrder.account_reference == entry_row.account_reference,
                    BrokerOrder.client_order_id == client_order_id,
                )
            ).scalar_one_or_none()
            submitted_at = _as_utc(request_row.submitted_at) or (_as_utc(order_row.first_submitted_at) if order_row is not None else None)
            if after_timestamp is not None and submitted_at is not None and submitted_at < after_timestamp:
                continue
            order_status = _normalize_optional_text(order_row.current_status if order_row is not None else None, lowercase=True)
            filled_quantity = _safe_decimal(order_row.filled_quantity if order_row is not None else None) or Decimal('0')
            if filled_quantity <= Decimal('0') and order_status not in {BrokerOrderStatus.CANCELED.value, BrokerOrderStatus.REJECTED.value}:
                continue
            candidates.append(
                {
                    'client_order_id': client_order_id,
                    'source': request_row.source,
                    'submission_status': _normalize_optional_text(request_row.submission_status, lowercase=True),
                    'order_status': order_status,
                    'filled_quantity': filled_quantity,
                    'average_fill_price': _safe_decimal(order_row.average_fill_price if order_row is not None else None),
                    'submitted_at': submitted_at.isoformat() if submitted_at else None,
                    'last_fill_at': (
                        _as_utc(order_row.last_fill_at).isoformat()
                        if order_row is not None and _as_utc(order_row.last_fill_at) is not None
                        else None
                    ),
                }
            )
        return candidates

    def _aggregate_order_pairs(self, *, pairs: Sequence[dict[str, Any]]) -> dict[str, Any]:
        filled_quantity_total = Decimal('0')
        weighted_price_notional = Decimal('0')
        submitted_at: datetime | None = None
        first_fill_at: datetime | None = None
        last_fill_at: datetime | None = None
        latest_pair_sort_at: datetime | None = None
        latest_request_status: str | None = None
        latest_order_status: str | None = None
        latest_order_request_id: int | None = None
        latest_broker_order_row_id: int | None = None
        latest_client_order_id: str | None = None
        latest_broker_order_id: str | None = None
        has_terminal_order_evidence = False
        client_order_ids: list[str] = []
        broker_order_ids: list[str] = []
        order_request_ids: list[int] = []
        broker_order_row_ids: list[int] = []
        fill_event_ids: list[int] = []

        for pair in pairs:
            request_row = pair.get('request')
            order_row = pair.get('order')
            events = pair.get('events') or []
            client_order_id = _normalize_optional_text(
                pair.get('client_order_id')
                or (request_row.client_order_id if request_row is not None else None)
                or (order_row.client_order_id if order_row is not None else None)
            )
            if client_order_id and client_order_id not in client_order_ids:
                client_order_ids.append(client_order_id)
            if request_row is not None and request_row.id not in order_request_ids:
                order_request_ids.append(int(request_row.id))
            if order_row is not None and order_row.id not in broker_order_row_ids:
                broker_order_row_ids.append(int(order_row.id))
            broker_order_id = _normalize_optional_text(order_row.broker_order_id if order_row is not None else None)
            if broker_order_id and broker_order_id not in broker_order_ids:
                broker_order_ids.append(broker_order_id)

            pair_submitted_at = (
                _as_utc(request_row.submitted_at) if request_row is not None else None
            ) or (
                _as_utc(order_row.first_submitted_at) if order_row is not None else None
            ) or (
                _as_utc(request_row.created_at) if request_row is not None else None
            )
            submitted_at = _min_datetime(submitted_at, pair_submitted_at)

            order_filled_quantity = _safe_decimal(order_row.filled_quantity if order_row is not None else None) or Decimal('0')
            average_fill_price = _safe_decimal(order_row.average_fill_price if order_row is not None else None)
            if order_filled_quantity > Decimal('0'):
                filled_quantity_total += order_filled_quantity
                resolved_average_price = average_fill_price or self._weighted_average_from_events(events=events)
                if resolved_average_price is not None:
                    weighted_price_notional += order_filled_quantity * resolved_average_price

            pair_fill_event_ids, pair_first_fill_at, pair_last_fill_at = self._extract_fill_event_info(
                events=events,
                fallback_fill_at=_as_utc(order_row.last_fill_at) if order_row is not None else None,
                filled_quantity=order_filled_quantity,
            )
            fill_event_ids.extend(pair_fill_event_ids)
            first_fill_at = _min_datetime(first_fill_at, pair_first_fill_at)
            last_fill_at = _max_datetime(last_fill_at, pair_last_fill_at)

            pair_sort_at = pair_submitted_at or (
                _as_utc(order_row.last_broker_update_at) if order_row is not None else None
            ) or (
                _as_utc(request_row.updated_at) if request_row is not None else None
            ) or datetime(1970, 1, 1, tzinfo=timezone.utc)
            if latest_pair_sort_at is None or pair_sort_at >= latest_pair_sort_at:
                latest_pair_sort_at = pair_sort_at
                latest_request_status = _normalize_optional_text(
                    request_row.submission_status if request_row is not None else None,
                    lowercase=True,
                )
                latest_order_status = _normalize_optional_text(
                    order_row.current_status if order_row is not None else None,
                    lowercase=True,
                )
                latest_order_request_id = int(request_row.id) if request_row is not None else None
                latest_broker_order_row_id = int(order_row.id) if order_row is not None else None
                latest_client_order_id = client_order_id
                latest_broker_order_id = broker_order_id
                has_terminal_order_evidence = self._has_terminal_order_evidence(
                    status=latest_order_status,
                    requested_quantity=(
                        order_row.requested_quantity
                        if order_row is not None
                        else request_row.requested_quantity
                        if request_row is not None
                        else None
                    ),
                    filled_quantity=order_filled_quantity,
                    remaining_quantity=(order_row.remaining_quantity if order_row is not None else None),
                )

        average_fill_price = (
            weighted_price_notional / filled_quantity_total
            if filled_quantity_total > Decimal('0')
            else None
        )
        return {
            'filled_quantity': filled_quantity_total,
            'average_fill_price': average_fill_price,
            'submitted_at': submitted_at,
            'first_fill_at': first_fill_at,
            'last_fill_at': last_fill_at,
            'latest_request_status': latest_request_status,
            'latest_order_status': latest_order_status,
            'latest_order_request_id': latest_order_request_id,
            'latest_broker_order_row_id': latest_broker_order_row_id,
            'latest_client_order_id': latest_client_order_id,
            'latest_broker_order_id': latest_broker_order_id,
            'client_order_ids': client_order_ids,
            'broker_order_ids': broker_order_ids,
            'order_request_ids': order_request_ids,
            'broker_order_row_ids': broker_order_row_ids,
            'fill_event_ids': fill_event_ids,
            'pair_count': len(pairs),
            'has_terminal_order_evidence': has_terminal_order_evidence,
        }

    def _aggregate_exec_readback_side(
        self,
        *,
        entry_row: SignalDeciderExecutionPlanItem,
        side: str,
    ) -> dict[str, Any]:
        readback = self._load_exec_readback(entry_row=entry_row)
        orders = self._readback_items(readback=readback, key='orders')
        executions = self._readback_items(readback=readback, key='executions')
        item_links = self._load_exec_item_links(entry_row=entry_row)
        order_link_by_broker_order_id = self._readback_order_link_by_broker_order_id(
            orders=orders,
            item_links=item_links,
        )
        execution_facts = [
            fact
            for payload in executions
            if (
                fact := self._exec_readback_fact(
                    payload=payload,
                    side=side,
                    source='execution',
                    item_links=item_links,
                    order_link_by_broker_order_id=order_link_by_broker_order_id,
                )
            )
        ]
        order_facts = [
            fact
            for payload in orders
            if (
                fact := self._exec_readback_fact(
                    payload=payload,
                    side=side,
                    source='order',
                    item_links=item_links,
                    order_link_by_broker_order_id=order_link_by_broker_order_id,
                )
            )
        ]
        execution_quantity_total, execution_weighted_price_notional = self._readback_fact_totals(
            facts=execution_facts,
        )
        order_quantity_total, order_weighted_price_notional = self._readback_fact_totals(
            facts=order_facts,
        )
        if execution_facts and execution_quantity_total >= order_quantity_total:
            selected_facts = execution_facts
            metadata_facts = execution_facts
            filled_quantity_total = execution_quantity_total
            weighted_price_notional = execution_weighted_price_notional
            readback_source = 'executions'
        elif order_facts:
            selected_facts = order_facts
            metadata_facts = [*execution_facts, *order_facts] if execution_facts else order_facts
            filled_quantity_total = order_quantity_total
            weighted_price_notional = order_weighted_price_notional
            readback_source = 'orders'
        else:
            selected_facts = []
            metadata_facts = []
            filled_quantity_total = Decimal('0')
            weighted_price_notional = Decimal('0')
            readback_source = None
        submitted_at: datetime | None = None
        first_fill_at: datetime | None = None
        last_fill_at: datetime | None = None
        latest_sort_at: datetime | None = None
        latest_order_status: str | None = None
        latest_client_order_id: str | None = None
        latest_broker_order_id: str | None = None
        client_order_ids: list[str] = []
        broker_order_ids: list[str] = []
        remote_execution_ids: list[str] = []
        remote_order_ids: list[str] = []
        remote_exec_plan_item_ids: list[str] = []

        for fact in metadata_facts:
            submitted_at = _min_datetime(submitted_at, fact.get('submitted_at'))
            first_fill_at = _min_datetime(first_fill_at, fact.get('filled_at'))
            last_fill_at = _max_datetime(last_fill_at, fact.get('filled_at'))
            if client_order_id := _normalize_optional_text(fact.get('client_order_id')):
                if client_order_id not in client_order_ids:
                    client_order_ids.append(client_order_id)
            if broker_order_id := _normalize_optional_text(fact.get('broker_order_id')):
                if broker_order_id not in broker_order_ids:
                    broker_order_ids.append(broker_order_id)
            if execution_id := _normalize_optional_text(fact.get('execution_id')):
                if execution_id not in remote_execution_ids:
                    remote_execution_ids.append(execution_id)
            if order_id := _normalize_optional_text(fact.get('order_id')):
                if order_id not in remote_order_ids:
                    remote_order_ids.append(order_id)
            if exec_plan_item_id := _normalize_optional_text(fact.get('exec_plan_item_id')):
                if exec_plan_item_id not in remote_exec_plan_item_ids:
                    remote_exec_plan_item_ids.append(exec_plan_item_id)
            sort_at = fact.get('sort_at') or datetime(1970, 1, 1, tzinfo=timezone.utc)
            if latest_sort_at is None or sort_at >= latest_sort_at:
                latest_sort_at = sort_at
                latest_order_status = _normalize_optional_text(fact.get('status'), lowercase=True)
                latest_client_order_id = _normalize_optional_text(fact.get('client_order_id'))
                latest_broker_order_id = _normalize_optional_text(fact.get('broker_order_id'))

        average_fill_price = None
        if filled_quantity_total > Decimal('0'):
            if weighted_price_notional > Decimal('0'):
                average_fill_price = weighted_price_notional / filled_quantity_total
            else:
                fallback_quantity_total = (
                    execution_quantity_total if readback_source == 'orders' else order_quantity_total
                )
                fallback_weighted_price_notional = (
                    execution_weighted_price_notional
                    if readback_source == 'orders'
                    else order_weighted_price_notional
                )
                if fallback_quantity_total > Decimal('0') and fallback_weighted_price_notional > Decimal('0'):
                    average_fill_price = fallback_weighted_price_notional / fallback_quantity_total
        if latest_order_status is None and filled_quantity_total > Decimal('0'):
            latest_order_status = BrokerOrderStatus.FILLED.value
        has_terminal_order_evidence = self._readback_has_terminal_order_evidence(
            side=side,
            order_facts=order_facts,
            item_links=item_links,
        )
        return {
            'filled_quantity': filled_quantity_total,
            'average_fill_price': average_fill_price,
            'submitted_at': submitted_at,
            'first_fill_at': first_fill_at,
            'last_fill_at': last_fill_at,
            'latest_request_status': None,
            'latest_order_status': latest_order_status,
            'latest_order_request_id': None,
            'latest_broker_order_row_id': None,
            'latest_client_order_id': latest_client_order_id,
            'latest_broker_order_id': latest_broker_order_id,
            'client_order_ids': client_order_ids,
            'broker_order_ids': broker_order_ids,
            'order_request_ids': [],
            'broker_order_row_ids': [],
            'fill_event_ids': [],
            'pair_count': len(selected_facts),
            'metric_source': 'exec_readback' if selected_facts else None,
            'has_terminal_order_evidence': has_terminal_order_evidence,
            'readback': {
                'last_synced_at': readback.get('last_synced_at') if isinstance(readback, dict) else None,
                'source': readback_source,
                'execution_count': len(execution_facts),
                'order_count': len(order_facts),
                'remote_execution_ids': remote_execution_ids,
                'remote_order_ids': remote_order_ids,
                'remote_exec_plan_item_ids': remote_exec_plan_item_ids,
            },
        }

    def _readback_fact_totals(self, *, facts: Sequence[dict[str, Any]]) -> tuple[Decimal, Decimal]:
        filled_quantity_total = Decimal('0')
        weighted_price_notional = Decimal('0')
        for fact in facts:
            quantity = _safe_decimal(fact.get('quantity')) or Decimal('0')
            price = _safe_decimal(fact.get('price'))
            if quantity <= Decimal('0'):
                continue
            filled_quantity_total += quantity
            if price is not None:
                weighted_price_notional += quantity * price
        return filled_quantity_total, weighted_price_notional

    def _select_execution_metrics(
        self,
        *,
        local_metrics: dict[str, Any],
        readback_metrics: dict[str, Any],
    ) -> dict[str, Any]:
        local_quantity = _safe_decimal(local_metrics.get('filled_quantity')) or Decimal('0')
        readback_quantity = _safe_decimal(readback_metrics.get('filled_quantity')) or Decimal('0')
        readback_has_state = bool(readback_metrics.get('pair_count') and readback_metrics.get('latest_order_status'))
        local_has_state = bool(local_metrics.get('pair_count') and local_metrics.get('latest_order_status'))
        if readback_quantity > local_quantity or (
            readback_quantity == local_quantity == Decimal('0')
            and readback_has_state
            and not local_has_state
        ):
            selected = dict(readback_metrics)
            selected['metric_source'] = 'exec_readback'
            for key in ('latest_order_request_id', 'latest_broker_order_row_id'):
                selected[key] = selected.get(key) or local_metrics.get(key)
            for key in ('order_request_ids', 'broker_order_row_ids', 'fill_event_ids'):
                selected[key] = list(local_metrics.get(key) or []) + list(selected.get(key) or [])
            if local_metrics.get('latest_client_order_id') and not selected.get('latest_client_order_id'):
                selected['latest_client_order_id'] = local_metrics.get('latest_client_order_id')
            return selected
        selected = dict(local_metrics)
        selected['metric_source'] = 'broker_orders' if local_quantity > Decimal('0') else 'plan_or_order_state'
        return selected

    def _load_exec_readback(self, *, entry_row: SignalDeciderExecutionPlanItem) -> dict[str, Any]:
        submission = entry_row.submission_payload if isinstance(entry_row.submission_payload, dict) else {}
        exec_dispatch = submission.get('exec_dispatch') if isinstance(submission.get('exec_dispatch'), dict) else {}
        readback = exec_dispatch.get('readback') if isinstance(exec_dispatch.get('readback'), dict) else {}
        return readback

    def _load_exec_item_links(self, *, entry_row: SignalDeciderExecutionPlanItem) -> list[dict[str, Any]]:
        submission = entry_row.submission_payload if isinstance(entry_row.submission_payload, dict) else {}
        exec_dispatch = submission.get('exec_dispatch') if isinstance(submission.get('exec_dispatch'), dict) else {}
        return [item for item in (exec_dispatch.get('item_links') or []) if isinstance(item, dict)]

    def _readback_items(self, *, readback: dict[str, Any], key: str) -> list[dict[str, Any]]:
        section = readback.get(key) if isinstance(readback, dict) else None
        if isinstance(section, dict):
            return [item for item in (section.get('items') or []) if isinstance(item, dict)]
        if isinstance(section, list):
            return [item for item in section if isinstance(item, dict)]
        return []

    def _exec_readback_fact(
        self,
        *,
        payload: dict[str, Any],
        side: str,
        source: str,
        item_links: Sequence[dict[str, Any]],
        order_link_by_broker_order_id: dict[str, dict[str, Any]],
    ) -> dict[str, Any] | None:
        payload_side = self._resolve_readback_side(
            payload=payload,
            item_links=item_links,
            order_link_by_broker_order_id=order_link_by_broker_order_id,
        )
        if payload_side != side:
            return None
        status = self._normalize_readback_order_status(
            payload.get('status') or payload.get('current_status') or payload.get('broker_status')
        )
        quantity = self._readback_filled_quantity(payload=payload, source=source, status=status)
        if (
            quantity <= Decimal('0')
            and status not in ACTIVE_ORDER_STATUSES
            and status not in FILLED_ORDER_STATUSES
            and status not in {BrokerOrderStatus.CANCELED.value, BrokerOrderStatus.REJECTED.value}
        ):
            return None
        filled_at = self._readback_fill_timestamp(payload=payload, source=source)
        submitted_at = self._readback_submitted_timestamp(payload=payload)
        return {
            'source': source,
            'quantity': quantity,
            'price': self._readback_fill_price(payload=payload),
            'filled_at': filled_at,
            'submitted_at': submitted_at,
            'sort_at': filled_at or submitted_at or _parse_datetime(payload.get('updated_at')) or datetime(1970, 1, 1, tzinfo=timezone.utc),
            'status': status,
            'requested_quantity': _safe_decimal(payload.get('requested_quantity') or payload.get('quantity')),
            'remaining_quantity': _safe_decimal(payload.get('remaining_quantity')),
            'client_order_id': _normalize_optional_text(payload.get('client_order_id')),
            'broker_order_id': self._readback_broker_order_id(payload=payload),
            'execution_id': _normalize_optional_text(
                payload.get('execution_id') or payload.get('broker_execution_id') or payload.get('id')
            ) if source == 'execution' else None,
            'order_id': _normalize_optional_text(
                payload.get('order_id') or payload.get('broker_order_id') or payload.get('id')
            ) if source == 'order' else None,
            'exec_plan_item_id': _normalize_optional_text(
                payload.get('execution_plan_item_id') or payload.get('plan_item_id') or payload.get('exec_plan_item_id')
            ),
        }

    def _resolve_readback_side(
        self,
        *,
        payload: dict[str, Any],
        item_links: Sequence[dict[str, Any]],
        order_link_by_broker_order_id: dict[str, dict[str, Any]],
    ) -> str | None:
        normalized_side = self._normalize_readback_side(payload.get('side'))
        if normalized_side:
            return normalized_side
        link = self._readback_link_for_payload(
            payload=payload,
            item_links=item_links,
            order_link_by_broker_order_id=order_link_by_broker_order_id,
        )
        intent = _normalize_optional_text(link.get('intent') if link else None, lowercase=True)
        if intent in {EXEC_INTENT_NEXT_OPEN_BUY, EXEC_INTENT_INTRADAY_WATCH_BUY}:
            return 'buy'
        if intent in {EXEC_INTENT_TARGET_SELL, EXEC_INTENT_STOP_SELL}:
            return 'sell'
        return None

    def _readback_has_terminal_order_evidence(
        self,
        *,
        side: str,
        order_facts: Sequence[dict[str, Any]],
        item_links: Sequence[dict[str, Any]],
    ) -> bool:
        if order_facts:
            return all(
                self._has_terminal_order_evidence(
                    status=order_fact.get('status'),
                    requested_quantity=order_fact.get('requested_quantity'),
                    filled_quantity=order_fact.get('quantity'),
                    remaining_quantity=order_fact.get('remaining_quantity'),
                )
                for order_fact in order_facts
            )

        matching_statuses: list[str | None] = []
        for link in item_links:
            link_side = self._normalize_readback_side(link.get('side'))
            intent = _normalize_optional_text(link.get('intent'), lowercase=True)
            if link_side is None:
                if intent in {EXEC_INTENT_NEXT_OPEN_BUY, EXEC_INTENT_INTRADAY_WATCH_BUY}:
                    link_side = 'buy'
                elif intent in {EXEC_INTENT_TARGET_SELL, EXEC_INTENT_STOP_SELL}:
                    link_side = 'sell'
            if link_side != side:
                continue
            status = self._normalize_readback_order_status(
                link.get('remote_status') or link.get('status')
            )
            matching_statuses.append(status)
        return bool(matching_statuses) and all(
            status in TERMINAL_ORDER_STATUSES for status in matching_statuses
        )

    def _has_terminal_order_evidence(
        self,
        *,
        status: Any,
        requested_quantity: Any,
        filled_quantity: Any,
        remaining_quantity: Any,
    ) -> bool:
        normalized_status = _normalize_optional_text(status, lowercase=True)
        if normalized_status in TERMINAL_ORDER_STATUSES:
            return True
        remaining = _safe_decimal(remaining_quantity)
        if remaining is not None:
            return remaining <= Decimal('0')
        requested = _safe_decimal(requested_quantity)
        filled = _safe_decimal(filled_quantity)
        return bool(
            requested is not None
            and requested > Decimal('0')
            and filled is not None
            and filled >= requested
        )

    def _readback_order_link_by_broker_order_id(
        self,
        *,
        orders: Sequence[dict[str, Any]],
        item_links: Sequence[dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        links_by_plan_item_id = self._readback_links_by_exec_plan_item_id(item_links=item_links)
        result: dict[str, dict[str, Any]] = {}
        for order in orders:
            link = self._readback_link_for_payload(
                payload=order,
                item_links=item_links,
                order_link_by_broker_order_id={},
            )
            if link is None:
                plan_item_id = _normalize_optional_text(
                    order.get('execution_plan_item_id') or order.get('plan_item_id') or order.get('exec_plan_item_id')
                )
                link = links_by_plan_item_id.get(plan_item_id or '')
            if link is None:
                continue
            for value in (order.get('broker_order_id'), order.get('order_id'), order.get('id')):
                broker_order_id = _normalize_optional_text(value)
                if broker_order_id:
                    result[broker_order_id] = link
        return result

    def _readback_link_for_payload(
        self,
        *,
        payload: dict[str, Any],
        item_links: Sequence[dict[str, Any]],
        order_link_by_broker_order_id: dict[str, dict[str, Any]],
    ) -> dict[str, Any] | None:
        plan_item_id = _normalize_optional_text(
            payload.get('execution_plan_item_id') or payload.get('plan_item_id') or payload.get('exec_plan_item_id')
        )
        if plan_item_id:
            links_by_plan_item_id = self._readback_links_by_exec_plan_item_id(item_links=item_links)
            if link := links_by_plan_item_id.get(plan_item_id):
                return link
        broker_order_id = self._readback_broker_order_id(payload=payload)
        if broker_order_id and broker_order_id in order_link_by_broker_order_id:
            return order_link_by_broker_order_id[broker_order_id]
        return None

    def _readback_links_by_exec_plan_item_id(
        self,
        *,
        item_links: Sequence[dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for link in item_links:
            for value in (
                link.get('exec_plan_item_id'),
                link.get('execution_plan_item_id'),
                link.get('plan_item_id'),
                (link.get('response_item') or {}).get('id') if isinstance(link.get('response_item'), dict) else None,
                (link.get('response_item') or {}).get('execution_plan_item_id')
                if isinstance(link.get('response_item'), dict)
                else None,
            ):
                plan_item_id = _normalize_optional_text(value)
                if plan_item_id:
                    result[plan_item_id] = link
        return result

    def _resolve_exec_readback_exit_trigger_rule(
        self,
        *,
        entry_row: SignalDeciderExecutionPlanItem,
    ) -> str | None:
        readback = self._load_exec_readback(entry_row=entry_row)
        orders = self._readback_items(readback=readback, key='orders')
        executions = self._readback_items(readback=readback, key='executions')
        item_links = self._load_exec_item_links(entry_row=entry_row)
        order_link_by_broker_order_id = self._readback_order_link_by_broker_order_id(
            orders=orders,
            item_links=item_links,
        )
        for payload in [*executions, *orders]:
            side = self._resolve_readback_side(
                payload=payload,
                item_links=item_links,
                order_link_by_broker_order_id=order_link_by_broker_order_id,
            )
            if side != 'sell':
                continue
            status = self._normalize_readback_order_status(
                payload.get('status') or payload.get('current_status') or payload.get('broker_status')
            )
            quantity = self._readback_filled_quantity(payload=payload, source='execution', status=status)
            if quantity <= Decimal('0') and status not in FILLED_ORDER_STATUSES:
                continue
            link = self._readback_link_for_payload(
                payload=payload,
                item_links=item_links,
                order_link_by_broker_order_id=order_link_by_broker_order_id,
            )
            intent = _normalize_optional_text(link.get('intent') if link else None, lowercase=True)
            if intent == EXEC_INTENT_TARGET_SELL:
                return EXIT_REASON_TARGET_HIT
            if intent == EXEC_INTENT_STOP_SELL:
                return EXIT_REASON_STOP_LOSS_HIT
        return None

    def _serialize_exec_readback_metrics(self, metrics: dict[str, Any]) -> dict[str, Any] | None:
        if (_safe_decimal(metrics.get('filled_quantity')) or Decimal('0')) <= Decimal('0') and not metrics.get('pair_count'):
            return None
        readback = metrics.get('readback') if isinstance(metrics.get('readback'), dict) else {}
        return {
            'source': readback.get('source'),
            'last_synced_at': readback.get('last_synced_at'),
            'filled_quantity': _decimal_to_text(_safe_decimal(metrics.get('filled_quantity'))),
            'average_fill_price': _to_json_number(_safe_decimal(metrics.get('average_fill_price'))),
            'first_fill_at': metrics.get('first_fill_at').isoformat() if metrics.get('first_fill_at') else None,
            'last_fill_at': metrics.get('last_fill_at').isoformat() if metrics.get('last_fill_at') else None,
            'execution_count': readback.get('execution_count'),
            'order_count': readback.get('order_count'),
            'remote_execution_ids': readback.get('remote_execution_ids') or [],
            'remote_order_ids': readback.get('remote_order_ids') or [],
            'remote_exec_plan_item_ids': readback.get('remote_exec_plan_item_ids') or [],
        }

    def _readback_filled_quantity(self, *, payload: dict[str, Any], source: str, status: str | None) -> Decimal:
        if source == 'execution':
            return _safe_decimal(payload.get('quantity') or payload.get('filled_quantity')) or Decimal('0')
        quantity = _safe_decimal(
            payload.get('filled_quantity')
            or payload.get('executed_quantity')
            or payload.get('cumulative_filled_quantity')
        )
        if quantity is None and status in FILLED_ORDER_STATUSES:
            quantity = _safe_decimal(payload.get('quantity'))
        return quantity or Decimal('0')

    def _readback_fill_price(self, *, payload: dict[str, Any]) -> Decimal | None:
        return _safe_decimal(
            payload.get('average_fill_price')
            or payload.get('avg_fill_price')
            or payload.get('fill_price')
            or payload.get('price')
            or payload.get('avg_price')
        )

    def _readback_fill_timestamp(self, *, payload: dict[str, Any], source: str) -> datetime | None:
        fields = (
            ('executed_at', 'filled_at', 'last_filled_at', 'created_at', 'updated_at')
            if source == 'execution'
            else ('filled_at', 'last_filled_at', 'updated_at', 'created_at', 'submitted_at')
        )
        for field in fields:
            if parsed := _parse_datetime(payload.get(field)):
                return parsed
        return None

    def _readback_submitted_timestamp(self, *, payload: dict[str, Any]) -> datetime | None:
        for field in ('submitted_at', 'created_at', 'updated_at'):
            if parsed := _parse_datetime(payload.get(field)):
                return parsed
        return None

    def _readback_broker_order_id(self, *, payload: dict[str, Any]) -> str | None:
        return _normalize_optional_text(
            payload.get('broker_order_id') or payload.get('order_id') or payload.get('broker_order_pk') or payload.get('id')
        )

    def _normalize_readback_side(self, value: Any) -> str | None:
        normalized = _normalize_optional_text(value, lowercase=True)
        if normalized in {'buy', 'b', 'bid'}:
            return 'buy'
        if normalized in {'sell', 's', 'ask'}:
            return 'sell'
        return None

    def _normalize_readback_order_status(self, value: Any) -> str | None:
        normalized = _normalize_optional_text(value, lowercase=True)
        if normalized is None:
            return None
        if normalized in REMOTE_FILLED_STATUS_ALIASES:
            return BrokerOrderStatus.FILLED.value
        if normalized in REMOTE_PARTIAL_STATUS_ALIASES:
            return BrokerOrderStatus.PARTIAL_FILL.value
        if normalized in REMOTE_CANCELLED_STATUS_ALIASES:
            return BrokerOrderStatus.CANCELED.value
        if normalized in REMOTE_REJECTED_STATUS_ALIASES:
            return BrokerOrderStatus.REJECTED.value
        if normalized in REMOTE_ACTIVE_STATUS_ALIASES:
            return BrokerOrderStatus.SUBMITTED.value
        return normalized

    def _extract_fill_event_info(
        self,
        *,
        events: Sequence[BrokerOrderEvent],
        fallback_fill_at: datetime | None,
        filled_quantity: Decimal,
    ) -> tuple[list[int], datetime | None, datetime | None]:
        fill_event_ids: list[int] = []
        fill_times: list[datetime] = []
        for event in events:
            if not self._event_has_fill(event):
                continue
            fill_event_ids.append(int(event.id))
            occurred_at = _as_utc(event.event_occurred_at)
            if occurred_at is not None:
                fill_times.append(occurred_at)
        if not fill_times and filled_quantity > Decimal('0') and fallback_fill_at is not None:
            fill_times.append(fallback_fill_at)
        if not fill_times:
            return fill_event_ids, None, None
        return fill_event_ids, min(fill_times), max(fill_times)

    def _weighted_average_from_events(self, *, events: Sequence[BrokerOrderEvent]) -> Decimal | None:
        weighted_notional = Decimal('0')
        quantity_total = Decimal('0')
        prior_cumulative = Decimal('0')
        for event in events:
            quantity_delta = _safe_decimal(event.quantity_delta)
            cumulative = _safe_decimal(event.cumulative_filled_quantity)
            if quantity_delta is None and cumulative is not None and cumulative > prior_cumulative:
                quantity_delta = cumulative - prior_cumulative
            if cumulative is not None and cumulative > prior_cumulative:
                prior_cumulative = cumulative
            fill_price = _safe_decimal(event.fill_price)
            if quantity_delta is None or quantity_delta <= Decimal('0') or fill_price is None:
                continue
            quantity_total += quantity_delta
            weighted_notional += quantity_delta * fill_price
        if quantity_total <= Decimal('0'):
            return None
        return weighted_notional / quantity_total

    def _event_has_fill(self, event: BrokerOrderEvent) -> bool:
        quantity_delta = _safe_decimal(event.quantity_delta)
        cumulative = _safe_decimal(event.cumulative_filled_quantity)
        fill_price = _safe_decimal(event.fill_price)
        status = _normalize_optional_text(event.resulting_status, lowercase=True)
        return bool(
            (quantity_delta is not None and quantity_delta > Decimal('0'))
            or (cumulative is not None and cumulative > Decimal('0'))
            or fill_price is not None
            or status in FILLED_ORDER_STATUSES
        )

    def _resolve_exit_trigger_rule(
        self,
        *,
        exit_row: SignalDeciderExecutionPlanItem | None,
        exit_pairs: Sequence[dict[str, Any]],
    ) -> str | None:
        if exit_row is not None and isinstance(exit_row.validation_payload, dict):
            trigger_rule = _normalize_optional_text(
                ((exit_row.validation_payload.get('trigger') or {}).get('rule')),
                lowercase=True,
            )
            if trigger_rule:
                return trigger_rule
        if exit_row is not None and isinstance(exit_row.submission_payload, dict):
            trigger_rule = _normalize_optional_text(
                ((exit_row.submission_payload.get('latest') or {}).get('rule')),
                lowercase=True,
            )
            if trigger_rule:
                return trigger_rule
        for pair in reversed(list(exit_pairs)):
            trigger_rule = _normalize_optional_text(pair.get('trigger_rule'), lowercase=True)
            if trigger_rule:
                return trigger_rule
            request_row = pair.get('request')
            if request_row is None:
                continue
            strategy_payload = request_row.strategy_context_payload if isinstance(request_row.strategy_context_payload, dict) else {}
            trigger_rule = _normalize_optional_text(
                ((strategy_payload.get('signal_decider_intraday_auto_sell') or {}).get('trigger_rule')),
                lowercase=True,
            )
            if trigger_rule:
                return trigger_rule
        return None

    def _resolve_lifecycle_and_exit_reason(
        self,
        *,
        entry_row: SignalDeciderExecutionPlanItem,
        entry_request: OrderRequest | None,
        entry_order: BrokerOrder | None,
        entry_metrics: dict[str, Any],
        entry_filled_quantity: Decimal,
        exit_row: SignalDeciderExecutionPlanItem | None,
        exit_trigger_rule: str | None,
        exit_metrics: dict[str, Any],
        closed_quantity: Decimal,
        open_quantity: Decimal,
        manual_candidate_fill_quantity: Decimal,
    ) -> tuple[str, str | None]:
        entry_request_status = _normalize_optional_text(
            entry_request.submission_status if entry_request is not None else None,
            lowercase=True,
        ) or _normalize_optional_text(entry_metrics.get('latest_request_status'), lowercase=True)
        entry_order_status = _normalize_optional_text(
            entry_order.current_status if entry_order is not None else None,
            lowercase=True,
        ) or _normalize_optional_text(entry_metrics.get('latest_order_status'), lowercase=True)
        exit_order_status = _normalize_optional_text(exit_metrics.get('latest_order_status'), lowercase=True)
        exit_request_status = _normalize_optional_text(exit_metrics.get('latest_request_status'), lowercase=True)

        if entry_row.execution_status == 'dry_run':
            return LIFECYCLE_DRY_RUN, EXIT_REASON_DRY_RUN
        if entry_filled_quantity <= Decimal('0'):
            if entry_order_status == BrokerOrderStatus.REJECTED.value or entry_request_status == 'rejected':
                return LIFECYCLE_ENTRY_REJECTED, EXIT_REASON_ENTRY_REJECTED
            if entry_order_status == BrokerOrderStatus.CANCELED.value:
                return LIFECYCLE_ENTRY_CANCELED, EXIT_REASON_ENTRY_CANCELED
            if entry_row.execution_status == 'submit_failed' or entry_request_status == 'failed':
                return LIFECYCLE_ENTRY_SUBMIT_FAILED, EXIT_REASON_ENTRY_SUBMIT_FAILED
            if entry_order_status in ACTIVE_ORDER_STATUSES or entry_request_status in {'submitting', 'submitted'}:
                return LIFECYCLE_ENTRY_SUBMITTED, EXIT_REASON_POSITION_OPEN
            return LIFECYCLE_PLANNED, None

        if manual_candidate_fill_quantity > Decimal('0') and closed_quantity <= Decimal('0'):
            return LIFECYCLE_UNRESOLVED, EXIT_REASON_UNRESOLVED

        if closed_quantity >= entry_filled_quantity and entry_filled_quantity > Decimal('0'):
            if not entry_metrics.get('has_terminal_order_evidence'):
                return LIFECYCLE_UNRESOLVED, EXIT_REASON_UNRESOLVED
            if exit_trigger_rule == EXIT_REASON_TARGET_HIT:
                return LIFECYCLE_CLOSED, EXIT_REASON_TARGET_HIT
            if exit_trigger_rule == EXIT_REASON_STOP_LOSS_HIT:
                return LIFECYCLE_CLOSED, EXIT_REASON_STOP_LOSS_HIT
            return LIFECYCLE_CLOSED, EXIT_REASON_UNRESOLVED

        if closed_quantity > Decimal('0'):
            if exit_trigger_rule == EXIT_REASON_TARGET_HIT:
                return LIFECYCLE_PARTIALLY_CLOSED, EXIT_REASON_TARGET_HIT
            if exit_trigger_rule == EXIT_REASON_STOP_LOSS_HIT:
                return LIFECYCLE_PARTIALLY_CLOSED, EXIT_REASON_STOP_LOSS_HIT
            return LIFECYCLE_PARTIALLY_CLOSED, EXIT_REASON_UNRESOLVED

        if exit_order_status in ACTIVE_ORDER_STATUSES or exit_request_status in {'submitting', 'submitted'}:
            if exit_trigger_rule == EXIT_REASON_TARGET_HIT:
                return LIFECYCLE_EXIT_SUBMITTED, EXIT_REASON_TARGET_HIT
            if exit_trigger_rule == EXIT_REASON_STOP_LOSS_HIT:
                return LIFECYCLE_EXIT_SUBMITTED, EXIT_REASON_STOP_LOSS_HIT
            return LIFECYCLE_EXIT_SUBMITTED, EXIT_REASON_POSITION_OPEN

        if entry_order_status == BrokerOrderStatus.PARTIAL_FILL.value:
            return LIFECYCLE_PARTIALLY_OPEN, EXIT_REASON_POSITION_OPEN
        if entry_order_status in FILLED_ORDER_STATUSES or open_quantity > Decimal('0'):
            return LIFECYCLE_OPEN, EXIT_REASON_POSITION_OPEN
        return LIFECYCLE_UNRESOLVED, EXIT_REASON_UNRESOLVED

    def _pair_sort_timestamp(self, pair: dict[str, Any]) -> datetime:
        request_row = pair.get('request')
        order_row = pair.get('order')
        return (
            (_as_utc(request_row.submitted_at) if request_row is not None else None)
            or (_as_utc(order_row.first_submitted_at) if order_row is not None else None)
            or (_as_utc(order_row.last_broker_update_at) if order_row is not None else None)
            or (_as_utc(request_row.created_at) if request_row is not None else None)
            or datetime(1970, 1, 1, tzinfo=timezone.utc)
        )

    def _return_pct(self, numerator_price: Decimal | None, denominator_price: Decimal | None) -> Decimal | None:
        if numerator_price is None or denominator_price is None or denominator_price <= Decimal('0'):
            return None
        return _quantize_return_pct((numerator_price / denominator_price) - Decimal('1'))

    def _upsert_outcome_row(self, *, session, item: dict[str, Any]) -> SignalDeciderActualExecutionOutcome:
        row = session.execute(
            select(SignalDeciderActualExecutionOutcome).where(
                SignalDeciderActualExecutionOutcome.entry_plan_item_id == int(item['entry_plan_item_id'])
            )
        ).scalar_one_or_none()
        if row is None:
            row = SignalDeciderActualExecutionOutcome(
                decision_snapshot_id=int(item['decision_snapshot_id']),
                entry_plan_item_id=int(item['entry_plan_item_id']),
            )
            session.add(row)
        for key, value in item.items():
            setattr(row, key, value)
        session.flush()
        session.refresh(row)
        return row


signal_decider_execution_outcome_service = SignalDeciderExecutionOutcomeService()
