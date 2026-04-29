from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import logging
import re
from typing import Any, Callable

import exchange_calendars as xcals
import pandas as pd
from sqlalchemy import select

from app.core.settings import settings
from app.db import session_scope
from app.db_models import SignalDeciderDecisionSnapshot, SignalDeciderExecutionPlanItem
from app.market.clock import MarketState, get_market_state
from app.orders.account_inquiry_service import (
    TradingAccountInquiryService,
    trading_account_inquiry_service,
)
from app.orders.account_scope import parse_kis_account_reference
from app.orders.domain import BrokerOrderStatus
from app.orders.next_open_execution import (
    ELIGIBILITY_BLOCKED,
    ELIGIBILITY_ELIGIBLE,
    EXECUTION_WINDOW_NEXT_OPEN,
)
from app.orders.signal_decider_execution_outcomes import signal_decider_execution_outcome_service
from app.orders.service import (
    TradingOrderPendingSubmissionError,
    TradingOrderRejectedError,
    TradingOrderService,
    TradingOrderUpstreamError,
    TradingOrderValidationError,
    trading_order_service,
)
from app.orders.store import list_broker_orders, load_broker_order, load_order_request
from app.quotes.service import QuoteService

logger = logging.getLogger(__name__)

EXECUTION_WINDOW_INTRADAY_AUTO_SELL = 'intraday_auto_sell'
EXECUTION_STATUS_PLANNED = 'planned'
EXECUTION_STATUS_DRY_RUN = 'dry_run'
EXECUTION_STATUS_SUBMITTED = 'submitted'
EXECUTION_STATUS_SUBMIT_FAILED = 'submit_failed'
ORDER_SOURCE = 'signal_decider_auto_sell'
TRIGGER_RULE_TARGET_HIT = 'target_hit'
TRIGGER_RULE_STOP_LOSS_HIT = 'stop_loss_hit'
ACTIVE_ORDER_STATUSES = {
    BrokerOrderStatus.PENDING_SUBMIT.value,
    BrokerOrderStatus.SUBMITTED.value,
    BrokerOrderStatus.ACCEPTED.value,
    BrokerOrderStatus.CANCEL_PENDING.value,
    BrokerOrderStatus.REPLACE_PENDING.value,
    BrokerOrderStatus.PARTIAL_FILL.value,
    BrokerOrderStatus.RECONCILE_ONLY.value,
}
FAILED_ORDER_STATUSES = {
    BrokerOrderStatus.CANCELED.value,
    BrokerOrderStatus.REJECTED.value,
}
SUCCESS_ORDER_STATUSES = {
    BrokerOrderStatus.FILLED.value,
}
KRX_EXCHANGE_CALENDAR = xcals.get_calendar('XKRX')


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


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


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None or value == '':
        return None
    try:
        return value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _decimal_to_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, 'f')


def _to_json_number(value: Decimal | None) -> int | float | None:
    if value is None:
        return None
    if value == value.to_integral():
        return int(value)
    return float(value)


def _parse_datetime(value: Any) -> datetime | None:
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        normalized = value.replace('Z', '+00:00')
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None
    return None


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _normalize_int(value: Any) -> int | None:
    normalized = _safe_decimal(value)
    if normalized is None:
        return None
    try:
        return int(normalized)
    except (TypeError, ValueError):
        return None


def _reason(code: str, message: str, **extra: Any) -> dict[str, Any]:
    payload = {'code': code, 'message': message}
    for key, value in extra.items():
        if value is not None:
            payload[key] = value
    return payload


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return _decimal_to_text(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    return value


def _holding_period_bounds(value: Any) -> tuple[int | None, int | None]:
    text = _normalize_optional_text(value)
    if text is None:
        return None, None
    matches = [int(item) for item in re.findall(r'\d+', text)]
    if not matches:
        return None, None
    if len(matches) == 1:
        return matches[0], matches[0]
    return min(matches), max(matches)


def _advance_trading_days(start_date: date, trading_days: int | None) -> date | None:
    if trading_days is None or trading_days < 1:
        return None
    current_session = KRX_EXCHANGE_CALENDAR.date_to_session(pd.Timestamp(start_date.isoformat()), direction='next')
    remaining = max(0, trading_days - 1)
    while remaining > 0:
        current_session = KRX_EXCHANGE_CALENDAR.next_session(current_session)
        remaining -= 1
    return current_session.date()


def _sort_key_for_order_pair(order_row: dict[str, Any] | None, request_row: dict[str, Any] | None) -> datetime:
    return (
        _parse_datetime((request_row or {}).get('submitted_at'))
        or _parse_datetime((order_row or {}).get('first_submitted_at'))
        or _parse_datetime((order_row or {}).get('last_broker_update_at'))
        or _parse_datetime((request_row or {}).get('updated_at'))
        or _parse_datetime((request_row or {}).get('created_at'))
        or datetime(1970, 1, 1, tzinfo=timezone.utc)
    )


@dataclass(frozen=True)
class MonitoredAccount:
    label: str
    account_reference: str
    account_number: str
    account_product_code: str


@dataclass(frozen=True)
class LinkedNextOpenPlan:
    row: SignalDeciderExecutionPlanItem
    request: dict[str, Any] | None
    order: dict[str, Any] | None


class SignalDeciderIntradayAutoSellService:
    def __init__(
        self,
        *,
        account_inquiry_service: TradingAccountInquiryService | None = None,
        quote_service: QuoteService | None = None,
        order_service: TradingOrderService | None = None,
        market_state_getter: Callable[[], MarketState] = get_market_state,
    ) -> None:
        self.account_inquiry_service = account_inquiry_service or trading_account_inquiry_service
        self.quote_service = quote_service or QuoteService()
        self.order_service = order_service or trading_order_service
        self.market_state_getter = market_state_getter

    def _current_time(self, market_state: MarketState | None = None) -> datetime:
        state = market_state or self.market_state_getter()
        return _as_utc(_parse_datetime(state.server_time)) or _utcnow()

    def runtime_status(self, *, account_reference: str | None = None) -> dict[str, Any]:
        accounts, errors, account_source = self._resolve_accounts(account_reference=account_reference)
        return {
            'execution_enabled': bool(settings.signal_decider_intraday_auto_sell_enabled),
            'dry_run': bool(settings.signal_decider_intraday_auto_sell_dry_run),
            'account_source': account_source,
            'account_references_configured': bool(accounts),
            'account_references_valid': not errors,
            'account_reference_errors': errors,
            'accounts': [
                {
                    'label': account.label,
                    'account_reference': account.account_reference,
                }
                for account in accounts
            ],
            'safety_rails': {
                'cooldown_seconds': max(0, int(settings.signal_decider_intraday_auto_sell_cooldown_seconds)),
            },
        }

    async def list_watch_items(
        self,
        *,
        account_reference: str | None = None,
        symbol: str | None = None,
        refresh: bool = False,
    ) -> dict[str, Any]:
        if refresh:
            return await self.refresh_watch_items(
                account_reference=account_reference,
                symbol=symbol,
                trigger='route_refresh',
            )

        market_state = self.market_state_getter()
        runtime = self.runtime_status(account_reference=account_reference)
        runtime.update(
            {
                'market_state': market_state.state,
                'server_time': market_state.server_time,
            }
        )
        rows = self._load_watch_rows(
            account_reference=account_reference,
            symbol=_normalize_optional_text(symbol, uppercase=True),
        )
        items = [self._serialize_row(row) for row in rows]
        return {
            'request': {
                'account_reference': account_reference,
                'symbol': _normalize_optional_text(symbol, uppercase=True),
                'refresh': refresh,
            },
            'runtime': runtime,
            'summary': self._build_summary(items=items, account_errors=[]),
            'account_errors': [],
            'items': items,
        }

    async def refresh_watch_items(
        self,
        *,
        account_reference: str | None = None,
        symbol: str | None = None,
        trigger: str = 'manual',
    ) -> dict[str, Any]:
        normalized_symbol = _normalize_optional_text(symbol, uppercase=True)
        market_state = self.market_state_getter()
        runtime = self.runtime_status(account_reference=account_reference)
        accounts, errors, _account_source = self._resolve_accounts(account_reference=account_reference)
        items: list[dict[str, Any]] = []
        account_errors: list[dict[str, Any]] = []

        for account in accounts:
            try:
                positions_payload = await self.account_inquiry_service.get_positions(
                    account_reference=account.account_reference,
                    symbol=normalized_symbol,
                    include_zero=False,
                )
            except Exception as exc:
                logger.warning('Intraday auto-sell positions lookup failed for %s: %s', account.account_reference, exc)
                account_errors.append(
                    {
                        'account_reference': account.account_reference,
                        'account_label': account.label,
                        'error': str(exc),
                    }
                )
                continue

            for position in positions_payload.get('items') or []:
                position_items = await self._refresh_position_watch_items(
                    account=account,
                    position=position,
                    positions_as_of=_normalize_optional_text(positions_payload.get('as_of')),
                    market_state=market_state,
                    trigger=trigger,
                )
                items.extend(position_items)

        items.sort(
            key=lambda item: (
                item.get('account_reference') or '',
                item.get('symbol') or '',
                item.get('target_trade_date') or '',
            )
        )
        runtime.update(
            {
                'market_state': market_state.state,
                'server_time': market_state.server_time,
                'account_reference_errors': errors,
            }
        )
        return {
            'request': {
                'account_reference': account_reference,
                'symbol': normalized_symbol,
                'trigger': trigger,
            },
            'runtime': runtime,
            'summary': self._build_summary(items=items, account_errors=account_errors),
            'account_errors': account_errors,
            'items': items,
        }

    async def run_intraday_auto_sell_if_due(
        self,
        *,
        account_reference: str | None = None,
        symbol: str | None = None,
        dry_run: bool | None = None,
        force: bool = False,
        trigger: str = 'manual',
    ) -> dict[str, Any]:
        payload = await self.refresh_watch_items(
            account_reference=account_reference,
            symbol=symbol,
            trigger=f'{trigger}_refresh',
        )
        market_state = self.market_state_getter()
        effective_dry_run = settings.signal_decider_intraday_auto_sell_dry_run if dry_run is None else dry_run
        payload['request'].update(
            {
                'dry_run': effective_dry_run,
                'force': force,
                'trigger': trigger,
            }
        )
        if not force and market_state.state != 'OPEN':
            payload['summary'] = {
                **payload['summary'],
                'triggered': False,
                'reason': 'market_not_open',
                'selected_count': 0,
                'submitted_count': 0,
                'dry_run_count': 0,
                'failed_count': 0,
                'idempotent_replay_count': 0,
            }
            return payload
        if not force and not settings.signal_decider_intraday_auto_sell_enabled and not effective_dry_run:
            payload['summary'] = {
                **payload['summary'],
                'triggered': False,
                'reason': 'execution_disabled',
                'selected_count': 0,
                'submitted_count': 0,
                'dry_run_count': 0,
                'failed_count': 0,
                'idempotent_replay_count': 0,
            }
            return payload

        updated_items: list[dict[str, Any]] = []
        selected_count = 0
        submitted_count = 0
        dry_run_count = 0
        failed_count = 0
        idempotent_replay_count = 0

        for item in payload['items']:
            if item.get('eligibility_status') != ELIGIBILITY_ELIGIBLE:
                updated_items.append(item)
                continue

            selected_count += 1
            if effective_dry_run:
                updated = self._mark_dry_run(
                    row_id=int(item['plan_item_id']),
                    trigger=trigger,
                )
                dry_run_count += 1
            else:
                updated = await self._submit_watch_row(
                    row_id=int(item['plan_item_id']),
                    trigger=trigger,
                )
                if updated.get('execution_status') == EXECUTION_STATUS_SUBMITTED:
                    submitted_count += 1
                    if updated.get('idempotent_replay'):
                        idempotent_replay_count += 1
                else:
                    failed_count += 1
            updated_items.append(updated)

        payload['items'] = updated_items
        payload['summary'] = {
            **self._build_summary(items=updated_items, account_errors=payload['account_errors']),
            'triggered': True,
            'selected_count': selected_count,
            'submitted_count': submitted_count,
            'dry_run_count': dry_run_count,
            'failed_count': failed_count,
            'idempotent_replay_count': idempotent_replay_count,
        }
        return payload

    async def _refresh_position_watch_items(
        self,
        *,
        account: MonitoredAccount,
        position: dict[str, Any],
        positions_as_of: str | None,
        market_state: MarketState,
        trigger: str,
    ) -> list[dict[str, Any]]:
        symbol = _normalize_optional_text(position.get('symbol'), uppercase=True)
        if symbol is None:
            return []

        sources = self._load_linked_source_plans(
            account_reference=account.account_reference,
            symbol=symbol,
        )
        if not sources:
            return []

        quantity = _safe_decimal(position.get('quantity')) or Decimal('0')
        sellable_quantity = _safe_decimal(position.get('sellable_quantity')) or Decimal('0')
        observed_price, price_source, observed_at = await self._resolve_observed_price(
            symbol=symbol,
            position=position,
            positions_as_of=positions_as_of,
        )
        current_trade_date = date.fromisoformat(market_state.server_time[:10])
        remaining_sellable_quantity = sellable_quantity
        items: list[dict[str, Any]] = []

        for source in sorted(
            sources,
            key=lambda item: (
                item.row.target_trade_date,
                _sort_key_for_order_pair(item.order, item.request),
                item.row.id,
            ),
        ):
            snapshot = self._load_snapshot(source.row.decision_snapshot_id)
            if snapshot is None:
                logger.warning(
                    'Intraday auto-sell could not load snapshot %s for %s/%s',
                    source.row.decision_snapshot_id,
                    account.account_reference,
                    symbol,
                )
                continue

            entry_filled_quantity = (
                _safe_decimal((source.order or {}).get('filled_quantity'))
                or _safe_decimal(source.row.planned_quantity)
                or Decimal('0')
            )
            auto_sell_attempts = self._load_auto_sell_attempt_pairs(
                account_reference=account.account_reference,
                symbol=symbol,
                decision_snapshot_id=snapshot.id,
            )
            latest_attempt = auto_sell_attempts[0] if auto_sell_attempts else None
            prior_auto_sell_filled_quantity = sum(
                _safe_decimal((pair.get('order') or {}).get('filled_quantity')) or Decimal('0')
                for pair in auto_sell_attempts
            )
            linked_remaining_quantity = max(entry_filled_quantity - prior_auto_sell_filled_quantity, Decimal('0'))
            available_sellable_quantity = remaining_sellable_quantity
            watched_quantity = min(available_sellable_quantity, linked_remaining_quantity)
            remaining_sellable_quantity = max(remaining_sellable_quantity - watched_quantity, Decimal('0'))

            holding_min_days, holding_max_days = _holding_period_bounds(snapshot.suggested_holding_period)
            holding_period_expiry_trade_date = (
                _advance_trading_days(source.row.target_trade_date, holding_max_days)
                if holding_max_days is not None
                else None
            )
            holding_period_expired = bool(
                holding_period_expiry_trade_date and current_trade_date > holding_period_expiry_trade_date
            )

            blocking_reasons: list[dict[str, Any]] = []
            warnings: list[dict[str, Any]] = []
            execution_status = self._derive_existing_execution_status(
                latest_attempt=latest_attempt,
            )
            cooldown_until: str | None = None

            if snapshot.target_price is None or snapshot.stop_loss_price is None:
                blocking_reasons.append(
                    _reason(
                        'target_or_stop_missing',
                        'The linked signal-decider snapshot does not have both target and stop prices.',
                    )
                )
            elif snapshot.target_price <= snapshot.stop_loss_price:
                blocking_reasons.append(
                    _reason(
                        'target_stop_invalid',
                        'The linked signal-decider snapshot has a non-sensical target/stop ordering.',
                    )
                )

            if observed_price is None or observed_price <= Decimal('0'):
                blocking_reasons.append(
                    _reason(
                        'live_price_unavailable',
                        'No usable live price was available for trigger evaluation.',
                    )
                )
            if sellable_quantity <= Decimal('0'):
                blocking_reasons.append(
                    _reason(
                        'sellable_quantity_zero',
                        'The linked holding does not currently have sellable quantity available.',
                    )
                )
            if linked_remaining_quantity <= Decimal('0'):
                blocking_reasons.append(
                    _reason(
                        'linked_quantity_depleted',
                        'The linked next-open entry has already been fully offset by prior auto-sell fills.',
                    )
                )
            elif watched_quantity <= Decimal('0'):
                blocking_reasons.append(
                    _reason(
                        'sellable_quantity_reserved',
                        'The live holding has no remaining sellable quantity after allocating earlier linked tranches.',
                    )
                )

            trigger_rule: str | None = None
            if (
                observed_price is not None
                and observed_price > Decimal('0')
                and snapshot.target_price
                and snapshot.stop_loss_price
            ):
                trigger_rule = self._resolve_trigger_rule(
                    observed_price=observed_price,
                    target_price=Decimal(str(snapshot.target_price)),
                    stop_loss_price=Decimal(str(snapshot.stop_loss_price)),
                )

            if latest_attempt is not None:
                existing_reason = self._classify_existing_attempt(
                    latest_attempt=latest_attempt,
                    current_row=self._load_watch_row(
                        target_trade_date=source.row.target_trade_date,
                        account_reference=account.account_reference,
                        symbol=symbol,
                    ),
                    trigger_rule=trigger_rule,
                    market_state=market_state,
                )
                if existing_reason is not None:
                    cooldown_until = existing_reason.get('cooldown_until')
                    blocking_reasons.append(existing_reason['reason'])
                    execution_status = existing_reason['execution_status']

            if not blocking_reasons and trigger_rule is None:
                blocking_reasons.append(
                    _reason(
                        'trigger_not_hit',
                        'The linked holding is active, but neither the target nor stop threshold has been hit.',
                    )
                )

            if holding_period_expired:
                warnings.append(
                    _reason(
                        'holding_period_expired',
                        'The linked signal-decider suggested holding period has expired.',
                        expiry_trade_date=holding_period_expiry_trade_date.isoformat()
                        if holding_period_expiry_trade_date
                        else None,
                    )
                )

            eligibility_status = ELIGIBILITY_ELIGIBLE if not blocking_reasons else ELIGIBILITY_BLOCKED
            if eligibility_status == ELIGIBILITY_ELIGIBLE:
                execution_status = (
                    execution_status if execution_status == EXECUTION_STATUS_SUBMIT_FAILED else EXECUTION_STATUS_PLANNED
                )

            validation_payload = {
                'blocking_reasons': blocking_reasons,
                'warnings': warnings,
                'trigger': {
                    'rule': trigger_rule,
                    'live_price': _to_json_number(observed_price),
                    'target_price': snapshot.target_price,
                    'stop_loss_price': snapshot.stop_loss_price,
                    'watched_quantity': _decimal_to_text(watched_quantity),
                    'entry_filled_quantity': _decimal_to_text(entry_filled_quantity),
                    'linked_remaining_quantity': _decimal_to_text(linked_remaining_quantity),
                    'prior_auto_sell_filled_quantity': _decimal_to_text(prior_auto_sell_filled_quantity),
                    'position_quantity': _decimal_to_text(quantity),
                    'sellable_quantity': _decimal_to_text(sellable_quantity),
                    'available_sellable_quantity': _decimal_to_text(available_sellable_quantity),
                    'price_source': price_source,
                    'observed_at': observed_at,
                    'holding_period_expired': holding_period_expired,
                    'cooldown_until': cooldown_until,
                },
                'position': {
                    'quantity': _to_json_number(quantity),
                    'sellable_quantity': _to_json_number(sellable_quantity),
                    'market_value': position.get('market_value'),
                    'average_price': position.get('average_price'),
                },
                'latest_auto_sell': self._summarize_attempt_pair(latest_attempt),
                'triggered_by': trigger,
            }
            traceability_payload = {
                'decision_snapshot_id': snapshot.id,
                'linked_next_open_plan_item_id': source.row.id,
                'linked_next_open_client_order_id': source.row.client_order_id,
                'linked_next_open_order_request_id': source.row.order_request_id,
                'linked_next_open_broker_order_row_id': source.row.broker_order_row_id,
                'linked_target_trade_date': source.row.target_trade_date.isoformat(),
                'linked_source_market_date': source.row.source_market_date.isoformat(),
                'linked_entry_filled_quantity': _decimal_to_text(entry_filled_quantity),
                'target_price': snapshot.target_price,
                'stop_loss_price': snapshot.stop_loss_price,
                'suggested_holding_period': snapshot.suggested_holding_period,
                'holding_period_min_days': holding_min_days,
                'holding_period_max_days': holding_max_days,
                'holding_period_expiry_trade_date': holding_period_expiry_trade_date.isoformat()
                if holding_period_expiry_trade_date
                else None,
            }

            row_id = self._upsert_watch_row(
                account=account,
                source=source,
                snapshot=snapshot,
                symbol=symbol,
                observed_price=observed_price,
                watched_quantity=watched_quantity,
                eligibility_status=eligibility_status,
                execution_status=execution_status,
                validation_payload=validation_payload,
                traceability_payload=traceability_payload,
                latest_attempt=latest_attempt,
            )
            signal_decider_execution_outcome_service.sync_entry_plan_item(entry_plan_item_id=source.row.id)
            items.append(self._serialize_row(self._load_watch_row_by_id(row_id=row_id)))

        return items

    async def _resolve_observed_price(
        self,
        *,
        symbol: str,
        position: dict[str, Any],
        positions_as_of: str | None,
    ) -> tuple[Decimal | None, str | None, str | None]:
        observed_price = _safe_decimal(position.get('last_price')) or _safe_decimal(position.get('current_price'))
        observed_at = positions_as_of
        price_source = 'position_snapshot' if observed_price is not None and observed_price > Decimal('0') else None

        if observed_price is None or observed_price <= Decimal('0'):
            try:
                quote_payload = await self.quote_service.get_quote(symbol)
            except Exception as exc:
                logger.warning('Intraday auto-sell quote lookup failed for %s: %s', symbol, exc)
            else:
                quote_price = _safe_decimal(quote_payload.get('price'))
                if quote_price is not None and quote_price > Decimal('0'):
                    observed_price = quote_price
                    observed_at = _normalize_optional_text(quote_payload.get('cached_at')) or _normalize_optional_text(
                        quote_payload.get('as_of')
                    )
                    price_source = _normalize_optional_text(quote_payload.get('source'), lowercase=True) or 'quote'
        return observed_price, price_source, observed_at

    def _resolve_trigger_rule(
        self,
        *,
        observed_price: Decimal,
        target_price: Decimal,
        stop_loss_price: Decimal,
    ) -> str | None:
        if observed_price <= stop_loss_price:
            return TRIGGER_RULE_STOP_LOSS_HIT
        if observed_price >= target_price:
            return TRIGGER_RULE_TARGET_HIT
        return None

    def _derive_existing_execution_status(self, *, latest_attempt: dict[str, Any] | None) -> str:
        if latest_attempt is None:
            return EXECUTION_STATUS_PLANNED
        order_status = _normalize_optional_text((latest_attempt.get('order') or {}).get('current_status'), lowercase=True)
        request_status = _normalize_optional_text(
            (latest_attempt.get('request') or {}).get('submission_status'),
            lowercase=True,
        )
        if order_status in SUCCESS_ORDER_STATUSES or order_status in ACTIVE_ORDER_STATUSES or request_status in {
            'submitted',
            'submitting',
            'pending_submit',
        }:
            return EXECUTION_STATUS_SUBMITTED
        if order_status in FAILED_ORDER_STATUSES or request_status in {'rejected', 'failed'}:
            return EXECUTION_STATUS_SUBMIT_FAILED
        return EXECUTION_STATUS_PLANNED

    def _classify_existing_attempt(
        self,
        *,
        latest_attempt: dict[str, Any],
        current_row: SignalDeciderExecutionPlanItem | None,
        trigger_rule: str | None,
        market_state: MarketState,
    ) -> dict[str, Any] | None:
        order_row = latest_attempt.get('order') or {}
        request_row = latest_attempt.get('request') or {}
        order_status = _normalize_optional_text(order_row.get('current_status'), lowercase=True)
        request_status = _normalize_optional_text(request_row.get('submission_status'), lowercase=True)

        if order_status in ACTIVE_ORDER_STATUSES or request_status in {'submitted', 'submitting', 'pending_submit'} or (
            order_status is None and request_status == 'submitted'
        ):
            return {
                'execution_status': EXECUTION_STATUS_SUBMITTED,
                'reason': _reason(
                    'sell_already_submitted',
                    'A linked intraday auto-sell order is already open or awaiting broker confirmation.',
                ),
            }
        if order_status in SUCCESS_ORDER_STATUSES:
            return {
                'execution_status': EXECUTION_STATUS_SUBMITTED,
                'reason': _reason(
                    'sell_already_filled',
                    'A linked intraday auto-sell order already filled for this position.',
                ),
            }
        if order_status in FAILED_ORDER_STATUSES or request_status in {'rejected', 'failed'}:
            if trigger_rule is None:
                return None
            latest_failure_at = self._latest_attempt_timestamp(
                current_row=current_row,
                request_row=request_row,
                order_row=order_row,
                market_state=market_state,
            )
            cooldown_until = latest_failure_at + timedelta(
                seconds=max(0, int(settings.signal_decider_intraday_auto_sell_cooldown_seconds))
            )
            reference_now = self._current_time(market_state)
            if current_row is not None:
                target_trade_day_floor = datetime.combine(
                    current_row.target_trade_date,
                    datetime.min.time(),
                    tzinfo=timezone.utc,
                )
                if target_trade_day_floor > reference_now:
                    reference_now = target_trade_day_floor
            if reference_now < cooldown_until:
                return {
                    'execution_status': EXECUTION_STATUS_SUBMIT_FAILED,
                    'cooldown_until': cooldown_until.isoformat(),
                    'reason': _reason(
                        'retry_cooldown_active',
                        'A prior linked auto-sell attempt failed and is still inside the retry cooldown window.',
                        cooldown_until=cooldown_until.isoformat(),
                    ),
                }
        return None

    def _latest_attempt_timestamp(
        self,
        *,
        current_row: SignalDeciderExecutionPlanItem | None,
        request_row: dict[str, Any],
        order_row: dict[str, Any],
        market_state: MarketState,
    ) -> datetime:
        latest_payload = (current_row.submission_payload or {}).get('latest') if current_row is not None else {}
        payload_executed_at = _as_utc(_parse_datetime((latest_payload or {}).get('executed_at')))
        order_last_update_at = _as_utc(_parse_datetime(order_row.get('last_broker_update_at')))
        order_first_submitted_at = _as_utc(_parse_datetime(order_row.get('first_submitted_at')))
        request_submitted_at = _as_utc(_parse_datetime(request_row.get('submitted_at')))
        request_updated_at = _as_utc(_parse_datetime(request_row.get('updated_at')))

        # When broker timestamps are explicitly rewound earlier than the original submit time,
        # treat that broker update as the cooldown anchor for the next retry decision.
        submit_anchor = request_submitted_at or order_first_submitted_at
        if (
            order_last_update_at is not None
            and submit_anchor is not None
            and order_last_update_at < submit_anchor
        ):
            return order_last_update_at

        return (
            payload_executed_at
            or order_last_update_at
            or order_first_submitted_at
            or request_submitted_at
            or request_updated_at
            or self._current_time(market_state)
        )

    def _resolve_accounts(
        self,
        *,
        account_reference: str | None,
    ) -> tuple[list[MonitoredAccount], list[str], str]:
        if account_reference:
            try:
                scope = parse_kis_account_reference(account_reference)
            except ValueError as exc:
                return [], [str(exc)], 'request_override'
            return [
                MonitoredAccount(
                    label=scope.account_reference,
                    account_reference=scope.account_reference,
                    account_number=scope.account_number,
                    account_product_code=scope.account_product_code,
                )
            ], [], 'request_override'

        configured_targets = settings.signal_decider_next_open_account_targets.strip()
        if configured_targets:
            return self._parse_next_open_account_targets(configured_targets)
        return self._discover_accounts_from_next_open_history()

    def _parse_next_open_account_targets(self, raw_value: str) -> tuple[list[MonitoredAccount], list[str], str]:
        accounts: list[MonitoredAccount] = []
        errors: list[str] = []
        seen: set[str] = set()
        for entry in [item.strip() for item in raw_value.split(',') if item.strip()]:
            label = None
            payload = entry
            if '=' in entry:
                label, payload = entry.split('=', 1)
                label = label.strip() or None
            account_reference = payload.split('@', 1)[0].strip()
            try:
                scope = parse_kis_account_reference(account_reference)
            except ValueError as exc:
                errors.append(f'Invalid SIGNAL_DECIDER_NEXT_OPEN_ACCOUNT_TARGETS entry "{entry}": {exc}')
                continue
            if scope.account_reference in seen:
                continue
            seen.add(scope.account_reference)
            accounts.append(
                MonitoredAccount(
                    label=label or scope.account_reference,
                    account_reference=scope.account_reference,
                    account_number=scope.account_number,
                    account_product_code=scope.account_product_code,
                )
            )
        return accounts, errors, 'signal_decider_next_open_account_targets'

    def _discover_accounts_from_next_open_history(self) -> tuple[list[MonitoredAccount], list[str], str]:
        with session_scope() as session:
            rows = session.execute(
                select(SignalDeciderExecutionPlanItem)
                .where(SignalDeciderExecutionPlanItem.execution_window == EXECUTION_WINDOW_NEXT_OPEN)
                .order_by(
                    SignalDeciderExecutionPlanItem.target_trade_date.desc(),
                    SignalDeciderExecutionPlanItem.submitted_at.desc(),
                    SignalDeciderExecutionPlanItem.id.desc(),
                )
            ).scalars().all()
            for row in rows:
                session.expunge(row)

        accounts: list[MonitoredAccount] = []
        errors: list[str] = []
        seen: set[str] = set()
        for row in rows:
            if row.account_reference in seen:
                continue
            seen.add(row.account_reference)
            try:
                scope = parse_kis_account_reference(row.account_reference)
            except ValueError as exc:
                errors.append(f'Invalid next-open account reference "{row.account_reference}": {exc}')
                continue
            accounts.append(
                MonitoredAccount(
                    label=row.account_label or scope.account_reference,
                    account_reference=scope.account_reference,
                    account_number=scope.account_number,
                    account_product_code=scope.account_product_code,
                )
            )
        return accounts, errors, 'next_open_execution_history'

    def _load_linked_source_plans(
        self,
        *,
        account_reference: str,
        symbol: str,
    ) -> list[LinkedNextOpenPlan]:
        with session_scope() as session:
            rows = session.execute(
                select(SignalDeciderExecutionPlanItem)
                .where(
                    SignalDeciderExecutionPlanItem.execution_window == EXECUTION_WINDOW_NEXT_OPEN,
                    SignalDeciderExecutionPlanItem.account_reference == account_reference,
                    SignalDeciderExecutionPlanItem.symbol == symbol,
                    SignalDeciderExecutionPlanItem.planned_side == 'buy',
                )
                .order_by(
                    SignalDeciderExecutionPlanItem.target_trade_date.desc(),
                    SignalDeciderExecutionPlanItem.submitted_at.desc(),
                    SignalDeciderExecutionPlanItem.id.desc(),
                )
            ).scalars().all()
            for row in rows:
                session.expunge(row)

        matches: list[LinkedNextOpenPlan] = []
        for row in rows:
            request = (
                load_order_request(
                    account_reference=account_reference,
                    client_order_id=row.client_order_id,
                )
                if row.client_order_id
                else None
            )
            order = (
                load_broker_order(
                    account_reference=account_reference,
                    client_order_id=row.client_order_id,
                )
                if row.client_order_id
                else None
            )
            filled_quantity = _safe_decimal((order or {}).get('filled_quantity')) or Decimal('0')
            current_status = _normalize_optional_text((order or {}).get('current_status'), lowercase=True)
            if filled_quantity > Decimal('0') or current_status in {
                BrokerOrderStatus.PARTIAL_FILL.value,
                BrokerOrderStatus.FILLED.value,
            }:
                matches.append(LinkedNextOpenPlan(row=row, request=request, order=order))
        return matches

    def _load_auto_sell_attempt_pairs(
        self,
        *,
        account_reference: str,
        symbol: str,
        decision_snapshot_id: int,
    ) -> list[dict[str, Any]]:
        rows = list_broker_orders(
            account_reference=account_reference,
            symbol=symbol,
            limit=100,
        )
        matches: list[dict[str, Any]] = []
        for order_row in rows:
            client_order_id = _normalize_optional_text(order_row.get('client_order_id'))
            if client_order_id is None:
                continue
            request_row = load_order_request(
                account_reference=account_reference,
                client_order_id=client_order_id,
            )
            if not isinstance(request_row, dict) or request_row.get('source') != ORDER_SOURCE:
                continue
            strategy_payload = request_row.get('strategy_context_payload') or {}
            auto_sell_payload = strategy_payload.get('signal_decider_intraday_auto_sell') or {}
            if int(auto_sell_payload.get('decision_snapshot_id') or 0) != int(decision_snapshot_id):
                continue
            matches.append({'request': request_row, 'order': order_row})

        matches.sort(
            key=lambda pair: _sort_key_for_order_pair(pair.get('order'), pair.get('request')),
            reverse=True,
        )
        return matches

    def _load_snapshot(self, snapshot_id: int | None) -> SignalDeciderDecisionSnapshot | None:
        if snapshot_id is None:
            return None
        with session_scope() as session:
            row = session.get(SignalDeciderDecisionSnapshot, int(snapshot_id))
            if row is None:
                return None
            session.expunge(row)
            return row

    def _upsert_watch_row(
        self,
        *,
        account: MonitoredAccount,
        source: LinkedNextOpenPlan,
        snapshot: SignalDeciderDecisionSnapshot,
        symbol: str,
        observed_price: Decimal | None,
        watched_quantity: Decimal,
        eligibility_status: str,
        execution_status: str,
        validation_payload: dict[str, Any],
        traceability_payload: dict[str, Any],
        latest_attempt: dict[str, Any] | None,
    ) -> int:
        now = _utcnow()
        estimated_reference_price = int(observed_price) if observed_price is not None and observed_price > Decimal('0') else None
        estimated_notional_krw = watched_quantity * observed_price if estimated_reference_price is not None else None
        latest_request = latest_attempt.get('request') if latest_attempt else None
        latest_order = latest_attempt.get('order') if latest_attempt else None

        with session_scope() as session:
            row = session.execute(
                select(SignalDeciderExecutionPlanItem).where(
                    SignalDeciderExecutionPlanItem.execution_window == EXECUTION_WINDOW_INTRADAY_AUTO_SELL,
                    SignalDeciderExecutionPlanItem.target_trade_date == source.row.target_trade_date,
                    SignalDeciderExecutionPlanItem.account_reference == account.account_reference,
                    SignalDeciderExecutionPlanItem.symbol == symbol,
                )
            ).scalar_one_or_none()

            if row is None:
                row = SignalDeciderExecutionPlanItem(
                    execution_window=EXECUTION_WINDOW_INTRADAY_AUTO_SELL,
                    decision_snapshot_id=snapshot.id,
                    broker_name='kis',
                    target_trade_date=source.row.target_trade_date,
                    source_market_date=source.row.source_market_date,
                    account_reference=account.account_reference,
                    account_label=account.label,
                    symbol=symbol,
                    policy_version=snapshot.policy_version,
                    planned_side='sell',
                    planned_order_type='market',
                    planned_time_in_force='day',
                    allocation_budget_krw=_safe_decimal(source.row.allocation_budget_krw) or Decimal('0'),
                    estimated_reference_price=estimated_reference_price,
                    planned_quantity=watched_quantity,
                    estimated_notional_krw=estimated_notional_krw,
                    eligibility_status=eligibility_status,
                    execution_status=execution_status,
                    client_order_id=_normalize_optional_text((latest_request or {}).get('client_order_id'))
                    or _normalize_optional_text((latest_order or {}).get('client_order_id')),
                    order_request_id=(latest_request or {}).get('id'),
                    broker_order_row_id=(latest_order or {}).get('id'),
                    validation_payload=validation_payload,
                    traceability_payload=traceability_payload,
                    submission_payload=None,
                    first_ready_at=now if eligibility_status == ELIGIBILITY_ELIGIBLE else None,
                    last_evaluated_at=now,
                    submitted_at=_parse_datetime((latest_request or {}).get('submitted_at'))
                    or _parse_datetime((latest_order or {}).get('first_submitted_at')),
                )
                session.add(row)
                session.flush()
            else:
                row.decision_snapshot_id = snapshot.id
                row.broker_name = 'kis'
                row.target_trade_date = source.row.target_trade_date
                row.source_market_date = source.row.source_market_date
                row.account_reference = account.account_reference
                row.account_label = account.label
                row.symbol = symbol
                row.policy_version = snapshot.policy_version
                row.planned_side = 'sell'
                row.planned_order_type = 'market'
                row.planned_time_in_force = 'day'
                row.allocation_budget_krw = _safe_decimal(source.row.allocation_budget_krw) or Decimal('0')
                row.estimated_reference_price = estimated_reference_price
                row.planned_quantity = watched_quantity
                row.estimated_notional_krw = estimated_notional_krw
                row.eligibility_status = eligibility_status
                row.execution_status = execution_status
                row.client_order_id = (
                    _normalize_optional_text((latest_request or {}).get('client_order_id'))
                    or _normalize_optional_text((latest_order or {}).get('client_order_id'))
                    or row.client_order_id
                )
                row.order_request_id = (latest_request or {}).get('id') or row.order_request_id
                row.broker_order_row_id = (latest_order or {}).get('id') or row.broker_order_row_id
                row.validation_payload = validation_payload
                row.traceability_payload = traceability_payload
                row.last_evaluated_at = now
                if eligibility_status == ELIGIBILITY_ELIGIBLE and row.first_ready_at is None:
                    row.first_ready_at = now
                if row.submitted_at is None and execution_status == EXECUTION_STATUS_SUBMITTED:
                    row.submitted_at = _parse_datetime((latest_request or {}).get('submitted_at')) or _parse_datetime(
                        (latest_order or {}).get('first_submitted_at')
                    )
                session.flush()

            row_id = row.id
        return row_id

    def _load_watch_row(
        self,
        *,
        target_trade_date: date,
        account_reference: str,
        symbol: str,
    ) -> SignalDeciderExecutionPlanItem | None:
        with session_scope() as session:
            row = session.execute(
                select(SignalDeciderExecutionPlanItem).where(
                    SignalDeciderExecutionPlanItem.execution_window == EXECUTION_WINDOW_INTRADAY_AUTO_SELL,
                    SignalDeciderExecutionPlanItem.target_trade_date == target_trade_date,
                    SignalDeciderExecutionPlanItem.account_reference == account_reference,
                    SignalDeciderExecutionPlanItem.symbol == symbol,
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            session.expunge(row)
            return row

    def _load_watch_row_by_id(self, *, row_id: int) -> SignalDeciderExecutionPlanItem:
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, row_id)
            if row is None:
                raise ValueError(f'plan item {row_id} was not found')
            session.expunge(row)
            return row

    def _load_watch_rows(
        self,
        *,
        account_reference: str | None,
        symbol: str | None,
    ) -> list[SignalDeciderExecutionPlanItem]:
        with session_scope() as session:
            stmt = (
                select(SignalDeciderExecutionPlanItem)
                .where(SignalDeciderExecutionPlanItem.execution_window == EXECUTION_WINDOW_INTRADAY_AUTO_SELL)
                .order_by(
                    SignalDeciderExecutionPlanItem.target_trade_date.desc(),
                    SignalDeciderExecutionPlanItem.account_reference.asc(),
                    SignalDeciderExecutionPlanItem.symbol.asc(),
                )
            )
            if account_reference:
                stmt = stmt.where(SignalDeciderExecutionPlanItem.account_reference == account_reference)
            if symbol:
                stmt = stmt.where(SignalDeciderExecutionPlanItem.symbol == symbol)
            rows = session.execute(stmt).scalars().all()
            for row in rows:
                session.expunge(row)
            return rows

    def _build_summary(
        self,
        *,
        items: list[dict[str, Any]],
        account_errors: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            'items_total': len(items),
            'eligible_count': sum(1 for item in items if item.get('eligibility_status') == ELIGIBILITY_ELIGIBLE),
            'ready_count': sum(1 for item in items if item.get('eligibility_status') == ELIGIBILITY_ELIGIBLE),
            'target_ready_count': sum(
                1
                for item in items
                if item.get('eligibility_status') == ELIGIBILITY_ELIGIBLE
                and ((item.get('validation') or {}).get('trigger') or {}).get('rule') == TRIGGER_RULE_TARGET_HIT
            ),
            'stop_ready_count': sum(
                1
                for item in items
                if item.get('eligibility_status') == ELIGIBILITY_ELIGIBLE
                and ((item.get('validation') or {}).get('trigger') or {}).get('rule') == TRIGGER_RULE_STOP_LOSS_HIT
            ),
            'blocked_count': sum(1 for item in items if item.get('eligibility_status') == ELIGIBILITY_BLOCKED),
            'planned_count': sum(1 for item in items if item.get('execution_status') == EXECUTION_STATUS_PLANNED),
            'dry_run_count': sum(1 for item in items if item.get('execution_status') == EXECUTION_STATUS_DRY_RUN),
            'submitted_count': sum(1 for item in items if item.get('execution_status') == EXECUTION_STATUS_SUBMITTED),
            'submit_failed_count': sum(
                1 for item in items if item.get('execution_status') == EXECUTION_STATUS_SUBMIT_FAILED
            ),
            'account_error_count': len(account_errors),
        }

    def _serialize_row(self, row: SignalDeciderExecutionPlanItem) -> dict[str, Any]:
        linked_order_request = (
            load_order_request(
                account_reference=row.account_reference,
                client_order_id=row.client_order_id,
            )
            if row.client_order_id
            else None
        )
        linked_broker_order = (
            load_broker_order(
                account_reference=row.account_reference,
                client_order_id=row.client_order_id,
            )
            if row.client_order_id
            else None
        )
        traceability = row.traceability_payload if isinstance(row.traceability_payload, dict) else {}
        linked_next_open_client_order_id = _normalize_optional_text(traceability.get('linked_next_open_client_order_id'))
        linked_source_order_request = (
            load_order_request(
                account_reference=row.account_reference,
                client_order_id=linked_next_open_client_order_id,
            )
            if linked_next_open_client_order_id
            else None
        )
        linked_source_broker_order = (
            load_broker_order(
                account_reference=row.account_reference,
                client_order_id=linked_next_open_client_order_id,
            )
            if linked_next_open_client_order_id
            else None
        )
        return {
            'plan_item_id': row.id,
            'execution_window': row.execution_window,
            'target_trade_date': row.target_trade_date.isoformat(),
            'source_market_date': row.source_market_date.isoformat(),
            'account_reference': row.account_reference,
            'account_label': row.account_label,
            'symbol': row.symbol,
            'policy_version': row.policy_version,
            'decision_snapshot_id': row.decision_snapshot_id,
            'planned_side': row.planned_side,
            'planned_order_type': row.planned_order_type,
            'planned_time_in_force': row.planned_time_in_force,
            'planned_quantity': _decimal_to_text(_safe_decimal(row.planned_quantity)),
            'estimated_reference_price': row.estimated_reference_price,
            'estimated_notional_krw': _decimal_to_text(_safe_decimal(row.estimated_notional_krw)),
            'eligibility_status': row.eligibility_status,
            'execution_status': row.execution_status,
            'client_order_id': row.client_order_id,
            'order_request_id': row.order_request_id,
            'broker_order_row_id': row.broker_order_row_id,
            'validation': row.validation_payload,
            'traceability': traceability,
            'submission': row.submission_payload,
            'first_ready_at': row.first_ready_at.isoformat() if row.first_ready_at else None,
            'last_evaluated_at': row.last_evaluated_at.isoformat() if row.last_evaluated_at else None,
            'submitted_at': row.submitted_at.isoformat() if row.submitted_at else None,
            'linked_order_request': linked_order_request,
            'linked_broker_order': linked_broker_order,
            'linked_source_order_request': linked_source_order_request,
            'linked_source_broker_order': linked_source_broker_order,
        }

    def _mark_dry_run(self, *, row_id: int, trigger: str) -> dict[str, Any]:
        now = _utcnow()
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, row_id)
            if row is None:
                raise ValueError(f'plan item {row_id} was not found')
            existing_submission = row.submission_payload if isinstance(row.submission_payload, dict) else {}
            validation = row.validation_payload if isinstance(row.validation_payload, dict) else {}
            row.execution_status = EXECUTION_STATUS_DRY_RUN
            row.submission_payload = {
                **existing_submission,
                'latest': {
                    'mode': 'dry_run',
                    'trigger': trigger,
                    'executed_at': now.isoformat(),
                    'rule': ((validation.get('trigger') or {}).get('rule')),
                },
            }
            row.last_evaluated_at = now
            linked_entry_plan_item_id = _normalize_int(
                ((row.traceability_payload or {}).get('linked_next_open_plan_item_id'))
            )
            session.flush()
        if linked_entry_plan_item_id is not None:
            signal_decider_execution_outcome_service.sync_entry_plan_item(
                entry_plan_item_id=linked_entry_plan_item_id
            )
        serialized = self._serialize_row(self._load_watch_row_by_id(row_id=row_id))
        serialized['idempotent_replay'] = False
        return serialized

    async def _submit_watch_row(self, *, row_id: int, trigger: str) -> dict[str, Any]:
        row = self._load_watch_row_by_id(row_id=row_id)
        quantity = _safe_decimal(row.planned_quantity)
        if quantity is None or quantity <= Decimal('0'):
            raise ValueError(f'plan item {row_id} does not have a sellable linked quantity')

        validation = row.validation_payload if isinstance(row.validation_payload, dict) else {}
        traceability = row.traceability_payload if isinstance(row.traceability_payload, dict) else {}
        trigger_payload = validation.get('trigger') or {}
        trigger_rule = _normalize_optional_text(trigger_payload.get('rule'), lowercase=True)
        if trigger_rule not in {TRIGGER_RULE_TARGET_HIT, TRIGGER_RULE_STOP_LOSS_HIT}:
            raise ValueError(f'plan item {row_id} does not have an active trigger rule')

        attempt_number = self._next_attempt_number(row=row)
        client_order_id = self._build_client_order_id(
            target_trade_date=row.target_trade_date,
            symbol=row.symbol,
            trigger_rule=trigger_rule,
            attempt_number=attempt_number,
        )
        scope = parse_kis_account_reference(row.account_reference)
        strategy_context_payload = {
            'signal_decider_intraday_auto_sell': {
                'plan_item_id': row.id,
                'execution_window': row.execution_window,
                'decision_snapshot_id': row.decision_snapshot_id,
                'linked_next_open_plan_item_id': traceability.get('linked_next_open_plan_item_id'),
                'linked_next_open_client_order_id': traceability.get('linked_next_open_client_order_id'),
                'linked_next_open_order_request_id': traceability.get('linked_next_open_order_request_id'),
                'linked_next_open_broker_order_row_id': traceability.get('linked_next_open_broker_order_row_id'),
                'source_target_trade_date': row.target_trade_date.isoformat(),
                'source_market_date': row.source_market_date.isoformat(),
                'account_reference': row.account_reference,
                'account_label': row.account_label,
                'symbol': row.symbol,
                'trigger_rule': trigger_rule,
                'target_price': traceability.get('target_price'),
                'stop_loss_price': traceability.get('stop_loss_price'),
                'trigger_price': traceability.get('target_price')
                if trigger_rule == TRIGGER_RULE_TARGET_HIT
                else traceability.get('stop_loss_price'),
                'observed_price': trigger_payload.get('live_price'),
                'observed_at': trigger_payload.get('observed_at'),
                'suggested_holding_period': traceability.get('suggested_holding_period'),
                'attempt': attempt_number,
                'trigger': trigger,
            }
        }
        attempted_at = self._current_time()
        try:
            result = await self.order_service.submit_sell_order(
                client_order_id=client_order_id,
                account_reference=row.account_reference,
                account_number=scope.account_number,
                account_product_code=scope.account_product_code,
                symbol=row.symbol,
                quantity=quantity,
                price=None,
                order_type='market',
                time_in_force='day',
                exchange_id='KRX',
                source=ORDER_SOURCE,
                strategy_context_payload=strategy_context_payload,
            )
            request = result.get('request') or {}
            order = result.get('order') or {}
            idempotent_replay = bool(result.get('idempotent_replay'))
            execution_status = EXECUTION_STATUS_SUBMITTED
            self._patch_submission_result(
                row_id=row_id,
                execution_status=execution_status,
                client_order_id=client_order_id,
                request=request,
                order=order,
                payload={
                    'mode': 'live',
                    'trigger': trigger,
                    'rule': trigger_rule,
                    'attempt': attempt_number,
                    'executed_at': attempted_at.isoformat(),
                    'client_order_id': client_order_id,
                    'idempotent_replay': idempotent_replay,
                    'request_status': request.get('submission_status'),
                    'order_status': order.get('current_status'),
                },
                submitted=True,
            )
            refreshed = self._serialize_row(self._load_watch_row_by_id(row_id=row_id))
            refreshed['idempotent_replay'] = idempotent_replay
            return refreshed
        except TradingOrderPendingSubmissionError as exc:
            self._patch_submission_result(
                row_id=row_id,
                execution_status=EXECUTION_STATUS_SUBMITTED,
                client_order_id=client_order_id,
                request=(exc.payload or {}).get('request') or {},
                order=(exc.payload or {}).get('order') or {},
                payload={
                    'mode': 'live',
                    'trigger': trigger,
                    'rule': trigger_rule,
                    'attempt': attempt_number,
                    'executed_at': attempted_at.isoformat(),
                    'client_order_id': client_order_id,
                    'pending_confirmation': True,
                    'error': str(exc),
                },
                submitted=True,
            )
        except (TradingOrderRejectedError, TradingOrderUpstreamError, TradingOrderValidationError) as exc:
            self._patch_submission_result(
                row_id=row_id,
                execution_status=EXECUTION_STATUS_SUBMIT_FAILED,
                client_order_id=client_order_id,
                request=(exc.payload or {}).get('request') or {},
                order=(exc.payload or {}).get('order') or {},
                payload={
                    'mode': 'live',
                    'trigger': trigger,
                    'rule': trigger_rule,
                    'attempt': attempt_number,
                    'executed_at': attempted_at.isoformat(),
                    'client_order_id': client_order_id,
                    'error': str(exc),
                    'error_detail': exc.payload,
                },
                submitted=False,
            )

        refreshed = self._serialize_row(self._load_watch_row_by_id(row_id=row_id))
        refreshed['idempotent_replay'] = False
        return refreshed

    def _patch_submission_result(
        self,
        *,
        row_id: int,
        execution_status: str,
        client_order_id: str,
        request: dict[str, Any],
        order: dict[str, Any],
        payload: dict[str, Any],
        submitted: bool,
    ) -> None:
        now = _utcnow()
        safe_payload = _json_safe(payload)
        linked_entry_plan_item_id: int | None = None
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, row_id)
            if row is None:
                raise ValueError(f'plan item {row_id} was not found')
            existing_submission = row.submission_payload if isinstance(row.submission_payload, dict) else {}
            attempts = list(existing_submission.get('attempts') or [])
            attempts.append(safe_payload)
            row.execution_status = execution_status
            row.client_order_id = client_order_id
            row.order_request_id = request.get('id') or row.order_request_id
            row.broker_order_row_id = order.get('id') or row.broker_order_row_id
            row.submission_payload = {
                **existing_submission,
                'latest': safe_payload,
                'attempts': attempts,
            }
            row.last_evaluated_at = now
            linked_entry_plan_item_id = _normalize_int(
                ((row.traceability_payload or {}).get('linked_next_open_plan_item_id'))
            )
            if submitted:
                row.submitted_at = row.submitted_at or _parse_datetime(request.get('submitted_at')) or _parse_datetime(
                    order.get('first_submitted_at')
                ) or now
            session.flush()
        if linked_entry_plan_item_id is not None:
            signal_decider_execution_outcome_service.sync_entry_plan_item(
                entry_plan_item_id=linked_entry_plan_item_id
            )

    def _next_attempt_number(self, *, row: SignalDeciderExecutionPlanItem) -> int:
        submission_payload = row.submission_payload if isinstance(row.submission_payload, dict) else {}
        persisted_attempts = len(submission_payload.get('attempts') or [])
        historical_attempts = len(
            self._load_auto_sell_attempt_pairs(
                account_reference=row.account_reference,
                symbol=row.symbol,
                decision_snapshot_id=row.decision_snapshot_id,
            )
        )
        return max(persisted_attempts, historical_attempts) + 1

    def _build_client_order_id(
        self,
        *,
        target_trade_date: date,
        symbol: str,
        trigger_rule: str,
        attempt_number: int,
    ) -> str:
        rule_code = 'TGT' if trigger_rule == TRIGGER_RULE_TARGET_HIT else 'STP'
        return f'SDIS-{target_trade_date.strftime("%Y%m%d")}-{symbol}-{rule_code}-A{attempt_number}'

    def _summarize_attempt_pair(self, pair: dict[str, Any] | None) -> dict[str, Any] | None:
        if pair is None:
            return None
        request = pair.get('request') or {}
        order = pair.get('order') or {}
        payload = (request.get('strategy_context_payload') or {}).get('signal_decider_intraday_auto_sell') or {}
        return {
            'client_order_id': request.get('client_order_id') or order.get('client_order_id'),
            'attempt': payload.get('attempt'),
            'rule': payload.get('trigger_rule'),
            'submission_status': request.get('submission_status'),
            'order_status': order.get('current_status'),
            'submitted_at': request.get('submitted_at').isoformat()
            if isinstance(request.get('submitted_at'), datetime)
            else request.get('submitted_at'),
            'last_broker_update_at': order.get('last_broker_update_at').isoformat()
            if isinstance(order.get('last_broker_update_at'), datetime)
            else order.get('last_broker_update_at'),
        }


signal_decider_intraday_auto_sell_service = SignalDeciderIntradayAutoSellService()
