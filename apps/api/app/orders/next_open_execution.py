from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Callable, Sequence

import exchange_calendars as xcals
import pandas as pd
from sqlalchemy import select

from app.agent_context.roles import SHORT_TERM_DECIDER_AGENT_NAME, SIGNAL_DECIDER_AGENT_NAME
from app.agent_context.signal_decider_loop_service import signal_decider_loop_service
from app.core.settings import settings
from app.db import session_scope
from app.db_models import SignalDeciderDecisionSnapshot, SignalDeciderExecutionPlanItem
from app.market.clock import MarketState, get_market_state
from app.market_data_store import load_daily_bars_for_symbol_range
from app.orders.account_scope import parse_kis_account_reference
from app.orders.signal_decider_execution_outcomes import signal_decider_execution_outcome_service
from app.orders.service import (
    TradingOrderPendingSubmissionError,
    TradingOrderRejectedError,
    TradingOrderService,
    TradingOrderUpstreamError,
    TradingOrderValidationError,
    trading_order_service,
)
from app.orders.store import load_broker_order, load_order_request

EXECUTION_WINDOW_NEXT_OPEN = 'next_open'
ELIGIBILITY_ELIGIBLE = 'eligible'
ELIGIBILITY_BLOCKED = 'blocked'
EXECUTION_STATUS_PLANNED = 'planned'
EXECUTION_STATUS_DRY_RUN = 'dry_run'
EXECUTION_STATUS_SUBMITTED = 'submitted'
EXECUTION_STATUS_SUBMIT_FAILED = 'submit_failed'
ORDER_SOURCE = 'signal_decider_next_open'
SHORT_TERM_ACTIONABLE_ACTIONS = {'scalp_buy_candidate', 'watch_for_trigger'}
KRX_EXCHANGE_CALENDAR = xcals.get_calendar('XKRX')
UNSPECIFIED_ROUTE_ACCOUNT_REFERENCE = 'KIS:00000000-00'
UNSPECIFIED_ROUTE_ACCOUNT_LABEL = 'UNSPECIFIED_ROUTE'
UNSPECIFIED_ROUTE_ACCOUNT_BUDGET_KRW = Decimal('0')
# Short-term candidates are no longer blocked when account routing is omitted.
# They still need a concrete row shape for exec routing; this synthetic rail
# is treated as a routing/budget placeholder and does not drive fixed sizing.


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_date(value: str | date | None, *, default: date) -> date:
    if value is None:
        return default
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _normalize_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _normalize_action(value: Any) -> str:
    normalized = _normalize_optional_text(value)
    return normalized.lower() if normalized else 'wait'


def _normalize_agent_name(value: Any) -> str:
    normalized = _normalize_optional_text(value)
    return normalized.lower() if normalized else SIGNAL_DECIDER_AGENT_NAME


def _to_decimal(value: Any) -> Decimal | None:
    if value is None or value == '':
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _decimal_to_float(value: Decimal | None) -> float | None:
    if value is None:
        return None
    return float(value)


def _decimal_to_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, 'f')


def _to_json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return _decimal_to_text(value)
    if isinstance(value, dict):
        return {str(key): _to_json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_to_json_safe(item) for item in value]
    return value


def _to_int(value: Any) -> int | None:
    if value is None or value == '':
        return None
    if isinstance(value, int):
        return value
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if parsed != parsed.to_integral_value():
        return None
    return int(parsed)


def _normalize_text_list(value: Any) -> list[str]:
    if isinstance(value, str):
        normalized = value.strip()
        return [normalized] if normalized else []
    if not isinstance(value, list):
        return []
    items: list[str] = []
    for item in value:
        normalized = _normalize_optional_text(item)
        if normalized:
            items.append(normalized)
    return items


def _snapshot_agent_name(snapshot: SignalDeciderDecisionSnapshot) -> str:
    return _normalize_agent_name(snapshot.agent_name)


def _snapshot_response_payload(snapshot: SignalDeciderDecisionSnapshot) -> dict[str, Any]:
    return snapshot.response_payload if isinstance(snapshot.response_payload, dict) else {}


def _snapshot_request_payload(snapshot: SignalDeciderDecisionSnapshot) -> dict[str, Any]:
    return snapshot.request_payload if isinstance(snapshot.request_payload, dict) else {}


def _snapshot_pre_decider_enrichment_context(snapshot: SignalDeciderDecisionSnapshot) -> dict[str, Any]:
    request_payload = _snapshot_request_payload(snapshot)
    context = request_payload.get('pre_decider_enrichment_context')
    if isinstance(context, dict):
        return context
    symbol_day_context = request_payload.get('symbol_day_context')
    if isinstance(symbol_day_context, dict) and isinstance(symbol_day_context.get('pre_decider_enrichment_context'), dict):
        return symbol_day_context['pre_decider_enrichment_context']
    return {}


def _short_term_tactical_action(snapshot: SignalDeciderDecisionSnapshot) -> str:
    payload = _snapshot_response_payload(snapshot)
    action = _normalize_optional_text(
        str(payload.get('tactical_action') or payload.get('preferred_action'))
        if (payload.get('tactical_action') is not None or payload.get('preferred_action') is not None)
        else None
    )
    action = action.lower() if action else 'wait'
    if action in SHORT_TERM_ACTIONABLE_ACTIONS or action in {'avoid_chase', 'wait', 'reject'}:
        return action
    return 'wait'


def _short_term_field_text(snapshot: SignalDeciderDecisionSnapshot, key: str) -> str | None:
    payload = _snapshot_response_payload(snapshot)
    value = payload.get(key)
    return _normalize_optional_text(str(value)) if value is not None else None


def _previous_trading_day(value: date) -> date:
    session_label = pd.Timestamp(value.isoformat())
    if KRX_EXCHANGE_CALENDAR.is_session(session_label):
        return KRX_EXCHANGE_CALENDAR.previous_session(session_label).date()
    return KRX_EXCHANGE_CALENDAR.date_to_session(session_label, direction='previous').date()


def _reason(code: str, message: str) -> dict[str, str]:
    return {'code': code, 'message': message}


@dataclass(frozen=True)
class NextOpenAccountTarget:
    label: str
    account_reference: str
    budget_krw: Decimal


class SignalDeciderNextOpenExecutionService:
    def __init__(
        self,
        *,
        order_service: TradingOrderService | None = None,
        market_state_getter: Callable[[], MarketState] = get_market_state,
    ) -> None:
        self.order_service = order_service or trading_order_service
        self.market_state_getter = market_state_getter

    def runtime_status(self) -> dict[str, Any]:
        targets, errors = self._parse_account_targets()
        return {
            'execution_enabled': bool(settings.signal_decider_next_open_enabled),
            'dry_run': bool(settings.signal_decider_next_open_dry_run),
            'account_targets_configured': bool(settings.signal_decider_next_open_account_targets.strip()),
            'account_targets_valid': not errors,
            'account_target_errors': errors,
            'account_targets': [
                {
                    'label': target.label,
                    'account_reference': target.account_reference,
                    'budget_krw': _decimal_to_text(target.budget_krw),
                }
                for target in targets
            ],
            'safety_rails': {
                'min_confidence_score_10': settings.signal_decider_next_open_min_confidence_score_10,
                'min_expected_return_pct': settings.signal_decider_next_open_min_expected_return_pct,
                'require_active_policy': bool(settings.signal_decider_next_open_require_active_policy),
                'block_on_data_warnings': bool(settings.signal_decider_next_open_block_on_data_warnings),
                'block_on_specialist_disagreement': bool(
                    settings.signal_decider_next_open_block_on_specialist_disagreement
                ),
                'require_target_and_stop': bool(settings.signal_decider_next_open_require_target_and_stop),
            },
        }

    def list_plan_items(
        self,
        *,
        target_trade_date: str | date | None = None,
        account_reference: str | None = None,
        refresh: bool = False,
    ) -> dict[str, Any]:
        resolved_target_date = self._resolve_target_trade_date(target_trade_date)
        if refresh:
            self.refresh_plan_items(
                target_trade_date=resolved_target_date,
                trigger='route_refresh',
            )
        rows = self._load_plan_rows(
            target_trade_date=resolved_target_date,
            account_reference=account_reference,
        )
        runtime = self.runtime_status()
        return {
            'request': {
                'target_trade_date': resolved_target_date.isoformat(),
                'account_reference': account_reference,
                'refresh': refresh,
            },
            'runtime': runtime,
            'summary': self._build_summary(
                rows=rows,
                target_trade_date=resolved_target_date,
                runtime=runtime,
            ),
            'items': [self._serialize_row(row) for row in rows],
        }

    def refresh_plan_if_due(self, *, target_trade_date: str | date | None = None) -> dict[str, Any]:
        return self.refresh_plan_items(
            target_trade_date=target_trade_date,
            trigger='preopen_scheduler',
        )

    def refresh_plan_items(
        self,
        *,
        target_trade_date: str | date | None = None,
        trigger: str = 'manual',
    ) -> dict[str, Any]:
        resolved_target_date = self._resolve_target_trade_date(target_trade_date)
        source_market_date = _previous_trading_day(resolved_target_date)
        runtime = self.runtime_status()
        account_targets, account_target_errors = self._parse_account_targets()
        account_targets_setting_configured = bool(settings.signal_decider_next_open_account_targets.strip())
        snapshots = self._load_latest_snapshots(source_market_date=source_market_date)
        existing_rows = self._load_plan_rows(target_trade_date=resolved_target_date, account_reference=None)
        policy_cache: dict[str, dict[str, Any]] = {}
        price_cache: dict[str, int | None] = {}
        desired_keys: set[tuple[str, str, str]] = set()
        now = _utcnow()

        with session_scope() as session:
            attached_existing = {
                (row.account_reference, row.symbol, _normalize_agent_name(row.decision_agent_name)): session.get(
                    SignalDeciderExecutionPlanItem,
                    row.id,
                )
                for row in existing_rows
            }
            for snapshot in snapshots:
                decision_agent_name = _snapshot_agent_name(snapshot)
                latest_action = (
                    _normalize_action(snapshot.next_day_open_action or snapshot.decision_action)
                    if decision_agent_name == SIGNAL_DECIDER_AGENT_NAME
                    else _short_term_tactical_action(snapshot)
                )
                actionable = latest_action == 'buy' if decision_agent_name == SIGNAL_DECIDER_AGENT_NAME else latest_action in SHORT_TERM_ACTIONABLE_ACTIONS
                if not actionable:
                    self._block_missing_symbol_rows(
                        rows=attached_existing,
                        symbol=snapshot.symbol,
                        decision_agent_name=decision_agent_name,
                        reason=_reason(
                            'latest_snapshot_not_actionable',
                            f'Latest {source_market_date.isoformat()} {decision_agent_name} action is {latest_action}.',
                        ),
                        now=now,
                    )
                    continue

                account_target_blocking_reasons: list[dict[str, str]] = []
                if not account_targets:
                    if decision_agent_name != SHORT_TERM_DECIDER_AGENT_NAME:
                        continue
                    account_targets_for_row = [
                        NextOpenAccountTarget(
                            label=UNSPECIFIED_ROUTE_ACCOUNT_LABEL,
                            account_reference=UNSPECIFIED_ROUTE_ACCOUNT_REFERENCE,
                            budget_krw=UNSPECIFIED_ROUTE_ACCOUNT_BUDGET_KRW,
                        )
                    ]
                    if account_target_errors:
                        account_target_blocking_reasons.append(
                            _reason(
                                'account_targets_invalid',
                                'SIGNAL_DECIDER_NEXT_OPEN_ACCOUNT_TARGETS is invalid; fix configured account '
                                f'targets before dispatching short-term candidates: {"; ".join(account_target_errors)}',
                            )
                        )
                    elif account_targets_setting_configured:
                        account_target_blocking_reasons.append(
                            _reason(
                                'account_targets_empty',
                                'SIGNAL_DECIDER_NEXT_OPEN_ACCOUNT_TARGETS is configured but contains no valid account targets.',
                            )
                        )
                else:
                    account_targets_for_row = account_targets

                for target in account_targets_for_row:
                    desired_keys.add((target.account_reference, snapshot.symbol, decision_agent_name))
                    row = attached_existing.get((target.account_reference, snapshot.symbol, decision_agent_name))
                    if row is None:
                        row = SignalDeciderExecutionPlanItem(
                            execution_window=EXECUTION_WINDOW_NEXT_OPEN,
                            decision_agent_name=decision_agent_name,
                            decision_snapshot_id=snapshot.id,
                            broker_name='kis',
                            target_trade_date=resolved_target_date,
                            source_market_date=source_market_date,
                            account_reference=target.account_reference,
                            account_label=target.label,
                            symbol=snapshot.symbol,
                            policy_version=snapshot.policy_version,
                            planned_side='buy',
                            planned_order_type='market' if decision_agent_name == SIGNAL_DECIDER_AGENT_NAME else 'limit',
                            planned_time_in_force='day',
                            allocation_budget_krw=target.budget_krw,
                            traceability_payload={},
                        )
                        session.add(row)
                        attached_existing[(target.account_reference, snapshot.symbol, decision_agent_name)] = row

                    if row.execution_status == EXECUTION_STATUS_SUBMITTED:
                        continue
                    policy_context = None
                    if decision_agent_name == SIGNAL_DECIDER_AGENT_NAME and snapshot.policy_version not in policy_cache:
                        policy_cache[snapshot.policy_version] = signal_decider_loop_service.get_policy_prompt_context(
                            policy_version=snapshot.policy_version
                        )
                    if decision_agent_name == SIGNAL_DECIDER_AGENT_NAME:
                        policy_context = policy_cache[snapshot.policy_version]
                    validation, computed = self._build_validation_payload(
                        snapshot=snapshot,
                        target=target,
                        policy_context=policy_context,
                        price_cache=price_cache,
                    )
                    if account_target_blocking_reasons:
                        validation['blocking_reasons'] = [
                            *(validation.get('blocking_reasons') or []),
                            *account_target_blocking_reasons,
                        ]
                    self._apply_plan_row(
                        row=row,
                        snapshot=snapshot,
                        target_trade_date=resolved_target_date,
                        source_market_date=source_market_date,
                        target=target,
                        validation_payload=validation,
                        computed=computed,
                        now=now,
                    )

            for key, row in attached_existing.items():
                if key in desired_keys or row is None:
                    continue
                if row.execution_status == EXECUTION_STATUS_SUBMITTED:
                    continue
                self._set_blocked_row(
                    row=row,
                    reason=_reason(
                        'candidate_removed',
                        f'No latest actionable decision remains for {row.decision_agent_name} on this account and symbol.',
                    ),
                    now=now,
                )

            session.flush()

        signal_decider_execution_outcome_service.sync_for_target_trade_date(
            target_trade_date=resolved_target_date,
        )
        rows = self._load_plan_rows(target_trade_date=resolved_target_date, account_reference=None)
        return {
            'request': {
                'target_trade_date': resolved_target_date.isoformat(),
                'trigger': trigger,
            },
            'runtime': runtime,
            'summary': self._build_summary(
                rows=rows,
                target_trade_date=resolved_target_date,
                runtime=runtime,
                source_market_date=source_market_date,
            ),
            'items': [self._serialize_row(row) for row in rows],
        }

    async def run_market_open_execution_if_due(
        self,
        *,
        target_trade_date: str | date | None = None,
        dry_run: bool | None = None,
        force: bool = False,
        trigger: str = 'manual',
    ) -> dict[str, Any]:
        market_state = self.market_state_getter()
        resolved_target_date = self._resolve_target_trade_date(
            target_trade_date,
            market_state=market_state,
        )
        effective_dry_run = settings.signal_decider_next_open_dry_run if dry_run is None else dry_run
        runtime = self.runtime_status()
        if not force:
            today = date.fromisoformat(market_state.server_time[:10])
            if market_state.state != 'OPEN':
                return {
                    'request': {
                        'target_trade_date': resolved_target_date.isoformat(),
                        'dry_run': effective_dry_run,
                        'force': force,
                        'trigger': trigger,
                    },
                    'runtime': {
                        **runtime,
                        'market_state': market_state.state,
                        'server_time': market_state.server_time,
                    },
                    'summary': {
                        'triggered': False,
                        'reason': 'market_not_open',
                        'selected_count': 0,
                        'submitted_count': 0,
                        'dry_run_count': 0,
                        'failed_count': 0,
                        'idempotent_replay_count': 0,
                    },
                    'items': [],
                }
            if resolved_target_date != today:
                return {
                    'request': {
                        'target_trade_date': resolved_target_date.isoformat(),
                        'dry_run': effective_dry_run,
                        'force': force,
                        'trigger': trigger,
                    },
                    'runtime': {
                        **runtime,
                        'market_state': market_state.state,
                        'server_time': market_state.server_time,
                    },
                    'summary': {
                        'triggered': False,
                        'reason': 'target_trade_date_not_today',
                        'selected_count': 0,
                        'submitted_count': 0,
                        'dry_run_count': 0,
                        'failed_count': 0,
                        'idempotent_replay_count': 0,
                    },
                    'items': [],
                }
            if not settings.signal_decider_next_open_enabled and not effective_dry_run:
                return {
                    'request': {
                        'target_trade_date': resolved_target_date.isoformat(),
                        'dry_run': effective_dry_run,
                        'force': force,
                        'trigger': trigger,
                    },
                    'runtime': {
                        **runtime,
                        'market_state': market_state.state,
                        'server_time': market_state.server_time,
                    },
                    'summary': {
                        'triggered': False,
                        'reason': 'execution_disabled',
                        'selected_count': 0,
                        'submitted_count': 0,
                        'dry_run_count': 0,
                        'failed_count': 0,
                        'idempotent_replay_count': 0,
                    },
                    'items': [],
                }

        self.refresh_plan_items(
            target_trade_date=resolved_target_date,
            trigger=f'{trigger}_prepare',
        )
        candidate_rows = self._load_plan_rows(
            target_trade_date=resolved_target_date,
            account_reference=None,
        )
        candidate_rows = [
            row
            for row in candidate_rows
            if _normalize_agent_name(row.decision_agent_name) == SIGNAL_DECIDER_AGENT_NAME
        ]
        if effective_dry_run:
            selected = [
                row.id
                for row in candidate_rows
                if row.eligibility_status == ELIGIBILITY_ELIGIBLE and row.execution_status == EXECUTION_STATUS_PLANNED
            ]
        else:
            selected = [
                row.id
                for row in candidate_rows
                if row.eligibility_status == ELIGIBILITY_ELIGIBLE
                and row.execution_status in {EXECUTION_STATUS_PLANNED, EXECUTION_STATUS_DRY_RUN}
            ]

        results: list[dict[str, Any]] = []
        submitted_count = 0
        dry_run_count = 0
        failed_count = 0
        idempotent_replay_count = 0

        for row_id in selected:
            if effective_dry_run:
                result = self._mark_dry_run(row_id=row_id, trigger=trigger)
                results.append(result)
                dry_run_count += 1
                continue

            result = await self._submit_plan_row(row_id=row_id, trigger=trigger)
            results.append(result)
            execution_status = _normalize_optional_text(result.get('execution_status'))
            if execution_status == EXECUTION_STATUS_SUBMITTED:
                submitted_count += 1
            elif execution_status == EXECUTION_STATUS_SUBMIT_FAILED:
                failed_count += 1
            if bool(result.get('idempotent_replay')):
                idempotent_replay_count += 1

        return {
            'request': {
                'target_trade_date': resolved_target_date.isoformat(),
                'dry_run': effective_dry_run,
                'force': force,
                'trigger': trigger,
            },
            'runtime': {
                **runtime,
                'market_state': market_state.state,
                'server_time': market_state.server_time,
            },
            'summary': {
                'triggered': True,
                'selected_count': len(selected),
                'submitted_count': submitted_count,
                'dry_run_count': dry_run_count,
                'failed_count': failed_count,
                'idempotent_replay_count': idempotent_replay_count,
            },
            'items': results,
        }

    def _resolve_target_trade_date(
        self,
        target_trade_date: str | date | None,
        *,
        market_state: MarketState | None = None,
    ) -> date:
        state = market_state or self.market_state_getter()
        default_date = date.fromisoformat(state.server_time[:10])
        return _normalize_date(target_trade_date, default=default_date)

    def _parse_account_targets(self) -> tuple[list[NextOpenAccountTarget], list[str]]:
        raw_value = settings.signal_decider_next_open_account_targets.strip()
        if not raw_value:
            return [], []
        items: list[NextOpenAccountTarget] = []
        errors: list[str] = []
        seen_accounts: set[str] = set()
        for entry in raw_value.split(','):
            normalized_entry = entry.strip()
            if not normalized_entry:
                continue
            label = None
            payload = normalized_entry
            if '=' in normalized_entry:
                label, payload = normalized_entry.split('=', 1)
                label = label.strip() or None
            if '@' not in payload:
                errors.append(
                    f'Invalid SIGNAL_DECIDER_NEXT_OPEN_ACCOUNT_TARGETS entry "{normalized_entry}". '
                    'Expected Label=KIS:12345678-01@1500000.'
                )
                continue
            account_reference_raw, budget_raw = payload.rsplit('@', 1)
            account_reference = account_reference_raw.strip()
            budget = _to_decimal(budget_raw.strip())
            try:
                parse_kis_account_reference(account_reference)
            except ValueError as exc:
                errors.append(str(exc))
                continue
            if budget is None or budget <= Decimal('0'):
                errors.append(f'Budget must be positive for next-open account target {account_reference}.')
                continue
            if account_reference in seen_accounts:
                errors.append(f'Duplicate next-open account target configured for {account_reference}.')
                continue
            seen_accounts.add(account_reference)
            items.append(
                NextOpenAccountTarget(
                    label=label or account_reference,
                    account_reference=account_reference,
                    budget_krw=budget,
                )
            )
        return items, errors

    def _load_latest_snapshots(self, *, source_market_date: date) -> list[SignalDeciderDecisionSnapshot]:
        with session_scope() as session:
            rows = session.execute(
                select(SignalDeciderDecisionSnapshot)
                .where(
                    SignalDeciderDecisionSnapshot.agent_name.in_(
                        [SIGNAL_DECIDER_AGENT_NAME, SHORT_TERM_DECIDER_AGENT_NAME]
                    ),
                    SignalDeciderDecisionSnapshot.market_date == source_market_date,
                )
                .order_by(
                    SignalDeciderDecisionSnapshot.symbol.asc(),
                    SignalDeciderDecisionSnapshot.agent_name.asc(),
                    SignalDeciderDecisionSnapshot.created_at.desc(),
                    SignalDeciderDecisionSnapshot.id.desc(),
                )
            ).scalars().all()
            for row in rows:
                session.expunge(row)
        deduped: dict[tuple[str, str], SignalDeciderDecisionSnapshot] = {}
        for row in rows:
            deduped.setdefault((row.symbol, _snapshot_agent_name(row)), row)
        return list(deduped.values())

    def _load_plan_rows(
        self,
        *,
        target_trade_date: date,
        account_reference: str | None,
    ) -> list[SignalDeciderExecutionPlanItem]:
        with session_scope() as session:
            stmt = (
                select(SignalDeciderExecutionPlanItem)
                .where(
                    SignalDeciderExecutionPlanItem.execution_window == EXECUTION_WINDOW_NEXT_OPEN,
                    SignalDeciderExecutionPlanItem.target_trade_date == target_trade_date,
                )
                .order_by(
                    SignalDeciderExecutionPlanItem.eligibility_status.desc(),
                    SignalDeciderExecutionPlanItem.execution_status.asc(),
                    SignalDeciderExecutionPlanItem.account_reference.asc(),
                    SignalDeciderExecutionPlanItem.decision_agent_name.asc(),
                    SignalDeciderExecutionPlanItem.symbol.asc(),
                )
            )
            if account_reference:
                stmt = stmt.where(SignalDeciderExecutionPlanItem.account_reference == account_reference)
            rows = session.execute(stmt).scalars().all()
            for row in rows:
                session.expunge(row)
            return rows

    def _build_validation_payload(
        self,
        *,
        snapshot: SignalDeciderDecisionSnapshot,
        target: NextOpenAccountTarget,
        policy_context: dict[str, Any] | None,
        price_cache: dict[str, int | None],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        decision_agent_name = _snapshot_agent_name(snapshot)
        response_payload = _snapshot_response_payload(snapshot)
        pre_decider_enrichment_context = _snapshot_pre_decider_enrichment_context(snapshot)
        blocking_reasons: list[dict[str, str]] = []
        warnings: list[dict[str, str]] = []
        gate_checks = (
            snapshot.traceability_payload.get('gate_checks')
            if isinstance(snapshot.traceability_payload, dict)
            else {}
        ) or {}
        missing_upstream_roles = _normalize_text_list(gate_checks.get('missing_upstream_roles'))
        if missing_upstream_roles:
            blocking_reasons.append(
                _reason(
                    'missing_upstream_roles',
                    f'Missing upstream roles: {", ".join(missing_upstream_roles)}.',
                )
            )

        confidence_score = _to_decimal(snapshot.confidence_score_10)
        if confidence_score is None or confidence_score < Decimal(str(settings.signal_decider_next_open_min_confidence_score_10)):
            blocking_reasons.append(
                _reason(
                    'confidence_below_threshold',
                    'Confidence score is below the next-open automation threshold.',
                )
            )
        if settings.signal_decider_next_open_block_on_data_warnings and _normalize_text_list(
            snapshot.data_quality_warnings_payload
        ):
            blocking_reasons.append(
                _reason(
                    'data_quality_warnings_present',
                    'Snapshot has data quality warnings.',
                )
            )
        if settings.signal_decider_next_open_block_on_specialist_disagreement and _normalize_text_list(
            snapshot.specialist_disagreement_payload
        ):
            blocking_reasons.append(
                _reason(
                    'specialist_disagreement_present',
                    'Snapshot has specialist disagreement warnings.',
                )
            )

        estimated_reference_price = price_cache.get(snapshot.symbol)
        if snapshot.symbol not in price_cache:
            bars = load_daily_bars_for_symbol_range(
                snapshot.symbol,
                start_date=snapshot.market_date,
                end_date=snapshot.market_date,
                limit=1,
                prefer_recent_limit=False,
            )
            estimated_reference_price = bars[-1]['close_price'] if bars else None
            price_cache[snapshot.symbol] = estimated_reference_price
        estimated_reference_price_basis = 'latest_source_market_close'

        if decision_agent_name == SIGNAL_DECIDER_AGENT_NAME:
            policy_context = policy_context or {}
            if settings.signal_decider_next_open_require_active_policy and policy_context.get('status') != 'active':
                blocking_reasons.append(
                    _reason(
                        'policy_not_active',
                        f'Policy {snapshot.policy_version} is {policy_context.get("status") or "unknown"}, not active.',
                    )
                )
            if snapshot.entry_price_basis != 'next_open':
                blocking_reasons.append(
                    _reason(
                        'unsupported_entry_price_basis',
                        f'Entry price basis {snapshot.entry_price_basis} is not next_open.',
                    )
                )
            expected_return = _to_decimal(snapshot.expected_return_pct)
            if expected_return is None or expected_return < Decimal(str(settings.signal_decider_next_open_min_expected_return_pct)):
                blocking_reasons.append(
                    _reason(
                        'expected_return_below_threshold',
                        'Expected return is below the next-open automation threshold.',
                    )
                )
            if settings.signal_decider_next_open_require_target_and_stop and (
                snapshot.target_price is None or snapshot.stop_loss_price is None
            ):
                blocking_reasons.append(
                    _reason(
                        'target_or_stop_missing',
                        'Automated next-open execution requires both target_price and stop_loss_price.',
                    )
                )
            if bool(gate_checks.get('decision_support_only')) or not bool(gate_checks.get('allow_execution_logic', False)):
                warnings.append(
                    _reason(
                        'external_execution_override',
                        'Signal-decider still emitted a decision-support-only contract; trading-core execution is an external opt-in.',
                    )
                )
        else:
            trigger_price = _to_decimal(response_payload.get('trigger_price'))
            chase_limit_price = _to_decimal(response_payload.get('chase_limit_price'))
            target_price = _to_decimal(
                snapshot.target_price if snapshot.target_price is not None else response_payload.get('target_price')
            )
            stop_loss_price = _to_decimal(
                snapshot.stop_loss_price if snapshot.stop_loss_price is not None else response_payload.get('stop_loss_price')
            )
            if trigger_price is None or trigger_price <= 0:
                blocking_reasons.append(
                    _reason(
                        'trigger_price_missing',
                        'Short-term dispatch requires an explicit positive trigger_price.',
                    )
                )
            if chase_limit_price is None or chase_limit_price <= 0:
                blocking_reasons.append(
                    _reason(
                        'chase_limit_price_missing',
                        'Short-term dispatch requires an explicit positive chase_limit_price.',
                    )
                )
            elif trigger_price is not None and chase_limit_price < trigger_price:
                blocking_reasons.append(
                    _reason(
                        'chase_limit_below_trigger',
                        'Short-term chase_limit_price must be greater than or equal to trigger_price.',
                    )
                )
            if target_price is None:
                warnings.append(
                    _reason(
                        'target_price_missing',
                        'Short-term dispatch will omit the target sell monitor because target_price is missing.',
                    )
                )
            if stop_loss_price is None:
                warnings.append(
                    _reason(
                        'stop_loss_price_missing',
                        'Short-term dispatch will omit the stop-loss monitor because stop_loss_price is missing.',
                    )
                )
            if target_price is not None and stop_loss_price is not None and target_price <= stop_loss_price:
                blocking_reasons.append(
                    _reason(
                        'invalid_target_stop_ordering',
                        'Short-term target_price must be greater than stop_loss_price when both are present.',
                    )
                )
            if estimated_reference_price is not None and estimated_reference_price > 0 and chase_limit_price is not None:
                estimated_reference_price = max(int(estimated_reference_price), int(chase_limit_price))
                estimated_reference_price_basis = 'max(previous_close,chase_limit_price)'
            if bool(gate_checks.get('decision_support_only')) or not bool(gate_checks.get('allow_execution_logic', False)):
                warnings.append(
                    _reason(
                        'external_execution_override',
                        'Short-term decisions remain decision-support-only in core; exec dispatch is a guarded external opt-in.',
                    )
                )

        if decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME:
            if estimated_reference_price is None or estimated_reference_price <= 0:
                warnings.append(
                    _reason(
                        'missing_reference_price',
                        'Could not resolve the prior close price used for optional sizing preview.',
                    )
                )
                planned_quantity = None
                estimated_notional = None
            else:
                if target.budget_krw > Decimal('0'):
                    planned_quantity = (target.budget_krw / Decimal(str(estimated_reference_price))).quantize(
                        Decimal('1'),
                        rounding=ROUND_DOWN,
                    )
                    if planned_quantity >= Decimal('1'):
                        estimated_notional = planned_quantity * Decimal(str(estimated_reference_price))
                    else:
                        planned_quantity = None
                        estimated_notional = None
                else:
                    planned_quantity = None
                    estimated_notional = None
            pricing_note = 'Sizing for short-term candidates is delegated to exec-agent.'
        else:
            if estimated_reference_price is None or estimated_reference_price <= 0:
                blocking_reasons.append(
                    _reason(
                        'missing_reference_price',
                        'Could not resolve the prior close price used for next-open quantity sizing.',
                    )
                )
                planned_quantity = None
                estimated_notional = None
            else:
                planned_quantity = (target.budget_krw / Decimal(str(estimated_reference_price))).quantize(
                    Decimal('1'),
                    rounding=ROUND_DOWN,
                )
                if planned_quantity < Decimal('1'):
                    blocking_reasons.append(
                        _reason(
                            'allocation_below_share_size',
                            'Configured account budget does not fund one share at the prior close.',
                        )
                    )
                    estimated_notional = None
                else:
                    estimated_notional = planned_quantity * Decimal(str(estimated_reference_price))
                if target.budget_krw <= Decimal('0'):
                    blocking_reasons.append(
                        _reason(
                            'allocation_below_share_size',
                            'Configured account budget does not fund one share at the prior close.',
                        )
                    )
            pricing_note = 'Configured budget rail is used for local quantity preview only.'

        validation_payload = {
            'blocking_reasons': blocking_reasons,
            'warnings': warnings,
            'decision_agent_name': decision_agent_name,
            'pricing_note': pricing_note,
            'policy_context': {
                'policy_version': snapshot.policy_version,
                'status': (policy_context or {}).get('status'),
                'activation_mode': (policy_context or {}).get('activation_mode'),
            },
            'gate_checks': {
                'missing_upstream_roles': missing_upstream_roles,
                'decision_support_only': bool(gate_checks.get('decision_support_only')),
                'allow_execution_logic': bool(gate_checks.get('allow_execution_logic', False)),
            },
            'pricing': {
                'budget_krw': _decimal_to_text(target.budget_krw),
                'estimated_reference_price': estimated_reference_price,
                'estimated_reference_price_basis': estimated_reference_price_basis,
                'planned_quantity': _decimal_to_text(planned_quantity),
                'estimated_notional_krw': _decimal_to_text(estimated_notional),
            },
            'pre_decider_enrichment_context': pre_decider_enrichment_context,
        }
        if decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME:
            validation_payload['short_term'] = {
                'tactical_action': _short_term_tactical_action(snapshot),
                'trigger_price': response_payload.get('trigger_price'),
                'chase_limit_price': response_payload.get('chase_limit_price'),
                'target_price': response_payload.get('target_price'),
                'stop_loss_price': response_payload.get('stop_loss_price'),
                'budget_krw': _decimal_to_text(target.budget_krw),
                'budget_note': pricing_note,
                'decision_snapshot_id': snapshot.id,
                'decision_run_id': snapshot.decision_run_id,
                'source_feedback_id': snapshot.feedback_id,
                'confidence_score_10': _to_json_safe(snapshot.confidence_score_10),
                'conviction_score_10': _to_json_safe(snapshot.conviction_score_10),
                'thesis_summary': snapshot.thesis_summary,
                'supporting_evidence': snapshot.supporting_evidence_payload,
                'decision_risks': snapshot.decision_risks_payload,
                'invalidation_conditions': snapshot.invalidation_conditions_payload,
                'data_quality_warnings': snapshot.data_quality_warnings_payload,
                'pre_decider_enrichment_context': pre_decider_enrichment_context,
                'response_payload_summary': _to_json_safe(response_payload),
                'response_summary': snapshot.response_summary_payload if isinstance(snapshot.response_summary_payload, dict) else None,
                'expected_holding_period': _short_term_field_text(snapshot, 'expected_holding_period')
                or snapshot.suggested_holding_period,
            }
        return validation_payload, {
            'estimated_reference_price': estimated_reference_price,
            'planned_quantity': planned_quantity,
            'estimated_notional_krw': estimated_notional,
        }

    def _apply_plan_row(
        self,
        *,
        row: SignalDeciderExecutionPlanItem,
        snapshot: SignalDeciderDecisionSnapshot,
        target_trade_date: date,
        source_market_date: date,
        target: NextOpenAccountTarget,
        validation_payload: dict[str, Any],
        computed: dict[str, Any],
        now: datetime,
    ) -> None:
        blocking_reasons = validation_payload.get('blocking_reasons') or []
        decision_agent_name = _snapshot_agent_name(snapshot)
        response_payload = _snapshot_response_payload(snapshot)
        target_price = snapshot.target_price
        stop_loss_price = snapshot.stop_loss_price
        suggested_holding_period = snapshot.suggested_holding_period
        if decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME:
            target_price = snapshot.target_price if snapshot.target_price is not None else _to_int(response_payload.get('target_price'))
            stop_loss_price = (
                snapshot.stop_loss_price if snapshot.stop_loss_price is not None else _to_int(response_payload.get('stop_loss_price'))
            )
            suggested_holding_period = _short_term_field_text(snapshot, 'expected_holding_period') or snapshot.suggested_holding_period
        row.decision_snapshot_id = snapshot.id
        row.decision_agent_name = decision_agent_name
        row.broker_name = 'kis'
        row.target_trade_date = target_trade_date
        row.source_market_date = source_market_date
        row.account_reference = target.account_reference
        row.account_label = target.label
        row.symbol = snapshot.symbol
        row.policy_version = snapshot.policy_version
        row.planned_side = 'buy'
        row.planned_order_type = 'market' if decision_agent_name == SIGNAL_DECIDER_AGENT_NAME else 'limit'
        row.planned_time_in_force = 'day'
        row.allocation_budget_krw = target.budget_krw
        row.estimated_reference_price = computed.get('estimated_reference_price')
        row.planned_quantity = computed.get('planned_quantity')
        row.estimated_notional_krw = computed.get('estimated_notional_krw')
        row.client_order_id = self._build_client_order_id(
            target_trade_date=target_trade_date,
            symbol=snapshot.symbol,
            decision_agent_name=decision_agent_name,
        )
        row.validation_payload = validation_payload
        row.traceability_payload = {
            'agent_name': decision_agent_name,
            'decision_agent_name': decision_agent_name,
            'decision_snapshot_id': snapshot.id,
            'source_feedback_id': snapshot.feedback_id,
            'feedback_id': snapshot.feedback_id,
            'decision_run_id': snapshot.decision_run_id,
            'policy_version': snapshot.policy_version,
            'symbol': snapshot.symbol,
            'target_trade_date': target_trade_date.isoformat(),
            'source_market_date': source_market_date.isoformat(),
            'account_reference': target.account_reference,
            'account_label': target.label,
            'allocation_budget_krw': _decimal_to_text(target.budget_krw),
            'allocation_mode': (
                'specified'
                if target.account_reference != UNSPECIFIED_ROUTE_ACCOUNT_REFERENCE
                else 'unspecified_route'
            ),
            'entry_price_basis': snapshot.entry_price_basis,
            'target_price': target_price,
            'stop_loss_price': stop_loss_price,
            'suggested_holding_period': suggested_holding_period,
            'expected_holding_period': _short_term_field_text(snapshot, 'expected_holding_period')
            or suggested_holding_period,
            'tactical_action': _short_term_tactical_action(snapshot)
            if decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME
            else None,
            'trigger_price': response_payload.get('trigger_price') if decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME else None,
            'chase_limit_price': response_payload.get('chase_limit_price')
            if decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME
            else None,
            'confidence_score_10': _to_json_safe(snapshot.confidence_score_10),
            'conviction_score_10': _to_json_safe(snapshot.conviction_score_10),
            'thesis_summary': snapshot.thesis_summary,
            'supporting_evidence': snapshot.supporting_evidence_payload,
            'decision_risks': snapshot.decision_risks_payload,
            'invalidation_conditions': snapshot.invalidation_conditions_payload,
            'data_quality_warnings': snapshot.data_quality_warnings_payload,
            'pre_decider_enrichment_context': validation_payload.get('pre_decider_enrichment_context') or {},
            'response_summary': snapshot.response_summary_payload if isinstance(snapshot.response_summary_payload, dict) else None,
            'response_payload_summary': _to_json_safe(response_payload),
            'decision_traceability': snapshot.traceability_payload if isinstance(snapshot.traceability_payload, dict) else {},
            'signal_traceability': snapshot.traceability_payload if isinstance(snapshot.traceability_payload, dict) else {},
        }
        row.last_evaluated_at = now
        row.eligibility_status = ELIGIBILITY_BLOCKED if blocking_reasons else ELIGIBILITY_ELIGIBLE
        if row.eligibility_status == ELIGIBILITY_ELIGIBLE and row.first_ready_at is None:
            row.first_ready_at = now
        if row.eligibility_status == ELIGIBILITY_BLOCKED and row.execution_status != EXECUTION_STATUS_SUBMITTED:
            row.execution_status = EXECUTION_STATUS_PLANNED
        elif row.eligibility_status == ELIGIBILITY_ELIGIBLE and row.execution_status not in {
            EXECUTION_STATUS_SUBMITTED,
            EXECUTION_STATUS_DRY_RUN,
            EXECUTION_STATUS_SUBMIT_FAILED,
        }:
            row.execution_status = EXECUTION_STATUS_PLANNED

    def _block_missing_symbol_rows(
        self,
        *,
        rows: dict[tuple[str, str, str], SignalDeciderExecutionPlanItem | None],
        symbol: str,
        decision_agent_name: str,
        reason: dict[str, str],
        now: datetime,
    ) -> None:
        for key, row in rows.items():
            if (
                row is None
                or key[1] != symbol
                or key[2] != decision_agent_name
                or row.execution_status == EXECUTION_STATUS_SUBMITTED
            ):
                continue
            self._set_blocked_row(row=row, reason=reason, now=now)

    def _set_blocked_row(
        self,
        *,
        row: SignalDeciderExecutionPlanItem,
        reason: dict[str, str],
        now: datetime,
    ) -> None:
        existing = row.validation_payload if isinstance(row.validation_payload, dict) else {}
        row.eligibility_status = ELIGIBILITY_BLOCKED
        if row.execution_status != EXECUTION_STATUS_SUBMITTED:
            row.execution_status = EXECUTION_STATUS_PLANNED
        row.last_evaluated_at = now
        row.validation_payload = {
            **existing,
            'blocking_reasons': [reason],
            'warnings': existing.get('warnings') or [],
        }

    def _build_client_order_id(self, *, target_trade_date: date, symbol: str, decision_agent_name: str) -> str:
        if decision_agent_name == SIGNAL_DECIDER_AGENT_NAME:
            return f'SDNO-{target_trade_date.strftime("%Y%m%d")}-{symbol}'
        if decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME:
            return f'STDE-{target_trade_date.strftime("%Y%m%d")}-{symbol}'
        return f'DEC-{target_trade_date.strftime("%Y%m%d")}-{symbol}'

    def _build_summary(
        self,
        *,
        rows: Sequence[SignalDeciderExecutionPlanItem],
        target_trade_date: date,
        runtime: dict[str, Any],
        source_market_date: date | None = None,
    ) -> dict[str, Any]:
        source_market_date = source_market_date or _previous_trading_day(target_trade_date)
        counts = {
            'items_total': len(rows),
            'eligible_count': sum(1 for row in rows if row.eligibility_status == ELIGIBILITY_ELIGIBLE),
            'blocked_count': sum(1 for row in rows if row.eligibility_status == ELIGIBILITY_BLOCKED),
            'planned_count': sum(1 for row in rows if row.execution_status == EXECUTION_STATUS_PLANNED),
            'dry_run_count': sum(1 for row in rows if row.execution_status == EXECUTION_STATUS_DRY_RUN),
            'submitted_count': sum(1 for row in rows if row.execution_status == EXECUTION_STATUS_SUBMITTED),
            'submit_failed_count': sum(1 for row in rows if row.execution_status == EXECUTION_STATUS_SUBMIT_FAILED),
            'decision_agent_counts': dict(
                sorted(
                    Counter(_normalize_agent_name(row.decision_agent_name) for row in rows).items()
                )
            ),
        }
        return {
            'target_trade_date': target_trade_date.isoformat(),
            'source_market_date': source_market_date.isoformat(),
            **counts,
            'execution_enabled': runtime['execution_enabled'],
            'dry_run': runtime['dry_run'],
            'account_targets_configured': runtime['account_targets_configured'],
            'account_targets_valid': runtime['account_targets_valid'],
            'account_target_errors': runtime['account_target_errors'],
        }

    def _serialize_row(self, row: SignalDeciderExecutionPlanItem) -> dict[str, Any]:
        request = (
            load_order_request(
                account_reference=row.account_reference,
                client_order_id=row.client_order_id,
            )
            if row.client_order_id
            else None
        )
        order = (
            load_broker_order(
                account_reference=row.account_reference,
                client_order_id=row.client_order_id,
            )
            if row.client_order_id
            else None
        )
        return {
            'plan_item_id': row.id,
            'execution_window': row.execution_window,
            'agent_name': row.decision_agent_name,
            'decision_agent_name': row.decision_agent_name,
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
            'allocation_budget_krw': _decimal_to_text(_to_decimal(row.allocation_budget_krw)),
            'estimated_reference_price': row.estimated_reference_price,
            'planned_quantity': _decimal_to_text(_to_decimal(row.planned_quantity)),
            'estimated_notional_krw': _decimal_to_text(_to_decimal(row.estimated_notional_krw)),
            'eligibility_status': row.eligibility_status,
            'execution_status': row.execution_status,
            'client_order_id': row.client_order_id,
            'order_request_id': row.order_request_id,
            'broker_order_row_id': row.broker_order_row_id,
            'validation': row.validation_payload,
            'traceability': row.traceability_payload,
            'submission': row.submission_payload,
            'first_ready_at': row.first_ready_at.isoformat() if row.first_ready_at else None,
            'last_evaluated_at': row.last_evaluated_at.isoformat() if row.last_evaluated_at else None,
            'submitted_at': row.submitted_at.isoformat() if row.submitted_at else None,
            'linked_order_request': request,
            'linked_broker_order': order,
        }

    def _mark_dry_run(self, *, row_id: int, trigger: str) -> dict[str, Any]:
        now = _utcnow()
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, row_id)
            if row is None:
                raise ValueError(f'plan item {row_id} was not found')
            row.execution_status = EXECUTION_STATUS_DRY_RUN
            row.submission_payload = {
                'mode': 'dry_run',
                'trigger': trigger,
                'simulated_at': now.isoformat(),
                'client_order_id': row.client_order_id,
            }
            session.flush()
        signal_decider_execution_outcome_service.sync_entry_plan_item(entry_plan_item_id=row_id)
        row = self._load_plan_row(row_id=row_id)
        serialized = self._serialize_row(row)
        serialized['idempotent_replay'] = False
        return serialized

    async def _submit_plan_row(self, *, row_id: int, trigger: str) -> dict[str, Any]:
        row = self._load_plan_row(row_id=row_id)
        quantity = _to_decimal(row.planned_quantity)
        if row is None or quantity is None:
            raise ValueError(f'plan item {row_id} is missing or does not have a planned quantity')
        scope = parse_kis_account_reference(row.account_reference)
        traceability = row.traceability_payload if isinstance(row.traceability_payload, dict) else {}
        strategy_context_payload = {
            'signal_decider_next_open': {
                'plan_item_id': row.id,
                'execution_window': row.execution_window,
                'target_trade_date': row.target_trade_date.isoformat(),
                'source_market_date': row.source_market_date.isoformat(),
                'decision_snapshot_id': row.decision_snapshot_id,
                'policy_version': row.policy_version,
                'account_reference': row.account_reference,
                'account_label': row.account_label,
                'allocation_budget_krw': _decimal_to_text(_to_decimal(row.allocation_budget_krw)),
                'estimated_reference_price': row.estimated_reference_price,
                'target_price': traceability.get('target_price'),
                'stop_loss_price': traceability.get('stop_loss_price'),
                'suggested_holding_period': traceability.get('suggested_holding_period'),
                'trigger': trigger,
            }
        }
        try:
            result = await self.order_service.submit_buy_order(
                client_order_id=row.client_order_id,
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
            request_status = _normalize_optional_text(request.get('submission_status')) or EXECUTION_STATUS_SUBMITTED
            execution_status = EXECUTION_STATUS_SUBMITTED if request_status in {'submitted', 'submitting'} else EXECUTION_STATUS_SUBMIT_FAILED
            self._patch_submission_result(
                row_id=row_id,
                execution_status=execution_status,
                request=request,
                order=order,
                payload={
                    'mode': 'live',
                    'trigger': trigger,
                    'executed_at': _utcnow().isoformat(),
                    'idempotent_replay': bool(result.get('idempotent_replay')),
                    'request_status': request.get('submission_status'),
                    'order_status': order.get('current_status'),
                },
                submitted=execution_status == EXECUTION_STATUS_SUBMITTED,
            )
            refreshed = self._load_plan_row(row_id=row_id)
            serialized = self._serialize_row(refreshed)
            serialized['idempotent_replay'] = bool(result.get('idempotent_replay'))
            return serialized
        except TradingOrderPendingSubmissionError as exc:
            self._patch_submission_result(
                row_id=row_id,
                execution_status=EXECUTION_STATUS_SUBMITTED,
                request=(exc.payload or {}).get('request') or {},
                order=(exc.payload or {}).get('order') or {},
                payload={
                    'mode': 'live',
                    'trigger': trigger,
                    'executed_at': _utcnow().isoformat(),
                    'pending_confirmation': True,
                    'error': str(exc),
                },
                submitted=True,
            )
        except (TradingOrderRejectedError, TradingOrderUpstreamError, TradingOrderValidationError) as exc:
            self._patch_submission_result(
                row_id=row_id,
                execution_status=EXECUTION_STATUS_SUBMIT_FAILED,
                request=(exc.payload or {}).get('request') or {},
                order=(exc.payload or {}).get('order') or {},
                payload={
                    'mode': 'live',
                    'trigger': trigger,
                    'executed_at': _utcnow().isoformat(),
                    'error': str(exc),
                    'error_detail': exc.payload,
                },
                submitted=False,
            )
        refreshed = self._load_plan_row(row_id=row_id)
        serialized = self._serialize_row(refreshed)
        serialized['idempotent_replay'] = False
        return serialized

    def _patch_submission_result(
        self,
        *,
        row_id: int,
        execution_status: str,
        request: dict[str, Any],
        order: dict[str, Any],
        payload: dict[str, Any],
        submitted: bool,
    ) -> None:
        now = _utcnow()
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, row_id)
            if row is None:
                raise ValueError(f'plan item {row_id} was not found')
            row.execution_status = execution_status
            row.order_request_id = request.get('id') or row.order_request_id
            row.broker_order_row_id = order.get('id') or row.broker_order_row_id
            row.submission_payload = payload
            if submitted:
                row.submitted_at = row.submitted_at or now
            session.flush()
        signal_decider_execution_outcome_service.sync_entry_plan_item(entry_plan_item_id=row_id)

    def _load_plan_row(self, *, row_id: int) -> SignalDeciderExecutionPlanItem:
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, row_id)
            if row is None:
                raise ValueError(f'plan item {row_id} was not found')
            session.expunge(row)
            return row


signal_decider_next_open_execution_service = SignalDeciderNextOpenExecutionService()
