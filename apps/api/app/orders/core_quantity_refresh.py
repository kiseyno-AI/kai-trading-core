"""Core-owned quantity authority, separate from immutable decider price contracts."""

import asyncio
from collections import defaultdict
import copy
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from time import monotonic
from typing import Any

from sqlalchemy import select

from app.core.settings import settings
from app.db import session_scope
from app.db_models import SignalDeciderExecutionPlanItem
from app.market.clock import KST, async_get_market_state
from app.market.session import KRX_EXCHANGE_CALENDAR
from app.market.symbol_codes import is_canonical_krx_short_code
from app.orders.account_inquiry_service import trading_account_inquiry_service
from app.orders.core_quantity_sizing import _decimal, _whole, calculate_core_quantity_targets
from app.orders.execution_handoff_contract import CoreQuantityPolicy, ExecutionQuantityRefreshPayload
from app.orders.next_open_execution import _execution_plan_row_authority_fingerprint, _trigger_entry_price_values


def _datetime(value: Any) -> datetime | None:
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo is not None else None


def _state(row: Any) -> dict:
    return copy.deepcopy((row.submission_payload or {}).get('core_quantity_sizing') or {})


def _object(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _error_code(exc: Exception) -> str:
    return str(exc) if isinstance(exc, ValueError) and str(exc).startswith('quantity_') else type(exc).__name__


def _elapsed_now(now: datetime, started: float) -> datetime:
    return now + timedelta(seconds=max(0, monotonic() - started))


def _authority(row: Any) -> str:
    return _execution_plan_row_authority_fingerprint(row)


def _session_window(day: date, now: datetime) -> tuple[str, datetime]:
    opened = KRX_EXCHANGE_CALENDAR.session_open(day.isoformat()).to_pydatetime()
    closed = KRX_EXCHANGE_CALENDAR.session_close(day.isoformat()).to_pydatetime()
    if now < opened:
        phase = 'preopen' if now.astimezone(KST).date() == day else 'provisional'
        return phase, min(now + timedelta(seconds=settings.core_quantity_refresh_seconds), closed) if phase == 'preopen' else opened
    if now >= closed:
        raise ValueError('quantity_session_closed')
    return 'open', min(now + timedelta(seconds=settings.core_quantity_refresh_seconds), closed)


class CoreQuantityRefreshService:
    def __init__(self, integration: Any, account_service: Any = None) -> None:
        self.integration = integration
        self.account_service = account_service or trading_account_inquiry_service
        self._lock = asyncio.Lock()

    @staticmethod
    def _candidate(row: Any, snapshot: Any, execution: dict | None = None) -> dict:
        values = _trigger_entry_price_values(snapshot)
        reference = _decimal(row.estimated_reference_price)
        entry_ceiling = values['chase_limit_price'] or values['trigger_price'] or reference
        frozen = {
            'decision_snapshot_id': row.decision_snapshot_id,
            'decision_agent_name': row.decision_agent_name, 'symbol': row.symbol,
            **{key: str(value) if value is not None else None for key, value in values.items()},
            'entry_ceiling_price': str(entry_ceiling) if entry_ceiling is not None else None,
        }
        digest = hashlib.sha256(json.dumps(frozen, sort_keys=True).encode()).hexdigest()
        # An immediate next-open BUY has no trigger contract; its stored reference is only a sizing basis.
        values['trigger_price'] = values['trigger_price'] or reference
        values['chase_limit_price'] = values['chase_limit_price'] or values['trigger_price']
        validation = _object(row.validation_payload)
        quality = _object(validation.get('decision_input_quality_context'))
        feasibility = _object(quality.get('execution_feasibility'))
        request = _object(getattr(snapshot, 'request_payload', None))
        technical = _object(_object(request.get('symbol_day_context')).get('technical_snapshot'))
        execution = execution or {}
        return {
            'core_plan_item_id': row.id, 'symbol': row.symbol, 'decision_agent_name': row.decision_agent_name,
            **values, 'price_contract_digest': digest,
            'confidence_score_10': getattr(snapshot, 'confidence_score_10', None),
            'expected_return_pct': getattr(snapshot, 'expected_return_pct', None),
            'atr_pct_14': technical.get('atr_pct_14'),
            'max_safe_notional_krw': feasibility.get('max_safe_notional_krw'),
            'filled_quantity': execution.get('filled_quantity', 0),
            'pending_buy_quantity': execution.get('open_quantity', 0),
            'entry_closed': execution.get('entry_closed') is True,
        }

    @staticmethod
    def policy_matches_source(row: Any, snapshot: Any) -> bool:
        state = _state(row)
        initial = _object(state.get('initial_policy'))
        if initial.get('type') != 'core_sized' or snapshot is None:
            return False
        return bool(
            state.get('source_binding') == _authority(row)
            and _object(initial.get('sizing_inputs')).get('price_contract_digest')
            == CoreQuantityRefreshService._candidate(row, snapshot)['price_contract_digest']
        )

    async def _remote(self, rows: list) -> tuple[dict[int, dict], bool]:
        states, plans = {}, {}
        uncertain = False
        for row in rows:
            dispatch = (row.submission_payload or {}).get('exec_dispatch') or {}
            external_id = dispatch.get('plan_id')
            remote_id = self.integration._accepted_execution_plan_id_for_rows([row])
            if not remote_id:
                continue
            if external_id not in plans:
                response = await self.integration.client.list_execution_plans(external_plan_id=external_id)
                found = [plan for plan in response.get('items', []) if (
                    plan.get('external_plan_id') == external_id and plan.get('execution_plan_id') == remote_id
                )]
                if len(found) != 1:
                    raise ValueError('quantity_plan_readback_unproven')
                plan = plans[external_id] = found[0]
                uncertain |= plan.get('account_submission_uncertain') is True
                uncertain |= any((item.get('core_quantity_state') or {}).get('submission_uncertain') is True
                                 for item in plan.get('item_states', []))
            links = [link for link in dispatch.get('item_links', []) if link.get('side') == 'buy']
            if len(links) != 1:
                raise ValueError('quantity_entry_binding_unproven')
            matches = [item for item in plans[external_id].get('item_states', [])
                       if item.get('core_item_key') == links[0].get('core_item_key')]
            if _state(row).get('initial_policy', {}).get('type') != 'core_sized':
                states[row.id] = {'legacy_quantity_reservations_unproven': True}
                continue
            if len(matches) != 1 or not isinstance(matches[0].get('core_quantity_state'), dict):
                raise ValueError('quantity_execution_state_unproven')
            state = matches[0]['core_quantity_state']
            if any(_whole(state.get(key)) is None for key in ('target_quantity', 'revision', 'filled_quantity', 'open_quantity')):
                raise ValueError('quantity_execution_state_invalid')
            states[row.id] = {**state, 'core_item_key': links[0]['core_item_key'],
                              'external_plan_id': external_id, 'execution_plan_id': remote_id}
        return states, uncertain

    async def _calculate(self, rows: list, snapshots: dict, now: datetime, context_rows: list | None = None) -> tuple[list, dict[int, dict], bool]:
        started = monotonic()
        context_rows = context_rows or rows
        executions, per_plan_uncertain = await self._remote(context_rows)
        uncertain = True
        candidates = [self._candidate(row, snapshots[row.decision_snapshot_id], executions.get(row.id))
                      for row in rows if row.decision_snapshot_id in snapshots]
        equity, cash, exposure, account_error = None, None, {}, None
        operational_limits = {}
        runtime_at, local_reserved = None, None
        outside_virtual = defaultdict(Decimal)
        outside_target_notional = Decimal(0)
        try:
            runtime = await self.integration.client.get_runtime_status()
            quantity_account = _object(runtime.get('core_quantity_account'))
            runtime_at = _datetime(quantity_account.get('as_of'))
            local_reserved = _decimal(quantity_account.get('account_local_unacknowledged_buy_notional_krw'))
            if (
                runtime_at is None or not isinstance(quantity_account.get('account_submission_uncertain'), bool)
                or local_reserved is None or local_reserved < 0
            ):
                raise ValueError('quantity_exec_account_state_unproven')
            limits = _object(quantity_account.get('limits'))
            parsed_limits = {key: _decimal(limits.get(key)) for key in (
                'max_symbol_budget_krw', 'max_total_budget_krw', 'risk_fraction', 'max_symbol_fraction',
            )}
            if (
                any(value is None or value <= 0 for value in parsed_limits.values())
                or parsed_limits['risk_fraction'] > 1 or parsed_limits['max_symbol_fraction'] > 1
            ):
                raise ValueError('quantity_exec_limits_unproven')
            operational_limits = parsed_limits
            uncertain = per_plan_uncertain or quantity_account['account_submission_uncertain']
            requested_ids = {row.id for row in rows}
            for context in context_rows:
                if context.id in requested_ids:
                    continue
                execution = executions.get(context.id) or {}
                initial = _state(context).get('initial_policy') or {}
                accepted = self.integration._accepted_execution_plan_id_for_rows([context])
                dispatch_status = (context.submission_payload or {}).get('exec_dispatch', {}).get('dispatch_status')
                if initial.get('type') != 'core_sized':
                    if accepted or dispatch_status == 'pending_remote_ack':
                        raise ValueError('quantity_legacy_quantity_reservations_unproven')
                    continue
                target = _whole(execution.get('target_quantity', initial.get('target_quantity')))
                filled = _whole(execution.get('filled_quantity', 0))
                opened = _whole(execution.get('open_quantity', 0))
                if target is None or filled is None or opened is None:
                    raise ValueError('quantity_existing_target_unproven')
                committed = max(target, filled + opened)
                if not committed:
                    continue
                ceiling = _decimal(_object(initial.get('sizing_inputs')).get('sizing_price'))
                if ceiling is None or ceiling <= 0:
                    raise ValueError('quantity_existing_target_price_unproven')
                outside_target_notional += committed * ceiling
                outside_virtual[context.symbol] += max(target - filled - opened, 0) * ceiling
            usable = next(item for item in candidates if _decimal(item['chase_limit_price']) is not None
                          and _decimal(item['chase_limit_price']) > 0)
            account = rows[0].account_reference
            summary, positions, buying_power = await asyncio.gather(
                self.account_service.get_account_summary(account_reference=account),
                self.account_service.get_positions(account_reference=account),
                self.account_service.get_buying_power(account_reference=account, symbol=usable['symbol'],
                                                     price=usable['chase_limit_price'], order_type='limit'),
            )
            checked_at = _elapsed_now(now, started)
            if not timedelta(0) <= checked_at - runtime_at <= timedelta(seconds=300):
                raise ValueError('quantity_exec_account_state_stale')
            for payload in (summary, positions, buying_power):
                at = _datetime(payload.get('as_of'))
                if payload.get('account_reference') != account or at is None or not timedelta(0) <= checked_at - at <= timedelta(seconds=300):
                    raise ValueError('quantity_account_snapshot_stale_or_unbound')
            if positions.get('completeness', {}).get('status') != 'complete':
                raise ValueError('quantity_positions_incomplete')
            for position in positions.get('items', []):
                amount = _decimal(position.get('market_value'))
                symbol = position.get('symbol')
                if (
                    not is_canonical_krx_short_code(symbol) or not isinstance(symbol, str) or symbol != symbol.strip().upper()
                    or amount is None or amount < 0
                ):
                    raise ValueError('quantity_position_exposure_unproven')
                exposure[position['symbol']] = exposure.get(position['symbol'], Decimal(0)) + amount
            equity = summary.get('valuation', {}).get('total_asset_value')
            orderable = _decimal(buying_power.get('buying_power', {}).get('orderable_cash'))
            cash = max(orderable - local_reserved - sum(outside_virtual.values(), Decimal(0)), Decimal(0)) if orderable is not None else None
        except Exception as exc:
            account_error = _error_code(exc)
        reserved_by_symbol = defaultdict(Decimal)
        for context in context_rows:
            execution = executions.get(context.id)
            snap = snapshots.get(context.decision_snapshot_id)
            if execution is None or snap is None or execution.get('legacy_quantity_reservations_unproven'):
                continue
            item = self._candidate(context, snap, execution)
            price = _decimal(item['chase_limit_price'])
            if price is not None:
                reserved_by_symbol[item['symbol']] += price * item['pending_buy_quantity']
        for symbol, amount in outside_virtual.items():
            reserved_by_symbol[symbol] += amount
        remaining_total_cap = operational_limits.get('max_total_budget_krw')
        if remaining_total_cap is not None:
            remaining_total_cap = max(remaining_total_cap - outside_target_notional, Decimal(0))
        sized = calculate_core_quantity_targets(
            candidates, account_equity_krw=equity, available_cash_krw=cash,
            symbol_exposure_krw=exposure, reserved_buy_notional_by_symbol=reserved_by_symbol,
            risk_fraction=min(Decimal(str(settings.core_quantity_risk_fraction)),
                              operational_limits.get('risk_fraction', Decimal(str(settings.core_quantity_risk_fraction)))),
            max_symbol_fraction=min(Decimal(str(settings.core_quantity_max_symbol_fraction)),
                                    operational_limits.get('max_symbol_fraction', Decimal(str(settings.core_quantity_max_symbol_fraction)))),
            round_trip_cost_bps=Decimal(str(settings.signal_decider_next_open_non_spread_round_trip_cost_bps)),
            max_symbol_notional_krw=operational_limits.get('max_symbol_budget_krw'),
            max_total_buy_notional_krw=remaining_total_cap,
        )
        by_id = {item['core_plan_item_id']: item for item in candidates}
        for item in sized:
            source = by_id[item['core_plan_item_id']]
            item['audit']['price_contract_digest'] = source['price_contract_digest']
            item['audit']['operational_limits'] = {key: str(value) for key, value in operational_limits.items()}
            item['audit']['account_submission_uncertain'] = uncertain
            item['audit']['runtime_account_as_of'] = runtime_at.isoformat() if runtime_at is not None else None
            item['audit']['account_local_unacknowledged_buy_notional_krw'] = str(local_reserved) if local_reserved is not None else None
            item['audit']['outside_cohort_target_notional_krw'] = str(outside_target_notional)
            item['audit']['outside_cohort_unsubmitted_notional_krw'] = str(sum(outside_virtual.values(), Decimal(0)))
            ceiling = _decimal(source['chase_limit_price'])
            if ceiling is not None and ceiling > 0:
                item['audit']['sizing_price'] = str(ceiling)
            if account_error:
                item['audit']['account_error'] = account_error
        return sized, executions, uncertain

    def _persist(self, row: Any, previous: dict, state: dict) -> None:
        with session_scope() as session:
            current = session.execute(select(SignalDeciderExecutionPlanItem).where(
                SignalDeciderExecutionPlanItem.id == row.id,
            ).with_for_update()).scalar_one()
            if _authority(current) != _authority(row) or _state(current) != previous:
                raise ValueError('quantity_source_authority_changed')
            if not self.integration._row_is_dispatchable(current):
                raise ValueError('quantity_source_not_dispatchable')
            current.submission_payload = {**(current.submission_payload or {}), 'core_quantity_sizing': state}
        row.submission_payload = {**(row.submission_payload or {}), 'core_quantity_sizing': copy.deepcopy(state)}

    async def prepare_initial(self, rows: list, snapshots: dict, now: datetime | None = None, dry_run: bool = False) -> dict:
        started = monotonic()
        now = now or datetime.now(timezone.utc)
        summary = {'prepared_count': 0, 'failed_count': 0, 'items': []}
        if not settings.core_quantity_sizing_enabled:
            return {**summary, 'status': 'disabled'}
        async with self._lock:
            groups = defaultdict(list)
            for row in rows:
                previous = _state(row)
                if previous.get('initial_policy', {}).get('type') == 'core_sized':
                    if self.policy_matches_source(row, snapshots.get(row.decision_snapshot_id)):
                        continue
                    dispatch = _object((row.submission_payload or {}).get('exec_dispatch'))
                    if dispatch or previous.get('pending_request') or self.integration._accepted_execution_plan_id_for_rows([row]):
                        summary['failed_count'] += 1
                        summary['items'].append({'core_plan_item_id': row.id, 'status': 'blocked',
                                                 'reason_code': 'quantity_source_binding_changed_after_dispatch'})
                        continue
                if (
                    row.decision_snapshot_id in snapshots and is_canonical_krx_short_code(row.symbol)
                    and row.broker_name == 'kis' and self.integration._row_is_dispatchable(row)
                    and not self.integration._accepted_execution_plan_id_for_rows([row])
                    and (row.submission_payload or {}).get('exec_dispatch', {}).get('dispatch_status') != 'pending_remote_ack'
                ):
                    groups[row.account_reference].append(row)
            for account_rows in groups.values():
                try:
                    context_rows = [row for row in rows if row.account_reference == account_rows[0].account_reference]
                    sized, _, uncertain = await self._calculate(account_rows, snapshots, _elapsed_now(now, started), context_rows)
                except Exception as exc:
                    summary['failed_count'] += len(account_rows)
                    summary['items'].append({'status': 'blocked', 'reason_code': _error_code(exc)})
                    continue
                by_id = {row.id: row for row in account_rows}
                for item in sized:
                    row = by_id[item['core_plan_item_id']]
                    try:
                        phase, valid_until = _session_window(row.target_trade_date, now)
                        previous = _state(row)
                        state = copy.deepcopy(previous)
                        if previous.get('initial_policy'):
                            retired = list(previous.get('retired_initial_policies') or [])[-2:]
                            retired.append({
                                'source_binding': previous.get('source_binding'),
                                'revision': previous['initial_policy'].get('revision'),
                                'target_quantity': previous['initial_policy'].get('target_quantity'),
                                'price_contract_digest': _object(previous['initial_policy'].get('sizing_inputs')).get('price_contract_digest'),
                                'retired_at': now.isoformat(), 'reason': 'source_changed_before_dispatch',
                            })
                            state['retired_initial_policies'] = retired
                        policy = CoreQuantityPolicy.model_validate({
                            'type': 'core_sized', 'target_quantity': 0 if uncertain else item['target_quantity'], 'revision': 1,
                            'calculated_at': now.isoformat(), 'valid_until': valid_until.isoformat(),
                            'sizing_inputs': item['audit'],
                        }).model_dump(mode='json')
                        state.update(initial_policy=policy, policy=policy, pending_request=None,
                                     source_binding=_authority(row), last_ack_at=None,
                                     status=item['status'], last_error=None, calculated_phase=phase)
                        if dry_run:
                            row.submission_payload = {**(row.submission_payload or {}), 'core_quantity_sizing': state}
                        else:
                            await asyncio.to_thread(self._persist, row, previous, state)
                        summary['prepared_count'] += 1
                        summary['items'].append({'core_plan_item_id': row.id, 'status': item['status']})
                    except Exception as exc:
                        summary['failed_count'] += 1
                        summary['items'].append({'core_plan_item_id': row.id, 'status': 'blocked', 'reason_code': _error_code(exc)})
        return summary

    async def _deliver_pending(self, row: Any, now: datetime) -> dict:
        previous = _state(row)
        request = previous['pending_request']
        try:
            payload = ExecutionQuantityRefreshPayload.model_validate({'items': [request]}).model_dump(mode='json')
            response = await self.integration.client.post_execution_plan_quantities(payload)
            matches = [item for item in response.get('items', []) if (
                item.get('core_item_key') == request['core_item_key']
                and item.get('execution_plan_id') == request['execution_plan_id']
            )]
            if len(matches) != 1:
                raise ValueError('quantity_ack_binding_unproven')
            ack = matches[0]
            if ack.get('status') not in {'updated', 'unchanged', 'blocked', 'conflict'}:
                raise ValueError('quantity_ack_status_unproven')
            state = copy.deepcopy(previous)
            if ack['status'] in {'updated', 'unchanged'}:
                if (
                    type(ack.get('revision')) is not int or type(ack.get('target_quantity')) is not int
                    or ack['revision'] != request['revision'] or ack['target_quantity'] != request['target_quantity']
                ):
                    raise ValueError('quantity_ack_material_mismatch')
                state['policy'] = CoreQuantityPolicy.model_validate({
                    'type': 'core_sized', **{key: request[key] for key in (
                        'target_quantity', 'revision', 'calculated_at', 'valid_until', 'sizing_inputs',
                    )},
                }).model_dump(mode='json')
                state.update(last_ack_at=now.isoformat(), status='acknowledged', last_error=None, pending_request=None)
            else:
                state.update(status='blocked', last_error=ack.get('reason_code') or ack['status'], pending_request=None)
            await asyncio.to_thread(self._persist, row, previous, state)
            return {'core_plan_item_id': row.id, 'status': state['status'], 'reason_code': state['last_error']}
        except Exception as exc:
            state = {**previous, 'status': 'pending_ack', 'last_error': _error_code(exc)}
            await asyncio.to_thread(self._persist, row, previous, state)
            return {'core_plan_item_id': row.id, 'status': 'pending_ack', 'reason_code': _error_code(exc)}

    async def _queue_revision(self, row: Any, item: dict, execution: dict, *, uncertain: bool,
                              phase: str, now: datetime, valid_until: datetime) -> dict:
        previous = _state(row)
        old_policy = previous['policy']
        if item['audit']['price_contract_digest'] != previous['initial_policy']['sizing_inputs']['price_contract_digest']:
            raise ValueError('quantity_price_contract_changed')
        if execution['revision'] != old_policy['revision']:
            raise ValueError('quantity_revision_drift')
        target = item['target_quantity']
        if uncertain:
            target = min(target, max(execution['filled_quantity'], execution['target_quantity']))
            item['audit']['increases_frozen_submission_uncertain'] = True
        request = {
            **{key: execution[key] for key in ('external_plan_id', 'execution_plan_id', 'core_item_key')},
            'decision_snapshot_id': str(row.decision_snapshot_id),
            'expected_revision': old_policy['revision'], 'revision': old_policy['revision'] + 1,
            'target_quantity': target, 'calculated_at': now.isoformat(),
            'valid_until': valid_until.isoformat(), 'sizing_inputs': item['audit'],
        }
        request = ExecutionQuantityRefreshPayload.model_validate({'items': [request]}).model_dump(mode='json')['items'][0]
        state = {**previous, 'pending_request': request, 'status': 'pending_ack',
                 'calculated_phase': phase, 'last_error': None}
        await asyncio.to_thread(self._persist, row, previous, state)
        return await self._deliver_pending(row, now)

    async def refresh_if_due(self, target_trade_date: str | date, now: datetime | None = None) -> dict:
        started = monotonic()
        now = now or datetime.now(timezone.utc)
        day = date.fromisoformat(target_trade_date) if isinstance(target_trade_date, str) else target_trade_date
        result = {'updated_count': 0, 'failed_count': 0, 'skipped_count': 0, 'items': []}
        if not settings.core_quantity_sizing_enabled:
            return {**result, 'status': 'disabled'}
        market = await async_get_market_state(now)
        local = now.astimezone(KST)
        if (
            local.date() != day or not market.is_trading_day
            or market.state not in {'PREOPEN', 'OPEN'}
            or market.calendar_source != 'kis_chk_holiday' or market.calendar_source_status != 'ok'
            or market.state == 'PREOPEN' and local.time() < time(8)
        ):
            return {**result, 'status': 'outside_refresh_session'}
        async with self._lock:
            rows = await asyncio.to_thread(self.integration._load_next_open_rows, target_trade_date=day)
            snapshots = await asyncio.to_thread(self.integration._load_snapshots, rows)
            groups = defaultdict(list)
            for row in rows:
                state = _state(row)
                if state.get('initial_policy', {}).get('type') == 'core_sized' and not self.policy_matches_source(row, snapshots.get(row.decision_snapshot_id)):
                    result['failed_count'] += 1
                    result['items'].append({'core_plan_item_id': row.id, 'status': 'blocked',
                                            'reason_code': 'quantity_source_binding_changed_after_dispatch'})
                    continue
                if (
                    state.get('initial_policy', {}).get('type') == 'core_sized'
                    and self.integration._accepted_execution_plan_id_for_rows([row])
                    and self.integration._row_is_dispatchable(row)
                ):
                    groups[row.account_reference].append(row)
            for account_rows in groups.values():
                due = []
                settled_rows = []
                replayed_pending = False
                phase, valid_until = _session_window(day, now)
                for row in account_rows:
                    state = _state(row)
                    if state.get('pending_request'):
                        replayed_pending = True
                        try:
                            outcome = await self._deliver_pending(row, now)
                        except Exception as exc:
                            outcome = {'core_plan_item_id': row.id, 'status': 'blocked', 'reason_code': _error_code(exc)}
                        result['items'].append(outcome)
                        result['updated_count' if outcome['status'] == 'acknowledged' else 'failed_count'] += 1
                        continue
                    settled_rows.append(row)
                    policy = state.get('policy') or {}
                    last_ack = _datetime(state.get('last_ack_at')) or _datetime(policy.get('calculated_at'))
                    expires = _datetime(policy.get('valid_until'))
                    if (
                        state.get('calculated_phase') != phase or last_ack is None or expires is None
                        or expires <= now or now - last_ack >= timedelta(seconds=settings.core_quantity_refresh_seconds)
                    ):
                        due.append(row)
                # Finish exact ACK replay before publishing another account-wide allocation.
                if replayed_pending or not due:
                    result['skipped_count'] += len(settled_rows)
                    continue
                due = settled_rows
                try:
                    context_rows = [row for row in rows if row.account_reference == account_rows[0].account_reference]
                    sized, executions, uncertain = await self._calculate(account_rows, snapshots, _elapsed_now(now, started), context_rows)
                    targets = {item['core_plan_item_id']: item for item in sized}
                except Exception as exc:
                    result['failed_count'] += len(due)
                    result['items'].append({'status': 'blocked', 'reason_code': _error_code(exc)})
                    continue
                deltas = {row.id: (
                    min(targets[row.id]['target_quantity'], max(executions[row.id]['filled_quantity'], executions[row.id]['target_quantity']))
                    if uncertain else targets[row.id]['target_quantity']
                ) - executions[row.id]['target_quantity'] for row in due}
                reductions_pending = False
                for row in sorted(due, key=lambda candidate: (deltas[candidate.id] >= 0, deltas[candidate.id] > 0, candidate.id)):
                    if deltas[row.id] > 0 and reductions_pending:
                        result['skipped_count'] += 1
                        result['items'].append({'core_plan_item_id': row.id, 'status': 'deferred',
                                                'reason_code': 'quantity_reduction_pending'})
                        continue
                    try:
                        outcome = await self._queue_revision(row, targets[row.id], executions[row.id], uncertain=uncertain,
                                                             phase=phase, now=now, valid_until=valid_until)
                    except Exception as exc:
                        outcome = {'core_plan_item_id': row.id, 'status': 'blocked', 'reason_code': _error_code(exc)}
                    result['items'].append(outcome)
                    result['updated_count' if outcome['status'] == 'acknowledged' else 'failed_count'] += 1
                    if deltas[row.id] < 0 and outcome['status'] != 'acknowledged':
                        reductions_pending = True
        return {**result, 'status': 'partial' if result['failed_count'] else 'completed'}
