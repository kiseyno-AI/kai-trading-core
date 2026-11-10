"""Report-only KRX watch and missing-decision history from stored evidence."""

from collections import Counter, defaultdict
from collections.abc import Callable
from datetime import date, datetime, time, timedelta, timezone
from functools import cache
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session, load_only

from app.agent_context.decider_evaluation_service import SUPPORTED_DECIDER_AGENT_NAMES
from app.db_models import (
    AgentContextFeedbackFailure,
    DailyPriceBar,
    SignalDeciderDecisionSnapshot,
    SignalDeciderExecutionPlanItem,
    Symbol,
)
from app.market.clock import KST
from app.market.session import DEFAULT_KRX_CALENDAR_SCAN_DAYS, get_krx_exchange_calendar_day
from app.orders.next_open_execution import (
    _is_krx_next_open_snapshot,
    _short_term_tactical_action,
    _trigger_entry_price_values,
)

LOOKBACK_DAYS = 14
SNAPSHOT_READ_BATCH_SIZE = 100


def _next_study_session(day: date) -> date | None:
    for offset in range(1, DEFAULT_KRX_CALENDAR_SCAN_DAYS + 1):
        candidate = day + timedelta(days=offset)
        if get_krx_exchange_calendar_day(candidate).is_open:
            return candidate
    return None


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _object(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _plan_evidence(plan: SignalDeciderExecutionPlanItem) -> dict:
    dispatch = _object(_object(plan.submission_payload).get('exec_dispatch'))
    activation = _object(_object(dispatch.get('contract_proof')).get('core_activation'))
    readback = _object(dispatch.get('readback'))
    counts = _object(_object(readback.get('derived_state')).get('plan_item_status_counts'))
    sync = _object(dispatch.get('core_state_sync'))
    observed = (
        readback.get('last_synced_at') if counts.get('watch_monitoring_active')
        else sync.get('synced_at') if sync.get('status') == 'exec_watch_monitoring_active'
        else None
    )
    reasons = _object(plan.validation_payload).get('blocking_reasons')
    return {
        'plan_item_id': plan.id,
        'target_trade_date': plan.target_trade_date.isoformat(),
        'eligibility_status': plan.eligibility_status,
        'execution_status': plan.execution_status,
        'blocking_codes': [
            reason['code'] for reason in reasons
            if isinstance(reason, dict) and isinstance(reason.get('code'), str)
        ] if isinstance(reasons, list) else [],
        'exec_activation_ack_at': activation.get('activated_at') if activation.get('status') == 'activated' else None,
        'exec_monitoring_observed_at': observed,
    }


def _simulate(
    *,
    source_date: date,
    known_at: datetime,
    as_of: date,
    levels: dict | None,
    next_session: Callable[[date], date | None],
    price_bar: Callable[[date], Any],
) -> dict:
    target = next_session(source_date)
    local = known_at.astimezone(KST)
    late = target is not None and (
        local.date() > target or (local.date() == target and local.time() >= time(9))
    )
    trade_date = target
    if late:
        trade_date = local.date() if (
            local.time() < time(9) and next_session(local.date() - timedelta(days=1)) == local.date()
        ) else next_session(local.date())
    result = {
        'scenario': 'missing_decision_open_to_close' if levels is None else 'wait_trigger_to_exit',
        'original_target_trade_date': target.isoformat() if target else None,
        'original_session_unobservable': late,
        'trade_date': trade_date.isoformat() if trade_date else None,
        'gross_pnl_krw_per_share': None,
    }
    if levels is not None:
        stop, trigger, chase, target_price = (
            levels[key] for key in ('stop_loss_price', 'trigger_price', 'chase_limit_price', 'target_price')
        )
        if None in (stop, trigger, chase, target_price) or not 0 < stop < trigger <= chase < target_price:
            return {**result, 'status': 'invalid_watch_levels'}
    if trade_date is None:
        return {**result, 'status': 'missing_session_calendar'}
    if trade_date > as_of:
        return {**result, 'status': 'awaiting_session'}
    bar = price_bar(trade_date)
    if bar is None or bar.volume <= 0 or not (
        0 < bar.low_price <= min(bar.open_price, bar.close_price)
        <= max(bar.open_price, bar.close_price) <= bar.high_price
    ):
        return {**result, 'status': 'missing_usable_price_bar'}

    entry, exit_price, exit_reason = bar.open_price, bar.close_price, 'session_close'
    if levels is not None:
        entry = max(bar.open_price, trigger)
        if bar.high_price < trigger or entry > chase:
            return {
                **result, 'status': 'no_entry', 'price_source': bar.source,
                'gross_pnl_krw_per_share': 0, 'exit_reason': (
                    'opened_above_chase_limit' if entry > chase else 'trigger_not_touched'
                ),
            }
        if bar.low_price <= stop:
            exit_price = stop
            exit_reason = 'stop_first_same_bar' if bar.high_price >= target_price else 'stop'
        elif bar.high_price >= target_price:
            exit_price, exit_reason = target_price, 'target'
    return {
        **result, 'status': 'simulated', 'price_source': bar.source,
        'entry_price': float(entry), 'exit_price': float(exit_price), 'exit_reason': exit_reason,
        'gross_pnl_krw_per_share': float(exit_price - entry),
        'gross_return_pct': round(float((exit_price - entry) / entry * 100), 6),
    }


def build_decider_watch_audit(
    db: Session,
    *,
    as_of_market_date: date,
    observed_at: datetime,
    next_session: Callable[[date], date | None] = _next_study_session,
) -> dict:
    """Evaluate one complete session per decision or failure, with no writes to the DB."""
    earliest = as_of_market_date - timedelta(days=LOOKBACK_DAYS - 1)
    snapshot = SignalDeciderDecisionSnapshot
    received_at = func.coalesce(snapshot.feedback_received_at, snapshot.created_at)
    symbol_day = snapshot.request_payload['symbol_day_context']
    latest = {}
    for row in db.execute(
        select(
            snapshot.id, snapshot.market_date, snapshot.symbol, snapshot.agent_name,
            snapshot.next_day_open_action, received_at.label('received_at'),
        ).where(
            snapshot.market_date.between(earliest, as_of_market_date),
            snapshot.agent_name.in_(SUPPORTED_DECIDER_AGENT_NAMES),
            snapshot.created_at <= observed_at,
            received_at <= observed_at,
        ).order_by(received_at.desc(), snapshot.id.desc())
    ):
        latest.setdefault((row.market_date, row.symbol, row.agent_name), row)
    candidate_ids = [
        row.id for row in latest.values()
        if row.next_day_open_action == 'wait' or row.agent_name == 'short-term-decider'
    ]
    watch_query = (
        select(
            snapshot.id, snapshot.market_date, snapshot.symbol, snapshot.agent_name,
            snapshot.decision_run_id, snapshot.created_at, received_at.label('received_at'),
            snapshot.next_day_open_action, snapshot.confidence_score_10,
            snapshot.target_price, snapshot.stop_loss_price, snapshot.response_payload,
            snapshot.traceability_payload,
            symbol_day['market'].as_string().label('symbol_day_market_claim'),
            symbol_day['company']['market'].as_string().label('company_market_claim'),
        )
    )
    # Limit each read of toasted decision JSON to the selected snapshots.
    watches = []
    for offset in range(0, len(candidate_ids), SNAPSHOT_READ_BATCH_SIZE):
        batch_ids = candidate_ids[offset:offset + SNAPSHOT_READ_BATCH_SIZE]
        for row in db.execute(watch_query.where(snapshot.id.in_(batch_ids))):
            if not _is_krx_next_open_snapshot(row):
                continue
            action = (
                _short_term_tactical_action(row)
                if row.agent_name == 'short-term-decider'
                else row.next_day_open_action
            )
            if action in {'wait', 'watch_for_trigger'}:
                watches.append(row)
    plan = SignalDeciderExecutionPlanItem
    plans = defaultdict(list)
    if watches:
        for row in db.scalars(select(plan).options(load_only(
            plan.id, plan.decision_snapshot_id, plan.target_trade_date,
            plan.eligibility_status, plan.execution_status,
            plan.submission_payload, plan.validation_payload,
        )).where(
            plan.execution_window == 'next_open',
            plan.decision_snapshot_id.in_([watch.id for watch in watches]),
        ).order_by(plan.id)):
            plans[row.decision_snapshot_id].append(_plan_evidence(row))

    failure = AgentContextFeedbackFailure
    failures = db.scalars(select(failure).options(load_only(
        failure.id, failure.market_date, failure.symbol, failure.agent_name,
        failure.run_id, failure.failure_kind, failure.summary_payload, failure.received_at,
    )).where(
        failure.market_date.between(earliest, as_of_market_date),
        failure.agent_name.in_(SUPPORTED_DECIDER_AGENT_NAMES),
        failure.received_at <= observed_at,
    )).all()
    symbols = {row.symbol for row in watches} | {row.symbol for row in failures}
    bar = DailyPriceBar
    bars = {
        (row.symbol, row.trade_date): row
        for row in db.execute(select(
            Symbol.symbol, bar.trade_date, bar.open_price, bar.high_price,
            bar.low_price, bar.close_price, bar.volume, bar.source,
        ).join(
            bar, bar.symbol_id == Symbol.id,
        ).where(
            Symbol.symbol.in_(symbols),
            bar.trade_date.between(earliest, as_of_market_date),
        ))
    } if symbols else {}
    items = []
    cached_next = cache(next_session)
    for row, kind in [(row, 'wait') for row in watches] + [(row, 'failed') for row in failures]:
        if kind == 'wait':
            known_at = max(_aware(row.received_at), _aware(row.created_at))
            levels = _trigger_entry_price_values(row)
            details = {
                'decision_snapshot_id': row.id, 'run_id': row.decision_run_id,
                'confidence_score_10': float(row.confidence_score_10) if row.confidence_score_10 is not None else None,
                'price_levels': {key: float(value) if value is not None else None for key, value in levels.items()},
                'plans': plans[row.id],
            }
        else:
            known_at = _aware(row.received_at)
            levels = None
            replacement = latest.get((row.market_date, row.symbol, row.agent_name))
            details = {
                'failure_id': row.id, 'run_id': row.run_id, 'failure_kind': row.failure_kind,
                'failure_mode': _object(row.summary_payload).get('failure_mode'),
                'later_decision_snapshot_id': (
                    replacement.id if replacement and _aware(replacement.received_at) > known_at else None
                ),
                'plans': [],
            }
        items.append({
            'kind': kind, 'source_market_date': row.market_date.isoformat(),
            'symbol': row.symbol, 'agent_name': row.agent_name, 'available_at': known_at.isoformat(),
            **details,
            'simulation': _simulate(
                source_date=row.market_date, known_at=known_at, as_of=as_of_market_date,
                levels=levels, next_session=cached_next,
                price_bar=lambda day: bars.get((row.symbol, day)),
            ),
        })
    items.sort(key=lambda item: (item['source_market_date'], item['symbol'], item['agent_name'], item['kind']))
    return {
        'schema_version': 'krx_decider_watch_audit_v1',
        'market_date_from': earliest.isoformat(),
        'as_of_market_date': as_of_market_date.isoformat(),
        'assumptions': {
            'mode': 'report_only', 'quantity': 1, 'currency': 'KRW', 'costs_included': False,
            'calendar': 'local_krx_exchange_calendar', 'session_open_kst': '09:00',
            'late_input_rule': 'first_full_session_after_evidence',
            'same_bar_rule': 'stop_first_when_order_unknown',
            'monitoring_evidence': 'persisted_exec_observation_only',
        },
        'summary': {
            'status': 'completed',
            'items': len(items),
            'by_kind': dict(Counter(item['kind'] for item in items)),
            'by_simulation_status': dict(Counter(item['simulation']['status'] for item in items)),
        },
        'items': items,
    }
