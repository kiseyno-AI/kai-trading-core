from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import logging
import threading
from typing import Any, Callable, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

from app.agent_context.roles import (
    SHORT_TERM_DECIDER_AGENT_NAME,
    SIGNAL_DECIDER_AGENT_NAME,
)
from app.core.settings import settings
from app.db import session_scope
from app.db_models import (
    SignalDeciderDecisionSnapshot,
    SignalDeciderWaitRecheckEvent,
)
from app.market.session import (
    CALENDAR_SOURCE_KIS_CHK_HOLIDAY,
    CALENDAR_SOURCE_STATUS_OK,
    is_krx_trading_day,
    previous_krx_trading_date,
)
from app.quotes.kis_realtime_prices import (
    KISRealtimeMonitoredSymbolSource,
    KISRealtimePriceTick,
)


logger = logging.getLogger(__name__)
SEOUL_TZ = ZoneInfo('Asia/Seoul')
WAIT_RECHECK_AGENT_NAMES = frozenset(
    {SIGNAL_DECIDER_AGENT_NAME, SHORT_TERM_DECIDER_AGENT_NAME}
)
WAIT_RECHECK_PROMOTABLE_ACTION_BY_AGENT = {
    SIGNAL_DECIDER_AGENT_NAME: 'buy',
    SHORT_TERM_DECIDER_AGENT_NAME: 'watch_for_trigger',
}
WAIT_RECHECK_LEGACY_CONTEXT_SCHEMA_VERSION = 'intraday_wait_recheck_context_v2'
WAIT_RECHECK_CONTEXT_SCHEMA_VERSION = 'intraday_wait_recheck_context_v3'
WAIT_RECHECK_EVIDENCE_SCHEMA_VERSION = 'wait_recheck_rolling_intraday_v1'
WAIT_RECHECK_QUOTE_SOURCE = 'kis_realtime_ws'
WAIT_RECHECK_PRICE_BASIS = 'best_ask'
SHORT_TERM_MIN_GROSS_REWARD_RISK_RATIO = Decimal('1.80')
RATIO_QUANTUM = Decimal('0.000001')
WAIT_RECHECK_MATERIAL_CHANGE_REASONS = frozenset(
    {
        'initial_supported_trigger',
        'reward_risk_threshold_crossed',
        'observed_entry_price_improved',
        'cumulative_volume_growth',
        'bid_depth_imbalance_improved',
    }
)
KRX_MARKETS = frozenset({'KRX', 'KOSPI', 'KOSDAQ', 'KONEX'})
KRX_REGULAR_SESSION_OPEN = time(9, 0)
KRX_REGULAR_SESSION_CLOSE = time(15, 30)
WAIT_RECHECK_ALLOWED_EVENT_STATUSES = frozenset(
    {'queued', 'running', 'completed', 'failed'}
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _target_regular_session_is_open_at(
    target_trade_date: date,
    value: datetime,
) -> bool:
    normalized = _aware_utc(value)
    if normalized is None:
        return False
    seoul_value = normalized.astimezone(SEOUL_TZ)
    local_time = seoul_value.time().replace(tzinfo=None)
    return bool(
        seoul_value.date() == target_trade_date
        and KRX_REGULAR_SESSION_OPEN
        <= local_time
        <= KRX_REGULAR_SESSION_CLOSE
    )


def _daily_claim_lock_key(target_trade_date: date) -> int:
    digest = hashlib.sha256(
        f'wait-recheck-claim:{target_trade_date.isoformat()}'.encode('utf-8')
    ).digest()
    return int.from_bytes(digest[:8], byteorder='big') & ((1 << 63) - 1)


def _event_retry_not_before(
    event: SignalDeciderWaitRecheckEvent,
) -> datetime | None:
    anchor = _aware_utc(
        event.last_attempt_at or event.updated_at or event.observed_at
    )
    if anchor is None or int(event.attempt_count) >= int(
        settings.signal_decider_wait_recheck_max_attempts
    ):
        return None
    if event.status == 'failed':
        return anchor + timedelta(
            seconds=(
                settings.signal_decider_wait_recheck_retry_cooldown_seconds
            )
        )
    if event.status == 'running':
        return anchor + timedelta(
            seconds=(
                settings.signal_decider_wait_recheck_running_timeout_seconds
            )
        )
    if event.status == 'expired':
        error_payload = _json_object(event.last_error_payload)
        if error_payload.get('code') in {
            'wait_recheck_observation_stale',
            'wait_recheck_market_session_revoked',
        }:
            # Both conditions discard the old observation.  Re-admission is
            # immediate only at the subscription layer; _claim_observation
            # still requires and durably binds a wholly fresh supported
            # websocket evidence window before the row can be queued again.
            return anchor
    return None


def _attempt_history_entry(
    event: SignalDeciderWaitRecheckEvent,
    *,
    archived_at: datetime,
) -> dict[str, Any]:
    def iso(value: Any) -> str | None:
        parsed = _aware_utc(value)
        return parsed.isoformat() if parsed is not None else None

    return {
        'attempt_count': int(event.attempt_count),
        'request_run_id': event.request_run_id,
        'status': event.status,
        'context_payload': _json_object(event.context_payload),
        'context_sha256': event.context_sha256,
        'observed_price': int(event.observed_price),
        'observed_current_price': (
            int(event.observed_current_price)
            if event.observed_current_price is not None
            else None
        ),
        'observed_bid_price': (
            int(event.observed_bid_price)
            if event.observed_bid_price is not None
            else None
        ),
        'price_as_of': iso(event.price_as_of),
        'observed_at': iso(event.observed_at),
        'last_attempt_at': iso(event.last_attempt_at),
        'last_error_payload': (
            _json_object(event.last_error_payload)
            if event.last_error_payload is not None
            else None
        ),
        'archived_at': archived_at.astimezone(timezone.utc).isoformat(),
    }


def _unaccepted_decision_run_ids(
    events: Iterable[SignalDeciderWaitRecheckEvent],
) -> set[str]:
    run_ids: set[str] = set()
    for event in events:
        request_run_id = str(event.request_run_id or '').strip()
        if event.status != 'completed' and request_run_id:
            run_ids.add(request_run_id)
        attempt_history = event.attempt_history_payload
        if not isinstance(attempt_history, list):
            continue
        for attempt in attempt_history:
            if not isinstance(attempt, dict):
                continue
            archived_run_id = str(
                attempt.get('request_run_id') or ''
            ).strip()
            if attempt.get('status') != 'completed' and archived_run_id:
                run_ids.add(archived_run_id)
    return run_ids


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _positive_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite() or parsed <= 0 or parsed != parsed.to_integral_value():
        return None
    return int(parsed)


def _nonnegative_decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value).replace(',', '').strip())
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite() or parsed < 0:
        return None
    return parsed


def _finite_decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value).replace(',', '').strip())
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None or not value.is_finite():
        return None
    try:
        normalized = value.quantize(RATIO_QUANTUM)
    except InvalidOperation:
        return None
    return format(normalized, 'f')


def _gross_reward_risk_ratio(
    *,
    entry_price: int,
    target_price: int,
    stop_loss_price: int,
) -> Decimal | None:
    reward = Decimal(target_price) - Decimal(entry_price)
    risk = Decimal(entry_price) - Decimal(stop_loss_price)
    if reward <= 0 or risk <= 0:
        return None
    return reward / risk


def _minimum_gross_reward_risk_ratio(agent_name: str) -> Decimal:
    if agent_name == SHORT_TERM_DECIDER_AGENT_NAME:
        return SHORT_TERM_MIN_GROSS_REWARD_RISK_RATIO
    return Decimal(str(settings.signal_decider_next_open_min_reward_risk_ratio))


def _raw_decimal(raw_fields: Mapping[str, Any], key: str) -> Decimal | None:
    return _nonnegative_decimal(raw_fields.get(key))


def _imbalance(*, bid: Decimal | None, ask: Decimal | None) -> Decimal | None:
    if bid is None or ask is None or bid + ask <= 0:
        return None
    return (bid - ask) / (bid + ask)


def _aware_utc(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace('Z', '+00:00'))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _canonical_json_sha256(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(',', ':'),
    ).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def _market_claims(snapshot: Any) -> tuple[str, ...]:
    request_payload = _json_object(getattr(snapshot, 'request_payload', None))
    context = request_payload.get('symbol_day_context')
    context = context if isinstance(context, dict) else {}
    company = context.get('company')
    company = company if isinstance(company, dict) else {}
    claims = [
        str(value).strip().upper()
        for value in (context.get('market'), company.get('market'))
        if str(value or '').strip()
    ]
    return tuple(claims)


@dataclass(frozen=True)
class WaitRecheckCandidate:
    source_snapshot_id: int
    source_snapshot_fingerprint: str
    source_market_date: date
    target_trade_date: date
    symbol: str
    agent_name: str
    trigger_price: int
    chase_limit_price: int | None
    target_price: int
    stop_loss_price: int
    source_version: datetime
    generation: int = 1
    rearmed_from_event_id: int | None = None
    rearm_not_before: datetime | None = None
    prior_observed_price: int | None = None
    prior_observed_at: datetime | None = None
    prior_gross_reward_risk_ratio: str | None = None
    prior_intraday_evidence: dict[str, Any] | None = field(
        default=None,
        compare=False,
    )
    retry_event_id: int | None = None


@dataclass(frozen=True)
class WaitRecheckObservation:
    candidate: WaitRecheckCandidate
    observed_price: int
    current_price: int | None
    bid_price: int | None
    price_basis: str
    price_as_of: datetime
    received_at: datetime
    quote_source: str
    intraday_evidence: dict[str, Any] = field(default_factory=dict)
    material_change_reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class WaitRecheckTickSample:
    symbol: str
    current_price: int | None
    bid_price: int | None
    ask_price: int
    cumulative_volume: Decimal | None
    cumulative_turnover: Decimal | None
    trade_volume: Decimal | None
    top_ask_quantity: Decimal | None
    top_bid_quantity: Decimal | None
    total_ask_quantity: Decimal | None
    total_bid_quantity: Decimal | None
    execution_strength: Decimal | None
    buy_rate: Decimal | None
    price_as_of: datetime
    received_at: datetime


@dataclass(frozen=True)
class WaitRecheckSubscriptionSource:
    symbols: tuple[str, ...]
    source_version: datetime | None
    membership_complete: bool
    candidates: tuple[WaitRecheckCandidate, ...] = ()


def _tick_sample(tick: KISRealtimePriceTick) -> WaitRecheckTickSample | None:
    ask_price = _positive_int(tick.ask_price)
    received_at = _aware_utc(tick.received_at)
    price_as_of = _aware_utc(tick.price_as_of)
    if ask_price is None or received_at is None or price_as_of is None:
        return None
    raw_fields = tick.raw_fields if isinstance(tick.raw_fields, dict) else {}
    return WaitRecheckTickSample(
        symbol=str(tick.symbol or '').strip().upper(),
        current_price=_positive_int(tick.current_price),
        bid_price=_positive_int(tick.bid_price),
        ask_price=ask_price,
        cumulative_volume=_nonnegative_decimal(tick.volume),
        cumulative_turnover=_nonnegative_decimal(tick.turnover),
        trade_volume=_raw_decimal(raw_fields, 'CNTG_VOL'),
        top_ask_quantity=_raw_decimal(raw_fields, 'ASKP_RSQN1'),
        top_bid_quantity=_raw_decimal(raw_fields, 'BIDP_RSQN1'),
        total_ask_quantity=_raw_decimal(raw_fields, 'TOTAL_ASKP_RSQN'),
        total_bid_quantity=_raw_decimal(raw_fields, 'TOTAL_BIDP_RSQN'),
        execution_strength=_raw_decimal(raw_fields, 'CTTR'),
        buy_rate=_raw_decimal(raw_fields, 'SHNU_RATE'),
        price_as_of=price_as_of,
        received_at=received_at,
    )


def _intraday_evidence(
    *,
    samples: Sequence[WaitRecheckTickSample],
    candidate: WaitRecheckCandidate,
) -> dict[str, Any]:
    ordered = sorted(samples, key=lambda sample: sample.received_at)
    sample_count = len(ordered)
    first = ordered[0]
    latest = ordered[-1]
    span_seconds = Decimal(
        str(max((latest.received_at - first.received_at).total_seconds(), 0))
    )
    qualifying = [
        sample
        for sample in ordered
        if candidate.trigger_price <= sample.ask_price < candidate.target_price
        and (
            candidate.chase_limit_price is None
            or sample.ask_price <= candidate.chase_limit_price
        )
    ]
    support_ratio = Decimal(len(qualifying)) / Decimal(sample_count)
    minimum_support_ratio = Decimal(
        str(settings.signal_decider_wait_recheck_evidence_min_support_ratio)
    )
    supported = bool(
        sample_count
        >= settings.signal_decider_wait_recheck_evidence_min_samples
        and span_seconds
        >= Decimal(
            settings.signal_decider_wait_recheck_evidence_min_span_seconds
        )
        and support_ratio >= minimum_support_ratio
    )

    first_volume = first.cumulative_volume
    latest_volume = latest.cumulative_volume
    volume_delta = (
        latest_volume - first_volume
        if first_volume is not None
        and latest_volume is not None
        and latest_volume >= first_volume
        else None
    )
    volume_growth_pct = (
        volume_delta / first_volume * Decimal(100)
        if volume_delta is not None and first_volume is not None and first_volume > 0
        else None
    )
    first_turnover = first.cumulative_turnover
    latest_turnover = latest.cumulative_turnover
    turnover_delta = (
        latest_turnover - first_turnover
        if first_turnover is not None
        and latest_turnover is not None
        and latest_turnover >= first_turnover
        else None
    )
    price_change_bps = (
        (Decimal(latest.ask_price) - Decimal(first.ask_price))
        / Decimal(first.ask_price)
        * Decimal(10_000)
    )
    spread_bps = (
        (Decimal(latest.ask_price) - Decimal(latest.bid_price))
        / Decimal(latest.ask_price)
        * Decimal(10_000)
        if latest.bid_price is not None
        and 0 < latest.bid_price <= latest.ask_price
        else None
    )
    top_depth_imbalance = _imbalance(
        bid=latest.top_bid_quantity,
        ask=latest.top_ask_quantity,
    )
    total_depth_imbalance = _imbalance(
        bid=latest.total_bid_quantity,
        ask=latest.total_ask_quantity,
    )
    return {
        'schema_version': WAIT_RECHECK_EVIDENCE_SCHEMA_VERSION,
        'source': WAIT_RECHECK_QUOTE_SOURCE,
        'status': 'supported' if supported else 'partial',
        'sample_count': sample_count,
        'window_started_at': first.received_at.isoformat(),
        'window_ended_at': latest.received_at.isoformat(),
        'observed_span_seconds': _decimal_text(span_seconds),
        'requirements': {
            'window_seconds': int(
                settings.signal_decider_wait_recheck_evidence_window_seconds
            ),
            'maximum_sample_count': int(
                settings.signal_decider_wait_recheck_evidence_max_samples
            ),
            'minimum_sample_count': int(
                settings.signal_decider_wait_recheck_evidence_min_samples
            ),
            'minimum_span_seconds': int(
                settings.signal_decider_wait_recheck_evidence_min_span_seconds
            ),
            'minimum_support_ratio': _decimal_text(minimum_support_ratio),
        },
        'trigger_support': {
            'qualifying_sample_count': len(qualifying),
            'ratio': _decimal_text(support_ratio),
        },
        'price': {
            'first_ask': first.ask_price,
            'last_ask': latest.ask_price,
            'minimum_ask': min(sample.ask_price for sample in ordered),
            'maximum_ask': max(sample.ask_price for sample in ordered),
            'change_bps': _decimal_text(price_change_bps),
            'latest_spread_bps': _decimal_text(spread_bps),
        },
        'flow': {
            'first_cumulative_volume': _decimal_text(first_volume),
            'last_cumulative_volume': _decimal_text(latest_volume),
            'cumulative_volume_delta': _decimal_text(volume_delta),
            'window_volume_growth_pct': _decimal_text(volume_growth_pct),
            'cumulative_turnover_delta': _decimal_text(turnover_delta),
            'latest_trade_volume': _decimal_text(latest.trade_volume),
            'latest_top_depth_imbalance': _decimal_text(
                top_depth_imbalance
            ),
            'latest_total_depth_imbalance': _decimal_text(
                total_depth_imbalance
            ),
            'latest_execution_strength': _decimal_text(
                latest.execution_strength
            ),
            'latest_buy_rate': _decimal_text(latest.buy_rate),
        },
    }


def _material_change_reasons(
    *,
    candidate: WaitRecheckCandidate,
    observed_price: int,
    evidence: Mapping[str, Any],
) -> tuple[str, ...]:
    if candidate.generation <= 1:
        return ('initial_supported_trigger',)
    prior_price = candidate.prior_observed_price
    if prior_price is None or prior_price <= 0:
        return ()

    reasons: list[str] = []
    current_ratio = _gross_reward_risk_ratio(
        entry_price=observed_price,
        target_price=candidate.target_price,
        stop_loss_price=candidate.stop_loss_price,
    )
    minimum_ratio = _minimum_gross_reward_risk_ratio(candidate.agent_name)
    prior_ratio = _finite_decimal(
        candidate.prior_gross_reward_risk_ratio
    ) or _gross_reward_risk_ratio(
        entry_price=prior_price,
        target_price=candidate.target_price,
        stop_loss_price=candidate.stop_loss_price,
    )
    if (
        current_ratio is not None
        and current_ratio >= minimum_ratio
        and prior_ratio is not None
        and prior_ratio < minimum_ratio
    ):
        reasons.append('reward_risk_threshold_crossed')

    price_improvement_bps = (
        (Decimal(prior_price) - Decimal(observed_price))
        / Decimal(prior_price)
        * Decimal(10_000)
    )
    if (
        current_ratio is not None
        and prior_ratio is not None
        and current_ratio > prior_ratio
        and price_improvement_bps
        >= Decimal(
            str(settings.signal_decider_wait_recheck_material_price_move_bps)
        )
    ):
        reasons.append('observed_entry_price_improved')

    flow = evidence.get('flow')
    flow = flow if isinstance(flow, Mapping) else {}
    prior_evidence = candidate.prior_intraday_evidence or {}
    prior_flow = prior_evidence.get('flow')
    prior_flow = prior_flow if isinstance(prior_flow, Mapping) else {}
    current_volume = _nonnegative_decimal(
        flow.get('last_cumulative_volume')
    )
    prior_volume = _nonnegative_decimal(
        prior_flow.get('last_cumulative_volume')
    )
    if (
        current_volume is not None
        and prior_volume is not None
        and prior_volume > 0
        and current_volume >= prior_volume
    ):
        growth_pct = (
            (current_volume - prior_volume) / prior_volume * Decimal(100)
        )
        if growth_pct >= Decimal(
            str(
                settings.signal_decider_wait_recheck_material_volume_growth_pct
            )
        ):
            reasons.append('cumulative_volume_growth')

    current_depth = _finite_decimal(
        flow.get('latest_total_depth_imbalance')
    )
    prior_depth = _finite_decimal(
        prior_flow.get('latest_total_depth_imbalance')
    )
    if current_depth is None or prior_depth is None:
        current_depth = _finite_decimal(
            flow.get('latest_top_depth_imbalance')
        )
        prior_depth = _finite_decimal(
            prior_flow.get('latest_top_depth_imbalance')
        )
    if (
        current_depth is not None
        and prior_depth is not None
        and current_depth - prior_depth
        >= Decimal(
            str(
                settings.signal_decider_wait_recheck_material_depth_imbalance_delta
            )
        )
    ):
        reasons.append('bid_depth_imbalance_improved')
    return tuple(dict.fromkeys(reasons))


def build_wait_recheck_candidate(
    snapshot: Any,
    *,
    target_trade_date: date,
) -> WaitRecheckCandidate | None:
    """Derive a non-authorizing watch from an exact, durable WAIT snapshot."""

    symbol = str(getattr(snapshot, 'symbol', '') or '').strip().upper()
    if not (len(symbol) == 6 and symbol.isascii() and symbol.isdigit()):
        return None
    agent_name = str(
        getattr(snapshot, 'agent_name', '') or ''
    ).strip().lower()
    if agent_name not in WAIT_RECHECK_AGENT_NAMES:
        return None
    claims = _market_claims(snapshot)
    if not claims or any(claim not in KRX_MARKETS for claim in claims):
        return None
    specific_claims = set(claims) - {'KRX'}
    if len(specific_claims) > 1:
        return None

    response = _json_object(getattr(snapshot, 'response_payload', None))
    action_values = {
        str(value or '').strip().lower()
        for value in (
            getattr(snapshot, 'decision_action', None),
            getattr(snapshot, 'preferred_action', None),
            getattr(snapshot, 'next_day_open_action', None),
            response.get('tactical_action'),
            response.get('preferred_action'),
        )
        if str(value or '').strip()
    }
    if action_values != {'wait'}:
        return None

    decision_delta = response.get('decision_delta')
    decision_delta = decision_delta if isinstance(decision_delta, dict) else {}
    upgrade_trigger = decision_delta.get('upgrade_trigger')
    upgrade_trigger = upgrade_trigger if isinstance(upgrade_trigger, dict) else {}
    expected_target_action = WAIT_RECHECK_PROMOTABLE_ACTION_BY_AGENT[
        agent_name
    ]
    if str(
        upgrade_trigger.get('target_action') or ''
    ).strip().lower() != expected_target_action:
        return None

    trigger_price = _positive_int(response.get('trigger_price'))
    upgrade_trigger_price = _positive_int(upgrade_trigger.get('trigger_price'))
    chase_limit_price = _positive_int(response.get('chase_limit_price'))
    target_price = _positive_int(getattr(snapshot, 'target_price', None))
    if target_price is None:
        target_price = _positive_int(response.get('target_price'))
    stop_loss_price = _positive_int(
        getattr(snapshot, 'stop_loss_price', None)
    )
    if stop_loss_price is None:
        stop_loss_price = _positive_int(response.get('stop_loss_price'))
    if (
        trigger_price is None
        or target_price is None
        or stop_loss_price is None
        or not stop_loss_price < trigger_price < target_price
        or (
            upgrade_trigger_price is not None
            and upgrade_trigger_price != trigger_price
        )
        or (
            chase_limit_price is not None
            and not trigger_price <= chase_limit_price < target_price
        )
    ):
        return None

    request_payload = _json_object(getattr(snapshot, 'request_payload', None))
    fence = request_payload.get('decision_input_generation_fence')
    durable_generation = request_payload.get('durable_decision_input_generation')
    if not (
        isinstance(fence, dict)
        and fence.get('enforcement') == 'required'
        and isinstance(durable_generation, dict)
        and durable_generation.get('schema_version')
        == 'durable_decider_input_generation_v1'
        and durable_generation.get('durable_reuse_eligible') is True
    ):
        return None

    fingerprint = str(
        getattr(snapshot, 'content_fingerprint', None) or ''
    ).strip().lower()
    if len(fingerprint) != 64 or any(value not in '0123456789abcdef' for value in fingerprint):
        return None
    source_version = _aware_utc(
        getattr(snapshot, 'feedback_received_at', None)
        or getattr(snapshot, 'created_at', None)
    )
    source_market_date = getattr(snapshot, 'market_date', None)
    if not isinstance(source_market_date, date) or source_version is None:
        return None
    try:
        snapshot_id = int(getattr(snapshot, 'id'))
    except (TypeError, ValueError):
        return None
    if snapshot_id <= 0:
        return None
    return WaitRecheckCandidate(
        source_snapshot_id=snapshot_id,
        source_snapshot_fingerprint=fingerprint,
        source_market_date=source_market_date,
        target_trade_date=target_trade_date,
        symbol=symbol,
        agent_name=agent_name,
        trigger_price=trigger_price,
        chase_limit_price=chase_limit_price,
        target_price=target_price,
        stop_loss_price=stop_loss_price,
        source_version=source_version,
    )


def canonical_wait_recheck_context(
    *,
    event_id: int,
    observation: WaitRecheckObservation | Any,
    market_state: str,
    market_server_time: str,
) -> tuple[dict[str, Any], str]:
    candidate = observation.candidate
    gross_reward_risk_ratio = _gross_reward_risk_ratio(
        entry_price=observation.observed_price,
        target_price=candidate.target_price,
        stop_loss_price=candidate.stop_loss_price,
    )
    minimum_gross_reward_risk_ratio = _minimum_gross_reward_risk_ratio(
        candidate.agent_name
    )
    material_change_reasons = tuple(
        str(reason).strip()
        for reason in getattr(observation, 'material_change_reasons', ())
        if str(reason).strip()
    )
    context = {
        'schema_version': WAIT_RECHECK_CONTEXT_SCHEMA_VERSION,
        'event_id': int(event_id),
        'symbol': candidate.symbol,
        'agent_name': candidate.agent_name,
        'source_snapshot': {
            'id': candidate.source_snapshot_id,
            'content_fingerprint': candidate.source_snapshot_fingerprint,
            'market_date': candidate.source_market_date.isoformat(),
        },
        'target_trade_date': candidate.target_trade_date.isoformat(),
        'generation_lineage': {
            'generation': int(candidate.generation),
            'rearmed_from_event_id': candidate.rearmed_from_event_id,
            'prior_observed_price': candidate.prior_observed_price,
            'prior_observed_at': (
                candidate.prior_observed_at.astimezone(timezone.utc).isoformat()
                if candidate.prior_observed_at is not None
                else None
            ),
            'material_change_required': candidate.generation > 1,
            'material_change_reasons': list(material_change_reasons),
        },
        'market_session': {
            'market': 'KRX',
            'state': str(market_state).strip().upper(),
            'server_time': str(market_server_time),
        },
        'trigger_contract': {
            'direction': 'at_or_above_with_optional_chase_below_target',
            'trigger_price': candidate.trigger_price,
            'chase_limit_price': candidate.chase_limit_price,
            'target_price': candidate.target_price,
            'stop_loss_price': candidate.stop_loss_price,
        },
        'observation': {
            'price_basis': observation.price_basis,
            'observed_price': observation.observed_price,
            'current_price': observation.current_price,
            'bid_price': observation.bid_price,
            'price_as_of': observation.price_as_of.astimezone(timezone.utc).isoformat(),
            'received_at': observation.received_at.astimezone(timezone.utc).isoformat(),
            'source': observation.quote_source,
            'freshness_status': 'fresh',
            'realtime_subscribed': True,
        },
        'economics_prefilter': {
            'schema_version': 'wait_recheck_reward_risk_prefilter_v1',
            'entry_price_basis': WAIT_RECHECK_PRICE_BASIS,
            'observed_entry_price': observation.observed_price,
            'target_price': candidate.target_price,
            'stop_loss_price': candidate.stop_loss_price,
            'gross_reward_risk_ratio': _decimal_text(
                gross_reward_risk_ratio
            ),
            'minimum_required_ratio': _decimal_text(
                minimum_gross_reward_risk_ratio
            ),
            'passed': bool(
                gross_reward_risk_ratio is not None
                and gross_reward_risk_ratio
                >= minimum_gross_reward_risk_ratio
            ),
        },
        'intraday_evidence': getattr(
            observation,
            'intraday_evidence',
            {},
        ),
        'execution_authority': {
            'direct_order_authority': False,
            'fresh_decider_required': True,
            'only_promotable_action': (
                WAIT_RECHECK_PROMOTABLE_ACTION_BY_AGENT[
                    candidate.agent_name
                ]
            ),
            'fresh_decider_must_supply_complete_price_levels': True,
        },
    }
    return context, _canonical_json_sha256(context)


def validate_wait_recheck_context(
    context: Any,
    *,
    expected_sha256: str,
) -> bool:
    if not isinstance(context, dict):
        return False
    schema_version = context.get('schema_version')
    execution_authority = context.get('execution_authority')
    observation = context.get('observation')
    trigger_contract = context.get('trigger_contract')
    agent_name = str(context.get('agent_name') or '').strip().lower()
    if not (
        schema_version
        in {
            WAIT_RECHECK_LEGACY_CONTEXT_SCHEMA_VERSION,
            WAIT_RECHECK_CONTEXT_SCHEMA_VERSION,
        }
        and agent_name in WAIT_RECHECK_AGENT_NAMES
        and isinstance(execution_authority, dict)
        and execution_authority.get('direct_order_authority') is False
        and execution_authority.get('fresh_decider_required') is True
        and execution_authority.get('only_promotable_action')
        == WAIT_RECHECK_PROMOTABLE_ACTION_BY_AGENT[agent_name]
        and execution_authority.get(
            'fresh_decider_must_supply_complete_price_levels'
        )
        is True
        and isinstance(observation, dict)
        and observation.get('price_basis') == WAIT_RECHECK_PRICE_BASIS
        and observation.get('source') == WAIT_RECHECK_QUOTE_SOURCE
        and observation.get('freshness_status') == 'fresh'
        and observation.get('realtime_subscribed') is True
        and isinstance(trigger_contract, dict)
        and trigger_contract.get('direction')
        == 'at_or_above_with_optional_chase_below_target'
    ):
        return False
    trigger_price = _positive_int(trigger_contract.get('trigger_price'))
    raw_chase_limit_price = trigger_contract.get('chase_limit_price')
    chase_limit_price = _positive_int(raw_chase_limit_price)
    target_price = _positive_int(trigger_contract.get('target_price'))
    stop_loss_price = _positive_int(trigger_contract.get('stop_loss_price'))
    observed_price = _positive_int(observation.get('observed_price'))
    if (
        trigger_price is None
        or target_price is None
        or stop_loss_price is None
        or observed_price is None
        or (
            raw_chase_limit_price is not None
            and chase_limit_price is None
        )
        or not stop_loss_price < trigger_price <= observed_price < target_price
        or (
            chase_limit_price is not None
            and not trigger_price <= observed_price <= chase_limit_price < target_price
        )
    ):
        return False

    if schema_version == WAIT_RECHECK_CONTEXT_SCHEMA_VERSION:
        generation_lineage = context.get('generation_lineage')
        economics = context.get('economics_prefilter')
        evidence = context.get('intraday_evidence')
        if not all(
            isinstance(value, dict)
            for value in (generation_lineage, economics, evidence)
        ):
            return False
        generation = _positive_int(generation_lineage.get('generation'))
        parent_event_id = _positive_int(
            generation_lineage.get('rearmed_from_event_id')
        )
        prior_observed_price = _positive_int(
            generation_lineage.get('prior_observed_price')
        )
        prior_observed_at = _aware_utc(
            generation_lineage.get('prior_observed_at')
        )
        raw_material_reasons = generation_lineage.get(
            'material_change_reasons'
        )
        material_reasons = (
            tuple(
                str(reason).strip()
                for reason in raw_material_reasons
                if str(reason).strip()
            )
            if isinstance(raw_material_reasons, list)
            else ()
        )
        if generation is None or not (
            (
                generation == 1
                and generation_lineage.get('rearmed_from_event_id') is None
                and generation_lineage.get('prior_observed_price') is None
                and generation_lineage.get('prior_observed_at') is None
                and generation_lineage.get('material_change_required') is False
                and material_reasons == ('initial_supported_trigger',)
            )
            or (
                generation > 1
                and parent_event_id is not None
                and prior_observed_price is not None
                and prior_observed_at is not None
                and generation_lineage.get('material_change_required') is True
                and bool(material_reasons)
                and set(material_reasons).issubset(
                    WAIT_RECHECK_MATERIAL_CHANGE_REASONS
                    - {'initial_supported_trigger'}
                )
            )
        ):
            return False

        gross_ratio = _gross_reward_risk_ratio(
            entry_price=observed_price,
            target_price=target_price,
            stop_loss_price=stop_loss_price,
        )
        minimum_ratio = _finite_decimal(
            economics.get('minimum_required_ratio')
        )
        expected_minimum_ratio = _minimum_gross_reward_risk_ratio(
            agent_name
        )
        if not (
            economics.get('schema_version')
            == 'wait_recheck_reward_risk_prefilter_v1'
            and economics.get('entry_price_basis')
            == WAIT_RECHECK_PRICE_BASIS
            and economics.get('observed_entry_price') == observed_price
            and economics.get('target_price') == target_price
            and economics.get('stop_loss_price') == stop_loss_price
            and gross_ratio is not None
            and economics.get('gross_reward_risk_ratio')
            == _decimal_text(gross_ratio)
            and minimum_ratio == expected_minimum_ratio
            and economics.get('minimum_required_ratio')
            == _decimal_text(expected_minimum_ratio)
            and gross_ratio >= minimum_ratio
            and economics.get('passed') is True
        ):
            return False

        requirements = evidence.get('requirements')
        trigger_support = evidence.get('trigger_support')
        price_evidence = evidence.get('price')
        if not all(
            isinstance(value, dict)
            for value in (requirements, trigger_support, price_evidence)
        ):
            return False
        sample_count = _positive_int(evidence.get('sample_count'))
        qualifying_count = _positive_int(
            trigger_support.get('qualifying_sample_count')
        )
        minimum_sample_count = _positive_int(
            requirements.get('minimum_sample_count')
        )
        maximum_sample_count = _positive_int(
            requirements.get('maximum_sample_count')
        )
        window_seconds = _positive_int(
            requirements.get('window_seconds')
        )
        minimum_span_seconds = _nonnegative_decimal(
            requirements.get('minimum_span_seconds')
        )
        minimum_support_ratio = _nonnegative_decimal(
            requirements.get('minimum_support_ratio')
        )
        support_ratio = _nonnegative_decimal(trigger_support.get('ratio'))
        observed_span_seconds = _nonnegative_decimal(
            evidence.get('observed_span_seconds')
        )
        window_started_at = _aware_utc(evidence.get('window_started_at'))
        window_ended_at = _aware_utc(evidence.get('window_ended_at'))
        observation_received_at = _aware_utc(observation.get('received_at'))
        if not (
            evidence.get('schema_version')
            == WAIT_RECHECK_EVIDENCE_SCHEMA_VERSION
            and evidence.get('source') == WAIT_RECHECK_QUOTE_SOURCE
            and evidence.get('status') == 'supported'
            and sample_count is not None
            and qualifying_count is not None
            and qualifying_count <= sample_count
            and window_seconds
            == settings.signal_decider_wait_recheck_evidence_window_seconds
            and maximum_sample_count
            == settings.signal_decider_wait_recheck_evidence_max_samples
            and sample_count <= maximum_sample_count
            and minimum_sample_count is not None
            and minimum_sample_count
            == settings.signal_decider_wait_recheck_evidence_min_samples
            and sample_count >= minimum_sample_count
            and minimum_span_seconds is not None
            and minimum_span_seconds
            == Decimal(
                settings.signal_decider_wait_recheck_evidence_min_span_seconds
            )
            and observed_span_seconds is not None
            and observed_span_seconds >= minimum_span_seconds
            and minimum_support_ratio is not None
            and minimum_support_ratio
            == Decimal(
                str(
                    settings.signal_decider_wait_recheck_evidence_min_support_ratio
                )
            )
            and minimum_support_ratio <= 1
            and support_ratio is not None
            and support_ratio <= 1
            and support_ratio >= minimum_support_ratio
            and trigger_support.get('ratio')
            == _decimal_text(
                Decimal(qualifying_count) / Decimal(sample_count)
            )
            and window_started_at is not None
            and window_ended_at is not None
            and window_started_at <= window_ended_at
            and observation_received_at == window_ended_at
            and evidence.get('observed_span_seconds')
            == _decimal_text(
                Decimal(
                    str(
                        (
                            window_ended_at - window_started_at
                        ).total_seconds()
                    )
                )
            )
            and price_evidence.get('last_ask') == observed_price
        ):
            return False
    normalized_sha256 = str(expected_sha256 or '').strip().lower()
    return bool(
        len(normalized_sha256) == 64
        and _canonical_json_sha256(context) == normalized_sha256
    )


def validate_wait_recheck_event_binding(
    *,
    event: SignalDeciderWaitRecheckEvent,
    context: Any,
    source_snapshot: SignalDeciderDecisionSnapshot | None,
    parent_event: SignalDeciderWaitRecheckEvent | None = None,
) -> bool:
    """Bind one immutable context to its durable event and source snapshot."""

    if not validate_wait_recheck_context(
        context,
        expected_sha256=event.context_sha256,
    ) or source_snapshot is None:
        return False
    source_context = context.get('source_snapshot')
    trigger_contract = context.get('trigger_contract')
    observation = context.get('observation')
    if not all(
        isinstance(value, dict)
        for value in (source_context, trigger_contract, observation)
    ):
        return False
    try:
        event_id = int(event.id)
        event_snapshot_id = int(event.source_snapshot_id)
        actual_snapshot_id = int(source_snapshot.id)
    except (TypeError, ValueError):
        return False
    event_symbol = str(event.symbol or '')
    event_agent_name = str(event.agent_name or '')
    event_fingerprint = str(event.source_snapshot_fingerprint or '')
    actual_fingerprint = str(source_snapshot.content_fingerprint or '')
    if not (
        context.get('event_id') == event_id
        and context.get('symbol')
        == event_symbol
        == source_snapshot.symbol
        and context.get('agent_name')
        == event_agent_name
        == source_snapshot.agent_name
        and source_context.get('id')
        == event_snapshot_id
        == actual_snapshot_id
        and source_context.get('content_fingerprint')
        == event_fingerprint
        == actual_fingerprint
        and source_context.get('market_date')
        == event.source_market_date.isoformat()
        == source_snapshot.market_date.isoformat()
        and context.get('target_trade_date')
        == event.target_trade_date.isoformat()
    ):
        return False

    schema_version = context.get('schema_version')
    event_generation = _positive_int(getattr(event, 'generation', 1))
    raw_event_parent_id = getattr(event, 'rearmed_from_event_id', None)
    event_parent_id = _positive_int(raw_event_parent_id)
    validated_parent_context: dict[str, Any] | None = None
    if schema_version == WAIT_RECHECK_LEGACY_CONTEXT_SCHEMA_VERSION:
        if not (
            event_generation == 1
            and raw_event_parent_id is None
        ):
            return False
    else:
        generation_lineage = context.get('generation_lineage')
        if not isinstance(generation_lineage, dict) or not (
            generation_lineage.get('generation') == event_generation
            and generation_lineage.get('rearmed_from_event_id')
            == event_parent_id
        ):
            return False
        if event_generation is not None and event_generation > 1:
            parent_context = (
                _json_object(parent_event.context_payload)
                if parent_event is not None
                else {}
            )
            parent_observed_at = (
                _aware_utc(parent_event.observed_at)
                if parent_event is not None
                else None
            )
            if not (
                parent_event is not None
                and int(parent_event.id) == event_parent_id
                and parent_event.target_trade_date
                == event.target_trade_date
                and parent_event.symbol == event_symbol
                and parent_event.agent_name == event_agent_name
                and int(parent_event.generation) + 1 == event_generation
                and parent_event.status == 'completed'
                and str(parent_event.result_action or '').strip().lower()
                == 'wait'
                and parent_event.result_snapshot_id is not None
                and int(parent_event.result_snapshot_id)
                == event_snapshot_id
                and parent_observed_at is not None
                and generation_lineage.get('prior_observed_price')
                == int(parent_event.observed_price)
                and generation_lineage.get('prior_observed_at')
                == parent_observed_at.isoformat()
                and validate_wait_recheck_context(
                    parent_context,
                    expected_sha256=parent_event.context_sha256,
                )
            ):
                return False
            validated_parent_context = parent_context

    candidate = build_wait_recheck_candidate(
        source_snapshot,
        target_trade_date=event.target_trade_date,
    )
    if candidate is None or not (
        candidate.source_snapshot_id == event_snapshot_id
        and candidate.source_snapshot_fingerprint == event_fingerprint
        and candidate.source_market_date == event.source_market_date
        and candidate.target_trade_date == event.target_trade_date
        and candidate.symbol == event_symbol
        and candidate.agent_name == event_agent_name
        and candidate.trigger_price == event.trigger_price
        and candidate.chase_limit_price == event.chase_limit_price
        and candidate.target_price == event.target_price
        and candidate.stop_loss_price == event.stop_loss_price
    ):
        return False

    if event_generation is not None and event_generation > 1:
        generation_lineage = context.get('generation_lineage')
        current_evidence = context.get('intraday_evidence')
        parent_economics = (
            validated_parent_context.get('economics_prefilter')
            if validated_parent_context is not None
            else None
        )
        parent_evidence = (
            validated_parent_context.get('intraday_evidence')
            if validated_parent_context is not None
            else None
        )
        if not (
            isinstance(generation_lineage, dict)
            and isinstance(current_evidence, dict)
            and isinstance(parent_economics, dict)
            and isinstance(parent_evidence, dict)
            and parent_event is not None
        ):
            return False
        expected_material_reasons = _material_change_reasons(
            candidate=replace(
                candidate,
                generation=event_generation,
                rearmed_from_event_id=event_parent_id,
                prior_observed_price=int(parent_event.observed_price),
                prior_observed_at=_aware_utc(parent_event.observed_at),
                prior_gross_reward_risk_ratio=(
                    str(parent_economics.get('gross_reward_risk_ratio'))
                    if parent_economics.get('gross_reward_risk_ratio')
                    is not None
                    else None
                ),
                prior_intraday_evidence=parent_evidence,
            ),
            observed_price=int(event.observed_price),
            evidence=current_evidence,
        )
        actual_material_reasons = tuple(
            str(reason).strip()
            for reason in generation_lineage.get(
                'material_change_reasons',
                (),
            )
            if str(reason).strip()
        )
        if actual_material_reasons != expected_material_reasons:
            return False

    def exact_price_matches(raw_value: Any, durable_value: Any) -> bool:
        if durable_value is None:
            return raw_value is None
        return bool(
            isinstance(raw_value, int)
            and not isinstance(raw_value, bool)
            and raw_value == int(durable_value)
        )

    def exact_timestamp_matches(raw_value: Any, durable_value: Any) -> bool:
        durable_timestamp = _aware_utc(durable_value)
        return bool(
            isinstance(raw_value, str)
            and durable_timestamp is not None
            and raw_value == durable_timestamp.isoformat()
        )

    return bool(
        exact_price_matches(
            trigger_contract.get('trigger_price'),
            event.trigger_price,
        )
        and int(event.trigger_price) == candidate.trigger_price
        and exact_price_matches(
            trigger_contract.get('chase_limit_price'),
            event.chase_limit_price,
        )
        and exact_price_matches(
            trigger_contract.get('target_price'),
            event.target_price,
        )
        and int(event.target_price) == candidate.target_price
        and exact_price_matches(
            trigger_contract.get('stop_loss_price'),
            event.stop_loss_price,
        )
        and int(event.stop_loss_price) == candidate.stop_loss_price
        and exact_price_matches(
            observation.get('observed_price'),
            event.observed_price,
        )
        and exact_price_matches(
            observation.get('current_price'),
            event.observed_current_price,
        )
        and exact_price_matches(
            observation.get('bid_price'),
            event.observed_bid_price,
        )
        and observation.get('price_basis') == event.price_basis
        and observation.get('source') == event.quote_source
        and exact_timestamp_matches(
            observation.get('price_as_of'),
            event.price_as_of,
        )
        and exact_timestamp_matches(
            observation.get('received_at'),
            event.observed_at,
        )
    )


def wait_recheck_dispatch_completed(result: Any) -> bool:
    summary = result.get('summary') if isinstance(result, dict) else None
    if not isinstance(summary, dict):
        return False
    return bool(
        summary.get('status') == 'completed'
        and summary.get('completed') == 1
        and summary.get('computed_count') == 1
        and summary.get('reused_count') == 0
        and summary.get('failed') == 0
    )


def merge_wait_recheck_monitored_source(
    base: KISRealtimeMonitoredSymbolSource,
    *,
    symbols: Sequence[str],
    source_version: datetime | None,
    membership_complete: bool,
) -> KISRealtimeMonitoredSymbolSource:
    base_symbols = tuple(dict.fromkeys(base.symbols))
    wait_recheck_symbols = tuple(dict.fromkeys(symbols))
    ordered_symbols = tuple(
        dict.fromkeys([*base_symbols, *wait_recheck_symbols])
    )
    return KISRealtimeMonitoredSymbolSource(
        symbols=ordered_symbols,
        market_date=base.market_date,
        source_version=base.source_version,
        membership_complete=base.membership_complete,
        protected_symbols=tuple(
            symbol
            for symbol in ordered_symbols
            if symbol in set(base.protected_symbols)
        ),
        critical_protected_symbols=tuple(
            symbol
            for symbol in ordered_symbols
            if symbol in set(base.critical_protected_symbols)
        ),
        base_symbols=base_symbols,
        wait_recheck_symbols=wait_recheck_symbols,
        wait_recheck_membership_complete=bool(membership_complete),
        wait_recheck_source_version=source_version,
    )


class SignalDeciderWaitRecheckService:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        # Linearizes session revocation with the actual orchestrator entry
        # point.  The orchestrator releases this lock through its first-line
        # callback, so close/shutdown never waits for the model call itself.
        self._dispatch_entry_lock = threading.RLock()
        self._candidates_by_key: dict[
            tuple[str, str], WaitRecheckCandidate
        ] = {}
        self._pending_by_key: dict[
            tuple[date, str, str, int], WaitRecheckObservation
        ] = {}
        self._reserved_keys: set[tuple[date, str, str, int]] = set()
        self._tick_samples_by_symbol: dict[
            str, deque[WaitRecheckTickSample]
        ] = {}
        self._latest_tick_gate_by_symbol: dict[
            str, tuple[datetime, bool]
        ] = {}
        self._wake_callback: Callable[[], None] | None = None
        self._wake_notification_count = 0
        self._rr_prefilter_rejection_count = 0
        self._evidence_rejection_count = 0
        self._material_change_rejection_count = 0
        self._last_source_refresh_at: datetime | None = None
        self._last_cycle_at: datetime | None = None
        self._last_cycle_result: dict[str, Any] | None = None
        self._candidate_cache_epoch = 0

    def runtime_snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                'enabled': bool(
                    settings.signal_decider_wait_recheck_monitoring_enabled
                ),
                'candidate_symbols': sorted(
                    {
                        candidate.symbol
                        for candidate in self._candidates_by_key.values()
                    }
                ),
                'candidate_count': len(self._candidates_by_key),
                'pending_count': len(self._pending_by_key),
                'reserved_count': len(self._reserved_keys),
                'rolling_evidence_symbols': sorted(
                    self._tick_samples_by_symbol
                ),
                'rolling_evidence_sample_count': sum(
                    len(samples)
                    for samples in self._tick_samples_by_symbol.values()
                ),
                'wake_notification_count': self._wake_notification_count,
                'reward_risk_prefilter_rejection_count': (
                    self._rr_prefilter_rejection_count
                ),
                'evidence_rejection_count': self._evidence_rejection_count,
                'material_change_rejection_count': (
                    self._material_change_rejection_count
                ),
                'last_source_refresh_at': (
                    self._last_source_refresh_at.isoformat()
                    if self._last_source_refresh_at is not None
                    else None
                ),
                'last_cycle_at': (
                    self._last_cycle_at.isoformat()
                    if self._last_cycle_at is not None
                    else None
                ),
                'last_cycle_result': self._last_cycle_result,
                'direct_order_authority': False,
            }

    def set_wake_callback(
        self,
        callback: Callable[[], None] | None,
    ) -> None:
        with self._lock:
            self._wake_callback = callback

    def _notify_wake(self) -> None:
        with self._lock:
            callback = self._wake_callback
        if callback is None:
            return
        try:
            callback()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                'WAIT recheck wake callback failed closed: %s',
                type(exc).__name__,
            )
            return
        with self._lock:
            self._wake_notification_count += 1

    def replace_candidate_cache(
        self,
        candidates: Iterable[WaitRecheckCandidate],
        *,
        expected_cache_epoch: int | None = None,
    ) -> bool:
        normalized = {
            (candidate.symbol, candidate.agent_name): candidate
            for candidate in candidates
        }
        active_keys = {
            (
                candidate.target_trade_date,
                candidate.symbol,
                candidate.agent_name,
                candidate.source_snapshot_id,
            )
            for candidate in normalized.values()
        }
        active_symbols = {
            candidate.symbol for candidate in normalized.values()
        }
        with self._lock:
            if (
                expected_cache_epoch is not None
                and expected_cache_epoch != self._candidate_cache_epoch
            ):
                return False
            self._candidates_by_key = normalized
            self._pending_by_key = {
                key: observation
                for key, observation in self._pending_by_key.items()
                if key in active_keys
            }
            self._reserved_keys.intersection_update(active_keys)
            self._tick_samples_by_symbol = {
                symbol: samples
                for symbol, samples in self._tick_samples_by_symbol.items()
                if symbol in active_symbols
            }
            self._latest_tick_gate_by_symbol = {
                symbol: gate
                for symbol, gate in self._latest_tick_gate_by_symbol.items()
                if symbol in active_symbols
            }
            self._last_source_refresh_at = _utcnow()
            return True

    def candidate_cache_epoch(self) -> int:
        with self._lock:
            return self._candidate_cache_epoch

    def clear_candidate_cache(self) -> bool:
        with self._dispatch_entry_lock:
            with self._lock:
                self._candidate_cache_epoch += 1
                had_cached_state = bool(
                    self._candidates_by_key
                    or self._pending_by_key
                    or self._reserved_keys
                    or self._tick_samples_by_symbol
                    or self._latest_tick_gate_by_symbol
                )
                if not had_cached_state:
                    return False
                self._candidates_by_key = {}
                self._pending_by_key = {}
                self._reserved_keys.clear()
                self._tick_samples_by_symbol = {}
                self._latest_tick_gate_by_symbol = {}
                self._last_source_refresh_at = _utcnow()
                return True

    def _refresh_candidate_cache_during_target_session(
        self,
        *,
        target_trade_date: date,
        expected_cache_epoch: int | None = None,
    ) -> None:
        refresh_cache_epoch = (
            self.candidate_cache_epoch()
            if expected_cache_epoch is None
            else expected_cache_epoch
        )
        if not _target_regular_session_is_open_at(
            target_trade_date,
            _utcnow(),
        ):
            self.clear_candidate_cache()
            return
        self.load_subscription_source(
            target_trade_date=target_trade_date,
            expected_cache_epoch=refresh_cache_epoch,
        )
        if not _target_regular_session_is_open_at(
            target_trade_date,
            _utcnow(),
        ):
            self.clear_candidate_cache()

    def _record_tick_gate(
        self,
        *,
        symbol: str,
        received_at: datetime,
        admitted: bool,
    ) -> None:
        with self._lock:
            existing = self._latest_tick_gate_by_symbol.get(symbol)
            if existing is not None and received_at < existing[0]:
                return
            self._latest_tick_gate_by_symbol[symbol] = (
                received_at,
                admitted,
            )
            if admitted:
                return
            # A rejected tick marks a discontinuity in trustworthy market
            # evidence.  Do not let pre-halt, limited, unsubscribed, or stale
            # samples satisfy the rolling window after the feed recovers.
            self._tick_samples_by_symbol.pop(symbol, None)
            for key, observation in tuple(self._pending_by_key.items()):
                if (
                    key[1] == symbol
                    and received_at >= observation.received_at
                ):
                    self._pending_by_key.pop(key, None)
                    self._reserved_keys.discard(key)

    def observe_realtime_tick(
        self,
        tick: KISRealtimePriceTick,
        *,
        subscribed: bool,
        limited: bool,
        now: datetime | None = None,
    ) -> bool:
        current = _aware_utc(now or _utcnow())
        received_at = _aware_utc(tick.received_at)
        price_as_of = _aware_utc(tick.price_as_of)
        symbol = str(tick.symbol or '').strip().upper()
        if current is None or received_at is None or price_as_of is None:
            return False
        received_seoul = received_at.astimezone(SEOUL_TZ)
        price_seoul = price_as_of.astimezone(SEOUL_TZ)
        base_admitted = bool(
            subscribed
            and not limited
            and tick.trade_halt is False
            and KRX_REGULAR_SESSION_OPEN
            <= received_seoul.time().replace(tzinfo=None)
            <= KRX_REGULAR_SESSION_CLOSE
            and KRX_REGULAR_SESSION_OPEN
            <= price_seoul.time().replace(tzinfo=None)
            <= KRX_REGULAR_SESSION_CLOSE
        )
        for observed_time in (received_at, price_as_of):
            age_seconds = (current - observed_time).total_seconds()
            if (
                age_seconds < -5
                or age_seconds
                > float(settings.kis_realtime_price_stale_after_seconds)
            ):
                base_admitted = False
        observed_price = _positive_int(tick.ask_price)
        if observed_price is None:
            base_admitted = False
        sample = _tick_sample(tick) if base_admitted else None
        if symbol:
            self._record_tick_gate(
                symbol=symbol,
                received_at=received_at,
                admitted=bool(base_admitted and sample is not None),
            )
        if not base_admitted or observed_price is None or sample is None:
            return False
        if sample.symbol != symbol:
            return False
        with self._lock:
            candidates = tuple(
                candidate
                for (candidate_symbol, _agent_name), candidate in sorted(
                    self._candidates_by_key.items()
                )
                if candidate_symbol == symbol
            )
            if not candidates:
                return False
            samples = self._tick_samples_by_symbol.setdefault(
                symbol,
                deque(),
            )
            if samples and received_at < samples[-1].received_at:
                return False
            if samples and price_as_of < samples[-1].price_as_of:
                return False
            if samples and price_as_of == samples[-1].price_as_of:
                # KIS domestic trade time is second-granularity. Repeated
                # frames from one exchange timestamp are one observation, not
                # independent evidence; retain only the freshest payload for
                # that market second.
                samples[-1] = sample
            else:
                samples.append(sample)
            window_cutoff = received_at - timedelta(
                seconds=(
                    settings.signal_decider_wait_recheck_evidence_window_seconds
                )
            )
            while samples and samples[0].received_at < window_cutoff:
                samples.popleft()
            while (
                len(samples)
                > settings.signal_decider_wait_recheck_evidence_max_samples
            ):
                samples.popleft()
            queued_new = False
            for candidate in candidates:
                key = (
                    candidate.target_trade_date,
                    symbol,
                    candidate.agent_name,
                    candidate.source_snapshot_id,
                )
                pending_observation = self._pending_by_key.get(key)
                if key in self._reserved_keys and pending_observation is None:
                    continue
                if (
                    pending_observation is not None
                    and received_at < pending_observation.received_at
                ):
                    continue
                within_band = bool(
                    received_seoul.date()
                    == candidate.target_trade_date
                    and price_seoul.date()
                    == candidate.target_trade_date
                    and observed_price >= candidate.trigger_price
                    and observed_price < candidate.target_price
                    and (
                        candidate.chase_limit_price is None
                        or observed_price <= candidate.chase_limit_price
                    )
                    and (
                        candidate.rearm_not_before is None
                        or received_at >= candidate.rearm_not_before
                    )
                )
                gross_ratio = _gross_reward_risk_ratio(
                    entry_price=observed_price,
                    target_price=candidate.target_price,
                    stop_loss_price=candidate.stop_loss_price,
                )
                ratio_passes = bool(
                    gross_ratio is not None
                    and gross_ratio
                    >= _minimum_gross_reward_risk_ratio(
                        candidate.agent_name
                    )
                )
                candidate_samples = tuple(
                    sample
                    for sample in samples
                    if candidate.rearm_not_before is None
                    or sample.received_at >= candidate.rearm_not_before
                )
                evidence = (
                    _intraday_evidence(
                        samples=candidate_samples,
                        candidate=candidate,
                    )
                    if candidate_samples
                    else {'status': 'partial'}
                )
                evidence_supported = evidence.get('status') == 'supported'
                material_change_reasons = _material_change_reasons(
                    candidate=candidate,
                    observed_price=observed_price,
                    evidence=evidence,
                )
                material_change_passes = bool(material_change_reasons)
                if not (
                    within_band
                    and ratio_passes
                    and evidence_supported
                    and material_change_passes
                ):
                    if pending_observation is not None:
                        self._pending_by_key.pop(key, None)
                        self._reserved_keys.discard(key)
                    if within_band and not ratio_passes:
                        self._rr_prefilter_rejection_count += 1
                    elif within_band and ratio_passes and not evidence_supported:
                        self._evidence_rejection_count += 1
                    elif (
                        within_band
                        and ratio_passes
                        and evidence_supported
                        and not material_change_passes
                    ):
                        self._material_change_rejection_count += 1
                    continue
                observation = WaitRecheckObservation(
                    candidate=candidate,
                    observed_price=observed_price,
                    current_price=_positive_int(tick.current_price),
                    bid_price=_positive_int(tick.bid_price),
                    price_basis=WAIT_RECHECK_PRICE_BASIS,
                    price_as_of=price_as_of,
                    received_at=received_at,
                    quote_source=WAIT_RECHECK_QUOTE_SOURCE,
                    intraday_evidence=evidence,
                    material_change_reasons=material_change_reasons,
                )
                self._pending_by_key[key] = observation
                self._reserved_keys.add(key)
                queued_new = queued_new or pending_observation is None
        if queued_new:
            self._notify_wake()
        return queued_new

    def pop_pending_observations(
        self,
        *,
        limit: int,
    ) -> list[WaitRecheckObservation]:
        with self._lock:
            selected_keys = sorted(
                self._pending_by_key,
                key=lambda key: self._pending_by_key[key].received_at,
            )[: max(int(limit), 0)]
            return [
                self._pending_by_key.pop(key)
                for key in selected_keys
            ]

    def _release_observation(self, observation: WaitRecheckObservation) -> None:
        key = (
            observation.candidate.target_trade_date,
            observation.candidate.symbol,
            observation.candidate.agent_name,
            observation.candidate.source_snapshot_id,
        )
        with self._lock:
            self._reserved_keys.discard(key)

    def _observation_is_dispatchable(
        self,
        observation: WaitRecheckObservation,
        *,
        market_now: datetime,
        session_open_at: datetime,
    ) -> bool:
        received_at = _aware_utc(observation.received_at)
        price_as_of = _aware_utc(observation.price_as_of)
        normalized_market_now = _aware_utc(market_now)
        normalized_session_open = _aware_utc(session_open_at)
        if (
            received_at is None
            or price_as_of is None
            or normalized_market_now is None
            or normalized_session_open is None
        ):
            return False
        stale_after_seconds = float(
            settings.kis_realtime_price_stale_after_seconds
        )
        for observed_time in (received_at, price_as_of):
            age_seconds = (
                normalized_market_now - observed_time
            ).total_seconds()
            if (
                age_seconds < -5
                or age_seconds > stale_after_seconds
                or observed_time < normalized_session_open
            ):
                return False
        candidate = observation.candidate
        with self._lock:
            latest_gate = self._latest_tick_gate_by_symbol.get(
                candidate.symbol
            )
        if latest_gate is not None and (
            latest_gate[0] > received_at
            or (
                latest_gate[0] == received_at
                and latest_gate[1] is False
            )
        ):
            return False
        gross_ratio = _gross_reward_risk_ratio(
            entry_price=observation.observed_price,
            target_price=candidate.target_price,
            stop_loss_price=candidate.stop_loss_price,
        )
        return bool(
            received_at.astimezone(SEOUL_TZ).date()
            == candidate.target_trade_date
            and price_as_of.astimezone(SEOUL_TZ).date()
            == candidate.target_trade_date
            and candidate.trigger_price <= observation.observed_price
            < candidate.target_price
            and (
                candidate.chase_limit_price is None
                or observation.observed_price <= candidate.chase_limit_price
            )
            and (
                candidate.rearm_not_before is None
                or received_at >= candidate.rearm_not_before
            )
            and gross_ratio is not None
            and gross_ratio
            >= _minimum_gross_reward_risk_ratio(candidate.agent_name)
            and observation.intraday_evidence.get('status') == 'supported'
            and bool(observation.material_change_reasons)
        )

    def load_subscription_source(
        self,
        *,
        target_trade_date: date,
        expected_cache_epoch: int | None = None,
    ) -> WaitRecheckSubscriptionSource:
        def empty_source(
            *,
            source_version: datetime | None,
            membership_complete: bool,
        ) -> WaitRecheckSubscriptionSource:
            committed = self.replace_candidate_cache(
                (),
                expected_cache_epoch=expected_cache_epoch,
            )
            return WaitRecheckSubscriptionSource(
                symbols=(),
                source_version=(source_version if committed else None),
                membership_complete=(
                    membership_complete if committed else False
                ),
            )

        if not settings.signal_decider_wait_recheck_monitoring_enabled:
            return empty_source(
                source_version=_utcnow(),
                membership_complete=True,
            )
        try:
            target_is_trading_day = is_krx_trading_day(
                target_trade_date,
                allow_fallback=False,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                'WAIT recheck target-date calendar lookup failed closed: %s',
                type(exc).__name__,
            )
            return empty_source(
                source_version=None,
                membership_complete=False,
            )
        if not target_is_trading_day:
            return empty_source(
                source_version=_utcnow(),
                membership_complete=True,
            )
        try:
            source_market_date = previous_krx_trading_date(
                target_trade_date,
                allow_fallback=False,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                'WAIT recheck trading-calendar lookup failed closed: %s',
                type(exc).__name__,
            )
            return empty_source(
                source_version=None,
                membership_complete=False,
            )
        if source_market_date is None:
            return empty_source(
                source_version=None,
                membership_complete=False,
            )
        try:
            with session_scope() as session:
                events = session.execute(
                    select(SignalDeciderWaitRecheckEvent)
                    .where(
                        SignalDeciderWaitRecheckEvent.target_trade_date
                        == target_trade_date
                    )
                    .order_by(
                        SignalDeciderWaitRecheckEvent.symbol.asc(),
                        SignalDeciderWaitRecheckEvent.agent_name.asc(),
                        SignalDeciderWaitRecheckEvent.generation.asc(),
                        SignalDeciderWaitRecheckEvent.id.asc(),
                    )
                ).scalars().all()
                events_by_key: dict[
                    tuple[str, str], list[SignalDeciderWaitRecheckEvent]
                ] = {}
                for event in events:
                    events_by_key.setdefault(
                        (str(event.symbol), str(event.agent_name)),
                        [],
                    ).append(event)
                events_by_id = {int(event.id): event for event in events}
                unaccepted_decision_run_ids = (
                    _unaccepted_decision_run_ids(events)
                )
                snapshots = session.execute(
                    select(SignalDeciderDecisionSnapshot)
                    .where(
                        SignalDeciderDecisionSnapshot.market_date
                        == source_market_date,
                        SignalDeciderDecisionSnapshot.agent_name.in_(
                            WAIT_RECHECK_AGENT_NAMES
                        ),
                    )
                    .order_by(
                        SignalDeciderDecisionSnapshot.symbol.asc(),
                        SignalDeciderDecisionSnapshot.agent_name.asc(),
                        SignalDeciderDecisionSnapshot.created_at.desc(),
                        SignalDeciderDecisionSnapshot.id.desc(),
                    )
                ).scalars().all()

                latest_by_key: dict[
                    tuple[str, str], SignalDeciderDecisionSnapshot
                ] = {}
                for snapshot in snapshots:
                    if (
                        str(snapshot.decision_run_id or '').strip()
                        in unaccepted_decision_run_ids
                    ):
                        # A provider call can persist a snapshot before its
                        # orchestration result is durably accepted. Failed or
                        # timed-out attempts must not displace the immutable
                        # source owned by their retry envelope.
                        continue
                    latest_by_key.setdefault(
                        (snapshot.symbol, snapshot.agent_name),
                        snapshot,
                    )
                eligible_candidates: list[WaitRecheckCandidate] = []
                daily_event_capacity_available = (
                    len(events)
                    < settings.signal_decider_wait_recheck_max_daily_events
                )
                if daily_event_capacity_available or any(
                    _event_retry_not_before(event) is not None
                    for event in events
                ):
                    for key in sorted(latest_by_key):
                        candidate = build_wait_recheck_candidate(
                            latest_by_key[key],
                            target_trade_date=target_trade_date,
                        )
                        if candidate is None:
                            continue
                        best_case_ratio = _gross_reward_risk_ratio(
                            entry_price=candidate.trigger_price,
                            target_price=candidate.target_price,
                            stop_loss_price=candidate.stop_loss_price,
                        )
                        if (
                            best_case_ratio is None
                            or best_case_ratio
                            < _minimum_gross_reward_risk_ratio(
                                candidate.agent_name
                            )
                        ):
                            # Even an exact trigger touch cannot satisfy the
                            # lane's economics. Do not spend a subscription or
                            # provider slot on an impossible promotion.
                            continue
                        history = events_by_key.get(key, [])
                        if not history:
                            if daily_event_capacity_available:
                                eligible_candidates.append(candidate)
                            continue
                        same_source_event = next(
                            (
                                event
                                for event in reversed(history)
                                if int(event.source_snapshot_id)
                                == candidate.source_snapshot_id
                            ),
                            None,
                        )
                        if same_source_event is not None:
                            retry_not_before = _event_retry_not_before(
                                same_source_event
                            )
                            if retry_not_before is not None:
                                prior_event = (
                                    events_by_id.get(
                                        int(
                                            same_source_event.rearmed_from_event_id
                                        )
                                    )
                                    if same_source_event.rearmed_from_event_id
                                    is not None
                                    else None
                                )
                                prior_context = (
                                    _json_object(
                                        prior_event.context_payload
                                    )
                                    if prior_event is not None
                                    else {}
                                )
                                prior_evidence = prior_context.get(
                                    'intraday_evidence'
                                )
                                prior_economics = prior_context.get(
                                    'economics_prefilter'
                                )
                                eligible_candidates.append(
                                    replace(
                                        candidate,
                                        generation=int(
                                            same_source_event.generation
                                        ),
                                        rearmed_from_event_id=(
                                            int(
                                                same_source_event.rearmed_from_event_id
                                            )
                                            if same_source_event.rearmed_from_event_id
                                            is not None
                                            else None
                                        ),
                                        rearm_not_before=retry_not_before,
                                        prior_observed_price=(
                                            int(prior_event.observed_price)
                                            if prior_event is not None
                                            else None
                                        ),
                                        prior_observed_at=(
                                            _aware_utc(
                                                prior_event.observed_at
                                            )
                                            if prior_event is not None
                                            else None
                                        ),
                                        prior_gross_reward_risk_ratio=(
                                            str(
                                                prior_economics.get(
                                                    'gross_reward_risk_ratio'
                                                )
                                            )
                                            if isinstance(
                                                prior_economics,
                                                dict,
                                            )
                                            and prior_economics.get(
                                                'gross_reward_risk_ratio'
                                            )
                                            is not None
                                            else None
                                        ),
                                        prior_intraday_evidence=(
                                            prior_evidence
                                            if isinstance(
                                                prior_evidence,
                                                dict,
                                            )
                                            else None
                                        ),
                                        retry_event_id=int(
                                            same_source_event.id
                                        ),
                                    )
                                )
                            # A durable row owns this immutable source. It may
                            # be refreshed only through the bounded, fresh-
                            # evidence retry path above.
                            continue
                        if not daily_event_capacity_available:
                            # The daily budget caps new durable rows, not a
                            # fresh-evidence retry of an existing bounded row.
                            continue
                        prior_event = history[-1]
                        prior_generation = _positive_int(
                            prior_event.generation
                        )
                        completed_at = _aware_utc(
                            prior_event.completed_at
                        )
                        if not (
                            prior_event.status == 'completed'
                            and str(
                                prior_event.result_action or ''
                            ).strip().lower()
                            == 'wait'
                            and prior_event.result_snapshot_id is not None
                            and int(prior_event.result_snapshot_id)
                            == candidate.source_snapshot_id
                            and prior_generation is not None
                            and prior_generation
                            < settings.signal_decider_wait_recheck_max_generations_per_day
                            and completed_at is not None
                        ):
                            continue
                        prior_context = _json_object(
                            prior_event.context_payload
                        )
                        prior_evidence = prior_context.get(
                            'intraday_evidence'
                        )
                        prior_economics = prior_context.get(
                            'economics_prefilter'
                        )
                        eligible_candidates.append(
                            replace(
                                candidate,
                                generation=prior_generation + 1,
                                rearmed_from_event_id=int(prior_event.id),
                                rearm_not_before=(
                                    completed_at
                                    + timedelta(
                                        seconds=(
                                            settings.signal_decider_wait_recheck_rearm_cooldown_seconds
                                        )
                                    )
                                ),
                                prior_observed_price=int(
                                    prior_event.observed_price
                                ),
                                prior_observed_at=_aware_utc(
                                    prior_event.observed_at
                                ),
                                prior_gross_reward_risk_ratio=(
                                    str(
                                        prior_economics.get(
                                            'gross_reward_risk_ratio'
                                        )
                                    )
                                    if isinstance(prior_economics, dict)
                                    and prior_economics.get(
                                        'gross_reward_risk_ratio'
                                    )
                                    is not None
                                    else None
                                ),
                                prior_intraday_evidence=(
                                    prior_evidence
                                    if isinstance(prior_evidence, dict)
                                    else None
                                ),
                            )
                        )
                selected_symbols = set(
                    sorted(
                        {
                            candidate.symbol
                            for candidate in eligible_candidates
                        }
                    )[: settings.signal_decider_wait_recheck_max_symbols]
                )
                candidates = [
                    candidate
                    for candidate in eligible_candidates
                    if candidate.symbol in selected_symbols
                ]
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                'WAIT recheck candidate lookup failed closed: %s',
                type(exc).__name__,
            )
            return empty_source(
                source_version=None,
                membership_complete=False,
            )

        committed = self.replace_candidate_cache(
            candidates,
            expected_cache_epoch=expected_cache_epoch,
        )
        if not committed:
            return WaitRecheckSubscriptionSource(
                symbols=(),
                source_version=None,
                membership_complete=False,
            )
        return WaitRecheckSubscriptionSource(
            symbols=tuple(
                dict.fromkeys(candidate.symbol for candidate in candidates)
            ),
            # This version identifies the successful membership observation,
            # not the newest candidate snapshot.  Candidate removal can make
            # the latter move backwards and cause the realtime source's
            # monotonic staleness gate to retain a retired WAIT symbol.
            source_version=_utcnow(),
            membership_complete=True,
            candidates=tuple(candidates),
        )

    def _claim_observation(
        self,
        *,
        observation: WaitRecheckObservation,
        market_state: str,
        market_server_time: str,
    ) -> int | None:
        candidate = observation.candidate
        try:
            with session_scope() as session:
                # Serialize the daily cap and lane-generation checks across
                # Core processes.  The unique constraint alone cannot prevent
                # two concurrent claims from both observing spare daily
                # capacity before either transaction commits.
                session.execute(
                    text('SELECT pg_advisory_xact_lock(:lock_key)'),
                    {
                        'lock_key': _daily_claim_lock_key(
                            candidate.target_trade_date
                        )
                    },
                )
                existing_event = session.execute(
                    select(SignalDeciderWaitRecheckEvent)
                    .where(
                        SignalDeciderWaitRecheckEvent.target_trade_date
                        == candidate.target_trade_date,
                        SignalDeciderWaitRecheckEvent.symbol
                        == candidate.symbol,
                        SignalDeciderWaitRecheckEvent.agent_name
                        == candidate.agent_name,
                        SignalDeciderWaitRecheckEvent.source_snapshot_id
                        == candidate.source_snapshot_id,
                    )
                    .with_for_update()
                ).scalar_one_or_none()
                lane_events = session.execute(
                    select(SignalDeciderWaitRecheckEvent)
                    .where(
                        SignalDeciderWaitRecheckEvent.target_trade_date
                        == candidate.target_trade_date,
                        SignalDeciderWaitRecheckEvent.symbol
                        == candidate.symbol,
                        SignalDeciderWaitRecheckEvent.agent_name
                        == candidate.agent_name,
                    )
                    .order_by(
                        SignalDeciderWaitRecheckEvent.generation.asc(),
                        SignalDeciderWaitRecheckEvent.id.asc(),
                    )
                ).scalars().all()
                candidate_snapshots = session.execute(
                    select(SignalDeciderDecisionSnapshot)
                    .where(
                        SignalDeciderDecisionSnapshot.market_date
                        == candidate.source_market_date,
                        SignalDeciderDecisionSnapshot.symbol
                        == candidate.symbol,
                        SignalDeciderDecisionSnapshot.agent_name
                        == candidate.agent_name,
                    )
                    .order_by(
                        SignalDeciderDecisionSnapshot.created_at.desc(),
                        SignalDeciderDecisionSnapshot.id.desc(),
                    )
                ).scalars().all()
                unaccepted_decision_run_ids = (
                    _unaccepted_decision_run_ids(lane_events)
                )
                latest_snapshot = next(
                    (
                        snapshot
                        for snapshot in candidate_snapshots
                        if str(snapshot.decision_run_id or '').strip()
                        not in unaccepted_decision_run_ids
                    ),
                    None,
                )
                if latest_snapshot is None:
                    return None
                latest_candidate = build_wait_recheck_candidate(
                    latest_snapshot,
                    target_trade_date=candidate.target_trade_date,
                )
                if latest_candidate is None or not all(
                    getattr(latest_candidate, attribute)
                    == getattr(candidate, attribute)
                    for attribute in (
                        'source_snapshot_id',
                        'source_snapshot_fingerprint',
                        'source_market_date',
                        'target_trade_date',
                        'symbol',
                        'agent_name',
                        'trigger_price',
                        'chase_limit_price',
                        'target_price',
                        'stop_loss_price',
                    )
                ):
                    return None
                if existing_event is not None:
                    if candidate.retry_event_id is None:
                        return (
                            int(existing_event.id)
                            if existing_event.status == 'queued'
                            else None
                        )
                    retry_not_before = _event_retry_not_before(
                        existing_event
                    )
                    if not (
                        int(existing_event.id) == candidate.retry_event_id
                        and retry_not_before is not None
                        and observation.received_at >= retry_not_before
                        and int(existing_event.generation)
                        == candidate.generation
                        and existing_event.rearmed_from_event_id
                        == candidate.rearmed_from_event_id
                    ):
                        return None
                    parent_event = (
                        session.get(
                            SignalDeciderWaitRecheckEvent,
                            existing_event.rearmed_from_event_id,
                        )
                        if existing_event.rearmed_from_event_id is not None
                        else None
                    )
                    if not validate_wait_recheck_event_binding(
                        event=existing_event,
                        context=_json_object(
                            existing_event.context_payload
                        ),
                        source_snapshot=latest_snapshot,
                        parent_event=parent_event,
                    ):
                        return None
                    history = existing_event.attempt_history_payload
                    if not isinstance(history, list):
                        return None
                    archived_at = _utcnow()
                    existing_event.attempt_history_payload = [
                        *history,
                        _attempt_history_entry(
                            existing_event,
                            archived_at=archived_at,
                        ),
                    ]
                    existing_event.observed_price = (
                        observation.observed_price
                    )
                    existing_event.observed_current_price = (
                        observation.current_price
                    )
                    existing_event.observed_bid_price = observation.bid_price
                    existing_event.price_basis = observation.price_basis
                    existing_event.quote_source = observation.quote_source
                    existing_event.price_as_of = observation.price_as_of
                    existing_event.observed_at = observation.received_at
                    existing_event.status = 'queued'
                    existing_event.request_run_id = None
                    existing_event.result_snapshot_id = None
                    existing_event.result_action = None
                    existing_event.last_error_payload = None
                    existing_event.last_attempt_at = None
                    existing_event.completed_at = None
                    context, context_sha256 = canonical_wait_recheck_context(
                        event_id=existing_event.id,
                        observation=observation,
                        market_state=market_state,
                        market_server_time=market_server_time,
                    )
                    existing_event.context_payload = context
                    existing_event.context_sha256 = context_sha256
                    if not validate_wait_recheck_event_binding(
                        event=existing_event,
                        context=context,
                        source_snapshot=latest_snapshot,
                        parent_event=parent_event,
                    ):
                        # The row was mutated in this transaction. Raising
                        # ensures session_scope rolls the entire refresh back,
                        # including the append-only archive entry.
                        raise ValueError(
                            'refreshed WAIT recheck binding is invalid'
                        )
                    return int(existing_event.id)

                daily_event_ids = session.execute(
                    select(SignalDeciderWaitRecheckEvent.id).where(
                        SignalDeciderWaitRecheckEvent.target_trade_date
                        == candidate.target_trade_date
                    )
                ).scalars().all()
                if (
                    len(daily_event_ids)
                    >= settings.signal_decider_wait_recheck_max_daily_events
                ):
                    return None
                if candidate.generation == 1:
                    if candidate.rearmed_from_event_id is not None or lane_events:
                        return None
                else:
                    if not (
                        candidate.generation
                        <= settings.signal_decider_wait_recheck_max_generations_per_day
                        and candidate.rearmed_from_event_id is not None
                        and lane_events
                    ):
                        return None
                    prior_event = lane_events[-1]
                    completed_at = _aware_utc(prior_event.completed_at)
                    prior_observed_at = _aware_utc(prior_event.observed_at)
                    if not (
                        int(prior_event.id)
                        == candidate.rearmed_from_event_id
                        and int(prior_event.generation) + 1
                        == candidate.generation
                        and prior_event.status == 'completed'
                        and str(
                            prior_event.result_action or ''
                        ).strip().lower()
                        == 'wait'
                        and prior_event.result_snapshot_id is not None
                        and int(prior_event.result_snapshot_id)
                        == candidate.source_snapshot_id
                        and completed_at is not None
                        and observation.received_at
                        >= completed_at
                        + timedelta(
                            seconds=settings.signal_decider_wait_recheck_rearm_cooldown_seconds
                        )
                        and int(prior_event.observed_price)
                        == candidate.prior_observed_price
                        and prior_observed_at
                        == candidate.prior_observed_at
                        and bool(observation.material_change_reasons)
                    ):
                        return None
                event = SignalDeciderWaitRecheckEvent(
                    generation=candidate.generation,
                    rearmed_from_event_id=(
                        candidate.rearmed_from_event_id
                    ),
                    source_snapshot_id=candidate.source_snapshot_id,
                    source_snapshot_fingerprint=(
                        candidate.source_snapshot_fingerprint
                    ),
                    source_market_date=candidate.source_market_date,
                    target_trade_date=candidate.target_trade_date,
                    symbol=candidate.symbol,
                    agent_name=candidate.agent_name,
                    trigger_price=candidate.trigger_price,
                    chase_limit_price=candidate.chase_limit_price,
                    target_price=candidate.target_price,
                    stop_loss_price=candidate.stop_loss_price,
                    observed_price=observation.observed_price,
                    observed_current_price=observation.current_price,
                    observed_bid_price=observation.bid_price,
                    price_basis=observation.price_basis,
                    quote_source=observation.quote_source,
                    price_as_of=observation.price_as_of,
                    observed_at=observation.received_at,
                    status='queued',
                    attempt_count=0,
                    context_payload={},
                    context_sha256='pending',
                )
                session.add(event)
                session.flush()
                context, context_sha256 = canonical_wait_recheck_context(
                    event_id=event.id,
                    observation=observation,
                    market_state=market_state,
                    market_server_time=market_server_time,
                )
                event.context_payload = context
                event.context_sha256 = context_sha256
                if not validate_wait_recheck_event_binding(
                    event=event,
                    context=context,
                    source_snapshot=latest_snapshot,
                    parent_event=(
                        prior_event
                        if candidate.generation > 1
                        else None
                    ),
                ):
                    session.delete(event)
                    return None
                return int(event.id)
        except IntegrityError:
            with session_scope() as session:
                existing_id = session.execute(
                    select(SignalDeciderWaitRecheckEvent.id).where(
                        SignalDeciderWaitRecheckEvent.target_trade_date
                        == candidate.target_trade_date,
                        SignalDeciderWaitRecheckEvent.symbol
                        == candidate.symbol,
                        SignalDeciderWaitRecheckEvent.agent_name
                        == candidate.agent_name,
                        SignalDeciderWaitRecheckEvent.source_snapshot_id
                        == candidate.source_snapshot_id,
                    )
                ).scalar_one_or_none()
                return int(existing_id) if existing_id is not None else None

    def _retry_event_ids(
        self,
        *,
        target_trade_date: date,
        limit: int,
        now: datetime,
    ) -> list[int]:
        if limit <= 0:
            return []
        del now
        with session_scope() as session:
            return [
                int(value)
                for value in session.execute(
                    select(SignalDeciderWaitRecheckEvent.id)
                    .where(
                        SignalDeciderWaitRecheckEvent.target_trade_date
                        == target_trade_date,
                        SignalDeciderWaitRecheckEvent.attempt_count
                        < settings.signal_decider_wait_recheck_max_attempts,
                        SignalDeciderWaitRecheckEvent.status == 'queued',
                    )
                    .order_by(
                        SignalDeciderWaitRecheckEvent.observed_at.asc(),
                        SignalDeciderWaitRecheckEvent.id.asc(),
                    )
                    .limit(limit)
                ).scalars()
            ]

    @staticmethod
    def _event_observation_is_fresh(
        event: SignalDeciderWaitRecheckEvent,
        *,
        now: datetime,
    ) -> bool:
        observed_at = _aware_utc(event.observed_at)
        price_as_of = _aware_utc(event.price_as_of)
        normalized_now = _aware_utc(now)
        if (
            observed_at is None
            or price_as_of is None
            or normalized_now is None
        ):
            return False
        stale_after_seconds = float(
            settings.kis_realtime_price_stale_after_seconds
        )
        for observed_time in (observed_at, price_as_of):
            seoul_observed = observed_time.astimezone(SEOUL_TZ)
            age_seconds = (normalized_now - observed_time).total_seconds()
            if (
                age_seconds < -5
                or age_seconds > stale_after_seconds
                or seoul_observed.date() != event.target_trade_date
                or not (
                    KRX_REGULAR_SESSION_OPEN
                    <= seoul_observed.time().replace(tzinfo=None)
                    <= KRX_REGULAR_SESSION_CLOSE
                )
            ):
                return False
        return True

    def _begin_attempt(
        self,
        event_id: int,
        *,
        now: datetime,
        expected_cache_epoch: int | None = None,
    ) -> dict[str, Any] | None:
        attempt_cache_epoch = (
            self.candidate_cache_epoch()
            if expected_cache_epoch is None
            else expected_cache_epoch
        )
        if self.candidate_cache_epoch() != attempt_cache_epoch:
            return None
        attempt_payload: dict[str, Any] | None = None
        refresh_target_trade_date: date | None = None
        with session_scope() as session:
            event = session.execute(
                select(SignalDeciderWaitRecheckEvent)
                .where(SignalDeciderWaitRecheckEvent.id == event_id)
                .with_for_update()
            ).scalar_one_or_none()
            if event is None:
                return None
            if (
                event.attempt_count
                >= settings.signal_decider_wait_recheck_max_attempts
            ):
                return None
            if event.status != 'queued':
                return None
            context = _json_object(event.context_payload)
            if not validate_wait_recheck_context(
                context,
                expected_sha256=event.context_sha256,
            ):
                # Durable evidence corruption is not retryable. Keeping this
                # row queued would consume the oldest-first budget forever.
                event.status = 'expired'
                event.last_attempt_at = now
                event.last_error_payload = {
                    'code': 'wait_recheck_context_invalid',
                }
                refresh_target_trade_date = event.target_trade_date
            else:
                source_snapshot = session.get(
                    SignalDeciderDecisionSnapshot,
                    event.source_snapshot_id,
                )
                parent_event = (
                    session.get(
                        SignalDeciderWaitRecheckEvent,
                        event.rearmed_from_event_id,
                    )
                    if event.rearmed_from_event_id is not None
                    else None
                )
                event_agent_name = str(
                    event.agent_name or ''
                ).strip().lower()
                if not validate_wait_recheck_event_binding(
                    event=event,
                    context=context,
                    source_snapshot=source_snapshot,
                    parent_event=parent_event,
                ):
                    event.status = 'expired'
                    event.last_attempt_at = now
                    event.last_error_payload = {
                        'code': 'wait_recheck_context_lineage_mismatch',
                    }
                    refresh_target_trade_date = event.target_trade_date
                elif not self._event_observation_is_fresh(event, now=now):
                    event.status = 'expired'
                    event.last_attempt_at = now
                    event.last_error_payload = {
                        'code': 'wait_recheck_observation_stale',
                    }
                    refresh_target_trade_date = event.target_trade_date
                else:
                    request_payload = _json_object(
                        source_snapshot.request_payload
                    )
                    revalidation = request_payload.get(
                        'source_revalidation_request'
                    )
                    revalidation = (
                        revalidation
                        if isinstance(revalidation, dict)
                        else {}
                    )
                    attempt_number = int(event.attempt_count) + 1
                    request_run_id = (
                        f'wait-recheck-'
                        f'{event.target_trade_date.isoformat()}-'
                        f'{event_agent_name}-{event.symbol}-{event.id}-'
                        f'{attempt_number}'
                    )
                    event.status = 'running'
                    event.attempt_count = attempt_number
                    event.request_run_id = request_run_id
                    event.last_attempt_at = now
                    event.last_error_payload = None
                    attempt_payload = {
                        'event_id': int(event.id),
                        'symbol': event.symbol,
                        'agent_name': event_agent_name,
                        'source_market_date': event.source_market_date,
                        'target_trade_date': event.target_trade_date,
                        'source_snapshot_id': int(
                            event.source_snapshot_id
                        ),
                        'source_decision_run_id': (
                            source_snapshot.decision_run_id
                        ),
                        'request_run_id': request_run_id,
                        'context': context,
                        'fs_div': revalidation.get('fs_div'),
                        'disclosures_limit': int(
                            revalidation.get('disclosures_limit') or 5
                        ),
                        'knowledge_cutoff_at': revalidation.get(
                            'knowledge_cutoff_at'
                        ),
                        'upstream_summary_run_id': revalidation.get(
                            'upstream_summary_run_id'
                        ),
                    }
        if refresh_target_trade_date is not None:
            # Refresh only after the expiry transaction commits. This restores
            # a fresh-evidence subscription immediately instead of waiting for
            # the independent membership poll.
            self._refresh_candidate_cache_during_target_session(
                target_trade_date=refresh_target_trade_date,
                expected_cache_epoch=attempt_cache_epoch,
            )
        return attempt_payload

    def _finish_event(
        self,
        *,
        event_id: int,
        request_run_id: str,
        status: str,
        expected_cache_epoch: int,
        result_snapshot_id: int | None = None,
        result_action: str | None = None,
        error_code: str | None = None,
    ) -> None:
        target_trade_date: date | None = None
        finished_at: datetime | None = None
        with session_scope() as session:
            event = session.execute(
                select(SignalDeciderWaitRecheckEvent)
                .where(
                    SignalDeciderWaitRecheckEvent.id == event_id,
                    SignalDeciderWaitRecheckEvent.status == 'running',
                    SignalDeciderWaitRecheckEvent.request_run_id
                    == request_run_id,
                )
                .with_for_update()
            ).scalar_one_or_none()
            if event is None:
                return
            finished_at = _utcnow()
            target_trade_date = event.target_trade_date
            event.status = status
            event.result_snapshot_id = result_snapshot_id
            event.result_action = result_action
            event.last_error_payload = (
                {'code': error_code} if error_code else None
            )
            event.completed_at = finished_at if status == 'completed' else None
            if status == 'failed':
                # Provider backoff starts when the failed call actually ends.
                # A slow dispatch must not consume its cooldown while it is
                # still occupying provider capacity.
                event.last_attempt_at = finished_at
            elif (
                status == 'expired'
                and error_code == 'wait_recheck_market_session_revoked'
            ):
                # _begin_attempt reserves one provider attempt before the
                # linearized admission boundary.  If close/shutdown wins that
                # boundary the provider never started, so restore the budget;
                # a later fresh-evidence rearm may reuse the durable row.
                event.attempt_count = max(int(event.attempt_count) - 1, 0)
                # The reserved run id was never presented to the provider.
                # Clearing it keeps the later history archive from poisoning
                # the same deterministic id when the restored attempt number
                # is legitimately reused after fresh evidence arrives.
                event.request_run_id = None
        if status in {'completed', 'failed'} and target_trade_date is not None:
            # The terminal result is now durable. Refreshing immediately keeps
            # completed-WAIT lineage and retryable provider failures available
            # through their bounded re-arm cooldowns without waiting for the
            # membership poll.
            self._refresh_candidate_cache_during_target_session(
                target_trade_date=target_trade_date,
                expected_cache_epoch=expected_cache_epoch,
            )

    @staticmethod
    def _expire_queued_event_after_session_revocation(
        event_id: int,
        *,
        now: datetime,
    ) -> None:
        with session_scope() as session:
            event = session.execute(
                select(SignalDeciderWaitRecheckEvent)
                .where(
                    SignalDeciderWaitRecheckEvent.id == event_id,
                    SignalDeciderWaitRecheckEvent.status == 'queued',
                )
                .with_for_update()
            ).scalar_one_or_none()
            if event is None:
                return
            event.status = 'expired'
            event.last_attempt_at = now
            event.last_error_payload = {
                'code': 'wait_recheck_market_session_revoked',
            }

    def _dispatch_event(
        self,
        event_id: int,
        *,
        now: datetime,
        expected_cache_epoch: int | None = None,
    ) -> dict[str, Any]:
        # A market-close or shutdown clear must revoke this dispatch's later
        # terminal refresh, even if it happens after this open cycle starts but
        # before the blocking provider is entered.
        dispatch_cache_epoch = (
            self.candidate_cache_epoch()
            if expected_cache_epoch is None
            else expected_cache_epoch
        )
        if self.candidate_cache_epoch() != dispatch_cache_epoch:
            self._expire_queued_event_after_session_revocation(
                event_id,
                now=now,
            )
            return {'event_id': event_id, 'status': 'not_dispatched'}
        attempt = self._begin_attempt(
            event_id,
            now=now,
            expected_cache_epoch=dispatch_cache_epoch,
        )
        if attempt is None:
            return {'event_id': event_id, 'status': 'not_dispatched'}
        from app.agent_context.orchestrator import agent_feedback_orchestrator

        provider_entry_lock_held = True
        self._dispatch_entry_lock.acquire()

        def release_dispatch_entry_lock() -> None:
            nonlocal provider_entry_lock_held
            if not provider_entry_lock_held:
                return
            provider_entry_lock_held = False
            self._dispatch_entry_lock.release()

        try:
            if self.candidate_cache_epoch() != dispatch_cache_epoch:
                # _begin_attempt durably owns the event before the provider
                # call.  The shared admission lock makes this final epoch read
                # and the orchestrator's actual entry one linearized step.
                # Release it before the DB-backed durable expiry so a slow row
                # lock cannot block the event-loop shutdown/inactive clear.
                release_dispatch_entry_lock()
                self._finish_event(
                    event_id=event_id,
                    request_run_id=attempt['request_run_id'],
                    status='expired',
                    error_code='wait_recheck_market_session_revoked',
                    expected_cache_epoch=dispatch_cache_epoch,
                )
                return {'event_id': event_id, 'status': 'not_dispatched'}
            result = agent_feedback_orchestrator.dispatch_for_symbols(
                market_date=attempt['source_market_date'],
                symbols=[attempt['symbol']],
                roles=(attempt['agent_name'],),
                fs_div=attempt['fs_div'],
                disclosures_limit=attempt['disclosures_limit'],
                knowledge_cutoff_at=attempt['knowledge_cutoff_at'],
                policy_context_as_of_date=attempt['target_trade_date'],
                reuse_existing=True,
                request_run_id=attempt['request_run_id'],
                parent_run_id=attempt['source_decision_run_id'],
                force=False,
                force_recompute=True,
                upstream_summary_run_id=attempt['upstream_summary_run_id'],
                source_context_overlays={
                    attempt['symbol']: attempt['context']
                },
                _dispatch_entry_callback=release_dispatch_entry_lock,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                'WAIT recheck dispatch failed without order authority: event=%s error=%s',
                event_id,
                type(exc).__name__,
            )
            self._finish_event(
                event_id=event_id,
                request_run_id=attempt['request_run_id'],
                status='failed',
                error_code='decider_dispatch_exception',
                expected_cache_epoch=dispatch_cache_epoch,
            )
            return {'event_id': event_id, 'status': 'failed'}
        finally:
            # Patched providers and pre-entry exceptions may not execute the
            # callback.  Never strand close/shutdown behind the admission lock.
            if provider_entry_lock_held:
                provider_entry_lock_held = False
                self._dispatch_entry_lock.release()

        if not wait_recheck_dispatch_completed(result):
            self._finish_event(
                event_id=event_id,
                request_run_id=attempt['request_run_id'],
                status='failed',
                error_code='decider_dispatch_not_completed',
                expected_cache_epoch=dispatch_cache_epoch,
            )
            return {'event_id': event_id, 'status': 'failed'}

        with session_scope() as session:
            result_snapshot = session.execute(
                select(SignalDeciderDecisionSnapshot)
                .where(
                    SignalDeciderDecisionSnapshot.decision_run_id
                    == attempt['request_run_id'],
                    SignalDeciderDecisionSnapshot.symbol == attempt['symbol'],
                    SignalDeciderDecisionSnapshot.agent_name
                    == attempt['agent_name'],
                )
                .order_by(
                    SignalDeciderDecisionSnapshot.created_at.desc(),
                    SignalDeciderDecisionSnapshot.id.desc(),
                )
                .limit(1)
            ).scalar_one_or_none()
            result_snapshot_id = (
                int(result_snapshot.id)
                if result_snapshot is not None
                else None
            )
            response_payload = (
                _json_object(result_snapshot.response_payload)
                if result_snapshot is not None
                else {}
            )
            result_action = str(
                (
                    response_payload.get('tactical_action')
                    if attempt['agent_name']
                    == SHORT_TERM_DECIDER_AGENT_NAME
                    else response_payload.get('next_day_open_action')
                )
                or response_payload.get('preferred_action')
                or ''
            ).strip().lower() or None
        if result_snapshot_id is None:
            summary = result.get('summary') if isinstance(result, dict) else {}
            self._finish_event(
                event_id=event_id,
                request_run_id=attempt['request_run_id'],
                status='failed',
                error_code=str(
                    (summary or {}).get('status')
                    or 'decider_result_snapshot_missing'
                ),
                expected_cache_epoch=dispatch_cache_epoch,
            )
            return {'event_id': event_id, 'status': 'failed'}

        self._finish_event(
            event_id=event_id,
            request_run_id=attempt['request_run_id'],
            status='completed',
            result_snapshot_id=result_snapshot_id,
            result_action=result_action,
            expected_cache_epoch=dispatch_cache_epoch,
        )
        return {
            'event_id': event_id,
            'status': 'completed',
            'result_snapshot_id': result_snapshot_id,
            'result_action': result_action,
            'direct_order_authority': False,
        }

    def run_open_cycle(
        self,
        *,
        market: Any,
        expected_cache_epoch: int | None = None,
    ) -> dict[str, Any]:
        cycle_cache_epoch = (
            self.candidate_cache_epoch()
            if expected_cache_epoch is None
            else expected_cache_epoch
        )
        if self.candidate_cache_epoch() != cycle_cache_epoch:
            return {
                'status': 'not_due',
                'direct_order_authority': False,
            }
        now = _utcnow()
        market_state = str(getattr(market, 'state', '') or '').strip().upper()
        market_name = str(getattr(market, 'market', '') or '').strip().upper()
        server_time = str(getattr(market, 'server_time', '') or '').strip()
        market_snapshot_time = _aware_utc(server_time)
        session_open_at = _aware_utc(
            getattr(market, 'session_open_at', None)
        )
        session_close_at = _aware_utc(
            getattr(market, 'session_close_at', None)
        )
        target_trade_date = (
            market_snapshot_time.astimezone(SEOUL_TZ).date()
            if market_snapshot_time is not None
            else None
        )
        if not (
            settings.signal_decider_wait_recheck_monitoring_enabled
            and market_name == 'KRX'
            and market_state == 'OPEN'
            and bool(getattr(market, 'is_trading_day', False))
            and bool(getattr(market, 'is_open', False))
            and getattr(market, 'calendar_source', None)
            == CALENDAR_SOURCE_KIS_CHK_HOLIDAY
            and getattr(market, 'calendar_source_status', None)
            == CALENDAR_SOURCE_STATUS_OK
            and target_trade_date is not None
            and session_open_at is not None
            and session_close_at is not None
            and session_open_at
            <= market_snapshot_time
            <= session_close_at
            and now.astimezone(SEOUL_TZ).date() == target_trade_date
            and session_open_at <= now <= session_close_at
        ):
            self.clear_candidate_cache()
            return {
                'status': 'not_due',
                'direct_order_authority': False,
            }

        limit = settings.signal_decider_wait_recheck_max_events_per_cycle
        results: list[dict[str, Any]] = []
        processed_event_ids: set[int] = set()
        inspection_budget = max(
            limit,
            settings.signal_decider_wait_recheck_max_symbols
            * len(WAIT_RECHECK_AGENT_NAMES),
        )
        inspected = 0
        while len(results) < limit and inspected < inspection_budget:
            inspected += 1
            dispatch_now = _utcnow()
            if dispatch_now > session_close_at:
                break
            pending = self.pop_pending_observations(limit=1)
            event_id: int | None = None
            claimed_observation: WaitRecheckObservation | None = None
            if pending:
                observation = pending[0]
                if not self._observation_is_dispatchable(
                    observation,
                    # Pop only immediately before dispatch. Other candidates
                    # stay replaceable by newer ticks while the single-flight
                    # provider handles this one.
                    market_now=dispatch_now,
                    session_open_at=session_open_at,
                ):
                    self._release_observation(observation)
                    continue
                try:
                    event_id = self._claim_observation(
                        observation=observation,
                        market_state=market_state,
                        market_server_time=(
                            dispatch_now.astimezone(SEOUL_TZ).isoformat()
                        ),
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        'WAIT recheck observation claim failed closed: %s',
                        type(exc).__name__,
                    )
                    self._release_observation(observation)
                    continue
                if event_id is None:
                    self._release_observation(observation)
                    continue
                claimed_observation = observation
            else:
                retry_ids = self._retry_event_ids(
                    target_trade_date=target_trade_date,
                    limit=limit,
                    now=dispatch_now,
                )
                event_id = next(
                    (
                        retry_id
                        for retry_id in retry_ids
                        if retry_id not in processed_event_ids
                    ),
                    None,
                )
                if event_id is None:
                    break
            if self.candidate_cache_epoch() != cycle_cache_epoch:
                self._expire_queued_event_after_session_revocation(
                    event_id,
                    now=dispatch_now,
                )
                if claimed_observation is not None:
                    self._release_observation(claimed_observation)
                break
            processed_event_ids.add(event_id)
            try:
                results.append(
                    self._dispatch_event(
                        event_id,
                        now=dispatch_now,
                        expected_cache_epoch=cycle_cache_epoch,
                    )
                )
            finally:
                if claimed_observation is not None:
                    # Keep the key reserved while the provider call is in
                    # flight, then release it so a failed/stale attempt can
                    # collect a wholly fresh window for its bounded retry.
                    self._release_observation(claimed_observation)
        with self._lock:
            pending_follow_up = bool(self._pending_by_key)
        cycle_result = {
            'status': 'completed' if results else 'idle',
            'target_trade_date': target_trade_date.isoformat(),
            'events_processed': len(results),
            'results': results,
            # The runner consumes this only after the single-flight task is
            # fully done. That prevents a completion wake from being consumed
            # while the task still occupies its slot, and avoids idle spin.
            'follow_up_required': bool(results or pending_follow_up),
            'direct_order_authority': False,
        }
        with self._lock:
            self._last_cycle_at = now
            self._last_cycle_result = cycle_result
        return cycle_result


def load_validated_wait_recheck_context(
    *,
    event_id: int,
    symbol: str,
    agent_name: str | None = None,
) -> dict[str, Any] | None:
    with session_scope() as session:
        event = session.get(SignalDeciderWaitRecheckEvent, int(event_id))
        if event is None or event.status not in WAIT_RECHECK_ALLOWED_EVENT_STATUSES:
            return None
        context = _json_object(event.context_payload)
        expected_agent_name = str(agent_name or '').strip().lower() or None
        event_agent_name = str(event.agent_name or '').strip().lower()
        source_snapshot = session.get(
            SignalDeciderDecisionSnapshot,
            event.source_snapshot_id,
        )
        parent_event = (
            session.get(
                SignalDeciderWaitRecheckEvent,
                event.rearmed_from_event_id,
            )
            if event.rearmed_from_event_id is not None
            else None
        )
        if (
            event.symbol != str(symbol or '').strip().upper()
            or event_agent_name not in WAIT_RECHECK_AGENT_NAMES
            or (
                expected_agent_name is not None
                and expected_agent_name != event_agent_name
            )
            or not validate_wait_recheck_event_binding(
                event=event,
                context=context,
                source_snapshot=source_snapshot,
                parent_event=parent_event,
            )
        ):
            return None
        return context


signal_decider_wait_recheck_service = SignalDeciderWaitRecheckService()
