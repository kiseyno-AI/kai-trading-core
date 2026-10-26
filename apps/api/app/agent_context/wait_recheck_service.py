from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import logging
import threading
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import and_, or_, select
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
from app.market.session import previous_krx_trading_date
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
WAIT_RECHECK_CONTEXT_SCHEMA_VERSION = 'intraday_wait_recheck_context_v2'
WAIT_RECHECK_QUOTE_SOURCE = 'kis_realtime_ws'
WAIT_RECHECK_PRICE_BASIS = 'best_ask'
KRX_MARKETS = frozenset({'KRX', 'KOSPI', 'KOSDAQ', 'KONEX'})
WAIT_RECHECK_ALLOWED_EVENT_STATUSES = frozenset(
    {'queued', 'running', 'completed', 'failed'}
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


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


@dataclass(frozen=True)
class WaitRecheckSubscriptionSource:
    symbols: tuple[str, ...]
    source_version: datetime | None
    membership_complete: bool
    candidates: tuple[WaitRecheckCandidate, ...] = ()


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
    execution_authority = context.get('execution_authority')
    observation = context.get('observation')
    trigger_contract = context.get('trigger_contract')
    agent_name = str(context.get('agent_name') or '').strip().lower()
    if not (
        context.get('schema_version') == WAIT_RECHECK_CONTEXT_SCHEMA_VERSION
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
        self._candidates_by_key: dict[
            tuple[str, str], WaitRecheckCandidate
        ] = {}
        self._pending_by_key: dict[
            tuple[date, str, str], WaitRecheckObservation
        ] = {}
        self._reserved_keys: set[tuple[date, str, str]] = set()
        self._last_source_refresh_at: datetime | None = None
        self._last_cycle_at: datetime | None = None
        self._last_cycle_result: dict[str, Any] | None = None

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

    def replace_candidate_cache(
        self,
        candidates: Iterable[WaitRecheckCandidate],
    ) -> None:
        normalized = {
            (candidate.symbol, candidate.agent_name): candidate
            for candidate in candidates
        }
        active_keys = {
            (
                candidate.target_trade_date,
                candidate.symbol,
                candidate.agent_name,
            )
            for candidate in normalized.values()
        }
        with self._lock:
            self._candidates_by_key = normalized
            self._pending_by_key = {
                key: observation
                for key, observation in self._pending_by_key.items()
                if key in active_keys
            }
            self._reserved_keys.intersection_update(active_keys)
            self._last_source_refresh_at = _utcnow()

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
        if (
            current is None
            or received_at is None
            or price_as_of is None
            or not subscribed
            or limited
            or tick.trade_halt is not False
        ):
            return False
        age_seconds = (current - received_at).total_seconds()
        if (
            age_seconds < -5
            or age_seconds
            > float(settings.kis_realtime_price_stale_after_seconds)
        ):
            return False
        observed_price = _positive_int(tick.ask_price)
        if observed_price is None:
            return False
        symbol = str(tick.symbol or '').strip().upper()
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
            queued_new = False
            for candidate in candidates:
                key = (
                    candidate.target_trade_date,
                    symbol,
                    candidate.agent_name,
                )
                pending_observation = self._pending_by_key.get(key)
                if key in self._reserved_keys and pending_observation is None:
                    continue
                if (
                    received_at.astimezone(SEOUL_TZ).date()
                    != candidate.target_trade_date
                    or observed_price < candidate.trigger_price
                    or observed_price >= candidate.target_price
                    or (
                        candidate.chase_limit_price is not None
                        and observed_price > candidate.chase_limit_price
                    )
                    or (
                        pending_observation is not None
                        and received_at < pending_observation.received_at
                    )
                ):
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
                )
                self._pending_by_key[key] = observation
                self._reserved_keys.add(key)
                queued_new = queued_new or pending_observation is None
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
        )
        with self._lock:
            self._reserved_keys.discard(key)

    @staticmethod
    def _observation_is_dispatchable(
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
        return bool(
            received_at.astimezone(SEOUL_TZ).date()
            == candidate.target_trade_date
            and candidate.trigger_price <= observation.observed_price
            < candidate.target_price
            and (
                candidate.chase_limit_price is None
                or observation.observed_price <= candidate.chase_limit_price
            )
        )

    def load_subscription_source(
        self,
        *,
        target_trade_date: date,
    ) -> WaitRecheckSubscriptionSource:
        if not settings.signal_decider_wait_recheck_monitoring_enabled:
            self.replace_candidate_cache(())
            return WaitRecheckSubscriptionSource(
                symbols=(),
                source_version=_utcnow(),
                membership_complete=True,
            )
        source_market_date = previous_krx_trading_date(
            target_trade_date,
            allow_fallback=False,
        )
        if source_market_date is None:
            self.replace_candidate_cache(())
            return WaitRecheckSubscriptionSource(
                symbols=(),
                source_version=None,
                membership_complete=False,
            )
        try:
            with session_scope() as session:
                claimed_keys = {
                    (str(symbol), str(agent_name))
                    for symbol, agent_name in session.execute(
                        select(
                            SignalDeciderWaitRecheckEvent.symbol,
                            SignalDeciderWaitRecheckEvent.agent_name,
                        ).where(
                            SignalDeciderWaitRecheckEvent.target_trade_date
                            == target_trade_date
                        )
                    ).all()
                }
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
                    latest_by_key.setdefault(
                        (snapshot.symbol, snapshot.agent_name),
                        snapshot,
                    )
                eligible_candidates = [
                    candidate
                    for key in sorted(latest_by_key)
                    if key not in claimed_keys
                    and (
                        candidate := build_wait_recheck_candidate(
                            latest_by_key[key],
                            target_trade_date=target_trade_date,
                        )
                    )
                    is not None
                ]
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
            self.replace_candidate_cache(())
            return WaitRecheckSubscriptionSource(
                symbols=(),
                source_version=None,
                membership_complete=False,
            )

        self.replace_candidate_cache(candidates)
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
                existing_id = session.execute(
                    select(SignalDeciderWaitRecheckEvent.id).where(
                        SignalDeciderWaitRecheckEvent.target_trade_date
                        == candidate.target_trade_date,
                        SignalDeciderWaitRecheckEvent.symbol
                        == candidate.symbol,
                        SignalDeciderWaitRecheckEvent.agent_name
                        == candidate.agent_name,
                    )
                ).scalar_one_or_none()
                if existing_id is not None:
                    return int(existing_id)
                latest_snapshot = session.execute(
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
                    .limit(1)
                ).scalar_one_or_none()
                if latest_snapshot is None:
                    return None
                latest_candidate = build_wait_recheck_candidate(
                    latest_snapshot,
                    target_trade_date=candidate.target_trade_date,
                )
                if latest_candidate != candidate:
                    return None
                event = SignalDeciderWaitRecheckEvent(
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
        retry_cutoff = now - timedelta(
            seconds=settings.signal_decider_wait_recheck_retry_cooldown_seconds
        )
        running_cutoff = now - timedelta(
            seconds=settings.signal_decider_wait_recheck_running_timeout_seconds
        )
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
                        or_(
                            SignalDeciderWaitRecheckEvent.status == 'queued',
                            and_(
                                SignalDeciderWaitRecheckEvent.status == 'failed',
                                SignalDeciderWaitRecheckEvent.updated_at
                                <= retry_cutoff,
                            ),
                            and_(
                                SignalDeciderWaitRecheckEvent.status == 'running',
                                SignalDeciderWaitRecheckEvent.updated_at
                                <= running_cutoff,
                            ),
                        ),
                    )
                    .order_by(
                        SignalDeciderWaitRecheckEvent.observed_at.asc(),
                        SignalDeciderWaitRecheckEvent.id.asc(),
                    )
                    .limit(limit)
                ).scalars()
            ]

    def _begin_attempt(self, event_id: int, *, now: datetime) -> dict[str, Any] | None:
        retry_cutoff = now - timedelta(
            seconds=settings.signal_decider_wait_recheck_retry_cooldown_seconds
        )
        running_cutoff = now - timedelta(
            seconds=settings.signal_decider_wait_recheck_running_timeout_seconds
        )
        with session_scope() as session:
            event = session.execute(
                select(SignalDeciderWaitRecheckEvent)
                .where(SignalDeciderWaitRecheckEvent.id == event_id)
                .with_for_update()
            ).scalar_one_or_none()
            if event is None:
                return None
            if event.attempt_count >= settings.signal_decider_wait_recheck_max_attempts:
                return None
            attempt_anchor = _aware_utc(
                event.last_attempt_at or event.updated_at
            )
            if event.status == 'failed':
                if attempt_anchor is None or attempt_anchor > retry_cutoff:
                    return None
            elif event.status == 'running':
                if attempt_anchor is None or attempt_anchor > running_cutoff:
                    return None
            elif event.status != 'queued':
                return None
            context = _json_object(event.context_payload)
            if not validate_wait_recheck_context(
                context,
                expected_sha256=event.context_sha256,
            ):
                # Durable evidence corruption is not retryable.  Keeping this
                # row in the failed queue would let it consume the oldest-first
                # retry budget forever and starve valid observations.
                event.status = 'expired'
                event.last_attempt_at = now
                event.last_error_payload = {
                    'code': 'wait_recheck_context_invalid',
                }
                return None
            source_snapshot = session.get(
                SignalDeciderDecisionSnapshot,
                event.source_snapshot_id,
            )
            event_agent_name = str(
                event.agent_name or ''
            ).strip().lower()
            if not validate_wait_recheck_event_binding(
                event=event,
                context=context,
                source_snapshot=source_snapshot,
            ):
                event.status = 'expired'
                event.last_attempt_at = now
                event.last_error_payload = {
                    'code': 'wait_recheck_context_lineage_mismatch',
                }
                return None
            request_payload = _json_object(source_snapshot.request_payload)
            revalidation = request_payload.get('source_revalidation_request')
            revalidation = revalidation if isinstance(revalidation, dict) else {}
            attempt_number = int(event.attempt_count) + 1
            request_run_id = (
                f'wait-recheck-{event.target_trade_date.isoformat()}-'
                f'{event_agent_name}-{event.symbol}-{event.id}-'
                f'{attempt_number}'
            )
            event.status = 'running'
            event.attempt_count = attempt_number
            event.request_run_id = request_run_id
            event.last_attempt_at = now
            event.last_error_payload = None
            return {
                'event_id': int(event.id),
                'symbol': event.symbol,
                'agent_name': event_agent_name,
                'source_market_date': event.source_market_date,
                'target_trade_date': event.target_trade_date,
                'source_snapshot_id': int(event.source_snapshot_id),
                'source_decision_run_id': source_snapshot.decision_run_id,
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

    def _finish_event(
        self,
        *,
        event_id: int,
        request_run_id: str,
        status: str,
        result_snapshot_id: int | None = None,
        result_action: str | None = None,
        error_code: str | None = None,
    ) -> None:
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
            event.status = status
            event.result_snapshot_id = result_snapshot_id
            event.result_action = result_action
            event.last_error_payload = (
                {'code': error_code} if error_code else None
            )
            event.completed_at = _utcnow() if status == 'completed' else None

    def _dispatch_event(self, event_id: int, *, now: datetime) -> dict[str, Any]:
        attempt = self._begin_attempt(event_id, now=now)
        if attempt is None:
            return {'event_id': event_id, 'status': 'not_dispatched'}
        try:
            from app.agent_context.orchestrator import agent_feedback_orchestrator

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
            )
            return {'event_id': event_id, 'status': 'failed'}

        if not wait_recheck_dispatch_completed(result):
            self._finish_event(
                event_id=event_id,
                request_run_id=attempt['request_run_id'],
                status='failed',
                error_code='decider_dispatch_not_completed',
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
            )
            return {'event_id': event_id, 'status': 'failed'}

        self._finish_event(
            event_id=event_id,
            request_run_id=attempt['request_run_id'],
            status='completed',
            result_snapshot_id=result_snapshot_id,
            result_action=result_action,
        )
        return {
            'event_id': event_id,
            'status': 'completed',
            'result_snapshot_id': result_snapshot_id,
            'result_action': result_action,
            'direct_order_authority': False,
        }

    def run_open_cycle(self, *, market: Any) -> dict[str, Any]:
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
            and target_trade_date is not None
            and session_open_at is not None
            and session_close_at is not None
            and now.astimezone(SEOUL_TZ).date() == target_trade_date
            and session_open_at <= now <= session_close_at
        ):
            return {
                'status': 'not_due',
                'direct_order_authority': False,
            }

        limit = settings.signal_decider_wait_recheck_max_events_per_cycle
        observations = self.pop_pending_observations(limit=limit)
        event_ids: list[int] = []
        for observation in observations:
            if not self._observation_is_dispatchable(
                observation,
                # The runner may have spent several seconds refreshing other
                # evidence after capturing its MarketState.  Quote freshness
                # must therefore use the dispatch-time clock, while the
                # authoritative OPEN/session bounds remain those of the
                # market snapshot above.
                market_now=now,
                session_open_at=session_open_at,
            ):
                self._release_observation(observation)
                continue
            try:
                event_id = self._claim_observation(
                    observation=observation,
                    market_state=market_state,
                    market_server_time=now.astimezone(SEOUL_TZ).isoformat(),
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    'WAIT recheck observation claim failed closed: %s',
                    type(exc).__name__,
                )
                self._release_observation(observation)
                continue
            if event_id is not None:
                event_ids.append(event_id)
            else:
                self._release_observation(observation)

        remaining = max(limit - len(event_ids), 0)
        if remaining:
            retry_ids = self._retry_event_ids(
                target_trade_date=target_trade_date,
                limit=remaining,
                now=now,
            )
            event_ids.extend(
                event_id
                for event_id in retry_ids
                if event_id not in set(event_ids)
            )
        results = [
            self._dispatch_event(event_id, now=now)
            for event_id in event_ids[:limit]
        ]
        cycle_result = {
            'status': 'completed' if results else 'idle',
            'target_trade_date': target_trade_date.isoformat(),
            'events_processed': len(results),
            'results': results,
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
            )
        ):
            return None
        return context


signal_decider_wait_recheck_service = SignalDeciderWaitRecheckService()
