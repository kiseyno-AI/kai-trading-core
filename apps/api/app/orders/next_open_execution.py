from __future__ import annotations

import asyncio
from collections import Counter
import copy
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
import hashlib
import inspect
import json
import logging
import math
from typing import Any, Callable, Sequence

from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import defer

from app.agent_context.read_service import (
    _build_market_open_drive_context,
    _build_market_regime_context,
)
from app.agent_context.decision_input_quality import build_decision_input_quality_envelope
from app.agent_context.feedback_failure_service import agent_context_feedback_failure_service
from app.agent_context.roles import SHORT_TERM_DECIDER_AGENT_NAME, SIGNAL_DECIDER_AGENT_NAME
from app.agent_context.run_state_service import agent_context_feedback_run_state_service
from app.orders.signal_decider_alignment_gate import (
    ALIGNMENT_GATE_ACTION_BLOCK,
    ALIGNMENT_GATE_ACTION_DOWNSIZE_PROBE,
    ALIGNMENT_GATE_ACTION_REPORT_ONLY_BLOCK,
    SIGNAL_DECIDER_ALIGNMENT_NO_CONTEXT,
    SIGNAL_DECIDER_ALIGNMENT_STALE_CONTEXT,
    SIGNAL_DECIDER_ALIGNMENT_WAIT_CONFLICT_BLOCK,
    SIGNAL_DECIDER_ALIGNMENT_WAIT_CONFLICT_OVERRIDE,
    apply_alignment_quantity_enforcement,
    build_signal_decider_alignment_context,
    build_signal_decider_alignment_gate,
)
from app.agent_context.signal_decider_loop_service import signal_decider_loop_service
from app.core.settings import settings
from app.db import session_scope
from app.db_models import (
    AgentContextFeedbackRun,
    CurrentMarketSnapshotCache,
    SignalDeciderDecisionSnapshot,
    SignalDeciderExecutionPlanItem,
)
from app.eod.pykrx_enrichment_gate import (
    REQUIRED_EXEC_DECIDER_PYKRX_ENRICHMENT_CATEGORIES,
    inspect_pykrx_enrichment_preflight,
)
from app.market.clock import MarketState, async_get_market_state, get_market_state
from app.market.session import (
    CALENDAR_SOURCE_EXCHANGE_CALENDARS,
    CALENDAR_SOURCE_STATUS_FALLBACK,
    CALENDAR_SOURCE_STATUS_MISSING_DATE,
    CALENDAR_SOURCE_STATUS_UNAVAILABLE,
    async_next_krx_open_date_on_or_after,
    async_previous_krx_trading_date,
    next_krx_open_date_on_or_after,
    previous_krx_trading_date,
)
from app.market_data_store import (
    load_current_market_snapshot_context,
    load_daily_bars_for_symbol_range,
    load_daily_pykrx_enrichment_rows,
)
from app.orders.account_scope import parse_kis_account_reference
from app.orders.signal_decider_execution_outcomes import signal_decider_execution_outcome_service
from app.orders.signal_decider_performance_gates import evaluate_next_open_performance_gate
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
ELIGIBILITY_MONITOR_CANDIDATE = 'monitor_candidate'
EXECUTION_STATUS_PLANNED = 'planned'
EXECUTION_STATUS_AWAITING_TRIGGER = 'awaiting_trigger'
EXECUTION_STATUS_DRY_RUN = 'dry_run'
EXECUTION_STATUS_SUBMITTED = 'submitted'
EXECUTION_STATUS_SUBMIT_FAILED = 'submit_failed'
EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING = 'delegated_for_exec_agent_sizing'
EXECUTION_STATUS_DISPATCH_PENDING_REMOTE_ACK = 'dispatch_pending_remote_ack'
EXECUTION_STATUS_REVOCATION_PENDING = 'revocation_pending'
EXECUTION_STATUS_REVOCATION_MANUAL_REVIEW = 'revocation_manual_review'
EXECUTION_STATUS_REVOKED = 'revoked'
EXECUTION_REVISION_HISTORY_LIMIT = 5
EXECUTION_REVOCATION_LOCKED_STATUSES = frozenset(
    {
        EXECUTION_STATUS_DISPATCH_PENDING_REMOTE_ACK,
        EXECUTION_STATUS_REVOCATION_PENDING,
        EXECUTION_STATUS_REVOCATION_MANUAL_REVIEW,
    }
)
ORDER_SOURCE = 'signal_decider_next_open'
NEXT_OPEN_COST_EVIDENCE_POLICY_VERSION = 'next_open_cost_evidence_v1'
NEXT_OPEN_PREOPEN_POLICY_FINGERPRINT_VERSION = 'next_open_preopen_policy_v1'
CURRENT_LIVE_MARKET_AUTHORITY_SCHEMA_VERSION = 'current_live_market_buy_authority_v1'
KRX_NEXT_OPEN_MARKETS = frozenset({'KRX', 'KOSPI', 'KOSDAQ', 'KONEX'})
SHORT_TERM_ACTIONABLE_ACTIONS = {'scalp_buy_candidate', 'watch_for_trigger'}
SHORT_TERM_GENERIC_ACTION_BY_TACTICAL_ACTION = {
    'scalp_buy_candidate': 'buy',
    'watch_for_trigger': 'monitor_candidate',
    'avoid_chase': 'wait',
    'wait': 'wait',
    'reject': 'reject',
}
SHORT_TERM_HANDOFF_INTENT_BY_TACTICAL_ACTION = {
    'scalp_buy_candidate': 'tactical_buy',
    'watch_for_trigger': 'intraday_watch_buy',
}
SHORT_TERM_MIN_RETURN_PERCENTILE = Decimal('0.60')
SHORT_TERM_HARD_MIN_RETURN_PERCENTILE = Decimal('0.50')
SHORT_TERM_MIN_VOLUME_PERCENTILE = Decimal('0.45')
SHORT_TERM_MIN_RAW_TRADING_VALUE_KRW = Decimal('3000000000')
SHORT_TERM_MIN_REWARD_RISK_RATIO = Decimal('1.80')
SHORT_TERM_MIN_BUY_QUALITY_SCORE = Decimal('60')
SHORT_TERM_HARD_MIN_BUY_QUALITY_SCORE = Decimal('50')
SHORT_TERM_RELAXED_DECISION_INPUT_HARD_BLOCK_CODES = frozenset({'source_turnover_missing'})
SHORT_TERM_WATCH_DELEGATED_ENTRY_QUALITY_BLOCK_CODES = frozenset(
    {
        'short_term_liquidity_floor_block',
        'short_term_reward_risk_ratio_block',
    }
)
UNSPECIFIED_ROUTE_ACCOUNT_REFERENCE = 'KIS:00000000-00'
UNSPECIFIED_ROUTE_ACCOUNT_LABEL = 'UNSPECIFIED_ROUTE'
UNSPECIFIED_ROUTE_ACCOUNT_BUDGET_KRW = Decimal('0')
CALENDAR_FALLBACK_STATUSES = {
    CALENDAR_SOURCE_STATUS_FALLBACK,
    CALENDAR_SOURCE_STATUS_MISSING_DATE,
    CALENDAR_SOURCE_STATUS_UNAVAILABLE,
}
# Short-term candidates are no longer blocked when account routing is omitted.
# They still need a concrete row shape for exec routing; this synthetic rail
# is treated as a routing/budget placeholder and does not drive fixed sizing.

logger = logging.getLogger(__name__)


class _PlanRefreshRevisionConflict(RuntimeError):
    pass


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
        return value if value.is_finite() else None
    try:
        parsed = Decimal(str(value))
        return parsed if parsed.is_finite() else None
    except (InvalidOperation, ValueError):
        return None


def _decimal_to_float(value: Decimal | None) -> float | None:
    if value is None or not value.is_finite():
        return None
    try:
        rendered = float(value)
    except (OverflowError, ValueError):
        return None
    return rendered if math.isfinite(rendered) else None


def _decimal_to_text(value: Decimal | None) -> str | None:
    if value is None or not value.is_finite():
        return None
    return format(value, 'f')


def _to_json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return _decimal_to_text(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _to_json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_to_json_safe(item) for item in value]
    return value


def _snapshot_cohort_fingerprint(
    snapshots: Sequence[SignalDeciderDecisionSnapshot],
) -> str:
    material = [
        {
            column.key: getattr(snapshot, column.key, None)
            for column in SignalDeciderDecisionSnapshot.__table__.columns
        }
        for snapshot in sorted(snapshots, key=lambda item: int(item.id))
    ]
    canonical = json.dumps(
        _to_json_safe(material),
        default=str,
        ensure_ascii=True,
        sort_keys=True,
        separators=(',', ':'),
    )
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def _live_market_snapshot_fingerprint(
    snapshot_context: dict[str, Any] | None,
    *,
    load_error: bool,
) -> str:
    canonical = json.dumps(
        _to_json_safe(
            {
                'load_error': load_error,
                'snapshot_context': snapshot_context,
            }
        ),
        default=str,
        ensure_ascii=True,
        sort_keys=True,
        separators=(',', ':'),
    )
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def _load_live_market_snapshot_context() -> tuple[dict[str, Any] | None, bool]:
    try:
        return load_current_market_snapshot_context(), False
    except (SQLAlchemyError, TimeoutError):
        return None, True


def _plan_refresh_advisory_lock_key(target_trade_date: date) -> int:
    digest = hashlib.sha256(
        f'signal-decider-next-open-refresh:{target_trade_date.isoformat()}'.encode('utf-8')
    ).digest()
    return int.from_bytes(digest[:8], byteorder='big', signed=False) & ((1 << 63) - 1)


def _lock_dispatch_authority_source_tables(session: Any) -> None:
    bind = session.get_bind()
    if bind is None or bind.dialect.name != 'postgresql':
        return
    preparer = bind.dialect.identifier_preparer
    for table in (
        AgentContextFeedbackRun.__table__,
        SignalDeciderDecisionSnapshot.__table__,
    ):
        session.execute(
            text(f'LOCK TABLE {preparer.format_table(table)} IN SHARE MODE')
        )


def _execution_plan_row_authority_fingerprint(
    row: SignalDeciderExecutionPlanItem,
) -> str:
    material = _to_json_safe(
        {
            'id': row.id,
            'execution_window': row.execution_window,
            'decision_snapshot_id': row.decision_snapshot_id,
            'decision_agent_name': row.decision_agent_name,
            'broker_name': row.broker_name,
            'target_trade_date': row.target_trade_date,
            'source_market_date': row.source_market_date,
            'account_reference': row.account_reference,
            'account_label': row.account_label,
            'symbol': row.symbol,
            'policy_version': row.policy_version,
            'planned_side': row.planned_side,
            'planned_order_type': row.planned_order_type,
            'planned_time_in_force': row.planned_time_in_force,
            'allocation_budget_krw': row.allocation_budget_krw,
            'estimated_reference_price': row.estimated_reference_price,
            'planned_quantity': row.planned_quantity,
            'estimated_notional_krw': row.estimated_notional_krw,
            'client_order_id': row.client_order_id,
            'validation_payload': row.validation_payload,
            'traceability_payload': row.traceability_payload,
        }
    )
    encoded = json.dumps(
        material,
        ensure_ascii=False,
        sort_keys=True,
        separators=(',', ':'),
    ).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def _to_int(value: Any) -> int | None:
    if value is None or value == '':
        return None
    if isinstance(value, int):
        return value
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite():
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


def _json_object_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return {}
        try:
            parsed = json.loads(normalized)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _snapshot_response_payload(snapshot: SignalDeciderDecisionSnapshot) -> dict[str, Any]:
    return _json_object_payload(getattr(snapshot, 'response_payload', None))


def _snapshot_request_payload(snapshot: SignalDeciderDecisionSnapshot) -> dict[str, Any]:
    return _json_object_payload(getattr(snapshot, 'request_payload', None))


def _snapshot_market_claims(
    snapshot: SignalDeciderDecisionSnapshot,
) -> tuple[str, ...]:
    symbol_day_context = _snapshot_request_payload(snapshot).get('symbol_day_context')
    if not isinstance(symbol_day_context, dict):
        return ()
    company = (
        symbol_day_context.get('company')
        if isinstance(symbol_day_context.get('company'), dict)
        else {}
    )
    claims: list[str] = []
    for value in (symbol_day_context.get('market'), company.get('market')):
        market = _normalize_optional_text(value)
        if market:
            claims.append(market.upper())
    return tuple(claims)


def _is_krx_next_open_snapshot(snapshot: SignalDeciderDecisionSnapshot) -> bool:
    market_claims = _snapshot_market_claims(snapshot)
    if not market_claims or any(
        market not in KRX_NEXT_OPEN_MARKETS for market in market_claims
    ):
        return False
    specific_markets = set(market_claims) - {'KRX'}
    return len(specific_markets) <= 1


def _current_live_market_authority_applies(
    *,
    market_state: object,
    target_trade_date: date,
) -> bool:
    if (
        str(getattr(market_state, 'market', '') or '').strip().upper() != 'KRX'
        or str(getattr(market_state, 'state', '') or '').strip().upper() != 'OPEN'
        or not bool(getattr(market_state, 'is_trading_day', False))
    ):
        return False
    raw_server_time = getattr(market_state, 'server_time', None)
    if not raw_server_time:
        return True
    try:
        parsed = datetime.fromisoformat(str(raw_server_time).replace('Z', '+00:00'))
    except ValueError:
        return True
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return True
    return parsed.date() == target_trade_date


def _specific_snapshot_krx_market(
    snapshot: SignalDeciderDecisionSnapshot,
) -> str | None:
    specific_markets = set(_snapshot_market_claims(snapshot)) - {'KRX'}
    if len(specific_markets) != 1:
        return None
    market = next(iter(specific_markets))
    return market if market in {'KOSPI', 'KOSDAQ'} else None


def _current_live_market_buy_authority(
    *,
    snapshot: SignalDeciderDecisionSnapshot,
    target_trade_date: date,
    market_state: object,
    snapshot_context: dict[str, Any] | None,
    snapshot_load_error: bool = False,
) -> dict[str, Any]:
    applies = _current_live_market_authority_applies(
        market_state=market_state,
        target_trade_date=target_trade_date,
    )
    base = {
        'schema_version': CURRENT_LIVE_MARKET_AUTHORITY_SCHEMA_VERSION,
        'applies': applies,
        'market_date': target_trade_date.isoformat(),
        'source': 'current_market_snapshot_cache',
    }
    if not applies:
        return {
            **base,
            'status': 'not_applicable',
            'authoritative': False,
            'action': 'not_applicable',
            'reason_code': 'market_session_not_open',
        }

    market = _specific_snapshot_krx_market(snapshot)
    if market is None:
        return {
            **base,
            'status': 'unavailable',
            'authoritative': False,
            'action': 'block_new_buy',
            'reason_code': 'market_identity_missing_or_ambiguous',
        }
    if snapshot_load_error or not isinstance(snapshot_context, dict):
        return {
            **base,
            'market': market,
            'status': 'unavailable',
            'authoritative': False,
            'action': 'block_new_buy',
            'reason_code': (
                'market_snapshot_storage_unavailable'
                if snapshot_load_error
                else 'market_snapshot_unavailable'
            ),
        }

    open_drive = _build_market_open_drive_context(
        requested_market_date=target_trade_date,
        market=market,
        decision_time=getattr(market_state, 'server_time', None),
        snapshot_context=snapshot_context,
        prior_market_turnover_baseline=None,
    )
    breadth = (
        open_drive.get('market_breadth_context')
        if isinstance(open_drive.get('market_breadth_context'), dict)
        else {}
    )
    breadth_provenance = (
        open_drive.get('market_breadth_provenance')
        if isinstance(open_drive.get('market_breadth_provenance'), dict)
        else {}
    )
    breadth_authoritative = bool(
        breadth.get('status') == 'available'
        and breadth.get('market') == market
        and breadth_provenance.get('freshness_status') == 'fresh'
        and breadth_provenance.get('provenance_status') == 'provider_sourced'
        and breadth_provenance.get('as_of_market_date') == target_trade_date.isoformat()
        and breadth.get('captured_at') == breadth_provenance.get('as_of_timestamp')
    )
    if not breadth_authoritative:
        return {
            **base,
            'market': market,
            'status': 'unavailable',
            'authoritative': False,
            'action': 'block_new_buy',
            'reason_code': str(
                breadth.get('reason')
                or open_drive.get('reason')
                or 'live_market_breadth_unavailable'
            ),
            'observed_at': breadth.get('captured_at') or open_drive.get('observed_at'),
        }

    unavailable_context = {'status': 'unavailable', 'missing_fields': ['context']}
    unavailable_provenance = {
        'freshness_status': 'missing',
        'provenance_status': 'missing',
    }
    regime = _build_market_regime_context(
        requested_market_date=target_trade_date,
        market=market,
        market_breadth_context=breadth,
        market_breadth_provenance=breadth_provenance,
        sector_peer_context=unavailable_context,
        sector_peer_provenance=unavailable_provenance,
        market_relative_context=unavailable_context,
        market_relative_provenance=unavailable_provenance,
        open_drive_context=open_drive,
    )
    risk_off = bool(
        regime.get('posture') == 'risk_off'
        and regime.get('recommended_exec_action') == 'reject'
    )
    return {
        **base,
        'market': market,
        'status': 'available',
        'authoritative': True,
        'action': 'reject' if risk_off else 'allow',
        'posture': regime.get('posture'),
        'market_regime': regime.get('market_regime'),
        'recommended_exec_action': regime.get('recommended_exec_action'),
        'reason_code': regime.get('reason_code'),
        'observed_at': breadth.get('captured_at'),
        'market_breadth': {
            key: breadth.get(key)
            for key in (
                'market_symbol_count',
                'up_count',
                'down_count',
                'flat_count',
                'advance_decline_ratio',
                'expected_symbol_count',
                'observed_symbol_count',
                'limit_bucket_semantics',
            )
        },
        'provenance': breadth_provenance,
    }


def _stable_live_market_authority_generation_material(
    authority: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(authority, dict):
        return None
    return {
        key: copy.deepcopy(authority.get(key))
        for key in (
            'schema_version',
            'applies',
            'market_date',
            'source',
            'market',
            'status',
            'authoritative',
            'action',
            'posture',
            'market_regime',
            'recommended_exec_action',
            'reason_code',
            'observed_at',
            'market_breadth',
            'provenance',
        )
        if key in authority
    }


def _stable_market_state_generation_material(
    market_state: object,
) -> dict[str, Any]:
    raw_server_time = getattr(market_state, 'server_time', None)
    server_market_date: str | None = None
    if raw_server_time:
        try:
            parsed = datetime.fromisoformat(
                str(raw_server_time).replace('Z', '+00:00')
            )
        except ValueError:
            server_market_date = 'invalid'
        else:
            server_market_date = (
                parsed.date().isoformat()
                if parsed.tzinfo is not None and parsed.utcoffset() is not None
                else 'timezone_naive'
            )
    return {
        'market': str(getattr(market_state, 'market', '') or '').strip().upper(),
        'state': str(getattr(market_state, 'state', '') or '').strip().upper(),
        'is_trading_day': bool(getattr(market_state, 'is_trading_day', False)),
        'server_market_date': server_market_date,
    }


def _dispatch_authority_issue_reason(reason_code: str) -> dict[str, str]:
    messages = {
        'current_live_market_risk_off': (
            'Fresh authoritative live market breadth is severely weak; new BUY '
            'authority is blocked.'
        ),
        'current_live_market_evidence_unavailable': (
            'The open-session live market breadth proof is unavailable, stale, or '
            'ambiguous; new BUY authority fails closed.'
        ),
        'latest_snapshot_not_actionable': (
            'The latest bound decision is no longer actionable; stale BUY authority '
            'is blocked.'
        ),
        'latest_snapshot_replaced': (
            'The decision snapshot bound to this BUY was replaced; stale authority '
            'is blocked.'
        ),
        'candidate_removed': (
            'The decision candidate bound to this BUY is no longer present; stale '
            'authority is blocked.'
        ),
        'policy_scope_changed': (
            'The execution policy, route, or dispatch material changed; stale BUY '
            'authority is blocked.'
        ),
    }
    return _reason(
        reason_code,
        messages.get(
            reason_code,
            'Dispatch authority changed; the stale BUY is blocked.',
        ),
    )


def _snapshot_traceability_payload(snapshot: SignalDeciderDecisionSnapshot) -> dict[str, Any]:
    return _json_object_payload(snapshot.traceability_payload)


def _snapshot_response_summary_payload(snapshot: SignalDeciderDecisionSnapshot) -> dict[str, Any]:
    return _json_object_payload(snapshot.response_summary_payload)


def _snapshot_pre_decider_enrichment_context(snapshot: SignalDeciderDecisionSnapshot) -> dict[str, Any]:
    request_payload = _snapshot_request_payload(snapshot)
    context = request_payload.get('pre_decider_enrichment_context')
    if isinstance(context, dict):
        return context
    symbol_day_context = request_payload.get('symbol_day_context')
    if isinstance(symbol_day_context, dict) and isinstance(symbol_day_context.get('pre_decider_enrichment_context'), dict):
        return symbol_day_context['pre_decider_enrichment_context']
    return {}


def _snapshot_market_regime_context(snapshot: SignalDeciderDecisionSnapshot) -> dict[str, Any]:
    request_payload = _snapshot_request_payload(snapshot)
    context = request_payload.get('market_regime_context')
    if isinstance(context, dict):
        return context
    symbol_day_context = request_payload.get('symbol_day_context')
    if isinstance(symbol_day_context, dict) and isinstance(symbol_day_context.get('market_regime_context'), dict):
        return symbol_day_context['market_regime_context']
    return {}


def _snapshot_decider_data_quality_context(snapshot: SignalDeciderDecisionSnapshot) -> dict[str, Any]:
    request_payload = _snapshot_request_payload(snapshot)
    context = request_payload.get('decider_data_quality_context')
    if isinstance(context, dict):
        return context
    symbol_day_context = request_payload.get('symbol_day_context')
    if isinstance(symbol_day_context, dict) and isinstance(symbol_day_context.get('decider_data_quality_context'), dict):
        return symbol_day_context['decider_data_quality_context']
    return {}


def _snapshot_decision_input_quality_context(
    snapshot: SignalDeciderDecisionSnapshot,
    *,
    target: NextOpenAccountTarget | None = None,
    estimated_reference_price: Any = None,
    estimated_notional_krw: Any = None,
) -> dict[str, Any]:
    request_payload = _snapshot_request_payload(snapshot)
    context = request_payload.get('decision_input_quality_context')
    if not isinstance(context, dict):
        symbol_day_context = request_payload.get('symbol_day_context')
        if isinstance(symbol_day_context, dict) and isinstance(symbol_day_context.get('decision_input_quality_context'), dict):
            context = symbol_day_context['decision_input_quality_context']
        else:
            context = {}
    if context and target is None and estimated_reference_price is None and estimated_notional_krw is None:
        return context
    decider_quality_context = _snapshot_decider_data_quality_context(snapshot)
    pre_decider_context = _snapshot_pre_decider_enrichment_context(snapshot)
    if context and not decider_quality_context and not pre_decider_context:
        return context
    return build_decision_input_quality_envelope(
        symbol=snapshot.symbol,
        market_date=snapshot.market_date.isoformat(),
        decider_data_quality_context=decider_quality_context,
        pre_decider_enrichment_context=pre_decider_context,
        allocation_budget_krw=target.budget_krw if target is not None else None,
        estimated_reference_price=estimated_reference_price,
        planned_notional_krw=estimated_notional_krw,
    )


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


def _is_actionable_snapshot(snapshot: SignalDeciderDecisionSnapshot) -> bool:
    if not _is_krx_next_open_snapshot(snapshot):
        return False
    decision_agent_name = _snapshot_agent_name(snapshot)
    if decision_agent_name == SIGNAL_DECIDER_AGENT_NAME:
        return _normalize_action(snapshot.next_day_open_action or snapshot.decision_action) == 'buy'
    if decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME:
        return _short_term_tactical_action(snapshot) in SHORT_TERM_ACTIONABLE_ACTIONS
    return False


def _is_actionable_short_term_snapshot(snapshot: SignalDeciderDecisionSnapshot) -> bool:
    return _snapshot_agent_name(snapshot) == SHORT_TERM_DECIDER_AGENT_NAME and _is_actionable_snapshot(snapshot)


def _short_term_generic_action(tactical_action: str) -> str:
    return SHORT_TERM_GENERIC_ACTION_BY_TACTICAL_ACTION.get(tactical_action, 'wait')


def _short_term_handoff_intent(tactical_action: str) -> str | None:
    return SHORT_TERM_HANDOFF_INTENT_BY_TACTICAL_ACTION.get(tactical_action)


def _short_term_entry_execution_mode(tactical_action: str) -> str | None:
    if tactical_action == 'scalp_buy_candidate':
        return 'guarded_tactical_buy'
    if tactical_action == 'watch_for_trigger':
        return 'pending_trigger_limit'
    return None


def _short_term_field_text(snapshot: SignalDeciderDecisionSnapshot, key: str) -> str | None:
    payload = _snapshot_response_payload(snapshot)
    value = payload.get(key)
    return _normalize_optional_text(str(value)) if value is not None else None


def _latest_signal_decider_action_by_symbol(
    snapshots: Sequence[SignalDeciderDecisionSnapshot],
) -> dict[str, dict[str, Any]]:
    actions: dict[str, dict[str, Any]] = {}
    for snapshot in snapshots:
        if _snapshot_agent_name(snapshot) != SIGNAL_DECIDER_AGENT_NAME:
            continue
        if snapshot.symbol in actions:
            continue
        actions[snapshot.symbol] = build_signal_decider_alignment_context(snapshot) or {}
    return actions


def _normalize_objective_percentile(value: Any, *, unit: Any = None) -> Decimal | None:
    """Normalize only explicitly bounded objective percentile evidence.

    Untagged values use the unit interval. Values on a 0..100 scale must say
    so explicitly; silently treating ``82`` as ``0.82`` lets malformed or LLM
    supplied values become executable gate evidence.
    """

    parsed = _to_decimal(value)
    if parsed is None or parsed < Decimal('0'):
        return None
    normalized_unit = str(unit or '').strip().lower()
    if normalized_unit in {'percent', 'percentage', 'pct', '0_to_100'}:
        if parsed > Decimal('100'):
            return None
        return parsed / Decimal('100')
    if normalized_unit not in {'', 'ratio', 'fraction', 'unit_interval', '0_to_1'}:
        return None
    return parsed if parsed <= Decimal('1') else None


def _short_term_objective_gate_features(
    snapshot: SignalDeciderDecisionSnapshot,
) -> dict[str, Any]:
    """Read executable gate inputs from server-built request context only."""

    request_payload = _snapshot_request_payload(snapshot)
    symbol_day_context = (
        request_payload.get('symbol_day_context')
        if isinstance(request_payload.get('symbol_day_context'), dict)
        else {}
    )
    technical = (
        symbol_day_context.get('technical_snapshot')
        if isinstance(symbol_day_context.get('technical_snapshot'), dict)
        else {}
    )
    action_summary = (
        symbol_day_context.get('action_summary')
        if isinstance(symbol_day_context.get('action_summary'), dict)
        else {}
    )
    action_summary_payload = (
        action_summary.get('summary_payload')
        if isinstance(action_summary.get('summary_payload'), dict)
        else {}
    )
    market_relative_context = (
        symbol_day_context.get('market_relative_context')
        if isinstance(symbol_day_context.get('market_relative_context'), dict)
        else {}
    )
    relative_strength = (
        market_relative_context.get('relative_strength')
        if isinstance(market_relative_context.get('relative_strength'), dict)
        else {}
    )
    if not relative_strength:
        market_regime = _snapshot_market_regime_context(snapshot)
        relative_strength = (
            market_regime.get('relative_strength_context')
            if isinstance(market_regime.get('relative_strength_context'), dict)
            else {}
        )
    pre_decider = _snapshot_pre_decider_enrichment_context(snapshot)
    pre_decider_trading_value = (
        pre_decider.get('trading_value')
        if isinstance(pre_decider.get('trading_value'), dict)
        else {}
    )
    liquidity_context = (
        request_payload.get('liquidity_context')
        if isinstance(request_payload.get('liquidity_context'), dict)
        else symbol_day_context.get('liquidity_context')
        if isinstance(symbol_day_context.get('liquidity_context'), dict)
        else {}
    )
    trading_value = (
        liquidity_context.get('turnover')
        if isinstance(liquidity_context.get('turnover'), dict)
        else pre_decider_trading_value
    )
    decision_input_quality = (
        request_payload.get('decision_input_quality_context')
        if isinstance(request_payload.get('decision_input_quality_context'), dict)
        else symbol_day_context.get('decision_input_quality_context')
        if isinstance(symbol_day_context.get('decision_input_quality_context'), dict)
        else {}
    )
    final_judgment = (
        decision_input_quality.get('final_judgment')
        if isinstance(decision_input_quality.get('final_judgment'), dict)
        else {}
    )

    return_raw = relative_strength.get('return_percentile')
    volume_raw = relative_strength.get('volume_percentile')
    common_percentile_unit = relative_strength.get('percentile_unit')
    return_percentile = _normalize_objective_percentile(
        return_raw,
        unit=relative_strength.get('return_percentile_unit') or common_percentile_unit,
    )
    volume_percentile = _normalize_objective_percentile(
        volume_raw,
        unit=relative_strength.get('volume_percentile_unit') or common_percentile_unit,
    )
    setup_state = _normalize_optional_text(action_summary_payload.get('setup_state'))
    normalized_setup_state = setup_state.lower() if setup_state else None
    trend_state = _normalize_optional_text(technical.get('trend_state'))
    normalized_trend_state = trend_state.lower() if trend_state else None
    breakout_20d = technical.get('breakout_20d') is True
    breakdown_20d = technical.get('breakdown_20d') is True
    continuation_setup_detected = bool(
        not breakdown_20d
        and (
            breakout_20d
            or normalized_setup_state
            in {'confirmed', 'continuation', 'breakout', 'pullback_support', 'reclaim'}
        )
    )
    trading_value_source = _normalize_optional_text(
        trading_value.get('source') or trading_value.get('trading_value_source')
    )
    raw_trading_value_krw = (
        _to_decimal(trading_value.get('raw_trading_value_krw'))
        if str(trading_value_source or '').lower() == 'raw'
        else None
    )
    return {
        'atr_pct_14': _to_decimal(technical.get('atr_pct_14')),
        'ret20': _to_decimal(technical.get('return_20d')),
        'ret5': _to_decimal(technical.get('return_5d')),
        'return_percentile': return_percentile,
        'return_percentile_status': (
            'missing' if return_raw is None else 'available' if return_percentile is not None else 'invalid'
        ),
        'volume_percentile': volume_percentile,
        'volume_percentile_status': (
            'missing' if volume_raw is None else 'available' if volume_percentile is not None else 'invalid'
        ),
        'raw_trading_value_krw': raw_trading_value_krw,
        'buy_quality_score': _to_decimal(final_judgment.get('buy_quality_score')),
        'decision_band': _normalize_optional_text(final_judgment.get('decision_band')),
        'trading_value_source': trading_value_source,
        'trend_state': normalized_trend_state,
        'setup_state': normalized_setup_state,
        'continuation_setup_detected': continuation_setup_detected,
    }


def _short_term_entry_quality_blocks(
    *,
    snapshot: SignalDeciderDecisionSnapshot,
    signal_decider_crosscheck: dict[str, Any] | None,
    tactical_action: str | None,
) -> tuple[list[dict[str, str]], list[dict[str, str]], dict[str, Any]]:
    objective_features = _short_term_objective_gate_features(snapshot)
    atr_pct = objective_features['atr_pct_14']
    ret20 = objective_features['ret20']
    ret5 = objective_features['ret5']
    return_percentile = objective_features['return_percentile']
    volume_percentile = objective_features['volume_percentile']
    raw_trading_value_krw = objective_features['raw_trading_value_krw']
    buy_quality_score = objective_features['buy_quality_score']
    decision_band = objective_features['decision_band']
    trading_value_source = objective_features['trading_value_source']
    response_payload = _snapshot_response_payload(snapshot)
    trigger_price = _to_decimal(response_payload.get('trigger_price'))
    target_price = _to_decimal(snapshot.target_price) or _to_decimal(
        response_payload.get('target_price')
    )
    stop_loss_price = _to_decimal(snapshot.stop_loss_price) or _to_decimal(
        response_payload.get('stop_loss_price')
    )
    trend_state = objective_features['trend_state']
    setup_state = objective_features['setup_state']
    reward_risk_ratio: Decimal | None = None
    if trigger_price is not None and target_price is not None and stop_loss_price is not None:
        upside = target_price - trigger_price
        downside = trigger_price - stop_loss_price
        if upside > 0 and downside > 0:
            reward_risk_ratio = upside / downside
    feature_snapshot = {
        'schema_version': 'short_term_entry_quality_v2',
        'atr_pct_14': _to_json_safe(atr_pct),
        'ret20': _to_json_safe(ret20),
        'ret5': _to_json_safe(ret5),
        'return_percentile': _to_json_safe(return_percentile),
        'return_percentile_status': objective_features['return_percentile_status'],
        'volume_percentile': _to_json_safe(volume_percentile),
        'volume_percentile_status': objective_features['volume_percentile_status'],
        'raw_trading_value_krw': _to_json_safe(raw_trading_value_krw),
        'buy_quality_score': _to_json_safe(buy_quality_score),
        'decision_band': decision_band,
        'trading_value_source': trading_value_source,
        'reward_risk_ratio': _to_json_safe(reward_risk_ratio),
        'trend_state': trend_state,
        'setup_state': setup_state,
        'continuation_evidence_source': 'objective_request_payload',
        'signal_decider_crosscheck': signal_decider_crosscheck,
    }
    blocks: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    signal_action = str((signal_decider_crosscheck or {}).get('action') or '').lower()
    if signal_action == 'reject':
        blocks.append(
            _reason(
                'signal_decider_reject_veto',
                'Same-day signal-decider rejected the setup; short-term buy handoff is blocked.',
            )
        )
    elif signal_action == 'wait' and tactical_action == 'scalp_buy_candidate':
        blocks.append(
            _reason(
                'signal_decider_wait_blocks_scalp_buy',
                'Same-day signal-decider is wait; immediate scalp buy is blocked and should be monitored only after a new trigger.',
            )
        )
    elif signal_action == 'wait':
        warnings.append(
            _reason(
                'signal_decider_wait_requires_trigger_confirmation',
                'Same-day signal-decider is wait; exec must require live trigger-time confirmation before buying.',
            )
        )

    trend_text = f'{trend_state or ""} {setup_state or ""}'.lower()
    if any(marker in trend_text for marker in ('downtrend', 'bearish', 'breakdown_20d', 'breakdown')):
        blocks.append(
            _reason(
                'short_term_downtrend_or_breakdown_block',
                'Short-term candidate is in a downtrend/bearish/breakdown setup.',
            )
        )
    if ret20 is not None and ret20 < Decimal('-10'):
        blocks.append(
            _reason(
                'short_term_ret20_deep_negative_block',
                '20-day return is deeply negative for a short-term buy candidate.',
            )
        )
    if atr_pct is not None and atr_pct >= Decimal('9') and ret20 is not None and ret20 <= Decimal('0'):
        blocks.append(
            _reason(
                'short_term_high_atr_weak_trend_block',
                'ATR is high while 20-day return is non-positive; recent stop-loss history shows this pattern is fragile.',
            )
        )
    if atr_pct is not None and atr_pct > Decimal('9') and ret5 is not None and ret5 > Decimal('15'):
        blocks.append(
            _reason(
                'short_term_high_atr_chase_block',
                '5-day return is already extended while ATR is high; avoid chasing the spike.',
            )
        )
    if buy_quality_score is not None and buy_quality_score < SHORT_TERM_HARD_MIN_BUY_QUALITY_SCORE:
        blocks.append(
            _reason(
                'short_term_buy_quality_score_block',
                'Decision input buy-quality score is too weak for short-term buy handoff.',
            )
        )
    elif buy_quality_score is not None and buy_quality_score < SHORT_TERM_MIN_BUY_QUALITY_SCORE:
        warnings.append(
            _reason(
                'short_term_buy_quality_score_warning',
                'Decision input buy-quality score is below the preferred threshold; exec should size conservatively.',
            )
        )
    if objective_features['return_percentile_status'] == 'invalid':
        blocks.append(
            _reason(
                'short_term_return_percentile_invalid_block',
                'Market-relative return percentile is non-finite, out of range, or missing an explicit percent unit.',
            )
        )
    elif return_percentile is None:
        warnings.append(
            _reason(
                'short_term_relative_strength_unverified_warning',
                'Market-relative return evidence is missing; exec should require stricter trigger-time confirmation.',
            )
        )
    elif return_percentile < SHORT_TERM_HARD_MIN_RETURN_PERCENTILE:
        blocks.append(
            _reason(
                'short_term_very_weak_relative_strength_block',
                'Short-term buy candidate has very weak market-relative return evidence.',
            )
        )
    elif return_percentile < SHORT_TERM_MIN_RETURN_PERCENTILE:
        warnings.append(
            _reason(
                'short_term_weak_relative_strength_warning',
                'Market-relative return percentile is below the preferred leadership threshold; exec should size conservatively.',
            )
        )
    if objective_features['volume_percentile_status'] == 'invalid':
        blocks.append(
            _reason(
                'short_term_volume_percentile_invalid_block',
                'Market-relative volume percentile is non-finite, out of range, or missing an explicit percent unit.',
            )
        )
    elif volume_percentile is None:
        warnings.append(
            _reason(
                'short_term_relative_volume_unverified_warning',
                'Market-relative participation evidence is missing; exec should require stricter trigger-time confirmation.',
            )
        )
    elif volume_percentile < SHORT_TERM_MIN_VOLUME_PERCENTILE:
        warnings.append(
            _reason(
                'short_term_weak_relative_volume_warning',
                'Market-relative volume percentile is below the preferred participation threshold; exec should size conservatively.',
            )
        )
    if raw_trading_value_krw is not None and raw_trading_value_krw < SHORT_TERM_MIN_RAW_TRADING_VALUE_KRW:
        blocks.append(
            _reason(
                'short_term_liquidity_floor_block',
                'Raw trading value is below the short-term liquidity floor for reliable entry and exit.',
            )
        )
    elif raw_trading_value_krw is None:
        warnings.append(
            _reason(
                'short_term_liquidity_unverified_warning',
                'Objective trading-value evidence is missing; exec should size conservatively and require live liquidity confirmation.',
            )
        )
    if reward_risk_ratio is not None and reward_risk_ratio < SHORT_TERM_MIN_REWARD_RISK_RATIO:
        blocks.append(
            _reason(
                'short_term_reward_risk_ratio_block',
                'Short-term candidate does not offer enough upside versus stop distance.',
            )
        )
    has_continuation_setup = bool(objective_features['continuation_setup_detected'])
    feature_snapshot['continuation_setup_detected'] = has_continuation_setup
    if tactical_action == 'scalp_buy_candidate' and not has_continuation_setup:
        blocks.append(
            _reason(
                'short_term_scalp_requires_continuation_setup',
                'Immediate scalp buy requires explicit pullback, reclaim, breakout, support, volume, or relative-strength continuation evidence.',
            )
        )
    elif tactical_action == 'watch_for_trigger' and not has_continuation_setup:
        warnings.append(
            _reason(
                'short_term_watch_needs_continuation_confirmation',
                'Watch buy lacks explicit continuation setup language; exec must require strict trigger-time confirmation.',
            )
        )
    return blocks, warnings, feature_snapshot


def _first_blocking_reason(validation_payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(validation_payload, dict):
        return None
    for reason in validation_payload.get('blocking_reasons') or []:
        if isinstance(reason, dict):
            return reason
        normalized = _normalize_optional_text(reason)
        if normalized:
            return {'code': 'blocked', 'message': normalized}
    return None


def normalize_next_open_target_trade_date(
    value: str | date | None,
    *,
    default: date,
    allow_fallback: bool = True,
) -> date:
    candidate = _normalize_date(value, default=default)
    normalized = next_krx_open_date_on_or_after(candidate, allow_fallback=allow_fallback)
    if normalized is None:
        raise ValueError(f'Could not resolve next KRX open date on or after {candidate.isoformat()}')
    return normalized


async def async_normalize_next_open_target_trade_date(
    value: str | date | None,
    *,
    default: date,
    allow_fallback: bool = True,
) -> date:
    candidate = _normalize_date(value, default=default)
    normalized = await async_next_krx_open_date_on_or_after(candidate, allow_fallback=allow_fallback)
    if normalized is None:
        raise ValueError(f'Could not resolve next KRX open date on or after {candidate.isoformat()}')
    return normalized


def _previous_trading_day(value: date, *, allow_fallback: bool = True) -> date:
    previous = previous_krx_trading_date(value, allow_fallback=allow_fallback)
    if previous is None:
        raise ValueError(f'Could not resolve previous KRX trading date before {value.isoformat()}')
    return previous


async def _async_previous_trading_day(value: date, *, allow_fallback: bool = True) -> date:
    previous = await async_previous_krx_trading_date(value, allow_fallback=allow_fallback)
    if previous is None:
        raise ValueError(f'Could not resolve previous KRX trading date before {value.isoformat()}')
    return previous


def _calendar_requires_operator_override(market_state: MarketState) -> bool:
    source = _normalize_optional_text(getattr(market_state, 'calendar_source', None))
    status = _normalize_optional_text(getattr(market_state, 'calendar_source_status', None))
    if source == CALENDAR_SOURCE_EXCHANGE_CALENDARS:
        return True
    return bool(status and status in CALENDAR_FALLBACK_STATUSES)


def _reason(code: str, message: str) -> dict[str, str]:
    return {'code': code, 'message': message}


def _delegate_short_term_watch_entry_quality_blocks_to_warnings(
    *,
    tactical_action: str | None,
    handoff_intent: str | None,
    blocks: Sequence[dict[str, str]],
    warnings: Sequence[dict[str, str]],
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    if tactical_action != 'watch_for_trigger' or handoff_intent != 'intraday_watch_buy':
        return list(blocks), list(warnings)
    delegated_blocks: list[dict[str, str]] = []
    delegated_warnings = list(warnings)
    existing_warning_codes = {
        str(warning.get('code') or '').strip() for warning in delegated_warnings if isinstance(warning, dict)
    }
    for block in blocks:
        code = str(block.get('code') or '').strip()
        if code not in SHORT_TERM_WATCH_DELEGATED_ENTRY_QUALITY_BLOCK_CODES:
            delegated_blocks.append(block)
            continue
        warning_code = f'{code}_delegated_to_exec_watch'
        if warning_code not in existing_warning_codes:
            delegated_warnings.append(
                _reason(
                    warning_code,
                    (
                        f"{block.get('message') or 'Short-term entry quality needs trigger-time confirmation.'} "
                        'Delegated to exec intraday watch; trigger-time exec-agent must confirm before any buy.'
                    ),
                )
            )
            existing_warning_codes.add(warning_code)
    return delegated_blocks, delegated_warnings


def _should_relax_short_term_decision_input_hard_block(
    *,
    decision_agent_name: str,
    handoff_intent: str | None,
    hard_block: dict[str, Any],
    warnings: Sequence[dict[str, str]],
) -> bool:
    if decision_agent_name != SHORT_TERM_DECIDER_AGENT_NAME or handoff_intent is None:
        return False
    code = str(hard_block.get('code') or '').strip()
    if code not in SHORT_TERM_RELAXED_DECISION_INPUT_HARD_BLOCK_CODES:
        return False
    warning_codes = {str(warning.get('code') or '').strip() for warning in warnings if isinstance(warning, dict)}
    return 'short_term_liquidity_unverified_warning' in warning_codes


def _exec_agent_sizing_dispatch_enabled() -> bool:
    return str(settings.signal_decider_exec_integration_mode or '').strip().lower() == 'dispatch'


def _is_dispatch_terminal_status(value: Any) -> bool:
    return _normalize_optional_text(value) in {
        EXECUTION_STATUS_SUBMITTED,
        EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING,
    }


def _uses_exec_agent_sizing_for_dispatch(decision_agent_name: str) -> bool:
    return _exec_agent_sizing_dispatch_enabled() and decision_agent_name in {
        SIGNAL_DECIDER_AGENT_NAME,
        SHORT_TERM_DECIDER_AGENT_NAME,
    }


def _next_open_performance_cost_evidence(
    *,
    dispatches_to_exec: bool,
    execution_feasibility_context: dict[str, Any],
    pre_decider_enrichment_context: dict[str, Any],
) -> tuple[Any, str]:
    quote = pre_decider_enrichment_context.get('quote_orderbook_snapshot')
    if (
        not dispatches_to_exec
        or pre_decider_enrichment_context.get('schema_version') != 'pre_decider_enrichment_v1'
        or not isinstance(quote, dict)
        or not quote
    ):
        return None, 'unavailable'
    return (
        execution_feasibility_context.get('expected_slippage_bps'),
        str(execution_feasibility_context.get('expected_slippage_basis') or 'unavailable'),
    )


def _is_canonical_closed_market_cost_absence(
    *,
    pre_decider_enrichment_context: dict[str, Any],
    execution_feasibility_context: dict[str, Any],
) -> bool:
    if (
        execution_feasibility_context.get('expected_slippage_bps') is not None
        or execution_feasibility_context.get('expected_slippage_basis') != 'unavailable'
    ):
        return False
    phase_contract = pre_decider_enrichment_context.get('market_data_phase_contract')
    quote = pre_decider_enrichment_context.get('quote_orderbook_snapshot')
    if not isinstance(phase_contract, dict) or not isinstance(quote, dict):
        return False
    quote_status = str(quote.get('status') or '').strip().lower()
    snapshot_type = str(quote.get('snapshot_type') or '').strip().lower()
    quote_freshness_status = str(quote.get('quote_freshness_status') or '').strip().lower()
    status_and_reason = (
        execution_feasibility_context.get('expected_slippage_status'),
        execution_feasibility_context.get('expected_slippage_reason'),
    )
    raw_spread = (
        quote.get('spread_bps')
        if 'spread_bps' in quote
        else quote.get('quote_spread_bps_of_price')
    )
    if status_and_reason == ('missing', 'quote_spread_missing'):
        if raw_spread is not None:
            return False
    elif status_and_reason == ('unavailable', 'quote_spread_not_executable_eod'):
        parsed_spread = _to_decimal(raw_spread)
        if parsed_spread is None or parsed_spread < 0:
            return False
    else:
        return False
    return bool(
        phase_contract.get('schema_version') == 'market_data_phase_contract_v1'
        and phase_contract.get('recommendation_basis') == 'latest_completed_eod'
        and phase_contract.get('realtime_quote_orderbook_required') is False
        and phase_contract.get('eod_reference_price_accepted') is True
        and phase_contract.get('not_applicable_realtime_is_expected') is True
        and (
            (quote_status, snapshot_type, quote_freshness_status)
            in {
                ('stale_eod', 'eod_stale', 'stale'),
                ('not_applicable', 'closed_market_no_eod_price', 'not_applicable'),
            }
        )
    )


def _pykrx_preflight_has_partial_coverage(preflight: dict[str, Any] | None) -> bool:
    if not isinstance(preflight, dict):
        return False
    if preflight.get('warning_categories'):
        return True
    category_gates = preflight.get('category_gates')
    if not isinstance(category_gates, list):
        return False
    for item in category_gates:
        if not isinstance(item, dict):
            continue
        coverage_status = _normalize_optional_text(item.get('coverage_status'))
        if item.get('gate_status') == 'ready' and coverage_status and coverage_status != 'complete':
            return True
    return False


def _should_inspect_short_term_candidate_pykrx(preflight: dict[str, Any] | None) -> bool:
    if not isinstance(preflight, dict):
        return False
    return not preflight.get('allowed') or _pykrx_preflight_has_partial_coverage(preflight)


def _row_validation_payload(row: SignalDeciderExecutionPlanItem) -> dict[str, Any]:
    payload = row.__dict__.get('validation_payload')
    return payload if isinstance(payload, dict) else {}


@dataclass(frozen=True)
class NextOpenAccountTarget:
    label: str
    account_reference: str
    budget_krw: Decimal


def _next_open_policy_fingerprint(account_targets: Sequence[NextOpenAccountTarget]) -> str:
    payload = {
        'schema_version': NEXT_OPEN_PREOPEN_POLICY_FINGERPRINT_VERSION,
        'integration_mode': str(settings.signal_decider_exec_integration_mode or '').strip().lower(),
        'next_open_execution_enabled': bool(settings.signal_decider_next_open_enabled),
        'execution_cost_evidence_policy_version': NEXT_OPEN_COST_EVIDENCE_POLICY_VERSION,
        'non_spread_round_trip_cost_bps': _decimal_to_text(
            _to_decimal(settings.signal_decider_next_open_non_spread_round_trip_cost_bps)
        ),
        'min_confidence_score_10': _decimal_to_text(
            _to_decimal(settings.signal_decider_next_open_min_confidence_score_10)
        ) or 'invalid',
        'min_expected_return_pct': _decimal_to_text(
            _to_decimal(settings.signal_decider_next_open_min_expected_return_pct)
        ) or 'invalid',
        'min_reward_risk_ratio': _decimal_to_text(
            _to_decimal(settings.signal_decider_next_open_min_reward_risk_ratio)
        ),
        'require_active_policy': bool(
            settings.signal_decider_next_open_require_active_policy
        ),
        'block_on_data_warnings': bool(
            settings.signal_decider_next_open_block_on_data_warnings
        ),
        'block_on_specialist_disagreement': bool(
            settings.signal_decider_next_open_block_on_specialist_disagreement
        ),
        'require_target_and_stop': bool(
            settings.signal_decider_next_open_require_target_and_stop
        ),
        'wait_conflict_gate_mode': str(
            settings.signal_decider_wait_conflict_gate_mode
        ).strip(),
        'wait_conflict_probe_quantity': int(
            settings.signal_decider_wait_conflict_probe_quantity
        ),
        'account_targets': sorted(
            (
                {
                    'label': target.label,
                    'account_reference': target.account_reference,
                    'budget_krw': _decimal_to_text(target.budget_krw),
                }
                for target in account_targets
            ),
            key=lambda item: (item['account_reference'], item['label'], item['budget_krw'] or ''),
        ),
    }
    canonical = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(',', ':'))
    return f'{NEXT_OPEN_PREOPEN_POLICY_FINGERPRINT_VERSION}:{hashlib.sha256(canonical.encode("utf-8")).hexdigest()}'


def _preopen_plan_covers_latest_snapshots(
    *,
    existing_rows: Sequence[SignalDeciderExecutionPlanItem],
    snapshots: Sequence[SignalDeciderDecisionSnapshot],
    account_targets: Sequence[NextOpenAccountTarget],
) -> bool:
    if any(
        not _is_dispatch_terminal_status(row.execution_status)
        for row in existing_rows
    ):
        return False
    latest_by_symbol_agent = {
        (snapshot.symbol, _snapshot_agent_name(snapshot)): snapshot
        for snapshot in snapshots
    }
    targets_by_reference = {target.account_reference: target for target in account_targets}
    for row in existing_rows:
        latest = latest_by_symbol_agent.get(
            (row.symbol, _normalize_agent_name(row.decision_agent_name))
        )
        if (
            latest is None
            or not _is_actionable_snapshot(latest)
            or int(row.decision_snapshot_id) != int(latest.id)
        ):
            return False
        target = targets_by_reference.get(row.account_reference)
        if target is None and _normalize_agent_name(row.decision_agent_name) == SHORT_TERM_DECIDER_AGENT_NAME:
            target = NextOpenAccountTarget(
                label=UNSPECIFIED_ROUTE_ACCOUNT_LABEL,
                account_reference=UNSPECIFIED_ROUTE_ACCOUNT_REFERENCE,
                budget_krw=UNSPECIFIED_ROUTE_ACCOUNT_BUDGET_KRW,
            )
        validation = _row_validation_payload(row)
        if (
            target is None
            or validation.get('preopen_policy_fingerprint')
            != _next_open_policy_fingerprint([target])
        ):
            return False

    expected_snapshots: dict[tuple[str, str, str], SignalDeciderDecisionSnapshot] = {}
    for snapshot in snapshots:
        if not _is_actionable_snapshot(snapshot):
            continue
        decision_agent_name = _snapshot_agent_name(snapshot)
        targets = list(account_targets)
        if not targets and decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME:
            targets = [
                NextOpenAccountTarget(
                    label=UNSPECIFIED_ROUTE_ACCOUNT_LABEL,
                    account_reference=UNSPECIFIED_ROUTE_ACCOUNT_REFERENCE,
                    budget_krw=UNSPECIFIED_ROUTE_ACCOUNT_BUDGET_KRW,
                )
            ]
        for target in targets:
            key = (target.account_reference, snapshot.symbol, decision_agent_name)
            expected_snapshots[key] = snapshot

    existing_by_key = {
        (row.account_reference, row.symbol, _normalize_agent_name(row.decision_agent_name)): row
        for row in existing_rows
    }
    if set(existing_by_key) != set(expected_snapshots):
        return False
    for key, snapshot in expected_snapshots.items():
        row = existing_by_key.get(key)
        if row is None or int(row.decision_snapshot_id) != int(snapshot.id):
            return False
    return True


class SignalDeciderNextOpenExecutionService:
    def __init__(
        self,
        *,
        order_service: TradingOrderService | None = None,
        market_state_getter: Callable[[], Any] | None = None,
    ) -> None:
        self.order_service = order_service or trading_order_service
        self.market_state_getter = market_state_getter or get_market_state
        self._uses_default_market_state_getter = market_state_getter is None

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

    def inspect_dispatch_authority_generation(
        self,
        *,
        target_trade_date: str | date,
        expected_row_bindings: dict[int, dict[str, Any]],
        _session: Any | None = None,
    ) -> dict[str, Any]:
        resolved_target_date = self._resolve_target_trade_date(target_trade_date)
        source_market_date = _previous_trading_day(resolved_target_date)
        normalized_bindings = {
            int(row_id): copy.deepcopy(binding)
            for row_id, binding in expected_row_bindings.items()
            if isinstance(binding, dict)
        }
        row_ids = sorted(normalized_bindings)
        snapshot_context: dict[str, Any] | None = None
        snapshot_load_error = False

        session_context = session_scope() if _session is None else nullcontext(_session)
        with session_context as session:
            session.execute(
                text('SELECT pg_advisory_xact_lock(:lock_key)'),
                {'lock_key': _plan_refresh_advisory_lock_key(resolved_target_date)},
            )
            _lock_dispatch_authority_source_tables(session)
            market_state = self.market_state_getter()
            live_market_authority_applies = _current_live_market_authority_applies(
                market_state=market_state,
                target_trade_date=resolved_target_date,
            )
            if live_market_authority_applies:
                session.execute(
                    select(CurrentMarketSnapshotCache.id)
                    .where(CurrentMarketSnapshotCache.cache_key == 'dashboard')
                    .with_for_update(read=True)
                ).scalar_one_or_none()
                snapshot_context, snapshot_load_error = (
                    _load_live_market_snapshot_context()
                )
            rows = session.execute(
                select(SignalDeciderExecutionPlanItem)
                .where(SignalDeciderExecutionPlanItem.id.in_(row_ids))
                .order_by(SignalDeciderExecutionPlanItem.id.asc())
                .with_for_update(read=True)
            ).scalars().all()
            latest_snapshots = self._load_latest_snapshots(
                source_market_date=source_market_date
            )
            run_generation = session.execute(
                select(AgentContextFeedbackRun)
                .where(AgentContextFeedbackRun.market_date == source_market_date)
                .order_by(
                    AgentContextFeedbackRun.started_at.asc(),
                    AgentContextFeedbackRun.id.asc(),
                )
            ).scalars().all()

            account_targets, account_target_errors = self._parse_account_targets()
            account_targets_by_reference = {
                target.account_reference: target for target in account_targets
            }
            latest_by_scope = {
                (snapshot.symbol.upper(), _snapshot_agent_name(snapshot)): snapshot
                for snapshot in latest_snapshots
            }
            rows_by_id = {int(row.id): row for row in rows}
            issues: list[dict[str, Any]] = []
            row_generation: list[dict[str, Any]] = []

            for row_id in row_ids:
                expected = normalized_bindings[row_id]
                row = rows_by_id.get(row_id)
                if row is None:
                    issues.append(
                        {
                            'row_id': row_id,
                            'reason_code': 'latest_snapshot_replaced',
                            'replacement_action': 'wait',
                            'replacement_actionable': False,
                        }
                    )
                    continue

                decision_agent_name = _normalize_agent_name(row.decision_agent_name)
                scope = (str(row.symbol or '').strip().upper(), decision_agent_name)
                latest = latest_by_scope.get(scope)
                latest_action = (
                    _short_term_tactical_action(latest)
                    if latest is not None
                    and decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME
                    else _normalize_action(
                        latest.next_day_open_action or latest.decision_action
                    )
                    if latest is not None
                    else 'wait'
                )
                reason_code: str | None = None
                replacement_action = latest_action
                replacement_actionable = bool(
                    latest is not None and _is_actionable_snapshot(latest)
                )

                if (
                    row.target_trade_date != resolved_target_date
                    or row.source_market_date != source_market_date
                    or row.execution_window != EXECUTION_WINDOW_NEXT_OPEN
                ):
                    reason_code = 'latest_snapshot_replaced'
                elif latest is None:
                    reason_code = 'candidate_removed'
                    replacement_action = 'wait'
                    replacement_actionable = False
                elif not _is_actionable_snapshot(latest):
                    reason_code = 'latest_snapshot_not_actionable'
                    replacement_actionable = False
                elif int(row.decision_snapshot_id) != int(latest.id):
                    reason_code = 'latest_snapshot_replaced'
                elif int(expected.get('decision_snapshot_id') or 0) != int(
                    row.decision_snapshot_id
                ):
                    reason_code = 'latest_snapshot_replaced'
                elif expected.get('row_revision_fingerprint') != (
                    _execution_plan_row_authority_fingerprint(row)
                ):
                    reason_code = 'policy_scope_changed'

                target = account_targets_by_reference.get(row.account_reference)
                if (
                    target is None
                    and decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME
                    and row.account_reference == UNSPECIFIED_ROUTE_ACCOUNT_REFERENCE
                ):
                    target = NextOpenAccountTarget(
                        label=UNSPECIFIED_ROUTE_ACCOUNT_LABEL,
                        account_reference=UNSPECIFIED_ROUTE_ACCOUNT_REFERENCE,
                        budget_krw=UNSPECIFIED_ROUTE_ACCOUNT_BUDGET_KRW,
                    )
                current_policy_fingerprint = (
                    _next_open_policy_fingerprint([target])
                    if target is not None
                    else None
                )
                if reason_code is None and (
                    account_target_errors
                    or current_policy_fingerprint is None
                    or _row_validation_payload(row).get(
                        'preopen_policy_fingerprint'
                    )
                    != current_policy_fingerprint
                ):
                    reason_code = 'policy_scope_changed'

                live_authority: dict[str, Any] | None = None
                if latest is not None and row.planned_side == 'buy':
                    live_authority = _current_live_market_buy_authority(
                        snapshot=latest,
                        target_trade_date=resolved_target_date,
                        market_state=market_state,
                        snapshot_context=snapshot_context,
                        snapshot_load_error=snapshot_load_error,
                    )
                    authority_action = live_authority.get('action')
                    if reason_code is None and authority_action == 'reject':
                        reason_code = 'current_live_market_risk_off'
                        replacement_action = 'reject'
                        replacement_actionable = False
                    elif reason_code is None and authority_action == 'block_new_buy':
                        reason_code = 'current_live_market_evidence_unavailable'
                        replacement_action = 'reject'
                        replacement_actionable = False

                if reason_code is not None:
                    issues.append(
                        {
                            'row_id': row_id,
                            'reason_code': reason_code,
                            'replacement_action': replacement_action,
                            'replacement_actionable': replacement_actionable,
                        }
                    )
                row_generation.append(
                    {
                        'row_id': row_id,
                        'row_revision_fingerprint': (
                            _execution_plan_row_authority_fingerprint(row)
                        ),
                        'decision_snapshot_id': int(row.decision_snapshot_id),
                        'latest_decision_snapshot_id': (
                            int(latest.id) if latest is not None else None
                        ),
                        'current_policy_fingerprint': current_policy_fingerprint,
                        'live_authority': (
                            _stable_live_market_authority_generation_material(
                                live_authority
                            )
                        ),
                    }
                )

            generation_material = {
                'schema_version': 'dispatch_authority_generation_v1',
                'target_trade_date': resolved_target_date,
                'source_market_date': source_market_date,
                'row_bindings': normalized_bindings,
                'rows': row_generation,
                'latest_snapshot_cohort_fingerprint': (
                    _snapshot_cohort_fingerprint(latest_snapshots)
                ),
                'run_generation': [
                    {
                        column.key: getattr(run, column.key, None)
                        for column in AgentContextFeedbackRun.__table__.columns
                    }
                    for run in run_generation
                ],
                'market_state': _stable_market_state_generation_material(
                    market_state
                ),
                'live_market_snapshot_fingerprint': (
                    _live_market_snapshot_fingerprint(
                        snapshot_context,
                        load_error=snapshot_load_error,
                    )
                ),
                'account_target_errors': account_target_errors,
                'issues': issues,
            }
            canonical = json.dumps(
                _to_json_safe(generation_material),
                ensure_ascii=True,
                sort_keys=True,
                separators=(',', ':'),
            )
            generation_fingerprint = hashlib.sha256(
                canonical.encode('utf-8')
            ).hexdigest()

        first_issue = issues[0] if issues else {}
        return {
            'status': 'invalid' if issues else 'valid',
            'generation_fingerprint': generation_fingerprint,
            'reason_code': first_issue.get('reason_code'),
            'replacement_action': first_issue.get('replacement_action'),
            'replacement_actionable': first_issue.get(
                'replacement_actionable'
            ),
            'issues': issues,
        }

    def apply_dispatch_authority_generation_fence(
        self,
        *,
        target_trade_date: str | date,
        expected_row_bindings: dict[int, dict[str, Any]],
    ) -> dict[str, Any]:
        """Durably fence exact rows after a dispatch-generation mismatch."""
        normalized_bindings = {
            int(row_id): copy.deepcopy(binding)
            for row_id, binding in expected_row_bindings.items()
            if isinstance(binding, dict)
        }
        now = _utcnow()
        blocked_count = 0
        staged_revocation_count = 0
        with session_scope() as session:
            generation = self.inspect_dispatch_authority_generation(
                target_trade_date=target_trade_date,
                expected_row_bindings=normalized_bindings,
                _session=session,
            )
            if generation.get('status') == 'valid':
                return {
                    **generation,
                    'blocked_count': 0,
                    'staged_revocation_count': 0,
                }

            issue_by_row_id = {
                int(issue['row_id']): issue
                for issue in generation.get('issues') or []
                if isinstance(issue, dict) and issue.get('row_id') is not None
            }
            rows = session.execute(
                select(SignalDeciderExecutionPlanItem)
                .where(
                    SignalDeciderExecutionPlanItem.id.in_(
                        sorted(normalized_bindings)
                    )
                )
                .order_by(SignalDeciderExecutionPlanItem.id.asc())
                .with_for_update()
            ).scalars().all()
            for row in rows:
                issue = issue_by_row_id.get(int(row.id))
                if issue is None:
                    continue
                reason_code = (
                    _normalize_optional_text(issue.get('reason_code'))
                    or 'current_live_market_evidence_unavailable'
                )
                reason = _dispatch_authority_issue_reason(reason_code)
                submission = (
                    row.submission_payload
                    if isinstance(row.submission_payload, dict)
                    else {}
                )
                exec_dispatch = (
                    submission.get('exec_dispatch')
                    if isinstance(submission.get('exec_dispatch'), dict)
                    else {}
                )
                item_links = exec_dispatch.get('item_links')
                has_exact_remote_acceptance = bool(
                    str(exec_dispatch.get('dispatch_status') or '').strip().lower()
                    == 'accepted'
                    and isinstance(item_links, list)
                    and item_links
                    and all(
                        isinstance(link, dict)
                        and _normalize_optional_text(link.get('core_item_key'))
                        and _normalize_optional_text(link.get('exec_plan_id'))
                        and _normalize_optional_text(link.get('exec_plan_item_id'))
                        for link in item_links
                    )
                )
                if _is_dispatch_terminal_status(row.execution_status) and (
                    has_exact_remote_acceptance
                ):
                    observation_material = '|'.join(
                        (
                            reason_code,
                            str(generation.get('generation_fingerprint') or ''),
                            str(row.id),
                            str(row.decision_snapshot_id),
                        )
                    )
                    self._stage_terminal_revocation(
                        row=row,
                        replacement_snapshot=None,
                        replacement_observation_id=(
                            'observation:dispatch_authority_generation:'
                            f'{hashlib.sha256(observation_material.encode("utf-8")).hexdigest()[:20]}'
                        ),
                        replacement_action=(
                            _normalize_optional_text(
                                issue.get('replacement_action')
                            )
                            or 'reject'
                        ),
                        reason=reason,
                        now=now,
                    )
                    staged_revocation_count += 1
                    continue
                self._set_blocked_row(row=row, reason=reason, now=now)
                blocked_count += 1
            session.flush()
        return {
            **generation,
            'blocked_count': blocked_count,
            'staged_revocation_count': staged_revocation_count,
        }

    async def _get_market_state_async(self) -> MarketState:
        if self._uses_default_market_state_getter:
            return await async_get_market_state()
        result = self.market_state_getter()
        if inspect.isawaitable(result):
            return await result
        return result

    def _build_agent_context_completeness_context(
        self,
        *,
        source_market_date: date,
        candidate_symbols: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        normalized_symbols = sorted(
            {
                str(symbol or '').strip().upper()
                for symbol in (candidate_symbols or [])
                if str(symbol or '').strip()
            }
        )
        try:
            latest_run = agent_context_feedback_run_state_service.find_latest_run(
                market_date=source_market_date,
                requested_symbols=normalized_symbols or None,
            )
            run_id = str(latest_run.get('run_id')).strip() if isinstance(latest_run, dict) and latest_run.get('run_id') else None
            if run_id:
                failure_summary = agent_context_feedback_failure_service.get_grouped_summary(
                    market_date=source_market_date,
                    run_id=run_id,
                    limit=20,
                )
            else:
                failure_summary = {'summary': {'failure_count': 0, 'group_count': 0}, 'groups': []}
        except Exception as exc:  # noqa: BLE001
            return {
                'schema_version': 'agent_context_completeness_v1',
                'source_market_date': source_market_date.isoformat(),
                'status': 'unavailable',
                'has_incomplete_agent_context': None,
                'reason': 'agent_context_completeness_lookup_failed',
                'error_type': type(exc).__name__,
            }

        summary = failure_summary.get('summary') if isinstance(failure_summary, dict) else {}
        groups = failure_summary.get('groups') if isinstance(failure_summary, dict) else []
        failure_groups = [group for group in groups if isinstance(group, dict)][:20] if isinstance(groups, list) else []
        failure_count = int(summary.get('failure_count') or 0) if isinstance(summary, dict) else 0
        run_status = (
            str(latest_run.get('status') or '').strip().lower()
            if isinstance(latest_run, dict)
            else None
        )
        incomplete_run_statuses = {'running', 'failed', 'partial', 'abandoned'}
        has_incomplete_agent_context = bool(failure_count > 0 or run_status in incomplete_run_statuses)
        if has_incomplete_agent_context:
            status = 'incomplete'
        elif latest_run is None:
            status = 'unknown'
        else:
            status = 'complete'
        return {
            'schema_version': 'agent_context_completeness_v1',
            'source_market_date': source_market_date.isoformat(),
            'candidate_symbols': normalized_symbols,
            'candidate_set_basis': 'latest_signal_decider_snapshots',
            'status': status,
            'has_incomplete_agent_context': has_incomplete_agent_context if latest_run is not None or failure_count else None,
            'latest_run': (
                {
                    'run_id': latest_run.get('run_id'),
                    'status': latest_run.get('status'),
                    'symbols_total': latest_run.get('symbols_total'),
                    'completed_count': latest_run.get('completed_count'),
                    'failed_count': latest_run.get('failed_count'),
                    'started_at': latest_run.get('started_at').isoformat()
                    if hasattr(latest_run.get('started_at'), 'isoformat')
                    else latest_run.get('started_at'),
                    'completed_at': latest_run.get('completed_at').isoformat()
                    if hasattr(latest_run.get('completed_at'), 'isoformat')
                    else latest_run.get('completed_at'),
                }
                if isinstance(latest_run, dict)
                else None
            ),
            'failure_count': failure_count,
            'failure_group_count': int(summary.get('group_count') or 0) if isinstance(summary, dict) else 0,
            'by_failure_kind': summary.get('by_failure_kind') if isinstance(summary, dict) else {},
            'failure_groups': failure_groups,
        }

    def list_plan_items(
        self,
        *,
        target_trade_date: str | date | None = None,
        account_reference: str | None = None,
        refresh: bool = False,
    ) -> dict[str, Any]:
        resolved_target_date = self._resolve_target_trade_date(target_trade_date)
        source_market_date = _previous_trading_day(resolved_target_date)
        if refresh:
            self.refresh_plan_items(
                target_trade_date=resolved_target_date,
                trigger='route_refresh',
            )
        rows = self._load_plan_rows(
            target_trade_date=resolved_target_date,
            account_reference=account_reference,
            include_payloads=True,
        )
        agent_context_completeness = self._build_agent_context_completeness_context(
            source_market_date=source_market_date,
            candidate_symbols=[row.symbol for row in rows],
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
                source_market_date=source_market_date,
                agent_context_completeness=agent_context_completeness,
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
        force: bool = False,
    ) -> dict[str, Any]:
        for attempt in range(2):
            try:
                return self._refresh_plan_items_once(
                    target_trade_date=target_trade_date,
                    trigger=trigger,
                    force=force,
                )
            except _PlanRefreshRevisionConflict:
                if attempt == 1:
                    raise RuntimeError(
                        'next-open plan refresh could not bind stable decision inputs'
                    )
                logger.info(
                    'Retrying next-open plan refresh after decision inputs changed: %s',
                    target_trade_date,
                )
        raise AssertionError('unreachable next-open plan refresh retry state')

    def _refresh_plan_items_once(
        self,
        *,
        target_trade_date: str | date | None = None,
        trigger: str = 'manual',
        force: bool = False,
    ) -> dict[str, Any]:
        resolved_target_date = self._resolve_target_trade_date(target_trade_date)
        source_market_date = _previous_trading_day(resolved_target_date)
        runtime = self.runtime_status()
        current_market_state = self.market_state_getter()
        live_market_authority_applies = _current_live_market_authority_applies(
            market_state=current_market_state,
            target_trade_date=resolved_target_date,
        )
        live_market_snapshot_context: dict[str, Any] | None = None
        live_market_snapshot_load_error = False
        if live_market_authority_applies:
            (
                live_market_snapshot_context,
                live_market_snapshot_load_error,
            ) = _load_live_market_snapshot_context()
        live_market_snapshot_fingerprint = _live_market_snapshot_fingerprint(
            live_market_snapshot_context,
            load_error=live_market_snapshot_load_error,
        )
        existing_rows = self._load_plan_rows(
            target_trade_date=resolved_target_date,
            account_reference=None,
            include_payloads=False,
            include_validation_payload=True,
        )
        agent_context_completeness = self._build_agent_context_completeness_context(
            source_market_date=source_market_date,
            candidate_symbols=[row.symbol for row in existing_rows],
        )
        snapshots = self._load_latest_snapshots(source_market_date=source_market_date)
        account_targets, account_target_errors = self._parse_account_targets()
        account_targets_setting_configured = bool(
            settings.signal_decider_next_open_account_targets.strip()
        )
        plan_policy_fingerprint = _next_open_policy_fingerprint(account_targets)
        if (
            existing_rows
            and not force
            and trigger == 'preopen_scheduler'
            and not live_market_authority_applies
            and not account_target_errors
            and all(
                _is_dispatch_terminal_status(row.execution_status)
                for row in existing_rows
            )
            and _preopen_plan_covers_latest_snapshots(
                existing_rows=existing_rows,
                snapshots=snapshots,
                account_targets=account_targets,
            )
        ):
            return {
                'request': {
                    'target_trade_date': resolved_target_date.isoformat(),
                    'trigger': trigger,
                },
                'runtime': runtime,
                'summary': {
                    **self._build_summary(
                        rows=existing_rows,
                        target_trade_date=resolved_target_date,
                        runtime=runtime,
                        source_market_date=source_market_date,
                        agent_context_completeness=agent_context_completeness,
                    ),
                    'reason': 'existing_next_open_plan_reused',
                },
                'enrichment_preflight': None,
                'items': [self._serialize_row(row) for row in existing_rows],
            }
        enrichment_preflight = (
            {
                'market_date': source_market_date.isoformat(),
                'status': 'force_bypassed',
                'allowed': True,
                'failed_categories': [],
                'category_row_counts': {},
            }
            if force
            else self.inspect_source_market_enrichment_preflight(
                source_market_date=source_market_date,
                trigger=trigger,
            )
        )
        enrichment_preflight_blocked = not force and not enrichment_preflight.get('allowed')
        agent_context_completeness = self._build_agent_context_completeness_context(
            source_market_date=source_market_date,
            candidate_symbols=[snapshot.symbol for snapshot in snapshots],
        )
        signal_decider_action_by_symbol = _latest_signal_decider_action_by_symbol(snapshots)
        actionable_snapshot_scopes = {
            (snapshot.symbol, _snapshot_agent_name(snapshot))
            for snapshot in snapshots
            if _is_actionable_snapshot(snapshot)
        }
        latest_snapshot_scopes = {
            (snapshot.symbol, _snapshot_agent_name(snapshot))
            for snapshot in snapshots
        }
        actionable_short_term_symbols = [
            snapshot.symbol for snapshot in snapshots if _is_actionable_short_term_snapshot(snapshot)
        ]
        inspect_candidate_pykrx = (
            not force
            and bool(actionable_short_term_symbols)
            and _should_inspect_short_term_candidate_pykrx(enrichment_preflight)
        )
        candidate_enrichment_by_symbol = (
            self._repair_and_inspect_candidate_pykrx_enrichment(
                source_market_date=source_market_date,
                symbols=actionable_short_term_symbols,
                categories=(
                    enrichment_preflight.get('required_categories')
                    or list(REQUIRED_EXEC_DECIDER_PYKRX_ENRICHMENT_CATEGORIES)
                ),
            )
            if inspect_candidate_pykrx
            else {}
        )
        if enrichment_preflight_blocked and not actionable_short_term_symbols:
            self._block_existing_plan_rows_for_enrichment_preflight(
                target_trade_date=resolved_target_date,
                source_market_date=source_market_date,
                preflight=enrichment_preflight,
            )
            rows = self._load_plan_rows(target_trade_date=resolved_target_date, account_reference=None)
            return {
                'request': {
                    'target_trade_date': resolved_target_date.isoformat(),
                    'trigger': trigger,
                },
                'runtime': runtime,
                'summary': {
                    **self._build_summary(
                        rows=rows,
                        target_trade_date=resolved_target_date,
                        runtime=runtime,
                        source_market_date=source_market_date,
                        enrichment_preflight=enrichment_preflight,
                        agent_context_completeness=agent_context_completeness,
                    ),
                    'reason': 'pykrx_enrichment_preflight_blocked',
                    'enrichment_preflight_allowed': False,
                    'enrichment_preflight_status': enrichment_preflight.get('status'),
                    'enrichment_failed_categories': enrichment_preflight.get('failed_categories') or [],
                },
                'enrichment_preflight': enrichment_preflight,
                'items': [self._serialize_row(row) for row in rows],
            }
        policy_cache: dict[str, dict[str, Any]] = {}
        price_cache: dict[str, int | None] = {}
        desired_keys: set[tuple[str, str, str]] = set()
        now = _utcnow()

        with session_scope() as session:
            session.execute(
                text('SELECT pg_advisory_xact_lock(:lock_key)'),
                {'lock_key': _plan_refresh_advisory_lock_key(resolved_target_date)},
            )
            _lock_dispatch_authority_source_tables(session)
            if (
                live_market_authority_applies
                and live_market_snapshot_context is not None
                and not live_market_snapshot_load_error
            ):
                session.execute(
                    select(CurrentMarketSnapshotCache.id)
                    .where(CurrentMarketSnapshotCache.cache_key == 'dashboard')
                    .with_for_update(read=True)
                ).scalar_one_or_none()
                (
                    current_live_market_snapshot_context,
                    current_live_market_snapshot_load_error,
                ) = _load_live_market_snapshot_context()
                if _live_market_snapshot_fingerprint(
                    current_live_market_snapshot_context,
                    load_error=current_live_market_snapshot_load_error,
                ) != live_market_snapshot_fingerprint:
                    raise _PlanRefreshRevisionConflict
                live_market_snapshot_context = current_live_market_snapshot_context
                live_market_snapshot_load_error = current_live_market_snapshot_load_error
            locked_existing_rows = session.execute(
                select(SignalDeciderExecutionPlanItem)
                .where(
                    SignalDeciderExecutionPlanItem.execution_window
                    == EXECUTION_WINDOW_NEXT_OPEN,
                    SignalDeciderExecutionPlanItem.target_trade_date
                    == resolved_target_date,
                )
                .order_by(SignalDeciderExecutionPlanItem.id.asc())
                .with_for_update()
            ).scalars().all()
            current_snapshots = self._load_latest_snapshots(
                source_market_date=source_market_date
            )
            if _snapshot_cohort_fingerprint(
                current_snapshots
            ) != _snapshot_cohort_fingerprint(snapshots):
                raise _PlanRefreshRevisionConflict
            current_account_targets, current_account_target_errors = (
                self._parse_account_targets()
            )
            if (
                current_account_target_errors != account_target_errors
                or bool(settings.signal_decider_next_open_account_targets.strip())
                != account_targets_setting_configured
                or _next_open_policy_fingerprint(current_account_targets)
                != plan_policy_fingerprint
            ):
                raise _PlanRefreshRevisionConflict
            attached_existing = {
                (
                    row.account_reference,
                    row.symbol,
                    _normalize_agent_name(row.decision_agent_name),
                ): row
                for row in locked_existing_rows
            }
            for snapshot in snapshots:
                decision_agent_name = _snapshot_agent_name(snapshot)
                current_live_market_authority = _current_live_market_buy_authority(
                    snapshot=snapshot,
                    target_trade_date=resolved_target_date,
                    market_state=current_market_state,
                    snapshot_context=live_market_snapshot_context,
                    snapshot_load_error=live_market_snapshot_load_error,
                )
                latest_action = (
                    _normalize_action(snapshot.next_day_open_action or snapshot.decision_action)
                    if decision_agent_name == SIGNAL_DECIDER_AGENT_NAME
                    else _short_term_tactical_action(snapshot)
                )
                actionable = _is_actionable_snapshot(snapshot)
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
                        replacement_snapshot=snapshot,
                        replacement_action=latest_action,
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

                    if row.execution_status in EXECUTION_REVOCATION_LOCKED_STATUSES:
                        continue
                    if row.execution_status == EXECUTION_STATUS_REVOKED:
                        if not self._recover_acknowledged_market_evidence_revocation(
                            row=row,
                            snapshot=snapshot,
                            target=target,
                            current_live_market_authority=current_live_market_authority,
                            now=now,
                        ):
                            continue
                    if _is_dispatch_terminal_status(row.execution_status):
                        planned_side = _normalize_optional_text(row.planned_side)
                        if (
                            planned_side == 'buy'
                            and current_live_market_authority.get('applies') is True
                        ):
                            row.validation_payload = {
                                **(
                                    copy.deepcopy(row.validation_payload)
                                    if isinstance(row.validation_payload, dict)
                                    else {}
                                ),
                                'current_live_market_authority': copy.deepcopy(
                                    current_live_market_authority
                                ),
                            }
                        if int(row.decision_snapshot_id) != int(snapshot.id):
                            self._stage_terminal_revocation(
                                row=row,
                                replacement_snapshot=snapshot,
                                replacement_action=latest_action,
                                reason=_reason(
                                    'latest_snapshot_replaced',
                                    (
                                        f'Latest {source_market_date.isoformat()} {decision_agent_name} '
                                        f'decision snapshot changed from {row.decision_snapshot_id} to {snapshot.id}.'
                                    ),
                                ),
                                now=now,
                            )
                            continue
                        policy_fingerprint = _next_open_policy_fingerprint([target])
                        if (
                            _row_validation_payload(row).get('preopen_policy_fingerprint')
                            != policy_fingerprint
                        ):
                            self._stage_terminal_revocation(
                                row=row,
                                replacement_snapshot=snapshot,
                                replacement_observation_id=(
                                    'observation:policy_scope_changed:'
                                    f'{hashlib.sha256(policy_fingerprint.encode("utf-8")).hexdigest()[:20]}'
                                ),
                                replacement_action=latest_action,
                                reason=_reason(
                                    'policy_scope_changed',
                                    'Current execution policy or account-route budget no longer matches '
                                    'the delegated plan authority.',
                                ),
                                now=now,
                            )
                            continue
                        authority_action = current_live_market_authority.get('action')
                        if (
                            planned_side == 'buy'
                            and authority_action in {'reject', 'block_new_buy'}
                        ):
                            revocation_reason_code = (
                                'current_live_market_risk_off'
                                if authority_action == 'reject'
                                else 'current_live_market_evidence_unavailable'
                            )
                            observation_material = '|'.join(
                                (
                                    revocation_reason_code,
                                    resolved_target_date.isoformat(),
                                    row.account_reference,
                                    row.symbol,
                                    str(current_live_market_authority.get('market') or ''),
                                    str(current_live_market_authority.get('observed_at') or ''),
                                    str(current_live_market_authority.get('reason_code') or ''),
                                )
                            )
                            self._stage_terminal_revocation(
                                row=row,
                                replacement_snapshot=snapshot,
                                replacement_observation_id=(
                                    f'observation:{revocation_reason_code}:'
                                    f'{hashlib.sha256(observation_material.encode("utf-8")).hexdigest()[:20]}'
                                ),
                                replacement_action=latest_action,
                                reason=_reason(
                                    revocation_reason_code,
                                    (
                                        'Fresh authoritative live market breadth is severely weak; '
                                        'unfilled delegated BUY authority must be revoked.'
                                        if authority_action == 'reject'
                                        else 'The open-session live market breadth proof is unavailable, stale, '
                                        'or ambiguous; unfilled delegated BUY authority must be revoked.'
                                    ),
                                ),
                                now=now,
                            )
                            continue
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
                        enrichment_preflight=enrichment_preflight,
                        candidate_enrichment=candidate_enrichment_by_symbol.get(snapshot.symbol),
                        signal_decider_crosscheck=(
                            signal_decider_action_by_symbol.get(snapshot.symbol)
                            if decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME
                            else None
                        ),
                        agent_context_completeness=agent_context_completeness,
                    )
                    if current_live_market_authority.get('applies') is True:
                        validation['current_live_market_authority'] = copy.deepcopy(
                            current_live_market_authority
                        )
                        authority_action = current_live_market_authority.get('action')
                        if authority_action == 'reject':
                            validation['blocking_reasons'] = [
                                *(validation.get('blocking_reasons') or []),
                                _reason(
                                    'current_live_market_risk_off',
                                    'Fresh authoritative live market breadth is severely weak; '
                                    'new BUY authority is blocked.',
                                ),
                            ]
                        elif authority_action != 'allow':
                            validation['blocking_reasons'] = [
                                *(validation.get('blocking_reasons') or []),
                                _reason(
                                    'current_live_market_evidence_unavailable',
                                    'The open-session live market breadth proof is unavailable, stale, '
                                    'or ambiguous; new BUY authority fails closed.',
                                ),
                            ]
                    if account_target_blocking_reasons:
                        validation['blocking_reasons'] = [
                            *(validation.get('blocking_reasons') or []),
                            *account_target_blocking_reasons,
                        ]
                    if enrichment_preflight_blocked:
                        self._apply_enrichment_preflight_to_validation(
                            validation=validation,
                            decision_agent_name=decision_agent_name,
                            source_market_date=source_market_date,
                            preflight=enrichment_preflight,
                        )
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
                if (
                    (row.symbol, _normalize_agent_name(row.decision_agent_name))
                    in latest_snapshot_scopes
                    and (
                        row.symbol,
                        _normalize_agent_name(row.decision_agent_name),
                    )
                    not in actionable_snapshot_scopes
                ):
                    continue
                removal_code = (
                    'account_route_removed'
                    if (row.symbol, _normalize_agent_name(row.decision_agent_name))
                    in actionable_snapshot_scopes
                    else 'candidate_removed'
                )
                removal_reason = _reason(
                    removal_code,
                    (
                        f'Configured account route no longer permits {row.decision_agent_name} '
                        f'for {row.symbol}.'
                        if removal_code == 'account_route_removed'
                        else (
                            f'No latest decision candidate remains for {row.decision_agent_name} '
                            f'on this account and symbol.'
                        )
                    ),
                )
                if _is_dispatch_terminal_status(row.execution_status):
                    observation_material = '|'.join(
                        (
                            removal_code,
                            source_market_date.isoformat(),
                            resolved_target_date.isoformat(),
                            row.account_reference,
                            row.symbol,
                            _normalize_agent_name(row.decision_agent_name),
                        )
                    )
                    observation_id = (
                        f'observation:{removal_code}:'
                        f'{hashlib.sha256(observation_material.encode("utf-8")).hexdigest()[:20]}'
                    )
                    self._stage_terminal_revocation(
                        row=row,
                        replacement_snapshot=None,
                        replacement_observation_id=observation_id,
                        replacement_action='reject',
                        reason=removal_reason,
                        now=now,
                    )
                    continue
                if (
                    row.execution_status in EXECUTION_REVOCATION_LOCKED_STATUSES
                    or row.execution_status == EXECUTION_STATUS_REVOKED
                ):
                    continue
                self._set_blocked_row(
                    row=row,
                    reason=removal_reason,
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
                enrichment_preflight=enrichment_preflight,
                agent_context_completeness=agent_context_completeness,
                reason='pykrx_enrichment_preflight_blocked' if enrichment_preflight_blocked else None,
            ),
            'enrichment_preflight': enrichment_preflight,
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
        market_state = await self._get_market_state_async()
        effective_dry_run = settings.signal_decider_next_open_dry_run if dry_run is None else dry_run
        runtime = self.runtime_status()
        if not force and not effective_dry_run and _calendar_requires_operator_override(market_state):
            default_date = date.fromisoformat(market_state.server_time[:10])
            resolved_target_date = await async_normalize_next_open_target_trade_date(
                target_trade_date,
                default=default_date,
                allow_fallback=True,
            )
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
                    'calendar_source': getattr(market_state, 'calendar_source', None),
                    'calendar_source_status': getattr(market_state, 'calendar_source_status', None),
                    'calendar_source_error': getattr(market_state, 'calendar_source_error', None),
                },
                'summary': {
                    'triggered': False,
                    'reason': 'calendar_unavailable',
                    'selected_count': 0,
                    'submitted_count': 0,
                    'dry_run_count': 0,
                    'failed_count': 0,
                    'idempotent_replay_count': 0,
                    'calendar_source': getattr(market_state, 'calendar_source', None),
                    'calendar_source_status': getattr(market_state, 'calendar_source_status', None),
                },
                'items': [],
            }
        resolved_target_date = await self._resolve_target_trade_date_async(
            target_trade_date,
            market_state=market_state,
        )
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
                        'calendar_source': getattr(market_state, 'calendar_source', None),
                        'calendar_source_status': getattr(market_state, 'calendar_source_status', None),
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
                        'calendar_source': getattr(market_state, 'calendar_source', None),
                        'calendar_source_status': getattr(market_state, 'calendar_source_status', None),
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
                        'calendar_source': getattr(market_state, 'calendar_source', None),
                        'calendar_source_status': getattr(market_state, 'calendar_source_status', None),
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

        refresh_result = await asyncio.to_thread(
            self.refresh_plan_items,
            target_trade_date=resolved_target_date,
            trigger=f'{trigger}_prepare',
            force=force,
        )
        enrichment_preflight = refresh_result.get('enrichment_preflight') if isinstance(refresh_result, dict) else None
        if (
            not force
            and isinstance(enrichment_preflight, dict)
            and not enrichment_preflight.get('allowed')
        ):
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
                    'calendar_source': getattr(market_state, 'calendar_source', None),
                    'calendar_source_status': getattr(market_state, 'calendar_source_status', None),
                },
                'summary': {
                    'triggered': False,
                    'reason': 'pykrx_enrichment_preflight_blocked',
                    'selected_count': 0,
                    'submitted_count': 0,
                    'dry_run_count': 0,
                    'failed_count': 0,
                    'idempotent_replay_count': 0,
                    'enrichment_preflight_status': enrichment_preflight.get('status'),
                    'enrichment_failed_categories': enrichment_preflight.get('failed_categories') or [],
                    'refresh_summary': refresh_result.get('summary') if isinstance(refresh_result, dict) else None,
                },
                'enrichment_preflight': enrichment_preflight,
                'items': refresh_result.get('items') if isinstance(refresh_result, dict) else [],
            }
        candidate_rows = self._load_plan_rows(
            target_trade_date=resolved_target_date,
            account_reference=None,
        )
        candidate_rows = [
            row
            for row in candidate_rows
            if _normalize_agent_name(row.decision_agent_name) == SIGNAL_DECIDER_AGENT_NAME
        ]
        core_local_execution_reason = (
            'exec_dispatch_owns_submission'
            if _exec_agent_sizing_dispatch_enabled()
            else 'core_local_execution_disabled'
        )
        # Core has no execution-time executable quote/cost recheck. It may
        # prepare plans for Exec, but it must never submit a next-open buy
        # through the legacy local order path in any integration mode.
        candidate_rows = []
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
                'calendar_source': getattr(market_state, 'calendar_source', None),
                'calendar_source_status': getattr(market_state, 'calendar_source_status', None),
            },
            'summary': {
                'triggered': False,
                'reason': core_local_execution_reason,
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
        allow_fallback: bool = True,
    ) -> date:
        state = market_state or self.market_state_getter()
        default_date = date.fromisoformat(state.server_time[:10])
        return normalize_next_open_target_trade_date(
            target_trade_date,
            default=default_date,
            allow_fallback=allow_fallback,
        )

    async def _resolve_target_trade_date_async(
        self,
        target_trade_date: str | date | None,
        *,
        market_state: MarketState | None = None,
        allow_fallback: bool = True,
    ) -> date:
        state = market_state or await self._get_market_state_async()
        default_date = date.fromisoformat(state.server_time[:10])
        return await async_normalize_next_open_target_trade_date(
            target_trade_date,
            default=default_date,
            allow_fallback=allow_fallback,
        )

    def inspect_source_market_enrichment_preflight(
        self,
        *,
        source_market_date: date,
        trigger: str,
    ) -> dict[str, Any]:
        preflight = inspect_pykrx_enrichment_preflight(market_date=source_market_date)
        log_payload = {
            'source_market_date': source_market_date.isoformat(),
            'trigger': trigger,
            'status': preflight.get('status'),
            'allowed': bool(preflight.get('allowed')),
            'category_row_counts': preflight.get('category_row_counts') or {},
            'failed_categories': preflight.get('failed_categories') or [],
            'zero_coverage_categories': preflight.get('zero_coverage_categories') or [],
        }
        if preflight.get('allowed'):
            logger.info(
                'Next-open PyKRX enrichment preflight allowed downstream use',
                extra={'pykrx_enrichment_preflight': log_payload},
            )
        else:
            logger.warning(
                'Next-open PyKRX enrichment preflight blocked downstream use',
                extra={'pykrx_enrichment_preflight': log_payload},
            )
        return preflight

    def _block_existing_plan_rows_for_enrichment_preflight(
        self,
        *,
        target_trade_date: date,
        source_market_date: date,
        preflight: dict[str, Any],
        decision_agent_names: Sequence[str] | None = None,
    ) -> None:
        now = _utcnow()
        normalized_agent_names = (
            {_normalize_agent_name(name) for name in decision_agent_names}
            if decision_agent_names is not None
            else None
        )
        reason = _reason(
            'pykrx_enrichment_preflight_blocked',
            (
                'PyKRX enrichment coverage is not ready for source_market_date '
                f'{source_market_date.isoformat()}; failed categories: '
                f'{", ".join(preflight.get("failed_categories") or []) or "unknown"}.'
            ),
        )
        with session_scope() as session:
            rows = session.execute(
                select(SignalDeciderExecutionPlanItem)
                .where(
                    SignalDeciderExecutionPlanItem.execution_window == EXECUTION_WINDOW_NEXT_OPEN,
                    SignalDeciderExecutionPlanItem.target_trade_date == target_trade_date,
                    SignalDeciderExecutionPlanItem.source_market_date == source_market_date,
                )
            ).scalars().all()
            for row in rows:
                if (
                    normalized_agent_names is not None
                    and _normalize_agent_name(row.decision_agent_name) not in normalized_agent_names
                ):
                    continue
                if (
                    _is_dispatch_terminal_status(row.execution_status)
                    or row.execution_status in EXECUTION_REVOCATION_LOCKED_STATUSES
                    or row.execution_status == EXECUTION_STATUS_REVOKED
                ):
                    continue
                existing = row.validation_payload if isinstance(row.validation_payload, dict) else {}
                existing_reasons = [
                    item
                    for item in (existing.get('blocking_reasons') or [])
                    if isinstance(item, dict) and item.get('code') != reason['code']
                ]
                row.eligibility_status = ELIGIBILITY_BLOCKED
                row.execution_status = EXECUTION_STATUS_PLANNED
                row.last_evaluated_at = now
                row.validation_payload = {
                    **existing,
                    'blocking_reasons': [*existing_reasons, reason],
                    'warnings': existing.get('warnings') or [],
                    'pykrx_enrichment_preflight': preflight,
                }
            session.flush()

    def block_existing_plan_rows_for_enrichment_preflight(
        self,
        *,
        target_trade_date: date,
        source_market_date: date,
        preflight: dict[str, Any],
        decision_agent_names: Sequence[str] | None = None,
    ) -> None:
        self._block_existing_plan_rows_for_enrichment_preflight(
            target_trade_date=target_trade_date,
            source_market_date=source_market_date,
            preflight=preflight,
            decision_agent_names=decision_agent_names,
        )

    def _inspect_candidate_pykrx_enrichment(
        self,
        *,
        source_market_date: date,
        symbols: Sequence[str],
        categories: Sequence[str],
    ) -> dict[str, dict[str, Any]]:
        ordered_symbols = list(dict.fromkeys(symbol for symbol in symbols if symbol))
        ordered_categories = list(dict.fromkeys(category for category in categories if category))
        if not ordered_symbols or not ordered_categories:
            return {}
        rows = load_daily_pykrx_enrichment_rows(
            market_date=source_market_date,
            categories=ordered_categories,
            symbols=ordered_symbols,
            limit=max(len(ordered_symbols) * len(ordered_categories), 1),
        )
        present_by_symbol: dict[str, set[str]] = {symbol: set() for symbol in ordered_symbols}
        for row in rows:
            symbol = _normalize_optional_text(row.get('symbol'))
            category = _normalize_optional_text(row.get('category'))
            if symbol in present_by_symbol and category in ordered_categories:
                present_by_symbol[symbol].add(category)
        result: dict[str, dict[str, Any]] = {}
        for symbol in ordered_symbols:
            present_categories = sorted(present_by_symbol.get(symbol) or set())
            missing_categories = [
                category for category in ordered_categories if category not in set(present_categories)
            ]
            result[symbol] = {
                'market_date': source_market_date.isoformat(),
                'required_categories': ordered_categories,
                'present_categories': present_categories,
                'missing_categories': missing_categories,
                'allowed': not missing_categories,
                'source': 'candidate_symbol_pykrx_enrichment',
            }
        return result

    def _repair_and_inspect_candidate_pykrx_enrichment(
        self,
        *,
        source_market_date: date,
        symbols: Sequence[str],
        categories: Sequence[str],
    ) -> dict[str, dict[str, Any]]:
        initial = self._inspect_candidate_pykrx_enrichment(
            source_market_date=source_market_date,
            symbols=symbols,
            categories=categories,
        )
        missing_categories = sorted(
            {
                category
                for item in initial.values()
                for category in (item.get('missing_categories') or [])
                if category in {'investor_flow_symbol', 'short_selling'}
            }
        )
        missing_symbols = sorted(
            {
                symbol
                for symbol, item in initial.items()
                if any(category in missing_categories for category in (item.get('missing_categories') or []))
            }
        )
        if missing_symbols and missing_categories:
            from app.eod.pykrx_enrichment import pykrx_enrichment_service

            pykrx_enrichment_service.collect_for_market_date(
                market_date=source_market_date,
                markets=['KOSPI', 'KOSDAQ', 'KONEX'],
                symbols=missing_symbols,
                categories=missing_categories,
                allow_kis_candidate_fallback=True,
            )
        return self._inspect_candidate_pykrx_enrichment(
            source_market_date=source_market_date,
            symbols=symbols,
            categories=categories,
        )

    def _apply_enrichment_preflight_to_validation(
        self,
        *,
        validation: dict[str, Any],
        decision_agent_name: str,
        source_market_date: date,
        preflight: dict[str, Any],
    ) -> None:
        reason = _reason(
            'pykrx_enrichment_preflight_blocked',
            (
                'PyKRX enrichment coverage is not ready for source_market_date '
                f'{source_market_date.isoformat()}; failed categories: '
                f'{", ".join(preflight.get("failed_categories") or []) or "unknown"}.'
            ),
        )
        existing_block_codes = {
            item.get('code')
            for item in validation.get('blocking_reasons') or []
            if isinstance(item, dict)
        }
        existing_warning_codes = {
            item.get('code')
            for item in validation.get('warnings') or []
            if isinstance(item, dict)
        }
        if decision_agent_name == SIGNAL_DECIDER_AGENT_NAME:
            if reason['code'] not in existing_block_codes:
                validation['blocking_reasons'] = [
                    *(validation.get('blocking_reasons') or []),
                    reason,
                ]
        elif reason['code'] not in existing_warning_codes:
            validation['warnings'] = [
                *(validation.get('warnings') or []),
                {
                    **reason,
                    'message': (
                        f"{reason['message']} Candidate remains visible for conditional "
                        'exec monitoring; candidate-level trigger/chase/risk validation still applies.'
                    ),
                },
            ]
        validation['pykrx_enrichment_preflight'] = preflight

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
            snapshots = session.execute(
                select(SignalDeciderDecisionSnapshot)
                .where(
                    SignalDeciderDecisionSnapshot.agent_name.in_(
                        [SIGNAL_DECIDER_AGENT_NAME, SHORT_TERM_DECIDER_AGENT_NAME]
                    ),
                    SignalDeciderDecisionSnapshot.market_date == source_market_date,
                )
                .order_by(
                    SignalDeciderDecisionSnapshot.created_at.desc(),
                    SignalDeciderDecisionSnapshot.id.desc(),
                )
            ).scalars().all()
            run_rows = session.execute(
                select(AgentContextFeedbackRun)
                .where(AgentContextFeedbackRun.market_date == source_market_date)
                .order_by(
                    AgentContextFeedbackRun.started_at.desc(),
                    AgentContextFeedbackRun.id.desc(),
                )
            ).scalars().all()
            run_snapshots = [
                agent_context_feedback_run_state_service._serialize_run(row)
                for row in run_rows
            ]
            for snapshot in snapshots:
                session.expunge(snapshot)

        snapshots_by_run_key: dict[
            tuple[str, tuple[str, str]],
            SignalDeciderDecisionSnapshot,
        ] = {}
        for snapshot in snapshots:
            run_id = str(snapshot.decision_run_id or '').strip()
            if not run_id:
                continue
            key = (snapshot.symbol.upper(), _snapshot_agent_name(snapshot))
            snapshots_by_run_key.setdefault((run_id, key), snapshot)

        selected_by_key: dict[
            tuple[str, str],
            SignalDeciderDecisionSnapshot | None,
        ] = {}
        explicitly_scoped_run_ids: set[str] = set()
        relevant_agents = {SIGNAL_DECIDER_AGENT_NAME, SHORT_TERM_DECIDER_AGENT_NAME}
        for run in run_snapshots:
            run_id = str(run.get('run_id') or '').strip()
            if not run_id:
                continue
            status = str(run.get('status') or '').strip().lower()
            if status == 'abandoned' or agent_context_feedback_run_state_service._is_stale_running_row(run):
                explicitly_scoped_run_ids.add(run_id)
                continue
            request_payload = run.get('request_payload') if isinstance(run.get('request_payload'), dict) else {}
            raw_role_scopes = request_payload.get('role_symbol_scopes')
            role_scopes: dict[str, list[str]] = {}
            if isinstance(raw_role_scopes, dict):
                for raw_role, raw_symbols in raw_role_scopes.items():
                    agent_name = _normalize_agent_name(raw_role)
                    if agent_name not in relevant_agents or not isinstance(raw_symbols, list):
                        continue
                    role_scopes[agent_name] = sorted(
                        {
                            str(symbol or '').strip().upper()
                            for symbol in raw_symbols
                            if str(symbol or '').strip()
                        }
                    )
            if not role_scopes:
                requested_symbols = agent_context_feedback_run_state_service.resolve_run_requested_symbols(
                    run_snapshot=run
                )
                raw_roles = request_payload.get('roles')
                if requested_symbols is not None and isinstance(raw_roles, list):
                    for raw_role in raw_roles:
                        agent_name = _normalize_agent_name(raw_role)
                        if agent_name in relevant_agents:
                            role_scopes[agent_name] = requested_symbols
            if not role_scopes:
                continue
            explicitly_scoped_run_ids.add(run_id)
            for agent_name, symbols in role_scopes.items():
                for symbol in symbols:
                    key = (symbol, agent_name)
                    if key in selected_by_key:
                        continue
                    # A requested-but-missing result is a deliberate tombstone: never
                    # mix an older BUY into this newer run's partial/failure cohort.
                    selected_by_key[key] = snapshots_by_run_key.get((run_id, key))

        for snapshot in snapshots:
            key = (snapshot.symbol.upper(), _snapshot_agent_name(snapshot))
            if key in selected_by_key:
                continue
            run_id = str(snapshot.decision_run_id or '').strip()
            if run_id and run_id in explicitly_scoped_run_ids:
                continue
            selected_by_key[key] = snapshot

        return sorted(
            (snapshot for snapshot in selected_by_key.values() if snapshot is not None),
            key=lambda snapshot: (snapshot.symbol, _snapshot_agent_name(snapshot), snapshot.id),
        )

    def _load_plan_rows(
        self,
        *,
        target_trade_date: date,
        account_reference: str | None,
        include_payloads: bool = True,
        include_validation_payload: bool = False,
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
            if not include_payloads:
                deferred_payloads = [
                    defer(SignalDeciderExecutionPlanItem.traceability_payload),
                    defer(SignalDeciderExecutionPlanItem.submission_payload),
                ]
                if not include_validation_payload:
                    deferred_payloads.append(defer(SignalDeciderExecutionPlanItem.validation_payload))
                stmt = stmt.options(*deferred_payloads)
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
        enrichment_preflight: dict[str, Any] | None = None,
        candidate_enrichment: dict[str, Any] | None = None,
        signal_decider_crosscheck: dict[str, Any] | None = None,
        agent_context_completeness: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        decision_agent_name = _snapshot_agent_name(snapshot)
        response_payload = _snapshot_response_payload(snapshot)
        pre_decider_enrichment_context = _snapshot_pre_decider_enrichment_context(snapshot)
        market_regime_context = _snapshot_market_regime_context(snapshot)
        short_term_tactical_action = (
            _short_term_tactical_action(snapshot)
            if decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME
            else None
        )
        generic_action = (
            _short_term_generic_action(short_term_tactical_action)
            if short_term_tactical_action is not None
            else _normalize_action(snapshot.next_day_open_action or snapshot.decision_action)
        )
        handoff_intent = (
            _short_term_handoff_intent(short_term_tactical_action)
            if short_term_tactical_action is not None
            else 'next_open_buy'
            if generic_action == 'buy'
            else None
        )
        alignment_gate_payload: dict[str, Any] | None = None
        blocking_reasons: list[dict[str, str]] = []
        warnings: list[dict[str, str]] = []
        min_confidence_threshold = _to_decimal(
            settings.signal_decider_next_open_min_confidence_score_10
        )
        min_expected_return_threshold = _to_decimal(
            settings.signal_decider_next_open_min_expected_return_pct
        )
        threshold_configuration_invalid = bool(
            min_confidence_threshold is None
            or min_confidence_threshold < Decimal('0')
            or min_confidence_threshold > Decimal('10')
            or min_expected_return_threshold is None
            or min_expected_return_threshold < Decimal('0')
        )
        if threshold_configuration_invalid:
            blocking_reasons.append(
                _reason(
                    'next_open_threshold_configuration_invalid',
                    'Next-open confidence or expected-return threshold is invalid; automated buy planning is disabled.',
                )
            )
        gate_checks = _snapshot_traceability_payload(snapshot).get('gate_checks') or {}
        missing_upstream_roles = _normalize_text_list(gate_checks.get('missing_upstream_roles'))
        if missing_upstream_roles:
            blocking_reasons.append(
                _reason(
                    'missing_upstream_roles',
                    f'Missing upstream roles: {", ".join(missing_upstream_roles)}.',
                )
            )
        agent_context_completeness_payload = (
            agent_context_completeness
            if isinstance(agent_context_completeness, dict)
            else {}
        )
        if agent_context_completeness_payload.get('has_incomplete_agent_context') is True:
            failure_count = int(agent_context_completeness_payload.get('failure_count') or 0)
            latest_run = (
                agent_context_completeness_payload.get('latest_run')
                if isinstance(agent_context_completeness_payload.get('latest_run'), dict)
                else {}
            )
            warnings.append(
                _reason(
                    'agent_context_incomplete_candidate_set',
                    (
                        'Candidate set is based on incomplete agent context'
                        f" from run {latest_run.get('run_id') or 'unknown'}"
                        f" ({latest_run.get('status') or agent_context_completeness_payload.get('status')});"
                        f' grouped failure count: {failure_count}.'
                    ),
                )
            )

        confidence_score = _to_decimal(snapshot.confidence_score_10)
        if (
            confidence_score is None
            or min_confidence_threshold is None
            or confidence_score < min_confidence_threshold
        ):
            blocking_reasons.append(
                _reason(
                    'confidence_below_threshold',
                    'Confidence score is below the next-open automation threshold.',
                )
            )
        data_quality_warnings = _normalize_text_list(snapshot.data_quality_warnings_payload)
        if settings.signal_decider_next_open_block_on_data_warnings and data_quality_warnings:
            blocking_reasons.append(
                _reason(
                    'data_quality_warnings_present',
                    'Snapshot has data quality warnings that Exec cannot independently revalidate.',
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
        if decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME and candidate_enrichment is not None:
            missing_categories = (
                _normalize_text_list(candidate_enrichment.get('missing_categories'))
                if isinstance(candidate_enrichment, dict)
                else []
            )
            if missing_categories:
                blocking_reasons.append(
                    _reason(
                        'candidate_pykrx_enrichment_missing',
                        (
                            'Short-term candidate is missing required symbol-level PyKRX enrichment categories: '
                            f'{", ".join(str(item) for item in missing_categories)}.'
                        ),
                    )
                )
            elif isinstance(candidate_enrichment, dict) and candidate_enrichment.get('allowed'):
                if _should_inspect_short_term_candidate_pykrx(enrichment_preflight):
                    warnings.append(
                        _reason(
                            'global_pykrx_enrichment_preflight_partial',
                            (
                                'Global PyKRX enrichment coverage is partial, but required symbol-level enrichment '
                                'is present for this short-term candidate.'
                            ),
                        )
                    )
            else:
                blocking_reasons.append(
                    _reason(
                        'candidate_pykrx_enrichment_unavailable',
                        'Could not verify required symbol-level PyKRX enrichment for this short-term candidate.',
                    )
                )
        if enrichment_preflight is not None and not enrichment_preflight.get('allowed'):
            failed_categories = enrichment_preflight.get('failed_categories') or []
            if decision_agent_name == SIGNAL_DECIDER_AGENT_NAME:
                blocking_reasons.append(
                    _reason(
                        'pykrx_enrichment_preflight_blocked',
                        (
                            'PyKRX enrichment coverage is not ready for source_market_date '
                            f'{snapshot.market_date.isoformat()}; failed categories: '
                            f'{", ".join(failed_categories) or "unknown"}.'
                        ),
                    )
                )
            elif decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME and candidate_enrichment is None:
                blocking_reasons.append(
                    _reason(
                        'candidate_pykrx_enrichment_unavailable',
                        'Could not verify required symbol-level PyKRX enrichment for this short-term candidate.',
                    )
                )

        estimated_reference_price_basis = 'latest_source_market_close'
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
        if decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME and (
            estimated_reference_price is None or estimated_reference_price <= 0
        ):
            estimated_reference_price = _to_int(response_payload.get('trigger_price')) or _to_int(
                response_payload.get('chase_limit_price')
            )
            price_cache[snapshot.symbol] = estimated_reference_price
            estimated_reference_price_basis = 'short_term_trigger_price'

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
            if (
                expected_return is None
                or min_expected_return_threshold is None
                or expected_return < min_expected_return_threshold
            ):
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
            entry_quality_blocks, entry_quality_warnings, entry_quality_snapshot = _short_term_entry_quality_blocks(
                snapshot=snapshot,
                signal_decider_crosscheck=signal_decider_crosscheck,
                tactical_action=short_term_tactical_action,
            )
            entry_quality_blocks, entry_quality_warnings = _delegate_short_term_watch_entry_quality_blocks_to_warnings(
                tactical_action=short_term_tactical_action,
                handoff_intent=handoff_intent,
                blocks=entry_quality_blocks,
                warnings=entry_quality_warnings,
            )
            blocking_reasons.extend(entry_quality_blocks)
            warnings.extend(entry_quality_warnings)
            alignment_gate_payload = build_signal_decider_alignment_gate(
                signal_decider_context=signal_decider_crosscheck,
                short_term_entry_quality=entry_quality_snapshot,
                tactical_action=short_term_tactical_action,
                mode=settings.signal_decider_wait_conflict_gate_mode,
                expected_market_date=snapshot.market_date,
            )
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
                blocking_reasons.append(
                    _reason(
                        'target_price_missing',
                        'Short-term dispatch requires target_price so exec can attach a guarded target monitor.',
                    )
                )
            if stop_loss_price is None:
                blocking_reasons.append(
                    _reason(
                        'stop_loss_price_missing',
                        'Short-term dispatch requires stop_loss_price so exec can attach a guarded stop monitor.',
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
                if Decimal(str(estimated_reference_price)) > chase_limit_price:
                    blocking_reasons.append(
                        _reason(
                            'reference_price_above_chase_limit',
                            'Latest available reference price is already above short-term chase_limit_price.',
                        )
                    )
                estimated_reference_price = max(int(estimated_reference_price), int(chase_limit_price))
                estimated_reference_price_basis = 'max(previous_close,chase_limit_price)'
            if bool(gate_checks.get('decision_support_only')) or not bool(gate_checks.get('allow_execution_logic', False)):
                warnings.append(
                    _reason(
                        'external_execution_override',
                        'Short-term decisions remain decision-support-only in core; exec dispatch is a guarded external opt-in.',
                    )
                )

        if decision_agent_name == SIGNAL_DECIDER_AGENT_NAME and _uses_exec_agent_sizing_for_dispatch(decision_agent_name):
            if estimated_reference_price is None or estimated_reference_price <= 0:
                warnings.append(
                    _reason(
                        'missing_reference_price',
                        'Could not resolve the prior close price for exec-agent sizing context.',
                    )
                )
            planned_quantity = None
            estimated_notional = None
            pricing_note = 'Sizing for signal-decider dispatch is delegated to exec-agent.'
        elif decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME:
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

        if decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME and alignment_gate_payload is not None:
            requested_quantity = planned_quantity
            if (
                requested_quantity is None
                and target.account_reference == UNSPECIFIED_ROUTE_ACCOUNT_REFERENCE
                and not settings.signal_decider_next_open_account_targets.strip()
                and alignment_gate_payload.get('alignment_gate_action') == ALIGNMENT_GATE_ACTION_DOWNSIZE_PROBE
            ):
                requested_quantity = settings.signal_decider_wait_conflict_probe_quantity
            alignment_gate_payload = apply_alignment_quantity_enforcement(
                alignment_gate_payload,
                requested_quantity=requested_quantity,
                probe_quantity=settings.signal_decider_wait_conflict_probe_quantity,
            )
            alignment = alignment_gate_payload.get('signal_decider_alignment')
            action = alignment_gate_payload.get('alignment_gate_action')
            reason_text = (
                alignment_gate_payload.get('alignment_gate_reason')
                or 'Signal-decider alignment could not approve this short-term buy candidate.'
            )
            unavailable_context = alignment in {
                SIGNAL_DECIDER_ALIGNMENT_NO_CONTEXT,
                SIGNAL_DECIDER_ALIGNMENT_STALE_CONTEXT,
            }
            if action == ALIGNMENT_GATE_ACTION_BLOCK:
                blocking_reasons.append(
                    _reason(
                        'signal_decider_context_unavailable_gate'
                        if unavailable_context
                        else 'signal_decider_wait_conflict_gate',
                        reason_text,
                    )
                )
                planned_quantity = Decimal('0')
                estimated_notional = Decimal('0')
            elif action == ALIGNMENT_GATE_ACTION_DOWNSIZE_PROBE:
                warnings.append(
                    _reason(
                        'signal_decider_context_unavailable_gate_downsized'
                        if unavailable_context
                        else 'signal_decider_wait_conflict_gate_downsized',
                        reason_text,
                    )
                )
                planned_quantity = _to_decimal(alignment_gate_payload.get('final_quantity'))
                estimated_notional = (
                    planned_quantity * Decimal(str(estimated_reference_price))
                    if planned_quantity is not None and estimated_reference_price is not None
                    else None
                )
                pricing_note = 'Core enforced a deterministic WAIT-conflict probe quantity before exec handoff.'
            elif action == ALIGNMENT_GATE_ACTION_REPORT_ONLY_BLOCK:
                warnings.append(
                    _reason(
                        'signal_decider_context_unavailable_gate_report_only'
                        if unavailable_context
                        else 'signal_decider_wait_conflict_gate_report_only',
                        reason_text,
                    )
                )
            elif alignment == SIGNAL_DECIDER_ALIGNMENT_WAIT_CONFLICT_OVERRIDE:
                warnings.append(
                    _reason(
                        'signal_decider_wait_conflict_override',
                        reason_text,
                    )
                )

        decision_input_quality_context = _snapshot_decision_input_quality_context(
            snapshot,
            target=target,
            estimated_reference_price=estimated_reference_price,
            estimated_notional_krw=estimated_notional,
        )
        execution_feasibility_context = (
            decision_input_quality_context.get('execution_feasibility')
            if isinstance(decision_input_quality_context.get('execution_feasibility'), dict)
            else {}
        )
        performance_gate: dict[str, Any] = {'status': 'not_applicable'}
        if decision_agent_name == SIGNAL_DECIDER_AGENT_NAME:
            dispatches_to_exec = _uses_exec_agent_sizing_for_dispatch(decision_agent_name)
            expected_slippage_bps, expected_slippage_basis = _next_open_performance_cost_evidence(
                dispatches_to_exec=dispatches_to_exec,
                execution_feasibility_context=execution_feasibility_context,
                pre_decider_enrichment_context=pre_decider_enrichment_context,
            )
            allow_pending_live_cost_confirmation = bool(
                dispatches_to_exec
                and _is_canonical_closed_market_cost_absence(
                    pre_decider_enrichment_context=pre_decider_enrichment_context,
                    execution_feasibility_context=execution_feasibility_context,
                )
            )
            performance_gate = evaluate_next_open_performance_gate(
                request_payload=_snapshot_request_payload(snapshot),
                decision_action=generic_action,
                confidence_score_10=snapshot.confidence_score_10,
                expected_return_pct=snapshot.expected_return_pct,
                reference_price=estimated_reference_price,
                target_price=snapshot.target_price,
                stop_loss_price=snapshot.stop_loss_price,
                expected_slippage_bps=expected_slippage_bps,
                expected_slippage_basis=expected_slippage_basis,
                allow_pending_live_cost_confirmation=allow_pending_live_cost_confirmation,
                server_non_spread_round_trip_cost_bps=(
                    None
                    if dispatches_to_exec
                    else settings.signal_decider_next_open_non_spread_round_trip_cost_bps
                ),
                defer_non_spread_round_trip_cost_to_exec=dispatches_to_exec,
                base_min_confidence_score_10=settings.signal_decider_next_open_min_confidence_score_10,
                base_min_expected_return_pct=settings.signal_decider_next_open_min_expected_return_pct,
                base_min_reward_risk_ratio=settings.signal_decider_next_open_min_reward_risk_ratio,
            )
            blocking_reasons.extend(
                reason
                for reason in performance_gate.get('blocking_reasons', [])
                if isinstance(reason, dict)
            )
            warnings.extend(
                warning
                for warning in performance_gate.get('warnings', [])
                if isinstance(warning, dict)
            )

        quality_hard_blocks = (
            ((decision_input_quality_context.get('final_judgment') or {}).get('hard_blocks') or [])
            if isinstance(decision_input_quality_context, dict)
            else []
        )
        existing_block_codes = {str(reason.get('code')) for reason in blocking_reasons if isinstance(reason, dict)}
        for hard_block in quality_hard_blocks:
            if not isinstance(hard_block, dict):
                continue
            code = f"decision_input_quality_{hard_block.get('code') or 'hard_block'}"
            if code in existing_block_codes:
                continue
            if _should_relax_short_term_decision_input_hard_block(
                decision_agent_name=decision_agent_name,
                handoff_intent=handoff_intent,
                hard_block=hard_block,
                warnings=warnings,
            ):
                continue
            raw_reasons = hard_block.get('reasons') if isinstance(hard_block.get('reasons'), list) else []
            reason_text = '; '.join(str(reason) for reason in raw_reasons if str(reason).strip())
            blocking_reasons.append(
                _reason(
                    code,
                    reason_text
                    or f"Decision input quality hard block: {hard_block.get('category') or hard_block.get('code') or 'unspecified'}.",
                )
            )
            existing_block_codes.add(code)
        validation_payload = {
            'blocking_reasons': blocking_reasons,
            'warnings': warnings,
            'decision_agent_name': decision_agent_name,
            'raw_action': short_term_tactical_action or generic_action,
            'tactical_action': short_term_tactical_action,
            'generic_action': generic_action,
            'handoff_intent': handoff_intent,
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
                'requested_quantity': (
                    alignment_gate_payload.get('requested_quantity')
                    if alignment_gate_payload is not None
                    else _decimal_to_text(planned_quantity)
                ),
                'final_quantity': (
                    alignment_gate_payload.get('final_quantity')
                    if alignment_gate_payload is not None
                    else _decimal_to_text(planned_quantity)
                ),
            },
            'performance_gate': performance_gate,
            'agent_context_completeness': agent_context_completeness_payload,
            'pre_decider_enrichment_context': pre_decider_enrichment_context,
            'market_regime_context': market_regime_context,
            'decision_input_quality_context': decision_input_quality_context,
            'preopen_policy_fingerprint': _next_open_policy_fingerprint([target]),
            'handoff': {
                'decision_agent_name': decision_agent_name,
                'raw_action': short_term_tactical_action or generic_action,
                'tactical_action': short_term_tactical_action,
                'generic_action': generic_action,
                'handoff_intent': handoff_intent,
                'exec_monitoring_active': bool(
                    decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME
                    and handoff_intent is not None
                    and not blocking_reasons
                ),
                'approval_state': (
                    'pending_exec_agent'
                    if decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME and handoff_intent is not None and not blocking_reasons
                    else 'blocked'
                    if blocking_reasons
                    else None
                ),
            },
        }
        if enrichment_preflight is not None:
            validation_payload['pykrx_enrichment_preflight'] = enrichment_preflight
        if candidate_enrichment is not None:
            validation_payload['candidate_pykrx_enrichment'] = candidate_enrichment
        if decision_agent_name == SIGNAL_DECIDER_AGENT_NAME:
            validation_payload['execution_cost_evidence_policy_version'] = (
                NEXT_OPEN_COST_EVIDENCE_POLICY_VERSION
            )
        if decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME:
            validation_payload['short_term'] = {
                'raw_action': short_term_tactical_action,
                'tactical_action': short_term_tactical_action,
                'generic_action': generic_action,
                'handoff_intent': handoff_intent,
                'entry_execution_mode': _short_term_entry_execution_mode(short_term_tactical_action or 'wait'),
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
                'market_regime_context': market_regime_context,
                'decision_input_quality_context': decision_input_quality_context,
                'response_payload_summary': _to_json_safe(response_payload),
                'response_summary': _snapshot_response_summary_payload(snapshot) or None,
                'expected_holding_period': _short_term_field_text(snapshot, 'expected_holding_period')
                or snapshot.suggested_holding_period,
            }
            validation_payload['short_term_entry_quality'] = entry_quality_snapshot
            if alignment_gate_payload is not None:
                validation_payload['alignment_gate'] = alignment_gate_payload
                validation_payload['signal_decider_alignment'] = alignment_gate_payload.get('signal_decider_alignment')
                validation_payload['alignment_gate_action'] = alignment_gate_payload.get('alignment_gate_action')
                validation_payload['alignment_gate_reason'] = alignment_gate_payload.get('alignment_gate_reason')
                validation_payload['final_enforcement_decision'] = alignment_gate_payload.get('enforcement_decision')
                validation_payload['requested_quantity'] = alignment_gate_payload.get('requested_quantity')
                validation_payload['final_quantity'] = alignment_gate_payload.get('final_quantity')
                validation_payload['alignment_signal_decider_decision_snapshot_id'] = alignment_gate_payload.get(
                    'signal_decider_decision_snapshot_id'
                )
                validation_payload['short_term']['alignment_gate'] = alignment_gate_payload
                validation_payload['short_term']['signal_decider_alignment'] = alignment_gate_payload.get(
                    'signal_decider_alignment'
                )
                validation_payload['short_term']['alignment_gate_action'] = alignment_gate_payload.get(
                    'alignment_gate_action'
                )
                validation_payload['short_term']['final_enforcement_decision'] = alignment_gate_payload.get(
                    'enforcement_decision'
                )
                validation_payload['short_term']['requested_quantity'] = alignment_gate_payload.get(
                    'requested_quantity'
                )
                validation_payload['short_term']['final_quantity'] = alignment_gate_payload.get('final_quantity')
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
        short_term_tactical_action = (
            _short_term_tactical_action(snapshot)
            if decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME
            else None
        )
        generic_action = (
            _short_term_generic_action(short_term_tactical_action)
            if short_term_tactical_action is not None
            else _normalize_action(snapshot.next_day_open_action or snapshot.decision_action)
        )
        handoff_intent = (
            _short_term_handoff_intent(short_term_tactical_action)
            if short_term_tactical_action is not None
            else 'next_open_buy'
            if generic_action == 'buy'
            else None
        )
        handoff_active = bool(
            decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME
            and handoff_intent is not None
            and not blocking_reasons
        )
        validation_payload['raw_action'] = short_term_tactical_action or generic_action
        validation_payload['tactical_action'] = short_term_tactical_action
        validation_payload['generic_action'] = generic_action
        validation_payload['handoff_intent'] = handoff_intent
        validation_payload['handoff'] = {
            **(validation_payload.get('handoff') if isinstance(validation_payload.get('handoff'), dict) else {}),
            'decision_agent_name': decision_agent_name,
            'raw_action': short_term_tactical_action or generic_action,
            'tactical_action': short_term_tactical_action,
            'generic_action': generic_action,
            'handoff_intent': handoff_intent,
            'exec_monitoring_active': handoff_active,
            'approval_state': 'pending_exec_agent' if handoff_active else 'blocked' if blocking_reasons else None,
        }
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
            'core_execution_classification': (
                'blocked'
                if blocking_reasons
                else EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING
                if _uses_exec_agent_sizing_for_dispatch(decision_agent_name)
                else 'core_fixed_quantity_next_open_candidate'
            ),
            'raw_action': short_term_tactical_action or generic_action,
            'tactical_action': short_term_tactical_action,
            'generic_action': generic_action,
            'handoff_intent': handoff_intent,
            'handoff_active': handoff_active,
            'exec_monitoring_active': handoff_active,
            'approval_state': 'pending_exec_agent' if handoff_active else 'blocked' if blocking_reasons else None,
            'entry_execution_mode': (
                _short_term_entry_execution_mode(short_term_tactical_action or 'wait')
                if decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME
                else 'next_open_market'
            ),
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
            'market_regime_context': validation_payload.get('market_regime_context') or {},
            'decision_input_quality_context': validation_payload.get('decision_input_quality_context') or {},
            'response_summary': _snapshot_response_summary_payload(snapshot) or None,
            'response_payload_summary': _to_json_safe(response_payload),
            'decision_traceability': _snapshot_traceability_payload(snapshot),
            'signal_traceability': _snapshot_traceability_payload(snapshot),
            'agent_context_completeness': validation_payload.get('agent_context_completeness') or {},
        }
        if decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME:
            alignment_gate_payload = (
                validation_payload.get('alignment_gate') if isinstance(validation_payload.get('alignment_gate'), dict) else None
            )
            row.traceability_payload.update(
                {
                    'alignment_gate': alignment_gate_payload,
                    'signal_decider_alignment': validation_payload.get('signal_decider_alignment'),
                    'alignment_gate_action': validation_payload.get('alignment_gate_action'),
                    'alignment_gate_reason': validation_payload.get('alignment_gate_reason'),
                    'final_enforcement_decision': validation_payload.get('final_enforcement_decision'),
                    'requested_quantity': validation_payload.get('requested_quantity'),
                    'final_quantity': validation_payload.get('final_quantity'),
                    'alignment_signal_decider_decision_snapshot_id': validation_payload.get(
                        'alignment_signal_decider_decision_snapshot_id'
                    ),
                }
            )
        row.last_evaluated_at = now
        is_monitor_candidate = (
            decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME
            and short_term_tactical_action == 'watch_for_trigger'
            and not blocking_reasons
        )
        row.eligibility_status = (
            ELIGIBILITY_BLOCKED
            if blocking_reasons
            else ELIGIBILITY_MONITOR_CANDIDATE
            if is_monitor_candidate
            else ELIGIBILITY_ELIGIBLE
        )
        if row.eligibility_status == ELIGIBILITY_ELIGIBLE and row.first_ready_at is None:
            row.first_ready_at = now
        if row.eligibility_status == ELIGIBILITY_BLOCKED and not _is_dispatch_terminal_status(row.execution_status):
            row.execution_status = EXECUTION_STATUS_PLANNED
        elif row.eligibility_status == ELIGIBILITY_MONITOR_CANDIDATE and not _is_dispatch_terminal_status(
            row.execution_status
        ):
            row.execution_status = EXECUTION_STATUS_AWAITING_TRIGGER
        elif row.eligibility_status == ELIGIBILITY_ELIGIBLE and row.execution_status not in {
            EXECUTION_STATUS_SUBMITTED,
            EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING,
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
        replacement_snapshot: SignalDeciderDecisionSnapshot | None = None,
        replacement_action: str | None = None,
    ) -> None:
        for key, row in rows.items():
            if (
                row is None
                or key[1] != symbol
                or key[2] != decision_agent_name
            ):
                continue
            if _is_dispatch_terminal_status(row.execution_status):
                if replacement_snapshot is not None and replacement_action is not None:
                    self._stage_terminal_revocation(
                        row=row,
                        replacement_snapshot=replacement_snapshot,
                        replacement_action=replacement_action,
                        reason=reason,
                        now=now,
                    )
                continue
            self._set_blocked_row(row=row, reason=reason, now=now)

    def _stage_terminal_revocation(
        self,
        *,
        row: SignalDeciderExecutionPlanItem,
        replacement_snapshot: SignalDeciderDecisionSnapshot | None,
        replacement_observation_id: str | None = None,
        replacement_action: str,
        reason: dict[str, str],
        now: datetime,
    ) -> None:
        existing_submission = (
            copy.deepcopy(row.submission_payload)
            if isinstance(row.submission_payload, dict)
            else {}
        )
        existing_revocation = (
            existing_submission.get('revocation')
            if isinstance(existing_submission.get('revocation'), dict)
            else {}
        )
        replacement_revision_id = (
            str(replacement_observation_id)
            if replacement_observation_id
            else str(replacement_snapshot.id)
            if replacement_snapshot is not None
            else ''
        )
        if not replacement_revision_id:
            raise ValueError('terminal revocation requires a stable replacement revision identity')
        if (
            row.execution_status == EXECUTION_STATUS_REVOCATION_PENDING
            and str(existing_revocation.get('replacement_decision_snapshot_id') or '')
            == replacement_revision_id
        ):
            return

        prior_row_authority_fingerprint = _execution_plan_row_authority_fingerprint(
            row
        )
        exec_dispatch = (
            existing_submission.get('exec_dispatch')
            if isinstance(existing_submission.get('exec_dispatch'), dict)
            else {}
        )
        prior_evidence = {
            'decision_snapshot_id': str(row.decision_snapshot_id),
            'decision_agent_name': row.decision_agent_name,
            'symbol': row.symbol,
            'eligibility_status': row.eligibility_status,
            'execution_status': row.execution_status,
            'validation_payload': copy.deepcopy(row.validation_payload),
            'traceability_payload': copy.deepcopy(row.traceability_payload),
            'dispatch_evidence': {
                'plan_id': exec_dispatch.get('plan_id'),
                'execution_plan_id': exec_dispatch.get('execution_plan_id'),
                'plan_fingerprint': exec_dispatch.get('plan_fingerprint'),
                'dispatch_status': exec_dispatch.get('dispatch_status'),
                'destination_binding': copy.deepcopy(
                    exec_dispatch.get('destination_binding')
                ),
                'item_links': copy.deepcopy(exec_dispatch.get('item_links') or []),
                'last_success_at': exec_dispatch.get('last_success_at'),
                'contract_proof': copy.deepcopy(
                    exec_dispatch.get('contract_proof')
                ),
                'last_request_summary': copy.deepcopy(
                    exec_dispatch.get('last_request_summary')
                ),
                'last_response_summary': copy.deepcopy(
                    exec_dispatch.get('last_response_summary')
                ),
            },
            'row_authority_fingerprint': prior_row_authority_fingerprint,
            'archived_at': now.isoformat(),
        }
        history = [
            copy.deepcopy(item)
            for item in (existing_submission.get('revision_history') or [])
            if isinstance(item, dict)
        ]
        if not any(
            str(item.get('decision_snapshot_id') or '') == str(row.decision_snapshot_id)
            and (item.get('dispatch_evidence') or {}).get('plan_id') == exec_dispatch.get('plan_id')
            for item in history
        ):
            history.append(prior_evidence)
        history = history[-EXECUTION_REVISION_HISTORY_LIMIT:]
        revocation = {
            'schema_version': 'core_execution_revocation_state_v1',
            'status': 'pending',
            'reason_code': reason['code'],
            'reason': copy.deepcopy(reason),
            'requested_at': now.isoformat(),
            'prior_decision_snapshot_id': str(row.decision_snapshot_id),
            'replacement_decision_snapshot_id': replacement_revision_id,
            'replacement_observation_id': replacement_observation_id,
            'replacement_action': replacement_action,
            'replacement_actionable': bool(
                replacement_snapshot is not None
                and _is_actionable_snapshot(replacement_snapshot)
            ),
            'plan_id': exec_dispatch.get('plan_id'),
            'destination_binding': copy.deepcopy(
                exec_dispatch.get('destination_binding')
            ),
            'item_links': copy.deepcopy(exec_dispatch.get('item_links') or []),
            'prior_row_authority_fingerprint': prior_row_authority_fingerprint,
        }
        row.eligibility_status = ELIGIBILITY_BLOCKED
        row.execution_status = EXECUTION_STATUS_REVOCATION_PENDING
        row.last_evaluated_at = now
        row.validation_payload = {
            **(
                copy.deepcopy(row.validation_payload)
                if isinstance(row.validation_payload, dict)
                else {}
            ),
            'blocking_reasons': [reason],
        }
        revocation['revoked_row_authority_fingerprint'] = (
            _execution_plan_row_authority_fingerprint(row)
        )
        row.submission_payload = {
            **existing_submission,
            'revision_history': history,
            'revocation': revocation,
        }

    def _recover_acknowledged_market_evidence_revocation(
        self,
        *,
        row: SignalDeciderExecutionPlanItem,
        snapshot: SignalDeciderDecisionSnapshot,
        target: NextOpenAccountTarget,
        current_live_market_authority: dict[str, Any],
        now: datetime,
    ) -> bool:
        """Retire an ACKed transient BUY authority only after exact live revalidation."""
        submission = (
            copy.deepcopy(row.submission_payload)
            if isinstance(row.submission_payload, dict)
            else {}
        )
        revocation = (
            copy.deepcopy(submission.get('revocation'))
            if isinstance(submission.get('revocation'), dict)
            else {}
        )
        exec_dispatch = (
            copy.deepcopy(submission.get('exec_dispatch'))
            if isinstance(submission.get('exec_dispatch'), dict)
            else {}
        )
        expected_policy_fingerprint = _next_open_policy_fingerprint([target])
        revoked_row_authority_fingerprint = _normalize_optional_text(
            revocation.get('revoked_row_authority_fingerprint')
        )
        prior_row_authority_fingerprint = _normalize_optional_text(
            revocation.get('prior_row_authority_fingerprint')
        )
        replacement_revision_id = _normalize_optional_text(
            revocation.get('replacement_decision_snapshot_id')
        )
        expected_links = revocation.get('item_links')
        dispatch_links = exec_dispatch.get('item_links')
        revision_history = [
            copy.deepcopy(item)
            for item in (submission.get('revision_history') or [])
            if isinstance(item, dict)
        ]
        archived_revision = next(
            (
                item
                for item in reversed(revision_history)
                if str(item.get('decision_snapshot_id') or '')
                == str(row.decision_snapshot_id)
                and isinstance(item.get('dispatch_evidence'), dict)
                and item['dispatch_evidence'].get('plan_id')
                == revocation.get('plan_id')
                and item.get('row_authority_fingerprint')
                == prior_row_authority_fingerprint
            ),
            None,
        )
        revocation_history = [
            item
            for item in (submission.get('revocation_history') or [])
            if isinstance(item, dict)
        ]
        acknowledged_record = revocation_history[-1] if revocation_history else {}
        acknowledged_request = (
            acknowledged_record.get('request')
            if isinstance(acknowledged_record.get('request'), dict)
            else {}
        )
        acknowledged_response = (
            acknowledged_record.get('response')
            if isinstance(acknowledged_record.get('response'), dict)
            else {}
        )
        response_contract_ack = (
            acknowledged_response.get('contract_ack')
            if isinstance(acknowledged_response.get('contract_ack'), dict)
            else {}
        )

        expected_item_bindings = {
            str(link.get('core_item_key')): str(link.get('exec_plan_item_id'))
            for link in expected_links or []
            if isinstance(link, dict)
            and link.get('core_item_key')
            and link.get('exec_plan_item_id')
        }
        requested_item_bindings = {
            str(item.get('core_item_key')): str(item.get('execution_plan_item_id'))
            for item in acknowledged_request.get('items') or []
            if isinstance(item, dict)
            and item.get('core_item_key')
            and item.get('execution_plan_item_id')
        }
        response_items = {
            str(item.get('core_item_key')): item
            for item in acknowledged_response.get('item_acks') or []
            if isinstance(item, dict) and item.get('core_item_key')
        }
        response_item_bindings = {
            key: str(item.get('execution_plan_item_id'))
            for key, item in response_items.items()
            if item.get('execution_plan_item_id')
        }
        revoked_buy_keys = {
            str(link.get('core_item_key'))
            for link in expected_links or []
            if isinstance(link, dict)
            and str(link.get('side') or '').strip().lower() == 'buy'
        }

        if (
            revocation.get('schema_version') != 'core_execution_revocation_state_v1'
            or revocation.get('status') != 'acknowledged'
            or revocation.get('ack_status') != 'revoked'
            or revocation.get('reason_code')
            != 'current_live_market_evidence_unavailable'
            or revocation.get('replacement_action') != 'reject'
            or revocation.get('replacement_actionable') is not False
            or not replacement_revision_id
            or not replacement_revision_id.startswith(
                'observation:current_live_market_evidence_unavailable:'
            )
            or str(revocation.get('prior_decision_snapshot_id') or '')
            != str(row.decision_snapshot_id)
            or int(row.decision_snapshot_id) != int(snapshot.id)
            or row.execution_window != EXECUTION_WINDOW_NEXT_OPEN
            or _normalize_optional_text(row.planned_side) != 'buy'
            or row.account_reference != target.account_reference
            or _row_validation_payload(row).get('preopen_policy_fingerprint')
            != expected_policy_fingerprint
            or not revoked_row_authority_fingerprint
            or _execution_plan_row_authority_fingerprint(row)
            != revoked_row_authority_fingerprint
            or not prior_row_authority_fingerprint
            or archived_revision is None
            or str(exec_dispatch.get('dispatch_status') or '').strip().lower()
            != 'accepted'
            or exec_dispatch.get('plan_id') != revocation.get('plan_id')
            or exec_dispatch.get('destination_binding')
            != revocation.get('destination_binding')
            or not isinstance(expected_links, list)
            or not expected_links
            or dispatch_links != expected_links
            or acknowledged_request.get('plan_id') != revocation.get('plan_id')
            or str(
                acknowledged_request.get('replacement_decision_snapshot_id') or ''
            )
            != replacement_revision_id
            or expected_item_bindings != requested_item_bindings
            or expected_item_bindings != response_item_bindings
            or not revoked_buy_keys
            or any(
                str(response_items.get(key, {}).get('status') or '').strip().lower()
                != 'revoked'
                for key in revoked_buy_keys
            )
            or str(response_contract_ack.get('status') or '').strip().lower()
            != 'revoked'
            or current_live_market_authority.get('schema_version')
            != CURRENT_LIVE_MARKET_AUTHORITY_SCHEMA_VERSION
            or current_live_market_authority.get('applies') is not True
            or current_live_market_authority.get('authoritative') is not True
            or current_live_market_authority.get('status') != 'available'
            or current_live_market_authority.get('action') != 'allow'
            or current_live_market_authority.get('market_date')
            != row.target_trade_date.isoformat()
        ):
            return False

        recovery_material = json.dumps(
            {
                'row_id': row.id,
                'plan_id': revocation.get('plan_id'),
                'replacement_revision_id': replacement_revision_id,
                'revoked_row_authority_fingerprint': (
                    revoked_row_authority_fingerprint
                ),
                'live_market_authority': current_live_market_authority,
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(',', ':'),
        )
        recovery_generation = hashlib.sha256(
            recovery_material.encode('utf-8')
        ).hexdigest()
        archived_dispatch_evidence = archived_revision.get('dispatch_evidence')
        if isinstance(archived_dispatch_evidence, dict):
            archived_dispatch_evidence.update(
                {
                    'retired_at': now.isoformat(),
                    'retirement_reason': 'transient_market_evidence_recovered',
                    'recovery_generation': recovery_generation,
                }
            )
        revocation['recovery'] = {
            'schema_version': 'core_execution_authority_recovery_v1',
            'status': 'authority_revalidated',
            'recovered_at': now.isoformat(),
            'recovery_generation': recovery_generation,
            'row_authority_fingerprint': revoked_row_authority_fingerprint,
            'live_market_observed_at': current_live_market_authority.get(
                'observed_at'
            ),
        }
        submission['revision_history'] = revision_history[
            -EXECUTION_REVISION_HISTORY_LIMIT:
        ]
        submission['revocation'] = revocation
        submission.pop('exec_dispatch', None)
        submission.pop('latest', None)
        row.submission_payload = submission
        row.execution_status = EXECUTION_STATUS_PLANNED
        row.submitted_at = None
        return True

    def _set_blocked_row(
        self,
        *,
        row: SignalDeciderExecutionPlanItem,
        reason: dict[str, str],
        now: datetime,
    ) -> None:
        existing = row.validation_payload if isinstance(row.validation_payload, dict) else {}
        row.eligibility_status = ELIGIBILITY_BLOCKED
        if (
            not _is_dispatch_terminal_status(row.execution_status)
            and row.execution_status not in EXECUTION_REVOCATION_LOCKED_STATUSES
            and row.execution_status != EXECUTION_STATUS_REVOKED
        ):
            row.execution_status = EXECUTION_STATUS_PLANNED
        row.last_evaluated_at = now
        row.validation_payload = {
            **existing,
            'blocking_reasons': [reason],
            'warnings': existing.get('warnings') or [],
        }

    def _stage_delegated_buy_revocations(
        self,
        *,
        target_trade_date: date,
        account_reference: str | None,
        reason_code: str,
        reason_message: str,
        observation_scope: str,
        observation_state: str,
    ) -> dict[str, int]:
        now = _utcnow()
        staged_count = 0
        pending_count = 0
        with session_scope() as session:
            stmt = (
                select(SignalDeciderExecutionPlanItem)
                .where(
                    SignalDeciderExecutionPlanItem.execution_window == EXECUTION_WINDOW_NEXT_OPEN,
                    SignalDeciderExecutionPlanItem.target_trade_date == target_trade_date,
                )
                .order_by(SignalDeciderExecutionPlanItem.id.asc())
                .with_for_update()
            )
            if account_reference:
                stmt = stmt.where(
                    SignalDeciderExecutionPlanItem.account_reference == account_reference
                )
            rows = session.execute(stmt).scalars().all()
            for row in rows:
                if row.execution_status == EXECUTION_STATUS_REVOCATION_PENDING:
                    pending_count += 1
                    continue
                submission = row.submission_payload if isinstance(row.submission_payload, dict) else {}
                reconciliation = (
                    submission.get('dispatch_reconciliation')
                    if isinstance(submission.get('dispatch_reconciliation'), dict)
                    else {}
                )
                ambiguous_remote_acceptance = bool(
                    row.execution_status == EXECUTION_STATUS_REVOCATION_MANUAL_REVIEW
                    and str(reconciliation.get('reason_code') or '').strip().lower()
                    in {
                        'ambiguous_remote_acceptance',
                        'ambiguous_remote_acceptance_after_core_revision',
                    }
                )
                if (
                    not (
                        _is_dispatch_terminal_status(row.execution_status)
                        or ambiguous_remote_acceptance
                    )
                    or str(_normalize_optional_text(row.planned_side) or '').lower() != 'buy'
                ):
                    continue
                exec_dispatch = (
                    submission.get('exec_dispatch')
                    if isinstance(submission.get('exec_dispatch'), dict)
                    else {}
                )
                item_links = exec_dispatch.get('item_links')
                exact_remote_links = bool(
                    isinstance(item_links, list)
                    and item_links
                    and any(
                        isinstance(link, dict)
                        and str(link.get('side') or '').strip().lower() == 'buy'
                        for link in item_links
                    )
                    and all(
                        isinstance(link, dict)
                        and _normalize_optional_text(link.get('core_item_key'))
                        and _normalize_optional_text(link.get('exec_plan_id'))
                        and _normalize_optional_text(link.get('exec_plan_item_id'))
                        for link in item_links
                    )
                )
                if (
                    not exact_remote_links
                    or (
                        str(exec_dispatch.get('dispatch_status') or '').strip().lower()
                        != 'accepted'
                        and not ambiguous_remote_acceptance
                    )
                ):
                    continue
                observation_material = '|'.join(
                    (
                        reason_code,
                        target_trade_date.isoformat(),
                        row.account_reference,
                        row.symbol,
                        _normalize_agent_name(row.decision_agent_name),
                        observation_state,
                    )
                )
                observation_id = (
                    f'observation:{observation_scope}:'
                    f'{hashlib.sha256(observation_material.encode("utf-8")).hexdigest()[:20]}'
                )
                self._stage_terminal_revocation(
                    row=row,
                    replacement_snapshot=None,
                    replacement_observation_id=observation_id,
                    replacement_action='reject',
                    reason=_reason(
                        reason_code,
                        reason_message,
                    ),
                    now=now,
                )
                staged_count += 1
                pending_count += 1
            session.flush()
        return {
            'staged_count': staged_count,
            'pending_count': pending_count,
        }

    def stage_execution_control_revocations(
        self,
        *,
        target_trade_date: date,
        account_reference: str | None = None,
    ) -> dict[str, int]:
        """Freeze delegated BUY authority after an explicit execution-control change."""
        control_state = '|'.join(
            (
                str(settings.signal_decider_exec_integration_mode or '').strip().lower(),
                str(bool(settings.signal_decider_next_open_enabled)).lower(),
            )
        )
        return self._stage_delegated_buy_revocations(
            target_trade_date=target_trade_date,
            account_reference=account_reference,
            reason_code='execution_control_disabled',
            reason_message=(
                'Explicit execution integration settings no longer permit this delegated BUY authority.'
            ),
            observation_scope='execution_control_disabled',
            observation_state=control_state,
        )

    def stage_market_evidence_unavailable_revocations(
        self,
        *,
        target_trade_date: date,
        account_reference: str | None = None,
        failure_source: str,
    ) -> dict[str, int]:
        """Freeze delegated BUY authority when the critical live-decision chain fails."""
        return self._stage_delegated_buy_revocations(
            target_trade_date=target_trade_date,
            account_reference=account_reference,
            reason_code='current_live_market_evidence_unavailable',
            reason_message=(
                'The critical market evidence or decision-plan refresh failed; '
                'unfilled delegated BUY authority must be revoked.'
            ),
            observation_scope='current_live_market_evidence_unavailable',
            observation_state=failure_source,
        )

    def apply_exec_revocation_ack(
        self,
        *,
        row_id: int,
        request_payload: dict[str, Any],
        response_payload: dict[str, Any],
        ack_status: str,
    ) -> dict[str, Any]:
        """Persist an exact Exec revocation ACK before allowing a replacement plan."""
        now = _utcnow()
        with session_scope() as session:
            row = session.scalar(
                select(SignalDeciderExecutionPlanItem)
                .where(SignalDeciderExecutionPlanItem.id == row_id)
                .with_for_update()
            )
            if row is None:
                raise ValueError('revocation row no longer exists')
            submission = (
                copy.deepcopy(row.submission_payload)
                if isinstance(row.submission_payload, dict)
                else {}
            )
            revocation = (
                copy.deepcopy(submission.get('revocation'))
                if isinstance(submission.get('revocation'), dict)
                else {}
            )
            if row.execution_status != EXECUTION_STATUS_REVOCATION_PENDING:
                raise ValueError('row is not awaiting an Exec revocation ACK')
            if (
                request_payload.get('plan_id') != revocation.get('plan_id')
                or str(request_payload.get('replacement_decision_snapshot_id') or '')
                != str(revocation.get('replacement_decision_snapshot_id') or '')
            ):
                raise ValueError('revocation ACK does not bind the pending Core revision')

            expected_items = {
                str(link.get('core_item_key')): str(link.get('exec_plan_item_id'))
                for link in (revocation.get('item_links') or [])
                if link.get('core_item_key') and link.get('exec_plan_item_id')
            }
            requested_items = {
                str(item.get('core_item_key')): str(item.get('execution_plan_item_id'))
                for item in (request_payload.get('items') or [])
                if isinstance(item, dict)
            }
            response_items = {
                str(item.get('core_item_key')): str(item.get('execution_plan_item_id'))
                for item in (response_payload.get('item_acks') or [])
                if isinstance(item, dict)
            }
            if not expected_items or requested_items != expected_items or response_items != expected_items:
                raise ValueError('revocation ACK item identities do not match pending Exec items')
            if ack_status not in {'revoked', 'manual_review_required'}:
                raise ValueError('revocation ACK status is not terminal')

            revocation_record = {
                'acknowledged_at': now.isoformat(),
                'request': copy.deepcopy(request_payload),
                'response': copy.deepcopy(response_payload),
            }
            revocation_history = [
                copy.deepcopy(item)
                for item in (submission.get('revocation_history') or [])
                if isinstance(item, dict)
            ]
            revocation_history.append(revocation_record)
            submission['revocation_history'] = revocation_history[-EXECUTION_REVISION_HISTORY_LIMIT:]
            submission['revocation'] = {
                **revocation,
                'status': (
                    'acknowledged'
                    if ack_status == 'revoked'
                    else 'manual_review_required'
                ),
                'acknowledged_at': now.isoformat(),
                'ack_status': ack_status,
            }

            if ack_status == 'manual_review_required':
                row.execution_status = EXECUTION_STATUS_REVOCATION_MANUAL_REVIEW
            elif revocation.get('replacement_actionable'):
                submission.pop('exec_dispatch', None)
                submission.pop('latest', None)
                row.execution_status = EXECUTION_STATUS_PLANNED
                row.submitted_at = None
            else:
                row.execution_status = EXECUTION_STATUS_REVOKED
            row.eligibility_status = ELIGIBILITY_BLOCKED
            row.last_evaluated_at = now
            row.submission_payload = submission
            session.flush()
            return self._serialize_row(row)

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
        enrichment_preflight: dict[str, Any] | None = None,
        agent_context_completeness: dict[str, Any] | None = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        source_market_date = source_market_date or _previous_trading_day(target_trade_date)
        agent_context_payload = (
            agent_context_completeness
            if isinstance(agent_context_completeness, dict)
            else {}
        )
        counts = {
            'items_total': len(rows),
            'eligible_count': sum(1 for row in rows if row.eligibility_status == ELIGIBILITY_ELIGIBLE),
            'blocked_count': sum(1 for row in rows if row.eligibility_status == ELIGIBILITY_BLOCKED),
            'monitor_candidate_count': sum(
                1 for row in rows if row.eligibility_status == ELIGIBILITY_MONITOR_CANDIDATE
            ),
            'planned_count': sum(1 for row in rows if row.execution_status == EXECUTION_STATUS_PLANNED),
            'awaiting_trigger_count': sum(
                1 for row in rows if row.execution_status == EXECUTION_STATUS_AWAITING_TRIGGER
            ),
            'dry_run_count': sum(1 for row in rows if row.execution_status == EXECUTION_STATUS_DRY_RUN),
            'submitted_count': sum(1 for row in rows if row.execution_status == EXECUTION_STATUS_SUBMITTED),
            'delegated_for_exec_agent_sizing_count': sum(
                1 for row in rows if row.execution_status == EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING
            ),
            'submit_failed_count': sum(1 for row in rows if row.execution_status == EXECUTION_STATUS_SUBMIT_FAILED),
            'decision_agent_counts': dict(
                sorted(
                    Counter(_normalize_agent_name(row.decision_agent_name) for row in rows).items()
                )
            ),
            'short_term_candidates_total': sum(
                1 for row in rows if _normalize_agent_name(row.decision_agent_name) == SHORT_TERM_DECIDER_AGENT_NAME
            ),
            'short_term_watch_active_count': sum(
                1
                for row in rows
                if _normalize_agent_name(row.decision_agent_name) == SHORT_TERM_DECIDER_AGENT_NAME
                and row.eligibility_status in {ELIGIBILITY_ELIGIBLE, ELIGIBILITY_MONITOR_CANDIDATE}
            ),
            'short_term_blocked_or_degraded_count': sum(
                1
                for row in rows
                if _normalize_agent_name(row.decision_agent_name) == SHORT_TERM_DECIDER_AGENT_NAME
                and (
                    row.eligibility_status == ELIGIBILITY_BLOCKED
                    or bool(_row_validation_payload(row).get('warnings'))
                )
            ),
            'signal_decider_global_preflight_blocked_count': sum(
                1
                for row in rows
                if _normalize_agent_name(row.decision_agent_name) == SIGNAL_DECIDER_AGENT_NAME
                and any(
                    isinstance(reason, dict) and reason.get('code') == 'pykrx_enrichment_preflight_blocked'
                    for reason in (_row_validation_payload(row).get('blocking_reasons') or [])
                )
            ),
            'short_term_wait_conflict_block_count': sum(
                1
                for row in rows
                if _normalize_agent_name(row.decision_agent_name) == SHORT_TERM_DECIDER_AGENT_NAME
                and _row_validation_payload(row).get('signal_decider_alignment')
                == SIGNAL_DECIDER_ALIGNMENT_WAIT_CONFLICT_BLOCK
            ),
            'short_term_wait_conflict_override_count': sum(
                1
                for row in rows
                if _normalize_agent_name(row.decision_agent_name) == SHORT_TERM_DECIDER_AGENT_NAME
                and _row_validation_payload(row).get('signal_decider_alignment')
                == SIGNAL_DECIDER_ALIGNMENT_WAIT_CONFLICT_OVERRIDE
            ),
        }
        counts['buy_eligible_count'] = counts['eligible_count']
        counts['ready_count'] = counts['eligible_count'] + counts['monitor_candidate_count']
        return {
            'target_trade_date': target_trade_date.isoformat(),
            'source_market_date': source_market_date.isoformat(),
            **counts,
            **({'reason': reason} if reason else {}),
            'execution_enabled': runtime['execution_enabled'],
            'dry_run': runtime['dry_run'],
            'account_targets_configured': runtime['account_targets_configured'],
            'account_targets_valid': runtime['account_targets_valid'],
            'account_target_errors': runtime['account_target_errors'],
            'enrichment_preflight_allowed': (
                bool(enrichment_preflight.get('allowed')) if enrichment_preflight is not None else None
            ),
            'enrichment_preflight_status': (
                enrichment_preflight.get('status') if enrichment_preflight is not None else None
            ),
            'enrichment_failed_categories': (
                enrichment_preflight.get('failed_categories') if enrichment_preflight is not None else []
            ),
            'agent_context_completeness_status': agent_context_payload.get('status'),
            'agent_context_incomplete': agent_context_payload.get('has_incomplete_agent_context'),
            'agent_context_failure_count': agent_context_payload.get('failure_count'),
            'agent_context_failure_groups': agent_context_payload.get('failure_groups') or [],
            'agent_context_completeness': agent_context_payload,
        }

    def _serialize_row(self, row: SignalDeciderExecutionPlanItem) -> dict[str, Any]:
        traceability_payload = row.__dict__.get('traceability_payload')
        validation_payload = row.__dict__.get('validation_payload')
        submission_payload = row.__dict__.get('submission_payload')
        traceability = traceability_payload if isinstance(traceability_payload, dict) else {}
        validation = validation_payload if isinstance(validation_payload, dict) else {}
        blocker_reason = _first_blocking_reason(validation)
        decision_agent_name = _normalize_agent_name(row.decision_agent_name)
        raw_action = traceability.get('raw_action') or traceability.get('tactical_action')
        generic_action = traceability.get('generic_action')
        if generic_action is None:
            if raw_action == 'scalp_buy_candidate':
                generic_action = 'buy'
            elif raw_action == 'watch_for_trigger':
                generic_action = 'monitor_candidate'
            elif decision_agent_name == SIGNAL_DECIDER_AGENT_NAME and row.planned_side == 'buy':
                generic_action = 'buy'
        handoff_intent = traceability.get('handoff_intent')
        if handoff_intent is None:
            if raw_action == 'scalp_buy_candidate':
                handoff_intent = 'tactical_buy'
            elif raw_action == 'watch_for_trigger':
                handoff_intent = 'intraday_watch_buy'
            elif decision_agent_name == SIGNAL_DECIDER_AGENT_NAME and row.planned_side == 'buy':
                handoff_intent = 'next_open_buy'
        handoff_active = bool(traceability.get('handoff_active')) or bool(
            decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME
            and row.eligibility_status in {ELIGIBILITY_ELIGIBLE, ELIGIBILITY_MONITOR_CANDIDATE}
            and handoff_intent in {'tactical_buy', 'intraday_watch_buy'}
        )
        exec_monitoring_active = bool(traceability.get('exec_monitoring_active')) or handoff_active
        approval_state = (
            traceability.get('approval_state')
            or ('pending_exec_agent' if handoff_active else 'blocked' if blocker_reason else None)
        )
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
            'decision_run_id': traceability.get('decision_run_id'),
            'source_feedback_id': traceability.get('source_feedback_id') or traceability.get('feedback_id'),
            'raw_action': raw_action,
            'tactical_action': traceability.get('tactical_action'),
            'generic_action': generic_action,
            'handoff_intent': handoff_intent,
            'handoff_active': handoff_active,
            'exec_monitoring_active': exec_monitoring_active,
            'exec_agent_approved': traceability.get('exec_agent_approved'),
            'approval_state': approval_state,
            'trigger_price': traceability.get('trigger_price'),
            'chase_limit_price': traceability.get('chase_limit_price'),
            'target_price': traceability.get('target_price'),
            'stop_loss_price': traceability.get('stop_loss_price'),
            'expected_holding_period': traceability.get('expected_holding_period')
            or traceability.get('suggested_holding_period'),
            'entry_execution_mode': traceability.get('entry_execution_mode'),
            'signal_decider_alignment': traceability.get('signal_decider_alignment')
            or validation.get('signal_decider_alignment'),
            'alignment_gate_action': traceability.get('alignment_gate_action')
            or validation.get('alignment_gate_action'),
            'alignment_gate_reason': traceability.get('alignment_gate_reason')
            or validation.get('alignment_gate_reason'),
            'final_enforcement_decision': traceability.get('final_enforcement_decision')
            or validation.get('final_enforcement_decision'),
            'requested_quantity': traceability.get('requested_quantity')
            if traceability.get('requested_quantity') is not None
            else validation.get('requested_quantity'),
            'final_quantity': traceability.get('final_quantity')
            if traceability.get('final_quantity') is not None
            else validation.get('final_quantity'),
            'alignment_signal_decider_decision_snapshot_id': traceability.get(
                'alignment_signal_decider_decision_snapshot_id'
            )
            or validation.get('alignment_signal_decider_decision_snapshot_id'),
            'alignment_gate': traceability.get('alignment_gate') or validation.get('alignment_gate'),
            'blocker_reason': blocker_reason,
            'data_quality_warnings': traceability.get('data_quality_warnings') or [],
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
            'agent_context_completeness': validation.get('agent_context_completeness')
            or traceability.get('agent_context_completeness')
            or {},
            'validation': validation,
            'traceability': traceability,
            'submission': submission_payload,
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
                'agent_context_completeness': traceability.get('agent_context_completeness') or {},
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
