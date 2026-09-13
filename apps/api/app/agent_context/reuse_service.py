from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Sequence

from fastapi.encoders import jsonable_encoder
from sqlalchemy import func, or_, select

from app.agent_context.feedback_service import _normalize_market_date, _normalize_symbols
from app.agent_context.roles import (
    DECISION_SUPPORT_AGENT_NAMES,
    DEFAULT_AGENT_NAMES,
    SIGNAL_DECIDER_AGENT_NAME,
)
from app.db import session_scope
from app.db_models import (
    AgentContextFeedback,
    AgentContextFeedbackFailure,
    AgentContextFeedbackRun,
    DailyActionSummary,
    DailyPriceBar,
    DailyPykrxEnrichmentRow,
    DailyTechnicalSnapshot,
    DartDisclosureEvent,
    DartFundamentalsHighlight,
    DartFundamentalsRow,
    DartFundamentalsSnapshot,
    SignalDeciderDecisionSnapshot,
    Symbol,
)


FUNDAMENTAL_SUMMARIZER_AGENT_NAME = 'fundamental-summarizer'
FUNDAMENTAL_SUMMARY_INPUT_FINGERPRINT_SCHEMA_VERSION = 'fundamental_summary_input_fingerprint_v1'
FUNDAMENTAL_SUMMARY_PROMPT_VERSION = 'fundamental_summarizer_prompt_v1'
FUNDAMENTAL_SUMMARY_RESPONSE_SCHEMA_VERSION = 'agent_feedback_response_contract_v1'
FUNDAMENTAL_SUMMARY_REUSE_POLICY_VERSION = 'fundamental_summary_reuse_policy_v1'
FUNDAMENTAL_SUMMARY_REUSE_REASON = 'reuse_unchanged_fundamental_inputs'
FEEDBACK_REUSE_SYMBOL_CHUNK_SIZE = 50

FUNDAMENTAL_REUSE_STALE_REASONS = {
    'forced_recompute',
    'fundamental_input_fingerprint_changed',
    'fundamental_input_fingerprint_missing',
    'stored_fundamental_input_fingerprint_missing',
    'source_updated',
    'stale_market_feedback',
}


@dataclass(frozen=True)
class FeedbackReuseDecision:
    symbol: str
    agent_name: str
    should_dispatch: bool
    reason: str
    latest_feedback_id: int | None
    latest_feedback_received_at: str | None
    latest_source_updated_at: str | None
    latest_feedback: dict[str, Any] | None
    latest_decision_snapshot_id: int | None = None
    latest_decision_snapshot_fingerprint: str | None = None
    latest_decision_snapshot_observed_at: str | None = None
    input_fingerprint: str | None = None
    stored_input_fingerprint: str | None = None
    input_fingerprint_payload: dict[str, Any] | None = None
    stored_input_fingerprint_payload: dict[str, Any] | None = None
    reuse_policy_version: str | None = None
    reuse_source: str | None = None


ROLE_TO_AGENT = {
    role: role
    for role in DEFAULT_AGENT_NAMES
}


def _chunked(values: Sequence[str], size: int) -> list[list[str]]:
    normalized = [str(value) for value in values]
    return [normalized[index:index + size] for index in range(0, len(normalized), size)]


def _isoformat(value: Any) -> str | None:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if value is None:
        return None
    return str(value)


def _stable_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        jsonable_encoder(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(',', ':'),
    )
    return 'sha256:' + hashlib.sha256(encoded.encode('utf-8')).hexdigest()


def _extract_fingerprint_payload(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    candidates = (
        payload.get('fundamental_summary_input_fingerprint'),
        payload.get('input_fingerprint'),
        (payload.get('reuse_metadata') or {}).get('input_fingerprint')
        if isinstance(payload.get('reuse_metadata'), dict)
        else None,
    )
    for candidate in candidates:
        if isinstance(candidate, dict):
            fingerprint = str(
                candidate.get('fingerprint')
                or candidate.get('value')
                or candidate.get('hash')
                or ''
            ).strip()
            if fingerprint:
                return candidate
    return None


def _fingerprint_value(payload: dict[str, Any] | None) -> str | None:
    if not isinstance(payload, dict):
        return None
    value = str(payload.get('fingerprint') or payload.get('value') or payload.get('hash') or '').strip()
    return value or None


def _is_stale_dispatch_reason(reason: str | None) -> bool:
    return str(reason or '') in FUNDAMENTAL_REUSE_STALE_REASONS


def _normalized_evidence_datetime(value: Any) -> datetime:
    if not isinstance(value, datetime):
        return datetime.min.replace(tzinfo=timezone.utc)
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _failure_supersedes_feedback(
    *,
    feedback: Any,
    failure: Any,
    run_order_by_id: dict[str, tuple[datetime, int, datetime, int]],
) -> bool:
    if failure is None:
        return False
    if feedback is None:
        return True

    feedback_market_date = getattr(feedback, 'market_date', None)
    failure_market_date = getattr(failure, 'market_date', None)
    if isinstance(feedback_market_date, date) and isinstance(
        failure_market_date,
        date,
    ):
        if failure_market_date > feedback_market_date:
            return True
        if failure_market_date < feedback_market_date:
            return False

    feedback_run_id = str(getattr(feedback, 'run_id', None) or '').strip()
    failure_run_id = str(getattr(failure, 'run_id', None) or '').strip()
    if feedback_run_id and failure_run_id and feedback_run_id != failure_run_id:
        feedback_run_order = run_order_by_id.get(feedback_run_id)
        failure_run_order = run_order_by_id.get(failure_run_id)
        if feedback_run_order is not None and failure_run_order is not None:
            return failure_run_order >= feedback_run_order

    feedback_received_at = _normalized_evidence_datetime(
        getattr(feedback, 'received_at', None)
    )
    failure_received_at = _normalized_evidence_datetime(
        getattr(failure, 'received_at', None)
    )
    # A same-run, missing-lineage, or equal-time conflict is ambiguous and must
    # remain retryable instead of allowing a potentially invalid success.
    return failure_received_at >= feedback_received_at


class AgentContextReuseService:
    def build_latest_decisions(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str],
        roles: Sequence[str],
        force_recompute: bool = False,
    ) -> dict[str, dict[str, FeedbackReuseDecision]]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbols = _normalize_symbols(symbols)
        requested_roles = [role for role in roles if role in ROLE_TO_AGENT]

        if not normalized_symbols:
            raise ValueError('symbols is required')
        if not requested_roles:
            raise ValueError('roles is required')

        decisions: dict[str, dict[str, FeedbackReuseDecision]] = {symbol: {} for symbol in normalized_symbols}
        if force_recompute and SIGNAL_DECIDER_AGENT_NAME not in requested_roles:
            for symbol in normalized_symbols:
                for role in requested_roles:
                    agent_name = ROLE_TO_AGENT[role]
                    decisions[symbol][role] = FeedbackReuseDecision(
                        symbol=symbol,
                        agent_name=agent_name,
                        should_dispatch=True,
                        reason='forced_recompute',
                        latest_feedback_id=None,
                        latest_feedback_received_at=None,
                        latest_source_updated_at=None,
                        latest_feedback=None,
                        reuse_policy_version=(
                            FUNDAMENTAL_SUMMARY_REUSE_POLICY_VERSION
                            if role == FUNDAMENTAL_SUMMARIZER_AGENT_NAME
                            else None
                        ),
                    )
            return decisions

        with session_scope() as session:
            symbol_rows = session.execute(
                select(Symbol.id, Symbol.symbol)
                .where(Symbol.symbol.in_(normalized_symbols))
            ).all()
            symbol_id_map = {str(row.symbol): int(row.id) for row in symbol_rows}

            latest_feedback_map: dict[tuple[str, str], Any] = {}
            latest_failure_map: dict[tuple[str, str], AgentContextFeedbackFailure] = {}
            for role in requested_roles:
                agent_name = ROLE_TO_AGENT[role]
                for chunk in _chunked(normalized_symbols, FEEDBACK_REUSE_SYMBOL_CHUNK_SIZE):
                    same_day_feedback_rows = session.execute(
                        select(
                            AgentContextFeedback.id,
                            AgentContextFeedback.market_date,
                            AgentContextFeedback.symbol,
                            AgentContextFeedback.agent_name,
                            AgentContextFeedback.run_id,
                            AgentContextFeedback.summary_payload,
                            AgentContextFeedback.feedback_payload,
                            AgentContextFeedback.context_payload,
                            AgentContextFeedback.received_at,
                        )
                        .where(AgentContextFeedback.market_date == normalized_market_date)
                        .where(AgentContextFeedback.symbol.in_(chunk))
                        .where(AgentContextFeedback.agent_name == agent_name)
                        .order_by(
                            AgentContextFeedback.received_at.asc(),
                            AgentContextFeedback.id.asc(),
                        )
                    ).all()
                    for row in same_day_feedback_rows:
                        if not hasattr(row, 'feedback_payload'):
                            continue
                        latest_feedback_map[(row.symbol, agent_name)] = row
                for symbol in normalized_symbols:
                    if (symbol, agent_name) in latest_feedback_map:
                        continue
                    if role == SIGNAL_DECIDER_AGENT_NAME:
                        continue
                    feedback_rows = session.execute(
                        select(
                            AgentContextFeedback.id,
                            AgentContextFeedback.market_date,
                            AgentContextFeedback.symbol,
                            AgentContextFeedback.agent_name,
                            AgentContextFeedback.run_id,
                            AgentContextFeedback.summary_payload,
                            AgentContextFeedback.feedback_payload,
                            AgentContextFeedback.context_payload,
                            AgentContextFeedback.received_at,
                        )
                        .where(AgentContextFeedback.market_date <= normalized_market_date)
                        .where(AgentContextFeedback.symbol == symbol)
                        .where(AgentContextFeedback.agent_name == agent_name)
                        .order_by(
                            AgentContextFeedback.market_date.desc(),
                            AgentContextFeedback.received_at.desc(),
                            AgentContextFeedback.id.desc(),
                        )
                        .limit(1)
                    ).all()
                    for row in feedback_rows:
                        if not hasattr(row, 'feedback_payload'):
                            continue
                        key = (row.symbol, agent_name)
                        if key not in latest_feedback_map:
                            latest_feedback_map[key] = row
                for symbol in normalized_symbols:
                    failure_row = session.execute(
                        select(AgentContextFeedbackFailure)
                        .where(AgentContextFeedbackFailure.market_date <= normalized_market_date)
                        .where(AgentContextFeedbackFailure.symbol == symbol)
                        .where(AgentContextFeedbackFailure.agent_name == agent_name)
                        .order_by(
                            AgentContextFeedbackFailure.market_date.desc(),
                            AgentContextFeedbackFailure.received_at.desc(),
                            AgentContextFeedbackFailure.id.desc(),
                        )
                        .limit(1)
                    ).scalars().first()
                    if failure_row is not None:
                        latest_failure_map[(failure_row.symbol, agent_name)] = failure_row

            evidence_run_ids = {
                str(row.run_id or '').strip()
                for row in (
                    *latest_feedback_map.values(),
                    *latest_failure_map.values(),
                )
                if str(getattr(row, 'run_id', None) or '').strip()
            }
            run_order_by_id: dict[
                str,
                tuple[datetime, int, datetime, int],
            ] = {}
            if evidence_run_ids:
                run_rows = session.execute(
                    select(
                        AgentContextFeedbackRun.id,
                        AgentContextFeedbackRun.run_id,
                        AgentContextFeedbackRun.started_at,
                        AgentContextFeedbackRun.request_payload,
                    ).where(
                        AgentContextFeedbackRun.run_id.in_(evidence_run_ids)
                    )
                ).all()
                run_metadata_by_id = {
                    str(row.run_id): {
                        'direct_order': (
                            _normalized_evidence_datetime(row.started_at),
                            int(row.id),
                        ),
                        'parent_run_id': str(
                            (
                                row.request_payload.get('parent_run_id')
                                if isinstance(row.request_payload, dict)
                                else None
                            )
                            or ''
                        ).strip(),
                    }
                    for row in run_rows
                    if str(row.run_id or '').strip()
                }
                parent_run_ids = {
                    metadata['parent_run_id']
                    for metadata in run_metadata_by_id.values()
                    if metadata['parent_run_id']
                }
                missing_parent_run_ids = parent_run_ids.difference(
                    run_metadata_by_id
                )
                if missing_parent_run_ids:
                    parent_rows = session.execute(
                        select(
                            AgentContextFeedbackRun.id,
                            AgentContextFeedbackRun.run_id,
                            AgentContextFeedbackRun.started_at,
                        ).where(
                            AgentContextFeedbackRun.run_id.in_(
                                missing_parent_run_ids
                            )
                        )
                    ).all()
                    for row in parent_rows:
                        run_id = str(row.run_id or '').strip()
                        if not run_id:
                            continue
                        run_metadata_by_id[run_id] = {
                            'direct_order': (
                                _normalized_evidence_datetime(row.started_at),
                                int(row.id),
                            ),
                            'parent_run_id': '',
                        }
                for run_id in evidence_run_ids:
                    metadata = run_metadata_by_id.get(run_id)
                    if metadata is None:
                        continue
                    direct_order = metadata['direct_order']
                    parent_run_id = metadata['parent_run_id']
                    if parent_run_id:
                        parent_metadata = run_metadata_by_id.get(parent_run_id)
                        if parent_metadata is None:
                            continue
                        lineage_order = parent_metadata['direct_order']
                    else:
                        lineage_order = direct_order
                    run_order_by_id[run_id] = (
                        lineage_order[0],
                        lineage_order[1],
                        direct_order[0],
                        direct_order[1],
                    )

            latest_snapshot_by_feedback_key: dict[
                tuple[int, str, str, str],
                Any,
            ] = {}
            feedback_ids = {
                int(row.id)
                for row in latest_feedback_map.values()
                if row.id is not None
            }
            if feedback_ids and set(requested_roles).intersection(
                DECISION_SUPPORT_AGENT_NAMES
            ):
                snapshot_rows = session.execute(
                    select(
                        SignalDeciderDecisionSnapshot.id,
                        SignalDeciderDecisionSnapshot.feedback_id,
                        SignalDeciderDecisionSnapshot.content_fingerprint,
                        SignalDeciderDecisionSnapshot.decision_run_id,
                        SignalDeciderDecisionSnapshot.feedback_received_at,
                        SignalDeciderDecisionSnapshot.symbol,
                        SignalDeciderDecisionSnapshot.agent_name,
                    )
                    .where(
                        SignalDeciderDecisionSnapshot.feedback_id.in_(
                            feedback_ids
                        ),
                        SignalDeciderDecisionSnapshot.agent_name.in_(
                            DECISION_SUPPORT_AGENT_NAMES
                        ),
                    )
                    .order_by(
                        SignalDeciderDecisionSnapshot.created_at.desc(),
                        SignalDeciderDecisionSnapshot.id.desc(),
                    )
                ).all()
                for snapshot_row in snapshot_rows:
                    key = (
                        int(snapshot_row.feedback_id),
                        str(snapshot_row.symbol),
                        str(snapshot_row.agent_name),
                        str(snapshot_row.decision_run_id or ''),
                    )
                    latest_snapshot_by_feedback_key.setdefault(key, snapshot_row)

            for symbol in normalized_symbols:
                symbol_id = symbol_id_map.get(symbol)
                for role in requested_roles:
                    agent_name = ROLE_TO_AGENT[role]
                    latest_feedback = latest_feedback_map.get((symbol, agent_name))
                    latest_failure = latest_failure_map.get((symbol, agent_name))

                    latest_source_updated_at = self._get_latest_source_updated_at(
                        session=session,
                        role=role,
                        market_date=normalized_market_date,
                        symbol=symbol,
                        symbol_id=symbol_id,
                    )
                    input_fingerprint_payload = (
                        self._build_fundamental_input_fingerprint_payload(
                            session=session,
                            market_date=normalized_market_date,
                            symbol=symbol,
                            symbol_id=symbol_id,
                        )
                        if role == FUNDAMENTAL_SUMMARIZER_AGENT_NAME
                        else None
                    )
                    input_fingerprint = _fingerprint_value(input_fingerprint_payload)

                    if latest_feedback is None:
                        decisions[symbol][role] = FeedbackReuseDecision(
                            symbol=symbol,
                            agent_name=agent_name,
                            should_dispatch=True,
                            reason='latest_failure_pending_retry' if latest_failure is not None else 'missing_feedback',
                            latest_feedback_id=None,
                            latest_feedback_received_at=None,
                            latest_source_updated_at=latest_source_updated_at.isoformat() if latest_source_updated_at else None,
                            latest_feedback=None,
                            input_fingerprint=input_fingerprint,
                            input_fingerprint_payload=input_fingerprint_payload,
                            reuse_policy_version=(
                                FUNDAMENTAL_SUMMARY_REUSE_POLICY_VERSION
                                if role == FUNDAMENTAL_SUMMARIZER_AGENT_NAME
                                else None
                            ),
                        )
                        continue

                    feedback_received_at = latest_feedback.received_at
                    latest_decision_snapshot = (
                        latest_snapshot_by_feedback_key.get(
                            (
                                int(latest_feedback.id),
                                symbol,
                                agent_name,
                                str(latest_feedback.run_id or ''),
                            )
                        )
                        if role in DECISION_SUPPORT_AGENT_NAMES
                        else None
                    )
                    has_newer_failure = _failure_supersedes_feedback(
                        feedback=latest_feedback,
                        failure=latest_failure,
                        run_order_by_id=run_order_by_id,
                    )
                    latest_feedback_payload = latest_feedback.feedback_payload if isinstance(latest_feedback.feedback_payload, dict) else {}
                    latest_risk_flags = latest_feedback_payload.get('risk_flags') if isinstance(latest_feedback_payload.get('risk_flags'), list) else []
                    has_retryable_feedback_gap = any(
                        isinstance(flag, str) and flag in {'missing_batch_feedback', 'unstructured_response'}
                        for flag in latest_risk_flags
                    ) or has_newer_failure
                    feedback_market_date = latest_feedback.market_date if latest_feedback is not None else None
                    is_stale_market_feedback = feedback_market_date is None or feedback_market_date != normalized_market_date
                    stored_input_fingerprint_payload = (
                        _extract_fingerprint_payload(latest_feedback.context_payload)
                        or _extract_fingerprint_payload(latest_feedback.summary_payload)
                    )
                    stored_input_fingerprint = _fingerprint_value(stored_input_fingerprint_payload)
                    if role == FUNDAMENTAL_SUMMARIZER_AGENT_NAME:
                        should_dispatch, reason = self._decide_fundamental_reuse(
                            force_recompute=force_recompute,
                            has_retryable_feedback_gap=has_retryable_feedback_gap,
                            input_fingerprint=input_fingerprint,
                            stored_input_fingerprint=stored_input_fingerprint,
                            latest_source_updated_at=latest_source_updated_at,
                            feedback_received_at=feedback_received_at,
                        )
                        if has_newer_failure:
                            reason = 'latest_failure_pending_retry'
                    else:
                        should_dispatch = force_recompute or is_stale_market_feedback or has_retryable_feedback_gap or (
                            latest_source_updated_at is not None and feedback_received_at is not None and latest_source_updated_at > feedback_received_at
                        )
                        reason = (
                            'forced_recompute' if force_recompute else (
                                'latest_failure_pending_retry' if has_newer_failure else (
                                    'stale_market_feedback' if is_stale_market_feedback else (
                                        'retryable_feedback_gap' if has_retryable_feedback_gap else ('source_updated' if should_dispatch else 'reuse_latest_feedback')
                                    )
                                )
                            )
                        )
                    decisions[symbol][role] = FeedbackReuseDecision(
                        symbol=symbol,
                        agent_name=agent_name,
                        should_dispatch=should_dispatch,
                        reason=reason,
                        latest_feedback_id=latest_feedback.id,
                        latest_feedback_received_at=feedback_received_at.isoformat() if feedback_received_at else None,
                        latest_source_updated_at=latest_source_updated_at.isoformat() if latest_source_updated_at else None,
                        latest_feedback={
                            'id': latest_feedback.id,
                            'market_date': latest_feedback.market_date.isoformat(),
                            'symbol': latest_feedback.symbol,
                            'agent_name': latest_feedback.agent_name,
                            'run_id': latest_feedback.run_id,
                            'summary_payload': latest_feedback.summary_payload,
                            'feedback_payload': latest_feedback.feedback_payload,
                            'context_payload': latest_feedback.context_payload,
                            'received_at': feedback_received_at.isoformat() if feedback_received_at else None,
                            'input_fingerprint': stored_input_fingerprint,
                            'input_fingerprint_payload': stored_input_fingerprint_payload,
                        },
                        latest_decision_snapshot_id=(
                            int(latest_decision_snapshot.id)
                            if latest_decision_snapshot is not None
                            else None
                        ),
                        latest_decision_snapshot_fingerprint=(
                            str(latest_decision_snapshot.content_fingerprint)
                            if latest_decision_snapshot is not None
                            and latest_decision_snapshot.content_fingerprint
                            else None
                        ),
                        latest_decision_snapshot_observed_at=(
                            latest_decision_snapshot.feedback_received_at.isoformat()
                            if latest_decision_snapshot is not None
                            and latest_decision_snapshot.feedback_received_at
                            else None
                        ),
                        input_fingerprint=input_fingerprint,
                        stored_input_fingerprint=stored_input_fingerprint,
                        input_fingerprint_payload=input_fingerprint_payload,
                        stored_input_fingerprint_payload=stored_input_fingerprint_payload,
                        reuse_policy_version=(
                            FUNDAMENTAL_SUMMARY_REUSE_POLICY_VERSION
                            if role == FUNDAMENTAL_SUMMARIZER_AGENT_NAME
                            else None
                        ),
                        reuse_source=(
                            'persisted_feedback_fingerprint'
                            if role == FUNDAMENTAL_SUMMARIZER_AGENT_NAME and not should_dispatch
                            else None
                        ),
                    )
        return decisions

    @staticmethod
    def _decide_fundamental_reuse(
        *,
        force_recompute: bool,
        has_retryable_feedback_gap: bool,
        input_fingerprint: str | None,
        stored_input_fingerprint: str | None,
        latest_source_updated_at: datetime | None,
        feedback_received_at: datetime | None,
    ) -> tuple[bool, str]:
        if force_recompute:
            return True, 'forced_recompute'
        if has_retryable_feedback_gap:
            return True, 'retryable_feedback_gap'
        if not input_fingerprint:
            return True, 'fundamental_input_fingerprint_missing'
        if not stored_input_fingerprint:
            return True, 'stored_fundamental_input_fingerprint_missing'
        if input_fingerprint != stored_input_fingerprint:
            return True, 'fundamental_input_fingerprint_changed'
        if latest_source_updated_at is not None and feedback_received_at is not None and latest_source_updated_at > feedback_received_at:
            return True, 'source_updated'
        return False, FUNDAMENTAL_SUMMARY_REUSE_REASON

    def build_latest_feedback_bundle(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str],
        roles: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbols = _normalize_symbols(symbols)
        requested_roles = list(roles or DEFAULT_AGENT_NAMES)
        decisions = self.build_latest_decisions(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
            roles=requested_roles,
        )
        items: list[dict[str, Any]] = []
        for symbol in normalized_symbols:
            role_items = []
            for role in requested_roles:
                decision = decisions.get(symbol, {}).get(role)
                if decision is None:
                    continue
                role_items.append(
                    {
                        'role': role,
                        'agent_name': decision.agent_name,
                        'should_dispatch': decision.should_dispatch,
                        'reason': decision.reason,
                        'latest_feedback_id': decision.latest_feedback_id,
                        'latest_feedback_received_at': decision.latest_feedback_received_at,
                        'latest_source_updated_at': decision.latest_source_updated_at,
                        'input_fingerprint': decision.input_fingerprint,
                        'stored_input_fingerprint': decision.stored_input_fingerprint,
                        'reuse_policy_version': decision.reuse_policy_version,
                        'reuse_source': decision.reuse_source,
                        'stale': _is_stale_dispatch_reason(decision.reason),
                        'latest_feedback': decision.latest_feedback,
                    }
                )
            items.append({'symbol': symbol, 'roles': role_items})
        return {
            'request': {
                'market_date': normalized_market_date.isoformat(),
                'symbols': normalized_symbols,
                'roles': requested_roles,
            },
            'items': items,
        }

    def _get_latest_source_updated_at(
        self,
        *,
        session,
        role: str,
        market_date: date,
        symbol: str,
        symbol_id: int | None,
    ) -> datetime | None:
        timestamps: list[datetime | None] = []
        if role == 'technical-summarizer':
            if symbol_id is not None:
                timestamps.append(
                    session.execute(
                        select(func.max(DailyTechnicalSnapshot.captured_at))
                        .where(DailyTechnicalSnapshot.symbol_id == symbol_id)
                        .where(DailyTechnicalSnapshot.market_date == market_date)
                    ).scalar_one()
                )
                timestamps.append(
                    session.execute(
                        select(func.max(DailyActionSummary.captured_at))
                        .where(DailyActionSummary.symbol_id == symbol_id)
                        .where(DailyActionSummary.market_date == market_date)
                    ).scalar_one()
                )
        elif role == 'fundamental-summarizer':
            timestamps.append(
                session.execute(
                    select(func.max(DartDisclosureEvent.updated_at))
                    .where(DartDisclosureEvent.stock_code == symbol)
                    .where(or_(DartDisclosureEvent.receipt_date == market_date, DartDisclosureEvent.receipt_date <= market_date))
                ).scalar_one()
            )
            timestamps.append(
                session.execute(
                    select(func.max(DartFundamentalsSnapshot.captured_at))
                    .join(DartDisclosureEvent, DartDisclosureEvent.id == DartFundamentalsSnapshot.disclosure_event_id)
                    .where(DartFundamentalsSnapshot.stock_code == symbol)
                    .where(DartDisclosureEvent.receipt_date <= market_date)
                ).scalar_one()
            )
        elif role == 'eod-summarizer':
            if symbol_id is not None:
                timestamps.append(
                    session.execute(
                        select(func.max(DailyPriceBar.captured_at))
                        .where(DailyPriceBar.symbol_id == symbol_id)
                        .where(DailyPriceBar.trade_date == market_date)
                    ).scalar_one()
                )
                timestamps.append(
                    session.execute(
                        select(func.max(DailyActionSummary.captured_at))
                        .where(DailyActionSummary.symbol_id == symbol_id)
                        .where(DailyActionSummary.market_date == market_date)
                    ).scalar_one()
                )
                timestamps.append(
                    session.execute(
                        select(func.max(DailyTechnicalSnapshot.captured_at))
                        .where(DailyTechnicalSnapshot.symbol_id == symbol_id)
                        .where(DailyTechnicalSnapshot.market_date == market_date)
                    ).scalar_one()
                )
        elif role in DECISION_SUPPORT_AGENT_NAMES:
            timestamps.append(
                session.execute(
                    select(func.max(AgentContextFeedback.received_at))
                    .where(AgentContextFeedback.symbol == symbol)
                    .where(AgentContextFeedback.market_date == market_date)
                    .where(
                        AgentContextFeedback.agent_name.in_(
                            ['eod-summarizer', 'technical-summarizer', 'fundamental-summarizer']
                        )
                    )
                ).scalar_one()
            )
        valid = [item for item in timestamps if item is not None]
        return max(valid) if valid else None

    def _build_fundamental_input_fingerprint_payload(
        self,
        *,
        session,
        market_date: date,
        symbol: str,
        symbol_id: int | None,
    ) -> dict[str, Any]:
        snapshot_pair = session.execute(
            select(DartFundamentalsSnapshot, DartDisclosureEvent)
            .join(DartDisclosureEvent, DartDisclosureEvent.id == DartFundamentalsSnapshot.disclosure_event_id)
            .where(DartFundamentalsSnapshot.stock_code == symbol)
            .where(or_(DartDisclosureEvent.receipt_date.is_(None), DartDisclosureEvent.receipt_date <= market_date))
            .order_by(
                DartDisclosureEvent.receipt_date.desc(),
                DartFundamentalsSnapshot.business_year.desc(),
                DartFundamentalsSnapshot.report_code.desc(),
                DartFundamentalsSnapshot.updated_at.desc(),
                DartFundamentalsSnapshot.id.desc(),
            )
            .limit(1)
        ).first()
        snapshot_payload: dict[str, Any] | None = None
        if snapshot_pair is not None:
            snapshot, event = snapshot_pair
            row_stats = session.execute(
                select(func.count(DartFundamentalsRow.id), func.max(DartFundamentalsRow.updated_at))
                .where(DartFundamentalsRow.snapshot_id == snapshot.id)
            ).one()
            highlight_stats = session.execute(
                select(func.count(DartFundamentalsHighlight.id), func.max(DartFundamentalsHighlight.updated_at))
                .where(DartFundamentalsHighlight.snapshot_id == snapshot.id)
            ).one()
            snapshot_payload = {
                'snapshot_id': snapshot.id,
                'disclosure_event_id': snapshot.disclosure_event_id,
                'rcept_no': snapshot.rcept_no,
                'corp_code': snapshot.corp_code,
                'stock_code': snapshot.stock_code,
                'business_year': snapshot.business_year,
                'report_code': snapshot.report_code,
                'filing_category': snapshot.filing_category,
                'fs_div': snapshot.fs_div,
                'statement_type': snapshot.statement_type,
                'period_type': snapshot.period_type,
                'period_end_month': snapshot.period_end_month,
                'currency': snapshot.currency,
                'source_status': snapshot.source_status,
                'source_message': snapshot.source_message,
                'captured_at': _isoformat(snapshot.captured_at),
                'created_at': _isoformat(snapshot.created_at),
                'updated_at': _isoformat(snapshot.updated_at),
                'statement_row_count': int(row_stats[0] or 0),
                'statement_rows_updated_at': _isoformat(row_stats[1]),
                'highlight_count': int(highlight_stats[0] or 0),
                'highlights_updated_at': _isoformat(highlight_stats[1]),
                'disclosure': {
                    'rcept_no': event.rcept_no,
                    'receipt_date': _isoformat(event.receipt_date),
                    'report_nm': event.report_nm,
                    'filing_category': event.filing_category,
                    'is_financial_report': event.is_financial_report,
                    'fundamentals_capture_status': event.fundamentals_capture_status,
                    'fundamentals_last_success_at': _isoformat(event.fundamentals_last_success_at),
                    'updated_at': _isoformat(event.updated_at),
                },
            }

        recent_disclosures = session.execute(
            select(DartDisclosureEvent)
            .where(DartDisclosureEvent.stock_code == symbol)
            .where(or_(DartDisclosureEvent.receipt_date.is_(None), DartDisclosureEvent.receipt_date <= market_date))
            .order_by(
                DartDisclosureEvent.receipt_date.desc(),
                DartDisclosureEvent.updated_at.desc(),
                DartDisclosureEvent.id.desc(),
            )
            .limit(5)
        ).scalars().all()
        disclosure_payloads = [
            {
                'id': row.id,
                'rcept_no': row.rcept_no,
                'receipt_date': _isoformat(row.receipt_date),
                'report_nm': row.report_nm,
                'filing_category': row.filing_category,
                'is_financial_report': row.is_financial_report,
                'fundamentals_capture_status': row.fundamentals_capture_status,
                'fundamentals_last_success_at': _isoformat(row.fundamentals_last_success_at),
                'updated_at': _isoformat(row.updated_at),
            }
            for row in recent_disclosures
        ]

        valuation_payloads: list[dict[str, Any]] = []
        if symbol_id is not None:
            valuation_rows = session.execute(
                select(DailyPykrxEnrichmentRow)
                .where(DailyPykrxEnrichmentRow.symbol_id == symbol_id)
                .where(DailyPykrxEnrichmentRow.market_date == market_date)
                .where(DailyPykrxEnrichmentRow.category.in_(('valuation_snapshot', 'foreign_ownership')))
                .order_by(DailyPykrxEnrichmentRow.category.asc(), DailyPykrxEnrichmentRow.source.asc())
            ).scalars().all()
            valuation_payloads = [
                {
                    'id': row.id,
                    'market_date': _isoformat(row.market_date),
                    'market': row.market,
                    'category': row.category,
                    'scope_type': row.scope_type,
                    'scope_key': row.scope_key,
                    'source': row.source,
                    'captured_at': _isoformat(row.captured_at),
                    'payload': row.payload if isinstance(row.payload, dict) else {},
                }
                for row in valuation_rows
            ]

        fingerprint_input = {
            'schema_version': FUNDAMENTAL_SUMMARY_INPUT_FINGERPRINT_SCHEMA_VERSION,
            'reuse_policy_version': FUNDAMENTAL_SUMMARY_REUSE_POLICY_VERSION,
            'agent_name': FUNDAMENTAL_SUMMARIZER_AGENT_NAME,
            'prompt_version': FUNDAMENTAL_SUMMARY_PROMPT_VERSION,
            'response_schema_version': FUNDAMENTAL_SUMMARY_RESPONSE_SCHEMA_VERSION,
            'symbol': symbol,
            'latest_fundamentals_snapshot': snapshot_payload,
            'recent_disclosures': disclosure_payloads,
            'valuation_inputs': valuation_payloads,
        }
        return {
            'schema_version': FUNDAMENTAL_SUMMARY_INPUT_FINGERPRINT_SCHEMA_VERSION,
            'reuse_policy_version': FUNDAMENTAL_SUMMARY_REUSE_POLICY_VERSION,
            'agent_name': FUNDAMENTAL_SUMMARIZER_AGENT_NAME,
            'prompt_version': FUNDAMENTAL_SUMMARY_PROMPT_VERSION,
            'response_schema_version': FUNDAMENTAL_SUMMARY_RESPONSE_SCHEMA_VERSION,
            'requested_market_date': market_date.isoformat(),
            'fingerprint': _stable_hash(fingerprint_input),
            'input': fingerprint_input,
        }


agent_context_reuse_service = AgentContextReuseService()
