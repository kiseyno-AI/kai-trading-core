from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Sequence

from fastapi.encoders import jsonable_encoder
from sqlalchemy import func, or_, select

from app.agent_context.feedback_service import _normalize_market_date, _normalize_symbols
from app.agent_context.roles import DEFAULT_AGENT_NAMES, SIGNAL_DECIDER_AGENT_NAME
from app.db import session_scope
from app.db_models import (
    AgentContextFeedback,
    AgentContextFeedbackFailure,
    DailyActionSummary,
    DailyPriceBar,
    DailyPykrxEnrichmentRow,
    DailyTechnicalSnapshot,
    DartDisclosureEvent,
    DartFundamentalsHighlight,
    DartFundamentalsRow,
    DartFundamentalsSnapshot,
    Symbol,
)


FUNDAMENTAL_SUMMARIZER_AGENT_NAME = 'fundamental-summarizer'
FUNDAMENTAL_SUMMARY_INPUT_FINGERPRINT_SCHEMA_VERSION = 'fundamental_summary_input_fingerprint_v1'
FUNDAMENTAL_SUMMARY_PROMPT_VERSION = 'fundamental_summarizer_prompt_v1'
FUNDAMENTAL_SUMMARY_RESPONSE_SCHEMA_VERSION = 'agent_feedback_response_contract_v1'
FUNDAMENTAL_SUMMARY_REUSE_POLICY_VERSION = 'fundamental_summary_reuse_policy_v1'
FUNDAMENTAL_SUMMARY_REUSE_REASON = 'reuse_unchanged_fundamental_inputs'

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
        with session_scope() as session:
            symbol_rows = session.execute(
                select(Symbol.id, Symbol.symbol)
                .where(Symbol.symbol.in_(normalized_symbols))
            ).all()
            symbol_id_map = {str(row.symbol): int(row.id) for row in symbol_rows}

            latest_feedback_map: dict[tuple[str, str], AgentContextFeedback] = {}
            latest_failure_map: dict[tuple[str, str], AgentContextFeedbackFailure] = {}
            for role in requested_roles:
                agent_name = ROLE_TO_AGENT[role]
                for chunk in _chunked(normalized_symbols, 500):
                    feedback_rows = session.execute(
                        select(AgentContextFeedback)
                        .where(AgentContextFeedback.market_date <= normalized_market_date)
                        .where(AgentContextFeedback.symbol.in_(chunk))
                        .where(AgentContextFeedback.agent_name == agent_name)
                        .order_by(AgentContextFeedback.symbol.asc(), AgentContextFeedback.market_date.desc(), AgentContextFeedback.received_at.desc(), AgentContextFeedback.id.desc())
                    ).scalars().all()
                    for row in feedback_rows:
                        key = (row.symbol, agent_name)
                        if key not in latest_feedback_map:
                            latest_feedback_map[key] = row
                    failure_rows = session.execute(
                        select(AgentContextFeedbackFailure)
                        .where(AgentContextFeedbackFailure.market_date <= normalized_market_date)
                        .where(AgentContextFeedbackFailure.symbol.in_(chunk))
                        .where(AgentContextFeedbackFailure.agent_name == agent_name)
                        .order_by(AgentContextFeedbackFailure.symbol.asc(), AgentContextFeedbackFailure.market_date.desc(), AgentContextFeedbackFailure.received_at.desc(), AgentContextFeedbackFailure.id.desc())
                    ).scalars().all()
                    for row in failure_rows:
                        key = (row.symbol, agent_name)
                        if key not in latest_failure_map:
                            latest_failure_map[key] = row

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
                    latest_feedback_payload = latest_feedback.feedback_payload if isinstance(latest_feedback.feedback_payload, dict) else {}
                    latest_risk_flags = latest_feedback_payload.get('risk_flags') if isinstance(latest_feedback_payload.get('risk_flags'), list) else []
                    has_retryable_feedback_gap = any(
                        isinstance(flag, str) and flag in {'missing_batch_feedback', 'unstructured_response'}
                        for flag in latest_risk_flags
                    )
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
                    else:
                        should_dispatch = force_recompute or is_stale_market_feedback or has_retryable_feedback_gap or (
                            latest_source_updated_at is not None and feedback_received_at is not None and latest_source_updated_at > feedback_received_at
                        )
                        reason = (
                            'forced_recompute' if force_recompute else (
                                'stale_market_feedback' if is_stale_market_feedback else (
                                    'retryable_feedback_gap' if has_retryable_feedback_gap else ('source_updated' if should_dispatch else 'reuse_latest_feedback')
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
        elif role == SIGNAL_DECIDER_AGENT_NAME:
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
