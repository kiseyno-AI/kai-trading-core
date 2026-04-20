from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Sequence

from sqlalchemy import func, or_, select

from app.agent_context.feedback_service import _normalize_market_date, _normalize_symbols
from app.agent_context.status_service import EXPECTED_AGENT_NAMES
from app.db import session_scope
from app.db_models import (
    AgentContextFeedback,
    AgentContextFeedbackFailure,
    DailyActionSummary,
    DailyPriceBar,
    DailyTechnicalSnapshot,
    DartDisclosureEvent,
    DartFundamentalsSnapshot,
    Symbol,
)


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


ROLE_TO_AGENT = {
    'eod-summarizer': 'eod-summarizer',
    'technical-summarizer': 'technical-summarizer',
    'fundamental-summarizer': 'fundamental-summarizer',
}


def _chunked(values: Sequence[str], size: int) -> list[list[str]]:
    normalized = [str(value) for value in values]
    return [normalized[index:index + size] for index in range(0, len(normalized), size)]


class AgentContextReuseService:
    def build_latest_decisions(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str],
        roles: Sequence[str],
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
                    should_dispatch = is_stale_market_feedback or has_retryable_feedback_gap or (
                        latest_source_updated_at is not None and feedback_received_at is not None and latest_source_updated_at > feedback_received_at
                    )
                    reason = (
                        'stale_market_feedback' if is_stale_market_feedback else (
                            'retryable_feedback_gap' if has_retryable_feedback_gap else ('source_updated' if should_dispatch else 'reuse_latest_feedback')
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
                        },
                    )
        return decisions

    def build_latest_feedback_bundle(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str],
        roles: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbols = _normalize_symbols(symbols)
        requested_roles = list(roles or EXPECTED_AGENT_NAMES)
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
        valid = [item for item in timestamps if item is not None]
        return max(valid) if valid else None


agent_context_reuse_service = AgentContextReuseService()
