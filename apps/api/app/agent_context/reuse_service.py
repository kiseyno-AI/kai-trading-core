from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Sequence

from sqlalchemy import func, select

from app.agent_context.feedback_service import _normalize_market_date, _normalize_symbols
from app.agent_context.status_service import EXPECTED_AGENT_NAMES
from app.db import session_scope
from app.db_models import (
    AgentContextFeedback,
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

            for symbol in normalized_symbols:
                symbol_id = symbol_id_map.get(symbol)
                for role in requested_roles:
                    agent_name = ROLE_TO_AGENT[role]
                    latest_feedback = session.execute(
                        select(AgentContextFeedback)
                        .where(AgentContextFeedback.market_date <= normalized_market_date)
                        .where(AgentContextFeedback.symbol == symbol)
                        .where(AgentContextFeedback.agent_name == agent_name)
                        .order_by(AgentContextFeedback.market_date.desc(), AgentContextFeedback.received_at.desc(), AgentContextFeedback.id.desc())
                        .limit(1)
                    ).scalar_one_or_none()

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
                            reason='missing_feedback',
                            latest_feedback_id=None,
                            latest_feedback_received_at=None,
                            latest_source_updated_at=latest_source_updated_at.isoformat() if latest_source_updated_at else None,
                            latest_feedback=None,
                        )
                        continue

                    feedback_received_at = latest_feedback.received_at
                    should_dispatch = latest_source_updated_at is not None and feedback_received_at is not None and latest_source_updated_at > feedback_received_at
                    decisions[symbol][role] = FeedbackReuseDecision(
                        symbol=symbol,
                        agent_name=agent_name,
                        should_dispatch=should_dispatch,
                        reason='source_updated' if should_dispatch else 'reuse_latest_feedback',
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
