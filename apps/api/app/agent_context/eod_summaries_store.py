from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import select

from app.db import session_scope
from app.agent_context.missing_data_taxonomy import build_missing_data_categories, enrich_missing_data_categories
from app.db_models import AgentContextFeedback

EOD_SUMMARIZER_AGENT_NAME = 'eod-summarizer'


def _feedback_payload(record: AgentContextFeedback) -> dict[str, Any]:
    if isinstance(record.normalized_feedback_payload, dict):
        return record.normalized_feedback_payload
    if isinstance(record.feedback_payload, dict):
        return record.feedback_payload
    return {}


def _serialize_eod_summary(record: AgentContextFeedback) -> dict[str, Any]:
    payload = _feedback_payload(record)
    risk_flags = payload.get('risk_flags') if isinstance(payload.get('risk_flags'), list) else []
    raw_categories = record.missing_data_categories_payload
    if not isinstance(raw_categories, list):
        raw_categories = build_missing_data_categories(
            data_gaps_payload=record.data_gaps_payload,
            improvement_notes_payload=record.improvement_notes_payload,
        )
    return {
        'symbol': record.symbol,
        'market_date': record.market_date.isoformat(),
        'agent_name': record.agent_name,
        'agent_version': record.agent_version,
        'run_id': record.run_id,
        'role': payload.get('role') if isinstance(payload.get('role'), str) else record.agent_name,
        'summary': payload.get('summary') if isinstance(payload.get('summary'), str) else None,
        'confidence': payload.get('confidence') if isinstance(payload.get('confidence'), (int, float)) else None,
        'risk_flags': risk_flags,
        'details': record.details_payload,
        'breadth_context': record.breadth_context_payload,
        'leaders_laggards': record.leaders_laggards_payload,
        'price_action': record.price_action_payload,
        'trend_context': record.trend_context_payload,
        'support_resistance': record.support_resistance_payload,
        'anomalies_and_gaps': record.anomalies_and_gaps_payload,
        'data_gaps': record.data_gaps_payload,
        'fundamental_backdrop': record.fundamental_backdrop_payload,
        'conservative_take': record.conservative_take_payload,
        'improvement_notes': record.improvement_notes_payload,
        'missing_data_categories': enrich_missing_data_categories(raw_categories),
        'received_at': record.received_at.isoformat() if record.received_at else None,
    }


class AgentContextEodSummariesStore:
    def list_for_market_date(self, *, market_date: date) -> list[dict[str, Any]]:
        with session_scope() as session:
            rows = session.execute(
                select(AgentContextFeedback)
                .where(AgentContextFeedback.market_date == market_date)
                .where(AgentContextFeedback.agent_name == EOD_SUMMARIZER_AGENT_NAME)
                .order_by(AgentContextFeedback.symbol.asc())
            ).scalars().all()
            return [_serialize_eod_summary(row) for row in rows]

    def get_for_symbol(self, *, market_date: date, symbol: str) -> dict[str, Any] | None:
        with session_scope() as session:
            row = session.execute(
                select(AgentContextFeedback)
                .where(AgentContextFeedback.market_date == market_date)
                .where(AgentContextFeedback.symbol == symbol)
                .where(AgentContextFeedback.agent_name == EOD_SUMMARIZER_AGENT_NAME)
                .limit(1)
            ).scalar_one_or_none()
            if row is None:
                return None
            return _serialize_eod_summary(row)


agent_context_eod_summaries_store = AgentContextEodSummariesStore()
