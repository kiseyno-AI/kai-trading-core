from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.agent_context.feedback_service import _normalize_agent_name, _normalize_json_object, _normalize_market_date, _normalize_optional_text, _normalize_symbol
from app.db import session_scope
from app.db_models import AgentContextFeedbackFailure


class AgentContextFeedbackFailureService:
    @staticmethod
    def _normalize_failure_kind(value: str) -> str:
        normalized = (value or '').strip() or 'dispatch_failed'
        if len(normalized) > 64:
            return normalized[:64]
        return normalized

    def record_failure(
        self,
        *,
        market_date: str | date,
        symbol: str,
        agent_name: str,
        run_id: str | None,
        failure_kind: str,
        retryable: bool,
        summary: dict[str, Any],
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        market_date_value = _normalize_market_date(market_date)
        symbol_value = _normalize_symbol(symbol)
        agent_name_value = _normalize_agent_name(agent_name)
        failure_kind_value = self._normalize_failure_kind(failure_kind)
        summary_payload = _normalize_json_object(summary, field_name='summary')
        context_payload = _normalize_json_object(context, field_name='context') if context is not None else None
        received_at = datetime.now(timezone.utc)
        with session_scope() as session:
            stmt = insert(AgentContextFeedbackFailure).values(
                market_date=market_date_value,
                symbol=symbol_value,
                agent_name=agent_name_value,
                run_id=_normalize_optional_text(run_id, field_name='run_id', max_length=128),
                failure_kind=failure_kind_value,
                retryable=retryable,
                summary_payload=summary_payload,
                context_payload=context_payload,
                received_at=received_at,
            )
            stmt = stmt.on_conflict_do_update(
                constraint='uq_agent_context_feedback_failures_market_symbol_agent',
                set_={
                    'run_id': _normalize_optional_text(run_id, field_name='run_id', max_length=128),
                    'failure_kind': failure_kind_value,
                    'retryable': retryable,
                    'summary_payload': summary_payload,
                    'context_payload': context_payload,
                    'received_at': received_at,
                    'updated_at': received_at,
                },
            ).returning(AgentContextFeedbackFailure.id)
            record_id = session.execute(stmt).scalar_one()
            record = session.execute(
                select(AgentContextFeedbackFailure).where(AgentContextFeedbackFailure.id == record_id)
            ).scalar_one()
        return {
            'id': record.id,
            'market_date': record.market_date.isoformat(),
            'symbol': record.symbol,
            'agent_name': record.agent_name,
            'run_id': record.run_id,
            'failure_kind': record.failure_kind,
            'retryable': record.retryable,
            'summary': record.summary_payload,
            'context': record.context_payload,
            'received_at': record.received_at.isoformat() if record.received_at else None,
        }


agent_context_feedback_failure_service = AgentContextFeedbackFailureService()
