from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy.dialects.postgresql import insert

from app.agent_context.feedback_service import _normalize_agent_name, _normalize_json_object, _normalize_market_date, _normalize_optional_text, _normalize_symbol
from app.agent_context.feedback_failure_taxonomy import classify_feedback_failure
from app.db import session_scope
from app.db_models import AgentContextFeedbackFailure


class AgentContextFeedbackFailureService:
    @staticmethod
    def _normalize_failure_kind(value: str) -> str:
        normalized = (value or '').strip() or 'dispatch_failed'
        if len(normalized) > 64:
            return normalized[:64]
        return normalized

    @staticmethod
    def _serialize_failure_row(row: Mapping[str, Any]) -> dict[str, Any]:
        market_date = row['market_date']
        received_at = row['received_at']
        return {
            'id': row['id'],
            'market_date': market_date.isoformat() if hasattr(market_date, 'isoformat') else str(market_date),
            'symbol': row['symbol'],
            'agent_name': row['agent_name'],
            'run_id': row['run_id'],
            'failure_kind': row['failure_kind'],
            'retryable': row['retryable'],
            'summary': row['summary_payload'],
            'context': row['context_payload'],
            'received_at': received_at.isoformat() if received_at else None,
        }

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
        summary_payload = _normalize_json_object(summary, field_name='summary')
        context_payload = _normalize_json_object(context, field_name='context') if context is not None else None
        raw_failure_kind = self._normalize_failure_kind(failure_kind)
        classification = classify_feedback_failure(
            failure_kind=raw_failure_kind,
            summary=summary_payload,
            context=context_payload,
            error=str(summary_payload.get('error') or '') or None,
            exception_type=str(summary_payload.get('exception_type') or '') or None,
        )
        failure_kind_value = self._normalize_failure_kind(classification.failure_kind)
        failure_classification = classification.as_payload()
        summary_payload = {
            **summary_payload,
            'failure_kind': failure_kind_value,
            'raw_failure_kind': raw_failure_kind,
            'failure_status': classification.failure_status,
            'failure_category': classification.failure_category,
            'failure_classification': failure_classification,
        }
        context_payload = (
            {
                **context_payload,
                'failure_classification': failure_classification,
            }
            if isinstance(context_payload, dict)
            else {'failure_classification': failure_classification}
        )
        run_id_value = _normalize_optional_text(run_id, field_name='run_id', max_length=128)
        received_at = datetime.now(timezone.utc)
        with session_scope() as session:
            stmt = insert(AgentContextFeedbackFailure).values(
                market_date=market_date_value,
                symbol=symbol_value,
                agent_name=agent_name_value,
                run_id=run_id_value,
                failure_kind=failure_kind_value,
                retryable=retryable,
                summary_payload=summary_payload,
                context_payload=context_payload,
                received_at=received_at,
            )
            stmt = stmt.on_conflict_do_update(
                constraint='uq_agent_context_feedback_failures_market_symbol_agent',
                set_={
                    'run_id': run_id_value,
                    'failure_kind': failure_kind_value,
                    'retryable': retryable,
                    'summary_payload': summary_payload,
                    'context_payload': context_payload,
                    'received_at': received_at,
                    'updated_at': received_at,
                },
            ).returning(
                AgentContextFeedbackFailure.id,
                AgentContextFeedbackFailure.market_date,
                AgentContextFeedbackFailure.symbol,
                AgentContextFeedbackFailure.agent_name,
                AgentContextFeedbackFailure.run_id,
                AgentContextFeedbackFailure.failure_kind,
                AgentContextFeedbackFailure.retryable,
                AgentContextFeedbackFailure.summary_payload,
                AgentContextFeedbackFailure.context_payload,
                AgentContextFeedbackFailure.received_at,
            )
            row = session.execute(stmt).mappings().one()
            return self._serialize_failure_row(row)


agent_context_feedback_failure_service = AgentContextFeedbackFailureService()
