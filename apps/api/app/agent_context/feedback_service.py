from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Sequence

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Query

from app.db import session_scope
from app.db_models import AgentContextFeedback


def _normalize_market_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _normalize_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if not normalized:
        raise ValueError('symbol is required')
    if len(normalized) > 32:
        raise ValueError('symbol must be 32 characters or fewer')
    return normalized


def _normalize_symbols(symbols: Sequence[str] | None) -> list[str]:
    if not symbols:
        return []

    normalized_symbols: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        for piece in (raw_symbol or '').split(','):
            symbol = _normalize_symbol(piece)
            if symbol in seen:
                continue
            seen.add(symbol)
            normalized_symbols.append(symbol)
    return normalized_symbols


def _normalize_agent_name(agent_name: str | None) -> str:
    normalized = (agent_name or 'openclaw').strip().lower()
    if not normalized:
        raise ValueError('agent_name is required')
    if len(normalized) > 64:
        raise ValueError('agent_name must be 64 characters or fewer')
    return normalized


def _normalize_optional_text(value: str | None, *, field_name: str, max_length: int) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if len(normalized) > max_length:
        raise ValueError(f'{field_name} must be {max_length} characters or fewer')
    return normalized


def _normalize_json_object(value: dict[str, Any] | None, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f'{field_name} must be a JSON object')
    return value


def _serialize_feedback(record: AgentContextFeedback) -> dict[str, Any]:
    return {
        'id': record.id,
        'market_date': record.market_date.isoformat(),
        'symbol': record.symbol,
        'agent': {
            'name': record.agent_name,
            'version': record.agent_version,
            'run_id': record.run_id,
        },
        'summary': record.summary_payload,
        'feedback': record.feedback_payload,
        'context': record.context_payload,
        'received_at': record.received_at.isoformat() if record.received_at else None,
    }


def _apply_feedback_filters(
    stmt: Query | Any,
    *,
    market_date: date,
    symbols: list[str],
    agent_name: str | None = None,
) -> Query | Any:
    stmt = stmt.where(AgentContextFeedback.market_date == market_date)
    if symbols:
        stmt = stmt.where(AgentContextFeedback.symbol.in_(symbols))
    if agent_name:
        stmt = stmt.where(AgentContextFeedback.agent_name == agent_name)
    return stmt


class AgentContextFeedbackRepository:
    def create_feedback(
        self,
        *,
        market_date: date,
        symbol: str,
        agent_name: str,
        agent_version: str | None,
        run_id: str | None,
        summary_payload: dict[str, Any],
        feedback_payload: dict[str, Any],
        context_payload: dict[str, Any] | None,
    ) -> dict[str, Any]:
        received_at = datetime.now(timezone.utc)
        with session_scope() as session:
            stmt = insert(AgentContextFeedback).values(
                market_date=market_date,
                symbol=symbol,
                agent_name=agent_name,
                agent_version=agent_version,
                run_id=run_id,
                summary_payload=summary_payload,
                feedback_payload=feedback_payload,
                context_payload=context_payload,
                received_at=received_at,
            )
            stmt = stmt.on_conflict_do_update(
                constraint='uq_agent_context_feedback_market_symbol_agent',
                set_={
                    'agent_version': agent_version,
                    'run_id': run_id,
                    'summary_payload': summary_payload,
                    'feedback_payload': feedback_payload,
                    'context_payload': context_payload,
                    'received_at': received_at,
                    'updated_at': received_at,
                },
            ).returning(AgentContextFeedback.id)
            record_id = session.execute(stmt).scalar_one()
            record = session.get(AgentContextFeedback, record_id)
            if record is None:
                raise RuntimeError('Failed to load stored feedback record after upsert')
            return _serialize_feedback(record)

    def list_feedback(
        self,
        *,
        market_date: date,
        symbols: list[str],
        agent_name: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        with session_scope() as session:
            stmt = select(AgentContextFeedback)
            stmt = _apply_feedback_filters(stmt, market_date=market_date, symbols=symbols, agent_name=agent_name)
            stmt = stmt.order_by(AgentContextFeedback.received_at.desc(), AgentContextFeedback.id.desc()).limit(limit)


            rows = session.execute(stmt).scalars().all()
            return [_serialize_feedback(row) for row in rows]


    def list_feedback_payloads(
        self,
        *,
        market_date: date,
        symbols: list[str],
        agent_name: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        with session_scope() as session:
            stmt = select(AgentContextFeedback)
            stmt = _apply_feedback_filters(stmt, market_date=market_date, symbols=symbols, agent_name=agent_name)
            stmt = stmt.order_by(AgentContextFeedback.received_at.desc(), AgentContextFeedback.id.desc()).limit(limit)
            rows = session.execute(stmt).scalars().all()
            return [
                {
                    'market_date': row.market_date.isoformat(),
                    'symbol': row.symbol,
                    'agent_name': row.agent_name,
                    'run_id': row.run_id,
                    'received_at': row.received_at.isoformat() if row.received_at else None,
                    'payload': row.feedback_payload,
                }
                for row in rows
            ]

    def list_request_payloads(
        self,
        *,
        market_date: date,
        symbols: list[str],
        agent_name: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        with session_scope() as session:
            stmt = select(AgentContextFeedback)
            stmt = _apply_feedback_filters(stmt, market_date=market_date, symbols=symbols, agent_name=agent_name)
            stmt = stmt.order_by(AgentContextFeedback.received_at.desc(), AgentContextFeedback.id.desc()).limit(limit)
            rows = session.execute(stmt).scalars().all()
            return [
                {
                    'market_date': row.market_date.isoformat(),
                    'symbol': row.symbol,
                    'agent_name': row.agent_name,
                    'run_id': row.run_id,
                    'received_at': row.received_at.isoformat() if row.received_at else None,
                    'payload': row.context_payload,
                }
                for row in rows
            ]


class AgentContextFeedbackService:
    def __init__(self, *, repository: AgentContextFeedbackRepository | None = None) -> None:
        self.repository = repository or AgentContextFeedbackRepository()

    def submit_feedback(
        self,
        *,
        market_date: str | date,
        symbol: str,
        agent_name: str | None = None,
        agent_version: str | None = None,
        run_id: str | None = None,
        summary: dict[str, Any] | None = None,
        feedback: dict[str, Any] | None = None,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        summary_payload = _normalize_json_object(summary, field_name='summary')
        feedback_payload = _normalize_json_object(feedback, field_name='feedback')
        context_payload = _normalize_json_object(context, field_name='context') if context is not None else None
        if not summary_payload and not feedback_payload:
            raise ValueError('Either summary or feedback must be provided')

        item = self.repository.create_feedback(
            market_date=_normalize_market_date(market_date),
            symbol=_normalize_symbol(symbol),
            agent_name=_normalize_agent_name(agent_name),
            agent_version=_normalize_optional_text(agent_version, field_name='agent_version', max_length=64),
            run_id=_normalize_optional_text(run_id, field_name='run_id', max_length=128),
            summary_payload=summary_payload,
            feedback_payload=feedback_payload,
            context_payload=context_payload,
        )
        return {
            'accepted': True,
            'item': item,
            'summary': {
                'summary_keys': sorted(summary_payload.keys()),
                'feedback_keys': sorted(feedback_payload.keys()),
            },
        }

    def list_feedback(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
        agent_name: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbols = _normalize_symbols(symbols)
        normalized_agent_name = _normalize_agent_name(agent_name) if agent_name is not None else None
        items = self.repository.list_feedback(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
            agent_name=normalized_agent_name,
            limit=limit,
        )
        return {
            'request': {
                'market_date': normalized_market_date.isoformat(),
                'symbols': normalized_symbols,
                'agent_name': normalized_agent_name,
                'limit': limit,
            },
            'summary': {
                'count': len(items),
            },
            'items': items,
        }


    def get_feedback_payloads(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str],
        agent_name: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbols = _normalize_symbols(symbols)
        normalized_agent_name = _normalize_agent_name(agent_name) if agent_name is not None else None
        items = self.repository.list_feedback_payloads(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
            agent_name=normalized_agent_name,
            limit=limit,
        )
        return {
            'request': {
                'market_date': normalized_market_date.isoformat(),
                'symbols': normalized_symbols,
                'agent_name': normalized_agent_name,
                'limit': limit,
            },
            'summary': {'count': len(items)},
            'items': items,
        }

    def get_request_payloads(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str],
        agent_name: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbols = _normalize_symbols(symbols)
        normalized_agent_name = _normalize_agent_name(agent_name) if agent_name is not None else None
        items = self.repository.list_request_payloads(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
            agent_name=normalized_agent_name,
            limit=limit,
        )
        return {
            'request': {
                'market_date': normalized_market_date.isoformat(),
                'symbols': normalized_symbols,
                'agent_name': normalized_agent_name,
                'limit': limit,
            },
            'summary': {'count': len(items)},
            'items': items,
        }


agent_context_feedback_service = AgentContextFeedbackService()
