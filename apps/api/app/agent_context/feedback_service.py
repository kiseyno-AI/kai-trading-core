from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Sequence

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Query

from app.db import session_scope
from app.db_models import AgentContextFeedback
from app.agent_context.missing_data_taxonomy import build_missing_data_categories, enrich_missing_data_categories
from app.agent_context.signal_decider_loop_service import signal_decider_loop_service

BULK_UPSERT_CHUNK_SIZE = 100


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


def _extract_list_feedback_context(record: AgentContextFeedback) -> dict[str, Any] | None:
    context_payload = record.context_payload
    if not isinstance(context_payload, dict):
        return None

    symbol_day_context = context_payload.get('symbol_day_context') or context_payload.get('symbolDayContext')
    if not isinstance(symbol_day_context, dict):
        return None

    list_symbol_context: dict[str, Any] = {}
    for key in ('symbol', 'market', 'sector', 'industry'):
        value = symbol_day_context.get(key)
        if value is not None:
            list_symbol_context[key] = value

    company = symbol_day_context.get('company')
    if isinstance(company, dict):
        list_company = {
            key: company[key]
            for key in ('name', 'name_ko', 'ticker')
            if company.get(key) is not None
        }
        if list_company:
            list_symbol_context['company'] = list_company

    if not list_symbol_context:
        return None
    return {'symbol_day_context': list_symbol_context}


def _serialize_feedback(record: AgentContextFeedback) -> dict[str, Any]:
    raw_categories = record.missing_data_categories_payload
    if not isinstance(raw_categories, list):
        raw_categories = build_missing_data_categories(
            data_gaps_payload=record.data_gaps_payload,
            improvement_notes_payload=record.improvement_notes_payload,
            context_payload=record.context_payload,
        )
    return {
        'id': record.id,
        'market_date': record.market_date.isoformat(),
        'symbol': record.symbol,
        'agent': {
            'name': record.agent_name,
            'version': record.agent_version,
            'run_id': record.run_id,
        },
        'summary': {},
        'feedback': record.feedback_payload,
        'normalized_feedback': record.normalized_feedback_payload,
        'details': record.details_payload,
        'data_gaps': record.data_gaps_payload,
        'improvement_notes': record.improvement_notes_payload,
        'missing_data_categories': enrich_missing_data_categories(raw_categories),
        'context': _extract_list_feedback_context(record),
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


def _feedback_conflict_key(item: dict[str, Any]) -> tuple[date, str, str]:
    return (item['market_date'], item['symbol'], item['agent_name'])


def _dedupe_feedback_items_for_upsert(items: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped_reversed: list[dict[str, Any]] = []
    seen_keys: set[tuple[date, str, str]] = set()

    for item in reversed(items):
        key = _feedback_conflict_key(item)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        deduped_reversed.append(item)

    deduped_reversed.reverse()
    return deduped_reversed


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
        normalized_feedback_payload: dict[str, Any] | None,
        details_payload: dict[str, Any] | None,
        breadth_context_payload: dict[str, Any] | None,
        leaders_laggards_payload: dict[str, Any] | None,
        price_action_payload: dict[str, Any] | None,
        trend_context_payload: dict[str, Any] | None,
        support_resistance_payload: dict[str, Any] | None,
        anomalies_and_gaps_payload: dict[str, Any] | None,
        data_gaps_payload: dict[str, Any] | None,
        fundamental_backdrop_payload: dict[str, Any] | None,
        conservative_take_payload: dict[str, Any] | None,
        improvement_notes_payload: list[Any] | dict[str, Any] | None,
        missing_data_categories_payload: list[Any] | None,
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
                normalized_feedback_payload=normalized_feedback_payload,
                details_payload=details_payload,
                breadth_context_payload=breadth_context_payload,
                leaders_laggards_payload=leaders_laggards_payload,
                price_action_payload=price_action_payload,
                trend_context_payload=trend_context_payload,
                support_resistance_payload=support_resistance_payload,
                anomalies_and_gaps_payload=anomalies_and_gaps_payload,
                data_gaps_payload=data_gaps_payload,
                fundamental_backdrop_payload=fundamental_backdrop_payload,
                conservative_take_payload=conservative_take_payload,
                improvement_notes_payload=improvement_notes_payload,
                missing_data_categories_payload=missing_data_categories_payload,
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
                    'normalized_feedback_payload': normalized_feedback_payload,
                    'details_payload': details_payload,
                    'breadth_context_payload': breadth_context_payload,
                    'leaders_laggards_payload': leaders_laggards_payload,
                    'price_action_payload': price_action_payload,
                    'trend_context_payload': trend_context_payload,
                    'support_resistance_payload': support_resistance_payload,
                    'anomalies_and_gaps_payload': anomalies_and_gaps_payload,
                    'data_gaps_payload': data_gaps_payload,
                    'fundamental_backdrop_payload': fundamental_backdrop_payload,
                    'conservative_take_payload': conservative_take_payload,
                    'improvement_notes_payload': improvement_notes_payload,
                    'missing_data_categories_payload': missing_data_categories_payload,
                    'context_payload': context_payload,
                    'received_at': received_at,
                    'updated_at': received_at,
                },
            ).returning(AgentContextFeedback.id)
            record_id = session.execute(stmt).scalar_one()
            record = session.get(AgentContextFeedback, record_id)
            if record is None:
                raise RuntimeError('Failed to load stored feedback record after upsert')
            serialized = _serialize_feedback(record)
            snapshot_item = signal_decider_loop_service.prepare_snapshot_persistence_item(
                feedback_id=record.id,
                market_date=record.market_date,
                symbol=record.symbol,
                agent_name=record.agent_name,
                agent_version=record.agent_version,
                run_id=record.run_id,
                summary_payload=record.summary_payload,
                feedback_payload=record.feedback_payload,
                normalized_feedback_payload=record.normalized_feedback_payload,
                context_payload=record.context_payload,
                received_at=record.received_at,
            )
            if snapshot_item is not None:
                signal_decider_loop_service.repository.insert_decision_snapshots(
                    session=session,
                    items=[snapshot_item],
                )
            return serialized

    def bulk_upsert_feedback(
        self,
        *,
        items: Sequence[dict[str, Any]],
        chunk_size: int = BULK_UPSERT_CHUNK_SIZE,
    ) -> list[dict[str, Any]]:
        deduped_items = _dedupe_feedback_items_for_upsert(items)
        if not deduped_items:
            return []

        resolved_chunk_size = max(1, chunk_size)
        accepted: list[dict[str, Any]] = []
        with session_scope() as session:
            for start in range(0, len(deduped_items), resolved_chunk_size):
                chunk = list(deduped_items[start:start + resolved_chunk_size])
                received_at = datetime.now(timezone.utc)
                chunk_rows = [{**item, 'received_at': received_at} for item in chunk]
                stmt = insert(AgentContextFeedback).values(chunk_rows)
                excluded = stmt.excluded
                stmt = stmt.on_conflict_do_update(
                    constraint='uq_agent_context_feedback_market_symbol_agent',
                    set_={
                        'agent_version': excluded.agent_version,
                        'run_id': excluded.run_id,
                        'summary_payload': excluded.summary_payload,
                        'feedback_payload': excluded.feedback_payload,
                        'normalized_feedback_payload': excluded.normalized_feedback_payload,
                        'details_payload': excluded.details_payload,
                        'breadth_context_payload': excluded.breadth_context_payload,
                        'leaders_laggards_payload': excluded.leaders_laggards_payload,
                        'price_action_payload': excluded.price_action_payload,
                        'trend_context_payload': excluded.trend_context_payload,
                        'support_resistance_payload': excluded.support_resistance_payload,
                        'anomalies_and_gaps_payload': excluded.anomalies_and_gaps_payload,
                        'data_gaps_payload': excluded.data_gaps_payload,
                        'fundamental_backdrop_payload': excluded.fundamental_backdrop_payload,
                        'conservative_take_payload': excluded.conservative_take_payload,
                        'improvement_notes_payload': excluded.improvement_notes_payload,
                        'missing_data_categories_payload': excluded.missing_data_categories_payload,
                        'context_payload': excluded.context_payload,
                        'received_at': excluded.received_at,
                        'updated_at': received_at,
                    },
                ).returning(
                    AgentContextFeedback.id,
                    AgentContextFeedback.market_date,
                    AgentContextFeedback.symbol,
                    AgentContextFeedback.agent_name,
                    AgentContextFeedback.agent_version,
                    AgentContextFeedback.run_id,
                    AgentContextFeedback.summary_payload,
                    AgentContextFeedback.feedback_payload,
                    AgentContextFeedback.normalized_feedback_payload,
                    AgentContextFeedback.details_payload,
                    AgentContextFeedback.breadth_context_payload,
                    AgentContextFeedback.leaders_laggards_payload,
                    AgentContextFeedback.price_action_payload,
                    AgentContextFeedback.trend_context_payload,
                    AgentContextFeedback.support_resistance_payload,
                    AgentContextFeedback.anomalies_and_gaps_payload,
                    AgentContextFeedback.data_gaps_payload,
                    AgentContextFeedback.fundamental_backdrop_payload,
                    AgentContextFeedback.conservative_take_payload,
                    AgentContextFeedback.improvement_notes_payload,
                    AgentContextFeedback.missing_data_categories_payload,
                    AgentContextFeedback.context_payload,
                    AgentContextFeedback.received_at,
                )
                returned_rows = session.execute(stmt).mappings().all()
                snapshot_items: list[dict[str, Any]] = []
                rows_by_key = {
                    (row['market_date'], row['symbol'], row['agent_name']): row
                    for row in returned_rows
                }
                for item in chunk:
                    row = rows_by_key[(item['market_date'], item['symbol'], item['agent_name'])]
                    snapshot_item = signal_decider_loop_service.prepare_snapshot_persistence_item(
                        feedback_id=row['id'],
                        market_date=row['market_date'],
                        symbol=row['symbol'],
                        agent_name=row['agent_name'],
                        agent_version=row['agent_version'],
                        run_id=row['run_id'],
                        summary_payload=row['summary_payload'],
                        feedback_payload=row['feedback_payload'],
                        normalized_feedback_payload=row['normalized_feedback_payload'],
                        context_payload=row['context_payload'],
                        received_at=row['received_at'],
                    )
                    if snapshot_item is not None:
                        snapshot_items.append(snapshot_item)
                    raw_categories = row['missing_data_categories_payload']
                    if not isinstance(raw_categories, list):
                        raw_categories = build_missing_data_categories(
                            data_gaps_payload=row['data_gaps_payload'],
                            improvement_notes_payload=row['improvement_notes_payload'],
                            context_payload=row['context_payload'],
                        )
                    accepted.append(
                        {
                            'id': row['id'],
                            'market_date': row['market_date'].isoformat(),
                            'symbol': row['symbol'],
                            'agent': {
                                'name': row['agent_name'],
                                'version': row['agent_version'],
                                'run_id': row['run_id'],
                            },
                            'summary': row['summary_payload'],
                            'feedback': row['feedback_payload'],
                            'normalized_feedback': row['normalized_feedback_payload'],
                            'details': row['details_payload'],
                            'breadth_context': row['breadth_context_payload'],
                            'leaders_laggards': row['leaders_laggards_payload'],
                            'price_action': row['price_action_payload'],
                            'trend_context': row['trend_context_payload'],
                            'support_resistance': row['support_resistance_payload'],
                            'anomalies_and_gaps': row['anomalies_and_gaps_payload'],
                            'data_gaps': row['data_gaps_payload'],
                            'fundamental_backdrop': row['fundamental_backdrop_payload'],
                            'conservative_take': row['conservative_take_payload'],
                            'improvement_notes': row['improvement_notes_payload'],
                            'missing_data_categories': enrich_missing_data_categories(raw_categories),
                            'context': row['context_payload'],
                            'received_at': row['received_at'].isoformat() if row['received_at'] else None,
                        }
                    )
                if snapshot_items:
                    signal_decider_loop_service.repository.insert_decision_snapshots(
                        session=session,
                        items=snapshot_items,
                    )
        return accepted

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
                    'missing_data_categories': enrich_missing_data_categories(
                        row.missing_data_categories_payload
                        if isinstance(row.missing_data_categories_payload, list)
                        else build_missing_data_categories(
                            data_gaps_payload=row.data_gaps_payload,
                            improvement_notes_payload=row.improvement_notes_payload,
                            context_payload=row.context_payload,
                        )
                    ),
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
    def __init__(
        self,
        *,
        repository: AgentContextFeedbackRepository | None = None,
        decision_loop_service=None,
    ) -> None:
        self.repository = repository or AgentContextFeedbackRepository()
        self.decision_loop_service = decision_loop_service or signal_decider_loop_service

    def _normalize_feedback_item(
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
        summary_payload, feedback_payload, context_payload = self.decision_loop_service.prepare_feedback_persistence(
            agent_name=_normalize_agent_name(agent_name),
            summary_payload=summary_payload,
            feedback_payload=feedback_payload,
            context_payload=context_payload,
        )
        details_payload = feedback_payload.get('details') if isinstance(feedback_payload.get('details'), dict) else {}
        data_gaps_payload = details_payload.get('data_gaps')
        improvement_notes_payload = feedback_payload.get('improvement_notes')
        return {
            'market_date': _normalize_market_date(market_date),
            'symbol': _normalize_symbol(symbol),
            'agent_name': _normalize_agent_name(agent_name),
            'agent_version': _normalize_optional_text(agent_version, field_name='agent_version', max_length=64),
            'run_id': _normalize_optional_text(run_id, field_name='run_id', max_length=128),
            'summary_payload': summary_payload,
            'feedback_payload': feedback_payload,
            'normalized_feedback_payload': feedback_payload,
            'details_payload': details_payload,
            'breadth_context_payload': details_payload.get('breadth_context'),
            'leaders_laggards_payload': details_payload.get('leaders_laggards'),
            'price_action_payload': details_payload.get('price_action'),
            'trend_context_payload': details_payload.get('trend_context'),
            'support_resistance_payload': details_payload.get('support_resistance'),
            'anomalies_and_gaps_payload': details_payload.get('anomalies_and_gaps'),
            'data_gaps_payload': data_gaps_payload,
            'fundamental_backdrop_payload': details_payload.get('fundamental_backdrop'),
            'conservative_take_payload': details_payload.get('conservative_take'),
            'improvement_notes_payload': improvement_notes_payload,
            'missing_data_categories_payload': build_missing_data_categories(
                data_gaps_payload=data_gaps_payload,
                improvement_notes_payload=improvement_notes_payload,
                context_payload=context_payload,
            ),
            'context_payload': context_payload,
        }

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
        normalized_item = self._normalize_feedback_item(
            market_date=market_date,
            symbol=symbol,
            agent_name=agent_name,
            agent_version=agent_version,
            run_id=run_id,
            summary=summary,
            feedback=feedback,
            context=context,
        )
        item = self.repository.create_feedback(**normalized_item)
        return {
            'accepted': True,
            'item': item,
            'summary': {
                'summary_keys': sorted(normalized_item['summary_payload'].keys()),
                'feedback_keys': sorted(normalized_item['feedback_payload'].keys()),
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

    def bulk_submit_feedback(
        self,
        *,
        items: Sequence[dict[str, Any]],
    ) -> dict[str, Any]:
        normalized_items = [self._normalize_feedback_item(**item) for item in items]
        accepted = self.repository.bulk_upsert_feedback(items=normalized_items)
        return {'accepted': True, 'count': len(accepted), 'items': accepted}


agent_context_feedback_service = AgentContextFeedbackService()
