from __future__ import annotations

from collections.abc import Mapping
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.agent_context.feedback_service import _normalize_agent_name, _normalize_json_object, _normalize_market_date, _normalize_optional_text, _normalize_symbol
from app.agent_context.feedback_failure_taxonomy import classify_feedback_failure
from app.core.settings import settings
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
        with session_scope(
            statement_timeout_ms=settings.agent_feedback_db_statement_timeout_ms,
            lock_timeout_ms=settings.agent_feedback_db_lock_timeout_ms,
        ) as session:
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

    @staticmethod
    def _normalize_optional_date(value: str | date | None, *, field_name: str) -> date | None:
        if value is None:
            return None
        try:
            return _normalize_market_date(value)
        except ValueError as exc:
            raise ValueError(f'{field_name} must be an ISO date') from exc

    @staticmethod
    def _serialize_received_at(value: Any) -> str | None:
        if value is None:
            return None
        return value.isoformat() if hasattr(value, 'isoformat') else str(value)

    def get_grouped_summary(
        self,
        *,
        market_date: str | date | None = None,
        market_date_from: str | date | None = None,
        market_date_to: str | date | None = None,
        run_id: str | None = None,
        agent_name: str | None = None,
        failure_kind: str | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        exact_market_date = self._normalize_optional_date(market_date, field_name='market_date')
        normalized_from = self._normalize_optional_date(market_date_from, field_name='market_date_from')
        normalized_to = self._normalize_optional_date(market_date_to, field_name='market_date_to')
        if exact_market_date is not None:
            normalized_from = exact_market_date
            normalized_to = exact_market_date
        if normalized_from is None and normalized_to is None:
            raise ValueError('market_date or market_date_from/market_date_to is required')
        if normalized_from is None:
            normalized_from = normalized_to
        if normalized_to is None:
            normalized_to = normalized_from
        if normalized_from is None or normalized_to is None:
            raise ValueError('market_date range could not be resolved')
        if normalized_from > normalized_to:
            raise ValueError('market_date_from must be on or before market_date_to')

        normalized_run_id = _normalize_optional_text(run_id, field_name='run_id', max_length=128)
        normalized_agent_name = (
            _normalize_agent_name(agent_name)
            if agent_name is not None and str(agent_name).strip()
            else None
        )
        normalized_failure_kind = (
            self._normalize_failure_kind(str(failure_kind))
            if failure_kind is not None and str(failure_kind).strip()
            else None
        )
        bounded_limit = max(1, min(int(limit or 200), 500))

        with session_scope() as session:
            stmt = select(AgentContextFeedbackFailure).where(
                AgentContextFeedbackFailure.market_date >= normalized_from,
                AgentContextFeedbackFailure.market_date <= normalized_to,
            )
            if normalized_run_id is not None:
                stmt = stmt.where(AgentContextFeedbackFailure.run_id == normalized_run_id)
            if normalized_agent_name is not None:
                stmt = stmt.where(AgentContextFeedbackFailure.agent_name == normalized_agent_name)
            if normalized_failure_kind is not None:
                stmt = stmt.where(AgentContextFeedbackFailure.failure_kind == normalized_failure_kind)
            rows = (
                session.execute(
                    stmt.order_by(
                        AgentContextFeedbackFailure.market_date.desc(),
                        AgentContextFeedbackFailure.run_id.asc(),
                        AgentContextFeedbackFailure.agent_name.asc(),
                        AgentContextFeedbackFailure.failure_kind.asc(),
                        AgentContextFeedbackFailure.received_at.desc(),
                        AgentContextFeedbackFailure.id.desc(),
                    )
                )
                .scalars()
                .all()
            )

            groups_by_key: dict[tuple[str, str | None, str, str], dict[str, Any]] = {}
            for row in rows:
                market_date_key = row.market_date.isoformat()
                group_key = (market_date_key, row.run_id, row.agent_name, row.failure_kind)
                group = groups_by_key.setdefault(
                    group_key,
                    {
                        'market_date': market_date_key,
                        'run_id': row.run_id,
                        'agent_name': row.agent_name,
                        'failure_kind': row.failure_kind,
                        'failure_count': 0,
                        'retryable_count': 0,
                        'symbols_sample': [],
                        'first_received_at': None,
                        'latest_received_at': None,
                    },
                )
                group['failure_count'] += 1
                if bool(row.retryable):
                    group['retryable_count'] += 1
                if row.symbol not in group['symbols_sample'] and len(group['symbols_sample']) < 10:
                    group['symbols_sample'].append(row.symbol)
                received_at = row.received_at
                if group['first_received_at'] is None or (
                    received_at is not None and received_at < group['first_received_at']
                ):
                    group['first_received_at'] = received_at
                if group['latest_received_at'] is None or (
                    received_at is not None and received_at > group['latest_received_at']
                ):
                    group['latest_received_at'] = received_at

        groups = list(groups_by_key.values())
        groups.sort(
            key=lambda item: (
                item['market_date'],
                item['run_id'] or '',
                item['agent_name'],
                item['failure_kind'],
            ),
            reverse=True,
        )
        groups = groups[:bounded_limit]
        run_ids: set[str] = set()
        agents: set[str] = set()
        failure_kinds: set[str] = set()
        dates: set[str] = set()
        by_failure_kind: dict[str, int] = defaultdict(int)
        for group in groups:
            classification = classify_feedback_failure(
                failure_kind=str(group['failure_kind'] or ''),
                summary={'failure_kind': group['failure_kind']},
            )
            group['failure_status'] = classification.failure_status
            group['failure_category'] = classification.failure_category
            group['reason'] = classification.reason
            group['first_received_at'] = self._serialize_received_at(group['first_received_at'])
            group['latest_received_at'] = self._serialize_received_at(group['latest_received_at'])
            if group.get('run_id'):
                run_ids.add(str(group['run_id']))
            agents.add(str(group['agent_name']))
            failure_kinds.add(str(group['failure_kind']))
            dates.add(str(group['market_date']))
            by_failure_kind[str(group['failure_kind'])] += int(group['failure_count'])

        total_failure_count = sum(int(group['failure_count']) for group in groups)
        return {
            'request': {
                'market_date': exact_market_date.isoformat() if exact_market_date is not None else None,
                'market_date_from': normalized_from.isoformat(),
                'market_date_to': normalized_to.isoformat(),
                'run_id': normalized_run_id,
                'agent_name': normalized_agent_name,
                'failure_kind': normalized_failure_kind,
                'limit': bounded_limit,
            },
            'summary': {
                'group_count': len(groups),
                'failure_count': total_failure_count,
                'retryable_count': sum(int(group['retryable_count']) for group in groups),
                'date_count': len(dates),
                'run_count': len(run_ids),
                'agent_count': len(agents),
                'failure_kind_count': len(failure_kinds),
                'by_failure_kind': dict(sorted(by_failure_kind.items())),
            },
            'groups': groups,
        }


agent_context_feedback_failure_service = AgentContextFeedbackFailureService()
