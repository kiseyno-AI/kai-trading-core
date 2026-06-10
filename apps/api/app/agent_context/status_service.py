from __future__ import annotations

from datetime import date
from typing import Any, Sequence

from sqlalchemy import select

from app.agent_context.feedback_failure_taxonomy import build_failure_groups, classify_feedback_failure
from app.agent_context.roles import DEFAULT_AGENT_NAMES
from app.agent_context.prerequisite_service import agent_feedback_prerequisite_service
from app.agent_context.feedback_service import _normalize_market_date, _normalize_symbols
from app.db import session_scope
from app.db_models import AgentContextFeedback, AgentContextFeedbackFailure


EXPECTED_AGENT_NAMES = DEFAULT_AGENT_NAMES
EXPECTED_AGENT_SET = set(EXPECTED_AGENT_NAMES)


def _group_feedback_rows(rows: Sequence[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row['symbol']), []).append(row)
    return grouped


def _group_latest_failures(rows: Sequence[dict[str, Any]]) -> dict[str, dict[str, dict[str, Any]]]:
    grouped: dict[str, dict[str, dict[str, Any]]] = {}
    for row in rows:
        symbol = str(row['symbol'])
        agent_name = str(row['agent_name'])
        grouped.setdefault(symbol, {})
        if agent_name not in grouped[symbol]:
            grouped[symbol][agent_name] = row
    return grouped


class AgentContextStatusService:
    def get_feedback_status(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str],
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbols = _normalize_symbols(symbols)
        if not normalized_symbols:
            raise ValueError('symbols is required')
        prerequisites = agent_feedback_prerequisite_service.inspect_feedback_prerequisites(
            market_date=normalized_market_date,
        )

        with session_scope() as session:
            orm_rows = session.execute(
                select(AgentContextFeedback)
                .where(AgentContextFeedback.market_date == normalized_market_date)
                .where(AgentContextFeedback.symbol.in_(normalized_symbols))
                .order_by(AgentContextFeedback.symbol.asc(), AgentContextFeedback.received_at.desc(), AgentContextFeedback.id.desc())
            ).scalars().all()
            rows = [
                {
                    'symbol': row.symbol,
                    'agent_name': row.agent_name,
                    'run_id': row.run_id,
                    'received_at': row.received_at.isoformat() if row.received_at else None,
                }
                for row in orm_rows
            ]
            orm_failure_rows = session.execute(
                select(AgentContextFeedbackFailure)
                .where(AgentContextFeedbackFailure.market_date == normalized_market_date)
                .where(AgentContextFeedbackFailure.symbol.in_(normalized_symbols))
                .where(AgentContextFeedbackFailure.agent_name.in_(EXPECTED_AGENT_NAMES))
                .order_by(
                    AgentContextFeedbackFailure.symbol.asc(),
                    AgentContextFeedbackFailure.agent_name.asc(),
                    AgentContextFeedbackFailure.received_at.desc(),
                    AgentContextFeedbackFailure.id.desc(),
                )
            ).scalars().all()
            failure_rows = []
            for row in orm_failure_rows:
                summary = row.summary_payload if isinstance(row.summary_payload, dict) else {}
                context = row.context_payload if isinstance(row.context_payload, dict) else {}
                classification = classify_feedback_failure(
                    failure_kind=row.failure_kind,
                    summary=summary,
                    context=context,
                    error=str(summary.get('error') or '') or None,
                    exception_type=str(summary.get('exception_type') or '') or None,
                )
                failure_rows.append(
                    {
                        'symbol': row.symbol,
                        'agent_name': row.agent_name,
                        'run_id': row.run_id,
                        'failure_kind': row.failure_kind,
                        'failure_status': classification.failure_status,
                        'failure_category': classification.failure_category,
                        'retryable': row.retryable,
                        'received_at': row.received_at.isoformat() if row.received_at else None,
                        'summary': summary,
                        'context': context,
                    }
                )

        grouped = _group_feedback_rows(rows)
        failures_by_symbol = _group_latest_failures(failure_rows)
        items: list[dict[str, Any]] = []
        completed_symbols = 0
        in_progress_symbols = 0
        partial_symbols = 0
        failed_symbols = 0
        pending_symbols = 0
        unresolved_failures: list[dict[str, Any]] = []

        for symbol in normalized_symbols:
            symbol_rows = grouped.get(symbol, [])
            latest_run_id = symbol_rows[0]['run_id'] if symbol_rows else None
            run_rows = [row for row in symbol_rows if row['run_id'] == latest_run_id] if latest_run_id else []

            agent_map: dict[str, dict[str, Any]] = {}
            for row in run_rows:
                if row['agent_name'] in EXPECTED_AGENT_SET and row['agent_name'] not in agent_map:
                    agent_map[row['agent_name']] = row

            completed_agents = sorted(agent_map.keys())
            symbol_failure_map = {
                agent_name: failure
                for agent_name, failure in failures_by_symbol.get(symbol, {}).items()
                if agent_name not in agent_map
            }
            failed_agents = sorted(symbol_failure_map.keys())
            symbol_failures = [symbol_failure_map[agent_name] for agent_name in failed_agents]
            unresolved_failures.extend(symbol_failures)
            pending_agents = [
                name
                for name in EXPECTED_AGENT_NAMES
                if name not in agent_map and name not in symbol_failure_map
            ]
            if len(completed_agents) == len(EXPECTED_AGENT_NAMES):
                status = 'completed'
                completed_symbols += 1
            elif completed_agents or failed_agents:
                if completed_agents:
                    status = 'partial'
                    partial_symbols += 1
                else:
                    status = 'failed'
                    failed_symbols += 1
            else:
                status = prerequisites['status'] if not prerequisites['ready'] else 'pending'
                pending_symbols += 1

            items.append(
                {
                    'symbol': symbol,
                    'status': status,
                    'latest_run_id': latest_run_id,
                    'completed_agents': completed_agents,
                    'failed_agents': failed_agents,
                    'pending_agents': pending_agents,
                    'completed_count': len(completed_agents),
                    'failed_count': len(failed_agents),
                    'expected_count': len(EXPECTED_AGENT_NAMES),
                    'failures': [
                        {
                            'agent_name': failure['agent_name'],
                            'run_id': failure['run_id'],
                            'failure_kind': failure['failure_kind'],
                            'failure_status': failure['failure_status'],
                            'failure_category': failure['failure_category'],
                            'retryable': failure['retryable'],
                            'received_at': failure['received_at'],
                        }
                        for failure in symbol_failures
                    ],
                    'failure_groups': build_failure_groups(symbol_failures),
                    'prerequisites_ready': prerequisites['ready'],
                    'missing_prerequisites': prerequisites['missing_prerequisites'],
                }
            )

        overall_status = 'completed'
        if in_progress_symbols:
            overall_status = 'in_progress'
        elif partial_symbols:
            overall_status = 'partial'
        elif failed_symbols:
            overall_status = 'failed'
        elif not prerequisites['ready'] and completed_symbols == 0:
            overall_status = prerequisites['status']
        elif pending_symbols and completed_symbols == 0:
            overall_status = 'pending'
        elif pending_symbols:
            overall_status = 'partial'

        return {
            'request': {
                'market_date': normalized_market_date.isoformat(),
                'symbols': normalized_symbols,
                'expected_agents': list(EXPECTED_AGENT_NAMES),
            },
            'summary': {
                'status': overall_status,
                'requested_symbol_count': len(normalized_symbols),
                'completed_symbol_count': completed_symbols,
                'in_progress_symbol_count': in_progress_symbols,
                'partial_symbol_count': partial_symbols,
                'failed_symbol_count': failed_symbols,
                'pending_symbol_count': pending_symbols,
                'completed_feedback_count': len(rows),
                'failed_feedback_count': len(unresolved_failures),
                'expected_feedback_count': len(normalized_symbols) * len(EXPECTED_AGENT_NAMES),
                'failure_groups': build_failure_groups(unresolved_failures),
            },
            'items': items,
            'prerequisites': prerequisites,
        }


agent_context_status_service = AgentContextStatusService()
