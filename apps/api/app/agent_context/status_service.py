from __future__ import annotations

from datetime import date
from typing import Any, Sequence

from sqlalchemy import select

from app.agent_context.feedback_service import _normalize_market_date, _normalize_symbols
from app.db import session_scope
from app.db_models import AgentContextFeedback


EXPECTED_AGENT_NAMES = ('eod-summarizer', 'technical-summarizer', 'fundamental-summarizer')
EXPECTED_AGENT_SET = set(EXPECTED_AGENT_NAMES)


def _group_feedback_rows(rows: Sequence[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row['symbol']), []).append(row)
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

        grouped = _group_feedback_rows(rows)
        items: list[dict[str, Any]] = []
        completed_symbols = 0
        in_progress_symbols = 0
        pending_symbols = 0

        for symbol in normalized_symbols:
            symbol_rows = grouped.get(symbol, [])
            latest_run_id = symbol_rows[0]['run_id'] if symbol_rows else None
            run_rows = [row for row in symbol_rows if row['run_id'] == latest_run_id] if latest_run_id else []

            agent_map: dict[str, dict[str, Any]] = {}
            for row in run_rows:
                if row['agent_name'] in EXPECTED_AGENT_SET and row['agent_name'] not in agent_map:
                    agent_map[row['agent_name']] = row

            completed_agents = sorted(agent_map.keys())
            pending_agents = [name for name in EXPECTED_AGENT_NAMES if name not in agent_map]
            if len(completed_agents) == len(EXPECTED_AGENT_NAMES):
                status = 'completed'
                completed_symbols += 1
            elif completed_agents:
                status = 'in_progress'
                in_progress_symbols += 1
            else:
                status = 'pending'
                pending_symbols += 1

            items.append(
                {
                    'symbol': symbol,
                    'status': status,
                    'latest_run_id': latest_run_id,
                    'completed_agents': completed_agents,
                    'pending_agents': pending_agents,
                    'completed_count': len(completed_agents),
                    'expected_count': len(EXPECTED_AGENT_NAMES),
                }
            )

        overall_status = 'completed'
        if in_progress_symbols:
            overall_status = 'in_progress'
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
                'pending_symbol_count': pending_symbols,
                'completed_feedback_count': len(rows),
                'expected_feedback_count': len(normalized_symbols) * len(EXPECTED_AGENT_NAMES),
            },
            'items': items,
        }


agent_context_status_service = AgentContextStatusService()
