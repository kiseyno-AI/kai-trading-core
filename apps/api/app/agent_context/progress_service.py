from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Sequence

from sqlalchemy import func, select

from app.agent_context.feedback_service import _normalize_market_date, _normalize_symbols
from app.agent_context.status_service import EXPECTED_AGENT_NAMES, EXPECTED_AGENT_SET
from app.db import session_scope
from app.db_models import AgentContextFeedback, Symbol


STALL_THRESHOLD_SECONDS = 300


class AgentContextProgressService:
    def get_feedback_progress(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbols = _normalize_symbols(symbols) if symbols is not None else None

        with session_scope() as session:
            universe_symbols_stmt = (
                select(Symbol.symbol)
                .where(Symbol.is_active.is_(True))
                .order_by(Symbol.symbol.asc())
            )
            if normalized_symbols:
                universe_symbols_stmt = universe_symbols_stmt.where(Symbol.symbol.in_(normalized_symbols))
            universe_symbols = [str(symbol) for symbol in session.execute(universe_symbols_stmt).scalars().all()]

            rows_stmt = (
                select(
                    AgentContextFeedback.symbol,
                    AgentContextFeedback.agent_name,
                    AgentContextFeedback.run_id,
                    AgentContextFeedback.received_at,
                    AgentContextFeedback.id,
                )
                .where(AgentContextFeedback.market_date == normalized_market_date)
                .order_by(
                    AgentContextFeedback.symbol.asc(),
                    AgentContextFeedback.received_at.desc(),
                    AgentContextFeedback.id.desc(),
                )
            )
            if normalized_symbols:
                rows_stmt = rows_stmt.where(AgentContextFeedback.symbol.in_(normalized_symbols))

            rows = [
                {
                    'symbol': row.symbol,
                    'agent_name': row.agent_name,
                    'run_id': row.run_id,
                    'received_at': row.received_at.isoformat() if row.received_at else None,
                    'id': row.id,
                }
                for row in session.execute(rows_stmt)
            ]

        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(str(row['symbol']), []).append(row)

        latest_feedback_at = rows[0]['received_at'] if rows else None
        latest_feedback_dt = None
        if latest_feedback_at:
            latest_feedback_dt = datetime.fromisoformat(str(latest_feedback_at))
            if latest_feedback_dt.tzinfo is None:
                latest_feedback_dt = latest_feedback_dt.replace(tzinfo=timezone.utc)

        completed_symbols = 0
        in_progress_symbols = 0
        pending_symbols = 0
        failed_items = 0

        target_symbols = universe_symbols

        for symbol in target_symbols:
            symbol_rows = grouped.get(symbol, [])
            latest_run_id = symbol_rows[0]['run_id'] if symbol_rows else None
            if latest_run_id is not None:
                run_rows = [row for row in symbol_rows if row['run_id'] == latest_run_id]
            else:
                run_rows = symbol_rows[:]

            agent_names = []
            seen_agents = set()
            for row in run_rows:
                agent_name = row['agent_name']
                if agent_name in EXPECTED_AGENT_SET and agent_name not in seen_agents:
                    seen_agents.add(agent_name)
                    agent_names.append(agent_name)
            if len(agent_names) == len(EXPECTED_AGENT_NAMES):
                completed_symbols += 1
            elif agent_names:
                in_progress_symbols += 1
            else:
                pending_symbols += 1

            failed_items += sum(1 for row in run_rows if row['agent_name'] not in EXPECTED_AGENT_SET)

        requested_symbol_count = len(target_symbols)
        processed_symbols = completed_symbols + in_progress_symbols
        progress_ratio = (processed_symbols / requested_symbol_count) if requested_symbol_count else 0.0
        completed_feedback_count = sum(
            1 for row in rows if row['agent_name'] in EXPECTED_AGENT_SET
        )
        expected_feedback_count = requested_symbol_count * len(EXPECTED_AGENT_NAMES)

        seconds_since_last_feedback = None
        if latest_feedback_dt is not None:
            seconds_since_last_feedback = max(
                0,
                int((datetime.now(timezone.utc) - latest_feedback_dt.astimezone(timezone.utc)).total_seconds()),
            )

        if completed_symbols == requested_symbol_count and requested_symbol_count > 0:
            run_status = 'completed'
        elif latest_feedback_dt is None:
            run_status = 'pending'
        elif seconds_since_last_feedback is not None and seconds_since_last_feedback <= STALL_THRESHOLD_SECONDS:
            run_status = 'running'
        else:
            run_status = 'stalled'

        return {
            'request': {
                'market_date': normalized_market_date.isoformat(),
                'symbols': normalized_symbols,
                'expected_agents': list(EXPECTED_AGENT_NAMES),
            },
            'summary': {
                'run_status': run_status,
                'is_live': run_status == 'running',
                'latest_feedback_at': latest_feedback_at,
                'seconds_since_last_feedback': seconds_since_last_feedback,
                'stalled_threshold_seconds': STALL_THRESHOLD_SECONDS,
                'total_symbols': requested_symbol_count,
                'processed_symbols': processed_symbols,
                'completed_symbols': completed_symbols,
                'in_progress_symbols': in_progress_symbols,
                'pending_symbols': pending_symbols,
                'progress_ratio': round(progress_ratio, 6),
                'progress_percent': round(progress_ratio * 100, 2),
                'failed_items': failed_items,
                'completed_feedback_count': completed_feedback_count,
                'expected_feedback_count': expected_feedback_count,
            },
        }


agent_context_progress_service = AgentContextProgressService()
