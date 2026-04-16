from __future__ import annotations

from datetime import date
from typing import Any, Sequence

from sqlalchemy import distinct, func, select

from app.agent_context.feedback_service import _normalize_market_date, _normalize_symbols
from app.agent_context.status_service import EXPECTED_AGENT_NAMES, EXPECTED_AGENT_SET
from app.db import session_scope
from app.db_models import AgentContextFeedback, DailyPriceBar, Symbol


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
                select(distinct(Symbol.symbol))
                .select_from(DailyPriceBar)
                .join(Symbol, Symbol.id == DailyPriceBar.symbol_id)
                .where(DailyPriceBar.trade_date == normalized_market_date)
                .order_by(Symbol.symbol.asc())
            )
            if normalized_symbols:
                universe_symbols_stmt = universe_symbols_stmt.where(Symbol.symbol.in_(normalized_symbols))
            universe_symbols = [str(symbol) for symbol in session.execute(universe_symbols_stmt).scalars().all()]
            total_symbols = len(universe_symbols)

            rows_stmt = (
                select(
                    AgentContextFeedback.symbol,
                    AgentContextFeedback.agent_name,
                    AgentContextFeedback.run_id,
                    AgentContextFeedback.received_at,
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
                }
                for row in session.execute(rows_stmt)
            ]

        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(str(row['symbol']), []).append(row)

        completed_symbols = 0
        in_progress_symbols = 0
        pending_symbols = 0
        failed_items = 0

        target_symbols = universe_symbols

        for symbol in target_symbols:
            symbol_rows = grouped.get(symbol, [])
            latest_run_id = symbol_rows[0]['run_id'] if symbol_rows else None
            run_rows = [row for row in symbol_rows if row['run_id'] == latest_run_id] if symbol_rows else []

            agent_names = {row['agent_name'] for row in run_rows if row['agent_name'] in EXPECTED_AGENT_SET}
            if len(agent_names) == len(EXPECTED_AGENT_NAMES):
                completed_symbols += 1
            elif agent_names:
                in_progress_symbols += 1
            else:
                pending_symbols += 1

            failed_items += sum(1 for row in run_rows if row['agent_name'] not in EXPECTED_AGENT_SET)

        requested_symbol_count = len(target_symbols) if normalized_symbols is not None else total_symbols
        processed_symbols = completed_symbols + in_progress_symbols
        progress_ratio = (processed_symbols / requested_symbol_count) if requested_symbol_count else 0.0
        completed_feedback_count = sum(
            1 for row in rows if row['agent_name'] in EXPECTED_AGENT_SET
        )
        expected_feedback_count = requested_symbol_count * len(EXPECTED_AGENT_NAMES)

        return {
            'request': {
                'market_date': normalized_market_date.isoformat(),
                'symbols': normalized_symbols,
                'expected_agents': list(EXPECTED_AGENT_NAMES),
            },
            'summary': {
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
