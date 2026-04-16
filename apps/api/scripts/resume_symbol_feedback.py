from __future__ import annotations

import json
from datetime import date

from app.agent_context.orchestrator import agent_feedback_orchestrator
from app.db import session_scope
from app.db_models import AgentContextFeedback, DailyPriceBar, Symbol
from sqlalchemy import func, select

MARKET_DATE = date.fromisoformat('2026-04-15')
ROLES = ('eod-summarizer', 'technical-summarizer', 'fundamental-summarizer')
OUTER_BATCH_SIZE = 20


def load_pending_symbols() -> list[str]:
    with session_scope() as session:
        universe_subq = (
            select(Symbol.symbol)
            .join(DailyPriceBar, DailyPriceBar.symbol_id == Symbol.id)
            .where(DailyPriceBar.trade_date == MARKET_DATE)
            .distinct()
            .subquery()
        )

        latest_subq = (
            select(
                AgentContextFeedback.symbol,
                AgentContextFeedback.agent_name,
                func.row_number().over(
                    partition_by=(AgentContextFeedback.symbol, AgentContextFeedback.agent_name),
                    order_by=(
                        AgentContextFeedback.market_date.desc(),
                        AgentContextFeedback.received_at.desc(),
                        AgentContextFeedback.id.desc(),
                    ),
                ).label('rn'),
            )
            .where(AgentContextFeedback.symbol != '__MARKET__')
            .where(AgentContextFeedback.agent_name.in_(ROLES))
            .where(AgentContextFeedback.market_date <= MARKET_DATE)
            .subquery()
        )

        latest_counts = (
            select(latest_subq.c.symbol, func.count().label('latest_role_count'))
            .where(latest_subq.c.rn == 1)
            .group_by(latest_subq.c.symbol)
            .subquery()
        )

        rows = session.execute(
            select(universe_subq.c.symbol)
            .outerjoin(latest_counts, latest_counts.c.symbol == universe_subq.c.symbol)
            .where(func.coalesce(latest_counts.c.latest_role_count, 0) < len(ROLES))
            .order_by(universe_subq.c.symbol.asc())
        ).all()
        return [str(row.symbol) for row in rows]


def main() -> None:
    pending_symbols = load_pending_symbols()
    total = len(pending_symbols)
    print(json.dumps({'market_date': MARKET_DATE.isoformat(), 'pending_symbols': total, 'outer_batch_size': OUTER_BATCH_SIZE}, ensure_ascii=False))
    for index in range(0, total, OUTER_BATCH_SIZE):
        batch = pending_symbols[index:index + OUTER_BATCH_SIZE]
        result = agent_feedback_orchestrator.dispatch_for_symbols(
            market_date=MARKET_DATE,
            symbols=batch,
            roles=list(ROLES),
            reuse_existing=True,
        )
        print(json.dumps({
            'offset': index,
            'batch_size': len(batch),
            'summary': result['summary'],
            'request': result['request'],
        }, ensure_ascii=False))


if __name__ == '__main__':
    main()
