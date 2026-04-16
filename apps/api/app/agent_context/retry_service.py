from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Sequence

from sqlalchemy import func, select

from app.agent_context.orchestrator import agent_feedback_orchestrator
from app.db import session_scope
from app.db_models import AgentContextFeedback, DailyPriceBar, Symbol

EXPECTED_ROLES: tuple[str, ...] = (
    'eod-summarizer',
    'technical-summarizer',
    'fundamental-summarizer',
)


@dataclass(frozen=True)
class FeedbackRetryCycleConfig:
    market_date: date
    roles: tuple[str, ...]
    max_cycles: int = 3
    outer_batch_size: int = 20
    reuse_existing: bool = True


class AgentFeedbackRetryService:
    def load_incomplete_symbols(self, *, market_date: date, roles: Sequence[str]) -> list[str]:
        expected_count = len(tuple(roles))
        with session_scope() as session:
            universe_subq = (
                select(Symbol.symbol)
                .join(DailyPriceBar, DailyPriceBar.symbol_id == Symbol.id)
                .where(DailyPriceBar.trade_date == market_date)
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
                .where(AgentContextFeedback.agent_name.in_(tuple(roles)))
                .where(AgentContextFeedback.market_date <= market_date)
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
                .where(func.coalesce(latest_counts.c.latest_role_count, 0) < expected_count)
                .order_by(universe_subq.c.symbol.asc())
            ).all()
            return [str(row.symbol) for row in rows]

    def run_retry_cycles(self, *, config: FeedbackRetryCycleConfig) -> dict[str, Any]:
        cycles: list[dict[str, Any]] = []
        remaining = self.load_incomplete_symbols(market_date=config.market_date, roles=config.roles)

        for cycle_number in range(1, config.max_cycles + 1):
            cycle_start_pending = len(remaining)
            if cycle_start_pending == 0:
                cycles.append(
                    {
                        'cycle': cycle_number,
                        'status': 'skipped',
                        'start_pending': 0,
                        'end_pending': 0,
                        'batches': [],
                    }
                )
                break

            batches: list[dict[str, Any]] = []
            for offset in range(0, len(remaining), config.outer_batch_size):
                batch_symbols = remaining[offset:offset + config.outer_batch_size]
                result = agent_feedback_orchestrator.dispatch_for_symbols(
                    market_date=config.market_date,
                    symbols=batch_symbols,
                    roles=list(config.roles),
                    reuse_existing=config.reuse_existing,
                )
                batches.append(
                    {
                        'offset': offset,
                        'batch_size': len(batch_symbols),
                        'symbols': batch_symbols,
                        'summary': result['summary'],
                        'request': result['request'],
                    }
                )

            remaining = self.load_incomplete_symbols(market_date=config.market_date, roles=config.roles)
            cycles.append(
                {
                    'cycle': cycle_number,
                    'status': 'completed' if not remaining else 'partial',
                    'start_pending': cycle_start_pending,
                    'end_pending': len(remaining),
                    'batches': batches,
                }
            )
            if not remaining:
                break

        return {
            'market_date': config.market_date.isoformat(),
            'roles': list(config.roles),
            'max_cycles': config.max_cycles,
            'outer_batch_size': config.outer_batch_size,
            'reuse_existing': config.reuse_existing,
            'cycles': cycles,
            'final_pending': len(remaining),
            'completed': len(remaining) == 0,
        }


agent_feedback_retry_service = AgentFeedbackRetryService()
