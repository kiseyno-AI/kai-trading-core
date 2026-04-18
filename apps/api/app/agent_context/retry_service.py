from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Sequence

from sqlalchemy import select

from app.agent_context.orchestrator import agent_feedback_orchestrator
from app.agent_context.progress_service import agent_context_progress_service
from app.agent_context.reuse_service import agent_context_reuse_service
from app.db import session_scope
from app.db_models import DailyPriceBar, Symbol

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
        expected_roles = tuple(roles)
        with session_scope() as session:
            rows = session.execute(
                select(Symbol.symbol)
                .join(DailyPriceBar, DailyPriceBar.symbol_id == Symbol.id)
                .where(DailyPriceBar.trade_date == market_date)
                .distinct()
                .order_by(Symbol.symbol.asc())
            ).all()
        symbols = [str(row.symbol) for row in rows]
        if not symbols:
            return []

        decisions = agent_context_reuse_service.build_latest_decisions(
            market_date=market_date,
            symbols=symbols,
            roles=expected_roles,
        )
        incomplete: list[str] = []
        for symbol in symbols:
            symbol_decisions = decisions.get(symbol, {})
            if any(
                symbol_decisions.get(role) is None or symbol_decisions[role].should_dispatch
                for role in expected_roles
            ):
                incomplete.append(symbol)
        return incomplete

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
                try:
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
                except Exception as exc:  # noqa: BLE001
                    batches.append(
                        {
                            'offset': offset,
                            'batch_size': len(batch_symbols),
                            'symbols': batch_symbols,
                            'summary': {
                                'status': 'failed',
                                'completed': 0,
                                'failed': len(batch_symbols) * len(config.roles),
                                'error_type': type(exc).__name__,
                                'error': str(exc),
                            },
                            'request': {
                                'market_date': config.market_date.isoformat(),
                                'symbols': list(batch_symbols),
                                'roles': list(config.roles),
                                'reuse_existing': config.reuse_existing,
                            },
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

        progress = self._get_retry_progress(
            market_date=config.market_date,
            roles=config.roles,
        )

        return {
            'market_date': config.market_date.isoformat(),
            'roles': list(config.roles),
            'max_cycles': config.max_cycles,
            'outer_batch_size': config.outer_batch_size,
            'reuse_existing': config.reuse_existing,
            'cycles': cycles,
            'final_pending': len(remaining),
            'completed': len(remaining) == 0,
            'progress': progress,
        }

    def _get_retry_progress(self, *, market_date: date, roles: Sequence[str]) -> dict[str, Any]:
        return agent_context_progress_service.get_feedback_progress(
            market_date=market_date,
            symbols=None,
            expected_agents=roles,
        )


agent_feedback_retry_service = AgentFeedbackRetryService()
