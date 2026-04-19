from __future__ import annotations

from datetime import date
from typing import Any, Sequence

from app.agent_context.feedback_service import _normalize_market_date, _normalize_symbols
from app.agent_context.status_service import EXPECTED_AGENT_NAMES
from app.agent_context.runtime_state import runtime_feedback_state_store


class AgentContextProgressService:
    def get_feedback_progress(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
        expected_agents: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbols = _normalize_symbols(symbols) if symbols is not None else None
        expected_agent_names = self._normalize_expected_agents(expected_agents)
        state = runtime_feedback_state_store.get_run(normalized_market_date)
        if state is None:
            return {
                'request': {
                    'market_date': normalized_market_date.isoformat(),
                    'symbols': normalized_symbols,
                    'expected_agents': list(expected_agent_names),
                },
                'summary': {
                    'run_id': None,
                    'run_status': 'pending',
                    'run_table_status': None,
                    'is_live': False,
                    'run_started_at': None,
                    'run_completed_at': None,
                    'current_role': None,
                    'parallel_per_role': None,
                    'first_feedback_at': None,
                    'latest_feedback_at': None,
                    'elapsed_seconds': None,
                    'seconds_since_last_feedback': None,
                    'stalled_threshold_seconds': 300,
                    'total_symbols': 0,
                    'processed_symbols': 0,
                    'completed_symbols': 0,
                    'in_progress_symbols': 0,
                    'pending_symbols': 0,
                    'progress_ratio': 0.0,
                    'progress_percent': 0.0,
                    'failed_items': 0,
                    'completed_feedback_count': 0,
                    'expected_feedback_count': 0,
                    'completed_feedback_per_minute': None,
                    'completed_symbols_per_minute': None,
                    'estimated_remaining_seconds': None,
                    'estimated_completion_at': None,
                    'eta_confidence': None,
                },
            }
        return {
            'request': {
                'market_date': normalized_market_date.isoformat(),
                'symbols': normalized_symbols,
                'expected_agents': list(expected_agent_names),
            },
            'summary': state.to_progress_summary(),
        }

    @staticmethod
    def _normalize_expected_agents(expected_agents: Sequence[str] | None) -> tuple[str, ...]:
        source_agents = EXPECTED_AGENT_NAMES if expected_agents is None else expected_agents
        normalized_agents: list[str] = []
        seen: set[str] = set()
        for agent_name in source_agents:
            if agent_name in seen:
                continue
            seen.add(agent_name)
            normalized_agents.append(agent_name)
        if not normalized_agents:
            raise ValueError('expected_agents is required')
        return tuple(normalized_agents)


agent_context_progress_service = AgentContextProgressService()
