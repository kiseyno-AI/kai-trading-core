from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
from math import ceil
from typing import Any, Sequence

from sqlalchemy import select

from app.agent_context.feedback_service import _normalize_market_date, _normalize_symbols
from app.agent_context.status_service import EXPECTED_AGENT_NAMES
from app.agent_context.runtime_state import runtime_feedback_state_store
from app.db import session_scope
from app.db_models import AgentContextFeedback, AgentContextFeedbackFailure, DailyPriceBar, Symbol


class AgentContextProgressService:
    def get_feedback_progress(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
        expected_agents: Sequence[str] | None = None,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbols = _normalize_symbols(symbols) if symbols is not None else None
        expected_agent_names = self._normalize_expected_agents(expected_agents)
        persisted_progress = self._build_persisted_progress(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
            expected_agents=expected_agent_names,
        )
        state = runtime_feedback_state_store.get_run(normalized_market_date, run_id=run_id, logical=(run_id is None))
        return {
            'request': {
                'market_date': normalized_market_date.isoformat(),
                'symbols': normalized_symbols,
                'expected_agents': list(expected_agent_names),
                'run_id': run_id,
            },
            'result': persisted_progress['result'],
            'summary': state.to_progress_summary() if state is not None else persisted_progress['summary'],
        }

    def _build_persisted_progress(
        self,
        *,
        market_date: date,
        symbols: Sequence[str] | None,
        expected_agents: Sequence[str],
    ) -> dict[str, Any]:
        scoped_symbols = list(symbols) if symbols is not None else self._load_market_date_symbols(market_date)
        scoped_symbol_set = set(scoped_symbols)
        completed_by_role: dict[str, set[str]] = {agent_name: set() for agent_name in expected_agents}
        failed_by_role: dict[str, set[str]] = {agent_name: set() for agent_name in expected_agents}
        feedback_received_at: list[datetime] = []

        if scoped_symbols:
            with session_scope() as session:
                feedback_rows = session.execute(
                    select(
                        AgentContextFeedback.symbol,
                        AgentContextFeedback.agent_name,
                        AgentContextFeedback.received_at,
                    )
                    .where(AgentContextFeedback.market_date == market_date)
                    .where(AgentContextFeedback.symbol.in_(scoped_symbols))
                    .where(AgentContextFeedback.agent_name.in_(expected_agents))
                ).all()
                failure_rows = session.execute(
                    select(
                        AgentContextFeedbackFailure.symbol,
                        AgentContextFeedbackFailure.agent_name,
                        AgentContextFeedbackFailure.received_at,
                    )
                    .where(AgentContextFeedbackFailure.market_date == market_date)
                    .where(AgentContextFeedbackFailure.symbol.in_(scoped_symbols))
                    .where(AgentContextFeedbackFailure.agent_name.in_(expected_agents))
                ).all()

            for row in feedback_rows:
                symbol = str(row.symbol)
                agent_name = str(row.agent_name)
                if symbol not in scoped_symbol_set or agent_name not in completed_by_role:
                    continue
                completed_by_role[agent_name].add(symbol)
                if row.received_at is not None:
                    feedback_received_at.append(row.received_at)

            for row in failure_rows:
                symbol = str(row.symbol)
                agent_name = str(row.agent_name)
                if symbol not in scoped_symbol_set or agent_name not in failed_by_role:
                    continue
                if symbol in completed_by_role[agent_name]:
                    continue
                failed_by_role[agent_name].add(symbol)

        first_feedback_at = min(feedback_received_at) if feedback_received_at else None
        latest_feedback_at = max(feedback_received_at) if feedback_received_at else None
        return {
            'result': self._build_result(
                market_date=market_date,
                requested_count=len(scoped_symbols),
                completed_by_role=completed_by_role,
                failed_by_role=failed_by_role,
                expected_agents=expected_agents,
            ),
            'summary': self._build_persisted_summary(
                requested_symbols=scoped_symbols,
                expected_agents=expected_agents,
                completed_by_role=completed_by_role,
                failed_by_role=failed_by_role,
                first_feedback_at=first_feedback_at,
                latest_feedback_at=latest_feedback_at,
            ),
        }

    @staticmethod
    def _load_market_date_symbols(market_date: date) -> list[str]:
        with session_scope() as session:
            rows = session.execute(
                select(Symbol.symbol)
                .join(DailyPriceBar, DailyPriceBar.symbol_id == Symbol.id)
                .where(DailyPriceBar.trade_date == market_date)
                .order_by(Symbol.symbol.asc())
            ).scalars().all()
        return [str(symbol) for symbol in rows]

    @staticmethod
    def _build_result(
        *,
        market_date: date,
        requested_count: int,
        completed_by_role: dict[str, set[str]],
        failed_by_role: dict[str, set[str]],
        expected_agents: Sequence[str],
    ) -> dict[str, Any]:
        return {
            'market_date': market_date.isoformat(),
            'roles': {
                agent_name: {
                    'requested_count': requested_count,
                    'completed_count': len(completed_by_role.get(agent_name, set())),
                    'error_count': len(failed_by_role.get(agent_name, set())),
                }
                for agent_name in expected_agents
            },
        }

    @staticmethod
    def _build_persisted_summary(
        *,
        requested_symbols: Sequence[str],
        expected_agents: Sequence[str],
        completed_by_role: dict[str, set[str]],
        failed_by_role: dict[str, set[str]],
        first_feedback_at: datetime | None,
        latest_feedback_at: datetime | None,
    ) -> dict[str, Any]:
        requested_count = len(requested_symbols)
        expected_feedback_count = requested_count * len(expected_agents)
        completed_feedback_count = sum(len(completed_by_role.get(agent_name, set())) for agent_name in expected_agents)
        failed_items = sum(len(failed_by_role.get(agent_name, set())) for agent_name in expected_agents)

        completed_roles_by_symbol: dict[str, set[str]] = defaultdict(set)
        for agent_name, symbols in completed_by_role.items():
            for symbol in symbols:
                completed_roles_by_symbol[symbol].add(agent_name)

        completed_symbols = 0
        in_progress_symbols = 0
        for symbol in requested_symbols:
            completed_roles = completed_roles_by_symbol.get(symbol, set())
            if len(completed_roles) == len(expected_agents) and expected_agents:
                completed_symbols += 1
            elif completed_roles:
                in_progress_symbols += 1
        pending_symbols = max(0, requested_count - completed_symbols - in_progress_symbols)
        processed_symbols = completed_symbols + in_progress_symbols
        progress_ratio = (processed_symbols / requested_count) if requested_count else 0.0

        role_progress: dict[str, dict[str, Any]] = {}
        for agent_name in expected_agents:
            completed_for_role = len(completed_by_role.get(agent_name, set()))
            failed_for_role = len(failed_by_role.get(agent_name, set()))
            processed_for_role = min(requested_count, completed_for_role + failed_for_role)
            pending_for_role = max(0, requested_count - processed_for_role)
            progress_ratio_for_role = (processed_for_role / requested_count) if requested_count else 0.0
            role_status = 'pending'
            if requested_count and pending_for_role == 0:
                if failed_for_role == 0:
                    role_status = 'completed'
                elif completed_for_role > 0:
                    role_status = 'partial'
                else:
                    role_status = 'failed'
            elif processed_for_role > 0:
                role_status = 'partial'
            role_progress[agent_name] = {
                'role': agent_name,
                'status': role_status,
                'total_symbols': requested_count,
                'processed_symbols': processed_for_role,
                'completed_symbols': completed_for_role,
                'in_progress_symbols': 0,
                'pending_symbols': pending_for_role,
                'failed_items': failed_for_role,
                'progress_ratio': round(progress_ratio_for_role, 6),
                'progress_percent': round(progress_ratio_for_role * 100, 2),
            }

        elapsed_seconds = None
        seconds_since_last_feedback = None
        completed_feedback_per_minute = None
        completed_symbols_per_minute = None
        estimated_remaining_seconds = None
        estimated_completion_at = None
        eta_confidence = None
        now = datetime.now(timezone.utc)
        if first_feedback_at is not None and latest_feedback_at is not None:
            elapsed_seconds = max(0, int((latest_feedback_at - first_feedback_at).total_seconds()))
            seconds_since_last_feedback = max(0, int((now - latest_feedback_at.astimezone(timezone.utc)).total_seconds()))
            remaining_feedback_count = max(0, expected_feedback_count - completed_feedback_count - failed_items)
            if elapsed_seconds > 0 and completed_feedback_count > 0:
                completed_rate = completed_feedback_count / elapsed_seconds
                completed_feedback_per_minute = round(completed_rate * 60, 2)
                completed_symbols_per_minute = round((completed_symbols / elapsed_seconds) * 60, 2)
                if remaining_feedback_count > 0 and completed_rate > 0:
                    estimated_remaining_seconds = ceil(remaining_feedback_count / completed_rate)
                    estimated_completion_at = (now + __import__('datetime').timedelta(seconds=estimated_remaining_seconds)).isoformat()
                    eta_confidence = 'low'
            if remaining_feedback_count == 0:
                estimated_remaining_seconds = 0
                estimated_completion_at = latest_feedback_at.isoformat()
                eta_confidence = 'high'

        run_status = 'pending'
        if requested_count and completed_symbols == requested_count and failed_items == 0:
            run_status = 'completed'
        elif completed_feedback_count or failed_items:
            run_status = 'partial'

        return {
            'run_id': None,
            'run_status': run_status,
            'run_table_status': None,
            'is_live': False,
            'run_started_at': None,
            'run_completed_at': None,
            'current_role': None,
            'parallel_per_role': None,
            'first_feedback_at': first_feedback_at.isoformat() if first_feedback_at else None,
            'latest_feedback_at': latest_feedback_at.isoformat() if latest_feedback_at else None,
            'elapsed_seconds': elapsed_seconds,
            'seconds_since_last_feedback': seconds_since_last_feedback,
            'stalled_threshold_seconds': 300,
            'total_symbols': requested_count,
            'processed_symbols': processed_symbols,
            'completed_symbols': completed_symbols,
            'in_progress_symbols': in_progress_symbols,
            'pending_symbols': pending_symbols,
            'progress_ratio': round(progress_ratio, 6),
            'progress_percent': round(progress_ratio * 100, 2),
            'failed_items': failed_items,
            'completed_feedback_count': completed_feedback_count,
            'expected_feedback_count': expected_feedback_count,
            'completed_feedback_per_minute': completed_feedback_per_minute,
            'completed_symbols_per_minute': completed_symbols_per_minute,
            'estimated_remaining_seconds': estimated_remaining_seconds,
            'estimated_completion_at': estimated_completion_at,
            'eta_confidence': eta_confidence,
            'role_progress': role_progress,
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
