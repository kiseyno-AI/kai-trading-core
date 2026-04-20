from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Sequence
from uuid import uuid4

from sqlalchemy import select

from app.agent_context.orchestrator import agent_feedback_orchestrator
from app.agent_context.progress_service import agent_context_progress_service
from app.agent_context.reuse_service import agent_context_reuse_service
from app.agent_context.runtime_state import RuntimeFeedbackRunState, runtime_feedback_state_store
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
        logical_run_id = f"feedback-followup-{config.market_date.isoformat()}-{uuid4().hex[:10]}"
        logical_state = RuntimeFeedbackRunState(
            run_id=logical_run_id,
            market_date=config.market_date,
            requested_symbols=list(remaining),
            requested_roles=list(config.roles),
            role_sequence=list(config.roles),
            parallel_per_role=0,
            batch_size=config.outer_batch_size,
            started_at=datetime.now(timezone.utc),
            current_role=config.roles[0] if config.roles else None,
            logical_total_symbols=len(remaining),
            logical_expected_feedback_count=len(remaining) * len(config.roles),
            logical_completed_symbols=0,
            logical_in_progress_symbols=0,
            logical_pending_symbols=len(remaining),
            logical_completed_feedback_count=0,
            logical_failed_items=0,
        )
        runtime_feedback_state_store.start_run(logical_state, replace=True)

        try:
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

                role_batches: list[dict[str, Any]] = []
                for current_role in config.roles:
                    role_remaining = self.load_incomplete_symbols(market_date=config.market_date, roles=[current_role])
                    total_symbols = len(role_remaining)
                    runtime_feedback_state_store.update_run(
                        config.market_date,
                        lambda current, current_role=current_role: self._set_logical_role_context(current, current_role),
                    )
                    batches: list[dict[str, Any]] = []
                    if role_remaining:
                        runtime_feedback_state_store.update_run(
                            config.market_date,
                            lambda current, current_role=current_role, total_symbols=total_symbols: self._set_single_symbol_slot_context(current, current_role, total_symbols),
                        )
                        try:
                            result = agent_feedback_orchestrator.dispatch_for_symbols(
                                market_date=config.market_date,
                                symbols=role_remaining,
                                roles=[current_role],
                                reuse_existing=config.reuse_existing,
                            )
                            batches.append(
                                {
                                    'offset': 0,
                                    'batch_size': 1,
                                    'symbols': role_remaining,
                                    'summary': result['summary'],
                                    'request': result['request'],
                                    'mode': 'single_symbol_slot_pool',
                                }
                            )
                            runtime_feedback_state_store.update_run(
                                config.market_date,
                                lambda current, result=result, role_remaining=role_remaining: self._merge_batch_progress(current, result, role_remaining),
                            )
                        except Exception as exc:  # noqa: BLE001
                            batches.append(
                                {
                                    'offset': 0,
                                    'batch_size': 1,
                                    'symbols': role_remaining,
                                    'summary': {
                                        'status': 'failed',
                                        'completed': 0,
                                        'failed': len(role_remaining),
                                        'error_type': type(exc).__name__,
                                        'error': str(exc),
                                    },
                                    'request': {
                                        'market_date': config.market_date.isoformat(),
                                        'symbols': list(role_remaining),
                                        'roles': [current_role],
                                        'reuse_existing': config.reuse_existing,
                                    },
                                    'mode': 'single_symbol_slot_pool',
                                }
                            )
                            runtime_feedback_state_store.update_run(
                                config.market_date,
                                lambda current, role_remaining=role_remaining: self._merge_batch_exception(current, role_remaining, [current_role]),
                            )
                    role_batches.append(
                        {
                            'role': current_role,
                            'batch_count': len(batches),
                            'batches': batches,
                            'mode': 'single_symbol_slot_pool',
                        }
                    )

                remaining = self.load_incomplete_symbols(market_date=config.market_date, roles=config.roles)
                runtime_feedback_state_store.update_run(
                    config.market_date,
                    lambda current, remaining=remaining: self._refresh_logical_pending(current, remaining),
                )
                cycles.append(
                    {
                        'cycle': cycle_number,
                        'status': 'completed' if not remaining else 'partial',
                        'start_pending': cycle_start_pending,
                        'end_pending': len(remaining),
                        'role_batches': role_batches,
                    }
                )
                if not remaining:
                    break
        finally:
            runtime_feedback_state_store.update_run(
                config.market_date,
                self._finalize_logical_run,
            )

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

    @staticmethod
    def _set_logical_role_context(current: RuntimeFeedbackRunState | None, current_role: str | None) -> None:
        if current is None:
            return
        current.status = 'running'
        current.current_role = current_role or current.current_role
        current.last_activity_at = datetime.now(timezone.utc)

    @staticmethod
    def _set_logical_batch_context(current: RuntimeFeedbackRunState | None, current_role: str | None, batch_index: int, total_batches: int) -> None:
        if current is None:
            return
        current.parallel_per_role = current.parallel_per_role or 0
        current.status = 'running'
        current.current_role = current_role or current.current_role
        current.last_activity_at = datetime.now(timezone.utc)

    @staticmethod
    def _set_single_symbol_slot_context(current: RuntimeFeedbackRunState | None, current_role: str | None, total_symbols: int) -> None:
        if current is None:
            return
        current.parallel_per_role = 20
        current.batch_size = 1
        current.status = 'running'
        current.current_role = current_role or current.current_role
        current.logical_total_symbols = total_symbols
        current.logical_pending_symbols = total_symbols
        current.logical_in_progress_symbols = min(20, total_symbols)
        current.last_activity_at = datetime.now(timezone.utc)

    @staticmethod
    def _merge_batch_progress(current: RuntimeFeedbackRunState | None, result: dict[str, Any], batch_symbols: Sequence[str]) -> None:
        if current is None:
            return
        request = result.get('request') if isinstance(result, dict) else {}
        summary = result.get('summary') if isinstance(result, dict) else {}
        current.parallel_per_role = int(request.get('parallel_subtasks') or current.parallel_per_role or 0)
        roles = request.get('roles') if isinstance(request.get('roles'), list) else None
        if roles:
            current.current_role = str(roles[0])
        completed_items = summary.get('items') if isinstance(summary.get('items'), list) else []
        completed_now = len([item for item in completed_items if isinstance(item, dict) and item.get('status') == 'completed'])
        failed_now = len([item for item in completed_items if isinstance(item, dict) and item.get('status') == 'failed']) + int(summary.get('failed', 0))
        current.logical_completed_feedback_count = (current.logical_completed_feedback_count or 0) + completed_now
        current.logical_failed_items = (current.logical_failed_items or 0) + failed_now
        completed_symbol_count = min(len(batch_symbols), completed_now)
        current.logical_completed_symbols = min(current.logical_total_symbols or 0, (current.logical_completed_symbols or 0) + completed_symbol_count)
        current.logical_pending_symbols = max(0, (current.logical_total_symbols or 0) - (current.logical_completed_symbols or 0))
        current.logical_in_progress_symbols = 0 if current.logical_pending_symbols == 0 else min(current.parallel_per_role or 0, current.logical_pending_symbols)
        current.last_activity_at = datetime.now(timezone.utc)

    @staticmethod
    def _merge_batch_exception(current: RuntimeFeedbackRunState | None, batch_symbols: Sequence[str], roles: Sequence[str]) -> None:
        if current is None:
            return
        current.logical_failed_items = (current.logical_failed_items or 0) + (len(batch_symbols) * len(roles))
        current.logical_pending_symbols = max(0, (current.logical_total_symbols or 0) - (current.logical_completed_symbols or 0))
        current.logical_in_progress_symbols = 0
        current.last_activity_at = datetime.now(timezone.utc)

    @staticmethod
    def _refresh_logical_pending(current: RuntimeFeedbackRunState | None, remaining: Sequence[str]) -> None:
        if current is None:
            return
        total = current.logical_total_symbols or len(current.requested_symbols)
        current.logical_pending_symbols = len(remaining)
        current.logical_completed_symbols = max(0, total - len(remaining))
        current.logical_in_progress_symbols = 0 if not remaining else min(current.parallel_per_role or 0, len(remaining))
        current.last_activity_at = datetime.now(timezone.utc)

    @staticmethod
    def _finalize_logical_run(current: RuntimeFeedbackRunState | None) -> None:
        if current is None:
            return
        current.status = 'completed' if (current.logical_pending_symbols or 0) == 0 else 'partial'
        current.completed_at = datetime.now(timezone.utc)
        current.in_flight_slots_by_role = {}
    def _get_retry_progress(self, *, market_date: date, roles: Sequence[str]) -> dict[str, Any]:
        return agent_context_progress_service.get_feedback_progress(
            market_date=market_date,
            symbols=None,
            expected_agents=roles,
        )


agent_feedback_retry_service = AgentFeedbackRetryService()
