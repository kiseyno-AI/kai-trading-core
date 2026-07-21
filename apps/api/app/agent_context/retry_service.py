from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Callable, Sequence
from uuid import uuid4

from sqlalchemy import select

from app.agent_context.orchestrator_helpers import normalize_requested_symbols as _normalize_requested_symbols
from app.agent_context.prerequisite_service import agent_feedback_prerequisite_service
from app.agent_context.eligibility import (
    AGENT_FEEDBACK_ELIGIBILITY_FILTERS,
    AGENT_FEEDBACK_ELIGIBILITY_RULE,
    build_agent_feedback_eligibility_diagnostics,
    build_agent_feedback_role_funnel,
    load_agent_feedback_eligible_symbols,
    load_agent_feedback_eligibility_records,
    resolve_agent_feedback_scope,
)
from app.agent_context.orchestrator import agent_feedback_orchestrator
from app.agent_context.progress_service import agent_context_progress_service
from app.agent_context.reuse_service import agent_context_reuse_service
from app.agent_context.roles import DEFAULT_AGENT_NAMES, SIGNAL_DECIDER_AGENT_NAME, SUMMARIZER_AGENT_NAMES
from app.agent_context.run_modes import (
    DAILY_RETRY_CYCLE_MODE,
    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
    normalize_feedback_scope_mode,
)
from app.agent_context.run_state_service import agent_context_feedback_run_state_service
from app.agent_context.runtime_state import RuntimeFeedbackRunState, runtime_feedback_state_store
from app.core.settings import settings
from app.db import session_scope
from app.db_models import AgentContextFeedback, AgentContextFeedbackFailure

EXPECTED_ROLES: tuple[str, ...] = DEFAULT_AGENT_NAMES


def _normalize_optional_symbol_scope(symbols: Sequence[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        for piece in str(raw_symbol or '').split(','):
            symbol = piece.strip().upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            normalized.append(symbol)
    return normalized


def _ordered_symbol_union(symbol_scopes: Sequence[Sequence[str]]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for scope in symbol_scopes:
        for symbol in _normalize_optional_symbol_scope(scope):
            if symbol in seen:
                continue
            seen.add(symbol)
            ordered.append(symbol)
    return ordered


@dataclass(frozen=True)
class FeedbackRetryCycleConfig:
    market_date: date
    roles: tuple[str, ...]
    symbols: tuple[str, ...] | None = None
    feedback_scope_mode: str = FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
    max_cycles: int = 3
    outer_batch_size: int = 20
    reuse_existing: bool = True
    force_recompute: bool = False
    progress_callback: Callable[..., Any] | None = None


@dataclass(frozen=True)
class RoleSequenceRoleState:
    role: str
    total_symbols: int
    completed_symbols: tuple[str, ...]
    failed_symbols: tuple[str, ...]
    terminal_symbols: tuple[str, ...]
    boundary_pending_symbols: tuple[str, ...]
    retry_pending_symbols: tuple[str, ...]
    boundary_status: str
    reused_symbols: tuple[str, ...] = ()
    stale_symbols: tuple[str, ...] = ()


@dataclass(frozen=True)
class RoleSequenceSnapshot:
    market_date: date
    symbols: tuple[str, ...]
    roles: tuple[str, ...]
    role_states: dict[str, RoleSequenceRoleState]
    role_symbol_scopes: dict[str, tuple[str, ...]] = field(default_factory=dict)

    def boundary_pending_roles(self) -> list[str]:
        return [
            role
            for role in self.roles
            if self.role_states[role].boundary_pending_symbols
        ]

    def retry_pending_roles(self) -> list[str]:
        return [
            role
            for role in self.roles
            if self.role_states[role].retry_pending_symbols
        ]


@dataclass(frozen=True)
class RetryCyclePlan:
    cycle_number: int
    completed_cycles: int
    mode: str
    current_role: str
    plan_roles: tuple[str, ...]
    completed_roles: tuple[str, ...]
    remaining_roles: tuple[str, ...]


class AgentFeedbackRetryService:
    def _load_eligible_symbols(
        self,
        *,
        market_date: date,
        requested_symbols: Sequence[str] | None = None,
    ) -> list[str]:
        return load_agent_feedback_eligible_symbols(
            market_date=market_date,
            requested_symbols=requested_symbols,
        )

    def load_incomplete_symbols(
        self,
        *,
        market_date: date,
        roles: Sequence[str],
        eligible_symbols: Sequence[str] | None = None,
        force_recompute: bool = False,
    ) -> list[str]:
        expected_roles = tuple(roles)
        symbols = list(eligible_symbols) if eligible_symbols is not None else self._load_eligible_symbols(market_date=market_date)
        if not symbols:
            return []

        decisions = agent_context_reuse_service.build_latest_decisions(
            market_date=market_date,
            symbols=symbols,
            roles=expected_roles,
            force_recompute=force_recompute,
        )
        completed_by_role: dict[str, set[str]] = {}
        if not force_recompute:
            completed_by_role, _ = self._load_same_day_terminal_symbols(
                market_date=market_date,
                roles=expected_roles,
                symbols=symbols,
            )
        incomplete: list[str] = []
        for symbol in symbols:
            if (
                not force_recompute
                and expected_roles
                and all(symbol in completed_by_role.get(role, set()) for role in expected_roles)
            ):
                continue
            symbol_decisions = decisions.get(symbol, {})
            if any(
                symbol_decisions.get(role) is None or symbol_decisions[role].should_dispatch
                for role in expected_roles
            ):
                incomplete.append(symbol)
        return incomplete

    def _load_same_day_terminal_symbols(
        self,
        *,
        market_date: date,
        roles: Sequence[str],
        symbols: Sequence[str],
    ) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
        completed_by_role: dict[str, set[str]] = defaultdict(set)
        failed_by_role: dict[str, set[str]] = defaultdict(set)
        if not roles or not symbols:
            return completed_by_role, failed_by_role

        with session_scope() as session:
            feedback_rows = session.execute(
                select(AgentContextFeedback.symbol, AgentContextFeedback.agent_name)
                .where(AgentContextFeedback.market_date == market_date)
                .where(AgentContextFeedback.symbol.in_(list(symbols)))
                .where(AgentContextFeedback.agent_name.in_(list(roles)))
            ).all()
            failure_rows = session.execute(
                select(AgentContextFeedbackFailure.symbol, AgentContextFeedbackFailure.agent_name)
                .where(AgentContextFeedbackFailure.market_date == market_date)
                .where(AgentContextFeedbackFailure.symbol.in_(list(symbols)))
                .where(AgentContextFeedbackFailure.agent_name.in_(list(roles)))
            ).all()

        for row in feedback_rows:
            completed_by_role[str(row.agent_name)].add(str(row.symbol))
        for row in failure_rows:
            role = str(row.agent_name)
            symbol = str(row.symbol)
            if symbol in completed_by_role.get(role, set()):
                continue
            failed_by_role[role].add(symbol)
        return completed_by_role, failed_by_role

    def _filter_decider_retry_ready_symbols(
        self,
        *,
        market_date: date,
        symbols: Sequence[str],
    ) -> tuple[str, ...]:
        normalized_symbols = _normalize_optional_symbol_scope(symbols)
        if not normalized_symbols:
            return ()

        upstream_decisions = agent_context_reuse_service.build_latest_decisions(
            market_date=market_date,
            symbols=normalized_symbols,
            roles=SUMMARIZER_AGENT_NAMES,
            force_recompute=False,
        )
        ready_symbols: list[str] = []
        for symbol in normalized_symbols:
            symbol_decisions = upstream_decisions.get(symbol, {})
            upstream_ready = True
            for role in SUMMARIZER_AGENT_NAMES:
                decision = symbol_decisions.get(role)
                latest_feedback = getattr(decision, 'latest_feedback', None) if decision is not None else None
                feedback_market_date = (
                    latest_feedback.get('market_date')
                    if isinstance(latest_feedback, dict)
                    else None
                )
                has_same_day_feedback = feedback_market_date == market_date.isoformat()
                has_reusable_feedback = (
                    decision is not None
                    and not bool(getattr(decision, 'should_dispatch', True))
                    and isinstance(latest_feedback, dict)
                )
                if not (has_same_day_feedback or has_reusable_feedback):
                    upstream_ready = False
                    break
            if upstream_ready:
                ready_symbols.append(symbol)
        return tuple(ready_symbols)

    @staticmethod
    def _classify_role_boundary_status(
        *,
        total_symbols: int,
        completed_count: int,
        failed_count: int,
        pending_count: int,
    ) -> str:
        if total_symbols == 0:
            return 'completed'
        if pending_count > 0:
            return 'incomplete' if (completed_count or failed_count) else 'pending'
        if failed_count == 0:
            return 'completed'
        if completed_count > 0:
            return 'completed_with_failures'
        return 'failed'

    def _build_role_sequence_snapshot(
        self,
        *,
        market_date: date,
        roles: Sequence[str],
        eligible_symbols: Sequence[str],
        role_symbol_scopes: dict[str, Sequence[str]] | None = None,
        force_recompute: bool = False,
    ) -> RoleSequenceSnapshot:
        normalized_roles = tuple(roles)
        normalized_symbols = tuple(eligible_symbols)
        normalized_symbol_set = set(normalized_symbols)
        normalized_role_scopes = {
            role: tuple(
                symbol
                for symbol in _normalize_optional_symbol_scope(scope)
                if symbol in normalized_symbol_set
            )
            for role, scope in (role_symbol_scopes or {}).items()
        }
        completed_by_role, failed_by_role = self._load_same_day_terminal_symbols(
            market_date=market_date,
            roles=normalized_roles,
            symbols=normalized_symbols,
        )
        reuse_decisions = (
            agent_context_reuse_service.build_latest_decisions(
                market_date=market_date,
                symbols=normalized_symbols,
                roles=normalized_roles,
                force_recompute=force_recompute,
            )
            if normalized_symbols and normalized_roles
            else {}
        )

        role_states: dict[str, RoleSequenceRoleState] = {}
        for role in normalized_roles:
            role_symbols = normalized_role_scopes.get(role, normalized_symbols)
            completed_same_day_symbols = (
                ()
                if force_recompute
                else tuple(symbol for symbol in role_symbols if symbol in completed_by_role.get(role, set()))
            )
            reused_symbols = (
                ()
                if force_recompute
                else tuple(
                    symbol
                    for symbol in role_symbols
                    if symbol not in set(completed_same_day_symbols)
                    and (reuse_decisions.get(symbol, {}).get(role) is not None)
                    and not reuse_decisions[symbol][role].should_dispatch
                )
            )
            completed_symbols = tuple(dict.fromkeys([*completed_same_day_symbols, *reused_symbols]))
            failed_symbols = tuple(
                symbol
                for symbol in role_symbols
                if not force_recompute
                and symbol in failed_by_role.get(role, set())
                and symbol not in set(completed_symbols)
            )
            stale_symbols = tuple(
                symbol
                for symbol in role_symbols
                if (reuse_decisions.get(symbol, {}).get(role) is not None)
                and reuse_decisions[symbol][role].should_dispatch
                and str(getattr(reuse_decisions[symbol][role], 'reason', '') or '')
                in {
                    'forced_recompute',
                    'fundamental_input_fingerprint_changed',
                    'fundamental_input_fingerprint_missing',
                    'stored_fundamental_input_fingerprint_missing',
                    'source_updated',
                    'stale_market_feedback',
                }
            )
            terminal_symbol_set = set(completed_symbols) | set(failed_symbols)
            boundary_pending_symbols = tuple(symbol for symbol in role_symbols if symbol not in terminal_symbol_set)
            retry_pending_symbols = tuple(
                self.load_incomplete_symbols(
                    market_date=market_date,
                    roles=[role],
                    eligible_symbols=role_symbols,
                    force_recompute=force_recompute,
                )
            )
            if role == SIGNAL_DECIDER_AGENT_NAME and not boundary_pending_symbols:
                retry_pending_symbols = self._filter_decider_retry_ready_symbols(
                    market_date=market_date,
                    symbols=failed_symbols,
                )
            role_states[role] = RoleSequenceRoleState(
                role=role,
                total_symbols=len(role_symbols),
                completed_symbols=completed_symbols,
                failed_symbols=failed_symbols,
                reused_symbols=reused_symbols,
                stale_symbols=stale_symbols,
                terminal_symbols=tuple(symbol for symbol in role_symbols if symbol in terminal_symbol_set),
                boundary_pending_symbols=boundary_pending_symbols,
                retry_pending_symbols=retry_pending_symbols,
                boundary_status=self._classify_role_boundary_status(
                    total_symbols=len(role_symbols),
                    completed_count=len(completed_symbols),
                    failed_count=len(failed_symbols),
                    pending_count=len(boundary_pending_symbols),
                ),
            )

        return RoleSequenceSnapshot(
            market_date=market_date,
            symbols=normalized_symbols,
            roles=normalized_roles,
            role_states=role_states,
            role_symbol_scopes={
                role: normalized_role_scopes.get(role, normalized_symbols)
                for role in normalized_roles
            },
        )

    @staticmethod
    def _extract_prior_cycle_state(previous_run: dict[str, Any] | None) -> tuple[int, int]:
        if not isinstance(previous_run, dict):
            return 0, 0
        summary_payload = previous_run.get('summary_payload') if isinstance(previous_run.get('summary_payload'), dict) else {}
        progress_summary = summary_payload.get('progress_summary') if isinstance(summary_payload.get('progress_summary'), dict) else {}
        previous_cycle_number = int(progress_summary.get('cycle_number') or summary_payload.get('cycle_number') or 0)
        previous_completed_cycles = int(progress_summary.get('completed_cycles') or summary_payload.get('completed_cycles') or 0)
        return previous_cycle_number, previous_completed_cycles

    def _resolve_cycle_plan(
        self,
        *,
        snapshot: RoleSequenceSnapshot,
        max_cycles: int,
        previous_cycle_number: int,
        previous_completed_cycles: int,
        enforce_max_cycles: bool = True,
        retry_attempted_symbols_by_role: dict[str, set[str]] | None = None,
    ) -> RetryCyclePlan | None:
        boundary_pending_roles = snapshot.boundary_pending_roles()
        if boundary_pending_roles:
            cycle_number = previous_cycle_number or 1
            if enforce_max_cycles and cycle_number > max_cycles:
                return None
            current_role = boundary_pending_roles[0]
            current_index = snapshot.roles.index(current_role)
            return RetryCyclePlan(
                cycle_number=cycle_number,
                completed_cycles=max(0, min(previous_completed_cycles, cycle_number - 1)),
                mode='boundary_resume',
                current_role=current_role,
                plan_roles=tuple(snapshot.roles[current_index:]),
                completed_roles=tuple(snapshot.roles[:current_index]),
                remaining_roles=tuple(snapshot.roles[current_index:]),
            )

        retry_pending_roles = self._retry_pending_roles(
            snapshot,
            retry_attempted_symbols_by_role=retry_attempted_symbols_by_role,
        )
        if not retry_pending_roles:
            return None

        cycle_number = max(1, previous_completed_cycles + 1)
        if enforce_max_cycles and cycle_number > max_cycles:
            return None
        return RetryCyclePlan(
            cycle_number=cycle_number,
            completed_cycles=max(0, cycle_number - 1),
            mode='retry_followup',
            current_role=retry_pending_roles[0],
            plan_roles=tuple(retry_pending_roles),
            completed_roles=tuple(role for role in snapshot.roles if role not in retry_pending_roles),
            remaining_roles=tuple(retry_pending_roles),
        )

    def _sync_runtime_state(
        self,
        *,
        market_date: date,
        run_id: str,
        snapshot: RoleSequenceSnapshot,
        current_role: str | None,
        completed_roles: Sequence[str],
        remaining_roles: Sequence[str],
        cycle_number: int | None,
        completed_cycles: int | None,
        max_cycles: int,
        cycle_mode: str | None,
        cycle_status: str,
        status: str,
        funnel_diagnostics: dict[str, Any] | None = None,
    ) -> RuntimeFeedbackRunState | None:
        role_symbol_scopes = {
            role: list(scope)
            for role, scope in snapshot.role_symbol_scopes.items()
        } if snapshot.role_symbol_scopes else {
            role: list(snapshot.symbols)
            for role in snapshot.roles
        }

        def _update(current: RuntimeFeedbackRunState | None) -> None:
            if current is None:
                return
            current.status = status
            current.current_role = current_role
            current.completed_roles = list(completed_roles)
            current.remaining_roles = list(remaining_roles)
            current.cycle_number = cycle_number
            current.completed_cycles = completed_cycles
            current.max_cycles = max_cycles
            current.cycle_mode = cycle_mode
            current.cycle_status = cycle_status
            current.completed_symbols_by_role = {
                role: set(role_state.completed_symbols)
                for role, role_state in snapshot.role_states.items()
                if role_state.completed_symbols
            }
            current.failed_symbols_by_role = {
                role: set(role_state.failed_symbols)
                for role, role_state in snapshot.role_states.items()
                if role_state.failed_symbols
            }
            current.reused_symbols_by_role = {
                role: set(role_state.reused_symbols)
                for role, role_state in snapshot.role_states.items()
                if role_state.reused_symbols
            }
            current.stale_symbols_by_role = {
                role: set(role_state.stale_symbols)
                for role, role_state in snapshot.role_states.items()
                if role_state.stale_symbols
            }
            current.retry_pending_symbols_by_role = {
                role: set(self._retry_symbols_for_role(role_state))
                for role, role_state in snapshot.role_states.items()
                if self._retry_symbols_for_role(role_state)
            }
            for role, role_state in snapshot.role_states.items():
                if role_state.boundary_pending_symbols:
                    continue
                current.first_pass_completed_symbols_by_role.setdefault(
                    role,
                    set(role_state.terminal_symbols),
                )
                current.first_pass_failed_symbols_by_role.setdefault(
                    role,
                    set(role_state.failed_symbols),
                )
                retry_subset = self._retry_symbols_for_role(role_state)
                if retry_subset:
                    current.retry_queued_symbols_by_role.setdefault(role, set(retry_subset))
            current.role_boundary_statuses = {
                role: role_state.boundary_status
                for role, role_state in snapshot.role_states.items()
            }
            current.role_symbol_scopes = role_symbol_scopes
            if funnel_diagnostics is not None:
                current.funnel_diagnostics = funnel_diagnostics
            current.logical_total_symbols = len(snapshot.symbols)
            current.logical_expected_feedback_count = sum(
                len(role_symbol_scopes.get(role, list(snapshot.symbols)))
                for role in snapshot.roles
            )
            current.logical_completed_feedback_count = sum(
                len(role_state.completed_symbols)
                for role_state in snapshot.role_states.values()
            )
            current.logical_failed_items = sum(
                len(role_state.failed_symbols)
                for role_state in snapshot.role_states.values()
            )
            current.logical_completed_symbols = None
            current.logical_in_progress_symbols = None
            current.logical_pending_symbols = None
            current.last_activity_at = datetime.now(timezone.utc)
            current.in_flight_slots_by_role = {}

        return runtime_feedback_state_store.update_run(
            market_date,
            _update,
            run_id=run_id,
            logical=True,
        )

    def _build_run_summary_payload(
        self,
        *,
        market_date: date,
        request_payload: dict[str, Any],
        state: RuntimeFeedbackRunState,
        lifecycle_stage: str,
        cycle_result: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        progress_summary = state.to_progress_summary()
        role_progress = progress_summary.get('role_progress') if isinstance(progress_summary.get('role_progress'), dict) else {}
        return {
            'market_date': market_date.isoformat(),
            'request': request_payload,
            'mode': DAILY_RETRY_CYCLE_MODE,
            'feedback_scope_mode': state.feedback_scope_mode or request_payload.get('feedback_scope_mode'),
            'lifecycle_stage': lifecycle_stage,
            'current_role': progress_summary.get('current_role'),
            'completed_roles': list(progress_summary.get('completed_roles') or []),
            'remaining_roles': list(progress_summary.get('remaining_roles') or []),
            'cycle_number': progress_summary.get('cycle_number'),
            'completed_cycles': progress_summary.get('completed_cycles'),
            'max_cycles': progress_summary.get('max_cycles'),
            'cycle_mode': progress_summary.get('cycle_mode'),
            'cycle_status': progress_summary.get('cycle_status'),
            'role_sequence': list(state.role_sequence),
            'role_statuses': {
                role: role_payload.get('status')
                for role, role_payload in role_progress.items()
                if isinstance(role_payload, dict)
            },
            'role_retry_pending_counts': {
                role: int(role_payload.get('retry_pending_symbols') or 0)
                for role, role_payload in role_progress.items()
                if isinstance(role_payload, dict)
            },
            'role_symbol_counts': progress_summary.get('role_symbol_counts'),
            'funnel_diagnostics': progress_summary.get('funnel_diagnostics'),
            'progress_summary': progress_summary,
            'cycle_result': cycle_result,
            'resumed_from_run_id': request_payload.get('resumed_from_run_id'),
            'last_activity_at': state.last_activity_at.isoformat() if state.last_activity_at else None,
        }

    def _checkpoint_run_state(
        self,
        *,
        market_date: date,
        request_run_id: str,
        request_payload: dict[str, Any],
        lifecycle_stage: str,
        status: str,
        cycle_result: dict[str, Any] | None = None,
        progress_callback: Callable[..., Any] | None = None,
    ) -> dict[str, Any]:
        state = runtime_feedback_state_store.get_run(market_date, run_id=request_run_id, logical=True)
        if state is None:
            raise RuntimeError(f'feedback retry runtime state missing for {request_run_id}')
        summary_payload = self._build_run_summary_payload(
            market_date=market_date,
            request_payload=request_payload,
            state=state,
            lifecycle_stage=lifecycle_stage,
            cycle_result=cycle_result,
        )
        completed_count = int(summary_payload['progress_summary'].get('completed_feedback_count') or 0)
        failed_count = int(summary_payload['progress_summary'].get('failed_items') or 0)
        checkpoint = agent_context_feedback_run_state_service.checkpoint_run(
            run_id=request_run_id,
            status=status,
            completed_count=completed_count,
            failed_count=failed_count,
            summary_payload=summary_payload,
        )
        self._emit_progress_callback(
            progress_callback=progress_callback,
            request_run_id=request_run_id,
            lifecycle_stage=lifecycle_stage,
            status=status,
            completed_count=completed_count,
            failed_count=failed_count,
            summary_payload=summary_payload,
            cycle_result=cycle_result,
        )
        return checkpoint

    @staticmethod
    def _emit_progress_callback(
        *,
        progress_callback: Callable[..., Any] | None,
        request_run_id: str,
        lifecycle_stage: str,
        status: str,
        completed_count: int,
        failed_count: int,
        summary_payload: dict[str, Any],
        cycle_result: dict[str, Any] | None,
    ) -> None:
        if progress_callback is None:
            return
        progress_summary = (
            summary_payload.get('progress_summary')
            if isinstance(summary_payload.get('progress_summary'), dict)
            else {}
        )
        total = int(progress_summary.get('expected_feedback_count') or summary_payload.get('expected_feedback_count') or 0)
        processed = min(completed_count + failed_count, total) if total else completed_count + failed_count
        current_role = str(progress_summary.get('current_role') or summary_payload.get('current_role') or '').strip() or None
        cycle_status = str(progress_summary.get('cycle_status') or summary_payload.get('cycle_status') or status).strip()
        activity_role = current_role or 'agent feedback'
        progress_callback(
            status=status,
            current_step=lifecycle_stage,
            current_activity=f'Agent feedback {lifecycle_stage.replace("_", " ")}: {activity_role}',
            processed=processed,
            total=total,
            succeeded=completed_count,
            failed=failed_count,
            remaining=max(total - processed, 0) if total else 0,
            current_period=current_role,
            source_run_ids={'agent_feedback_run_id': request_run_id},
            details={
                'source': 'agent_context_feedback_run_state',
                'run_id': request_run_id,
                'cycle_status': cycle_status,
                'cycle_number': progress_summary.get('cycle_number') or summary_payload.get('cycle_number'),
                'completed_cycles': progress_summary.get('completed_cycles') or summary_payload.get('completed_cycles'),
                'max_cycles': progress_summary.get('max_cycles') or summary_payload.get('max_cycles'),
                'completed_roles': list(progress_summary.get('completed_roles') or summary_payload.get('completed_roles') or []),
                'remaining_roles': list(progress_summary.get('remaining_roles') or summary_payload.get('remaining_roles') or []),
                'expected_feedback_count': total,
                'completed_feedback_count': completed_count,
                'failed_items': failed_count,
                'role_progress': progress_summary.get('role_progress') if isinstance(progress_summary.get('role_progress'), dict) else {},
                'role_symbol_counts': progress_summary.get('role_symbol_counts') or summary_payload.get('role_symbol_counts'),
                'feedback_scope_mode': progress_summary.get('feedback_scope_mode') or summary_payload.get('feedback_scope_mode'),
                'current_role': current_role,
                'cycle_result': cycle_result,
            },
        )

    @staticmethod
    def _retry_symbols_for_role(
        role_state: RoleSequenceRoleState,
        *,
        attempted_symbols: set[str] | None = None,
    ) -> tuple[str, ...]:
        attempted = attempted_symbols or set()
        if role_state.role == SIGNAL_DECIDER_AGENT_NAME and not role_state.boundary_pending_symbols:
            retry_ready = set(role_state.retry_pending_symbols)
            return tuple(
                symbol for symbol in role_state.failed_symbols
                if symbol in retry_ready and symbol not in attempted
            )
        return tuple(symbol for symbol in role_state.retry_pending_symbols if symbol not in attempted)

    def _retry_pending_roles(
        self,
        snapshot: RoleSequenceSnapshot,
        *,
        retry_attempted_symbols_by_role: dict[str, set[str]] | None = None,
    ) -> list[str]:
        return [
            role
            for role in snapshot.roles
            if self._retry_symbols_for_role(
                snapshot.role_states[role],
                attempted_symbols=(retry_attempted_symbols_by_role or {}).get(role),
            )
        ]

    def _collect_retry_pending_symbols(
        self,
        snapshot: RoleSequenceSnapshot,
        *,
        retry_attempted_symbols_by_role: dict[str, set[str]] | None = None,
    ) -> list[str]:
        ordered: list[str] = []
        seen: set[str] = set()
        for role in snapshot.roles:
            for symbol in self._retry_symbols_for_role(
                snapshot.role_states[role],
                attempted_symbols=(retry_attempted_symbols_by_role or {}).get(role),
            ):
                if symbol in seen:
                    continue
                seen.add(symbol)
                ordered.append(symbol)
        return ordered

    @staticmethod
    def _mark_retry_attempted_symbols(
        *,
        market_date: date,
        run_id: str,
        role: str,
        symbols: Sequence[str],
    ) -> None:
        def _mark_retry_attempted(current: RuntimeFeedbackRunState) -> None:
            current.retry_attempted_symbols_by_role.setdefault(role, set()).update(symbols)
            current.last_activity_at = datetime.now(timezone.utc)

        runtime_feedback_state_store.update_run(
            market_date,
            _mark_retry_attempted,
            run_id=run_id,
            logical=True,
        )

    @staticmethod
    def _collect_terminal_items(snapshot: RoleSequenceSnapshot) -> int:
        return sum(
            len(role_state.terminal_symbols)
            for role_state in snapshot.role_states.values()
        )

    @staticmethod
    def _symbol_chunks(symbols: Sequence[str], *, batch_size: int) -> list[list[str]]:
        resolved_batch_size = max(1, int(batch_size or 1))
        return [
            list(symbols[index:index + resolved_batch_size])
            for index in range(0, len(symbols), resolved_batch_size)
        ]

    @staticmethod
    def _aggregate_dispatch_summaries(dispatch_results: Sequence[dict[str, Any]]) -> dict[str, Any]:
        summaries = [
            result.get('summary') if isinstance(result.get('summary'), dict) else {}
            for result in dispatch_results
        ]
        completed = sum(int(summary.get('completed') or 0) for summary in summaries)
        failed = sum(int(summary.get('failed') or 0) for summary in summaries)
        statuses = {str(summary.get('status') or '').strip().lower() for summary in summaries}
        status = 'partial' if failed > 0 or statuses.intersection({'failed', 'partial', 'blocked'}) else 'completed'
        return {
            'status': status,
            'completed': completed,
            'failed': failed,
            'chunks': len(dispatch_results),
        }

    def run_retry_cycles(self, *, config: FeedbackRetryCycleConfig, force: bool = False) -> dict[str, Any]:
        roles = tuple(config.roles)
        progress_callback = getattr(config, 'progress_callback', None)
        feedback_scope_mode = normalize_feedback_scope_mode(getattr(config, 'feedback_scope_mode', None))
        effective_force_recompute = bool(force or getattr(config, 'force_recompute', False))
        raw_requested_symbols = getattr(config, 'symbols', None)
        requested_symbols = (
            tuple(_normalize_requested_symbols(raw_requested_symbols))
            if raw_requested_symbols is not None
            else None
        )
        prerequisites = agent_feedback_prerequisite_service.inspect_feedback_prerequisites(
            market_date=config.market_date,
        )
        if not force and not prerequisites['ready']:
            progress = self._get_retry_progress(
                market_date=config.market_date,
                roles=roles,
                symbols=list(requested_symbols) if requested_symbols is not None else None,
                run_id=None,
            )
            return {
                'market_date': config.market_date.isoformat(),
                'roles': list(roles),
                'max_cycles': config.max_cycles,
                'outer_batch_size': config.outer_batch_size,
                'reuse_existing': config.reuse_existing,
                'force_recompute': effective_force_recompute,
                'feedback_scope_mode': feedback_scope_mode,
                'requested_symbols': list(requested_symbols) if requested_symbols is not None else None,
                'eligible_symbols': None,
                'skipped_symbols': [],
                'run_id': None,
                'status': prerequisites['status'],
                'gated': True,
                'cycles': [],
                'final_pending': int(progress.get('summary', {}).get('pending_symbols') or 0),
                'completed': False,
                'prerequisites': prerequisites,
                'progress': progress,
            }
        eligible_symbols = self._load_eligible_symbols(
            market_date=config.market_date,
            requested_symbols=requested_symbols,
        )
        eligibility_records = load_agent_feedback_eligibility_records(
            market_date=config.market_date,
            requested_symbols=requested_symbols,
        )
        eligibility_diagnostics = build_agent_feedback_eligibility_diagnostics(
            records=eligibility_records,
            requested_symbols=requested_symbols,
        )
        role_funnel = build_agent_feedback_role_funnel(
            records=eligibility_records,
            roles=roles,
            requested_symbols=requested_symbols,
            candidate_symbols=eligible_symbols,
            feedback_scope_mode=feedback_scope_mode,
        )
        role_symbol_scopes = {
            role: tuple(symbols)
            for role, symbols in role_funnel.get('role_symbols', {}).items()
            if isinstance(symbols, list)
        }
        scoped_feedback_symbols = _ordered_symbol_union(
            [role_symbol_scopes.get(role, ()) for role in roles]
        ) or list(eligible_symbols)
        skipped_symbols = (
            [symbol for symbol in requested_symbols or () if symbol not in set(eligible_symbols)]
            if requested_symbols is not None
            else []
        )
        previous_run = None
        if not effective_force_recompute:
            previous_run = agent_context_feedback_run_state_service.find_latest_run(
                market_date=config.market_date,
                requested_symbols=list(requested_symbols) if requested_symbols is not None else None,
                requested_roles=list(roles),
                request_modes=[DAILY_RETRY_CYCLE_MODE],
                feedback_scope_mode=feedback_scope_mode,
            )
        previous_cycle_number, previous_completed_cycles = self._extract_prior_cycle_state(previous_run)
        initial_snapshot = self._build_role_sequence_snapshot(
            market_date=config.market_date,
            roles=roles,
            eligible_symbols=scoped_feedback_symbols,
            role_symbol_scopes=role_symbol_scopes,
            force_recompute=effective_force_recompute,
        )
        next_plan = self._resolve_cycle_plan(
            snapshot=initial_snapshot,
            max_cycles=config.max_cycles,
            previous_cycle_number=previous_cycle_number,
            previous_completed_cycles=previous_completed_cycles,
        )

        request_run_id = f"feedback-cycle-{config.market_date.isoformat()}-{uuid4().hex[:10]}"
        request_payload = {
            'market_date': config.market_date.isoformat(),
            'symbols': list(scoped_feedback_symbols),
            'broad_symbols': list(eligible_symbols),
            'broad_symbol_count': len(eligible_symbols),
            'requested_symbols': list(requested_symbols) if requested_symbols is not None else list(scoped_feedback_symbols),
            'request_run_id': request_run_id,
            'roles': list(roles),
            'max_cycles': config.max_cycles,
            'outer_batch_size': config.outer_batch_size,
            'parallel_subtasks': max(1, int(settings.openclaw_agent_parallel_subtasks)),
            'reuse_existing': config.reuse_existing,
            'force_recompute': effective_force_recompute,
            'scope': resolve_agent_feedback_scope(requested_symbols=requested_symbols),
            'feedback_scope_mode': feedback_scope_mode,
            'eligibility_rule': AGENT_FEEDBACK_ELIGIBILITY_RULE,
            'eligibility_filters': list(AGENT_FEEDBACK_ELIGIBILITY_FILTERS),
            'eligibility_diagnostics': eligibility_diagnostics,
            'funnel_diagnostics': role_funnel,
            'role_symbol_scopes': {
                role: list(symbols)
                for role, symbols in role_symbol_scopes.items()
            },
            'role_symbol_counts': {
                role: len(symbols)
                for role, symbols in role_symbol_scopes.items()
            },
            'skipped_symbols': skipped_symbols,
            'mode': DAILY_RETRY_CYCLE_MODE,
            'resumed_from_run_id': previous_run.get('run_id') if isinstance(previous_run, dict) else None,
        }
        logical_state = RuntimeFeedbackRunState(
            run_id=request_run_id,
            market_date=config.market_date,
            requested_symbols=list(scoped_feedback_symbols),
            requested_roles=list(roles),
            role_sequence=list(roles),
            parallel_per_role=max(1, int(settings.openclaw_agent_parallel_subtasks)),
            batch_size=max(1, config.outer_batch_size),
            started_at=datetime.now(timezone.utc),
            current_role=next_plan.current_role if next_plan is not None else None,
            role_symbol_scopes={
                role: list(symbols)
                for role, symbols in role_symbol_scopes.items()
            },
            funnel_diagnostics=role_funnel,
            feedback_scope_mode=feedback_scope_mode,
        )
        runtime_feedback_state_store.start_run(logical_state, replace=True, logical=True)
        self._sync_runtime_state(
            market_date=config.market_date,
            run_id=request_run_id,
            snapshot=initial_snapshot,
            current_role=next_plan.current_role if next_plan is not None else None,
            completed_roles=next_plan.completed_roles if next_plan is not None else tuple(roles),
            remaining_roles=next_plan.remaining_roles if next_plan is not None else (),
            cycle_number=next_plan.cycle_number if next_plan is not None else previous_completed_cycles,
            completed_cycles=next_plan.completed_cycles if next_plan is not None else previous_completed_cycles,
            max_cycles=config.max_cycles,
            cycle_mode=next_plan.mode if next_plan is not None else None,
            cycle_status='running' if next_plan is not None else 'completed',
            status='running' if next_plan is not None else 'completed',
            funnel_diagnostics=role_funnel,
        )

        state = runtime_feedback_state_store.get_run(config.market_date, run_id=request_run_id, logical=True)
        if state is None:
            raise RuntimeError(f'feedback retry runtime state missing for {request_run_id}')
        agent_context_feedback_run_state_service.start_run(
            run_id=request_run_id,
            market_date=config.market_date,
            symbols_total=len(scoped_feedback_symbols),
            request_payload=request_payload,
            summary_payload=self._build_run_summary_payload(
                market_date=config.market_date,
                request_payload=request_payload,
                state=state,
                lifecycle_stage='run_started',
                cycle_result=None,
            ),
        )

        cycles: list[dict[str, Any]] = []
        blocked = False
        active_plan = next_plan
        completed_cycles = next_plan.completed_cycles if next_plan is not None else previous_completed_cycles

        try:
            while active_plan is not None:
                cycle_roles: list[dict[str, Any]] = []
                cycle_state_before = runtime_feedback_state_store.get_run(
                    config.market_date,
                    run_id=request_run_id,
                    logical=True,
                )
                retry_attempted_before = (
                    cycle_state_before.retry_attempted_symbols_by_role
                    if cycle_state_before is not None
                    else {}
                )
                cycle_start_pending = len(
                    self._collect_retry_pending_symbols(
                        initial_snapshot if not cycles else snapshot_after_cycle,
                        retry_attempted_symbols_by_role=retry_attempted_before,
                    )
                )
                self._sync_runtime_state(
                    market_date=config.market_date,
                    run_id=request_run_id,
                    snapshot=initial_snapshot if not cycles else snapshot_after_cycle,
                    current_role=active_plan.current_role,
                    completed_roles=active_plan.completed_roles,
                    remaining_roles=active_plan.remaining_roles,
                    cycle_number=active_plan.cycle_number,
                    completed_cycles=active_plan.completed_cycles,
                    max_cycles=config.max_cycles,
                    cycle_mode=active_plan.mode,
                    cycle_status='running',
                    status='running',
                )
                self._checkpoint_run_state(
                    market_date=config.market_date,
                    request_run_id=request_run_id,
                    request_payload=request_payload,
                    lifecycle_stage='cycle_started',
                    status='running',
                    progress_callback=progress_callback,
                    cycle_result={
                        'cycle': active_plan.cycle_number,
                        'mode': active_plan.mode,
                        'completed_roles': list(active_plan.completed_roles),
                        'remaining_roles': list(active_plan.remaining_roles),
                    },
                )

                for index, role in enumerate(active_plan.plan_roles):
                    snapshot_before_role = self._build_role_sequence_snapshot(
                        market_date=config.market_date,
                        roles=roles,
                        eligible_symbols=scoped_feedback_symbols,
                        role_symbol_scopes=role_symbol_scopes,
                    )
                    completed_roles_before = tuple(
                        list(active_plan.completed_roles) + list(active_plan.plan_roles[:index])
                    )
                    remaining_roles_before = tuple(active_plan.plan_roles[index:])
                    self._sync_runtime_state(
                        market_date=config.market_date,
                        run_id=request_run_id,
                        snapshot=snapshot_before_role,
                        current_role=role,
                        completed_roles=completed_roles_before,
                        remaining_roles=remaining_roles_before,
                        cycle_number=active_plan.cycle_number,
                        completed_cycles=active_plan.completed_cycles,
                        max_cycles=config.max_cycles,
                        cycle_mode=active_plan.mode,
                        cycle_status='running',
                        status='running',
                        funnel_diagnostics=role_funnel,
                    )

                    role_state_before = snapshot_before_role.role_states[role]
                    dispatch_symbols = list(
                        role_symbol_scopes.get(role, scoped_feedback_symbols)
                        if effective_force_recompute
                        else role_state_before.boundary_pending_symbols
                        if active_plan.mode == 'boundary_resume'
                        else self._retry_symbols_for_role(role_state_before)
                    )
                    if active_plan.mode == 'retry_followup' and dispatch_symbols:
                        self._mark_retry_attempted_symbols(
                            market_date=config.market_date,
                            run_id=request_run_id,
                            role=role,
                            symbols=dispatch_symbols,
                        )
                    self._checkpoint_run_state(
                        market_date=config.market_date,
                        request_run_id=request_run_id,
                        request_payload=request_payload,
                        lifecycle_stage='role_started',
                        status='running',
                        progress_callback=progress_callback,
                        cycle_result={
                            'cycle': active_plan.cycle_number,
                            'mode': active_plan.mode,
                            'role': role,
                            'dispatch_symbols': list(dispatch_symbols),
                        },
                    )

                    role_result: dict[str, Any] = {
                        'role': role,
                        'dispatch_symbols': list(dispatch_symbols),
                    }
                    role_exception: Exception | None = None
                    if dispatch_symbols:
                        try:
                            dispatch_results: list[dict[str, Any]] = []
                            dispatch_chunks = self._symbol_chunks(
                                dispatch_symbols,
                                batch_size=config.outer_batch_size,
                            )
                            for chunk_index, dispatch_chunk in enumerate(dispatch_chunks, start=1):
                                dispatch_result = agent_feedback_orchestrator.dispatch_for_symbols(
                                    market_date=config.market_date,
                                    symbols=dispatch_chunk,
                                    roles=[role],
                                    reuse_existing=False if effective_force_recompute else config.reuse_existing,
                                    force=force,
                                    force_recompute=effective_force_recompute,
                                    auto_scope=requested_symbols is None,
                                    role_symbol_scopes={role: dispatch_chunk},
                                    funnel_diagnostics={
                                        **role_funnel,
                                        'role_symbols': {role: list(dispatch_chunk)},
                                        'role_symbol_counts': {role: len(dispatch_chunk)},
                                        'role_diagnostics': {
                                            role: role_funnel.get('role_diagnostics', {}).get(role, {})
                                        },
                                        'dispatch_chunk': {
                                            'index': chunk_index,
                                            'total': len(dispatch_chunks),
                                            'outer_batch_size': max(1, int(config.outer_batch_size or 1)),
                                        },
                                    },
                                )
                                dispatch_results.append(dispatch_result)
                                if chunk_index >= len(dispatch_chunks):
                                    continue
                                snapshot_after_chunk = self._build_role_sequence_snapshot(
                                    market_date=config.market_date,
                                    roles=roles,
                                    eligible_symbols=scoped_feedback_symbols,
                                    role_symbol_scopes=role_symbol_scopes,
                                    force_recompute=effective_force_recompute,
                                )
                                self._sync_runtime_state(
                                    market_date=config.market_date,
                                    run_id=request_run_id,
                                    snapshot=snapshot_after_chunk,
                                    current_role=role,
                                    completed_roles=completed_roles_before,
                                    remaining_roles=remaining_roles_before,
                                    cycle_number=active_plan.cycle_number,
                                    completed_cycles=active_plan.completed_cycles,
                                    max_cycles=config.max_cycles,
                                    cycle_mode=active_plan.mode,
                                    cycle_status='running',
                                    status='running',
                                    funnel_diagnostics=role_funnel,
                                )
                                self._checkpoint_run_state(
                                    market_date=config.market_date,
                                    request_run_id=request_run_id,
                                    request_payload=request_payload,
                                    lifecycle_stage='role_batch_completed',
                                    status='running',
                                    progress_callback=progress_callback,
                                    cycle_result={
                                        'cycle': active_plan.cycle_number,
                                        'mode': active_plan.mode,
                                        'role': role,
                                        'chunk_index': chunk_index,
                                        'chunk_total': len(dispatch_chunks),
                                        'chunk_symbols': list(dispatch_chunk),
                                        'chunk_request': dispatch_result.get('request'),
                                        'chunk_summary': dispatch_result.get('summary'),
                                    },
                                )
                            aggregate_summary = self._aggregate_dispatch_summaries(dispatch_results)
                            role_result.update(
                                {
                                    'status': aggregate_summary['status'],
                                    'summary': aggregate_summary,
                                    'request': {
                                        'roles': [role],
                                        'symbols': list(dispatch_symbols),
                                        'chunks': len(dispatch_results),
                                    },
                                    'chunk_results': [
                                        {
                                            'request': result.get('request'),
                                            'summary': result.get('summary'),
                                        }
                                        for result in dispatch_results
                                    ],
                                }
                            )
                        except Exception as exc:  # noqa: BLE001
                            role_exception = exc
                            role_result.update(
                                {
                                    'status': 'failed',
                                    'error_type': type(exc).__name__,
                                    'error': str(exc),
                                }
                            )
                    else:
                        role_result.update(
                            {
                                'status': 'skipped',
                                'reason': 'no_symbols_remaining_for_role',
                            }
                        )

                    snapshot_after_role = self._build_role_sequence_snapshot(
                        market_date=config.market_date,
                        roles=roles,
                        eligible_symbols=scoped_feedback_symbols,
                        role_symbol_scopes=role_symbol_scopes,
                    )
                    role_state_after = snapshot_after_role.role_states[role]
                    role_boundary_complete = len(role_state_after.boundary_pending_symbols) == 0
                    completed_roles_after = tuple(
                        list(active_plan.completed_roles) + list(active_plan.plan_roles[:index + (1 if role_boundary_complete else 0)])
                    )
                    remaining_roles_after = tuple(
                        active_plan.plan_roles[index + 1:]
                        if role_boundary_complete
                        else active_plan.plan_roles[index:]
                    )
                    current_role_after = (
                        active_plan.plan_roles[index + 1]
                        if role_boundary_complete and index + 1 < len(active_plan.plan_roles)
                        else (role if not role_boundary_complete else None)
                    )
                    cycle_status = 'blocked' if not role_boundary_complete else ('running' if remaining_roles_after else 'completed')
                    self._sync_runtime_state(
                        market_date=config.market_date,
                        run_id=request_run_id,
                        snapshot=snapshot_after_role,
                        current_role=current_role_after,
                        completed_roles=completed_roles_after,
                        remaining_roles=remaining_roles_after,
                        cycle_number=active_plan.cycle_number,
                        completed_cycles=active_plan.completed_cycles,
                        max_cycles=config.max_cycles,
                        cycle_mode=active_plan.mode,
                        cycle_status=cycle_status,
                        status='running',
                        funnel_diagnostics=role_funnel,
                    )
                    self._checkpoint_run_state(
                        market_date=config.market_date,
                        request_run_id=request_run_id,
                        request_payload=request_payload,
                        lifecycle_stage='role_completed',
                        status='running',
                        progress_callback=progress_callback,
                        cycle_result={
                            **role_result,
                            'cycle': active_plan.cycle_number,
                            'mode': active_plan.mode,
                            'boundary_status': role_state_after.boundary_status,
                            'retry_pending_symbols': len(role_state_after.retry_pending_symbols),
                            'completed_roles': list(completed_roles_after),
                            'remaining_roles': list(remaining_roles_after),
                        },
                    )
                    cycle_roles.append(
                        {
                            **role_result,
                            'boundary_status': role_state_after.boundary_status,
                            'retry_pending_symbols': len(role_state_after.retry_pending_symbols),
                            'completed_roles': list(completed_roles_after),
                            'remaining_roles': list(remaining_roles_after),
                        }
                    )
                    if role_exception is not None and not role_boundary_complete:
                        blocked = True
                        break

                snapshot_after_cycle = self._build_role_sequence_snapshot(
                    market_date=config.market_date,
                    roles=roles,
                    eligible_symbols=scoped_feedback_symbols,
                    role_symbol_scopes=role_symbol_scopes,
                )
                cycle_state = runtime_feedback_state_store.get_run(
                    config.market_date,
                    run_id=request_run_id,
                    logical=True,
                )
                retry_attempted_after = (
                    cycle_state.retry_attempted_symbols_by_role
                    if cycle_state is not None
                    else {}
                )
                retry_pending_symbols = self._collect_retry_pending_symbols(
                    snapshot_after_cycle,
                    retry_attempted_symbols_by_role=retry_attempted_after,
                )
                cycle_status = 'blocked' if blocked else ('completed' if not retry_pending_symbols else 'partial')
                if not blocked:
                    completed_cycles = active_plan.cycle_number
                cycles.append(
                    {
                        'cycle': active_plan.cycle_number,
                        'mode': active_plan.mode,
                        'status': cycle_status,
                        'start_pending': cycle_start_pending,
                        'end_pending': len(retry_pending_symbols),
                        'completed_roles': list(cycle_state.completed_roles if cycle_state is not None else active_plan.completed_roles),
                        'remaining_roles': list(cycle_state.remaining_roles if cycle_state is not None else active_plan.remaining_roles),
                        'role_batches': cycle_roles,
                    }
                )
                if blocked or not retry_pending_symbols:
                    initial_snapshot = snapshot_after_cycle
                    break

                initial_snapshot = snapshot_after_cycle
                active_plan = self._resolve_cycle_plan(
                    snapshot=snapshot_after_cycle,
                    max_cycles=config.max_cycles,
                    previous_cycle_number=active_plan.cycle_number,
                    previous_completed_cycles=completed_cycles,
                    retry_attempted_symbols_by_role=retry_attempted_after,
                )
        finally:
            final_snapshot = self._build_role_sequence_snapshot(
                market_date=config.market_date,
                roles=roles,
                eligible_symbols=scoped_feedback_symbols,
                role_symbol_scopes=role_symbol_scopes,
            )
            final_state_before_finish = runtime_feedback_state_store.get_run(
                config.market_date,
                run_id=request_run_id,
                logical=True,
            )
            final_retry_attempted = (
                final_state_before_finish.retry_attempted_symbols_by_role
                if final_state_before_finish is not None
                else {}
            )
            final_retry_pending_symbols = self._collect_retry_pending_symbols(
                final_snapshot,
                retry_attempted_symbols_by_role=final_retry_attempted,
            )
            final_boundary_pending_roles = final_snapshot.boundary_pending_roles()
            terminal_items = self._collect_terminal_items(final_snapshot)
            if not eligible_symbols:
                final_status = 'completed'
            elif final_retry_pending_symbols or final_boundary_pending_roles:
                final_status = 'partial' if terminal_items > 0 else 'failed'
            else:
                final_status = 'completed'

            resume_plan = self._resolve_cycle_plan(
                snapshot=final_snapshot,
                max_cycles=max(config.max_cycles, completed_cycles + 1),
                previous_cycle_number=active_plan.cycle_number if active_plan is not None else completed_cycles,
                previous_completed_cycles=completed_cycles,
                enforce_max_cycles=False,
                retry_attempted_symbols_by_role=final_retry_attempted,
            )
            resolved_cycle_status = (
                'failed'
                if final_status == 'failed'
                else ('partial' if final_status == 'partial' else 'completed')
            )
            self._sync_runtime_state(
                market_date=config.market_date,
                run_id=request_run_id,
                snapshot=final_snapshot,
                current_role=resume_plan.current_role if resume_plan is not None else None,
                completed_roles=resume_plan.completed_roles if resume_plan is not None else tuple(roles),
                remaining_roles=resume_plan.remaining_roles if resume_plan is not None else (),
                cycle_number=resume_plan.cycle_number if resume_plan is not None else completed_cycles,
                completed_cycles=completed_cycles,
                max_cycles=config.max_cycles,
                cycle_mode=resume_plan.mode if resume_plan is not None else None,
                cycle_status=resolved_cycle_status,
                status=final_status,
                funnel_diagnostics=role_funnel,
            )
            final_state = runtime_feedback_state_store.get_run(config.market_date, run_id=request_run_id, logical=True)
            if final_state is not None:
                final_state.completed_at = datetime.now(timezone.utc)
                final_state.last_activity_at = final_state.completed_at
            if final_state is None:
                raise RuntimeError(f'feedback retry runtime state missing for {request_run_id}')
            agent_context_feedback_run_state_service.finish_run(
                run_id=request_run_id,
                status=final_status,
                completed_count=int(final_state.to_progress_summary().get('completed_feedback_count') or 0),
                failed_count=int(final_state.to_progress_summary().get('failed_items') or 0),
                summary_payload=self._build_run_summary_payload(
                    market_date=config.market_date,
                    request_payload=request_payload,
                    state=final_state,
                    lifecycle_stage='finished',
                    cycle_result={
                        'cycles_completed': len(cycles),
                        'blocked': blocked,
                        'final_pending_symbols': list(final_retry_pending_symbols),
                        'final_boundary_pending_roles': list(final_boundary_pending_roles),
                    },
                ),
            )

        progress = self._get_retry_progress(
            market_date=config.market_date,
            roles=roles,
            symbols=list(requested_symbols) if requested_symbols is not None else None,
            run_id=request_run_id,
        )
        final_pending_symbols = self._collect_retry_pending_symbols(
            self._build_role_sequence_snapshot(
                market_date=config.market_date,
                roles=roles,
                eligible_symbols=scoped_feedback_symbols,
                role_symbol_scopes=role_symbol_scopes,
            ),
            retry_attempted_symbols_by_role=(
                final_state.retry_attempted_symbols_by_role
                if final_state is not None
                else {}
            ),
        )

        return {
            'market_date': config.market_date.isoformat(),
            'roles': list(roles),
            'max_cycles': config.max_cycles,
            'outer_batch_size': config.outer_batch_size,
            'reuse_existing': config.reuse_existing,
            'force_recompute': effective_force_recompute,
            'feedback_scope_mode': feedback_scope_mode,
            'requested_symbols': list(requested_symbols) if requested_symbols is not None else None,
            'eligible_symbols': list(eligible_symbols) if requested_symbols is not None else None,
            'skipped_symbols': skipped_symbols,
            'run_id': request_run_id,
            'status': str(progress.get('summary', {}).get('run_status') or 'completed'),
            'cycles': cycles,
            'final_pending': len(final_pending_symbols),
            'completed': len(final_pending_symbols) == 0,
            'progress': progress,
        }

    def _get_retry_progress(
        self,
        *,
        market_date: date,
        roles: Sequence[str],
        symbols: Sequence[str] | None = None,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        return agent_context_progress_service.get_feedback_progress(
            market_date=market_date,
            symbols=list(symbols) if symbols is not None else None,
            expected_agents=roles,
            run_id=run_id,
        )


agent_feedback_retry_service = AgentFeedbackRetryService()
