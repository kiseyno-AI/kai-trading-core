from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
from math import ceil
from typing import Any, Sequence

from sqlalchemy import select

from app.agent_context.feedback_failure_taxonomy import build_failure_groups
from app.agent_context.feedback_service import _normalize_market_date, _normalize_symbols
from app.agent_context.prerequisite_service import agent_feedback_prerequisite_service
from app.agent_context.run_modes import DAILY_RETRY_CYCLE_MODE
from app.agent_context.run_state_service import agent_context_feedback_run_state_service
from app.agent_context.status_service import EXPECTED_AGENT_NAMES
from app.agent_context.reuse_service import FUNDAMENTAL_REUSE_STALE_REASONS, agent_context_reuse_service
from app.agent_context.runtime_state import _build_top_level_progress_fields, runtime_feedback_state_store
from app.db import session_scope
from app.db_models import AgentContextFeedback, AgentContextFeedbackFailure, DailyPriceBar, Symbol


PROGRESS_SYMBOL_CHUNK_SIZE = 500


def _chunked_symbols(symbols: Sequence[str], size: int = PROGRESS_SYMBOL_CHUNK_SIZE) -> list[list[str]]:
    if size < 1:
        raise ValueError('size must be >= 1')
    return [list(symbols[index:index + size]) for index in range(0, len(symbols), size)]


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
        resolved_run = self._resolve_run_snapshot(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
            expected_agents=expected_agent_names,
            run_id=run_id,
        )
        if expected_agents is None and resolved_run is not None:
            run_expected_agents = self._resolve_run_requested_agents(resolved_run)
            if run_expected_agents:
                expected_agent_names = self._normalize_expected_agents(run_expected_agents)
        state = None
        if run_id is not None:
            state = runtime_feedback_state_store.get_run(normalized_market_date, run_id=run_id)
        elif resolved_run is not None:
            state = runtime_feedback_state_store.get_run(normalized_market_date, run_id=resolved_run['run_id'])
        if state is None and run_id is None and resolved_run is None and normalized_symbols is None:
            state = runtime_feedback_state_store.get_run(normalized_market_date, logical=True)
        prerequisites = agent_feedback_prerequisite_service.inspect_feedback_prerequisites(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
            feedback_run_context=self._build_feedback_run_context_for_prerequisites(
                state=state,
                resolved_run=resolved_run,
            ),
        )

        scoped_symbols, scope_resolved_from_run = self._resolve_progress_scope_symbols(
            requested_symbols=normalized_symbols,
            resolved_run=resolved_run,
            runtime_state=state,
        )
        if scoped_symbols is None and normalized_symbols is None and state is None and resolved_run is None:
            # Progress reads are polled frequently by dashboards.  If there is
            # no live or durable feedback run to report, avoid recomputing the
            # full eligible-symbol universe just to render an empty progress
            # state.
            scoped_symbols = []
        resolved_run_mode = self._resolve_run_mode(resolved_run)
        persisted_child_run_id = self._resolve_persisted_child_run_id(
            resolved_run=resolved_run,
            resolved_run_mode=resolved_run_mode,
        )
        persisted_progress = self._build_persisted_progress(
            market_date=normalized_market_date,
            symbols=scoped_symbols,
            expected_agents=expected_agent_names,
            child_run_id=persisted_child_run_id,
            include_reuse=(
                self._should_include_prior_feedback_reuse(
                    resolved_run=resolved_run,
                    child_run_id=persisted_child_run_id,
                )
                and normalized_symbols is not None
            ),
        )

        result_payload = persisted_progress['result']
        summary_payload = persisted_progress['summary']
        resolved_feedback_scope_mode = (
            self._resolve_feedback_scope_mode(resolved_run)
            or getattr(state, 'feedback_scope_mode', None)
        )
        if state is not None:
            runtime_summary = state.to_progress_summary()
            if resolved_run is not None and resolved_run_mode == DAILY_RETRY_CYCLE_MODE and scope_resolved_from_run:
                summary_payload = self._merge_live_retry_summary_with_persisted_progress(
                    persisted_summary=persisted_progress['summary'],
                    runtime_summary=runtime_summary,
                    run_snapshot=resolved_run,
                )
                result_payload = self._build_result_from_summary(
                    market_date=normalized_market_date,
                    summary=summary_payload,
                    expected_agents=expected_agent_names,
                )
            else:
                summary_payload = runtime_summary
                if resolved_run is not None:
                    summary_payload = self._merge_run_table_metadata(summary=summary_payload, run_snapshot=resolved_run)
                    result_payload = self._build_result_from_summary(
                        market_date=normalized_market_date,
                        summary=summary_payload,
                        expected_agents=expected_agent_names,
                    )
        elif resolved_run is not None:
            if (
                run_id is None
                and not self._is_live_run_snapshot(resolved_run)
                and self._is_persisted_summary_complete(persisted_progress['summary'])
            ):
                summary_payload = persisted_progress['summary']
                result_payload = persisted_progress['result']
            elif (
                self._is_live_run_snapshot(resolved_run)
                and scope_resolved_from_run
            ):
                summary_payload = self._merge_live_run_snapshot_with_persisted_progress(
                    persisted_summary=persisted_progress['summary'],
                    run_snapshot=resolved_run,
                )
                result_payload = self._build_result_from_summary(
                    market_date=normalized_market_date,
                    summary=summary_payload,
                    expected_agents=expected_agent_names,
                )
            else:
                summary_payload = self._build_summary_from_run_snapshot(
                    run_snapshot=resolved_run,
                    fallback_summary=persisted_progress['summary'],
                )
                result_payload = self._build_result_from_summary(
                    market_date=normalized_market_date,
                    summary=summary_payload,
                    expected_agents=expected_agent_names,
                )

        if (
            not prerequisites['ready']
            and state is None
            and resolved_run is None
            and int(summary_payload.get('completed_feedback_count') or 0) == 0
        ):
            summary_payload = {
                **summary_payload,
                'run_id': run_id,
                'run_status': prerequisites['status'],
                'run_table_status': prerequisites['status'],
                'is_live': False,
                'current_role': None,
                'completed_roles': [],
                'remaining_roles': list(expected_agent_names),
                'cycle_status': prerequisites['status'],
                'prerequisites_ready': False,
                'missing_prerequisites': prerequisites['missing_prerequisites'],
            }
        summary_payload = self._sanitize_inactive_terminal_summary(summary_payload)
        summary_payload = self._self_heal_completed_summary(
            summary=summary_payload,
            fallback_summary=persisted_progress['summary'],
        )
        summary_payload = self._normalize_summary_progress_fields(summary_payload)

        return {
            'request': {
                'market_date': normalized_market_date.isoformat(),
                'symbols': normalized_symbols,
                'expected_agents': list(expected_agent_names),
                'run_id': run_id,
                'feedback_scope_mode': resolved_feedback_scope_mode or summary_payload.get('feedback_scope_mode'),
            },
            'result': result_payload,
            'summary': summary_payload,
            'prerequisites': prerequisites,
        }

    def _resolve_progress_scope_symbols(
        self,
        *,
        requested_symbols: Sequence[str] | None,
        resolved_run: dict[str, Any] | None,
        runtime_state: Any,
    ) -> tuple[list[str] | None, bool]:
        if isinstance(resolved_run, dict):
            run_scope_symbols = agent_context_feedback_run_state_service.resolve_run_requested_symbols(
                run_snapshot=resolved_run,
            )
            if run_scope_symbols is not None:
                return run_scope_symbols, True
        if requested_symbols is not None:
            return list(requested_symbols), False
        runtime_symbols = self._normalize_scope_symbols(getattr(runtime_state, 'requested_symbols', None))
        if runtime_symbols is not None:
            return runtime_symbols, resolved_run is not None
        return None, False

    @staticmethod
    def _normalize_scope_symbols(symbols: Any) -> list[str] | None:
        if symbols is None:
            return None
        return _normalize_symbols(symbols)

    @staticmethod
    def _resolve_run_mode(run_snapshot: dict[str, Any] | None) -> str | None:
        if not isinstance(run_snapshot, dict):
            return None
        request_payload = run_snapshot.get('request_payload') if isinstance(run_snapshot.get('request_payload'), dict) else {}
        summary_payload = run_snapshot.get('summary_payload') if isinstance(run_snapshot.get('summary_payload'), dict) else {}
        mode = request_payload.get('mode')
        if mode is None:
            mode = summary_payload.get('mode')
        return str(mode).strip() if mode is not None else None

    def _resolve_persisted_child_run_id(
        self,
        *,
        resolved_run: dict[str, Any] | None,
        resolved_run_mode: str | None,
    ) -> str | None:
        if (
            not isinstance(resolved_run, dict)
            or not self._is_live_run_snapshot(resolved_run)
            or resolved_run_mode == DAILY_RETRY_CYCLE_MODE
        ):
            return None
        run_id = str(resolved_run.get('run_id') or '').strip()
        return run_id or None

    @staticmethod
    def _should_include_prior_feedback_reuse(
        *,
        resolved_run: dict[str, Any] | None,
        child_run_id: str | None,
    ) -> bool:
        if child_run_id is not None:
            return False
        if not isinstance(resolved_run, dict):
            return True
        request_payload = (
            resolved_run.get('request_payload')
            if isinstance(resolved_run.get('request_payload'), dict)
            else {}
        )
        reuse_existing = bool(request_payload.get('reuse_existing', True))
        force_recompute = bool(request_payload.get('force_recompute', False))
        return reuse_existing and not force_recompute

    @staticmethod
    def _resolve_feedback_scope_mode(run_snapshot: dict[str, Any] | None) -> str | None:
        if not isinstance(run_snapshot, dict):
            return None
        payloads: list[dict[str, Any]] = []
        request_payload = run_snapshot.get('request_payload') if isinstance(run_snapshot.get('request_payload'), dict) else None
        summary_payload = run_snapshot.get('summary_payload') if isinstance(run_snapshot.get('summary_payload'), dict) else None
        if request_payload is not None:
            payloads.append(request_payload)
        if summary_payload is not None:
            payloads.append(summary_payload)
            progress_summary = summary_payload.get('progress_summary')
            if isinstance(progress_summary, dict):
                payloads.append(progress_summary)
        for payload in payloads:
            value = payload.get('feedback_scope_mode')
            if value is None:
                continue
            mode = str(value).strip()
            if mode:
                return mode
        return None

    @staticmethod
    def _resolve_run_requested_agents(run_snapshot: dict[str, Any]) -> list[str] | None:
        payloads: list[dict[str, Any]] = []
        request_payload = run_snapshot.get('request_payload') if isinstance(run_snapshot.get('request_payload'), dict) else None
        summary_payload = run_snapshot.get('summary_payload') if isinstance(run_snapshot.get('summary_payload'), dict) else None
        if request_payload is not None:
            payloads.append(request_payload)
        if summary_payload is not None:
            payloads.append(summary_payload)
            progress_summary = summary_payload.get('progress_summary')
            if isinstance(progress_summary, dict):
                payloads.append(progress_summary)
        for payload in payloads:
            roles = payload.get('roles') or payload.get('requested_roles') or payload.get('expected_agents')
            if not isinstance(roles, Sequence) or isinstance(roles, (str, bytes)):
                continue
            normalized = [str(role).strip() for role in roles if str(role).strip()]
            if normalized:
                return normalized
        return None

    @staticmethod
    def _build_feedback_run_context_for_prerequisites(
        *,
        state: Any,
        resolved_run: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if state is not None:
            summary = state.to_progress_summary()
            return {
                'source': 'runtime_feedback_state',
                'run_id': getattr(state, 'run_id', None) or summary.get('run_id'),
                'status': summary.get('run_status') or getattr(state, 'status', None),
                'requested_symbols': AgentContextProgressService._normalize_scope_symbols(
                    getattr(state, 'requested_symbols', None)
                ) or [],
                'summary': summary,
            }
        if resolved_run is None:
            return None
        summary_payload = (
            resolved_run.get('summary_payload')
            if isinstance(resolved_run.get('summary_payload'), dict)
            else {}
        )
        progress_summary = (
            summary_payload.get('progress_summary')
            if isinstance(summary_payload.get('progress_summary'), dict)
            else summary_payload
        )
        summary = dict(progress_summary)
        status = str(summary.get('run_status') or resolved_run.get('status') or '').strip().lower()
        if status:
            summary.setdefault('run_status', status)
        summary.setdefault('run_table_status', resolved_run.get('status'))
        summary.setdefault('completed_feedback_count', resolved_run.get('completed_count'))
        summary.setdefault('failed_items', resolved_run.get('failed_count'))
        requested_symbols = agent_context_feedback_run_state_service.resolve_run_requested_symbols(
            run_snapshot=resolved_run,
        )
        return {
            'source': 'agent_context_feedback_runs',
            'run_id': resolved_run.get('run_id'),
            'status': status or None,
            'requested_symbols': requested_symbols or [],
            'summary': summary,
        }

    def _merge_live_retry_summary_with_persisted_progress(
        self,
        *,
        persisted_summary: dict[str, Any],
        runtime_summary: dict[str, Any],
        run_snapshot: dict[str, Any],
    ) -> dict[str, Any]:
        merged = self._merge_run_table_metadata(summary=dict(persisted_summary), run_snapshot=run_snapshot)
        for key in (
            'run_status',
            'is_live',
            'run_started_at',
            'run_completed_at',
            'current_role',
            'completed_roles',
            'remaining_roles',
            'cycle_number',
            'completed_cycles',
            'max_cycles',
            'cycle_mode',
            'cycle_status',
            'feedback_scope_mode',
            'parallel_per_role',
            'broad_symbol_count',
            'role_symbol_counts',
            'role_symbol_scopes',
            'funnel_diagnostics',
            'seconds_since_last_feedback',
            'expected_feedback_count',
            'buffered_feedback_count',
            'buffered_failure_count',
            'pending_feedback_count',
            'pending_failure_count',
            'flushed_feedback_count',
            'flushed_failure_count',
            'processed_symbols',
            'completed_symbols',
            'in_progress_symbols',
            'partial_symbols',
            'pending_symbols',
        ):
            if runtime_summary.get(key) is not None:
                merged[key] = runtime_summary[key]

        current_role = str(merged.get('current_role') or '').strip()
        role_progress = merged.get('role_progress') if isinstance(merged.get('role_progress'), dict) else {}
        runtime_role_progress = (
            runtime_summary.get('role_progress')
            if isinstance(runtime_summary.get('role_progress'), dict)
            else {}
        )
        if self._is_terminal_summary(runtime_summary) and runtime_role_progress:
            # Final retry state is rebuilt from the parent lineage with exact
            # per-role scopes. Keep its aggregate counts and role counts
            # together instead of mixing them with the union-scope fallback.
            for key in (
                'completed_feedback_count',
                'computed_feedback_count',
                'reused_feedback_count',
                'stale_feedback_count',
                'failed_items',
                'failed_symbols_by_role',
            ):
                if runtime_summary.get(key) is not None:
                    merged[key] = runtime_summary[key]
            merged['role_progress'] = {
                **role_progress,
                **{
                    role: dict(role_summary)
                    for role, role_summary in runtime_role_progress.items()
                    if isinstance(role_summary, dict)
                },
            }
            return merged

        current_role_progress = runtime_role_progress.get(current_role)
        if current_role and isinstance(role_progress.get(current_role), dict):
            updated_role_progress = self._merge_live_retry_current_role_progress(
                persisted_role_progress=role_progress[current_role],
                runtime_role_progress=current_role_progress,
            )
            role_progress = {**role_progress, current_role: updated_role_progress}
            merged['role_progress'] = role_progress
        return merged

    def _merge_live_run_snapshot_with_persisted_progress(
        self,
        *,
        persisted_summary: dict[str, Any],
        run_snapshot: dict[str, Any],
    ) -> dict[str, Any]:
        payload = run_snapshot.get('summary_payload') if isinstance(run_snapshot.get('summary_payload'), dict) else {}
        live_summary = payload.get('progress_summary') if isinstance(payload.get('progress_summary'), dict) else {}
        merged = self._merge_run_table_metadata(summary=dict(persisted_summary), run_snapshot=run_snapshot)
        for key in (
            'run_status',
            'is_live',
            'run_started_at',
            'run_completed_at',
            'current_role',
            'completed_roles',
            'remaining_roles',
            'cycle_number',
            'completed_cycles',
            'max_cycles',
            'cycle_mode',
            'cycle_status',
            'feedback_scope_mode',
            'parallel_per_role',
            'broad_symbol_count',
            'role_symbol_counts',
            'role_symbol_scopes',
            'funnel_diagnostics',
            'seconds_since_last_feedback',
            'expected_feedback_count',
            'buffered_feedback_count',
            'buffered_failure_count',
            'pending_feedback_count',
            'pending_failure_count',
            'flushed_feedback_count',
            'flushed_failure_count',
            'processed_symbols',
            'completed_symbols',
            'in_progress_symbols',
            'partial_symbols',
            'pending_symbols',
        ):
            value = live_summary.get(key)
            if value is None and key in payload:
                value = payload.get(key)
            if value is not None:
                merged[key] = value

        current_role = str(merged.get('current_role') or '').strip()
        role_progress = merged.get('role_progress') if isinstance(merged.get('role_progress'), dict) else {}
        live_role_progress = (
            live_summary.get('role_progress')
            if isinstance(live_summary.get('role_progress'), dict)
            else {}
        )
        current_role_progress = live_role_progress.get(current_role)
        if (
            current_role
            and isinstance(role_progress.get(current_role), dict)
            and self._role_progress_has_live_runtime_signal(current_role_progress)
        ):
            updated_role_progress = self._merge_live_retry_current_role_progress(
                persisted_role_progress=role_progress[current_role],
                runtime_role_progress=current_role_progress,
            )
            role_progress = {**role_progress, current_role: updated_role_progress}
            merged['role_progress'] = role_progress
        return merged

    @staticmethod
    def _role_progress_has_live_runtime_signal(role_progress: Any) -> bool:
        if not isinstance(role_progress, dict):
            return False
        for key in (
            'processed_symbols',
            'completed_symbols',
            'in_progress_symbols',
            'failed_items',
            'terminal_symbols',
            'retry_pending_symbols',
            'first_pass_completed_symbols',
            'first_pass_failed_symbols',
            'retry_queued_symbols',
            'retry_attempted_symbols',
        ):
            try:
                if int(role_progress.get(key) or 0) > 0:
                    return True
            except (TypeError, ValueError):
                continue
        status = str(role_progress.get('status') or '').strip().lower()
        boundary_status = str(role_progress.get('boundary_status') or '').strip().lower()
        return any(value and value != 'pending' for value in (status, boundary_status))

    @staticmethod
    def _is_live_run_snapshot(run_snapshot: dict[str, Any]) -> bool:
        if str(run_snapshot.get('status') or '').strip().lower() == 'running':
            return True
        payload = run_snapshot.get('summary_payload') if isinstance(run_snapshot.get('summary_payload'), dict) else {}
        progress_summary = payload.get('progress_summary') if isinstance(payload.get('progress_summary'), dict) else {}
        run_status = str(progress_summary.get('run_status') or payload.get('run_status') or '').strip().lower()
        return run_status == 'running'

    @staticmethod
    def _is_persisted_summary_complete(summary: dict[str, Any]) -> bool:
        return (
            str(summary.get('run_status') or '').strip().lower() == 'completed'
            and int(summary.get('expected_feedback_count') or 0) > 0
            and int(summary.get('completed_feedback_count') or 0) >= int(summary.get('expected_feedback_count') or 0)
            and int(summary.get('failed_items') or 0) == 0
        )

    @staticmethod
    def _merge_live_retry_current_role_progress(
        *,
        persisted_role_progress: dict[str, Any],
        runtime_role_progress: dict[str, Any] | None,
    ) -> dict[str, Any]:
        merged_role_progress = dict(persisted_role_progress)
        runtime_progress = runtime_role_progress if isinstance(runtime_role_progress, dict) else {}

        total_symbols = max(
            int(merged_role_progress.get('total_symbols') or 0),
            int(runtime_progress.get('total_symbols') or 0),
        )
        completed_symbols = max(
            int(merged_role_progress.get('completed_symbols') or 0),
            int(runtime_progress.get('completed_symbols') or 0),
        )
        failed_items = max(
            int(merged_role_progress.get('failed_items') or 0),
            int(runtime_progress.get('failed_items') or 0),
        )
        terminal_symbols = max(
            int(runtime_progress.get('terminal_symbols') or 0),
            completed_symbols + failed_items,
        )
        in_progress_symbols = max(int(runtime_progress.get('in_progress_symbols') or 0), 0)
        processed_symbols = min(
            total_symbols,
            max(
                int(merged_role_progress.get('processed_symbols') or 0),
                int(runtime_progress.get('processed_symbols') or 0),
                terminal_symbols + in_progress_symbols,
            ),
        )
        pending_symbols = max(0, total_symbols - processed_symbols)
        progress_ratio = (processed_symbols / total_symbols) if total_symbols else 0.0

        for key in (
            'computed_symbols',
            'reused_symbols',
            'stale_symbols',
            'retry_pending_symbols',
            'first_pass_completed_symbols',
            'first_pass_failed_symbols',
            'retry_queued_symbols',
            'retry_attempted_symbols',
        ):
            if key in runtime_progress:
                merged_role_progress[key] = runtime_progress[key]

        merged_role_progress.update(
            {
                'status': runtime_progress.get('status') or 'running',
                'total_symbols': total_symbols,
                'processed_symbols': processed_symbols,
                'completed_symbols': completed_symbols,
                'in_progress_symbols': in_progress_symbols,
                'pending_symbols': pending_symbols,
                'failed_items': failed_items,
                'terminal_symbols': terminal_symbols,
                'progress_ratio': round(progress_ratio, 6),
                'progress_percent': round(progress_ratio * 100, 2),
            }
        )
        if runtime_progress.get('boundary_status') is not None:
            merged_role_progress['boundary_status'] = runtime_progress['boundary_status']
        return merged_role_progress

    @staticmethod
    def _is_terminal_summary(summary: dict[str, Any]) -> bool:
        terminal_statuses = {'completed', 'partial', 'failed', 'skipped', 'cancelled', 'canceled'}
        active_statuses = {'running', 'in_progress'}
        run_table_status = str(summary.get('run_table_status') or '').strip().lower()
        run_status = str(summary.get('run_status') or '').strip().lower()
        if run_table_status in terminal_statuses:
            return True
        if summary.get('run_completed_at') is not None:
            return True
        if run_status in terminal_statuses and run_table_status not in active_statuses:
            return True
        cycle_status = str(summary.get('cycle_status') or '').strip().lower()
        run_is_active = (
            run_table_status in active_statuses
            or run_status in active_statuses
            or summary.get('is_live') is True
        )
        if cycle_status in terminal_statuses and not run_is_active:
            return True
        return False

    @staticmethod
    def _sanitize_inactive_terminal_summary(summary: dict[str, Any]) -> dict[str, Any]:
        if not AgentContextProgressService._is_terminal_summary(summary):
            return summary

        normalized = dict(summary)
        total_symbols = int(normalized.get('total_symbols') or 0)
        completed_symbols = int(normalized.get('completed_symbols') or 0)
        processed_symbols = int(normalized.get('processed_symbols') or 0)
        stale_in_progress_symbols = int(normalized.get('in_progress_symbols') or 0)
        partial_symbols = max(
            int(normalized.get('partial_symbols') or 0),
            stale_in_progress_symbols,
            processed_symbols - completed_symbols,
        )
        if total_symbols > 0:
            partial_symbols = min(partial_symbols, max(0, total_symbols - completed_symbols))
        normalized['is_live'] = False
        normalized['current_role'] = None
        normalized['in_progress_symbols'] = 0
        normalized['partial_symbols'] = max(0, partial_symbols)
        normalized['processed_symbols'] = min(
            total_symbols,
            max(processed_symbols, completed_symbols + normalized['partial_symbols']),
        ) if total_symbols else max(processed_symbols, completed_symbols + normalized['partial_symbols'])
        normalized['pending_symbols'] = max(
            0,
            total_symbols - int(normalized.get('completed_symbols') or 0) - int(normalized.get('partial_symbols') or 0),
        )

        role_progress = normalized.get('role_progress')
        if isinstance(role_progress, dict):
            sanitized_role_progress: dict[str, Any] = {}
            for role, role_summary in role_progress.items():
                if not isinstance(role_summary, dict):
                    sanitized_role_progress[role] = role_summary
                    continue
                role_payload = dict(role_summary)
                role_total = int(role_payload.get('total_symbols') or total_symbols or 0)
                role_completed = int(role_payload.get('completed_symbols') or 0)
                role_processed = int(role_payload.get('processed_symbols') or 0)
                role_failed = int(role_payload.get('failed_items') or 0)
                role_stale_in_progress = int(role_payload.get('in_progress_symbols') or 0)
                role_partial = max(
                    int(role_payload.get('partial_symbols') or 0),
                    role_stale_in_progress,
                    role_processed - role_completed,
                    role_failed,
                )
                if role_total > 0:
                    role_partial = min(role_partial, max(0, role_total - role_completed))
                role_payload['in_progress_symbols'] = 0
                role_payload['partial_symbols'] = max(0, role_partial)
                role_payload['processed_symbols'] = min(
                    role_total,
                    max(role_processed, role_completed + role_payload['partial_symbols']),
                ) if role_total else max(role_processed, role_completed + role_payload['partial_symbols'])
                role_payload['pending_symbols'] = max(
                    0,
                    role_total - role_completed - role_payload['partial_symbols'],
                )
                role_status = str(role_payload.get('status') or '').strip().lower()
                if role_status in {'running', 'in_progress'}:
                    role_payload['status'] = 'partial' if role_payload['processed_symbols'] else 'pending'
                boundary_status = str(role_payload.get('boundary_status') or '').strip().lower()
                if boundary_status in {'running', 'in_progress'}:
                    role_payload['boundary_status'] = role_payload.get('status')
                sanitized_role_progress[role] = role_payload
            normalized['role_progress'] = sanitized_role_progress
        return normalized

    @staticmethod
    def _normalize_summary_progress_fields(summary: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(summary)
        normalized.update(
            _build_top_level_progress_fields(
                processed_symbols=int(normalized.get('processed_symbols') or 0),
                total_symbols=int(normalized.get('total_symbols') or 0),
                completed_feedback_count=int(normalized.get('completed_feedback_count') or 0),
                expected_feedback_count=int(normalized.get('expected_feedback_count') or 0),
                failed_items=int(normalized.get('failed_items') or 0),
            )
        )
        return normalized

    def _build_persisted_progress(
        self,
        *,
        market_date: date,
        symbols: Sequence[str] | None,
        expected_agents: Sequence[str],
        child_run_id: str | None = None,
        include_reuse: bool = True,
    ) -> dict[str, Any]:
        scoped_symbols = list(symbols) if symbols is not None else self._load_market_date_symbols(market_date)
        scoped_symbol_set = set(scoped_symbols)
        completed_by_role: dict[str, set[str]] = {agent_name: set() for agent_name in expected_agents}
        failed_by_role: dict[str, set[str]] = {agent_name: set() for agent_name in expected_agents}
        reused_by_role: dict[str, set[str]] = {agent_name: set() for agent_name in expected_agents}
        stale_by_role: dict[str, set[str]] = {agent_name: set() for agent_name in expected_agents}
        feedback_received_at: list[datetime] = []
        failure_received_at: list[datetime] = []

        if scoped_symbols:
            with session_scope() as session:
                feedback_rows = []
                failure_rows = []
                for symbol_chunk in _chunked_symbols(scoped_symbols):
                    feedback_query = (
                        select(
                            AgentContextFeedback.symbol,
                            AgentContextFeedback.agent_name,
                            AgentContextFeedback.run_id,
                            AgentContextFeedback.received_at,
                        )
                        .where(AgentContextFeedback.market_date == market_date)
                        .where(AgentContextFeedback.symbol.in_(symbol_chunk))
                        .where(AgentContextFeedback.agent_name.in_(expected_agents))
                    )
                    failure_query = (
                        select(
                            AgentContextFeedbackFailure.symbol,
                            AgentContextFeedbackFailure.agent_name,
                            AgentContextFeedbackFailure.run_id,
                            AgentContextFeedbackFailure.received_at,
                            AgentContextFeedbackFailure.failure_kind,
                            AgentContextFeedbackFailure.retryable,
                            AgentContextFeedbackFailure.summary_payload,
                            AgentContextFeedbackFailure.context_payload,
                        )
                        .where(AgentContextFeedbackFailure.market_date == market_date)
                        .where(AgentContextFeedbackFailure.symbol.in_(symbol_chunk))
                        .where(AgentContextFeedbackFailure.agent_name.in_(expected_agents))
                    )
                    if child_run_id is not None:
                        feedback_query = feedback_query.where(AgentContextFeedback.run_id == child_run_id)
                        failure_query = failure_query.where(AgentContextFeedbackFailure.run_id == child_run_id)
                    feedback_rows.extend(session.execute(feedback_query).all())
                    failure_rows.extend(session.execute(failure_query).all())

            for row in feedback_rows:
                symbol = str(row.symbol)
                agent_name = str(row.agent_name)
                if child_run_id is not None and str(getattr(row, 'run_id', '') or '') != child_run_id:
                    continue
                if symbol not in scoped_symbol_set or agent_name not in completed_by_role:
                    continue
                completed_by_role[agent_name].add(symbol)
                if row.received_at is not None:
                    feedback_received_at.append(row.received_at)

            unresolved_failures: list[dict[str, Any]] = []
            for row in failure_rows:
                symbol = str(row.symbol)
                agent_name = str(row.agent_name)
                if child_run_id is not None and str(getattr(row, 'run_id', '') or '') != child_run_id:
                    continue
                if symbol not in scoped_symbol_set or agent_name not in failed_by_role:
                    continue
                if symbol in completed_by_role[agent_name]:
                    continue
                failed_by_role[agent_name].add(symbol)
                if row.received_at is not None:
                    failure_received_at.append(row.received_at)
                unresolved_failures.append(
                    {
                        'symbol': symbol,
                        'agent_name': agent_name,
                        'failure_kind': str(getattr(row, 'failure_kind', '') or 'dispatch_failed'),
                        'retryable': bool(getattr(row, 'retryable', True)),
                        'summary_payload': getattr(row, 'summary_payload', None),
                        'context_payload': getattr(row, 'context_payload', None),
                        'received_at': getattr(row, 'received_at', None),
                    }
                )

            reuse_decisions = {}
            if include_reuse:
                try:
                    reuse_decisions = agent_context_reuse_service.build_latest_decisions(
                        market_date=market_date,
                        symbols=scoped_symbols,
                        roles=expected_agents,
                    )
                except Exception:  # noqa: BLE001
                    reuse_decisions = {}
            for symbol in scoped_symbols:
                for agent_name in expected_agents:
                    if symbol in completed_by_role.get(agent_name, set()) or symbol in failed_by_role.get(agent_name, set()):
                        continue
                    decision = reuse_decisions.get(symbol, {}).get(agent_name)
                    if decision is None:
                        continue
                    if not decision.should_dispatch:
                        completed_by_role[agent_name].add(symbol)
                        reused_by_role[agent_name].add(symbol)
                    elif str(decision.reason or '') in FUNDAMENTAL_REUSE_STALE_REASONS:
                        stale_by_role[agent_name].add(symbol)
        else:
            unresolved_failures = []

        first_feedback_at = min(feedback_received_at) if feedback_received_at else None
        latest_feedback_at = max(feedback_received_at) if feedback_received_at else None
        first_failed_feedback_at = min(failure_received_at) if failure_received_at else None
        latest_failed_feedback_at = max(failure_received_at) if failure_received_at else None
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
                reused_by_role=reused_by_role,
                stale_by_role=stale_by_role,
                first_feedback_at=first_feedback_at,
                latest_feedback_at=latest_feedback_at,
                first_failed_feedback_at=first_failed_feedback_at,
                latest_failed_feedback_at=latest_failed_feedback_at,
                unresolved_failures=unresolved_failures,
            ),
        }

    def _resolve_run_snapshot(
        self,
        *,
        market_date: date,
        symbols: Sequence[str] | None,
        expected_agents: Sequence[str],
        run_id: str | None,
    ) -> dict[str, Any] | None:
        if run_id is not None:
            return agent_context_feedback_run_state_service.get_run(run_id=run_id)
        return agent_context_feedback_run_state_service.find_latest_run(
            market_date=market_date,
            requested_symbols=list(symbols) if symbols is not None else None,
            requested_roles=list(expected_agents),
            request_modes=[DAILY_RETRY_CYCLE_MODE] if symbols is None else None,
        )

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
    def _build_result_from_summary(
        *,
        market_date: date,
        summary: dict[str, Any],
        expected_agents: Sequence[str],
    ) -> dict[str, Any]:
        role_progress = summary.get('role_progress') if isinstance(summary.get('role_progress'), dict) else {}
        total_symbols = int(summary.get('total_symbols') or 0)
        roles_payload: dict[str, dict[str, int]] = {}
        for agent_name in expected_agents:
            role_summary = role_progress.get(agent_name) if isinstance(role_progress.get(agent_name), dict) else {}
            roles_payload[agent_name] = {
                'requested_count': int(role_summary.get('total_symbols') or total_symbols),
                'completed_count': int(role_summary.get('completed_symbols') or 0),
                'error_count': int(role_summary.get('failed_items') or 0),
            }
        return {
            'market_date': market_date.isoformat(),
            'roles': roles_payload,
        }

    def _build_summary_from_run_snapshot(
        self,
        *,
        run_snapshot: dict[str, Any],
        fallback_summary: dict[str, Any],
    ) -> dict[str, Any]:
        payload = run_snapshot.get('summary_payload') if isinstance(run_snapshot.get('summary_payload'), dict) else {}
        progress_summary = payload.get('progress_summary') if isinstance(payload.get('progress_summary'), dict) else None
        if progress_summary is not None:
            summary = dict(progress_summary)
        else:
            summary = dict(fallback_summary)
            summary['failed_items'] = int(run_snapshot.get('failed_count') or summary.get('failed_items') or 0)
            summary['completed_feedback_count'] = int(
                run_snapshot.get('completed_count') or summary.get('completed_feedback_count') or 0
            )
            summary['current_role'] = payload.get('current_role')
            summary['parallel_per_role'] = payload.get('parallel_per_role')
        for key in (
            'buffered_feedback_count',
            'buffered_failure_count',
            'pending_feedback_count',
            'pending_failure_count',
            'flushed_feedback_count',
            'flushed_failure_count',
        ):
            if key in payload and key not in summary:
                summary[key] = payload[key]
        merged = self._merge_run_table_metadata(summary=summary, run_snapshot=run_snapshot)
        return self._self_heal_completed_summary(summary=merged, fallback_summary=fallback_summary)

    @staticmethod
    def _merge_run_table_metadata(*, summary: dict[str, Any], run_snapshot: dict[str, Any]) -> dict[str, Any]:
        merged = dict(summary)
        request_payload = run_snapshot.get('request_payload') if isinstance(run_snapshot.get('request_payload'), dict) else {}
        started_at = run_snapshot.get('started_at')
        completed_at = run_snapshot.get('completed_at')
        merged['run_id'] = run_snapshot.get('run_id')
        if merged.get('feedback_scope_mode') is None and request_payload.get('feedback_scope_mode') is not None:
            merged['feedback_scope_mode'] = request_payload.get('feedback_scope_mode')
        merged['run_status'] = merged.get('run_status') or run_snapshot.get('status')
        merged['run_table_status'] = run_snapshot.get('status')
        merged['is_live'] = merged.get('is_live') if merged.get('is_live') is not None else run_snapshot.get('status') == 'running'
        merged['run_started_at'] = merged.get('run_started_at') or (started_at.isoformat() if started_at else None)
        merged['run_completed_at'] = merged.get('run_completed_at') or (completed_at.isoformat() if completed_at else None)
        return merged

    @staticmethod
    def _self_heal_completed_summary(*, summary: dict[str, Any], fallback_summary: dict[str, Any]) -> dict[str, Any]:
        merged = dict(summary)
        completed_feedback_count = int(
            merged.get('completed_feedback_count') or fallback_summary.get('completed_feedback_count') or 0
        )
        expected_feedback_count = int(
            merged.get('expected_feedback_count') or fallback_summary.get('expected_feedback_count') or 0
        )
        failed_items = int(merged.get('failed_items') or fallback_summary.get('failed_items') or 0)
        all_feedback_completed = expected_feedback_count > 0 and completed_feedback_count >= expected_feedback_count and failed_items == 0
        no_work_remaining = (
            int(merged.get('pending_symbols') or 0) == 0
            and int(merged.get('in_progress_symbols') or 0) == 0
        )
        if not all_feedback_completed or not no_work_remaining:
            return merged

        merged['run_status'] = 'completed'
        merged['run_table_status'] = 'completed'
        merged['is_live'] = False
        merged['current_role'] = None
        merged['completed_feedback_count'] = completed_feedback_count
        merged['expected_feedback_count'] = expected_feedback_count
        merged['failed_items'] = 0
        merged['progress_ratio'] = 1.0
        merged['progress_percent'] = 100.0
        if no_work_remaining:
            merged['processed_symbols'] = merged.get('total_symbols')
            merged['completed_symbols'] = merged.get('total_symbols')
        role_progress = merged.get('role_progress')
        if isinstance(role_progress, dict):
            for role_summary in role_progress.values():
                if not isinstance(role_summary, dict):
                    continue
                total_symbols = int(role_summary.get('total_symbols') or 0)
                completed_symbols = int(role_summary.get('completed_symbols') or 0)
                failed_for_role = int(role_summary.get('failed_items') or 0)
                if total_symbols > 0 and completed_symbols >= total_symbols and failed_for_role == 0:
                    role_summary['status'] = 'completed'
                    role_summary['boundary_status'] = 'completed'
                    role_summary['processed_symbols'] = total_symbols
                    role_summary['completed_symbols'] = total_symbols
                    role_summary['pending_symbols'] = 0
                    role_summary['in_progress_symbols'] = 0
                    role_summary['progress_ratio'] = 1.0
                    role_summary['progress_percent'] = 100.0
        if isinstance(merged.get('role_progress'), dict):
            completed_roles = [
                role
                for role, role_summary in merged['role_progress'].items()
                if isinstance(role_summary, dict) and role_summary.get('status') == 'completed'
            ]
            remaining_roles = [
                role
                for role, role_summary in merged['role_progress'].items()
                if not (isinstance(role_summary, dict) and role_summary.get('status') == 'completed')
            ]
            merged['completed_roles'] = completed_roles
            merged['remaining_roles'] = remaining_roles
        return merged

    @staticmethod
    def _build_persisted_summary(
        *,
        requested_symbols: Sequence[str],
        expected_agents: Sequence[str],
        completed_by_role: dict[str, set[str]],
        failed_by_role: dict[str, set[str]],
        reused_by_role: dict[str, set[str]],
        stale_by_role: dict[str, set[str]],
        first_feedback_at: datetime | None,
        latest_feedback_at: datetime | None,
        first_failed_feedback_at: datetime | None,
        latest_failed_feedback_at: datetime | None,
        unresolved_failures: Sequence[dict[str, Any]],
    ) -> dict[str, Any]:
        requested_count = len(requested_symbols)
        expected_feedback_count = requested_count * len(expected_agents)
        completed_feedback_count = sum(len(completed_by_role.get(agent_name, set())) for agent_name in expected_agents)
        reused_feedback_count = sum(len(reused_by_role.get(agent_name, set())) for agent_name in expected_agents)
        computed_feedback_count = max(0, completed_feedback_count - reused_feedback_count)
        stale_feedback_count = sum(len(stale_by_role.get(agent_name, set())) for agent_name in expected_agents)
        failed_items = sum(len(failed_by_role.get(agent_name, set())) for agent_name in expected_agents)
        failure_groups = build_failure_groups(unresolved_failures)

        completed_roles_by_symbol: dict[str, set[str]] = defaultdict(set)
        failed_roles_by_symbol: dict[str, set[str]] = defaultdict(set)
        for agent_name, symbols in completed_by_role.items():
            for symbol in symbols:
                completed_roles_by_symbol[symbol].add(agent_name)
        for agent_name, symbols in failed_by_role.items():
            for symbol in symbols:
                failed_roles_by_symbol[symbol].add(agent_name)

        completed_symbols = 0
        partial_symbols = 0
        for symbol in requested_symbols:
            completed_roles = completed_roles_by_symbol.get(symbol, set())
            failed_roles = failed_roles_by_symbol.get(symbol, set())
            if len(completed_roles) == len(expected_agents) and expected_agents:
                completed_symbols += 1
            elif completed_roles or failed_roles:
                partial_symbols += 1
        in_progress_symbols = 0
        pending_symbols = max(0, requested_count - completed_symbols - partial_symbols)
        processed_symbols = completed_symbols + partial_symbols
        progress_fields = _build_top_level_progress_fields(
            processed_symbols=processed_symbols,
            total_symbols=requested_count,
            completed_feedback_count=completed_feedback_count,
            expected_feedback_count=expected_feedback_count,
            failed_items=failed_items,
        )

        role_progress: dict[str, dict[str, Any]] = {}
        completed_roles: list[str] = []
        remaining_roles: list[str] = []
        for agent_name in expected_agents:
            completed_for_role = len(completed_by_role.get(agent_name, set()))
            failed_for_role = len(failed_by_role.get(agent_name, set()))
            reused_for_role = len(reused_by_role.get(agent_name, set()))
            computed_for_role = max(0, completed_for_role - reused_for_role)
            stale_for_role = len(stale_by_role.get(agent_name, set()))
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
            if role_status == 'completed':
                completed_roles.append(agent_name)
            else:
                remaining_roles.append(agent_name)
            role_progress[agent_name] = {
                'role': agent_name,
                'status': role_status,
                'total_symbols': requested_count,
                'processed_symbols': processed_for_role,
                'completed_symbols': completed_for_role,
                'computed_symbols': computed_for_role,
                'reused_symbols': reused_for_role,
                'stale_symbols': stale_for_role,
                'in_progress_symbols': 0,
                'partial_symbols': max(0, processed_for_role - completed_for_role),
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
            remaining_feedback_count = max(0, expected_feedback_count - progress_fields['terminal_feedback_count'])
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
            'first_failed_feedback_at': first_failed_feedback_at.isoformat() if first_failed_feedback_at else None,
            'latest_failed_feedback_at': latest_failed_feedback_at.isoformat() if latest_failed_feedback_at else None,
            'elapsed_seconds': elapsed_seconds,
            'seconds_since_last_feedback': seconds_since_last_feedback,
            'stalled_threshold_seconds': 300,
            'total_symbols': requested_count,
            'processed_symbols': processed_symbols,
            'completed_symbols': completed_symbols,
            'in_progress_symbols': in_progress_symbols,
            'partial_symbols': partial_symbols,
            'pending_symbols': pending_symbols,
            'terminal_feedback_count': progress_fields['terminal_feedback_count'],
            'boundary_progress_ratio': progress_fields['boundary_progress_ratio'],
            'boundary_progress_percent': progress_fields['boundary_progress_percent'],
            'progress_ratio': progress_fields['progress_ratio'],
            'progress_percent': progress_fields['progress_percent'],
            'failed_items': failed_items,
            'failure_groups': failure_groups,
            'completed_feedback_count': completed_feedback_count,
            'computed_feedback_count': computed_feedback_count,
            'reused_feedback_count': reused_feedback_count,
            'stale_feedback_count': stale_feedback_count,
            'expected_feedback_count': expected_feedback_count,
            'completed_feedback_per_minute': completed_feedback_per_minute,
            'completed_symbols_per_minute': completed_symbols_per_minute,
            'estimated_remaining_seconds': estimated_remaining_seconds,
            'estimated_completion_at': estimated_completion_at,
            'eta_confidence': eta_confidence,
            'role_progress': role_progress,
            'completed_roles': completed_roles,
            'remaining_roles': remaining_roles,
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
