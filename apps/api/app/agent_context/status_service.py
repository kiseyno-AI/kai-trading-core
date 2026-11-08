from __future__ import annotations

from datetime import date
from typing import Any, Sequence

from sqlalchemy import select

from app.agent_context.feedback_failure_taxonomy import build_failure_groups, classify_feedback_failure
from app.agent_context.roles import DEFAULT_AGENT_NAMES
from app.agent_context.prerequisite_service import agent_feedback_prerequisite_service
from app.agent_context.feedback_service import _normalize_market_date, _normalize_symbols
from app.agent_context.run_modes import DAILY_RETRY_CYCLE_MODE
from app.agent_context.run_state_service import agent_context_feedback_run_state_service
from app.agent_context.reuse_service import FUNDAMENTAL_REUSE_STALE_REASONS, agent_context_reuse_service
from app.db import session_scope
from app.db_models import AgentContextFeedback, AgentContextFeedbackFailure


EXPECTED_AGENT_NAMES = DEFAULT_AGENT_NAMES
EXPECTED_AGENT_SET = set(EXPECTED_AGENT_NAMES)
FEEDBACK_STATUS_SYMBOL_CHUNK_SIZE = 500


def _chunked_symbols(symbols: Sequence[str], size: int = FEEDBACK_STATUS_SYMBOL_CHUNK_SIZE) -> list[list[str]]:
    if size < 1:
        raise ValueError('size must be >= 1')
    return [list(symbols[index:index + size]) for index in range(0, len(symbols), size)]


def _group_feedback_rows(rows: Sequence[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row['symbol']), []).append(row)
    return grouped


def _group_latest_failures(rows: Sequence[dict[str, Any]]) -> dict[str, dict[str, dict[str, Any]]]:
    grouped: dict[str, dict[str, dict[str, Any]]] = {}
    for row in rows:
        symbol = str(row['symbol'])
        agent_name = str(row['agent_name'])
        grouped.setdefault(symbol, {})
        if agent_name not in grouped[symbol]:
            grouped[symbol][agent_name] = row
    return grouped


class AgentContextStatusService:
    @staticmethod
    def _feedback_scope_mode_from_run(run_snapshot: dict[str, Any] | None) -> str | None:
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

    def get_feedback_status(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str],
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbols = _normalize_symbols(symbols)
        if not normalized_symbols:
            raise ValueError('symbols is required')
        prerequisites = agent_feedback_prerequisite_service.inspect_feedback_prerequisites(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
        )
        try:
            latest_run = agent_context_feedback_run_state_service.find_latest_run(
                market_date=normalized_market_date,
                requested_symbols=None,
                requested_roles=list(EXPECTED_AGENT_NAMES),
                request_modes=[DAILY_RETRY_CYCLE_MODE],
            )
        except Exception:  # noqa: BLE001
            latest_run = None
        latest_run_scope_mode = self._feedback_scope_mode_from_run(latest_run)

        with session_scope() as session:
            feedback_rows = []
            orm_failure_rows = []
            for symbol_chunk in _chunked_symbols(normalized_symbols):
                feedback_rows.extend(
                    session.execute(
                        select(
                            AgentContextFeedback.symbol,
                            AgentContextFeedback.agent_name,
                            AgentContextFeedback.run_id,
                            AgentContextFeedback.received_at,
                        )
                        .where(AgentContextFeedback.market_date == normalized_market_date)
                        .where(AgentContextFeedback.symbol.in_(symbol_chunk))
                        .order_by(
                            AgentContextFeedback.symbol.asc(),
                            AgentContextFeedback.received_at.desc(),
                            AgentContextFeedback.id.desc(),
                        )
                    ).all()
                )
                orm_failure_rows.extend(
                    session.execute(
                        select(AgentContextFeedbackFailure)
                        .where(AgentContextFeedbackFailure.market_date == normalized_market_date)
                        .where(AgentContextFeedbackFailure.symbol.in_(symbol_chunk))
                        .where(AgentContextFeedbackFailure.agent_name.in_(EXPECTED_AGENT_NAMES))
                        .order_by(
                            AgentContextFeedbackFailure.symbol.asc(),
                            AgentContextFeedbackFailure.agent_name.asc(),
                            AgentContextFeedbackFailure.received_at.desc(),
                            AgentContextFeedbackFailure.id.desc(),
                        )
                    ).scalars().all()
                )
            rows = [
                {
                    'symbol': row.symbol,
                    'agent_name': row.agent_name,
                    'run_id': row.run_id,
                    'received_at': row.received_at.isoformat() if row.received_at else None,
                }
                for row in feedback_rows
            ]
            failure_rows = []
            for row in orm_failure_rows:
                summary = row.summary_payload if isinstance(row.summary_payload, dict) else {}
                context = row.context_payload if isinstance(row.context_payload, dict) else {}
                classification = classify_feedback_failure(
                    failure_kind=row.failure_kind,
                    summary=summary,
                    context=context,
                    error=str(summary.get('error') or '') or None,
                    exception_type=str(summary.get('exception_type') or '') or None,
                )
                failure_rows.append(
                    {
                        'symbol': row.symbol,
                        'agent_name': row.agent_name,
                        'run_id': row.run_id,
                        'failure_kind': row.failure_kind,
                        'failure_status': classification.failure_status,
                        'failure_category': classification.failure_category,
                        'retryable': row.retryable,
                        'received_at': row.received_at.isoformat() if row.received_at else None,
                        'summary': summary,
                        'context': context,
                    }
                )

        grouped = _group_feedback_rows(rows)
        failures_by_symbol = _group_latest_failures(failure_rows)
        try:
            reuse_decisions = agent_context_reuse_service.build_latest_decisions(
                market_date=normalized_market_date,
                symbols=normalized_symbols,
                roles=list(EXPECTED_AGENT_NAMES),
            )
        except Exception:  # noqa: BLE001
            reuse_decisions = {}
        items: list[dict[str, Any]] = []
        completed_symbols = 0
        in_progress_symbols = 0
        partial_symbols = 0
        failed_symbols = 0
        pending_symbols = 0
        reused_feedback_count = 0
        stale_feedback_count = 0
        unresolved_failures: list[dict[str, Any]] = []

        for symbol in normalized_symbols:
            symbol_rows = grouped.get(symbol, [])
            latest_run_id = symbol_rows[0]['run_id'] if symbol_rows else None
            run_rows = [row for row in symbol_rows if row['run_id'] == latest_run_id] if latest_run_id else []

            agent_map: dict[str, dict[str, Any]] = {}
            for row in run_rows:
                if row['agent_name'] in EXPECTED_AGENT_SET and row['agent_name'] not in agent_map:
                    agent_map[row['agent_name']] = row

            computed_agents = sorted(agent_map.keys())
            reused_agent_map = {
                agent_name: decision
                for agent_name, decision in reuse_decisions.get(symbol, {}).items()
                if agent_name not in agent_map and not decision.should_dispatch
            }
            reused_agents = sorted(reused_agent_map.keys())
            stale_agent_map = {
                agent_name: decision
                for agent_name, decision in reuse_decisions.get(symbol, {}).items()
                if agent_name not in agent_map
                and decision.should_dispatch
                and str(decision.reason or '') in FUNDAMENTAL_REUSE_STALE_REASONS
            }
            stale_agents = sorted(stale_agent_map.keys())
            completed_agents = sorted(set(computed_agents) | set(reused_agents))
            reused_feedback_count += len(reused_agents)
            stale_feedback_count += len(stale_agents)
            symbol_failure_map = {
                agent_name: failure
                for agent_name, failure in failures_by_symbol.get(symbol, {}).items()
                if agent_name not in agent_map
                and agent_name not in reused_agent_map
            }
            failed_agents = sorted(symbol_failure_map.keys())
            symbol_failures = [symbol_failure_map[agent_name] for agent_name in failed_agents]
            unresolved_failures.extend(symbol_failures)
            pending_agents = [
                name
                for name in EXPECTED_AGENT_NAMES
                if name not in agent_map and name not in symbol_failure_map
                and name not in reused_agent_map
            ]
            if len(completed_agents) == len(EXPECTED_AGENT_NAMES):
                status = 'completed'
                completed_symbols += 1
            elif completed_agents or failed_agents:
                if completed_agents:
                    status = 'partial'
                    partial_symbols += 1
                else:
                    status = 'failed'
                    failed_symbols += 1
            else:
                status = prerequisites['status'] if not prerequisites['ready'] else 'pending'
                pending_symbols += 1

            items.append(
                {
                    'symbol': symbol,
                    'status': status,
                    'latest_run_id': latest_run_id,
                    'completed_agents': completed_agents,
                    'computed_agents': computed_agents,
                    'reused_agents': reused_agents,
                    'stale_agents': stale_agents,
                    'failed_agents': failed_agents,
                    'pending_agents': pending_agents,
                    'completed_count': len(completed_agents),
                    'computed_count': len(computed_agents),
                    'reused_count': len(reused_agents),
                    'stale_count': len(stale_agents),
                    'failed_count': len(failed_agents),
                    'expected_count': len(EXPECTED_AGENT_NAMES),
                    'reuse': {
                        agent_name: {
                            'reason': decision.reason,
                            'latest_feedback_id': decision.latest_feedback_id,
                            'latest_feedback_market_date': (
                                decision.latest_feedback.get('market_date')
                                if isinstance(decision.latest_feedback, dict)
                                else None
                            ),
                            'latest_feedback_received_at': decision.latest_feedback_received_at,
                            'input_fingerprint': decision.input_fingerprint,
                            'stored_input_fingerprint': decision.stored_input_fingerprint,
                            'reuse_policy_version': decision.reuse_policy_version,
                        }
                        for agent_name, decision in reused_agent_map.items()
                    },
                    'stale_feedback': {
                        agent_name: {
                            'reason': decision.reason,
                            'latest_feedback_id': decision.latest_feedback_id,
                            'latest_feedback_received_at': decision.latest_feedback_received_at,
                            'input_fingerprint': decision.input_fingerprint,
                            'stored_input_fingerprint': decision.stored_input_fingerprint,
                            'reuse_policy_version': decision.reuse_policy_version,
                        }
                        for agent_name, decision in stale_agent_map.items()
                    },
                    'failures': [
                        {
                            'agent_name': failure['agent_name'],
                            'run_id': failure['run_id'],
                            'failure_kind': failure['failure_kind'],
                            'failure_status': failure['failure_status'],
                            'failure_category': failure['failure_category'],
                            'retryable': failure['retryable'],
                            'received_at': failure['received_at'],
                        }
                        for failure in symbol_failures
                    ],
                    'failure_groups': build_failure_groups(symbol_failures),
                    'prerequisites_ready': prerequisites['ready'],
                    'missing_prerequisites': prerequisites['missing_prerequisites'],
                }
            )

        overall_status = 'completed'
        if in_progress_symbols:
            overall_status = 'in_progress'
        elif partial_symbols:
            overall_status = 'partial'
        elif failed_symbols:
            overall_status = 'failed'
        elif not prerequisites['ready'] and completed_symbols == 0:
            overall_status = prerequisites['status']
        elif pending_symbols and completed_symbols == 0:
            overall_status = 'pending'
        elif pending_symbols:
            overall_status = 'partial'

        return {
            'request': {
                'market_date': normalized_market_date.isoformat(),
                'symbols': normalized_symbols,
                'expected_agents': list(EXPECTED_AGENT_NAMES),
                'latest_run_id': latest_run.get('run_id') if isinstance(latest_run, dict) else None,
                'feedback_scope_mode': latest_run_scope_mode,
            },
            'summary': {
                'status': overall_status,
                'latest_run_id': latest_run.get('run_id') if isinstance(latest_run, dict) else None,
                'feedback_scope_mode': latest_run_scope_mode,
                'requested_symbol_count': len(normalized_symbols),
                'completed_symbol_count': completed_symbols,
                'in_progress_symbol_count': in_progress_symbols,
                'partial_symbol_count': partial_symbols,
                'failed_symbol_count': failed_symbols,
                'pending_symbol_count': pending_symbols,
                'completed_feedback_count': len(rows),
                'computed_feedback_count': len(rows),
                'reused_feedback_count': reused_feedback_count,
                'stale_feedback_count': stale_feedback_count,
                'failed_feedback_count': len(unresolved_failures),
                'expected_feedback_count': len(normalized_symbols) * len(EXPECTED_AGENT_NAMES),
                'failure_groups': build_failure_groups(unresolved_failures),
            },
            'items': items,
            'prerequisites': prerequisites,
        }


agent_context_status_service = AgentContextStatusService()
