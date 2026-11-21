"""Read-only checks for legacy/scheduled terminal publications, never execution admission."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.orm import load_only

from app.agent_context.response_contract import normalize_agent_response
from app.agent_context.model_identity import apply_decider_model_identity_fence, expected_decider_model
from app.agent_context.reuse_service import (
    _authoritative_summary_reuse_evidence,
    _request_payload_knowledge_cutoff_at,
)
from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES, SUMMARIZER_AGENT_NAMES
from app.agent_context.retry_service import _exact_completed_summary_scope, resolve_terminal_role_success_intersection
from app.agent_context.run_modes import DAILY_RETRY_CYCLE_MODE
from app.agent_context.signal_decider_loop_service import (
    _normalize_action, _normalize_optional_text, _normalize_short_term_action, _short_term_generic_action, _to_int,
)
from app.db import session_scope
from app.core.settings import settings
from app.core.numbers import finite_float
from app.market.symbol_codes import is_canonical_krx_short_code
from app.db_models import AgentContextFeedback, AgentContextFeedbackRun, SignalDeciderDecisionSnapshot, SignalDeciderExecutionPlanItem


def validate_feedback_publications(
    *, market_date: str, result: dict[str, Any], source_summary_run_id: str | None = None,
) -> dict[str, Any]:
    """Bind published rows to a completed run or a proven terminal summary intersection."""
    invalid = {'valid': False, 'decision_identities': []}
    try:
        market_day = date.fromisoformat(market_date)
        cutoff = datetime.fromisoformat(str(result.get('knowledge_cutoff_at')))
        if cutoff.tzinfo is None or cutoff.utcoffset() is None:
            return invalid
        cutoff = cutoff.astimezone(timezone.utc)
        roles = result.get('roles')
        if not isinstance(roles, list) or len(roles) != len(set(roles)):
            return invalid
        is_decider = set(roles) == set(DECISION_SUPPORT_AGENT_NAMES)
        if not is_decider and set(roles) != set(SUMMARIZER_AGENT_NAMES):
            return invalid
        summary = result['progress']['summary']
        scopes = summary['role_symbol_scopes']
        if not isinstance(scopes, dict) or set(scopes) != set(roles):
            return invalid
        admitted = {(symbol, role) for role, symbols in scopes.items() for symbol in symbols}
        if not admitted or any(
            not isinstance(symbols, list) or len(symbols) != len(set(symbols))
            or any(not isinstance(s, str) or not is_canonical_krx_short_code(s) for s in symbols)
            for symbols in scopes.values()
        ):
            return invalid
        run_id = result['run_id']
        partial_summary = not is_decider and result.get('status') == 'partial'
        terminal_scope = None
        if partial_summary:
            terminal_scope = resolve_terminal_role_success_intersection(
                progress_summary=summary, roles=roles, expected_role_symbol_scopes=scopes,
            )
            if terminal_scope.get('ready') is not True or summary.get('run_id') != run_id:
                return invalid
            expected = {(symbol, role) for symbol in terminal_scope['completed_symbols'] for role in roles}
        else:
            expected = admitted
        if (result.get('market_date') != market_date or not partial_summary
                and (result.get('status') != 'completed' or result.get('completed') is not True)):
            return invalid
        if 'final_pending' in result and (type(result['final_pending']) is not int or result['final_pending'] != 0):
            return invalid
        with session_scope(statement_timeout_ms=10000, lock_timeout_ms=1000) as session:
            session.execute(text('SET TRANSACTION READ ONLY'))
            run = session.execute(select(AgentContextFeedbackRun).where(
                AgentContextFeedbackRun.run_id == run_id,
            )).scalar_one_or_none()
            if run is None:
                return invalid
            request = run.request_payload or {}
            if not (
                run.market_date == market_day and run.status == ('partial' if partial_summary else 'completed')
                and run.completed_at is not None
                and type(run.symbols_total) is int and run.symbols_total == len(admitted)
                and type(run.completed_count) is int and type(run.failed_count) is int
                and run.completed_count == (terminal_scope['completed_items'] if partial_summary else len(expected))
                and run.failed_count == (terminal_scope['failed_items'] if partial_summary else 0)
                and request.get('mode') == DAILY_RETRY_CYCLE_MODE
                and str(request.get('market_date'))[:10] == market_date
                and set(request.get('roles') or []) == set(roles)
                and len(request.get('roles') or []) == len(roles)
                and request.get('feedback_scope_mode') == result.get('feedback_scope_mode')
                and request.get('role_symbol_scopes') == scopes
                and _request_payload_knowledge_cutoff_at(request) == cutoff
                and (not is_decider or request.get('source_summary_run_id') == source_summary_run_id)
            ):
                return invalid
            if partial_summary:
                dispatched_scope = _exact_completed_summary_scope(request)
                persisted_scope = resolve_terminal_role_success_intersection(
                    progress_summary=(run.summary_payload or {}).get('progress_summary') or {},
                    roles=roles, expected_role_symbol_scopes=scopes,
                )
                summary_admitted = request.get('requested_symbols')
                if (dispatched_scope is None or persisted_scope != terminal_scope
                        or not isinstance(summary_admitted, list) or not summary_admitted
                        or len(summary_admitted) != len(set(summary_admitted))
                        or any(not isinstance(s, str) or not is_canonical_krx_short_code(s) for s in summary_admitted)
                        or not set(terminal_scope['admitted_symbols']) <= set(summary_admitted)):
                    return invalid
            symbols = sorted({symbol for symbol, _ in expected})
            rows = session.execute(select(AgentContextFeedback).where(
                AgentContextFeedback.market_date == market_day,
                AgentContextFeedback.agent_name.in_(roles),
                AgentContextFeedback.symbol.in_(symbols),
            ).options(load_only(
                AgentContextFeedback.id, AgentContextFeedback.symbol,
                AgentContextFeedback.agent_name, AgentContextFeedback.run_id,
                AgentContextFeedback.received_at, AgentContextFeedback.summary_payload,
                AgentContextFeedback.normalized_feedback_payload,
            ))).scalars().all()
            rows_by_key = {(row.symbol, row.agent_name): row for row in rows}
            if not expected <= rows_by_key.keys():
                return invalid
            child_ids = {rows_by_key[key].run_id for key in expected}
            children = session.execute(select(AgentContextFeedbackRun).where(
                AgentContextFeedbackRun.run_id.in_(child_ids),
            )).scalars().all()
            child_by_id = {child.run_id: child for child in children}
            reused = _authoritative_summary_reuse_evidence(
                run=run, market_date=market_day, symbols=symbols,
                roles=roles, knowledge_cutoff_at=cutoff, fs_div=request.get('fs_div') or 'CFS',
            )
            normalized_by_key = {}
            for key in expected:
                row = rows_by_key[key]
                child = child_by_id.get(row.run_id)
                child_request = (child.request_payload or {}) if child else {}
                reuse = reused.get(key) or {}
                # A terminal partial batch can still publish this independently validated row.
                if not (
                    child and child.market_date == market_day
                    and child.status in {'completed', 'partial'} and child.completed_at is not None
                    and _request_payload_knowledge_cutoff_at(child_request) == cutoff
                ):
                    return invalid
                direct = child_request.get('parent_run_id') == run_id
                if not direct and not (
                    reuse.get('feedback_id') == row.id
                    and reuse.get('feedback_run_id') == row.run_id
                    and datetime.fromisoformat(str(reuse.get('feedback_received_at'))) == row.received_at
                ):
                    return invalid
                payload = row.normalized_feedback_payload
                if not isinstance(payload, dict) or payload.get('response_contract_valid') is not True:
                    return invalid
                normalized = normalize_agent_response(
                    role=row.agent_name, output_text=json.dumps(payload), raw_response={},
                    market_date=market_day, symbol=row.symbol, agent_name=row.agent_name,
                )
                if normalized.get('response_contract_valid') is not True:
                    return invalid
                if is_decider:
                    provenance = (row.summary_payload or {}).get('decision_provenance') or {}
                    normalized = apply_decider_model_identity_fence(
                        role=row.agent_name, normalized_feedback=normalized,
                        model_identity=payload.get('model_identity') or provenance.get('model_identity'),
                        expected_model=expected_decider_model(row.agent_name, settings_obj=settings),
                        runtime_contract=payload.get('runtime_contract') or provenance.get('runtime_contract'),
                    )
                    if normalized.get('response_contract_valid') is not True:
                        return invalid
                normalized_by_key[key] = normalized
            identities = []
            if is_decider:
                snapshots = session.execute(select(SignalDeciderDecisionSnapshot).where(
                    SignalDeciderDecisionSnapshot.market_date == market_day,
                    SignalDeciderDecisionSnapshot.feedback_id.in_([rows_by_key[k].id for k in expected]),
                ).options(load_only(
                    SignalDeciderDecisionSnapshot.id, SignalDeciderDecisionSnapshot.feedback_id,
                    SignalDeciderDecisionSnapshot.symbol, SignalDeciderDecisionSnapshot.agent_name,
                    SignalDeciderDecisionSnapshot.decision_run_id,
                    SignalDeciderDecisionSnapshot.response_payload,
                    SignalDeciderDecisionSnapshot.response_summary_payload,
                    SignalDeciderDecisionSnapshot.next_day_open_action,
                    SignalDeciderDecisionSnapshot.target_price, SignalDeciderDecisionSnapshot.stop_loss_price,
                    SignalDeciderDecisionSnapshot.decision_action,
                    SignalDeciderDecisionSnapshot.preferred_action,
                    SignalDeciderDecisionSnapshot.entry_price_basis,
                    SignalDeciderDecisionSnapshot.confidence_score_10,
                    SignalDeciderDecisionSnapshot.conviction_score_10,
                    SignalDeciderDecisionSnapshot.expected_return_pct,
                    SignalDeciderDecisionSnapshot.suggested_holding_period,
                )).order_by(SignalDeciderDecisionSnapshot.id.desc())).scalars().all()
                selected = {}
                for snapshot in snapshots:
                    key = (snapshot.symbol, snapshot.agent_name)
                    if key not in expected or key in selected:
                        continue
                    row = rows_by_key[key]
                    if snapshot.feedback_id != row.id or snapshot.decision_run_id != row.run_id:
                        return invalid
                    normalized = normalize_agent_response(
                        role=snapshot.agent_name, output_text=json.dumps(snapshot.response_payload),
                        raw_response={}, market_date=market_day, symbol=snapshot.symbol,
                        agent_name=snapshot.agent_name,
                    )
                    if normalized.get('response_contract_valid') is not True:
                        return invalid
                    snapshot_provenance = (snapshot.response_summary_payload or {}).get('decision_provenance') or {}
                    fenced = apply_decider_model_identity_fence(
                        role=snapshot.agent_name, normalized_feedback=normalized,
                        model_identity=(snapshot.response_payload or {}).get('model_identity') or snapshot_provenance.get('model_identity'),
                        expected_model=expected_decider_model(snapshot.agent_name, settings_obj=settings),
                        runtime_contract=(snapshot.response_payload or {}).get('runtime_contract') or snapshot_provenance.get('runtime_contract'),
                    )
                    if fenced.get('response_contract_valid') is not True:
                        return invalid
                    for field in ('preferred_action', 'next_day_open_action', 'tactical_action',
                                  'entry_price_basis', 'trigger_price', 'chase_limit_price',
                                  'target_price', 'stop_loss_price', 'confidence_score_10',
                                  'conviction_score_10'):
                        if normalized.get(field) != normalized_by_key[key].get(field):
                            return invalid
                    expected_action = (
                        _short_term_generic_action(_normalize_short_term_action(
                            normalized.get('tactical_action') or normalized.get('preferred_action'),
                        )) if snapshot.agent_name == 'short-term-decider'
                        else _normalize_action(normalized.get('next_day_open_action') or normalized.get('preferred_action'))
                    )
                    short_term = snapshot.agent_name == 'short-term-decider'
                    holding = normalized.get('expected_holding_period' if short_term else 'suggested_holding_period')
                    expected_columns = {
                        'decision_action': expected_action,
                        'preferred_action': expected_action if short_term else _normalize_action(normalized.get('preferred_action')),
                        'next_day_open_action': expected_action if short_term else _normalize_action(normalized.get('next_day_open_action')),
                        'entry_price_basis': 'trigger_limit' if short_term else _normalize_optional_text(
                            normalized.get('entry_price_basis'), default='next_open',
                        ) or 'next_open',
                        'target_price': _to_int(normalized.get('target_price')),
                        'stop_loss_price': _to_int(normalized.get('stop_loss_price')),
                        'confidence_score_10': finite_float(normalized.get('confidence_score_10')),
                        'conviction_score_10': finite_float(normalized.get('conviction_score_10')),
                        'expected_return_pct': None if short_term else finite_float(normalized.get('expected_return_pct')),
                        'suggested_holding_period': _normalize_optional_text(
                            str(holding) if holding is not None else None, max_length=64,
                        ),
                    }
                    for field, expected_value in expected_columns.items():
                        stored_value = getattr(snapshot, field)
                        if stored_value is not None and field in {'confidence_score_10', 'conviction_score_10', 'expected_return_pct'}:
                            stored_value = finite_float(stored_value)
                            if stored_value is None:
                                return invalid
                            # PostgreSQL persists these Numeric(12, 6) projections at six decimal places.
                            stored_value = Decimal(str(stored_value)).quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP)
                            if expected_value is not None:
                                expected_value = Decimal(str(expected_value)).quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP)
                        if stored_value != expected_value:
                            return invalid
                    selected[key] = snapshot
                if selected.keys() != expected:
                    return invalid
                identities = sorted(({
                    'symbol': s.symbol, 'decision_agent_name': s.agent_name,
                    'decision_snapshot_id': s.id, 'decision_run_id': s.decision_run_id,
                } for s in selected.values()), key=lambda v: (v['symbol'], v['decision_agent_name']))
            publication = {'valid': True, 'decision_identities': identities}
            if partial_summary:
                publication['summary_cutoff_resolution'] = {
                    'ready': True, 'source_run_id': run_id,
                    'knowledge_cutoff_at': cutoff.isoformat(),
                    'feedback_scope_mode': result['feedback_scope_mode'],
                    'requested_symbols': terminal_scope['completed_symbols'],
                    'completed_scope_proof': {
                        'schema_version': 'completed_summarizer_intersection_v1',
                        'completed_symbols': terminal_scope['completed_symbols'],
                        'admitted_symbols': sorted(summary_admitted),
                    },
                    'terminal_role_scope_proof': {
                        'schema_version': 'terminal_role_success_intersection_v1',
                        **{key: terminal_scope[key] for key in (
                            'failed_symbols_by_role', 'total_items', 'completed_items', 'failed_items',
                        )},
                    },
                }
            return publication
    except (KeyError, TypeError, ValueError, AttributeError, InvalidOperation):
        return invalid


def load_plan_publication_items(*, market_date: str, result: dict[str, Any]) -> list[dict[str, Any]]:
    """Restore omitted item identities from stored rows without refreshing a plan."""
    target = date.fromisoformat(result['summary']['target_trade_date'])
    source = date.fromisoformat(market_date)
    if result['summary']['source_market_date'] != market_date or target <= source:
        return []
    entries = result['completion_manifest']['decision_partition']
    expected = {(item['plan_item_id'], item['account_reference'], entry['symbol'],
                 entry['decision_agent_name'], entry['decision_snapshot_id'], entry['decision_run_id'])
                for entry in entries for item in entry['included_items']}
    published_items = {item['plan_item_id']: item for entry in entries for item in entry['included_items']}
    if not expected:
        return []
    with session_scope(statement_timeout_ms=10000, lock_timeout_ms=1000) as session:
        session.execute(text('SET TRANSACTION READ ONLY'))
        rows = session.execute(select(SignalDeciderExecutionPlanItem, SignalDeciderDecisionSnapshot.decision_run_id).join(
            SignalDeciderDecisionSnapshot,
            SignalDeciderDecisionSnapshot.id == SignalDeciderExecutionPlanItem.decision_snapshot_id,
        ).where(
            SignalDeciderExecutionPlanItem.id.in_([identity[0] for identity in expected]),
            SignalDeciderExecutionPlanItem.source_market_date == source,
            SignalDeciderExecutionPlanItem.target_trade_date == target,
            SignalDeciderExecutionPlanItem.execution_window == 'next_open',
        ).options(load_only(
            SignalDeciderExecutionPlanItem.id, SignalDeciderExecutionPlanItem.account_reference,
            SignalDeciderExecutionPlanItem.symbol, SignalDeciderExecutionPlanItem.decision_agent_name,
            SignalDeciderExecutionPlanItem.decision_snapshot_id,
        ))).all()
        items = [{
            'plan_item_id': row.id, 'account_reference': row.account_reference,
            'symbol': row.symbol, 'decision_agent_name': row.decision_agent_name,
            'decision_snapshot_id': row.decision_snapshot_id,
            'decision_run_id': decision_run_id,
            **{key: published_items[row.id].get(key) for key in (
                'eligibility_status', 'handoff_active', 'exec_monitoring_active',
            )},
        } for row, decision_run_id in rows]
    actual = {(i['plan_item_id'], i['account_reference'], i['symbol'], i['decision_agent_name'],
               i['decision_snapshot_id'], i['decision_run_id']) for i in items}
    return items if actual == expected else []
