from __future__ import annotations

import concurrent.futures
import json
import uuid
from datetime import date
from typing import Any, Callable, Sequence

from fastapi.encoders import jsonable_encoder

from app.agent_context.orchestrator_helpers import (
    build_completed_dispatch_item,
    build_failed_dispatch_item,
    resolve_dispatch_status,
)


def build_buffer_counts(snapshot: Any | None) -> dict[str, int | None]:
    if snapshot is None:
        return {
            'buffered_feedback_count': None,
            'buffered_failure_count': None,
            'pending_feedback_count': None,
            'pending_failure_count': None,
        }
    return {
        'buffered_feedback_count': len(snapshot.buffered_feedback_rows),
        'buffered_failure_count': len(snapshot.buffered_failures),
        'pending_feedback_count': len(snapshot.pending_feedback_rows),
        'pending_failure_count': len(snapshot.pending_failures),
    }


def build_pending_feedback_row(
    *,
    market_date: date,
    symbol: str,
    agent_name: str,
    run_id: str,
    response: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    summary = jsonable_encoder(response)
    normalized_feedback = summary.get('normalized_feedback') if isinstance(summary, dict) else None
    return {
        'market_date': market_date,
        'symbol': symbol,
        'agent_name': agent_name,
        'run_id': run_id,
        'summary': summary,
        'feedback': normalized_feedback if isinstance(normalized_feedback, dict) else {},
        'context': jsonable_encoder(context),
    }


def build_pending_failure_row(
    *,
    role: str,
    symbol: str,
    error: str,
    retryable: bool,
    agent_name: str | None = None,
    context: dict[str, Any] | None = None,
    dispatch_id: str | None = None,
    attempt_id: str | None = None,
    slot: int | None = None,
    session_key: str | None = None,
    batch_symbols: Sequence[str] | None = None,
) -> dict[str, Any]:
    return {
        'role': role,
        'agent_name': agent_name or role,
        'symbol': symbol,
        'status': 'failed',
        'retryable': retryable,
        'error': error,
        'failure_kind': error,
        'dispatch_id': dispatch_id,
        'attempt_id': attempt_id,
        'slot': slot,
        'session_key': session_key,
        'batch_symbols': list(batch_symbols) if batch_symbols is not None else None,
        'context': jsonable_encoder(context) if isinstance(context, dict) else None,
    }


def resolve_batch_dispatch_context(
    *,
    batch_result: dict[str, Any] | None,
    source_item: dict[str, Any],
    batch: Sequence[dict[str, Any]],
    slot: int | None,
) -> dict[str, Any]:
    resolved_batch_result = batch_result or {}
    session_ref = resolved_batch_result.get('session_ref') if isinstance(resolved_batch_result.get('session_ref'), dict) else {}
    batch_symbols = resolved_batch_result.get('batch_symbols')
    if not isinstance(batch_symbols, list):
        batch_symbols = [str(item.get('symbol') or '') for item in batch if str(item.get('symbol') or '').strip()]
    return {
        'dispatch_id': source_item.get('dispatch_id') or resolved_batch_result.get('dispatch_id'),
        'attempt_id': resolved_batch_result.get('attempt_id'),
        'batch_symbols': batch_symbols,
        'slot': slot,
        'session_key': session_ref.get('session_key') or source_item.get('session_key'),
    }


def normalize_symbol_feedback(
    *,
    normalize_agent_response: Callable[..., dict[str, Any]],
    role: str,
    agent_name: str,
    feedback_candidate: Any,
    market_date: str,
    symbol: str,
) -> dict[str, Any]:
    return normalize_agent_response(
        role=role,
        output_text=json.dumps(feedback_candidate, ensure_ascii=False),
        raw_response=feedback_candidate if isinstance(feedback_candidate, dict) else {'feedback': feedback_candidate},
        market_date=market_date,
        symbol=symbol,
        agent_name=agent_name,
    )


def dispatch_and_store(
    orchestrator,
    *,
    market_date: date,
    request_run_id: str,
    specs: Sequence[Any],
    payloads: dict[str, dict[str, Any]],
    symbol: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    items: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for spec in specs:
        payload = payloads[spec.role]
        response: dict[str, Any] | None = None
        try:
            response = orchestrator._get_client().dispatch(spec=spec, payload=payload)
            stored = orchestrator._store_feedback(
                market_date=market_date,
                agent_name=spec.agent_name,
                run_id=request_run_id,
                payload={'role': spec.role, 'symbol': symbol},
                response=response,
                context=payload,
            )
            items.append(
                build_completed_dispatch_item(
                    role=spec.role,
                    agent_name=spec.agent_name,
                    symbol=symbol,
                    normalized_feedback=response.get('normalized_feedback') or {},
                    storage=stored,
                )
            )
        except Exception as exc:  # noqa: BLE001
            retryable = exc.__class__.__name__ != 'AgentDispatchTerminalError'
            failures.append(
                build_failed_dispatch_item(
                    role=spec.role,
                    agent_name=spec.agent_name,
                    symbol=symbol,
                    retryable=retryable,
                    error=str(exc),
                )
            )
    return items, failures


def dispatch_for_symbols_via_subagents(
    orchestrator,
    *,
    market_date: str | date,
    symbols: Sequence[str],
    roles: Sequence[str] | None,
    fs_div: str | None,
    disclosures_limit: int,
    per_role_parallelism: int,
    signal_decider_agent_name: str,
    normalize_requested_symbols: Callable[[Sequence[str]], list[str]],
    split_requested_symbols_by_piotroski_passed_filter: Callable[..., tuple[list[str], list[str]]],
    agent_context_read_service,
    normalize_agent_response: Callable[..., dict[str, Any]],
    build_response_storage_payload: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    normalized_market_date = market_date if isinstance(market_date, date) else date.fromisoformat(market_date)
    requested_symbols = normalize_requested_symbols(symbols)
    dispatch_symbols, skipped_symbols = split_requested_symbols_by_piotroski_passed_filter(
        market_date=normalized_market_date,
        requested_symbols=requested_symbols,
    )
    specs = orchestrator._select_specs(roles=roles)
    if any(spec.role == signal_decider_agent_name for spec in specs):
        raise ValueError('signal-decider is not supported via subagent fanout; use dispatch_for_symbols so upstream summaries are available in-sequence')
    request_run_id = f'subagent-symbol-run-{normalized_market_date.isoformat()}-{uuid.uuid4().hex[:10]}'
    if not dispatch_symbols:
        return {
            'request': {
                'market_date': normalized_market_date.isoformat(),
                'symbols': [],
                'requested_symbols': requested_symbols,
                'request_run_id': request_run_id,
                'roles': [spec.role for spec in specs],
                'fs_div': fs_div,
                'disclosures_limit': disclosures_limit,
                'per_role_parallelism': max(1, int(per_role_parallelism)),
                'batch_size': 2,
                'total_parallel_subtasks': max(1, len(specs) * max(1, int(per_role_parallelism))),
                'scope': 'piotroski_passed_symbols_only',
                'skipped_symbols': skipped_symbols,
                'mode': 'slot_parent_subagent_fanout',
            },
            'summary': {
                'status': 'skipped',
                'completed': 0,
                'failed': 0,
                'skipped': len(skipped_symbols),
            },
            'items': [],
            'failures': [],
        }
    symbol_context = agent_context_read_service.get_symbol_day_context(
        market_date=normalized_market_date,
        symbols=dispatch_symbols,
        fs_div=fs_div,
        disclosures_limit=disclosures_limit,
    )
    items: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    batch_size = 2
    role_worker_count = max(1, int(per_role_parallelism))
    total_max_workers = max(1, len(specs) * role_worker_count)

    role_batches: list[tuple[Any, list[dict[str, Any]]]] = []
    for spec in specs:
        dispatch_items = [{**item} for item in symbol_context['items']]
        for batch_index, start in enumerate(range(0, len(dispatch_items), batch_size)):
            slot = (batch_index % role_worker_count) + 1
            batch = dispatch_items[start:start + batch_size]
            batch_with_session = [
                {
                    **batch_item,
                    'session_slot': slot,
                    'session_key': f"feedback/{spec.agent_name}/slot-{slot:02d}",
                }
                for batch_item in batch
            ]
            role_batches.append((spec, batch_with_session))

    with concurrent.futures.ThreadPoolExecutor(max_workers=total_max_workers) as executor:
        future_map = {
            executor.submit(
                orchestrator._get_client().dispatch_batch,
                spec=spec,
                market_date=normalized_market_date,
                batch_items=batch,
            ): (spec, batch)
            for spec, batch in role_batches
        }
        for future in concurrent.futures.as_completed(future_map):
            spec, batch = future_map[future]
            try:
                batch_result = future.result()
                decoded_items = batch_result.get('decoded', {}).get('items', [])
                by_symbol = {
                    item['symbol']: item
                    for item in decoded_items
                    if isinstance(item, dict) and item.get('symbol')
                }
                for source_item in batch:
                    symbol = source_item['symbol']
                    feedback_candidate = by_symbol.get(symbol, {}).get('feedback')
                    if not feedback_candidate:
                        failures.append(
                            build_failed_dispatch_item(
                                role=spec.role,
                                agent_name=spec.agent_name,
                                symbol=symbol,
                                retryable=True,
                                error='parent_slot_missing_symbol_feedback',
                            )
                        )
                        continue
                    normalized_feedback = normalize_symbol_feedback(
                        normalize_agent_response=normalize_agent_response,
                        role=spec.role,
                        agent_name=spec.agent_name,
                        feedback_candidate=feedback_candidate,
                        market_date=source_item.get('market_date') or normalized_market_date.isoformat(),
                        symbol=symbol,
                    )
                    stored = orchestrator._store_feedback(
                        market_date=normalized_market_date,
                        agent_name=spec.agent_name,
                        run_id=request_run_id,
                        payload={'role': spec.role, 'symbol': symbol},
                        response=build_response_storage_payload(
                            spec=spec,
                            batch_result=batch_result,
                            normalized_feedback=normalized_feedback,
                            collector_mode='slot_parent_subagent_fanout',
                        ),
                        context=source_item,
                    )
                    items.append(
                        build_completed_dispatch_item(
                            role=spec.role,
                            agent_name=spec.agent_name,
                            symbol=symbol,
                            normalized_feedback=normalized_feedback,
                            storage=stored,
                            session_ref=batch_result.get('session_ref'),
                        )
                    )
            except Exception as exc:  # noqa: BLE001
                for source_item in batch:
                    failures.append(
                        build_failed_dispatch_item(
                            role=spec.role,
                            agent_name=spec.agent_name,
                            symbol=source_item['symbol'],
                            retryable=True,
                            error=f'{type(exc).__name__}: {exc}',
                        )
                    )

    status = resolve_dispatch_status(completed=len(items), failed=len(failures))

    return {
        'request': {
            'market_date': normalized_market_date.isoformat(),
            'symbols': list(dispatch_symbols),
            'requested_symbols': requested_symbols,
            'request_run_id': request_run_id,
            'roles': [spec.role for spec in specs],
            'fs_div': fs_div,
            'disclosures_limit': disclosures_limit,
            'per_role_parallelism': role_worker_count,
            'batch_size': batch_size,
            'total_parallel_subtasks': total_max_workers,
            'scope': 'piotroski_passed_symbols_only',
            'skipped_symbols': skipped_symbols,
            'mode': 'slot_parent_subagent_fanout',
        },
        'summary': {
            'status': status,
            'completed': len(items),
            'failed': len(failures),
            'skipped': len(skipped_symbols),
        },
        'items': items,
        'failures': failures,
    }
