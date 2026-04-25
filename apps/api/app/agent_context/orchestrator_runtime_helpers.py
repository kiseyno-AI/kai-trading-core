from __future__ import annotations

import concurrent.futures
import json
import uuid
from datetime import date
from typing import Any, Callable, Sequence


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
                {
                    'role': spec.role,
                    'agent_name': spec.agent_name,
                    'symbol': symbol,
                    'status': 'completed',
                    'storage': stored,
                    'response': {'normalized_feedback': response.get('normalized_feedback')},
                }
            )
        except Exception as exc:  # noqa: BLE001
            retryable = exc.__class__.__name__ != 'AgentDispatchTerminalError'
            failures.append(
                {
                    'role': spec.role,
                    'agent_name': spec.agent_name,
                    'symbol': symbol,
                    'status': 'failed',
                    'retryable': retryable,
                    'error': str(exc),
                }
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
                            {
                                'role': spec.role,
                                'agent_name': spec.agent_name,
                                'symbol': symbol,
                                'status': 'failed',
                                'retryable': True,
                                'error': 'parent_slot_missing_symbol_feedback',
                            }
                        )
                        continue
                    normalized_feedback = normalize_agent_response(
                        role=spec.role,
                        output_text=json.dumps(feedback_candidate, ensure_ascii=False),
                        raw_response=feedback_candidate if isinstance(feedback_candidate, dict) else {'feedback': feedback_candidate},
                        market_date=source_item.get('market_date') or normalized_market_date.isoformat(),
                        symbol=symbol,
                        agent_name=spec.agent_name,
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
                        {
                            'role': spec.role,
                            'agent_name': spec.agent_name,
                            'symbol': symbol,
                            'status': 'completed',
                            'storage': stored,
                            'response': {'normalized_feedback': normalized_feedback},
                            'session_ref': batch_result.get('session_ref'),
                        }
                    )
            except Exception as exc:  # noqa: BLE001
                for source_item in batch:
                    failures.append(
                        {
                            'role': spec.role,
                            'agent_name': spec.agent_name,
                            'symbol': source_item['symbol'],
                            'status': 'failed',
                            'retryable': True,
                            'error': f'{type(exc).__name__}: {exc}',
                        }
                    )

    status = 'completed'
    if failures and items:
        status = 'partial'
    elif failures and not items:
        status = 'failed'

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
