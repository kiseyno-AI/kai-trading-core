from __future__ import annotations

import json
import hashlib
import re
import uuid
from datetime import date, datetime, timezone
from typing import Any, Sequence

from fastapi.encoders import jsonable_encoder
from app.agent_context.signal_envelope import (
    SIGNAL_ENVELOPE_SCHEMA_VERSION,
    build_decider_signal_envelopes,
)
from app.agent_context.signal_decider_loop_service import signal_decider_loop_service

ABORTLIKE_ERROR_MARKERS = (
    'this operation was aborted',
    'aborted',
    'abortedlastrun',
    'killed',
    'failed',
)


def is_abortlike_error(error: Any) -> bool:
    text = str(error or '').lower()
    return any(marker in text for marker in ABORTLIKE_ERROR_MARKERS)


FEEDBACK_SESSION_KEY_PREFIX = 'feedback'
FRESH_FEEDBACK_SESSION_KEY_PREFIX = 'feedback-fresh'


def feedback_session_key(*, agent_name: str, slot: int) -> str:
    return f"{FEEDBACK_SESSION_KEY_PREFIX}/{agent_name}/slot-{slot:02d}"


def fresh_feedback_session_key(*, agent_name: str, request_run_id: str, slot: int) -> str:
    return f"{FRESH_FEEDBACK_SESSION_KEY_PREFIX}/{agent_name}/{request_run_id}/slot-{slot:02d}/{uuid.uuid4().hex[:10]}"


def subagent_test_session_key(*, agent_name: str, request_run_id: str, symbol: str) -> str:
    safe_symbol = ''.join(ch for ch in symbol if ch.isalnum())[:24] or 'symbol'
    return f"subagent-test/{agent_name}/{request_run_id}/{safe_symbol}-{uuid.uuid4().hex[:8]}"


def normalize_requested_symbols(symbols: Sequence[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        for piece in (raw_symbol or '').split(','):
            symbol = piece.strip().upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            normalized.append(symbol)
    if not normalized:
        raise ValueError('At least one symbol is required')
    return normalized


def strip_runtime_session_fields(item: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in item.items()
        if key not in {'session_slot', 'session_key'}
    }


def feedback_row_lookup(rows: Sequence[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        symbol = str(row.get('symbol') or '').strip()
        agent_name = str(row.get('agent_name') or '').strip()
        if not symbol or not agent_name:
            continue
        lookup[(symbol, agent_name)] = row
    return lookup


def request_scope_from_items(batch_items: Sequence[dict[str, Any]], key: str) -> str | None:
    values = {
        str(item.get(key)).strip()
        for item in batch_items
        if item.get(key) is not None and str(item.get(key)).strip()
    }
    if not values:
        return None
    if len(values) > 1:
        raise ValueError(f'batch items mixed {key}: {sorted(values)}')
    return next(iter(values))


def _canonical_symbol(value: Any) -> str | None:
    symbol = str(value or '').strip().upper()
    return symbol or None


def _feedback_reported_symbol(feedback: Any) -> str | None:
    if not isinstance(feedback, dict):
        return None
    symbol = _canonical_symbol(feedback.get('symbol'))
    if symbol:
        return symbol
    traceability = feedback.get('traceability')
    if isinstance(traceability, dict):
        return _canonical_symbol(traceability.get('symbol'))
    return None


def _append_feedback_risk_flags(feedback: dict[str, Any], flags: Sequence[str]) -> None:
    existing = feedback.get('risk_flags')
    risk_flags = [str(flag) for flag in existing if isinstance(flag, str)] if isinstance(existing, list) else []
    feedback['risk_flags'] = list(dict.fromkeys([*risk_flags, *flags]))


def _annotate_feedback_attribution(
    *,
    feedback: Any,
    expected_symbol: str,
    item_symbol: str | None,
    feedback_symbol: str | None,
    fallback_reason: str,
    session_reuse_mode: str | None,
) -> None:
    if not isinstance(feedback, dict):
        return
    flags = ['fallback_symbol_attribution']
    if item_symbol is None and feedback_symbol is None:
        flags.append('response_symbol_missing')
    if item_symbol is not None and item_symbol != expected_symbol:
        flags.append('response_symbol_mismatch')
    if feedback_symbol is not None and feedback_symbol != expected_symbol:
        flags.append('reported_symbol_mismatch')
    _append_feedback_risk_flags(feedback, flags)
    traceability = feedback.get('traceability') if isinstance(feedback.get('traceability'), dict) else {}
    feedback['traceability'] = {
        **traceability,
        'response_attribution': {
            'expected_symbol': expected_symbol,
            'item_symbol': item_symbol,
            'feedback_symbol': feedback_symbol,
            'fallback_reason': fallback_reason,
            'session_reuse_mode': session_reuse_mode,
        },
    }


def coerce_decoded_batch_response(decoded: dict[str, Any] | None) -> dict[str, Any] | None:
    """Normalize common agent output-shape mistakes before symbol correlation.

    Some agents occasionally return {"batch": {"items": [...]}} or put feedback
    fields directly on each item instead of under item.feedback. Preserve the
    response but coerce it into the canonical {"items": [{"symbol", "feedback"}]}
    envelope so downstream role-specific normalizers can apply their usual
    mismatch/risk-flag handling instead of failing the whole batch early.
    """
    if not isinstance(decoded, dict):
        return decoded

    working = decoded
    batch = working.get('batch')
    if isinstance(batch, dict) and isinstance(batch.get('items'), list):
        working = {**batch, '_coerced_from_batch_key': True}

    items = working.get('items')
    if not isinstance(items, list):
        return working

    normalized_items: list[Any] = []
    for item in items:
        if not isinstance(item, dict):
            normalized_items.append(item)
            continue
        if isinstance(item.get('feedback'), dict):
            normalized_items.append(item)
            continue
        symbol = item.get('symbol')
        feedback = {
            key: value
            for key, value in item.items()
            if key not in {'feedback'}
        }
        normalized_items.append({'symbol': symbol, 'feedback': feedback, '_coerced_feedback_from_item': True})
    return {**working, 'items': normalized_items}


def correlate_decoded_batch_response(
    *,
    decoded: dict[str, Any] | None,
    expected_symbols: Sequence[str],
    session_reuse_mode: str | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    expected = [_canonical_symbol(symbol) for symbol in expected_symbols]
    expected = [symbol for symbol in expected if symbol is not None]
    metadata: dict[str, Any] = {
        'expected_symbols': expected,
        'parsed_symbols': [],
        'response_symbols': [],
        'missing_symbols': expected,
        'unexpected_symbols': [],
        'fallback_attributions': [],
        'attribution_status': 'not_decoded',
        'failure_class': 'invalid_json',
        'session_reuse_mode': session_reuse_mode,
    }
    if decoded is None:
        return None, metadata

    decoded_items = decoded.get('items') if isinstance(decoded, dict) else None
    if not isinstance(decoded_items, list):
        metadata['attribution_status'] = 'missing_items'
        metadata['failure_class'] = 'batch_response_missing_items'
        return decoded, metadata

    canonical_by_symbol = {symbol: symbol for symbol in expected}
    response_symbols: list[str] = []
    normalized_items: list[Any] = []
    for item in decoded_items:
        if not isinstance(item, dict):
            normalized_items.append(item)
            continue
        copied = dict(item)
        item_symbol = _canonical_symbol(copied.get('symbol'))
        if item_symbol:
            response_symbols.append(item_symbol)
            if item_symbol in canonical_by_symbol:
                copied['symbol'] = canonical_by_symbol[item_symbol]
        normalized_items.append(copied)

    normalized_decoded = {**decoded, 'items': normalized_items}
    response_symbol_set = set(response_symbols)
    expected_set = set(expected)
    missing_symbols = [symbol for symbol in expected if symbol not in response_symbol_set]
    unexpected_symbols = [symbol for symbol in response_symbols if symbol not in expected_set]

    if len(expected) == 1 and len(normalized_items) == 1 and isinstance(normalized_items[0], dict):
        expected_symbol = expected[0]
        item = dict(normalized_items[0])
        feedback = item.get('feedback')
        item_symbol = _canonical_symbol(item.get('symbol'))
        feedback_symbol = _feedback_reported_symbol(feedback)
        feedback_symbol_matches = feedback_symbol in (None, expected_symbol)
        if item_symbol is None and feedback_symbol_matches:
            fallback_reason = 'single_symbol_batch'
            item['symbol'] = expected_symbol
            _annotate_feedback_attribution(
                feedback=feedback,
                expected_symbol=expected_symbol,
                item_symbol=item_symbol,
                feedback_symbol=feedback_symbol,
                fallback_reason=fallback_reason,
                session_reuse_mode=session_reuse_mode,
            )
            normalized_decoded = {**decoded, 'items': [item]}
            metadata.update(
                {
                    'parsed_symbols': response_symbols,
                    'response_symbols': [expected_symbol],
                    'missing_symbols': [],
                    'unexpected_symbols': [],
                    'fallback_attributions': [
                        {
                            'symbol': expected_symbol,
                            'item_symbol': item_symbol,
                            'feedback_symbol': feedback_symbol,
                            'reason': fallback_reason,
                        }
                    ],
                    'attribution_status': 'fallback_attributed',
                    'failure_class': None,
                }
            )
            return normalized_decoded, metadata

    metadata.update(
        {
            'parsed_symbols': response_symbols,
            'response_symbols': response_symbols,
            'missing_symbols': missing_symbols,
            'unexpected_symbols': unexpected_symbols,
            'attribution_status': 'matched' if not missing_symbols and not unexpected_symbols else 'mismatch',
            'failure_class': None if not missing_symbols and not unexpected_symbols else 'batch_response_attribution_mismatch',
        }
    )
    return normalized_decoded, metadata


def slot_from_items(batch_items: Sequence[dict[str, Any]]) -> int | None:
    raw_slot = request_scope_from_items(batch_items, 'session_slot')
    if raw_slot is None:
        return None
    return int(raw_slot)


def extract_response_mismatch_flags(normalized_feedback: dict[str, Any] | None) -> list[str]:
    if not isinstance(normalized_feedback, dict):
        return []
    risk_flags = normalized_feedback.get('risk_flags')
    if not isinstance(risk_flags, list):
        return []
    return [
        str(flag)
        for flag in risk_flags
        if isinstance(flag, str) and flag in {'role_mismatch', 'symbol_mismatch', 'market_date_mismatch'}
    ]


def failure_log_fields(failure: dict[str, Any]) -> dict[str, Any]:
    context = failure.get('context') if isinstance(failure.get('context'), dict) else {}
    diagnostics = failure.get('diagnostics') if isinstance(failure.get('diagnostics'), dict) else {}
    dispatch_diagnostics = (
        context.get('dispatch_diagnostics')
        if isinstance(context.get('dispatch_diagnostics'), dict)
        else {}
    )
    batch_symbols = context.get('batch_symbols')
    return {
        'dispatch_id': failure.get('dispatch_id') or context.get('dispatch_id'),
        'attempt_id': failure.get('attempt_id') or context.get('attempt_id'),
        'slot': failure.get('slot') or context.get('session_slot'),
        'session_key': failure.get('session_key') or context.get('session_key'),
        'session_id': (
            failure.get('session_id')
            or context.get('session_id')
            or diagnostics.get('session_id')
            or dispatch_diagnostics.get('session_id')
        ),
        'response_id': (
            failure.get('response_id')
            or context.get('response_id')
            or diagnostics.get('response_id')
            or dispatch_diagnostics.get('response_id')
        ),
        'request_marker': (
            failure.get('request_marker')
            or context.get('request_marker')
            or diagnostics.get('request_marker')
            or dispatch_diagnostics.get('request_marker')
        ),
        'batch_symbols': batch_symbols if isinstance(batch_symbols, list) else None,
        'diagnostics': diagnostics or dispatch_diagnostics or None,
    }


def build_completed_dispatch_item(
    *,
    role: str,
    agent_name: str,
    symbol: str,
    normalized_feedback: dict[str, Any],
    storage: dict[str, Any] | None = None,
    session_ref: dict[str, Any] | None = None,
) -> dict[str, Any]:
    item = {
        'role': role,
        'agent_name': agent_name,
        'symbol': symbol,
        'status': 'completed',
        'response': {'normalized_feedback': normalized_feedback},
    }
    if storage is not None:
        item['storage'] = storage
    if session_ref is not None:
        item['session_ref'] = session_ref
    return item


def build_failed_dispatch_item(
    *,
    role: str,
    agent_name: str,
    symbol: str,
    error: str,
    retryable: bool,
    failure_kind: str | None = None,
    context: dict[str, Any] | None = None,
    dispatch_id: str | None = None,
    attempt_id: str | None = None,
    slot: int | None = None,
    session_key: str | None = None,
    session_id: str | None = None,
    response_id: str | None = None,
    request_marker: str | None = None,
    diagnostics: dict[str, Any] | None = None,
    batch_symbols: Sequence[str] | None = None,
) -> dict[str, Any]:
    item = {
        'role': role,
        'agent_name': agent_name,
        'symbol': symbol,
        'status': 'failed',
        'retryable': retryable,
        'error': error,
    }
    if failure_kind is not None:
        item['failure_kind'] = failure_kind
    if context is not None:
        item['context'] = context
    if dispatch_id is not None:
        item['dispatch_id'] = dispatch_id
    if attempt_id is not None:
        item['attempt_id'] = attempt_id
    if slot is not None:
        item['slot'] = slot
    if session_key is not None:
        item['session_key'] = session_key
    if session_id is not None:
        item['session_id'] = session_id
    if response_id is not None:
        item['response_id'] = response_id
    if request_marker is not None:
        item['request_marker'] = request_marker
    if diagnostics is not None:
        item['diagnostics'] = diagnostics
    if batch_symbols is not None:
        item['batch_symbols'] = list(batch_symbols)
    return item


def resolve_dispatch_status(*, completed: int, failed: int) -> str:
    if failed and completed:
        return 'partial'
    if failed:
        return 'failed'
    return 'completed'


def build_response_storage_payload(
    *,
    spec,
    batch_result: dict[str, Any],
    normalized_feedback: dict[str, Any],
    collector_mode: str,
) -> dict[str, Any]:
    raw_response = batch_result.get('response') if isinstance(batch_result.get('response'), dict) else None
    model_name = None
    if raw_response is not None:
        model_name = raw_response.get('model') or raw_response.get('model_name')
    return {
        'attempt': batch_result.get('attempt', 1),
        'attempt_id': batch_result.get('attempt_id'),
        'dispatch_id': batch_result.get('dispatch_id'),
        'request_run_id': batch_result.get('request_run_id'),
        'request_marker': batch_result.get('request_marker'),
        'market_date': batch_result.get('market_date'),
        'role': spec.role,
        'agent_name': spec.agent_name,
        'batch_symbols': batch_result.get('batch_symbols'),
        'slot': batch_result.get('slot'),
        'session_key': batch_result.get('session_ref', {}).get('session_key') if isinstance(batch_result.get('session_ref'), dict) else None,
        'session_id': batch_result.get('session_ref', {}).get('session_id') if isinstance(batch_result.get('session_ref'), dict) else None,
        'response_id': batch_result.get('session_ref', {}).get('response_id') if isinstance(batch_result.get('session_ref'), dict) else None,
        'started_at': batch_result.get('started_at'),
        'completed_at': batch_result.get('completed_at'),
        'model': model_name or f'openclaw/{spec.agent_name}',
        'output_text': batch_result.get('output_text'),
        'raw_response': raw_response,
        'normalized_feedback': normalized_feedback,
        'normalization_mismatch_flags': extract_response_mismatch_flags(normalized_feedback),
        'session_ref': batch_result.get('session_ref'),
        'response_correlation': batch_result.get('response_correlation'),
        'session_reuse_mode': batch_result.get('session_reuse_mode'),
        'collector_mode': collector_mode,
    }


def _compact_symbol_day_context_for_decider(context: dict[str, Any]) -> dict[str, Any]:
    """Keep decision-relevant symbol context without shipping bulky raw provenance."""
    keep_keys = {
        'symbol',
        'market_date',
        'company',
        'market_session',
        'market_data_phase_contract',
        'quote_context',
        'price_context',
        'technical_snapshot',
        'action_summary',
        'intraday_execution_context',
        'valuation_context',
        'investor_flow_context',
        'short_selling_context',
        'sector_peer_context',
        'market_relative_context',
        'market_breadth_context',
        'macro_context',
        'recent_disclosures',
        'recent_dart_events',
        'recent_external_news',
        'fundamentals',
    }
    compacted = {key: context.get(key) for key in keep_keys if key in context}
    completeness = context.get('context_completeness')
    if isinstance(completeness, dict):
        compacted['context_completeness'] = {
            key: completeness.get(key)
            for key in ('summary', 'missing_sections', 'critical_missing_sections', 'coverage_status')
            if key in completeness
        }
    provenance = context.get('context_provenance')
    if isinstance(provenance, dict):
        compacted['context_provenance'] = {
            key: provenance.get(key)
            for key in ('summary', 'missing_sources', 'stale_sources', 'source_status_counts')
            if key in provenance
        }
    return compacted


def _compact_factor_scores(scores: dict[str, Any]) -> dict[str, Any]:
    compacted: dict[str, Any] = {}
    keep_fields = {
        'score',
        'rank',
        'percentile',
        'status',
        'value_score',
        'value_rank',
        'value_percentile',
        'quality_score',
        'quality_rank',
        'quality_percentile',
        'momentum_score',
        'momentum_rank',
        'momentum_percentile',
        'signal_count',
        'volume_power_rank',
        'volume_power',
        'overtime_volume_rank',
        'overtime_volume',
        'overtime_fluctuation_rank',
        'overtime_fluctuation_rate',
    }
    for name, payload in scores.items():
        if isinstance(payload, dict):
            compacted[name] = {key: payload.get(key) for key in keep_fields if key in payload}
        else:
            compacted[name] = payload
    return compacted


def _compact_decider_data_quality_context(context: dict[str, Any]) -> dict[str, Any]:
    compacted = {
        key: context.get(key)
        for key in ('status', 'requested_market_date', 'block_status_counts', 'decision_priority')
        if key in context
    }
    blocks = context.get('blocks')
    if isinstance(blocks, dict):
        compacted['blocks'] = {
            name: {
                key: block.get(key)
                for key in ('status', 'decision_guidance', 'providers')
                if isinstance(block, dict) and key in block
            }
            for name, block in blocks.items()
            if isinstance(block, dict)
        }
    return compacted


def _compact_pre_decider_enrichment_context(context: dict[str, Any]) -> dict[str, Any]:
    compacted = {
        key: context.get(key)
        for key in ('schema_version', 'status', 'market_date')
        if key in context
    }
    for key in ('quote_orderbook_snapshot', 'trading_value'):
        value = context.get(key)
        if isinstance(value, dict):
            compacted[key] = value
    flow_context = context.get('flow_short_sector_context')
    if isinstance(flow_context, dict):
        compacted['flow_short_sector_context'] = {
            field: flow_context.get(field)
            for field in (
                'status',
                'summary',
                'investor_flow',
                'short_selling',
                'sector_relative_strength',
                'missing_fields',
                'risk_flags',
            )
            if field in flow_context
        }
    return compacted


def _compact_market_regime_context(context: dict[str, Any]) -> dict[str, Any]:
    compacted = {
        key: context.get(key)
        for key in (
            'schema_version',
            'status',
            'market_date',
            'market',
            'posture',
            'reason_code',
            'reason_summary',
            'source_status',
            'recommended_exec_action',
            'recommended_sizing_multiplier',
            'missing_fields',
        )
        if key in context
    }
    for key in ('market_breadth', 'sector_context', 'relative_strength_context', 'freshness'):
        value = context.get(key)
        if isinstance(value, dict):
            compacted[key] = value
    return compacted


def trim_batch_item_for_prompt(item: dict[str, Any]) -> dict[str, Any]:
    trimmed = json.loads(json.dumps(item, ensure_ascii=False))
    symbol_day_context = trimmed.get('symbol_day_context')
    if isinstance(symbol_day_context, dict):
        trimmed['symbol_day_context'] = trim_batch_item_for_prompt(
            _compact_symbol_day_context_for_decider(symbol_day_context)
        )
    else:
        bulky_duplicate_keys = {
            'context_provenance',
            'persisted_factor_rows',
            'price_context_history',
        }
        for key in bulky_duplicate_keys:
            if key in trimmed:
                trimmed[key] = {
                    'compressed_payload': _compress_prompt_value(trimmed[key]),
                    'source_field': key,
                    'note': 'Bulky supporting context compressed for prompt transport; not dropped.',
                }
    scores = trimmed.get('scores')
    if isinstance(scores, dict):
        trimmed['scores'] = _compact_factor_scores(scores)
    factor_summary = trimmed.get('factor_summary')
    if isinstance(factor_summary, dict):
        factor_scores = factor_summary.get('factor_scores')
        if isinstance(factor_scores, dict):
            factor_summary['factor_scores'] = _compact_factor_scores(factor_scores)
    decider_data_quality_context = trimmed.get('decider_data_quality_context')
    if isinstance(decider_data_quality_context, dict):
        trimmed['decider_data_quality_context'] = _compact_decider_data_quality_context(decider_data_quality_context)
    pre_decider_enrichment_context = trimmed.get('pre_decider_enrichment_context')
    if isinstance(pre_decider_enrichment_context, dict):
        trimmed['pre_decider_enrichment_context'] = _compact_pre_decider_enrichment_context(pre_decider_enrichment_context)
    market_regime_context = trimmed.get('market_regime_context')
    if isinstance(market_regime_context, dict):
        trimmed['market_regime_context'] = _compact_market_regime_context(market_regime_context)
    technical_snapshot = trimmed.get('technical_snapshot')
    if isinstance(technical_snapshot, dict):
        technical_snapshot.pop('signal_payload', None)
    action_summary = trimmed.get('action_summary')
    if isinstance(action_summary, dict):
        action_summary.pop('summary_payload', None)
    fundamentals = trimmed.get('fundamentals')
    if isinstance(fundamentals, dict):
        latest_snapshot = fundamentals.get('latest_snapshot')
        if isinstance(latest_snapshot, dict):
            latest_snapshot.pop('raw_rows', None)
            statement = latest_snapshot.get('statement')
            if isinstance(statement, dict):
                keep_statement_keys = {'fs_div', 'fs_label', 'statement_type', 'currency', 'source_status', 'source_message', 'row_count'}
                latest_snapshot['statement'] = {k: v for k, v in statement.items() if k in keep_statement_keys}
            highlights = latest_snapshot.get('highlights')
            if isinstance(highlights, dict):
                latest_snapshot['highlights'] = {
                    key: highlights.get(key)
                    for key in (
                        'revenue',
                        'operating_profit',
                        'net_income',
                        'total_assets',
                        'total_liabilities',
                        'equity',
                        'operating_cash_flow',
                        'free_cash_flow',
                        'summary',
                        'data_quality',
                    )
                    if key in highlights
                }
            fundamental_context = latest_snapshot.get('fundamental_context')
            if isinstance(fundamental_context, dict):
                latest_snapshot['fundamental_context'] = {
                    key: fundamental_context.get(key)
                    for key in (
                        'quality_summary',
                        'growth_summary',
                        'balance_sheet_summary',
                        'cash_flow_summary',
                        'risk_flags',
                        'missing_fields',
                    )
                    if key in fundamental_context
                }
        history_summary = fundamentals.get('history_summary')
        if isinstance(history_summary, dict):
            recent_periods = history_summary.get('recent_periods')
            if isinstance(recent_periods, list):
                history_summary['recent_periods'] = recent_periods[:3]
    recent_disclosures = trimmed.get('recent_disclosures')
    if isinstance(recent_disclosures, dict):
        items = recent_disclosures.get('items')
        if isinstance(items, list):
            recent_disclosures['items'] = items[:3]
            recent_disclosures['count'] = min(recent_disclosures.get('count', len(items)), len(recent_disclosures['items']))
    recent_dart_events = trimmed.get('recent_dart_events')
    if isinstance(recent_dart_events, dict):
        items = recent_dart_events.get('items')
        if isinstance(items, list):
            recent_dart_events['items'] = items[:3]
            recent_dart_events['item_count'] = min(recent_dart_events.get('item_count', len(items)), len(recent_dart_events['items']))
    recent_external_news = trimmed.get('recent_external_news')
    if isinstance(recent_external_news, dict):
        for key in ('material_items', 'unknown_items'):
            grouped_items = recent_external_news.get(key)
            if isinstance(grouped_items, list):
                recent_external_news[key] = grouped_items[:3]
        noise_filter = recent_external_news.get('noise_filter')
        if isinstance(noise_filter, dict):
            noise_items = noise_filter.get('items')
            if isinstance(noise_items, list):
                noise_filter['items'] = noise_items[:2]
                noise_filter['included_count'] = min(int(noise_filter.get('included_count') or len(noise_items)), len(noise_filter['items']))
                noise_filter['omitted_count'] = max(0, int(noise_filter.get('item_count') or len(noise_items)) - len(noise_filter['items']))
        items = recent_external_news.get('items')
        if isinstance(items, list):
            recent_external_news['items'] = items[:3]
            recent_external_news['item_count'] = min(recent_external_news.get('item_count', len(items)), len(recent_external_news['items']))
    return trimmed


_PROMPT_COMPRESS_RAW_KEYS = {
    'raw',
    'raw_item',
    'raw_items',
    'raw_payload',
    'raw_response',
    'raw_rows',
    'source_payload',
    'full_payload',
    'context_provenance',
    'persisted_factor_rows',
    'price_context_history',
    'request_payload',
    'response_payload',
    'summary_payload',
}

_PROMPT_SLIM_PRIORITY_KEYS = (
    'symbol',
    'market_date',
    'agent_name',
    'role',
    'preferred_action',
    'next_day_open_action',
    'tactical_action',
    'decision_action',
    'wait_reason',
    'confidence',
    'confidence_score_10',
    'conviction_score_10',
    'summary',
    'thesis_summary',
    'target_price',
    'stop_loss_price',
    'trigger_price',
    'chase_limit_price',
    'expected_return_pct',
    'risk_flags',
    'supporting_evidence',
    'decision_risks',
    'tactical_risks',
    'data_quality_warnings',
    'specialist_disagreement',
    'traceability',
    'market_regime_context',
    'decision_input_quality_context',
    'decider_data_quality_context',
    'pre_decider_enrichment_context',
    'upstream_summaries',
    'upstream_traceability',
    'previous_decision_context',
)


def _truncate_prompt_string(value: str, *, limit: int) -> str:
    if len(value) <= limit:
        return value
    return f'{value[: max(0, limit - 32)]}...[truncated {len(value) - limit} chars]'


def _stable_prompt_hash(value: Any) -> str:
    encoded = json.dumps(jsonable_encoder(value), ensure_ascii=False, sort_keys=True, default=str)
    return 'sha256:' + hashlib.sha256(encoded.encode('utf-8')).hexdigest()[:16]


def _text_evidence_summary(value: str) -> dict[str, Any]:
    stripped = value.strip()
    sentences = [part.strip() for part in re.split(r'(?<=[.!?。！？])\s+|\n+', stripped) if part.strip()]
    if not sentences:
        sentences = [stripped] if stripped else []
    snippets: list[str] = []
    for sentence in sentences:
        if sentence in snippets:
            continue
        snippets.append(_truncate_prompt_string(sentence, limit=260))
        if len(snippets) >= 5:
            break
    if len(stripped) > 800 and stripped[-260:].strip() not in snippets:
        snippets.append(_truncate_prompt_string(stripped[-260:].strip(), limit=260))
    return {
        'compression': 'text_evidence_summary_v1',
        'char_count': len(value),
        'hash': _stable_prompt_hash(value),
        'snippets': snippets,
    }


def _scalar_summary(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return _truncate_prompt_string(value, limit=220)
    return _truncate_prompt_string(str(value), limit=220)


def _compressed_list_summary(value: list[Any], *, depth: int) -> dict[str, Any]:
    numeric_values = [item for item in value if isinstance(item, (int, float)) and not isinstance(item, bool)]
    scalar_counts: dict[str, int] = {}
    for item in value:
        if item is None or isinstance(item, (bool, int, float, str)):
            label = str(_scalar_summary(item))
            scalar_counts[label] = scalar_counts.get(label, 0) + 1
    summary: dict[str, Any] = {
        'compression': 'list_summary_v1',
        'item_count': len(value),
        'hash': _stable_prompt_hash(value),
        'samples': [_compress_prompt_value(item, depth=depth + 1) for item in value[:5]],
    }
    if len(value) > 5:
        summary['tail_sample'] = _compress_prompt_value(value[-1], depth=depth + 1)
    if numeric_values:
        summary['numeric_range'] = {
            'min': min(numeric_values),
            'max': max(numeric_values),
            'avg': sum(float(item) for item in numeric_values) / len(numeric_values),
        }
    if scalar_counts:
        summary['top_scalar_values'] = [
            {'value': key, 'count': count}
            for key, count in sorted(scalar_counts.items(), key=lambda item: (-item[1], item[0]))[:8]
        ]
    return summary


def _compressed_dict_summary(value: dict[str, Any], *, depth: int) -> dict[str, Any]:
    priority_payload = {
        key: _compress_prompt_value(value[key], depth=depth + 1)
        for key in _PROMPT_SLIM_PRIORITY_KEYS
        if key in value
    }
    numeric_fields: dict[str, Any] = {}
    scalar_fields: dict[str, Any] = {}
    nested_shapes: dict[str, Any] = {}
    for key, item in value.items():
        key_text = str(key)
        if key_text in priority_payload or key_text.startswith('_'):
            continue
        if isinstance(item, (int, float)) and not isinstance(item, bool):
            numeric_fields[key_text] = item
        elif item is None or isinstance(item, (bool, str)):
            scalar_fields[key_text] = _scalar_summary(item)
        elif isinstance(item, list):
            nested_shapes[key_text] = {'type': 'list', 'item_count': len(item)}
        elif isinstance(item, dict):
            nested_shapes[key_text] = {'type': 'dict', 'key_count': len(item), 'keys': list(item.keys())[:12]}
    summary: dict[str, Any] = {
        'compression': 'dict_summary_v1',
        'key_count': len(value),
        'keys': list(value.keys())[:30],
        'hash': _stable_prompt_hash(value),
    }
    if priority_payload:
        summary['priority_fields'] = priority_payload
    if numeric_fields:
        summary['numeric_fields'] = numeric_fields
    if scalar_fields:
        summary['scalar_fields'] = scalar_fields
    if nested_shapes:
        summary['nested_shapes'] = nested_shapes
    return summary


def _compress_prompt_value(value: Any, *, depth: int = 0) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        if len(value) > (1600 if depth <= 2 else 700):
            return _text_evidence_summary(value)
        return value
    if isinstance(value, list):
        if len(value) > (8 if depth <= 2 else 4):
            return _compressed_list_summary(value, depth=depth)
        return [_compress_prompt_value(item, depth=depth + 1) for item in value]
    if isinstance(value, dict):
        if depth >= 5:
            return _compressed_dict_summary(value, depth=depth)

        ordered_keys = [
            key for key in _PROMPT_SLIM_PRIORITY_KEYS if key in value
        ]
        ordered_keys.extend(
            key
            for key in value
            if key not in set(ordered_keys)
            and not str(key).startswith('_')
        )
        limit = 18 if depth <= 1 else 10
        selected_keys = ordered_keys[:limit]
        compact = {
            str(key): (
                {
                    'compressed_payload': _compress_prompt_value(value[key], depth=depth + 1),
                    'source_field': str(key),
                    'note': 'Large/raw field compressed for prompt transport; not dropped.',
                }
                if str(key) in _PROMPT_COMPRESS_RAW_KEYS
                else _compress_prompt_value(value[key], depth=depth + 1)
            )
            for key in selected_keys
        }
        dropped_count = max(0, len(ordered_keys) - len(selected_keys))
        if dropped_count:
            remaining = {str(key): value[key] for key in ordered_keys[limit:]}
            compact['compressed_remaining_fields'] = _compressed_dict_summary(remaining, depth=depth + 1)
        return compact
    return _text_evidence_summary(str(value))


def compressed_batch_item_for_prompt(item: dict[str, Any]) -> dict[str, Any]:
    """Return a bounded one-symbol prompt payload without silently dropping raw evidence."""
    trimmed = trim_batch_item_for_prompt(item)
    compressed = _compress_prompt_value(trimmed)
    return compressed if isinstance(compressed, dict) else {'payload': compressed}


def slim_batch_item_for_prompt(item: dict[str, Any]) -> dict[str, Any]:
    """Backward-compatible alias for older tests/imports."""
    return compressed_batch_item_for_prompt(item)


def extract_json_object(text: str | None) -> str | None:
    if not text:
        return None
    normalized = text.strip()
    if not normalized:
        return None
    if normalized.startswith('```'):
        parts = normalized.split('```')
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if part.startswith('json'):
                part = part[4:].strip()
            candidate = extract_json_object(part)
            if candidate:
                return candidate
    if normalized.startswith('[intent:'):
        newline_index = normalized.find("\n")
        if newline_index != -1:
            candidate = extract_json_object(normalized[newline_index + 1:])
            if candidate:
                return candidate
    if '[tool_code]' in normalized and '[/tool_code]' in normalized:
        start_marker = normalized.find('[tool_code]') + len('[tool_code]')
        end_marker = normalized.find('[/tool_code]', start_marker)
        if end_marker != -1:
            tool_block = normalized[start_marker:end_marker].strip()
            json_call_match = re.search(r'json\.dumps\((\{.*\})\)\s*$', tool_block, re.DOTALL)
            if json_call_match:
                candidate = extract_json_object(json_call_match.group(1))
                if candidate:
                    return candidate
            candidate = extract_json_object(tool_block)
            if candidate:
                return candidate
    start = normalized.find('{')
    if start == -1:
        return None
    depth = 0
    in_string = False
    escaped = False
    for idx in range(start, len(normalized)):
        char = normalized[idx]
        if in_string:
            if escaped:
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
            continue
        if char == '{':
            depth += 1
        elif char == '}':
            depth -= 1
            if depth == 0:
                candidate = normalized[start:idx + 1]
                try:
                    json.loads(candidate)
                    return candidate
                except json.JSONDecodeError:
                    continue
    return None


def repair_json_tail(text: str | None) -> str | None:
    if not text:
        return None
    normalized = text.strip()
    if not normalized:
        return None
    try:
        json.loads(normalized)
        return normalized
    except json.JSONDecodeError as exc:
        tail_threshold = max(len(normalized) - 64, int(len(normalized) * 0.9))
        if exc.pos < tail_threshold:
            return None

    repaired_chars: list[str] = []
    stack: list[str] = []
    in_string = False
    escaped = False
    altered = False

    for char in normalized:
        if in_string:
            repaired_chars.append(char)
            if escaped:
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
            repaired_chars.append(char)
            continue

        if char == '{':
            stack.append('}')
            repaired_chars.append(char)
            continue
        if char == '[':
            stack.append(']')
            repaired_chars.append(char)
            continue
        if char in {'}', ']'}:
            if stack and char == stack[-1]:
                stack.pop()
                repaired_chars.append(char)
                continue
            altered = True
            continue

        repaired_chars.append(char)

    if stack:
        altered = True
        repaired_chars.extend(reversed(stack))

    if not altered:
        return None

    candidate = ''.join(repaired_chars)
    try:
        json.loads(candidate)
    except json.JSONDecodeError:
        return None
    return candidate


def build_upstream_feedback_bundle(
    *,
    symbol: str,
    market_date: date,
    buffered_lookup: dict[tuple[str, str], dict[str, Any]],
    upstream_reuse_decisions: dict[str, dict[str, Any]],
    upstream_summary_roles: Sequence[str],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], list[str]]:
    upstream_summaries: dict[str, dict[str, Any]] = {}
    upstream_traceability: dict[str, dict[str, Any]] = {}
    missing_roles: list[str] = []
    symbol_decisions = upstream_reuse_decisions.get(symbol, {})

    for role in upstream_summary_roles:
        buffered_row = buffered_lookup.get((symbol, role))
        if buffered_row is not None:
            feedback_payload = buffered_row.get('feedback') if isinstance(buffered_row.get('feedback'), dict) else None
            if feedback_payload:
                upstream_summaries[role] = feedback_payload
                upstream_traceability[role] = {
                    'agent_name': role,
                    'run_id': buffered_row.get('run_id'),
                    'market_date': market_date.isoformat(),
                    'source': 'current_run_buffer',
                }
                continue

        decision = symbol_decisions.get(role)
        if decision is not None and bool(decision.should_dispatch):
            missing_roles.append(role)
            continue
        latest_feedback = decision.latest_feedback if decision is not None else None
        feedback_payload = latest_feedback.get('feedback_payload') if isinstance(latest_feedback, dict) else None
        feedback_market_date = latest_feedback.get('market_date') if isinstance(latest_feedback, dict) else None
        reuse_reason = str(getattr(decision, 'reason', '') or '')
        fingerprint_reuse_allowed = (
            role == 'fundamental-summarizer'
            and reuse_reason == 'reuse_unchanged_fundamental_inputs'
            and decision is not None
            and not bool(decision.should_dispatch)
        )
        if isinstance(feedback_payload, dict) and (
            feedback_market_date == market_date.isoformat() or fingerprint_reuse_allowed
        ):
            upstream_summaries[role] = feedback_payload
            upstream_traceability[role] = {
                'agent_name': role,
                'run_id': latest_feedback.get('run_id'),
                'market_date': feedback_market_date,
                'received_at': latest_feedback.get('received_at'),
                'feedback_id': latest_feedback.get('id'),
                'source': 'persisted_feedback_fingerprint_reuse' if fingerprint_reuse_allowed else 'persisted_feedback',
                'reuse_reason': reuse_reason if fingerprint_reuse_allowed else None,
                'input_fingerprint': getattr(decision, 'input_fingerprint', None) if fingerprint_reuse_allowed else None,
                'stored_input_fingerprint': getattr(decision, 'stored_input_fingerprint', None) if fingerprint_reuse_allowed else None,
            }
            continue

        missing_roles.append(role)

    return upstream_summaries, upstream_traceability, missing_roles


def build_signal_decider_payload(
    *,
    item: dict[str, Any],
    market_date: date,
    request_run_id: str,
    market_context: dict[str, Any] | None,
    upstream_summaries: dict[str, dict[str, Any]],
    upstream_traceability: dict[str, dict[str, Any]],
    signal_decider_agent_name: str,
    signal_decider_policy: dict[str, Any] | None = None,
    previous_decision_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    symbol_day_context = jsonable_encoder(strip_runtime_session_fields(item))
    score_context = symbol_day_context.pop('scores', {}) if isinstance(symbol_day_context.get('scores'), dict) else {}
    factor_summary = (
        symbol_day_context.pop('factor_summary', {})
        if isinstance(symbol_day_context.get('factor_summary'), dict)
        else {}
    )
    liquidity_context = (
        symbol_day_context.pop('liquidity_context', {})
        if isinstance(symbol_day_context.get('liquidity_context'), dict)
        else {}
    )
    decider_data_quality_context = (
        symbol_day_context.pop('decider_data_quality_context', {})
        if isinstance(symbol_day_context.get('decider_data_quality_context'), dict)
        else {}
    )
    decision_input_quality_context = (
        symbol_day_context.pop('decision_input_quality_context', {})
        if isinstance(symbol_day_context.get('decision_input_quality_context'), dict)
        else {}
    )
    pre_decider_enrichment_context = (
        symbol_day_context.pop('pre_decider_enrichment_context', {})
        if isinstance(symbol_day_context.get('pre_decider_enrichment_context'), dict)
        else {}
    )
    market_regime_context = (
        symbol_day_context.pop('market_regime_context', {})
        if isinstance(symbol_day_context.get('market_regime_context'), dict)
        else {}
    )
    macro_context = (
        symbol_day_context.pop('macro_context', {})
        if isinstance(symbol_day_context.get('macro_context'), dict)
        else {}
    )
    existing_signal_envelopes = (
        symbol_day_context.pop('signal_envelopes', [])
        if isinstance(symbol_day_context.get('signal_envelopes'), list)
        else []
    )
    symbol = str(item.get('symbol') or '')
    signal_envelopes = build_decider_signal_envelopes(
        symbol=symbol,
        market_date=market_date,
        factor_summary=factor_summary,
        upstream_summaries=upstream_summaries,
        upstream_traceability=upstream_traceability,
        existing_envelopes=existing_signal_envelopes,
    )
    requested_at = datetime.now(timezone.utc).isoformat()
    resolved_signal_decider_policy = jsonable_encoder(
        signal_decider_policy
        if signal_decider_policy is not None
        else (
            signal_decider_loop_service.get_active_policy_context()
            if signal_decider_agent_name == 'signal-decider'
            else {}
        )
    )
    required_feedback_fields = [
        'role',
        'agent_name',
        'symbol',
        'market_date',
        'preferred_action',
        'next_day_open_action',
        'wait_reason',
        'entry_price_basis',
        'target_price',
        'expected_return_pct',
        'stop_loss_price',
        'suggested_holding_period',
        'confidence_score_10',
        'conviction_score_10',
        'thesis_summary',
        'supporting_evidence',
        'decision_risks',
        'decision_delta',
        'invalidation_conditions',
        'data_quality_warnings',
        'specialist_disagreement',
        'traceability',
        'summary',
        'confidence',
        'risk_flags',
        'details',
        'improvement_notes',
    ]
    if signal_decider_agent_name == 'short-term-decider':
        required_feedback_fields = [
            'role',
            'agent_name',
            'symbol',
            'market_date',
            'tactical_action',
            'preferred_action',
            'wait_reason',
            'trigger_price',
            'chase_limit_price',
            'target_price',
            'stop_loss_price',
            'expected_holding_period',
            'confidence_score_10',
            'conviction_score_10',
            'thesis_summary',
            'trigger_conditions',
            'supporting_evidence',
            'tactical_risks',
            'decision_delta',
            'invalidation_conditions',
            'data_quality_warnings',
            'traceability',
            'summary',
            'confidence',
            'risk_flags',
            'details',
            'improvement_notes',
        ]

    return {
        'role': signal_decider_agent_name,
        'agent_name': signal_decider_agent_name,
        'symbol': symbol,
        'market_date': market_date.isoformat(),
        'symbol_day_context': symbol_day_context,
        'scores': jsonable_encoder(score_context),
        'factor_summary': jsonable_encoder(factor_summary),
        'liquidity_context': jsonable_encoder(liquidity_context),
        'pre_decider_enrichment_context': jsonable_encoder(pre_decider_enrichment_context),
        'market_regime_context': jsonable_encoder(market_regime_context),
        'decider_data_quality_context': jsonable_encoder(decider_data_quality_context),
        'decision_input_quality_context': jsonable_encoder(decision_input_quality_context),
        'macro_context': jsonable_encoder(macro_context),
        'signal_envelopes': jsonable_encoder(signal_envelopes),
        'market_context': jsonable_encoder(market_context or {}),
        'upstream_summaries': jsonable_encoder(upstream_summaries),
        'upstream_traceability': jsonable_encoder(upstream_traceability),
        'previous_decision_context': jsonable_encoder(previous_decision_context or {}),
        'decision_support_constraints': {
            'decision_support_only': True,
            'allow_execution_logic': False,
            'allow_position_sizing': False,
            'allow_leverage_guidance': False,
            'avoid_fake_precision': True,
            'decision_history_guidance': {
                'use_previous_decision_context_when_available': True,
                'repeat_wait_requires_delta_summary': True,
                'forbid_copy_paste_repeat_wait_explanations': True,
                'required_transition_coverage': ['wait_to_wait', 'wait_to_buy_or_watch', 'buy_or_watch_to_wait'],
                'required_delta_fields': [
                    'previous_market_date',
                    'previous_recommendation',
                    'previous_recommendation_class',
                    'previous_wait_reason',
                    'current_recommendation',
                    'current_recommendation_class',
                    'current_wait_reason',
                    'transition',
                    'persistence_trend',
                    'changed_fields',
                    'stable_fields',
                    'what_changed',
                    'what_stayed_same',
                    'upgrade_trigger',
                ],
            },
            'missing_context_policy': {
                'core_execution_blocks': ['liquidity_turnover', 'intraday_realtime'],
                'supporting_context_blocks': [
                    'factor_score_coverage',
                    'technical_evidence',
                    'flows_short_selling',
                    'sector_relative_strength',
                ],
                'auxiliary_context_blocks': ['disclosures_news', 'financials_cashflow'],
                'auxiliary_context_absence_not_tradeability_blocker': True,
                'empty_recent_news_is_neutral': True,
                'quarterly_financial_detail_confidence_only_unless_thesis_depends_on_it': True,
                'forbid_untradeable_from_auxiliary_absence_only': True,
                'pre_decider_enrichment_context_authoritative': True,
                'market_regime_context_authoritative': True,
                'market_regime_posture_values': ['normal', 'caution', 'risk_off', 'unavailable', 'stale'],
                'market_regime_exec_actions': ['allow', 'downsize', 'reject', 'no_opinion'],
                'account_for_market_regime_when_choosing_buy_wait_reject': True,
                'risk_off_default_wait_or_reject_unless_exceptional_leadership': True,
                'caution_requires_stronger_evidence_and_reduced_sizing': True,
                'weak_sector_requires_sector_leadership_or_confirmation': True,
                'macro_context_contextual_input_only': True,
                'macro_context_not_a_direct_buy_sell_signal': True,
                'macro_context_regime_summary_is_deterministic_no_llm': True,
                'signal_envelope_schema_version': SIGNAL_ENVELOPE_SCHEMA_VERSION,
                'signal_envelopes_are_compact_normalized_inputs': True,
                'signal_envelopes_do_not_replace_raw_payloads': True,
                'decision_input_quality_context_authoritative': True,
                'decision_input_quality_sections': [
                    'data_quality',
                    'market_setup_edge',
                    'execution_feasibility',
                    'final_judgment',
                ],
                'respect_decision_input_quality_hard_blocks': True,
                'pre_decider_enrichment_blocks': [
                    'quote_orderbook_snapshot',
                    'trading_value',
                    'flow_short_sector_context',
                ],
                'trading_value_source_values': ['raw', 'estimated', 'missing'],
                'do_not_emit_missing_warning_when_pre_decider_block_available': True,
            },
            'external_news_contract': {
                'materiality_values': ['material', 'noise', 'unknown'],
                'access_status_values': ['full_text', 'headline_only', 'paywalled', 'fetch_failed'],
                'material_items_first': True,
                'bounded_noise_filter_required': True,
                'source_evidence_required_for_claims': True,
                'price_forecast_from_news_alone_prohibited': True,
                'headline_only_or_paywalled_not_full_text': True,
            },
        },
        'signal_decider_policy': resolved_signal_decider_policy,
        'output_contract': {
            'batch_envelope_required': True,
            'assigned_symbol': symbol,
            'one_item_per_assigned_symbol': True,
            'symbol_identity': {
                'mandatory': True,
                'exact_match_required': True,
                'item_symbol_must_equal_assigned_symbol': True,
                'feedback_symbol_must_equal_item_symbol': True,
                'traceability_symbol_must_equal_item_symbol': True,
                'forbidden': ['omitted_symbol', 'alias_symbol', 'normalized_without_leading_zeroes', 'unrequested_symbol'],
            },
            'top_level_batch_shape': {'items': [{'symbol': symbol, 'feedback': {'symbol': symbol}}]},
            'single_symbol_batch_rule': 'Even one-symbol requests must return the batch envelope with exactly one item.',
            'failure_safe': 'If inputs are missing, weak, conflicting, or truncated, return a complete conservative wait or reject item with null unsupported numeric fields and explicit warnings.',
            'required_feedback_fields': required_feedback_fields,
            'self_check_before_final_output': [
                'output_is_parseable_json_object',
                'top_level_has_items_array',
                'exactly_one_item_for_each_assigned_symbol',
                'no_missing_or_unexpected_symbols',
                'item_symbol_feedback_symbol_and_traceability_symbol_match_assigned_symbol',
                'all_required_feedback_fields_present',
            ],
        },
        'orchestration': {
            'request_run_id': request_run_id,
            'requested_at': requested_at,
            'request_model': f'openclaw/{signal_decider_agent_name}',
            'source': 'agent_feedback_orchestrator.dispatch_for_symbols',
            'mode': 'role_sequential_runtime_buffered',
            'required_upstream_roles': list(upstream_summaries.keys()),
        },
    }
