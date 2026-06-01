from __future__ import annotations

from datetime import date
from typing import Any, Sequence

from app.market_data_store import (
    load_daily_market_risk_filter_results,
    load_daily_piotroski_filter_results,
)

AGENT_FEEDBACK_ELIGIBILITY_FILTERS: tuple[str, ...] = (
    'market_risk_filter',
    'piotroski_filter',
)
AGENT_FEEDBACK_ELIGIBILITY_RULE = 'strategy_aware_prefilter_scoring_v1'
AGENT_FEEDBACK_VALUE_LONG_ONLY_ELIGIBILITY_RULE = 'market_risk_filter_passed_and_piotroski_passed_v1'
AGENT_FEEDBACK_STRATEGY_SCOPE_SHORT_TERM_MOMENTUM = 'short_term_momentum_breakout'
AGENT_FEEDBACK_STRATEGY_SCOPE_VALUE_LONG_ONLY = 'value_long_only'
AGENT_FEEDBACK_DEFAULT_STRATEGY_SCOPE = AGENT_FEEDBACK_STRATEGY_SCOPE_SHORT_TERM_MOMENTUM
AGENT_FEEDBACK_SCOPE_ALL = 'active_strategy_aware_prefilter_scoring'
AGENT_FEEDBACK_SCOPE_REQUESTED = 'requested_symbols_scored_by_strategy_aware_prefilter'


def _normalize_symbol(value: Any) -> str:
    return str(value or '').strip().upper()


def _normalize_symbol_scope(symbols: Sequence[str] | None) -> list[str] | None:
    if symbols is None:
        return None
    normalized: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        value = _normalize_symbol(symbol)
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _rows_by_symbol(rows: Sequence[dict]) -> dict[str, dict]:
    mapped: dict[str, dict] = {}
    for row in rows:
        symbol = _normalize_symbol(row.get('symbol'))
        if symbol and symbol not in mapped:
            mapped[symbol] = dict(row)
    return mapped


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _reason(
    *,
    code: str,
    source: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        'code': code,
        'source': source,
        'severity': 'hard_block',
        'message': message,
    }
    if details:
        payload['details'] = details
    return payload


def _penalty(
    *,
    code: str,
    source: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        'code': code,
        'source': source,
        'severity': 'soft_penalty',
        'message': message,
    }
    if details:
        payload['details'] = details
    return payload


def _market_risk_filter_signals(row: dict | None) -> dict[str, Any]:
    if row is None:
        return {'available': False}
    filter_payload = _as_dict(row.get('filter_payload'))
    return {
        'available': True,
        'source': row.get('source'),
        'feature_version': row.get('feature_version') or row.get('source'),
        'market_date': row.get('market_date'),
        'risk_state': row.get('risk_state'),
        'passed_filter': row.get('passed_filter'),
        'watchlist_flagged': row.get('watchlist_flagged'),
        'blocking_reasons': list(filter_payload.get('blocking_reasons') or []),
        'watchlist_reasons': list(filter_payload.get('watchlist_reasons') or []),
        'captured_at': row.get('captured_at'),
    }


def _piotroski_filter_signals(row: dict | None) -> dict[str, Any]:
    if row is None:
        return {'available': False}
    filter_payload = _as_dict(row.get('filter_payload'))
    score_summary = _as_dict(filter_payload.get('score_summary'))
    thresholds = _as_dict(filter_payload.get('thresholds'))
    valuation = _as_dict(filter_payload.get('valuation'))
    return {
        'available': True,
        'source': row.get('source'),
        'feature_version': row.get('feature_version') or row.get('source'),
        'market_date': row.get('market_date'),
        'passed_filter': row.get('passed_filter'),
        'pbr': row.get('pbr'),
        'pbr_percentile_rank': row.get('pbr_percentile_rank'),
        'is_low_pbr': row.get('is_low_pbr'),
        'low_pbr_percentile_threshold': thresholds.get('low_pbr_percentile'),
        'has_profitability_issue': row.get('has_profitability_issue'),
        'has_capital_impairment': row.get('has_capital_impairment'),
        'has_audit_issue': row.get('has_audit_issue'),
        'piotroski_score': row.get('piotroski_score'),
        'passes_piotroski_score': row.get('passes_piotroski_score'),
        'piotroski_min_score': thresholds.get('piotroski_min_score'),
        'score_ratio': score_summary.get('score_ratio'),
        'momentum_percentile_rank': row.get('momentum_percentile_rank'),
        'is_bottom_momentum': row.get('is_bottom_momentum'),
        'valuation_available': valuation.get('available'),
        'valuation_source_status': valuation.get('source_status'),
        'captured_at': row.get('captured_at'),
    }


def _market_risk_hard_blocks(row: dict | None) -> list[dict[str, Any]]:
    if row is None:
        return [
            _reason(
                code='missing_market_risk_filter',
                source='market_risk_filter',
                message='Market-risk filter row is missing, so the symbol cannot be dispatched safely.',
            )
        ]
    if bool(row.get('passed_filter')):
        return []
    filter_payload = _as_dict(row.get('filter_payload'))
    blocking_reasons = list(filter_payload.get('blocking_reasons') or [])
    if not blocking_reasons:
        blocking_reasons = ['market_risk_filter_failed']
    return [
        _reason(
            code=f'market_risk_{reason}',
            source='market_risk_filter',
            message=f'Market-risk filter hard blocker: {reason}.',
            details={
                'risk_state': row.get('risk_state'),
                'passed_filter': row.get('passed_filter'),
            },
        )
        for reason in blocking_reasons
    ]


def _market_risk_soft_penalties(row: dict | None) -> list[dict[str, Any]]:
    if row is None or not bool(row.get('passed_filter')) or not bool(row.get('watchlist_flagged')):
        return []
    filter_payload = _as_dict(row.get('filter_payload'))
    watchlist_reasons = list(filter_payload.get('watchlist_reasons') or [])
    if not watchlist_reasons:
        watchlist_reasons = ['market_risk_watchlist']
    return [
        _penalty(
            code=f'market_risk_watchlist_{reason}',
            source='market_risk_filter',
            message=f'Market-risk watchlist signal carried as a soft penalty: {reason}.',
            details={
                'risk_state': row.get('risk_state'),
                'watchlist_flagged': row.get('watchlist_flagged'),
            },
        )
        for reason in watchlist_reasons
    ]


def _piotroski_hard_blocks_for_value_scope(row: dict | None) -> list[dict[str, Any]]:
    if row is None:
        return [
            _reason(
                code='missing_piotroski_filter_for_value_scope',
                source='piotroski_filter',
                message='Piotroski/value filter row is missing for the strict value/long-only scope.',
            )
        ]
    if bool(row.get('passed_filter')):
        return []
    return [
        _reason(
            code='piotroski_filter_failed_for_value_scope',
            source='piotroski_filter',
            message='Strict value/long-only scope preserves the legacy Piotroski/value hard gate.',
            details=_piotroski_filter_signals(row),
        )
    ]


def _piotroski_soft_penalties(row: dict | None) -> list[dict[str, Any]]:
    if row is None:
        return [
            _penalty(
                code='missing_piotroski_filter',
                source='piotroski_filter',
                message='Piotroski/value filter row is missing; dispatch can continue with a quality-data penalty.',
            )
        ]
    if bool(row.get('passed_filter')):
        return []

    penalties = [
        _penalty(
            code='piotroski_filter_failed',
            source='piotroski_filter',
            message='Piotroski/value filter did not pass; short-term momentum dispatch keeps the symbol with this penalty.',
            details=_piotroski_filter_signals(row),
        )
    ]
    filter_payload = _as_dict(row.get('filter_payload'))
    thresholds = _as_dict(filter_payload.get('thresholds'))
    if row.get('is_low_pbr') is False:
        penalties.append(
            _penalty(
                code='not_low_pbr',
                source='piotroski_filter',
                message='Symbol is not in the low-PBR value bucket.',
                details={
                    'pbr': row.get('pbr'),
                    'pbr_percentile_rank': row.get('pbr_percentile_rank'),
                    'low_pbr_percentile_threshold': thresholds.get('low_pbr_percentile'),
                },
            )
        )
    if row.get('passes_piotroski_score') is False:
        penalties.append(
            _penalty(
                code='low_piotroski_score',
                source='piotroski_filter',
                message='Piotroski score does not meet the value filter threshold.',
                details={
                    'piotroski_score': row.get('piotroski_score'),
                    'piotroski_min_score': thresholds.get('piotroski_min_score'),
                },
            )
        )
    for field, message in (
        ('has_profitability_issue', 'Profitability issue flagged by the Piotroski/value filter.'),
        ('has_capital_impairment', 'Capital impairment risk flagged by the Piotroski/value filter.'),
        ('has_audit_issue', 'Audit issue flagged by the Piotroski/value filter.'),
        ('is_bottom_momentum', 'Bottom-momentum signal flagged by the Piotroski/value filter.'),
    ):
        if row.get(field) is True:
            penalties.append(
                _penalty(
                    code=field,
                    source='piotroski_filter',
                    message=message,
                )
            )
    return penalties


def _record_order(
    *,
    requested_symbols: Sequence[str] | None,
    market_risk_rows: Sequence[dict],
    piotroski_rows: Sequence[dict],
) -> list[str]:
    if requested_symbols is not None:
        return list(requested_symbols)
    ordered: list[str] = []
    seen: set[str] = set()
    for row in [*market_risk_rows, *piotroski_rows]:
        symbol = _normalize_symbol(row.get('symbol'))
        if symbol and symbol not in seen:
            seen.add(symbol)
            ordered.append(symbol)
    return ordered


def _build_eligibility_record(
    *,
    symbol: str,
    market_date: date,
    strategy_scope: str,
    market_risk_row: dict | None,
    piotroski_row: dict | None,
) -> dict[str, Any]:
    hard_block_reasons = _market_risk_hard_blocks(market_risk_row)
    soft_penalties = _market_risk_soft_penalties(market_risk_row)
    if strategy_scope == AGENT_FEEDBACK_STRATEGY_SCOPE_VALUE_LONG_ONLY:
        hard_block_reasons.extend(_piotroski_hard_blocks_for_value_scope(piotroski_row))
        rule_version = AGENT_FEEDBACK_VALUE_LONG_ONLY_ELIGIBILITY_RULE
        candidate_path = AGENT_FEEDBACK_STRATEGY_SCOPE_VALUE_LONG_ONLY
    else:
        soft_penalties.extend(_piotroski_soft_penalties(piotroski_row))
        rule_version = AGENT_FEEDBACK_ELIGIBILITY_RULE
        candidate_path = AGENT_FEEDBACK_STRATEGY_SCOPE_SHORT_TERM_MOMENTUM

    hard_blocked = bool(hard_block_reasons)
    status = (
        'hard_blocked'
        if hard_blocked
        else 'soft_penalized_dispatched'
        if soft_penalties
        else 'dispatched'
    )
    return {
        'symbol': symbol,
        'market_date': market_date.isoformat(),
        'strategy_scope': strategy_scope,
        'candidate_path': candidate_path,
        'rule_version': rule_version,
        'eligible': not hard_blocked,
        'hard_blocked': hard_blocked,
        'status': status,
        'hard_block_reasons': hard_block_reasons,
        'soft_penalties': soft_penalties,
        'filter_signals': {
            'market_risk_filter': _market_risk_filter_signals(market_risk_row),
            'piotroski_filter': _piotroski_filter_signals(piotroski_row),
        },
    }


def load_agent_feedback_eligibility_records(
    *,
    market_date: date,
    requested_symbols: Sequence[str] | None = None,
    strategy_scope: str = AGENT_FEEDBACK_DEFAULT_STRATEGY_SCOPE,
) -> list[dict[str, Any]]:
    symbol_scope = _normalize_symbol_scope(requested_symbols)
    if requested_symbols is not None and not symbol_scope:
        return []
    piotroski_rows = load_daily_piotroski_filter_results(
        market_date=market_date,
        symbols=symbol_scope,
        passed_filter=None,
        limit=None,
    )
    market_risk_rows = load_daily_market_risk_filter_results(
        market_date=market_date,
        symbols=symbol_scope,
        passed_filter=None,
        limit=None,
    )
    piotroski_by_symbol = _rows_by_symbol(piotroski_rows)
    market_risk_by_symbol = _rows_by_symbol(market_risk_rows)
    return [
        _build_eligibility_record(
            symbol=symbol,
            market_date=market_date,
            strategy_scope=strategy_scope,
            market_risk_row=market_risk_by_symbol.get(symbol),
            piotroski_row=piotroski_by_symbol.get(symbol),
        )
        for symbol in _record_order(
            requested_symbols=symbol_scope,
            market_risk_rows=market_risk_rows,
            piotroski_rows=piotroski_rows,
        )
    ]


def build_agent_feedback_eligibility_diagnostics(
    *,
    records: Sequence[dict[str, Any]],
    requested_symbols: Sequence[str] | None = None,
) -> dict[str, Any]:
    normalized_requested = _normalize_symbol_scope(requested_symbols)
    hard_blocked = [record for record in records if bool(record.get('hard_blocked'))]
    dispatched = [record for record in records if not bool(record.get('hard_blocked'))]
    soft_penalized = [record for record in dispatched if record.get('soft_penalties')]
    rule_version = records[0].get('rule_version') if records else AGENT_FEEDBACK_ELIGIBILITY_RULE
    strategy_scope = records[0].get('strategy_scope') if records else AGENT_FEEDBACK_DEFAULT_STRATEGY_SCOPE
    return {
        'rule_version': rule_version,
        'strategy_scope': strategy_scope,
        'requested_symbol_count': len(normalized_requested or []),
        'evaluated_symbol_count': len(records),
        'dispatched_symbol_count': len(dispatched),
        'hard_blocked_symbol_count': len(hard_blocked),
        'soft_penalized_dispatched_symbol_count': len(soft_penalized),
        'hard_blocked_symbols': [str(record.get('symbol')) for record in hard_blocked],
        'soft_penalized_symbols': [str(record.get('symbol')) for record in soft_penalized],
        'hard_block_reasons_by_symbol': {
            str(record.get('symbol')): list(record.get('hard_block_reasons') or [])
            for record in hard_blocked
        },
        'soft_penalties_by_symbol': {
            str(record.get('symbol')): list(record.get('soft_penalties') or [])
            for record in soft_penalized
        },
    }


def resolve_agent_feedback_scope(*, requested_symbols: Sequence[str] | None) -> str:
    return AGENT_FEEDBACK_SCOPE_REQUESTED if requested_symbols is not None else AGENT_FEEDBACK_SCOPE_ALL


def load_agent_feedback_eligible_symbols(
    *,
    market_date: date,
    requested_symbols: Sequence[str] | None = None,
    strategy_scope: str = AGENT_FEEDBACK_DEFAULT_STRATEGY_SCOPE,
) -> list[str]:
    records = load_agent_feedback_eligibility_records(
        market_date=market_date,
        requested_symbols=requested_symbols,
        strategy_scope=strategy_scope,
    )
    return [str(record['symbol']) for record in records if not bool(record.get('hard_blocked'))]


def split_requested_symbols_by_agent_feedback_eligibility(
    *,
    market_date: date,
    requested_symbols: Sequence[str],
    strategy_scope: str = AGENT_FEEDBACK_DEFAULT_STRATEGY_SCOPE,
) -> tuple[list[str], list[str]]:
    eligible_symbols, skipped_symbols, _ = split_requested_symbols_by_agent_feedback_eligibility_with_diagnostics(
        market_date=market_date,
        requested_symbols=requested_symbols,
        strategy_scope=strategy_scope,
    )
    return eligible_symbols, skipped_symbols


def split_requested_symbols_by_agent_feedback_eligibility_with_diagnostics(
    *,
    market_date: date,
    requested_symbols: Sequence[str],
    strategy_scope: str = AGENT_FEEDBACK_DEFAULT_STRATEGY_SCOPE,
) -> tuple[list[str], list[str], dict[str, Any]]:
    normalized_requested = _normalize_symbol_scope(requested_symbols) or []
    records = load_agent_feedback_eligibility_records(
        market_date=market_date,
        requested_symbols=normalized_requested,
        strategy_scope=strategy_scope,
    )
    records_by_symbol = {str(record.get('symbol')): record for record in records}
    eligible_symbols = [
        symbol
        for symbol in normalized_requested
        if symbol in records_by_symbol and not bool(records_by_symbol[symbol].get('hard_blocked'))
    ]
    eligible_symbol_set = set(eligible_symbols)
    skipped_symbols = [symbol for symbol in normalized_requested if symbol not in eligible_symbol_set]
    diagnostics = build_agent_feedback_eligibility_diagnostics(
        records=records,
        requested_symbols=normalized_requested,
    )
    diagnostics['records_by_symbol'] = records_by_symbol
    return eligible_symbols, skipped_symbols, diagnostics
