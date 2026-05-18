from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

DECISION_INPUT_QUALITY_SCHEMA_VERSION = 'decision_input_quality_v1'

STATUS_AVAILABLE = 'available'
STATUS_PARTIAL = 'partial'
STATUS_STALE = 'stale'
STATUS_EMPTY = 'empty'
STATUS_UNAVAILABLE = 'unavailable'
STATUS_PROVIDER_ERROR = 'provider_error'
STATUS_NOT_APPLICABLE = 'not_applicable'

EFFECT_CONFIRMED = 'confirmed'
EFFECT_NEUTRAL_SECONDARY_CONTEXT = 'neutral_secondary_context'
EFFECT_MINOR_DISCOUNT = 'minor_discount'
EFFECT_MAJOR_DISCOUNT = 'major_discount'
EFFECT_HARD_BLOCK = 'hard_block'

SOURCE_QUALITY_PROVIDER = 'provider_sourced'
SOURCE_QUALITY_ESTIMATED_FALLBACK = 'estimated_fallback'
SOURCE_QUALITY_FALLBACK = 'fallback'
SOURCE_QUALITY_DERIVED = 'derived'
SOURCE_QUALITY_MISSING = 'missing'
SOURCE_QUALITY_PROVIDER_ERROR = 'provider_error'
SOURCE_QUALITY_NOT_APPLICABLE = 'not_applicable'
SOURCE_QUALITY_UNKNOWN = 'unknown'

CORE_BLOCKS = frozenset({'liquidity_turnover', 'intraday_realtime'})
MARKET_SETUP_BLOCKS = frozenset(
    {
        'factor_score_coverage',
        'flows_short_selling',
        'sector_relative_strength',
        'technical_evidence',
    }
)
AUXILIARY_BLOCKS = frozenset({'disclosures_news', 'financials_cashflow'})

RAW_TURNOVER_SAFE_NOTIONAL_RATE = Decimal('0.005')
ESTIMATED_TURNOVER_SAFE_NOTIONAL_RATE = Decimal('0.002')


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _as_decimal(value: Any) -> Decimal | None:
    if value in (None, ''):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _as_float(value: Any) -> float | None:
    decimal_value = _as_decimal(value)
    return float(decimal_value) if decimal_value is not None else None


def _as_int(value: Any) -> int | None:
    decimal_value = _as_decimal(value)
    if decimal_value is None:
        return None
    return int(decimal_value)


def _dedupe_text(items: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = str(item or '').strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _first_present(*values: Any) -> Any:
    for value in values:
        if value not in (None, '', [], {}):
            return value
    return None


def _freshness_from_block(block: dict[str, Any]) -> str | None:
    candidates = [
        _as_dict(block.get('freshness')).get('freshness_status'),
        _as_dict(_as_dict(block.get('turnover')).get('freshness')).get('freshness_status'),
        _as_dict(_as_dict(block.get('liquidity_ranking')).get('freshness')).get('freshness_status'),
        _as_dict(_as_dict(block.get('intraday_liquidity')).get('freshness')).get('freshness_status'),
        _as_dict(_as_dict(block.get('quote')).get('freshness')).get('freshness_status'),
        _as_dict(_as_dict(block.get('intraday_snapshot')).get('freshness')).get('freshness_status'),
        _as_dict(_as_dict(block.get('market_relative')).get('freshness')).get('freshness_status'),
        _as_dict(_as_dict(block.get('sector_peers')).get('freshness')).get('freshness_status'),
        _as_dict(_as_dict(block.get('market_breadth')).get('freshness')).get('freshness_status'),
        _as_dict(_as_dict(block.get('investor_flow')).get('freshness')).get('freshness_status'),
        _as_dict(_as_dict(block.get('short_selling')).get('freshness')).get('freshness_status'),
    ]
    return _as_text(_first_present(*candidates))


def _source_quality_from_block(block_name: str, block: dict[str, Any]) -> str:
    status = str(block.get('status') or '').strip().lower()
    if status == STATUS_PROVIDER_ERROR:
        return SOURCE_QUALITY_PROVIDER_ERROR
    if status == STATUS_NOT_APPLICABLE:
        return SOURCE_QUALITY_NOT_APPLICABLE

    turnover = _as_dict(block.get('turnover'))
    trading_value_source = str(turnover.get('trading_value_source') or '').strip().lower()
    if trading_value_source == 'raw':
        return SOURCE_QUALITY_PROVIDER
    if trading_value_source == 'estimated':
        return SOURCE_QUALITY_ESTIMATED_FALLBACK
    if trading_value_source == 'missing':
        return SOURCE_QUALITY_MISSING

    freshness = _as_dict(block.get('freshness'))
    provenance_status = str(freshness.get('provenance_status') or '').strip().lower()
    issue_types = {str(item).strip().lower() for item in _as_list(freshness.get('issue_types'))}
    if 'fallback' in provenance_status or 'derived_or_fallback' in issue_types:
        return SOURCE_QUALITY_FALLBACK
    if provenance_status == 'derived_from_provider_data':
        return SOURCE_QUALITY_DERIVED
    if provenance_status == 'provider_sourced':
        return SOURCE_QUALITY_PROVIDER

    if block_name == 'disclosures_news' and status == STATUS_EMPTY:
        return SOURCE_QUALITY_NOT_APPLICABLE
    if status in {STATUS_UNAVAILABLE, STATUS_EMPTY}:
        return SOURCE_QUALITY_MISSING
    return SOURCE_QUALITY_UNKNOWN


def _block_reason(block_name: str, block: dict[str, Any], *, freshness_status: str | None, source_quality: str) -> list[str]:
    reasons = _dedupe_text(
        [
            _as_text(block.get('reason')) or '',
            _as_text(_as_dict(block.get('turnover')).get('reason')) or '',
            _as_text(_as_dict(block.get('liquidity_ranking')).get('reason')) or '',
            _as_text(_as_dict(block.get('intraday_liquidity')).get('reason')) or '',
            _as_text(_as_dict(block.get('quote')).get('reason')) or '',
            _as_text(_as_dict(block.get('intraday_snapshot')).get('reason')) or '',
        ]
    )
    if not reasons:
        status = str(block.get('status') or STATUS_UNAVAILABLE)
        reasons.append(f'{block_name} status is {status}.')
    if freshness_status in {'stale', 'available_but_old'} and not any('stale' in reason for reason in reasons):
        reasons.append(f'{block_name} freshness is {freshness_status}.')
    if source_quality in {SOURCE_QUALITY_FALLBACK, SOURCE_QUALITY_ESTIMATED_FALLBACK}:
        reasons.append(f'{block_name} depends on {source_quality} source quality.')
    return reasons


def _effect_for_block(block_name: str, block: dict[str, Any], *, source_quality: str) -> tuple[str, int]:
    status = str(block.get('status') or STATUS_UNAVAILABLE).strip().lower()
    if block_name == 'disclosures_news':
        if status in {STATUS_EMPTY, STATUS_UNAVAILABLE, STATUS_NOT_APPLICABLE}:
            return EFFECT_NEUTRAL_SECONDARY_CONTEXT, 0
        if status == STATUS_PROVIDER_ERROR:
            return EFFECT_MINOR_DISCOUNT, 3
    if block_name in AUXILIARY_BLOCKS and status in {STATUS_EMPTY, STATUS_UNAVAILABLE, STATUS_NOT_APPLICABLE}:
        return EFFECT_NEUTRAL_SECONDARY_CONTEXT, 0
    if source_quality == SOURCE_QUALITY_ESTIMATED_FALLBACK:
        return EFFECT_MAJOR_DISCOUNT, 20
    if source_quality == SOURCE_QUALITY_FALLBACK:
        return EFFECT_MAJOR_DISCOUNT, 25
    if source_quality == SOURCE_QUALITY_MISSING and block_name == 'liquidity_turnover':
        return EFFECT_HARD_BLOCK, 40
    if block_name in CORE_BLOCKS and status in {STATUS_STALE, STATUS_UNAVAILABLE, STATUS_PROVIDER_ERROR}:
        return EFFECT_HARD_BLOCK, 35
    if block_name in CORE_BLOCKS and status == STATUS_PARTIAL:
        return EFFECT_MAJOR_DISCOUNT, 18
    if status == STATUS_AVAILABLE:
        return EFFECT_CONFIRMED, 0
    if status == STATUS_PARTIAL:
        return EFFECT_MINOR_DISCOUNT, 8
    if status in {STATUS_STALE, STATUS_PROVIDER_ERROR, STATUS_UNAVAILABLE}:
        return EFFECT_MINOR_DISCOUNT, 12
    return EFFECT_NEUTRAL_SECONDARY_CONTEXT, 0


def _normalize_quality_blocks(decider_data_quality_context: dict[str, Any]) -> dict[str, dict[str, Any]]:
    blocks = _as_dict(decider_data_quality_context.get('blocks'))
    normalized: dict[str, dict[str, Any]] = {}
    for block_name, raw_block in blocks.items():
        block = _as_dict(raw_block)
        guidance = _as_dict(block.get('decision_guidance'))
        category = str(guidance.get('category') or block_name)
        freshness_status = _freshness_from_block(block)
        source_quality = _source_quality_from_block(str(block_name), block)
        effect, penalty = _effect_for_block(str(block_name), block, source_quality=source_quality)
        normalized[str(block_name)] = {
            'category': category,
            'freshness_status': freshness_status,
            'source_quality': source_quality,
            'coverage_status': block.get('status') or STATUS_UNAVAILABLE,
            'decision_tier': guidance.get('decision_tier'),
            'decision_effect': effect,
            'confidence_penalty': penalty,
            'reasons': _block_reason(
                str(block_name),
                block,
                freshness_status=freshness_status,
                source_quality=source_quality,
            ),
        }
    return normalized


def _quote_context(pre_decider_enrichment_context: dict[str, Any]) -> dict[str, Any]:
    return _as_dict(pre_decider_enrichment_context.get('quote_orderbook_snapshot'))


def _trading_value_context(pre_decider_enrichment_context: dict[str, Any]) -> dict[str, Any]:
    return _as_dict(pre_decider_enrichment_context.get('trading_value'))


def _is_quote_stale_or_fallback(quote: dict[str, Any]) -> tuple[bool, list[str]]:
    status = str(quote.get('status') or '').strip().lower()
    snapshot_type = str(quote.get('snapshot_type') or '').strip().lower()
    quote_freshness_status = str(quote.get('quote_freshness_status') or '').strip().lower()
    reasons: list[str] = []
    if status in {'stale', 'stale_eod'} or quote_freshness_status in {'stale', 'available_but_old'}:
        reasons.append('quote_orderbook_snapshot is stale.')
    if 'fallback' in snapshot_type or snapshot_type in {'eod_stale'}:
        reasons.append(f'quote_orderbook_snapshot uses {snapshot_type} pricing context.')
    freshness = _as_dict(quote.get('freshness'))
    for nested in freshness.values():
        nested_payload = _as_dict(nested)
        issue_types = {str(item).strip().lower() for item in _as_list(nested_payload.get('issue_types'))}
        provenance_status = str(nested_payload.get('provenance_status') or '').strip().lower()
        if 'derived_or_fallback' in issue_types or 'fallback' in provenance_status:
            reasons.append('quote_orderbook_snapshot provenance includes fallback data.')
    return bool(reasons), _dedupe_text(reasons)


def _liquidity_tier(trading_value_krw: int | None, avg_trading_value_20d: float | None) -> str:
    reference_value = trading_value_krw if trading_value_krw is not None else avg_trading_value_20d
    if reference_value is None:
        return 'unknown'
    if reference_value >= 10_000_000_000:
        return 'high'
    if reference_value >= 2_000_000_000:
        return 'medium'
    if reference_value >= 300_000_000:
        return 'weak'
    return 'very_weak'


def _safe_notional(
    *,
    trading_value_source: str,
    raw_trading_value_krw: int | None,
    estimated_trading_value_krw: int | None,
    avg_trading_value_20d: float | None,
) -> int | None:
    if trading_value_source == 'raw' and raw_trading_value_krw is not None:
        return int(Decimal(raw_trading_value_krw) * RAW_TURNOVER_SAFE_NOTIONAL_RATE)
    fallback_value = estimated_trading_value_krw
    if fallback_value is None and avg_trading_value_20d is not None:
        fallback_value = int(avg_trading_value_20d)
    if fallback_value is not None:
        return int(Decimal(fallback_value) * ESTIMATED_TURNOVER_SAFE_NOTIONAL_RATE)
    return None


def _execution_feasibility(
    *,
    decider_data_quality_context: dict[str, Any],
    pre_decider_enrichment_context: dict[str, Any],
    allocation_budget_krw: Decimal | None,
    estimated_reference_price: Decimal | None,
    planned_notional_krw: Decimal | None,
) -> dict[str, Any]:
    quote = _quote_context(pre_decider_enrichment_context)
    trading_value = _trading_value_context(pre_decider_enrichment_context)
    liquidity_block = _as_dict(_as_dict(decider_data_quality_context.get('blocks')).get('liquidity_turnover'))
    liquidity_ranking = _as_dict(liquidity_block.get('liquidity_ranking'))
    turnover = _as_dict(liquidity_block.get('turnover'))

    trading_value_source = str(
        trading_value.get('source')
        or turnover.get('trading_value_source')
        or 'missing'
    ).strip().lower()
    raw_trading_value_krw = _as_int(
        _first_present(
            trading_value.get('raw_trading_value_krw'),
            turnover.get('raw_trading_value_krw'),
            trading_value.get('trading_value_krw') if trading_value_source == 'raw' else None,
        )
    )
    estimated_trading_value_krw = _as_int(
        _first_present(
            trading_value.get('estimated_trading_value_krw'),
            turnover.get('estimated_trading_value_krw'),
            trading_value.get('trading_value_krw') if trading_value_source == 'estimated' else None,
        )
    )
    avg_trading_value_20d = _as_float(liquidity_ranking.get('avg_trading_value_20d'))
    liquidity_tier = _liquidity_tier(raw_trading_value_krw or estimated_trading_value_krw, avg_trading_value_20d)
    max_safe_notional_krw = _safe_notional(
        trading_value_source=trading_value_source,
        raw_trading_value_krw=raw_trading_value_krw,
        estimated_trading_value_krw=estimated_trading_value_krw,
        avg_trading_value_20d=avg_trading_value_20d,
    )
    spread_bps = _as_float(_first_present(quote.get('spread_bps'), quote.get('quote_spread_bps_of_price')))
    if spread_bps is None:
        spread_bps = {
            'high': 10.0,
            'medium': 25.0,
            'weak': 75.0,
            'very_weak': 150.0,
        }.get(liquidity_tier)

    blocking_reasons: list[dict[str, Any]] = []
    quote_degraded, quote_reasons = _is_quote_stale_or_fallback(quote)
    if quote_degraded:
        blocking_reasons.append(
            {
                'code': 'quote_stale_or_fallback',
                'decision_effect': EFFECT_MAJOR_DISCOUNT,
                'reasons': quote_reasons,
            }
        )
    if trading_value_source == 'missing':
        blocking_reasons.append(
            {
                'code': 'source_turnover_missing',
                'decision_effect': EFFECT_HARD_BLOCK,
                'reasons': ['Raw source trading value is missing; do not infer buy quality from missing turnover.'],
            }
        )
    elif trading_value_source != 'raw':
        blocking_reasons.append(
            {
                'code': 'source_turnover_not_raw',
                'decision_effect': EFFECT_MAJOR_DISCOUNT,
                'reasons': [f'Trading value source is {trading_value_source}, not raw provider turnover.'],
            }
        )

    comparable_notional = planned_notional_krw or allocation_budget_krw
    sizing_hints: list[dict[str, Any]] = []
    if max_safe_notional_krw is not None:
        hint: dict[str, Any] = {
            'code': 'max_safe_notional',
            'max_safe_notional_krw': max_safe_notional_krw,
            'basis': (
                'raw_trading_value_0_5pct'
                if trading_value_source == 'raw' and raw_trading_value_krw is not None
                else 'estimated_or_average_trading_value_0_2pct'
            ),
        }
        if estimated_reference_price is not None and estimated_reference_price > 0:
            hint['max_safe_quantity'] = int((Decimal(max_safe_notional_krw) / estimated_reference_price).to_integral_value(rounding='ROUND_FLOOR'))
        if comparable_notional is not None and comparable_notional > Decimal(max_safe_notional_krw):
            hint['budget_exceeds_safe_notional'] = True
            hint['requested_or_planned_notional_krw'] = str(comparable_notional)
            blocking_reasons.append(
                {
                    'code': 'requested_notional_exceeds_liquidity_hint',
                    'decision_effect': EFFECT_MAJOR_DISCOUNT,
                    'reasons': ['Requested or planned notional exceeds the liquidity-derived safe notional hint.'],
                }
            )
        sizing_hints.append(hint)
    else:
        sizing_hints.append(
            {
                'code': 'max_safe_notional_unavailable',
                'reason': 'No raw or estimated trading value was available for sizing hints.',
            }
        )

    score = 100
    score -= 35 if any(item['code'] == 'source_turnover_missing' for item in blocking_reasons) else 0
    score -= 20 if quote_degraded else 0
    score -= 15 if trading_value_source not in {'raw', 'missing'} else 0
    score -= {'high': 0, 'medium': 8, 'weak': 20, 'very_weak': 35, 'unknown': 25}.get(liquidity_tier, 20)
    score -= 8 if spread_bps is not None and spread_bps >= 100 else 0
    score = max(0, min(100, score))

    return {
        'score': score,
        'liquidity_tier': liquidity_tier,
        'max_safe_notional_krw': max_safe_notional_krw,
        'expected_slippage_bps': spread_bps,
        'blocking_reasons': blocking_reasons,
        'sizing_hints': sizing_hints,
        'inputs': {
            'trading_value_source': trading_value_source,
            'raw_trading_value_krw': raw_trading_value_krw,
            'estimated_trading_value_krw': estimated_trading_value_krw,
            'avg_trading_value_20d': avg_trading_value_20d,
            'quote_status': quote.get('status'),
            'quote_snapshot_type': quote.get('snapshot_type'),
            'allocation_budget_krw': str(allocation_budget_krw) if allocation_budget_krw is not None else None,
            'planned_notional_krw': str(planned_notional_krw) if planned_notional_krw is not None else None,
            'estimated_reference_price': str(estimated_reference_price) if estimated_reference_price is not None else None,
        },
    }


def _market_setup_edge(normalized_blocks: dict[str, dict[str, Any]]) -> dict[str, Any]:
    confirmations: list[str] = []
    penalties: list[dict[str, Any]] = []
    missing_context: list[str] = []
    for block_name in MARKET_SETUP_BLOCKS:
        block = normalized_blocks.get(block_name)
        if not block:
            continue
        if block['decision_effect'] == EFFECT_CONFIRMED:
            confirmations.append(f'{block_name}_confirmed')
        elif block['decision_effect'] in {EFFECT_MINOR_DISCOUNT, EFFECT_MAJOR_DISCOUNT}:
            penalties.append(
                {
                    'code': f'{block_name}_degraded',
                    'category': block['category'],
                    'confidence_penalty': block['confidence_penalty'],
                    'reasons': block['reasons'],
                }
            )
            missing_context.append(block_name)
    score = max(0, 100 - sum(int(item['confidence_penalty']) for item in penalties))
    return {
        'score': score,
        'positive_confirmations': confirmations,
        'quality_penalties': penalties,
        'missing_context': missing_context,
    }


def _final_judgment(
    *,
    normalized_blocks: dict[str, dict[str, Any]],
    execution_feasibility: dict[str, Any],
    market_setup_edge: dict[str, Any],
) -> dict[str, Any]:
    hard_blocks: list[dict[str, Any]] = []
    quality_penalties: list[dict[str, Any]] = []
    positive_confirmations: list[str] = list(market_setup_edge.get('positive_confirmations') or [])
    missing_context: list[str] = list(market_setup_edge.get('missing_context') or [])
    secondary_context: list[dict[str, Any]] = []

    for block_name, block in normalized_blocks.items():
        if block['decision_effect'] == EFFECT_CONFIRMED:
            positive_confirmations.append(f'{block_name}_available')
            continue
        if block['decision_effect'] == EFFECT_HARD_BLOCK:
            hard_blocks.append(
                {
                    'code': f'{block_name}_hard_block',
                    'category': block['category'],
                    'reasons': block['reasons'],
                }
            )
            missing_context.append(block_name)
        elif block['decision_effect'] in {EFFECT_MINOR_DISCOUNT, EFFECT_MAJOR_DISCOUNT}:
            quality_penalties.append(
                {
                    'code': f'{block_name}_quality_penalty',
                    'category': block['category'],
                    'decision_effect': block['decision_effect'],
                    'confidence_penalty': block['confidence_penalty'],
                    'reasons': block['reasons'],
                }
            )
            missing_context.append(block_name)
        elif block['decision_effect'] == EFFECT_NEUTRAL_SECONDARY_CONTEXT:
            secondary_context.append(
                {
                    'block_name': block_name,
                    'category': block['category'],
                    'coverage_status': block['coverage_status'],
                    'decision_effect': block['decision_effect'],
                    'confidence_penalty': 0,
                }
            )

    for reason in execution_feasibility.get('blocking_reasons') or []:
        code = str(_as_dict(reason).get('code') or '')
        effect = str(_as_dict(reason).get('decision_effect') or '')
        if effect == EFFECT_HARD_BLOCK:
            hard_blocks.append(reason)
        else:
            quality_penalties.append(
                {
                    'code': code or 'execution_feasibility_penalty',
                    'category': 'execution_feasibility',
                    'decision_effect': effect or EFFECT_MAJOR_DISCOUNT,
                    'confidence_penalty': 25 if effect == EFFECT_MAJOR_DISCOUNT else 12,
                    'reasons': list(_as_dict(reason).get('reasons') or []),
                }
            )

    total_penalty = sum(int(item.get('confidence_penalty') or 0) for item in quality_penalties)
    score = min(
        int(execution_feasibility.get('score') or 0),
        int(market_setup_edge.get('score') or 0),
        max(0, 100 - total_penalty),
    )
    if hard_blocks:
        score = min(score, 35)
    if hard_blocks:
        decision_band = 'blocked'
    elif score >= 80:
        decision_band = 'qualified'
    elif score >= 55:
        decision_band = 'discounted'
    else:
        decision_band = 'weak_quality'
    return {
        'buy_quality_score': score,
        'decision_band': decision_band,
        'hard_blocks': hard_blocks,
        'quality_penalties': quality_penalties,
        'positive_confirmations': _dedupe_text(positive_confirmations),
        'missing_context': _dedupe_text(missing_context),
        'secondary_context': secondary_context,
    }


def build_decision_input_quality_envelope(
    *,
    symbol: str | None = None,
    market_date: str | None = None,
    decider_data_quality_context: dict[str, Any] | None = None,
    pre_decider_enrichment_context: dict[str, Any] | None = None,
    allocation_budget_krw: Any = None,
    estimated_reference_price: Any = None,
    planned_notional_krw: Any = None,
) -> dict[str, Any]:
    quality_context = _as_dict(decider_data_quality_context)
    pre_decider_context = _as_dict(pre_decider_enrichment_context)
    normalized_blocks = _normalize_quality_blocks(quality_context)
    allocation_budget = _as_decimal(allocation_budget_krw)
    reference_price = _as_decimal(estimated_reference_price)
    planned_notional = _as_decimal(planned_notional_krw)
    execution_feasibility = _execution_feasibility(
        decider_data_quality_context=quality_context,
        pre_decider_enrichment_context=pre_decider_context,
        allocation_budget_krw=allocation_budget,
        estimated_reference_price=reference_price,
        planned_notional_krw=planned_notional,
    )
    market_setup_edge = _market_setup_edge(normalized_blocks)
    final_judgment = _final_judgment(
        normalized_blocks=normalized_blocks,
        execution_feasibility=execution_feasibility,
        market_setup_edge=market_setup_edge,
    )
    return {
        'schema_version': DECISION_INPUT_QUALITY_SCHEMA_VERSION,
        'symbol': symbol,
        'market_date': market_date,
        'quality_provenance': {
            'source_contexts': [
                'decider_data_quality_context',
                'pre_decider_enrichment_context',
                'next_open_validation_context',
            ],
            'builder': 'app.agent_context.decision_input_quality.build_decision_input_quality_envelope',
            'deterministic': True,
        },
        'data_quality': {
            'status': quality_context.get('status') or STATUS_UNAVAILABLE,
            'blocks': normalized_blocks,
        },
        'market_setup_edge': market_setup_edge,
        'execution_feasibility': execution_feasibility,
        'final_judgment': final_judgment,
    }
