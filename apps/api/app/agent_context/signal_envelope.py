from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Sequence

SIGNAL_ENVELOPE_SCHEMA_VERSION = 'signal_envelope_v1'
DEFAULT_SIGNAL_ENVELOPE_HORIZON = '5d'
SUMMARY_MAX_LENGTH = 240
EVIDENCE_REF_LIMIT = 5

SOURCE_TYPE_VALUES: tuple[str, ...] = ('agent', 'factor', 'news', 'macro', 'risk')

POSITIVE_ACTION_VALUES: dict[str, float] = {
    'buy': 0.75,
    'bullish': 0.65,
    'constructive': 0.55,
    'scalp_buy_candidate': 0.65,
    'watch_for_trigger': 0.45,
}
NEGATIVE_ACTION_VALUES: dict[str, float] = {
    'reject': -0.65,
    'bearish': -0.65,
    'risk': -0.55,
    'avoid': -0.75,
    'avoid_chase': -0.55,
}
NEUTRAL_ACTIONS: set[str] = {'wait', 'neutral', 'hold', 'no clear edge', 'no_clear_edge'}

POSITIVE_KEYWORDS: tuple[str, ...] = (
    'bullish',
    'buy',
    'constructive',
    'breakout',
    'positive',
    'improving',
    'strength',
    'supportive',
)
NEGATIVE_KEYWORDS: tuple[str, ...] = (
    'bearish',
    'reject',
    'risk',
    'avoid',
    'negative',
    'weak',
    'deteriorating',
    'concern',
    'downgrade',
    'lawsuit',
    'recall',
)
NEUTRAL_KEYWORDS: tuple[str, ...] = ('neutral', 'wait', 'no clear edge', 'sideways')

MISSING_OR_LOW_QUALITY_STATUSES: set[str] = {
    'missing',
    'missing_source_row',
    'not_computed_for_market_date',
    'storage_unavailable',
    'unavailable',
    'provider_error',
    'insufficient_history',
    'empty',
}


@dataclass(frozen=True)
class SignalEnvelope:
    source_name: str
    source_type: str
    symbol: str
    market_date: str
    horizon: str
    value: float
    confidence: float
    evidence_count: int
    risk_flags: tuple[str, ...]
    evidence_refs: tuple[dict[str, Any], ...]
    summary: str | None
    raw_ref: dict[str, Any]
    schema_version: str = SIGNAL_ENVELOPE_SCHEMA_VERSION

    def to_payload(self) -> dict[str, Any]:
        return {
            'schema_version': self.schema_version,
            'source_name': self.source_name,
            'source_type': self.source_type,
            'symbol': self.symbol,
            'market_date': self.market_date,
            'horizon': self.horizon,
            'value': self.value,
            'confidence': self.confidence,
            'evidence_count': self.evidence_count,
            'risk_flags': list(self.risk_flags),
            'evidence_refs': list(self.evidence_refs),
            'summary': self.summary,
            'raw_ref': self.raw_ref,
        }


def _normalize_market_date(value: str | date) -> str:
    if isinstance(value, date):
        return value.isoformat()
    return date.fromisoformat(str(value)).isoformat()


def _normalize_symbol(value: Any) -> str:
    return str(value or '').strip().upper()


def _normalize_horizon(value: Any, *, default: str = DEFAULT_SIGNAL_ENVELOPE_HORIZON) -> str:
    text = str(value or '').strip().lower()
    return text or default


def _normalize_source_type(source_name: str, source_type: str | None = None) -> str:
    explicit = str(source_type or '').strip().lower()
    if explicit in SOURCE_TYPE_VALUES:
        return explicit
    lowered = source_name.lower()
    if lowered.startswith('factor:'):
        return 'factor'
    if lowered.startswith('news:') or 'news' in lowered:
        return 'news'
    if lowered.startswith('macro:') or 'macro' in lowered:
        return 'macro'
    if lowered.startswith('risk:') or 'risk' in lowered:
        return 'risk'
    return 'agent'


def _clip_float(value: float, *, min_value: float, max_value: float) -> float:
    return max(min_value, min(max_value, value))


def _round_score(value: float) -> float:
    return round(_clip_float(value, min_value=-1.0, max_value=1.0), 4)


def _round_confidence(value: float) -> float:
    return round(_clip_float(value, min_value=0.0, max_value=1.0), 4)


def _to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip().replace(',', '')
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _to_int(value: Any) -> int | None:
    numeric = _to_float(value)
    if numeric is None:
        return None
    return int(round(numeric))


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if value is None:
        return []
    return [value]


def _normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _short_text(value: Any, *, max_length: int = SUMMARY_MAX_LENGTH) -> str | None:
    text = _normalize_text(value)
    if text is None:
        return None
    if len(text) <= max_length:
        return text
    return text[: max_length - 3].rstrip() + '...'


def _compact_strings(values: Any, *, limit: int = EVIDENCE_REF_LIMIT) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in _as_list(values):
        text = _short_text(item, max_length=160)
        if text is None or text in seen:
            continue
        seen.add(text)
        result.append(text)
        if len(result) >= limit:
            break
    return result


def _dedupe_strings(values: Sequence[Any]) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()
    for item in values:
        text = _normalize_text(item)
        if text is None:
            continue
        normalized = text.strip().lower().replace(' ', '_')
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return tuple(result)


def _first_text(*values: Any) -> str | None:
    for value in values:
        text = _short_text(value)
        if text is not None:
            return text
    return None


def _action_value(action: Any) -> float | None:
    text = _normalize_text(action)
    if text is None:
        return None
    normalized = text.lower().replace('-', '_')
    if normalized in POSITIVE_ACTION_VALUES:
        return POSITIVE_ACTION_VALUES[normalized]
    if normalized in NEGATIVE_ACTION_VALUES:
        return NEGATIVE_ACTION_VALUES[normalized]
    if normalized in NEUTRAL_ACTIONS:
        return 0.0
    return None


def _keyword_value(text: str, *, source_type: str) -> float | None:
    lowered = text.lower()
    if any(keyword in lowered for keyword in NEUTRAL_KEYWORDS):
        return 0.0
    positive = any(keyword in lowered for keyword in POSITIVE_KEYWORDS)
    negative = any(keyword in lowered for keyword in NEGATIVE_KEYWORDS)
    if positive and not negative:
        return 0.45
    if negative and not positive:
        return -0.45 if source_type not in {'news', 'risk'} else -0.6
    if positive and negative:
        return 0.0
    return None


def _directional_value_from_payload(payload: dict[str, Any], *, source_name: str, source_type: str) -> float:
    for key in ('value', 'signal_value', 'conviction_value'):
        numeric = _to_float(payload.get(key))
        if numeric is not None:
            return _round_score(numeric)

    for key in ('preferred_action', 'next_day_open_action', 'tactical_action', 'action', 'recommendation', 'bias_direction'):
        value = _action_value(payload.get(key))
        if value is not None:
            return _round_score(value)

    details = _as_dict(payload.get('details'))
    for block_name in ('decision_support', 'tactical_decision_support'):
        block = _as_dict(details.get(block_name))
        for key in ('preferred_action', 'next_day_open_action', 'tactical_action', 'action', 'recommendation'):
            value = _action_value(block.get(key))
            if value is not None:
                return _round_score(value)

    text_parts: list[str] = []
    for key in ('summary', 'thesis_summary', 'market_bias', 'bias', 'materiality_reason', 'event_type', 'sentiment'):
        text = _normalize_text(payload.get(key))
        if text:
            text_parts.append(text)
    if source_type in {'news', 'risk'}:
        text_parts.extend(_compact_strings(payload.get('risk_flags')))
        text_parts.extend(_compact_strings(payload.get('decision_risks')))
    keyword_value = _keyword_value(' '.join(text_parts), source_type=source_type) if text_parts else None
    if keyword_value is not None:
        return _round_score(keyword_value)

    if source_name.startswith('risk:'):
        return -0.35
    return 0.0


def _confidence_from_payload(payload: dict[str, Any], *, evidence_count: int) -> float:
    confidence = _to_float(payload.get('confidence'))
    if confidence is not None:
        return _round_confidence(confidence if confidence <= 1 else confidence / 10)

    score_values = [
        _to_float(payload.get('confidence_score_10')),
        _to_float(payload.get('conviction_score_10')),
    ]
    score_values = [value for value in score_values if value is not None]
    if score_values:
        return _round_confidence(sum(score_values) / len(score_values) / 10)

    tier = str(payload.get('confidence_tier') or '').strip().lower()
    if tier == 'high':
        return 0.75
    if tier == 'medium':
        return 0.55
    if tier == 'low':
        return 0.3
    return 0.45 if evidence_count else 0.2


def _agent_evidence_count(payload: dict[str, Any]) -> int:
    details = _as_dict(payload.get('details'))
    nested_support = _as_dict(details.get('decision_support')) or _as_dict(details.get('tactical_decision_support'))
    counts = [
        len(_as_list(payload.get('supporting_evidence'))),
        len(_as_list(payload.get('trigger_conditions'))),
        len(_as_list(payload.get('evidence_refs'))),
        len(_as_list(nested_support.get('supporting_evidence'))),
    ]
    return max(counts)


def _agent_risk_flags(payload: dict[str, Any], *, evidence_count: int) -> tuple[str, ...]:
    details = _as_dict(payload.get('details'))
    nested_support = _as_dict(details.get('decision_support')) or _as_dict(details.get('tactical_decision_support'))
    flags: list[Any] = [
        *_as_list(payload.get('risk_flags')),
        *_as_list(payload.get('data_quality_warnings')),
        *_as_list(nested_support.get('data_quality_warnings')),
    ]
    if evidence_count <= 0:
        flags.append('low_evidence')
    return _dedupe_strings(flags)


def _agent_evidence_refs(
    *,
    source_name: str,
    traceability: dict[str, Any] | None,
    payload: dict[str, Any],
) -> tuple[dict[str, Any], ...]:
    refs: list[dict[str, Any]] = []
    trace = _as_dict(traceability)
    if trace:
        refs.append(
            {
                key: trace.get(key)
                for key in ('source', 'feedback_id', 'run_id', 'market_date', 'received_at', 'reuse_reason')
                if trace.get(key) is not None
            }
        )
    compact_evidence = _compact_strings(payload.get('supporting_evidence'))
    for index, text in enumerate(compact_evidence, start=1):
        refs.append({'source': source_name, 'kind': 'supporting_evidence', 'index': index, 'text': text})
    return tuple(ref for ref in refs[:EVIDENCE_REF_LIMIT] if ref)


def build_agent_signal_envelope(
    *,
    source_name: str,
    payload: dict[str, Any],
    symbol: str,
    market_date: str | date,
    traceability: dict[str, Any] | None = None,
    source_type: str | None = None,
    horizon: str = DEFAULT_SIGNAL_ENVELOPE_HORIZON,
) -> dict[str, Any]:
    normalized_source_type = _normalize_source_type(source_name, source_type)
    evidence_count = _agent_evidence_count(payload)
    trace = _as_dict(traceability)
    raw_ref = {
        'type': 'agent_context_feedback',
        'source_name': source_name,
        'path': f'upstream_summaries.{source_name}',
    }
    for key in ('feedback_id', 'run_id', 'market_date', 'received_at', 'source'):
        if trace.get(key) is not None:
            raw_ref[key] = trace[key]
    return SignalEnvelope(
        source_name=source_name,
        source_type=normalized_source_type,
        symbol=_normalize_symbol(symbol),
        market_date=_normalize_market_date(market_date),
        horizon=_normalize_horizon(payload.get('horizon'), default=horizon),
        value=_directional_value_from_payload(payload, source_name=source_name, source_type=normalized_source_type),
        confidence=_confidence_from_payload(payload, evidence_count=evidence_count),
        evidence_count=evidence_count,
        risk_flags=_agent_risk_flags(payload, evidence_count=evidence_count),
        evidence_refs=_agent_evidence_refs(source_name=source_name, traceability=trace, payload=payload),
        summary=_first_text(payload.get('thesis_summary'), payload.get('summary'), payload.get('materiality_reason')),
        raw_ref=raw_ref,
    ).to_payload()


def _factor_status_risk_flags(entry: dict[str, Any]) -> tuple[str, ...]:
    status = str(entry.get('status') or '').strip().lower()
    source_status = str(entry.get('source_status') or '').strip().lower()
    flags: list[str] = []
    if status and status != 'available':
        flags.append(status)
    if source_status and source_status not in {'scored', 'available'}:
        flags.append(source_status)
    flags.extend(_as_list(entry.get('coverage_notes')))
    if status in MISSING_OR_LOW_QUALITY_STATUSES:
        flags.append('low_quality_or_missing_source')
    return _dedupe_strings(flags)


def _factor_confidence(entry: dict[str, Any], *, status: str, evidence_count: int) -> float:
    if status in {'missing_source_row', 'storage_unavailable', 'unavailable'}:
        return 0.0
    if status in {'not_computed_for_market_date', 'insufficient_history', 'empty'}:
        return 0.1
    if status == 'not_eligible':
        return 0.25
    components_used = _to_int(entry.get('components_used'))
    components_possible = _to_int(entry.get('components_possible'))
    if components_used is not None and components_possible and components_possible > 0:
        return _round_confidence(components_used / components_possible)
    return 0.65 if evidence_count else 0.35


def build_factor_signal_envelopes(
    *,
    symbol: str,
    market_date: str | date,
    factor_summary: dict[str, Any] | None,
    horizon: str = DEFAULT_SIGNAL_ENVELOPE_HORIZON,
) -> list[dict[str, Any]]:
    summary = _as_dict(factor_summary)
    envelopes: list[dict[str, Any]] = []
    factor_scores = _as_dict(summary.get('factor_scores'))
    for family, raw_entry in factor_scores.items():
        entry = _as_dict(raw_entry)
        source_name = f'factor:{family}'
        status = str(entry.get('status') or 'missing_source_row').strip().lower()
        score = _to_float(entry.get('score'))
        if score is None:
            score = _to_float(entry.get('percentile'))
        value = 0.0 if score is None or status != 'available' else (score * 2.0) - 1.0
        evidence_count = _to_int(entry.get('components_used')) or (1 if score is not None else 0)
        evidence_ref = {
            key: entry.get(key)
            for key in ('source', 'feature_version', 'as_of_market_date', 'captured_at', 'rank', 'percentile', 'source_status')
            if entry.get(key) is not None
        }
        envelopes.append(
            SignalEnvelope(
                source_name=source_name,
                source_type='factor',
                symbol=_normalize_symbol(symbol),
                market_date=_normalize_market_date(market_date),
                horizon=horizon,
                value=_round_score(value),
                confidence=_factor_confidence(entry, status=status, evidence_count=evidence_count),
                evidence_count=max(0, evidence_count),
                risk_flags=_factor_status_risk_flags(entry),
                evidence_refs=(evidence_ref,) if evidence_ref else tuple(),
                summary=_short_text(
                    f'{family} factor status={status}'
                    + (f' score={score:.4f}' if score is not None else '')
                ),
                raw_ref={
                    'type': 'factor_summary',
                    'path': f'factor_summary.factor_scores.{family}',
                    'as_of_market_date': entry.get('as_of_market_date') or summary.get('requested_market_date'),
                },
            ).to_payload()
        )

    signal_features = _as_dict(summary.get('signal_features'))
    if signal_features:
        status = str(signal_features.get('status') or 'missing_source_row').strip().lower()
        signal_count = _to_int(signal_features.get('signal_count')) or 0
        direct_source_count = _to_int(signal_features.get('direct_signal_source_count')) or 0
        evidence_count = max(signal_count, direct_source_count)
        row_kind = str(signal_features.get('row_kind') or '').strip()
        risk_flags = list(_factor_status_risk_flags(signal_features))
        if row_kind == 'fallback_only':
            risk_flags.append('fallback_only')
        value = min(1.0, signal_count / 3.0) if status == 'available' else 0.0
        confidence = _round_confidence(min(0.85, 0.25 + (0.2 * evidence_count))) if status == 'available' else _factor_confidence(
            signal_features,
            status=status,
            evidence_count=evidence_count,
        )
        signal_feature_ref = {
            key: signal_features.get(key)
            for key in (
                'source',
                'feature_version',
                'as_of_market_date',
                'captured_at',
                'row_kind',
                'direct_signal_source_count',
            )
            if signal_features.get(key) is not None
        }
        envelopes.append(
            SignalEnvelope(
                source_name='factor:kis_ranking',
                source_type='factor',
                symbol=_normalize_symbol(symbol),
                market_date=_normalize_market_date(market_date),
                horizon='1d',
                value=_round_score(value),
                confidence=confidence,
                evidence_count=max(0, evidence_count),
                risk_flags=_dedupe_strings(risk_flags),
                evidence_refs=(signal_feature_ref,) if signal_feature_ref else tuple(),
                summary=_short_text(
                    f'KIS ranking signal_count={signal_count}; status={status}'
                    + (f'; row_kind={row_kind}' if row_kind else '')
                ),
                raw_ref={
                    'type': 'factor_summary',
                    'path': 'factor_summary.signal_features',
                    'as_of_market_date': signal_features.get('as_of_market_date') or summary.get('requested_market_date'),
                },
            ).to_payload()
        )
    return envelopes


def build_news_signal_envelope(
    *,
    symbol: str,
    market_date: str | date,
    recent_external_news: dict[str, Any] | None,
    horizon: str = DEFAULT_SIGNAL_ENVELOPE_HORIZON,
) -> dict[str, Any]:
    news = _as_dict(recent_external_news)
    status = str(news.get('status') or 'unavailable').strip().lower()
    material_items = [item for item in _as_list(news.get('material_items')) if isinstance(item, dict)]
    selected_items = material_items or [item for item in _as_list(news.get('items')) if isinstance(item, dict)]
    evidence_count = len(selected_items)
    risk_flags: list[Any] = []
    if status != 'available':
        risk_flags.append(status)
        if news.get('reason'):
            risk_flags.append(news.get('reason'))
    if material_items:
        risk_flags.append('material_news')
    sentiment_values = [str(item.get('sentiment') or '').strip().lower() for item in selected_items]
    negative_count = sum(1 for value in sentiment_values if value == 'negative')
    positive_count = sum(1 for value in sentiment_values if value == 'positive')
    if negative_count:
        risk_flags.append('negative_news_sentiment')
    if any(str(item.get('access_status') or '').strip().lower() != 'full_text' for item in selected_items):
        risk_flags.append('headline_or_limited_text')

    materiality_score = _to_float(_as_dict(news.get('classification_summary')).get('max_materiality_score'))
    if materiality_score is None and selected_items:
        materiality_score = max((_to_float(item.get('materiality_score')) or 0.0 for item in selected_items), default=0.0)
    confidence = 0.0 if status in {'unavailable', 'provider_error'} else _round_confidence(materiality_score or (0.2 if evidence_count else 0.1))
    if status == 'empty':
        confidence = 0.1
    if negative_count > positive_count:
        value = -max(0.35, materiality_score or 0.35)
    elif positive_count > negative_count:
        value = max(0.25, materiality_score or 0.25)
    else:
        value = 0.0

    refs: list[dict[str, Any]] = []
    for index, item in enumerate(selected_items[:EVIDENCE_REF_LIMIT], start=1):
        refs.append(
            {
                key: item.get(key)
                for key in ('title', 'url', 'published_at', 'event_type', 'materiality', 'materiality_score', 'access_status')
                if item.get(key) is not None
            }
            | {'index': index}
        )

    titles = [str(item.get('title') or '').strip() for item in selected_items if item.get('title')]
    return SignalEnvelope(
        source_name='news:external',
        source_type='news',
        symbol=_normalize_symbol(symbol),
        market_date=_normalize_market_date(market_date),
        horizon=horizon,
        value=_round_score(value),
        confidence=confidence,
        evidence_count=evidence_count,
        risk_flags=_dedupe_strings(risk_flags),
        evidence_refs=tuple(ref for ref in refs if ref),
        summary=_short_text('; '.join(titles) if titles else news.get('reason') or f'external news status={status}'),
        raw_ref={
            'type': 'symbol_day_context',
            'path': 'recent_external_news',
            'status': status,
            'latest_published_at': news.get('latest_published_at'),
        },
    ).to_payload()


def build_symbol_signal_envelopes(
    *,
    symbol: str,
    market_date: str | date,
    factor_summary: dict[str, Any] | None = None,
    recent_external_news: dict[str, Any] | None = None,
    horizon: str = DEFAULT_SIGNAL_ENVELOPE_HORIZON,
) -> list[dict[str, Any]]:
    envelopes = build_factor_signal_envelopes(
        symbol=symbol,
        market_date=market_date,
        factor_summary=factor_summary,
        horizon=horizon,
    )
    if recent_external_news is not None:
        envelopes.append(
            build_news_signal_envelope(
                symbol=symbol,
                market_date=market_date,
                recent_external_news=recent_external_news,
                horizon=horizon,
            )
        )
    return envelopes


def _envelope_key(envelope: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(envelope.get('source_name') or ''),
        str(envelope.get('horizon') or ''),
        str(_as_dict(envelope.get('raw_ref')).get('path') or ''),
    )


def merge_signal_envelopes(*groups: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for group in groups:
        for envelope in group:
            if not isinstance(envelope, dict):
                continue
            key = _envelope_key(envelope)
            if key in seen:
                continue
            seen.add(key)
            merged.append(envelope)
    return merged


def build_decider_signal_envelopes(
    *,
    symbol: str,
    market_date: str | date,
    factor_summary: dict[str, Any] | None,
    upstream_summaries: dict[str, dict[str, Any]] | None,
    upstream_traceability: dict[str, dict[str, Any]] | None,
    existing_envelopes: Sequence[dict[str, Any]] | None = None,
    horizon: str = DEFAULT_SIGNAL_ENVELOPE_HORIZON,
) -> list[dict[str, Any]]:
    upstream_payloads = _as_dict(upstream_summaries)
    traceability = _as_dict(upstream_traceability)
    agent_envelopes = [
        build_agent_signal_envelope(
            source_name=source_name,
            payload=payload,
            symbol=symbol,
            market_date=market_date,
            traceability=traceability.get(source_name) if isinstance(traceability.get(source_name), dict) else None,
            horizon=horizon,
        )
        for source_name, payload in upstream_payloads.items()
        if isinstance(payload, dict)
    ]
    factor_envelopes = build_factor_signal_envelopes(
        symbol=symbol,
        market_date=market_date,
        factor_summary=factor_summary,
        horizon=horizon,
    )
    return merge_signal_envelopes(list(existing_envelopes or []), agent_envelopes, factor_envelopes)
