from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Sequence

MISSING_DATA_CATEGORY_STATUSES = (
    'available_but_not_wired',
    'partially_available',
    'external_data_required',
    'data_quality_issue',
)
MAX_MISSING_DATA_NOTE_LENGTH = 240


@dataclass(frozen=True)
class MissingDataCategoryDefinition:
    key: str
    label: str
    status: str
    internal_tables: tuple[str, ...]
    external_providers: tuple[str, ...]
    operator_hint: str
    keywords: tuple[str, ...]


CATEGORY_DEFINITIONS: tuple[MissingDataCategoryDefinition, ...] = (
    MissingDataCategoryDefinition(
        key='liquidity_turnover_slippage',
        label='liquidity / turnover / slippage',
        status='partially_available',
        internal_tables=('daily_price_bars', 'daily_price_contexts', 'quote_snapshots', 'current_quote_cache'),
        external_providers=('KIS quote/orderbook',),
        operator_hint=(
            'Daily OHLCV and derived liquidity context already exist, but precise spread, depth, and slippage '
            'still require richer realtime quote wiring.'
        ),
        keywords=(
            '거래대금',
            '유동성',
            '체결비용',
            '슬리피지',
            '호가',
            '스프레드',
            'liquidity',
            'turnover',
            'slippage',
            'spread',
            'orderbook',
            'order book',
            'bid/ask',
            'bid ask',
            'depth',
        ),
    ),
    MissingDataCategoryDefinition(
        key='financial_detail_quarter_cashflow',
        label='financial detail / quarter / cashflow',
        status='partially_available',
        internal_tables=('dart_fundamentals_snapshots', 'dart_fundamentals_rows', 'dart_disclosure_events'),
        external_providers=('DART filings/XBRL',),
        operator_hint=(
            'Stored DART fundamentals cover many filings, but quarterly detail and cash-flow completeness still vary '
            'by capture coverage and field mapping.'
        ),
        keywords=(
            '재무',
            '분기',
            '현금흐름',
            'cashflow',
            'cash flow',
            'quarter',
            'quarterly',
            'financial detail',
            'financial statement',
            'fundamental detail',
            'fcf',
            'ebitda',
        ),
    ),
    MissingDataCategoryDefinition(
        key='market_sector_peer_relative',
        label='market / sector / peer relative comparison',
        status='partially_available',
        internal_tables=('daily_market_relative_contexts', 'daily_market_participation_contexts', 'daily_price_bars'),
        external_providers=('KRX sector/peer classification',),
        operator_hint=(
            'Market breadth and market-relative context are derivable internally, while sector and peer mapping are '
            'still only partially wired.'
        ),
        keywords=(
            '시장',
            '업종',
            '피어',
            '상대비교',
            '상대 비교',
            'sector',
            'peer',
            'relative',
            'breadth',
            'market-relative',
            'market relative',
            'peer comparison',
        ),
    ),
    MissingDataCategoryDefinition(
        key='factor_score_coverage',
        label='factor / score coverage',
        status='available_but_not_wired',
        internal_tables=(
            'daily_unified_value_factor_results',
            'daily_quality_factor_results',
            'daily_momentum_factor_results',
            'daily_composite_factor_results',
            'daily_signal_features',
        ),
        external_providers=(),
        operator_hint=(
            'Most persisted factor families already exist. Remaining complaints are largely context wiring gaps or '
            'sparse signal-feature coverage, not a missing storage layer.'
        ),
        keywords=(
            '팩터',
            '스코어',
            'factor',
            'score',
            'signal feature',
            'signal_features',
            'signal feature source',
            'value score',
            'quality score',
            'momentum score',
            '모멘텀',
            '퀄리티',
            '가치',
        ),
    ),
    MissingDataCategoryDefinition(
        key='technical_evidence_support_resistance',
        label='support / resistance / technical evidence',
        status='partially_available',
        internal_tables=('daily_technical_snapshots', 'daily_action_summaries', 'daily_price_contexts', 'daily_price_bars'),
        external_providers=(),
        operator_hint=(
            'Technical read models already exist, but evidence chains and some confirmation context are still partial '
            'or sensitive to bar-quality gaps.'
        ),
        keywords=(
            '지지',
            '저항',
            '기술',
            'support',
            'resistance',
            'technical evidence',
            'technical basis',
            'trendline',
            'breakout',
            'pullback',
            'pattern',
        ),
    ),
    MissingDataCategoryDefinition(
        key='investor_flows_short_selling',
        label='investor flows / short-selling',
        status='external_data_required',
        internal_tables=(),
        external_providers=('KIS investor flow endpoints', 'KRX short-selling statistics'),
        operator_hint='Investor-segment flow and short-selling datasets are not currently persisted as a first-class internal table set.',
        keywords=(
            '수급',
            '투자자별',
            '공매도',
            'investor flow',
            'flows',
            'foreign flow',
            'institutional flow',
            'short selling',
            'short-selling',
            'short interest',
        ),
    ),
    MissingDataCategoryDefinition(
        key='disclosures_news_events',
        label='disclosures / news / events',
        status='partially_available',
        internal_tables=('dart_disclosure_events', 'dart_fundamentals_snapshots'),
        external_providers=('DART filings/XBRL', 'news/event feed'),
        operator_hint=(
            'Disclosure events are stored, but broader news and event context still requires extra provider coverage '
            'or better summarization wiring.'
        ),
        keywords=(
            '공시',
            '뉴스',
            '이벤트',
            'disclosure',
            'filing',
            'news',
            'event',
            'earnings call',
            'headline',
            'macro event',
        ),
    ),
    MissingDataCategoryDefinition(
        key='after_hours_intraday_realtime',
        label='after-hours / intraday / realtime',
        status='partially_available',
        internal_tables=('quote_snapshots', 'current_quote_cache', 'daily_signal_features'),
        external_providers=('KIS realtime quote/trending APIs',),
        operator_hint=(
            'Current quote and ranking snapshots exist, but full intraday or after-hours history is not persisted '
            'end-to-end yet.'
        ),
        keywords=(
            '시간외',
            '장중',
            '실시간',
            'after-hours',
            'after hours',
            'intraday',
            'real-time',
            'realtime',
            'premarket',
            'pre-market',
            'postmarket',
            'tick',
            'minute bar',
        ),
    ),
    MissingDataCategoryDefinition(
        key='price_bar_quality_session_state',
        label='price-bar quality / session state',
        status='data_quality_issue',
        internal_tables=('daily_price_bars', 'daily_price_contexts'),
        external_providers=(),
        operator_hint=(
            'The main gap is data quality and session-state normalization on already stored OHLCV rows, not a brand '
            'new provider dependency.'
        ),
        keywords=(
            '가격바',
            '세션 상태',
            '거래정지',
            '단일가',
            '수정주가',
            '시가',
            'open price',
            'open_price',
            'bar quality',
            'session state',
            'halt',
            'auction session',
            'gap unreliable',
            'volume zero',
        ),
    ),
    MissingDataCategoryDefinition(
        key='consensus_guidance',
        label='consensus / guidance',
        status='external_data_required',
        internal_tables=(),
        external_providers=('analyst consensus provider', 'management guidance feed'),
        operator_hint='Analyst consensus and structured management-guidance series are not currently stored as a first-class dataset.',
        keywords=(
            '컨센서스',
            '가이던스',
            'consensus',
            'guidance',
            'estimate revision',
            'estimate',
            'analyst',
            'broker estimate',
        ),
    ),
)

CATEGORY_DEFINITIONS_BY_KEY = {definition.key: definition for definition in CATEGORY_DEFINITIONS}
CATEGORY_CONTEXT_PROVENANCE_PATHS: dict[str, tuple[str, ...]] = {
    'liquidity_turnover_slippage': ('quote_context', 'price_context', 'signal_features'),
    'financial_detail_quarter_cashflow': ('fundamentals',),
    'market_sector_peer_relative': ('factor_rows.composite', 'signal_features'),
    'factor_score_coverage': (
        'factor_rows.unified_value',
        'factor_rows.quality',
        'factor_rows.momentum',
        'factor_rows.advanced_value',
        'factor_rows.low_risk',
        'factor_rows.beta',
        'factor_rows.composite',
        'signal_features',
    ),
    'technical_evidence_support_resistance': ('price_context',),
    'disclosures_news_events': ('fundamentals', 'recent_disclosures'),
    'after_hours_intraday_realtime': ('quote_context', 'signal_features'),
    'price_bar_quality_session_state': ('price_context',),
}


def _normalize_whitespace(value: str) -> str:
    return re.sub(r'\s+', ' ', value).strip()


def sanitize_missing_data_note(note: str, *, max_length: int = MAX_MISSING_DATA_NOTE_LENGTH) -> str:
    normalized = _normalize_whitespace(note)
    if len(normalized) <= max_length:
        return normalized
    return normalized[: max_length - 3].rstrip() + '...'


def normalize_missing_data_category_filters(categories: Sequence[str] | None) -> list[str]:
    if not categories:
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_category in categories:
        for piece in (raw_category or '').split(','):
            category = _normalize_whitespace(piece).lower().replace('-', '_').replace(' ', '_')
            if not category:
                continue
            if category not in CATEGORY_DEFINITIONS_BY_KEY:
                raise ValueError(f'Unknown missing-data category: {piece.strip() or raw_category!r}')
            if category in seen:
                continue
            seen.add(category)
            normalized.append(category)
    return normalized


def _extract_note_entries(value: Any, *, source: str, notes_by_key: dict[str, dict[str, Any]]) -> None:
    if isinstance(value, str):
        text = sanitize_missing_data_note(value)
        if not text:
            return
        note_key = text.casefold()
        entry = notes_by_key.get(note_key)
        if entry is None:
            notes_by_key[note_key] = {'text': text, 'sources': [source]}
            return
        if source not in entry['sources']:
            entry['sources'].append(source)
        return
    if isinstance(value, dict):
        for nested_value in value.values():
            _extract_note_entries(nested_value, source=source, notes_by_key=notes_by_key)
        return
    if isinstance(value, (list, tuple, set)):
        for item in value:
            _extract_note_entries(item, source=source, notes_by_key=notes_by_key)


def _extract_context_provenance(context_payload: Any) -> dict[str, Any]:
    if not isinstance(context_payload, dict):
        return {}
    direct = context_payload.get('context_provenance')
    if isinstance(direct, dict):
        return direct
    symbol_day_context = context_payload.get('symbol_day_context')
    if isinstance(symbol_day_context, dict):
        nested = symbol_day_context.get('context_provenance')
        if isinstance(nested, dict):
            return nested
    return {}


def _resolve_context_provenance_path(context_provenance: dict[str, Any], path: str) -> dict[str, Any] | None:
    current: Any = context_provenance
    for segment in path.split('.'):
        if not isinstance(current, dict):
            return None
        current = current.get(segment)
    if isinstance(current, dict):
        return current
    return None


def _build_category_context_evidence(*, category: str, context_payload: Any) -> tuple[list[str], list[dict[str, Any]]]:
    context_provenance = _extract_context_provenance(context_payload)
    if not context_provenance:
        return [], []

    evidence: list[dict[str, Any]] = []
    issue_types: set[str] = set()
    for path in CATEGORY_CONTEXT_PROVENANCE_PATHS.get(category, ()):
        metadata = _resolve_context_provenance_path(context_provenance, path)
        if metadata is None:
            continue
        block_issue_types = [
            str(issue_type).strip()
            for issue_type in metadata.get('issue_types', [])
            if str(issue_type).strip()
        ]
        issue_types.update(block_issue_types)
        evidence.append(
            {
                'block': path,
                'issue_types': block_issue_types,
                'freshness_status': metadata.get('freshness_status'),
                'provenance_status': metadata.get('provenance_status'),
                'as_of_market_date': metadata.get('as_of_market_date'),
                'latest_available_market_date': metadata.get('latest_available_market_date'),
                'source_tables': list(metadata.get('source_tables') or []),
                'requested_symbol_coverage': metadata.get('requested_symbol_coverage'),
            }
        )

    return sorted(issue_types), evidence


def extract_missing_data_notes(
    *,
    data_gaps_payload: Any,
    improvement_notes_payload: Any,
) -> list[dict[str, Any]]:
    notes_by_key: dict[str, dict[str, Any]] = {}
    _extract_note_entries(data_gaps_payload, source='data_gaps', notes_by_key=notes_by_key)
    _extract_note_entries(improvement_notes_payload, source='improvement_notes', notes_by_key=notes_by_key)
    return list(notes_by_key.values())


def _classify_note_categories(note_text: str) -> list[str]:
    normalized_note = note_text.casefold()
    matched: list[str] = []
    for definition in CATEGORY_DEFINITIONS:
        if any(keyword.casefold() in normalized_note for keyword in definition.keywords):
            matched.append(definition.key)
    return matched


def build_missing_data_classification(
    *,
    data_gaps_payload: Any,
    improvement_notes_payload: Any,
    context_payload: Any = None,
) -> dict[str, Any]:
    note_entries = extract_missing_data_notes(
        data_gaps_payload=data_gaps_payload,
        improvement_notes_payload=improvement_notes_payload,
    )
    category_buckets: dict[str, dict[str, Any]] = {}
    matched_note_keys: set[str] = set()
    for entry in note_entries:
        matched_categories = _classify_note_categories(entry['text'])
        if not matched_categories:
            continue
        matched_note_keys.add(entry['text'].casefold())
        for category in matched_categories:
            bucket = category_buckets.get(category)
            if bucket is None:
                bucket = {'category': category, 'notes': [], 'sources': set()}
                category_buckets[category] = bucket
            if entry['text'] not in bucket['notes']:
                bucket['notes'].append(entry['text'])
            bucket['sources'].update(entry['sources'])

    categories: list[dict[str, Any]] = []
    for definition in CATEGORY_DEFINITIONS:
        bucket = category_buckets.get(definition.key)
        if bucket is None:
            continue
        notes = [sanitize_missing_data_note(note) for note in bucket['notes']]
        context_issue_types, context_evidence = _build_category_context_evidence(
            category=definition.key,
            context_payload=context_payload,
        )
        categories.append(
            {
                'category': definition.key,
                'note_count': len(notes),
                'notes': notes,
                'sources': sorted(bucket['sources']),
                'context_issue_types': context_issue_types,
                'context_evidence': context_evidence,
            }
        )
    uncategorized_notes = [
        entry['text']
        for entry in note_entries
        if entry['text'].casefold() not in matched_note_keys
    ]
    return {
        'notes': note_entries,
        'categories': categories,
        'uncategorized_notes': uncategorized_notes,
    }


def build_missing_data_categories(
    *,
    data_gaps_payload: Any,
    improvement_notes_payload: Any,
    context_payload: Any = None,
) -> list[dict[str, Any]]:
    return build_missing_data_classification(
        data_gaps_payload=data_gaps_payload,
        improvement_notes_payload=improvement_notes_payload,
        context_payload=context_payload,
    )['categories']


def enrich_missing_data_categories(raw_categories: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_categories, list):
        return []
    enriched: list[dict[str, Any]] = []
    for item in raw_categories:
        if not isinstance(item, dict):
            continue
        category = _normalize_whitespace(str(item.get('category') or '')).lower()
        definition = CATEGORY_DEFINITIONS_BY_KEY.get(category)
        if definition is None:
            continue
        notes = [
            sanitize_missing_data_note(str(note))
            for note in item.get('notes', [])
            if isinstance(note, str) and sanitize_missing_data_note(note)
        ]
        note_count = len(notes) if notes else int(item.get('note_count') or 0)
        sources = sorted(
            {
                _normalize_whitespace(str(source))
                for source in item.get('sources', [])
                if _normalize_whitespace(str(source))
            }
        )
        context_issue_types = sorted(
            {
                _normalize_whitespace(str(issue_type)).lower()
                for issue_type in item.get('context_issue_types', [])
                if _normalize_whitespace(str(issue_type))
            }
        )
        context_evidence: list[dict[str, Any]] = []
        for evidence in item.get('context_evidence', []):
            if not isinstance(evidence, dict):
                continue
            context_evidence.append(
                {
                    'block': _normalize_whitespace(str(evidence.get('block') or '')),
                    'issue_types': sorted(
                        {
                            _normalize_whitespace(str(issue_type)).lower()
                            for issue_type in evidence.get('issue_types', [])
                            if _normalize_whitespace(str(issue_type))
                        }
                    ),
                    'freshness_status': evidence.get('freshness_status'),
                    'provenance_status': evidence.get('provenance_status'),
                    'as_of_market_date': evidence.get('as_of_market_date'),
                    'latest_available_market_date': evidence.get('latest_available_market_date'),
                    'source_tables': list(evidence.get('source_tables') or []),
                    'requested_symbol_coverage': evidence.get('requested_symbol_coverage'),
                }
            )
        enriched.append(
            {
                'category': definition.key,
                'label': definition.label,
                'status': definition.status,
                'internal_tables': list(definition.internal_tables),
                'external_providers': list(definition.external_providers),
                'operator_hint': definition.operator_hint,
                'note_count': note_count,
                'notes': notes,
                'sources': sources,
                'context_issue_types': context_issue_types,
                'context_evidence': context_evidence,
            }
        )
    return enriched
