from __future__ import annotations

from collections import Counter
from datetime import date
from typing import Any, Sequence

from sqlalchemy import select

from app.agent_context.missing_data_taxonomy import (
    CATEGORY_DEFINITIONS_BY_KEY,
    build_missing_data_classification,
    extract_missing_data_note_mentions,
    normalize_missing_data_category_filters,
)
from app.db import session_scope
from app.db_models import AgentContextFeedback

DEFAULT_CATEGORY_NOTE_EXAMPLE_LIMIT = 3
DEFAULT_SYMBOL_EXAMPLE_LIMIT = 25
DEFAULT_UNCATEGORIZED_EXAMPLE_LIMIT = 5


def _normalize_market_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _normalize_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if not normalized:
        raise ValueError('symbol is required')
    if len(normalized) > 32:
        raise ValueError('symbol must be 32 characters or fewer')
    return normalized


def _normalize_symbols(symbols: Sequence[str] | None) -> list[str]:
    if not symbols:
        return []
    normalized_symbols: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        for piece in (raw_symbol or '').split(','):
            symbol = _normalize_symbol(piece)
            if symbol in seen:
                continue
            seen.add(symbol)
            normalized_symbols.append(symbol)
    return normalized_symbols


def _normalize_agent_name(agent_name: str | None) -> str:
    normalized = (agent_name or 'openclaw').strip().lower()
    if not normalized:
        raise ValueError('agent_name is required')
    if len(normalized) > 64:
        raise ValueError('agent_name must be 64 characters or fewer')
    return normalized


def _resolve_date_window(
    *,
    market_date: str | date | None,
    market_date_from: str | date | None,
    market_date_to: str | date | None,
) -> tuple[date, date]:
    if market_date is not None:
        resolved = _normalize_market_date(market_date)
        return resolved, resolved
    if market_date_from is None and market_date_to is None:
        raise ValueError('market_date or market_date_from/market_date_to is required')
    resolved_from = _normalize_market_date(market_date_from or market_date_to)
    resolved_to = _normalize_market_date(market_date_to or market_date_from)
    if resolved_from > resolved_to:
        raise ValueError('market_date_from must be on or before market_date_to')
    return resolved_from, resolved_to


def _feedback_role(row: dict[str, Any]) -> str:
    payload = row.get('normalized_feedback_payload')
    if not isinstance(payload, dict):
        payload = row.get('feedback_payload')
    if isinstance(payload, dict):
        role = payload.get('role')
        if isinstance(role, str) and role.strip():
            return role.strip().lower()
    return str(row.get('agent_name') or '').strip().lower()


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)


def _trend_direction(delta: int) -> str:
    if delta < 0:
        return 'improving'
    if delta > 0:
        return 'worsening'
    return 'flat'


def _normalize_requested_symbol_coverage(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None

    if 'present_rows' in value or 'missing_rows' in value:
        present_rows = int(value.get('present_rows') or 0)
        partial_rows = int(value.get('partial_rows') or 0)
        unavailable_rows = int(value.get('missing_rows') or 0)
    elif 'available_rows' in value or 'partial_rows' in value or 'unavailable_rows' in value:
        present_rows = int(value.get('available_rows') or 0)
        partial_rows = int(value.get('partial_rows') or 0)
        unavailable_rows = int(value.get('unavailable_rows') or 0)
    else:
        return None

    requested_symbol_count = present_rows + partial_rows + unavailable_rows
    coverage_status = str(value.get('coverage_status') or '').strip().lower()
    if not coverage_status:
        if requested_symbol_count <= 0:
            coverage_status = 'unknown'
        elif unavailable_rows == 0 and partial_rows == 0:
            coverage_status = 'complete'
        elif present_rows > 0 or partial_rows > 0:
            coverage_status = 'partial'
        else:
            coverage_status = 'missing'

    return {
        'requested_symbol_count': requested_symbol_count,
        'present_rows': present_rows,
        'partial_rows': partial_rows,
        'unavailable_rows': unavailable_rows,
        'coverage_ratio': _ratio(present_rows + partial_rows, requested_symbol_count),
        'coverage_status': coverage_status,
    }


def _serialize_dimension_breakdown(
    buckets: dict[str, dict[str, Any]],
    *,
    key_name: str,
) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for key, bucket in sorted(
        buckets.items(),
        key=lambda item: (-item[1]['matched_note_count'], -len(item[1]['feedback_row_keys']), item[0]),
    ):
        payload.append(
            {
                key_name: key,
                'feedback_rows': len(bucket['feedback_row_keys']),
                'matched_note_count': bucket['matched_note_count'],
                'raw_note_mentions': bucket['raw_note_mentions'],
                'duplicate_note_mentions': bucket['duplicate_note_mentions'],
                'duplicate_note_rate': _ratio(bucket['duplicate_note_mentions'], bucket['raw_note_mentions']),
                'categories': sorted(bucket['categories']),
                'symbols': sorted(bucket['symbols']),
                'agents': sorted(bucket['agents']),
                'roles': sorted(bucket['roles']),
                'context_issue_type_note_mentions': dict(sorted(bucket['context_issue_type_note_mentions'].items())),
            }
        )
    return payload


def _build_category_trend(
    category_payload: dict[str, Any],
    *,
    before_market_date: str,
    after_market_date: str,
) -> dict[str, Any] | None:
    if before_market_date == after_market_date:
        return None

    breakdown_by_date = {
        item['market_date']: item
        for item in category_payload.get('date_breakdown') or []
    }
    before = breakdown_by_date.get(
        before_market_date,
        {
            'market_date': before_market_date,
            'feedback_rows': 0,
            'note_mentions': 0,
        },
    )
    after = breakdown_by_date.get(
        after_market_date,
        {
            'market_date': after_market_date,
            'feedback_rows': 0,
            'note_mentions': 0,
        },
    )
    note_mentions_delta = int(after['note_mentions']) - int(before['note_mentions'])
    feedback_rows_delta = int(after['feedback_rows']) - int(before['feedback_rows'])
    return {
        'before_market_date': before['market_date'],
        'after_market_date': after['market_date'],
        'before_note_mentions': before['note_mentions'],
        'after_note_mentions': after['note_mentions'],
        'note_mentions_delta': note_mentions_delta,
        'before_feedback_rows': before['feedback_rows'],
        'after_feedback_rows': after['feedback_rows'],
        'feedback_rows_delta': feedback_rows_delta,
        'direction': _trend_direction(note_mentions_delta),
    }


def _build_range_comparison(
    *,
    date_breakdown_payload: list[dict[str, Any]],
    categories_payload: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if len(date_breakdown_payload) < 2:
        return None

    before = date_breakdown_payload[0]
    after = date_breakdown_payload[-1]
    matched_note_delta = int(after['matched_note_count']) - int(before['matched_note_count'])
    duplicate_note_rate_delta = round(
        float(after['duplicate_note_rate']) - float(before['duplicate_note_rate']),
        4,
    )

    category_changes: list[dict[str, Any]] = []
    improving_count = 0
    worsening_count = 0
    flat_count = 0
    for item in sorted(categories_payload, key=lambda category: category['category']):
        trend = item.get('trend')
        if trend is None:
            continue
        direction = trend['direction']
        if direction == 'improving':
            improving_count += 1
        elif direction == 'worsening':
            worsening_count += 1
        else:
            flat_count += 1
        category_changes.append(
            {
                'category': item['category'],
                'direction': direction,
                'note_mentions_delta': trend['note_mentions_delta'],
                'feedback_rows_delta': trend['feedback_rows_delta'],
            }
        )

    return {
        'before_market_date': before['market_date'],
        'after_market_date': after['market_date'],
        'day_count': len(date_breakdown_payload),
        'matched_note_delta': matched_note_delta,
        'duplicate_note_rate_delta': duplicate_note_rate_delta,
        'direction': _trend_direction(matched_note_delta),
        'categories_improving': improving_count,
        'categories_worsening': worsening_count,
        'categories_flat': flat_count,
        'category_changes': category_changes,
    }


class AgentContextMissingDataReportRepository:
    def list_feedback_rows(
        self,
        *,
        market_date_from: date,
        market_date_to: date,
        symbols: Sequence[str] | None = None,
        agent_name: str | None = None,
    ) -> list[dict[str, Any]]:
        with session_scope() as session:
            stmt = (
                select(
                    AgentContextFeedback.id,
                    AgentContextFeedback.market_date,
                    AgentContextFeedback.symbol,
                    AgentContextFeedback.agent_name,
                    AgentContextFeedback.feedback_payload,
                    AgentContextFeedback.normalized_feedback_payload,
                    AgentContextFeedback.data_gaps_payload,
                    AgentContextFeedback.improvement_notes_payload,
                    AgentContextFeedback.missing_data_categories_payload,
                    AgentContextFeedback.context_payload,
                )
                .where(AgentContextFeedback.market_date >= market_date_from)
                .where(AgentContextFeedback.market_date <= market_date_to)
                .order_by(
                    AgentContextFeedback.market_date.asc(),
                    AgentContextFeedback.symbol.asc(),
                    AgentContextFeedback.agent_name.asc(),
                )
            )
            if symbols:
                stmt = stmt.where(AgentContextFeedback.symbol.in_(symbols))
            if agent_name:
                stmt = stmt.where(AgentContextFeedback.agent_name == agent_name)
            return [dict(row) for row in session.execute(stmt).mappings().all()]

    def update_missing_data_categories(self, *, rows: Sequence[dict[str, Any]]) -> int:
        if not rows:
            return 0
        updates_by_id = {int(row['id']): row['missing_data_categories_payload'] for row in rows}
        updated = 0
        with session_scope() as session:
            stmt = select(AgentContextFeedback).where(AgentContextFeedback.id.in_(tuple(updates_by_id.keys())))
            for record in session.execute(stmt).scalars():
                payload = updates_by_id.get(record.id)
                if record.missing_data_categories_payload == payload:
                    continue
                record.missing_data_categories_payload = payload
                updated += 1
        return updated


class AgentContextMissingDataReportService:
    def __init__(
        self,
        *,
        repository: AgentContextMissingDataReportRepository | None = None,
        category_note_example_limit: int = DEFAULT_CATEGORY_NOTE_EXAMPLE_LIMIT,
        symbol_example_limit: int = DEFAULT_SYMBOL_EXAMPLE_LIMIT,
        uncategorized_example_limit: int = DEFAULT_UNCATEGORIZED_EXAMPLE_LIMIT,
    ) -> None:
        self.repository = repository or AgentContextMissingDataReportRepository()
        self.category_note_example_limit = category_note_example_limit
        self.symbol_example_limit = symbol_example_limit
        self.uncategorized_example_limit = uncategorized_example_limit

    def get_summary(
        self,
        *,
        market_date: str | date | None = None,
        market_date_from: str | date | None = None,
        market_date_to: str | date | None = None,
        symbols: Sequence[str] | None = None,
        agent_name: str | None = None,
        categories: Sequence[str] | None = None,
        persist_missing_categories: bool = False,
    ) -> dict[str, Any]:
        resolved_from, resolved_to = _resolve_date_window(
            market_date=market_date,
            market_date_from=market_date_from,
            market_date_to=market_date_to,
        )
        normalized_symbols = _normalize_symbols(symbols)
        normalized_agent_name = _normalize_agent_name(agent_name) if agent_name is not None else None
        normalized_categories = normalize_missing_data_category_filters(categories)
        rows = self.repository.list_feedback_rows(
            market_date_from=resolved_from,
            market_date_to=resolved_to,
            symbols=normalized_symbols,
            agent_name=normalized_agent_name,
        )

        scanned_roles: set[str] = set()
        scanned_agents: set[str] = set()
        scanned_symbols: set[str] = set()
        matched_roles: set[str] = set()
        matched_agents: set[str] = set()
        matched_symbols: set[str] = set()
        category_aggregates: dict[str, dict[str, Any]] = {}
        date_breakdown: dict[str, dict[str, Any]] = {}
        context_block_aggregates: dict[str, dict[str, Any]] = {}
        symbol_breakdown: dict[str, dict[str, Any]] = {}
        agent_breakdown: dict[str, dict[str, Any]] = {}
        feedback_rows_with_notes = 0
        feedback_rows_with_duplicate_notes = 0
        feedback_rows_matched = 0
        raw_note_mentions_total = 0
        duplicate_note_mentions_total = 0
        unique_note_count = 0
        matched_note_count = 0
        unmatched_note_count = 0
        matched_unclassified_note_mentions = 0
        uncategorized_examples: list[dict[str, Any]] = []
        persist_updates: list[dict[str, Any]] = []

        for row in rows:
            market_date_iso = row['market_date'].isoformat()
            row_key = (row['id'], market_date_iso, row['symbol'], row['agent_name'])
            role = _feedback_role(row)
            scanned_roles.add(role)
            scanned_agents.add(row['agent_name'])
            scanned_symbols.add(row['symbol'])
            day_bucket = date_breakdown.setdefault(
                market_date_iso,
                {
                    'market_date': market_date_iso,
                    'feedback_row_keys': set(),
                    'matched_row_keys': set(),
                    'symbols': set(),
                    'matched_symbols': set(),
                    'raw_note_mentions': 0,
                    'duplicate_note_mentions': 0,
                    'feedback_rows_with_duplicate_notes': 0,
                    'unique_note_count': 0,
                    'matched_note_count': 0,
                    'category_counts': {},
                },
            )
            day_bucket['feedback_row_keys'].add(row_key)
            day_bucket['symbols'].add(row['symbol'])

            raw_note_mentions = extract_missing_data_note_mentions(
                data_gaps_payload=row.get('data_gaps_payload'),
                improvement_notes_payload=row.get('improvement_notes_payload'),
            )
            classification = build_missing_data_classification(
                data_gaps_payload=row.get('data_gaps_payload'),
                improvement_notes_payload=row.get('improvement_notes_payload'),
                context_payload=row.get('context_payload'),
            )
            raw_categories = classification['categories']
            if persist_missing_categories and row.get('missing_data_categories_payload') != raw_categories:
                persist_updates.append(
                    {
                        'id': row['id'],
                        'missing_data_categories_payload': raw_categories,
                    }
                )
            note_entries = classification['notes']
            raw_note_mentions_total += len(raw_note_mentions)
            day_bucket['raw_note_mentions'] += len(raw_note_mentions)
            unique_note_count += len(note_entries)
            day_bucket['unique_note_count'] += len(note_entries)
            duplicate_note_mentions = max(len(raw_note_mentions) - len(note_entries), 0)
            duplicate_note_mentions_total += duplicate_note_mentions
            day_bucket['duplicate_note_mentions'] += duplicate_note_mentions
            if duplicate_note_mentions > 0:
                feedback_rows_with_duplicate_notes += 1
                day_bucket['feedback_rows_with_duplicate_notes'] += 1
            if note_entries:
                feedback_rows_with_notes += 1

            matched_items = [
                item for item in raw_categories if not normalized_categories or item['category'] in normalized_categories
            ]
            matched_categories = {item['category'] for item in matched_items}
            matched_row_notes = {
                note
                for item in matched_items
                for note in item.get('notes', [])
                if isinstance(note, str) and note
            }
            matched_row_note_keys = {note.casefold() for note in matched_row_notes}
            row_unmatched_notes = [
                entry['text']
                for entry in note_entries
                if isinstance(entry.get('text'), str) and entry['text'].casefold() not in matched_row_note_keys
            ]
            unmatched_note_count += len(row_unmatched_notes)
            matched_issue_type_note_mentions_for_row: Counter[str] = Counter()
            row_context_blocks: dict[str, dict[str, Any]] = {}
            if matched_items:
                feedback_rows_matched += 1
                matched_roles.add(role)
                matched_agents.add(row['agent_name'])
                matched_symbols.add(row['symbol'])
                matched_note_count += len(matched_row_notes)
                day_bucket['matched_row_keys'].add(row_key)
                day_bucket['matched_symbols'].add(row['symbol'])
                day_bucket['matched_note_count'] += len(matched_row_notes)

            if row_unmatched_notes and len(uncategorized_examples) < self.uncategorized_example_limit:
                uncategorized_examples.append(
                    {
                        'market_date': market_date_iso,
                        'symbol': row['symbol'],
                        'agent_name': row['agent_name'],
                        'role': role,
                        'note': row_unmatched_notes[0],
                    }
                )

            for item in matched_items:
                category = item['category']
                definition = CATEGORY_DEFINITIONS_BY_KEY[category]
                note_count = int(item.get('note_count') or 0)
                aggregate = category_aggregates.setdefault(
                    category,
                    {
                        'category': category,
                        'label': definition.label,
                        'status': definition.status,
                        'internal_tables': list(definition.internal_tables),
                        'external_providers': list(definition.external_providers),
                        'operator_hint': definition.operator_hint,
                        'feedback_row_keys': set(),
                        'symbols': set(),
                        'agents': set(),
                        'roles': set(),
                        'example_notes': [],
                        'date_counts': {},
                        'context_issue_type_counts': {},
                        'context_issue_type_note_mentions': {},
                    },
                )
                aggregate['feedback_row_keys'].add(row_key)
                aggregate['symbols'].add(row['symbol'])
                aggregate['agents'].add(row['agent_name'])
                aggregate['roles'].add(role)
                for issue_type in item.get('context_issue_types', []):
                    normalized_issue_type = str(issue_type).strip().lower()
                    if not normalized_issue_type:
                        continue
                    aggregate['context_issue_type_counts'][normalized_issue_type] = (
                        aggregate['context_issue_type_counts'].get(normalized_issue_type, 0) + 1
                    )
                    aggregate['context_issue_type_note_mentions'][normalized_issue_type] = (
                        aggregate['context_issue_type_note_mentions'].get(normalized_issue_type, 0) + note_count
                    )
                    matched_issue_type_note_mentions_for_row[normalized_issue_type] += note_count
                if not item.get('context_issue_types'):
                    matched_unclassified_note_mentions += note_count
                for note in item.get('notes', []):
                    if note not in aggregate['example_notes'] and len(aggregate['example_notes']) < self.category_note_example_limit:
                        aggregate['example_notes'].append(note)
                date_counts = aggregate['date_counts'].setdefault(
                    market_date_iso,
                    {'feedback_row_keys': set(), 'note_count': 0},
                )
                date_counts['feedback_row_keys'].add(row_key)
                date_counts['note_count'] += note_count

                per_day_category = day_bucket['category_counts'].setdefault(
                    category,
                    {'feedback_row_keys': set(), 'note_count': 0},
                )
                per_day_category['feedback_row_keys'].add(row_key)
                per_day_category['note_count'] += note_count

                for evidence in item.get('context_evidence', []):
                    if not isinstance(evidence, dict):
                        continue
                    block = str(evidence.get('block') or '').strip()
                    if not block:
                        continue
                    row_block = row_context_blocks.setdefault(
                        block,
                        {
                            'categories': set(),
                            'issue_types': set(),
                            'requested_symbol_coverage': None,
                        },
                    )
                    row_block['categories'].add(category)
                    row_block['issue_types'].update(
                        {
                            str(issue_type).strip().lower()
                            for issue_type in evidence.get('issue_types', [])
                            if str(issue_type).strip()
                        }
                    )
                    coverage = _normalize_requested_symbol_coverage(evidence.get('requested_symbol_coverage'))
                    if row_block['requested_symbol_coverage'] is None and coverage is not None:
                        row_block['requested_symbol_coverage'] = coverage

            if matched_items:
                symbol_bucket = symbol_breakdown.setdefault(
                    row['symbol'],
                    {
                        'feedback_row_keys': set(),
                        'matched_note_count': 0,
                        'raw_note_mentions': 0,
                        'duplicate_note_mentions': 0,
                        'categories': set(),
                        'symbols': set(),
                        'agents': set(),
                        'roles': set(),
                        'context_issue_type_note_mentions': {},
                    },
                )
                agent_bucket = agent_breakdown.setdefault(
                    row['agent_name'],
                    {
                        'feedback_row_keys': set(),
                        'matched_note_count': 0,
                        'raw_note_mentions': 0,
                        'duplicate_note_mentions': 0,
                        'categories': set(),
                        'symbols': set(),
                        'agents': set(),
                        'roles': set(),
                        'context_issue_type_note_mentions': {},
                    },
                )
                for bucket in (symbol_bucket, agent_bucket):
                    bucket['feedback_row_keys'].add(row_key)
                    bucket['matched_note_count'] += len(matched_row_notes)
                    bucket['raw_note_mentions'] += len(raw_note_mentions)
                    bucket['duplicate_note_mentions'] += duplicate_note_mentions
                    bucket['categories'].update(matched_categories)
                    bucket['symbols'].add(row['symbol'])
                    bucket['agents'].add(row['agent_name'])
                    bucket['roles'].add(role)
                    for issue_type, count in matched_issue_type_note_mentions_for_row.items():
                        bucket['context_issue_type_note_mentions'][issue_type] = (
                            bucket['context_issue_type_note_mentions'].get(issue_type, 0) + int(count)
                        )

                for block, row_block in row_context_blocks.items():
                    aggregate = context_block_aggregates.setdefault(
                        block,
                        {
                            'feedback_row_keys': set(),
                            'symbols': set(),
                            'agents': set(),
                            'roles': set(),
                            'categories': set(),
                            'issue_type_counts': {},
                            'coverage_status_counts': {},
                            'coverage_ratio_sum': 0.0,
                            'coverage_ratio_samples': 0,
                            'coverage_ratio_min': None,
                            'coverage_ratio_max': None,
                            'requested_symbol_count_max': 0,
                            'present_rows_max': 0,
                            'partial_rows_max': 0,
                            'unavailable_rows_max': 0,
                        },
                    )
                    aggregate['feedback_row_keys'].add(row_key)
                    aggregate['symbols'].add(row['symbol'])
                    aggregate['agents'].add(row['agent_name'])
                    aggregate['roles'].add(role)
                    aggregate['categories'].update(row_block['categories'])
                    for issue_type in row_block['issue_types']:
                        aggregate['issue_type_counts'][issue_type] = aggregate['issue_type_counts'].get(issue_type, 0) + 1
                    coverage = row_block['requested_symbol_coverage']
                    if coverage is None:
                        continue
                    coverage_status = coverage['coverage_status']
                    aggregate['coverage_status_counts'][coverage_status] = (
                        aggregate['coverage_status_counts'].get(coverage_status, 0) + 1
                    )
                    coverage_ratio = float(coverage['coverage_ratio'])
                    aggregate['coverage_ratio_sum'] += coverage_ratio
                    aggregate['coverage_ratio_samples'] += 1
                    aggregate['coverage_ratio_min'] = (
                        coverage_ratio
                        if aggregate['coverage_ratio_min'] is None
                        else min(float(aggregate['coverage_ratio_min']), coverage_ratio)
                    )
                    aggregate['coverage_ratio_max'] = (
                        coverage_ratio
                        if aggregate['coverage_ratio_max'] is None
                        else max(float(aggregate['coverage_ratio_max']), coverage_ratio)
                    )
                    aggregate['requested_symbol_count_max'] = max(
                        aggregate['requested_symbol_count_max'],
                        int(coverage['requested_symbol_count']),
                    )
                    aggregate['present_rows_max'] = max(aggregate['present_rows_max'], int(coverage['present_rows']))
                    aggregate['partial_rows_max'] = max(aggregate['partial_rows_max'], int(coverage['partial_rows']))
                    aggregate['unavailable_rows_max'] = max(
                        aggregate['unavailable_rows_max'],
                        int(coverage['unavailable_rows']),
                    )

        if persist_missing_categories:
            persisted_updates = self.repository.update_missing_data_categories(rows=persist_updates)
        else:
            persisted_updates = 0

        categories_payload: list[dict[str, Any]] = []
        status_counts = {}
        context_issue_type_counts = {}
        context_issue_type_note_mentions = {}
        for category in sorted(
            category_aggregates.values(),
            key=lambda item: (-len(item['feedback_row_keys']), item['category']),
        ):
            status_counts[category['status']] = status_counts.get(category['status'], 0) + 1
            for issue_type, count in category['context_issue_type_counts'].items():
                context_issue_type_counts[issue_type] = context_issue_type_counts.get(issue_type, 0) + int(count)
            for issue_type, count in category['context_issue_type_note_mentions'].items():
                context_issue_type_note_mentions[issue_type] = (
                    context_issue_type_note_mentions.get(issue_type, 0) + int(count)
                )
            categories_payload.append(
                {
                    'category': category['category'],
                    'label': category['label'],
                    'status': category['status'],
                    'internal_tables': category['internal_tables'],
                    'external_providers': category['external_providers'],
                    'operator_hint': category['operator_hint'],
                    'counts': {
                        'feedback_rows': len(category['feedback_row_keys']),
                        'note_mentions': sum(count['note_count'] for count in category['date_counts'].values()),
                        'symbols': len(category['symbols']),
                        'agents': len(category['agents']),
                        'roles': len(category['roles']),
                    },
                    'affected_symbols': sorted(category['symbols'])[: self.symbol_example_limit],
                    'agents': sorted(category['agents']),
                    'roles': sorted(category['roles']),
                    'example_notes': list(category['example_notes']),
                    'context_issue_type_counts': dict(category['context_issue_type_counts']),
                    'context_issue_type_note_mentions': dict(sorted(category['context_issue_type_note_mentions'].items())),
                    'date_breakdown': [
                        {
                            'market_date': market_date_iso,
                            'feedback_rows': len(day_counts['feedback_row_keys']),
                            'note_mentions': day_counts['note_count'],
                        }
                        for market_date_iso, day_counts in sorted(category['date_counts'].items())
                    ],
                }
            )

        date_breakdown_payload = []
        for market_date_iso, day_bucket in sorted(date_breakdown.items()):
            date_breakdown_payload.append(
                {
                    'market_date': market_date_iso,
                    'feedback_rows_scanned': len(day_bucket['feedback_row_keys']),
                    'feedback_rows_matched': len(day_bucket['matched_row_keys']),
                    'distinct_symbols': len(day_bucket['symbols']),
                    'matched_symbols': len(day_bucket['matched_symbols']),
                    'raw_note_mentions': day_bucket['raw_note_mentions'],
                    'duplicate_note_mentions': day_bucket['duplicate_note_mentions'],
                    'duplicate_note_rate': _ratio(day_bucket['duplicate_note_mentions'], day_bucket['raw_note_mentions']),
                    'feedback_rows_with_duplicate_notes': day_bucket['feedback_rows_with_duplicate_notes'],
                    'unique_note_count': day_bucket['unique_note_count'],
                    'matched_note_count': day_bucket['matched_note_count'],
                    'category_counts': {
                        category: {
                            'feedback_rows': len(counts['feedback_row_keys']),
                            'note_mentions': counts['note_count'],
                        }
                        for category, counts in sorted(day_bucket['category_counts'].items())
                    },
                }
            )

        for category_payload in categories_payload:
            category_payload['trend'] = _build_category_trend(
                category_payload,
                before_market_date=resolved_from.isoformat(),
                after_market_date=resolved_to.isoformat(),
            )

        resolution_metrics = {
            'resolved_by_existing_data': {
                'category_count': 0,
                'feedback_rows': 0,
                'note_mentions': 0,
                'categories': [],
            },
            'requires_external_data': {
                'category_count': 0,
                'feedback_rows': 0,
                'note_mentions': 0,
                'categories': [],
            },
        }
        for category_payload in categories_payload:
            group = (
                'requires_external_data'
                if category_payload['external_providers']
                else 'resolved_by_existing_data'
            )
            group_bucket = resolution_metrics[group]
            group_bucket['category_count'] += 1
            group_bucket['feedback_rows'] += int(category_payload['counts']['feedback_rows'])
            group_bucket['note_mentions'] += int(category_payload['counts']['note_mentions'])
            group_bucket['categories'].append(category_payload['category'])
        for group_bucket in resolution_metrics.values():
            group_bucket['categories'].sort()

        context_block_coverage_payload: list[dict[str, Any]] = []
        for block, aggregate in sorted(
            context_block_aggregates.items(),
            key=lambda item: (-len(item[1]['feedback_row_keys']), item[0]),
        ):
            requested_symbol_coverage = None
            if aggregate['coverage_ratio_samples'] > 0:
                requested_symbol_coverage = {
                    'sample_count': aggregate['coverage_ratio_samples'],
                    'average_coverage_ratio': round(
                        aggregate['coverage_ratio_sum'] / aggregate['coverage_ratio_samples'],
                        4,
                    ),
                    'min_coverage_ratio': round(float(aggregate['coverage_ratio_min']), 4),
                    'max_coverage_ratio': round(float(aggregate['coverage_ratio_max']), 4),
                    'coverage_status_counts': dict(sorted(aggregate['coverage_status_counts'].items())),
                    'max_requested_symbol_count': aggregate['requested_symbol_count_max'],
                    'max_present_rows': aggregate['present_rows_max'],
                    'max_partial_rows': aggregate['partial_rows_max'],
                    'max_unavailable_rows': aggregate['unavailable_rows_max'],
                }
            context_block_coverage_payload.append(
                {
                    'block': block,
                    'feedback_rows': len(aggregate['feedback_row_keys']),
                    'symbols': len(aggregate['symbols']),
                    'agents': len(aggregate['agents']),
                    'roles': len(aggregate['roles']),
                    'categories': sorted(aggregate['categories']),
                    'issue_type_counts': dict(sorted(aggregate['issue_type_counts'].items())),
                    'requested_symbol_coverage': requested_symbol_coverage,
                }
            )

        quality_payload = {
            'note_metrics': {
                'raw_note_mentions': raw_note_mentions_total,
                'unique_note_count': unique_note_count,
                'duplicate_note_mentions': duplicate_note_mentions_total,
                'duplicate_note_rate': _ratio(duplicate_note_mentions_total, raw_note_mentions_total),
                'feedback_rows_with_duplicate_notes': feedback_rows_with_duplicate_notes,
                'matched_note_count': matched_note_count,
            },
            'classification_metrics': {
                'context_issue_type_note_mentions': dict(sorted(context_issue_type_note_mentions.items())),
                'unclassified_note_mentions': matched_unclassified_note_mentions,
            },
            'resolution_metrics': resolution_metrics,
            'agent_breakdown': _serialize_dimension_breakdown(agent_breakdown, key_name='agent_name'),
            'symbol_breakdown': _serialize_dimension_breakdown(symbol_breakdown, key_name='symbol'),
            'context_block_coverage': context_block_coverage_payload,
            'range_comparison': _build_range_comparison(
                date_breakdown_payload=date_breakdown_payload,
                categories_payload=categories_payload,
            ),
        }

        return {
            'request': {
                'market_date': market_date.isoformat() if isinstance(market_date, date) else market_date,
                'market_date_from': resolved_from.isoformat(),
                'market_date_to': resolved_to.isoformat(),
                'symbols': normalized_symbols or None,
                'agent_name': normalized_agent_name,
                'categories': normalized_categories or None,
                'persist_missing_categories': persist_missing_categories,
            },
            'summary': {
                'feedback_rows_scanned': len(rows),
                'feedback_rows_with_notes': feedback_rows_with_notes,
                'feedback_rows_matched': feedback_rows_matched,
                'feedback_rows_unmatched': max(feedback_rows_with_notes - feedback_rows_matched, 0),
                'feedback_rows_with_duplicate_notes': feedback_rows_with_duplicate_notes,
                'raw_note_mentions': raw_note_mentions_total,
                'duplicate_note_mentions': duplicate_note_mentions_total,
                'duplicate_note_rate': _ratio(duplicate_note_mentions_total, raw_note_mentions_total),
                'unique_note_count': unique_note_count,
                'matched_note_count': matched_note_count,
                'unmatched_note_count': unmatched_note_count,
                'distinct_symbols_scanned': len(scanned_symbols),
                'distinct_symbols_matched': len(matched_symbols),
                'distinct_agents_scanned': len(scanned_agents),
                'distinct_agents_matched': len(matched_agents),
                'distinct_roles_scanned': len(scanned_roles),
                'distinct_roles_matched': len(matched_roles),
                'category_count': len(categories_payload),
                'status_counts': status_counts,
                'context_issue_type_counts': context_issue_type_counts,
                'persisted_updates': persisted_updates,
            },
            'quality': quality_payload,
            'categories': categories_payload,
            'date_breakdown': date_breakdown_payload,
            'uncategorized_examples': uncategorized_examples,
        }


agent_context_missing_data_report_service = AgentContextMissingDataReportService()
