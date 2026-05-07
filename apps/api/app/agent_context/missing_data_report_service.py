from __future__ import annotations

from datetime import date
from typing import Any, Sequence

from sqlalchemy import select

from app.agent_context.missing_data_taxonomy import (
    CATEGORY_DEFINITIONS_BY_KEY,
    build_missing_data_classification,
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
        feedback_rows_with_notes = 0
        feedback_rows_matched = 0
        unique_note_count = 0
        matched_note_count = 0
        unmatched_note_count = 0
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
                    'unique_note_count': 0,
                    'matched_note_count': 0,
                    'category_counts': {},
                },
            )
            day_bucket['feedback_row_keys'].add(row_key)
            day_bucket['symbols'].add(row['symbol'])

            classification = build_missing_data_classification(
                data_gaps_payload=row.get('data_gaps_payload'),
                improvement_notes_payload=row.get('improvement_notes_payload'),
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
            unique_note_count += len(note_entries)
            day_bucket['unique_note_count'] += len(note_entries)
            if note_entries:
                feedback_rows_with_notes += 1

            matched_items = [
                item for item in raw_categories if not normalized_categories or item['category'] in normalized_categories
            ]
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
                    },
                )
                aggregate['feedback_row_keys'].add(row_key)
                aggregate['symbols'].add(row['symbol'])
                aggregate['agents'].add(row['agent_name'])
                aggregate['roles'].add(role)
                for note in item.get('notes', []):
                    if note not in aggregate['example_notes'] and len(aggregate['example_notes']) < self.category_note_example_limit:
                        aggregate['example_notes'].append(note)
                date_counts = aggregate['date_counts'].setdefault(
                    market_date_iso,
                    {'feedback_row_keys': set(), 'note_count': 0},
                )
                date_counts['feedback_row_keys'].add(row_key)
                date_counts['note_count'] += int(item.get('note_count') or 0)

                per_day_category = day_bucket['category_counts'].setdefault(
                    category,
                    {'feedback_row_keys': set(), 'note_count': 0},
                )
                per_day_category['feedback_row_keys'].add(row_key)
                per_day_category['note_count'] += int(item.get('note_count') or 0)

        if persist_missing_categories:
            persisted_updates = self.repository.update_missing_data_categories(rows=persist_updates)
        else:
            persisted_updates = 0

        categories_payload: list[dict[str, Any]] = []
        status_counts = {}
        for category in sorted(
            category_aggregates.values(),
            key=lambda item: (-len(item['feedback_row_keys']), item['category']),
        ):
            status_counts[category['status']] = status_counts.get(category['status'], 0) + 1
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
                'persisted_updates': persisted_updates,
            },
            'categories': categories_payload,
            'date_breakdown': date_breakdown_payload,
            'uncategorized_examples': uncategorized_examples,
        }


agent_context_missing_data_report_service = AgentContextMissingDataReportService()
