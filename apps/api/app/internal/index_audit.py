from __future__ import annotations

from collections import defaultdict
from typing import Any

from sqlalchemy import text

from app.db import session_scope


HEAVY_TABLE_SCAN_SQL = text(
    """
    select schemaname, relname, seq_scan, seq_tup_read, coalesce(idx_scan, 0) as idx_scan, coalesce(n_live_tup, 0) as n_live_tup
    from pg_stat_user_tables
    order by seq_tup_read desc nulls last
    """
)

INDEX_LIST_SQL = text(
    """
    select tablename, indexname, indexdef
    from pg_indexes
    where schemaname = 'public'
    order by tablename, indexname
    """
)


def _suggest_indexes(table_name: str, index_defs: list[str]) -> list[str]:
    defs = ' '.join(index_defs)
    suggestions: list[str] = []

    if table_name == 'daily_price_bars' and 'trade_date' not in defs:
        suggestions.append('consider index on trade_date DESC')
    if table_name in {'daily_technical_snapshots', 'daily_action_summaries', 'daily_trending_ranks'} and 'market_date' not in defs:
        suggestions.append('consider index on market_date DESC')
    if table_name == 'dart_fundamentals_rows' and 'business_year' not in defs:
        suggestions.append('consider composite index for symbol_id, business_year DESC or snapshot/date access patterns')
    if table_name == 'dart_disclosure_events' and 'receipt_date' not in defs and 'rcept_dt' not in defs:
        suggestions.append('consider index on receipt_date DESC if recent-event reads matter')

    return suggestions


def collect_index_audit(*, min_seq_tup_read: int = 10_000) -> dict[str, Any]:
    with session_scope() as session:
        tables = session.execute(HEAVY_TABLE_SCAN_SQL).all()
        indexes = session.execute(INDEX_LIST_SQL).all()

    indexes_by_table: dict[str, list[dict[str, str]]] = defaultdict(list)
    index_defs_by_table: dict[str, list[str]] = defaultdict(list)
    for row in indexes:
        mapping = dict(row._mapping)
        indexes_by_table[mapping['tablename']].append(
            {
                'index_name': mapping['indexname'],
                'index_def': mapping['indexdef'],
            }
        )
        index_defs_by_table[mapping['tablename']].append(mapping['indexdef'])

    heavy_tables = []
    recommendations = []
    for row in tables:
        mapping = dict(row._mapping)
        if (mapping.get('seq_tup_read') or 0) < min_seq_tup_read:
            continue
        table_name = mapping['relname']
        table_entry = {
            'table_name': table_name,
            'live_rows_estimate': int(mapping.get('n_live_tup') or 0),
            'seq_scan': int(mapping.get('seq_scan') or 0),
            'seq_tup_read': int(mapping.get('seq_tup_read') or 0),
            'idx_scan': int(mapping.get('idx_scan') or 0),
            'indexes': indexes_by_table.get(table_name, []),
        }
        heavy_tables.append(table_entry)

        suggestions = _suggest_indexes(table_name, index_defs_by_table.get(table_name, []))
        if suggestions:
            recommendations.append(
                {
                    'table_name': table_name,
                    'suggestions': suggestions,
                }
            )

    return {
        'heavy_tables': heavy_tables,
        'recommendations': recommendations,
    }


def print_index_audit(*, min_seq_tup_read: int = 10_000) -> None:
    report = collect_index_audit(min_seq_tup_read=min_seq_tup_read)
    print('| table | live_rows | seq_scan | seq_tup_read | idx_scan | indexes |')
    print('|---|---:|---:|---:|---:|---|')
    for entry in report['heavy_tables']:
        index_names = ', '.join(index['index_name'] for index in entry['indexes']) or '(none)'
        print(
            f"| {entry['table_name']} | {entry['live_rows_estimate']} | {entry['seq_scan']} | "
            f"{entry['seq_tup_read']} | {entry['idx_scan']} | {index_names} |"
        )

    print('\n## Suggested indexes')
    if not report['recommendations']:
        print('- no simple recommendations from this audit')
        return
    for item in report['recommendations']:
        print(f"- {item['table_name']}: {', '.join(item['suggestions'])}")


if __name__ == '__main__':
    print_index_audit()
