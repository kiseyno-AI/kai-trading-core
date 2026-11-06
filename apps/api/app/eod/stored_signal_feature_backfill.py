"""Backfill signal-feature rows that were never written for a trading day.

``exact_signal_feature_repair`` only refreshes rows that already exist and only
for dates frozen in a pending price-bar rebuild batch.  A day whose collection
never ran (or ran for a handful of symbols) has no such batch, so nothing could
fill it.  This tool fills exactly the missing rows from stored bars and factor
rows, using the same deterministic builders as the daily collection.

Guarantees:
* dry-run is the default; ``--apply`` is required to write;
* only symbols that have a daily bar but no signal-feature row are written —
  existing rows are never touched;
* rows carry no KIS ranking evidence and no quote snapshot, and are tagged
  ``collection.repair_mode`` so they stay distinguishable;
* ``--compare-date`` rebuilds a fully collected day in memory and reports how
  closely the stored-data rows match what the live collection stored, which is
  the fidelity check for this approach.
"""

from __future__ import annotations

import argparse
from contextlib import AbstractContextManager
from datetime import date
import json
from typing import Any, Callable, Mapping, Sequence

from app.eod.exact_signal_feature_repair import exact_signal_feature_guard
from app.eod.finality import validate_finalized_krx_write_date
from app.market_data_store import (
    list_symbols_with_daily_bar_on_date,
    load_raw_daily_signal_features_for_repair,
    upsert_daily_signal_features,
)
from app.signals.daily_signal_features import daily_signal_feature_service

# Fields compared for fidelity: the deterministic content, not capture times.
_COMPARED_KEY = 'fallback_features'
_IGNORED_FALLBACK_GROUPS = ('quote_snapshot',)


def _stored_symbols(market_date: str, symbols: Sequence[str]) -> set[str]:
    rows = load_raw_daily_signal_features_for_repair(
        market_date=market_date,
        symbols=symbols,
    )
    return {str(row['symbol']) for row in rows}


def plan_backfill(
    market_date: str,
    *,
    service: Any = daily_signal_feature_service,
    bar_symbols_loader: Callable[[str], Sequence[str]] = list_symbols_with_daily_bar_on_date,
    stored_symbols_loader: Callable[[str, Sequence[str]], set[str]] = _stored_symbols,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Build (without writing) the rows missing for ``market_date``."""

    bar_symbols = sorted(set(bar_symbols_loader(market_date)))
    existing = stored_symbols_loader(market_date, bar_symbols)
    missing = [symbol for symbol in bar_symbols if symbol not in existing]
    rows: list[dict[str, Any]] = []
    build_summary: dict[str, Any] = {'without_stored_inputs': []}
    if missing:
        rows, build_summary = service.build_stored_owned_fallback_rows(
            market_date=market_date,
            symbols=missing,
        )
    return rows, {
        'market_date': market_date,
        'daily_bar_symbol_count': len(bar_symbols),
        'existing_row_count': len(existing),
        'missing_symbol_count': len(missing),
        'built_row_count': len(rows),
        'without_stored_inputs': build_summary.get('without_stored_inputs', []),
    }


def _comparable(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = row.get('feature_payload') or {}
    groups = dict(payload.get(_COMPARED_KEY) or {})
    for name in _IGNORED_FALLBACK_GROUPS:
        groups.pop(name, None)
    return groups


def compare_with_stored(
    market_date: str,
    *,
    sample_limit: int = 5,
    service: Any = daily_signal_feature_service,
    bar_symbols_loader: Callable[[str], Sequence[str]] = list_symbols_with_daily_bar_on_date,
) -> dict[str, Any]:
    """Rebuild a collected day in memory and diff its fallback content."""

    symbols = sorted(set(bar_symbols_loader(market_date)))
    stored_rows = {
        str(row['symbol']): row
        for row in load_raw_daily_signal_features_for_repair(
            market_date=market_date,
            symbols=symbols,
        )
    }
    rebuilt, _ = service.build_stored_owned_fallback_rows(
        market_date=market_date,
        symbols=sorted(stored_rows),
    )
    identical = 0
    differing: list[str] = []
    group_mismatch: dict[str, int] = {}
    for row in rebuilt:
        stored = stored_rows[row['symbol']]
        mine, theirs = _comparable(row), _comparable(stored)
        if mine == theirs:
            identical += 1
            continue
        differing.append(row['symbol'])
        for group in sorted(set(mine) | set(theirs)):
            if mine.get(group) != theirs.get(group):
                group_mismatch[group] = group_mismatch.get(group, 0) + 1
    return {
        'market_date': market_date,
        'stored_row_count': len(stored_rows),
        'rebuilt_row_count': len(rebuilt),
        'identical_fallback_rows': identical,
        'differing_row_count': len(differing),
        'differing_groups': group_mismatch,
        'differing_symbol_sample': differing[:sample_limit],
    }


def apply_backfill(
    market_date: str,
    *,
    guard_factory: Callable[..., AbstractContextManager[Any]] = exact_signal_feature_guard,
    finality_validator: Callable[..., Any] = validate_finalized_krx_write_date,
    service: Any = daily_signal_feature_service,
    persister: Callable[..., int] = upsert_daily_signal_features,
    bar_symbols_loader: Callable[[str], Sequence[str]] = list_symbols_with_daily_bar_on_date,
    stored_symbols_loader: Callable[[str, Sequence[str]], set[str]] = _stored_symbols,
) -> dict[str, Any]:
    """Write the missing rows for one date under the derived-stage writer fence."""

    target = finality_validator(
        market_date,
        operation='stored-data signal-feature backfill',
    )
    with guard_factory(market_date=target) as guard:
        acquired, reason = guard
        if not acquired:
            # An active daily/EOD/DART writer or an unavailable advisory lock:
            # write nothing and let the caller retry later.
            return {
                'market_date': market_date,
                'applied': False,
                'status': 'blocked',
                'reason': str(reason or 'active_writer'),
                'persisted_row_count': 0,
            }
        # Plan inside the fence so a concurrent writer cannot change the gap
        # between "what is missing" and "what is written".
        rows, summary = plan_backfill(
            market_date,
            service=service,
            bar_symbols_loader=bar_symbols_loader,
            stored_symbols_loader=stored_symbols_loader,
        )
        persisted = (
            persister(rows, delete_rows_outside_scope=False) if rows else 0
        )
        if persisted != len(rows):
            raise RuntimeError(
                'stored signal-feature backfill persistence mismatch: '
                f'expected={len(rows)}, observed={persisted}'
            )
    return {**summary, 'applied': True, 'status': 'completed', 'persisted_row_count': persisted}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--market-date', action='append', default=[])
    parser.add_argument(
        '--compare-date',
        action='append',
        default=[],
        help='fully collected date to rebuild in memory and diff (read-only)',
    )
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args(argv)
    if not args.market_date and not args.compare_date:
        parser.error('give at least one --market-date or --compare-date')

    results: dict[str, Any] = {'compare': [], 'plan': [], 'applied': args.apply}
    for value in args.compare_date:
        results['compare'].append(
            compare_with_stored(date.fromisoformat(value).isoformat())
        )
    for value in args.market_date:
        market_date = date.fromisoformat(value).isoformat()
        if args.apply:
            results['plan'].append(apply_backfill(market_date))
        else:
            _, summary = plan_backfill(market_date)
            results['plan'].append({**summary, 'applied': False})
    print(json.dumps(results, ensure_ascii=False, indent=2, sort_keys=True))
    # A fence that refused the write is not a success: make it visible to cron/CI.
    return 2 if any(item.get('status') == 'blocked' for item in results['plan']) else 0


if __name__ == '__main__':
    raise SystemExit(main())
