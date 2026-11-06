"""Restate stored daily bars to the official KRX values.

Why this exists: until the collection fix, daily bars were built from PyKRX's
per-symbol range endpoint, which differs from the official market-wide table
(and from KIS history, which matches the official table exactly) on close/high/
low for most symbols, and carried no or partial trading value. Those bars are not
"partial days that grow into the final day", so ``kis_final_bar_repair``'s
monotonic extension rule refuses roughly a quarter of them.

This module reuses everything else that tool does -- the KIS fetch, the row
lock and drift check, immutable before/after audit rows, the pending rebuild
batch that gates Core startup -- and replaces only the safety predicate:

* a row is rewritten only when the stored bar differs from the official table
  AND KIS history returns exactly the official values (two independent sources
  agree; this also keeps adjusted-price history out);
* a row that already equals the official table is left alone whatever KIS says;
* the official reference must cover the date completely (both markets), and a
  close change beyond any KRX daily limit is refused as a basis problem.

Run ``python -m app.eod.official_bar_restatement`` for a dry run;
``--apply`` writes. After an apply the documented downstream rebuild of the
pending batches is mandatory (see ``docs/TARGETED_REPAIR_TOOLING.md``).
"""

from __future__ import annotations

import argparse
import asyncio
import csv
from datetime import date
import json
from pathlib import Path
import sys
import time
from typing import Any, Callable, Iterable, Mapping, Sequence

from app.eod.kis_final_bar_repair import (
    DEFAULT_SYMBOL_LIMIT,
    KISFinalBarRepairService,
    ProviderFinalBar,
    StoredFinalBar,
    repair_existing_final_bars,
)

MARKETS = ('KOSPI', 'KOSDAQ')
# Same safety floors the turnover repair uses for a complete market frame.
MINIMUM_MARKET_ROWS = {'KOSPI': 700, 'KOSDAQ': 1200}
COLUMNS = ('시가', '고가', '저가', '종가', '거래량', '거래대금')
# KRX daily price limit is 30%; anything beyond is a basis problem, not a correction.
MAX_PLAUSIBLE_CLOSE_CHANGE = 0.31

Values = tuple[int, int, int, int, int, int]


class OfficialReference:
    """Official OHLCV by (symbol, date), plus the dates it covers completely."""

    def __init__(self, rows: Mapping[tuple[str, date], Values], *, covered_dates: Iterable[date]) -> None:
        self._rows = dict(rows)
        self.covered_dates = set(covered_dates)

    def get(self, symbol: str, trade_date: date) -> Values | None:
        return self._rows.get((symbol, trade_date))

    def covers(self, trade_date: date) -> bool:
        return trade_date in self.covered_dates

    @classmethod
    def load(
        cls,
        dates: Iterable[date],
        *,
        cache_dir: str | Path | None = None,
        fetcher: Callable[[str, str], Any] | None = None,
        minimum_rows: Mapping[str, int] | None = None,
        min_fetch_interval_seconds: float = 20.0,
        max_consecutive_fetch_failures: int = 2,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> 'OfficialReference':
        if fetcher is None:
            from app.eod.pykrx_backfill import _get_market_ohlcv_by_ticker as fetcher  # noqa: N813
        # KRX Data Marketplace blocks an IP for a day when it sees bulk automated
        # reads (observed 2026-11-07 after ~1,000 unthrottled historical requests
        # and a fresh login per process). Historical frames never change once
        # persisted, so read the cache first, space live fetches out, and stop at
        # the first sign of trouble instead of retrying into a ban.
        floors = dict(minimum_rows or MINIMUM_MARKET_ROWS)
        last_live_fetch: float | None = None
        consecutive_failures = 0
        cache = Path(cache_dir) if cache_dir is not None else None
        rows: dict[tuple[str, date], Values] = {}
        covered: set[date] = set()
        for trade_date in sorted(set(dates)):
            compact = trade_date.strftime('%Y%m%d')
            per_market: dict[str, dict[str, Values]] = {}
            for market in MARKETS:
                path = cache / f'{compact}_{market}.csv' if cache is not None else None
                frame_rows = _read_cached(path) if path is not None and path.exists() else None
                if frame_rows is None:
                    if consecutive_failures >= max_consecutive_fetch_failures:
                        per_market = {}
                        break  # circuit open: no further live requests this run
                    if last_live_fetch is not None:
                        wait = min_fetch_interval_seconds - (monotonic() - last_live_fetch)
                        if wait > 0:
                            sleep(wait)
                    last_live_fetch = monotonic()
                    try:
                        frame_rows = _frame_to_rows(fetcher(compact, market))
                        consecutive_failures = 0
                    except Exception:  # noqa: BLE001
                        frame_rows = None
                        consecutive_failures += 1
                if frame_rows is None or len(frame_rows) < floors[market]:
                    per_market = {}
                    break
                per_market[market] = frame_rows
            if len(per_market) != len(MARKETS):
                continue  # never keep half a reference for a date
            if cache is not None:
                cache.mkdir(parents=True, exist_ok=True)
                for market, frame_rows in per_market.items():
                    path = cache / f'{compact}_{market}.csv'
                    if not path.exists():
                        _write_cache(path, frame_rows)
            for frame_rows in per_market.values():
                for symbol, values in frame_rows.items():
                    rows[(symbol, trade_date)] = values
            covered.add(trade_date)
        return cls(rows, covered_dates=covered)


def _frame_to_rows(frame: Any) -> dict[str, Values]:
    if frame is None or getattr(frame, 'empty', True):
        raise ValueError('official market frame is empty')
    out: dict[str, Values] = {}
    for code, row in frame.iterrows():
        out[str(code).strip().upper()] = tuple(int(round(float(row[column]))) for column in COLUMNS)  # type: ignore[assignment]
    return out


def _read_cached(path: Path) -> dict[str, Values] | None:
    try:
        with path.open(newline='', encoding='utf-8') as handle:
            reader = csv.DictReader(handle)
            return {
                str(row['code']).strip().upper(): tuple(int(round(float(row[column]))) for column in COLUMNS)  # type: ignore[misc]
                for row in reader
            }
    except (OSError, KeyError, ValueError):
        return None


def _write_cache(path: Path, rows: Mapping[str, Values]) -> None:
    with path.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.writer(handle)
        writer.writerow(['code', *COLUMNS])
        for code, values in rows.items():
            writer.writerow([code, *values])


def plan_official_restatement(
    *,
    stored_rows: Sequence[StoredFinalBar],
    provider_bars: Mapping[tuple[str, date], ProviderFinalBar],
    official: OfficialReference,
) -> dict[str, Any]:
    stored_by_key: dict[tuple[str, date], StoredFinalBar] = {}
    for row in stored_rows:
        key = (row.symbol, row.trade_date)
        if key in stored_by_key:
            raise RuntimeError(f'duplicate stored final-bar key: {key}')
        stored_by_key[key] = row

    missing: list[tuple[str, date]] = []
    unsafe: list[dict[str, str]] = []
    unchanged = 0
    updates: list[tuple[StoredFinalBar, ProviderFinalBar]] = []

    def refuse(row: StoredFinalBar, reason: str) -> None:
        unsafe.append({'symbol': row.symbol, 'trade_date': row.trade_date.isoformat(), 'reason': reason})

    for key, row in stored_by_key.items():
        provider = provider_bars.get(key)
        if not official.covers(row.trade_date):
            refuse(row, 'official_reference_unavailable')
            continue
        target = official.get(row.symbol, row.trade_date)
        if target is None:
            refuse(row, 'official_reference_missing')
            continue
        if row.values == target:
            unchanged += 1
            continue
        if provider is None:
            missing.append(key)
            continue
        if provider.values != target:
            refuse(row, 'provider_official_disagree')
            continue
        if row.close_price > 0 and abs(provider.close_price - row.close_price) / row.close_price > MAX_PLAUSIBLE_CLOSE_CHANGE:
            refuse(row, 'implausible_close_change')
            continue
        updates.append((row, provider))

    missing.sort()
    unsafe.sort(key=lambda item: (item['symbol'], item['trade_date']))
    return {
        'target_count': len(stored_by_key),
        'provider_matched_count': len(stored_by_key) - len(missing),
        'missing_provider_row_count': len(missing),
        'missing_provider_rows': [
            {'symbol': symbol, 'trade_date': trade_date.isoformat()} for symbol, trade_date in missing
        ],
        'unsafe_row_count': len(unsafe),
        'unsafe_rows': unsafe,
        'unchanged_count': unchanged,
        'safe_changed_count': len(updates),
        'safe_changed_symbols': sorted({provider.symbol for _, provider in updates}),
        'safe_changed_trade_dates': sorted({provider.trade_date.isoformat() for _, provider in updates}),
        'updates': updates,
    }


def make_planner(official: OfficialReference) -> Callable[..., dict[str, Any]]:
    def planner(*, stored_rows: Sequence[StoredFinalBar], provider_bars: Mapping[tuple[str, date], ProviderFinalBar]):
        return plan_official_restatement(stored_rows=stored_rows, provider_bars=provider_bars, official=official)

    return planner


def build_service(official: OfficialReference) -> KISFinalBarRepairService:
    planner = make_planner(official)

    def repairer(**kwargs: Any) -> dict[str, Any]:
        return repair_existing_final_bars(planner=planner, **kwargs)

    return KISFinalBarRepairService(repairer=repairer)


def _dates_between(start: date, end: date) -> list[date]:
    from sqlalchemy import select

    from app.db import session_scope
    from app.db_models import DailyPriceBar

    with session_scope() as session:
        return list(
            session.scalars(
                select(DailyPriceBar.trade_date)
                .where(DailyPriceBar.trade_date >= start, DailyPriceBar.trade_date <= end)
                .distinct()
                .order_by(DailyPriceBar.trade_date)
            ).all()
        )


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='Restate stored bars to official KRX values (KIS must agree).')
    parser.add_argument('--start-date', required=True)
    parser.add_argument('--end-date', required=True)
    parser.add_argument('--after-symbol')
    parser.add_argument('--symbols', help='comma-separated explicit symbols for a retry page')
    parser.add_argument('--symbol-limit', type=int, default=DEFAULT_SYMBOL_LIMIT)
    parser.add_argument('--official-cache-dir', help='directory of per-date official frames (reused, filled on demand)')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--apply', action='store_true')
    mode.add_argument('--dry-run', action='store_false', dest='apply')
    parser.set_defaults(apply=False)
    args = parser.parse_args(list(argv) if argv is not None else None)

    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    try:
        official = OfficialReference.load(_dates_between(start, end), cache_dir=args.official_cache_dir)
        result = asyncio.run(
            build_service(official).repair_page(
                start_date=start,
                end_date=end,
                after_symbol=args.after_symbol,
                symbols=[item.strip() for item in args.symbols.split(',') if item.strip()] if args.symbols else None,
                symbol_limit=args.symbol_limit,
                apply=args.apply,
            )
        )
    except Exception as exc:  # noqa: BLE001
        print(
            json.dumps(
                {'status': 'failed', 'applied': False, 'error_type': type(exc).__name__, 'error_message': str(exc)},
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1
    result['official_reference_uncovered_dates'] = sorted(
        d.isoformat() for d in _dates_between(start, end) if not official.covers(d)
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    if result.get('status') in {'ready', 'completed'}:
        return 0
    if result.get('status') in {'partial', 'blocked', 'rebuild_required'}:
        return 2
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
