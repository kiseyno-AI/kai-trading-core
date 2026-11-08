"""Next-day check that stored daily bars equal the official KRX table.

Why this exists: from 2026-09-14 to 2026-11-06 the daily job stored provisional
bars from PyKRX's per-symbol endpoint (about 2,200 of ~2,700 rows a day differed
from the official table) and nothing noticed for eight weeks. The collection now
reads the official market-wide frame first, but two things can still bring
provisional bars back silently: the per-symbol fallback (official frame missing,
lacks a symbol, or an incoherent row) and an official frame that is not yet final
at the 20:10 KST collection time.

So once per finalized session, outside the CLOSED pipeline window, this job reads
the official frame for the latest stored session (one call per market, cached on
disk under ``var/official-daily-frames`` where ``official_bar_restatement`` can
reuse it), compares all six fields of every stored bar, and records the outcome:

* ``var/daily-bar-reconciliation/<date>.json`` (durable, one per session),
* ``background_runner.snapshot()['daily_bar_reconciliation']`` (``GET /eod/status``),
* one WARNING log line when stored bars differ or the collection used the
  per-symbol fallback.

It only reads. Fixing a mismatch rewrites prices and needs the audited
restatement plus downstream rebuild (``docs/TARGETED_REPAIR_TOOLING.md``).

Run ``python -m app.eod.daily_bar_reconciliation --market-date YYYY-MM-DD`` for a
one-off check (prints JSON, writes the report, exit 3 on a mismatch).
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import date, datetime
import json
import logging
import os
from pathlib import Path
import sys
from typing import Any, Callable, Iterable, Mapping
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

KST = ZoneInfo('Asia/Seoul')
SCHEMA_VERSION = 'daily_bar_reconciliation_v1'
FIELDS = ('open', 'high', 'low', 'close', 'volume', 'turnover')
PRICE_COLLECTION_STAGE_ID = 'price_collection'
# Statuses that settle a session; anything else is retried (bounded per process).
FINAL_STATUSES = frozenset({'ok', 'mismatch'})
MAX_ATTEMPTS_PER_DATE = 3
SAMPLE_LIMIT = 20

Values = tuple[int, int, int, int, int, int]


def default_var_dir() -> Path:
    return Path(__file__).resolve().parents[2] / 'var'


def compare_bars(stored: Mapping[str, Values], official: Mapping[str, Values]) -> dict[str, Any]:
    """Compare stored bars with official rows on all six fields."""

    common = stored.keys() & official.keys()
    mismatched = sorted(symbol for symbol in common if tuple(stored[symbol]) != tuple(official[symbol]))
    by_field = {
        name: sum(1 for symbol in mismatched if stored[symbol][index] != official[symbol][index])
        for index, name in enumerate(FIELDS)
    }
    not_stored = sorted(official.keys() - stored.keys())
    not_official = sorted(stored.keys() - official.keys())
    return {
        'stored_count': len(stored),
        'official_count': len(official),
        'compared_count': len(common),
        'mismatch_count': len(mismatched),
        'mismatch_by_field': by_field,
        'mismatch_samples': [
            {
                'symbol': symbol,
                'stored': dict(zip(FIELDS, stored[symbol])),
                'official': dict(zip(FIELDS, official[symbol])),
            }
            for symbol in mismatched[:SAMPLE_LIMIT]
        ],
        # Informational: the stored universe is narrower than the market table
        # (e.g. symbols outside the collection universe), so these do not fail the check.
        'official_not_stored_count': len(not_stored),
        'official_not_stored_samples': not_stored[:SAMPLE_LIMIT],
        'stored_not_official_count': len(not_official),
        'stored_not_official_samples': not_official[:SAMPLE_LIMIT],
    }


def _find_price_source(payload: Any, depth: int = 0) -> dict[str, int] | None:
    if depth > 4:
        return None
    if isinstance(payload, Mapping):
        value = payload.get('price_source')
        if isinstance(value, Mapping) and 'per_symbol_bars' in value:
            return {str(key): int(value[key] or 0) for key in value}
        for nested in payload.values():
            found = _find_price_source(nested, depth + 1)
            if found is not None:
                return found
    return None


# ---------------------------------------------------------------------- DB readers
def load_latest_stored_session(before: date) -> date | None:
    from sqlalchemy import func, select

    from app.db import session_scope
    from app.db_models import DailyPriceBar

    with session_scope() as session:
        return session.scalar(select(func.max(DailyPriceBar.trade_date)).where(DailyPriceBar.trade_date < before))


def load_stored_bars(market_date: date) -> dict[str, Values]:
    from sqlalchemy import select

    from app.db import session_scope
    from app.db_models import DailyPriceBar, Symbol

    def as_int(value: Any) -> int:
        return int(round(float(value or 0)))

    with session_scope() as session:
        rows = session.execute(
            select(
                Symbol.symbol,
                DailyPriceBar.open_price,
                DailyPriceBar.high_price,
                DailyPriceBar.low_price,
                DailyPriceBar.close_price,
                DailyPriceBar.volume,
                DailyPriceBar.turnover,
            )
            .join(Symbol, Symbol.id == DailyPriceBar.symbol_id)
            .where(DailyPriceBar.trade_date == market_date, Symbol.market.in_(('KOSPI', 'KOSDAQ')))
        ).all()
    return {str(symbol).strip().upper(): tuple(as_int(v) for v in values) for symbol, *values in rows}  # type: ignore[misc]


def load_collection_price_source(market_date: date) -> dict[str, Any] | None:
    """Price-source counters of the latest price-collection stage record for the date."""

    from sqlalchemy import select

    from app.db import session_scope
    from app.db_models import DailyPipelineStageRun

    with session_scope() as session:
        row = session.execute(
            select(DailyPipelineStageRun.status, DailyPipelineStageRun.details)
            .where(
                DailyPipelineStageRun.market_date == market_date,
                DailyPipelineStageRun.stage_id == PRICE_COLLECTION_STAGE_ID,
            )
            .order_by(DailyPipelineStageRun.updated_at.desc(), DailyPipelineStageRun.id.desc())
            .limit(1)
        ).first()
    if row is None:
        return None
    status, details = row
    return {'stage_status': status, 'price_source': _find_price_source(details)}


def _load_reference(dates: Iterable[date], *, cache_dir: Path):
    from app.eod.official_bar_restatement import OfficialReference

    return OfficialReference.load(dates, cache_dir=cache_dir)


def _validate_finalized(market_date: date, now: datetime) -> None:
    from app.eod.finality import validate_finalized_krx_write_date

    validate_finalized_krx_write_date(market_date, now=now, operation='daily bar reconciliation')


# ---------------------------------------------------------------------- service
@dataclass
class DailyBarReconciliation:
    reports_dir: Path
    frames_dir: Path
    session_loader: Callable[[date], date | None] = load_latest_stored_session
    stored_loader: Callable[[date], Mapping[str, Values]] = load_stored_bars
    collection_loader: Callable[[date], Mapping[str, Any] | None] = load_collection_price_source
    reference_loader: Callable[..., Any] = _load_reference
    finality_validator: Callable[[date, datetime], None] = _validate_finalized
    clock: Callable[[], datetime] = lambda: datetime.now(KST)
    _attempts: dict[date, int] = field(default_factory=dict, init=False)
    last_result: dict[str, Any] | None = field(default=None, init=False)

    def report_path(self, market_date: date) -> Path:
        return self.reports_dir / f'{market_date.isoformat()}.json'

    def run_if_due(self, *, now: datetime | None = None) -> dict[str, Any] | None:
        """Check the latest finalized session once; cheap no-op once it is settled."""

        current = now or self.clock()
        target = self.session_loader(current.astimezone(KST).date())
        if target is None:
            return self.last_result
        try:
            self.finality_validator(target, current)
        except Exception:  # noqa: BLE001
            return self.last_result
        existing = self._read_report(target)
        if existing is not None and existing.get('status') in FINAL_STATUSES:
            self.last_result = existing
            return existing
        if self._attempts.get(target, 0) >= MAX_ATTEMPTS_PER_DATE:
            return self.last_result
        self._attempts[target] = self._attempts.get(target, 0) + 1
        return self.check(target, now=current)

    def check(self, market_date: date, *, now: datetime | None = None) -> dict[str, Any]:
        current = now or self.clock()
        base = {
            'schema_version': SCHEMA_VERSION,
            'market_date': market_date.isoformat(),
            'checked_at': current.isoformat(),
            'attempt': self._attempts.get(market_date, 1),
        }
        collection = self.collection_loader(market_date)
        price_source = (collection or {}).get('price_source')
        base['collection'] = dict(collection) if collection is not None else None
        official = self.reference_loader([market_date], cache_dir=self.frames_dir)
        if not official.covers(market_date):
            result = {**base, 'status': 'reference_unavailable', 'warnings': ['official_frame_unavailable']}
            self._write_report(market_date, result)
            logger.warning(
                'Daily bar reconciliation: official KRX frame unavailable for %s (attempt %s/%s)',
                market_date,
                base['attempt'],
                MAX_ATTEMPTS_PER_DATE,
            )
            self.last_result = result
            return result

        comparison = compare_bars(self.stored_loader(market_date), official.rows_for(market_date))
        warnings: list[str] = []
        if comparison['mismatch_count']:
            warnings.append('stored_bars_differ_from_official')
        per_symbol = int((price_source or {}).get('per_symbol_bars') or 0)
        if per_symbol:
            warnings.append('collection_used_per_symbol_fallback')
        if price_source is None:
            warnings.append('collection_price_source_unrecorded')
        result = {
            **base,
            'status': 'mismatch' if comparison['mismatch_count'] else 'ok',
            'warnings': warnings,
            **comparison,
        }
        self._write_report(market_date, result)
        if comparison['mismatch_count'] or per_symbol:
            logger.warning(
                'Daily bar reconciliation: %s has %d of %d stored bars that differ from the official KRX table '
                '(by field %s; per-symbol fallback bars %d; e.g. %s). Restate with app.eod.official_bar_restatement.',
                market_date,
                comparison['mismatch_count'],
                comparison['compared_count'],
                comparison['mismatch_by_field'],
                per_symbol,
                ', '.join(item['symbol'] for item in comparison['mismatch_samples'][:5]) or '-',
            )
        else:
            logger.info(
                'Daily bar reconciliation: %s ok (%d bars match the official KRX table)',
                market_date,
                comparison['compared_count'],
            )
        self.last_result = result
        return result

    def _read_report(self, market_date: date) -> dict[str, Any] | None:
        path = self.report_path(market_date)
        try:
            with path.open(encoding='utf-8') as handle:
                payload = json.load(handle)
        except (OSError, ValueError):
            return None
        return payload if isinstance(payload, dict) else None

    def _write_report(self, market_date: date, result: Mapping[str, Any]) -> None:
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        path = self.report_path(market_date)
        tmp = path.with_suffix('.json.tmp')
        with tmp.open('w', encoding='utf-8') as handle:
            json.dump(result, handle, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, path)


def summarize(result: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Compact form for the runner snapshot."""

    if result is None:
        return None
    keep = (
        'market_date',
        'checked_at',
        'status',
        'warnings',
        'compared_count',
        'mismatch_count',
        'mismatch_by_field',
        'official_not_stored_count',
        'attempt',
    )
    summary = {key: result[key] for key in keep if key in result}
    summary['mismatch_symbols'] = [item['symbol'] for item in result.get('mismatch_samples') or []][:5]
    collection = result.get('collection') or {}
    summary['collection_price_source'] = collection.get('price_source')
    return summary


_service: DailyBarReconciliation | None = None


def get_service() -> DailyBarReconciliation:
    global _service
    if _service is None:
        var_dir = default_var_dir()
        _service = DailyBarReconciliation(
            reports_dir=var_dir / 'daily-bar-reconciliation',
            frames_dir=var_dir / 'official-daily-frames',
        )
    return _service


def run_if_due() -> dict[str, Any] | None:
    """Entry point for the background runner (blocking, runs in a thread)."""

    return get_service().run_if_due()


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='Compare stored daily bars of one session with the official KRX table.')
    parser.add_argument('--market-date', required=True, help='YYYY-MM-DD')
    args = parser.parse_args(list(argv) if argv is not None else None)
    service = get_service()
    try:
        result = service.check(date.fromisoformat(args.market_date))
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({'status': 'failed', 'error_type': type(exc).__name__, 'error_message': str(exc)}),
              file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    if result['status'] == 'ok':
        return 0
    return 3 if result['status'] == 'mismatch' else 1


if __name__ == '__main__':
    raise SystemExit(main())
