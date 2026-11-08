"""Next-day check that stored daily bars equal the KRX regular-session bars.

Why this exists: stored bars are the input of every return, factor, signal and
evaluation, and a wrong source can go unnoticed for weeks. On 2026-11-08 we found
that PyKRX's market-wide ticker frame, which had been treated as the official
reference, differs from the KRX regular session for most symbols since about
2026-09-14 (it appears to carry trading outside the regular session), and that the
KIS ``J`` daily chart is not stable either (some dates carry the regular close,
others a different close or volume). The stored per-symbol bars equalled Naver's
daily chart on every sampled price (36/36 symbol-dates), so Naver is the reference.

Once per finalized session, outside the CLOSED pipeline window, this job takes a
deterministic, date-seeded sample of the session's stored KOSPI/KOSDAQ symbols
(``DAILY_BAR_RECONCILIATION_SAMPLE_SIZE``, default 200; a different sample every
day), reads their Naver bar for that date (one request per symbol, spaced), and
compares open/high/low/close and volume:

* a price difference makes the session ``mismatch`` and logs one WARNING;
* a volume-only difference (small after-hours volume is counted differently by
  sources) stays ``ok`` with a ``stored_volume_differs_from_reference`` note;
* the report is ``var/daily-bar-reconciliation/<date>.json`` and the summary is
  ``background_runner.snapshot()['daily_bar_reconciliation']`` (``GET /eod/status``).

It only reads. Naver has no trading value, so turnover is not checked.

Run ``python -m app.eod.daily_bar_reconciliation --market-date YYYY-MM-DD
[--sample-size N]`` for a one-off check (exit 0 ok, 3 mismatch, 1 failure).
"""

from __future__ import annotations

import argparse
import ast
from dataclasses import dataclass, field
from datetime import date, datetime
import hashlib
import json
import logging
import os
from pathlib import Path
import sys
import time
from typing import Any, Callable, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

KST = ZoneInfo('Asia/Seoul')
SCHEMA_VERSION = 'daily_bar_reconciliation_v2'
REFERENCE = 'naver_daily_chart'
# Naver has no trading value, so turnover is not compared.
FIELDS = ('open', 'high', 'low', 'close', 'volume')
# Statuses that settle a session; anything else is retried (bounded per process).
FINAL_STATUSES = frozenset({'ok', 'mismatch'})
MAX_ATTEMPTS_PER_DATE = 3
# Below this share of the sample answered by the reference the check is inconclusive.
MIN_REFERENCE_COVERAGE = 0.8
FETCH_SPACING_SECONDS = 0.3
SAMPLE_LIMIT = 20

Values = tuple[int, int, int, int, int]


def default_var_dir() -> Path:
    return Path(__file__).resolve().parents[2] / 'var'


def select_sample(symbols: Iterable[str], market_date: date, size: int) -> list[str]:
    """Deterministic, date-seeded sample: reproducible, different every session."""

    seed = market_date.isoformat()
    ranked = sorted(set(symbols), key=lambda symbol: hashlib.sha256(f'{seed}:{symbol}'.encode()).hexdigest())
    return sorted(ranked[: max(size, 0)])


def _halted(values: Values) -> bool:
    open_, high, low, _close, volume = values
    return volume == 0 and open_ == high == low == 0


def _differs(stored: Values, reference: Values) -> tuple[bool, ...]:
    """Per-field difference; a no-trade day is compared on close and volume only.

    The collection stores a halted symbol as O=H=L=0 with the previous close and
    zero volume, while chart sources fill O=H=L with that close.
    """

    if _halted(stored) and reference[4] == 0:
        return (False, False, False, stored[3] != reference[3], False)
    return tuple(a != b for a, b in zip(stored, reference))


def compare_bars(stored: Mapping[str, Values], reference: Mapping[str, Values]) -> dict[str, Any]:
    """Compare stored bars with reference bars field by field."""

    common = sorted(stored.keys() & reference.keys())
    diffs = {symbol: _differs(tuple(stored[symbol]), tuple(reference[symbol])) for symbol in common}
    price_mismatched = [symbol for symbol in common if any(diffs[symbol][:4])]
    volume_only = [symbol for symbol in common if not any(diffs[symbol][:4]) and diffs[symbol][4]]
    by_field = {name: sum(1 for symbol in common if diffs[symbol][index]) for index, name in enumerate(FIELDS)}

    def sample(symbols: Sequence[str]) -> list[dict[str, Any]]:
        return [
            {
                'symbol': symbol,
                'stored': dict(zip(FIELDS, stored[symbol])),
                'reference': dict(zip(FIELDS, reference[symbol])),
            }
            for symbol in symbols[:SAMPLE_LIMIT]
        ]

    return {
        'compared_count': len(common),
        'price_mismatch_count': len(price_mismatched),
        'volume_only_mismatch_count': len(volume_only),
        'mismatch_by_field': by_field,
        'mismatch_samples': sample(price_mismatched),
        'volume_mismatch_samples': sample(volume_only),
    }


# ---------------------------------------------------------------------- readers
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
            )
            .join(Symbol, Symbol.id == DailyPriceBar.symbol_id)
            .where(DailyPriceBar.trade_date == market_date, Symbol.market.in_(('KOSPI', 'KOSDAQ')))
        ).all()
    return {str(symbol).strip().upper(): tuple(as_int(v) for v in values) for symbol, *values in rows}  # type: ignore[misc]


def fetch_naver_bars(
    symbols: Sequence[str],
    market_date: date,
    *,
    opener: Callable[..., Any] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[dict[str, Values], dict[str, str]]:
    """Naver daily chart (regular-session OHLCV) for one date; (bars, errors by symbol)."""

    import urllib.request

    open_url = opener or (lambda request, timeout: urllib.request.urlopen(request, timeout=timeout))
    compact = market_date.strftime('%Y%m%d')
    bars: dict[str, Values] = {}
    errors: dict[str, str] = {}
    for index, symbol in enumerate(symbols):
        if index:
            sleep(FETCH_SPACING_SECONDS)
        url = (
            'https://api.finance.naver.com/siseJson.naver?'
            f'symbol={symbol}&requestType=1&startTime={compact}&endTime={compact}&timeframe=day'
        )
        request = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        try:
            with open_url(request, timeout=10) as response:
                text = response.read().decode('utf-8', 'replace')
            rows = ast.literal_eval(text.strip())
            row = next((item for item in rows[1:] if str(item[0]) == compact), None)
        except Exception as exc:  # noqa: BLE001
            errors[symbol] = f'{type(exc).__name__}: {exc}'[:200]
            continue
        if row is None:
            errors[symbol] = 'no_row_for_date'
            continue
        bars[symbol] = tuple(int(value) for value in row[1:6])  # type: ignore[assignment]
    return bars, errors


def _validate_finalized(market_date: date, now: datetime) -> None:
    from app.eod.finality import validate_finalized_krx_write_date

    validate_finalized_krx_write_date(market_date, now=now, operation='daily bar reconciliation')


def _default_sample_size() -> int:
    from app.core.settings import settings

    return int(settings.daily_bar_reconciliation_sample_size)


# ---------------------------------------------------------------------- service
@dataclass
class DailyBarReconciliation:
    reports_dir: Path
    sample_size: Callable[[], int] = _default_sample_size
    session_loader: Callable[[date], date | None] = load_latest_stored_session
    stored_loader: Callable[[date], Mapping[str, Values]] = load_stored_bars
    reference_fetcher: Callable[[Sequence[str], date], tuple[Mapping[str, Values], Mapping[str, str]]] = fetch_naver_bars
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
        # A report from the earlier (market-frame) reference does not settle a session.
        if (
            existing is not None
            and existing.get('status') in FINAL_STATUSES
            and existing.get('schema_version') == SCHEMA_VERSION
        ):
            self.last_result = existing
            return existing
        if self._attempts.get(target, 0) >= MAX_ATTEMPTS_PER_DATE:
            return self.last_result
        self._attempts[target] = self._attempts.get(target, 0) + 1
        return self.check(target, now=current)

    def check(
        self,
        market_date: date,
        *,
        now: datetime | None = None,
        sample_size: int | None = None,
    ) -> dict[str, Any]:
        current = now or self.clock()
        stored = self.stored_loader(market_date)
        size = self.sample_size() if sample_size is None else sample_size
        sample = select_sample(stored.keys(), market_date, size)
        reference, errors = self.reference_fetcher(sample, market_date)
        base = {
            'schema_version': SCHEMA_VERSION,
            'reference': REFERENCE,
            'market_date': market_date.isoformat(),
            'checked_at': current.isoformat(),
            'attempt': self._attempts.get(market_date, 1),
            'stored_count': len(stored),
            'sample_count': len(sample),
            'reference_count': len(reference),
            'reference_error_count': len(errors),
            'reference_error_samples': dict(sorted(errors.items())[:5]),
        }
        if not sample or len(reference) < MIN_REFERENCE_COVERAGE * len(sample):
            result = {**base, 'status': 'reference_unavailable', 'warnings': ['reference_coverage_too_low']}
            self._write_report(market_date, result)
            logger.warning(
                'Daily bar reconciliation: the reference answered %d of %d sampled symbols for %s (attempt %s/%s)',
                len(reference),
                len(sample),
                market_date,
                base['attempt'],
                MAX_ATTEMPTS_PER_DATE,
            )
            self.last_result = result
            return result

        comparison = compare_bars({symbol: stored[symbol] for symbol in sample}, reference)
        warnings: list[str] = []
        if comparison['price_mismatch_count']:
            warnings.append('stored_prices_differ_from_reference')
        if comparison['volume_only_mismatch_count']:
            warnings.append('stored_volume_differs_from_reference')
        result = {
            **base,
            'status': 'mismatch' if comparison['price_mismatch_count'] else 'ok',
            'warnings': warnings,
            **comparison,
        }
        self._write_report(market_date, result)
        if comparison['price_mismatch_count']:
            logger.warning(
                'Daily bar reconciliation: %s has %d of %d sampled stored bars whose prices differ from the '
                'regular-session reference (by field %s; e.g. %s).',
                market_date,
                comparison['price_mismatch_count'],
                comparison['compared_count'],
                comparison['mismatch_by_field'],
                ', '.join(item['symbol'] for item in comparison['mismatch_samples'][:5]) or '-',
            )
        else:
            logger.info(
                'Daily bar reconciliation: %s ok (%d sampled bars, %d volume-only differences)',
                market_date,
                comparison['compared_count'],
                comparison['volume_only_mismatch_count'],
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
        'reference',
        'warnings',
        'sample_count',
        'compared_count',
        'price_mismatch_count',
        'volume_only_mismatch_count',
        'mismatch_by_field',
        'reference_error_count',
        'attempt',
    )
    summary = {key: result[key] for key in keep if key in result}
    summary['mismatch_symbols'] = [item['symbol'] for item in result.get('mismatch_samples') or []][:5]
    return summary


_service: DailyBarReconciliation | None = None


def get_service() -> DailyBarReconciliation:
    global _service
    if _service is None:
        _service = DailyBarReconciliation(reports_dir=default_var_dir() / 'daily-bar-reconciliation')
    return _service


def run_if_due() -> dict[str, Any] | None:
    """Entry point for the background runner (blocking, runs in a thread)."""

    return get_service().run_if_due()


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='Compare a sample of stored daily bars with the Naver daily chart.')
    parser.add_argument('--market-date', required=True, help='YYYY-MM-DD')
    parser.add_argument('--sample-size', type=int, help='symbols to compare (default: the setting)')
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        result = get_service().check(date.fromisoformat(args.market_date), sample_size=args.sample_size)
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
