"""Capture KIS after-hours (시간외 단일가) rankings while they still exist.

KIS ``overtime-volume`` / ``overtime-fluctuation`` rankings describe the
current day's KRX after-hours single-price session (16:00-18:00 KST). By the
time the daily pipeline runs (after the 20:10 KST finality floor) KIS returns
every row with zero after-hours volume and an empty fluctuation list, so every
overtime signal since 2026-09-11 was discarded as invalid. Before that date
the signal stage happened to run around 16:50 and received real values.

This module takes one read-only snapshot near the end of the session and
stores the raw provider rows per market date. The nightly signal-feature
collection uses that capture (point-in-time evidence from the same day) and
falls back to a live call only when no usable capture exists.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

KST = ZoneInfo('Asia/Seoul')
OVERTIME_VOLUME_SOURCE = 'overtime-volume'
OVERTIME_FLUCTUATION_SOURCE = 'overtime-fluctuation'
CAPTURE_SOURCES = (OVERTIME_VOLUME_SOURCE, OVERTIME_FLUCTUATION_SOURCE)
CAPTURE_SCHEMA_VERSION = 'kis_overtime_ranking_capture_v1'
# The after-hours single-price session closes at 18:00 KST. Start shortly
# before the close so the snapshot is near-final, and keep trying a little
# after it in case the first attempts return nothing usable.
CAPTURE_WINDOW_START = time(17, 45)
CAPTURE_WINDOW_END = time(18, 30)
MIN_ATTEMPT_INTERVAL = timedelta(minutes=5)

_last_attempt_at: dict[str, datetime] = {}


def default_capture_dir() -> Path:
    configured = os.environ.get('KIS_OVERTIME_CAPTURE_DIR', '').strip()
    if configured:
        return Path(configured).expanduser()
    return Path(__file__).resolve().parents[2] / 'var' / 'kis-overtime-captures'


def _to_int(value: Any) -> int | None:
    try:
        return int(float(str(value).replace(',', '').strip()))
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> float | None:
    try:
        return float(str(value).replace(',', '').strip())
    except (TypeError, ValueError):
        return None


def usable_row_count(source: str, rows: list[dict[str, Any]]) -> int:
    """Rows the signal collector would accept as evidence."""

    if source == OVERTIME_VOLUME_SOURCE:
        return sum(1 for row in rows if (_to_int(row.get('ovtm_untp_vol')) or 0) > 0)
    if source == OVERTIME_FLUCTUATION_SOURCE:
        return sum(1 for row in rows if _to_float(row.get('ovtm_untp_prdy_ctrt')) is not None)
    return 0


class OvertimeRankingCaptureStore:
    def __init__(self, root: Path | None = None) -> None:
        self.root = Path(root) if root is not None else default_capture_dir()

    def path(self, market_date: str) -> Path:
        return self.root / f'{market_date}.json'

    def load(self, market_date: str) -> dict[str, Any] | None:
        try:
            payload = json.loads(self.path(market_date).read_text(encoding='utf-8'))
        except (OSError, ValueError):
            return None
        if not isinstance(payload, dict) or payload.get('schema_version') != CAPTURE_SCHEMA_VERSION:
            return None
        if payload.get('market_date') != market_date:
            return None
        return payload

    def save(self, payload: Mapping[str, Any]) -> Path:
        self.root.mkdir(parents=True, exist_ok=True)
        target = self.path(str(payload['market_date']))
        fd, tmp = tempfile.mkstemp(prefix='.capture-', dir=str(self.root))
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as handle:
                json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
            os.replace(tmp, target)
        finally:
            Path(tmp).unlink(missing_ok=True)
        return target

    def usable_rows(self, market_date: str, source: str) -> tuple[list[dict[str, Any]], dict[str, Any]] | None:
        """Return (rows, metadata) when the capture has usable rows for source."""

        payload = self.load(market_date)
        if payload is None:
            return None
        entry = (payload.get('sources') or {}).get(source)
        if not isinstance(entry, dict):
            return None
        rows = [row for row in entry.get('rows') or [] if isinstance(row, dict)]
        if usable_row_count(source, rows) <= 0:
            return None
        return rows, {
            'origin': 'after_hours_capture',
            'captured_at': payload.get('captured_at'),
            'summary_rows': entry.get('summary_rows') or [],
            'page_count': entry.get('page_count'),
            'max_pages_reached': entry.get('max_pages_reached'),
        }


def _capture_is_complete(payload: Mapping[str, Any] | None) -> bool:
    if not payload:
        return False
    sources = payload.get('sources') or {}
    return all(
        int((sources.get(source) or {}).get('usable_count') or 0) > 0
        for source in CAPTURE_SOURCES
    )


def _usable_total(payload: Mapping[str, Any] | None) -> int:
    if not payload:
        return 0
    return sum(
        int((source or {}).get('usable_count') or 0)
        for source in (payload.get('sources') or {}).values()
        if isinstance(source, dict)
    )


async def capture_overtime_rankings_if_due(
    *,
    market: Any,
    client: Any = None,
    store: OvertimeRankingCaptureStore | None = None,
    now: datetime | None = None,
    max_pages: int = 10,
) -> dict[str, Any]:
    """Take one read-only after-hours ranking snapshot when due.

    Due means: a KRX trading day, market state CLOSED, KST time inside the
    capture window, no complete capture yet, and at least
    ``MIN_ATTEMPT_INTERVAL`` since this process last tried.
    """

    now_kst = (now or datetime.now(timezone.utc)).astimezone(KST)
    if getattr(market, 'state', None) != 'CLOSED' or getattr(market, 'is_trading_day', False) is not True:
        return {'status': 'skipped', 'reason': 'not_after_close_on_trading_day'}
    if not (CAPTURE_WINDOW_START <= now_kst.time() < CAPTURE_WINDOW_END):
        return {'status': 'skipped', 'reason': 'outside_capture_window'}
    market_date = now_kst.date().isoformat()
    store = store or OvertimeRankingCaptureStore()
    existing = store.load(market_date)
    if _capture_is_complete(existing):
        return {'status': 'skipped', 'reason': 'already_captured', 'market_date': market_date}
    last = _last_attempt_at.get(market_date)
    if last is not None and now_kst - last < MIN_ATTEMPT_INTERVAL:
        return {'status': 'skipped', 'reason': 'attempt_interval', 'market_date': market_date}
    _last_attempt_at[market_date] = now_kst

    if client is None:
        from app.brokers.kis.provider import kis_client as client  # noqa: PLC0415

    fetchers = {
        OVERTIME_VOLUME_SOURCE: (client.fetch_domestic_ranking_overtime_volume, 'output2'),
        OVERTIME_FLUCTUATION_SOURCE: (client.fetch_domestic_ranking_overtime_fluctuation, 'output2'),
    }
    sources: dict[str, Any] = {}
    errors: dict[str, str] = {}
    for source, (fetch, rows_key) in fetchers.items():
        try:
            response = await fetch(max_pages=max_pages)
        except Exception as exc:  # noqa: BLE001 - one source must not block the other
            errors[source] = str(exc).strip() or type(exc).__name__
            continue
        rows = [row for row in response.get(rows_key) or [] if isinstance(row, dict)]
        summary = response.get('output1') or []
        sources[source] = {
            'rows': rows,
            'summary_rows': summary if isinstance(summary, list) else [summary],
            'raw_count': len(rows),
            'usable_count': usable_row_count(source, rows),
            'page_count': response.get('page_count'),
            'max_pages_reached': response.get('max_pages_reached'),
        }
    payload = {
        'schema_version': CAPTURE_SCHEMA_VERSION,
        'market_date': market_date,
        'captured_at': now_kst.isoformat(),
        'sources': sources,
        'errors': errors,
    }
    if existing is not None and _usable_total(existing) >= _usable_total(payload):
        return {
            'status': 'kept_existing',
            'market_date': market_date,
            'usable_counts': {k: v.get('usable_count') for k, v in sources.items()},
            'errors': errors,
        }
    store.save(payload)
    result = {
        'status': 'captured',
        'market_date': market_date,
        'usable_counts': {k: v.get('usable_count') for k, v in sources.items()},
        'errors': errors,
    }
    logger.info('kis_overtime_ranking_capture %s', result)
    return result
