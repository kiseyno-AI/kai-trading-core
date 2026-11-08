"""Collect each listed symbol's KSIC industry code from DART and classify it.

Run from apps/api:

    python -m app.market.business_line_profiles            # due symbols only
    python -m app.market.business_line_profiles --force    # re-verify all
    python -m app.market.business_line_profiles --symbols 005930,000660

DART company overview is one request per company (~2,800 for the KRX
universe, well inside the 20,000/day key limit); symbols verified within
``REVERIFY_AFTER_DAYS`` are skipped so reruns cost little. No KRX calls.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

from sqlalchemy import select, text

from app.db import session_scope
from app.db_models import SymbolIndustryProfile
from app.integrations.dart.client import DartClient
from app.market.business_lines import BUSINESS_LINES, classify_business_line
from app.market.symbol_codes import normalize_krx_short_code

logger = logging.getLogger(__name__)
KST = ZoneInfo('Asia/Seoul')
REVERIFY_AFTER_DAYS = 90
REQUEST_SPACING_SECONDS = 0.12


def _active_symbols() -> list[str]:
    with session_scope() as session:
        rows = session.execute(
            text(
                "select symbol from symbols where is_active and market in ('KOSPI', 'KOSDAQ') "
                "and instrument_type = 'equity' order by symbol"
            )
        ).scalars()
        return [code for code in (normalize_krx_short_code(row) for row in rows) if code]


def _known_corp_codes(symbols: list[str]) -> dict[str, tuple[str, str]]:
    """stock code -> (corp_code, corp_name) from stored DART snapshots."""

    with session_scope() as session:
        rows = session.execute(
            text(
                'select distinct on (stock_code) stock_code, corp_code, corp_name '
                'from dart_fundamentals_snapshots where stock_code = any(:codes) '
                'order by stock_code, id desc'
            ),
            {'codes': symbols},
        ).all()
    return {row[0]: (row[1], row[2]) for row in rows}


def _latest_krx_sectors() -> dict[str, str]:
    with session_scope() as session:
        rows = session.execute(
            text(
                "select s.symbol, r.payload->>'sector_name' from daily_pykrx_enrichment_rows r "
                'join symbols s on s.id = r.symbol_id '
                "where r.category = 'sector_classification' and r.market_date = "
                "(select max(market_date) from daily_pykrx_enrichment_rows where category = 'sector_classification')"
            )
        ).all()
    return {row[0]: row[1] for row in rows if row[1]}


def _latest_profiles() -> dict[str, SymbolIndustryProfile]:
    with session_scope() as session:
        rows = session.execute(
            select(SymbolIndustryProfile).order_by(
                SymbolIndustryProfile.symbol, SymbolIndustryProfile.effective_from
            )
        ).scalars().all()
        session.expunge_all()
    latest: dict[str, SymbolIndustryProfile] = {}
    for row in rows:
        latest[row.symbol] = row
    return latest


def record_profile(
    *,
    symbol: str,
    corp_code: str | None,
    corp_name: str | None,
    ksic_code: str | None,
    krx_sector: str | None,
    now: datetime,
    payload: dict[str, Any] | None = None,
) -> str:
    """Insert a new effective row on change, else refresh last_verified_at.

    Returns ``inserted`` / ``unchanged``.
    """

    line, source = classify_business_line(symbol=symbol, ksic_code=ksic_code, krx_sector=krx_sector)
    today = now.astimezone(KST).date()
    with session_scope() as session:
        current = session.execute(
            select(SymbolIndustryProfile)
            .where(SymbolIndustryProfile.symbol == symbol)
            .order_by(SymbolIndustryProfile.effective_from.desc())
            .limit(1)
        ).scalar_one_or_none()
        if current is not None and (current.ksic_code, current.business_line) == (ksic_code, line):
            current.last_verified_at = now
            current.krx_sector = krx_sector or current.krx_sector
            return 'unchanged'
        if current is not None and current.effective_from == today:
            session.delete(current)
            session.flush()
        session.add(
            SymbolIndustryProfile(
                symbol=symbol,
                corp_code=corp_code,
                corp_name=corp_name,
                ksic_code=ksic_code,
                krx_sector=krx_sector,
                business_line=line,
                line_source=source,
                effective_from=today,
                last_verified_at=now,
                payload=payload,
            )
        )
    return 'inserted'


async def refresh_profiles(
    *,
    symbols: list[str] | None = None,
    force: bool = False,
    limit: int | None = None,
    client: DartClient | None = None,
    now: datetime | None = None,
    sleep: Callable[[float], Any] = asyncio.sleep,
) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    universe = symbols or _active_symbols()
    latest = _latest_profiles()
    due = [
        symbol
        for symbol in universe
        if force
        or symbol not in latest
        or now - latest[symbol].last_verified_at > timedelta(days=REVERIFY_AFTER_DAYS)
    ]
    if limit is not None:
        due = due[: max(0, limit)]
    corp_codes = _known_corp_codes(due)
    sectors = _latest_krx_sectors()
    client = client or DartClient()
    outcome: Counter[str] = Counter()
    errors: list[dict[str, str]] = []
    for index, symbol in enumerate(due):
        corp_code, corp_name = corp_codes.get(symbol, (None, None))
        ksic_code = None
        if corp_code:
            if index:
                await sleep(REQUEST_SPACING_SECONDS)
            try:
                overview = await client.fetch_company_overview(corp_code)
                ksic_code = str(overview.get('induty_code') or '').strip() or None
                corp_name = overview.get('corp_name') or corp_name
            except Exception as exc:  # noqa: BLE001 - keep going; KRX sector still classifies
                errors.append({'symbol': symbol, 'error': f'{type(exc).__name__}: {exc}'[:200]})
                if len(errors) >= 20 and len(errors) > index // 2:
                    outcome['aborted_after_errors'] = 1
                    break
        outcome[
            record_profile(
                symbol=symbol,
                corp_code=corp_code,
                corp_name=corp_name,
                ksic_code=ksic_code,
                krx_sector=sectors.get(symbol),
                now=now,
                payload={'ksic_source': 'dart_company_overview' if ksic_code else None},
            )
        ] += 1
    summary = {
        'universe': len(universe),
        'due': len(due),
        'inserted': outcome['inserted'],
        'unchanged': outcome['unchanged'],
        'without_corp_code': sum(1 for symbol in due if symbol not in corp_codes),
        'errors': len(errors),
        'error_sample': errors[:5],
        'aborted': bool(outcome['aborted_after_errors']),
    }
    logger.info('business_line_profiles_refresh %s', json.dumps(summary, ensure_ascii=False))
    return summary


def profiles_as_of(day: date) -> dict[str, str]:
    """symbol -> business line effective on ``day`` (latest row on or before it)."""

    with session_scope() as session:
        rows = session.execute(
            select(SymbolIndustryProfile.symbol, SymbolIndustryProfile.business_line)
            .where(SymbolIndustryProfile.effective_from <= day)
            .order_by(SymbolIndustryProfile.symbol, SymbolIndustryProfile.effective_from)
        ).all()
    return {symbol: line for symbol, line in rows}


def business_line_summary(symbols: list[str] | None = None) -> dict[str, Any]:
    latest = _latest_profiles()
    counts = Counter(profile.business_line for profile in latest.values())
    result: dict[str, Any] = {
        'business_lines': [
            {'key': key, 'label': label, 'symbols': counts.get(key, 0)} for key, label in BUSINESS_LINES.items()
        ],
        'classified_symbols': len(latest),
        'sources': dict(Counter(profile.line_source for profile in latest.values())),
    }
    if symbols:
        result['profiles'] = [
            {
                'symbol': symbol,
                'business_line': profile.business_line,
                'label': BUSINESS_LINES.get(profile.business_line),
                'line_source': profile.line_source,
                'ksic_code': profile.ksic_code,
                'krx_sector': profile.krx_sector,
                'corp_name': profile.corp_name,
                'effective_from': profile.effective_from.isoformat(),
            }
            for symbol in symbols
            if (profile := latest.get(symbol)) is not None
        ]
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description='Refresh DART industry codes and business lines.')
    parser.add_argument('--symbols', help='comma-separated stock codes (default: active KRX equities)')
    parser.add_argument('--force', action='store_true', help='re-verify symbols checked recently')
    parser.add_argument('--limit', type=int)
    args = parser.parse_args()
    symbols = [code for code in (normalize_krx_short_code(s) for s in (args.symbols or '').split(',')) if code] or None
    print(json.dumps(asyncio.run(refresh_profiles(symbols=symbols, force=args.force, limit=args.limit)), ensure_ascii=False, indent=1))


if __name__ == '__main__':
    main()
