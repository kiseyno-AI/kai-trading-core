"""Load FRED series history with the date each value became public.

    python -m app.macro_context.history            # 2006 onward, default series

``released_on`` is the US calendar date a value became public. Monthly
releases (CPI, unemployment) and weekly ones (USD/KRW H.10) come from ALFRED
first releases. Dense daily market series exceed ALFRED's 2,000-vintage
limit; their close is public the same US day (``'same_day_close'``). Korea
trades the next morning, so readers use values with ``released_on`` strictly
before the Korean trading day. Research only: the live macro context is
unchanged.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.db import session_scope
from app.db_models import MacroSeriesHistory
from app.integrations.fred.client import FREDAPIError, FREDClient

HISTORY_SERIES = (
    'DEXKOUS', 'DFF', 'DGS10', 'DGS2', 'T10Y2Y', 'VIXCLS', 'BAMLH0A0HYM2', 'DCOILWTICO', 'CPIAUCSL', 'UNRATE',
)
DEFAULT_START = '2006-01-01'


def history_rows(observations: list[dict[str, Any]], *, initial_release: bool) -> list[dict[str, Any]]:
    rows = []
    for item in observations:
        try:
            value = float(item.get('value'))
            observed = date.fromisoformat(str(item.get('date'))[:10])
        except (TypeError, ValueError):
            continue  # FRED marks missing days with '.'
        if initial_release:
            released = date.fromisoformat(str(item.get('realtime_start'))[:10])
        else:
            released = observed  # US close, public that US evening
        rows.append(
            {
                'observation_date': observed,
                'value': value,
                'released_on': max(released, observed),
                'release_basis': 'alfred_initial_release' if initial_release else 'same_day_close',
            }
        )
    return rows


async def load_fred_history(
    *,
    series: tuple[str, ...] = HISTORY_SERIES,
    start: str = DEFAULT_START,
    client: FREDClient | None = None,
    store: Callable[[str, list[dict[str, Any]], datetime], int] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    client = client or FREDClient()
    store = store or store_history
    now = now or datetime.now(timezone.utc)
    result: dict[str, Any] = {}
    for series_id in series:
        try:
            try:
                observations = await client.fetch_observation_history(
                    series_id=series_id, observation_start=start, initial_release=True
                )
                initial = True
            except FREDAPIError as exc:
                if 'vintage dates' not in str(exc):
                    raise
                observations = await client.fetch_observation_history(
                    series_id=series_id, observation_start=start, initial_release=False
                )
                initial = False
            rows = history_rows(observations, initial_release=initial)
            if initial and rows:
                rows = await _backfill_before_first_release(client, series_id, start, rows)
            result[series_id] = {
                'rows': store(series_id, rows, now),
                'release_basis': 'alfred_initial_release' if initial else 'same_day_close',
                'first': rows[0]['observation_date'].isoformat() if rows else None,
                'last': rows[-1]['observation_date'].isoformat() if rows else None,
            }
        except FREDAPIError as exc:
            result[series_id] = {'error': str(exc)[:200]}
    return result


async def _backfill_before_first_release(
    client: FREDClient, series_id: str, start: str, rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Fill dates older than ALFRED's first vintage from latest values.

    Their release date is unknown, so it is set to the observation date plus
    the median publication lag seen in the ALFRED part (at least one day):
    late rather than early, so research never sees a value too soon.
    """

    first = rows[0]['observation_date']
    if first <= date.fromisoformat(start) + timedelta(days=7):
        return rows
    lags = sorted((row['released_on'] - row['observation_date']).days for row in rows)
    lag = timedelta(days=max(1, lags[len(lags) // 2]))
    latest = history_rows(
        await client.fetch_observation_history(series_id=series_id, observation_start=start, initial_release=False),
        initial_release=False,
    )
    older = [
        {**row, 'released_on': row['observation_date'] + lag, 'release_basis': 'median_lag_assumed'}
        for row in latest
        if row['observation_date'] < first
    ]
    return older + rows


def store_history(series_id: str, rows: list[dict[str, Any]], fetched_at: datetime) -> int:
    if not rows:
        return 0
    with session_scope() as session:
        for start in range(0, len(rows), 2000):
            batch = [{'source': 'fred', 'series_id': series_id, 'fetched_at': fetched_at, **row} for row in rows[start:start + 2000]]
            statement = insert(MacroSeriesHistory).values(batch)
            session.execute(
                statement.on_conflict_do_update(
                    constraint='uq_macro_series_history_obs',
                    set_={
                        'value': statement.excluded.value,
                        'released_on': statement.excluded.released_on,
                        'release_basis': statement.excluded.release_basis,
                        'fetched_at': statement.excluded.fetched_at,
                    },
                )
            )
    return len(rows)


def known_series(series_id: str, *, session_factory=session_scope) -> list[tuple[date, date, float]]:
    """``(observation_date, released_on, value)`` oldest first, for research reads."""

    with session_factory() as session:
        rows = session.execute(
            select(MacroSeriesHistory.observation_date, MacroSeriesHistory.released_on, MacroSeriesHistory.value)
            .where(MacroSeriesHistory.series_id == series_id)
            .order_by(MacroSeriesHistory.observation_date)
        ).all()
    return [(observed, released, float(value)) for observed, released, value in rows]


def main() -> None:
    parser = argparse.ArgumentParser(description='Load FRED history with release dates.')
    parser.add_argument('--start', default=DEFAULT_START)
    parser.add_argument('--series', help='comma-separated FRED series ids')
    args = parser.parse_args()
    series = tuple(s.strip() for s in args.series.split(',') if s.strip()) if args.series else HISTORY_SERIES
    print(json.dumps(asyncio.run(load_fred_history(series=series, start=args.start)), ensure_ascii=False, indent=1))


if __name__ == '__main__':
    main()
