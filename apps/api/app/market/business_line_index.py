"""Daily equal-weight returns per business line from stored daily bars.

    python -m app.market.business_line_index --start 2014-01-01

Stored daily bars cover the whole universe only from 2014 (one or two
symbols per day before that), so earlier years add nothing.

No provider calls: one SQL pass over ``daily_price_bars`` (close-to-close
return per symbol, moves beyond ±35 % dropped as data errors — KRX limits
are ±30 %), grouped by business line. The market return is the equal-weight
mean of every classified symbol that day, so ``excess_return`` is the line
against the same universe.

History uses today's classification (``classification_basis =
'current_profile'``): delisted companies and past reclassifications are not
represented, which flatters older periods (survivorship).
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import date, datetime
from zoneinfo import ZoneInfo
from typing import Any

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

from app.db import engine, session_scope
from app.db_models import BusinessLineDailyMetric
from app.market.business_line_profiles import profiles_as_of

MAX_ABS_DAILY_RETURN = 0.35

_AGGREGATE_SQL = text(
    """
    with mapping as (
        select * from unnest(cast(:symbols as text[]), cast(:lines as text[])) as m(symbol, line)
    ),
    bars as (
        select m.line, b.trade_date, b.close_price, b.turnover,
               lag(b.close_price) over (partition by b.symbol_id order by b.trade_date) as prev_close
        from daily_price_bars b
        join symbols s on s.id = b.symbol_id
        join mapping m on m.symbol = s.symbol
        where b.trade_date between :lag_start and :end
    ),
    returns as (
        select trade_date, line, close_price::float8 / prev_close::float8 - 1 as ret, turnover
        from bars
        where trade_date between :start and :end and prev_close > 0 and close_price > 0
    )
    select trade_date, line, count(*) as n, sum(ret) as sum_ret,
           sum(case when ret > 0 then 1 else 0 end) as advancers, sum(turnover) as turnover
    from returns
    where abs(ret) < :max_abs
    group by trade_date, line
    order by trade_date, line
    """
)


def compute_daily_metrics(connection, *, start: date, end: date, mapping: dict[str, str]) -> list[dict[str, Any]]:
    symbols = sorted(mapping)
    raw = connection.execute(
        _AGGREGATE_SQL,
        {
            'symbols': symbols,
            'lines': [mapping[symbol] for symbol in symbols],
            'lag_start': date(start.year - 1 if start.month == 1 else start.year, 1, 1),
            'start': start,
            'end': end,
            'max_abs': MAX_ABS_DAILY_RETURN,
        },
    ).all()
    by_day: dict[date, list[Any]] = defaultdict(list)
    for row in raw:
        by_day[row.trade_date].append(row)
    metrics = []
    for day, rows in by_day.items():
        total_n = sum(row.n for row in rows)
        market = sum(float(row.sum_ret) for row in rows) / total_n
        total_turnover = sum(float(row.turnover or 0) for row in rows)
        for row in rows:
            line_return = float(row.sum_ret) / row.n
            metrics.append(
                {
                    'trade_date': day,
                    'business_line': row.line,
                    'symbols': int(row.n),
                    'ew_return': line_return,
                    'market_ew_return': market,
                    'excess_return': line_return - market,
                    'advancers_ratio': row.advancers / row.n,
                    'turnover': float(row.turnover) if row.turnover is not None else None,
                    'turnover_share': (float(row.turnover or 0) / total_turnover) if total_turnover else None,
                    'classification_basis': 'current_profile',
                }
            )
    return metrics


def store_daily_metrics(rows: list[dict[str, Any]], *, session_factory=session_scope) -> int:
    with session_factory() as session:
        for start in range(0, len(rows), 5000):
            statement = insert(BusinessLineDailyMetric).values(rows[start:start + 5000])
            session.execute(
                statement.on_conflict_do_update(
                    constraint='uq_business_line_daily_metrics_day_line',
                    set_={column: statement.excluded[column] for column in rows[0] if column not in {'trade_date', 'business_line'}},
                )
            )
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description='Rebuild business-line daily metrics from stored bars.')
    parser.add_argument('--start', default='2014-01-01')
    # Profiles are dated in KST; the host clock may be on US time, where
    # 'today' is still the previous day and no profile is effective yet.
    parser.add_argument('--end', default=datetime.now(ZoneInfo('Asia/Seoul')).date().isoformat())
    args = parser.parse_args()
    end = date.fromisoformat(args.end)
    mapping = profiles_as_of(end)
    stored = 0
    # One query per calendar year: a single multi-year window pass exceeds
    # the production connection's statement timeout.
    for chunk_start, chunk_end in year_chunks(date.fromisoformat(args.start), end):
        with engine.connect() as connection:
            rows = compute_daily_metrics(connection, start=chunk_start, end=chunk_end, mapping=mapping)
        stored += store_daily_metrics(rows) if rows else 0
        print(json.dumps({'year': chunk_start.year, 'rows': len(rows)}), flush=True)
    print(json.dumps({'symbols': len(mapping), 'rows': stored}))


def year_chunks(start: date, end: date) -> list[tuple[date, date]]:
    return [
        (max(start, date(year, 1, 1)), min(end, date(year, 12, 31)))
        for year in range(start.year, end.year + 1)
    ]


if __name__ == '__main__':
    main()
