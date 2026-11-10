"""One-time import of the Observatory equity history into Core."""

import argparse
from contextlib import closing
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
import re
import sqlite3

from app.orders.portfolio_equity_history import (
    MAX_AMOUNT,
    SEOUL,
    store_equity_days,
)


def import_history(path: Path) -> int:
    valuations: dict[date, dict[str, Decimal]] = {}
    with closing(sqlite3.connect(f'{path.resolve().as_uri()}?mode=ro', uri=True)) as legacy:
        for raw_date, currency, raw_amount in legacy.execute(
            'SELECT date, currency, equity FROM valuations ORDER BY date, currency'
        ):
            day = date.fromisoformat(raw_date)
            amount = Decimal(str(raw_amount))
            if not re.fullmatch('[A-Z]{3}', currency) or not amount.is_finite() or abs(amount) > MAX_AMOUNT:
                raise ValueError('Invalid legacy valuation')
            valuations.setdefault(day, {})[currency] = amount

    store_equity_days(
        {datetime.combine(day, time.min, SEOUL): totals for day, totals in valuations.items()},
        verify_existing=True,
    )
    return len(valuations)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('sqlite_path', type=Path)
    args = parser.parse_args()
    try:
        imported = import_history(args.sqlite_path)
    except Exception:
        parser.exit(1, 'Could not import and verify Observatory history. Inspect the source and Core privately.\n')
    print(f'Verified {imported} imported valuation day(s).')
