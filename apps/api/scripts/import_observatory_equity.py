"""One-time import of the Observatory equity history into Core."""

import argparse
from contextlib import closing
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
import re
import sqlite3

from app.orders.portfolio_equity_history import (
    CENT,
    MAX_AMOUNT,
    SEOUL,
    read_equity_history,
    store_daily_equity,
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
            valuations.setdefault(day, {})[currency] = amount.quantize(CENT)

    for day, totals in valuations.items():
        store_daily_equity(datetime.combine(day, time.min, SEOUL), totals)

    actual = {
        (point['date'], series['currency']): point['equity']
        for series in read_equity_history()['equity']
        for point in series['points']
    }
    for day, totals in valuations.items():
        recorded = {currency: amount for (date_key, currency), amount in actual.items() if date_key == day.isoformat()}
        if recorded != {currency: float(amount) for currency, amount in totals.items()}:
            raise ValueError('Legacy valuations do not match the Core history for one or more dates')
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
