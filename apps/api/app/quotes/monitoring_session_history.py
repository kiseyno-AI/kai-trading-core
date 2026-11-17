"""Durable regular-session monitoring membership and last accepted trades."""

import fcntl
import json
import os
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from app.agent_context.evaluation_refresh import default_marker_dir
from app.market.clock import KST
from app.market.session import KRX_EXCHANGE_CALENDAR, get_krx_exchange_calendar_day
from app.market.symbol_codes import is_canonical_krx_short_code


@dataclass(frozen=True)
class MonitoringLastQuote:
    price: Decimal
    as_of: datetime


class MonitoringSessionHistory:
    def __init__(self, root: Path | None = None) -> None:
        self.root = root or default_marker_dir() / 'watch-audit' / 'monitoring-sessions'

    def dates(self) -> set[date]:
        return {date.fromisoformat(path.stem) for path in self.root.glob('????-??-??.json')}

    def read(self, market_date: date) -> dict[str, MonitoringLastQuote | None]:
        try:
            data = json.loads((self.root / f'{market_date}.json').read_text(encoding='utf-8'))
        except FileNotFoundError:
            return {}
        return {
            symbol: MonitoringLastQuote(Decimal(quote['price']), datetime.fromisoformat(quote['as_of']))
            if quote is not None else None
            for symbol, quote in data.items()
        }

    def record_exec_membership(self, symbols: Iterable[str], observed_at: datetime) -> None:
        day = observed_at.astimezone(KST).date()
        if get_krx_exchange_calendar_day(day).is_open and (
            KRX_EXCHANGE_CALENDAR.session_open(day.isoformat())
            <= observed_at <= KRX_EXCHANGE_CALENDAR.session_close(day.isoformat())
        ):
            self.record(day, dict.fromkeys(symbols))

    def record(self, market_date: date, observed: dict[str, MonitoringLastQuote | None]) -> None:
        if not observed:
            return
        self.root.mkdir(parents=True, exist_ok=True)
        path = self.root / f'{market_date}.json'
        with path.with_suffix('.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            previous = self.read(market_date)
            merged = previous.copy()
            for symbol, quote in observed.items():
                if not is_canonical_krx_short_code(symbol):
                    continue
                current = merged.setdefault(symbol, None)
                if (
                    quote is not None
                    and quote.as_of.tzinfo is not None
                    and quote.as_of.astimezone(KST).date() == market_date
                    and quote.price.is_finite()
                    and quote.price > 0
                    and (current is None or quote.as_of >= current.as_of)
                ):
                    merged[symbol] = quote
            if merged == previous:
                return
            fd, temp_path = tempfile.mkstemp(prefix='.monitoring-', dir=self.root)
            try:
                with os.fdopen(fd, 'w', encoding='utf-8') as file:
                    json.dump({
                        symbol: {'price': str(quote.price), 'as_of': quote.as_of.isoformat()}
                        if quote is not None else None
                        for symbol, quote in merged.items()
                    }, file)
                    file.flush()
                    os.fsync(file.fileno())
                os.replace(temp_path, path)
            finally:
                Path(temp_path).unlink(missing_ok=True)


monitoring_session_history = MonitoringSessionHistory()
