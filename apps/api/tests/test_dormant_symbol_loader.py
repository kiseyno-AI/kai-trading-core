from __future__ import annotations

from contextlib import contextmanager
from datetime import date
import unittest
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy.orm import Session

from app.db import engine
from app.db_models import DailyPriceBar, Symbol
from app.market_data_store import load_symbols_without_recent_trading

SESSIONS = [date(2099, 11, day) for day in (2, 3, 4, 5)]


def _code() -> str:
    return f'{900000 + uuid4().int % 99_999:06d}'


class LoadSymbolsWithoutRecentTradingTests(unittest.TestCase):
    def _run(self, plan: dict[str, list[tuple[date, int]]], *, lookback: int = 3):
        """``plan`` maps symbol -> [(trade_date, volume)]; returns the loader result."""

        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)
            try:
                for code, bars in plan.items():
                    symbol = Symbol(
                        symbol=code,
                        name_ko=code,
                        market='KOSPI',
                        instrument_type='equity',
                        source='test',
                        is_active=True,
                    )
                    session.add(symbol)
                    session.flush()
                    for trade_date, volume in bars:
                        session.add(
                            DailyPriceBar(
                                symbol_id=symbol.id,
                                trade_date=trade_date,
                                open_price=100,
                                high_price=100,
                                low_price=100,
                                close_price=100,
                                volume=volume,
                                turnover=volume * 100,
                                source='test',
                            )
                        )
                session.flush()

                @contextmanager
                def scope():
                    yield session

                with patch('app.market_data_store.session_scope', scope):
                    return load_symbols_without_recent_trading(
                        market_date=SESSIONS[-1],
                        symbols=list(plan),
                        lookback_trading_days=lookback,
                    )
            finally:
                session.close()
                transaction.rollback()

    def test_only_a_full_window_of_zero_volume_counts(self) -> None:
        halted, traded, resumed, gap, new_listing = (_code() for _ in range(5))
        window = SESSIONS[-3:]
        plan = {
            halted: [(d, 0) for d in SESSIONS],
            traded: [(d, 10) for d in SESSIONS],
            # Traded on the last session after being halted: exclusion ends at once.
            resumed: [(d, 0) for d in SESSIONS[:-1]] + [(SESSIONS[-1], 5)],
            # Missing a bar inside the window is a data gap, not a halt.
            gap: [(window[0], 0), (window[2], 0)],
            # Fewer bars than the window (new listing) is never excluded.
            new_listing: [(window[2], 0)],
        }

        result = self._run(plan)

        self.assertEqual(result, [halted])

    def test_window_is_the_last_n_sessions_not_calendar_days(self) -> None:
        old_trade_then_halted = _code()
        plan = {
            # Traded on the first session only; the last 3 sessions are all zero.
            old_trade_then_halted: [(SESSIONS[0], 7)] + [(d, 0) for d in SESSIONS[1:]],
        }

        self.assertEqual(self._run(plan, lookback=3), [old_trade_then_halted])
        self.assertEqual(self._run(plan, lookback=4), [])

    def test_too_little_history_excludes_nobody(self) -> None:
        code = _code()

        self.assertEqual(self._run({code: [(SESSIONS[-1], 0)]}, lookback=10), [])


if __name__ == '__main__':
    unittest.main()
