"""Spec item 14: the Tier A detector SQL against a real (isolated) database.

Every test uses far-future dates inside a transaction that is rolled back.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
import unittest
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy.orm import Session

from app.db import engine
from app.db_models import (
    DailyMarketParticipationContext,
    DailyPriceBar,
    DailySignalFeature,
    Symbol,
)
from app.eod import data_gap_fill as gap_fill

D1 = date(2099, 10, 5)
D2 = date(2099, 10, 6)


def _code() -> str:
    return f'{900000 + uuid4().int % 99_999:06d}'


class DetectorSqlTests(unittest.TestCase):
    def _with_session(self, setup, check):
        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)
            try:
                setup(session)
                session.flush()

                @contextmanager
                def scope(*_args, **_kwargs):
                    yield session

                with (
                    patch('app.db.session_scope', scope),
                    patch('app.market_data_store.session_scope', scope),
                ):
                    return check()
            finally:
                session.close()
                transaction.rollback()

    @staticmethod
    def _symbol(session, *, market='KOSDAQ', active=True) -> Symbol:
        symbol = Symbol(
            symbol=_code(),
            name_ko='gap-fill detector',
            market=market,
            instrument_type='equity',
            source='test',
            is_active=active,
        )
        session.add(symbol)
        session.flush()
        return symbol

    @staticmethod
    def _bar(session, symbol, trade_date, *, volume=100, turnover=10_000) -> None:
        session.add(
            DailyPriceBar(
                symbol_id=symbol.id,
                trade_date=trade_date,
                open_price=100,
                high_price=110,
                low_price=90,
                close_price=105,
                volume=volume,
                turnover=turnover,
                source='test',
            )
        )

    @staticmethod
    def _feature(session, symbol, market_date) -> None:
        session.add(
            DailySignalFeature(
                symbol_id=symbol.id,
                market_date=market_date,
                source='kis_ranking',
                signal_count=0,
                feature_payload={},
            )
        )

    def test_sessions_loader_returns_the_latest_distinct_dates_ascending(self) -> None:
        def setup(session):
            symbol = self._symbol(session)
            for trade_date in (D1, D2):
                self._bar(session, symbol, trade_date)
                self._bar(session, self._symbol(session), trade_date)  # same date twice

        result = self._with_session(setup, lambda: gap_fill.load_recent_sessions(2))

        self.assertEqual(result, [D1.isoformat(), D2.isoformat()])

    def test_signal_features_missing_counts_bar_symbols_without_a_row(self) -> None:
        def setup(session):
            symbols = [self._symbol(session) for _ in range(3)]
            for symbol in symbols:
                self._bar(session, symbol, D1)
            self._feature(session, symbols[0], D1)

        gap = self._with_session(setup, lambda: gap_fill.detect_signal_features_missing(D1.isoformat()))

        # Other tests' committed rows cannot exist on a 2099 date, so counts are exact.
        self.assertEqual((gap.kind, gap.count), ('signal_features_missing', 2))
        self.assertEqual(gap.detail, {'bar_symbols': 3, 'stored_rows': 1})

    def test_signal_features_complete_is_not_a_gap(self) -> None:
        def setup(session):
            symbol = self._symbol(session)
            self._bar(session, symbol, D1)
            self._feature(session, symbol, D1)

        self.assertIsNone(
            self._with_session(setup, lambda: gap_fill.detect_signal_features_missing(D1.isoformat()))
        )

    def test_unrelated_features_do_not_hide_missing_bar_members(self) -> None:
        def setup(session):
            missing, complete, unrelated = [self._symbol(session) for _ in range(3)]
            self._bar(session, missing, D1)
            self._bar(session, complete, D1)
            self._feature(session, complete, D1)
            self._feature(session, unrelated, D1)
            self._feature(session, missing, D2)

        gap = self._with_session(setup, lambda: gap_fill.detect_signal_features_missing(D1.isoformat()))

        self.assertIsNotNone(gap)
        self.assertEqual(gap.count, 1)
        self.assertEqual(gap.detail, {'bar_symbols': 2, 'stored_rows': 1})

    def test_participation_missing_or_stale_is_a_gap_and_matching_is_not(self) -> None:
        def make(captured):
            def setup(session):
                for _ in range(2):
                    self._feature(session, self._symbol(session), D1)
                if captured is not None:
                    session.add(
                        DailyMarketParticipationContext(
                            market='KRX',
                            market_date=D1,
                            source='kis_ranking',
                            context_payload={'captured_symbol_count': captured},
                        )
                    )

            return setup

        detect = lambda: gap_fill.detect_participation_context_missing(D1.isoformat())  # noqa: E731

        self.assertIsNotNone(self._with_session(make(None), detect))  # no context row
        stale = self._with_session(make(1), detect)  # built from fewer rows than exist
        self.assertEqual(stale.detail, {'signal_feature_rows': 2, 'captured_symbol_count': 1})
        self.assertIsNone(self._with_session(make(2), detect))  # up to date

    def test_participation_has_nothing_to_report_without_signal_rows(self) -> None:
        self.assertIsNone(
            self._with_session(lambda session: None, lambda: gap_fill.detect_participation_context_missing(D1.isoformat()))
        )

    def test_turnover_incomplete_counts_only_active_domestic_traded_zero_turnover_bars(self) -> None:
        def setup(session):
            counted = self._symbol(session)
            self._bar(session, counted, D1, volume=100, turnover=0)
            halted = self._symbol(session)
            self._bar(session, halted, D1, volume=0, turnover=0)  # no volume: not a gap
            fine = self._symbol(session)
            self._bar(session, fine, D1, volume=100, turnover=500)
            inactive = self._symbol(session, active=False)
            self._bar(session, inactive, D1, volume=100, turnover=0)  # ETF-like, ignored
            konex = self._symbol(session, market='KONEX')
            self._bar(session, konex, D1, volume=100, turnover=0)

        gap = self._with_session(setup, lambda: gap_fill.detect_turnover_incomplete(D1.isoformat()))

        self.assertEqual((gap.kind, gap.count), ('turnover_incomplete', 1))


if __name__ == '__main__':
    unittest.main()


def test_provider_frames_are_fetched_from_krx_once_per_session_across_service_rebuilds(monkeypatch):
    import pandas as pd

    from app.eod import data_gap_fill as module

    calls = []

    def fake_frame(compact, market):
        calls.append((compact, market))
        return pd.DataFrame({'시가': [1], '고가': [2], '저가': [1], '종가': [2]}, index=['005930'])

    monkeypatch.setattr(module, '_PROVIDER_FRAME_CACHE', {})
    monkeypatch.setattr('app.eod.pykrx_backfill._get_market_ohlcv_by_ticker', fake_frame)
    first = module._load_provider_frame('2026-11-05')
    second = module._load_provider_frame('2026-11-05')  # a later tick: no new KRX read
    assert first == second == {'005930': (1, 2, 1, 2)}
    assert calls == [('20261105', 'KOSPI'), ('20261105', 'KOSDAQ')]


def test_empty_provider_frames_are_not_cached(monkeypatch):
    import pandas as pd

    from app.eod import data_gap_fill as module

    calls = []

    def empty_frame(compact, market):
        calls.append(market)
        return pd.DataFrame({'시가': [], '고가': [], '저가': [], '종가': []})

    monkeypatch.setattr(module, '_PROVIDER_FRAME_CACHE', {})
    monkeypatch.setattr('app.eod.pykrx_backfill._get_market_ohlcv_by_ticker', empty_frame)
    assert module._load_provider_frame('2026-11-05') == {}
    module._load_provider_frame('2026-11-05')
    assert len(calls) == 4 and module._PROVIDER_FRAME_CACHE == {}
