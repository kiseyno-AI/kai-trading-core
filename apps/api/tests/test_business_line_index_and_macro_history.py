import asyncio
import unittest
from datetime import date, datetime, timezone

from sqlalchemy import delete, text

from app.db import engine, session_scope
from app.db_models import BusinessLineDailyMetric, DailyPriceBar, MacroSeriesHistory, Symbol
from app.integrations.fred.client import FREDAPIError
from app.macro_context import history
from app.market.business_line_index import compute_daily_metrics, store_daily_metrics


class FakeFred:
    def __init__(self):
        self.calls = []

    async def fetch_observation_history(self, *, series_id, observation_start, initial_release):
        self.calls.append((series_id, initial_release))
        if series_id == 'DGS10' and initial_release:
            raise FREDAPIError('FRED request failed [400]: There are 5145 vintage dates ... exceeds the maximum number of vintage dates allowed')
        if series_id == 'CPIAUCSL':
            return [
                {'date': '2024-01-01', 'realtime_start': '2024-02-13', 'value': '309.685'},
                {'date': '2024-02-01', 'realtime_start': '2024-03-12', 'value': '311.054'},
            ]
        return [
            {'date': '2024-01-01', 'value': '.'},
            {'date': '2024-01-02', 'value': '3.95'},
        ]


class MacroHistoryTests(unittest.TestCase):
    def test_release_dates_from_alfred_and_next_day_fallback(self):
        stored = {}
        fred = FakeFred()

        result = asyncio.run(
            history.load_fred_history(
                series=('CPIAUCSL', 'DGS10'),
                start='2024-01-01',
                client=fred,
                store=lambda series_id, rows, _now: stored.setdefault(series_id, rows) and len(rows),
            )
        )

        self.assertEqual(result['CPIAUCSL']['release_basis'], 'alfred_initial_release')
        self.assertEqual(stored['CPIAUCSL'][0]['released_on'], date(2024, 2, 13))
        self.assertEqual(result['DGS10']['release_basis'], 'same_day_close')
        # '.' placeholder skipped; a US close is public that US day.
        self.assertEqual([(r['observation_date'], r['released_on']) for r in stored['DGS10']], [(date(2024, 1, 2), date(2024, 1, 2))])
        self.assertEqual(fred.calls, [('CPIAUCSL', True), ('DGS10', True), ('DGS10', False)])

    def test_dates_before_the_first_alfred_vintage_use_the_median_lag(self):
        class LateVintage:
            async def fetch_observation_history(self, *, series_id, observation_start, initial_release):
                if initial_release:
                    return [
                        {'date': '2014-03-10', 'realtime_start': '2014-03-17', 'value': '1070'},
                        {'date': '2014-03-11', 'realtime_start': '2014-03-17', 'value': '1071'},
                        {'date': '2014-03-12', 'realtime_start': '2014-03-17', 'value': '1072'},
                    ]
                return [
                    {'date': '2006-01-02', 'value': '1000'},
                    {'date': '2014-03-10', 'value': '1070.5'},
                ]

        stored = {}
        asyncio.run(
            history.load_fred_history(
                series=('DEXKOUS',), client=LateVintage(),
                store=lambda series_id, rows, _now: stored.setdefault(series_id, rows) and len(rows),
            )
        )

        first, *rest = stored['DEXKOUS']
        self.assertEqual(
            (first['observation_date'], first['released_on'], first['release_basis']),
            (date(2006, 1, 2), date(2006, 1, 8), 'median_lag_assumed'),  # median lag 6 days
        )
        # The ALFRED first release wins for dates it covers.
        self.assertEqual(rest[0]['value'], 1070.0)

    def test_store_and_read_back(self):
        MacroSeriesHistory.__table__.create(bind=engine, checkfirst=True)
        with session_scope() as session:
            session.execute(delete(MacroSeriesHistory))
        self.addCleanup(lambda: self._clean())
        rows = history.history_rows(
            [{'date': '2024-01-01', 'realtime_start': '2024-02-13', 'value': '309.685'}], initial_release=True
        )
        now = datetime(2026, 11, 8, tzinfo=timezone.utc)
        history.store_history('CPIAUCSL', rows, now)
        history.store_history('CPIAUCSL', rows, now)  # idempotent

        self.assertEqual(history.known_series('CPIAUCSL'), [(date(2024, 1, 1), date(2024, 2, 13), 309.685)])

    def _clean(self):
        with session_scope() as session:
            session.execute(delete(MacroSeriesHistory))


class BusinessLineIndexTests(unittest.TestCase):
    SYMBOLS = {'900001': 'shipbuilding', '900002': 'shipbuilding', '900003': 'banks'}

    def setUp(self):
        for table in (Symbol.__table__, DailyPriceBar.__table__, BusinessLineDailyMetric.__table__):
            table.create(bind=engine, checkfirst=True)
        self._clean()
        self.addCleanup(self._clean)
        closes = {
            '900001': [100, 110, 99],
            '900002': [100, 90, 90],
            '900003': [100, 100, 300],  # +200 % is a data error, dropped
        }
        days = [date(2026, 11, 4), date(2026, 11, 5), date(2026, 11, 6)]
        with session_scope() as session:
            for symbol, prices in closes.items():
                row = Symbol(symbol=symbol, name_ko=symbol, market='KOSPI', source='test', source_payload={})
                session.add(row)
                session.flush()
                for day, price in zip(days, prices):
                    session.add(
                        DailyPriceBar(
                            symbol_id=row.id, trade_date=day, open_price=price, high_price=price,
                            low_price=price, close_price=price, volume=1000, turnover=price * 1000, source='test',
                        )
                    )

    def _clean(self):
        with session_scope() as session:
            session.execute(text("delete from daily_price_bars where symbol_id in (select id from symbols where source = 'test' and symbol like '9000%')"))
            session.execute(text("delete from symbols where source = 'test' and symbol like '9000%'"))
            session.execute(delete(BusinessLineDailyMetric))

    def test_equal_weight_line_returns_against_the_same_universe(self):
        with engine.connect() as connection:
            rows = compute_daily_metrics(connection, start=date(2026, 11, 5), end=date(2026, 11, 6), mapping=self.SYMBOLS)
        got = {(r['trade_date'].isoformat(), r['business_line']): r for r in rows}

        ship_d1 = got[('2026-11-05', 'shipbuilding')]
        self.assertAlmostEqual(ship_d1['ew_return'], 0.0)  # +10 % and -10 %
        self.assertAlmostEqual(ship_d1['market_ew_return'], 0.0)  # banks 0 % too
        self.assertAlmostEqual(ship_d1['advancers_ratio'], 0.5)
        ship_d2 = got[('2026-11-06', 'shipbuilding')]
        self.assertAlmostEqual(ship_d2['ew_return'], (-0.1 + 0.0) / 2)
        # The +200 % bank print is excluded, so banks has no row that day.
        self.assertNotIn(('2026-11-06', 'banks'), got)
        self.assertAlmostEqual(ship_d2['excess_return'], 0.0)

        self.assertEqual(store_daily_metrics(rows), len(rows))
        self.assertEqual(store_daily_metrics(rows), len(rows))  # upsert
        with session_scope() as session:
            self.assertEqual(session.query(BusinessLineDailyMetric).count(), len(rows))


if __name__ == '__main__':
    unittest.main()


def test_index_cli_defaults_to_the_korean_date():
    import sys
    from datetime import datetime
    from unittest.mock import patch
    from zoneinfo import ZoneInfo

    from app.market import business_line_index as index

    seen = {}
    with (
        patch.object(sys, 'argv', ['business_line_index']),
        patch.object(index, 'profiles_as_of', side_effect=lambda day: seen.setdefault('day', day) and {}),
    ):
        index.main()

    assert seen['day'] == datetime.now(ZoneInfo('Asia/Seoul')).date()


def test_index_cli_splits_the_range_by_calendar_year():
    from app.market.business_line_index import year_chunks

    assert year_chunks(date(2024, 6, 1), date(2026, 2, 3)) == [
        (date(2024, 6, 1), date(2024, 12, 31)),
        (date(2025, 1, 1), date(2025, 12, 31)),
        (date(2026, 1, 1), date(2026, 2, 3)),
    ]
