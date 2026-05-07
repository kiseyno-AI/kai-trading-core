import unittest
from datetime import date, datetime, timezone

from sqlalchemy import delete, select
from sqlalchemy.dialects import postgresql

from app.db import engine, session_scope
from app.db_models import DailyPriceBar, EODCollectionRun, Symbol
from app.market_data_store import _build_daily_market_relative_rows_query, load_daily_market_relative_contexts


class MarketRelativeQueryShapeTests(unittest.TestCase):
    def test_market_relative_rows_query_uses_bounded_per_market_trade_dates(self) -> None:
        statement = _build_daily_market_relative_rows_query(
            target_markets=['KOSPI', 'KOSDAQ'],
            market_date=date(2026, 4, 8),
        )

        compiled = str(
            statement.compile(
                dialect=postgresql.dialect(),
                compile_kwargs={'literal_binds': True},
            )
        ).lower()

        self.assertIn('join lateral', compiled)
        self.assertIn('limit 2', compiled)
        self.assertIn("daily_price_bars.trade_date <= '2026-04-08'", compiled)
        self.assertNotIn('row_number', compiled)


class MarketRelativeContextIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        EODCollectionRun.__table__.create(bind=engine, checkfirst=True)
        Symbol.__table__.create(bind=engine, checkfirst=True)
        DailyPriceBar.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        self.kospi_market = 'TEST_KOSPI_319'
        self.kosdaq_market = 'TEST_KOSDAQ_319'
        self.symbols = ['236101', '236102', '336101', '336102', '336103']
        self._seed_rows()

    def tearDown(self) -> None:
        with session_scope() as session:
            symbol_ids = list(
                session.execute(select(Symbol.id).where(Symbol.symbol.in_(self.symbols))).scalars().all()
            )
            if symbol_ids:
                session.execute(delete(DailyPriceBar).where(DailyPriceBar.symbol_id.in_(symbol_ids)))
            session.execute(delete(Symbol).where(Symbol.symbol.in_(self.symbols)))

    def test_load_daily_market_relative_contexts_uses_current_and_prior_trade_dates_per_market(self) -> None:
        items = load_daily_market_relative_contexts(
            market_date='2026-04-08',
            symbols=['236101', '336101', '336102', '336103'],
        )
        items_by_symbol = {item['symbol']: item for item in items}

        kospi_item = items_by_symbol['236101']
        self.assertEqual(kospi_item['status'], 'available')
        self.assertEqual(kospi_item['market'], self.kospi_market)
        self.assertEqual(kospi_item['breadth']['market_symbol_count'], 2)
        self.assertEqual(kospi_item['breadth']['up_count'], 1)
        self.assertEqual(kospi_item['breadth']['down_count'], 1)
        self.assertEqual(kospi_item['breadth']['missing_previous_close_count'], 0)
        self.assertEqual(kospi_item['relative_strength']['return_pct'], 10.0)
        self.assertEqual(kospi_item['relative_strength']['return_rank'], 1)
        self.assertEqual(kospi_item['relative_strength']['return_population'], 2)
        self.assertEqual(kospi_item['relative_strength']['volume_rank'], 2)
        self.assertEqual(kospi_item['relative_strength']['volume_population'], 2)

        kosdaq_available_item = items_by_symbol['336101']
        self.assertEqual(kosdaq_available_item['status'], 'available')
        self.assertEqual(kosdaq_available_item['market'], self.kosdaq_market)
        self.assertEqual(kosdaq_available_item['breadth']['market_symbol_count'], 2)
        self.assertEqual(kosdaq_available_item['breadth']['up_count'], 1)
        self.assertEqual(kosdaq_available_item['breadth']['down_count'], 0)
        self.assertEqual(kosdaq_available_item['breadth']['missing_previous_close_count'], 1)
        self.assertEqual(kosdaq_available_item['relative_strength']['return_pct'], 5.0)
        self.assertEqual(kosdaq_available_item['relative_strength']['return_rank'], 1)
        self.assertEqual(kosdaq_available_item['relative_strength']['return_population'], 1)
        self.assertEqual(kosdaq_available_item['relative_strength']['volume_rank'], 2)
        self.assertEqual(kosdaq_available_item['relative_strength']['volume_population'], 2)

        kosdaq_partial_item = items_by_symbol['336102']
        self.assertEqual(kosdaq_partial_item['status'], 'partial')
        self.assertEqual(kosdaq_partial_item['market'], self.kosdaq_market)
        self.assertEqual(kosdaq_partial_item['breadth']['missing_previous_close_count'], 1)
        self.assertIsNone(kosdaq_partial_item['relative_strength']['return_pct'])
        self.assertIsNone(kosdaq_partial_item['relative_strength']['return_rank'])
        self.assertEqual(kosdaq_partial_item['relative_strength']['return_population'], 1)
        self.assertEqual(kosdaq_partial_item['relative_strength']['volume_rank'], 1)
        self.assertEqual(kosdaq_partial_item['relative_strength']['volume_population'], 2)

        kosdaq_unavailable_item = items_by_symbol['336103']
        self.assertEqual(kosdaq_unavailable_item['status'], 'unavailable')
        self.assertEqual(kosdaq_unavailable_item['market'], self.kosdaq_market)
        self.assertIsNone(kosdaq_unavailable_item['breadth'])
        self.assertIsNone(kosdaq_unavailable_item['relative_strength'])

    def _seed_rows(self) -> None:
        captured_at = datetime(2026, 4, 8, 6, 0, tzinfo=timezone.utc)
        with session_scope() as session:
            symbol_rows = [
                Symbol(
                    symbol='236101',
                    name_ko='Market Alpha',
                    market=self.kospi_market,
                    instrument_type='equity',
                    source='test',
                    is_active=True,
                ),
                Symbol(
                    symbol='236102',
                    name_ko='Market Beta',
                    market=self.kospi_market,
                    instrument_type='equity',
                    source='test',
                    is_active=True,
                ),
                Symbol(
                    symbol='336101',
                    name_ko='Market Gamma',
                    market=self.kosdaq_market,
                    instrument_type='equity',
                    source='test',
                    is_active=True,
                ),
                Symbol(
                    symbol='336102',
                    name_ko='Market Delta',
                    market=self.kosdaq_market,
                    instrument_type='equity',
                    source='test',
                    is_active=True,
                ),
                Symbol(
                    symbol='336103',
                    name_ko='Market Epsilon',
                    market=self.kosdaq_market,
                    instrument_type='equity',
                    source='test',
                    is_active=True,
                ),
            ]
            session.add_all(symbol_rows)
            session.flush()

            symbol_id_by_symbol = {row.symbol: row.id for row in symbol_rows}
            session.add_all(
                [
                    DailyPriceBar(
                        symbol_id=symbol_id_by_symbol['236101'],
                        trade_date=date(2026, 4, 8),
                        open_price=105,
                        high_price=111,
                        low_price=104,
                        close_price=110,
                        volume=1500,
                        turnover=165000,
                        source='test',
                        captured_at=captured_at,
                    ),
                    DailyPriceBar(
                        symbol_id=symbol_id_by_symbol['236101'],
                        trade_date=date(2026, 4, 7),
                        open_price=98,
                        high_price=101,
                        low_price=97,
                        close_price=100,
                        volume=1400,
                        turnover=140000,
                        source='test',
                        captured_at=captured_at,
                    ),
                    DailyPriceBar(
                        symbol_id=symbol_id_by_symbol['236101'],
                        trade_date=date(2026, 4, 1),
                        open_price=68,
                        high_price=71,
                        low_price=67,
                        close_price=70,
                        volume=900,
                        turnover=63000,
                        source='test',
                        captured_at=captured_at,
                    ),
                    DailyPriceBar(
                        symbol_id=symbol_id_by_symbol['236102'],
                        trade_date=date(2026, 4, 8),
                        open_price=95,
                        high_price=96,
                        low_price=89,
                        close_price=90,
                        volume=2500,
                        turnover=225000,
                        source='test',
                        captured_at=captured_at,
                    ),
                    DailyPriceBar(
                        symbol_id=symbol_id_by_symbol['236102'],
                        trade_date=date(2026, 4, 7),
                        open_price=102,
                        high_price=103,
                        low_price=99,
                        close_price=100,
                        volume=2400,
                        turnover=240000,
                        source='test',
                        captured_at=captured_at,
                    ),
                    DailyPriceBar(
                        symbol_id=symbol_id_by_symbol['336101'],
                        trade_date=date(2026, 4, 8),
                        open_price=205,
                        high_price=211,
                        low_price=204,
                        close_price=210,
                        volume=500,
                        turnover=105000,
                        source='test',
                        captured_at=captured_at,
                    ),
                    DailyPriceBar(
                        symbol_id=symbol_id_by_symbol['336101'],
                        trade_date=date(2026, 4, 6),
                        open_price=198,
                        high_price=201,
                        low_price=197,
                        close_price=200,
                        volume=450,
                        turnover=90000,
                        source='test',
                        captured_at=captured_at,
                    ),
                    DailyPriceBar(
                        symbol_id=symbol_id_by_symbol['336101'],
                        trade_date=date(2026, 4, 4),
                        open_price=148,
                        high_price=151,
                        low_price=147,
                        close_price=150,
                        volume=430,
                        turnover=64500,
                        source='test',
                        captured_at=captured_at,
                    ),
                    DailyPriceBar(
                        symbol_id=symbol_id_by_symbol['336102'],
                        trade_date=date(2026, 4, 8),
                        open_price=175,
                        high_price=181,
                        low_price=174,
                        close_price=180,
                        volume=800,
                        turnover=144000,
                        source='test',
                        captured_at=captured_at,
                    ),
                    DailyPriceBar(
                        symbol_id=symbol_id_by_symbol['336102'],
                        trade_date=date(2026, 4, 4),
                        open_price=158,
                        high_price=161,
                        low_price=157,
                        close_price=160,
                        volume=700,
                        turnover=112000,
                        source='test',
                        captured_at=captured_at,
                    ),
                    DailyPriceBar(
                        symbol_id=symbol_id_by_symbol['336103'],
                        trade_date=date(2026, 4, 6),
                        open_price=118,
                        high_price=121,
                        low_price=117,
                        close_price=120,
                        volume=300,
                        turnover=36000,
                        source='test',
                        captured_at=captured_at,
                    ),
                ]
            )


if __name__ == '__main__':
    unittest.main()
