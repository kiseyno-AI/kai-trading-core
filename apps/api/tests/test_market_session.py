from datetime import date, datetime
from types import SimpleNamespace
import unittest
from zoneinfo import ZoneInfo

from app.market.clock import get_market_state
from app.market.session import (
    RECOMMENDATION_BASIS_LATEST_COMPLETED_EOD,
    RECOMMENDATION_BASIS_LIVE_INTRADAY,
    build_market_session_metadata,
    is_krx_trading_day,
    market_session_status_for_date,
)

KST = ZoneInfo('Asia/Seoul')


class MarketSessionTests(unittest.TestCase):
    def test_build_market_session_metadata_marks_weekend_as_latest_completed_eod(self):
        current_state = SimpleNamespace(
            state='WEEKEND',
            is_trading_day=False,
            is_open=False,
            reason='weekend',
            server_time='2026-04-11T12:00:00+09:00',
            current_calendar_date='2026-04-11',
            latest_valid_trading_date='2026-04-10',
            market_session='weekend',
        )

        result = build_market_session_metadata(
            requested_date='2026-04-11',
            current_state=current_state,
        )

        self.assertEqual(result['status'], 'weekend')
        self.assertEqual(result['requested_date'], '2026-04-11')
        self.assertEqual(result['latest_valid_trading_date'], '2026-04-10')
        self.assertFalse(result['is_trading_day'])
        self.assertFalse(result['intraday_data_expected'])
        self.assertFalse(result['quote_data_expected'])
        self.assertEqual(result['recommendation_basis'], RECOMMENDATION_BASIS_LATEST_COMPLETED_EOD)

    def test_get_market_state_marks_real_holiday_as_holiday(self):
        result = get_market_state(datetime(2026, 5, 5, 12, 0, tzinfo=KST))

        self.assertEqual(result.state, 'HOLIDAY')
        self.assertEqual(result.reason, 'holiday')
        self.assertEqual(result.current_calendar_date, '2026-05-05')
        self.assertEqual(result.latest_valid_trading_date, '2026-05-04')
        self.assertEqual(result.market_session, 'holiday')

    def test_market_session_helpers_recognize_real_holiday_and_open_session(self):
        self.assertFalse(is_krx_trading_day(date(2026, 5, 5)))
        self.assertEqual(market_session_status_for_date('2026-05-05'), 'holiday')

        open_state = SimpleNamespace(
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            reason=None,
            server_time='2026-04-29T10:05:00+09:00',
            current_calendar_date='2026-04-29',
            latest_valid_trading_date='2026-04-29',
            market_session='open',
        )
        result = build_market_session_metadata(
            requested_date='2026-04-29',
            current_state=open_state,
        )

        self.assertTrue(result['is_trading_day'])
        self.assertEqual(result['status'], 'open')
        self.assertTrue(result['intraday_data_expected'])
        self.assertTrue(result['quote_data_expected'])
        self.assertEqual(result['recommendation_basis'], RECOMMENDATION_BASIS_LIVE_INTRADAY)


if __name__ == '__main__':
    unittest.main()
