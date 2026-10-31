from datetime import date, datetime
import unittest

from app.agent_context.signal_decider_market_context import SignalDeciderMarketContextBuilder


class FakeSnapshotService:
    def __init__(self, payload: dict):
        self.payload = payload

    async def get_market_snapshot(self) -> dict:
        return self.payload


class ErrorSnapshotService:
    async def get_market_snapshot(self) -> dict:
        raise RuntimeError('snapshot unavailable')


class FakeWeekendMarketState:
    state = 'WEEKEND'
    is_trading_day = False
    is_open = False
    reason = 'weekend'
    server_time = '2026-05-09T12:00:00+09:00'
    current_calendar_date = '2026-05-09'
    latest_valid_trading_date = '2026-05-08'
    market_session = 'weekend'


class FakeCurrentMarketState:
    def __init__(self, *, state: str, server_time: str, is_open: bool) -> None:
        self.state = state
        self.is_trading_day = True
        self.is_open = is_open
        self.reason = None
        self.server_time = server_time
        self.current_calendar_date = server_time[:10]
        self.latest_valid_trading_date = self.current_calendar_date
        self.market_session = state.lower()
        self.calendar_source = 'exchange_calendars'
        self.calendar_source_status = 'ok'
        self.calendar_source_error = None


def _snapshot_payload(
    *,
    fallback_applied: bool = False,
    legacy_cached_flow_values: bool = False,
    missing_institutional: bool = False,
    fx_status: str = 'ready',
    fx_stale_reason: str | None = None,
    fx_source: str = 'live',
) -> dict:
    kospi_foreign_value = -1455933 if legacy_cached_flow_values else -1455933000000
    kospi_institution_value = 283792 if legacy_cached_flow_values else 283792000000
    kosdaq_foreign_value = -211205 if legacy_cached_flow_values else -211205000000
    kosdaq_institution_value = None
    if not missing_institutional:
        kosdaq_institution_value = -304502 if legacy_cached_flow_values else -304502000000

    payload = {
        'as_of': datetime.fromisoformat('2026-04-29T10:05:12+09:00'),
        'status': 'ready',
        'missing_fields': [],
        'stale_reason': None,
        'markets': {
            'kospi': {
                'label': 'KOSPI',
                'index_code': '0001',
                'value': 2642.07,
                'change': 11.78,
                'change_percent': 0.45,
                'trading_value': 4736153,
                'trading_value_unit': 'million_krw',
                'advancers': 702,
                'unchanged': 81,
                'decliners': 198,
                'foreign_net_value': kospi_foreign_value,
                'institution_net_value': kospi_institution_value,
                'investor_flow_value_unit': 'krw',
            },
            'kosdaq': {
                'label': 'KOSDAQ',
                'index_code': '1001',
                'value': 857.60,
                'change': 4.61,
                'change_percent': 0.54,
                'trading_value': 11507962,
                'trading_value_unit': 'million_krw',
                'advancers': 901,
                'unchanged': 92,
                'decliners': 501,
                'foreign_net_value': kosdaq_foreign_value,
                'institution_net_value': kosdaq_institution_value,
                'investor_flow_value_unit': 'krw',
            },
        },
        'usd_krw': {
            'rate': 1444.5,
            'effective_date': '2026-04-29',
            'fallback_applied': fallback_applied,
            'status': fx_status,
            'source': fx_source,
            'stale_reason': fx_stale_reason,
            'detail': None,
            'captured_at': '2026-04-29T10:05:12+09:00',
        },
    }
    if legacy_cached_flow_values:
        for row in payload['markets'].values():
            row.pop('trading_value_unit', None)
            row.pop('investor_flow_value_unit', None)
    return payload


class SignalDeciderMarketContextBuilderTests(unittest.TestCase):
    def test_historical_build_is_stable_across_current_session_transitions(self) -> None:
        snapshot_service = FakeSnapshotService(_snapshot_payload())
        preopen_builder = SignalDeciderMarketContextBuilder(
            snapshot_service=snapshot_service,
            market_state_loader=lambda: FakeCurrentMarketState(
                state='PREOPEN',
                server_time='2026-04-30T08:40:00+09:00',
                is_open=False,
            ),
        )
        open_builder = SignalDeciderMarketContextBuilder(
            snapshot_service=snapshot_service,
            market_state_loader=lambda: FakeCurrentMarketState(
                state='OPEN',
                server_time='2026-04-30T09:10:00+09:00',
                is_open=True,
            ),
        )

        preopen_result = preopen_builder.build(market_date=date(2026, 4, 29))
        open_result = open_builder.build(market_date=date(2026, 4, 29))

        self.assertEqual(preopen_result, open_result)
        self.assertIsNone(preopen_result['market_session']['current_calendar_date'])
        self.assertIsNone(preopen_result['market_session']['current_market_state'])
        self.assertIsNone(preopen_result['market_session']['current_market_session'])
        self.assertEqual(
            preopen_result['market_session']['recommendation_basis'],
            'latest_completed_eod',
        )

    def test_historical_build_rejects_current_session_snapshot(self) -> None:
        snapshot = _snapshot_payload()
        snapshot['as_of'] = datetime.fromisoformat('2026-04-30T09:10:00+09:00')
        builder = SignalDeciderMarketContextBuilder(
            snapshot_service=FakeSnapshotService(snapshot),
            market_state_loader=lambda: FakeCurrentMarketState(
                state='OPEN',
                server_time='2026-04-30T09:10:00+09:00',
                is_open=True,
            ),
        )

        result = builder.build(market_date=date(2026, 4, 29))

        self.assertEqual(result['status'], 'unavailable')
        self.assertEqual(result['indices'], {})
        self.assertEqual(result['fx'], {})
        self.assertEqual(result['flows'], {})
        self.assertEqual(result['stale_reason'], 'historical_eod_snapshot_unavailable')

    def test_current_date_build_preserves_live_session_state(self) -> None:
        builder = SignalDeciderMarketContextBuilder(
            snapshot_service=FakeSnapshotService(_snapshot_payload()),
            market_state_loader=lambda: FakeCurrentMarketState(
                state='OPEN',
                server_time='2026-04-29T10:05:12+09:00',
                is_open=True,
            ),
        )

        result = builder.build(market_date=date(2026, 4, 29))

        self.assertEqual(result['status'], 'ready')
        self.assertEqual(result['market_session']['market_state'], 'OPEN')
        self.assertEqual(result['market_session']['current_market_state'], 'OPEN')
        self.assertEqual(result['market_session']['recommendation_basis'], 'live_intraday')

    def test_build_returns_ready_context_when_snapshot_is_complete(self) -> None:
        builder = SignalDeciderMarketContextBuilder(snapshot_service=FakeSnapshotService(_snapshot_payload()))

        result = builder.build(market_date=date(2026, 4, 29))

        self.assertEqual(result['status'], 'ready')
        self.assertEqual(result['summary']['market_bias'], 'risk_on')
        self.assertEqual(result['indices']['kospi']['level'], 2642.07)
        self.assertEqual(result['indices']['kosdaq']['change_rate'], 0.54)
        self.assertEqual(result['fx']['usdkrw']['value'], 1444.5)
        self.assertFalse(result['fx']['usdkrw']['provider_gap'])
        self.assertEqual(result['fx']['usdkrw']['provider_freshness'], 'fresh')
        self.assertTrue(result['fx']['usdkrw']['value_available'])
        self.assertEqual(result['flows']['foreign']['value'], -1667138000000)
        self.assertEqual(result['flows']['foreign']['unit'], 'krw')
        self.assertEqual(result['flows']['institutional']['value'], -20710000000)
        self.assertEqual(result['flows']['institutional']['unit'], 'krw')
        self.assertEqual(result['missing_fields'], [])
        self.assertIsNone(result['stale_reason'])
        self.assertIn('KOSPI +0.45%', result['summary']['line'])
        self.assertIn('KOSDAQ breadth 901/92/501', result['summary']['breadth_summary'])

    def test_build_normalizes_legacy_cached_flow_values_before_labeling_krw(self) -> None:
        builder = SignalDeciderMarketContextBuilder(
            snapshot_service=FakeSnapshotService(_snapshot_payload(legacy_cached_flow_values=True))
        )

        result = builder.build(market_date=date(2026, 4, 29))

        self.assertEqual(result['status'], 'ready')
        self.assertEqual(result['flows']['foreign']['value'], -1667138000000)
        self.assertEqual(result['flows']['foreign']['unit'], 'krw')
        self.assertEqual(result['flows']['institutional']['value'], -20710000000)
        self.assertEqual(result['flows']['institutional']['unit'], 'krw')
        self.assertEqual(result['missing_fields'], [])

    def test_build_returns_partial_when_flow_inputs_are_missing(self) -> None:
        builder = SignalDeciderMarketContextBuilder(
            snapshot_service=FakeSnapshotService(_snapshot_payload(missing_institutional=True))
        )

        result = builder.build(market_date=date(2026, 4, 29))

        self.assertEqual(result['status'], 'partial')
        self.assertIn('flows.institutional', result['missing_fields'])
        self.assertIn('foreign', result['flows'])
        self.assertNotIn('institutional', result['flows'])
        self.assertIsNone(result['stale_reason'])

    def test_build_returns_stale_when_fx_fallback_was_used(self) -> None:
        builder = SignalDeciderMarketContextBuilder(
            snapshot_service=FakeSnapshotService(_snapshot_payload(fallback_applied=True))
        )

        result = builder.build(market_date=date(2026, 4, 29))

        self.assertEqual(result['status'], 'stale')
        self.assertEqual(result['stale_reason'], 'usdkrw_fallback_applied')
        self.assertTrue(result['fx']['usdkrw']['fallback_applied'])

    def test_build_returns_stale_when_snapshot_marks_fx_as_cached_stale(self) -> None:
        builder = SignalDeciderMarketContextBuilder(
            snapshot_service=FakeSnapshotService(
                _snapshot_payload(
                    fallback_applied=False,
                    fx_status='stale',
                    fx_stale_reason='koreaexim_quota_exhausted',
                    fx_source='cache',
                )
            )
        )

        result = builder.build(market_date=date(2026, 4, 29))

        self.assertEqual(result['status'], 'stale')
        self.assertEqual(result['stale_reason'], 'koreaexim_quota_exhausted')
        self.assertEqual(result['fx']['usdkrw']['source'], 'cache')
        self.assertEqual(result['fx']['usdkrw']['status'], 'stale')
        self.assertTrue(result['fx']['usdkrw']['provider_gap'])
        self.assertEqual(result['fx']['usdkrw']['provider_freshness'], 'stale')
        self.assertTrue(result['fx']['usdkrw']['value_available'])

    def test_build_preserves_unavailable_fx_provider_gap_metadata(self) -> None:
        snapshot = _snapshot_payload()
        snapshot['status'] = 'partial'
        snapshot['stale_reason'] = 'koreaexim_rate_not_found'
        snapshot['usd_krw'] = {
            'rate': None,
            'effective_date': None,
            'fallback_applied': False,
            'status': 'missing',
            'source': 'unavailable',
            'stale_reason': 'koreaexim_rate_not_found',
            'detail': 'USD/KRW rate unavailable for requested date',
            'captured_at': None,
        }
        builder = SignalDeciderMarketContextBuilder(
            snapshot_service=FakeSnapshotService(snapshot)
        )

        result = builder.build(market_date=date(2026, 4, 29))

        self.assertEqual(result['status'], 'stale')
        self.assertEqual(result['stale_reason'], 'koreaexim_rate_not_found')
        self.assertIn('fx.usdkrw', result['missing_fields'])
        self.assertIsNone(result['fx']['usdkrw']['value'])
        self.assertTrue(result['fx']['usdkrw']['provider_gap'])
        self.assertEqual(result['fx']['usdkrw']['provider_freshness'], 'missing')
        self.assertFalse(result['fx']['usdkrw']['value_available'])

    def test_build_does_not_mark_fx_fresh_without_effective_date(self) -> None:
        snapshot = _snapshot_payload()
        snapshot['usd_krw']['effective_date'] = None
        builder = SignalDeciderMarketContextBuilder(
            snapshot_service=FakeSnapshotService(snapshot)
        )

        result = builder.build(market_date=date(2026, 4, 29))

        self.assertEqual(result['status'], 'partial')
        self.assertIn('fx.usdkrw.effective_date', result['missing_fields'])
        self.assertTrue(result['fx']['usdkrw']['provider_gap'])
        self.assertEqual(result['fx']['usdkrw']['provider_freshness'], 'stale')
        self.assertTrue(result['fx']['usdkrw']['value_available'])

    def test_build_returns_unavailable_when_snapshot_fetch_fails(self) -> None:
        builder = SignalDeciderMarketContextBuilder(snapshot_service=ErrorSnapshotService())

        result = builder.build(market_date=date(2026, 4, 29))

        self.assertEqual(result['status'], 'unavailable')
        self.assertEqual(result['summary']['market_bias'], 'unknown')
        self.assertEqual(result['indices'], {})
        self.assertEqual(result['fx'], {})
        self.assertEqual(result['flows'], {})
        self.assertIn('indices.kospi', result['missing_fields'])
        self.assertIn('fx.usdkrw', result['missing_fields'])

    def test_build_includes_market_session_metadata_for_closed_market_dates(self) -> None:
        builder = SignalDeciderMarketContextBuilder(
            snapshot_service=ErrorSnapshotService(),
            market_state_loader=lambda: FakeWeekendMarketState(),
        )

        result = builder.build(market_date=date(2026, 5, 9))

        self.assertEqual(result['market_session']['requested_date'], '2026-05-09')
        self.assertEqual(result['market_session']['status'], 'weekend')
        self.assertEqual(result['market_session']['latest_valid_trading_date'], '2026-05-08')
        self.assertFalse(result['market_session']['intraday_data_expected'])
        self.assertEqual(result['market_session']['recommendation_basis'], 'latest_completed_eod')


if __name__ == '__main__':
    unittest.main()
