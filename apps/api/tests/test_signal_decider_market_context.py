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
    def test_build_returns_ready_context_when_snapshot_is_complete(self) -> None:
        builder = SignalDeciderMarketContextBuilder(snapshot_service=FakeSnapshotService(_snapshot_payload()))

        result = builder.build(market_date=date(2026, 4, 29))

        self.assertEqual(result['status'], 'ready')
        self.assertEqual(result['summary']['market_bias'], 'risk_on')
        self.assertEqual(result['indices']['kospi']['level'], 2642.07)
        self.assertEqual(result['indices']['kosdaq']['change_rate'], 0.54)
        self.assertEqual(result['fx']['usdkrw']['value'], 1444.5)
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


if __name__ == '__main__':
    unittest.main()
