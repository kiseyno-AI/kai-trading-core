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
    missing_institutional: bool = False,
    fx_status: str = 'ready',
    fx_stale_reason: str | None = None,
    fx_source: str = 'live',
) -> dict:
    return {
        'as_of': datetime.fromisoformat('2026-04-29T10:05:12+09:00'),
        'status': 'ready',
        'missing_fields': [],
        'stale_reason': None,
        'markets': {
            'kospi': {
                'label': 'KOSPI',
                'value': 2642.07,
                'change': 11.78,
                'change_percent': 0.45,
                'advancers': 702,
                'unchanged': 81,
                'decliners': 198,
                'foreign_net_value': -1455933000000,
                'institution_net_value': 283792000000,
            },
            'kosdaq': {
                'label': 'KOSDAQ',
                'value': 857.60,
                'change': 4.61,
                'change_percent': 0.54,
                'advancers': 901,
                'unchanged': 92,
                'decliners': 501,
                'foreign_net_value': -211205000000,
                'institution_net_value': None if missing_institutional else -304502000000,
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
