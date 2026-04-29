import unittest

from app.core.settings import settings
from app.market.snapshot import (
    MarketSnapshotConfigurationError,
    MarketSnapshotService,
    MarketSnapshotUpstreamError,
)


class FakeKISClient:
    def __init__(self) -> None:
        self.index_calls: list[str] = []
        self.flow_calls: list[tuple[str, str]] = []
        self.token_calls = 0

    async def issue_access_token(self, force_refresh: bool = False) -> dict:
        self.token_calls += 1
        return {'access_token': 'token', 'cached': False}

    async def fetch_market_index_price(self, *, index_code: str) -> dict:
        self.index_calls.append(index_code)
        if index_code == '0001':
            return {
                'output': {
                    'bstp_nmix_prpr': '2642.07',
                    'bstp_nmix_prdy_vrss': '2.78',
                    'bstp_nmix_prdy_ctrt': '0.11',
                    'bstp_nmix_oprc': '2651.63',
                    'bstp_nmix_hgpr': '2653.87',
                    'bstp_nmix_lwpr': '2634.29',
                    'acml_vol': '305715',
                    'acml_tr_pbmn': '4736153',
                    'ascn_issu_cnt': '512',
                    'stnr_issu_cnt': '94',
                    'down_issu_cnt': '330',
                }
            }
        return {
            'output': {
                'bstp_nmix_prpr': '857.60',
                'bstp_nmix_prdy_vrss': '-1.61',
                'bstp_nmix_prdy_ctrt': '-0.19',
                'bstp_nmix_oprc': '863.69',
                'bstp_nmix_hgpr': '864.24',
                'bstp_nmix_lwpr': '854.72',
                'acml_vol': '1312496',
                'acml_tr_pbmn': '11507962',
                'ascn_issu_cnt': '828',
                'stnr_issu_cnt': '94',
                'down_issu_cnt': '716',
            }
        }

    async def fetch_investor_time_by_market(self, *, market_code: str, sector_code: str) -> dict:
        self.flow_calls.append((market_code, sector_code))
        if market_code == 'KSP':
            return {
                'output': [
                    {
                        'frgn_ntby_qty': '-5290',
                        'frgn_ntby_tr_pbmn': '148656',
                        'orgn_ntby_qty': '720',
                        'orgn_ntby_tr_pbmn': '-55532',
                    }
                ]
            }
        return {
            'output': [
                {
                    'frgn_ntby_qty': '-18565',
                    'frgn_ntby_tr_pbmn': '-597490',
                    'orgn_ntby_qty': '-4738',
                    'orgn_ntby_tr_pbmn': '-150685',
                }
            ]
        }


class FakeFXService:
    async def get_exchange_rates(self, **kwargs):
        return {
            'provider': 'koreaexim',
            'requested_date': '20260429',
            'effective_date': '20260428',
            'fallback_applied': True,
            'currency': kwargs.get('currency'),
            'count': 1,
            'items': [{'currency_code': 'USD', 'exchange_rate': 1444.5}],
        }


class EmptyFXService:
    async def get_exchange_rates(self, **kwargs):
        return {
            'provider': 'koreaexim',
            'requested_date': '20260429',
            'effective_date': '20260429',
            'fallback_applied': False,
            'currency': kwargs.get('currency'),
            'count': 0,
            'items': [],
        }


class MarketSnapshotServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.original_settings = {
            'kis_use_mock': settings.kis_use_mock,
            'market_snapshot_kis_use_real': settings.market_snapshot_kis_use_real,
            'kis_app_key': settings.kis_app_key,
            'kis_app_secret': settings.kis_app_secret,
            'koreaexim_api_key': settings.koreaexim_api_key,
        }
        settings.kis_app_key = 'test-kis-key'
        settings.kis_app_secret = 'test-kis-secret'
        settings.koreaexim_api_key = 'test-koreaexim-key'

    def tearDown(self) -> None:
        for field_name, value in self.original_settings.items():
            setattr(settings, field_name, value)

    async def test_get_market_snapshot_uses_real_domain_override_when_global_kis_is_mock(self) -> None:
        original_kis_use_mock = settings.kis_use_mock
        original_market_snapshot_kis_use_real = settings.market_snapshot_kis_use_real
        settings.kis_use_mock = True
        settings.market_snapshot_kis_use_real = True
        client = FakeKISClient()
        service = MarketSnapshotService(real_domain_kis_api_client=client, koreaexim_service=FakeFXService())
        try:
            result = await service.get_market_snapshot()
        finally:
            settings.kis_use_mock = original_kis_use_mock
            settings.market_snapshot_kis_use_real = original_market_snapshot_kis_use_real

        self.assertEqual(client.token_calls, 1)
        self.assertEqual(client.index_calls, ['0001', '1001'])
        self.assertEqual(client.flow_calls, [('KSP', '0001'), ('KSQ', '1001')])
        self.assertEqual(service.runtime_status()['effective_kis_environment'], 'real')
        self.assertTrue(service.runtime_status()['uses_real_domain_override'])
        self.assertEqual(result['markets']['kospi']['label'], 'KOSPI')
        self.assertEqual(result['markets']['kospi']['value'], 2642.07)
        self.assertEqual(result['markets']['kospi']['foreign_net_quantity'], -5290)
        self.assertEqual(result['markets']['kosdaq']['institution_net_value'], -150685)
        self.assertEqual(result['usd_krw']['pair'], 'USD/KRW')
        self.assertEqual(result['usd_krw']['rate'], 1444.5)
        self.assertEqual(result['usd_krw']['effective_date'], '2026-04-28')
        self.assertTrue(result['usd_krw']['fallback_applied'])

    async def test_get_market_snapshot_requires_real_kis_domain_when_override_is_disabled(self) -> None:
        original_kis_use_mock = settings.kis_use_mock
        original_market_snapshot_kis_use_real = settings.market_snapshot_kis_use_real
        settings.kis_use_mock = True
        settings.market_snapshot_kis_use_real = False
        service = MarketSnapshotService(kis_api_client=FakeKISClient(), koreaexim_service=FakeFXService())
        try:
            with self.assertRaises(MarketSnapshotConfigurationError) as ctx:
                await service.get_market_snapshot()
        finally:
            settings.kis_use_mock = original_kis_use_mock
            settings.market_snapshot_kis_use_real = original_market_snapshot_kis_use_real

        self.assertIn('MARKET_SNAPSHOT_KIS_USE_REAL', str(ctx.exception))

    async def test_get_market_snapshot_rejects_empty_fx_payload(self) -> None:
        original_kis_use_mock = settings.kis_use_mock
        original_market_snapshot_kis_use_real = settings.market_snapshot_kis_use_real
        settings.kis_use_mock = False
        settings.market_snapshot_kis_use_real = True
        service = MarketSnapshotService(kis_api_client=FakeKISClient(), koreaexim_service=EmptyFXService())
        try:
            with self.assertRaises(MarketSnapshotUpstreamError):
                await service.get_market_snapshot()
        finally:
            settings.kis_use_mock = original_kis_use_mock
            settings.market_snapshot_kis_use_real = original_market_snapshot_kis_use_real


if __name__ == '__main__':
    unittest.main()
