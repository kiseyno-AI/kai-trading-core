import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.core.settings import settings
from app.market.kis_intraday_snapshots import KISIntradaySnapshotService


class FakeKISIntradayClient:
    async def fetch_domestic_quote(self, symbol: str, market_code: str = 'J') -> dict:
        return {
            'output': {
                'stck_prpr': '71550',
                'prdy_vrss': '50',
                'prdy_ctrt': '0.07',
                'acml_vol': '150000',
                'acml_tr_pbmn': '9000000000',
                'bidp': '71500',
                'askp': '71600',
                'stck_cntg_hour': '101530',
                'stck_bsop_date': '20260507',
            }
        }

    async def fetch_domestic_orderbook(self, symbol: str, market_code: str = 'J') -> dict:
        return {
            'output1': {
                'aspr_acpt_hour': '101531',
                'askp1': '71600',
                'bidp1': '71500',
                'askp_rsqn1': '1200',
                'bidp_rsqn1': '1500',
                'total_askp_rsqn': '8400',
                'total_bidp_rsqn': '9800',
                'ntby_aspr_rsqn': '1400',
            },
            'output2': {
                'antc_cnpr': '71550',
                'antc_cntg_vrss': '50',
                'antc_cntg_prdy_ctrt': '0.07',
                'antc_vol': '1000',
            },
        }

    async def fetch_domestic_program_trade_by_stock(self, symbol: str, market_code: str = 'J') -> dict:
        return {
            'output': [
                {
                    'bsop_hour': '101530',
                    'stck_prpr': '71550',
                    'acml_vol': '150000',
                    'whol_smtn_ntby_qty': '2300',
                    'whol_smtn_ntby_tr_pbmn': '164565000',
                    'whol_smtn_seln_vol': '8200',
                    'whol_smtn_shnu_vol': '10500',
                    'whol_smtn_seln_tr_pbmn': '586710000',
                    'whol_smtn_shnu_tr_pbmn': '751275000',
                }
            ]
        }

    async def fetch_domestic_investor_trend_estimate(self, symbol: str) -> dict:
        return {
            'output2': [
                {
                    'bsop_hour_gb': '4',
                    'frgn_fake_ntby_qty': '1200',
                    'orgn_fake_ntby_qty': '900',
                    'sum_fake_ntby_qty': '2100',
                }
            ]
        }

    async def fetch_domestic_news_titles(self, **kwargs) -> dict:
        return {
            'output': [
                {
                    'cntt_usiq_srno': '1',
                    'news_ofer_entp_code': '01',
                    'data_dt': '20260507',
                    'data_tm': '100500',
                    'hts_pbnt_titl_cntt': '삼성전자 실적 기대감에 강세',
                    'news_lrdv_code': '10',
                    'dorg': '연합뉴스',
                    'iscd1': '005930',
                },
                {
                    'cntt_usiq_srno': '2',
                    'news_ofer_entp_code': '02',
                    'data_dt': '20260507',
                    'data_tm': '095500',
                    'hts_pbnt_titl_cntt': '삼성전자 대형 공급 계약 체결',
                    'news_lrdv_code': '10',
                    'dorg': '한국경제',
                    'iscd1': '005930',
                },
            ],
            'page_count': 1,
            'max_pages_reached': False,
        }

    async def fetch_domestic_ranking_volume_power(self, **kwargs) -> dict:
        return {
            'output': [
                {
                    'stck_shrn_iscd': '005930',
                    'data_rank': '3',
                    'tday_rltv': '138.5',
                    'shnu_cnqn_smtn': '1020',
                    'seln_cnqn_smtn': '740',
                }
            ],
            'page_count': 1,
            'max_pages_reached': False,
        }

    async def fetch_domestic_volume_rank(self, **kwargs) -> dict:
        return {
            'output': [
                {
                    'mksc_shrn_iscd': '005930',
                    'data_rank': '9',
                }
            ]
        }

    async def fetch_domestic_quote_balance_ranking(self, **kwargs) -> dict:
        return {
            'output': [
                {
                    'mksc_shrn_iscd': '005930',
                    'data_rank': '4',
                    'total_askp_rsqn': '8400',
                    'total_bidp_rsqn': '9800',
                    'total_ntsl_bidp_rsqn': '1400',
                    'shnu_rsqn_rate': '53.8',
                    'seln_rsqn_rate': '46.2',
                }
            ],
            'page_count': 1,
            'max_pages_reached': False,
        }


class KISIntradaySnapshotServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.original_app_key = settings.kis_app_key
        self.original_app_secret = settings.kis_app_secret
        self.original_enabled = settings.kis_intraday_snapshot_enabled
        settings.kis_app_key = 'test-key'
        settings.kis_app_secret = 'test-secret'
        settings.kis_intraday_snapshot_enabled = True

    def tearDown(self) -> None:
        settings.kis_app_key = self.original_app_key
        settings.kis_app_secret = self.original_app_secret
        settings.kis_intraday_snapshot_enabled = self.original_enabled

    async def test_collect_snapshots_normalizes_execution_context_and_persists_rows(self) -> None:
        service = KISIntradaySnapshotService(client=FakeKISIntradayClient())

        with (
            patch(
                'app.market.kis_intraday_snapshots.async_get_market_state',
                return_value=SimpleNamespace(state='OPEN', server_time='2026-05-07T10:15:35+09:00'),
            ),
            patch('app.market.kis_intraday_snapshots.persist_kis_intraday_snapshot') as persist_snapshot,
        ):
            result = await service.collect_snapshots(symbols=['005930'], max_ranking_pages=1, max_news_items=2)

        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['snapshots_persisted'], 1)
        self.assertEqual(result['summary']['available_count'], 1)
        persist_snapshot.assert_called_once()

        payload = result['items'][0]
        self.assertEqual(payload['availability_status'], 'available')
        self.assertEqual(payload['quote']['spread_points'], 100)
        self.assertAlmostEqual(payload['orderbook']['imbalance'], 0.076923, places=5)
        self.assertEqual(payload['liquidity']['volume_power_rank'], 3)
        self.assertEqual(payload['liquidity']['quote_balance_rank'], 4)
        self.assertEqual(
            payload['investor_program_activity']['program_trade']['net_buy_quantity'],
            2300,
        )
        self.assertEqual(
            payload['investor_program_activity']['investor_trend_estimate']['combined_estimated_net_quantity'],
            2100,
        )
        self.assertEqual(payload['news_hints']['item_count'], 2)
        self.assertEqual(payload['news_hints']['event_tags'], ['contract', 'earnings'])
        self.assertEqual(payload['coverage']['status'], 'available')
        self.assertEqual(payload['freshness_status'], 'fresh')

    async def test_collect_snapshots_is_disabled_without_credentials_before_universe_lookup(self) -> None:
        settings.kis_app_key = ''
        settings.kis_app_secret = ''
        client = FakeKISIntradayClient()
        service = KISIntradaySnapshotService(client=client)

        with (
            patch.object(service, '_capture_universe', side_effect=AssertionError('should not load universe')),
            patch.object(client, 'fetch_domestic_quote', new=AsyncMock(side_effect=AssertionError('should not call KIS'))),
        ):
            result = await service.collect_snapshots(symbols=['005930'])

        self.assertEqual(result['status'], 'disabled')
        self.assertEqual(result['reason'], 'kis_credentials_missing')
        self.assertEqual(result['requested_symbols'], ['005930'])
        self.assertEqual(result['summary']['matched_symbol_count'], 1)
        self.assertEqual(result['snapshots_persisted'], 0)
        self.assertFalse(result['items'])

    async def test_collect_snapshots_is_disabled_by_feature_flag_before_universe_lookup(self) -> None:
        settings.kis_intraday_snapshot_enabled = False
        client = FakeKISIntradayClient()
        service = KISIntradaySnapshotService(client=client)

        with (
            patch.object(service, '_capture_universe', side_effect=AssertionError('should not load universe')),
            patch.object(client, 'fetch_domestic_quote', new=AsyncMock(side_effect=AssertionError('should not call KIS'))),
        ):
            result = await service.collect_snapshots(symbols=['005930'])

        self.assertEqual(result['status'], 'disabled')
        self.assertEqual(result['reason'], 'kis_intraday_snapshot_disabled')
        self.assertEqual(result['requested_symbols'], ['005930'])
        self.assertEqual(result['summary']['matched_symbol_count'], 1)
        self.assertEqual(result['snapshots_persisted'], 0)
        self.assertFalse(result['items'])

    def test_inspect_summarizes_current_rows(self) -> None:
        service = KISIntradaySnapshotService(client=FakeKISIntradayClient())

        with patch(
            'app.market.kis_intraday_snapshots.load_current_kis_intraday_snapshots',
            return_value=[
                {'symbol': '005930', 'availability_status': 'available', 'freshness_status': 'fresh'},
                {'symbol': '000660', 'availability_status': 'partial', 'freshness_status': 'stale'},
            ],
        ):
            result = service.inspect(symbols=['005930', '000660'], limit=10)

        self.assertEqual(result['summary']['entries'], 2)
        self.assertEqual(result['summary']['available_count'], 1)
        self.assertEqual(result['summary']['partial_count'], 1)
        self.assertEqual(result['summary']['fresh_count'], 1)
        self.assertEqual(result['summary']['stale_count'], 1)


if __name__ == '__main__':
    unittest.main()
