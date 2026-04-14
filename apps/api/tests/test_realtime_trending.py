import unittest
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_scanner
from app.brokers.kis.client import KISClient
from app.scanner.realtime_trending import RealtimeTrendingService, parse_source_ids


class FakeKISRankingClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    async def fetch_domestic_volume_rank(self, market_code: str = 'J') -> dict:
        self.calls.append(('volume-rank', {'market_code': market_code}))
        return {
            'output': [
                {
                    'mksc_shrn_iscd': '005930',
                    'hts_kor_isnm': 'Samsung Electronics',
                    'data_rank': '1',
                    'stck_prpr': '61500',
                    'prdy_vrss_sign': '2',
                    'prdy_vrss': '500',
                    'prdy_ctrt': '0.82',
                    'acml_vol': '250000',
                    'acml_tr_pbmn': '15375000000',
                }
            ]
        }

    async def fetch_domestic_fluctuation_rank(self, market_code: str = 'J', count: int = 20) -> dict:
        self.calls.append(('fluctuation', {'market_code': market_code, 'count': count}))
        return {
            'output': [
                {
                    'stck_shrn_iscd': '005930',
                    'hts_kor_isnm': 'Samsung Electronics',
                    'data_rank': '3',
                    'stck_prpr': '61500',
                    'prdy_vrss_sign': '2',
                    'prdy_vrss': '500',
                    'prdy_ctrt': '0.82',
                    'acml_vol': '250000',
                },
                {
                    'stck_shrn_iscd': '000660',
                    'hts_kor_isnm': 'SK hynix',
                    'data_rank': '4',
                    'stck_prpr': '192000',
                    'prdy_vrss_sign': '5',
                    'prdy_vrss': '1200',
                    'prdy_ctrt': '1.50',
                    'acml_vol': '100000',
                },
            ]
        }

    async def fetch_domestic_hts_top_view(self) -> dict:
        self.calls.append(('hts-top-view', {}))
        return {
            'output1': [
                {
                    'mksc_shrn_iscd': '035420',
                    'mrkt_div_cls_code': 'J',
                }
            ]
        }


class RealtimeTrendingServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_realtime_trending_merges_sources_and_normalizes_metrics(self) -> None:
        fake_client = FakeKISRankingClient()
        service = RealtimeTrendingService(client=fake_client, cache_ttl_seconds=0)

        result = await service.get_realtime_trending(
            limit=2,
            market_code='J',
            sources='volume-rank,fluctuation,hts-top-view',
        )

        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['summary']['source_count'], 3)
        self.assertEqual(result['summary']['item_count'], 3)
        self.assertFalse(result['cache']['hit'])

        volume_bucket = result['sources'][0]
        self.assertEqual(volume_bucket['source'], 'volume-rank')
        self.assertEqual(volume_bucket['items'][0]['metric']['name'], 'volume')
        self.assertEqual(volume_bucket['items'][0]['metric']['value'], 250000)

        items_by_symbol = {item['symbol']: item for item in result['items']}
        samsung = items_by_symbol['005930']
        self.assertEqual(samsung['source_count'], 2)
        self.assertEqual([source['source'] for source in samsung['sources']], ['volume-rank', 'fluctuation'])
        self.assertEqual(samsung['best_rank'], 1)

        hynix = items_by_symbol['000660']
        self.assertEqual(hynix['change'], -1200)
        self.assertEqual(hynix['change_pct'], -1.5)

    async def test_realtime_trending_uses_short_cache_for_same_request(self) -> None:
        fake_client = FakeKISRankingClient()
        service = RealtimeTrendingService(client=fake_client, cache_ttl_seconds=30)

        first = await service.get_realtime_trending(limit=1, market_code='J', sources='volume-rank')
        second = await service.get_realtime_trending(limit=1, market_code='J', sources='volume-rank')

        self.assertFalse(first['cache']['hit'])
        self.assertTrue(second['cache']['hit'])
        self.assertEqual([call[0] for call in fake_client.calls], ['volume-rank'])

    async def test_realtime_trending_returns_partial_status_on_one_source_error(self) -> None:
        fake_client = FakeKISRankingClient()
        fake_client.fetch_domestic_hts_top_view = AsyncMock(side_effect=RuntimeError('upstream down'))
        service = RealtimeTrendingService(client=fake_client, cache_ttl_seconds=0)

        result = await service.get_realtime_trending(
            limit=1,
            market_code='J',
            sources='volume-rank,hts-top-view',
        )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['summary']['ok_source_count'], 1)
        self.assertEqual(result['summary']['error_source_count'], 1)
        self.assertEqual(result['sources'][1]['error'], 'kis_source_request_failed')

    async def test_realtime_trending_uses_row_market_label_when_source_market_differs(self) -> None:
        fake_client = FakeKISRankingClient()
        service = RealtimeTrendingService(client=fake_client, cache_ttl_seconds=0)

        result = await service.get_realtime_trending(limit=1, market_code='NX', sources='hts-top-view')

        item = result['items'][0]
        self.assertEqual(item['market_code'], 'J')
        self.assertEqual(item['market'], 'KRX')
        self.assertEqual(item['sources'][0]['market'], 'KRX')

    async def test_realtime_trending_rejects_unknown_market_code(self) -> None:
        service = RealtimeTrendingService(client=FakeKISRankingClient(), cache_ttl_seconds=0)

        with self.assertRaisesRegex(ValueError, 'market_code'):
            await service.get_realtime_trending(limit=1, market_code='UN', sources='volume-rank')


class RealtimeTrendingParserTests(unittest.TestCase):
    def test_parse_source_ids_deduplicates_and_rejects_unknown_sources(self) -> None:
        self.assertEqual(parse_source_ids('volume-rank,fluctuation,volume-rank'), ['volume-rank', 'fluctuation'])

        with self.assertRaisesRegex(ValueError, 'sources'):
            parse_source_ids('volume-rank,market-cap')


class KISRankingClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_kis_client_uses_expected_ranking_paths_and_tr_ids(self) -> None:
        client = KISClient()
        mock_get = AsyncMock(return_value={'ok': True})

        with patch.object(client, '_get', new=mock_get):
            await client.fetch_domestic_volume_rank(market_code='NX')
            await client.fetch_domestic_fluctuation_rank(market_code='J', count=7)
            await client.fetch_domestic_hts_top_view()

        volume_call, fluctuation_call, hts_call = mock_get.await_args_list
        self.assertEqual(volume_call.args[0], '/uapi/domestic-stock/v1/quotations/volume-rank')
        self.assertEqual(volume_call.kwargs['tr_id'], 'FHPST01710000')
        self.assertEqual(volume_call.kwargs['params']['FID_COND_MRKT_DIV_CODE'], 'NX')
        self.assertEqual(volume_call.kwargs['params']['FID_TRGT_EXLS_CLS_CODE'], '000000')

        self.assertEqual(fluctuation_call.args[0], '/uapi/domestic-stock/v1/ranking/fluctuation')
        self.assertEqual(fluctuation_call.kwargs['tr_id'], 'FHPST01700000')
        self.assertEqual(fluctuation_call.kwargs['params']['fid_input_cnt_1'], '7')

        self.assertEqual(hts_call.args[0], '/uapi/domestic-stock/v1/ranking/hts-top-view')
        self.assertEqual(hts_call.kwargs['tr_id'], 'HHMCM000100C0')
        self.assertEqual(hts_call.kwargs['params'], {})


class FakeRealtimeTrendingService:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[dict] = []

    async def get_realtime_trending(self, **kwargs) -> dict:
        self.calls.append(kwargs)
        if self.fail:
            raise ValueError('bad sources')
        return {'status': 'ok', 'received': kwargs}


class RealtimeTrendingRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_service = routes_scanner.realtime_trending_service
        self.app = FastAPI()
        self.app.include_router(routes_scanner.router)
        self.client = TestClient(self.app)

    def tearDown(self) -> None:
        routes_scanner.realtime_trending_service = self.original_service

    def test_realtime_trending_route_calls_service(self) -> None:
        fake_service = FakeRealtimeTrendingService()
        routes_scanner.realtime_trending_service = fake_service

        response = self.client.get(
            '/scanner/realtime-trending',
            params={
                'limit': '7',
                'market_code': 'J',
                'sources': 'volume-rank,hts-top-view',
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            fake_service.calls,
            [{'limit': 7, 'market_code': 'J', 'sources': 'volume-rank,hts-top-view'}],
        )

    def test_realtime_trending_route_maps_service_validation_error_to_400(self) -> None:
        routes_scanner.realtime_trending_service = FakeRealtimeTrendingService(fail=True)

        response = self.client.get('/scanner/realtime-trending')

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()['detail'], 'bad sources')


if __name__ == '__main__':
    unittest.main()
