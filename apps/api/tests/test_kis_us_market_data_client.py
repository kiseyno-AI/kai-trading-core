import unittest
from unittest.mock import AsyncMock, patch

import httpx

from app.brokers.kis.client import KISAPIError, KISClient, KISClientError, KISTimeoutError
from app.core.settings import settings


class CapturingKISClient(KISClient):
    def __init__(self, response=None):
        super().__init__(use_mock=True)
        self.calls = []
        self.response = response if response is not None else {'output': {'last': '220.10'}}

    async def _get_response(self, path, *, tr_id, params, tr_cont=''):
        self.calls.append((path, tr_id, params))
        return self.response, ''


class PaginatedKISClient(KISClient):
    def __init__(self):
        super().__init__(use_mock=True)
        self.calls = []
        self.responses = [
            ({'output2': [{'symb': 'AAPL'}], 'keyb': 'NEXT'}, 'M'),
            ({'output2': [{'symb': 'MSFT'}], 'keyb': ''}, ''),
        ]

    async def _get_response(self, path, *, tr_id, params, tr_cont=''):
        self.calls.append((path, tr_id, dict(params), tr_cont))
        return self.responses.pop(0)


class KISUSMarketDataClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetches_us_quote_with_price_exchange_code(self):
        client = CapturingKISClient()

        await client.fetch_overseas_quote(exchange='NASDAQ', symbol='AAPL')

        self.assertEqual(client.calls, [(
            '/uapi/overseas-price/v1/quotations/price',
            'HHDFS00000300',
            {'AUTH': '', 'EXCD': 'NAS', 'SYMB': 'AAPL'},
        )])

    async def test_fetches_adjusted_daily_history(self):
        client = CapturingKISClient()

        await client.fetch_overseas_daily_prices(
            exchange='NYSE', symbol='BRK/B', as_of_date='20260819', adjusted=True
        )

        self.assertEqual(client.calls[0][0], '/uapi/overseas-price/v1/quotations/dailyprice')
        self.assertEqual(client.calls[0][1], 'HHDFS76240000')
        self.assertEqual(client.calls[0][2], {
            'AUTH': '', 'EXCD': 'NYS', 'SYMB': 'BRK/B', 'GUBN': '0', 'BYMD': '20260819', 'MODP': '1'
        })

    async def test_fetches_index_daily_history_without_changing_provider_rows(self):
        response = {
            'rt_cd': '0',
            'output1': {'stck_shrn_iscd': 'SPX'},
            'output2': [
                {'stck_bsop_date': '20261124', 'ovrs_nmix_prpr': '7200.00'},
                {'stck_bsop_date': '20261123', 'ovrs_nmix_prpr': '7100.00'},
            ],
        }
        client = CapturingKISClient(response)

        result = await client.fetch_overseas_index_daily_prices(
            index_code=' spx ', start_date='20261120', end_date='20261124'
        )

        self.assertEqual(client.calls, [(
            '/uapi/overseas-price/v1/quotations/inquire-daily-chartprice',
            'FHKST03030100',
            {
                'FID_COND_MRKT_DIV_CODE': 'N',
                'FID_INPUT_ISCD': 'SPX',
                'FID_INPUT_DATE_1': '20261120',
                'FID_INPUT_DATE_2': '20261124',
                'FID_PERIOD_DIV_CODE': 'D',
            },
        )])
        self.assertEqual(result, response)

    async def test_index_history_keeps_an_empty_provider_result_empty(self):
        response = {'rt_cd': '0', 'output1': {}, 'output2': []}
        client = CapturingKISClient(response)

        result = await client.fetch_overseas_index_daily_prices(
            index_code='COMP', start_date='20261123', end_date='20261123'
        )

        self.assertEqual(result, response)

    async def test_invalid_index_history_inputs_do_not_call_provider(self):
        for overrides in (
            {'index_code': '   '},
            {'start_date': ''},
            {'start_date': '2026112'},
            {'start_date': '2026-11-23'},
            {'start_date': '20260229'},
            {'end_date': '20261131'},
            {'start_date': '20261124', 'end_date': '20261123'},
        ):
            with self.subTest(overrides=overrides):
                client = CapturingKISClient()
                arguments = {
                    'index_code': 'SPX', 'start_date': '20261120', 'end_date': '20261124',
                    **overrides,
                }
                with self.assertRaises(ValueError):
                    await client.fetch_overseas_index_daily_prices(**arguments)
                self.assertEqual(client.calls, [])

    async def test_index_history_rejects_incomplete_pages_without_extra_requests(self):
        for continuation in ('M', 'F'):
            with self.subTest(continuation=continuation):
                client = KISClient(use_mock=True)
                response = httpx.Response(200, headers={'tr_cont': continuation}, json={
                    'rt_cd': '0',
                    'output2': [{'stck_bsop_date': '20261123', 'ovrs_nmix_prpr': '7100.00'}],
                })
                with (
                    patch.dict(settings.__dict__, {'kis_app_key': 'test-key', 'kis_app_secret': 'test-secret'}),
                    patch.object(client, 'issue_access_token', AsyncMock(return_value={'access_token': 'test-token'})),
                    patch('app.brokers.kis.client.request_with_audit', AsyncMock(return_value=response)) as request,
                ):
                    with self.assertRaisesRegex(KISClientError, 'incomplete'):
                        await client.fetch_overseas_index_daily_prices(
                            index_code='SPX', start_date='20200101', end_date='20261124'
                        )
                self.assertEqual(request.await_count, 1)

    async def test_index_history_preserves_kis_rejection(self):
        client = KISClient(use_mock=True)
        response = httpx.Response(200, json={
            'rt_cd': '1', 'msg_cd': 'TEST_INDEX_REJECTED', 'msg1': 'Index unavailable',
        })
        with (
            patch.dict(settings.__dict__, {'kis_app_key': 'test-key', 'kis_app_secret': 'test-secret'}),
            patch.object(client, 'issue_access_token', AsyncMock(return_value={'access_token': 'test-token'})),
            patch('app.brokers.kis.client.request_with_audit', AsyncMock(return_value=response)) as request,
        ):
            with self.assertRaises(KISAPIError) as failure:
                await client.fetch_overseas_index_daily_prices(
                    index_code='SOX', start_date='20261120', end_date='20261124'
                )
        self.assertEqual(failure.exception.msg_code, 'TEST_INDEX_REJECTED')
        self.assertEqual(request.await_count, 1)

    async def test_index_history_preserves_timeout_without_an_extra_retry(self):
        client = KISClient(use_mock=True)
        with (
            patch.dict(settings.__dict__, {'kis_app_key': 'test-key', 'kis_app_secret': 'test-secret'}),
            patch.object(client, 'issue_access_token', AsyncMock(return_value={'access_token': 'test-token'})),
            patch('app.brokers.kis.client.request_with_audit', AsyncMock(side_effect=httpx.ReadTimeout('test timeout'))) as request,
        ):
            with self.assertRaises(KISTimeoutError) as failure:
                await client.fetch_overseas_index_daily_prices(
                    index_code='SPX', start_date='20261120', end_date='20261124'
                )
        self.assertTrue(failure.exception.retryable)
        self.assertFalse(failure.exception.ambiguous_submission)
        self.assertEqual(request.await_count, 1)

    async def test_market_cap_ranking_paginates_with_keyb(self):
        client = PaginatedKISClient()

        result = await client.fetch_overseas_market_cap_ranking(exchange='NASDAQ', max_pages=3)

        self.assertEqual([item['symb'] for item in result['output2']], ['AAPL', 'MSFT'])
        self.assertEqual(client.calls[0][2]['CURR_GB'], '0')
        self.assertEqual(client.calls[1][2]['KEYB'], 'NEXT')
        self.assertEqual(client.calls[1][3], 'N')
        self.assertEqual(result['page_count'], 2)


if __name__ == '__main__':
    unittest.main()
