import unittest

from app.core.settings import settings
from app.fx.service import KoreaEximRateNotFoundError, KoreaEximService


class FakeFXClient:
    def __init__(self):
        self.calls = []

    async def fetch_exchange_rates(self, *, search_date, data='AP01'):
        self.calls.append({'search_date': search_date, 'data': data})
        if search_date == '20260406':
            return []
        if search_date == '20260405':
            return [
                {
                    'result': 1,
                    'cur_unit': 'JPY(100)',
                    'cur_nm': '일본 옌',
                    'ttb': '980.10',
                    'tts': '999.90',
                    'deal_bas_r': '990.00',
                    'bkpr': '990',
                    'yy_efee_r': '0',
                    'ten_dd_efee_r': '0',
                    'kftc_bkpr': '990',
                    'kftc_deal_bas_r': '990.00',
                },
                {
                    'result': 1,
                    'cur_unit': 'USD',
                    'cur_nm': '미국 달러',
                    'ttb': '1,430.10',
                    'tts': '1,458.90',
                    'deal_bas_r': '1,444.50',
                    'bkpr': '1,444',
                    'yy_efee_r': '0',
                    'ten_dd_efee_r': '0',
                    'kftc_bkpr': '1,444',
                    'kftc_deal_bas_r': '1,444.50',
                },
            ]
        return []


class KoreaEximServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_exchange_rates_applies_fallback_and_normalizes_rates(self):
        service = KoreaEximService(client=FakeFXClient())

        result = await service.get_exchange_rates(search_date='20260406', fallback_days=1, currency='usd')

        self.assertEqual(result['requested_date'], '20260406')
        self.assertEqual(result['effective_date'], '20260405')
        self.assertTrue(result['fallback_applied'])
        self.assertEqual(result['currency'], 'USD')
        self.assertEqual(result['count'], 1)
        self.assertEqual(result['items'][0]['currency_code'], 'USD')
        self.assertEqual(result['items'][0]['exchange_rate'], 1444.5)

    async def test_get_exchange_rates_uses_configured_fallback_days_by_default(self):
        original_fallback_days = settings.koreaexim_fallback_days
        settings.koreaexim_fallback_days = 1
        client = FakeFXClient()
        service = KoreaEximService(client=client)
        try:
            await service.get_exchange_rates(search_date='20260406')
        finally:
            settings.koreaexim_fallback_days = original_fallback_days

        self.assertEqual([call['search_date'] for call in client.calls], ['20260406', '20260405'])

    async def test_get_exchange_rates_raises_when_no_data_found(self):
        service = KoreaEximService(client=FakeFXClient())

        with self.assertRaises(KoreaEximRateNotFoundError):
            await service.get_exchange_rates(search_date='20260406', currency='EUR', fallback_days=1)


if __name__ == '__main__':
    unittest.main()
