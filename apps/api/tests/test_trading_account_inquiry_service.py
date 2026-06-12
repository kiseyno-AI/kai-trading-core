import unittest
from decimal import Decimal

from app.orders.account_inquiry_service import (
    TradingAccountInquiryService,
    TradingAccountInquiryUpstreamError,
    TradingAccountInquiryValidationError,
)
from app.brokers.kis.client import KISConfigurationError


class FakeKISAccountClient:
    def __init__(self) -> None:
        self.balance_calls: list[dict] = []
        self.asset_summary_calls: list[dict] = []
        self.buying_power_calls: list[dict] = []
        self.balance_payload = {
            'output1': [
                {
                    'pdno': '005930',
                    'prdt_name': 'Samsung Electronics',
                    'hldg_qty': '10',
                    'ord_psbl_qty': '8',
                    'pchs_avg_pric': '55000',
                    'pchs_amt': '550000',
                    'prpr': '57200',
                    'evlu_amt': '572000',
                    'evlu_pfls_amt': '22000',
                    'evlu_pfls_rt': '4.00',
                },
                {
                    'pdno': '000660',
                    'prdt_name': 'SK hynix',
                    'hldg_qty': '0',
                    'ord_psbl_qty': '0',
                    'pchs_avg_pric': '180000',
                    'pchs_amt': '0',
                    'prpr': '181000',
                    'evlu_amt': '0',
                    'evlu_pfls_amt': '0',
                    'evlu_pfls_rt': '0',
                },
            ],
            'output2': {
                'dnca_tot_amt': '2500000',
                'thdt_buy_amt': '100000',
                'thdt_sll_amt': '50000',
                'scts_evlu_amt': '572000',
                'tot_evlu_amt': '3072000',
                'pchs_amt_smtl_amt': '550000',
                'evlu_pfls_smtl_amt': '22000',
                'prvs_rcdl_excc_amt': '2480000',
            },
            'raw_pages': [],
            'page_count': 1,
            'max_pages_reached': False,
        }
        self.asset_summary_payload = {
            'output1': [{'asset_type': 'domestic_stock'}],
            'output2': {
                'dncl_amt': '2400000',
                'tot_dncl_amt': '2450000',
                'tot_asst_amt': '3172000',
                'pchs_amt_smtl': '550000',
                'evlu_pfls_amt_smtl': '22000',
            },
        }
        self.buying_power_payload = {
            'output': {
                'ord_psbl_cash': '970000',
                'nrcvb_buy_amt': '969000',
                'nrcvb_buy_qty': '17',
                'max_buy_amt': '1292000',
                'max_buy_qty': '22',
            }
        }
        self.balance_exception: Exception | None = None

    async def fetch_domestic_account_balance(self, **kwargs):
        self.balance_calls.append(kwargs)
        if self.balance_exception is not None:
            raise self.balance_exception
        return self.balance_payload

    async def fetch_domestic_account_asset_summary(self, **kwargs):
        self.asset_summary_calls.append(kwargs)
        return self.asset_summary_payload

    async def fetch_domestic_buying_power(self, **kwargs):
        self.buying_power_calls.append(kwargs)
        return self.buying_power_payload


class TradingAccountInquiryServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.client = FakeKISAccountClient()
        self.service = TradingAccountInquiryService(client=self.client)
        self.account_reference = 'KIS:12345678-01'

    async def test_get_account_summary_normalizes_balance_and_asset_totals(self) -> None:
        result = await self.service.get_account_summary(account_reference=self.account_reference)

        self.assertEqual(result['cash']['settled_cash'], 2500000)
        self.assertEqual(result['cash']['withdrawable_cash'], 2400000)
        self.assertEqual(result['valuation']['securities_value'], 572000)
        self.assertEqual(result['valuation']['total_asset_value'], 3072000)
        self.assertAlmostEqual(result['valuation']['pnl_pct'], 0.04)
        self.assertEqual(result['valuation']['estimated_d2_deposit'], 2480000)
        self.assertEqual(result['raw']['source_api'], 'inquire-balance|inquire-account-balance')

    async def test_get_account_summary_parses_day_pnl_from_asset_summary(self) -> None:
        self.client.asset_summary_payload['output2']['asst_icdc_amt'] = '20366'
        self.client.asset_summary_payload['output2']['asst_icdc_erng_rt'] = '0.66'

        result = await self.service.get_account_summary(account_reference=self.account_reference)

        self.assertEqual(result['valuation']['day_pnl_amount'], 20366)
        self.assertAlmostEqual(result['valuation']['day_pnl_pct'], 0.0066)

    async def test_get_account_summary_falls_back_to_balance_day_pnl_field(self) -> None:
        self.client.balance_payload['output2']['asst_icdc_amt'] = '20366'
        self.client.balance_payload['output2']['asst_icdc_erng_rt'] = '0.66'

        result = await self.service.get_account_summary(account_reference=self.account_reference)

        self.assertEqual(result['valuation']['day_pnl_amount'], 20366)
        self.assertAlmostEqual(result['valuation']['day_pnl_pct'], 0.0066)

    async def test_get_positions_filters_zero_rows_by_default_and_derives_weights(self) -> None:
        result = await self.service.get_positions(account_reference=self.account_reference)

        self.assertEqual(result['count'], 1)
        self.assertEqual(result['items'][0]['symbol'], '005930')
        self.assertEqual(result['items'][0]['quantity'], 10)
        self.assertAlmostEqual(result['items'][0]['pnl_pct'], 0.04)
        self.assertAlmostEqual(result['items'][0]['weight_pct'], 572000 / 3072000)

        filtered = await self.service.get_positions(
            account_reference=self.account_reference,
            symbol='000660',
            include_zero=True,
        )
        self.assertEqual(filtered['count'], 1)
        self.assertEqual(filtered['items'][0]['symbol'], '000660')
        self.assertEqual(filtered['items'][0]['quantity'], 0)

    async def test_get_buying_power_uses_symbol_price_and_market_division(self) -> None:
        result = await self.service.get_buying_power(
            account_reference=self.account_reference,
            symbol='005930',
            price=Decimal('57000'),
            order_type='market',
            include_cma=True,
            include_overseas=False,
        )

        self.assertEqual(result['symbol'], '005930')
        self.assertEqual(result['buying_power']['buyable_quantity'], 17)
        self.assertEqual(result['buying_power']['max_buy_amount'], 1292000)
        self.assertTrue(result['buying_power']['cash_limited'])
        self.assertEqual(self.client.buying_power_calls[0]['order_division_code'], '01')
        self.assertEqual(self.client.buying_power_calls[0]['include_cma_evaluation'], 'Y')

    async def test_get_buying_power_rejects_non_buy_side(self) -> None:
        with self.assertRaises(TradingAccountInquiryValidationError):
            await self.service.get_buying_power(
                account_reference=self.account_reference,
                symbol='005930',
                price='57000',
                order_type='limit',
                side='sell',
            )

    async def test_invalid_account_reference_and_upstream_configuration_errors_are_mapped(self) -> None:
        with self.assertRaises(TradingAccountInquiryValidationError):
            await self.service.get_account_summary(account_reference='bad-reference')

        self.client.balance_exception = KISConfigurationError('KIS credentials must be configured')
        with self.assertRaises(TradingAccountInquiryUpstreamError) as ctx:
            await self.service.get_account_summary(account_reference=self.account_reference)

        self.assertIn('configured', str(ctx.exception))


if __name__ == '__main__':
    unittest.main()
