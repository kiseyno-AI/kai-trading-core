import unittest

from app.us_equities.kis_market_codes import (
    KISUSExchange,
    from_kis_order_exchange,
    to_kis_order_exchange,
    to_kis_price_exchange,
    to_kis_product_type_code,
)


class KISUSMarketCodeTests(unittest.TestCase):
    def test_maps_each_supported_us_exchange_for_price_order_and_product_calls(self):
        self.assertEqual(to_kis_price_exchange(KISUSExchange.NASDAQ), 'NAS')
        self.assertEqual(to_kis_order_exchange(KISUSExchange.NASDAQ), 'NASD')
        self.assertEqual(to_kis_product_type_code(KISUSExchange.NASDAQ), '512')
        self.assertEqual(to_kis_price_exchange('NYSE'), 'NYS')
        self.assertEqual(to_kis_order_exchange('NYSE'), 'NYSE')
        self.assertEqual(to_kis_product_type_code('NYSE_AMERICAN'), '529')

    def test_rejects_non_us_exchange(self):
        with self.assertRaises(ValueError):
            to_kis_order_exchange('NYSE_ARCA')

    def test_maps_order_code_back_to_canonical_exchange(self):
        self.assertEqual(from_kis_order_exchange('AMEX'), KISUSExchange.NYSE_AMERICAN)


if __name__ == '__main__':
    unittest.main()
