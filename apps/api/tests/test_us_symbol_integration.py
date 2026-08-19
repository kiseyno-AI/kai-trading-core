import unittest

from app.integrations.kis_overseas_master.client import KISOverseasMasterSecurity
from app.integrations.sec_edgar.client import SECCompanyTicker
from app.us_equities.symbols import build_us_symbol_universe, canonical_us_symbol
from app.us_equities.universe import USListedSecurity


def _listed(symbol: str, exchange: str = 'NYSE') -> USListedSecurity:
    return USListedSecurity(symbol, f'{symbol} Common Stock', exchange, False, False, 'N')


def _kis(symbol: str, exchange: str = 'NYSE', **overrides) -> KISOverseasMasterSecurity:
    values = {
        'symbol': symbol,
        'realtime_symbol': f'NYS{symbol}',
        'exchange': exchange,
        'korean_name': symbol,
        'english_name': f'{symbol} INC',
        'security_type': '2',
        'currency': 'USD',
        'decimal_places': 4,
        'base_price': '100.0000',
        'bid_order_size': '1',
        'ask_order_size': '1',
        'market_open_hhmm': '930',
        'market_close_hhmm': '1600',
        'is_depositary_receipt': False,
        'industry_code': '013',
        'constituent_flag': '1',
        'etp_type_code': '004',
    }
    values.update(overrides)
    return KISOverseasMasterSecurity(**values)


class USSymbolIntegrationTests(unittest.TestCase):
    def test_normalizes_class_share_separators_across_sources(self):
        self.assertEqual(canonical_us_symbol('BRK/B'), 'BRK.B')
        self.assertEqual(canonical_us_symbol('BRK-B'), 'BRK.B')

        result = build_us_symbol_universe(
            [_listed('BRK.B')],
            [_kis('BRK/B')],
            [SECCompanyTicker('0001067983', 'Berkshire Hathaway', 'BRK-B', 'NYSE')],
        )

        self.assertEqual(len(result.symbols), 1)
        self.assertEqual(result.symbols[0].symbol, 'BRK.B')
        self.assertEqual(result.symbols[0].kis_symbol, 'BRK/B')
        self.assertEqual(result.symbols[0].cik, '0001067983')

    def test_requires_kis_tradability_but_keeps_missing_sec_mapping_explicit(self):
        result = build_us_symbol_universe(
            [_listed('GOOD'), _listed('NO_KIS'), _listed('DR')],
            [_kis('GOOD'), _kis('DR', is_depositary_receipt=True)],
            [],
        )

        self.assertEqual([item.symbol for item in result.symbols], ['GOOD'])
        self.assertEqual(result.symbols[0].sec_mapping_status, 'missing')
        self.assertEqual(result.exclusions['NO_KIS'], ('not_supported_by_kis',))
        self.assertEqual(result.exclusions['DR'], ('kis_depositary_receipt',))


if __name__ == '__main__':
    unittest.main()
