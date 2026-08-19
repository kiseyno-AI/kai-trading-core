import unittest

from app.us_equities.universe import (
    USListedSecurity,
    filter_us_equity_universe,
    parse_nasdaq_listed,
    parse_other_listed,
)


class USListedSecurityParserTests(unittest.TestCase):
    def test_parses_nasdaq_and_ignores_footer(self):
        payload = '''Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares
AAPL|Apple Inc. - Common Stock|Q|N|N|100|N|N
QQQ|Invesco QQQ Trust|Q|N|N|100|Y|N
File Creation Time: 0819202617:00|||||||
'''

        rows = parse_nasdaq_listed(payload)

        self.assertEqual([row.symbol for row in rows], ['AAPL', 'QQQ'])
        self.assertEqual(rows[0].exchange, 'NASDAQ')
        self.assertFalse(rows[0].is_etf)
        self.assertTrue(rows[1].is_etf)

    def test_parses_other_exchange_codes(self):
        payload = '''ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol
IBM|International Business Machines Corporation Common Stock |N|IBM|N|100|N|IBM
SPY|SPDR S&P 500 ETF Trust|P|SPY|Y|100|N|SPY
File Creation Time: 0819202617:00|||||||
'''

        rows = parse_other_listed(payload)

        self.assertEqual(rows[0].exchange, 'NYSE')
        self.assertEqual(rows[1].exchange, 'NYSE_ARCA')


class USEquityUniverseFilterTests(unittest.TestCase):
    def test_returns_deterministic_eligible_rows_and_explicit_exclusions(self):
        securities = [
            USListedSecurity('TEST', 'Test Common Stock', 'NASDAQ', False, True, 'N'),
            USListedSecurity('QQQ', 'Invesco QQQ Trust', 'NASDAQ', True, False, 'N'),
            USListedSecurity('BAD', 'Bad Corp Common Stock', 'NASDAQ', False, False, 'D'),
            USListedSecurity('WXYZW', 'Example Corp - Warrant', 'NASDAQ', False, False, 'N'),
            USListedSecurity('ADR', 'Example ADR American Depositary Shares', 'NYSE', False, False, 'N'),
            USListedSecurity('AAPL', 'Apple Inc. - Common Stock', 'NASDAQ', False, False, 'N'),
            USListedSecurity('IBM', 'International Business Machines Corporation Common Stock', 'NYSE', False, False, None),
        ]

        result = filter_us_equity_universe(reversed(securities))

        self.assertEqual([item.symbol for item in result.eligible], ['AAPL', 'IBM'])
        self.assertEqual(result.exclusions['TEST'], ('test_issue',))
        self.assertEqual(result.exclusions['QQQ'], ('etf',))
        self.assertEqual(result.exclusions['BAD'], ('financial_status_D',))
        self.assertEqual(result.exclusions['WXYZW'], ('unsupported_security_type_warrant',))
        self.assertEqual(result.exclusions['ADR'], ('adr',))

    def test_duplicate_symbol_prefers_supported_primary_exchange_deterministically(self):
        rows = [
            USListedSecurity('DUPE', 'Dupe Common Stock', 'NYSE_ARCA', False, False, None),
            USListedSecurity('DUPE', 'Dupe Inc. Common Stock', 'NASDAQ', False, False, 'N'),
        ]

        result = filter_us_equity_universe(rows)

        self.assertEqual(len(result.eligible), 1)
        self.assertEqual(result.eligible[0].exchange, 'NASDAQ')


if __name__ == '__main__':
    unittest.main()
