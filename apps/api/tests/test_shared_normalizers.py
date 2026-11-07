import json
import math
import unittest
from decimal import Decimal

from app.agent_context import payload_builder, signal_decider_loop_service
from app.core.numbers import finite_float
from app.external_news import service as external_news_service
from app.market.symbol_codes import (
    normalize_symbol_scope,
    normalize_symbol_text,
    split_symbol_list,
)
from app.signals import momentum_factor, value_factor


class FiniteFloatTests(unittest.TestCase):
    def test_only_real_finite_numbers_survive(self):
        self.assertEqual(finite_float(3), 3.0)
        self.assertEqual(finite_float(Decimal('12.5')), 12.5)
        self.assertEqual(finite_float(' 1,234.5 '), 1234.5)
        for value in (None, '', '  ', 'n/a', True, False, float('nan'), float('inf'), Decimal('NaN'), Decimal('sNaN'), object()):
            with self.subTest(value=value):
                self.assertIsNone(finite_float(value))

    def test_nan_from_a_stored_metric_cannot_reach_agent_json(self):
        # Before: payload_builder returned float('nan') and json.dumps wrote
        # the invalid token NaN into the agent request.
        value = payload_builder._to_float(float('nan'))
        self.assertIsNone(value)
        self.assertEqual(json.loads(json.dumps({'metric': value})), {'metric': None})
        self.assertFalse(math.isnan(payload_builder._to_float(Decimal('1.5'))))


class SymbolNormalizationTests(unittest.TestCase):
    def test_one_rule_for_krx_and_other_symbols(self):
        self.assertEqual(normalize_symbol_text('5930'), '005930')
        self.assertEqual(normalize_symbol_text(' 45226k '), '45226K')
        self.assertEqual(normalize_symbol_text('aapl'), 'AAPL')
        self.assertEqual(normalize_symbol_text(None), '')

    def test_scope_keeps_none_and_rejects_blank_members(self):
        self.assertIsNone(normalize_symbol_scope(None))
        self.assertEqual(normalize_symbol_scope(['5930', '005930', '000660']), ['005930', '000660'])
        with self.assertRaises(ValueError):
            normalize_symbol_scope(['005930', ' '])

    def test_comma_lists_skip_blanks_and_duplicates(self):
        self.assertEqual(split_symbol_list('5930, 000660,,005930'), ['005930', '000660'])
        self.assertEqual(split_symbol_list(['005930,000660', '35420']), ['005930', '000660', '035420'])
        self.assertEqual(split_symbol_list(None), [])

    def test_agent_context_and_factor_paths_agree_on_a_short_code(self):
        # Before: factor stages padded '5930' to '005930' while the decider
        # loop and news kept '5930', so the same request matched no rows.
        factor_scope = momentum_factor._normalize_symbols(['5930'])
        self.assertEqual(factor_scope, value_factor._normalize_symbols(['5930']))
        self.assertEqual(signal_decider_loop_service._normalize_symbols(['5930']), factor_scope)
        self.assertEqual(external_news_service._normalize_symbols('5930'), factor_scope)


if __name__ == '__main__':
    unittest.main()
