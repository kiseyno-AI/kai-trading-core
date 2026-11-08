"""One bad item in an agent batch fails only that item, unless attribution is ambiguous."""

from __future__ import annotations

import unittest
from app.agent_context.orchestrator_runtime_helpers import salvageable_batch_item_failures

SYMBOLS = ['005930', '000660', '035720']


def _result(**correlation):
    base = {
        'missing_symbols': [],
        'unexpected_symbols': [],
        'duplicate_symbols': [],
        'item_symbol_mismatches': [],
        'structure_mismatch_flags': [],
    }
    base.update(correlation)
    return {
        'response_correlation': base,
        'decoded': {'items': [{'symbol': s, 'feedback': {}} for s in SYMBOLS]},
    }


class SalvageRuleTests(unittest.TestCase):
    def test_missing_symbols_fail_alone(self):
        failures = salvageable_batch_item_failures(
            batch_result=_result(missing_symbols=['000660']),
            batch_symbols=SYMBOLS,
            failure_class='batch_response_attribution_mismatch',
        )
        self.assertEqual(list(failures), ['000660'])
        self.assertEqual(failures['000660']['failure'], 'batch_response_attribution_mismatch')

    def test_an_item_without_a_feedback_object_fails_alone(self):
        failures = salvageable_batch_item_failures(
            batch_result=_result(structure_mismatch_flags=['item_feedback_missing:2']),
            batch_symbols=SYMBOLS,
            failure_class='batch_response_structure_mismatch',
        )
        self.assertEqual(list(failures), ['035720'])

    def test_ambiguous_attribution_still_fails_the_whole_batch(self):
        for correlation in (
            {'unexpected_symbols': ['999999'], 'missing_symbols': ['000660']},
            {'duplicate_symbols': ['005930'], 'missing_symbols': ['000660']},
            {'item_symbol_mismatches': [{'item_index': 0}], 'missing_symbols': ['000660']},
            {'structure_mismatch_flags': ['batch_envelope_wrapped']},
            {'structure_mismatch_flags': ['item_feedback_missing:9']},
            {},  # nothing attributable to an item
            {'missing_symbols': SYMBOLS},  # every item failed anyway
        ):
            with self.subTest(correlation=correlation):
                self.assertIsNone(
                    salvageable_batch_item_failures(
                        batch_result=_result(**correlation),
                        batch_symbols=SYMBOLS,
                        failure_class='batch_response_attribution_mismatch',
                    )
                )

    def test_other_batch_failures_are_never_salvaged(self):
        self.assertIsNone(
            salvageable_batch_item_failures(
                batch_result=_result(missing_symbols=['000660']),
                batch_symbols=SYMBOLS,
                failure_class='response_identity_mismatch',
            )
        )


if __name__ == '__main__':
    unittest.main()
