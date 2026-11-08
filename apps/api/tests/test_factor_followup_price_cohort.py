"""Price-only factors must not shrink to the DART/Piotroski cohort.

2026-11-04: the DART refresh succeeded for 82 of 2,769 symbols, Piotroski therefore
admitted 82, and every factor stage (including the price-only ones) ran on those 82.
"""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import ExitStack
from unittest import TestCase
from unittest.mock import patch

from app.eod.market_membership import market_membership_sha256
from app.eod.pykrx_pipeline_runner import (
    FACTOR_FOLLOWUP_STAGE_ORDER,
    NON_DART_FACTOR_FOLLOWUP_STAGE_IDS,
    _run_daily_factor_followup,
)
from app.eod.scope_outcomes import build_exact_symbol_scope_outcome

MARKET_DATE = '2026-11-04'
UNIVERSE = [f'{index:06d}' for index in range(1, 101)]
DART_COHORT = UNIVERSE[:40]

PATCH_TARGETS = {
    'momentum_factors': 'app.signals.momentum_factor.daily_momentum_factor_service.rebuild_for_market_date',
    'value_factors': 'app.signals.value_factor.daily_value_factor_service.rebuild_for_market_date',
    'low_risk_factors': 'app.signals.low_risk_factor.daily_low_risk_factor_service.rebuild_for_market_date',
    'beta_factors': 'app.signals.beta_factor.daily_beta_factor_service.rebuild_for_market_date',
    'advanced_value_factors': 'app.signals.advanced_value_factor.daily_advanced_value_factor_service.rebuild_for_market_date',
    'unified_value_factors': 'app.signals.unified_value_factor.daily_unified_value_factor_service.rebuild_for_market_date',
    'quality_factors': 'app.signals.quality_factor.daily_quality_factor_service.rebuild_for_market_date',
    'composite_factors': 'app.signals.composite_factor.daily_composite_factor_service.rebuild_for_market_date',
}


def _scope_finalization(stage_id: str, exact: Sequence[str], successful: Sequence[str]) -> dict:
    return {
        'stage': stage_id,
        'market_date': MARKET_DATE,
        'exact_symbol_count': len(exact),
        'exact_membership_sha256': market_membership_sha256(exact),
        'successful_symbol_count': len(successful),
        'successful_membership_sha256': market_membership_sha256(successful),
        'deleted_stale_row_count': 0,
        'ready': True,
    }


def _run(*, admitted, price_admitted=..., failing: dict[str, set[str]] | None = None):
    """Run the follow-up with fake services; returns (result, kwargs-by-stage)."""

    failing = failing or {}
    calls: dict[str, dict] = {}

    def stage_result(stage_id: str, **kwargs):
        calls[stage_id] = kwargs
        selected = list(kwargs['symbols'])
        debt = [s for s in selected if s in failing.get(stage_id, set())]
        successful = [s for s in selected if s not in set(debt)]
        return {
            'status': 'partial' if debt else 'completed',
            'gate_status': 'ready',
            'results_persisted': len(successful),
            'results_failed': len(debt),
            'scope_finalization': _scope_finalization(stage_id, UNIVERSE, successful),
            **build_exact_symbol_scope_outcome(
                manifest_version='factor_scope_outcomes_v1',
                selected_symbols=selected,
                successful_symbols=successful,
                retry_debt_symbols=debt,
            ),
        }

    kwargs = {
        'market_date': MARKET_DATE,
        'exact_membership_symbols': UNIVERSE,
        'admitted_symbols': admitted,
    }
    if price_admitted is not ...:
        kwargs['price_admitted_symbols'] = price_admitted
    with ExitStack() as stack:
        for stage_id, target in PATCH_TARGETS.items():
            stack.enter_context(
                patch(
                    target,
                    side_effect=lambda stage_id=stage_id, **kw: stage_result(stage_id, **kw),
                )
            )
        result = _run_daily_factor_followup(**kwargs)
    return result, calls


class FactorFollowupPriceCohortTests(TestCase):
    def test_the_non_dart_stage_set_is_exactly_the_price_and_pykrx_factors(self) -> None:
        self.assertEqual(
            NON_DART_FACTOR_FOLLOWUP_STAGE_IDS,
            {'momentum_factors', 'value_factors', 'low_risk_factors', 'beta_factors'},
        )
        self.assertTrue(NON_DART_FACTOR_FOLLOWUP_STAGE_IDS <= set(FACTOR_FOLLOWUP_STAGE_ORDER))

    def test_price_only_factors_use_the_full_price_cohort_while_dart_stages_keep_the_dart_cohort(self) -> None:
        result, calls = _run(admitted=DART_COHORT, price_admitted=UNIVERSE)

        for stage_id in NON_DART_FACTOR_FOLLOWUP_STAGE_IDS:
            self.assertEqual(calls[stage_id]['symbols'], UNIVERSE, stage_id)
        for stage_id in ('advanced_value_factors', 'unified_value_factors', 'quality_factors', 'composite_factors'):
            self.assertEqual(calls[stage_id]['symbols'], DART_COHORT, stage_id)
        for stage_id in PATCH_TARGETS:
            self.assertEqual(calls[stage_id]['ranking_symbols'], UNIVERSE, stage_id)
            self.assertEqual(calls[stage_id]['exact_scope_symbols'], UNIVERSE, stage_id)
        # The aggregate cohort handed downstream is still the DART/Piotroski one.
        self.assertEqual(result['selected_stock_codes'], DART_COHORT)
        self.assertEqual(result['successful_stock_codes'], DART_COHORT)
        self.assertEqual(result['retry_debt_stock_codes'], [])

    def test_without_a_price_cohort_every_stage_keeps_the_admitted_cohort(self) -> None:
        _result, calls = _run(admitted=DART_COHORT)

        for stage_id in PATCH_TARGETS:
            self.assertEqual(calls[stage_id]['symbols'], DART_COHORT, stage_id)

    def test_the_price_cohort_never_excludes_a_dart_admitted_symbol(self) -> None:
        _result, calls = _run(admitted=DART_COHORT, price_admitted=UNIVERSE[:10])

        for stage_id in NON_DART_FACTOR_FOLLOWUP_STAGE_IDS:
            self.assertEqual(calls[stage_id]['symbols'], DART_COHORT, stage_id)

    def test_a_failure_outside_the_dart_cohort_does_not_become_downstream_retry_debt(self) -> None:
        outside_symbol = UNIVERSE[90]
        result, calls = _run(
            admitted=DART_COHORT,
            price_admitted=UNIVERSE,
            failing={'momentum_factors': {outside_symbol}},
        )

        self.assertEqual(result['status'], 'partial')  # the momentum stage itself is partial
        self.assertEqual(result['retry_debt_stock_codes'], [])
        self.assertEqual(result['successful_stock_codes'], DART_COHORT)
        self.assertEqual(calls['composite_factors']['symbols'], DART_COHORT)

    def test_a_failure_inside_the_dart_cohort_still_blocks_composite_for_that_symbol(self) -> None:
        failing_symbol = DART_COHORT[5]
        result, calls = _run(
            admitted=DART_COHORT,
            price_admitted=UNIVERSE,
            failing={'momentum_factors': {failing_symbol}},
        )

        self.assertNotIn(failing_symbol, calls['composite_factors']['symbols'])
        self.assertEqual(result['retry_debt_stock_codes'], [failing_symbol])

    def test_a_price_cohort_outside_the_exact_membership_fails_closed(self) -> None:
        with self.assertRaisesRegex(RuntimeError, 'price-admitted scope falls outside exact EOD membership'):
            _run(admitted=DART_COHORT, price_admitted=[*UNIVERSE[:5], '999999'])

    def test_a_malformed_price_cohort_fails_closed(self) -> None:
        with self.assertRaisesRegex(RuntimeError, 'price-admitted scope contains a malformed KRX short code'):
            _run(admitted=DART_COHORT, price_admitted=[*UNIVERSE[:5], 'NOT-A-CODE'])
