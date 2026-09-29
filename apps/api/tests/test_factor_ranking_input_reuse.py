import unittest
from contextlib import ExitStack
from datetime import date
from unittest.mock import call, patch

from app.signals.advanced_value_factor import DailyAdvancedValueFactorService
from app.signals.beta_factor import DailyBetaFactorService
from app.signals.low_risk_factor import DailyLowRiskFactorService
from app.signals.quality_factor import DailyQualityFactorService
from app.signals.unified_value_factor import DailyUnifiedValueFactorService
from app.signals.value_factor import DailyValueFactorService


SERVICE_CASES = (
    (
        DailyValueFactorService,
        'app.signals.value_factor.upsert_daily_value_factor_results',
        None,
    ),
    (
        DailyLowRiskFactorService,
        'app.signals.low_risk_factor.upsert_daily_low_risk_factor_results',
        None,
    ),
    (
        DailyBetaFactorService,
        'app.signals.beta_factor.upsert_daily_beta_factor_results',
        None,
    ),
    (
        DailyAdvancedValueFactorService,
        'app.signals.advanced_value_factor.upsert_daily_advanced_value_factor_results',
        None,
    ),
    (
        DailyUnifiedValueFactorService,
        'app.signals.unified_value_factor.upsert_daily_unified_value_factor_results',
        'app.signals.unified_value_factor.load_symbols',
    ),
    (
        DailyQualityFactorService,
        'app.signals.quality_factor.upsert_daily_quality_factor_results',
        None,
    ),
)


class FactorRankingInputReuseTests(unittest.TestCase):
    market_date = date(2026, 9, 23)
    requested_symbols = ['000001']

    def _rebuild(self, service_class, upsert_target, symbol_loader_target, *, ranking_symbols):
        service = service_class()
        with ExitStack() as stack:
            load_inputs = stack.enter_context(
                patch.object(
                    service,
                    '_load_inputs',
                    return_value=([], self.requested_symbols, []),
                )
            )
            stack.enter_context(patch(upsert_target))
            if symbol_loader_target is not None:
                stack.enter_context(
                    patch(
                        symbol_loader_target,
                        return_value=[{'symbol': self.requested_symbols[0]}],
                    )
                )

            service.rebuild_for_market_date(
                market_date=self.market_date,
                symbols=self.requested_symbols,
                ranking_symbols=ranking_symbols,
            )

        return load_inputs

    def test_equal_requested_and_ranking_scopes_load_inputs_once(self) -> None:
        for service_class, upsert_target, symbol_loader_target in SERVICE_CASES:
            with self.subTest(service=service_class.__name__):
                load_inputs = self._rebuild(
                    service_class,
                    upsert_target,
                    symbol_loader_target,
                    ranking_symbols=self.requested_symbols,
                )

                load_inputs.assert_called_once_with(
                    market_date=self.market_date,
                    symbols=self.requested_symbols,
                )

    def test_distinct_requested_and_ranking_scopes_load_inputs_separately(self) -> None:
        ranking_symbols = [*self.requested_symbols, '000002']
        for service_class, upsert_target, symbol_loader_target in SERVICE_CASES:
            with self.subTest(service=service_class.__name__):
                load_inputs = self._rebuild(
                    service_class,
                    upsert_target,
                    symbol_loader_target,
                    ranking_symbols=ranking_symbols,
                )

                self.assertEqual(
                    load_inputs.call_args_list,
                    [
                        call(
                            market_date=self.market_date,
                            symbols=self.requested_symbols,
                        ),
                        call(
                            market_date=self.market_date,
                            symbols=ranking_symbols,
                        ),
                    ],
                )


if __name__ == '__main__':
    unittest.main()
