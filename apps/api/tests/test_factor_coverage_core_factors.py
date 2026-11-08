import unittest

from app.signals.beta_factor import DailyBetaFactorService
from app.signals.factor_coverage import factor_stage_result_contract
from app.signals.low_risk_factor import DailyLowRiskFactorService
from app.signals.momentum_factor import DailyMomentumFactorService
from app.signals.value_factor import DailyValueFactorService


class CoreFactorCoverageContractTests(unittest.TestCase):
    @staticmethod
    def _cases():
        return (
            (
                'momentum',
                DailyMomentumFactorService._build_coverage_contract,
                {
                    'symbol': '000001',
                    'momentum_12_1': 0.20,
                    'momentum_percentile': 0.90,
                    'ma_60': 100.0,
                    'ma_120': 90.0,
                    'return_5d': 0.02,
                    'return_5d_realized_volatility_20d': 0.01,
                    'return_5d_vol_adjusted': 0.89,
                    'has_sufficient_history': True,
                    'momentum_eligibility_status': 'eligible',
                },
                {
                    'symbol': '000002',
                    'momentum_12_1': None,
                    'momentum_percentile': None,
                    'ma_60': None,
                    'ma_120': None,
                    'return_5d': None,
                    'return_5d_realized_volatility_20d': None,
                    'return_5d_vol_adjusted': None,
                    'has_sufficient_history': False,
                    'momentum_eligibility_status': 'insufficient_history',
                },
            ),
            (
                'value',
                DailyValueFactorService._build_coverage_contract,
                {
                    'symbol': '000001',
                    'value_score': 0.80,
                    'value_status': 'scored',
                    'per_status': 'available',
                    'factor_payload': {
                        'metric_statuses': {
                            'per': 'available',
                            'pbr': 'available',
                            'dividend_yield': 'available',
                        }
                    },
                },
                {
                    'symbol': '000002',
                    'value_score': None,
                    'value_status': 'missing_metric',
                    'per_status': 'missing',
                    'factor_payload': {
                        'metric_statuses': {
                            'per': 'missing',
                            'pbr': 'missing',
                            'dividend_yield': 'missing',
                        }
                    },
                },
            ),
            (
                'low_risk',
                DailyLowRiskFactorService._build_coverage_contract,
                {
                    'symbol': '000001',
                    'low_risk_score': 0.70,
                    'low_risk_classification': 'eligible',
                    'factor_payload': {
                        'availability': {
                            'metrics': {
                                'volatility_60d': True,
                                'volatility_120d': True,
                                'max_drawdown_120d': True,
                                'avg_trading_value_20d': True,
                            }
                        }
                    },
                },
                {
                    'symbol': '000002',
                    'low_risk_score': None,
                    'low_risk_classification': 'insufficient_history',
                    'factor_payload': {
                        'availability': {
                            'metrics': {
                                'volatility_60d': False,
                                'volatility_120d': False,
                                'max_drawdown_120d': False,
                                'avg_trading_value_20d': False,
                            }
                        }
                    },
                },
            ),
            (
                'beta',
                DailyBetaFactorService._build_coverage_contract,
                {
                    'symbol': '000001',
                    'low_beta_score': 0.70,
                    'low_beta_classification': 'eligible',
                    'factor_payload': {
                        'availability': {
                            'metrics': {
                                'beta_60d': True,
                                'beta_120d': True,
                            }
                        }
                    },
                },
                {
                    'symbol': '000002',
                    'low_beta_score': None,
                    'low_beta_classification': 'insufficient_history',
                    'factor_payload': {
                        'availability': {
                            'metrics': {
                                'beta_60d': False,
                                'beta_120d': False,
                            }
                        }
                    },
                },
            ),
        )

    def test_mixed_usable_cohort_is_partial_but_nonblocking(self) -> None:
        for name, builder, usable, unusable in self._cases():
            with self.subTest(factor=name):
                summary = builder(
                    items=[usable, unusable],
                    persisted_items=[usable, unusable],
                    missing_symbols=[],
                    failed_symbols=[],
                )
                stage = factor_stage_result_contract(summary)

                self.assertEqual(summary['coverage_status'], 'partial')
                self.assertEqual(summary['coverage_reason'], 'partial_usable_coverage')
                self.assertEqual(summary['persisted_count'], 2)
                self.assertEqual(summary['ready_count'], 1)
                self.assertEqual(summary['complete_count'], 1)
                self.assertEqual(summary['unusable_count'], 1)
                self.assertEqual(stage['gate_status'], 'ready')
                self.assertEqual(stage['blocking_status'], 'non_blocking')

    def test_all_unusable_cohort_is_partial_and_blocking(self) -> None:
        for name, builder, _, unusable in self._cases():
            with self.subTest(factor=name):
                summary = builder(
                    items=[unusable],
                    persisted_items=[unusable],
                    missing_symbols=[],
                    failed_symbols=[],
                )
                stage = factor_stage_result_contract(summary)

                self.assertEqual(summary['coverage_status'], 'partial')
                self.assertEqual(summary['coverage_reason'], 'no_usable_factor_rows')
                self.assertEqual(summary['ready_count'], 0)
                self.assertEqual(summary['complete_count'], 0)
                self.assertEqual(summary['unusable_count'], 1)
                self.assertEqual(summary['quality_status'], 'unavailable')
                self.assertEqual(stage['gate_status'], 'blocked')
                self.assertEqual(stage['blocking_status'], 'blocking')

    def test_persistence_failure_remains_explicit_retry_debt(self) -> None:
        for name, builder, usable, _ in self._cases():
            with self.subTest(factor=name):
                summary = builder(
                    items=[usable],
                    persisted_items=[],
                    missing_symbols=[],
                    failed_symbols=['000001'],
                )
                stage = factor_stage_result_contract(summary)

                self.assertEqual(summary['coverage_status'], 'partial')
                self.assertEqual(summary['retryable_symbol_count'], 1)
                self.assertEqual(summary['retryable_symbols'], ['000001'])
                self.assertEqual(stage['remaining_retryable_events'], 1)
                self.assertEqual(stage['retryable_symbols'], ['000001'])
                self.assertEqual(stage['gate_status'], 'blocked')

    def test_one_persistence_failure_admits_other_ninety_nine_symbols(self) -> None:
        for name, builder, usable, _ in self._cases():
            with self.subTest(factor=name):
                items = [
                    {
                        **usable,
                        'symbol': f'{index:06d}',
                    }
                    for index in range(1, 101)
                ]
                failed_symbol = items[-1]['symbol']
                persisted_items = items[:-1]

                summary = builder(
                    items=items,
                    persisted_items=persisted_items,
                    missing_symbols=[],
                    failed_symbols=[failed_symbol],
                )
                stage = factor_stage_result_contract(summary)

                self.assertEqual(stage['status'], 'partial')
                self.assertEqual(stage['gate_status'], 'ready')
                self.assertEqual(
                    stage['scope_outcome_manifest_version'],
                    'factor_scope_outcomes_v1',
                )
                self.assertEqual(stage['selected_stock_codes'], [
                    item['symbol'] for item in items
                ])
                self.assertEqual(stage['successful_stock_codes'], [
                    item['symbol'] for item in persisted_items
                ])
                self.assertEqual(
                    stage['retry_debt_stock_codes'],
                    [failed_symbol],
                )
                self.assertEqual(
                    stage['scope_outcome_manifest'][-1],
                    {
                        'stock_code': failed_symbol,
                        'outcome': 'retry_debt',
                    },
                )

    def test_missing_symbol_is_counted_as_retry_debt(self) -> None:
        for name, builder, usable, _ in self._cases():
            with self.subTest(factor=name):
                summary = builder(
                    items=[usable],
                    persisted_items=[usable],
                    missing_symbols=['000002'],
                    failed_symbols=[],
                )
                stage = factor_stage_result_contract(summary)

                self.assertEqual(stage['status'], 'partial')
                self.assertEqual(stage['gate_status'], 'ready')
                self.assertEqual(stage['remaining_retryable_events'], 1)
                self.assertEqual(stage['retryable_symbols'], ['000002'])
                self.assertEqual(
                    stage['retry_debt_stock_codes'],
                    ['000002'],
                )

    def test_ambiguous_symbol_identity_blocks_without_exact_manifest(self) -> None:
        for name, builder, usable, _ in self._cases():
            with self.subTest(factor=name):
                duplicate = {**usable, 'symbol': usable['symbol']}
                summary = builder(
                    items=[usable, duplicate],
                    persisted_items=[usable],
                    missing_symbols=[],
                    failed_symbols=[],
                )
                stage = factor_stage_result_contract(summary)

                self.assertNotIn(
                    'scope_outcome_manifest_version',
                    summary,
                )
                self.assertEqual(stage['status'], 'partial')
                self.assertEqual(
                    stage['reason'],
                    'scope_outcome_manifest_invalid',
                )
                self.assertEqual(stage['gate_status'], 'blocked')
                self.assertEqual(stage['blocking_status'], 'blocking')
                self.assertGreater(stage['remaining_retryable_events'], 0)


if __name__ == '__main__':
    unittest.main()
