import unittest
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from sqlalchemy.exc import IntegrityError, OperationalError

from app.eod.daily_pipeline_outcomes import daily_pipeline_outcome_is_satisfied
from app.signals.factor_coverage import (
    attach_ranking_dependency_contract,
    attach_symbol_payloads,
    build_factor_coverage_contract,
    build_symbol_results,
    factor_stage_result_contract,
    partition_symbol_failures,
    persist_symbol_items_with_isolation,
    run_symbol_computation,
)
from app.signals.beta_factor import BetaFactorInput, DailyBetaFactorService
from app.signals.composite_factor import DailyCompositeFactorService
from app.signals.market_risk_filter import DailyMarketRiskFilterService
from app.signals.market_risk_snapshot import DailyMarketRiskSnapshotService
from app.signals.momentum_factor import DailyMomentumFactorService
from app.signals.piotroski_filter import (
    DailyPiotroskiFilterService,
    PiotroskiInputs,
)
from app.signals.value_factor import DailyValueFactorService, ValueFactorInput


class SymbolComputationIsolationTests(unittest.TestCase):
    def test_symbol_local_error_keeps_valid_siblings(self) -> None:
        items, failed_symbols, failed_symbol_errors = build_symbol_results(
            ['000001', '000002', '000003'],
            symbol_of=lambda symbol: symbol,
            build=lambda symbol: run_symbol_computation(
                lambda: (
                    (_ for _ in ()).throw(ValueError('malformed metric'))
                    if symbol == '000002'
                    else {'symbol': symbol}
                )
            ),
        )

        self.assertEqual(
            [item['symbol'] for item in items],
            ['000001', '000003'],
        )
        self.assertEqual(failed_symbols, ['000002'])
        self.assertIn('symbol_processing_failed', failed_symbol_errors[0]['error'])

    def test_non_data_stage_error_is_not_downgraded(self) -> None:
        with self.assertRaisesRegex(RuntimeError, 'session lost'):
            build_symbol_results(
                ['000001'],
                symbol_of=lambda symbol: symbol,
                build=lambda _symbol: run_symbol_computation(
                    lambda: (_ for _ in ()).throw(
                        RuntimeError('session lost')
                    )
                ),
            )

    def test_unmarked_data_shaped_error_remains_fatal(self) -> None:
        with self.assertRaisesRegex(ValueError, 'shared invariant failed'):
            build_symbol_results(
                ['000001'],
                symbol_of=lambda symbol: symbol,
                build=lambda _symbol: (_ for _ in ()).throw(
                    ValueError('shared invariant failed')
                ),
            )

    def test_broad_lookup_error_is_not_downgraded(self) -> None:
        with self.assertRaisesRegex(IndexError, 'shared lookup invariant failed'):
            build_symbol_results(
                ['000001'],
                symbol_of=lambda symbol: symbol,
                build=lambda _symbol: run_symbol_computation(
                    lambda: (_ for _ in ()).throw(
                        IndexError('shared lookup invariant failed')
                    )
                ),
            )

    def test_database_integrity_error_is_not_downgraded(self) -> None:
        database_error = IntegrityError(
            'insert into factor_results',
            {},
            RuntimeError('duplicate factor identity'),
        )

        with self.assertRaises(IntegrityError) as raised:
            build_symbol_results(
                ['000001'],
                symbol_of=lambda symbol: symbol,
                build=lambda _symbol: (_ for _ in ()).throw(database_error),
            )

        self.assertIs(raised.exception, database_error)

    def test_factor_stage_does_not_downgrade_database_integrity_error(
        self,
    ) -> None:
        service = DailyValueFactorService()
        database_error = IntegrityError(
            'select factor inputs',
            {},
            RuntimeError('database integrity failure'),
        )
        with (
            patch.object(
                service,
                '_load_inputs',
                return_value=(
                    [SimpleNamespace(symbol='000001')],
                    ['000001'],
                    [],
                ),
            ),
            patch.object(
                service,
                '_build_symbol_result',
                side_effect=database_error,
            ),
            patch(
                'app.signals.value_factor.upsert_daily_value_factor_results'
            ) as upsert,
        ):
            with self.assertRaises(IntegrityError) as raised:
                service.rebuild_for_market_date(
                    market_date=date(2026, 10, 30)
                )

        self.assertIs(raised.exception, database_error)
        upsert.assert_not_called()

    def test_missing_symbol_identity_remains_fatal(self) -> None:
        with self.assertRaisesRegex(ValueError, 'symbol identity is required'):
            build_symbol_results(
                [{'value': 1}],
                symbol_of=lambda _item: '',
                build=lambda item: item,
            )

    def test_symbol_payload_error_keeps_valid_siblings(self) -> None:
        items, failed_symbols, _ = attach_symbol_payloads(
            [{'symbol': '000001'}, {'symbol': '000002'}],
            payload_builder=lambda item: (
                (_ for _ in ()).throw(KeyError('missing audit field'))
                if item['symbol'] == '000002'
                else {'source': 'test'}
            ),
        )

        self.assertEqual([item['symbol'] for item in items], ['000001'])
        self.assertEqual(items[0]['factor_payload'], {'source': 'test'})
        self.assertEqual(failed_symbols, ['000002'])

    def test_coverage_counts_failed_symbol_as_explicit_retry_debt(self) -> None:
        persisted_item = {
            'symbol': '000001',
            'ready': True,
            'complete': True,
            'input_available': True,
        }

        coverage = build_factor_coverage_contract(
            items=[persisted_item],
            persisted_items=[persisted_item],
            missing_symbols=[],
            failed_symbols=['000002'],
            failed_symbol_errors=[],
            is_ready=lambda item: bool(item['ready']),
            is_complete=lambda item: bool(item['complete']),
            unusable_reason=lambda _item: None,
            input_families={
                'test_input': (
                    lambda item: bool(item['input_available']),
                    lambda _item: None,
                )
            },
        )

        self.assertEqual(coverage['universe_count'], 2)
        self.assertEqual(coverage['computed_count'], 1)
        self.assertEqual(coverage['persisted_count'], 1)
        self.assertEqual(coverage['successful_stock_codes'], ['000001'])
        self.assertEqual(coverage['retry_debt_stock_codes'], ['000002'])
        self.assertEqual(coverage['retryable_symbol_count'], 1)
        self.assertEqual(coverage['retryable_symbols'], ['000002'])
        self.assertEqual(
            coverage['missing_reason_counts'],
            {'persistence_failed': 1},
        )

    def test_coverage_distinguishes_processing_from_persistence_failure(
        self,
    ) -> None:
        persisted_item = {
            'symbol': '000001',
            'ready': True,
            'complete': True,
            'input_available': True,
        }
        coverage = build_factor_coverage_contract(
            items=[persisted_item],
            persisted_items=[persisted_item],
            missing_symbols=[],
            failed_symbols=['000002', '000003'],
            failed_symbol_errors=[
                {
                    'symbol': '000002',
                    'error': 'symbol_processing_failed: ValueError: malformed',
                },
                {'symbol': '000003', 'error': 'database write failed'},
            ],
            is_ready=lambda item: bool(item['ready']),
            is_complete=lambda item: bool(item['complete']),
            unusable_reason=lambda _item: None,
            input_families={
                'test_input': (
                    lambda item: bool(item['input_available']),
                    lambda _item: None,
                )
            },
        )

        self.assertEqual(coverage['symbol_processing_failed_count'], 1)
        self.assertEqual(coverage['persistence_failed_count'], 1)
        self.assertEqual(
            coverage['missing_reason_counts'],
            {
                'persistence_failed': 1,
                'symbol_processing_failed': 1,
            },
        )

    def test_ranked_stage_shared_contract_preserves_target_scope_and_peer_debt(
        self,
    ) -> None:
        for stage_id in (
            'value_factor',
            'low_risk_factor',
            'beta_factor',
            'advanced_value_factor',
            'unified_value_factor',
            'quality_factor',
            'composite_factor',
            'momentum_factor',
            'piotroski_filter',
        ):
            with self.subTest(stage_id=stage_id):
                (
                    target_failures,
                    target_errors,
                    dependency_failures,
                    dependency_errors,
                ) = partition_symbol_failures(
                    failed_symbols=['000002'],
                    failed_symbol_errors=[
                        {
                            'symbol': '000002',
                            'error': (
                                'symbol_processing_failed: ValueError: malformed'
                            ),
                        }
                    ],
                    target_symbols=['000001'],
                )
                self.assertEqual(target_failures, [])
                self.assertEqual(target_errors, [])
                summary = build_factor_coverage_contract(
                    items=[
                        {
                            'symbol': '000001',
                            'ready': True,
                            'complete': True,
                            'input_available': True,
                        }
                    ],
                    persisted_items=[
                        {
                            'symbol': '000001',
                            'ready': True,
                            'complete': True,
                            'input_available': True,
                        }
                    ],
                    missing_symbols=[],
                    failed_symbols=[],
                    failed_symbol_errors=[],
                    is_ready=lambda item: bool(item['ready']),
                    is_complete=lambda item: bool(item['complete']),
                    unusable_reason=lambda _item: None,
                    input_families={
                        'test_input': (
                            lambda item: bool(item['input_available']),
                            lambda _item: None,
                        )
                    },
                )
                attach_ranking_dependency_contract(
                    summary,
                    failed_symbols=dependency_failures,
                    failed_symbol_errors=dependency_errors,
                )
                result = factor_stage_result_contract(summary)

                self.assertEqual(result['status'], 'partial')
                self.assertEqual(
                    result['reason'],
                    'ranking_dependency_incomplete',
                )
                self.assertEqual(result['selected_stock_codes'], ['000001'])
                self.assertEqual(result['successful_stock_codes'], ['000001'])
                self.assertEqual(result['retry_debt_stock_codes'], [])
                self.assertEqual(result['remaining_retryable_events'], 0)
                self.assertEqual(result['gate_status'], 'blocked')
                self.assertEqual(result['blocking_status'], 'blocking')
                self.assertEqual(
                    result['ranking_dependency_retry_debt_symbols'],
                    ['000002'],
                )

    def test_factor_rebuild_persists_sibling_and_exposes_retry_debt(self) -> None:
        service = DailyValueFactorService()
        market_date = date(2026, 10, 30)

        def factor_input(symbol: str) -> ValueFactorInput:
            return ValueFactorInput(
                symbol=symbol,
                name_ko=symbol,
                market='KOSPI',
                bars=[
                    {
                        'trade_date': market_date.isoformat(),
                        'close_price': 100,
                        'per': 10.0,
                        'pbr': 1.0,
                        'dividend_yield': 0.02,
                        'source': 'pykrx',
                    }
                ],
            )

        inputs = [factor_input('000001'), factor_input('000002')]
        original_build = service._build_symbol_result

        def build_with_one_bad_symbol(**kwargs):
            if kwargs['input_row'].symbol == '000002':
                raise ValueError('malformed symbol metric')
            return original_build(**kwargs)

        with (
            patch.object(
                service,
                '_load_inputs',
                return_value=(inputs, ['000001', '000002'], []),
            ),
            patch.object(
                service,
                '_build_symbol_result',
                side_effect=build_with_one_bad_symbol,
            ),
            patch(
                'app.signals.value_factor.upsert_daily_value_factor_results'
            ) as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date)

        self.assertEqual(
            [item['symbol'] for item in upsert.call_args.args[0]],
            ['000001'],
        )
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['results_persisted'], 1)
        self.assertEqual(result['results_failed'], 1)
        self.assertEqual(result['failed_symbols'], ['000002'])
        self.assertIn(
            'symbol_processing_failed: ValueError: malformed symbol metric',
            result['failed_symbol_errors'][0]['error'],
        )
        self.assertEqual(result['retry_debt_stock_codes'], ['000002'])
        self.assertEqual(result['successful_stock_codes'], ['000001'])
        self.assertEqual(result['remaining_retryable_events'], 1)

    def test_factor_cohort_invariant_failure_remains_fatal(self) -> None:
        service = DailyValueFactorService()
        market_date = date(2026, 10, 30)
        inputs = [
            ValueFactorInput(
                symbol='000001',
                name_ko='000001',
                market='KOSPI',
                bars=[
                    {
                        'trade_date': market_date.isoformat(),
                        'close_price': 100,
                        'per': 10.0,
                        'pbr': 1.0,
                        'dividend_yield': 0.02,
                        'source': 'pykrx',
                    }
                ],
            )
        ]

        with (
            patch.object(
                service,
                '_load_inputs',
                return_value=(inputs, ['000001'], []),
            ),
            patch.object(
                service,
                '_assign_metric_rankings',
                side_effect=ValueError('cohort invariant violated'),
            ),
            patch(
                'app.signals.value_factor.upsert_daily_value_factor_results'
            ) as upsert,
        ):
            with self.assertRaisesRegex(
                ValueError,
                'cohort invariant violated',
            ):
                service.rebuild_for_market_date(market_date=market_date)

        upsert.assert_not_called()

    def test_value_subset_exposes_malformed_ranking_peer_as_dependency_debt(
        self,
    ) -> None:
        service = DailyValueFactorService()
        market_date = date(2026, 10, 30)

        def factor_input(symbol: str) -> ValueFactorInput:
            return ValueFactorInput(
                symbol=symbol,
                name_ko=symbol,
                market='KOSPI',
                bars=[
                    {
                        'trade_date': market_date.isoformat(),
                        'close_price': 100,
                        'per': 10.0,
                        'pbr': 1.0,
                        'dividend_yield': 0.02,
                        'source': 'pykrx',
                    }
                ],
            )

        target_input = factor_input('000001')
        peer_input = factor_input('000002')
        original_build = service._build_symbol_result

        def load_inputs(*, market_date, symbols):
            del market_date
            if symbols == ['000001']:
                return [target_input], ['000001'], []
            return [target_input, peer_input], ['000001', '000002'], []

        def build_with_bad_peer(**kwargs):
            if kwargs['input_row'].symbol == '000002':
                raise ValueError('malformed ranking peer')
            return original_build(**kwargs)

        with (
            patch.object(service, '_load_inputs', side_effect=load_inputs),
            patch.object(
                service,
                '_build_symbol_result',
                side_effect=build_with_bad_peer,
            ),
            patch(
                'app.signals.value_factor.upsert_daily_value_factor_results'
            ) as upsert,
        ):
            result = service.rebuild_for_market_date(
                market_date=market_date,
                symbols=['000001'],
                ranking_symbols=['000001', '000002'],
            )

        self.assertEqual(
            [item['symbol'] for item in upsert.call_args.args[0]],
            ['000001'],
        )
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['reason'], 'ranking_dependency_incomplete')
        self.assertEqual(result['failed_symbols'], [])
        self.assertEqual(result['results_failed'], 0)
        self.assertEqual(result['selected_stock_codes'], ['000001'])
        self.assertEqual(result['successful_stock_codes'], ['000001'])
        self.assertEqual(result['retry_debt_stock_codes'], [])
        self.assertEqual(result['remaining_retryable_events'], 0)
        self.assertEqual(
            result['ranking_dependency_retry_debt_symbols'],
            ['000002'],
        )

    def test_beta_validates_peer_returns_before_benchmark_construction(
        self,
    ) -> None:
        service = DailyBetaFactorService()
        market_date = date(2026, 10, 30)

        def factor_input(
            symbol: str,
            *,
            malformed: bool = False,
        ) -> BetaFactorInput:
            bars = [
                {
                    'trade_date': (
                        market_date - timedelta(days=offset)
                    ).isoformat(),
                    'close_price': 100 + (120 - offset),
                    'source': 'test',
                }
                for offset in range(121)
            ]
            if malformed:
                bars[17]['close_price'] = 'malformed'
            return BetaFactorInput(
                symbol=symbol,
                name_ko=symbol,
                market='KOSPI',
                bars=bars,
            )

        target_input = factor_input('000001')
        peer_input = factor_input('000002', malformed=True)

        def load_inputs(*, market_date, symbols):
            del market_date
            if symbols == ['000001']:
                return [target_input], ['000001'], []
            return [target_input, peer_input], ['000001', '000002'], []

        with (
            patch.object(service, '_load_inputs', side_effect=load_inputs),
            patch(
                'app.signals.beta_factor.upsert_daily_beta_factor_results'
            ) as upsert,
        ):
            result = service.rebuild_for_market_date(
                market_date=market_date,
                symbols=['000001'],
                ranking_symbols=['000001', '000002'],
            )

        self.assertEqual(
            [item['symbol'] for item in upsert.call_args.args[0]],
            ['000001'],
        )
        self.assertEqual(result['failed_symbols'], [])
        self.assertEqual(result['retry_debt_stock_codes'], [])
        self.assertEqual(
            result['ranking_dependency_retry_debt_symbols'],
            ['000002'],
        )
        self.assertIn(
            'symbol_processing_failed: ValueError',
            result['ranking_dependency_failed_symbol_errors'][0]['error'],
        )

    def test_missing_ranking_peer_is_not_silently_dropped(self) -> None:
        service = DailyValueFactorService()
        market_date = date(2026, 10, 30)
        target_input = ValueFactorInput(
            symbol='000001',
            name_ko='000001',
            market='KOSPI',
            bars=[
                {
                    'trade_date': market_date.isoformat(),
                    'close_price': 100,
                    'per': 10.0,
                    'pbr': 1.0,
                    'dividend_yield': 0.02,
                    'source': 'pykrx',
                }
            ],
        )

        def load_inputs(*, market_date, symbols):
            del market_date
            if symbols == ['000001']:
                return [target_input], ['000001'], []
            return [target_input], ['000001'], ['000002']

        with (
            patch.object(service, '_load_inputs', side_effect=load_inputs),
            patch(
                'app.signals.value_factor.upsert_daily_value_factor_results'
            ),
        ):
            result = service.rebuild_for_market_date(
                market_date=market_date,
                symbols=['000001'],
                ranking_symbols=['000001', '000002'],
            )

        self.assertEqual(result['retry_debt_stock_codes'], [])
        self.assertEqual(
            result['ranking_dependency_retry_debt_symbols'],
            ['000002'],
        )
        self.assertEqual(
            result['ranking_dependency_failed_symbol_errors'],
            [{'symbol': '000002', 'error': 'ranking_input_missing'}],
        )

    def test_composite_missing_shared_family_remains_fatal(self) -> None:
        service = DailyCompositeFactorService()
        factor_rows = {
            'momentum': {},
            'value_v1': {},
            'advanced_value': {},
            'low_risk': {},
        }

        with (
            patch(
                'app.signals.composite_factor.'
                'list_symbols_with_daily_bar_on_date',
                return_value=['000001'],
            ),
            patch.object(
                service,
                '_load_factor_rows',
                return_value=factor_rows,
            ),
            patch(
                'app.signals.composite_factor.'
                'upsert_daily_composite_factor_results'
            ) as upsert,
        ):
            with self.assertRaisesRegex(
                KeyError,
                'missing shared composite factor families: beta',
            ):
                service.rebuild_for_market_date(
                    market_date='2026-10-30',
                    symbols=['000001'],
                    ranking_symbols=['000001'],
                )

        upsert.assert_not_called()

    def test_composite_subset_surfaces_malformed_ranking_peer(self) -> None:
        service = DailyCompositeFactorService()
        symbols = ('000001', '000002')
        factor_rows = {
            'momentum': {
                symbol: {
                    'symbol': symbol,
                    'momentum_percentile': 0.8,
                    'is_momentum_eligible': True,
                }
                for symbol in symbols
            },
            'value_v1': {
                '000001': {
                    'symbol': '000001',
                    'value_score': 0.8,
                    'value_status': 'scored',
                },
                '000002': {
                    'symbol': '000002',
                    'value_score': 'malformed',
                    'value_status': 'scored',
                },
            },
            'advanced_value': {
                symbol: {
                    'symbol': symbol,
                    'advanced_value_score': 0.8,
                    'advanced_value_classification': 'eligible',
                }
                for symbol in symbols
            },
            'low_risk': {
                symbol: {
                    'symbol': symbol,
                    'low_risk_score': 0.8,
                    'low_risk_classification': 'eligible',
                    'liquidity_floor_passed': True,
                }
                for symbol in symbols
            },
            'beta': {
                symbol: {
                    'symbol': symbol,
                    'low_beta_score': 0.8,
                    'low_beta_classification': 'eligible',
                }
                for symbol in symbols
            },
        }

        with (
            patch(
                'app.signals.composite_factor.'
                'list_symbols_with_daily_bar_on_date',
                return_value=list(symbols),
            ),
            patch.object(
                service,
                '_load_factor_rows',
                return_value=factor_rows,
            ),
            patch(
                'app.signals.composite_factor.load_symbols',
                return_value=[
                    {'symbol': symbol, 'name_ko': symbol, 'market': 'KOSPI'}
                    for symbol in symbols
                ],
            ),
            patch(
                'app.signals.composite_factor.'
                'upsert_daily_composite_factor_results',
                return_value=1,
            ) as upsert,
        ):
            result = service.rebuild_for_market_date(
                market_date='2026-10-30',
                symbols=['000001'],
                ranking_symbols=list(symbols),
            )

        self.assertEqual(
            [item['symbol'] for item in upsert.call_args.args[0]],
            ['000001'],
        )
        self.assertEqual(result['failed_symbols'], [])
        self.assertEqual(result['retry_debt_stock_codes'], [])
        self.assertEqual(
            result['ranking_dependency_retry_debt_symbols'],
            ['000002'],
        )

    def test_momentum_factor_isolates_malformed_symbol_history(self) -> None:
        service = DailyMomentumFactorService()
        market_date = date(2026, 10, 30)

        def history(close_price: int | str) -> list[dict[str, object]]:
            start = market_date - timedelta(days=252)
            return [
                {
                    'trade_date': (start + timedelta(days=offset)).isoformat(),
                    'close_price': close_price,
                    'source': 'test',
                }
                for offset in range(253)
            ]

        histories = {
            '000001': history(100),
            '000002': history('malformed'),
        }
        with (
            patch(
                'app.signals.momentum_factor.'
                'list_symbols_with_daily_bar_on_date',
                return_value=['000001', '000002'],
            ),
            patch(
                'app.signals.momentum_factor.'
                'load_recent_price_bars_by_symbol',
                return_value=histories,
            ),
            patch(
                'app.signals.momentum_factor.'
                'upsert_daily_momentum_factor_results'
            ) as upsert,
        ):
            result = service.rebuild_for_market_date(
                market_date=market_date
            )

        self.assertEqual(
            [item['symbol'] for item in upsert.call_args.args[0]],
            ['000001'],
        )
        self.assertEqual(result['results_persisted'], 1)
        self.assertEqual(result['results_failed'], 1)
        self.assertEqual(result['failed_symbols'], ['000002'])
        self.assertIn(
            'symbol_processing_failed: ValueError',
            result['failed_symbol_errors'][0]['error'],
        )
        self.assertEqual(result['successful_stock_codes'], ['000001'])
        self.assertEqual(result['retry_debt_stock_codes'], ['000002'])

    def test_piotroski_filter_keeps_valid_sibling_and_retry_debt(self) -> None:
        service = DailyPiotroskiFilterService()
        market_date = date(2026, 10, 30)

        def factor_input(symbol: str) -> PiotroskiInputs:
            return PiotroskiInputs(
                symbol=symbol,
                name_ko=symbol,
                market_date=market_date,
                pbr=1.0,
                return_20d=0.1,
                total_equity=100.0,
                net_income=20.0,
                prior_net_income=10.0,
                operating_cash_flow=30.0,
                total_assets=200.0,
                prior_total_assets=180.0,
                debt_ratio=0.3,
                prior_debt_ratio=0.4,
                current_ratio=1.8,
                prior_current_ratio=1.2,
                gross_margin=0.35,
                prior_gross_margin=0.30,
                asset_turnover=1.4,
                prior_asset_turnover=1.2,
                has_audit_issue=False,
                has_dilution_signal=False,
            )

        valid_input = factor_input('000001')
        malformed_input = factor_input('000002')
        malformed_input.fundamentals_provenance = None  # type: ignore[assignment]

        with (
            patch.object(
                service,
                '_load_inputs',
                return_value=[valid_input, malformed_input],
            ),
            patch(
                'app.signals.piotroski_filter.'
                'upsert_daily_piotroski_filter_results'
            ) as upsert,
        ):
            result = service.rebuild_for_market_date(
                market_date=market_date
            )

        self.assertEqual(
            [item['symbol'] for item in upsert.call_args.args[0]],
            ['000001'],
        )
        self.assertEqual(result['results_persisted'], 1)
        self.assertEqual(result['results_failed'], 1)
        self.assertEqual(result['failed_symbols'], ['000002'])
        self.assertIn(
            'symbol_processing_failed: TypeError',
            result['failed_symbol_errors'][0]['error'],
        )
        self.assertEqual(
            result['selected_stock_codes'],
            ['000001', '000002'],
        )
        self.assertEqual(result['successful_stock_codes'], ['000001'])
        self.assertEqual(result['retry_debt_stock_codes'], ['000002'])

    def test_piotroski_ranking_peer_failure_blocks_authority(self) -> None:
        service = DailyPiotroskiFilterService()
        market_date = date(2026, 10, 30)

        def factor_input(symbol: str) -> PiotroskiInputs:
            return PiotroskiInputs(
                symbol=symbol,
                name_ko=symbol,
                market_date=market_date,
                pbr=1.0,
                return_20d=0.1,
                total_equity=100.0,
                net_income=20.0,
                prior_net_income=10.0,
                operating_cash_flow=30.0,
                total_assets=200.0,
                prior_total_assets=180.0,
                debt_ratio=0.3,
                prior_debt_ratio=0.4,
                current_ratio=1.8,
                prior_current_ratio=1.2,
                gross_margin=0.35,
                prior_gross_margin=0.30,
                asset_turnover=1.4,
                prior_asset_turnover=1.2,
                has_audit_issue=False,
                has_dilution_signal=False,
            )

        target_input = factor_input('000001')
        malformed_peer = factor_input('000002')
        malformed_peer.fundamentals_provenance = None  # type: ignore[assignment]

        with (
            patch.object(
                service,
                '_load_inputs',
                return_value=[target_input, malformed_peer],
            ),
            patch(
                'app.signals.piotroski_filter.'
                'upsert_daily_piotroski_filter_results'
            ) as upsert,
        ):
            result = service.rebuild_for_market_date(
                market_date=market_date,
                symbols=['000001'],
                ranking_symbols=['000001', '000002'],
            )

        self.assertEqual(
            [item['symbol'] for item in upsert.call_args.args[0]],
            ['000001'],
        )
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['reason'], 'ranking_dependency_incomplete')
        self.assertEqual(result['gate_status'], 'blocked')
        self.assertEqual(result['blocking_status'], 'blocking')
        self.assertEqual(result['selected_stock_codes'], ['000001'])
        self.assertEqual(result['successful_stock_codes'], ['000001'])
        self.assertEqual(result['retry_debt_stock_codes'], [])
        self.assertEqual(
            result['ranking_dependency_retry_debt_symbols'],
            ['000002'],
        )
        self.assertFalse(
            daily_pipeline_outcome_is_satisfied(
                {'stage': 'piotroski_filter', **result}
            )
        )


class SymbolPersistenceIsolationTests(unittest.TestCase):
    def test_database_and_unknown_runtime_bulk_failures_remain_fatal(
        self,
    ) -> None:
        error_factories = {
            'database_operational_error': lambda: OperationalError(
                'insert into factor_results',
                {},
                RuntimeError('connection lost'),
            ),
            'unknown_runtime_error': lambda: RuntimeError(
                'unexpected persistence invariant'
            ),
        }
        items = [{'symbol': '000001'}, {'symbol': '000002'}]

        for error_name, error_factory in error_factories.items():
            with self.subTest(error_name=error_name):
                error = error_factory()
                singleton_attempts: list[str] = []

                def fail_bulk(_items) -> None:
                    raise error

                with self.assertRaises(type(error)) as raised:
                    persist_symbol_items_with_isolation(
                        items,
                        bulk_write=fail_bulk,
                        item_write=lambda item: singleton_attempts.append(
                            item['symbol']
                        ),
                    )

                self.assertIs(raised.exception, error)
                self.assertEqual(singleton_attempts, [])

    def test_database_and_unknown_runtime_singleton_failures_remain_fatal(
        self,
    ) -> None:
        error_factories = {
            'database_operational_error': lambda: OperationalError(
                'insert into factor_results',
                {},
                RuntimeError('connection lost'),
            ),
            'unknown_runtime_error': lambda: RuntimeError(
                'unexpected persistence invariant'
            ),
        }
        items = [{'symbol': '000001'}]

        for error_name, error_factory in error_factories.items():
            with self.subTest(error_name=error_name):
                error = error_factory()

                def fail_bulk(_items) -> None:
                    raise ValueError('bulk payload contains malformed row')

                def fail_singleton(_item) -> None:
                    raise error

                with self.assertRaises(type(error)) as raised:
                    persist_symbol_items_with_isolation(
                        items,
                        bulk_write=fail_bulk,
                        item_write=fail_singleton,
                    )

                self.assertIs(raised.exception, error)

    def test_genuine_row_local_persistence_failure_is_isolated(self) -> None:
        items = [
            {'symbol': '000001'},
            {'symbol': '000002'},
            {'symbol': '000003'},
        ]
        singleton_attempts: list[str] = []

        def fail_bulk(_items) -> None:
            raise ValueError('bulk payload contains malformed row')

        def write_singleton(item) -> None:
            singleton_attempts.append(item['symbol'])
            if item['symbol'] == '000002':
                raise ValueError('malformed persisted metric')

        persisted, failed_symbols, failed_symbol_errors = (
            persist_symbol_items_with_isolation(
                items,
                bulk_write=fail_bulk,
                item_write=write_singleton,
            )
        )

        self.assertEqual(singleton_attempts, ['000001', '000002', '000003'])
        self.assertEqual(
            [item['symbol'] for item in persisted],
            ['000001', '000003'],
        )
        self.assertEqual(failed_symbols, ['000002'])
        self.assertEqual(
            failed_symbol_errors,
            [
                {
                    'symbol': '000002',
                    'error': (
                        'persistence_failed: ValueError: '
                        'malformed persisted metric'
                    ),
                }
            ],
        )


class FilterSymbolComputationIsolationTests(unittest.IsolatedAsyncioTestCase):
    async def test_market_risk_filter_keeps_valid_sibling_and_retry_debt(
        self,
    ) -> None:
        snapshot_service = DailyMarketRiskSnapshotService()
        service = DailyMarketRiskFilterService(snapshot_service=snapshot_service)
        snapshot_collection = {
            'market_date': '2026-10-30',
            'requested_symbols': ['000001', '000002'],
            'matched_symbols': ['000001', '000002'],
            'missing_symbols': [],
            'snapshots_persisted': 2,
            'snapshots_failed': 0,
            'failed_symbols': [],
            'failed_symbol_errors': [],
            'items': [
                {'symbol': '000001'},
                {'symbol': '000002'},
            ],
        }

        def build_filter_result(snapshot, **_kwargs):
            if snapshot['symbol'] == '000002':
                raise ValueError('malformed market-risk snapshot')
            return {
                'symbol': snapshot['symbol'],
                'passed_filter': True,
                'watchlist_flagged': False,
                'filter_payload': {},
            }

        with (
            patch.object(
                snapshot_service,
                'collect_for_market_date',
                new=AsyncMock(return_value=snapshot_collection),
            ),
            patch(
                'app.signals.market_risk_filter.'
                'build_market_risk_filter_result_from_snapshot',
                side_effect=build_filter_result,
            ),
            patch(
                'app.signals.market_risk_filter.'
                'upsert_daily_market_risk_filter_results'
            ) as upsert,
        ):
            result = await service.rebuild_for_market_date(
                market_date='2026-10-30'
            )

        self.assertEqual(
            [item['symbol'] for item in upsert.call_args.args[0]],
            ['000001'],
        )
        self.assertEqual(result['results_persisted'], 1)
        self.assertEqual(result['results_failed'], 1)
        self.assertEqual(result['failed_symbols'], ['000002'])
        self.assertIn(
            'symbol_processing_failed: ValueError',
            result['failed_symbol_errors'][0]['error'],
        )
        self.assertEqual(result['successful_stock_codes'], ['000001'])
        self.assertEqual(result['retry_debt_stock_codes'], ['000002'])


if __name__ == '__main__':
    unittest.main()
