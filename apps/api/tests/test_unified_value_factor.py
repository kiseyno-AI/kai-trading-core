import unittest
from datetime import date
from unittest.mock import patch

from sqlalchemy import delete, select

from app.db import engine, session_scope
from app.db_models import DailyUnifiedValueFactorResult, Symbol
from app.market_data_store import load_daily_unified_value_factor_results
from app.signals.advanced_value_factor import AnnualFundamentalsSnapshot, AdvancedValueFactorInput
from app.signals.unified_value_factor import (
    DailyUnifiedValueFactorService,
    UNIFIED_VALUE_FACTOR_VERSION,
    UNIFIED_VALUE_STATUS_INSUFFICIENT_COMPONENTS,
    UNIFIED_VALUE_STATUS_INSUFFICIENT_FUNDAMENTALS,
    UNIFIED_VALUE_STATUS_INVALID_PER,
    UNIFIED_VALUE_STATUS_SCORED,
)


def _highlight_row(metric_key: str, *, account_id: str, account_name: str, amount: int, statement_type: str) -> dict:
    return {
        'metric_key': metric_key,
        'statement_type': statement_type,
        'statement_name': 'financial_statement',
        'account_id': account_id,
        'account_name': account_name,
        'current_amount': amount,
        'current_cumulative_amount': None,
        'currency': 'KRW',
    }


def _statement_row(
    row_key: str,
    *,
    statement_type: str,
    account_id: str | None,
    account_name: str,
    amount: int,
    row_order: int,
) -> dict:
    return {
        'row_key': row_key,
        'statement_type': statement_type,
        'statement_name': 'financial_statement',
        'account_id': account_id,
        'account_name': account_name,
        'account_detail': None,
        'current_amount': amount,
        'current_cumulative_amount': None,
        'row_order': row_order,
        'currency': 'KRW',
    }


def _snapshot(
    symbol: str,
    *,
    total_equity: int,
    total_liabilities: int,
    operating_income: int,
    operating_cash_flow: int,
    cash: int,
    capex_ppe: int | None,
    capex_intangible: int | None = None,
    depreciation_and_amortization: int | None = None,
    snapshot_id: int = 1,
) -> AnnualFundamentalsSnapshot:
    rows = [
        _statement_row(
            'cash',
            statement_type='BS',
            account_id='ifrs-full_CashAndCashEquivalents',
            account_name='현금및현금성자산',
            amount=cash,
            row_order=1,
        ),
    ]
    if capex_ppe is not None:
        rows.append(
            _statement_row(
                'capex_ppe',
                statement_type='CF',
                account_id='ifrs-full_PurchaseOfPropertyPlantAndEquipment',
                account_name='유형자산의 취득',
                amount=capex_ppe,
                row_order=2,
            )
        )
    if capex_intangible is not None:
        rows.append(
            _statement_row(
                'capex_intangible',
                statement_type='CF',
                account_id='ifrs-full_PurchaseOfIntangibleAssets',
                account_name='무형자산의 취득',
                amount=capex_intangible,
                row_order=3,
            )
        )
    if depreciation_and_amortization is not None:
        rows.append(
            _statement_row(
                'da',
                statement_type='CF',
                account_id=None,
                account_name='감가상각비및상각비',
                amount=depreciation_and_amortization,
                row_order=4,
            )
        )

    return AnnualFundamentalsSnapshot(
        symbol=symbol,
        snapshot_id=snapshot_id,
        corp_name=symbol,
        business_year=2025,
        report_code='11011',
        fs_div='CFS',
        period_type='annual',
        receipt_date='2026-03-20',
        rcept_no=f'20260320{snapshot_id:08d}',
        captured_at='2026-03-20T12:00:00+09:00',
        currency='KRW',
        highlights_by_key={
            'total_equity': _highlight_row(
                'total_equity',
                account_id='ifrs-full_Equity',
                account_name='자본총계',
                amount=total_equity,
                statement_type='BS',
            ),
            'total_liabilities': _highlight_row(
                'total_liabilities',
                account_id='ifrs-full_Liabilities',
                account_name='부채총계',
                amount=total_liabilities,
                statement_type='BS',
            ),
            'operating_income': _highlight_row(
                'operating_income',
                account_id='dart_OperatingIncomeLoss',
                account_name='영업이익',
                amount=operating_income,
                statement_type='CIS',
            ),
            'operating_cash_flow': _highlight_row(
                'operating_cash_flow',
                account_id='ifrs-full_CashFlowsFromUsedInOperatingActivities',
                account_name='영업활동순현금흐름',
                amount=operating_cash_flow,
                statement_type='CF',
            ),
        },
        statement_rows=rows,
    )


def _bars(market_date: date, *, per: float | None, pbr: float | None, dividend_yield: float | None) -> list[dict]:
    return [
        {
            'trade_date': market_date.isoformat(),
            'open_price': 100000,
            'high_price': 101000,
            'low_price': 99000,
            'close_price': 100000,
            'volume': 1000000,
            'turnover': 100000000,
            'per': per,
            'pbr': pbr,
            'dividend_yield': dividend_yield,
            'source': 'pykrx',
            'captured_at': f'{market_date.isoformat()}T15:30:00+09:00',
        }
    ]


def _input(
    symbol: str,
    market_date: date,
    *,
    per: float | None,
    pbr: float | None,
    dividend_yield: float | None,
    snapshot: AnnualFundamentalsSnapshot | None,
    market_cap: float | None = None,
    valuation_snapshot: dict | None = None,
) -> AdvancedValueFactorInput:
    return AdvancedValueFactorInput(
        symbol=symbol,
        name_ko=symbol,
        market='KOSPI',
        bars=_bars(market_date, per=per, pbr=pbr, dividend_yield=dividend_yield),
        annual_fundamentals=snapshot,
        market_cap=market_cap,
        valuation_snapshot=valuation_snapshot,
    )


class DailyUnifiedValueFactorServiceTests(unittest.TestCase):
    def test_uses_same_day_valuation_snapshot_when_bar_metrics_are_null(self) -> None:
        service = DailyUnifiedValueFactorService()
        market_date = date(2026, 4, 24)
        inputs = [
            _input(
                '005930',
                market_date,
                per=None,
                pbr=None,
                dividend_yield=None,
                market_cap=2_000_000,
                valuation_snapshot={
                    'per': 8.0,
                    'pbr': 0.9,
                    'dividend_yield': 0.03,
                    'market_cap': 2_000_000,
                },
                snapshot=_snapshot(
                    '005930',
                    total_equity=1000,
                    total_liabilities=300,
                    operating_income=100,
                    operating_cash_flow=220,
                    cash=100,
                    capex_ppe=-60,
                    depreciation_and_amortization=40,
                    snapshot_id=1,
                ),
            ),
        ]

        with (
            patch.object(service, '_load_inputs', return_value=(inputs, ['005930'], [])),
            patch('app.signals.unified_value_factor.upsert_daily_unified_value_factor_result') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date)

        persisted = upsert.call_args.args[0]
        self.assertEqual(persisted['unified_value_status'], UNIFIED_VALUE_STATUS_SCORED)
        self.assertEqual(persisted['per'], 8.0)
        self.assertEqual(persisted['pbr'], 0.9)
        self.assertEqual(persisted['dividend_yield'], 0.03)
        self.assertEqual(
            persisted['factor_payload']['audit']['valuation_source'],
            'pykrx_valuation_snapshot',
        )
        self.assertEqual(result['summary']['scored_count'], 1)

    def test_observed_market_cap_keeps_advanced_metrics_without_pbr(self) -> None:
        service = DailyUnifiedValueFactorService()
        market_date = date(2026, 4, 24)
        inputs = [
            _input(
                '005930',
                market_date,
                per=8.0,
                pbr=None,
                dividend_yield=0.03,
                market_cap=2_000_000,
                snapshot=_snapshot(
                    '005930',
                    total_equity=1000,
                    total_liabilities=300,
                    operating_income=100,
                    operating_cash_flow=220,
                    cash=100,
                    capex_ppe=-60,
                    depreciation_and_amortization=40,
                    snapshot_id=1,
                ),
            ),
        ]

        with (
            patch.object(service, '_load_inputs', return_value=(inputs, ['005930'], [])),
            patch('app.signals.unified_value_factor.upsert_daily_unified_value_factor_result') as upsert,
        ):
            service.rebuild_for_market_date(market_date=market_date)

        persisted = upsert.call_args.args[0]
        self.assertEqual(persisted['fcf_yield_status'], 'available')
        self.assertEqual(persisted['ev_to_ebitda_status'], 'available')
        self.assertEqual(persisted['factor_payload']['inputs']['market_cap_proxy'], 2_000_000)
        self.assertEqual(
            persisted['factor_payload']['audit']['market_cap']['source_type'],
            'pykrx_valuation_snapshot',
        )

    def test_rebuild_scores_all_five_component_percentiles(self) -> None:
        service = DailyUnifiedValueFactorService()
        market_date = date(2026, 4, 24)
        inputs = [
            _input(
                '000001',
                market_date,
                per=5.0,
                pbr=0.5,
                dividend_yield=0.05,
                snapshot=_snapshot(
                    '000001',
                    total_equity=1000,
                    total_liabilities=200,
                    operating_income=100,
                    operating_cash_flow=250,
                    cash=100,
                    capex_ppe=-50,
                    depreciation_and_amortization=50,
                    snapshot_id=1,
                ),
            ),
            _input(
                '000002',
                market_date,
                per=10.0,
                pbr=1.0,
                dividend_yield=0.03,
                snapshot=_snapshot(
                    '000002',
                    total_equity=1000,
                    total_liabilities=250,
                    operating_income=100,
                    operating_cash_flow=180,
                    cash=50,
                    capex_ppe=-80,
                    depreciation_and_amortization=50,
                    snapshot_id=2,
                ),
            ),
            _input(
                '000003',
                market_date,
                per=20.0,
                pbr=2.0,
                dividend_yield=0.01,
                snapshot=_snapshot(
                    '000003',
                    total_equity=1000,
                    total_liabilities=300,
                    operating_income=100,
                    operating_cash_flow=70,
                    cash=20,
                    capex_ppe=-50,
                    depreciation_and_amortization=40,
                    snapshot_id=3,
                ),
            ),
        ]

        with (
            patch.object(service, '_load_inputs', return_value=(inputs, ['000001', '000002', '000003'], [])),
            patch('app.signals.unified_value_factor.upsert_daily_unified_value_factor_result') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date)

        self.assertEqual(result['feature_version'], UNIFIED_VALUE_FACTOR_VERSION)
        self.assertEqual(result['results_persisted'], 3)
        self.assertEqual(upsert.call_count, 3)
        persisted_by_symbol = {call.args[0]['symbol']: call.args[0] for call in upsert.call_args_list}

        best = persisted_by_symbol['000001']
        middle = persisted_by_symbol['000002']
        worst = persisted_by_symbol['000003']

        self.assertEqual(best['per_rank'], 1)
        self.assertEqual(best['pbr_rank'], 1)
        self.assertEqual(best['dividend_yield_rank'], 1)
        self.assertEqual(best['fcf_yield_rank'], 1)
        self.assertEqual(best['ev_to_ebitda_rank'], 1)
        self.assertEqual(best['unified_value_rank'], 1)
        self.assertEqual(best['unified_value_percentile'], 1.0)
        self.assertEqual(best['unified_value_status'], UNIFIED_VALUE_STATUS_SCORED)

        self.assertEqual(middle['unified_value_rank'], 2)
        self.assertAlmostEqual(middle['unified_value_score'], 0.5, places=6)
        self.assertEqual(middle['components_used'], 5)

        self.assertEqual(worst['unified_value_rank'], 3)
        self.assertEqual(worst['unified_value_percentile'], 0.0)
        self.assertEqual(result['summary']['scored_count'], 3)
        self.assertEqual(result['summary']['ranking_universe_counts']['unified_value_score'], 3)

    def test_rebuild_marks_invalid_per_as_partial_auditable_row(self) -> None:
        service = DailyUnifiedValueFactorService()
        market_date = date(2026, 4, 24)
        inputs = [
            _input(
                '000001',
                market_date,
                per=-3.0,
                pbr=0.7,
                dividend_yield=0.02,
                snapshot=_snapshot(
                    '000001',
                    total_equity=1000,
                    total_liabilities=210,
                    operating_income=100,
                    operating_cash_flow=220,
                    cash=90,
                    capex_ppe=-50,
                    depreciation_and_amortization=40,
                    snapshot_id=1,
                ),
            ),
            _input(
                '000002',
                market_date,
                per=8.0,
                pbr=1.0,
                dividend_yield=0.03,
                snapshot=_snapshot(
                    '000002',
                    total_equity=1000,
                    total_liabilities=250,
                    operating_income=100,
                    operating_cash_flow=180,
                    cash=50,
                    capex_ppe=-80,
                    depreciation_and_amortization=50,
                    snapshot_id=2,
                ),
            ),
        ]

        with (
            patch.object(service, '_load_inputs', return_value=(inputs, ['000001', '000002'], [])),
            patch('app.signals.unified_value_factor.upsert_daily_unified_value_factor_result') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date)

        persisted_by_symbol = {call.args[0]['symbol']: call.args[0] for call in upsert.call_args_list}
        invalid = persisted_by_symbol['000001']

        self.assertEqual(invalid['per_status'], 'invalid_non_positive')
        self.assertEqual(invalid['unified_value_status'], UNIFIED_VALUE_STATUS_INVALID_PER)
        self.assertEqual(invalid['components_used'], 4)
        self.assertIsNone(invalid['unified_value_score'])
        self.assertIsNone(invalid['unified_value_rank'])
        self.assertEqual(result['summary']['invalid_per_count'], 1)

    def test_rebuild_marks_missing_fundamentals_as_insufficient_fundamentals(self) -> None:
        service = DailyUnifiedValueFactorService()
        market_date = date(2026, 4, 24)
        inputs = [
            _input(
                '035420',
                market_date,
                per=12.0,
                pbr=1.1,
                dividend_yield=0.02,
                snapshot=None,
            ),
        ]

        with (
            patch.object(service, '_load_inputs', return_value=(inputs, ['035420'], [])),
            patch('app.signals.unified_value_factor.upsert_daily_unified_value_factor_result') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date)

        persisted = upsert.call_args.args[0]
        self.assertEqual(persisted['unified_value_status'], UNIFIED_VALUE_STATUS_INSUFFICIENT_FUNDAMENTALS)
        self.assertEqual(persisted['components_used'], 3)
        self.assertIsNone(persisted['unified_value_score'])
        self.assertEqual(result['summary']['insufficient_fundamentals_count'], 1)

    def test_rebuild_marks_missing_advanced_component_as_insufficient_components(self) -> None:
        service = DailyUnifiedValueFactorService()
        market_date = date(2026, 4, 24)
        inputs = [
            _input(
                '005930',
                market_date,
                per=8.0,
                pbr=1.0,
                dividend_yield=0.03,
                snapshot=_snapshot(
                    '005930',
                    total_equity=1000,
                    total_liabilities=300,
                    operating_income=100,
                    operating_cash_flow=220,
                    cash=100,
                    capex_ppe=-60,
                    depreciation_and_amortization=None,
                    snapshot_id=1,
                ),
            ),
        ]

        with (
            patch.object(service, '_load_inputs', return_value=(inputs, ['005930'], [])),
            patch('app.signals.unified_value_factor.upsert_daily_unified_value_factor_result') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date)

        persisted = upsert.call_args.args[0]
        self.assertEqual(persisted['unified_value_status'], UNIFIED_VALUE_STATUS_INSUFFICIENT_COMPONENTS)
        self.assertEqual(persisted['components_used'], 4)
        self.assertIsNone(persisted['unified_value_score'])
        self.assertEqual(result['summary']['insufficient_components_count'], 1)

    def test_subset_rerun_uses_full_universe_rankings(self) -> None:
        service = DailyUnifiedValueFactorService()
        market_date = date(2026, 4, 24)
        all_inputs = [
            _input(
                '000001',
                market_date,
                per=5.0,
                pbr=0.5,
                dividend_yield=0.05,
                snapshot=_snapshot(
                    '000001',
                    total_equity=1000,
                    total_liabilities=200,
                    operating_income=100,
                    operating_cash_flow=250,
                    cash=100,
                    capex_ppe=-50,
                    depreciation_and_amortization=50,
                    snapshot_id=1,
                ),
            ),
            _input(
                '000002',
                market_date,
                per=10.0,
                pbr=1.0,
                dividend_yield=0.03,
                snapshot=_snapshot(
                    '000002',
                    total_equity=1000,
                    total_liabilities=250,
                    operating_income=100,
                    operating_cash_flow=180,
                    cash=50,
                    capex_ppe=-80,
                    depreciation_and_amortization=50,
                    snapshot_id=2,
                ),
            ),
            _input(
                '000003',
                market_date,
                per=20.0,
                pbr=2.0,
                dividend_yield=0.01,
                snapshot=_snapshot(
                    '000003',
                    total_equity=1000,
                    total_liabilities=300,
                    operating_income=100,
                    operating_cash_flow=70,
                    cash=20,
                    capex_ppe=-50,
                    depreciation_and_amortization=40,
                    snapshot_id=3,
                ),
            ),
        ]

        def load_inputs_side_effect(*, market_date, symbols):
            if symbols is None:
                return (all_inputs, ['000001', '000002', '000003'], [])
            return ([all_inputs[1]], ['000002'], [])

        with (
            patch('app.signals.unified_value_factor.load_symbols', return_value=[{'symbol': '000002'}]),
            patch.object(service, '_load_inputs', side_effect=load_inputs_side_effect),
            patch('app.signals.unified_value_factor.upsert_daily_unified_value_factor_result') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date, symbols=['000002'])

        persisted = upsert.call_args.args[0]
        self.assertEqual(result['results_persisted'], 1)
        self.assertEqual(result['summary']['ranking_universe_counts']['per'], 3)
        self.assertEqual(result['summary']['ranking_universe_counts']['unified_value_score'], 3)
        self.assertEqual(persisted['symbol'], '000002')
        self.assertEqual(persisted['unified_value_rank'], 2)

    def test_inspect_returns_single_item_for_symbol(self) -> None:
        service = DailyUnifiedValueFactorService()
        stored_items = [
            {
                'symbol': '005930',
                'market_date': '2026-04-24',
                'unified_value_status': UNIFIED_VALUE_STATUS_SCORED,
                'unified_value_score': 0.8,
            }
        ]

        with patch(
            'app.signals.unified_value_factor.load_daily_unified_value_factor_results',
            return_value=stored_items,
        ) as loader:
            result = service.inspect(market_date='2026-04-24', symbol='5930', limit=50)

        loader.assert_called_once_with(
            market_date=date(2026, 4, 24),
            symbols=['005930'],
            limit=1,
        )
        self.assertEqual(result['feature_version'], UNIFIED_VALUE_FACTOR_VERSION)
        self.assertEqual(result['summary']['requested_symbol'], '005930')
        self.assertEqual(result['item'], stored_items[0])


class DailyUnifiedValueFactorPersistenceIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        Symbol.__table__.create(bind=engine, checkfirst=True)
        DailyUnifiedValueFactorResult.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        self.service = DailyUnifiedValueFactorService()
        self.market_date = date(2026, 4, 24)
        self.symbols = ['TUV2601', 'TUV2602', 'TUV2603']

    def tearDown(self) -> None:
        with session_scope() as session:
            symbol_ids = list(
                session.execute(select(Symbol.id).where(Symbol.symbol.in_(self.symbols))).scalars().all()
            )
            if symbol_ids:
                session.execute(
                    delete(DailyUnifiedValueFactorResult).where(
                        DailyUnifiedValueFactorResult.symbol_id.in_(symbol_ids)
                    )
                )
            session.execute(delete(Symbol).where(Symbol.symbol.in_(self.symbols)))

    def test_rebuild_persists_and_inspect_reads_single_symbol(self) -> None:
        inputs = [
            _input(
                self.symbols[0],
                self.market_date,
                per=5.0,
                pbr=0.5,
                dividend_yield=0.05,
                snapshot=_snapshot(
                    self.symbols[0],
                    total_equity=1000,
                    total_liabilities=200,
                    operating_income=100,
                    operating_cash_flow=250,
                    cash=100,
                    capex_ppe=-50,
                    depreciation_and_amortization=50,
                    snapshot_id=1,
                ),
            ),
            _input(
                self.symbols[1],
                self.market_date,
                per=10.0,
                pbr=1.0,
                dividend_yield=0.03,
                snapshot=_snapshot(
                    self.symbols[1],
                    total_equity=1000,
                    total_liabilities=250,
                    operating_income=100,
                    operating_cash_flow=180,
                    cash=50,
                    capex_ppe=-80,
                    depreciation_and_amortization=50,
                    snapshot_id=2,
                ),
            ),
            _input(
                self.symbols[2],
                self.market_date,
                per=20.0,
                pbr=2.0,
                dividend_yield=0.01,
                snapshot=_snapshot(
                    self.symbols[2],
                    total_equity=1000,
                    total_liabilities=300,
                    operating_income=100,
                    operating_cash_flow=70,
                    cash=20,
                    capex_ppe=-50,
                    depreciation_and_amortization=40,
                    snapshot_id=3,
                ),
            ),
        ]

        with patch.object(self.service, '_load_inputs', return_value=(inputs, self.symbols, [])):
            result = self.service.rebuild_for_market_date(market_date=self.market_date)

        inspection = self.service.inspect(market_date=self.market_date, symbol=self.symbols[0])
        stored_items = load_daily_unified_value_factor_results(
            market_date=self.market_date,
            symbols=[self.symbols[0]],
        )

        self.assertEqual(result['results_persisted'], 3)
        self.assertEqual(inspection['item']['symbol'], self.symbols[0])
        self.assertTrue(stored_items)
        self.assertEqual(stored_items[0]['symbol'], self.symbols[0])
        self.assertEqual(stored_items[0]['unified_value_status'], UNIFIED_VALUE_STATUS_SCORED)
        self.assertEqual(stored_items[0]['components_used'], 5)
