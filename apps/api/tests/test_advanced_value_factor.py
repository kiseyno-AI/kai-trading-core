import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy import delete, select, text

from app.db import engine, session_scope
from app.db_models import (
    DailyAdvancedValueFactorResult,
    DartDisclosureEvent,
    DartDisclosureSyncRun,
    DartFundamentalsCaptureRun,
    DartFundamentalsHighlight,
    DartFundamentalsRow,
    DartFundamentalsSnapshot,
    Symbol,
)
from app.market_data_store import load_daily_advanced_value_factor_results
from app.signals.advanced_value_factor import (
    ADVANCED_VALUE_ELIGIBLE_SCORE,
    ADVANCED_VALUE_FACTOR_VERSION,
    ADVANCED_VALUE_WATCHLIST_SCORE,
    AnnualFundamentalsSnapshot,
    AdvancedValueFactorInput,
    DailyAdvancedValueFactorService,
    METRIC_STATUS_AVAILABLE,
    METRIC_STATUS_INVALID_NON_POSITIVE_EBITDA,
    derive_annual_financial_context,
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
    depreciation: int | None = None,
    amortization: int | None = None,
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
    if depreciation is not None:
        rows.append(
            _statement_row(
                'depreciation',
                statement_type='CF',
                account_id=None,
                account_name='감가상각비',
                amount=depreciation,
                row_order=5,
            )
        )
    if amortization is not None:
        rows.append(
            _statement_row(
                'amortization',
                statement_type='CF',
                account_id=None,
                account_name='무형자산상각비',
                amount=amortization,
                row_order=6,
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


def _bars(market_date: date, *, pbr: float) -> list[dict]:
    return [
        {
            'trade_date': market_date.isoformat(),
            'open_price': 100000,
            'high_price': 101000,
            'low_price': 99000,
            'close_price': 100000,
            'volume': 1000000,
            'turnover': 100000000,
            'pbr': pbr,
            'source': 'pykrx',
            'captured_at': f'{market_date.isoformat()}T15:30:00+09:00',
        }
    ]


def _input(symbol: str, market_date: date, *, pbr: float, snapshot: AnnualFundamentalsSnapshot | None) -> AdvancedValueFactorInput:
    return AdvancedValueFactorInput(
        symbol=symbol,
        name_ko=symbol,
        market='KOSPI',
        bars=_bars(market_date, pbr=pbr),
        annual_fundamentals=snapshot,
    )


class AdvancedValueFactorMathTests(unittest.TestCase):
    def test_derive_annual_financial_context_uses_highlights_and_statement_row_proxies(self) -> None:
        snapshot = _snapshot(
            '005930',
            total_equity=1000,
            total_liabilities=400,
            operating_income=120,
            operating_cash_flow=260,
            cash=150,
            capex_ppe=-80,
            capex_intangible=-20,
            depreciation_and_amortization=35,
        )

        context = derive_annual_financial_context(snapshot)

        self.assertEqual(context['total_equity'], 1000.0)
        self.assertEqual(context['cash_and_cash_equivalents'], 150.0)
        self.assertEqual(context['capital_expenditure'], 100.0)
        self.assertEqual(context['depreciation_and_amortization'], 35.0)
        self.assertEqual(context['audit']['annual_snapshot']['snapshot_id'], 1)
        self.assertEqual(
            context['audit']['matched_inputs']['capex_ppe']['account_name'],
            '유형자산의 취득',
        )

    def test_derive_annual_financial_context_excludes_disposal_rows_from_capex_matching(self) -> None:
        snapshot = _snapshot(
            '005930',
            total_equity=1000,
            total_liabilities=400,
            operating_income=120,
            operating_cash_flow=260,
            cash=150,
            capex_ppe=None,
            depreciation_and_amortization=35,
        )
        snapshot.statement_rows.append(
            _statement_row(
                'capex_ppe_disposal',
                statement_type='CF',
                account_id='ifrs-full_PurchaseOfPropertyPlantAndEquipment',
                account_name='유형자산의 처분',
                amount=80,
                row_order=2,
            )
        )

        context = derive_annual_financial_context(snapshot)

        self.assertIsNone(context['capital_expenditure'])
        self.assertIsNone(context['audit']['matched_inputs']['capex_ppe'])

    def test_derive_annual_financial_context_sums_separate_depreciation_and_amortization_rows(self) -> None:
        snapshot = _snapshot(
            '000660',
            total_equity=1000,
            total_liabilities=300,
            operating_income=110,
            operating_cash_flow=200,
            cash=90,
            capex_ppe=-60,
            depreciation=None,
            amortization=None,
            depreciation_and_amortization=None,
        )
        snapshot.statement_rows.extend(
            [
                _statement_row(
                    'dep',
                    statement_type='CF',
                    account_id=None,
                    account_name='감가상각비',
                    amount=25,
                    row_order=4,
                ),
                _statement_row(
                    'amo',
                    statement_type='CF',
                    account_id=None,
                    account_name='무형자산상각비',
                    amount=15,
                    row_order=5,
                ),
            ]
        )

        context = derive_annual_financial_context(snapshot)

        self.assertEqual(context['depreciation_and_amortization'], 40.0)


class DailyAdvancedValueFactorServiceTests(unittest.TestCase):
    def test_rebuild_scores_higher_fcf_yield_and_lower_ev_to_ebitda_as_better(self) -> None:
        service = DailyAdvancedValueFactorService()
        market_date = date(2026, 4, 24)
        inputs = [
            _input(
                '000001',
                market_date,
                pbr=1.0,
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
                pbr=1.0,
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
                pbr=1.0,
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
            patch('app.signals.advanced_value_factor.upsert_daily_advanced_value_factor_result') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date)

        self.assertEqual(result['feature_version'], ADVANCED_VALUE_FACTOR_VERSION)
        self.assertEqual(result['results_persisted'], 3)
        self.assertEqual(upsert.call_count, 3)
        persisted_by_symbol = {call.args[0]['symbol']: call.args[0] for call in upsert.call_args_list}

        best = persisted_by_symbol['000001']
        middle = persisted_by_symbol['000002']
        worst = persisted_by_symbol['000003']

        self.assertEqual(best['fcf_yield_rank'], 1)
        self.assertEqual(best['ev_to_ebitda_rank'], 1)
        self.assertEqual(best['advanced_value_rank'], 1)
        self.assertEqual(best['advanced_value_percentile'], 1.0)
        self.assertTrue(best['is_advanced_value_eligible'])
        self.assertGreaterEqual(best['advanced_value_score'], ADVANCED_VALUE_ELIGIBLE_SCORE)

        self.assertEqual(middle['fcf_yield_rank'], 2)
        self.assertEqual(middle['ev_to_ebitda_rank'], 2)
        self.assertEqual(middle['advanced_value_classification'], 'watchlist')
        self.assertAlmostEqual(middle['advanced_value_score'], ADVANCED_VALUE_WATCHLIST_SCORE, places=6)

        self.assertEqual(worst['fcf_yield_rank'], 3)
        self.assertEqual(worst['ev_to_ebitda_rank'], 3)
        self.assertEqual(worst['advanced_value_classification'], 'ineligible')
        self.assertFalse(worst['is_advanced_value_eligible'])
        self.assertEqual(result['summary']['eligible_count'], 1)
        self.assertEqual(result['summary']['watchlist_count'], 1)

    def test_rebuild_treats_negative_fcf_yield_as_valid_but_unattractive(self) -> None:
        service = DailyAdvancedValueFactorService()
        market_date = date(2026, 4, 24)
        inputs = [
            _input(
                '000001',
                market_date,
                pbr=1.0,
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
                pbr=1.0,
                snapshot=_snapshot(
                    '000002',
                    total_equity=1000,
                    total_liabilities=210,
                    operating_income=100,
                    operating_cash_flow=20,
                    cash=90,
                    capex_ppe=-80,
                    depreciation_and_amortization=50,
                    snapshot_id=2,
                ),
            ),
        ]

        with (
            patch.object(service, '_load_inputs', return_value=(inputs, ['000001', '000002'], [])),
            patch('app.signals.advanced_value_factor.upsert_daily_advanced_value_factor_result') as upsert,
        ):
            service.rebuild_for_market_date(market_date=market_date)

        persisted_by_symbol = {call.args[0]['symbol']: call.args[0] for call in upsert.call_args_list}
        positive = persisted_by_symbol['000001']
        negative = persisted_by_symbol['000002']

        self.assertEqual(negative['fcf_yield_status'], METRIC_STATUS_AVAILABLE)
        self.assertTrue(negative['fcf_yield_valid_for_scoring'])
        self.assertLess(negative['fcf_yield'], 0.0)
        self.assertEqual(negative['fcf_yield_rank'], 2)
        self.assertEqual(positive['fcf_yield_rank'], 1)

    def test_rebuild_marks_negative_ebitda_as_invalid_for_scoring(self) -> None:
        service = DailyAdvancedValueFactorService()
        market_date = date(2026, 4, 24)
        inputs = [
            _input(
                '005930',
                market_date,
                pbr=1.0,
                snapshot=_snapshot(
                    '005930',
                    total_equity=1000,
                    total_liabilities=300,
                    operating_income=-50,
                    operating_cash_flow=200,
                    cash=100,
                    capex_ppe=-60,
                    depreciation_and_amortization=10,
                    snapshot_id=1,
                ),
            ),
        ]

        with (
            patch.object(service, '_load_inputs', return_value=(inputs, ['005930'], [])),
            patch('app.signals.advanced_value_factor.upsert_daily_advanced_value_factor_result') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date)

        persisted = upsert.call_args.args[0]
        self.assertEqual(persisted['ev_to_ebitda_status'], METRIC_STATUS_INVALID_NON_POSITIVE_EBITDA)
        self.assertFalse(persisted['ev_to_ebitda_valid_for_scoring'])
        self.assertIsNone(persisted['advanced_value_score'])
        self.assertEqual(persisted['advanced_value_classification'], 'insufficient_components')
        self.assertEqual(result['summary']['insufficient_components_count'], 1)

    def test_rebuild_persists_missing_fundamentals_as_insufficient_fundamentals(self) -> None:
        service = DailyAdvancedValueFactorService()
        market_date = date(2026, 4, 24)
        inputs = [
            _input(
                '035420',
                market_date,
                pbr=1.0,
                snapshot=None,
            ),
        ]

        with (
            patch.object(service, '_load_inputs', return_value=(inputs, ['035420'], [])),
            patch('app.signals.advanced_value_factor.upsert_daily_advanced_value_factor_result') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date)

        persisted = upsert.call_args.args[0]
        self.assertEqual(persisted['advanced_value_classification'], 'insufficient_fundamentals')
        self.assertIsNone(persisted['advanced_value_score'])
        self.assertFalse(persisted['is_advanced_value_eligible'])
        self.assertEqual(result['summary']['insufficient_fundamentals_count'], 1)

    def test_subset_rerun_uses_full_universe_rankings(self) -> None:
        service = DailyAdvancedValueFactorService()
        market_date = date(2026, 4, 24)
        all_inputs = [
            _input(
                '000001',
                market_date,
                pbr=1.0,
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
                pbr=1.0,
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
                pbr=1.0,
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
            patch.object(service, '_load_inputs', side_effect=load_inputs_side_effect),
            patch('app.signals.advanced_value_factor.upsert_daily_advanced_value_factor_result') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date, symbols=['000002'])

        persisted = upsert.call_args.args[0]
        self.assertEqual(result['results_persisted'], 1)
        self.assertEqual(result['summary']['ranking_universe_counts']['fcf_yield'], 3)
        self.assertEqual(result['summary']['ranking_universe_counts']['advanced_value_score'], 3)
        self.assertEqual(persisted['symbol'], '000002')
        self.assertEqual(persisted['fcf_yield_rank'], 2)
        self.assertEqual(persisted['ev_to_ebitda_rank'], 2)
        self.assertEqual(persisted['advanced_value_rank'], 2)

    def test_subset_rerun_short_circuits_when_requested_symbols_do_not_resolve(self) -> None:
        service = DailyAdvancedValueFactorService()
        market_date = date(2026, 4, 24)

        with (
            patch.object(service, '_load_inputs', return_value=([], [], ['999999'])) as load_inputs,
            patch('app.signals.advanced_value_factor.upsert_daily_advanced_value_factor_result') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date, symbols=['999999'])

        load_inputs.assert_called_once_with(
            market_date=market_date,
            symbols=['999999'],
        )
        upsert.assert_not_called()
        self.assertEqual(result['requested_symbols'], ['999999'])
        self.assertEqual(result['matched_symbols'], [])
        self.assertEqual(result['missing_symbols'], ['999999'])
        self.assertEqual(result['results_persisted'], 0)
        self.assertEqual(result['results_failed'], 0)
        self.assertEqual(result['items'], [])
        self.assertEqual(
            result['summary'],
            {
                'requested_symbol_count': 1,
                'matched_symbol_count': 0,
                'missing_symbol_count': 1,
                'symbols_with_market_date_bar': 0,
                'symbols_with_annual_fundamentals': 0,
                'fcf_yield_valid_count': 0,
                'ev_to_ebitda_valid_count': 0,
                'eligible_count': 0,
                'watchlist_count': 0,
                'ineligible_count': 0,
                'insufficient_components_count': 0,
                'insufficient_fundamentals_count': 0,
                'missing_market_date_bar_count': 0,
                'ranking_universe_counts': {
                    'fcf_yield': 0,
                    'ev_to_ebitda': 0,
                    'advanced_value_score': 0,
                },
            },
        )

    def test_inspect_returns_single_item_for_symbol(self) -> None:
        service = DailyAdvancedValueFactorService()
        stored_items = [
            {
                'symbol': '005930',
                'market_date': '2026-04-24',
                'advanced_value_classification': 'eligible',
                'is_advanced_value_eligible': True,
            }
        ]

        with patch(
            'app.signals.advanced_value_factor.load_daily_advanced_value_factor_results',
            return_value=stored_items,
        ) as loader:
            result = service.inspect(market_date='2026-04-24', symbol='5930', limit=25)

        loader.assert_called_once_with(
            market_date=date(2026, 4, 24),
            symbols=['005930'],
            limit=1,
        )
        self.assertEqual(result['feature_version'], ADVANCED_VALUE_FACTOR_VERSION)
        self.assertEqual(result['summary']['requested_symbol'], '005930')
        self.assertEqual(result['item'], stored_items[0])


class _FakeLoaderResult:
    def __init__(self, rows: list) -> None:
        self.rows = rows

    def mappings(self):
        return self

    def all(self):
        return self.rows

    def scalars(self):
        return self.rows


class _FakeSessionScope:
    def __init__(self, session) -> None:
        self.session = session

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc, traceback) -> bool:
        return False


class _FakeFundamentalsLoaderSession:
    def __init__(self, symbols: list[str]) -> None:
        self.snapshot_rows_by_symbol = {
            symbol: {
                'snapshot_id': index + 1,
                'stock_code': symbol,
                'corp_name': f'Test Corp {symbol}',
                'business_year': 2025,
                'report_code': '11011',
                'fs_div': 'CFS',
                'period_type': 'annual',
                'receipt_date': date(2026, 3, 20),
                'rcept_no': f'TAV{index + 1:012d}',
                'captured_at': None,
                'currency': 'KRW',
            }
            for index, symbol in enumerate(symbols)
        }
        self.highlight_chunks: list[list[int]] = []
        self.statement_row_chunks: list[list[int]] = []

    def execute(self, statement, params=None):
        if params and any(str(key).startswith('symbol_') for key in params):
            requested_symbols = [
                params[key]
                for key in sorted(params)
                if str(key).startswith('symbol_')
            ]
            return _FakeLoaderResult(
                [
                    self.snapshot_rows_by_symbol[symbol]
                    for symbol in requested_symbols
                    if symbol in self.snapshot_rows_by_symbol
                ]
            )

        entity = None
        column_descriptions = getattr(statement, 'column_descriptions', None)
        if column_descriptions:
            entity = column_descriptions[0].get('entity')
        if entity in {DartFundamentalsHighlight, DartFundamentalsRow}:
            snapshot_ids = next(
                value
                for value in statement.compile().params.values()
                if isinstance(value, list)
            )
            if entity is DartFundamentalsHighlight:
                self.highlight_chunks.append(list(snapshot_ids))
                return _FakeLoaderResult(
                    [
                        SimpleNamespace(
                            snapshot_id=snapshot_id,
                            metric_key='total_equity',
                            metric_type='balance',
                            statement_type='BS',
                            statement_name='financial_statement',
                            account_id='ifrs-full_Equity',
                            account_name='자본총계',
                            current_amount=1000 + snapshot_id,
                            current_cumulative_amount=None,
                            ratio_value=None,
                            currency='KRW',
                        )
                        for snapshot_id in snapshot_ids
                    ]
                )
            self.statement_row_chunks.append(list(snapshot_ids))
            return _FakeLoaderResult([])

        return _FakeLoaderResult([])


class DailyAdvancedValueFactorFundamentalsLoaderTests(unittest.TestCase):
    def test_load_latest_annual_fundamentals_loads_highlights_for_large_universe_without_rows(self) -> None:
        service = DailyAdvancedValueFactorService()
        symbols = [f'TAVH{index:04d}' for index in range(501)]
        fake_session = _FakeFundamentalsLoaderSession(symbols)

        with patch(
            'app.signals.advanced_value_factor.session_scope',
            return_value=_FakeSessionScope(fake_session),
        ):
            result = service._load_latest_annual_fundamentals(
                market_date=date(2026, 4, 24),
                symbols=symbols,
            )

        self.assertEqual(len(result), len(symbols))
        self.assertEqual([len(chunk) for chunk in fake_session.highlight_chunks], [500, 1])
        self.assertEqual(fake_session.statement_row_chunks, [])
        self.assertEqual(result[symbols[0]].highlights_by_key['total_equity']['current_amount'], 1001)
        self.assertEqual(result[symbols[-1]].highlights_by_key['total_equity']['current_amount'], 1501)
        self.assertEqual(result[symbols[0]].statement_rows, [])


class DailyAdvancedValueFactorFundamentalsLoaderIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        DartDisclosureSyncRun.__table__.create(bind=engine, checkfirst=True)
        DartFundamentalsCaptureRun.__table__.create(bind=engine, checkfirst=True)
        DartDisclosureEvent.__table__.create(bind=engine, checkfirst=True)
        DartFundamentalsSnapshot.__table__.create(bind=engine, checkfirst=True)
        DartFundamentalsHighlight.__table__.create(bind=engine, checkfirst=True)
        DartFundamentalsRow.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        self.service = DailyAdvancedValueFactorService()
        self.created_event_ids: list[int] = []
        self.created_snapshot_ids: list[int] = []

    def tearDown(self) -> None:
        if not self.created_event_ids and not self.created_snapshot_ids:
            return
        with session_scope() as session:
            session.execute(text("SET LOCAL statement_timeout = '60s'"))
            if self.created_snapshot_ids:
                session.execute(
                    delete(DartFundamentalsHighlight).where(
                        DartFundamentalsHighlight.snapshot_id.in_(self.created_snapshot_ids)
                    )
                )
                session.execute(
                    delete(DartFundamentalsRow).where(
                        DartFundamentalsRow.snapshot_id.in_(self.created_snapshot_ids)
                    )
                )
                session.execute(
                    delete(DartFundamentalsSnapshot).where(
                        DartFundamentalsSnapshot.id.in_(self.created_snapshot_ids)
                    )
                )
            if self.created_event_ids:
                session.execute(
                    delete(DartDisclosureEvent).where(DartDisclosureEvent.id.in_(self.created_event_ids))
                )

    def _create_fundamentals_snapshot(
        self,
        *,
        stock_code: str,
        business_year: int,
        receipt_date: date,
        total_equity: int,
    ) -> int:
        suffix = uuid4().hex[:16].upper()
        rcept_no = f'TAV{suffix}'
        corp_code = f'C{suffix[:12]}'
        with session_scope() as session:
            session.execute(text("SET LOCAL statement_timeout = '60s'"))
            event = DartDisclosureEvent(
                rcept_no=rcept_no,
                corp_code=corp_code,
                corp_name=f'Test Corp {stock_code}',
                stock_code=stock_code,
                corp_cls='Y',
                report_nm=f'Annual Report ({business_year}.12)',
                filing_category='annual_report',
                is_financial_report=True,
                receipt_date=receipt_date,
                payload={'rcept_no': rcept_no},
                fundamentals_capture_status='captured',
            )
            session.add(event)
            session.flush()

            snapshot = DartFundamentalsSnapshot(
                disclosure_event_id=event.id,
                capture_run_id=None,
                rcept_no=rcept_no,
                corp_code=corp_code,
                stock_code=stock_code,
                corp_name=f'Test Corp {stock_code}',
                business_year=business_year,
                report_code='11011',
                filing_category='annual_report',
                fs_div='CFS',
                statement_type='financial_statement',
                period_type='annual',
                period_end_month=12,
                currency='KRW',
                source_status='000',
                source_message='OK',
                request_payload={},
                response_payload={},
            )
            session.add(snapshot)
            session.flush()

            session.add(
                DartFundamentalsHighlight(
                    snapshot_id=snapshot.id,
                    metric_key='total_equity',
                    metric_type='balance',
                    statement_type='BS',
                    statement_name='financial_statement',
                    account_id='ifrs-full_Equity',
                    account_name='자본총계',
                    current_amount=total_equity,
                    current_cumulative_amount=None,
                    currency='KRW',
                )
            )
            event_id = event.id
            snapshot_id = snapshot.id

        self.created_event_ids.append(event_id)
        self.created_snapshot_ids.append(snapshot_id)
        return snapshot_id

    def test_load_latest_annual_fundamentals_respects_receipt_date_for_historical_market_date(self) -> None:
        stock_code = f'TAVD{uuid4().hex[:8].upper()}'
        older_snapshot_id = self._create_fundamentals_snapshot(
            stock_code=stock_code,
            business_year=2024,
            receipt_date=date(2025, 3, 20),
            total_equity=900,
        )
        newer_snapshot_id = self._create_fundamentals_snapshot(
            stock_code=stock_code,
            business_year=2025,
            receipt_date=date(2026, 3, 20),
            total_equity=1200,
        )

        before_filing = self.service._load_latest_annual_fundamentals(
            market_date=date(2026, 2, 15),
            symbols=[stock_code],
        )
        after_filing = self.service._load_latest_annual_fundamentals(
            market_date=date(2026, 3, 20),
            symbols=[stock_code],
        )

        self.assertEqual(before_filing[stock_code].snapshot_id, older_snapshot_id)
        self.assertEqual(before_filing[stock_code].business_year, 2024)
        self.assertEqual(before_filing[stock_code].receipt_date, '2025-03-20')
        self.assertEqual(before_filing[stock_code].highlights_by_key['total_equity']['current_amount'], 900)
        self.assertEqual(after_filing[stock_code].snapshot_id, newer_snapshot_id)
        self.assertEqual(after_filing[stock_code].business_year, 2025)
        self.assertEqual(after_filing[stock_code].receipt_date, '2026-03-20')


class DailyAdvancedValueFactorPersistenceIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        DailyAdvancedValueFactorResult.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        self.service = DailyAdvancedValueFactorService()
        self.market_date = date(2026, 4, 24)
        self.symbols = ['TAV1601', 'TAV1602', 'TAV1603']

    def tearDown(self) -> None:
        with session_scope() as session:
            symbol_ids = list(
                session.execute(select(Symbol.id).where(Symbol.symbol.in_(self.symbols))).scalars().all()
            )
            if symbol_ids:
                session.execute(
                    delete(DailyAdvancedValueFactorResult).where(
                        DailyAdvancedValueFactorResult.symbol_id.in_(symbol_ids)
                    )
                )
            session.execute(delete(Symbol).where(Symbol.symbol.in_(self.symbols)))

    def test_rebuild_persists_and_inspect_reads_single_symbol(self) -> None:
        inputs = [
            _input(
                self.symbols[0],
                self.market_date,
                pbr=1.0,
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
                pbr=1.0,
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
                pbr=1.0,
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
        stored_items = load_daily_advanced_value_factor_results(
            market_date=self.market_date,
            symbols=[self.symbols[0]],
        )

        self.assertEqual(result['results_persisted'], 3)
        self.assertEqual(inspection['item']['symbol'], self.symbols[0])
        self.assertTrue(stored_items)
        self.assertEqual(stored_items[0]['symbol'], self.symbols[0])
        self.assertEqual(stored_items[0]['advanced_value_classification'], 'eligible')
        self.assertTrue(stored_items[0]['is_advanced_value_eligible'])
