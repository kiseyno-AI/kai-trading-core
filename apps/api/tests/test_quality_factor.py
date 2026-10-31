import unittest
from datetime import date
from unittest.mock import patch

from sqlalchemy import delete, select

from app.db import engine, session_scope
from app.db_models import DailyQualityFactorResult, Symbol
from app.market_data_store import load_daily_quality_factor_results
from app.signals.advanced_value_factor import AnnualFundamentalsSnapshot, AdvancedValueFactorInput
from app.signals.piotroski_filter import INDEX_ASSET_TURNOVER, INDEX_DEBT_RATIO, INDEX_GROSS_MARGIN
from app.signals.quality_factor import (
    DailyQualityFactorService,
    QUALITY_FACTOR_VERSION,
    QUALITY_STATUS_INSUFFICIENT_FUNDAMENTALS,
    QUALITY_STATUS_SCORED_COMPLETE,
    QUALITY_STATUS_SCORED_PARTIAL,
)


def _highlight_row(
    metric_key: str,
    *,
    statement_type: str,
    account_id: str | None,
    account_name: str,
    amount: int | None = None,
    ratio_value: float | None = None,
) -> dict:
    return {
        'metric_key': metric_key,
        'statement_type': statement_type,
        'statement_name': 'financial_statement',
        'account_id': account_id,
        'account_name': account_name,
        'current_amount': amount,
        'current_cumulative_amount': None,
        'ratio_value': ratio_value,
        'currency': 'KRW',
    }


def _statement_row(
    row_key: str,
    *,
    statement_type: str,
    account_id: str,
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
    net_income: int,
    total_assets: int,
    operating_cash_flow: int,
    gross_margin: float | None,
    asset_turnover: float | None,
    debt_ratio: float | None,
    snapshot_id: int = 1,
) -> AnnualFundamentalsSnapshot:
    highlights_by_key = {
        'total_equity': _highlight_row(
            'total_equity',
            statement_type='BS',
            account_id='ifrs-full_Equity',
            account_name='자본총계',
            amount=total_equity,
        ),
        'total_liabilities': _highlight_row(
            'total_liabilities',
            statement_type='BS',
            account_id='ifrs-full_Liabilities',
            account_name='부채총계',
            amount=total_liabilities,
        ),
        'net_income': _highlight_row(
            'net_income',
            statement_type='CIS',
            account_id='ifrs-full_ProfitLoss',
            account_name='당기순이익',
            amount=net_income,
        ),
        'total_assets': _highlight_row(
            'total_assets',
            statement_type='BS',
            account_id='ifrs-full_Assets',
            account_name='자산총계',
            amount=total_assets,
        ),
        'operating_cash_flow': _highlight_row(
            'operating_cash_flow',
            statement_type='CF',
            account_id='ifrs-full_CashFlowsFromUsedInOperatingActivities',
            account_name='영업활동순현금흐름',
            amount=operating_cash_flow,
        ),
    }
    if gross_margin is not None:
        highlights_by_key[INDEX_GROSS_MARGIN] = _highlight_row(
            INDEX_GROSS_MARGIN,
            statement_type='RATIO',
            account_id=None,
            account_name='매출총이익률',
            ratio_value=gross_margin,
        )
    if asset_turnover is not None:
        highlights_by_key[INDEX_ASSET_TURNOVER] = _highlight_row(
            INDEX_ASSET_TURNOVER,
            statement_type='RATIO',
            account_id=None,
            account_name='총자산회전율',
            ratio_value=asset_turnover,
        )
    if debt_ratio is not None:
        highlights_by_key[INDEX_DEBT_RATIO] = _highlight_row(
            INDEX_DEBT_RATIO,
            statement_type='RATIO',
            account_id=None,
            account_name='부채비율',
            ratio_value=debt_ratio,
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
        highlights_by_key=highlights_by_key,
        statement_rows=[],
    )


def _bars(market_date: date) -> list[dict]:
    return [
        {
            'trade_date': market_date.isoformat(),
            'open_price': 100000,
            'high_price': 101000,
            'low_price': 99000,
            'close_price': 100000,
            'volume': 1000000,
            'turnover': 100000000,
            'source': 'pykrx',
            'captured_at': f'{market_date.isoformat()}T15:30:00+09:00',
        }
    ]


def _input(
    symbol: str,
    market_date: date,
    *,
    snapshot: AnnualFundamentalsSnapshot | None,
) -> AdvancedValueFactorInput:
    return AdvancedValueFactorInput(
        symbol=symbol,
        name_ko=symbol,
        market='KOSPI',
        bars=_bars(market_date),
        annual_fundamentals=snapshot,
    )


class DailyQualityFactorServiceTests(unittest.TestCase):
    def test_negative_equity_cannot_rank_as_low_leverage(self) -> None:
        service = DailyQualityFactorService()
        market_date = date(2026, 4, 24)
        snapshot = _snapshot(
            '005930',
            total_equity=-100,
            total_liabilities=200,
            net_income=20,
            total_assets=100,
            operating_cash_flow=30,
            gross_margin=0.4,
            asset_turnover=1.0,
            debt_ratio=-2.0,
        )

        result = service._build_symbol_result(
            input_row=_input('005930', market_date, snapshot=snapshot),
            market_date=market_date,
            source=QUALITY_FACTOR_VERSION,
        )

        self.assertIsNone(result['debt_ratio'])
        self.assertEqual(result['debt_ratio_status'], 'invalid_equity')

    def test_rebuild_derives_gross_profit_to_assets_from_canonical_raw_facts_with_provenance(self) -> None:
        service = DailyQualityFactorService()
        market_date = date(2026, 4, 24)
        snapshot = _snapshot(
            '005930',
            total_equity=800,
            total_liabilities=160,
            net_income=240,
            total_assets=1_000,
            operating_cash_flow=300,
            gross_margin=None,
            asset_turnover=None,
            debt_ratio=None,
        )
        snapshot.highlights_by_key['gross_profit'] = _highlight_row(
            'gross_profit',
            statement_type='CIS',
            account_id='ifrs-full_GrossProfit',
            account_name='매출총이익',
            amount=250,
        )
        snapshot.highlights_by_key['debt_ratio'] = _highlight_row(
            'debt_ratio',
            statement_type='RATIO',
            account_id=None,
            account_name='부채비율',
            ratio_value=0.2,
        )
        inputs = [_input('005930', market_date, snapshot=snapshot)]

        with (
            patch.object(service, '_load_inputs', return_value=(inputs, ['005930'], [])),
            patch('app.signals.quality_factor.upsert_daily_quality_factor_results') as upsert,
        ):
            service.rebuild_for_market_date(market_date=market_date)

        persisted = upsert.call_args.args[0][0]
        self.assertEqual(persisted['gross_profit_to_assets'], 0.25)
        self.assertEqual(persisted['debt_ratio'], 0.2)
        gross_profit_audit = persisted['factor_payload']['audit']['matched_highlights']['gross_profit_to_assets']
        self.assertEqual(gross_profit_audit['source_type'], 'derived_formula')
        self.assertEqual(gross_profit_audit['formula'], 'gross_profit / total_assets')
        self.assertEqual(gross_profit_audit['gross_profit']['metric_key'], 'gross_profit')
        self.assertEqual(gross_profit_audit['total_assets']['metric_key'], 'total_assets')

    def test_build_uses_statement_rows_when_canonical_quality_highlights_are_missing(self) -> None:
        service = DailyQualityFactorService()
        market_date = date(2026, 4, 24)
        snapshot = _snapshot(
            '005930',
            total_equity=800,
            total_liabilities=160,
            net_income=240,
            total_assets=1_000,
            operating_cash_flow=300,
            gross_margin=None,
            asset_turnover=None,
            debt_ratio=None,
        )
        snapshot.highlights_by_key.pop('net_income')
        snapshot.highlights_by_key.pop('total_assets')
        snapshot.statement_rows.extend(
            [
                _statement_row(
                    'profit_loss',
                    statement_type='CIS',
                    account_id='ifrs-full_ProfitLoss',
                    account_name='당기순이익',
                    amount=240,
                    row_order=1,
                ),
                _statement_row(
                    'assets',
                    statement_type='BS',
                    account_id='ifrs-full_Assets',
                    account_name='자산총계',
                    amount=1_000,
                    row_order=2,
                ),
                _statement_row(
                    'gross_profit',
                    statement_type='CIS',
                    account_id='ifrs-full_GrossProfit',
                    account_name='매출총이익',
                    amount=250,
                    row_order=3,
                ),
            ]
        )

        result = service._build_symbol_result(
            input_row=_input('005930', market_date, snapshot=snapshot),
            market_date=market_date,
            source=QUALITY_FACTOR_VERSION,
        )

        self.assertEqual(result['roe'], 0.3)
        self.assertEqual(result['roa'], 0.24)
        self.assertEqual(result['gross_profit_to_assets'], 0.25)
        self.assertEqual(result['_net_income_source']['source_type'], 'statement_row')
        self.assertEqual(result['_total_assets_source']['source_type'], 'statement_row')
        self.assertEqual(
            result['_gross_profit_to_assets_source']['gross_profit']['source_type'],
            'statement_row',
        )

    def test_rebuild_scores_complete_universe_and_uses_required_accruals_formula(self) -> None:
        service = DailyQualityFactorService()
        market_date = date(2026, 4, 24)
        inputs = [
            _input(
                '000001',
                market_date,
                snapshot=_snapshot(
                    '000001',
                    total_equity=800,
                    total_liabilities=160,
                    net_income=240,
                    total_assets=1000,
                    operating_cash_flow=300,
                    gross_margin=0.50,
                    asset_turnover=1.40,
                    debt_ratio=0.20,
                    snapshot_id=1,
                ),
            ),
            _input(
                '000002',
                market_date,
                snapshot=_snapshot(
                    '000002',
                    total_equity=900,
                    total_liabilities=450,
                    net_income=180,
                    total_assets=1200,
                    operating_cash_flow=180,
                    gross_margin=0.35,
                    asset_turnover=1.00,
                    debt_ratio=0.50,
                    snapshot_id=2,
                ),
            ),
            _input(
                '000003',
                market_date,
                snapshot=_snapshot(
                    '000003',
                    total_equity=1000,
                    total_liabilities=800,
                    net_income=120,
                    total_assets=1500,
                    operating_cash_flow=60,
                    gross_margin=0.20,
                    asset_turnover=0.80,
                    debt_ratio=0.80,
                    snapshot_id=3,
                ),
            ),
        ]

        with (
            patch.object(service, '_load_inputs', return_value=(inputs, ['000001', '000002', '000003'], [])),
            patch('app.signals.quality_factor.upsert_daily_quality_factor_results') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date)

        self.assertEqual(result['feature_version'], QUALITY_FACTOR_VERSION)
        self.assertEqual(result['results_persisted'], 3)
        upsert.assert_called_once()
        persisted_by_symbol = {item['symbol']: item for item in upsert.call_args.args[0]}

        best = persisted_by_symbol['000001']
        middle = persisted_by_symbol['000002']
        worst = persisted_by_symbol['000003']

        self.assertEqual(best['quality_status'], QUALITY_STATUS_SCORED_COMPLETE)
        self.assertEqual(best['components_possible'], 6)
        self.assertEqual(best['components_used'], 6)
        self.assertTrue(best['is_quality_score_complete'])
        self.assertEqual(best['quality_rank'], 1)
        self.assertEqual(best['quality_percentile'], 1.0)
        self.assertEqual(best['roe_rank'], 1)
        self.assertEqual(best['debt_ratio_rank'], 1)
        self.assertAlmostEqual(best['accruals_ratio'], -0.06, places=6)

        self.assertEqual(middle['quality_rank'], 2)
        self.assertAlmostEqual(middle['quality_score'], 0.5, places=6)

        self.assertEqual(worst['quality_rank'], 3)
        self.assertEqual(worst['quality_percentile'], 0.0)
        self.assertEqual(result['summary']['scored_complete_count'], 3)
        self.assertEqual(result['summary']['ranking_universe_counts']['quality_score'], 3)

    def test_rebuild_allows_partial_scoring_with_component_accounting(self) -> None:
        service = DailyQualityFactorService()
        market_date = date(2026, 4, 24)
        inputs = [
            _input(
                '005930',
                market_date,
                snapshot=_snapshot(
                    '005930',
                    total_equity=1200,
                    total_liabilities=300,
                    net_income=180,
                    total_assets=1600,
                    operating_cash_flow=220,
                    gross_margin=None,
                    asset_turnover=1.10,
                    debt_ratio=None,
                    snapshot_id=1,
                ),
            ),
        ]

        with (
            patch.object(service, '_load_inputs', return_value=(inputs, ['005930'], [])),
            patch('app.signals.quality_factor.upsert_daily_quality_factor_results') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date)

        persisted = upsert.call_args.args[0][0]
        self.assertEqual(persisted['quality_status'], QUALITY_STATUS_SCORED_PARTIAL)
        self.assertEqual(persisted['components_possible'], 6)
        self.assertEqual(persisted['components_used'], 5)
        self.assertFalse(persisted['is_quality_score_complete'])
        self.assertIsNotNone(persisted['quality_score'])
        self.assertEqual(result['summary']['scored_partial_count'], 1)

    def test_rebuild_marks_missing_fundamentals_as_insufficient_fundamentals(self) -> None:
        service = DailyQualityFactorService()
        market_date = date(2026, 4, 24)
        inputs = [
            _input(
                '035420',
                market_date,
                snapshot=None,
            ),
        ]

        with (
            patch.object(service, '_load_inputs', return_value=(inputs, ['035420'], [])),
            patch('app.signals.quality_factor.upsert_daily_quality_factor_results') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date)

        persisted = upsert.call_args.args[0][0]
        self.assertEqual(persisted['quality_status'], QUALITY_STATUS_INSUFFICIENT_FUNDAMENTALS)
        self.assertEqual(persisted['components_used'], 0)
        self.assertIsNone(persisted['quality_score'])
        self.assertEqual(result['summary']['insufficient_fundamentals_count'], 1)
        self.assertEqual(result['summary']['ready_count'], 0)
        self.assertEqual(result['summary']['unready_count'], 1)

    def test_subset_rerun_uses_full_universe_rankings(self) -> None:
        service = DailyQualityFactorService()
        market_date = date(2026, 4, 24)
        all_inputs = [
            _input(
                '000001',
                market_date,
                snapshot=_snapshot(
                    '000001',
                    total_equity=800,
                    total_liabilities=160,
                    net_income=240,
                    total_assets=1000,
                    operating_cash_flow=300,
                    gross_margin=0.50,
                    asset_turnover=1.40,
                    debt_ratio=0.20,
                    snapshot_id=1,
                ),
            ),
            _input(
                '000002',
                market_date,
                snapshot=_snapshot(
                    '000002',
                    total_equity=900,
                    total_liabilities=450,
                    net_income=180,
                    total_assets=1200,
                    operating_cash_flow=180,
                    gross_margin=0.35,
                    asset_turnover=1.00,
                    debt_ratio=0.50,
                    snapshot_id=2,
                ),
            ),
            _input(
                '000003',
                market_date,
                snapshot=_snapshot(
                    '000003',
                    total_equity=1000,
                    total_liabilities=800,
                    net_income=120,
                    total_assets=1500,
                    operating_cash_flow=60,
                    gross_margin=0.20,
                    asset_turnover=0.80,
                    debt_ratio=0.80,
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
            patch('app.signals.quality_factor.upsert_daily_quality_factor_results') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date, symbols=['000002'])

        persisted = upsert.call_args.args[0][0]
        self.assertEqual(result['results_persisted'], 1)
        self.assertEqual(result['summary']['ranking_universe_counts']['quality_score'], 3)
        self.assertEqual(persisted['symbol'], '000002')
        self.assertEqual(persisted['quality_rank'], 2)

    def test_inspect_returns_single_item_for_symbol(self) -> None:
        service = DailyQualityFactorService()
        stored_items = [
            {
                'symbol': '005930',
                'market_date': '2026-04-24',
                'quality_status': QUALITY_STATUS_SCORED_PARTIAL,
                'quality_score': 0.8,
                'components_used': 4,
                'components_possible': 6,
                'is_quality_score_complete': False,
            }
        ]

        with patch(
            'app.signals.quality_factor.load_daily_quality_factor_results',
            return_value=stored_items,
        ) as loader:
            result = service.inspect(market_date='2026-04-24', symbol='5930', limit=50)

        loader.assert_called_once_with(
            market_date=date(2026, 4, 24),
            symbols=['005930'],
            limit=1,
        )
        self.assertEqual(result['feature_version'], QUALITY_FACTOR_VERSION)
        self.assertEqual(result['summary']['requested_symbol'], '005930')
        self.assertEqual(result['item'], stored_items[0])


class DailyQualityFactorPersistenceIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        Symbol.__table__.create(bind=engine, checkfirst=True)
        DailyQualityFactorResult.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        self.service = DailyQualityFactorService()
        self.market_date = date(2026, 4, 24)
        self.symbols = ['TQF2601', 'TQF2602']

    def tearDown(self) -> None:
        with session_scope() as session:
            symbol_ids = list(
                session.execute(select(Symbol.id).where(Symbol.symbol.in_(self.symbols))).scalars().all()
            )
            if symbol_ids:
                session.execute(
                    delete(DailyQualityFactorResult).where(
                        DailyQualityFactorResult.symbol_id.in_(symbol_ids)
                    )
                )
            session.execute(delete(Symbol).where(Symbol.symbol.in_(self.symbols)))

    def test_rebuild_persists_and_inspect_reads_single_symbol(self) -> None:
        inputs = [
            _input(
                self.symbols[0],
                self.market_date,
                snapshot=_snapshot(
                    self.symbols[0],
                    total_equity=800,
                    total_liabilities=160,
                    net_income=240,
                    total_assets=1000,
                    operating_cash_flow=300,
                    gross_margin=0.50,
                    asset_turnover=1.40,
                    debt_ratio=0.20,
                    snapshot_id=1,
                ),
            ),
            _input(
                self.symbols[1],
                self.market_date,
                snapshot=_snapshot(
                    self.symbols[1],
                    total_equity=900,
                    total_liabilities=450,
                    net_income=180,
                    total_assets=1200,
                    operating_cash_flow=180,
                    gross_margin=0.35,
                    asset_turnover=1.00,
                    debt_ratio=0.50,
                    snapshot_id=2,
                ),
            ),
        ]

        with patch.object(self.service, '_load_inputs', return_value=(inputs, self.symbols, [])):
            result = self.service.rebuild_for_market_date(market_date=self.market_date)

        inspection = self.service.inspect(market_date=self.market_date, symbol=self.symbols[0])
        stored_items = load_daily_quality_factor_results(
            market_date=self.market_date,
            symbols=[self.symbols[0]],
        )

        self.assertEqual(result['results_persisted'], 2)
        self.assertEqual(inspection['item']['symbol'], self.symbols[0])
        self.assertTrue(stored_items)
        self.assertEqual(stored_items[0]['symbol'], self.symbols[0])
        self.assertEqual(stored_items[0]['quality_status'], QUALITY_STATUS_SCORED_COMPLETE)
        self.assertEqual(stored_items[0]['components_used'], 6)
