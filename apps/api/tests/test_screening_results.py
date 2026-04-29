import unittest
from datetime import date, datetime, timezone

from sqlalchemy import delete, select

from app.db import engine, session_scope
from app.db_models import (
    DailyCompositeFactorResult,
    DailyMarketRiskFilterResult,
    DailyUnifiedValueFactorResult,
    Symbol,
)
from app.eod.screening_results import screening_result_read_service


class ScreeningResultReadServiceIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        Symbol.__table__.create(bind=engine, checkfirst=True)
        DailyCompositeFactorResult.__table__.create(bind=engine, checkfirst=True)
        DailyMarketRiskFilterResult.__table__.create(bind=engine, checkfirst=True)
        DailyUnifiedValueFactorResult.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        self.symbols = ['236001', '236002', '236003']
        self.composite_dates = [date(2026, 4, 24), date(2026, 4, 25)]
        self.market_risk_dates = [date(2026, 4, 14)]
        self._seed_rows()

    def tearDown(self) -> None:
        with session_scope() as session:
            symbol_ids = list(
                session.execute(select(Symbol.id).where(Symbol.symbol.in_(self.symbols))).scalars().all()
            )
            if symbol_ids:
                session.execute(
                    delete(DailyCompositeFactorResult).where(
                        DailyCompositeFactorResult.symbol_id.in_(symbol_ids)
                    )
                )
                session.execute(
                    delete(DailyMarketRiskFilterResult).where(
                        DailyMarketRiskFilterResult.symbol_id.in_(symbol_ids)
                    )
                )
                session.execute(
                    delete(DailyUnifiedValueFactorResult).where(
                        DailyUnifiedValueFactorResult.symbol_id.in_(symbol_ids)
                    )
                )
            session.execute(delete(Symbol).where(Symbol.symbol.in_(self.symbols)))

    def test_read_returns_compact_paginated_composite_screening_rows(self) -> None:
        result = screening_result_read_service.read(
            family='composite',
            market_date='2026-04-24',
            limit=1,
            offset=0,
        )

        self.assertEqual(result['family'], 'composite')
        self.assertEqual(result['detail_route'], '/eod/composite-factors')
        self.assertEqual(result['feature_version'], 'composite_v1')
        self.assertEqual(result['summary']['entries'], 2)
        self.assertEqual(result['summary']['eligible_count'], 1)
        self.assertEqual(result['summary']['watchlist_count'], 1)
        self.assertEqual(result['pagination']['limit'], 1)
        self.assertEqual(result['pagination']['offset'], 0)
        self.assertEqual(result['pagination']['returned'], 1)
        self.assertEqual(result['pagination']['total'], 2)
        self.assertTrue(result['pagination']['has_more'])
        self.assertEqual(result['pagination']['next_offset'], 1)
        self.assertEqual(result['items'][0]['symbol'], '236001')
        self.assertEqual(result['items'][0]['composite_classification'], 'eligible')
        self.assertNotIn('factor_payload', result['items'][0])

    def test_read_uses_latest_fallback_for_market_risk_screening(self) -> None:
        result = screening_result_read_service.read(
            family='market_risk_filter',
            market_date='2026-04-15',
            limit=50,
            offset=0,
        )

        self.assertEqual(result['requested_market_date'], '2026-04-15')
        self.assertEqual(result['resolved_market_date'], '2026-04-14')
        self.assertTrue(result['is_latest_fallback'])
        self.assertEqual(result['summary']['entries'], 2)
        self.assertEqual(result['summary']['passed_count'], 1)
        self.assertEqual(result['summary']['watchlist_count'], 1)
        self.assertEqual(result['summary']['failed_count'], 1)
        self.assertEqual([item['symbol'] for item in result['items']], ['236001', '236002'])
        self.assertNotIn('filter_payload', result['items'][0])

    def test_read_uses_latest_mode_when_market_date_is_omitted(self) -> None:
        result = screening_result_read_service.read(
            family='composite',
            limit=50,
            offset=0,
        )

        self.assertTrue(result['latest_mode'])
        self.assertEqual(result['resolved_market_date'], '2026-04-25')
        self.assertEqual(result['summary']['entries'], 1)
        self.assertEqual(result['items'][0]['symbol'], '236003')

    def test_read_rejects_unknown_family(self) -> None:
        with self.assertRaisesRegex(ValueError, 'family must be one of'):
            screening_result_read_service.read(family='unknown', market_date='2026-04-24')

    def test_read_returns_compact_unified_value_screening_rows(self) -> None:
        result = screening_result_read_service.read(
            family='unified_value',
            market_date='2026-04-24',
            limit=50,
            offset=0,
        )

        self.assertEqual(result['family'], 'unified_value')
        self.assertEqual(result['detail_route'], '/eod/unified-value-factors')
        self.assertEqual(result['feature_version'], 'unified_value_v1')
        self.assertEqual(result['summary']['entries'], 2)
        self.assertEqual(result['summary']['scored_count'], 1)
        self.assertEqual(result['summary']['invalid_per_count'], 1)
        self.assertEqual(result['pagination']['total'], 2)
        self.assertEqual(result['items'][0]['symbol'], '236001')
        self.assertEqual(result['items'][0]['unified_value_status'], 'scored')
        self.assertNotIn('factor_payload', result['items'][0])

    def _seed_rows(self) -> None:
        with session_scope() as session:
            symbol_rows = [
                Symbol(
                    symbol='236001',
                    name_ko='Screening Alpha',
                    market='KOSPI',
                    instrument_type='equity',
                    source='test',
                    is_active=True,
                ),
                Symbol(
                    symbol='236002',
                    name_ko='Screening Beta',
                    market='KOSDAQ',
                    instrument_type='equity',
                    source='test',
                    is_active=True,
                ),
                Symbol(
                    symbol='236003',
                    name_ko='Screening Gamma',
                    market='KOSPI',
                    instrument_type='equity',
                    source='test',
                    is_active=True,
                ),
            ]
            session.add_all(symbol_rows)
            session.flush()

            symbol_id_by_symbol = {row.symbol: row.id for row in symbol_rows}
            captured_at = datetime(2026, 4, 24, 6, 30, tzinfo=timezone.utc)
            session.add_all(
                [
                    DailyCompositeFactorResult(
                        symbol_id=symbol_id_by_symbol['236001'],
                        market_date=date(2026, 4, 24),
                        source='composite_v1',
                        momentum_input=0.91,
                        value_input=0.74,
                        advanced_value_input=0.66,
                        low_risk_input=0.72,
                        low_beta_input=0.63,
                        momentum_contribution=0.2275,
                        value_contribution=0.111,
                        advanced_value_contribution=0.132,
                        low_risk_contribution=0.144,
                        low_beta_contribution=0.126,
                        valid_factor_count=5,
                        available_base_weight=1.0,
                        has_required_inputs=True,
                        is_hard_excluded=False,
                        composite_score=0.7405,
                        composite_rank=1,
                        composite_percentile=1.0,
                        composite_classification='eligible',
                        is_composite_eligible=True,
                        factor_payload={
                            'weights': {'momentum': 0.25},
                            'note': 'payload should stay off compact screening rows',
                        },
                        captured_at=captured_at,
                    ),
                    DailyCompositeFactorResult(
                        symbol_id=symbol_id_by_symbol['236002'],
                        market_date=date(2026, 4, 24),
                        source='composite_v1',
                        momentum_input=0.62,
                        value_input=0.58,
                        advanced_value_input=0.51,
                        low_risk_input=0.44,
                        low_beta_input=0.47,
                        momentum_contribution=0.155,
                        value_contribution=0.087,
                        advanced_value_contribution=0.102,
                        low_risk_contribution=0.088,
                        low_beta_contribution=0.094,
                        valid_factor_count=5,
                        available_base_weight=1.0,
                        has_required_inputs=True,
                        is_hard_excluded=False,
                        composite_score=0.526,
                        composite_rank=2,
                        composite_percentile=0.0,
                        composite_classification='watchlist',
                        is_composite_eligible=False,
                        factor_payload={
                            'weights': {'momentum': 0.25},
                            'note': 'payload should stay off compact screening rows',
                        },
                        captured_at=captured_at,
                    ),
                    DailyCompositeFactorResult(
                        symbol_id=symbol_id_by_symbol['236003'],
                        market_date=date(2026, 4, 25),
                        source='composite_v1',
                        momentum_input=0.95,
                        value_input=0.81,
                        advanced_value_input=0.79,
                        low_risk_input=0.68,
                        low_beta_input=0.72,
                        momentum_contribution=0.2375,
                        value_contribution=0.1215,
                        advanced_value_contribution=0.158,
                        low_risk_contribution=0.136,
                        low_beta_contribution=0.144,
                        valid_factor_count=5,
                        available_base_weight=1.0,
                        has_required_inputs=True,
                        is_hard_excluded=False,
                        composite_score=0.797,
                        composite_rank=1,
                        composite_percentile=1.0,
                        composite_classification='eligible',
                        is_composite_eligible=True,
                        factor_payload={
                            'weights': {'momentum': 0.25},
                            'note': 'latest-mode screening route should not return this payload block',
                        },
                        captured_at=captured_at,
                    ),
                    DailyMarketRiskFilterResult(
                        symbol_id=symbol_id_by_symbol['236001'],
                        market_date=date(2026, 4, 14),
                        source='kis_market_risk_v1',
                        risk_state='blocked',
                        passed_filter=False,
                        watchlist_flagged=False,
                        market_warning_code='03',
                        market_warning_name='투자경고',
                        is_management_item=False,
                        is_investment_caution=False,
                        is_short_overheated=False,
                        short_overheat_class_code=None,
                        is_short_sale_overheated=False,
                        vi_class_code=None,
                        is_vi_triggered=False,
                        is_dishonest_disclosure=False,
                        is_trade_halted=True,
                        filter_payload={'reasons': ['trade_halted']},
                        captured_at=captured_at,
                    ),
                    DailyMarketRiskFilterResult(
                        symbol_id=symbol_id_by_symbol['236002'],
                        market_date=date(2026, 4, 14),
                        source='kis_market_risk_v1',
                        risk_state='watchlist',
                        passed_filter=True,
                        watchlist_flagged=True,
                        market_warning_code='01',
                        market_warning_name='투자주의',
                        is_management_item=False,
                        is_investment_caution=True,
                        is_short_overheated=False,
                        short_overheat_class_code=None,
                        is_short_sale_overheated=False,
                        vi_class_code=None,
                        is_vi_triggered=False,
                        is_dishonest_disclosure=False,
                        is_trade_halted=False,
                        filter_payload={'reasons': ['investment_caution']},
                        captured_at=captured_at,
                    ),
                    DailyUnifiedValueFactorResult(
                        symbol_id=symbol_id_by_symbol['236001'],
                        market_date=date(2026, 4, 24),
                        source='unified_value_v1',
                        per=6.1,
                        pbr=0.7,
                        dividend_yield=0.04,
                        fcf_yield=0.09,
                        ev_to_ebitda=4.8,
                        per_status='available',
                        pbr_status='available',
                        dividend_yield_status='available',
                        fcf_yield_status='available',
                        ev_to_ebitda_status='available',
                        per_rank=1,
                        pbr_rank=1,
                        dividend_yield_rank=1,
                        fcf_yield_rank=1,
                        ev_to_ebitda_rank=1,
                        per_percentile=1.0,
                        pbr_percentile=1.0,
                        dividend_yield_percentile=1.0,
                        fcf_yield_percentile=1.0,
                        ev_to_ebitda_percentile=1.0,
                        unified_value_score=1.0,
                        unified_value_rank=1,
                        unified_value_percentile=1.0,
                        components_used=5,
                        unified_value_status='scored',
                        factor_payload={'note': 'payload should stay off compact screening rows'},
                        captured_at=captured_at,
                    ),
                    DailyUnifiedValueFactorResult(
                        symbol_id=symbol_id_by_symbol['236002'],
                        market_date=date(2026, 4, 24),
                        source='unified_value_v1',
                        per=-1.2,
                        pbr=0.9,
                        dividend_yield=0.03,
                        fcf_yield=0.06,
                        ev_to_ebitda=6.2,
                        per_status='invalid_non_positive',
                        pbr_status='available',
                        dividend_yield_status='available',
                        fcf_yield_status='available',
                        ev_to_ebitda_status='available',
                        per_rank=None,
                        pbr_rank=2,
                        dividend_yield_rank=2,
                        fcf_yield_rank=2,
                        ev_to_ebitda_rank=2,
                        per_percentile=None,
                        pbr_percentile=0.0,
                        dividend_yield_percentile=0.0,
                        fcf_yield_percentile=0.0,
                        ev_to_ebitda_percentile=0.0,
                        unified_value_score=None,
                        unified_value_rank=None,
                        unified_value_percentile=None,
                        components_used=4,
                        unified_value_status='invalid_per',
                        factor_payload={'note': 'payload should stay off compact screening rows'},
                        captured_at=captured_at,
                    ),
                ]
            )


if __name__ == '__main__':
    unittest.main()
