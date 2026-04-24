import unittest
from datetime import date
from unittest.mock import patch

from sqlalchemy import delete, select

from app.db import engine, session_scope
from app.db_models import DailyCompositeFactorResult, Symbol
from app.market_data_store import load_daily_composite_factor_results
from app.signals.composite_factor import (
    CLASSIFICATION_HARD_EXCLUDED,
    CLASSIFICATION_INSUFFICIENT_INPUTS,
    COMPOSITE_ELIGIBLE_SCORE,
    COMPOSITE_FACTOR_VERSION,
    DailyCompositeFactorService,
)


def _symbol_rows(symbols: list[str]) -> list[dict]:
    return [
        {
            'symbol': symbol,
            'name_ko': symbol,
            'market': 'KOSPI',
        }
        for symbol in symbols
    ]


def _momentum_row(
    symbol: str,
    market_date: date,
    *,
    percentile: float | None,
    rank: int | None = None,
    eligible: bool = True,
    overheated: bool = False,
    has_sufficient_history: bool = True,
) -> dict:
    return {
        'symbol': symbol,
        'market_date': market_date.isoformat(),
        'source': 'daily_price_bars',
        'feature_version': 'price_momentum_v1',
        'momentum_rank': rank,
        'momentum_percentile': percentile,
        'is_momentum_eligible': eligible,
        'is_recent_overheated': overheated,
        'has_sufficient_history': has_sufficient_history,
    }


def _value_row(
    symbol: str,
    market_date: date,
    *,
    score: float | None,
    rank: int | None = None,
    percentile: float | None = None,
    status: str = 'scored',
) -> dict:
    return {
        'symbol': symbol,
        'name_ko': symbol,
        'market': 'KOSPI',
        'market_date': market_date.isoformat(),
        'source': 'value_factor_v1',
        'feature_version': 'value_factor_v1',
        'value_score': score,
        'value_rank': rank,
        'value_percentile': percentile,
        'value_status': status,
    }


def _advanced_value_row(
    symbol: str,
    market_date: date,
    *,
    score: float | None,
    rank: int | None = None,
    percentile: float | None = None,
    classification: str = 'eligible',
    eligible: bool = True,
) -> dict:
    return {
        'symbol': symbol,
        'name_ko': symbol,
        'market': 'KOSPI',
        'market_date': market_date.isoformat(),
        'source': 'advanced_value_v1',
        'feature_version': 'advanced_value_v1',
        'advanced_value_score': score,
        'advanced_value_rank': rank,
        'advanced_value_percentile': percentile,
        'advanced_value_classification': classification,
        'is_advanced_value_eligible': eligible,
    }


def _low_risk_row(
    symbol: str,
    market_date: date,
    *,
    score: float | None,
    classification: str = 'eligible',
    eligible: bool = True,
    passes_liquidity_floor: bool | None = True,
) -> dict:
    return {
        'symbol': symbol,
        'name_ko': symbol,
        'market': 'KOSPI',
        'market_date': market_date.isoformat(),
        'source': 'low_risk_v1',
        'feature_version': 'low_risk_v1',
        'low_risk_score': score,
        'low_risk_classification': classification,
        'is_low_risk_eligible': eligible,
        'passes_liquidity_floor': passes_liquidity_floor,
    }


def _beta_row(
    symbol: str,
    market_date: date,
    *,
    score: float | None,
    classification: str = 'eligible',
    eligible: bool = True,
) -> dict:
    return {
        'symbol': symbol,
        'name_ko': symbol,
        'market': 'KOSPI',
        'market_date': market_date.isoformat(),
        'source': 'beta_v1',
        'feature_version': 'beta_v1',
        'low_beta_score': score,
        'low_beta_classification': classification,
        'is_low_beta_eligible': eligible,
    }


class DailyCompositeFactorServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = DailyCompositeFactorService()
        self.market_date = date(2026, 4, 24)

    def test_rebuild_combines_normalized_inputs_and_ranks_higher_score_better(self) -> None:
        momentum_rows = [
            _momentum_row('000001', self.market_date, percentile=0.90, rank=1),
            _momentum_row('000002', self.market_date, percentile=0.70, rank=2),
            _momentum_row('000003', self.market_date, percentile=0.30, rank=3, eligible=False),
        ]
        value_rows = [
            _value_row('000001', self.market_date, score=0.80, rank=1, percentile=1.0),
            _value_row('000002', self.market_date, score=0.60, rank=2, percentile=0.5),
            _value_row('000003', self.market_date, score=0.20, rank=3, percentile=0.0),
        ]
        advanced_rows = [
            _advanced_value_row('000001', self.market_date, score=0.70, rank=1, percentile=1.0),
            _advanced_value_row('000002', self.market_date, score=0.50, rank=2, percentile=0.5, classification='watchlist', eligible=False),
            _advanced_value_row('000003', self.market_date, score=0.10, rank=3, percentile=0.0, classification='ineligible', eligible=False),
        ]
        low_risk_rows = [
            _low_risk_row('000001', self.market_date, score=0.60),
            _low_risk_row('000002', self.market_date, score=0.50, classification='watchlist', eligible=False),
            _low_risk_row('000003', self.market_date, score=0.20, classification='ineligible', eligible=False),
        ]
        beta_rows = [
            _beta_row('000001', self.market_date, score=0.50),
            _beta_row('000002', self.market_date, score=0.40, classification='watchlist', eligible=False),
            _beta_row('000003', self.market_date, score=0.10, classification='ineligible', eligible=False),
        ]

        with (
            patch('app.signals.composite_factor.load_daily_momentum_factor_results', return_value=momentum_rows),
            patch('app.signals.composite_factor.load_daily_value_factor_results', return_value=value_rows),
            patch('app.signals.composite_factor.load_daily_advanced_value_factor_results', return_value=advanced_rows),
            patch('app.signals.composite_factor.load_daily_low_risk_factor_results', return_value=low_risk_rows),
            patch('app.signals.composite_factor.load_daily_beta_factor_results', return_value=beta_rows),
            patch('app.signals.composite_factor.load_symbols', return_value=_symbol_rows(['000001', '000002', '000003'])),
            patch('app.signals.composite_factor.upsert_daily_composite_factor_result') as upsert,
        ):
            result = self.service.rebuild_for_market_date(market_date=self.market_date)

        self.assertEqual(result['feature_version'], COMPOSITE_FACTOR_VERSION)
        self.assertEqual(result['results_persisted'], 3)
        self.assertEqual(upsert.call_count, 3)
        persisted_by_symbol = {call.args[0]['symbol']: call.args[0] for call in upsert.call_args_list}

        best = persisted_by_symbol['000001']
        middle = persisted_by_symbol['000002']
        worst = persisted_by_symbol['000003']

        self.assertEqual(best['inputs']['momentum_percentile'], 0.9)
        self.assertEqual(best['inputs']['value_score_v1'], 0.8)
        self.assertAlmostEqual(best['contributions']['momentum'], 0.225, places=6)
        self.assertAlmostEqual(best['contributions']['value_v1'], 0.12, places=6)
        self.assertAlmostEqual(best['composite_score'], 0.705, places=6)
        self.assertGreaterEqual(best['composite_score'], COMPOSITE_ELIGIBLE_SCORE)
        self.assertEqual(best['composite_rank'], 1)
        self.assertEqual(best['composite_percentile'], 1.0)
        self.assertTrue(best['is_composite_eligible'])

        self.assertAlmostEqual(middle['composite_score'], 0.545, places=6)
        self.assertEqual(middle['composite_rank'], 2)
        self.assertEqual(middle['composite_classification'], 'watchlist')

        self.assertAlmostEqual(worst['composite_score'], 0.185, places=6)
        self.assertEqual(worst['composite_rank'], 3)
        self.assertEqual(worst['composite_percentile'], 0.0)
        self.assertEqual(worst['composite_classification'], 'ineligible')

        self.assertEqual(result['summary']['eligible_count'], 1)
        self.assertEqual(result['summary']['watchlist_count'], 1)
        self.assertEqual(result['summary']['ineligible_count'], 1)
        self.assertEqual(result['summary']['input_universe_counts']['composite_score'], 3)

    def test_rebuild_renormalizes_weights_across_available_inputs(self) -> None:
        with (
            patch(
                'app.signals.composite_factor.load_daily_momentum_factor_results',
                return_value=[_momentum_row('005930', self.market_date, percentile=0.80, rank=1)],
            ),
            patch(
                'app.signals.composite_factor.load_daily_value_factor_results',
                return_value=[_value_row('005930', self.market_date, score=0.60, rank=1, percentile=1.0)],
            ),
            patch('app.signals.composite_factor.load_daily_advanced_value_factor_results', return_value=[]),
            patch(
                'app.signals.composite_factor.load_daily_low_risk_factor_results',
                return_value=[_low_risk_row('005930', self.market_date, score=0.40, classification='watchlist', eligible=False)],
            ),
            patch(
                'app.signals.composite_factor.load_daily_beta_factor_results',
                return_value=[_beta_row('005930', self.market_date, score=0.20, classification='watchlist', eligible=False)],
            ),
            patch('app.signals.composite_factor.load_symbols', return_value=_symbol_rows(['005930'])),
            patch('app.signals.composite_factor.upsert_daily_composite_factor_result') as upsert,
        ):
            result = self.service.rebuild_for_market_date(market_date=self.market_date)

        persisted = upsert.call_args.args[0]
        self.assertEqual(result['summary']['input_universe_counts']['advanced_value'], 0)
        self.assertEqual(persisted['valid_factor_count'], 4)
        self.assertAlmostEqual(persisted['available_base_weight'], 0.8, places=6)
        self.assertTrue(persisted['has_required_inputs'])
        self.assertAlmostEqual(persisted['weights']['applied']['momentum'], 0.3125, places=6)
        self.assertAlmostEqual(persisted['weights']['applied']['value_v1'], 0.1875, places=6)
        self.assertAlmostEqual(persisted['weights']['applied']['low_risk'], 0.25, places=6)
        self.assertAlmostEqual(persisted['weights']['applied']['beta'], 0.25, places=6)
        self.assertIsNone(persisted['weights']['applied']['advanced_value'])
        self.assertAlmostEqual(persisted['contributions']['momentum'], 0.25, places=6)
        self.assertAlmostEqual(persisted['contributions']['value_v1'], 0.1125, places=6)
        self.assertAlmostEqual(persisted['contributions']['low_risk'], 0.1, places=6)
        self.assertAlmostEqual(persisted['contributions']['beta'], 0.05, places=6)
        self.assertIsNone(persisted['contributions']['advanced_value'])
        self.assertAlmostEqual(persisted['composite_score'], 0.5125, places=6)
        self.assertEqual(persisted['composite_classification'], 'watchlist')

    def test_rebuild_marks_insufficient_inputs_when_no_valuation_family_is_available(self) -> None:
        with (
            patch(
                'app.signals.composite_factor.load_daily_momentum_factor_results',
                return_value=[_momentum_row('005930', self.market_date, percentile=0.90, rank=1)],
            ),
            patch('app.signals.composite_factor.load_daily_value_factor_results', return_value=[]),
            patch('app.signals.composite_factor.load_daily_advanced_value_factor_results', return_value=[]),
            patch(
                'app.signals.composite_factor.load_daily_low_risk_factor_results',
                return_value=[_low_risk_row('005930', self.market_date, score=0.80)],
            ),
            patch(
                'app.signals.composite_factor.load_daily_beta_factor_results',
                return_value=[_beta_row('005930', self.market_date, score=0.70)],
            ),
            patch('app.signals.composite_factor.load_symbols', return_value=_symbol_rows(['005930'])),
            patch('app.signals.composite_factor.upsert_daily_composite_factor_result') as upsert,
        ):
            result = self.service.rebuild_for_market_date(market_date=self.market_date)

        persisted = upsert.call_args.args[0]
        self.assertFalse(persisted['has_required_inputs'])
        self.assertIsNone(persisted['composite_score'])
        self.assertEqual(persisted['composite_classification'], CLASSIFICATION_INSUFFICIENT_INPUTS)
        self.assertEqual(result['summary']['insufficient_inputs_count'], 1)

    def test_rebuild_marks_hard_excluded_but_preserves_score_when_gate_fails(self) -> None:
        with (
            patch(
                'app.signals.composite_factor.load_daily_momentum_factor_results',
                return_value=[
                    _momentum_row('000001', self.market_date, percentile=0.95, rank=1, overheated=True, eligible=False),
                    _momentum_row('000002', self.market_date, percentile=0.85, rank=2),
                ],
            ),
            patch(
                'app.signals.composite_factor.load_daily_value_factor_results',
                return_value=[
                    _value_row('000001', self.market_date, score=0.80),
                    _value_row('000002', self.market_date, score=0.75),
                ],
            ),
            patch(
                'app.signals.composite_factor.load_daily_advanced_value_factor_results',
                return_value=[
                    _advanced_value_row('000001', self.market_date, score=0.70),
                    _advanced_value_row('000002', self.market_date, score=0.65),
                ],
            ),
            patch(
                'app.signals.composite_factor.load_daily_low_risk_factor_results',
                return_value=[
                    _low_risk_row('000001', self.market_date, score=0.70),
                    _low_risk_row('000002', self.market_date, score=0.70, passes_liquidity_floor=False, classification='liquidity_excluded', eligible=False),
                ],
            ),
            patch(
                'app.signals.composite_factor.load_daily_beta_factor_results',
                return_value=[
                    _beta_row('000001', self.market_date, score=0.60),
                    _beta_row('000002', self.market_date, score=0.60),
                ],
            ),
            patch('app.signals.composite_factor.load_symbols', return_value=_symbol_rows(['000001', '000002'])),
            patch('app.signals.composite_factor.upsert_daily_composite_factor_result') as upsert,
        ):
            result = self.service.rebuild_for_market_date(market_date=self.market_date)

        persisted_by_symbol = {call.args[0]['symbol']: call.args[0] for call in upsert.call_args_list}

        overheated = persisted_by_symbol['000001']
        illiquid = persisted_by_symbol['000002']

        self.assertEqual(overheated['composite_classification'], CLASSIFICATION_HARD_EXCLUDED)
        self.assertFalse(overheated['is_composite_eligible'])
        self.assertTrue(overheated['is_hard_excluded'])
        self.assertIn('momentum_recent_overheat', overheated['gates']['hard_exclusion_reasons'])
        self.assertIsNotNone(overheated['composite_score'])

        self.assertEqual(illiquid['composite_classification'], CLASSIFICATION_HARD_EXCLUDED)
        self.assertFalse(illiquid['is_composite_eligible'])
        self.assertTrue(illiquid['is_hard_excluded'])
        self.assertIn('low_risk_liquidity_floor', illiquid['gates']['hard_exclusion_reasons'])
        self.assertIsNotNone(illiquid['composite_score'])

        self.assertEqual(result['summary']['hard_excluded_count'], 2)

    def test_inspect_returns_single_item_for_symbol(self) -> None:
        stored_items = [
            {
                'symbol': '005930',
                'market_date': '2026-04-24',
                'composite_score': 0.82,
                'composite_classification': 'eligible',
                'is_composite_eligible': True,
            }
        ]

        with patch(
            'app.signals.composite_factor.load_daily_composite_factor_results',
            return_value=stored_items,
        ) as loader:
            result = self.service.inspect(market_date='2026-04-24', symbol='5930', limit=25)

        loader.assert_called_once_with(
            market_date=date(2026, 4, 24),
            symbols=['005930'],
            limit=1,
        )
        self.assertEqual(result['feature_version'], COMPOSITE_FACTOR_VERSION)
        self.assertEqual(result['summary']['requested_symbol'], '005930')
        self.assertEqual(result['item'], stored_items[0])


class DailyCompositeFactorPersistenceIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        DailyCompositeFactorResult.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        self.service = DailyCompositeFactorService()
        self.market_date = date(2026, 4, 24)
        self.symbols = ['TCF1641', 'TCF1642', 'TCF1643']

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
            session.execute(delete(Symbol).where(Symbol.symbol.in_(self.symbols)))

    def test_rebuild_persists_and_inspect_reads_single_symbol(self) -> None:
        with (
            patch(
                'app.signals.composite_factor.load_daily_momentum_factor_results',
                return_value=[
                    _momentum_row(self.symbols[0], self.market_date, percentile=0.90, rank=1),
                    _momentum_row(self.symbols[1], self.market_date, percentile=0.70, rank=2),
                    _momentum_row(self.symbols[2], self.market_date, percentile=0.50, rank=3),
                ],
            ),
            patch(
                'app.signals.composite_factor.load_daily_value_factor_results',
                return_value=[
                    _value_row(self.symbols[0], self.market_date, score=0.80),
                    _value_row(self.symbols[1], self.market_date, score=0.60),
                    _value_row(self.symbols[2], self.market_date, score=0.40),
                ],
            ),
            patch(
                'app.signals.composite_factor.load_daily_advanced_value_factor_results',
                return_value=[
                    _advanced_value_row(self.symbols[0], self.market_date, score=0.75),
                    _advanced_value_row(self.symbols[1], self.market_date, score=0.65, classification='watchlist', eligible=False),
                    _advanced_value_row(self.symbols[2], self.market_date, score=0.55, classification='watchlist', eligible=False),
                ],
            ),
            patch(
                'app.signals.composite_factor.load_daily_low_risk_factor_results',
                return_value=[
                    _low_risk_row(self.symbols[0], self.market_date, score=0.70),
                    _low_risk_row(self.symbols[1], self.market_date, score=0.60, classification='watchlist', eligible=False),
                    _low_risk_row(self.symbols[2], self.market_date, score=0.50, classification='watchlist', eligible=False),
                ],
            ),
            patch(
                'app.signals.composite_factor.load_daily_beta_factor_results',
                return_value=[
                    _beta_row(self.symbols[0], self.market_date, score=0.65),
                    _beta_row(self.symbols[1], self.market_date, score=0.55, classification='watchlist', eligible=False),
                    _beta_row(self.symbols[2], self.market_date, score=0.45, classification='watchlist', eligible=False),
                ],
            ),
            patch('app.signals.composite_factor.load_symbols', return_value=_symbol_rows(self.symbols)),
        ):
            result = self.service.rebuild_for_market_date(market_date=self.market_date)

        inspection = self.service.inspect(market_date=self.market_date, symbol=self.symbols[0])
        stored_items = load_daily_composite_factor_results(
            market_date=self.market_date,
            symbols=[self.symbols[0]],
        )

        self.assertEqual(result['results_persisted'], 3)
        self.assertEqual(inspection['item']['symbol'], self.symbols[0])
        self.assertTrue(stored_items)
        self.assertEqual(stored_items[0]['symbol'], self.symbols[0])
        self.assertIn('inputs', stored_items[0])
        self.assertIn('contributions', stored_items[0])
        self.assertEqual(stored_items[0]['composite_classification'], 'eligible')
        self.assertTrue(stored_items[0]['is_composite_eligible'])
