import unittest
from datetime import date, timedelta
from unittest.mock import patch

from sqlalchemy import delete, select

from app.db import engine, session_scope
from app.db_models import DailyBetaFactorResult, Symbol
from app.market_data_store import load_daily_beta_factor_results
from app.signals.beta_factor import (
    BETA_FACTOR_VERSION,
    LOW_BETA_ELIGIBLE_SCORE,
    LOW_BETA_WATCHLIST_SCORE,
    BenchmarkContext,
    BetaFactorInput,
    DailyBetaFactorService,
    benchmark_id_for_market,
    build_simple_return_series_from_bars,
    compute_beta,
    resolve_beta_benchmark_market,
)


def _build_close_series(initial_close: float, returns: list[float]) -> list[float]:
    closes = [initial_close]
    for value in returns:
        closes.append(closes[-1] * (1.0 + value))
    return closes


def _build_bars(
    market_date: date,
    *,
    closes: list[float],
) -> list[dict]:
    bars: list[dict] = []
    for offset, close_price in enumerate(reversed(closes)):
        trade_date = (market_date - timedelta(days=offset)).isoformat()
        scaled_close = int(round(close_price * 1000))
        bars.append(
            {
                'trade_date': trade_date,
                'open_price': scaled_close,
                'high_price': scaled_close,
                'low_price': scaled_close,
                'close_price': scaled_close,
                'volume': 1_000_000,
                'turnover': 10_000_000,
                'source': 'pykrx',
                'captured_at': f'{trade_date}T15:30:00+09:00',
            }
        )
    return bars


def _input(symbol: str, market: str, market_date: date, returns: list[float]) -> BetaFactorInput:
    return BetaFactorInput(
        symbol=symbol,
        name_ko=symbol,
        market=market,
        bars=_build_bars(
            market_date,
            closes=_build_close_series(100.0, returns),
        ),
    )


def _benchmark_context(market_date: date, benchmark_returns: list[float], *, contributors: int = 3) -> BenchmarkContext:
    bars = _build_bars(
        market_date,
        closes=_build_close_series(100.0, benchmark_returns),
    )
    return_series = build_simple_return_series_from_bars(bars)
    counts = {trade_date: contributors for trade_date in return_series}
    return BenchmarkContext(
        returns_by_market={
            'KOSPI': return_series,
            'KOSDAQ': return_series,
            'KRX': return_series,
        },
        contributor_counts_by_market={
            'KOSPI': counts,
            'KOSDAQ': counts,
            'KRX': counts,
        },
    )


class BetaFactorMathTests(unittest.TestCase):
    def test_compute_beta_matches_covariance_variance_ratio(self) -> None:
        benchmark_returns = [0.01, -0.02, 0.015, -0.01, 0.005, 0.02]
        symbol_returns = [value * 2.0 for value in benchmark_returns]

        result = compute_beta(symbol_returns, benchmark_returns)

        self.assertAlmostEqual(result, 2.0, places=6)

    def test_benchmark_policy_resolves_market_specific_and_fallback_buckets(self) -> None:
        self.assertEqual(resolve_beta_benchmark_market('KOSPI'), 'KOSPI')
        self.assertEqual(resolve_beta_benchmark_market('kosdaq'), 'KOSDAQ')
        self.assertEqual(resolve_beta_benchmark_market('KONEX'), 'KRX')
        self.assertEqual(resolve_beta_benchmark_market(None), 'KRX')
        self.assertEqual(benchmark_id_for_market('KOSPI'), 'KOSPI_EQW')
        self.assertEqual(benchmark_id_for_market('KOSDAQ'), 'KOSDAQ_EQW')
        self.assertEqual(benchmark_id_for_market('KRX'), 'KRX_EQW')


class DailyBetaFactorServiceTests(unittest.TestCase):
    def test_rebuild_assigns_lower_beta_better_rankings_and_scores(self) -> None:
        service = DailyBetaFactorService()
        market_date = date(2026, 4, 23)
        benchmark_returns = ([0.01, -0.008, 0.012, -0.007] * 30)[:120]
        inputs = [
            _input('000001', 'KOSPI', market_date, [value * 0.4 for value in benchmark_returns]),
            _input('000002', 'KOSPI', market_date, [value * 1.0 for value in benchmark_returns]),
            _input('000003', 'KOSPI', market_date, [value * 1.6 for value in benchmark_returns]),
        ]

        with (
            patch.object(service, '_load_inputs', return_value=(inputs, ['000001', '000002', '000003'], [])),
            patch.object(service, '_build_benchmark_context', return_value=_benchmark_context(market_date, benchmark_returns)),
            patch('app.signals.beta_factor.upsert_daily_beta_factor_result') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date)

        self.assertEqual(result['feature_version'], BETA_FACTOR_VERSION)
        self.assertEqual(result['results_persisted'], 3)
        self.assertEqual(upsert.call_count, 3)
        persisted_by_symbol = {call.args[0]['symbol']: call.args[0] for call in upsert.call_args_list}

        low_beta = persisted_by_symbol['000001']
        mid_beta = persisted_by_symbol['000002']
        high_beta = persisted_by_symbol['000003']

        self.assertAlmostEqual(low_beta['beta_60d'], 0.4, places=3)
        self.assertAlmostEqual(low_beta['beta_120d'], 0.4, places=3)
        self.assertEqual(low_beta['beta_60d_rank'], 1)
        self.assertEqual(low_beta['beta_120d_rank'], 1)
        self.assertEqual(low_beta['beta_60d_percentile'], 1.0)
        self.assertEqual(low_beta['beta_120d_percentile'], 1.0)
        self.assertEqual(low_beta['low_beta_classification'], 'eligible')
        self.assertTrue(low_beta['is_low_beta_eligible'])
        self.assertGreaterEqual(low_beta['low_beta_score'], LOW_BETA_ELIGIBLE_SCORE)

        self.assertAlmostEqual(mid_beta['beta_120d'], 1.0, places=3)
        self.assertEqual(mid_beta['beta_60d_rank'], 2)
        self.assertEqual(mid_beta['beta_120d_rank'], 2)
        self.assertEqual(mid_beta['low_beta_classification'], 'watchlist')
        self.assertFalse(mid_beta['is_low_beta_eligible'])
        self.assertAlmostEqual(mid_beta['low_beta_score'], LOW_BETA_WATCHLIST_SCORE, places=6)

        self.assertAlmostEqual(high_beta['beta_120d'], 1.6, places=3)
        self.assertEqual(high_beta['beta_60d_rank'], 3)
        self.assertEqual(high_beta['beta_120d_rank'], 3)
        self.assertEqual(high_beta['beta_60d_percentile'], 0.0)
        self.assertEqual(high_beta['beta_120d_percentile'], 0.0)
        self.assertEqual(high_beta['low_beta_classification'], 'ineligible')
        self.assertFalse(high_beta['is_low_beta_eligible'])

        methodology = low_beta['factor_payload']['methodology']
        self.assertEqual(methodology['return_type'], 'simple')
        self.assertEqual(result['summary']['eligible_count'], 1)
        self.assertEqual(result['summary']['watchlist_count'], 1)

    def test_rebuild_marks_insufficient_history_but_preserves_available_60d_beta(self) -> None:
        service = DailyBetaFactorService()
        market_date = date(2026, 4, 23)
        benchmark_returns = ([0.008, -0.006, 0.01, -0.007] * 30)[:120]
        short_history_returns = benchmark_returns[:79]
        inputs = [
            _input('005930', 'KOSPI', market_date, [value * 0.8 for value in short_history_returns]),
        ]

        with (
            patch.object(service, '_load_inputs', return_value=(inputs, ['005930'], [])),
            patch.object(service, '_build_benchmark_context', return_value=_benchmark_context(market_date, benchmark_returns)),
            patch('app.signals.beta_factor.upsert_daily_beta_factor_result') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date)

        persisted = upsert.call_args.args[0]
        self.assertIsNotNone(persisted['beta_60d'])
        self.assertIsNone(persisted['beta_120d'])
        self.assertEqual(persisted['low_beta_classification'], 'insufficient_history')
        self.assertIsNone(persisted['low_beta_score'])
        self.assertFalse(persisted['is_low_beta_eligible'])
        self.assertEqual(result['summary']['insufficient_history_count'], 1)

    def test_rebuild_populates_60d_and_120d_outputs_when_history_exists(self) -> None:
        service = DailyBetaFactorService()
        market_date = date(2026, 4, 23)
        benchmark_returns = ([0.009, -0.007, 0.011, -0.006] * 30)[:120]
        inputs = [_input('005930', 'KOSPI', market_date, [value * 0.9 for value in benchmark_returns])]

        with (
            patch.object(service, '_load_inputs', return_value=(inputs, ['005930'], [])),
            patch.object(service, '_build_benchmark_context', return_value=_benchmark_context(market_date, benchmark_returns, contributors=1)),
            patch('app.signals.beta_factor.upsert_daily_beta_factor_result') as upsert,
        ):
            service.rebuild_for_market_date(market_date=market_date)

        persisted = upsert.call_args.args[0]
        self.assertIsNotNone(persisted['beta_60d'])
        self.assertIsNotNone(persisted['beta_120d'])
        self.assertEqual(persisted['factor_payload']['availability']['observations']['beta_60d'], 60)
        self.assertEqual(persisted['factor_payload']['availability']['observations']['beta_120d'], 120)

    def test_inspect_returns_single_item_for_symbol(self) -> None:
        service = DailyBetaFactorService()
        stored_items = [
            {
                'symbol': '005930',
                'market_date': '2026-04-23',
                'benchmark_id': 'KOSPI_EQW',
                'low_beta_classification': 'eligible',
                'is_low_beta_eligible': True,
            }
        ]

        with patch('app.signals.beta_factor.load_daily_beta_factor_results', return_value=stored_items) as loader:
            result = service.inspect(market_date='2026-04-23', symbol='5930', limit=50)

        loader.assert_called_once_with(
            market_date=date(2026, 4, 23),
            symbols=['005930'],
            limit=1,
        )
        self.assertEqual(result['feature_version'], BETA_FACTOR_VERSION)
        self.assertEqual(result['summary']['requested_symbol'], '005930')
        self.assertEqual(result['item'], stored_items[0])


class DailyBetaFactorPersistenceIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        DailyBetaFactorResult.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        self.service = DailyBetaFactorService()
        self.market_date = date(2026, 4, 23)
        self.symbols = ['TB1591', 'TB1592', 'TB1593']

    def tearDown(self) -> None:
        with session_scope() as session:
            symbol_ids = list(
                session.execute(select(Symbol.id).where(Symbol.symbol.in_(self.symbols))).scalars().all()
            )
            if symbol_ids:
                session.execute(delete(DailyBetaFactorResult).where(DailyBetaFactorResult.symbol_id.in_(symbol_ids)))
            session.execute(delete(Symbol).where(Symbol.symbol.in_(self.symbols)))

    def test_rebuild_persists_and_inspect_reads_single_symbol(self) -> None:
        benchmark_returns = ([0.01, -0.006, 0.011, -0.008] * 30)[:120]
        inputs = [
            _input(self.symbols[0], 'KOSPI', self.market_date, [value * 0.5 for value in benchmark_returns]),
            _input(self.symbols[1], 'KOSPI', self.market_date, [value * 1.0 for value in benchmark_returns]),
            _input(self.symbols[2], 'KOSPI', self.market_date, [value * 1.4 for value in benchmark_returns]),
        ]

        with (
            patch.object(self.service, '_load_inputs', return_value=(inputs, self.symbols, [])),
            patch.object(
                self.service,
                '_build_benchmark_context',
                return_value=_benchmark_context(self.market_date, benchmark_returns),
            ),
        ):
            result = self.service.rebuild_for_market_date(market_date=self.market_date)

        inspection = self.service.inspect(market_date=self.market_date, symbol=self.symbols[0])
        stored_items = load_daily_beta_factor_results(
            market_date=self.market_date,
            symbols=[self.symbols[0]],
        )

        self.assertEqual(result['results_persisted'], 3)
        self.assertEqual(inspection['item']['symbol'], self.symbols[0])
        self.assertEqual(inspection['item']['benchmark_id'], 'KOSPI_EQW')
        self.assertTrue(stored_items)
        self.assertEqual(stored_items[0]['symbol'], self.symbols[0])
        self.assertEqual(stored_items[0]['benchmark_market'], 'KOSPI')
        self.assertIsNotNone(stored_items[0]['low_beta_score'])
