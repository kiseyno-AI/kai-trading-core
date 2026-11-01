from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping, Sequence

from app.market_data_store import (
    list_symbols_with_daily_bar_on_date,
    load_daily_beta_factor_results,
    load_recent_price_bars_by_symbol,
    load_symbols,
    replace_daily_derived_results_exact_scope,
    upsert_daily_beta_factor_result,
    upsert_daily_beta_factor_results,
)
from app.repositories.eod_run_repository import daily_price_bar_writer_admission
from app.signals.factor_coverage import (
    build_factor_coverage_contract,
    factor_stage_result_contract,
)

BETA_FACTOR_VERSION = 'beta_v1'
BETA_FACTOR_SOURCE = BETA_FACTOR_VERSION
BETA_RETURN_TYPE = 'simple'
BETA_60D_RETURN_DAYS = 60
BETA_120D_RETURN_DAYS = 120
BETA_REQUIRED_HISTORY_DAYS = BETA_120D_RETURN_DAYS + 1
LOW_BETA_ELIGIBLE_SCORE = 0.70
LOW_BETA_WATCHLIST_SCORE = 0.50
LOW_BETA_SCORE_WEIGHTS = {
    'beta_60d_percentile': 0.50,
    'beta_120d_percentile': 0.50,
}
BENCHMARK_MARKET_KOSPI = 'KOSPI'
BENCHMARK_MARKET_KOSDAQ = 'KOSDAQ'
BENCHMARK_MARKET_KRX = 'KRX'
BENCHMARK_ID_BY_MARKET = {
    BENCHMARK_MARKET_KOSPI: 'KOSPI_EQW',
    BENCHMARK_MARKET_KOSDAQ: 'KOSDAQ_EQW',
    BENCHMARK_MARKET_KRX: 'KRX_EQW',
}
EMPTY_RANKING_UNIVERSE_COUNTS = {
    'beta_60d': 0,
    'beta_120d': 0,
    'low_beta_score': 0,
}


def _round(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def _normalize_market_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _normalize_symbol(symbol: str) -> str:
    normalized = str(symbol or '').strip().upper()
    if not normalized:
        raise ValueError('symbol must not be blank')
    if normalized.isdigit():
        normalized = normalized.zfill(6)
    return normalized


def _normalize_symbols(symbols: Sequence[str] | None) -> list[str] | None:
    if symbols is None:
        return None
    return list(dict.fromkeys(_normalize_symbol(symbol) for symbol in symbols))


def _percentile_from_rank(rank: int, total: int) -> float:
    if total <= 1:
        return 1.0
    return _round((total - rank) / (total - 1), 6) or 0.0


def _sort_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        items,
        key=lambda item: (
            not bool(item.get('is_low_beta_eligible')),
            item.get('low_beta_score') is None,
            -(float(item['low_beta_score'])) if item.get('low_beta_score') is not None else 999999.0,
            float(item['beta_120d']) if item.get('beta_120d') is not None else 999999.0,
            item['symbol'],
        ),
    )


def resolve_beta_benchmark_market(market: str | None) -> str:
    normalized_market = str(market or '').strip().upper()
    if normalized_market == BENCHMARK_MARKET_KOSPI:
        return BENCHMARK_MARKET_KOSPI
    if normalized_market == BENCHMARK_MARKET_KOSDAQ:
        return BENCHMARK_MARKET_KOSDAQ
    return BENCHMARK_MARKET_KRX


def benchmark_id_for_market(benchmark_market: str) -> str:
    return BENCHMARK_ID_BY_MARKET.get(benchmark_market, BENCHMARK_ID_BY_MARKET[BENCHMARK_MARKET_KRX])


def _ordered_bars(bars: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(bars, key=lambda item: str(item['trade_date']))


def build_simple_return_series_from_bars(bars: Sequence[dict[str, Any]]) -> dict[str, float]:
    ordered = _ordered_bars(bars)
    returns: dict[str, float] = {}
    previous_close: float | None = None
    for item in ordered:
        close_price = item.get('close_price')
        if close_price is None:
            previous_close = None
            continue
        current_close = float(close_price)
        if previous_close is not None and previous_close > 0 and current_close > 0:
            returns[str(item['trade_date'])] = _round((current_close / previous_close) - 1.0, 6) or 0.0
        previous_close = current_close
    return returns


def compute_beta(symbol_returns: Sequence[float], benchmark_returns: Sequence[float]) -> float | None:
    if len(symbol_returns) != len(benchmark_returns):
        raise ValueError('symbol_returns and benchmark_returns must have the same length')
    if len(symbol_returns) < 2:
        return None

    symbol_mean = sum(symbol_returns) / float(len(symbol_returns))
    benchmark_mean = sum(benchmark_returns) / float(len(benchmark_returns))
    benchmark_variance = sum((value - benchmark_mean) ** 2 for value in benchmark_returns)
    if benchmark_variance == 0:
        return None
    covariance = sum(
        (symbol_value - symbol_mean) * (benchmark_value - benchmark_mean)
        for symbol_value, benchmark_value in zip(symbol_returns, benchmark_returns)
    )
    return _round(covariance / benchmark_variance, 6)


@dataclass(slots=True)
class BetaFactorInput:
    symbol: str
    name_ko: str | None
    market: str | None
    bars: list[dict[str, Any]]


@dataclass(slots=True)
class BenchmarkContext:
    returns_by_market: dict[str, dict[str, float]]
    contributor_counts_by_market: dict[str, dict[str, int]]


class DailyBetaFactorService:
    @daily_price_bar_writer_admission
    def rebuild_for_market_date(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
        ranking_symbols: Sequence[str] | None = None,
        source: str = BETA_FACTOR_SOURCE,
        exact_scope_symbols: Sequence[str] | None = None,
        authoritative_market_scope: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbols = _normalize_symbols(symbols)
        normalized_ranking_symbols = _normalize_symbols(ranking_symbols)
        if ranking_symbols is not None and normalized_symbols is None:
            normalized_symbols = list(normalized_ranking_symbols or [])
        if ranking_symbols is not None and normalized_symbols is not None:
            outside_ranking_scope = sorted(
                set(normalized_symbols).difference(normalized_ranking_symbols or [])
            )
            if outside_ranking_scope:
                raise ValueError(
                    'requested symbols must be contained in ranking_symbols: '
                    + ','.join(outside_ranking_scope[:20])
                )
        requested_symbols = list(normalized_symbols or [])
        if normalized_symbols == []:
            summary = self._build_summary(
                [],
                requested_symbols=requested_symbols,
                matched_symbols=[],
                missing_symbols=[],
                universe_counts=EMPTY_RANKING_UNIVERSE_COUNTS,
            )
            result = {
                'status': 'completed',
                'reason': 'explicit_empty_scope_noop',
                'quality_status': summary['quality_status'],
                'quality_warning': summary['quality_warning'],
                'gate_status': 'ready',
                'blocking_status': 'non_blocking',
                'remaining_retryable_events': 0,
                'market_date': normalized_market_date.isoformat(),
                'source': source,
                'feature_version': source,
                'requested_symbols': requested_symbols,
                'matched_symbols': [],
                'missing_symbols': [],
                'summary': summary,
                'results_persisted': 0,
                'results_failed': 0,
                'failed_symbols': [],
                'failed_symbol_errors': [],
                'items': [],
            }
            if exact_scope_symbols is not None:
                result['scope_finalization'] = (
                    replace_daily_derived_results_exact_scope(
                        market_date=normalized_market_date,
                        stage_id='beta_factors',
                        results=[],
                        exact_symbols=exact_scope_symbols,
                        authoritative_market_scope=(
                            authoritative_market_scope
                        ),
                    )['scope_finalization']
                )
            return result

        inputs, matched_symbols, missing_symbols = self._load_inputs(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
        )
        ranking_inputs = inputs
        if (
            normalized_symbols is not None
            and normalized_symbols != normalized_ranking_symbols
        ):
            ranking_inputs, _, _ = self._load_inputs(
                market_date=normalized_market_date,
                symbols=(
                    normalized_ranking_symbols
                    if ranking_symbols is not None
                    else None
                ),
            )

        benchmark_context = self._build_benchmark_context(ranking_inputs)
        ranking_items = [
            self._build_symbol_result(
                input_row=item,
                market_date=normalized_market_date,
                source=source,
                benchmark_context=benchmark_context,
            )
            for item in ranking_inputs
        ]
        universe_counts = self._assign_rankings(ranking_items)
        self._assign_scores_and_classifications(ranking_items)
        universe_counts['low_beta_score'] = sum(1 for item in ranking_items if item.get('low_beta_score') is not None)

        if normalized_symbols is None:
            items = _sort_items(ranking_items)
        else:
            ranked_items_by_symbol = {item['symbol']: item for item in ranking_items}
            items = []
            for input_row in inputs:
                item = ranked_items_by_symbol.get(input_row.symbol)
                if item is None:
                    item = self._build_symbol_result(
                        input_row=input_row,
                        market_date=normalized_market_date,
                        source=source,
                        benchmark_context=benchmark_context,
                    )
                    self._assign_scores_and_classifications([item])
                items.append(item)

        for item in items:
            item['factor_payload'] = self._build_factor_payload(
                item=item,
                source=source,
                universe_counts=universe_counts,
            )
        summary = self._build_summary(
            items,
            requested_symbols=requested_symbols,
            matched_symbols=matched_symbols,
            missing_symbols=missing_symbols,
            universe_counts=universe_counts,
        )
        for item in items:
            self._strip_internal_fields(item)

        persisted_count = 0
        failed_symbols: list[str] = []
        failed_symbol_errors: list[dict[str, str]] = []
        scope_finalization: dict[str, Any] | None = None
        if exact_scope_symbols is not None:
            publication = replace_daily_derived_results_exact_scope(
                market_date=normalized_market_date,
                stage_id='beta_factors',
                results=items,
                exact_symbols=exact_scope_symbols,
                authoritative_market_scope=authoritative_market_scope,
            )
            persisted_count = int(publication['results_persisted'])
            failed_symbols = list(publication['failed_symbols'])
            failed_symbol_errors = list(publication['failed_symbol_errors'])
            scope_finalization = publication['scope_finalization']
        else:
            try:
                upsert_daily_beta_factor_results(items)
                persisted_count = len(items)
            except Exception:
                for item in items:
                    try:
                        upsert_daily_beta_factor_result(item)
                        persisted_count += 1
                    except Exception as exc:
                        failed_symbols.append(item['symbol'])
                        failed_symbol_errors.append({'symbol': item['symbol'], 'error': str(exc)})

        failed_symbol_set = set(failed_symbols)
        persisted_items = [
            item for item in items if item['symbol'] not in failed_symbol_set
        ]
        persisted_ready_count = sum(
            1
            for item in persisted_items
            if item['low_beta_classification'] in {'eligible', 'watchlist', 'ineligible'}
        )
        summary['persisted_ready_count'] = persisted_ready_count
        summary['persisted_unready_count'] = len(persisted_items) - persisted_ready_count
        summary.update(
            self._build_coverage_contract(
                items=items,
                persisted_items=persisted_items,
                missing_symbols=missing_symbols,
                failed_symbols=failed_symbols,
            )
        )

        result = {
            **factor_stage_result_contract(summary),
            'market_date': normalized_market_date.isoformat(),
            'source': source,
            'feature_version': source,
            'requested_symbols': requested_symbols,
            'matched_symbols': matched_symbols,
            'missing_symbols': missing_symbols,
            'summary': summary,
            'results_persisted': persisted_count,
            'results_failed': len(failed_symbols),
            'failed_symbols': failed_symbols,
            'failed_symbol_errors': failed_symbol_errors,
            'items': _sort_items(items),
        }
        if scope_finalization is not None:
            result['scope_finalization'] = scope_finalization
        return result

    def inspect(
        self,
        *,
        market_date: str | date,
        symbol: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbol = _normalize_symbol(symbol) if symbol is not None else None
        requested_symbols = [normalized_symbol] if normalized_symbol else []
        items = load_daily_beta_factor_results(
            market_date=normalized_market_date,
            symbols=requested_symbols or None,
            limit=(1 if normalized_symbol else limit),
        )
        matched_symbols = [item['symbol'] for item in items]
        matched_symbol_set = set(matched_symbols)
        summary = {
            'entries': len(items),
            'requested_symbol': normalized_symbol,
            'matched_symbols': matched_symbols,
            'missing_symbols': [candidate for candidate in requested_symbols if candidate not in matched_symbol_set],
            'eligible_count': sum(1 for item in items if item.get('is_low_beta_eligible')),
            'watchlist_count': sum(1 for item in items if item.get('low_beta_classification') == 'watchlist'),
            'insufficient_history_count': sum(
                1 for item in items if item.get('low_beta_classification') == 'insufficient_history'
            ),
        }
        payload: dict[str, Any] = {
            'market_date': normalized_market_date.isoformat(),
            'feature_version': BETA_FACTOR_VERSION,
            'summary': summary,
        }
        if normalized_symbol:
            payload['item'] = items[0] if items else None
        else:
            payload['items'] = items
        return payload

    def evaluate_symbol(
        self,
        *,
        symbol: str,
        market_date: str | date,
    ) -> dict[str, Any]:
        inspection = self.inspect(market_date=market_date, symbol=symbol)
        return {
            'market_date': inspection['market_date'],
            'feature_version': inspection['feature_version'],
            'symbol': _normalize_symbol(symbol),
            'summary': inspection['summary'],
            'item': inspection.get('item'),
        }

    def _load_inputs(
        self,
        *,
        market_date: date,
        symbols: list[str] | None,
    ) -> tuple[list[BetaFactorInput], list[str], list[str]]:
        if symbols == []:
            return [], [], []
        if symbols is None:
            matched_symbols = list_symbols_with_daily_bar_on_date(market_date.isoformat())
            if not matched_symbols:
                return [], [], []
            missing_symbols: list[str] = []
        else:
            symbol_rows = load_symbols(symbols=symbols)
            matched_symbols = [item['symbol'] for item in symbol_rows]
            matched_symbol_set = set(matched_symbols)
            missing_symbols = [symbol for symbol in symbols if symbol not in matched_symbol_set]
            if not matched_symbols:
                return [], [], missing_symbols

        symbol_rows = load_symbols(symbols=matched_symbols)
        bars_by_symbol = load_recent_price_bars_by_symbol(
            market_date=market_date,
            symbols=matched_symbols,
            history_days=BETA_REQUIRED_HISTORY_DAYS,
        )
        inputs = [
            BetaFactorInput(
                symbol=row['symbol'],
                name_ko=row.get('name_ko'),
                market=row.get('market'),
                bars=bars_by_symbol.get(row['symbol'], []),
            )
            for row in symbol_rows
        ]
        return inputs, matched_symbols, missing_symbols

    def _build_benchmark_context(self, inputs: Sequence[BetaFactorInput]) -> BenchmarkContext:
        return_lists_by_market: dict[str, dict[str, list[float]]] = {
            BENCHMARK_MARKET_KOSPI: {},
            BENCHMARK_MARKET_KOSDAQ: {},
            BENCHMARK_MARKET_KRX: {},
        }

        for input_row in inputs:
            symbol_returns = build_simple_return_series_from_bars(input_row.bars)
            contribution_markets = [BENCHMARK_MARKET_KRX]
            market_bucket = resolve_beta_benchmark_market(input_row.market)
            if market_bucket in {BENCHMARK_MARKET_KOSPI, BENCHMARK_MARKET_KOSDAQ}:
                contribution_markets.append(market_bucket)
            for benchmark_market in contribution_markets:
                bucket = return_lists_by_market.setdefault(benchmark_market, {})
                for trade_date, return_value in symbol_returns.items():
                    bucket.setdefault(trade_date, []).append(return_value)

        returns_by_market: dict[str, dict[str, float]] = {}
        contributor_counts_by_market: dict[str, dict[str, int]] = {}
        for benchmark_market, values_by_date in return_lists_by_market.items():
            returns_by_market[benchmark_market] = {
                trade_date: _round(sum(values) / float(len(values)), 6) or 0.0
                for trade_date, values in values_by_date.items()
                if values
            }
            contributor_counts_by_market[benchmark_market] = {
                trade_date: len(values)
                for trade_date, values in values_by_date.items()
                if values
            }
        return BenchmarkContext(
            returns_by_market=returns_by_market,
            contributor_counts_by_market=contributor_counts_by_market,
        )

    def _build_symbol_result(
        self,
        *,
        input_row: BetaFactorInput,
        market_date: date,
        source: str,
        benchmark_context: BenchmarkContext,
    ) -> dict[str, Any]:
        market_date_iso = market_date.isoformat()
        bars = input_row.bars
        current_bar = bars[0] if bars and bars[0].get('trade_date') == market_date_iso else None
        history_days_available = len(bars)
        symbol_returns = build_simple_return_series_from_bars(bars) if current_bar is not None else {}
        benchmark_market = resolve_beta_benchmark_market(input_row.market)
        benchmark_id = benchmark_id_for_market(benchmark_market)
        benchmark_returns = benchmark_context.returns_by_market.get(benchmark_market, {})
        contributor_counts = benchmark_context.contributor_counts_by_market.get(benchmark_market, {})
        beta_60d_window = self._compute_window_beta(
            symbol_returns=symbol_returns,
            benchmark_returns=benchmark_returns,
            window_days=BETA_60D_RETURN_DAYS,
        )
        beta_120d_window = self._compute_window_beta(
            symbol_returns=symbol_returns,
            benchmark_returns=benchmark_returns,
            window_days=BETA_120D_RETURN_DAYS,
        )

        return {
            'symbol': input_row.symbol,
            'name_ko': input_row.name_ko,
            'market': input_row.market,
            'market_date': market_date_iso,
            'source': source,
            'feature_version': source,
            'benchmark_id': benchmark_id,
            'benchmark_market': benchmark_market,
            'beta_60d': beta_60d_window['beta'],
            'beta_120d': beta_120d_window['beta'],
            'beta_60d_rank': None,
            'beta_120d_rank': None,
            'beta_60d_percentile': None,
            'beta_120d_percentile': None,
            'low_beta_score': None,
            'low_beta_classification': 'missing_market_date_bar' if current_bar is None else 'insufficient_history',
            'is_low_beta_eligible': False,
            'history_days_available': history_days_available,
            'factor_payload': {},
            '_current_bar_available': current_bar is not None,
            '_history_start_date': bars[-1]['trade_date'] if bars else None,
            '_bar_source': current_bar.get('source') if current_bar is not None else None,
            '_bar_captured_at': current_bar.get('captured_at') if current_bar is not None else None,
            '_benchmark_return_days_available': len(benchmark_returns),
            '_benchmark_constituents_on_market_date': contributor_counts.get(market_date_iso),
            '_benchmark_return_on_market_date': benchmark_returns.get(market_date_iso),
            '_beta_60d_observations': beta_60d_window['observation_count'],
            '_beta_60d_start_date': beta_60d_window['start_date'],
            '_beta_60d_end_date': beta_60d_window['end_date'],
            '_beta_120d_observations': beta_120d_window['observation_count'],
            '_beta_120d_start_date': beta_120d_window['start_date'],
            '_beta_120d_end_date': beta_120d_window['end_date'],
        }

    def _compute_window_beta(
        self,
        *,
        symbol_returns: dict[str, float],
        benchmark_returns: dict[str, float],
        window_days: int,
    ) -> dict[str, Any]:
        common_dates = sorted(set(symbol_returns).intersection(benchmark_returns))
        if len(common_dates) < window_days:
            return {
                'beta': None,
                'observation_count': len(common_dates),
                'start_date': common_dates[0] if common_dates else None,
                'end_date': common_dates[-1] if common_dates else None,
            }

        window_dates = common_dates[-window_days:]
        symbol_window = [float(symbol_returns[trade_date]) for trade_date in window_dates]
        benchmark_window = [float(benchmark_returns[trade_date]) for trade_date in window_dates]
        return {
            'beta': compute_beta(symbol_window, benchmark_window),
            'observation_count': len(window_dates),
            'start_date': window_dates[0],
            'end_date': window_dates[-1],
        }

    def _assign_rankings(self, items: list[dict[str, Any]]) -> dict[str, int]:
        return {
            'beta_60d': self._assign_metric_rankings(
                items,
                value_key='beta_60d',
                rank_key='beta_60d_rank',
                percentile_key='beta_60d_percentile',
            ),
            'beta_120d': self._assign_metric_rankings(
                items,
                value_key='beta_120d',
                rank_key='beta_120d_rank',
                percentile_key='beta_120d_percentile',
            ),
        }

    def _assign_metric_rankings(
        self,
        items: list[dict[str, Any]],
        *,
        value_key: str,
        rank_key: str,
        percentile_key: str,
    ) -> int:
        ordered = sorted(
            [item for item in items if item.get(value_key) is not None],
            key=lambda item: (float(item[value_key]), item['symbol']),
        )
        total = len(ordered)
        for rank, item in enumerate(ordered, start=1):
            item[rank_key] = rank
            item[percentile_key] = _percentile_from_rank(rank, total)
        return total

    def _assign_scores_and_classifications(self, items: list[dict[str, Any]]) -> None:
        for item in items:
            if not item['_current_bar_available']:
                item['low_beta_classification'] = 'missing_market_date_bar'
                item['is_low_beta_eligible'] = False
                continue
            if any(item.get(key) is None for key in ('beta_60d', 'beta_120d')):
                item['low_beta_classification'] = 'insufficient_history'
                item['is_low_beta_eligible'] = False
                continue
            if any(item.get(key) is None for key in ('beta_60d_percentile', 'beta_120d_percentile')):
                item['low_beta_classification'] = 'insufficient_history'
                item['is_low_beta_eligible'] = False
                continue

            item['low_beta_score'] = _round(
                sum(float(item[key]) * weight for key, weight in LOW_BETA_SCORE_WEIGHTS.items()),
                6,
            )
            if float(item['low_beta_score']) >= LOW_BETA_ELIGIBLE_SCORE:
                item['low_beta_classification'] = 'eligible'
                item['is_low_beta_eligible'] = True
            elif float(item['low_beta_score']) >= LOW_BETA_WATCHLIST_SCORE:
                item['low_beta_classification'] = 'watchlist'
                item['is_low_beta_eligible'] = False
            else:
                item['low_beta_classification'] = 'ineligible'
                item['is_low_beta_eligible'] = False

    def _build_factor_payload(
        self,
        *,
        item: dict[str, Any],
        source: str,
        universe_counts: dict[str, int],
    ) -> dict[str, Any]:
        return {
            'feature_version': source,
            'benchmark_policy': {
                'selection_rule': 'KOSPI equities use KOSPI_EQW, KOSDAQ equities use KOSDAQ_EQW, others use KRX_EQW',
                'benchmark_source': 'equal_weight_simple_return_from_stored_daily_price_bars',
                'benchmark_id': item['benchmark_id'],
                'benchmark_market': item['benchmark_market'],
                'constituents_with_return_on_market_date': item['_benchmark_constituents_on_market_date'],
            },
            'methodology': {
                'input_table': 'daily_price_bars',
                'price_field': 'close_price',
                'return_type': BETA_RETURN_TYPE,
                'beta_formula': 'cov(symbol_returns, benchmark_returns) / var(benchmark_returns)',
                'windows': {
                    'beta_60d_return_days': BETA_60D_RETURN_DAYS,
                    'beta_120d_return_days': BETA_120D_RETURN_DAYS,
                },
            },
            'ranking': {
                'percentile_definition': 'best_symbol=1.0, worst_symbol=0.0; ties break on symbol for deterministic ranks',
                'metric_directions': {
                    'beta_60d': 'lower_is_better',
                    'beta_120d': 'lower_is_better',
                    'low_beta_score': 'higher_is_better',
                },
                'beta_60d_rank': item['beta_60d_rank'],
                'beta_120d_rank': item['beta_120d_rank'],
                'beta_60d_percentile': item['beta_60d_percentile'],
                'beta_120d_percentile': item['beta_120d_percentile'],
                'ranking_universe_counts': universe_counts,
            },
            'weights': LOW_BETA_SCORE_WEIGHTS,
            'classification_rule': {
                'eligible_min_score': LOW_BETA_ELIGIBLE_SCORE,
                'watchlist_min_score': LOW_BETA_WATCHLIST_SCORE,
                'classification': item['low_beta_classification'],
            },
            'availability': {
                'current_bar_available': item['_current_bar_available'],
                'history_days_available': item['history_days_available'],
                'required_history_days': BETA_REQUIRED_HISTORY_DAYS,
                'benchmark_return_days_available': item['_benchmark_return_days_available'],
                'observations': {
                    'beta_60d': item['_beta_60d_observations'],
                    'beta_120d': item['_beta_120d_observations'],
                },
                'metrics': {
                    'beta_60d': item['beta_60d'] is not None,
                    'beta_120d': item['beta_120d'] is not None,
                },
            },
            'window': {
                'market_date': item['market_date'],
                'history_start_date': item['_history_start_date'],
                'beta_60d_start_date': item['_beta_60d_start_date'],
                'beta_60d_end_date': item['_beta_60d_end_date'],
                'beta_120d_start_date': item['_beta_120d_start_date'],
                'beta_120d_end_date': item['_beta_120d_end_date'],
            },
            'audit': {
                'name_ko': item.get('name_ko'),
                'market': item.get('market'),
                'bar_source': item.get('_bar_source'),
                'bar_captured_at': item.get('_bar_captured_at'),
                'benchmark_return_on_market_date': item['_benchmark_return_on_market_date'],
            },
        }

    def _build_summary(
        self,
        items: list[dict[str, Any]],
        *,
        requested_symbols: list[str],
        matched_symbols: list[str],
        missing_symbols: list[str],
        universe_counts: dict[str, int],
    ) -> dict[str, Any]:
        ready_count = sum(
            1
            for item in items
            if item['low_beta_classification'] in {'eligible', 'watchlist', 'ineligible'}
        )
        summary = {
            'requested_symbol_count': len(requested_symbols) if requested_symbols else len(matched_symbols),
            'matched_symbol_count': len(matched_symbols),
            'missing_symbol_count': len(missing_symbols),
            'symbols_with_market_date_bar': sum(1 for item in items if item['_current_bar_available']),
            'missing_market_date_bar_count': sum(
                1 for item in items if item['low_beta_classification'] == 'missing_market_date_bar'
            ),
            'insufficient_history_count': sum(
                1 for item in items if item['low_beta_classification'] == 'insufficient_history'
            ),
            'eligible_count': sum(1 for item in items if item['is_low_beta_eligible']),
            'watchlist_count': sum(1 for item in items if item['low_beta_classification'] == 'watchlist'),
            'ready_count': ready_count,
            'unready_count': len(missing_symbols) + len(items) - ready_count,
            'ranking_universe_counts': universe_counts,
        }
        summary.update(
            self._build_coverage_contract(
                items=items,
                persisted_items=items,
                missing_symbols=missing_symbols,
                failed_symbols=[],
            )
        )
        return summary

    @staticmethod
    def _build_coverage_contract(
        *,
        items: list[dict[str, Any]],
        persisted_items: list[dict[str, Any]],
        missing_symbols: list[str],
        failed_symbols: list[str],
    ) -> dict[str, Any]:
        def metric_available(item: dict[str, Any], metric: str) -> bool:
            payload = item.get('factor_payload')
            availability = payload.get('availability') if isinstance(payload, dict) else None
            metrics = availability.get('metrics') if isinstance(availability, dict) else None
            return bool(metrics.get(metric)) if isinstance(metrics, dict) else False

        return build_factor_coverage_contract(
            items=items,
            persisted_items=persisted_items,
            missing_symbols=missing_symbols,
            failed_symbols=failed_symbols,
            is_ready=lambda item: item.get('low_beta_score') is not None,
            is_complete=lambda item: all(
                metric_available(item, metric)
                for metric in ('beta_60d', 'beta_120d')
            ),
            unusable_reason=lambda item: item.get('low_beta_classification'),
            input_families={
                metric: (
                    lambda item, metric=metric: metric_available(item, metric),
                    lambda item, metric=metric: (
                        None
                        if metric_available(item, metric)
                        else f'missing_{metric}'
                    ),
                )
                for metric in ('beta_60d', 'beta_120d')
            },
        )

    @staticmethod
    def _strip_internal_fields(item: dict[str, Any]) -> None:
        for key in (
            '_current_bar_available',
            '_history_start_date',
            '_bar_source',
            '_bar_captured_at',
            '_benchmark_return_days_available',
            '_benchmark_constituents_on_market_date',
            '_benchmark_return_on_market_date',
            '_beta_60d_observations',
            '_beta_60d_start_date',
            '_beta_60d_end_date',
            '_beta_120d_observations',
            '_beta_120d_start_date',
            '_beta_120d_end_date',
        ):
            item.pop(key, None)


daily_beta_factor_service = DailyBetaFactorService()
