from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from statistics import stdev
from typing import Any, Sequence

from app.market_data_store import (
    list_symbols_with_daily_bar_on_date,
    load_daily_low_risk_factor_results,
    load_recent_price_bars_by_symbol,
    load_symbols,
    upsert_daily_low_risk_factor_result,
    upsert_daily_low_risk_factor_results,
)

LOW_RISK_FEATURE_VERSION = 'low_risk_v1'
LOW_RISK_SOURCE = LOW_RISK_FEATURE_VERSION
VOLATILITY_RETURN_TYPE = 'simple'
VOLATILITY_ANNUALIZATION_DAYS = 252
VOLATILITY_60D_RETURN_DAYS = 60
VOLATILITY_120D_RETURN_DAYS = 120
MAX_DRAWDOWN_120D_BARS = 120
AVG_TRADING_VALUE_20D_BARS = 20
LOW_RISK_REQUIRED_HISTORY_DAYS = VOLATILITY_120D_RETURN_DAYS + 1
LOW_RISK_ELIGIBLE_SCORE = 0.70
LOW_RISK_WATCHLIST_SCORE = 0.50
LOW_RISK_LIQUIDITY_FLOOR_PERCENTILE = 0.20
LOW_RISK_SCORE_WEIGHTS = {
    'volatility_60d_percentile': 0.30,
    'volatility_120d_percentile': 0.30,
    'max_drawdown_120d_percentile': 0.25,
    'liquidity_percentile': 0.15,
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
    normalized = symbol.strip().upper()
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


def build_close_series_from_bars(bars: Sequence[dict]) -> list[float]:
    return [float(bar['close_price']) for bar in reversed(bars)]


def build_turnover_series_from_bars(bars: Sequence[dict]) -> list[float]:
    turnovers: list[float] = []
    for bar in bars:
        try:
            turnover = float(bar['turnover'])
            volume = float(bar['volume'])
        except (KeyError, TypeError, ValueError):
            turnovers.append(math.nan)
            continue
        if (
            not math.isfinite(turnover)
            or not math.isfinite(volume)
            or turnover < 0
            or volume < 0
            or (volume > 0 and turnover == 0)
        ):
            turnovers.append(math.nan)
            continue
        turnovers.append(turnover)
    return turnovers


def compute_realized_volatility(
    close_prices: Sequence[float],
    *,
    return_days: int,
    annualization_days: int = VOLATILITY_ANNUALIZATION_DAYS,
) -> float | None:
    if len(close_prices) < return_days + 1:
        return None

    window = list(close_prices[-(return_days + 1) :])
    returns: list[float] = []
    for previous_close, current_close in zip(window, window[1:]):
        if previous_close <= 0:
            return None
        returns.append((current_close / previous_close) - 1.0)
    if len(returns) < 2:
        return None

    # v1 uses simple close-to-close returns and annualizes by sqrt(252).
    return _round(stdev(returns) * math.sqrt(float(annualization_days)), 6)


def compute_max_drawdown(close_prices: Sequence[float], *, lookback_bars: int) -> float | None:
    if len(close_prices) < lookback_bars:
        return None

    window = list(close_prices[-lookback_bars:])
    running_peak = window[0]
    worst_drawdown = 0.0
    for close_price in window:
        if close_price > running_peak:
            running_peak = close_price
        if running_peak <= 0:
            return None
        drawdown = (close_price / running_peak) - 1.0
        if drawdown < worst_drawdown:
            worst_drawdown = drawdown
    return _round(worst_drawdown, 6)


def compute_average_trading_value(turnovers: Sequence[float], *, lookback_bars: int) -> float | None:
    if lookback_bars <= 0:
        return None
    window = list(turnovers[:lookback_bars])
    if len(window) < lookback_bars:
        return None
    normalized_turnovers: list[float] = []
    for turnover in window:
        try:
            normalized_turnover = float(turnover)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(normalized_turnover) or normalized_turnover < 0:
            return None
        normalized_turnovers.append(normalized_turnover)
    return _round(sum(normalized_turnovers) / lookback_bars, 4)


@dataclass(slots=True)
class LowRiskFactorInput:
    symbol: str
    name_ko: str | None
    market: str | None
    bars: list[dict]


class DailyLowRiskFactorService:
    def rebuild_for_market_date(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
        ranking_symbols: Sequence[str] | None = None,
        source: str = LOW_RISK_SOURCE,
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

        ranking_items = [
            self._build_symbol_result(
                input_row=item,
                market_date=normalized_market_date,
                source=source,
            )
            for item in ranking_inputs
        ]

        universe_counts = self._assign_rankings(ranking_items)
        self._assign_liquidity_floor(ranking_items)
        self._assign_scores_and_classifications(ranking_items)
        if normalized_symbols is None:
            items = ranking_items
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
                    )
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
        try:
            upsert_daily_low_risk_factor_results(items)
            persisted_count = len(items)
        except Exception:
            for item in items:
                try:
                    upsert_daily_low_risk_factor_result(item)
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
            if item['low_risk_classification']
            in {'eligible', 'watchlist', 'ineligible', 'liquidity_excluded'}
        )
        summary['persisted_ready_count'] = persisted_ready_count
        summary['persisted_unready_count'] = len(persisted_items) - persisted_ready_count

        return {
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
            'items': items,
        }

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
        items = load_daily_low_risk_factor_results(
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
            'eligible_count': sum(1 for item in items if item.get('is_low_risk_eligible')),
            'liquidity_floor_pass_count': sum(1 for item in items if item.get('passes_liquidity_floor') is True),
            'insufficient_history_count': sum(
                1 for item in items if item.get('low_risk_classification') == 'insufficient_history'
            ),
        }
        payload: dict[str, Any] = {
            'market_date': normalized_market_date.isoformat(),
            'feature_version': LOW_RISK_FEATURE_VERSION,
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
    ) -> tuple[list[LowRiskFactorInput], list[str], list[str]]:
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
            history_days=LOW_RISK_REQUIRED_HISTORY_DAYS,
        )
        inputs = [
            LowRiskFactorInput(
                symbol=row['symbol'],
                name_ko=row.get('name_ko'),
                market=row.get('market'),
                bars=bars_by_symbol.get(row['symbol'], []),
            )
            for row in symbol_rows
        ]
        return inputs, matched_symbols, missing_symbols

    def _build_symbol_result(
        self,
        *,
        input_row: LowRiskFactorInput,
        market_date: date,
        source: str,
    ) -> dict[str, Any]:
        market_date_iso = market_date.isoformat()
        bars = input_row.bars
        current_bar = bars[0] if bars and bars[0].get('trade_date') == market_date_iso else None
        history_days_available = len(bars)
        close_prices = build_close_series_from_bars(bars) if current_bar is not None else []
        turnovers = build_turnover_series_from_bars(bars) if current_bar is not None else []
        volatility_60d = compute_realized_volatility(close_prices, return_days=VOLATILITY_60D_RETURN_DAYS)
        volatility_120d = compute_realized_volatility(close_prices, return_days=VOLATILITY_120D_RETURN_DAYS)
        max_drawdown_120d = compute_max_drawdown(close_prices, lookback_bars=MAX_DRAWDOWN_120D_BARS)
        avg_trading_value_20d = compute_average_trading_value(turnovers, lookback_bars=AVG_TRADING_VALUE_20D_BARS)

        return {
            'symbol': input_row.symbol,
            'name_ko': input_row.name_ko,
            'market': input_row.market,
            'market_date': market_date_iso,
            'source': source,
            'feature_version': source,
            'volatility_60d': volatility_60d,
            'volatility_120d': volatility_120d,
            'max_drawdown_120d': max_drawdown_120d,
            'avg_trading_value_20d': avg_trading_value_20d,
            'volatility_60d_rank': None,
            'volatility_120d_rank': None,
            'max_drawdown_120d_rank': None,
            'liquidity_rank': None,
            'volatility_60d_percentile': None,
            'volatility_120d_percentile': None,
            'max_drawdown_120d_percentile': None,
            'liquidity_percentile': None,
            'passes_liquidity_floor': None,
            'low_risk_score': None,
            'low_risk_classification': (
                'missing_market_date_bar' if current_bar is None else 'insufficient_history'
            ),
            'is_low_risk_eligible': False,
            'history_days_available': history_days_available,
            'factor_payload': {},
            '_current_bar_available': current_bar is not None,
            '_history_start_date': bars[-1]['trade_date'] if bars else None,
            '_latest_close_price': current_bar['close_price'] if current_bar is not None else None,
            '_latest_turnover': current_bar['turnover'] if current_bar is not None else None,
        }

    def _assign_rankings(self, items: list[dict[str, Any]]) -> dict[str, int]:
        universe_counts = {
            'volatility_60d': self._assign_metric_rankings(
                items,
                value_key='volatility_60d',
                rank_key='volatility_60d_rank',
                percentile_key='volatility_60d_percentile',
                best='lower',
            ),
            'volatility_120d': self._assign_metric_rankings(
                items,
                value_key='volatility_120d',
                rank_key='volatility_120d_rank',
                percentile_key='volatility_120d_percentile',
                best='lower',
            ),
            'max_drawdown_120d': self._assign_metric_rankings(
                items,
                value_key='max_drawdown_120d',
                rank_key='max_drawdown_120d_rank',
                percentile_key='max_drawdown_120d_percentile',
                best='higher',
            ),
            'liquidity': self._assign_metric_rankings(
                items,
                value_key='avg_trading_value_20d',
                rank_key='liquidity_rank',
                percentile_key='liquidity_percentile',
                best='higher',
                positive_only=True,
            ),
        }
        universe_counts['combined_score'] = sum(
            1
            for item in items
            if all(
                item[key] is not None
                for key in (
                    'volatility_60d_percentile',
                    'volatility_120d_percentile',
                    'max_drawdown_120d_percentile',
                    'liquidity_percentile',
                )
            )
        )
        return universe_counts

    def _assign_metric_rankings(
        self,
        items: list[dict[str, Any]],
        *,
        value_key: str,
        rank_key: str,
        percentile_key: str,
        best: str,
        positive_only: bool = False,
    ) -> int:
        candidates = [
            item
            for item in items
            if item.get(value_key) is not None
            and (
                not positive_only
                or float(item[value_key]) > 0
            )
        ]
        if best == 'lower':
            ordered = sorted(candidates, key=lambda item: (float(item[value_key]), item['symbol']))
        elif best == 'higher':
            ordered = sorted(candidates, key=lambda item: (-float(item[value_key]), item['symbol']))
        else:
            raise ValueError(f'Unsupported ranking direction: {best}')

        total = len(ordered)
        for rank, item in enumerate(ordered, start=1):
            item[rank_key] = rank
            item[percentile_key] = _percentile_from_rank(rank, total)
        return total

    def _assign_liquidity_floor(self, items: list[dict[str, Any]]) -> None:
        for item in items:
            if item.get('liquidity_percentile') is None:
                item['passes_liquidity_floor'] = None
                continue
            item['passes_liquidity_floor'] = (
                float(item['liquidity_percentile']) >= LOW_RISK_LIQUIDITY_FLOOR_PERCENTILE
            )

    def _assign_scores_and_classifications(self, items: list[dict[str, Any]]) -> None:
        for item in items:
            if not item['_current_bar_available']:
                item['low_risk_classification'] = 'missing_market_date_bar'
                item['is_low_risk_eligible'] = False
                continue

            if any(
                item[key] is None
                for key in (
                    'volatility_60d',
                    'volatility_120d',
                    'max_drawdown_120d',
                    'avg_trading_value_20d',
                )
            ):
                item['low_risk_classification'] = 'insufficient_history'
                item['is_low_risk_eligible'] = False
                continue

            if any(
                item[key] is None
                for key in (
                    'volatility_60d_percentile',
                    'volatility_120d_percentile',
                    'max_drawdown_120d_percentile',
                    'liquidity_percentile',
                )
            ):
                item['low_risk_classification'] = 'insufficient_history'
                item['is_low_risk_eligible'] = False
                continue

            item['low_risk_score'] = _round(
                sum(
                    float(item[key]) * weight
                    for key, weight in LOW_RISK_SCORE_WEIGHTS.items()
                ),
                6,
            )
            if item['passes_liquidity_floor'] is False:
                item['low_risk_classification'] = 'liquidity_excluded'
                item['is_low_risk_eligible'] = False
            elif float(item['low_risk_score']) >= LOW_RISK_ELIGIBLE_SCORE:
                item['low_risk_classification'] = 'eligible'
                item['is_low_risk_eligible'] = True
            elif float(item['low_risk_score']) >= LOW_RISK_WATCHLIST_SCORE:
                item['low_risk_classification'] = 'watchlist'
                item['is_low_risk_eligible'] = False
            else:
                item['low_risk_classification'] = 'ineligible'
                item['is_low_risk_eligible'] = False

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
            if item['low_risk_classification'] in {'eligible', 'watchlist', 'ineligible', 'liquidity_excluded'}
        )
        return {
            'requested_symbol_count': len(requested_symbols) if requested_symbols else len(matched_symbols),
            'matched_symbol_count': len(matched_symbols),
            'missing_symbol_count': len(missing_symbols),
            'symbols_with_market_date_bar': sum(1 for item in items if item['_current_bar_available']),
            'missing_market_date_bar_count': sum(
                1 for item in items if item['low_risk_classification'] == 'missing_market_date_bar'
            ),
            'insufficient_history_count': sum(
                1 for item in items if item['low_risk_classification'] == 'insufficient_history'
            ),
            'liquidity_floor_pass_count': sum(
                1 for item in items if item['passes_liquidity_floor'] is True
            ),
            'eligible_count': sum(1 for item in items if item['is_low_risk_eligible']),
            'watchlist_count': sum(1 for item in items if item['low_risk_classification'] == 'watchlist'),
            'liquidity_excluded_count': sum(
                1 for item in items if item['low_risk_classification'] == 'liquidity_excluded'
            ),
            'ready_count': ready_count,
            'unready_count': len(missing_symbols) + len(items) - ready_count,
            'ranking_universe_counts': universe_counts,
        }

    @staticmethod
    def _strip_internal_fields(item: dict[str, Any]) -> None:
        item.pop('_current_bar_available', None)
        item.pop('_history_start_date', None)
        item.pop('_latest_close_price', None)
        item.pop('_latest_turnover', None)

    def _build_factor_payload(
        self,
        *,
        item: dict[str, Any],
        source: str,
        universe_counts: dict[str, int],
    ) -> dict[str, Any]:
        return {
            'feature_version': source,
            'methodology': {
                'price_field': 'close_price',
                'return_type': VOLATILITY_RETURN_TYPE,
                'annualization_days': VOLATILITY_ANNUALIZATION_DAYS,
                'volatility': {
                    'volatility_60d_return_days': VOLATILITY_60D_RETURN_DAYS,
                    'volatility_120d_return_days': VOLATILITY_120D_RETURN_DAYS,
                },
                'drawdown': {
                    'max_drawdown_120d_bars': MAX_DRAWDOWN_120D_BARS,
                    'formula': 'min(close / running_peak - 1.0)',
                },
                'liquidity': {
                    'avg_trading_value_20d_bars': AVG_TRADING_VALUE_20D_BARS,
                    'price_field': 'turnover',
                },
            },
            'ranking': {
                'percentile_definition': 'best_symbol=1.0, worst_symbol=0.0; ties break on symbol for deterministic ranks',
                'metric_directions': {
                    'volatility_60d': 'lower_is_better',
                    'volatility_120d': 'lower_is_better',
                    'max_drawdown_120d': 'higher_is_better',
                    'liquidity': 'higher_is_better',
                },
                'universe_counts': universe_counts,
            },
            'weights': LOW_RISK_SCORE_WEIGHTS,
            'liquidity_rule': {
                'floor_percentile': LOW_RISK_LIQUIDITY_FLOOR_PERCENTILE,
                'passes_liquidity_floor': item['passes_liquidity_floor'],
            },
            'classification_rule': {
                'eligible_min_score': LOW_RISK_ELIGIBLE_SCORE,
                'watchlist_min_score': LOW_RISK_WATCHLIST_SCORE,
                'requires_liquidity_floor': True,
                'classification': item['low_risk_classification'],
            },
            'availability': {
                'current_bar_available': item['_current_bar_available'],
                'history_days_available': item['history_days_available'],
                'required_history_days': LOW_RISK_REQUIRED_HISTORY_DAYS,
                'metrics': {
                    'volatility_60d': item['volatility_60d'] is not None,
                    'volatility_120d': item['volatility_120d'] is not None,
                    'max_drawdown_120d': item['max_drawdown_120d'] is not None,
                    'avg_trading_value_20d': item['avg_trading_value_20d'] is not None,
                },
            },
            'window': {
                'market_date': item['market_date'],
                'history_start_date': item['_history_start_date'],
            },
            'inputs': {
                'latest_close_price': item['_latest_close_price'],
                'latest_turnover': item['_latest_turnover'],
            },
        }


daily_low_risk_factor_service = DailyLowRiskFactorService()
