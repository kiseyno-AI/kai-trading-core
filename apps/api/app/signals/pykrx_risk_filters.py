from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Sequence

from app.market_data_store import load_recent_price_bars_by_symbol, load_symbols

TRADING_HALT_LIKE_HEURISTIC = 'pykrx_ohlcv_zero_fill_with_nonzero_close_v1'
RISK_FILTER_SOURCE = 'pykrx_daily_price_bars'
DEFAULT_LOOKBACK_DAYS = 20


def _normalize_market_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _normalize_market(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().upper()
    if not normalized:
        raise ValueError('market must not be blank')
    return normalized


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
    normalized = [_normalize_symbol(symbol) for symbol in symbols]
    return list(dict.fromkeys(normalized))


def _round(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def _percentile_for_rank(rank: int, total: int) -> float:
    if total <= 1:
        return 0.0
    return _round((rank - 1) / (total - 1), 6) or 0.0


@dataclass(frozen=True, slots=True)
class LowLiquidityRuleConfig:
    kind: str = 'bottom_percentile'
    lookback_days: int = DEFAULT_LOOKBACK_DAYS
    percentile_threshold: float = 0.10
    max_rank: int | None = None
    max_avg_trading_value: float | None = None

    def __post_init__(self) -> None:
        if self.kind not in {'bottom_percentile', 'bottom_rank', 'max_avg_trading_value'}:
            raise ValueError(f'Unsupported low-liquidity rule kind: {self.kind}')
        if self.lookback_days <= 0:
            raise ValueError('lookback_days must be positive')
        if self.kind == 'bottom_percentile' and not 0 <= float(self.percentile_threshold) <= 1:
            raise ValueError('percentile_threshold must be between 0 and 1')
        if self.kind == 'bottom_rank' and (self.max_rank is None or self.max_rank <= 0):
            raise ValueError('max_rank must be set to a positive integer for bottom_rank rules')
        if self.kind == 'max_avg_trading_value' and (
            self.max_avg_trading_value is None or float(self.max_avg_trading_value) < 0
        ):
            raise ValueError(
                'max_avg_trading_value must be set to a non-negative number for max_avg_trading_value rules'
            )

    def to_metadata(self) -> dict[str, Any]:
        metadata = {
            'kind': self.kind,
            'lookback_days': self.lookback_days,
            'is_configurable': True,
        }
        if self.kind == 'bottom_percentile':
            metadata['threshold'] = float(self.percentile_threshold)
        elif self.kind == 'bottom_rank':
            metadata['threshold'] = int(self.max_rank or 0)
        else:
            metadata['threshold'] = float(self.max_avg_trading_value or 0.0)
        return metadata

    def classify(
        self,
        *,
        avg_trading_value: float,
        liquidity_rank: int,
        liquidity_percentile: float,
    ) -> tuple[bool, str]:
        if self.kind == 'bottom_percentile':
            flagged = liquidity_percentile <= float(self.percentile_threshold)
            reason = (
                'bottom_percentile_threshold_met'
                if flagged
                else 'bottom_percentile_threshold_not_met'
            )
            return flagged, reason
        if self.kind == 'bottom_rank':
            flagged = liquidity_rank <= int(self.max_rank or 0)
            reason = 'bottom_rank_threshold_met' if flagged else 'bottom_rank_threshold_not_met'
            return flagged, reason
        flagged = avg_trading_value <= float(self.max_avg_trading_value or 0.0)
        reason = (
            'max_avg_trading_value_threshold_met'
            if flagged
            else 'max_avg_trading_value_threshold_not_met'
        )
        return flagged, reason


class PykrxRiskFilterService:
    def inspect(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
        market: str | None = None,
        liquidity_rule: LowLiquidityRuleConfig | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_market = _normalize_market(market)
        normalized_symbols = _normalize_symbols(symbols)
        resolved_rule = liquidity_rule or LowLiquidityRuleConfig()
        requested_symbols = list(normalized_symbols or [])

        universe = load_symbols(
            is_active=None if normalized_symbols else True,
            markets=[normalized_market] if normalized_market else None,
            symbols=normalized_symbols,
        )
        matched_symbols = [item['symbol'] for item in universe]
        matched_symbol_set = set(matched_symbols)
        missing_symbols = [symbol for symbol in requested_symbols if symbol not in matched_symbol_set]

        bars_by_symbol = load_recent_price_bars_by_symbol(
            market_date=normalized_market_date,
            symbols=matched_symbols,
            history_days=resolved_rule.lookback_days,
        )
        items = [
            self._build_symbol_result(
                symbol_row=item,
                bars=bars_by_symbol.get(item['symbol'], []),
                market_date=normalized_market_date,
                liquidity_rule=resolved_rule,
            )
            for item in universe
        ]

        liquidity_candidates = [
            item for item in items if item['low_liquidity_20d']['available']
        ]
        ranked_candidates = sorted(
            liquidity_candidates,
            key=lambda item: (
                item['low_liquidity_20d']['avg_trading_value_20d'],
                item['symbol'],
            ),
        )
        for index, item in enumerate(ranked_candidates, start=1):
            percentile = _percentile_for_rank(index, len(ranked_candidates))
            flagged, reason = resolved_rule.classify(
                avg_trading_value=float(item['low_liquidity_20d']['avg_trading_value_20d']),
                liquidity_rank=index,
                liquidity_percentile=percentile,
            )
            item['low_liquidity_20d']['liquidity_rank'] = index
            item['low_liquidity_20d']['liquidity_percentile'] = percentile
            item['low_liquidity_20d']['is_low_liquidity'] = flagged
            item['low_liquidity_20d']['reason'] = reason
            item['is_low_liquidity'] = flagged

        items.sort(key=lambda item: item['symbol'])
        market_date_iso = normalized_market_date.isoformat()
        return {
            'market_date': market_date_iso,
            'source': RISK_FILTER_SOURCE,
            'market': normalized_market,
            'requested_symbols': requested_symbols,
            'matched_symbols': matched_symbols,
            'missing_symbols': missing_symbols,
            'liquidity_rule': resolved_rule.to_metadata(),
            'summary': {
                'requested_symbol_count': len(requested_symbols) if requested_symbols else len(matched_symbols),
                'matched_symbol_count': len(matched_symbols),
                'symbols_with_market_date_bar': sum(
                    1 for item in items if item['trading_halt_like']['available']
                ),
                'symbols_missing_market_date_bar': sum(
                    1 for item in items if not item['trading_halt_like']['available']
                ),
                'trading_halt_like_count': sum(
                    1 for item in items if item['is_trading_halt_like'] is True
                ),
                'liquidity_eligible_count': len(ranked_candidates),
                'low_liquidity_count': sum(
                    1 for item in items if item['is_low_liquidity'] is True
                ),
                'insufficient_history_count': sum(
                    1
                    for item in items
                    if item['low_liquidity_20d']['reason'] == 'insufficient_history'
                ),
            },
            'items': items,
        }

    def evaluate_symbol(
        self,
        *,
        symbol: str,
        market_date: str | date,
        market: str | None = None,
        liquidity_rule: LowLiquidityRuleConfig | None = None,
    ) -> dict[str, Any]:
        normalized_symbol = _normalize_symbol(symbol)
        inspection = self.inspect(
            market_date=market_date,
            symbols=[normalized_symbol],
            market=market,
            liquidity_rule=liquidity_rule,
        )
        return {
            'market_date': inspection['market_date'],
            'symbol': normalized_symbol,
            'market': inspection['market'],
            'liquidity_rule': inspection['liquidity_rule'],
            'summary': inspection['summary'],
            'item': inspection['items'][0] if inspection['items'] else None,
        }

    def get_trading_halt_like_symbols(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
        market: str | None = None,
    ) -> dict[str, Any]:
        inspection = self.inspect(
            market_date=market_date,
            symbols=symbols,
            market=market,
        )
        items = [
            item for item in inspection['items'] if item['is_trading_halt_like'] is True
        ]
        items.sort(key=lambda item: item['symbol'])
        return {
            'market_date': inspection['market_date'],
            'source': inspection['source'],
            'market': inspection['market'],
            'requested_symbols': inspection['requested_symbols'],
            'matched_symbols': inspection['matched_symbols'],
            'missing_symbols': inspection['missing_symbols'],
            'heuristic': {
                'name': TRADING_HALT_LIKE_HEURISTIC,
                'is_heuristic': True,
            },
            'summary': {
                'matched_symbol_count': inspection['summary']['matched_symbol_count'],
                'trading_halt_like_count': len(items),
            },
            'items': items,
        }

    def get_low_liquidity_symbols(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
        market: str | None = None,
        liquidity_rule: LowLiquidityRuleConfig | None = None,
    ) -> dict[str, Any]:
        inspection = self.inspect(
            market_date=market_date,
            symbols=symbols,
            market=market,
            liquidity_rule=liquidity_rule,
        )
        items = [item for item in inspection['items'] if item['is_low_liquidity'] is True]
        items.sort(
            key=lambda item: (
                item['low_liquidity_20d']['liquidity_rank'] or 999999,
                item['symbol'],
            )
        )
        return {
            'market_date': inspection['market_date'],
            'source': inspection['source'],
            'market': inspection['market'],
            'requested_symbols': inspection['requested_symbols'],
            'matched_symbols': inspection['matched_symbols'],
            'missing_symbols': inspection['missing_symbols'],
            'liquidity_rule': inspection['liquidity_rule'],
            'summary': {
                'matched_symbol_count': inspection['summary']['matched_symbol_count'],
                'liquidity_eligible_count': inspection['summary']['liquidity_eligible_count'],
                'low_liquidity_count': len(items),
            },
            'items': items,
        }

    def _build_symbol_result(
        self,
        *,
        symbol_row: dict[str, Any],
        bars: list[dict[str, Any]],
        market_date: date,
        liquidity_rule: LowLiquidityRuleConfig,
    ) -> dict[str, Any]:
        market_date_iso = market_date.isoformat()
        current_bar = bars[0] if bars and bars[0].get('trade_date') == market_date_iso else None
        source = current_bar.get('source') if current_bar else (bars[0].get('source') if bars else None)
        trading_halt_payload = self._build_trading_halt_payload(current_bar=current_bar)
        liquidity_payload = self._build_low_liquidity_payload(
            bars=bars,
            current_bar=current_bar,
            market_date_iso=market_date_iso,
            liquidity_rule=liquidity_rule,
        )
        return {
            'market_date': market_date_iso,
            'symbol': symbol_row['symbol'],
            'name_ko': symbol_row.get('name_ko'),
            'market': symbol_row.get('market'),
            'source': source,
            'is_trading_halt_like': trading_halt_payload['is_trading_halt_like'],
            'is_low_liquidity': liquidity_payload['is_low_liquidity'],
            'trading_halt_like': trading_halt_payload,
            'low_liquidity_20d': liquidity_payload,
        }

    def _build_trading_halt_payload(self, *, current_bar: dict[str, Any] | None) -> dict[str, Any]:
        if current_bar is None:
            return {
                'available': False,
                'is_trading_halt_like': None,
                'reason': 'missing_market_date_bar',
                'heuristic': {
                    'name': TRADING_HALT_LIKE_HEURISTIC,
                    'is_heuristic': True,
                },
                'evidence': None,
            }

        evidence = {
            'trade_date': current_bar['trade_date'],
            'open': current_bar['open_price'],
            'high': current_bar['high_price'],
            'low': current_bar['low_price'],
            'close': current_bar['close_price'],
            'volume': current_bar['volume'],
            'trading_value': current_bar['turnover'],
        }
        # pykrx market-wide daily rows can represent halted names with a non-zero close
        # while the rest of the OHLCV fields are zero-filled.
        is_trading_halt_like = (
            current_bar['close_price'] > 0
            and current_bar['open_price'] == 0
            and current_bar['high_price'] == 0
            and current_bar['low_price'] == 0
            and current_bar['volume'] == 0
            and current_bar['turnover'] == 0
        )
        return {
            'available': True,
            'is_trading_halt_like': is_trading_halt_like,
            'reason': (
                'pykrx_zero_fill_ohlcv_with_nonzero_close'
                if is_trading_halt_like
                else 'row_does_not_match_halt_heuristic'
            ),
            'heuristic': {
                'name': TRADING_HALT_LIKE_HEURISTIC,
                'is_heuristic': True,
            },
            'evidence': evidence,
        }

    def _build_low_liquidity_payload(
        self,
        *,
        bars: list[dict[str, Any]],
        current_bar: dict[str, Any] | None,
        market_date_iso: str,
        liquidity_rule: LowLiquidityRuleConfig,
    ) -> dict[str, Any]:
        if current_bar is None:
            return {
                'available': False,
                'is_low_liquidity': None,
                'reason': 'missing_market_date_bar',
                'avg_trading_value_20d': None,
                'liquidity_rank': None,
                'liquidity_percentile': None,
                'history_days_available': len(bars),
                'required_history_days': liquidity_rule.lookback_days,
                'window': None,
                'rule': liquidity_rule.to_metadata(),
            }

        if len(bars) < liquidity_rule.lookback_days:
            return {
                'available': False,
                'is_low_liquidity': None,
                'reason': 'insufficient_history',
                'avg_trading_value_20d': None,
                'liquidity_rank': None,
                'liquidity_percentile': None,
                'history_days_available': len(bars),
                'required_history_days': liquidity_rule.lookback_days,
                'window': {
                    'start_date': bars[-1]['trade_date'],
                    'end_date': market_date_iso,
                },
                'rule': liquidity_rule.to_metadata(),
            }

        # Stored `daily_price_bars` rows are trade-date rows, so a 20-row lookback is a
        # 20-trading-day liquidity window rather than a calendar-day window.
        avg_trading_value = _round(
            sum(float(bar['turnover']) for bar in bars) / liquidity_rule.lookback_days,
            4,
        )
        return {
            'available': True,
            'is_low_liquidity': None,
            'reason': 'awaiting_universe_ranking',
            'avg_trading_value_20d': avg_trading_value,
            'liquidity_rank': None,
            'liquidity_percentile': None,
            'history_days_available': len(bars),
            'required_history_days': liquidity_rule.lookback_days,
            'window': {
                'start_date': bars[-1]['trade_date'],
                'end_date': market_date_iso,
            },
            'rule': liquidity_rule.to_metadata(),
        }


pykrx_risk_filter_service = PykrxRiskFilterService()
