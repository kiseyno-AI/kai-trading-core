from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from typing import Callable, Iterable, Sequence

from app.agent_context.derived_contexts import build_price_context
from app.eod.scope_outcomes import build_exact_symbol_scope_outcome
from app.market_data_store import (
    list_symbols_with_daily_bar_on_date,
    load_daily_bars_for_symbol,
    load_daily_technical_snapshots,
    upsert_daily_technical_snapshot,
)


TECHNICAL_SNAPSHOT_SCOPE_MANIFEST_VERSION = (
    'technical_snapshots_scope_outcomes_v1'
)


def _round(value: float | None, digits: int = 4) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def _average(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _moving_average(values: Sequence[float], window: int) -> float | None:
    if len(values) < window:
        return None
    return _average(values[-window:])


def _return_pct(values: Sequence[float], window: int) -> float | None:
    if len(values) <= window:
        return None
    baseline = values[-(window + 1)]
    if baseline == 0:
        return None
    return ((values[-1] / baseline) - 1.0) * 100.0


def _true_ranges(bars: Sequence[dict]) -> list[float]:
    values: list[float] = []
    for index in range(1, len(bars)):
        current = bars[index]
        previous_close = float(bars[index - 1]['close_price'])
        high = float(current['high_price'])
        low = float(current['low_price'])
        values.append(max(high - low, abs(high - previous_close), abs(low - previous_close)))
    return values


def _build_price_bar_context(bars: Sequence[dict]) -> dict | None:
    if not bars:
        return None
    latest_and_previous = list(reversed(list(bars[-2:])))
    return build_price_context(latest_and_previous)


@dataclass
class TechnicalSnapshotComputation:
    symbol: str
    market_date: str
    source: str
    close_price: int
    volume: int
    trend_state: str
    ma_alignment: str
    breakout_20d: bool
    breakdown_20d: bool
    return_5d: float | None
    return_20d: float | None
    volume_ratio_20d: float | None
    atr_14: float | None
    atr_pct_14: float | None
    signal_payload: dict

    def to_row(self) -> dict:
        return {
            'symbol': self.symbol,
            'market_date': self.market_date,
            'source': self.source,
            'close_price': self.close_price,
            'volume': self.volume,
            'trend_state': self.trend_state,
            'ma_alignment': self.ma_alignment,
            'breakout_20d': self.breakout_20d,
            'breakdown_20d': self.breakdown_20d,
            'return_5d': self.return_5d,
            'return_20d': self.return_20d,
            'volume_ratio_20d': self.volume_ratio_20d,
            'atr_14': self.atr_14,
            'atr_pct_14': self.atr_pct_14,
            'signal_payload': self.signal_payload,
        }


class DailyTechnicalSnapshotService:
    history_window = 60

    def compute_snapshot(
        self,
        *,
        symbol: str,
        market_date: str,
        bars: Sequence[dict],
        source: str = 'pykrx',
    ) -> TechnicalSnapshotComputation:
        if not bars:
            raise ValueError(f'No daily bars available for {symbol} on or before {market_date}.')

        latest = bars[-1]
        latest_trade_date = latest['trade_date']
        if latest_trade_date != market_date:
            raise ValueError(f'Latest stored bar for {symbol} is {latest_trade_date}, not {market_date}.')

        closes = [float(item['close_price']) for item in bars]
        volumes = [float(item['volume']) for item in bars]

        ma_10 = _moving_average(closes, 10)
        ma_20 = _moving_average(closes, 20)
        ma_50 = _moving_average(closes, 50)
        return_5d = _round(_return_pct(closes, 5))
        return_20d = _round(_return_pct(closes, 20))

        average_volume_20d = _average(volumes[-20:]) if len(volumes) >= 20 else None
        volume_ratio_20d = _round((volumes[-1] / average_volume_20d) if average_volume_20d not in (None, 0) else None)

        trailing_20 = bars[-21:-1] if len(bars) >= 21 else []
        prior_high_20 = max((float(item['high_price']) for item in trailing_20), default=None)
        prior_low_20 = min((float(item['low_price']) for item in trailing_20), default=None)
        close_price = int(latest['close_price'])
        price_bar_context = _build_price_bar_context(bars)

        breakout_20d = bool(prior_high_20 is not None and close_price > prior_high_20)
        breakdown_20d = bool(prior_low_20 is not None and close_price < prior_low_20)

        true_ranges = _true_ranges(bars)
        atr_14 = _round(_average(true_ranges[-14:]) if len(true_ranges) >= 14 else None)
        atr_pct_14 = _round((atr_14 / close_price) * 100.0 if atr_14 is not None and close_price else None)

        ma_alignment = 'insufficient_history'
        trend_state = 'insufficient_history'
        if None not in (ma_10, ma_20, ma_50):
            if close_price > ma_10 > ma_20 > ma_50:
                ma_alignment = 'bullish'
                trend_state = 'uptrend'
            elif close_price < ma_10 < ma_20 < ma_50:
                ma_alignment = 'bearish'
                trend_state = 'downtrend'
            else:
                ma_alignment = 'mixed'
                trend_state = 'range'

        stop_multiple_1_atr = _round(close_price - atr_14 if atr_14 is not None else None)
        signal_payload = {
            'symbol': symbol,
            'market_date': market_date,
            'source': source,
            'close_price': close_price,
            'moving_averages': {
                'ma_10': _round(ma_10),
                'ma_20': _round(ma_20),
                'ma_50': _round(ma_50),
                'alignment': ma_alignment,
            },
            'returns': {
                'return_5d_pct': return_5d,
                'return_20d_pct': return_20d,
            },
            'volume': {
                'current': int(latest['volume']),
                'average_20d': _round(average_volume_20d),
                'ratio_20d': volume_ratio_20d,
                'unusual_volume': bool(volume_ratio_20d is not None and volume_ratio_20d >= 1.5),
            },
            'range': {
                'prior_20d_high': _round(prior_high_20),
                'prior_20d_low': _round(prior_low_20),
                'breakout_20d': breakout_20d,
                'breakdown_20d': breakdown_20d,
            },
            'volatility': {
                'atr_14': atr_14,
                'atr_pct_14': atr_pct_14,
                'stop_context': {
                    'one_atr_below_close': stop_multiple_1_atr,
                },
            },
            'price_bar_context': price_bar_context,
            'trend_state': trend_state,
        }

        return TechnicalSnapshotComputation(
            symbol=symbol,
            market_date=market_date,
            source=source,
            close_price=close_price,
            volume=int(latest['volume']),
            trend_state=trend_state,
            ma_alignment=ma_alignment,
            breakout_20d=breakout_20d,
            breakdown_20d=breakdown_20d,
            return_5d=return_5d,
            return_20d=return_20d,
            volume_ratio_20d=volume_ratio_20d,
            atr_14=atr_14,
            atr_pct_14=atr_pct_14,
            signal_payload=signal_payload,
        )

    def rebuild_for_market_date(
        self,
        *,
        market_date: str,
        symbols: Sequence[str] | None = None,
        source: str = 'pykrx',
        run_id: int | None = None,
        progress_callback: Callable[[dict], None] | None = None,
        include_retry_metadata: bool = False,
    ) -> dict:
        target_symbols = list(
            dict.fromkeys(
                symbols
                if symbols
                else list_symbols_with_daily_bar_on_date(market_date)
            )
        )
        snapshots: list[dict] = []
        failed_symbols: list[str] = []

        def _emit_progress(payload: dict) -> None:
            if not progress_callback:
                return
            try:
                progress_callback(payload)
            except Exception:
                # Progress persistence is best-effort and must not interrupt snapshot processing.
                return

        if progress_callback:
            first_symbol = target_symbols[0] if target_symbols else None
            _emit_progress(
                {
                    'status': 'running' if first_symbol else 'completed',
                    'processed': 0,
                    'succeeded': 0,
                    'failed': 0,
                    'total': len(target_symbols),
                    'current_symbol': first_symbol,
                    'current_step': 'computing_symbol_snapshot' if first_symbol else 'technical_snapshots_completed',
                    'current_activity': (
                        f'Computing daily technical snapshots for {first_symbol}'
                        if first_symbol
                        else 'No symbols available for daily technical snapshots'
                    ),
                }
            )

        for index, symbol in enumerate(target_symbols):
            try:
                bars = load_daily_bars_for_symbol(symbol, end_date=market_date, limit=self.history_window)
                snapshot = self.compute_snapshot(symbol=symbol, market_date=market_date, bars=bars, source=source)
                upsert_daily_technical_snapshot(snapshot.to_row(), run_id=run_id)
                snapshots.append(snapshot.to_row())
            except Exception:
                failed_symbols.append(symbol)
            if progress_callback:
                next_symbol = target_symbols[index + 1] if index + 1 < len(target_symbols) else None
                _emit_progress(
                    {
                        'status': (
                            'running'
                            if next_symbol is not None
                            else ('completed' if not failed_symbols else 'partial')
                        ),
                        'processed': len(snapshots) + len(failed_symbols),
                        'succeeded': len(snapshots),
                        'failed': len(failed_symbols),
                        'total': len(target_symbols),
                        'current_symbol': next_symbol,
                        'current_step': (
                            'computing_symbol_snapshot'
                            if next_symbol is not None
                            else 'technical_snapshots_completed'
                        ),
                        'current_activity': (
                            f'Computing daily technical snapshots for {next_symbol}'
                            if next_symbol is not None
                            else 'Completed daily technical snapshots'
                        ),
                    }
                )

        result = {
            'market_date': market_date,
            'source': source,
            'requested_symbols': len(target_symbols),
            'snapshots_persisted': len(snapshots),
            'snapshots_failed': len(failed_symbols),
            'failed_symbols': failed_symbols[:50],
            'items': snapshots,
            **build_exact_symbol_scope_outcome(
                manifest_version=(
                    TECHNICAL_SNAPSHOT_SCOPE_MANIFEST_VERSION
                ),
                selected_symbols=target_symbols,
                successful_symbols=[
                    snapshot['symbol'] for snapshot in snapshots
                ],
                retry_debt_symbols=failed_symbols,
            ),
        }
        if include_retry_metadata:
            result['_retry_metadata'] = {'failed_symbols': failed_symbols}
        return result

    def inspect(
        self,
        *,
        market_date: str,
        symbols: Sequence[str] | None = None,
        limit: int = 100,
    ) -> dict:
        items = load_daily_technical_snapshots(market_date=market_date, symbols=list(symbols or []), limit=limit)
        requested_symbols = list(symbols or [])
        matched_symbols = [item['symbol'] for item in items]
        return {
            'market_date': market_date,
            'summary': {
                'entries': len(items),
                'requested_symbols': requested_symbols,
                'matched_symbols': matched_symbols,
                'missing_symbols': [symbol for symbol in requested_symbols if symbol not in set(matched_symbols)],
            },
            'items': items,
        }


daily_technical_snapshot_service = DailyTechnicalSnapshotService()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Compute daily technical snapshots from stored EOD bars')
    parser.add_argument('--market-date', required=True, help='Market date in YYYY-MM-DD format')
    parser.add_argument('--source', default='pykrx')
    parser.add_argument('--symbol', action='append', dest='symbols')
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = daily_technical_snapshot_service.rebuild_for_market_date(
        market_date=date.fromisoformat(args.market_date).isoformat(),
        symbols=args.symbols,
        source=args.source,
    )
    print(result)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
