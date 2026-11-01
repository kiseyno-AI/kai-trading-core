
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Iterable, Mapping, Sequence

from app.eod.scope_outcomes import build_exact_symbol_scope_outcome
from app.market_data_store import (
    list_symbols_with_daily_bar_on_date,
    load_daily_action_summaries,
    load_daily_technical_snapshot_inputs,
    replace_daily_action_summaries_exact_scope,
)
from app.repositories.eod_run_repository import daily_price_bar_writer_admission

SUMMARY_VERSION = 'mvp_v1'
ACTION_SUMMARY_SCOPE_MANIFEST_VERSION = (
    'action_summaries_scope_outcomes_v1'
)


def _round(value: float | None, digits: int = 4) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def _score_short_return(value: float | None) -> int:
    if value is None:
        return 0
    if value >= 4:
        return 10
    if value >= 1:
        return 6
    if value > 0:
        return 3
    if value <= -4:
        return -10
    if value <= -1:
        return -6
    if value < 0:
        return -3
    return 0


def _score_medium_return(value: float | None) -> int:
    if value is None:
        return 0
    if value >= 12:
        return 15
    if value >= 5:
        return 10
    if value > 0:
        return 5
    if value <= -12:
        return -15
    if value <= -5:
        return -10
    if value < 0:
        return -5
    return 0


def _clamp_score(value: int) -> int:
    return max(-100, min(100, int(value)))


@dataclass
class DailyActionSummaryComputation:
    technical_snapshot_id: int | None
    symbol: str
    market_date: str
    source: str
    action_label: str
    bias_direction: str
    signal_score: int
    confidence_tier: str
    risk_tier: str
    trigger_price: float | None
    invalidation_price: float | None
    support_price: float | None
    resistance_price: float | None
    reasons: list[str]
    summary_payload: dict

    def to_row(self) -> dict:
        return {
            'technical_snapshot_id': self.technical_snapshot_id,
            'symbol': self.symbol,
            'market_date': self.market_date,
            'source': self.source,
            'action_label': self.action_label,
            'bias_direction': self.bias_direction,
            'signal_score': self.signal_score,
            'confidence_tier': self.confidence_tier,
            'risk_tier': self.risk_tier,
            'trigger_price': self.trigger_price,
            'invalidation_price': self.invalidation_price,
            'support_price': self.support_price,
            'resistance_price': self.resistance_price,
            'reasons': self.reasons,
            'summary_payload': self.summary_payload,
        }


class DailyActionSummaryService:
    def compute_summary(self, snapshot: dict) -> DailyActionSummaryComputation:
        signal_payload = snapshot.get('signal_payload') or {}
        range_context = signal_payload.get('range') or {}
        volume_context = signal_payload.get('volume') or {}
        volatility_context = signal_payload.get('volatility') or {}
        stop_context = volatility_context.get('stop_context') or {}
        price_bar_context = signal_payload.get('price_bar_context') or {}
        price_bar_quality = price_bar_context.get('price_bar_quality') or {}
        liquidity_context = price_bar_context.get('liquidity') or {}
        gap_context = price_bar_context.get('gap') or {}

        close_price = int(snapshot['close_price'])
        atr_14 = snapshot.get('atr_14')
        atr_pct_14 = snapshot.get('atr_pct_14')
        return_5d = snapshot.get('return_5d')
        return_20d = snapshot.get('return_20d')
        volume_ratio_20d = snapshot.get('volume_ratio_20d')
        breakout_20d = bool(snapshot.get('breakout_20d'))
        breakdown_20d = bool(snapshot.get('breakdown_20d'))
        prior_high_20 = _round(range_context.get('prior_20d_high'))
        prior_low_20 = _round(range_context.get('prior_20d_low'))
        one_atr_below_close = _round(stop_context.get('one_atr_below_close'))
        one_atr_above_close = _round(close_price + atr_14 if atr_14 is not None else None)

        components: dict[str, int] = {}
        reasons: list[str] = []

        ma_alignment = snapshot['ma_alignment']
        trend_state = snapshot['trend_state']
        components['ma_alignment'] = {
            'bullish': 25,
            'bearish': -25,
            'mixed': 0,
            'insufficient_history': 0,
        }.get(ma_alignment, 0)
        if ma_alignment == 'bullish':
            reasons.append('bullish_ma_alignment')
        elif ma_alignment == 'bearish':
            reasons.append('bearish_ma_alignment')
        elif ma_alignment == 'mixed':
            reasons.append('mixed_ma_alignment')
        else:
            reasons.append('insufficient_history')

        components['trend_state'] = {
            'uptrend': 20,
            'downtrend': -20,
            'range': 0,
            'insufficient_history': 0,
        }.get(trend_state, 0)
        if trend_state == 'uptrend':
            reasons.append('uptrend_state')
        elif trend_state == 'downtrend':
            reasons.append('downtrend_state')
        elif trend_state == 'range':
            reasons.append('range_bound_state')

        components['return_5d'] = _score_short_return(return_5d)
        if components['return_5d'] > 0:
            reasons.append('positive_5d_momentum')
        elif components['return_5d'] < 0:
            reasons.append('negative_5d_momentum')

        components['return_20d'] = _score_medium_return(return_20d)
        if components['return_20d'] > 0:
            reasons.append('positive_20d_momentum')
        elif components['return_20d'] < 0:
            reasons.append('negative_20d_momentum')

        components['breakout_breakdown'] = 0
        if breakout_20d:
            components['breakout_breakdown'] += 20
            reasons.append('fresh_20d_breakout')
        if breakdown_20d:
            components['breakout_breakdown'] -= 20
            reasons.append('fresh_20d_breakdown')

        components['volume_confirmation'] = 0
        if volume_ratio_20d is not None:
            if volume_ratio_20d >= 1.8:
                if breakout_20d or trend_state == 'uptrend':
                    components['volume_confirmation'] = 10
                    reasons.append('volume_confirms_strength')
                elif breakdown_20d or trend_state == 'downtrend':
                    components['volume_confirmation'] = -10
                    reasons.append('volume_confirms_weakness')
                else:
                    components['volume_confirmation'] = 3
                    reasons.append('unusual_volume_present')
            elif volume_ratio_20d >= 1.2:
                if trend_state == 'uptrend':
                    components['volume_confirmation'] = 4
                    reasons.append('volume_supports_trend')
                elif trend_state == 'downtrend':
                    components['volume_confirmation'] = -4
                    reasons.append('volume_supports_downtrend')
            elif volume_ratio_20d <= 0.7:
                components['volume_confirmation'] = -4 if trend_state == 'uptrend' else 0
                reasons.append('weak_volume_participation')

        raw_score = sum(components.values())
        signal_score = _clamp_score(raw_score)
        bias_direction = 'neutral'
        if signal_score >= 15:
            bias_direction = 'long'
        elif signal_score <= -15:
            bias_direction = 'short'

        if ma_alignment == 'insufficient_history':
            action_label = 'insufficient_data'
        elif signal_score >= 70 and breakout_20d:
            action_label = 'long_breakout'
        elif signal_score >= 45:
            action_label = 'long_bias'
        elif signal_score >= 15:
            action_label = 'watch_long'
        elif signal_score <= -70 and breakdown_20d:
            action_label = 'short_breakdown'
        elif signal_score <= -45:
            action_label = 'risk_off'
        elif signal_score <= -15:
            action_label = 'watch_short'
        else:
            action_label = 'neutral'

        observed_fields = [return_5d, return_20d, volume_ratio_20d, atr_pct_14, prior_high_20, prior_low_20]
        data_points = sum(value is not None for value in observed_fields)
        if ma_alignment != 'insufficient_history':
            data_points += 1
        if trend_state != 'insufficient_history':
            data_points += 1
        if abs(signal_score) >= 70 and data_points >= 6:
            confidence_tier = 'high'
        elif abs(signal_score) >= 35 and data_points >= 4:
            confidence_tier = 'medium'
        else:
            confidence_tier = 'low'

        if atr_pct_14 is None:
            risk_tier = 'unknown'
        elif atr_pct_14 >= 5.5:
            risk_tier = 'high'
            reasons.append('high_atr_risk')
        elif atr_pct_14 >= 3.0:
            risk_tier = 'medium'
            reasons.append('moderate_atr_risk')
        else:
            risk_tier = 'low'
            reasons.append('contained_atr_risk')

        support_price = prior_low_20
        resistance_price = prior_high_20
        if action_label in {'long_breakout', 'long_bias'}:
            trigger_price = _round(close_price)
            invalidation_price = one_atr_below_close or support_price
        elif action_label == 'watch_long':
            trigger_price = resistance_price or _round(close_price)
            invalidation_price = one_atr_below_close or support_price
        elif action_label in {'short_breakdown', 'risk_off'}:
            trigger_price = _round(close_price)
            invalidation_price = one_atr_above_close or resistance_price
        elif action_label == 'watch_short':
            trigger_price = support_price or _round(close_price)
            invalidation_price = one_atr_above_close or resistance_price
        elif bias_direction == 'long':
            trigger_price = resistance_price or _round(close_price)
            invalidation_price = one_atr_below_close
        elif bias_direction == 'short':
            trigger_price = support_price or _round(close_price)
            invalidation_price = one_atr_above_close
        else:
            trigger_price = None
            invalidation_price = None

        setup_state = {
            'long_breakout': 'confirmed',
            'short_breakdown': 'confirmed',
            'long_bias': 'active',
            'risk_off': 'active',
            'watch_long': 'watch',
            'watch_short': 'watch',
            'neutral': 'neutral',
            'insufficient_data': 'neutral',
        }[action_label]

        summary_payload = {
            'summary_version': SUMMARY_VERSION,
            'symbol': snapshot['symbol'],
            'market_date': snapshot['market_date'],
            'bias_direction': bias_direction,
            'setup_state': setup_state,
            'reason_codes': reasons,
            'score_components': components,
            'levels': {
                'trigger_price': trigger_price,
                'invalidation_price': invalidation_price,
                'support_price': support_price,
                'resistance_price': resistance_price,
            },
            'technical_context': {
                'trend_state': trend_state,
                'ma_alignment': ma_alignment,
                'breakout_20d': breakout_20d,
                'breakdown_20d': breakdown_20d,
                'return_5d': return_5d,
                'return_20d': return_20d,
                'volume_ratio_20d': volume_ratio_20d,
                'atr_pct_14': atr_pct_14,
            },
            'data_quality': {
                'price_bar_quality': {
                    'open_price_zero': bool(price_bar_quality.get('open_price_zero')),
                    'turnover_missing_or_zero': bool(price_bar_quality.get('turnover_missing_or_zero')),
                    'volume_zero': bool(price_bar_quality.get('volume_zero')),
                    'gap_unreliable_due_to_open': bool(price_bar_quality.get('gap_unreliable_due_to_open')),
                    'possible_suspension_or_special_session': bool(
                        price_bar_quality.get('possible_suspension_or_special_session')
                    ),
                    'issue_codes': list(price_bar_quality.get('issue_codes') or []),
                },
                'liquidity': {
                    'turnover_krw': liquidity_context.get('turnover_krw'),
                    'turnover_source': liquidity_context.get('turnover_source'),
                    'turnover_is_estimated': bool(liquidity_context.get('turnover_is_estimated')),
                    'provider_turnover_krw': liquidity_context.get('provider_turnover_krw'),
                    'estimated_turnover_krw': liquidity_context.get('estimated_turnover_krw'),
                },
                'gap': {
                    'direction': gap_context.get('direction'),
                    'gap_percent': gap_context.get('gap_percent'),
                    'reliable': gap_context.get('reliable'),
                },
            },
        }

        return DailyActionSummaryComputation(
            technical_snapshot_id=snapshot.get('technical_snapshot_id'),
            symbol=snapshot['symbol'],
            market_date=snapshot['market_date'],
            source=snapshot.get('source', 'pykrx'),
            action_label=action_label,
            bias_direction=bias_direction,
            signal_score=signal_score,
            confidence_tier=confidence_tier,
            risk_tier=risk_tier,
            trigger_price=trigger_price,
            invalidation_price=invalidation_price,
            support_price=support_price,
            resistance_price=resistance_price,
            reasons=list(dict.fromkeys(reasons)),
            summary_payload=summary_payload,
        )

    @daily_price_bar_writer_admission
    def rebuild_for_market_date(
        self,
        *,
        market_date: str,
        symbols: Sequence[str] | None = None,
        exact_scope_symbols: Sequence[str] | None = None,
        source: str = 'pykrx',
        run_id: int | None = None,
        progress_callback: Callable[[dict], None] | None = None,
        include_retry_metadata: bool = False,
        authoritative_market_scope: Mapping[str, Any] | None = None,
    ) -> dict:
        requested_symbols = list(dict.fromkeys(
            symbols
            if symbols is not None
            else (
                exact_scope_symbols
                if exact_scope_symbols is not None
                else list_symbols_with_daily_bar_on_date(market_date)
            )
        ))
        authority_symbols = list(dict.fromkeys(
            exact_scope_symbols
            if exact_scope_symbols is not None
            else requested_symbols
        ))
        outside_authority = sorted(
            set(requested_symbols) - set(authority_symbols)
        )
        if outside_authority:
            raise ValueError(
                'action summary processing symbols outside exact scope: '
                + ','.join(outside_authority[:20])
            )
        snapshots = (
            load_daily_technical_snapshot_inputs(
                market_date=market_date,
                symbols=requested_symbols,
                limit=max(len(requested_symbols), 100),
            )
            if requested_symbols
            else []
        )
        available_symbols = {item['symbol'] for item in snapshots}

        items: list[dict] = []
        failed_symbols: list[str] = [symbol for symbol in requested_symbols if symbol not in available_symbols]

        def _emit_progress(payload: dict) -> None:
            if not progress_callback:
                return
            try:
                progress_callback(payload)
            except Exception:
                # Progress persistence is best-effort and must not interrupt action summary processing.
                return

        if progress_callback:
            first_symbol = snapshots[0]['symbol'] if snapshots else None
            _emit_progress(
                {
                    'status': 'running' if first_symbol else ('completed' if not failed_symbols else 'partial'),
                    'processed': len(failed_symbols),
                    'succeeded': 0,
                    'failed': len(failed_symbols),
                    'total': len(requested_symbols),
                    'current_symbol': first_symbol,
                    'current_step': 'computing_action_summary' if first_symbol else 'action_summaries_completed',
                    'current_activity': (
                        f'Computing daily action summaries for {first_symbol}'
                        if first_symbol
                        else 'No technical snapshots available for daily action summaries'
                    ),
                }
            )

        for index, snapshot in enumerate(snapshots):
            try:
                summary = self.compute_summary(snapshot)
                row = summary.to_row()
                row['source'] = source
                items.append(row)
            except Exception:
                failed_symbols.append(snapshot['symbol'])
            if progress_callback:
                next_symbol = snapshots[index + 1]['symbol'] if index + 1 < len(snapshots) else None
                _emit_progress(
                    {
                        'status': (
                            'running'
                            if next_symbol is not None
                            else ('completed' if not failed_symbols else 'partial')
                        ),
                        'processed': len(items) + len(failed_symbols),
                        'succeeded': len(items),
                        'failed': len(failed_symbols),
                        'total': len(requested_symbols),
                        'current_symbol': next_symbol,
                        'current_step': (
                            'computing_action_summary'
                            if next_symbol is not None
                            else 'action_summaries_completed'
                        ),
                        'current_activity': (
                            f'Computing daily action summaries for {next_symbol}'
                            if next_symbol is not None
                            else 'Completed daily action summaries'
                        ),
                    }
                )

        publication = (
            replace_daily_action_summaries_exact_scope(
                market_date=market_date,
                rows=items,
                exact_symbols=authority_symbols,
                run_id=run_id,
                **(
                    {
                        'authoritative_market_scope': (
                            authoritative_market_scope
                        )
                    }
                    if authoritative_market_scope is not None
                    else {}
                ),
            )
            if authority_symbols
            else None
        )
        successful_symbols = (
            list(publication['successful_symbols'])
            if publication is not None
            else []
        )
        successful_set = set(successful_symbols)
        items = [
            item for item in items if item['symbol'] in successful_set
        ]
        deduped_failed = [
            symbol
            for symbol in authority_symbols
            if symbol not in successful_set
        ]
        scope_finalization = (
            publication['scope_finalization']
            if publication is not None
            else None
        )
        result = {
            'market_date': market_date,
            'source': source,
            'requested_symbols': len(authority_symbols),
            'action_summaries_persisted': len(items),
            'action_summaries_failed': len(deduped_failed),
            'failed_symbols': deduped_failed[:50],
            'items': items,
            'scope_finalization': scope_finalization,
            'denominator_source': 'same_day_daily_price_bars',
            **build_exact_symbol_scope_outcome(
                manifest_version=ACTION_SUMMARY_SCOPE_MANIFEST_VERSION,
                selected_symbols=authority_symbols,
                successful_symbols=successful_symbols,
                retry_debt_symbols=deduped_failed,
            ),
        }
        if include_retry_metadata:
            result['_retry_metadata'] = {'failed_symbols': deduped_failed}
        return result

    def inspect(
        self,
        *,
        market_date: str,
        symbols: Sequence[str] | None = None,
        limit: int = 100,
    ) -> dict:
        items = load_daily_action_summaries(market_date=market_date, symbols=list(symbols or []), limit=limit)
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


daily_action_summary_service = DailyActionSummaryService()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Compute daily action summaries from stored technical snapshots')
    parser.add_argument('--market-date', required=True, help='Market date in YYYY-MM-DD format')
    parser.add_argument('--source', default='pykrx')
    parser.add_argument('--symbol', action='append', dest='symbols')
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = daily_action_summary_service.rebuild_for_market_date(
        market_date=date.fromisoformat(args.market_date).isoformat(),
        symbols=args.symbols,
        source=args.source,
    )
    print(result)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
