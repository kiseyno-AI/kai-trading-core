from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Mapping, Sequence

from app.market_data_store import (
    list_symbols_with_daily_bar_on_date,
    load_daily_signal_feature_context_rows,
    load_recent_daily_price_trade_dates,
    load_recent_price_bars_by_symbol,
    replace_daily_price_contexts_exact_scope,
    upsert_daily_market_participation_context,
)
from app.eod.market_membership import market_membership_sha256
from app.eod.scope_outcomes import build_exact_symbol_scope_outcome
from app.repositories.eod_run_repository import daily_price_bar_writer_admission


SIGNAL_PARTICIPATION_SOURCES: tuple[tuple[str, str, str], ...] = (
    ('volume-power', 'volume_power_rank', 'volume_power'),
    ('overtime-volume', 'overtime_volume_rank', 'overtime_volume'),
    ('overtime-fluctuation', 'overtime_fluctuation_rank', 'overtime_fluctuation_rate'),
)
PARTICIPATION_CONTEXT_LEADER_LIMIT = 20
DEFAULT_DERIVED_CONTEXT_HISTORY_DAYS = 7
PRICE_CONTEXT_SCOPE_MANIFEST_VERSION = 'price_contexts_scope_outcomes_v1'


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        text = value.strip().replace(',', '')
        if not text:
            return None
        try:
            return int(float(text))
        except ValueError:
            return None
    return None


def _round_float(value: float | None, *, digits: int = 4) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def _ratio(value: int | float | None, total: int | float | None) -> float | None:
    if value is None or total in (None, 0):
        return None
    return round(float(value) / float(total), 4)


def _percent(value: int | float | None, base: int | float | None) -> float | None:
    if value is None or base in (None, 0):
        return None
    return _round_float((float(value) / float(base)) * 100.0)


def _classify_body_size(body_percent_of_range: float | None) -> str | None:
    if body_percent_of_range is None:
        return None
    if body_percent_of_range <= 10.0:
        return 'small'
    if body_percent_of_range <= 40.0:
        return 'medium'
    return 'large'


def _classify_wick_bias(*, upper_wick_points: int, lower_wick_points: int) -> str:
    if upper_wick_points == 0 and lower_wick_points == 0:
        return 'none'
    larger = max(upper_wick_points, lower_wick_points)
    smaller = min(upper_wick_points, lower_wick_points)
    if smaller == 0:
        return 'upper' if upper_wick_points > lower_wick_points else 'lower'
    if larger / smaller < 1.25:
        return 'balanced'
    return 'upper' if upper_wick_points > lower_wick_points else 'lower'


def _classify_close_position(position: float | None) -> str | None:
    if position is None:
        return None
    if position <= 0.2:
        return 'near_low'
    if position <= 0.4:
        return 'lower_half'
    if position < 0.6:
        return 'middle'
    if position < 0.8:
        return 'upper_half'
    return 'near_high'


def _build_liquidity_context(
    *,
    close_price: int,
    volume: int | None,
    turnover: int | None,
) -> dict[str, Any]:
    provider_turnover_krw = turnover if turnover is not None and turnover > 0 else None
    estimated_turnover_krw = close_price * volume if close_price > 0 and volume is not None and volume > 0 else None
    if provider_turnover_krw is not None:
        turnover_source = 'provider'
        turnover_krw = provider_turnover_krw
    elif estimated_turnover_krw is not None:
        turnover_source = 'estimated'
        turnover_krw = estimated_turnover_krw
    else:
        turnover_source = 'missing'
        turnover_krw = None
    return {
        'turnover_krw': turnover_krw,
        'turnover_source': turnover_source,
        'turnover_is_estimated': turnover_source == 'estimated',
        'provider_turnover_krw': provider_turnover_krw,
        'estimated_turnover_krw': estimated_turnover_krw,
    }


def _build_price_bar_quality(
    *,
    open_price: int,
    high_price: int,
    low_price: int,
    close_price: int,
    volume: int | None,
    turnover: int | None,
) -> dict[str, Any]:
    open_price_zero = open_price <= 0
    turnover_missing_or_zero = turnover is None or turnover <= 0
    volume_zero = volume is None or volume <= 0
    gap_unreliable_due_to_open = open_price_zero
    possible_suspension_or_special_session = (
        open_price_zero
        and close_price > 0
        and (
            volume_zero
            or turnover_missing_or_zero
            or high_price <= 0
            or low_price <= 0
        )
    )
    issue_codes = [
        code
        for code, is_present in (
            ('open_price_zero', open_price_zero),
            ('turnover_missing_or_zero', turnover_missing_or_zero),
            ('volume_zero', volume_zero),
            ('gap_unreliable_due_to_open', gap_unreliable_due_to_open),
            ('possible_suspension_or_special_session', possible_suspension_or_special_session),
        )
        if is_present
    ]
    return {
        'open_price_zero': open_price_zero,
        'turnover_missing_or_zero': turnover_missing_or_zero,
        'volume_zero': volume_zero,
        'gap_unreliable_due_to_open': gap_unreliable_due_to_open,
        'possible_suspension_or_special_session': possible_suspension_or_special_session,
        'has_any_issue': bool(issue_codes),
        'issue_codes': issue_codes,
    }


def build_price_context(price_bars: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not price_bars:
        return None

    current = price_bars[0]
    previous = price_bars[1] if len(price_bars) > 1 else None

    open_price = _to_int(current.get('open_price'))
    high_price = _to_int(current.get('high_price'))
    low_price = _to_int(current.get('low_price'))
    close_price = _to_int(current.get('close_price'))
    if None in (open_price, high_price, low_price, close_price):
        return None
    volume = _to_int(current.get('volume'))
    turnover = _to_int(current.get('turnover'))
    previous_close_price = _to_int(previous.get('close_price')) if previous else None
    has_valid_open = open_price > 0

    range_points = max(high_price - low_price, 0)
    close_position_in_range = (
        _round_float((close_price - low_price) / range_points) if range_points > 0 else None
    )
    if has_valid_open:
        body_points_signed = close_price - open_price
        body_points = abs(body_points_signed)
        body_percent_of_range = _percent(body_points, range_points)
        upper_wick_points = max(high_price - max(open_price, close_price), 0)
        lower_wick_points = max(min(open_price, close_price) - low_price, 0)
        body_percent_of_open = _percent(body_points, open_price)
        session_range_percent_of_open = _percent(range_points, open_price)
        wick_bias = _classify_wick_bias(
            upper_wick_points=upper_wick_points,
            lower_wick_points=lower_wick_points,
        )
        if body_points_signed > 0:
            candle_direction = 'bullish'
        elif body_points_signed < 0:
            candle_direction = 'bearish'
        else:
            candle_direction = 'flat'
    else:
        body_points_signed = None
        body_points = None
        body_percent_of_range = None
        upper_wick_points = None
        lower_wick_points = None
        body_percent_of_open = None
        session_range_percent_of_open = None
        wick_bias = None
        candle_direction = 'unknown'

    gap_reliable = previous_close_price is not None and previous_close_price > 0 and has_valid_open
    gap_points = open_price - previous_close_price if gap_reliable else None
    gap_percent = _percent(gap_points, previous_close_price) if gap_points is not None else None
    gap_filled_intraday = None
    if gap_points is not None and gap_points > 0:
        gap_filled_intraday = low_price <= previous_close_price
    elif gap_points is not None and gap_points < 0:
        gap_filled_intraday = high_price >= previous_close_price

    if gap_points is None:
        gap_direction = None
    elif gap_points > 0:
        gap_direction = 'up'
    elif gap_points < 0:
        gap_direction = 'down'
    else:
        gap_direction = 'flat'

    liquidity = _build_liquidity_context(
        close_price=close_price,
        volume=volume,
        turnover=turnover,
    )
    price_bar_quality = _build_price_bar_quality(
        open_price=open_price,
        high_price=high_price,
        low_price=low_price,
        close_price=close_price,
        volume=volume,
        turnover=turnover,
    )

    return {
        'trade_date': current.get('trade_date'),
        'source': current.get('source'),
        'captured_at': current.get('captured_at'),
        'open_price': open_price,
        'high_price': high_price,
        'low_price': low_price,
        'close_price': close_price,
        'volume': volume,
        'turnover': turnover,
        'liquidity': liquidity,
        'price_bar_quality': price_bar_quality,
        'candle': {
            'direction': candle_direction,
            'body_points': body_points,
            'body_percent_of_open': body_percent_of_open,
            'body_percent_of_range': body_percent_of_range,
            'body_size': _classify_body_size(body_percent_of_range),
            'upper_wick_points': upper_wick_points,
            'lower_wick_points': lower_wick_points,
            'wick_bias': wick_bias,
        },
        'session_range': {
            'points': range_points,
            'percent_of_open': session_range_percent_of_open,
            'close_position_in_range': close_position_in_range,
            'close_position_label': _classify_close_position(close_position_in_range),
        },
        'gap': {
            'previous_trade_date': previous.get('trade_date') if previous else None,
            'previous_close_price': previous_close_price,
            'gap_points': gap_points,
            'gap_percent': gap_percent,
            'direction': gap_direction,
            'filled_intraday': gap_filled_intraday,
            'reliable': gap_reliable,
        },
    }


def build_signal_participation_context(
    rows: list[dict[str, Any]],
    *,
    limit: int,
) -> dict[str, Any]:
    distribution: dict[str, int] = {}
    row_kind_counts: dict[str, int] = {}
    for row in rows:
        signal_count = row.get('signal_count')
        if signal_count is None:
            continue
        key = str(int(signal_count))
        distribution[key] = distribution.get(key, 0) + 1
        row_kind = str((row.get('provenance') or {}).get('row_kind') or row.get('row_kind') or 'unknown')
        row_kind_counts[row_kind] = row_kind_counts.get(row_kind, 0) + 1

    source_counts = {
        source: sum(1 for row in rows if row.get(rank_key) is not None)
        for source, rank_key, _ in SIGNAL_PARTICIPATION_SOURCES
    }

    leaders_by_source: dict[str, list[dict[str, Any]]] = {}
    for source, rank_key, value_key in SIGNAL_PARTICIPATION_SOURCES:
        source_rows = [
            {
                'symbol': row['symbol'],
                'name_ko': row['name_ko'],
                'market': row['market'],
                'rank': row[rank_key],
                'metric_name': value_key,
                'metric_value': _to_float(row[value_key]),
                'signal_count': row['signal_count'],
            }
            for row in rows
            if row.get(rank_key) is not None
        ]
        source_rows.sort(key=lambda item: (item['rank'], item['symbol']))
        leaders_by_source[source] = source_rows[:limit]

    return {
        'captured_symbol_count': len(rows),
        'signal_count_distribution': dict(sorted(distribution.items())),
        'row_kind_counts': row_kind_counts,
        'direct_signal_source_row_count': sum(1 for row in rows if int(row.get('signal_count') or 0) > 0),
        'fallback_only_row_count': row_kind_counts.get('fallback_only', 0),
        'source_counts': source_counts,
        'leaders_by_source': leaders_by_source,
    }


def apply_participation_run_coverage(
    context: dict[str, Any] | None,
    *,
    symbols_total: int | None,
    symbols_succeeded: int | None,
) -> dict[str, Any]:
    base = dict(context or {})
    captured_symbol_count = int(base.get('captured_symbol_count') or 0)
    source_counts = {
        source: int((base.get('source_counts') or {}).get(source) or 0)
        for source, _, _ in SIGNAL_PARTICIPATION_SOURCES
    }
    leaders_by_source = {
        source: list((base.get('leaders_by_source') or {}).get(source) or [])
        for source, _, _ in SIGNAL_PARTICIPATION_SOURCES
    }

    return {
        'captured_symbol_count': captured_symbol_count,
        'coverage_ratio_vs_run_total': _ratio(captured_symbol_count, symbols_total),
        'coverage_ratio_vs_run_succeeded': _ratio(captured_symbol_count, symbols_succeeded),
        'signal_count_distribution': dict(base.get('signal_count_distribution') or {}),
        'source_counts': source_counts,
        'source_coverage_ratio_vs_run_succeeded': {
            source: _ratio(count, symbols_succeeded)
            for source, count in source_counts.items()
        },
        'leaders_by_source': leaders_by_source,
    }


class DailyPriceContextService:
    @daily_price_bar_writer_admission
    def rebuild_for_market_date(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
        exact_scope_symbols: Sequence[str] | None = None,
        source: str = 'pykrx',
        run_id: int | None = None,
        include_retry_metadata: bool = False,
        authoritative_market_scope: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = market_date if isinstance(market_date, date) else date.fromisoformat(market_date)
        target_symbols = list(dict.fromkeys(
            symbols
            if symbols is not None
            else (
                exact_scope_symbols
                if exact_scope_symbols is not None
                else list_symbols_with_daily_bar_on_date(
                    normalized_market_date.isoformat()
                )
            )
        ))
        authority_symbols = list(dict.fromkeys(
            exact_scope_symbols
            if exact_scope_symbols is not None
            else target_symbols
        ))
        outside_authority = sorted(set(target_symbols) - set(authority_symbols))
        if outside_authority:
            raise ValueError(
                'price context processing symbols outside exact scope: '
                + ','.join(outside_authority[:20])
            )
        price_bars_by_symbol = load_recent_price_bars_by_symbol(
            market_date=normalized_market_date,
            symbols=target_symbols,
        )

        items: list[dict[str, Any]] = []
        rows: list[dict[str, Any]] = []
        failed_symbols: list[str] = []
        expected_trade_date = normalized_market_date.isoformat()
        for symbol in target_symbols:
            try:
                price_bars = price_bars_by_symbol.get(symbol, [])
                if (
                    not price_bars
                    or price_bars[0].get('trade_date')
                    != expected_trade_date
                ):
                    failed_symbols.append(symbol)
                    continue

                context_payload = build_price_context(price_bars)
                if context_payload is None:
                    failed_symbols.append(symbol)
                    continue
                rows.append(
                    {
                        'symbol': symbol,
                        'market_date': normalized_market_date.isoformat(),
                        'source': source,
                        'context_payload': context_payload,
                    }
                )
            except Exception:
                failed_symbols.append(symbol)
                continue
            items.append(
                {
                    'symbol': symbol,
                    'market_date': normalized_market_date.isoformat(),
                    'price_context': context_payload,
                }
            )

        publication = (
            replace_daily_price_contexts_exact_scope(
                market_date=normalized_market_date,
                rows=rows,
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
        failed_symbols = [
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
            'market_date': normalized_market_date.isoformat(),
            'source': source,
            'requested_symbols': len(authority_symbols),
            'contexts_persisted': len(items),
            'contexts_failed': len(failed_symbols),
            'failed_symbols': failed_symbols[:50],
            'items': items,
            'scope_finalization': scope_finalization,
            **build_exact_symbol_scope_outcome(
                manifest_version=PRICE_CONTEXT_SCOPE_MANIFEST_VERSION,
                selected_symbols=authority_symbols,
                successful_symbols=successful_symbols,
                retry_debt_symbols=failed_symbols,
            ),
        }
        if include_retry_metadata:
            result['_retry_metadata'] = {'failed_symbols': failed_symbols}
        return result


class DailyMarketParticipationContextService:
    @daily_price_bar_writer_admission
    def rebuild_from_rows(
        self,
        *,
        market_date: str | date,
        rows: list[dict[str, Any]],
        market: str = 'KRX',
        source: str = 'kis_ranking',
        leader_limit: int = PARTICIPATION_CONTEXT_LEADER_LIMIT,
    ) -> dict[str, Any]:
        normalized_market_date = market_date if isinstance(market_date, date) else date.fromisoformat(market_date)
        context_payload = build_signal_participation_context(rows, limit=leader_limit)
        upsert_daily_market_participation_context(
            market_date=normalized_market_date,
            market=market,
            source=source,
            context_payload=context_payload,
        )
        return {
            'market': market,
            'market_date': normalized_market_date.isoformat(),
            'source': source,
            'captured_symbol_count': context_payload['captured_symbol_count'],
            'context_payload': context_payload,
        }

    @daily_price_bar_writer_admission
    def rebuild_for_market_date(
        self,
        *,
        market_date: str | date,
        market: str = 'KRX',
        source: str = 'kis_ranking',
        leader_limit: int = PARTICIPATION_CONTEXT_LEADER_LIMIT,
        exact_symbols: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = market_date if isinstance(market_date, date) else date.fromisoformat(market_date)
        normalized_exact_symbols = (
            sorted(
                {
                    str(symbol).strip().upper().zfill(6)
                    for symbol in exact_symbols
                    if str(symbol).strip()
                }
            )
            if exact_symbols is not None
            else None
        )
        if exact_symbols is not None and not normalized_exact_symbols:
            raise ValueError('participation exact scope must not be empty')
        rows = load_daily_signal_feature_context_rows(
            market_date=normalized_market_date,
            symbols=normalized_exact_symbols,
        )
        result = self.rebuild_from_rows(
            market_date=normalized_market_date,
            rows=rows,
            market=market,
            source=source,
            leader_limit=leader_limit,
        )
        result.update(
            {
                'exact_scope': normalized_exact_symbols is not None,
                'scope_symbol_count': len(normalized_exact_symbols or rows),
                'scope_membership_sha256': market_membership_sha256(
                    normalized_exact_symbols
                    if normalized_exact_symbols is not None
                    else [str(row.get('symbol') or '') for row in rows]
                ),
            }
        )
        return result


class DerivedContextBackfillService:
    def backfill_recent_history(
        self,
        *,
        end_date: str | date | None = None,
        trading_days: int = DEFAULT_DERIVED_CONTEXT_HISTORY_DAYS,
        market: str = 'KRX',
        price_context_source: str = 'pykrx',
        participation_context_source: str = 'kis_ranking',
    ) -> dict[str, Any]:
        if trading_days <= 0:
            raise ValueError('trading_days must be greater than 0')

        normalized_end_date = (
            end_date
            if isinstance(end_date, date)
            else date.fromisoformat(end_date)
            if end_date
            else (date.today() - timedelta(days=1))
        )
        market_dates = load_recent_daily_price_trade_dates(
            end_date=normalized_end_date,
            limit=trading_days,
        )
        if not market_dates:
            return {
                'market': market,
                'requested_trading_days': trading_days,
                'end_date': normalized_end_date.isoformat(),
                'market_dates': [],
                'price_contexts': {
                    'dates_processed': 0,
                    'contexts_persisted': 0,
                    'contexts_failed': 0,
                },
                'participation_contexts': {
                    'dates_processed': 0,
                    'dates_skipped': 0,
                    'captured_symbol_count': 0,
                    'skipped_dates': [],
                },
                'items': [],
            }

        items: list[dict[str, Any]] = []
        price_contexts_persisted = 0
        price_contexts_failed = 0
        participation_dates_processed = 0
        participation_dates_skipped = 0
        participation_captured_symbol_count = 0
        skipped_participation_dates: list[dict[str, str]] = []

        for market_date in market_dates:
            price_result = daily_price_context_service.rebuild_for_market_date(
                market_date=market_date,
                source=price_context_source,
            )
            price_contexts_persisted += int(price_result.get('contexts_persisted') or 0)
            price_contexts_failed += int(price_result.get('contexts_failed') or 0)

            participation_rows = load_daily_signal_feature_context_rows(market_date=market_date)
            participation_result: dict[str, Any]
            if participation_rows:
                participation_result = daily_market_participation_context_service.rebuild_from_rows(
                    market_date=market_date,
                    rows=participation_rows,
                    market=market,
                    source=participation_context_source,
                )
                participation_dates_processed += 1
                participation_captured_symbol_count += int(participation_result.get('captured_symbol_count') or 0)
            else:
                participation_dates_skipped += 1
                participation_result = {
                    'market': market,
                    'market_date': market_date,
                    'source': participation_context_source,
                    'skipped': True,
                    'reason': 'no_signal_features_for_market_date',
                }
                skipped_participation_dates.append(
                    {
                        'market_date': market_date,
                        'reason': 'no_signal_features_for_market_date',
                    }
                )

            items.append(
                {
                    'market_date': market_date,
                    'price_contexts': price_result,
                    'participation_context': participation_result,
                }
            )

        return {
            'market': market,
            'requested_trading_days': trading_days,
            'end_date': normalized_end_date.isoformat(),
            'market_dates': market_dates,
            'price_contexts': {
                'dates_processed': len(market_dates),
                'contexts_persisted': price_contexts_persisted,
                'contexts_failed': price_contexts_failed,
            },
            'participation_contexts': {
                'dates_processed': participation_dates_processed,
                'dates_skipped': participation_dates_skipped,
                'captured_symbol_count': participation_captured_symbol_count,
                'skipped_dates': skipped_participation_dates,
            },
            'items': items,
        }


daily_price_context_service = DailyPriceContextService()
daily_market_participation_context_service = DailyMarketParticipationContextService()
derived_context_backfill_service = DerivedContextBackfillService()
