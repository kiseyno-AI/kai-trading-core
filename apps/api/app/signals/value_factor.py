from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping, Sequence

from app.market_data_store import (
    load_daily_pykrx_enrichment_rows,
    list_symbols_with_daily_bar_on_date,
    load_daily_value_factor_results,
    load_recent_price_bars_by_symbol,
    load_symbols,
    replace_daily_derived_results_exact_scope,
    upsert_daily_value_factor_result,
    upsert_daily_value_factor_results,
)
from app.repositories.eod_run_repository import daily_price_bar_writer_admission
from app.signals.factor_coverage import (
    attach_ranking_dependency_contract,
    attach_symbol_payloads,
    build_symbol_results,
    build_factor_coverage_contract,
    factor_stage_result_contract,
    merge_symbol_failures,
    partition_symbol_failures,
    persist_symbol_items_with_isolation,
    run_symbol_computation,
)

VALUE_FACTOR_VERSION = 'value_factor_v1'
VALUE_FACTOR_SOURCE = VALUE_FACTOR_VERSION
VALUE_STATUS_SCORED = 'scored'
VALUE_STATUS_INVALID_PER = 'invalid_per'
VALUE_STATUS_MISSING_METRIC = 'missing_metric'
VALUE_STATUS_MISSING_MARKET_DATE_BAR = 'missing_market_date_bar'
METRIC_STATUS_AVAILABLE = 'available'
METRIC_STATUS_MISSING = 'missing'
METRIC_STATUS_INVALID_NON_POSITIVE = 'invalid_non_positive'
METRIC_STATUS_INVALID_NEGATIVE = 'invalid_negative'
VALUE_SCORE_COMPONENT_KEYS = (
    'per_percentile',
    'pbr_percentile',
    'dividend_yield_percentile',
)
EMPTY_RANKING_UNIVERSE_COUNTS = {
    'per': 0,
    'pbr': 0,
    'dividend_yield': 0,
    'composite_score': 0,
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


def _positive_only_metric_status(value: float | None) -> str:
    if value is None:
        return METRIC_STATUS_MISSING
    if float(value) <= 0:
        return METRIC_STATUS_INVALID_NON_POSITIVE
    return METRIC_STATUS_AVAILABLE


def _non_negative_metric_status(value: float | None) -> str:
    if value is None:
        return METRIC_STATUS_MISSING
    if float(value) < 0:
        return METRIC_STATUS_INVALID_NEGATIVE
    return METRIC_STATUS_AVAILABLE


def _sort_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        items,
        key=lambda item: (
            item.get('value_status') != VALUE_STATUS_SCORED,
            item.get('value_rank') is None,
            item.get('value_rank') if item.get('value_rank') is not None else 999999,
            item['symbol'],
        ),
    )


@dataclass(slots=True)
class ValueFactorInput:
    symbol: str
    name_ko: str | None
    market: str | None
    bars: list[dict[str, Any]]
    valuation_snapshot: dict[str, Any] | None = None


class DailyValueFactorService:
    @daily_price_bar_writer_admission
    def rebuild_for_market_date(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
        ranking_symbols: Sequence[str] | None = None,
        source: str = VALUE_FACTOR_SOURCE,
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
                        stage_id='value_factors',
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
        ranking_missing_symbols: list[str] = []
        if (
            normalized_symbols is not None
            and normalized_symbols != normalized_ranking_symbols
        ):
            ranking_inputs, _, ranking_missing_symbols = self._load_inputs(
                market_date=normalized_market_date,
                symbols=(
                    normalized_ranking_symbols
                    if ranking_symbols is not None
                    else None
                ),
            )

        (
            ranking_items,
            ranking_failed_symbols,
            ranking_failed_symbol_errors,
        ) = build_symbol_results(
            ranking_inputs,
            symbol_of=lambda item: item.symbol,
            build=lambda item: run_symbol_computation(
                lambda: self._build_symbol_result(
                    input_row=item,
                    market_date=normalized_market_date,
                    source=source,
                )
            ),
        )
        universe_counts = self._assign_metric_rankings(ranking_items)
        self._assign_scores_and_rankings(ranking_items)

        if normalized_symbols is None:
            items = _sort_items(ranking_items)
        else:
            ranked_items_by_symbol = {item['symbol']: item for item in ranking_items}
            ranking_failed_symbol_set = set(ranking_failed_symbols)
            items = []
            for input_row in inputs:
                if input_row.symbol in ranking_failed_symbol_set:
                    continue
                item = ranked_items_by_symbol.get(input_row.symbol)
                if item is None:
                    fallback_items, fallback_failed, fallback_errors = (
                        build_symbol_results(
                            [input_row],
                            symbol_of=lambda value: value.symbol,
                            build=lambda value: run_symbol_computation(
                                lambda: self._build_symbol_result(
                                    input_row=value,
                                    market_date=normalized_market_date,
                                    source=source,
                                )
                            ),
                        )
                    )
                    ranking_failed_symbols.extend(fallback_failed)
                    ranking_failed_symbol_errors.extend(fallback_errors)
                    if not fallback_items:
                        continue
                    item = fallback_items[0]
                    self._assign_scores_and_rankings([item])
                items.append(item)

        (
            computation_failed_symbols,
            computation_failed_symbol_errors,
            ranking_dependency_failed_symbols,
            ranking_dependency_failed_symbol_errors,
        ) = partition_symbol_failures(
            failed_symbols=ranking_failed_symbols,
            failed_symbol_errors=ranking_failed_symbol_errors,
            target_symbols=[input_row.symbol for input_row in inputs],
        )
        ranking_dependency_missing_symbols = [
            symbol
            for symbol in ranking_missing_symbols
            if normalized_symbols is None or symbol not in normalized_symbols
        ]
        (
            ranking_dependency_failed_symbols,
            ranking_dependency_failed_symbol_errors,
        ) = merge_symbol_failures(
            (
                ranking_dependency_failed_symbols,
                ranking_dependency_failed_symbol_errors,
            ),
            (
                ranking_dependency_missing_symbols,
                [
                    {'symbol': symbol, 'error': 'ranking_input_missing'}
                    for symbol in ranking_dependency_missing_symbols
                ],
            ),
        )

        items, payload_failed_symbols, payload_failed_errors = (
            attach_symbol_payloads(
                items,
                payload_builder=lambda item: self._build_factor_payload(
                    item=item,
                    source=source,
                    universe_counts=universe_counts,
                ),
            )
        )
        computation_failed_symbols, computation_failed_symbol_errors = (
            merge_symbol_failures(
                (computation_failed_symbols, computation_failed_symbol_errors),
                (payload_failed_symbols, payload_failed_errors),
            )
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
        failed_symbols = list(computation_failed_symbols)
        failed_symbol_errors = list(computation_failed_symbol_errors)
        scope_finalization: dict[str, Any] | None = None
        if exact_scope_symbols is not None:
            publication = replace_daily_derived_results_exact_scope(
                market_date=normalized_market_date,
                stage_id='value_factors',
                results=items,
                exact_symbols=exact_scope_symbols,
                authoritative_market_scope=authoritative_market_scope,
            )
            persisted_count = int(publication['results_persisted'])
            failed_symbols, failed_symbol_errors = merge_symbol_failures(
                (failed_symbols, failed_symbol_errors),
                (
                    publication['failed_symbols'],
                    publication['failed_symbol_errors'],
                ),
            )
            scope_finalization = publication['scope_finalization']
        else:
            (
                written_items,
                write_failed_symbols,
                write_failed_symbol_errors,
            ) = persist_symbol_items_with_isolation(
                items,
                bulk_write=upsert_daily_value_factor_results,
                item_write=upsert_daily_value_factor_result,
            )
            persisted_count = len(written_items)
            failed_symbols, failed_symbol_errors = merge_symbol_failures(
                (failed_symbols, failed_symbol_errors),
                (write_failed_symbols, write_failed_symbol_errors),
            )

        failed_symbol_set = set(failed_symbols)
        persisted_items = [
            item for item in items if item['symbol'] not in failed_symbol_set
        ]
        persisted_ready_count = sum(
            1
            for item in persisted_items
            if item.get('value_status') == VALUE_STATUS_SCORED
        )
        summary['persisted_ready_count'] = persisted_ready_count
        summary['persisted_unready_count'] = len(persisted_items) - persisted_ready_count
        summary.update(
            self._build_coverage_contract(
                items=items,
                persisted_items=persisted_items,
                missing_symbols=missing_symbols,
                failed_symbols=failed_symbols,
                failed_symbol_errors=failed_symbol_errors,
            )
        )
        attach_ranking_dependency_contract(
            summary,
            failed_symbols=ranking_dependency_failed_symbols,
            failed_symbol_errors=ranking_dependency_failed_symbol_errors,
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
        items = load_daily_value_factor_results(
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
            'scored_count': sum(1 for item in items if item.get('value_status') == VALUE_STATUS_SCORED),
            'invalid_per_count': sum(1 for item in items if item.get('value_status') == VALUE_STATUS_INVALID_PER),
            'missing_metric_count': sum(1 for item in items if item.get('value_status') == VALUE_STATUS_MISSING_METRIC),
        }
        payload: dict[str, Any] = {
            'market_date': normalized_market_date.isoformat(),
            'feature_version': VALUE_FACTOR_VERSION,
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
    ) -> tuple[list[ValueFactorInput], list[str], list[str]]:
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
            history_days=1,
        )
        valuation_snapshot_by_symbol = {
            str(item.get('symbol') or item.get('scope_key') or '').strip().upper(): dict(
                item.get('payload') or {}
            )
            for item in load_daily_pykrx_enrichment_rows(
                market_date=market_date,
                categories=['valuation_snapshot'],
                scope_types=['symbol'],
                symbols=matched_symbols,
                source='pykrx',
                limit=None,
            )
            if str(item.get('symbol') or item.get('scope_key') or '').strip()
        }
        inputs = [
            ValueFactorInput(
                symbol=row['symbol'],
                name_ko=row.get('name_ko'),
                market=row.get('market'),
                bars=bars_by_symbol.get(row['symbol'], []),
                valuation_snapshot=valuation_snapshot_by_symbol.get(row['symbol']),
            )
            for row in symbol_rows
        ]
        return inputs, matched_symbols, missing_symbols

    def _build_symbol_result(
        self,
        *,
        input_row: ValueFactorInput,
        market_date: date,
        source: str,
    ) -> dict[str, Any]:
        market_date_iso = market_date.isoformat()
        current_bar = input_row.bars[0] if input_row.bars and input_row.bars[0].get('trade_date') == market_date_iso else None
        valuation_snapshot = input_row.valuation_snapshot or {}

        def valuation_metric(key: str) -> tuple[float | None, str | None]:
            raw_value = valuation_snapshot.get(key)
            if raw_value is not None:
                return _round(float(raw_value), 6), 'pykrx_valuation_snapshot'
            if current_bar is not None and current_bar.get(key) is not None:
                return _round(float(current_bar[key]), 6), 'daily_price_bars'
            return None, None

        per, per_source = valuation_metric('per')
        pbr, pbr_source = valuation_metric('pbr')
        dividend_yield, dividend_yield_source = valuation_metric('dividend_yield')
        valuation_metric_sources = {
            'per': per_source,
            'pbr': pbr_source,
            'dividend_yield': dividend_yield_source,
        }
        resolved_valuation_sources = {
            metric_source
            for metric_source in valuation_metric_sources.values()
            if metric_source is not None
        }
        valuation_source = (
            'mixed'
            if len(resolved_valuation_sources) > 1
            else next(iter(resolved_valuation_sources), None)
        )
        per_status = _positive_only_metric_status(per) if current_bar is not None else METRIC_STATUS_MISSING
        pbr_status = _positive_only_metric_status(pbr) if current_bar is not None else METRIC_STATUS_MISSING
        dividend_yield_status = _non_negative_metric_status(dividend_yield) if current_bar is not None else METRIC_STATUS_MISSING

        return {
            'symbol': input_row.symbol,
            'name_ko': input_row.name_ko,
            'market': input_row.market,
            'market_date': market_date_iso,
            'source': source,
            'feature_version': source,
            'per': per,
            'pbr': pbr,
            'dividend_yield': dividend_yield,
            'per_status': per_status,
            'per_rank': None,
            'pbr_rank': None,
            'dividend_yield_rank': None,
            'per_percentile': None,
            'pbr_percentile': None,
            'dividend_yield_percentile': None,
            'value_score': None,
            'value_rank': None,
            'value_percentile': None,
            'value_status': VALUE_STATUS_MISSING_METRIC,
            'factor_payload': {},
            '_current_bar_available': current_bar is not None,
            '_bar_source': current_bar.get('source') if current_bar is not None else None,
            '_bar_captured_at': current_bar.get('captured_at') if current_bar is not None else None,
            '_valuation_source': valuation_source,
            '_valuation_metric_sources': valuation_metric_sources,
            '_pbr_status': pbr_status,
            '_dividend_yield_status': dividend_yield_status,
        }

    def _assign_metric_rankings(self, items: list[dict[str, Any]]) -> dict[str, int]:
        return {
            'per': self._assign_single_metric_rankings(
                items,
                value_key='per',
                status_key='per_status',
                rank_key='per_rank',
                percentile_key='per_percentile',
                best='lower',
            ),
            'pbr': self._assign_single_metric_rankings(
                items,
                value_key='pbr',
                status_key='_pbr_status',
                rank_key='pbr_rank',
                percentile_key='pbr_percentile',
                best='lower',
            ),
            'dividend_yield': self._assign_single_metric_rankings(
                items,
                value_key='dividend_yield',
                status_key='_dividend_yield_status',
                rank_key='dividend_yield_rank',
                percentile_key='dividend_yield_percentile',
                best='higher',
            ),
            'composite_score': sum(
                1
                for item in items
                if all(item.get(key) is not None for key in VALUE_SCORE_COMPONENT_KEYS)
            ),
        }

    def _assign_single_metric_rankings(
        self,
        items: list[dict[str, Any]],
        *,
        value_key: str,
        status_key: str,
        rank_key: str,
        percentile_key: str,
        best: str,
    ) -> int:
        candidates = [
            item
            for item in items
            if item.get('_current_bar_available')
            and item.get(status_key) == METRIC_STATUS_AVAILABLE
            and item.get(value_key) is not None
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

    def _assign_scores_and_rankings(self, items: list[dict[str, Any]]) -> None:
        for item in items:
            if not item.get('_current_bar_available'):
                item['value_status'] = VALUE_STATUS_MISSING_MARKET_DATE_BAR
                continue
            if item.get('per_status') == METRIC_STATUS_INVALID_NON_POSITIVE:
                item['value_status'] = VALUE_STATUS_INVALID_PER
                continue
            if item.get('per_status') != METRIC_STATUS_AVAILABLE:
                item['value_status'] = VALUE_STATUS_MISSING_METRIC
                continue
            if item.get('_pbr_status') != METRIC_STATUS_AVAILABLE:
                item['value_status'] = VALUE_STATUS_MISSING_METRIC
                continue
            if item.get('_dividend_yield_status') != METRIC_STATUS_AVAILABLE:
                item['value_status'] = VALUE_STATUS_MISSING_METRIC
                continue
            if any(item.get(key) is None for key in VALUE_SCORE_COMPONENT_KEYS):
                item['value_status'] = VALUE_STATUS_MISSING_METRIC
                continue

            item['value_score'] = _round(
                sum(float(item[key]) for key in VALUE_SCORE_COMPONENT_KEYS) / float(len(VALUE_SCORE_COMPONENT_KEYS)),
                6,
            )
            item['value_status'] = VALUE_STATUS_SCORED

        scored_items = sorted(
            [item for item in items if item.get('value_status') == VALUE_STATUS_SCORED],
            key=lambda item: (-float(item['value_score']), item['symbol']),
        )
        total = len(scored_items)
        for rank, item in enumerate(scored_items, start=1):
            item['value_rank'] = rank
            item['value_percentile'] = _percentile_from_rank(rank, total)

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
                'input_tables': [
                    'daily_price_bars',
                    'daily_pykrx_enrichment_rows',
                ],
                'score_method': 'equal_weight_mean_of_component_percentiles',
                'ranking_directions': {
                    'per': 'lower_better_positive_only',
                    'pbr': 'lower_better_positive_only',
                    'dividend_yield': 'higher_better_non_negative',
                    'value_score': 'higher_better',
                },
                'tie_breaker': 'symbol_asc',
            },
            'metric_statuses': {
                'per': item['per_status'],
                'pbr': item['_pbr_status'],
                'dividend_yield': item['_dividend_yield_status'],
            },
            'ranking': {
                'per_rank': item['per_rank'],
                'pbr_rank': item['pbr_rank'],
                'dividend_yield_rank': item['dividend_yield_rank'],
                'value_rank': item['value_rank'],
                'per_percentile': item['per_percentile'],
                'pbr_percentile': item['pbr_percentile'],
                'dividend_yield_percentile': item['dividend_yield_percentile'],
                'value_percentile': item['value_percentile'],
                'ranking_universe_counts': universe_counts,
            },
            'audit': {
                'trade_date': item['market_date'],
                'name_ko': item.get('name_ko'),
                'market': item.get('market'),
                'bar_source': item.get('_bar_source'),
                'bar_captured_at': item.get('_bar_captured_at'),
                'valuation_source': item.get('_valuation_source'),
                'valuation_metric_sources': item.get('_valuation_metric_sources'),
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
            1 for item in items if item.get('value_status') == VALUE_STATUS_SCORED
        )
        summary = {
            'requested_symbol_count': len(requested_symbols) if requested_symbols else len(matched_symbols),
            'matched_symbol_count': len(matched_symbols),
            'missing_symbol_count': len(missing_symbols),
            'symbols_with_market_date_bar': sum(1 for item in items if item.get('_current_bar_available')),
            'missing_market_date_bar_count': sum(
                1 for item in items if item.get('value_status') == VALUE_STATUS_MISSING_MARKET_DATE_BAR
            ),
            'invalid_per_count': sum(1 for item in items if item.get('value_status') == VALUE_STATUS_INVALID_PER),
            'missing_metric_count': sum(1 for item in items if item.get('value_status') == VALUE_STATUS_MISSING_METRIC),
            'scored_count': ready_count,
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
                failed_symbol_errors=[],
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
        failed_symbol_errors: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        def metric_status(item: dict[str, Any], metric: str) -> str | None:
            if metric == 'per' and item.get('per_status') is not None:
                return str(item['per_status'])
            payload = item.get('factor_payload')
            statuses = payload.get('metric_statuses') if isinstance(payload, dict) else None
            value = statuses.get(metric) if isinstance(statuses, dict) else None
            return str(value) if value is not None else None

        return build_factor_coverage_contract(
            items=items,
            persisted_items=persisted_items,
            missing_symbols=missing_symbols,
            failed_symbols=failed_symbols,
            failed_symbol_errors=failed_symbol_errors or [],
            is_ready=lambda item: item.get('value_score') is not None,
            is_complete=lambda item: item.get('value_status') == VALUE_STATUS_SCORED,
            unusable_reason=lambda item: item.get('value_status'),
            input_families={
                metric: (
                    lambda item, metric=metric: metric_status(item, metric)
                    == METRIC_STATUS_AVAILABLE,
                    lambda item, metric=metric: metric_status(item, metric),
                )
                for metric in ('per', 'pbr', 'dividend_yield')
            },
        )

    @staticmethod
    def _strip_internal_fields(item: dict[str, Any]) -> None:
        for key in (
            '_current_bar_available',
            '_bar_source',
            '_bar_captured_at',
            '_valuation_source',
            '_valuation_metric_sources',
            '_pbr_status',
            '_dividend_yield_status',
        ):
            item.pop(key, None)


daily_value_factor_service = DailyValueFactorService()
