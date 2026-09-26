from __future__ import annotations

from datetime import date
from typing import Any, Sequence

from app.market_data_store import (
    load_daily_unified_value_factor_results,
    load_symbols,
    upsert_daily_unified_value_factor_result,
    upsert_daily_unified_value_factor_results,
)
from app.signals.advanced_value_factor import (
    ADVANCED_VALUE_FUNDAMENTALS_FS_DIV,
    ADVANCED_VALUE_FUNDAMENTALS_PERIOD_TYPE,
    ADVANCED_VALUE_FUNDAMENTALS_REPORT_CODE,
    AdvancedValueFactorInput,
    DailyAdvancedValueFactorService,
    METRIC_STATUS_INVALID_ENTERPRISE_VALUE,
    METRIC_STATUS_INVALID_MARKET_CAP_PROXY,
    METRIC_STATUS_INVALID_NON_POSITIVE_EBITDA,
    METRIC_STATUS_MISSING_CAPITAL_EXPENDITURE,
    METRIC_STATUS_MISSING_CASH_AND_CASH_EQUIVALENTS,
    METRIC_STATUS_MISSING_DEPRECIATION_AND_AMORTIZATION,
    METRIC_STATUS_MISSING_FUNDAMENTALS_SNAPSHOT,
    METRIC_STATUS_MISSING_MARKET_DATE_BAR,
    METRIC_STATUS_MISSING_OPERATING_CASH_FLOW,
    METRIC_STATUS_MISSING_OPERATING_INCOME,
    METRIC_STATUS_MISSING_TOTAL_LIABILITIES,
    compute_ebitda,
    compute_enterprise_value,
    compute_ev_to_ebitda,
    compute_fcf_yield,
    compute_free_cash_flow,
    compute_market_cap_proxy,
    derive_annual_financial_context,
)

UNIFIED_VALUE_FACTOR_VERSION = 'unified_value_v1'
UNIFIED_VALUE_FACTOR_SOURCE = UNIFIED_VALUE_FACTOR_VERSION

UNIFIED_VALUE_STATUS_SCORED = 'scored'
UNIFIED_VALUE_STATUS_INVALID_PER = 'invalid_per'
UNIFIED_VALUE_STATUS_MISSING_METRIC = 'missing_metric'
UNIFIED_VALUE_STATUS_INSUFFICIENT_COMPONENTS = 'insufficient_components'
UNIFIED_VALUE_STATUS_MISSING_MARKET_DATE_BAR = 'missing_market_date_bar'
UNIFIED_VALUE_STATUS_INSUFFICIENT_FUNDAMENTALS = 'insufficient_fundamentals'

METRIC_STATUS_AVAILABLE = 'available'
METRIC_STATUS_MISSING = 'missing'
METRIC_STATUS_INVALID_NON_POSITIVE = 'invalid_non_positive'
METRIC_STATUS_INVALID_NEGATIVE = 'invalid_negative'

UNIFIED_VALUE_COMPONENT_PERCENTILES = (
    'per_percentile',
    'pbr_percentile',
    'dividend_yield_percentile',
    'fcf_yield_percentile',
    'ev_to_ebitda_percentile',
)
UNIFIED_VALUE_COMPONENT_STATUS_KEYS = (
    'per_status',
    'pbr_status',
    'dividend_yield_status',
    'fcf_yield_status',
    'ev_to_ebitda_status',
)
STATUS_SORT_ORDER = {
    UNIFIED_VALUE_STATUS_SCORED: 0,
    UNIFIED_VALUE_STATUS_INVALID_PER: 1,
    UNIFIED_VALUE_STATUS_MISSING_METRIC: 2,
    UNIFIED_VALUE_STATUS_INSUFFICIENT_COMPONENTS: 3,
    UNIFIED_VALUE_STATUS_INSUFFICIENT_FUNDAMENTALS: 4,
    UNIFIED_VALUE_STATUS_MISSING_MARKET_DATE_BAR: 5,
}
EMPTY_RANKING_UNIVERSE_COUNTS = {
    'per': 0,
    'pbr': 0,
    'dividend_yield': 0,
    'fcf_yield': 0,
    'ev_to_ebitda': 0,
    'unified_value_score': 0,
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
            STATUS_SORT_ORDER.get(item.get('unified_value_status'), 99),
            item.get('unified_value_rank') is None,
            item.get('unified_value_rank') if item.get('unified_value_rank') is not None else 999999,
            -(float(item['unified_value_score'])) if item.get('unified_value_score') is not None else 999999.0,
            -int(item.get('components_used') or 0),
            item['symbol'],
        ),
    )


class DailyUnifiedValueFactorService(DailyAdvancedValueFactorService):
    def rebuild_for_market_date(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
        ranking_symbols: Sequence[str] | None = None,
        source: str = UNIFIED_VALUE_FACTOR_SOURCE,
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
            return {
                'market_date': normalized_market_date.isoformat(),
                'source': source,
                'feature_version': source,
                'requested_symbols': requested_symbols,
                'matched_symbols': [],
                'missing_symbols': [],
                'summary': self._build_summary(
                    [],
                    requested_symbols=requested_symbols,
                    matched_symbols=[],
                    missing_symbols=[],
                    universe_counts=EMPTY_RANKING_UNIVERSE_COUNTS,
                ),
                'results_persisted': 0,
                'results_failed': 0,
                'failed_symbols': [],
                'failed_symbol_errors': [],
                'items': [],
            }

        if normalized_symbols is not None:
            resolved_symbols = [item['symbol'] for item in load_symbols(symbols=normalized_symbols)]
            if not resolved_symbols:
                return {
                    'market_date': normalized_market_date.isoformat(),
                    'source': source,
                    'feature_version': source,
                    'requested_symbols': requested_symbols,
                    'matched_symbols': [],
                    'missing_symbols': requested_symbols,
                    'summary': self._build_summary(
                        [],
                        requested_symbols=requested_symbols,
                        matched_symbols=[],
                        missing_symbols=requested_symbols,
                        universe_counts=EMPTY_RANKING_UNIVERSE_COUNTS,
                    ),
                    'results_persisted': 0,
                    'results_failed': 0,
                    'failed_symbols': [],
                    'failed_symbol_errors': [],
                    'items': [],
                }

        inputs, matched_symbols, missing_symbols = self._load_inputs(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
        )
        ranking_inputs = inputs
        if normalized_symbols is not None:
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
        universe_counts = self._assign_metric_rankings(ranking_items)
        self._assign_scores_and_rankings(ranking_items)
        universe_counts['unified_value_score'] = sum(
            1 for item in ranking_items if item.get('unified_value_status') == UNIFIED_VALUE_STATUS_SCORED
        )

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
                    )
                    self._assign_scores_and_rankings([item])
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
            upsert_daily_unified_value_factor_results(items)
            persisted_count = len(items)
        except Exception:
            for item in items:
                try:
                    upsert_daily_unified_value_factor_result(item)
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
            if item.get('unified_value_score') is not None
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
            'items': _sort_items(items),
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
        items = load_daily_unified_value_factor_results(
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
            'scored_count': sum(1 for item in items if item.get('unified_value_status') == UNIFIED_VALUE_STATUS_SCORED),
            'invalid_per_count': sum(
                1 for item in items if item.get('unified_value_status') == UNIFIED_VALUE_STATUS_INVALID_PER
            ),
            'missing_metric_count': sum(
                1 for item in items if item.get('unified_value_status') == UNIFIED_VALUE_STATUS_MISSING_METRIC
            ),
            'insufficient_components_count': sum(
                1
                for item in items
                if item.get('unified_value_status') == UNIFIED_VALUE_STATUS_INSUFFICIENT_COMPONENTS
            ),
            'insufficient_fundamentals_count': sum(
                1
                for item in items
                if item.get('unified_value_status') == UNIFIED_VALUE_STATUS_INSUFFICIENT_FUNDAMENTALS
            ),
            'missing_market_date_bar_count': sum(
                1
                for item in items
                if item.get('unified_value_status') == UNIFIED_VALUE_STATUS_MISSING_MARKET_DATE_BAR
            ),
        }
        payload: dict[str, Any] = {
            'market_date': normalized_market_date.isoformat(),
            'feature_version': UNIFIED_VALUE_FACTOR_VERSION,
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

    def _build_symbol_result(
        self,
        *,
        input_row: AdvancedValueFactorInput,
        market_date: date,
        source: str,
    ) -> dict[str, Any]:
        market_date_iso = market_date.isoformat()
        current_bar = input_row.bars[0] if input_row.bars and input_row.bars[0].get('trade_date') == market_date_iso else None
        annual_fundamentals = input_row.annual_fundamentals
        financial_context = derive_annual_financial_context(annual_fundamentals) if annual_fundamentals is not None else None

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

        total_equity = financial_context.get('total_equity') if financial_context else None
        total_liabilities = financial_context.get('total_liabilities') if financial_context else None
        operating_income = financial_context.get('operating_income') if financial_context else None
        operating_cash_flow = financial_context.get('operating_cash_flow') if financial_context else None
        cash_and_cash_equivalents = financial_context.get('cash_and_cash_equivalents') if financial_context else None
        capital_expenditure = financial_context.get('capital_expenditure') if financial_context else None
        depreciation_and_amortization = (
            financial_context.get('depreciation_and_amortization') if financial_context else None
        )

        observed_market_cap = (
            _round(float(input_row.market_cap), 4)
            if input_row.market_cap is not None and float(input_row.market_cap) > 0
            else None
        )
        if observed_market_cap is not None:
            market_cap_proxy = observed_market_cap
            market_cap_source = 'pykrx_valuation_snapshot'
        else:
            market_cap_proxy = compute_market_cap_proxy(pbr=pbr, total_equity=total_equity)
            market_cap_source = 'pbr_times_total_equity' if market_cap_proxy is not None else None
        free_cash_flow = compute_free_cash_flow(
            operating_cash_flow=operating_cash_flow,
            capital_expenditure=capital_expenditure,
        )
        fcf_yield = compute_fcf_yield(
            free_cash_flow=free_cash_flow,
            market_cap_proxy=market_cap_proxy,
        )
        ebitda = compute_ebitda(
            operating_income=operating_income,
            depreciation_and_amortization=depreciation_and_amortization,
        )
        enterprise_value = compute_enterprise_value(
            market_cap_proxy=market_cap_proxy,
            total_liabilities=total_liabilities,
            cash_and_cash_equivalents=cash_and_cash_equivalents,
        )
        ev_to_ebitda = compute_ev_to_ebitda(
            enterprise_value=enterprise_value,
            ebitda=ebitda,
        )

        if current_bar is None:
            fcf_yield_status = METRIC_STATUS_MISSING_MARKET_DATE_BAR
            ev_to_ebitda_status = METRIC_STATUS_MISSING_MARKET_DATE_BAR
        elif annual_fundamentals is None:
            fcf_yield_status = METRIC_STATUS_MISSING_FUNDAMENTALS_SNAPSHOT
            ev_to_ebitda_status = METRIC_STATUS_MISSING_FUNDAMENTALS_SNAPSHOT
        else:
            if operating_cash_flow is None:
                fcf_yield_status = METRIC_STATUS_MISSING_OPERATING_CASH_FLOW
            elif capital_expenditure is None:
                fcf_yield_status = METRIC_STATUS_MISSING_CAPITAL_EXPENDITURE
            elif market_cap_proxy is None or market_cap_proxy <= 0:
                fcf_yield_status = METRIC_STATUS_INVALID_MARKET_CAP_PROXY
            else:
                fcf_yield_status = METRIC_STATUS_AVAILABLE

            if total_liabilities is None:
                ev_to_ebitda_status = METRIC_STATUS_MISSING_TOTAL_LIABILITIES
            elif cash_and_cash_equivalents is None:
                ev_to_ebitda_status = METRIC_STATUS_MISSING_CASH_AND_CASH_EQUIVALENTS
            elif operating_income is None:
                ev_to_ebitda_status = METRIC_STATUS_MISSING_OPERATING_INCOME
            elif depreciation_and_amortization is None:
                ev_to_ebitda_status = METRIC_STATUS_MISSING_DEPRECIATION_AND_AMORTIZATION
            elif market_cap_proxy is None or market_cap_proxy <= 0:
                ev_to_ebitda_status = METRIC_STATUS_INVALID_MARKET_CAP_PROXY
            elif enterprise_value is None or enterprise_value <= 0:
                ev_to_ebitda_status = METRIC_STATUS_INVALID_ENTERPRISE_VALUE
            elif ebitda is None or ebitda <= 0:
                ev_to_ebitda_status = METRIC_STATUS_INVALID_NON_POSITIVE_EBITDA
            else:
                ev_to_ebitda_status = METRIC_STATUS_AVAILABLE

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
            'fcf_yield': _round(fcf_yield, 6),
            'ev_to_ebitda': _round(ev_to_ebitda, 6),
            'per_status': per_status,
            'pbr_status': pbr_status,
            'dividend_yield_status': dividend_yield_status,
            'fcf_yield_status': fcf_yield_status,
            'ev_to_ebitda_status': ev_to_ebitda_status,
            'per_rank': None,
            'pbr_rank': None,
            'dividend_yield_rank': None,
            'fcf_yield_rank': None,
            'ev_to_ebitda_rank': None,
            'per_percentile': None,
            'pbr_percentile': None,
            'dividend_yield_percentile': None,
            'fcf_yield_percentile': None,
            'ev_to_ebitda_percentile': None,
            'unified_value_score': None,
            'unified_value_rank': None,
            'unified_value_percentile': None,
            'components_used': 0,
            'unified_value_status': UNIFIED_VALUE_STATUS_INSUFFICIENT_COMPONENTS,
            'factor_payload': {},
            '_market_date_bar_available': current_bar is not None,
            '_annual_snapshot_available': annual_fundamentals is not None,
            '_price_bar_source': current_bar.get('source') if current_bar is not None else None,
            '_price_bar_captured_at': current_bar.get('captured_at') if current_bar is not None else None,
            '_valuation_source': valuation_source,
            '_valuation_metric_sources': valuation_metric_sources,
            '_fundamentals_audit_payload': financial_context.get('audit') if financial_context else None,
            '_observed_market_cap': observed_market_cap,
            '_market_cap_source': market_cap_source,
            '_market_cap_proxy': market_cap_proxy,
            '_enterprise_value': enterprise_value,
            '_ebitda': ebitda,
            '_free_cash_flow': free_cash_flow,
            '_total_equity': total_equity,
            '_total_liabilities': total_liabilities,
            '_operating_income': operating_income,
            '_operating_cash_flow': operating_cash_flow,
            '_cash_and_cash_equivalents': cash_and_cash_equivalents,
            '_capital_expenditure': capital_expenditure,
            '_depreciation_and_amortization': depreciation_and_amortization,
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
                status_key='pbr_status',
                rank_key='pbr_rank',
                percentile_key='pbr_percentile',
                best='lower',
            ),
            'dividend_yield': self._assign_single_metric_rankings(
                items,
                value_key='dividend_yield',
                status_key='dividend_yield_status',
                rank_key='dividend_yield_rank',
                percentile_key='dividend_yield_percentile',
                best='higher',
            ),
            'fcf_yield': self._assign_single_metric_rankings(
                items,
                value_key='fcf_yield',
                status_key='fcf_yield_status',
                rank_key='fcf_yield_rank',
                percentile_key='fcf_yield_percentile',
                best='higher',
            ),
            'ev_to_ebitda': self._assign_single_metric_rankings(
                items,
                value_key='ev_to_ebitda',
                status_key='ev_to_ebitda_status',
                rank_key='ev_to_ebitda_rank',
                percentile_key='ev_to_ebitda_percentile',
                best='lower',
            ),
            'unified_value_score': 0,
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
            if item.get(status_key) == METRIC_STATUS_AVAILABLE and item.get(value_key) is not None
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
            item['components_used'] = sum(
                1 for key in UNIFIED_VALUE_COMPONENT_PERCENTILES if item.get(key) is not None
            )
            if all(item.get(key) is not None for key in UNIFIED_VALUE_COMPONENT_PERCENTILES):
                item['unified_value_score'] = _round(
                    sum(float(item[key]) for key in UNIFIED_VALUE_COMPONENT_PERCENTILES)
                    / float(len(UNIFIED_VALUE_COMPONENT_PERCENTILES)),
                    6,
                )
                item['unified_value_status'] = UNIFIED_VALUE_STATUS_SCORED
                continue
            item['unified_value_status'] = self._resolve_unified_status(item)

        scored_items = sorted(
            [item for item in items if item.get('unified_value_status') == UNIFIED_VALUE_STATUS_SCORED],
            key=lambda item: (-float(item['unified_value_score']), item['symbol']),
        )
        total = len(scored_items)
        for rank, item in enumerate(scored_items, start=1):
            item['unified_value_rank'] = rank
            item['unified_value_percentile'] = _percentile_from_rank(rank, total)

    @staticmethod
    def _resolve_unified_status(item: dict[str, Any]) -> str:
        if not item.get('_market_date_bar_available'):
            return UNIFIED_VALUE_STATUS_MISSING_MARKET_DATE_BAR
        if not item.get('_annual_snapshot_available'):
            return UNIFIED_VALUE_STATUS_INSUFFICIENT_FUNDAMENTALS
        if item.get('per_status') == METRIC_STATUS_INVALID_NON_POSITIVE:
            return UNIFIED_VALUE_STATUS_INVALID_PER
        if any(
            item.get(status_key) != METRIC_STATUS_AVAILABLE
            for status_key in ('per_status', 'pbr_status', 'dividend_yield_status')
        ):
            return UNIFIED_VALUE_STATUS_MISSING_METRIC
        return UNIFIED_VALUE_STATUS_INSUFFICIENT_COMPONENTS

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
                'input_tables': {
                    'price_bars': 'daily_price_bars',
                    'valuation_snapshots': 'daily_pykrx_enrichment_rows',
                    'fundamentals_snapshots': 'dart_fundamentals_snapshots',
                    'fundamentals_rows': 'dart_fundamentals_rows',
                    'fundamentals_highlights': 'dart_fundamentals_highlights',
                },
                'financial_snapshot_policy': {
                    'report_code': ADVANCED_VALUE_FUNDAMENTALS_REPORT_CODE,
                    'period_type': ADVANCED_VALUE_FUNDAMENTALS_PERIOD_TYPE,
                    'fs_div': ADVANCED_VALUE_FUNDAMENTALS_FS_DIV,
                    'fs_div_priority': ['CFS', 'OFS'],
                    'selection_rule': (
                        'latest_business_year_then_latest_filing_then_cfs_'
                        'preferred_within_same_filing_on_or_before_market_date'
                    ),
                },
                'score_method': 'equal_weight_mean_of_five_component_percentiles',
                'score_requirement': 'all_five_components_required',
                'ranking_directions': {
                    'per': 'lower_better_positive_only',
                    'pbr': 'lower_better_positive_only',
                    'dividend_yield': 'higher_better_non_negative',
                    'fcf_yield': 'higher_better',
                    'ev_to_ebitda': 'lower_better',
                    'unified_value_score': 'higher_better',
                },
                'tie_breaker': 'symbol_asc',
                'advanced_value_formulas': {
                    'free_cash_flow': 'operating_cash_flow - capital_expenditure',
                    'fcf_yield': 'free_cash_flow / market_cap_proxy',
                    'market_cap_proxy': 'observed_market_cap, else pbr * total_equity',
                    'enterprise_value': 'market_cap_proxy + total_liabilities - cash_and_cash_equivalents',
                    'ebitda': 'operating_income + depreciation_and_amortization',
                    'ev_to_ebitda': 'enterprise_value / ebitda',
                },
            },
            'component_statuses': {
                'per': item['per_status'],
                'pbr': item['pbr_status'],
                'dividend_yield': item['dividend_yield_status'],
                'fcf_yield': item['fcf_yield_status'],
                'ev_to_ebitda': item['ev_to_ebitda_status'],
            },
            'availability': {
                'components_used': item['components_used'],
                'components_required': len(UNIFIED_VALUE_COMPONENT_PERCENTILES),
                'market_date_bar_available': item.get('_market_date_bar_available'),
                'annual_fundamentals_available': item.get('_annual_snapshot_available'),
                'valid_component_keys': [
                    key.removesuffix('_percentile')
                    for key in UNIFIED_VALUE_COMPONENT_PERCENTILES
                    if item.get(key) is not None
                ],
                'missing_component_keys': [
                    key.removesuffix('_percentile')
                    for key in UNIFIED_VALUE_COMPONENT_PERCENTILES
                    if item.get(key) is None
                ],
            },
            'ranking': {
                'per_rank': item['per_rank'],
                'pbr_rank': item['pbr_rank'],
                'dividend_yield_rank': item['dividend_yield_rank'],
                'fcf_yield_rank': item['fcf_yield_rank'],
                'ev_to_ebitda_rank': item['ev_to_ebitda_rank'],
                'unified_value_rank': item['unified_value_rank'],
                'per_percentile': item['per_percentile'],
                'pbr_percentile': item['pbr_percentile'],
                'dividend_yield_percentile': item['dividend_yield_percentile'],
                'fcf_yield_percentile': item['fcf_yield_percentile'],
                'ev_to_ebitda_percentile': item['ev_to_ebitda_percentile'],
                'unified_value_percentile': item['unified_value_percentile'],
                'ranking_universe_counts': universe_counts,
            },
            'inputs': {
                'market_cap_proxy': item.get('_market_cap_proxy'),
                'enterprise_value': item.get('_enterprise_value'),
                'ebitda': item.get('_ebitda'),
                'free_cash_flow': item.get('_free_cash_flow'),
                'total_equity': item.get('_total_equity'),
                'total_liabilities': item.get('_total_liabilities'),
                'operating_income': item.get('_operating_income'),
                'operating_cash_flow': item.get('_operating_cash_flow'),
                'cash_and_cash_equivalents': item.get('_cash_and_cash_equivalents'),
                'capital_expenditure': item.get('_capital_expenditure'),
                'depreciation_and_amortization': item.get('_depreciation_and_amortization'),
            },
            'audit': {
                'trade_date': item['market_date'],
                'name_ko': item.get('name_ko'),
                'market': item.get('market'),
                'price_bar': {
                    'source': item.get('_price_bar_source'),
                    'captured_at': item.get('_price_bar_captured_at'),
                },
                'valuation_source': item.get('_valuation_source'),
                'valuation_metric_sources': item.get('_valuation_metric_sources'),
                'market_cap': {
                    'source_type': item.get('_market_cap_source'),
                    'observed_market_cap': item.get('_observed_market_cap'),
                    'resolved_market_cap': item.get('_market_cap_proxy'),
                },
                'fundamentals': item.get('_fundamentals_audit_payload'),
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
            1 for item in items if item.get('unified_value_score') is not None
        )
        return {
            'requested_symbol_count': len(requested_symbols) if requested_symbols else len(matched_symbols),
            'matched_symbol_count': len(matched_symbols),
            'missing_symbol_count': len(missing_symbols),
            'symbols_with_market_date_bar': sum(1 for item in items if item.get('_market_date_bar_available')),
            'symbols_with_annual_fundamentals': sum(1 for item in items if item.get('_annual_snapshot_available')),
            'per_valid_count': sum(1 for item in items if item.get('per_status') == METRIC_STATUS_AVAILABLE),
            'pbr_valid_count': sum(1 for item in items if item.get('pbr_status') == METRIC_STATUS_AVAILABLE),
            'dividend_yield_valid_count': sum(
                1 for item in items if item.get('dividend_yield_status') == METRIC_STATUS_AVAILABLE
            ),
            'fcf_yield_valid_count': sum(1 for item in items if item.get('fcf_yield_status') == METRIC_STATUS_AVAILABLE),
            'ev_to_ebitda_valid_count': sum(
                1 for item in items if item.get('ev_to_ebitda_status') == METRIC_STATUS_AVAILABLE
            ),
            'scored_count': ready_count,
            'ready_count': ready_count,
            'unready_count': len(missing_symbols) + len(items) - ready_count,
            'invalid_per_count': sum(
                1 for item in items if item.get('unified_value_status') == UNIFIED_VALUE_STATUS_INVALID_PER
            ),
            'missing_metric_count': sum(
                1 for item in items if item.get('unified_value_status') == UNIFIED_VALUE_STATUS_MISSING_METRIC
            ),
            'insufficient_components_count': sum(
                1
                for item in items
                if item.get('unified_value_status') == UNIFIED_VALUE_STATUS_INSUFFICIENT_COMPONENTS
            ),
            'insufficient_fundamentals_count': sum(
                1
                for item in items
                if item.get('unified_value_status') == UNIFIED_VALUE_STATUS_INSUFFICIENT_FUNDAMENTALS
            ),
            'missing_market_date_bar_count': sum(
                1
                for item in items
                if item.get('unified_value_status') == UNIFIED_VALUE_STATUS_MISSING_MARKET_DATE_BAR
            ),
            'ranking_universe_counts': universe_counts,
        }

    @staticmethod
    def _strip_internal_fields(item: dict[str, Any]) -> None:
        for key in (
            '_market_date_bar_available',
            '_annual_snapshot_available',
            '_price_bar_source',
            '_price_bar_captured_at',
            '_valuation_source',
            '_valuation_metric_sources',
            '_fundamentals_audit_payload',
            '_observed_market_cap',
            '_market_cap_source',
            '_market_cap_proxy',
            '_enterprise_value',
            '_ebitda',
            '_free_cash_flow',
            '_total_equity',
            '_total_liabilities',
            '_operating_income',
            '_operating_cash_flow',
            '_cash_and_cash_equivalents',
            '_capital_expenditure',
            '_depreciation_and_amortization',
        ):
            item.pop(key, None)


daily_unified_value_factor_service = DailyUnifiedValueFactorService()
