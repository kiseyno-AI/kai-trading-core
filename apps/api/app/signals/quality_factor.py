from __future__ import annotations

from datetime import date
from typing import Any, Sequence

from app.market_data_store import (
    load_daily_quality_factor_results,
    upsert_daily_quality_factor_result,
    upsert_daily_quality_factor_results,
)
from app.signals.advanced_value_factor import (
    ADVANCED_VALUE_FUNDAMENTALS_FS_DIV,
    ADVANCED_VALUE_FUNDAMENTALS_PERIOD_TYPE,
    ADVANCED_VALUE_FUNDAMENTALS_REPORT_CODE,
    AdvancedValueFactorInput,
    DailyAdvancedValueFactorService,
    derive_annual_financial_context,
)
from app.signals.piotroski_filter import INDEX_ASSET_TURNOVER, INDEX_DEBT_RATIO, INDEX_GROSS_MARGIN

QUALITY_FACTOR_VERSION = 'quality_v1'
QUALITY_FACTOR_SOURCE = QUALITY_FACTOR_VERSION
QUALITY_COMPONENT_COUNT = 6

QUALITY_STATUS_SCORED_COMPLETE = 'scored_complete'
QUALITY_STATUS_SCORED_PARTIAL = 'scored_partial'
QUALITY_STATUS_MISSING_METRIC = 'missing_metric'
QUALITY_STATUS_INVALID_EQUITY = 'invalid_equity'
QUALITY_STATUS_INVALID_ASSETS = 'invalid_assets'
QUALITY_STATUS_INVALID_NET_INCOME = 'invalid_net_income'
QUALITY_STATUS_INSUFFICIENT_COMPONENTS = 'insufficient_components'
QUALITY_STATUS_INSUFFICIENT_FUNDAMENTALS = 'insufficient_fundamentals'
QUALITY_STATUS_MISSING_MARKET_DATE_BAR = 'missing_market_date_bar'

COMPONENT_STATUS_AVAILABLE = 'available'
COMPONENT_STATUS_MISSING_MARKET_DATE_BAR = 'missing_market_date_bar'
COMPONENT_STATUS_MISSING_FUNDAMENTALS_SNAPSHOT = 'missing_fundamentals_snapshot'
COMPONENT_STATUS_MISSING_NET_INCOME = 'missing_net_income'
COMPONENT_STATUS_MISSING_TOTAL_EQUITY = 'missing_total_equity'
COMPONENT_STATUS_INVALID_EQUITY = 'invalid_equity'
COMPONENT_STATUS_MISSING_TOTAL_ASSETS = 'missing_total_assets'
COMPONENT_STATUS_INVALID_ASSETS = 'invalid_assets'
COMPONENT_STATUS_MISSING_GROSS_MARGIN = 'missing_gross_margin'
COMPONENT_STATUS_MISSING_ASSET_TURNOVER = 'missing_asset_turnover'
COMPONENT_STATUS_MISSING_OPERATING_CASH_FLOW = 'missing_operating_cash_flow'
COMPONENT_STATUS_INVALID_NET_INCOME = 'invalid_net_income'
COMPONENT_STATUS_MISSING_DEBT_RATIO = 'missing_debt_ratio'

QUALITY_COMPONENT_PERCENTILES = (
    'roe_percentile',
    'roa_percentile',
    'gross_profit_to_assets_percentile',
    'operating_cash_flow_to_net_income_percentile',
    'debt_ratio_percentile',
    'accruals_ratio_percentile',
)
QUALITY_COMPONENT_STATUS_KEYS = (
    'roe_status',
    'roa_status',
    'gross_profit_to_assets_status',
    'operating_cash_flow_to_net_income_status',
    'debt_ratio_status',
    'accruals_ratio_status',
)
STATUS_SORT_ORDER = {
    QUALITY_STATUS_SCORED_COMPLETE: 0,
    QUALITY_STATUS_SCORED_PARTIAL: 1,
    QUALITY_STATUS_INVALID_ASSETS: 2,
    QUALITY_STATUS_INVALID_EQUITY: 3,
    QUALITY_STATUS_INVALID_NET_INCOME: 4,
    QUALITY_STATUS_MISSING_METRIC: 5,
    QUALITY_STATUS_INSUFFICIENT_COMPONENTS: 6,
    QUALITY_STATUS_INSUFFICIENT_FUNDAMENTALS: 7,
    QUALITY_STATUS_MISSING_MARKET_DATE_BAR: 8,
}
EMPTY_RANKING_UNIVERSE_COUNTS = {
    'roe': 0,
    'roa': 0,
    'gross_profit_to_assets': 0,
    'operating_cash_flow_to_net_income': 0,
    'debt_ratio': 0,
    'accruals_ratio': 0,
    'quality_score': 0,
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
            STATUS_SORT_ORDER.get(item.get('quality_status'), 99),
            item.get('quality_rank') is None,
            item.get('quality_rank') if item.get('quality_rank') is not None else 999999,
            not bool(item.get('is_quality_score_complete')),
            -(float(item['quality_score'])) if item.get('quality_score') is not None else 999999.0,
            -int(item.get('components_used') or 0),
            item['symbol'],
        ),
    )


def _highlight_amount(
    highlight_row: dict[str, Any] | None,
    *,
    flow: bool,
) -> tuple[float | None, str | None]:
    if highlight_row is None:
        return None, None
    if flow and highlight_row.get('current_amount') is None and highlight_row.get('current_cumulative_amount') is not None:
        amount_field = 'current_cumulative_amount'
    else:
        amount_field = 'current_amount'
    amount = highlight_row.get(amount_field)
    if amount is None:
        return None, amount_field
    return _round(float(amount), 4), amount_field


def _highlight_ratio(highlight_row: dict[str, Any] | None) -> float | None:
    if highlight_row is None:
        return None
    ratio_value = highlight_row.get('ratio_value')
    if ratio_value is None:
        return None
    return _round(float(ratio_value), 6)


def _highlight_audit_payload(
    highlight_row: dict[str, Any] | None,
    *,
    amount_field: str | None = None,
    ratio_source: bool = False,
) -> dict[str, Any] | None:
    if highlight_row is None:
        return None
    payload = {
        'metric_key': highlight_row.get('metric_key'),
        'statement_type': highlight_row.get('statement_type'),
        'statement_name': highlight_row.get('statement_name'),
        'account_id': highlight_row.get('account_id'),
        'account_name': highlight_row.get('account_name'),
        'currency': highlight_row.get('currency'),
    }
    if ratio_source:
        payload['value_source'] = 'ratio_value'
        payload['ratio_value'] = _round(float(highlight_row['ratio_value']), 6) if highlight_row.get('ratio_value') is not None else None
    else:
        payload['value_source'] = amount_field
        payload['amount_field'] = amount_field
        amount_value = highlight_row.get(amount_field) if amount_field else None
        payload['amount_value'] = _round(float(amount_value), 4) if amount_value is not None else None
    return payload


class DailyQualityFactorService(DailyAdvancedValueFactorService):
    def rebuild_for_market_date(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
        ranking_symbols: Sequence[str] | None = None,
        source: str = QUALITY_FACTOR_SOURCE,
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
            return self._empty_rebuild_result(
                market_date=normalized_market_date,
                source=source,
                requested_symbols=requested_symbols,
                matched_symbols=[],
                missing_symbols=[],
            )

        inputs, matched_symbols, missing_symbols = self._load_inputs(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
        )
        if normalized_symbols is not None and not matched_symbols:
            return self._empty_rebuild_result(
                market_date=normalized_market_date,
                source=source,
                requested_symbols=requested_symbols,
                matched_symbols=[],
                missing_symbols=missing_symbols,
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
        universe_counts = self._assign_metric_rankings(ranking_items)
        self._assign_scores_and_rankings(ranking_items)
        universe_counts['quality_score'] = sum(1 for item in ranking_items if item.get('quality_score') is not None)

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
            upsert_daily_quality_factor_results(items)
            persisted_count = len(items)
        except Exception:
            for item in items:
                try:
                    upsert_daily_quality_factor_result(item)
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
            if item.get('quality_score') is not None
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
        items = load_daily_quality_factor_results(
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
            'scored_complete_count': sum(
                1 for item in items if item.get('quality_status') == QUALITY_STATUS_SCORED_COMPLETE
            ),
            'scored_partial_count': sum(
                1 for item in items if item.get('quality_status') == QUALITY_STATUS_SCORED_PARTIAL
            ),
            'invalid_assets_count': sum(
                1 for item in items if item.get('quality_status') == QUALITY_STATUS_INVALID_ASSETS
            ),
            'invalid_equity_count': sum(
                1 for item in items if item.get('quality_status') == QUALITY_STATUS_INVALID_EQUITY
            ),
            'invalid_net_income_count': sum(
                1 for item in items if item.get('quality_status') == QUALITY_STATUS_INVALID_NET_INCOME
            ),
            'missing_metric_count': sum(
                1 for item in items if item.get('quality_status') == QUALITY_STATUS_MISSING_METRIC
            ),
            'insufficient_components_count': sum(
                1 for item in items if item.get('quality_status') == QUALITY_STATUS_INSUFFICIENT_COMPONENTS
            ),
            'insufficient_fundamentals_count': sum(
                1 for item in items if item.get('quality_status') == QUALITY_STATUS_INSUFFICIENT_FUNDAMENTALS
            ),
            'missing_market_date_bar_count': sum(
                1 for item in items if item.get('quality_status') == QUALITY_STATUS_MISSING_MARKET_DATE_BAR
            ),
        }
        payload: dict[str, Any] = {
            'market_date': normalized_market_date.isoformat(),
            'feature_version': QUALITY_FACTOR_VERSION,
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

    def _empty_rebuild_result(
        self,
        *,
        market_date: date,
        source: str,
        requested_symbols: list[str],
        matched_symbols: list[str],
        missing_symbols: list[str],
    ) -> dict[str, Any]:
        return {
            'market_date': market_date.isoformat(),
            'source': source,
            'feature_version': source,
            'requested_symbols': requested_symbols,
            'matched_symbols': matched_symbols,
            'missing_symbols': missing_symbols,
            'summary': self._build_summary(
                [],
                requested_symbols=requested_symbols,
                matched_symbols=matched_symbols,
                missing_symbols=missing_symbols,
                universe_counts=EMPTY_RANKING_UNIVERSE_COUNTS,
            ),
            'results_persisted': 0,
            'results_failed': 0,
            'failed_symbols': [],
            'failed_symbol_errors': [],
            'items': [],
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
        highlights = annual_fundamentals.highlights_by_key if annual_fundamentals is not None else {}
        blocked_status = None
        if current_bar is None:
            blocked_status = COMPONENT_STATUS_MISSING_MARKET_DATE_BAR
        elif annual_fundamentals is None:
            blocked_status = COMPONENT_STATUS_MISSING_FUNDAMENTALS_SNAPSHOT

        net_income_row = highlights.get('net_income')
        total_assets_row = highlights.get('total_assets')
        gross_profit_row = highlights.get('gross_profit')
        gross_margin_row = highlights.get('gross_margin') or highlights.get(INDEX_GROSS_MARGIN)
        asset_turnover_row = highlights.get('asset_turnover') or highlights.get(INDEX_ASSET_TURNOVER)
        debt_ratio_row = highlights.get('debt_ratio') or highlights.get(INDEX_DEBT_RATIO)

        net_income, net_income_amount_field = _highlight_amount(net_income_row, flow=True)
        total_assets, total_assets_amount_field = _highlight_amount(total_assets_row, flow=False)
        gross_profit, gross_profit_amount_field = _highlight_amount(gross_profit_row, flow=True)
        gross_margin = _highlight_ratio(gross_margin_row)
        asset_turnover = _highlight_ratio(asset_turnover_row)
        debt_ratio = _highlight_ratio(debt_ratio_row)

        total_equity = financial_context.get('total_equity') if financial_context else None
        total_liabilities = financial_context.get('total_liabilities') if financial_context else None
        operating_cash_flow = financial_context.get('operating_cash_flow') if financial_context else None

        if debt_ratio is None and total_liabilities is not None and total_equity is not None and total_equity > 0:
            debt_ratio = _round(float(total_liabilities) / float(total_equity), 6)
            debt_ratio_source = {
                'source_type': 'derived_formula',
                'formula': 'total_liabilities / total_equity',
                'total_liabilities': total_liabilities,
                'total_equity': total_equity,
            }
        else:
            debt_ratio_source = _highlight_audit_payload(debt_ratio_row, ratio_source=True)

        roe, roe_status = self._compute_roe(
            blocked_status=blocked_status,
            net_income=net_income,
            total_equity=total_equity,
        )
        roa, roa_status = self._compute_roa(
            blocked_status=blocked_status,
            net_income=net_income,
            total_assets=total_assets,
        )
        gross_profit_to_assets, gross_profit_to_assets_status = self._compute_gross_profit_to_assets(
            blocked_status=blocked_status,
            gross_profit=gross_profit,
            total_assets=total_assets,
            gross_margin=gross_margin,
            asset_turnover=asset_turnover,
        )
        if gross_profit is not None and total_assets not in (None, 0):
            gross_profit_to_assets_source = {
                'source_type': 'derived_formula',
                'formula': 'gross_profit / total_assets',
                'gross_profit': _highlight_audit_payload(
                    gross_profit_row,
                    amount_field=gross_profit_amount_field,
                ),
                'total_assets': _highlight_audit_payload(
                    total_assets_row,
                    amount_field=total_assets_amount_field,
                ),
            }
        else:
            gross_profit_to_assets_source = {
                'source_type': 'derived_formula',
                'formula': 'gross_margin * asset_turnover',
                'gross_margin': _highlight_audit_payload(gross_margin_row, ratio_source=True),
                'asset_turnover': _highlight_audit_payload(asset_turnover_row, ratio_source=True),
            }
        operating_cash_flow_to_net_income, operating_cash_flow_to_net_income_status = (
            self._compute_operating_cash_flow_to_net_income(
                blocked_status=blocked_status,
                net_income=net_income,
                operating_cash_flow=operating_cash_flow,
            )
        )
        debt_ratio_value, debt_ratio_status = self._compute_debt_ratio(
            blocked_status=blocked_status,
            debt_ratio=debt_ratio,
            total_equity=total_equity,
        )
        accruals_ratio, accruals_ratio_status = self._compute_accruals_ratio(
            blocked_status=blocked_status,
            net_income=net_income,
            operating_cash_flow=operating_cash_flow,
            total_assets=total_assets,
        )

        return {
            'symbol': input_row.symbol,
            'name_ko': input_row.name_ko,
            'market': input_row.market,
            'market_date': market_date_iso,
            'source': source,
            'feature_version': source,
            'roe': roe,
            'roa': roa,
            'gross_profit_to_assets': gross_profit_to_assets,
            'operating_cash_flow_to_net_income': operating_cash_flow_to_net_income,
            'debt_ratio': debt_ratio_value,
            'accruals_ratio': accruals_ratio,
            'roe_status': roe_status,
            'roa_status': roa_status,
            'gross_profit_to_assets_status': gross_profit_to_assets_status,
            'operating_cash_flow_to_net_income_status': operating_cash_flow_to_net_income_status,
            'debt_ratio_status': debt_ratio_status,
            'accruals_ratio_status': accruals_ratio_status,
            'roe_rank': None,
            'roa_rank': None,
            'gross_profit_to_assets_rank': None,
            'operating_cash_flow_to_net_income_rank': None,
            'debt_ratio_rank': None,
            'accruals_ratio_rank': None,
            'roe_percentile': None,
            'roa_percentile': None,
            'gross_profit_to_assets_percentile': None,
            'operating_cash_flow_to_net_income_percentile': None,
            'debt_ratio_percentile': None,
            'accruals_ratio_percentile': None,
            'quality_score': None,
            'quality_rank': None,
            'quality_percentile': None,
            'components_possible': QUALITY_COMPONENT_COUNT,
            'components_used': 0,
            'is_quality_score_complete': False,
            'quality_status': QUALITY_STATUS_INSUFFICIENT_COMPONENTS,
            'factor_payload': {},
            '_market_date_bar_available': current_bar is not None,
            '_annual_snapshot_available': annual_fundamentals is not None,
            '_price_bar_source': current_bar.get('source') if current_bar is not None else None,
            '_price_bar_captured_at': current_bar.get('captured_at') if current_bar is not None else None,
            '_fundamentals_audit_payload': financial_context.get('audit') if financial_context else None,
            '_net_income': net_income,
            '_total_assets': total_assets,
            '_gross_profit': gross_profit,
            '_total_equity': total_equity,
            '_total_liabilities': total_liabilities,
            '_operating_cash_flow': operating_cash_flow,
            '_gross_margin': gross_margin,
            '_asset_turnover': asset_turnover,
            '_net_income_source': _highlight_audit_payload(
                net_income_row,
                amount_field=net_income_amount_field,
            ),
            '_total_assets_source': _highlight_audit_payload(
                total_assets_row,
                amount_field=total_assets_amount_field,
            ),
            '_gross_profit_to_assets_source': gross_profit_to_assets_source,
            '_gross_margin_source': _highlight_audit_payload(gross_margin_row, ratio_source=True),
            '_asset_turnover_source': _highlight_audit_payload(asset_turnover_row, ratio_source=True),
            '_debt_ratio_source': debt_ratio_source,
        }

    def _assign_metric_rankings(self, items: list[dict[str, Any]]) -> dict[str, int]:
        return {
            'roe': self._assign_single_metric_rankings(
                items,
                value_key='roe',
                status_key='roe_status',
                rank_key='roe_rank',
                percentile_key='roe_percentile',
                best='higher',
            ),
            'roa': self._assign_single_metric_rankings(
                items,
                value_key='roa',
                status_key='roa_status',
                rank_key='roa_rank',
                percentile_key='roa_percentile',
                best='higher',
            ),
            'gross_profit_to_assets': self._assign_single_metric_rankings(
                items,
                value_key='gross_profit_to_assets',
                status_key='gross_profit_to_assets_status',
                rank_key='gross_profit_to_assets_rank',
                percentile_key='gross_profit_to_assets_percentile',
                best='higher',
            ),
            'operating_cash_flow_to_net_income': self._assign_single_metric_rankings(
                items,
                value_key='operating_cash_flow_to_net_income',
                status_key='operating_cash_flow_to_net_income_status',
                rank_key='operating_cash_flow_to_net_income_rank',
                percentile_key='operating_cash_flow_to_net_income_percentile',
                best='higher',
            ),
            'debt_ratio': self._assign_single_metric_rankings(
                items,
                value_key='debt_ratio',
                status_key='debt_ratio_status',
                rank_key='debt_ratio_rank',
                percentile_key='debt_ratio_percentile',
                best='lower',
            ),
            'accruals_ratio': self._assign_single_metric_rankings(
                items,
                value_key='accruals_ratio',
                status_key='accruals_ratio_status',
                rank_key='accruals_ratio_rank',
                percentile_key='accruals_ratio_percentile',
                best='lower',
            ),
            'quality_score': 0,
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
            if item.get(status_key) == COMPONENT_STATUS_AVAILABLE and item.get(value_key) is not None
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
            item['components_possible'] = QUALITY_COMPONENT_COUNT
            item['components_used'] = sum(1 for key in QUALITY_COMPONENT_PERCENTILES if item.get(key) is not None)
            item['is_quality_score_complete'] = item['components_used'] == QUALITY_COMPONENT_COUNT
            if item['components_used'] > 0:
                item['quality_score'] = _round(
                    sum(float(item[key]) for key in QUALITY_COMPONENT_PERCENTILES if item.get(key) is not None)
                    / float(item['components_used']),
                    6,
                )
                item['quality_status'] = (
                    QUALITY_STATUS_SCORED_COMPLETE
                    if item['is_quality_score_complete']
                    else QUALITY_STATUS_SCORED_PARTIAL
                )
                continue
            item['quality_status'] = self._resolve_quality_status(item)

        scored_items = sorted(
            [item for item in items if item.get('quality_score') is not None],
            key=lambda item: (
                -float(item['quality_score']),
                not bool(item.get('is_quality_score_complete')),
                -int(item.get('components_used') or 0),
                item['symbol'],
            ),
        )
        total = len(scored_items)
        for rank, item in enumerate(scored_items, start=1):
            item['quality_rank'] = rank
            item['quality_percentile'] = _percentile_from_rank(rank, total)

    @staticmethod
    def _resolve_quality_status(item: dict[str, Any]) -> str:
        if not item.get('_market_date_bar_available'):
            return QUALITY_STATUS_MISSING_MARKET_DATE_BAR
        if not item.get('_annual_snapshot_available'):
            return QUALITY_STATUS_INSUFFICIENT_FUNDAMENTALS
        if any(item.get(status_key) == COMPONENT_STATUS_INVALID_ASSETS for status_key in QUALITY_COMPONENT_STATUS_KEYS):
            return QUALITY_STATUS_INVALID_ASSETS
        if any(item.get(status_key) == COMPONENT_STATUS_INVALID_EQUITY for status_key in QUALITY_COMPONENT_STATUS_KEYS):
            return QUALITY_STATUS_INVALID_EQUITY
        if any(
            item.get(status_key) == COMPONENT_STATUS_INVALID_NET_INCOME for status_key in QUALITY_COMPONENT_STATUS_KEYS
        ):
            return QUALITY_STATUS_INVALID_NET_INCOME
        if any(item.get(status_key, '').startswith('missing_') for status_key in QUALITY_COMPONENT_STATUS_KEYS):
            return QUALITY_STATUS_MISSING_METRIC
        return QUALITY_STATUS_INSUFFICIENT_COMPONENTS

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
                'score_method': 'equal_weight_mean_of_available_component_percentiles',
                'score_requirement': 'at_least_one_component_required',
                'complete_requirement': 'all_six_components_required',
                'ranking_directions': {
                    'roe': 'higher_better',
                    'roa': 'higher_better',
                    'gross_profit_to_assets': 'higher_better',
                    'operating_cash_flow_to_net_income': 'higher_better_positive_net_income_required',
                    'debt_ratio': 'lower_better',
                    'accruals_ratio': 'lower_better',
                    'quality_score': 'higher_better',
                },
                'component_formulas': {
                    'roe': 'net_income / total_equity',
                    'roa': 'net_income / total_assets',
                    'gross_profit_to_assets': (
                        'gross_profit / total_assets; fallback gross_margin * asset_turnover'
                    ),
                    'operating_cash_flow_to_net_income': 'operating_cash_flow / net_income',
                    'debt_ratio': (
                        'stored_debt_ratio; fallback total_liabilities / total_equity; '
                        'positive_total_equity_required'
                    ),
                    'accruals_ratio': '(net_income - operating_cash_flow) / total_assets',
                },
                'tie_breaker': 'symbol_asc',
            },
            'component_statuses': {
                'roe': item['roe_status'],
                'roa': item['roa_status'],
                'gross_profit_to_assets': item['gross_profit_to_assets_status'],
                'operating_cash_flow_to_net_income': item['operating_cash_flow_to_net_income_status'],
                'debt_ratio': item['debt_ratio_status'],
                'accruals_ratio': item['accruals_ratio_status'],
            },
            'availability': {
                'components_possible': item['components_possible'],
                'components_used': item['components_used'],
                'is_quality_score_complete': item['is_quality_score_complete'],
                'market_date_bar_available': item.get('_market_date_bar_available'),
                'annual_fundamentals_available': item.get('_annual_snapshot_available'),
                'valid_component_keys': [
                    key.removesuffix('_percentile')
                    for key in QUALITY_COMPONENT_PERCENTILES
                    if item.get(key) is not None
                ],
                'missing_component_keys': [
                    key.removesuffix('_percentile')
                    for key in QUALITY_COMPONENT_PERCENTILES
                    if item.get(key) is None
                ],
            },
            'ranking': {
                'roe_rank': item['roe_rank'],
                'roa_rank': item['roa_rank'],
                'gross_profit_to_assets_rank': item['gross_profit_to_assets_rank'],
                'operating_cash_flow_to_net_income_rank': item['operating_cash_flow_to_net_income_rank'],
                'debt_ratio_rank': item['debt_ratio_rank'],
                'accruals_ratio_rank': item['accruals_ratio_rank'],
                'quality_rank': item['quality_rank'],
                'roe_percentile': item['roe_percentile'],
                'roa_percentile': item['roa_percentile'],
                'gross_profit_to_assets_percentile': item['gross_profit_to_assets_percentile'],
                'operating_cash_flow_to_net_income_percentile': item['operating_cash_flow_to_net_income_percentile'],
                'debt_ratio_percentile': item['debt_ratio_percentile'],
                'accruals_ratio_percentile': item['accruals_ratio_percentile'],
                'quality_percentile': item['quality_percentile'],
                'ranking_universe_counts': universe_counts,
            },
            'inputs': {
                'net_income': item.get('_net_income'),
                'total_assets': item.get('_total_assets'),
                'gross_profit': item.get('_gross_profit'),
                'total_equity': item.get('_total_equity'),
                'total_liabilities': item.get('_total_liabilities'),
                'operating_cash_flow': item.get('_operating_cash_flow'),
                'gross_margin': item.get('_gross_margin'),
                'asset_turnover': item.get('_asset_turnover'),
                'debt_ratio': item.get('debt_ratio'),
            },
            'audit': {
                'trade_date': item['market_date'],
                'name_ko': item.get('name_ko'),
                'market': item.get('market'),
                'price_bar': {
                    'source': item.get('_price_bar_source'),
                    'captured_at': item.get('_price_bar_captured_at'),
                },
                'fundamentals': item.get('_fundamentals_audit_payload'),
                'matched_highlights': {
                    'net_income': item.get('_net_income_source'),
                    'total_assets': item.get('_total_assets_source'),
                    'gross_profit_to_assets': item.get('_gross_profit_to_assets_source'),
                    'gross_margin': item.get('_gross_margin_source'),
                    'asset_turnover': item.get('_asset_turnover_source'),
                    'debt_ratio': item.get('_debt_ratio_source'),
                },
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
        ready_count = sum(1 for item in items if item.get('quality_score') is not None)
        return {
            'requested_symbol_count': len(requested_symbols) if requested_symbols else len(matched_symbols),
            'matched_symbol_count': len(matched_symbols),
            'missing_symbol_count': len(missing_symbols),
            'symbols_with_market_date_bar': sum(1 for item in items if item.get('_market_date_bar_available')),
            'symbols_with_annual_fundamentals': sum(1 for item in items if item.get('_annual_snapshot_available')),
            'roe_valid_count': sum(1 for item in items if item.get('roe_status') == COMPONENT_STATUS_AVAILABLE),
            'roa_valid_count': sum(1 for item in items if item.get('roa_status') == COMPONENT_STATUS_AVAILABLE),
            'gross_profit_to_assets_valid_count': sum(
                1 for item in items if item.get('gross_profit_to_assets_status') == COMPONENT_STATUS_AVAILABLE
            ),
            'operating_cash_flow_to_net_income_valid_count': sum(
                1
                for item in items
                if item.get('operating_cash_flow_to_net_income_status') == COMPONENT_STATUS_AVAILABLE
            ),
            'debt_ratio_valid_count': sum(
                1 for item in items if item.get('debt_ratio_status') == COMPONENT_STATUS_AVAILABLE
            ),
            'accruals_ratio_valid_count': sum(
                1 for item in items if item.get('accruals_ratio_status') == COMPONENT_STATUS_AVAILABLE
            ),
            'scored_complete_count': sum(
                1 for item in items if item.get('quality_status') == QUALITY_STATUS_SCORED_COMPLETE
            ),
            'scored_partial_count': sum(
                1 for item in items if item.get('quality_status') == QUALITY_STATUS_SCORED_PARTIAL
            ),
            'scored_count': ready_count,
            'ready_count': ready_count,
            'unready_count': len(missing_symbols) + len(items) - ready_count,
            'invalid_assets_count': sum(
                1 for item in items if item.get('quality_status') == QUALITY_STATUS_INVALID_ASSETS
            ),
            'invalid_equity_count': sum(
                1 for item in items if item.get('quality_status') == QUALITY_STATUS_INVALID_EQUITY
            ),
            'invalid_net_income_count': sum(
                1 for item in items if item.get('quality_status') == QUALITY_STATUS_INVALID_NET_INCOME
            ),
            'missing_metric_count': sum(
                1 for item in items if item.get('quality_status') == QUALITY_STATUS_MISSING_METRIC
            ),
            'insufficient_components_count': sum(
                1 for item in items if item.get('quality_status') == QUALITY_STATUS_INSUFFICIENT_COMPONENTS
            ),
            'insufficient_fundamentals_count': sum(
                1 for item in items if item.get('quality_status') == QUALITY_STATUS_INSUFFICIENT_FUNDAMENTALS
            ),
            'missing_market_date_bar_count': sum(
                1 for item in items if item.get('quality_status') == QUALITY_STATUS_MISSING_MARKET_DATE_BAR
            ),
            'ranking_universe_counts': universe_counts,
        }

    @staticmethod
    def _compute_roe(
        *,
        blocked_status: str | None,
        net_income: float | None,
        total_equity: float | None,
    ) -> tuple[float | None, str]:
        if blocked_status is not None:
            return None, blocked_status
        if net_income is None:
            return None, COMPONENT_STATUS_MISSING_NET_INCOME
        if total_equity is None:
            return None, COMPONENT_STATUS_MISSING_TOTAL_EQUITY
        if total_equity <= 0:
            return None, COMPONENT_STATUS_INVALID_EQUITY
        return _round(net_income / total_equity, 6), COMPONENT_STATUS_AVAILABLE

    @staticmethod
    def _compute_roa(
        *,
        blocked_status: str | None,
        net_income: float | None,
        total_assets: float | None,
    ) -> tuple[float | None, str]:
        if blocked_status is not None:
            return None, blocked_status
        if net_income is None:
            return None, COMPONENT_STATUS_MISSING_NET_INCOME
        if total_assets is None:
            return None, COMPONENT_STATUS_MISSING_TOTAL_ASSETS
        if total_assets <= 0:
            return None, COMPONENT_STATUS_INVALID_ASSETS
        return _round(net_income / total_assets, 6), COMPONENT_STATUS_AVAILABLE

    @staticmethod
    def _compute_gross_profit_to_assets(
        *,
        blocked_status: str | None,
        gross_profit: float | None,
        total_assets: float | None,
        gross_margin: float | None,
        asset_turnover: float | None,
    ) -> tuple[float | None, str]:
        if blocked_status is not None:
            return None, blocked_status
        if gross_profit is not None:
            if total_assets is None:
                return None, COMPONENT_STATUS_MISSING_TOTAL_ASSETS
            if total_assets <= 0:
                return None, COMPONENT_STATUS_INVALID_ASSETS
            return _round(gross_profit / total_assets, 6), COMPONENT_STATUS_AVAILABLE
        if gross_margin is None:
            return None, COMPONENT_STATUS_MISSING_GROSS_MARGIN
        if asset_turnover is None:
            return None, COMPONENT_STATUS_MISSING_ASSET_TURNOVER
        return _round(gross_margin * asset_turnover, 6), COMPONENT_STATUS_AVAILABLE

    @staticmethod
    def _compute_operating_cash_flow_to_net_income(
        *,
        blocked_status: str | None,
        net_income: float | None,
        operating_cash_flow: float | None,
    ) -> tuple[float | None, str]:
        if blocked_status is not None:
            return None, blocked_status
        if operating_cash_flow is None:
            return None, COMPONENT_STATUS_MISSING_OPERATING_CASH_FLOW
        if net_income is None:
            return None, COMPONENT_STATUS_MISSING_NET_INCOME
        if net_income <= 0:
            return None, COMPONENT_STATUS_INVALID_NET_INCOME
        return _round(operating_cash_flow / net_income, 6), COMPONENT_STATUS_AVAILABLE

    @staticmethod
    def _compute_debt_ratio(
        *,
        blocked_status: str | None,
        debt_ratio: float | None,
        total_equity: float | None,
    ) -> tuple[float | None, str]:
        if blocked_status is not None:
            return None, blocked_status
        if total_equity is None:
            return None, COMPONENT_STATUS_MISSING_TOTAL_EQUITY
        if total_equity <= 0:
            return None, COMPONENT_STATUS_INVALID_EQUITY
        if debt_ratio is None:
            return None, COMPONENT_STATUS_MISSING_DEBT_RATIO
        return debt_ratio, COMPONENT_STATUS_AVAILABLE

    @staticmethod
    def _compute_accruals_ratio(
        *,
        blocked_status: str | None,
        net_income: float | None,
        operating_cash_flow: float | None,
        total_assets: float | None,
    ) -> tuple[float | None, str]:
        if blocked_status is not None:
            return None, blocked_status
        if net_income is None:
            return None, COMPONENT_STATUS_MISSING_NET_INCOME
        if operating_cash_flow is None:
            return None, COMPONENT_STATUS_MISSING_OPERATING_CASH_FLOW
        if total_assets is None:
            return None, COMPONENT_STATUS_MISSING_TOTAL_ASSETS
        if total_assets <= 0:
            return None, COMPONENT_STATUS_INVALID_ASSETS
        return _round((net_income - operating_cash_flow) / total_assets, 6), COMPONENT_STATUS_AVAILABLE

    @staticmethod
    def _strip_internal_fields(item: dict[str, Any]) -> None:
        for key in (
            '_market_date_bar_available',
            '_annual_snapshot_available',
            '_price_bar_source',
            '_price_bar_captured_at',
            '_fundamentals_audit_payload',
            '_net_income',
            '_total_assets',
            '_gross_profit',
            '_total_equity',
            '_total_liabilities',
            '_operating_cash_flow',
            '_gross_margin',
            '_asset_turnover',
            '_net_income_source',
            '_total_assets_source',
            '_gross_profit_to_assets_source',
            '_gross_margin_source',
            '_asset_turnover_source',
            '_debt_ratio_source',
        ):
            item.pop(key, None)


daily_quality_factor_service = DailyQualityFactorService()
