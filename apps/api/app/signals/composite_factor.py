from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping, Sequence

from app.market_data_store import (
    list_symbols_with_daily_bar_on_date,
    load_daily_advanced_value_factor_results,
    load_daily_beta_factor_results,
    load_daily_composite_factor_results,
    load_daily_low_risk_factor_results,
    load_daily_momentum_factor_results,
    load_daily_value_factor_results,
    load_symbols,
    replace_daily_derived_results_exact_scope,
    upsert_daily_composite_factor_result,
    upsert_daily_composite_factor_results,
)
from app.repositories.eod_run_repository import daily_price_bar_writer_admission
from app.signals.momentum_factor import enrich_momentum_audit_fields
from app.signals.factor_coverage import (
    build_factor_coverage_contract,
    factor_stage_result_contract,
)

COMPOSITE_FACTOR_VERSION = 'composite_v1'
COMPOSITE_FACTOR_SOURCE = COMPOSITE_FACTOR_VERSION
COMPOSITE_WEIGHT_MODEL_VERSION = 'composite_weights_v1'

COMPOSITE_ELIGIBLE_SCORE = 0.70
COMPOSITE_WATCHLIST_SCORE = 0.50
COMPOSITE_MIN_VALID_INPUTS = 3
COMPOSITE_MIN_AVAILABLE_WEIGHT = 0.60
COMPOSITE_REQUIRED_VALUATION_FACTORS = frozenset({'value_v1', 'advanced_value'})

CLASSIFICATION_ELIGIBLE = 'eligible'
CLASSIFICATION_WATCHLIST = 'watchlist'
CLASSIFICATION_INELIGIBLE = 'ineligible'
CLASSIFICATION_HARD_EXCLUDED = 'hard_excluded'
CLASSIFICATION_INSUFFICIENT_INPUTS = 'insufficient_inputs'

HARD_EXCLUSION_MOMENTUM_OVERHEAT = 'momentum_recent_overheat'
HARD_EXCLUSION_LOW_RISK_LIQUIDITY = 'low_risk_liquidity_floor'
HARD_EXCLUSION_LOW_RISK_EVIDENCE_MISSING = 'low_risk_liquidity_evidence_missing'


@dataclass(frozen=True, slots=True)
class CompositeFactorDefinition:
    family: str
    input_name: str
    input_field: str
    contribution_field: str
    base_weight: float
    source_table: str


FACTOR_DEFINITIONS: tuple[CompositeFactorDefinition, ...] = (
    CompositeFactorDefinition(
        family='momentum',
        input_name='momentum_percentile',
        input_field='momentum_input',
        contribution_field='momentum_contribution',
        base_weight=0.25,
        source_table='daily_momentum_factor_results',
    ),
    CompositeFactorDefinition(
        family='value_v1',
        input_name='value_score_v1',
        input_field='value_input',
        contribution_field='value_contribution',
        base_weight=0.15,
        source_table='daily_value_factor_results',
    ),
    CompositeFactorDefinition(
        family='advanced_value',
        input_name='advanced_value_score',
        input_field='advanced_value_input',
        contribution_field='advanced_value_contribution',
        base_weight=0.20,
        source_table='daily_advanced_value_factor_results',
    ),
    CompositeFactorDefinition(
        family='low_risk',
        input_name='low_risk_score',
        input_field='low_risk_input',
        contribution_field='low_risk_contribution',
        base_weight=0.20,
        source_table='daily_low_risk_factor_results',
    ),
    CompositeFactorDefinition(
        family='beta',
        input_name='low_beta_score',
        input_field='low_beta_input',
        contribution_field='low_beta_contribution',
        base_weight=0.20,
        source_table='daily_beta_factor_results',
    ),
)

FACTOR_DEFINITION_BY_FAMILY = {definition.family: definition for definition in FACTOR_DEFINITIONS}
BASE_WEIGHTS = {definition.family: definition.base_weight for definition in FACTOR_DEFINITIONS}
EMPTY_INPUT_UNIVERSE_COUNTS = {
    definition.family: 0 for definition in FACTOR_DEFINITIONS
} | {'composite_score': 0}
CLASSIFICATION_PRIORITY = {
    CLASSIFICATION_ELIGIBLE: 0,
    CLASSIFICATION_WATCHLIST: 1,
    CLASSIFICATION_INELIGIBLE: 2,
    CLASSIFICATION_HARD_EXCLUDED: 3,
    CLASSIFICATION_INSUFFICIENT_INPUTS: 4,
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
            CLASSIFICATION_PRIORITY.get(item.get('composite_classification'), 999),
            item.get('composite_rank') is None,
            item.get('composite_rank') if item.get('composite_rank') is not None else 999999,
            item['symbol'],
        ),
    )


class DailyCompositeFactorService:
    @daily_price_bar_writer_admission
    def rebuild_for_market_date(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
        ranking_symbols: Sequence[str] | None = None,
        source: str = COMPOSITE_FACTOR_SOURCE,
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
        full_scope_replacement = symbols is None and ranking_symbols is None
        if normalized_symbols == []:
            result = self._empty_rebuild_result(
                market_date=normalized_market_date,
                source=source,
                requested_symbols=requested_symbols,
                matched_symbols=[],
                missing_symbols=[],
                explicit_empty_scope=True,
            )
            if exact_scope_symbols is not None:
                result['scope_finalization'] = (
                    replace_daily_derived_results_exact_scope(
                        market_date=normalized_market_date,
                        stage_id='composite_factors',
                        results=[],
                        exact_symbols=exact_scope_symbols,
                        authoritative_market_scope=(
                            authoritative_market_scope
                        ),
                    )['scope_finalization']
                )
            return result

        authoritative_universe = list_symbols_with_daily_bar_on_date(
            normalized_market_date.isoformat()
        )
        authoritative_symbol_set = set(authoritative_universe)
        authoritative_ranking_symbols = (
            [
                symbol
                for symbol in (normalized_ranking_symbols or [])
                if symbol in authoritative_symbol_set
            ]
            if ranking_symbols is not None
            else list(authoritative_universe)
        )
        ranking_rows_by_family = self._load_factor_rows(
            market_date=normalized_market_date,
            symbols=authoritative_ranking_symbols,
        )
        ranking_symbols = authoritative_ranking_symbols

        if normalized_symbols is None:
            matched_symbols = ranking_symbols
            missing_symbols: list[str] = []
            target_symbols = ranking_symbols
        else:
            matched_symbols = [
                symbol
                for symbol in normalized_symbols
                if symbol in authoritative_symbol_set
            ]
            matched_symbol_set = set(matched_symbols)
            missing_symbols = [symbol for symbol in normalized_symbols if symbol not in matched_symbol_set]
            target_symbols = matched_symbols
            if not matched_symbols:
                result = self._empty_rebuild_result(
                    market_date=normalized_market_date,
                    source=source,
                    requested_symbols=requested_symbols,
                    matched_symbols=[],
                    missing_symbols=missing_symbols,
                )
                if exact_scope_symbols is not None:
                    result['scope_finalization'] = (
                        replace_daily_derived_results_exact_scope(
                            market_date=normalized_market_date,
                            stage_id='composite_factors',
                            results=[],
                            exact_symbols=exact_scope_symbols,
                            authoritative_market_scope=(
                                authoritative_market_scope
                            ),
                        )['scope_finalization']
                    )
                return result

        metadata_map = self._load_symbol_metadata(
            symbols=sorted(set(ranking_symbols).union(target_symbols)),
            factor_rows_by_family=ranking_rows_by_family,
        )

        items_by_symbol: dict[str, dict[str, Any]] = {}
        for symbol in ranking_symbols:
            item = self._build_symbol_result(
                symbol=symbol,
                market_date=normalized_market_date,
                source=source,
                metadata=metadata_map.get(symbol),
                factor_rows_by_family=ranking_rows_by_family,
            )
            items_by_symbol[symbol] = item

        extra_symbols = [symbol for symbol in target_symbols if symbol not in items_by_symbol]
        for symbol in extra_symbols:
            item = self._build_symbol_result(
                symbol=symbol,
                market_date=normalized_market_date,
                source=source,
                metadata=metadata_map.get(symbol),
                factor_rows_by_family=ranking_rows_by_family,
            )
            items_by_symbol[symbol] = item

        all_items = list(items_by_symbol.values())
        input_universe_counts = self._assign_scores_and_rankings(all_items)
        for item in all_items:
            self._attach_output_blocks(item)
            item['factor_payload'] = self._build_factor_payload(
                item=item,
                source=source,
                input_universe_counts=input_universe_counts,
            )

        if normalized_symbols is None:
            items = _sort_items([items_by_symbol[symbol] for symbol in ranking_symbols])
        else:
            items = _sort_items([items_by_symbol[symbol] for symbol in target_symbols])

        summary = self._build_summary(
            items,
            requested_symbols=requested_symbols,
            matched_symbols=matched_symbols,
            missing_symbols=missing_symbols,
            input_universe_counts=input_universe_counts,
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
                stage_id='composite_factors',
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
                persisted_count = (
                    upsert_daily_composite_factor_results(
                        items,
                        replace_market_date=normalized_market_date,
                    )
                    if full_scope_replacement
                    else upsert_daily_composite_factor_results(items)
                )
                if persisted_count != len(items):
                    raise RuntimeError(
                        f'composite batch upsert persisted {persisted_count} of {len(items)} results'
                    )
            except Exception as batch_exc:
                persisted_count = 0
                if full_scope_replacement:
                    failed_symbols = [item['symbol'] for item in items]
                    failed_symbol_errors = [
                        {
                            'symbol': item['symbol'],
                            'error': f'atomic_full_scope_replace_failed: {batch_exc}',
                        }
                        for item in items
                    ]
                else:
                    for item in items:
                        try:
                            upsert_daily_composite_factor_result(item)
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
            if item.get('composite_score') is not None
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
        summary['common_complete_cohort_count'] = summary['complete_count']
        summary['renormalized_partial_cohort_count'] = summary['partial_count']

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
            'items': items,
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
        items = load_daily_composite_factor_results(
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
            'scored_count': sum(1 for item in items if item.get('composite_score') is not None),
            'eligible_count': sum(1 for item in items if item.get('is_composite_eligible')),
            'watchlist_count': sum(
                1 for item in items if item.get('composite_classification') == CLASSIFICATION_WATCHLIST
            ),
            'ineligible_count': sum(
                1 for item in items if item.get('composite_classification') == CLASSIFICATION_INELIGIBLE
            ),
            'hard_excluded_count': sum(
                1 for item in items if item.get('composite_classification') == CLASSIFICATION_HARD_EXCLUDED
            ),
            'insufficient_inputs_count': sum(
                1 for item in items if item.get('composite_classification') == CLASSIFICATION_INSUFFICIENT_INPUTS
            ),
        }
        payload: dict[str, Any] = {
            'market_date': normalized_market_date.isoformat(),
            'feature_version': COMPOSITE_FACTOR_VERSION,
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

    def _load_factor_rows(
        self,
        *,
        market_date: date,
        symbols: list[str] | None,
    ) -> dict[str, dict[str, dict[str, Any]]]:
        if symbols == []:
            return {family: {} for family in FACTOR_DEFINITION_BY_FAMILY}
        return {
            'momentum': {
                item['symbol']: item
                for item in load_daily_momentum_factor_results(
                    market_date=market_date,
                    symbols=symbols,
                    limit=None,
                )
            },
            'value_v1': {
                item['symbol']: item
                for item in load_daily_value_factor_results(
                    market_date=market_date,
                    symbols=symbols,
                    limit=None,
                )
            },
            'advanced_value': {
                item['symbol']: item
                for item in load_daily_advanced_value_factor_results(
                    market_date=market_date,
                    symbols=symbols,
                    limit=None,
                )
            },
            'low_risk': {
                item['symbol']: item
                for item in load_daily_low_risk_factor_results(
                    market_date=market_date,
                    symbols=symbols,
                    limit=None,
                )
            },
            'beta': {
                item['symbol']: item
                for item in load_daily_beta_factor_results(
                    market_date=market_date,
                    symbols=symbols,
                    limit=None,
                )
            },
        }

    @staticmethod
    def _collect_symbol_universe(
        factor_rows_by_family: dict[str, dict[str, dict[str, Any]]],
    ) -> list[str]:
        symbols: set[str] = set()
        for rows in factor_rows_by_family.values():
            symbols.update(rows.keys())
        return sorted(symbols)

    def _load_symbol_metadata(
        self,
        *,
        symbols: list[str],
        factor_rows_by_family: dict[str, dict[str, dict[str, Any]]],
    ) -> dict[str, dict[str, Any]]:
        if not symbols:
            return {}

        metadata_map = {
            item['symbol']: {
                'symbol': item['symbol'],
                'name_ko': item.get('name_ko'),
                'market': item.get('market'),
            }
            for item in load_symbols(symbols=symbols)
        }
        for symbol in symbols:
            if symbol in metadata_map:
                continue
            metadata_map[symbol] = {
                'symbol': symbol,
                'name_ko': self._first_available_metadata_value(
                    symbol=symbol,
                    factor_rows_by_family=factor_rows_by_family,
                    key='name_ko',
                ),
                'market': self._first_available_metadata_value(
                    symbol=symbol,
                    factor_rows_by_family=factor_rows_by_family,
                    key='market',
                ),
            }
        return metadata_map

    @staticmethod
    def _first_available_metadata_value(
        *,
        symbol: str,
        factor_rows_by_family: dict[str, dict[str, dict[str, Any]]],
        key: str,
    ) -> Any:
        for family in ('value_v1', 'advanced_value', 'low_risk', 'beta', 'momentum'):
            row = factor_rows_by_family.get(family, {}).get(symbol)
            if row is not None and row.get(key) is not None:
                return row.get(key)
        return None

    def _build_symbol_result(
        self,
        *,
        symbol: str,
        market_date: date,
        source: str,
        metadata: dict[str, Any] | None,
        factor_rows_by_family: dict[str, dict[str, dict[str, Any]]],
    ) -> dict[str, Any]:
        factor_snapshot = {
            'momentum': self._build_momentum_snapshot(factor_rows_by_family['momentum'].get(symbol)),
            'value_v1': self._build_value_snapshot(factor_rows_by_family['value_v1'].get(symbol)),
            'advanced_value': self._build_advanced_value_snapshot(
                factor_rows_by_family['advanced_value'].get(symbol)
            ),
            'low_risk': self._build_low_risk_snapshot(factor_rows_by_family['low_risk'].get(symbol)),
            'beta': self._build_beta_snapshot(factor_rows_by_family['beta'].get(symbol)),
        }
        return {
            'symbol': symbol,
            'name_ko': (metadata or {}).get('name_ko'),
            'market': (metadata or {}).get('market'),
            'market_date': market_date.isoformat(),
            'source': source,
            'feature_version': source,
            'momentum_input': factor_snapshot['momentum']['score'],
            'value_input': factor_snapshot['value_v1']['score'],
            'advanced_value_input': factor_snapshot['advanced_value']['score'],
            'low_risk_input': factor_snapshot['low_risk']['score'],
            'low_beta_input': factor_snapshot['beta']['score'],
            'momentum_contribution': None,
            'value_contribution': None,
            'advanced_value_contribution': None,
            'low_risk_contribution': None,
            'low_beta_contribution': None,
            'valid_factor_count': 0,
            'available_base_weight': 0.0,
            'has_required_inputs': False,
            'is_hard_excluded': False,
            'composite_score': None,
            'composite_rank': None,
            'composite_percentile': None,
            'composite_classification': CLASSIFICATION_INSUFFICIENT_INPUTS,
            'is_composite_eligible': False,
            'factor_payload': {},
            '_factor_snapshot': factor_snapshot,
            '_applied_weights': {definition.family: None for definition in FACTOR_DEFINITIONS},
            '_hard_exclusion_reasons': [],
            '_has_valuation_input': False,
        }

    @staticmethod
    def _build_momentum_snapshot(row: dict[str, Any] | None) -> dict[str, Any]:
        definition = FACTOR_DEFINITION_BY_FAMILY['momentum']
        enriched_row = enrich_momentum_audit_fields(row) if row is not None else {}
        score = _round(enriched_row.get('momentum_percentile')) if row is not None else None
        if row is None:
            status = 'missing_factor_row'
        elif score is None and enriched_row.get('has_sufficient_history') is False:
            status = 'insufficient_history'
        elif score is None:
            status = 'missing_percentile'
        else:
            status = enriched_row.get('momentum_eligibility_status') or 'scored'
        return {
            'input_name': definition.input_name,
            'source_table': definition.source_table,
            'base_weight': definition.base_weight,
            'score': score,
            'available': score is not None,
            'status': status,
            'source_feature_version': enriched_row.get('feature_version') or enriched_row.get('source'),
            'rank': enriched_row.get('momentum_rank'),
            'percentile': enriched_row.get('momentum_percentile'),
            'classification': None,
            'is_eligible': enriched_row.get('is_momentum_eligible'),
            'gate_flags': {
                'is_recent_overheated': enriched_row.get('is_recent_overheated'),
                'has_sufficient_history': enriched_row.get('has_sufficient_history'),
                'is_price_above_ma120': enriched_row.get('is_price_above_ma120'),
                'is_ma60_above_ma120': enriched_row.get('is_ma60_above_ma120'),
                'passes_momentum_top_percentile': enriched_row.get('passes_momentum_top_percentile'),
                'momentum_percentile_threshold': enriched_row.get('momentum_percentile_threshold'),
            },
        }

    @staticmethod
    def _build_value_snapshot(row: dict[str, Any] | None) -> dict[str, Any]:
        definition = FACTOR_DEFINITION_BY_FAMILY['value_v1']
        score = _round(row.get('value_score')) if row is not None and row.get('value_status') == 'scored' else None
        return {
            'input_name': definition.input_name,
            'source_table': definition.source_table,
            'base_weight': definition.base_weight,
            'score': score,
            'available': score is not None,
            'status': (row or {}).get('value_status') or 'missing_factor_row',
            'source_feature_version': (row or {}).get('feature_version') or (row or {}).get('source'),
            'rank': (row or {}).get('value_rank'),
            'percentile': (row or {}).get('value_percentile'),
            'classification': (row or {}).get('value_status'),
            'is_eligible': (row or {}).get('value_status') == 'scored',
            'gate_flags': {},
        }

    @staticmethod
    def _build_advanced_value_snapshot(row: dict[str, Any] | None) -> dict[str, Any]:
        definition = FACTOR_DEFINITION_BY_FAMILY['advanced_value']
        score = _round(row.get('advanced_value_score')) if row is not None else None
        return {
            'input_name': definition.input_name,
            'source_table': definition.source_table,
            'base_weight': definition.base_weight,
            'score': score,
            'available': score is not None,
            'status': (row or {}).get('advanced_value_classification') or 'missing_factor_row',
            'source_feature_version': (row or {}).get('feature_version') or (row or {}).get('source'),
            'rank': (row or {}).get('advanced_value_rank'),
            'percentile': (row or {}).get('advanced_value_percentile'),
            'classification': (row or {}).get('advanced_value_classification'),
            'is_eligible': (row or {}).get('is_advanced_value_eligible'),
            'gate_flags': {},
        }

    @staticmethod
    def _build_low_risk_snapshot(row: dict[str, Any] | None) -> dict[str, Any]:
        definition = FACTOR_DEFINITION_BY_FAMILY['low_risk']
        score = _round(row.get('low_risk_score')) if row is not None else None
        return {
            'input_name': definition.input_name,
            'source_table': definition.source_table,
            'base_weight': definition.base_weight,
            'score': score,
            'available': score is not None,
            'status': (row or {}).get('low_risk_classification') or 'missing_factor_row',
            'source_feature_version': (row or {}).get('feature_version') or (row or {}).get('source'),
            'rank': None,
            'percentile': None,
            'classification': (row or {}).get('low_risk_classification'),
            'is_eligible': (row or {}).get('is_low_risk_eligible'),
            'gate_flags': {
                'passes_liquidity_floor': (row or {}).get('passes_liquidity_floor'),
            },
        }

    @staticmethod
    def _build_beta_snapshot(row: dict[str, Any] | None) -> dict[str, Any]:
        definition = FACTOR_DEFINITION_BY_FAMILY['beta']
        score = _round(row.get('low_beta_score')) if row is not None else None
        return {
            'input_name': definition.input_name,
            'source_table': definition.source_table,
            'base_weight': definition.base_weight,
            'score': score,
            'available': score is not None,
            'status': (row or {}).get('low_beta_classification') or 'missing_factor_row',
            'source_feature_version': (row or {}).get('feature_version') or (row or {}).get('source'),
            'rank': None,
            'percentile': None,
            'classification': (row or {}).get('low_beta_classification'),
            'is_eligible': (row or {}).get('is_low_beta_eligible'),
            'gate_flags': {},
        }

    def _assign_scores_and_rankings(self, items: list[dict[str, Any]]) -> dict[str, int]:
        input_universe_counts = {
            family: sum(
                1
                for item in items
                if item['_factor_snapshot'][family]['available']
            )
            for family in FACTOR_DEFINITION_BY_FAMILY
        }

        for item in items:
            factor_snapshot = item['_factor_snapshot']
            valid_families = [
                family
                for family, snapshot in factor_snapshot.items()
                if snapshot.get('available')
            ]
            available_base_weight = _round(sum(BASE_WEIGHTS[family] for family in valid_families), 6) or 0.0
            has_valuation_input = any(
                factor_snapshot[family].get('available') for family in COMPOSITE_REQUIRED_VALUATION_FACTORS
            )
            has_required_inputs = (
                len(valid_families) >= COMPOSITE_MIN_VALID_INPUTS
                and available_base_weight >= COMPOSITE_MIN_AVAILABLE_WEIGHT
                and has_valuation_input
            )

            item['valid_factor_count'] = len(valid_families)
            item['available_base_weight'] = available_base_weight
            item['has_required_inputs'] = has_required_inputs
            item['_has_valuation_input'] = has_valuation_input

            for definition in FACTOR_DEFINITIONS:
                item['_applied_weights'][definition.family] = None
                item[definition.contribution_field] = None

            if not has_required_inputs:
                item['composite_classification'] = CLASSIFICATION_INSUFFICIENT_INPUTS
                item['is_composite_eligible'] = False
                item['is_hard_excluded'] = False
                item['_hard_exclusion_reasons'] = []
                continue

            weighted_sum = 0.0
            for definition in FACTOR_DEFINITIONS:
                snapshot = factor_snapshot[definition.family]
                if not snapshot.get('available'):
                    continue
                applied_weight = _round(definition.base_weight / available_base_weight, 6)
                contribution = _round(float(snapshot['score']) * float(applied_weight), 6)
                item['_applied_weights'][definition.family] = applied_weight
                item[definition.contribution_field] = contribution
                weighted_sum += float(contribution)

            item['composite_score'] = _round(weighted_sum, 6)
            hard_exclusion_reasons: list[str] = []
            if factor_snapshot['momentum']['gate_flags'].get('is_recent_overheated') is True:
                hard_exclusion_reasons.append(HARD_EXCLUSION_MOMENTUM_OVERHEAT)
            liquidity_evidence = factor_snapshot['low_risk']['gate_flags'].get('passes_liquidity_floor')
            if not factor_snapshot['low_risk']['available'] or liquidity_evidence is None:
                hard_exclusion_reasons.append(HARD_EXCLUSION_LOW_RISK_EVIDENCE_MISSING)
            elif liquidity_evidence is False:
                hard_exclusion_reasons.append(HARD_EXCLUSION_LOW_RISK_LIQUIDITY)
            item['_hard_exclusion_reasons'] = hard_exclusion_reasons
            item['is_hard_excluded'] = bool(hard_exclusion_reasons)

            if item['is_hard_excluded']:
                item['composite_classification'] = CLASSIFICATION_HARD_EXCLUDED
                item['is_composite_eligible'] = False
            elif float(item['composite_score']) >= COMPOSITE_ELIGIBLE_SCORE:
                item['composite_classification'] = CLASSIFICATION_ELIGIBLE
                item['is_composite_eligible'] = True
            elif float(item['composite_score']) >= COMPOSITE_WATCHLIST_SCORE:
                item['composite_classification'] = CLASSIFICATION_WATCHLIST
                item['is_composite_eligible'] = False
            else:
                item['composite_classification'] = CLASSIFICATION_INELIGIBLE
                item['is_composite_eligible'] = False

        scored_items = sorted(
            [item for item in items if item.get('composite_score') is not None],
            key=lambda item: (-float(item['composite_score']), item['symbol']),
        )
        total = len(scored_items)
        for rank, item in enumerate(scored_items, start=1):
            item['composite_rank'] = rank
            item['composite_percentile'] = _percentile_from_rank(rank, total)

        input_universe_counts['composite_score'] = total
        return input_universe_counts

    def _build_factor_payload(
        self,
        *,
        item: dict[str, Any],
        source: str,
        input_universe_counts: dict[str, int],
    ) -> dict[str, Any]:
        factor_snapshot = item['_factor_snapshot']
        applied_weights = item['_applied_weights']
        contributions = {
            definition.family: item.get(definition.contribution_field)
            for definition in FACTOR_DEFINITIONS
        }
        factor_inputs = {
            definition.family: {
                'input_name': factor_snapshot[definition.family]['input_name'],
                'normalized_score': factor_snapshot[definition.family]['score'],
                'available': factor_snapshot[definition.family]['available'],
                'status': factor_snapshot[definition.family]['status'],
                'source_table': factor_snapshot[definition.family]['source_table'],
                'source_feature_version': factor_snapshot[definition.family]['source_feature_version'],
                'base_weight': factor_snapshot[definition.family]['base_weight'],
                'applied_weight': applied_weights[definition.family],
                'contribution': contributions[definition.family],
                'classification': factor_snapshot[definition.family]['classification'],
                'is_factor_eligible': factor_snapshot[definition.family]['is_eligible'],
                'rank': factor_snapshot[definition.family]['rank'],
                'percentile': factor_snapshot[definition.family]['percentile'],
                'gate_flags': factor_snapshot[definition.family]['gate_flags'],
            }
            for definition in FACTOR_DEFINITIONS
        }
        return {
            'feature_version': source,
            'weight_model_version': COMPOSITE_WEIGHT_MODEL_VERSION,
            'methodology': {
                'score_method': 'weighted_mean_of_normalized_factor_inputs',
                'input_direction': 'higher_is_better_for_every_factor',
                'weight_policy': 'renormalize_across_available_inputs_after_input_requirements_pass',
                'tie_breaker': 'symbol_asc',
            },
            'requirements': {
                'minimum_valid_inputs': COMPOSITE_MIN_VALID_INPUTS,
                'minimum_available_weight': COMPOSITE_MIN_AVAILABLE_WEIGHT,
                'requires_any_of_factors': sorted(COMPOSITE_REQUIRED_VALUATION_FACTORS),
                'has_valuation_input': item['_has_valuation_input'],
                'has_required_inputs': item['has_required_inputs'],
            },
            'weights': {
                'base': BASE_WEIGHTS,
                'applied': applied_weights,
                'available_base_weight': item['available_base_weight'],
            },
            'inputs': factor_inputs,
            'contributions': contributions,
            'gates': {
                'has_required_inputs': item['has_required_inputs'],
                'is_recent_overheated': factor_snapshot['momentum']['gate_flags'].get('is_recent_overheated'),
                'passes_momentum_top_percentile': factor_snapshot['momentum']['gate_flags'].get(
                    'passes_momentum_top_percentile'
                ),
                'momentum_percentile_threshold': factor_snapshot['momentum']['gate_flags'].get(
                    'momentum_percentile_threshold'
                ),
                'passes_liquidity_floor': factor_snapshot['low_risk']['gate_flags'].get('passes_liquidity_floor'),
                'is_hard_excluded': item['is_hard_excluded'],
                'hard_exclusion_reasons': item['_hard_exclusion_reasons'],
                'hard_exclusion_rules': {
                    HARD_EXCLUSION_MOMENTUM_OVERHEAT: {
                        'type': 'hard_exclusion',
                        'description': 'Exclude symbols flagged as recently overheated by the momentum slice.',
                    },
                    HARD_EXCLUSION_LOW_RISK_LIQUIDITY: {
                        'type': 'hard_exclusion',
                        'description': 'Exclude symbols that fail the low-risk liquidity floor.',
                    },
                    HARD_EXCLUSION_LOW_RISK_EVIDENCE_MISSING: {
                        'type': 'hard_exclusion',
                        'description': 'Exclude live candidates when low-risk or liquidity evidence is unavailable.',
                    },
                },
                'informational_only_rules': {
                    'momentum_eligibility_status': factor_snapshot['momentum']['status'],
                    'value_status': factor_snapshot['value_v1']['status'],
                    'advanced_value_classification': factor_snapshot['advanced_value']['classification'],
                    'low_beta_classification': factor_snapshot['beta']['classification'],
                    'low_risk_classification': factor_snapshot['low_risk']['classification'],
                },
            },
            'classification_rule': {
                'eligible_min_score': COMPOSITE_ELIGIBLE_SCORE,
                'watchlist_min_score': COMPOSITE_WATCHLIST_SCORE,
                'classification': item['composite_classification'],
            },
            'ranking': {
                'composite_rank': item['composite_rank'],
                'composite_percentile': item['composite_percentile'],
                'input_universe_counts': input_universe_counts,
            },
            'audit': {
                'trade_date': item['market_date'],
                'name_ko': item.get('name_ko'),
                'market': item.get('market'),
            },
        }

    def _attach_output_blocks(self, item: dict[str, Any]) -> None:
        item['inputs'] = {
            definition.input_name: item.get(definition.input_field)
            for definition in FACTOR_DEFINITIONS
        }
        item['weights'] = {
            'weight_model_version': COMPOSITE_WEIGHT_MODEL_VERSION,
            'base': BASE_WEIGHTS,
            'applied': item['_applied_weights'],
        }
        item['contributions'] = {
            definition.family: item.get(definition.contribution_field)
            for definition in FACTOR_DEFINITIONS
        }
        item['gates'] = {
            'has_required_inputs': item['has_required_inputs'],
            'valid_input_count': item['valid_factor_count'],
            'minimum_valid_input_count': COMPOSITE_MIN_VALID_INPUTS,
            'available_base_weight': item['available_base_weight'],
            'minimum_available_weight': COMPOSITE_MIN_AVAILABLE_WEIGHT,
            'has_valuation_input': item['_has_valuation_input'],
            'requires_any_of_factors': sorted(COMPOSITE_REQUIRED_VALUATION_FACTORS),
            'is_recent_overheated': item['_factor_snapshot']['momentum']['gate_flags'].get('is_recent_overheated'),
            'passes_momentum_top_percentile': item['_factor_snapshot']['momentum']['gate_flags'].get(
                'passes_momentum_top_percentile'
            ),
            'momentum_percentile_threshold': item['_factor_snapshot']['momentum']['gate_flags'].get(
                'momentum_percentile_threshold'
            ),
            'momentum_eligibility_status': item['_factor_snapshot']['momentum']['status'],
            'passes_liquidity_floor': item['_factor_snapshot']['low_risk']['gate_flags'].get('passes_liquidity_floor'),
            'is_hard_excluded': item['is_hard_excluded'],
            'hard_exclusion_reasons': item['_hard_exclusion_reasons'],
        }

    def _build_summary(
        self,
        items: list[dict[str, Any]],
        *,
        requested_symbols: list[str],
        matched_symbols: list[str],
        missing_symbols: list[str],
        input_universe_counts: dict[str, int],
    ) -> dict[str, Any]:
        ready_count = sum(1 for item in items if item.get('composite_score') is not None)
        summary = {
            'requested_symbol_count': len(requested_symbols) if requested_symbols else len(matched_symbols),
            'matched_symbol_count': len(matched_symbols),
            'missing_symbol_count': len(missing_symbols),
            'symbols_with_any_input': sum(1 for item in items if item.get('valid_factor_count', 0) > 0),
            'sufficient_input_count': sum(1 for item in items if item.get('has_required_inputs')),
            'scored_count': ready_count,
            'ready_count': ready_count,
            'unready_count': len(missing_symbols) + len(items) - ready_count,
            'eligible_count': sum(1 for item in items if item.get('is_composite_eligible')),
            'watchlist_count': sum(
                1 for item in items if item.get('composite_classification') == CLASSIFICATION_WATCHLIST
            ),
            'ineligible_count': sum(
                1 for item in items if item.get('composite_classification') == CLASSIFICATION_INELIGIBLE
            ),
            'hard_excluded_count': sum(
                1 for item in items if item.get('composite_classification') == CLASSIFICATION_HARD_EXCLUDED
            ),
            'insufficient_inputs_count': sum(
                1 for item in items if item.get('composite_classification') == CLASSIFICATION_INSUFFICIENT_INPUTS
            ),
            'hard_exclusion_counts': {
                HARD_EXCLUSION_MOMENTUM_OVERHEAT: sum(
                    1
                    for item in items
                    if HARD_EXCLUSION_MOMENTUM_OVERHEAT in item.get('_hard_exclusion_reasons', [])
                ),
                HARD_EXCLUSION_LOW_RISK_LIQUIDITY: sum(
                    1
                    for item in items
                    if HARD_EXCLUSION_LOW_RISK_LIQUIDITY in item.get('_hard_exclusion_reasons', [])
                ),
                HARD_EXCLUSION_LOW_RISK_EVIDENCE_MISSING: sum(
                    1
                    for item in items
                    if HARD_EXCLUSION_LOW_RISK_EVIDENCE_MISSING in item.get('_hard_exclusion_reasons', [])
                ),
            },
            'input_universe_counts': input_universe_counts,
        }
        summary.update(
            self._build_coverage_contract(
                items=items,
                persisted_items=items,
                missing_symbols=missing_symbols,
                failed_symbols=[],
            )
        )
        summary['common_complete_cohort_count'] = summary['complete_count']
        summary['renormalized_partial_cohort_count'] = summary['partial_count']
        return summary

    @staticmethod
    def _build_coverage_contract(
        *,
        items: list[dict[str, Any]],
        persisted_items: list[dict[str, Any]],
        missing_symbols: list[str],
        failed_symbols: list[str],
    ) -> dict[str, Any]:
        def snapshot(item: dict[str, Any], family: str) -> dict[str, Any]:
            payload = item.get('factor_payload')
            inputs = payload.get('inputs') if isinstance(payload, dict) else None
            family_payload = inputs.get(family) if isinstance(inputs, dict) else None
            return family_payload if isinstance(family_payload, dict) else {}

        return build_factor_coverage_contract(
            items=items,
            persisted_items=persisted_items,
            missing_symbols=missing_symbols,
            failed_symbols=failed_symbols,
            is_ready=lambda item: item.get('composite_score') is not None,
            is_complete=lambda item: int(item.get('valid_factor_count') or 0)
            == len(FACTOR_DEFINITIONS),
            unusable_reason=lambda item: item.get('composite_classification'),
            input_families={
                family: (
                    lambda item, family=family: snapshot(item, family).get('available')
                    is True,
                    lambda item, family=family: snapshot(item, family).get('status'),
                )
                for family in FACTOR_DEFINITION_BY_FAMILY
            },
        )

    @staticmethod
    def _strip_internal_fields(item: dict[str, Any]) -> None:
        item.pop('_factor_snapshot', None)
        item.pop('_applied_weights', None)
        item.pop('_hard_exclusion_reasons', None)
        item.pop('_has_valuation_input', None)

    def _empty_rebuild_result(
        self,
        *,
        market_date: date,
        source: str,
        requested_symbols: list[str],
        matched_symbols: list[str],
        missing_symbols: list[str],
        explicit_empty_scope: bool = False,
    ) -> dict[str, Any]:
        summary = self._build_summary(
            [],
            requested_symbols=requested_symbols,
            matched_symbols=matched_symbols,
            missing_symbols=missing_symbols,
            input_universe_counts=dict(EMPTY_INPUT_UNIVERSE_COUNTS),
        )
        stage_contract = (
            {
                'status': 'completed',
                'reason': 'explicit_empty_scope_noop',
                'quality_status': summary['quality_status'],
                'quality_warning': summary['quality_warning'],
                'gate_status': 'ready',
                'blocking_status': 'non_blocking',
                'remaining_retryable_events': 0,
            }
            if explicit_empty_scope
            else factor_stage_result_contract(summary)
        )
        return {
            **stage_contract,
            'market_date': market_date.isoformat(),
            'source': source,
            'feature_version': source,
            'requested_symbols': requested_symbols,
            'matched_symbols': matched_symbols,
            'missing_symbols': missing_symbols,
            'summary': summary,
            'results_persisted': 0,
            'results_failed': 0,
            'failed_symbols': [],
            'failed_symbol_errors': [],
            'items': [],
        }


daily_composite_factor_service = DailyCompositeFactorService()
