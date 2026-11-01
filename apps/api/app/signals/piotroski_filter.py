from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Sequence

from sqlalchemy import and_, case, func, select

from app.dart.service import (
    build_highlights_from_normalized_items,
)
from app.dart.signals_service import (
    SHARE_ISSUANCE_AUTHORITY_POLICY_VERSION,
    SHARE_ISSUANCE_LATE_ARRIVAL_POLICY,
)
from app.db import session_scope
from app.db_models import (
    DailyPriceBar,
    DailyPykrxEnrichmentRow,
    DailyTechnicalSnapshot,
    DartAuditSignal,
    DartDilutionSignal,
    DartDisclosureEvent,
    DartFundamentalsHighlight,
    DartFundamentalsRow,
    DartFundamentalsSnapshot,
    Symbol,
)
from app.eod.finality import validate_finalized_krx_write_date
from app.market_data_store import (
    load_daily_piotroski_filter_results,
    load_latest_available_daily_piotroski_filter_result_date,
    upsert_daily_piotroski_filter_result,
    upsert_daily_piotroski_filter_results,
)
from app.signals.preferred_share_fundamentals import (
    resolve_preferred_fundamentals_issuer_symbols,
)

FILTER_SOURCE = 'strategy_piotroski_v1'
LOW_PBR_PERCENTILE = 0.30
BOTTOM_MOMENTUM_PERCENTILE = 0.30
PIOTROSKI_PASS_SCORE = 7
PIOTROSKI_SIGNAL_COUNT = 9
PIOTROSKI_OPTIONAL_EVENT_SIGNAL_NAMES = ('no_new_share_issuance',)
PIOTROSKI_REQUIRED_FINANCIAL_SIGNAL_NAMES = (
    'positive_roa',
    'positive_operating_cash_flow',
    'improving_roa',
    'accrual_quality',
    'lower_leverage',
    'improving_current_ratio',
    'improving_gross_margin',
    'improving_asset_turnover',
)
LATEST_FALLBACK_POLICY = 'latest_on_or_before_requested_market_date'
PIOTROSKI_INPUT_QUERY_CHUNK_SIZE = 500
DILUTION_NEGATIVE_COVERAGE_SCHEMA = 'dart_share_issuance_coverage_v1'
DILUTION_NEGATIVE_COVERAGE_RULE_VERSION = 'dart_share_issuance_rules_v1'
DILUTION_EVIDENCE_SIGNAL_TYPES = (
    'share_issuance',
    'paid_increase',
    'convertible_bond',
    'bond_with_warrant',
    'exchange_bond',
)
DILUTION_NEGATIVE_COVERAGE_SOURCE = 'dart_disclosure_events'
DILUTION_NEGATIVE_COVERAGE_DAYS = 365
PIOTROSKI_RAW_FALLBACK_FACT_KEYS = (
    'total_assets',
    'current_assets',
    'current_liabilities',
    'gross_profit',
    'revenue',
)

INDEX_GROSS_MARGIN = 'M211300'
INDEX_CURRENT_RATIO = 'M221200'
INDEX_ASSET_TURNOVER = 'M241000'
INDEX_DEBT_RATIO = 'M221100'

CANONICAL_GROSS_MARGIN = 'gross_margin'
CANONICAL_CURRENT_RATIO = 'current_ratio'
CANONICAL_ASSET_TURNOVER = 'asset_turnover'
CANONICAL_DEBT_RATIO = 'debt_ratio'


@dataclass(slots=True)
class PiotroskiInputs:
    symbol: str
    name_ko: str | None
    market_date: date
    pbr: float | None
    return_20d: float | None
    total_equity: float | None
    net_income: float | None
    prior_net_income: float | None
    operating_cash_flow: float | None
    total_assets: float | None
    prior_total_assets: float | None
    debt_ratio: float | None
    prior_debt_ratio: float | None
    current_ratio: float | None
    prior_current_ratio: float | None
    gross_margin: float | None
    prior_gross_margin: float | None
    asset_turnover: float | None
    prior_asset_turnover: float | None
    has_audit_issue: bool | None
    has_dilution_signal: bool | None
    annual_year: int | None = None
    prior_annual_year: int | None = None
    fundamentals_provenance: dict[int, dict[str, Any]] = field(default_factory=dict)
    audit_provenance: dict[str, Any] | None = None
    dilution_provenance: dict[str, Any] | None = None
    valuation_source: str = 'daily_price_bars'
    valuation_provenance: dict[str, Any] | None = None


@dataclass(slots=True)
class PiotroskiSignalResult:
    value: bool | None
    available: bool


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _percentile_rank(values: list[float], current: float, *, ascending: bool) -> float:
    ordered = sorted(values, reverse=not ascending)
    if len(ordered) <= 1:
        return 0.0
    return ordered.index(current) / (len(ordered) - 1)


def _signal_result(value: bool | None) -> PiotroskiSignalResult:
    return PiotroskiSignalResult(value=value, available=value is not None)


def _chunks(items: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        return [items]
    return [items[index:index + size] for index in range(0, len(items), size)]


def _normalize_symbols(symbols: Sequence[str] | None) -> list[str] | None:
    if symbols is None:
        return None
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        symbol = str(raw_symbol or '').strip().upper()
        if not symbol:
            raise ValueError('symbol must not be blank')
        if symbol.isdigit():
            symbol = symbol.zfill(6)
        if symbol in seen:
            continue
        seen.add(symbol)
        normalized.append(symbol)
    return normalized


def _current_highlight_value(highlight: dict[str, Any] | None) -> Any:
    if not isinstance(highlight, dict):
        return None
    statement_type = str(highlight.get('statement_type') or '').strip().upper()
    if statement_type in {'IS', 'CIS', 'CF'}:
        cumulative_amount = highlight.get('current_cumulative_amount')
        if cumulative_amount is not None:
            return cumulative_amount
    current_amount = highlight.get('current_amount')
    if current_amount is not None:
        return current_amount
    return highlight.get('current_cumulative_amount')


def _validated_negative_dilution_coverage(
    payload: Any,
    *,
    market_date: date,
    expected_corp_code: str,
    row_corp_code: str,
) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    coverage = payload.get('evidence_coverage')
    if not isinstance(coverage, dict):
        return None
    if coverage.get('schema_version') != DILUTION_NEGATIVE_COVERAGE_SCHEMA:
        return None
    if coverage.get('rule_version') != DILUTION_NEGATIVE_COVERAGE_RULE_VERSION:
        return None
    if coverage.get('status') != 'complete':
        return None
    if coverage.get('source') != DILUTION_NEGATIVE_COVERAGE_SOURCE:
        return None
    if coverage.get('pagination_completed') is not True:
        return None
    if coverage.get('authoritative_as_of_cutoff') is not True:
        return None
    if (
        coverage.get('authority_policy_version')
        != SHARE_ISSUANCE_AUTHORITY_POLICY_VERSION
    ):
        return None
    if (
        coverage.get('late_arrival_reconciliation')
        != SHARE_ISSUANCE_LATE_ARRIVAL_POLICY
    ):
        return None
    if not expected_corp_code or row_corp_code != expected_corp_code:
        return None
    if coverage.get('corp_code') != expected_corp_code:
        return None
    if coverage.get('matched_event_count') != 0:
        return None
    try:
        window_start = date.fromisoformat(str(coverage.get('window_start') or ''))
        window_end = date.fromisoformat(str(coverage.get('window_end') or ''))
        scan_completed_at = datetime.fromisoformat(
            str(coverage.get('scan_completed_at') or '')
        )
    except ValueError:
        return None
    if scan_completed_at.tzinfo is None:
        return None
    try:
        validate_finalized_krx_write_date(
            market_date,
            now=scan_completed_at,
            operation='DART share issuance negative coverage',
        )
    except ValueError:
        return None
    if window_end != market_date:
        return None
    if window_start > market_date - timedelta(days=DILUTION_NEGATIVE_COVERAGE_DAYS):
        return None
    return {
        **coverage,
        'window_start': window_start.isoformat(),
        'window_end': window_end.isoformat(),
    }


def _mapped_float(
    values_by_symbol: dict[str, dict[tuple[str, int], Any]],
    *,
    symbol: str,
    business_year: int | None,
    metric_keys: tuple[str, ...],
) -> float | None:
    if business_year is None:
        return None
    symbol_values = values_by_symbol.get(symbol, {})
    for metric_key in metric_keys:
        value = _as_float(symbol_values.get((metric_key, business_year)))
        if value is not None:
            return value
    return None


def _ratio_or_derived(
    *,
    symbol: str,
    business_year: int | None,
    ratios_by_symbol: dict[str, dict[tuple[str, int], Any]],
    highlights_by_symbol: dict[str, dict[tuple[str, int], Any]],
    ratio_metric_keys: tuple[str, ...],
    numerator_metric_key: str,
    denominator_metric_key: str,
    require_positive_denominator: bool = False,
) -> float | None:
    denominator = _mapped_float(
        highlights_by_symbol,
        symbol=symbol,
        business_year=business_year,
        metric_keys=(denominator_metric_key,),
    )
    if require_positive_denominator and (
        denominator is None or denominator <= 0
    ):
        return None
    stored_ratio = _mapped_float(
        ratios_by_symbol,
        symbol=symbol,
        business_year=business_year,
        metric_keys=ratio_metric_keys,
    )
    if stored_ratio is not None:
        return stored_ratio
    numerator = _mapped_float(
        highlights_by_symbol,
        symbol=symbol,
        business_year=business_year,
        metric_keys=(numerator_metric_key,),
    )
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def _compute_signals(item: PiotroskiInputs) -> tuple[dict[str, PiotroskiSignalResult], int, int]:
    roa = (item.net_income / item.total_assets) if item.net_income is not None and item.total_assets not in (None, 0) else None
    prior_roa = None
    if item.prior_net_income is not None and item.prior_total_assets not in (None, 0):
        prior_roa = item.prior_net_income / item.prior_total_assets

    no_new_share_issuance = None if item.has_dilution_signal is None else (not item.has_dilution_signal)
    signals = {
        'positive_roa': _signal_result(roa > 0 if roa is not None else None),
        'positive_operating_cash_flow': _signal_result(item.operating_cash_flow > 0 if item.operating_cash_flow is not None else None),
        'improving_roa': _signal_result(roa > prior_roa if roa is not None and prior_roa is not None else None),
        'accrual_quality': _signal_result(item.operating_cash_flow > item.net_income if item.operating_cash_flow is not None and item.net_income is not None else None),
        'lower_leverage': _signal_result(item.debt_ratio < item.prior_debt_ratio if item.debt_ratio is not None and item.prior_debt_ratio is not None else None),
        'improving_current_ratio': _signal_result(item.current_ratio > item.prior_current_ratio if item.current_ratio is not None and item.prior_current_ratio is not None else None),
        'no_new_share_issuance': _signal_result(no_new_share_issuance),
        'improving_gross_margin': _signal_result(item.gross_margin > item.prior_gross_margin if item.gross_margin is not None and item.prior_gross_margin is not None else None),
        'improving_asset_turnover': _signal_result(item.asset_turnover > item.prior_asset_turnover if item.asset_turnover is not None and item.prior_asset_turnover is not None else None),
    }
    available_count = sum(1 for result in signals.values() if result.available)
    score = sum(1 for result in signals.values() if result.value is True)
    return signals, score, available_count


def _required_financial_signal_missing_reasons(
    item: PiotroskiInputs,
    signals: dict[str, PiotroskiSignalResult],
) -> dict[str, list[str]]:
    candidate_reasons = {
        'positive_roa': [
            ('missing_net_income', item.net_income is None),
            ('missing_total_assets', item.total_assets is None),
            ('zero_total_assets', item.total_assets == 0),
        ],
        'positive_operating_cash_flow': [
            (
                'missing_operating_cash_flow',
                item.operating_cash_flow is None,
            ),
        ],
        'improving_roa': [
            ('missing_net_income', item.net_income is None),
            ('missing_total_assets', item.total_assets is None),
            ('zero_total_assets', item.total_assets == 0),
            ('missing_prior_net_income', item.prior_net_income is None),
            ('missing_prior_total_assets', item.prior_total_assets is None),
            ('zero_prior_total_assets', item.prior_total_assets == 0),
        ],
        'accrual_quality': [
            (
                'missing_operating_cash_flow',
                item.operating_cash_flow is None,
            ),
            ('missing_net_income', item.net_income is None),
        ],
        'lower_leverage': [
            ('missing_debt_ratio', item.debt_ratio is None),
            ('missing_prior_debt_ratio', item.prior_debt_ratio is None),
        ],
        'improving_current_ratio': [
            ('missing_current_ratio', item.current_ratio is None),
            (
                'missing_prior_current_ratio',
                item.prior_current_ratio is None,
            ),
        ],
        'improving_gross_margin': [
            ('missing_gross_margin', item.gross_margin is None),
            (
                'missing_prior_gross_margin',
                item.prior_gross_margin is None,
            ),
        ],
        'improving_asset_turnover': [
            ('missing_asset_turnover', item.asset_turnover is None),
            (
                'missing_prior_asset_turnover',
                item.prior_asset_turnover is None,
            ),
        ],
    }
    return {
        signal_name: [
            reason
            for reason, is_missing in candidate_reasons[signal_name]
            if is_missing
        ]
        for signal_name in PIOTROSKI_REQUIRED_FINANCIAL_SIGNAL_NAMES
        if not signals[signal_name].available
    }


class DailyPiotroskiFilterService:
    def rebuild_for_market_date(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
        ranking_symbols: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = date.fromisoformat(market_date) if isinstance(market_date, str) else market_date
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
        default_annual_year = normalized_market_date.year - 1
        default_prior_annual_year = default_annual_year - 1
        ranking_inputs = self._load_inputs(
            normalized_market_date,
            symbols=(
                normalized_ranking_symbols
                if ranking_symbols is not None
                else None
            ),
        )
        target_symbol_set = (
            set(normalized_symbols) if normalized_symbols is not None else None
        )
        pbr_values = [item.pbr for item in ranking_inputs if item.pbr is not None and item.pbr > 0]
        momentum_values = [item.return_20d for item in ranking_inputs if item.return_20d is not None]
        passed_count = 0
        persisted_count = 0
        result_payloads: list[dict[str, Any]] = []
        failed_symbols: list[str] = []
        failed_symbol_errors: list[dict[str, str]] = []
        full_signal_usable_count = 0
        full_signal_incomplete_count = 0
        full_signal_unusable_count = 0
        required_signal_missing_counts: dict[str, int] = {}
        required_input_missing_counts: dict[str, int] = {}

        for item in ranking_inputs:
            annual_year = item.annual_year if item.annual_year is not None else default_annual_year
            prior_annual_year = (
                item.prior_annual_year
                if item.prior_annual_year is not None
                else default_prior_annual_year
            )
            valuation_available = item.pbr is not None and item.pbr > 0
            valuation_source_status = 'available' if valuation_available else 'missing_pbr'
            pbr_rank = _percentile_rank(pbr_values, item.pbr, ascending=True) if item.pbr in pbr_values else None
            momentum_rank = _percentile_rank(momentum_values, item.return_20d, ascending=True) if item.return_20d in momentum_values else None
            signals, score, available_signal_count = _compute_signals(item)
            required_financial_signals_available = sum(
                1
                for signal_name in PIOTROSKI_REQUIRED_FINANCIAL_SIGNAL_NAMES
                if signals[signal_name].available
            )
            missing_required_financial_signal_reasons = (
                _required_financial_signal_missing_reasons(item, signals)
            )
            missing_required_financial_signal_names = list(
                missing_required_financial_signal_reasons
            )
            full_signal_coverage_status = (
                'usable'
                if required_financial_signals_available
                == len(PIOTROSKI_REQUIRED_FINANCIAL_SIGNAL_NAMES)
                else (
                    'unusable'
                    if required_financial_signals_available == 0
                    else 'incomplete'
                )
            )
            missing_optional_signal_names = [
                signal_name
                for signal_name in PIOTROSKI_OPTIONAL_EVENT_SIGNAL_NAMES
                if not signals[signal_name].available
            ]
            is_low_pbr = pbr_rank is not None and pbr_rank <= LOW_PBR_PERCENTILE
            has_profitability_issue = item.net_income is None or item.net_income <= 0
            has_capital_impairment = item.total_equity is None or item.total_equity <= 0
            audit_evidence_status = (
                'unknown'
                if item.has_audit_issue is None
                else ('issue' if item.has_audit_issue else 'clean')
            )
            has_audit_issue = item.has_audit_issue is not False
            profitability_evidence_status = (
                'unavailable'
                if item.net_income is None
                else ('issue' if item.net_income <= 0 else 'clean')
            )
            capital_evidence_status = (
                'unavailable'
                if item.total_equity is None
                else ('issue' if item.total_equity <= 0 else 'clean')
            )
            financial_signal_coverage_status = (
                'complete'
                if required_financial_signals_available
                == len(PIOTROSKI_REQUIRED_FINANCIAL_SIGNAL_NAMES)
                else 'unavailable'
            )
            score_ratio = score / PIOTROSKI_SIGNAL_COUNT
            signal_coverage_ratio = available_signal_count / PIOTROSKI_SIGNAL_COUNT
            passes_piotroski_score = (
                required_financial_signals_available
                == len(PIOTROSKI_REQUIRED_FINANCIAL_SIGNAL_NAMES)
                and score >= PIOTROSKI_PASS_SCORE
            )
            dilution_evidence_available = signals[
                'no_new_share_issuance'
            ].available
            is_bottom_momentum = momentum_rank is not None and momentum_rank <= BOTTOM_MOMENTUM_PERCENTILE
            passed_filter = valuation_available and all([
                is_low_pbr,
                not has_profitability_issue,
                not has_capital_impairment,
                not has_audit_issue,
                passes_piotroski_score,
                dilution_evidence_available,
                not is_bottom_momentum,
            ])
            payload = {
                'symbol': item.symbol,
                'market_date': normalized_market_date.isoformat(),
                'source': FILTER_SOURCE,
                'pbr': item.pbr,
                'valuation_available': valuation_available,
                'valuation_source_status': valuation_source_status,
                'pbr_percentile_rank': pbr_rank,
                'is_low_pbr': is_low_pbr,
                'has_profitability_issue': has_profitability_issue,
                'has_capital_impairment': has_capital_impairment,
                'has_audit_issue': has_audit_issue,
                'piotroski_score': score,
                'passes_piotroski_score': passes_piotroski_score,
                'momentum_percentile_rank': momentum_rank,
                'is_bottom_momentum': is_bottom_momentum,
                'passed_filter': passed_filter,
                'filter_payload': {
                    'filter_name': 'piotroski_filter_d',
                    'name_ko': item.name_ko,
                    'evidence_status': {
                        'profitability': profitability_evidence_status,
                        'capital': capital_evidence_status,
                        'audit': (
                            'unavailable'
                            if audit_evidence_status == 'unknown'
                            else audit_evidence_status
                        ),
                        'financial_signal_coverage': (
                            financial_signal_coverage_status
                        ),
                        'dilution': (
                            'clean'
                            if dilution_evidence_available
                            and item.has_dilution_signal is False
                            else (
                                'issue'
                                if item.has_dilution_signal is True
                                else 'unavailable'
                            )
                        ),
                    },
                    'signals': {key: {'value': result.value, 'available': result.available} for key, result in signals.items()},
                    'score_summary': {
                        'score': score,
                        'available_signal_count': available_signal_count,
                        'score_ratio': score_ratio,
                        'signal_coverage_ratio': signal_coverage_ratio,
                        'full_signal_coverage_required': True,
                        'full_signal_coverage_status': (
                            full_signal_coverage_status
                        ),
                        'required_financial_signal_count': len(
                            PIOTROSKI_REQUIRED_FINANCIAL_SIGNAL_NAMES
                        ),
                        'required_financial_signals_available': (
                            required_financial_signals_available
                        ),
                        'required_financial_signal_coverage_ratio': (
                            required_financial_signals_available
                            / len(PIOTROSKI_REQUIRED_FINANCIAL_SIGNAL_NAMES)
                        ),
                        'missing_required_financial_signal_names': (
                            missing_required_financial_signal_names
                        ),
                        'missing_required_financial_signal_reasons': (
                            missing_required_financial_signal_reasons
                        ),
                        'missing_optional_signal_names': (
                            missing_optional_signal_names
                        ),
                        'normalized_pass_ratio': (
                            PIOTROSKI_PASS_SCORE / PIOTROSKI_SIGNAL_COUNT
                        ),
                    },
                    'thresholds': {
                        'low_pbr_percentile': LOW_PBR_PERCENTILE,
                        'piotroski_min_score': PIOTROSKI_PASS_SCORE,
                        'bottom_momentum_percentile': BOTTOM_MOMENTUM_PERCENTILE,
                        'annual_year': annual_year,
                        'prior_annual_year': prior_annual_year,
                    },
                    'provenance': {
                        'as_of_market_date': normalized_market_date.isoformat(),
                        'universe_policy': 'exact_market_date_daily_price_bar',
                        'fundamentals_selection_policy': (
                            'latest_two_available_annual_years_as_of_market_date; '
                            'latest_filing_per_year; CFS_preferred_within_same_filing_'
                            'with_OFS_fallback'
                        ),
                        'fundamentals_by_business_year': {
                            str(year): dict(metadata)
                            for year, metadata in sorted(item.fundamentals_provenance.items())
                        },
                        'audit_signal': item.audit_provenance,
                        'audit_evidence_status': audit_evidence_status,
                        'dilution_signal': item.dilution_provenance,
                    },
                    'valuation': {
                        'available': valuation_available,
                        'source_status': valuation_source_status,
                        'source': item.valuation_source,
                        'provenance': item.valuation_provenance,
                        'excluded_from_filter_universe': not valuation_available,
                    },
                    'inputs': {
                        'return_20d': item.return_20d,
                        'total_equity': item.total_equity,
                        'net_income': item.net_income,
                        'operating_cash_flow': item.operating_cash_flow,
                        'total_assets': item.total_assets,
                        'prior_total_assets': item.prior_total_assets,
                        'debt_ratio': item.debt_ratio,
                        'prior_debt_ratio': item.prior_debt_ratio,
                        'current_ratio': item.current_ratio,
                        'prior_current_ratio': item.prior_current_ratio,
                        'gross_margin': item.gross_margin,
                        'prior_gross_margin': item.prior_gross_margin,
                        'asset_turnover': item.asset_turnover,
                        'prior_asset_turnover': item.prior_asset_turnover,
                        'has_dilution_signal': item.has_dilution_signal,
                    },
                },
            }
            if target_symbol_set is not None and item.symbol not in target_symbol_set:
                continue
            if full_signal_coverage_status == 'usable':
                full_signal_usable_count += 1
            elif full_signal_coverage_status == 'incomplete':
                full_signal_incomplete_count += 1
            else:
                full_signal_unusable_count += 1
            missing_input_reasons: set[str] = set()
            for signal_name, reasons in (
                missing_required_financial_signal_reasons.items()
            ):
                required_signal_missing_counts[signal_name] = (
                    required_signal_missing_counts.get(signal_name, 0) + 1
                )
                missing_input_reasons.update(reasons)
            for reason in missing_input_reasons:
                required_input_missing_counts[reason] = (
                    required_input_missing_counts.get(reason, 0) + 1
                )
            if passed_filter:
                passed_count += 1
            result_payloads.append(payload)

        if result_payloads:
            try:
                upsert_daily_piotroski_filter_results(result_payloads)
                persisted_count = len(result_payloads)
            except Exception:
                for payload in result_payloads:
                    try:
                        upsert_daily_piotroski_filter_result(payload)
                        persisted_count += 1
                    except Exception as exc:
                        symbol = str(payload['symbol'])
                        failed_symbols.append(symbol)
                        failed_symbol_errors.append({'symbol': symbol, 'error': str(exc)})

        evaluated_symbols = {
            str(payload['symbol']) for payload in result_payloads
        }
        selected_scope_symbols = (
            list(normalized_symbols)
            if normalized_symbols is not None
            else sorted(evaluated_symbols)
        )
        missing_input_symbols = sorted(
            set(selected_scope_symbols) - evaluated_symbols
        )
        if missing_input_symbols:
            missing_input_count = len(missing_input_symbols)
            full_signal_unusable_count += missing_input_count
            for signal_name in PIOTROSKI_REQUIRED_FINANCIAL_SIGNAL_NAMES:
                required_signal_missing_counts[signal_name] = (
                    required_signal_missing_counts.get(signal_name, 0)
                    + missing_input_count
                )
            required_input_missing_counts['missing_piotroski_input_row'] = (
                required_input_missing_counts.get(
                    'missing_piotroski_input_row',
                    0,
                )
                + missing_input_count
            )
        successful_symbols = sorted(
            evaluated_symbols - set(failed_symbols)
        )
        retry_debt_symbols = sorted(
            set(selected_scope_symbols) - set(successful_symbols)
        )
        scope_status = (
            'completed'
            if not retry_debt_symbols
            else ('partial' if successful_symbols else 'failed')
        )
        full_signal_total_count = len(selected_scope_symbols)
        full_signal_coverage_ratio = (
            full_signal_usable_count / full_signal_total_count
            if full_signal_total_count > 0
            else 0.0
        )
        full_signal_coverage_status = (
            'usable'
            if full_signal_total_count > 0
            and full_signal_usable_count == full_signal_total_count
            else (
                'unusable'
                if full_signal_usable_count == 0
                else 'incomplete'
            )
        )
        status = scope_status
        evidence_reason: str | None = None
        if scope_status != 'failed':
            if full_signal_coverage_status == 'unusable':
                status = 'partial'
                evidence_reason = 'piotroski_evidence_unusable'
            elif full_signal_coverage_status == 'incomplete':
                status = 'partial'
                evidence_reason = 'piotroski_evidence_incomplete'

        result = {
            'status': status,
            'market_date': normalized_market_date.isoformat(),
            'source': FILTER_SOURCE,
            'requested_symbol_count': full_signal_total_count,
            'symbols_evaluated': len(result_payloads),
            'symbols_excluded_missing_valuation': sum(
                1
                for item in ranking_inputs
                if (target_symbol_set is None or item.symbol in target_symbol_set)
                and (item.pbr is None or item.pbr <= 0)
            ),
            'symbols_passed': passed_count,
            'symbols_not_passed': len(result_payloads) - passed_count,
            'results_persisted': persisted_count,
            'results_failed': len(failed_symbols),
            'failed_symbols': failed_symbols,
            'failed_symbol_errors': failed_symbol_errors,
            'full_signal_required_signal_count': len(
                PIOTROSKI_REQUIRED_FINANCIAL_SIGNAL_NAMES
            ),
            'full_signal_usable_count': full_signal_usable_count,
            'full_signal_incomplete_count': full_signal_incomplete_count,
            'full_signal_unusable_count': full_signal_unusable_count,
            'full_signal_coverage_ratio': full_signal_coverage_ratio,
            'full_signal_coverage_status': full_signal_coverage_status,
            'required_financial_signal_missing_counts': {
                signal_name: required_signal_missing_counts[signal_name]
                for signal_name in PIOTROSKI_REQUIRED_FINANCIAL_SIGNAL_NAMES
                if signal_name in required_signal_missing_counts
            },
            'required_financial_input_missing_counts': dict(
                sorted(required_input_missing_counts.items())
            ),
            'scope_outcome_manifest_version': (
                'piotroski_scope_outcomes_v1'
            ),
            'selected_stock_codes': selected_scope_symbols,
            'successful_stock_codes': successful_symbols,
            'retry_debt_stock_codes': retry_debt_symbols,
            'scope_outcome_manifest': [
                {
                    'stock_code': symbol,
                    'outcome': (
                        'success'
                        if symbol in successful_symbols
                        else 'retry_debt'
                    ),
                }
                for symbol in selected_scope_symbols
            ],
        }
        if evidence_reason is not None:
            result['reason'] = evidence_reason
            result['quality_warning'] = evidence_reason
        return result

    def inspect(
        self,
        *,
        market_date: str | date | None = None,
        symbol: str | None = None,
        limit: int | None = None,
        summary_only: bool = False,
    ) -> dict[str, Any]:
        normalized_requested_market_date = self._normalize_market_date(market_date)
        normalized_symbol = self._normalize_symbol(symbol)
        requested_symbols = [normalized_symbol] if normalized_symbol else []
        resolved_market_date = load_latest_available_daily_piotroski_filter_result_date(
            market_date=normalized_requested_market_date,
            symbols=requested_symbols or None,
        )
        items = (
            load_daily_piotroski_filter_results(
                market_date=resolved_market_date,
                symbols=requested_symbols or None,
                limit=(1 if normalized_symbol else limit),
            )
            if resolved_market_date is not None
            else []
        )
        matched_symbols = [item['symbol'] for item in items]
        matched_symbol_set = set(matched_symbols)
        response_matched_symbols = [] if summary_only and not normalized_symbol else matched_symbols
        requested_market_date = (
            normalized_requested_market_date.isoformat() if normalized_requested_market_date is not None else None
        )
        is_latest_fallback = (
            requested_market_date is not None
            and resolved_market_date is not None
            and resolved_market_date != requested_market_date
        )
        summary = {
            'entries': len(items),
            'requested_symbol': normalized_symbol,
            'matched_symbols': response_matched_symbols,
            'missing_symbols': [candidate for candidate in requested_symbols if candidate not in matched_symbol_set],
            'passed_count': sum(1 for item in items if item.get('passed_filter')),
            'not_passed_count': sum(1 for item in items if not item.get('passed_filter')),
        }
        payload: dict[str, Any] = {
            'requested_market_date': requested_market_date,
            'resolved_market_date': resolved_market_date,
            'market_date': resolved_market_date,
            'latest_mode': normalized_requested_market_date is None,
            'is_latest_fallback': is_latest_fallback,
            'fallback_policy': LATEST_FALLBACK_POLICY,
            'summary': summary,
        }
        if normalized_symbol:
            payload['item'] = items[0] if items else None
        elif summary_only:
            payload['items'] = []
        else:
            payload['items'] = items
        return payload

    @staticmethod
    def _normalize_market_date(market_date: str | date | None) -> date | None:
        if market_date is None:
            return None
        if isinstance(market_date, date):
            return market_date
        normalized = market_date.strip()
        if not normalized:
            raise ValueError('market_date must not be blank')
        return date.fromisoformat(normalized)

    @staticmethod
    def _normalize_symbol(symbol: str | None) -> str | None:
        if symbol is None:
            return None
        normalized = symbol.strip().upper()
        if not normalized:
            raise ValueError('symbol must not be blank')
        if normalized.isdigit():
            normalized = normalized.zfill(6)
        return normalized

    def _load_inputs(
        self,
        market_date: date,
        *,
        symbols: Sequence[str] | None = None,
    ) -> list[PiotroskiInputs]:
        with session_scope() as session:
            base_query = (
                select(
                    Symbol.symbol,
                    Symbol.name_ko,
                    DailyPriceBar.pbr,
                    DailyTechnicalSnapshot.return_20d,
                )
                .join(DailyPriceBar, and_(DailyPriceBar.symbol_id == Symbol.id, DailyPriceBar.trade_date == market_date))
                .join(
                    DailyTechnicalSnapshot,
                    and_(
                        DailyTechnicalSnapshot.symbol_id == Symbol.id,
                        DailyTechnicalSnapshot.market_date == market_date,
                    ),
                    isouter=True,
                )
                .order_by(Symbol.symbol.asc())
            )
            if symbols is not None:
                base_query = base_query.where(Symbol.symbol.in_(list(symbols)))
            base_rows = session.execute(base_query).all()

            symbols = [row.symbol for row in base_rows]
            if not symbols:
                return []
            fundamentals_issuer_by_symbol = (
                resolve_preferred_fundamentals_issuer_symbols(
                    session,
                    symbols,
                )
            )
            fundamentals_query_symbols = list(
                dict.fromkeys(
                    [
                        *symbols,
                        *fundamentals_issuer_by_symbol.values(),
                    ]
                )
            )

            valuation_pbr_by_symbol: dict[str, float] = {}
            valuation_market_cap_by_symbol: dict[str, float] = {}
            valuation_rows = session.execute(
                select(
                    Symbol.symbol,
                    DailyPykrxEnrichmentRow.payload,
                )
                .join(
                    DailyPykrxEnrichmentRow,
                    DailyPykrxEnrichmentRow.symbol_id == Symbol.id,
                )
                .where(
                    Symbol.symbol.in_(symbols),
                    DailyPykrxEnrichmentRow.market_date == market_date,
                    DailyPykrxEnrichmentRow.category == 'valuation_snapshot',
                    DailyPykrxEnrichmentRow.scope_type == 'symbol',
                    DailyPykrxEnrichmentRow.source == 'pykrx',
                )
            ).all()
            for valuation_row in valuation_rows:
                payload = dict(valuation_row.payload or {})
                pbr = _as_float(payload.get('pbr'))
                if pbr is not None and pbr > 0:
                    valuation_pbr_by_symbol[str(valuation_row.symbol)] = pbr
                market_cap = _as_float(payload.get('market_cap'))
                if market_cap is not None and market_cap > 0:
                    valuation_market_cap_by_symbol[
                        str(valuation_row.symbol)
                    ] = market_cap

            metric_keys = {
                'total_equity',
                'net_income',
                'operating_cash_flow',
                'total_assets',
                'total_liabilities',
                'current_assets',
                'current_liabilities',
                'gross_profit',
                'revenue',
                CANONICAL_DEBT_RATIO,
                CANONICAL_CURRENT_RATIO,
                CANONICAL_GROSS_MARGIN,
                CANONICAL_ASSET_TURNOVER,
                INDEX_DEBT_RATIO,
                INDEX_CURRENT_RATIO,
                INDEX_GROSS_MARGIN,
                INDEX_ASSET_TURNOVER,
            }
            highlights_by_symbol: dict[str, dict[tuple[str, int], Any]] = {}
            ratios_by_symbol: dict[str, dict[tuple[str, int], Any]] = {}
            highlight_fact_provenance_by_symbol: dict[
                str,
                dict[tuple[str, int], dict[str, Any]],
            ] = {}
            fundamentals_provenance_by_symbol: dict[str, dict[int, dict[str, Any]]] = {}
            for symbol_chunk in _chunks(
                fundamentals_query_symbols,
                PIOTROSKI_INPUT_QUERY_CHUNK_SIZE,
            ):
                latest_rank = func.row_number().over(
                    partition_by=(
                        DartFundamentalsSnapshot.stock_code,
                        DartFundamentalsSnapshot.business_year,
                    ),
                    order_by=(
                        DartDisclosureEvent.receipt_date.desc(),
                        DartDisclosureEvent.rcept_no.desc(),
                        case(
                            (DartFundamentalsSnapshot.fs_div == 'CFS', 0),
                            else_=1,
                        ).asc(),
                        DartFundamentalsSnapshot.id.desc(),
                    ),
                ).label('row_rank')
                ranked_snapshots = (
                    select(
                        DartFundamentalsSnapshot.id.label('snapshot_id'),
                        DartFundamentalsSnapshot.disclosure_event_id.label('disclosure_event_id'),
                        DartFundamentalsSnapshot.stock_code.label('symbol'),
                        DartFundamentalsSnapshot.business_year.label('business_year'),
                        DartFundamentalsSnapshot.report_code.label('report_code'),
                        DartFundamentalsSnapshot.fs_div.label('fs_div'),
                        DartFundamentalsSnapshot.rcept_no.label('rcept_no'),
                        DartFundamentalsSnapshot.captured_at.label('captured_at'),
                        DartFundamentalsSnapshot.currency.label('currency'),
                        DartDisclosureEvent.corp_code.label('corp_code'),
                        DartDisclosureEvent.receipt_date.label('receipt_date'),
                        latest_rank,
                    )
                    .join(
                        DartDisclosureEvent,
                        DartDisclosureEvent.id == DartFundamentalsSnapshot.disclosure_event_id,
                    )
                    .where(
                        DartFundamentalsSnapshot.stock_code.in_(symbol_chunk),
                        DartFundamentalsSnapshot.business_year <= market_date.year,
                        DartFundamentalsSnapshot.report_code == '11011',
                        DartFundamentalsSnapshot.fs_div.in_(['CFS', 'OFS']),
                        DartFundamentalsSnapshot.source_status == '000',
                        DartDisclosureEvent.fundamentals_capture_status == 'captured',
                        DartDisclosureEvent.receipt_date.is_not(None),
                        DartDisclosureEvent.receipt_date <= market_date,
                    )
                    .subquery()
                )
                selected_snapshots = session.execute(
                    select(ranked_snapshots).where(ranked_snapshots.c.row_rank == 1)
                ).mappings().all()
                latest_two_by_symbol: dict[str, list[Any]] = {}
                for row in selected_snapshots:
                    latest_two_by_symbol.setdefault(str(row['symbol']), []).append(row)
                selected_snapshots = [
                    row
                    for rows in latest_two_by_symbol.values()
                    for row in sorted(
                        rows,
                        key=lambda candidate: int(candidate['business_year']),
                        reverse=True,
                    )[:2]
                ]
                snapshot_metadata_by_id = {
                    int(row['snapshot_id']): dict(row)
                    for row in selected_snapshots
                }
                snapshot_ids = list(snapshot_metadata_by_id)
                if not snapshot_ids:
                    continue
                stored_highlights_by_snapshot: dict[int, dict[str, Any]] = {}
                stored_ratios_by_snapshot: dict[int, dict[str, Any]] = {}
                highlight_rows = session.execute(
                    select(
                        DartFundamentalsHighlight.id.label('highlight_id'),
                        DartFundamentalsHighlight.snapshot_id.label('snapshot_id'),
                        DartFundamentalsHighlight.metric_key.label('metric_key'),
                        DartFundamentalsHighlight.current_amount.label('current_amount'),
                        DartFundamentalsHighlight.current_cumulative_amount.label(
                            'current_cumulative_amount'
                        ),
                        DartFundamentalsHighlight.ratio_value.label('ratio_value'),
                        DartFundamentalsHighlight.currency.label('currency'),
                    )
                    .where(
                        DartFundamentalsHighlight.snapshot_id.in_(snapshot_ids),
                        DartFundamentalsHighlight.metric_key.in_(metric_keys),
                    )
                ).mappings().all()
                for row in highlight_rows:
                    snapshot_id = int(row['snapshot_id'])
                    metadata = snapshot_metadata_by_id[snapshot_id]
                    symbol = str(metadata['symbol'])
                    business_year = int(metadata['business_year'])
                    current_amount = row['current_amount']
                    amount_field = 'current_amount'
                    if current_amount is None:
                        current_amount = row['current_cumulative_amount']
                        amount_field = 'current_cumulative_amount'
                    stored_highlights_by_snapshot.setdefault(snapshot_id, {})[
                        str(row['metric_key'])
                    ] = current_amount
                    stored_ratios_by_snapshot.setdefault(snapshot_id, {})[
                        str(row['metric_key'])
                    ] = row['ratio_value']
                    highlights_by_symbol.setdefault(symbol, {})[
                        (row['metric_key'], business_year)
                    ] = current_amount
                    if current_amount is not None:
                        highlight_fact_provenance_by_symbol.setdefault(
                            symbol,
                            {},
                        )[(str(row['metric_key']), business_year)] = {
                            'source': 'dart_fundamentals_highlight',
                            'highlight_id': int(row['highlight_id']),
                            'snapshot_id': snapshot_id,
                            'metric_key': str(row['metric_key']),
                            'amount_field': amount_field,
                            'currency': str(row['currency'] or '').strip().upper(),
                        }
                    ratios_by_symbol.setdefault(symbol, {})[
                        (row['metric_key'], business_year)
                    ] = row['ratio_value']

                raw_fallback_snapshot_ids: list[int] = []
                for snapshot_id in snapshot_ids:
                    stored_highlights = stored_highlights_by_snapshot.get(snapshot_id, {})
                    stored_ratios = stored_ratios_by_snapshot.get(snapshot_id, {})
                    required_raw_fact_missing = any(
                        stored_highlights.get(metric_key) is None
                        for metric_key in PIOTROSKI_RAW_FALLBACK_FACT_KEYS
                    )
                    current_ratio_missing = all(
                        stored_ratios.get(metric_key) is None
                        for metric_key in (CANONICAL_CURRENT_RATIO, INDEX_CURRENT_RATIO)
                    ) and any(
                        stored_highlights.get(metric_key) is None
                        for metric_key in ('current_assets', 'current_liabilities')
                    )
                    gross_margin_missing = all(
                        stored_ratios.get(metric_key) is None
                        for metric_key in (CANONICAL_GROSS_MARGIN, INDEX_GROSS_MARGIN)
                    ) and any(
                        stored_highlights.get(metric_key) is None
                        for metric_key in ('gross_profit', 'revenue')
                    )
                    asset_turnover_missing = all(
                        stored_ratios.get(metric_key) is None
                        for metric_key in (
                            CANONICAL_ASSET_TURNOVER,
                            INDEX_ASSET_TURNOVER,
                        )
                    ) and any(
                        stored_highlights.get(metric_key) is None
                        for metric_key in ('revenue', 'total_assets')
                    )
                    if (
                        required_raw_fact_missing
                        or current_ratio_missing
                        or gross_margin_missing
                        or asset_turnover_missing
                    ):
                        raw_fallback_snapshot_ids.append(snapshot_id)

                raw_rows_by_snapshot: dict[int, list[dict[str, Any]]] = {}
                for start in range(
                    0,
                    len(raw_fallback_snapshot_ids),
                    PIOTROSKI_INPUT_QUERY_CHUNK_SIZE,
                ):
                    raw_snapshot_chunk = raw_fallback_snapshot_ids[
                        start:start + PIOTROSKI_INPUT_QUERY_CHUNK_SIZE
                    ]
                    raw_rows = session.execute(
                        select(
                            DartFundamentalsRow.snapshot_id.label('snapshot_id'),
                            DartFundamentalsRow.statement_type.label('statement_type'),
                            DartFundamentalsRow.statement_name.label('statement_name'),
                            DartFundamentalsRow.account_id.label('account_id'),
                            DartFundamentalsRow.account_name.label('account_name'),
                            DartFundamentalsRow.account_detail.label('account_detail'),
                            DartFundamentalsRow.current_amount.label('current_amount'),
                            DartFundamentalsRow.current_cumulative_amount.label(
                                'current_cumulative_amount'
                            ),
                            DartFundamentalsRow.prior_amount.label('prior_amount'),
                            DartFundamentalsRow.prior_cumulative_amount.label(
                                'prior_cumulative_amount'
                            ),
                            DartFundamentalsRow.pre_prior_amount.label(
                                'pre_prior_amount'
                            ),
                            DartFundamentalsRow.row_order.label('order'),
                            DartFundamentalsRow.currency.label('currency'),
                        ).where(
                            DartFundamentalsRow.snapshot_id.in_(raw_snapshot_chunk),
                        )
                    ).mappings().all()
                    for raw_row in raw_rows:
                        snapshot_id = int(raw_row['snapshot_id'])
                        raw_rows_by_snapshot.setdefault(snapshot_id, []).append(
                            dict(raw_row)
                        )

                raw_fallback_metrics_by_snapshot: dict[int, list[str]] = {}
                raw_derived_ratios_by_snapshot: dict[int, list[str]] = {}
                for snapshot_id, raw_rows in raw_rows_by_snapshot.items():
                    metadata = snapshot_metadata_by_id[snapshot_id]
                    symbol = str(metadata['symbol'])
                    business_year = int(metadata['business_year'])
                    derived_highlights = build_highlights_from_normalized_items(raw_rows)
                    fallback_metric_names: list[str] = []
                    for metric_key in PIOTROSKI_RAW_FALLBACK_FACT_KEYS:
                        mapped_key = (metric_key, business_year)
                        existing_value = highlights_by_symbol.setdefault(symbol, {}).get(
                            mapped_key
                        )
                        derived_value = _current_highlight_value(
                            derived_highlights.get(metric_key)
                        )
                        if existing_value is None and derived_value is not None:
                            highlights_by_symbol[symbol][mapped_key] = derived_value
                            fallback_metric_names.append(metric_key)
                    derived_ratios = derived_highlights.get('ratios') or {}
                    derived_ratio_names: list[str] = []
                    for ratio_key in (CANONICAL_CURRENT_RATIO, CANONICAL_GROSS_MARGIN):
                        mapped_key = (ratio_key, business_year)
                        if (
                            ratios_by_symbol.setdefault(symbol, {}).get(mapped_key)
                            is not None
                        ):
                            continue
                        derived_ratio = _as_float(derived_ratios.get(ratio_key))
                        if derived_ratio is not None:
                            ratios_by_symbol[symbol][mapped_key] = derived_ratio
                            derived_ratio_names.append(ratio_key)
                    if fallback_metric_names:
                        raw_fallback_metrics_by_snapshot[snapshot_id] = sorted(
                            fallback_metric_names
                        )
                    if derived_ratio_names:
                        raw_derived_ratios_by_snapshot[snapshot_id] = sorted(
                            derived_ratio_names
                        )
                for metadata in snapshot_metadata_by_id.values():
                    symbol = str(metadata['symbol'])
                    business_year = int(metadata['business_year'])
                    provenance = {
                        'requested_symbol': symbol,
                        'fundamentals_issuer_symbol': symbol,
                        'snapshot_id': int(metadata['snapshot_id']),
                        'disclosure_event_id': int(metadata['disclosure_event_id']),
                        'business_year': business_year,
                        'report_code': str(metadata['report_code']),
                        'fs_div': str(metadata['fs_div']),
                        'rcept_no': str(metadata['rcept_no']),
                        'corp_code': str(metadata['corp_code']),
                        'receipt_date': metadata['receipt_date'].isoformat(),
                        'captured_at': (
                            metadata['captured_at'].isoformat()
                            if metadata['captured_at'] is not None
                            else None
                        ),
                        'currency': str(metadata['currency'] or '').strip().upper(),
                    }
                    raw_fallback_metrics = raw_fallback_metrics_by_snapshot.get(
                        int(metadata['snapshot_id'])
                    )
                    if raw_fallback_metrics:
                        provenance['raw_row_fallback_metrics'] = raw_fallback_metrics
                    raw_derived_ratios = raw_derived_ratios_by_snapshot.get(
                        int(metadata['snapshot_id'])
                    )
                    if raw_derived_ratios:
                        provenance['raw_row_derived_ratios'] = raw_derived_ratios
                    fundamentals_provenance_by_symbol.setdefault(symbol, {})[
                        business_year
                    ] = provenance

            for requested_symbol, issuer_symbol in (
                fundamentals_issuer_by_symbol.items()
            ):
                if fundamentals_provenance_by_symbol.get(requested_symbol):
                    continue
                issuer_provenance = fundamentals_provenance_by_symbol.get(
                    issuer_symbol
                )
                if not issuer_provenance:
                    continue
                highlights_by_symbol[requested_symbol] = dict(
                    highlights_by_symbol.get(issuer_symbol, {})
                )
                ratios_by_symbol[requested_symbol] = dict(
                    ratios_by_symbol.get(issuer_symbol, {})
                )
                issuer_fact_provenance = (
                    highlight_fact_provenance_by_symbol.get(issuer_symbol, {})
                )
                highlight_fact_provenance_by_symbol[requested_symbol] = {
                    key: {
                        **metadata,
                        'requested_symbol': requested_symbol,
                        'fundamentals_issuer_symbol': issuer_symbol,
                    }
                    for key, metadata in issuer_fact_provenance.items()
                }
                fundamentals_provenance_by_symbol[requested_symbol] = {
                    business_year: {
                        **metadata,
                        'requested_symbol': requested_symbol,
                        'fundamentals_issuer_symbol': issuer_symbol,
                    }
                    for business_year, metadata in issuer_provenance.items()
                }

            expected_audit_targets_by_rcept: dict[
                str,
                list[tuple[str, str]],
            ] = {}
            for symbol in symbols:
                provenance = fundamentals_provenance_by_symbol.get(symbol, {})
                if not provenance:
                    continue
                selected_year = max(provenance)
                selected_provenance = provenance[selected_year]
                rcept_no = str(
                    selected_provenance.get('rcept_no') or ''
                ).strip()
                if rcept_no:
                    issuer_symbol = str(
                        selected_provenance.get(
                            'fundamentals_issuer_symbol',
                            symbol,
                        )
                    )
                    expected_audit_targets_by_rcept.setdefault(
                        rcept_no,
                        [],
                    ).append((symbol, issuer_symbol))

            latest_audit: dict[str, Any] = {}
            audit_provenance_by_symbol: dict[str, dict[str, Any]] = {}
            for rcept_chunk in _chunks(
                list(expected_audit_targets_by_rcept),
                PIOTROSKI_INPUT_QUERY_CHUNK_SIZE,
            ):
                audit_rows = session.execute(
                    select(
                        DartAuditSignal.id.label('id'),
                        DartAuditSignal.stock_code.label('symbol'),
                        DartAuditSignal.rcept_no.label('rcept_no'),
                        DartAuditSignal.receipt_date.label('receipt_date'),
                        DartAuditSignal.has_audit_issue.label('has_audit_issue'),
                    ).where(
                        DartAuditSignal.rcept_no.in_(rcept_chunk),
                        DartAuditSignal.receipt_date.is_not(None),
                        DartAuditSignal.receipt_date <= market_date,
                    )
                ).mappings().all()
                for row in audit_rows:
                    rcept_no = str(row['rcept_no'])
                    for requested_symbol, issuer_symbol in (
                        expected_audit_targets_by_rcept.get(rcept_no, [])
                    ):
                        if str(row['symbol']) != issuer_symbol:
                            continue
                        latest_audit[requested_symbol] = row['has_audit_issue']
                        audit_provenance_by_symbol[requested_symbol] = {
                            'requested_symbol': requested_symbol,
                            'fundamentals_issuer_symbol': issuer_symbol,
                            'id': int(row['id']),
                            'rcept_no': rcept_no,
                            'receipt_date': row['receipt_date'].isoformat(),
                        }

            latest_dilution: dict[str, Any] = {}
            dilution_provenance_by_symbol: dict[str, dict[str, Any]] = {}
            current_corp_code_by_symbol = {
                symbol: str(provenance[max(provenance)]['corp_code'])
                for symbol, provenance in fundamentals_provenance_by_symbol.items()
                if provenance
            }
            current_fundamentals_issuer_by_symbol = {
                symbol: str(
                    provenance[max(provenance)].get(
                        'fundamentals_issuer_symbol',
                        symbol,
                    )
                )
                for symbol, provenance in fundamentals_provenance_by_symbol.items()
                if provenance
            }
            dilution_window_start = market_date - timedelta(
                days=DILUTION_NEGATIVE_COVERAGE_DAYS
            )
            for symbol_chunk in _chunks(symbols, PIOTROSKI_INPUT_QUERY_CHUNK_SIZE):
                expected_corp_codes = {
                    current_corp_code_by_symbol[symbol]
                    for symbol in symbol_chunk
                    if current_corp_code_by_symbol.get(symbol)
                }
                dilution_rows = session.execute(
                    select(
                        DartDilutionSignal.id.label('id'),
                        DartDilutionSignal.stock_code.label('symbol'),
                        DartDilutionSignal.corp_code.label('corp_code'),
                        DartDilutionSignal.rcept_no.label('rcept_no'),
                        DartDilutionSignal.receipt_date.label('receipt_date'),
                        DartDilutionSignal.has_dilution_signal.label('has_dilution_signal'),
                        DartDilutionSignal.payload.label('payload'),
                    )
                    .where(
                        DartDilutionSignal.corp_code.in_(expected_corp_codes),
                        DartDilutionSignal.signal_type.in_(
                            DILUTION_EVIDENCE_SIGNAL_TYPES
                        ),
                        DartDilutionSignal.receipt_date.is_not(None),
                        DartDilutionSignal.receipt_date >= dilution_window_start,
                        DartDilutionSignal.receipt_date <= market_date,
                    )
                    .order_by(
                        DartDilutionSignal.stock_code.asc(),
                        DartDilutionSignal.receipt_date.desc(),
                        DartDilutionSignal.id.desc(),
                    )
                ).mappings().all()
                dilution_rows_by_corp: dict[str, list[Any]] = {}
                for row in dilution_rows:
                    dilution_rows_by_corp.setdefault(
                        str(row['corp_code']), []
                    ).append(row)
                for symbol in symbol_chunk:
                    selected_row: Any | None = None
                    selected_coverage: dict[str, Any] | None = None
                    expected_corp_code = current_corp_code_by_symbol.get(symbol, '')
                    symbol_dilution_rows = dilution_rows_by_corp.get(
                        expected_corp_code, []
                    )
                    positive_rows = [
                        row
                        for row in symbol_dilution_rows
                        if bool(row['has_dilution_signal'])
                        and str(row['corp_code']) == expected_corp_code
                    ]
                    if positive_rows:
                        selected_row = positive_rows[0]
                    else:
                        negative_rows = [
                            row
                            for row in symbol_dilution_rows
                            if not bool(row['has_dilution_signal'])
                            and str(row['corp_code']) == expected_corp_code
                        ]
                        authority_attempt_rows = [
                            row
                            for row in negative_rows
                            if str(row['rcept_no']).startswith('DILATT-')
                            or (
                                isinstance(row['payload'], dict)
                                and row['payload'].get('evidence_semantics')
                                in {
                                    'completed_scan_attempt',
                                    'incomplete_scan_attempt',
                                }
                            )
                        ]
                        latest_attempt = (
                            authority_attempt_rows[0]
                            if authority_attempt_rows
                            else (negative_rows[0] if negative_rows else None)
                        )
                        if latest_attempt is not None:
                            selected_coverage = _validated_negative_dilution_coverage(
                                latest_attempt['payload'],
                                market_date=market_date,
                                expected_corp_code=expected_corp_code,
                                row_corp_code=str(latest_attempt['corp_code']),
                            )
                            if selected_coverage is not None:
                                selected_row = latest_attempt
                    if selected_row is None:
                        continue
                    has_dilution_signal = bool(selected_row['has_dilution_signal'])
                    latest_dilution[symbol] = has_dilution_signal
                    dilution_provenance = {
                        'requested_symbol': symbol,
                        'fundamentals_issuer_symbol': (
                            current_fundamentals_issuer_by_symbol.get(
                                symbol,
                                symbol,
                            )
                        ),
                        'id': int(selected_row['id']),
                        'signal_stock_code': str(selected_row['symbol']),
                        'corp_code': str(selected_row['corp_code']),
                        'rcept_no': str(selected_row['rcept_no']),
                        'receipt_date': selected_row['receipt_date'].isoformat(),
                        'evidence_status': (
                            'positive_event'
                            if has_dilution_signal
                            else 'complete_negative_coverage'
                        ),
                    }
                    if selected_coverage is not None:
                        dilution_provenance['evidence_coverage'] = selected_coverage
                    dilution_provenance_by_symbol[symbol] = dilution_provenance

        inputs: list[PiotroskiInputs] = []
        for row in base_rows:
            symbol = str(row.symbol)
            provenance = fundamentals_provenance_by_symbol.get(symbol, {})
            available_years = sorted(provenance, reverse=True)
            selected_annual_year = available_years[0] if available_years else None
            selected_prior_annual_year = available_years[1] if len(available_years) > 1 else None
            total_equity = _mapped_float(
                highlights_by_symbol,
                symbol=symbol,
                business_year=selected_annual_year,
                metric_keys=('total_equity',),
            )
            valuation_provenance: dict[str, Any] | None = None
            if symbol in valuation_pbr_by_symbol:
                pbr = valuation_pbr_by_symbol[symbol]
                valuation_source = 'pykrx_valuation_snapshot'
            else:
                daily_price_pbr = _as_float(row.pbr)
                if daily_price_pbr is not None and daily_price_pbr > 0:
                    pbr = daily_price_pbr
                    valuation_source = 'daily_price_bars'
                else:
                    total_equity_fact = highlight_fact_provenance_by_symbol.get(
                        symbol,
                        {},
                    ).get(('total_equity', selected_annual_year or -1))
                    market_cap = valuation_market_cap_by_symbol.get(symbol)
                    currency = str(
                        (total_equity_fact or {}).get('currency') or ''
                    ).upper()
                    if (
                        market_cap is not None
                        and market_cap > 0
                        and total_equity is not None
                        and total_equity > 0
                        and total_equity_fact is not None
                        and currency == 'KRW'
                    ):
                        pbr = market_cap / total_equity
                        valuation_source = 'dart_annual_equity_market_cap_derived'
                        valuation_provenance = {
                            'source': valuation_source,
                            'formula': 'same_day_market_cap / annual_total_equity',
                            'market_cap': market_cap,
                            'total_equity': total_equity,
                            'currency': currency,
                            'business_year': selected_annual_year,
                            'market_date': market_date.isoformat(),
                            'market_cap_source': 'pykrx_valuation_snapshot',
                            'total_equity_source': 'dart_annual_fundamentals',
                            'fundamentals_snapshot_id': total_equity_fact.get(
                                'snapshot_id'
                            ),
                            'total_equity_fact': dict(total_equity_fact),
                        }
                    else:
                        pbr = None
                        valuation_source = 'unavailable'
            ratio_arguments = {
                'symbol': symbol,
                'ratios_by_symbol': ratios_by_symbol,
                'highlights_by_symbol': highlights_by_symbol,
            }
            inputs.append(
                PiotroskiInputs(
                    symbol=symbol,
                    name_ko=row.name_ko,
                    market_date=market_date,
                    pbr=pbr,
                    return_20d=_as_float(row.return_20d),
                    total_equity=total_equity,
                    net_income=_mapped_float(
                        highlights_by_symbol,
                        symbol=symbol,
                        business_year=selected_annual_year,
                        metric_keys=('net_income',),
                    ),
                    prior_net_income=_mapped_float(
                        highlights_by_symbol,
                        symbol=symbol,
                        business_year=selected_prior_annual_year,
                        metric_keys=('net_income',),
                    ),
                    operating_cash_flow=_mapped_float(
                        highlights_by_symbol,
                        symbol=symbol,
                        business_year=selected_annual_year,
                        metric_keys=('operating_cash_flow',),
                    ),
                    total_assets=_mapped_float(
                        highlights_by_symbol,
                        symbol=symbol,
                        business_year=selected_annual_year,
                        metric_keys=('total_assets',),
                    ),
                    prior_total_assets=_mapped_float(
                        highlights_by_symbol,
                        symbol=symbol,
                        business_year=selected_prior_annual_year,
                        metric_keys=('total_assets',),
                    ),
                    debt_ratio=_ratio_or_derived(
                        **ratio_arguments,
                        business_year=selected_annual_year,
                        ratio_metric_keys=(CANONICAL_DEBT_RATIO, INDEX_DEBT_RATIO),
                        numerator_metric_key='total_liabilities',
                        denominator_metric_key='total_equity',
                        require_positive_denominator=True,
                    ),
                    prior_debt_ratio=_ratio_or_derived(
                        **ratio_arguments,
                        business_year=selected_prior_annual_year,
                        ratio_metric_keys=(CANONICAL_DEBT_RATIO, INDEX_DEBT_RATIO),
                        numerator_metric_key='total_liabilities',
                        denominator_metric_key='total_equity',
                        require_positive_denominator=True,
                    ),
                    current_ratio=_ratio_or_derived(
                        **ratio_arguments,
                        business_year=selected_annual_year,
                        ratio_metric_keys=(CANONICAL_CURRENT_RATIO, INDEX_CURRENT_RATIO),
                        numerator_metric_key='current_assets',
                        denominator_metric_key='current_liabilities',
                    ),
                    prior_current_ratio=_ratio_or_derived(
                        **ratio_arguments,
                        business_year=selected_prior_annual_year,
                        ratio_metric_keys=(CANONICAL_CURRENT_RATIO, INDEX_CURRENT_RATIO),
                        numerator_metric_key='current_assets',
                        denominator_metric_key='current_liabilities',
                    ),
                    gross_margin=_ratio_or_derived(
                        **ratio_arguments,
                        business_year=selected_annual_year,
                        ratio_metric_keys=(CANONICAL_GROSS_MARGIN, INDEX_GROSS_MARGIN),
                        numerator_metric_key='gross_profit',
                        denominator_metric_key='revenue',
                    ),
                    prior_gross_margin=_ratio_or_derived(
                        **ratio_arguments,
                        business_year=selected_prior_annual_year,
                        ratio_metric_keys=(CANONICAL_GROSS_MARGIN, INDEX_GROSS_MARGIN),
                        numerator_metric_key='gross_profit',
                        denominator_metric_key='revenue',
                    ),
                    asset_turnover=_ratio_or_derived(
                        **ratio_arguments,
                        business_year=selected_annual_year,
                        ratio_metric_keys=(CANONICAL_ASSET_TURNOVER, INDEX_ASSET_TURNOVER),
                        numerator_metric_key='revenue',
                        denominator_metric_key='total_assets',
                    ),
                    prior_asset_turnover=_ratio_or_derived(
                        **ratio_arguments,
                        business_year=selected_prior_annual_year,
                        ratio_metric_keys=(CANONICAL_ASSET_TURNOVER, INDEX_ASSET_TURNOVER),
                        numerator_metric_key='revenue',
                        denominator_metric_key='total_assets',
                    ),
                    has_audit_issue=(
                        None if symbol not in latest_audit else bool(latest_audit.get(symbol))
                    ),
                    has_dilution_signal=(
                        None if symbol not in latest_dilution else bool(latest_dilution.get(symbol))
                    ),
                    annual_year=selected_annual_year,
                    prior_annual_year=selected_prior_annual_year,
                    fundamentals_provenance=provenance,
                    audit_provenance=audit_provenance_by_symbol.get(symbol),
                    dilution_provenance=dilution_provenance_by_symbol.get(symbol),
                    valuation_source=valuation_source,
                    valuation_provenance=valuation_provenance,
                )
            )
        return inputs


daily_piotroski_filter_service = DailyPiotroskiFilterService()
