from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from sqlalchemy import and_, case, func, select

from app.db import session_scope
from app.db_models import (
    DailyPriceBar,
    DailyTechnicalSnapshot,
    DartAuditSignal,
    DartDilutionSignal,
    DartFundamentalsHighlight,
    DartFundamentalsSnapshot,
    Symbol,
)
from app.market_data_store import (
    load_daily_piotroski_filter_results,
    load_latest_available_daily_piotroski_filter_result_date,
    upsert_daily_piotroski_filter_result,
)

FILTER_SOURCE = 'strategy_piotroski_v1'
LOW_PBR_PERCENTILE = 0.30
BOTTOM_MOMENTUM_PERCENTILE = 0.30
PIOTROSKI_PASS_SCORE = 7
LATEST_FALLBACK_POLICY = 'latest_on_or_before_requested_market_date'

INDEX_GROSS_MARGIN = 'M211300'
INDEX_CURRENT_RATIO = 'M221200'
INDEX_ASSET_TURNOVER = 'M241000'
INDEX_DEBT_RATIO = 'M221100'


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
    has_audit_issue: bool
    has_dilution_signal: bool | None


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


class DailyPiotroskiFilterService:
    def rebuild_for_market_date(self, *, market_date: str | date) -> dict[str, Any]:
        normalized_market_date = date.fromisoformat(market_date) if isinstance(market_date, str) else market_date
        annual_year = normalized_market_date.year - 1
        prior_annual_year = annual_year - 1
        inputs = self._load_inputs(normalized_market_date, annual_year=annual_year, prior_annual_year=prior_annual_year)
        pbr_values = [item.pbr for item in inputs if item.pbr is not None and item.pbr > 0]
        momentum_values = [item.return_20d for item in inputs if item.return_20d is not None]
        passed_count = 0
        persisted_count = 0
        failed_symbols: list[str] = []
        failed_symbol_errors: list[dict[str, str]] = []

        for item in inputs:
            valuation_available = item.pbr is not None and item.pbr > 0
            valuation_source_status = 'available' if valuation_available else 'missing_pbr'
            pbr_rank = _percentile_rank(pbr_values, item.pbr, ascending=True) if item.pbr in pbr_values else None
            momentum_rank = _percentile_rank(momentum_values, item.return_20d, ascending=True) if item.return_20d in momentum_values else None
            signals, score, available_signal_count = _compute_signals(item)
            is_low_pbr = pbr_rank is not None and pbr_rank <= LOW_PBR_PERCENTILE
            has_profitability_issue = item.net_income is None or item.net_income <= 0
            has_capital_impairment = item.total_equity is None or item.total_equity <= 0
            has_audit_issue = item.has_audit_issue
            score_ratio = (score / available_signal_count) if available_signal_count else 0.0
            passes_piotroski_score = available_signal_count > 0 and score_ratio >= (PIOTROSKI_PASS_SCORE / 9.0)
            is_bottom_momentum = momentum_rank is not None and momentum_rank <= BOTTOM_MOMENTUM_PERCENTILE
            passed_filter = valuation_available and all([
                is_low_pbr,
                not has_profitability_issue,
                not has_capital_impairment,
                not has_audit_issue,
                passes_piotroski_score,
                not is_bottom_momentum,
            ])
            if passed_filter:
                passed_count += 1
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
                    'signals': {key: {'value': result.value, 'available': result.available} for key, result in signals.items()},
                    'score_summary': {
                        'score': score,
                        'available_signal_count': available_signal_count,
                        'score_ratio': score_ratio,
                        'normalized_pass_ratio': PIOTROSKI_PASS_SCORE / 9.0,
                    },
                    'thresholds': {
                        'low_pbr_percentile': LOW_PBR_PERCENTILE,
                        'piotroski_min_score': PIOTROSKI_PASS_SCORE,
                        'bottom_momentum_percentile': BOTTOM_MOMENTUM_PERCENTILE,
                        'annual_year': annual_year,
                        'prior_annual_year': prior_annual_year,
                    },
                    'valuation': {
                        'available': valuation_available,
                        'source_status': valuation_source_status,
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
            try:
                upsert_daily_piotroski_filter_result(payload)
                persisted_count += 1
            except Exception as exc:
                failed_symbols.append(item.symbol)
                failed_symbol_errors.append({'symbol': item.symbol, 'error': str(exc)})

        return {
            'market_date': normalized_market_date.isoformat(),
            'source': FILTER_SOURCE,
            'symbols_evaluated': len(inputs),
            'symbols_excluded_missing_valuation': sum(1 for item in inputs if item.pbr is None or item.pbr <= 0),
            'symbols_passed': passed_count,
            'symbols_not_passed': len(inputs) - passed_count,
            'results_persisted': persisted_count,
            'results_failed': len(failed_symbols),
            'failed_symbols': failed_symbols,
            'failed_symbol_errors': failed_symbol_errors,
        }

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
            'matched_symbols': matched_symbols,
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

    def _load_inputs(self, market_date: date, *, annual_year: int, prior_annual_year: int) -> list[PiotroskiInputs]:
        with session_scope() as session:
            latest_highlights = (
                select(
                    DartFundamentalsSnapshot.stock_code.label('symbol'),
                    DartFundamentalsSnapshot.business_year.label('business_year'),
                    DartFundamentalsHighlight.metric_key.label('metric_key'),
                    DartFundamentalsHighlight.current_amount.label('current_amount'),
                    DartFundamentalsHighlight.ratio_value.label('ratio_value'),
                )
                .join(DartFundamentalsHighlight, DartFundamentalsHighlight.snapshot_id == DartFundamentalsSnapshot.id)
                .where(
                    DartFundamentalsSnapshot.stock_code.is_not(None),
                    DartFundamentalsSnapshot.business_year.in_([annual_year, prior_annual_year]),
                    DartFundamentalsSnapshot.report_code == '11011',
                    DartFundamentalsSnapshot.fs_div == 'CFS',
                )
                .subquery()
            )
            latest_audit = (
                select(
                    DartAuditSignal.stock_code.label('symbol'),
                    DartAuditSignal.has_audit_issue.label('has_audit_issue'),
                    func.row_number().over(
                        partition_by=DartAuditSignal.stock_code,
                        order_by=DartAuditSignal.receipt_date.desc(),
                    ).label('rk'),
                ).subquery()
            )
            latest_dilution = (
                select(
                    DartDilutionSignal.stock_code.label('symbol'),
                    DartDilutionSignal.has_dilution_signal.label('has_dilution_signal'),
                    func.row_number().over(
                        partition_by=DartDilutionSignal.stock_code,
                        order_by=DartDilutionSignal.receipt_date.desc(),
                    ).label('rk'),
                ).subquery()
            )

            stmt = (
                select(
                    Symbol.symbol,
                    Symbol.name_ko,
                    DailyPriceBar.pbr,
                    DailyTechnicalSnapshot.return_20d,
                    latest_audit.c.has_audit_issue,
                    latest_dilution.c.has_dilution_signal,
                    func.max(case((and_(latest_highlights.c.metric_key == 'total_equity', latest_highlights.c.business_year == annual_year), latest_highlights.c.current_amount), else_=None)).label('total_equity'),
                    func.max(case((and_(latest_highlights.c.metric_key == 'net_income', latest_highlights.c.business_year == annual_year), latest_highlights.c.current_amount), else_=None)).label('net_income'),
                    func.max(case((and_(latest_highlights.c.metric_key == 'net_income', latest_highlights.c.business_year == prior_annual_year), latest_highlights.c.current_amount), else_=None)).label('prior_net_income'),
                    func.max(case((and_(latest_highlights.c.metric_key == 'operating_cash_flow', latest_highlights.c.business_year == annual_year), latest_highlights.c.current_amount), else_=None)).label('operating_cash_flow'),
                    func.max(case((and_(latest_highlights.c.metric_key == 'total_assets', latest_highlights.c.business_year == annual_year), latest_highlights.c.current_amount), else_=None)).label('total_assets'),
                    func.max(case((and_(latest_highlights.c.metric_key == 'total_assets', latest_highlights.c.business_year == prior_annual_year), latest_highlights.c.current_amount), else_=None)).label('prior_total_assets'),
                    func.max(case((and_(latest_highlights.c.metric_key == INDEX_DEBT_RATIO, latest_highlights.c.business_year == annual_year), latest_highlights.c.ratio_value), else_=None)).label('debt_ratio'),
                    func.max(case((and_(latest_highlights.c.metric_key == INDEX_DEBT_RATIO, latest_highlights.c.business_year == prior_annual_year), latest_highlights.c.ratio_value), else_=None)).label('prior_debt_ratio'),
                    func.max(case((and_(latest_highlights.c.metric_key == INDEX_CURRENT_RATIO, latest_highlights.c.business_year == annual_year), latest_highlights.c.ratio_value), else_=None)).label('current_ratio'),
                    func.max(case((and_(latest_highlights.c.metric_key == INDEX_CURRENT_RATIO, latest_highlights.c.business_year == prior_annual_year), latest_highlights.c.ratio_value), else_=None)).label('prior_current_ratio'),
                    func.max(case((and_(latest_highlights.c.metric_key == INDEX_GROSS_MARGIN, latest_highlights.c.business_year == annual_year), latest_highlights.c.ratio_value), else_=None)).label('gross_margin'),
                    func.max(case((and_(latest_highlights.c.metric_key == INDEX_GROSS_MARGIN, latest_highlights.c.business_year == prior_annual_year), latest_highlights.c.ratio_value), else_=None)).label('prior_gross_margin'),
                    func.max(case((and_(latest_highlights.c.metric_key == INDEX_ASSET_TURNOVER, latest_highlights.c.business_year == annual_year), latest_highlights.c.ratio_value), else_=None)).label('asset_turnover'),
                    func.max(case((and_(latest_highlights.c.metric_key == INDEX_ASSET_TURNOVER, latest_highlights.c.business_year == prior_annual_year), latest_highlights.c.ratio_value), else_=None)).label('prior_asset_turnover'),
                )
                .join(DailyPriceBar, and_(DailyPriceBar.symbol_id == Symbol.id, DailyPriceBar.trade_date == market_date))
                .join(DailyTechnicalSnapshot, and_(DailyTechnicalSnapshot.symbol_id == Symbol.id, DailyTechnicalSnapshot.market_date == market_date), isouter=True)
                .join(latest_highlights, latest_highlights.c.symbol == Symbol.symbol, isouter=True)
                .join(latest_audit, and_(latest_audit.c.symbol == Symbol.symbol, latest_audit.c.rk == 1), isouter=True)
                .join(latest_dilution, and_(latest_dilution.c.symbol == Symbol.symbol, latest_dilution.c.rk == 1), isouter=True)
                .where(Symbol.is_active.is_(True))
                .group_by(Symbol.symbol, Symbol.name_ko, DailyPriceBar.pbr, DailyTechnicalSnapshot.return_20d, latest_audit.c.has_audit_issue, latest_dilution.c.has_dilution_signal)
                .order_by(Symbol.symbol.asc())
            )
            rows = session.execute(stmt).all()

        return [
            PiotroskiInputs(
                symbol=row.symbol,
                name_ko=row.name_ko,
                market_date=market_date,
                pbr=_as_float(row.pbr),
                return_20d=_as_float(row.return_20d),
                total_equity=_as_float(row.total_equity),
                net_income=_as_float(row.net_income),
                prior_net_income=_as_float(row.prior_net_income),
                operating_cash_flow=_as_float(row.operating_cash_flow),
                total_assets=_as_float(row.total_assets),
                prior_total_assets=_as_float(row.prior_total_assets),
                debt_ratio=_as_float(row.debt_ratio),
                prior_debt_ratio=_as_float(row.prior_debt_ratio),
                current_ratio=_as_float(row.current_ratio),
                prior_current_ratio=_as_float(row.prior_current_ratio),
                gross_margin=_as_float(row.gross_margin),
                prior_gross_margin=_as_float(row.prior_gross_margin),
                asset_turnover=_as_float(row.asset_turnover),
                prior_asset_turnover=_as_float(row.prior_asset_turnover),
                has_audit_issue=bool(row.has_audit_issue),
                has_dilution_signal=(None if row.has_dilution_signal is None else bool(row.has_dilution_signal)),
            )
            for row in rows
        ]


daily_piotroski_filter_service = DailyPiotroskiFilterService()
